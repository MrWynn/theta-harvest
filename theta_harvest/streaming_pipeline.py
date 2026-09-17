from __future__ import annotations

import csv
from datetime import date
import logging
import os
from pathlib import Path
import tempfile
import traceback
from uuid import uuid4

import polars as pl

from .pipeline import (
    FailedRequest,
    HarvestResult,
    KEY_COLUMNS,
    ThetaOptionHarvester as BaseThetaOptionHarvester,
    _date_values,
    _to_polars,
    call_with_retry,
)


LOGGER = logging.getLogger(__name__)
DATA_DATE_COLUMN = "data_date"


class StreamingSymbolStager:
    def __init__(self, symbol: str, output_dir: Path) -> None:
        self.symbol = symbol
        self._temporary_directory = tempfile.TemporaryDirectory(
            prefix=f".{symbol}-staging-", dir=output_dir
        )
        self.directory = Path(self._temporary_directory.name)
        self.parts_by_year: dict[int, list[Path]] = {}
        self.partitions_by_year: dict[int, set[tuple[str, str]]] = {}

    def add(self, frame: pl.DataFrame, data_date: date, expiration: date) -> None:
        if frame.is_empty():
            return
        parts = self.parts_by_year.setdefault(data_date.year, [])
        path = self.directory / f"{data_date.year}-part-{len(parts):06d}.csv"
        frame.write_csv(path)
        parts.append(path)
        self.partitions_by_year.setdefault(data_date.year, set()).add(
            (data_date.isoformat(), expiration.isoformat())
        )

    def close(self) -> None:
        self._temporary_directory.cleanup()


def _read_header(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8", newline="") as csv_file:
        reader = csv.reader(csv_file)
        try:
            return next(reader)
        except StopIteration as exc:
            raise ValueError(f"CSV 文件为空: {path}") from exc


def _union_headers(paths: list[Path]) -> list[str]:
    columns: list[str] = []
    seen: set[str] = set()
    for path in paths:
        for column in _read_header(path):
            if column not in seen:
                columns.append(column)
                seen.add(column)
    required = [*KEY_COLUMNS, DATA_DATE_COLUMN]
    missing = [column for column in required if column not in seen]
    if missing:
        raise ValueError(f"CSV 缺少幂等写入字段 {missing}: {paths}")
    return columns


def _copy_rows(
    source: Path,
    writer: csv.DictWriter,
    *,
    skip_partitions: set[tuple[str, str]] | None = None,
) -> int:
    count = 0
    with source.open("r", encoding="utf-8", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            if skip_partitions is not None:
                partition = (row.get(DATA_DATE_COLUMN, ""), row.get("expiration", ""))
                if partition in skip_partitions:
                    continue
            writer.writerow(row)
            count += 1
    return count


def merge_into_year_csv_streaming(
    symbol: str,
    year: int,
    parts: list[Path],
    refreshed_partitions: set[tuple[str, str]],
    output_dir: Path,
) -> tuple[Path, int]:
    year_dir = output_dir / str(year)
    year_dir.mkdir(parents=True, exist_ok=True)
    output_path = year_dir / f"{symbol}.csv"

    header_sources = [parts[0]]
    if output_path.exists():
        header_sources.append(output_path)
    header_sources.extend(parts[1:])
    fieldnames = _union_headers(header_sources)

    temporary_path = year_dir / f".{symbol}.{uuid4().hex}.tmp"
    row_count = 0
    try:
        with temporary_path.open("w", encoding="utf-8", newline="") as csv_file:
            writer = csv.DictWriter(
                csv_file,
                fieldnames=fieldnames,
                extrasaction="ignore",
                restval="",
            )
            writer.writeheader()
            if output_path.exists():
                row_count += _copy_rows(
                    output_path,
                    writer,
                    skip_partitions=refreshed_partitions,
                )
            for part in parts:
                row_count += _copy_rows(part, writer)
        os.replace(temporary_path, output_path)
    except Exception:
        if temporary_path.exists():
            temporary_path.unlink()
        raise
    return output_path, row_count


class ThetaOptionHarvester(BaseThetaOptionHarvester):
    def _run_symbol(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
        result: HarvestResult,
    ) -> None:
        LOGGER.info("开始处理 %s，日期范围 %s 至 %s", symbol, start_date, end_date)
        try:
            expiration_frame = _to_polars(
                call_with_retry(
                    lambda: self.client.option_list_expirations(symbol=symbol),
                    operation_name="option_list_expirations",
                    context=f"symbol={symbol}",
                )
            )
        except Exception as exc:
            result.failures.append(
                FailedRequest("option_list_expirations", symbol, error=str(exc))
            )
            return

        expirations = sorted(
            expiration
            for expiration in _date_values(expiration_frame, "expiration")
            if expiration >= start_date
        )
        if not expirations:
            LOGGER.warning("%s 没有可用于该日期范围的期权到期日", symbol)
            return

        stager = StreamingSymbolStager(symbol, self.output_dir)
        try:
            for expiration in expirations:
                dates = self._discover_dates(symbol, expiration, start_date, end_date, result)
                for data_date in dates:
                    merged = self._fetch_batch(symbol, expiration, data_date, result)
                    if merged is not None and not merged.is_empty():
                        stager.add(merged, data_date, expiration)
                        LOGGER.info(
                            "完成 symbol=%s expiration=%s date=%s rows=%d",
                            symbol,
                            expiration,
                            data_date,
                            merged.height,
                        )

            written_paths: list[Path] = []
            for year, parts in sorted(stager.parts_by_year.items()):
                output_path, row_count = merge_into_year_csv_streaming(
                    symbol,
                    year,
                    parts,
                    stager.partitions_by_year[year],
                    self.output_dir,
                )
                written_paths.append(output_path)
                result.rows_by_file[str(output_path)] = row_count
                LOGGER.info("写入完成: %s，共 %d 行", output_path, row_count)

            if written_paths:
                result.written_files[symbol] = written_paths
            else:
                LOGGER.warning("%s 在指定日期范围内没有历史期权数据", symbol)
        except Exception as exc:
            LOGGER.error("处理 %s 时发生未恢复错误\n%s", symbol, traceback.format_exc())
            result.failures.append(FailedRequest("symbol_pipeline", symbol, error=str(exc)))
        finally:
            stager.close()

    def _fetch_batch(
        self,
        symbol: str,
        expiration: date,
        data_date: date,
        result: HarvestResult,
    ) -> pl.DataFrame | None:
        merged = super()._fetch_batch(symbol, expiration, data_date, result)
        if merged is None or merged.is_empty():
            return merged
        remaining = [column for column in merged.columns if column not in KEY_COLUMNS]
        return merged.with_columns(
            pl.lit(data_date).cast(pl.Date).alias(DATA_DATE_COLUMN)
        ).select([*KEY_COLUMNS, DATA_DATE_COLUMN, *remaining])
