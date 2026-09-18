from __future__ import annotations

from datetime import date, timedelta
import logging
import os
from pathlib import Path
import tempfile
import traceback
from uuid import uuid4

import polars as pl

from .completion import (
    csv_path,
    day_directory,
    remove_completion_marker,
    validate_completion,
    write_completion_marker,
)
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


def _date_range(start_date: date, end_date: date) -> list[date]:
    day_count = (end_date - start_date).days
    return [start_date + timedelta(days=offset) for offset in range(day_count + 1)]


def _csv_ready_frame(frame: pl.DataFrame) -> pl.DataFrame:
    expressions: list[pl.Expr] = []
    for column, dtype in frame.schema.items():
        if dtype == pl.Date:
            expressions.append(pl.col(column).dt.to_string("%Y-%m-%d"))
        elif isinstance(dtype, pl.Datetime) and dtype.time_zone is not None:
            expressions.append(
                pl.col(column).dt.to_string("%Y-%m-%dT%H:%M:%S%.f%z")
            )
    return frame.with_columns(expressions) if expressions else frame


class StreamingSymbolStager:
    def __init__(self, symbol: str, output_dir: Path) -> None:
        self.symbol = symbol
        self._temporary_directory = tempfile.TemporaryDirectory(
            prefix=f".{symbol}-staging-", dir=output_dir
        )
        self.directory = Path(self._temporary_directory.name)
        self.parts_by_date: dict[date, list[Path]] = {}
        self.rows_by_date: dict[date, int] = {}

    def add(self, frame: pl.DataFrame, data_date: date) -> None:
        if frame.is_empty():
            return
        parts = self.parts_by_date.setdefault(data_date, [])
        path = self.directory / f"{data_date:%Y%m%d}-part-{len(parts):06d}.arrow"
        _csv_ready_frame(frame).write_ipc(path, compression="uncompressed")
        parts.append(path)
        self.rows_by_date[data_date] = self.rows_by_date.get(data_date, 0) + frame.height

    def close(self) -> None:
        self._temporary_directory.cleanup()


def _count_csv_rows(path: Path) -> int:
    line_count = 0
    with path.open("rb", buffering=8 * 1024 * 1024) as csv_file:
        while chunk := csv_file.read(8 * 1024 * 1024):
            line_count += chunk.count(b"\n")
    return max(0, line_count - 1)


def merge_into_daily_csv(
    symbol: str,
    data_date: date,
    parts: list[Path],
    refreshed_expirations: set[str],
    new_row_count: int,
    output_dir: Path,
    *,
    replace_entire_day: bool,
) -> tuple[Path, int]:
    directory = day_directory(output_dir, data_date)
    directory.mkdir(parents=True, exist_ok=True)
    output_path = csv_path(output_dir, symbol, data_date)
    had_existing_file = output_path.exists()

    frames = [pl.scan_ipc(path) for path in parts]
    if had_existing_file and not replace_entire_day:
        existing = pl.scan_csv(
            output_path,
            try_parse_dates=False,
            infer_schema_length=10_000,
        ).filter(~pl.col("expiration").is_in(sorted(refreshed_expirations)))
        frames.append(existing)
    if not frames:
        raise ValueError(f"没有可写入的 CSV 数据源: {symbol} {data_date}")

    combined = pl.concat(frames, how="diagonal_relaxed")
    temporary_path = directory / f".{symbol}.{uuid4().hex}.tmp"
    try:
        combined.sink_csv(
            temporary_path,
            batch_size=65_536,
            maintain_order=True,
        )
        if replace_entire_day or not had_existing_file:
            row_count = new_row_count
        else:
            row_count = _count_csv_rows(temporary_path)
        os.replace(temporary_path, output_path)
    except Exception:
        if temporary_path.exists():
            temporary_path.unlink()
        raise
    return output_path, row_count


class ThetaOptionHarvester(BaseThetaOptionHarvester):
    def run(
        self,
        symbols: tuple[str, ...],
        start_date: date,
        end_date: date,
        *,
        force: bool = False,
    ) -> HarvestResult:
        self._force = force
        return super().run(symbols, start_date, end_date)

    def _run_symbol(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
        result: HarvestResult,
    ) -> None:
        requested_dates = _date_range(start_date, end_date)
        pending_dates: list[date] = []
        skipped_dates: list[date] = []
        for data_date in requested_dates:
            if self._force:
                if remove_completion_marker(self.output_dir, symbol, data_date):
                    LOGGER.info("强制重抓，已移除完成标记: symbol=%s date=%s", symbol, data_date)
                pending_dates.append(data_date)
                continue
            is_complete, reason, marker = validate_completion(
                self.output_dir, symbol, data_date
            )
            if is_complete:
                skipped_dates.append(data_date)
                LOGGER.info(
                    "跳过已完整抓取日期: symbol=%s date=%s status=%s",
                    symbol,
                    data_date,
                    marker["status"] if marker else "unknown",
                )
            else:
                pending_dates.append(data_date)
                LOGGER.info(
                    "日期需要抓取: symbol=%s date=%s reason=%s",
                    symbol,
                    data_date,
                    reason,
                )

        if not pending_dates:
            LOGGER.info(
                "%s 请求范围内 %d 个日期均已完整抓取，不调用 ThetaData API",
                symbol,
                len(skipped_dates),
            )
            return

        LOGGER.info(
            "开始处理 %s，待抓取日期=%s，已跳过日期=%s",
            symbol,
            ",".join(value.isoformat() for value in pending_dates),
            ",".join(value.isoformat() for value in skipped_dates) or "无",
        )
        pending_set = set(pending_dates)
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
            if expiration >= min(pending_dates)
        )
        checked_expirations = {data_date: set() for data_date in pending_dates}
        refreshed_expirations = {data_date: set() for data_date in pending_dates}
        written_expirations = {data_date: set() for data_date in pending_dates}
        incomplete_dates: set[date] = set()

        stager = StreamingSymbolStager(symbol, self.output_dir)
        try:
            for expiration in expirations:
                relevant_dates = {
                    data_date for data_date in pending_dates if data_date <= expiration
                }
                if not relevant_dates:
                    continue
                failure_count = len(result.failures)
                dates = self._discover_dates(
                    symbol,
                    expiration,
                    min(relevant_dates),
                    max(relevant_dates),
                    result,
                )
                if len(result.failures) != failure_count:
                    incomplete_dates.update(relevant_dates)
                    continue
                for data_date in relevant_dates:
                    checked_expirations[data_date].add(expiration)

                for data_date in dates:
                    if data_date not in pending_set:
                        continue
                    merged = self._fetch_batch(symbol, expiration, data_date, result)
                    if merged is None:
                        incomplete_dates.add(data_date)
                        continue
                    refreshed_expirations[data_date].add(expiration)
                    if not merged.is_empty():
                        stager.add(merged, data_date)
                        written_expirations[data_date].add(expiration)
                        LOGGER.info(
                            "完成 symbol=%s expiration=%s date=%s rows=%d",
                            symbol,
                            expiration,
                            data_date,
                            merged.height,
                        )

            written_paths: list[Path] = []
            for data_date in pending_dates:
                is_complete = data_date not in incomplete_dates
                parts = stager.parts_by_date.get(data_date, [])
                output_path = csv_path(self.output_dir, symbol, data_date)
                refreshed = refreshed_expirations[data_date]
                should_write_partial = bool(parts) or (
                    not is_complete and output_path.exists() and bool(refreshed)
                )

                if is_complete and not parts:
                    try:
                        if output_path.exists():
                            output_path.unlink()
                        marker = write_completion_marker(
                            self.output_dir,
                            symbol,
                            data_date,
                            status="no_data",
                            row_count=0,
                            checked_expirations=checked_expirations[data_date],
                            written_expirations=set(),
                        )
                        LOGGER.info(
                            "日期完整但无数据，已写完成标记: symbol=%s date=%s marker=%s",
                            symbol,
                            data_date,
                            marker,
                        )
                    except Exception as exc:
                        LOGGER.error(
                            "写入无数据完成标记失败: symbol=%s date=%s\n%s",
                            symbol,
                            data_date,
                            traceback.format_exc(),
                        )
                        result.failures.append(
                            FailedRequest(
                                "completion_marker:no_data",
                                symbol,
                                data_date=data_date,
                                error=str(exc),
                            )
                        )
                    continue

                if is_complete or should_write_partial:
                    try:
                        output_path, row_count = merge_into_daily_csv(
                            symbol,
                            data_date,
                            parts,
                            {value.isoformat() for value in refreshed},
                            stager.rows_by_date.get(data_date, 0),
                            self.output_dir,
                            replace_entire_day=is_complete,
                        )
                        written_paths.append(output_path)
                        result.rows_by_file[str(output_path)] = row_count
                        LOGGER.info("写入完成: %s，共 %d 行", output_path, row_count)
                    except Exception as exc:
                        incomplete_dates.add(data_date)
                        LOGGER.error(
                            "写入每日 CSV 失败: symbol=%s date=%s\n%s",
                            symbol,
                            data_date,
                            traceback.format_exc(),
                        )
                        result.failures.append(
                            FailedRequest(
                                "daily_csv_write",
                                symbol,
                                data_date=data_date,
                                error=str(exc),
                            )
                        )
                        continue

                if data_date in incomplete_dates:
                    LOGGER.warning(
                        "日期未完整抓取，不写完成标记: symbol=%s date=%s",
                        symbol,
                        data_date,
                    )
                    continue

                try:
                    marker = write_completion_marker(
                        self.output_dir,
                        symbol,
                        data_date,
                        status="complete",
                        row_count=result.rows_by_file[str(output_path)],
                        checked_expirations=checked_expirations[data_date],
                        written_expirations=written_expirations[data_date],
                    )
                    LOGGER.info(
                        "日期完整抓取成功: symbol=%s date=%s marker=%s",
                        symbol,
                        data_date,
                        marker,
                    )
                except Exception as exc:
                    LOGGER.error(
                        "写入完成标记失败: symbol=%s date=%s\n%s",
                        symbol,
                        data_date,
                        traceback.format_exc(),
                    )
                    result.failures.append(
                        FailedRequest(
                            "completion_marker",
                            symbol,
                            data_date=data_date,
                            error=str(exc),
                        )
                    )

            if written_paths:
                result.written_files[symbol] = written_paths
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
