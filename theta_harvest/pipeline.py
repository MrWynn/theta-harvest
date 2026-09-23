from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date
import logging
import os
from pathlib import Path
import tempfile
import time
import traceback
from typing import Any, TypeVar
from uuid import uuid4

import polars as pl


LOGGER = logging.getLogger(__name__)

KEY_COLUMNS = ["symbol", "expiration", "strike", "right", "timestamp"]
SOURCE_PRIORITY = ["quote", "greeks", "ohlc"]
T = TypeVar("T")


@dataclass(frozen=True)
class FailedRequest:
    operation: str
    symbol: str
    expiration: date | None = None
    data_date: date | None = None
    error: str = ""


@dataclass
class HarvestResult:
    written_files: dict[str, list[Path]] = field(default_factory=dict)
    rows_by_file: dict[str, int] = field(default_factory=dict)
    failures: list[FailedRequest] = field(default_factory=list)

    @property
    def exit_code(self) -> int:
        return 1 if self.failures else 0


class RetryExhaustedError(RuntimeError):
    pass


class SymbolStager:
    def __init__(self, symbol: str, output_dir: Path) -> None:
        self.symbol = symbol
        self._temporary_directory = tempfile.TemporaryDirectory(
            prefix=f".{symbol}-staging-", dir=output_dir
        )
        self.directory = Path(self._temporary_directory.name)
        self.parts_by_year: dict[int, list[Path]] = {}

    def add(self, frame: pl.DataFrame, year: int) -> None:
        if frame.is_empty():
            return
        parts = self.parts_by_year.setdefault(year, [])
        path = self.directory / f"{year}-part-{len(parts):06d}.csv"
        frame.write_csv(path)
        parts.append(path)

    def close(self) -> None:
        self._temporary_directory.cleanup()


def call_with_retry(
    operation: Callable[[], T],
    *,
    operation_name: str,
    context: str,
    attempts: int = 5,
    on_exhausted: Callable[[str, str, BaseException], None] | None = None,
) -> T:
    for attempt in range(1, attempts + 1):
        try:
            return operation()
        except Exception as exc:
            LOGGER.exception(
                "%s 失败 (%s)，第 %d/%d 次尝试: %s: %s",
                operation_name,
                context,
                attempt,
                attempts,
                type(exc).__name__,
                exc,
            )
            if attempt == attempts:
                exhausted = RetryExhaustedError(
                    f"{operation_name} 在 {attempts} 次尝试后仍失败 ({context})"
                )
                if on_exhausted is not None:
                    on_exhausted(operation_name, context, exc)
                    try:
                        setattr(exhausted, "_lark_notified", True)
                    except Exception:
                        pass
                raise exhausted from exc
            delay = min(2 ** (attempt - 1), 60)
            LOGGER.warning("%d 秒后重试 %s (%s)", delay, operation_name, context)
            time.sleep(delay)

    raise AssertionError("unreachable")


def _to_polars(value: Any) -> pl.DataFrame:
    if isinstance(value, pl.DataFrame):
        return value
    if value.__class__.__module__.startswith("pandas"):
        return pl.from_pandas(value)
    raise TypeError(f"ThetaData 返回了不支持的数据类型: {type(value).__name__}")


def _date_values(frame: pl.DataFrame, column: str) -> set[date]:
    if frame.is_empty() or column not in frame.columns:
        return set()
    values: set[date] = set()
    for value in frame.get_column(column).to_list():
        if value is None:
            continue
        if isinstance(value, date):
            values.add(value)
        else:
            values.add(date.fromisoformat(str(value)[:10]))
    return values


def _normalize_history(frame: pl.DataFrame, symbol: str) -> pl.DataFrame:
    if frame.is_empty():
        return frame

    missing = [column for column in KEY_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError(f"历史接口响应缺少唯一键字段: {missing}")

    return frame.with_columns(
        pl.lit(symbol).alias("symbol"),
        pl.col("expiration").cast(pl.Date, strict=False),
        pl.col("strike").cast(pl.Float64, strict=False),
        pl.col("right").cast(pl.String, strict=False),
    ).unique(subset=KEY_COLUMNS, keep="last", maintain_order=True)


def merge_history_frames(
    ohlc: pl.DataFrame,
    quote: pl.DataFrame,
    greeks: pl.DataFrame,
    *,
    symbol: str,
    context: str,
) -> pl.DataFrame:
    frames = {
        "ohlc": _normalize_history(ohlc, symbol),
        "quote": _normalize_history(quote, symbol),
        "greeks": _normalize_history(greeks, symbol),
    }
    non_empty = {name: frame for name, frame in frames.items() if not frame.is_empty()}
    if not non_empty:
        return pl.DataFrame()

    keys = pl.concat(
        [frame.select(KEY_COLUMNS) for frame in non_empty.values()],
        how="vertical_relaxed",
    ).unique(subset=KEY_COLUMNS, maintain_order=True)

    occurrences: dict[str, list[str]] = {}
    for source, frame in non_empty.items():
        for column in frame.columns:
            if column not in KEY_COLUMNS:
                occurrences.setdefault(column, []).append(source)

    merged = keys
    for source, frame in non_empty.items():
        rename = {
            column: f"{column}__{source}"
            for column in frame.columns
            if column not in KEY_COLUMNS and len(occurrences[column]) > 1
        }
        merged = merged.join(
            frame.rename(rename),
            on=KEY_COLUMNS,
            how="left",
            validate="1:1",
        )

    for column, sources in occurrences.items():
        if len(sources) == 1:
            continue
        ordered_sources = [source for source in SOURCE_PRIORITY if source in sources]
        temporary_columns = [f"{column}__{source}" for source in ordered_sources]

        if column in {"bid", "ask"} and "quote" in sources and "greeks" in sources:
            disagreement_count = merged.filter(
                pl.col(f"{column}__quote").is_not_null()
                & pl.col(f"{column}__greeks").is_not_null()
                & (pl.col(f"{column}__quote") != pl.col(f"{column}__greeks"))
            ).height
            if disagreement_count:
                LOGGER.warning(
                    "%s 的 %s 有 %d 行 Quote/Greeks 值不一致，保留 Quote 值",
                    context,
                    column,
                    disagreement_count,
                )

        merged = merged.with_columns(
            pl.coalesce([pl.col(name) for name in temporary_columns]).alias(column)
        ).drop(temporary_columns)

    ordered_columns = KEY_COLUMNS.copy()
    for source in ("ohlc", "quote", "greeks"):
        for column in frames[source].columns:
            if column not in ordered_columns and column in merged.columns:
                ordered_columns.append(column)
    ordered_columns.extend(
        column for column in merged.columns if column not in ordered_columns
    )
    return merged.select(ordered_columns).sort(
        ["timestamp", "expiration", "strike", "right"]
    )


def merge_into_year_csv(
    symbol: str,
    year: int,
    parts: list[Path],
    output_dir: Path,
) -> tuple[Path, int]:
    year_dir = output_dir / str(year)
    year_dir.mkdir(parents=True, exist_ok=True)
    output_path = year_dir / f"{symbol}.csv"

    sources: list[Path] = []
    if output_path.exists():
        sources.append(output_path)
    sources.extend(parts)
    if not sources:
        return output_path, 0

    lazy_frames = [
        pl.scan_csv(path, try_parse_dates=True, infer_schema_length=10_000)
        for path in sources
    ]
    combined = pl.concat(lazy_frames, how="diagonal_relaxed").unique(
        subset=KEY_COLUMNS,
        keep="last",
    ).sort(["timestamp", "expiration", "strike", "right"])

    temporary_path = year_dir / f".{symbol}.{uuid4().hex}.tmp"
    try:
        combined.sink_csv(temporary_path)
        row_count = pl.scan_csv(temporary_path).select(pl.len()).collect().item()
        os.replace(temporary_path, output_path)
    except Exception:
        if temporary_path.exists():
            temporary_path.unlink()
        raise
    return output_path, row_count


class ThetaOptionHarvester:
    def __init__(self, client: Any, output_dir: Path, notifier: Any | None = None) -> None:
        self.client = client
        self.output_dir = output_dir
        self.notifier = notifier
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _notify_error(
        self, operation: str, context: str, exc: BaseException
    ) -> None:
        if self.notifier is not None:
            self.notifier.notify_error(operation, context, exc)

    def run(self, symbols: tuple[str, ...], start_date: date, end_date: date) -> HarvestResult:
        result = HarvestResult()
        for symbol in symbols:
            self._run_symbol(symbol, start_date, end_date, result)

        if result.failures:
            LOGGER.error("共有 %d 个请求批次最终失败:", len(result.failures))
            for failure in result.failures:
                LOGGER.error(
                    "operation=%s symbol=%s expiration=%s date=%s error=%s",
                    failure.operation,
                    failure.symbol,
                    failure.expiration,
                    failure.data_date,
                    failure.error,
                )
        return result

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
                    on_exhausted=self._notify_error,
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

        stager = SymbolStager(symbol, self.output_dir)
        try:
            for expiration in expirations:
                dates = self._discover_dates(symbol, expiration, start_date, end_date, result)
                for data_date in dates:
                    merged = self._fetch_batch(symbol, expiration, data_date, result)
                    if merged is not None and not merged.is_empty():
                        stager.add(merged, data_date.year)
                        LOGGER.info(
                            "完成 symbol=%s expiration=%s date=%s rows=%d",
                            symbol,
                            expiration,
                            data_date,
                            merged.height,
                        )

            written_paths: list[Path] = []
            for year, parts in sorted(stager.parts_by_year.items()):
                output_path, row_count = merge_into_year_csv(
                    symbol, year, parts, self.output_dir
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

    def _discover_dates(
        self,
        symbol: str,
        expiration: date,
        start_date: date,
        end_date: date,
        result: HarvestResult,
    ) -> list[date]:
        available_dates: set[date] = set()
        for request_type in ("trade", "quote"):
            context = f"symbol={symbol} expiration={expiration} type={request_type}"
            try:
                frame = _to_polars(
                    call_with_retry(
                        lambda request_type=request_type: self.client.option_list_dates(
                            request_type=request_type,
                            symbol=symbol,
                            expiration=expiration,
                            strike="*",
                            right="both",
                        ),
                        operation_name="option_list_dates",
                        context=context,
                        on_exhausted=self._notify_error,
                    )
                )
                available_dates.update(_date_values(frame, "date"))
            except Exception as exc:
                result.failures.append(
                    FailedRequest(
                        f"option_list_dates:{request_type}",
                        symbol,
                        expiration=expiration,
                        error=str(exc),
                    )
                )
                return []

        return sorted(
            data_date
            for data_date in available_dates
            if start_date <= data_date <= end_date and data_date <= expiration
        )

    def _fetch_batch(
        self,
        symbol: str,
        expiration: date,
        data_date: date,
        result: HarvestResult,
    ) -> pl.DataFrame | None:
        context = f"symbol={symbol} expiration={expiration} date={data_date}"
        common_arguments = {
            "symbol": symbol,
            "expiration": expiration,
            "date": data_date,
            "interval": "1m",
            "strike": "*",
            "right": "both",
        }
        operations = {
            "ohlc": self.client.option_history_ohlc,
            "quote": self.client.option_history_quote,
            "greeks": self.client.option_history_greeks_all,
        }
        frames: dict[str, pl.DataFrame] = {}
        for name, operation in operations.items():
            try:
                frames[name] = _to_polars(
                    call_with_retry(
                        lambda operation=operation: operation(**common_arguments),
                        operation_name=f"option_history_{name}",
                        context=context,
                        on_exhausted=self._notify_error,
                    )
                )
            except Exception as exc:
                result.failures.append(
                    FailedRequest(
                        f"option_history_{name}",
                        symbol,
                        expiration=expiration,
                        data_date=data_date,
                        error=str(exc),
                    )
                )
                return None

        try:
            return merge_history_frames(
                frames["ohlc"],
                frames["quote"],
                frames["greeks"],
                symbol=symbol,
                context=context,
            )
        except Exception as exc:
            LOGGER.error("拼接失败 (%s)\n%s", context, traceback.format_exc())
            self._notify_error("merge_history", context, exc)
            result.failures.append(
                FailedRequest(
                    "merge_history",
                    symbol,
                    expiration=expiration,
                    data_date=data_date,
                    error=str(exc),
                )
            )
            return None
