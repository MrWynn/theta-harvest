from __future__ import annotations

from datetime import date
import logging
from pathlib import Path
import tempfile
import time

import polars as pl

from .clickhouse_storage import DATA_COLUMNS, ClickHouseStorage
from .pipeline import (
    FailedRequest,
    HarvestResult,
    DateDiscoveryJob,
    HistoryJob,
    ThetaOptionHarvester as BaseThetaOptionHarvester,
    _date_values,
    _to_polars,
    call_with_retry,
)
from .streaming_pipeline import _date_range


LOGGER = logging.getLogger(__name__)
FLOAT_COLUMNS = {
    "open", "high", "low", "close", "vwap", "bid", "ask", "delta",
    "gamma", "theta", "vega", "rho", "underlying_price",
}
UINT32_COLUMNS = {"count", "bid_size", "bid_condition", "ask_size", "ask_condition"}
UINT16_COLUMNS = {"bid_exchange", "ask_exchange"}


def _datetime_expression(frame: pl.DataFrame, source: str, target: str) -> pl.Expr:
    dtype = frame.schema[source]
    column = pl.col(source)
    if isinstance(dtype, pl.Datetime):
        if dtype.time_zone is None:
            return column.dt.replace_time_zone("America/New_York").cast(
                pl.Datetime("ms", "America/New_York")
            ).alias(target)
        return column.dt.convert_time_zone("America/New_York").cast(
            pl.Datetime("ms", "America/New_York")
        ).alias(target)
    return column.cast(pl.String).str.to_datetime(
        time_unit="ms", time_zone="America/New_York", strict=False
    ).alias(target)


def _optional_expression(
    frame: pl.DataFrame, source: str, target: str, dtype: pl.DataType
) -> pl.Expr:
    if source not in frame.columns:
        return pl.lit(None, dtype=dtype).alias(target)
    expression = pl.col(source).cast(dtype, strict=False)
    if dtype == pl.Float64:
        expression = pl.when(expression.is_finite()).then(expression).otherwise(None)
    return expression.alias(target)


def prepare_clickhouse_frame(frame: pl.DataFrame, data_date: date) -> pl.DataFrame:
    if frame.is_empty():
        return pl.DataFrame(schema={name: pl.Null for name in DATA_COLUMNS})
    required = {"symbol", "expiration", "strike", "right", "timestamp"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"ClickHouse 数据缺少唯一键字段: {missing}")

    right = (
        pl.col("right")
        .cast(pl.String)
        .str.to_uppercase()
        .replace({"C": "CALL", "P": "PUT"})
        .alias("right")
    )
    expressions: list[pl.Expr] = [
        pl.col("symbol").cast(pl.String),
        pl.col("expiration").cast(pl.Date, strict=False),
        pl.col("strike").cast(pl.Float64, strict=False),
        right,
        _datetime_expression(frame, "timestamp", "timestamp"),
        pl.lit(data_date, dtype=pl.Date).alias("data_date"),
    ]
    for name in DATA_COLUMNS[6:]:
        if name in FLOAT_COLUMNS:
            expressions.append(_optional_expression(frame, name, name, pl.Float64))
        elif name == "volume":
            expressions.append(_optional_expression(frame, name, name, pl.UInt64))
        elif name in UINT32_COLUMNS:
            expressions.append(_optional_expression(frame, name, name, pl.UInt32))
        elif name in UINT16_COLUMNS:
            expressions.append(_optional_expression(frame, name, name, pl.UInt16))
        elif name == "underlying_time":
            source = "underlying_time" if "underlying_time" in frame.columns else "underlying_timestamp"
            if source in frame.columns:
                expressions.append(_datetime_expression(frame, source, name))
            else:
                expressions.append(
                    pl.lit(None, dtype=pl.Datetime("ms", "America/New_York")).alias(name)
                )

    result = frame.select(expressions).select(DATA_COLUMNS)
    invalid = result.filter(
        pl.any_horizontal(
            pl.col(name).is_null()
            for name in ("symbol", "expiration", "strike", "right", "timestamp", "data_date")
        )
        | ~pl.col("strike").is_finite()
        | ~pl.col("right").is_in(["CALL", "PUT"])
    )
    if invalid.height:
        raise ValueError(f"ClickHouse 唯一键或 right 存在 {invalid.height} 行无效值")
    return result


class ClickHouseSymbolStager:
    def __init__(self, symbol: str) -> None:
        self._temporary_directory = tempfile.TemporaryDirectory(
            prefix=f".{symbol}-clickhouse-staging-"
        )
        self.directory = Path(self._temporary_directory.name)
        self.parts_by_date: dict[date, list[Path]] = {}
        self.rows_by_date: dict[date, int] = {}

    def add(self, frame: pl.DataFrame, data_date: date) -> None:
        if frame.is_empty():
            return
        prepared = prepare_clickhouse_frame(frame, data_date)
        parts = self.parts_by_date.setdefault(data_date, [])
        path = self.directory / f"{data_date:%Y%m%d}-part-{len(parts):06d}.arrow"
        prepared.write_ipc(path, compression="uncompressed")
        parts.append(path)
        self.rows_by_date[data_date] = self.rows_by_date.get(data_date, 0) + prepared.height

    def discard_date(self, data_date: date) -> None:
        for path in self.parts_by_date.pop(data_date, []):
            path.unlink(missing_ok=True)
        self.rows_by_date.pop(data_date, None)

    def close(self) -> None:
        self._temporary_directory.cleanup()


class ClickHouseThetaOptionHarvester(BaseThetaOptionHarvester):
    def __init__(
        self,
        client: object,
        storage: ClickHouseStorage,
        completed_dates: set[tuple[str, date]],
        notifier: object,
        max_concurrent_requests: int = 1,
    ) -> None:
        super().__init__(
            client,
            Path(tempfile.gettempdir()),
            notifier,
            max_concurrent_requests,
        )
        self.storage = storage
        self.completed_dates = completed_dates

    def _run_symbol(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
        result: HarvestResult,
    ) -> None:
        requested_dates = _date_range(start_date, end_date)
        pending_dates = [
            value for value in requested_dates if (symbol, value) not in self.completed_dates
        ]
        skipped_dates = [value for value in requested_dates if value not in pending_dates]
        for data_date in skipped_dates:
            LOGGER.info("ClickHouse 跳过已同步日期: symbol=%s date=%s", symbol, data_date)
        if not pending_dates:
            LOGGER.info("%s 请求范围日期均已同步，不调用 ThetaData API", symbol)
            return

        LOGGER.info(
            "开始 ClickHouse 同步 %s，待抓取日期=%s，已跳过日期=%s",
            symbol,
            ",".join(value.isoformat() for value in pending_dates),
            ",".join(value.isoformat() for value in skipped_dates) or "无",
        )
        total_started = time.perf_counter()
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
            result.failures.append(FailedRequest("option_list_expirations", symbol, error=str(exc)))
            return

        expirations = sorted(
            value
            for value in _date_values(expiration_frame, "expiration")
            if value >= min(pending_dates)
        )
        pending_set = set(pending_dates)
        checked = {value: set() for value in pending_dates}
        written = {value: set() for value in pending_dates}
        incomplete: set[date] = set()
        stager = ClickHouseSymbolStager(symbol)
        try:
            discovery_jobs: list[DateDiscoveryJob] = []
            for expiration in expirations:
                relevant = {value for value in pending_dates if value <= expiration}
                if not relevant:
                    continue
                discovery_jobs.append(
                    DateDiscoveryJob(
                        symbol=symbol,
                        expiration=expiration,
                        start_date=min(relevant),
                        end_date=max(relevant),
                        relevant_dates=frozenset(relevant),
                    )
                )

            discovery_started = time.perf_counter()
            history_jobs_by_date: dict[date, list[HistoryJob]] = {
                data_date: [] for data_date in pending_dates
            }
            discovery_failures = 0
            for outcome in self._discover_dates_parallel(discovery_jobs):
                result.failures.extend(outcome.failures)
                if outcome.failures:
                    discovery_failures += 1
                    incomplete.update(outcome.job.relevant_dates)
                    continue
                for data_date in outcome.job.relevant_dates:
                    checked[data_date].add(outcome.job.expiration)
                for data_date in outcome.dates:
                    if data_date not in pending_set:
                        continue
                    history_jobs_by_date[data_date].append(
                        HistoryJob(symbol, outcome.job.expiration, data_date)
                    )
            discovery_elapsed = time.perf_counter() - discovery_started
            LOGGER.info(
                "日期发现阶段完成: symbol=%s workers=%d tasks=%d success=%d "
                "failed=%d elapsed=%.3fs tasks_per_sec=%.3f",
                symbol,
                self.max_concurrent_requests,
                len(discovery_jobs),
                len(discovery_jobs) - discovery_failures,
                discovery_failures,
                discovery_elapsed,
                len(discovery_jobs) / discovery_elapsed if discovery_elapsed else 0.0,
            )

            history_started = time.perf_counter()
            history_successes = 0
            history_failures = 0
            history_task_count = sum(len(jobs) for jobs in history_jobs_by_date.values())
            insert_elapsed = 0.0
            for data_date in pending_dates:
                if data_date in incomplete:
                    LOGGER.warning(
                        "日期发现不完整，不请求历史数据或写入进度: symbol=%s date=%s",
                        symbol, data_date,
                    )
                    continue

                day_jobs = history_jobs_by_date[data_date]
                day_started = time.perf_counter()
                day_successes = 0
                day_failures = 0
                LOGGER.info(
                    "开始历史日批次: symbol=%s date=%s workers=%d tasks=%d",
                    symbol,
                    data_date,
                    self.max_concurrent_requests,
                    len(day_jobs),
                )
                for outcome in self._fetch_history_parallel(day_jobs):
                    result.failures.extend(outcome.failures)
                    expiration = outcome.job.expiration
                    merged = outcome.frame
                    if outcome.failures or merged is None:
                        history_failures += 1
                        day_failures += 1
                        incomplete.add(data_date)
                        continue
                    history_successes += 1
                    day_successes += 1
                    if merged.is_empty():
                        continue
                    try:
                        stager.add(merged, data_date)
                    except Exception as exc:
                        self._notify_error(
                            "arrow_staging",
                            f"symbol={symbol} expiration={expiration} data_date={data_date}",
                            exc,
                        )
                        result.failures.append(
                            FailedRequest(
                                "arrow_staging", symbol, expiration, data_date, str(exc)
                            )
                        )
                        history_failures += 1
                        history_successes -= 1
                        day_failures += 1
                        day_successes -= 1
                        incomplete.add(data_date)
                        continue
                    written[data_date].add(expiration)
                    LOGGER.info(
                        "完成暂存 symbol=%s expiration=%s date=%s rows=%d",
                        symbol, expiration, data_date, merged.height,
                    )
                day_elapsed = time.perf_counter() - day_started
                LOGGER.info(
                    "历史日批次完成: symbol=%s date=%s tasks=%d success=%d "
                    "failed=%d elapsed=%.3fs",
                    symbol,
                    data_date,
                    len(day_jobs),
                    day_successes,
                    day_failures,
                    day_elapsed,
                )

                try:
                    if data_date in incomplete:
                        LOGGER.warning(
                            "日期未完整抓取，不写 ClickHouse 或进度: symbol=%s date=%s",
                            symbol,
                            data_date,
                        )
                        continue
                    parts = stager.parts_by_date.get(data_date, [])
                    insert_started = time.perf_counter()
                    if not parts:
                        self.storage.write_no_data(
                            symbol=symbol,
                            data_date=data_date,
                            checked_expiration_count=len(checked[data_date]),
                        )
                        LOGGER.info(
                            "ClickHouse 无数据进度完成: symbol=%s date=%s",
                            symbol,
                            data_date,
                        )
                    else:
                        row_count = stager.rows_by_date[data_date]
                        self.storage.insert_day(
                            parts,
                            symbol=symbol,
                            data_date=data_date,
                            row_count=row_count,
                            checked_expiration_count=len(checked[data_date]),
                            written_expiration_count=len(written[data_date]),
                        )
                        LOGGER.info(
                            "ClickHouse 日期同步完成: symbol=%s date=%s rows=%d",
                            symbol,
                            data_date,
                            row_count,
                        )
                    insert_elapsed += time.perf_counter() - insert_started
                finally:
                    stager.discard_date(data_date)

            history_elapsed = time.perf_counter() - history_started - insert_elapsed
            LOGGER.info(
                "历史采集阶段完成: symbol=%s workers=%d tasks=%d success=%d "
                "failed=%d elapsed=%.3fs batches_per_sec=%.3f",
                symbol,
                self.max_concurrent_requests,
                history_task_count,
                history_successes,
                history_failures,
                history_elapsed,
                history_task_count / history_elapsed if history_elapsed else 0.0,
            )
            LOGGER.info(
                "ClickHouse 写入阶段完成: symbol=%s elapsed=%.3fs total_elapsed=%.3fs",
                symbol,
                insert_elapsed,
                time.perf_counter() - total_started,
            )
        finally:
            stager.close()
