from __future__ import annotations

from datetime import date
import logging
from pathlib import Path
import re
from typing import Any, Iterable

import polars as pl

from .config import ClickHouseConfig


LOGGER = logging.getLogger(__name__)
INSERT_BATCH_SIZE = 250_000

DATA_COLUMNS = [
    "symbol", "expiration", "strike", "right", "timestamp", "data_date",
    "open", "high", "low", "close", "volume", "count", "vwap",
    "bid_size", "bid_exchange", "bid", "bid_condition", "ask_size",
    "ask_exchange", "ask", "ask_condition", "delta", "gamma", "theta",
    "vega", "rho", "underlying_time", "underlying_price",
]

EXPECTED_DATA_TYPES = {
    "symbol": "LowCardinality(String)",
    "expiration": "Date",
    "strike": "Float64",
    "right": "Enum8('CALL' = 1, 'PUT' = 2)",
    "timestamp": "DateTime64(3, 'America/New_York')",
    "data_date": "Date",
    "open": "Nullable(Float64)", "high": "Nullable(Float64)",
    "low": "Nullable(Float64)", "close": "Nullable(Float64)",
    "volume": "Nullable(UInt64)", "count": "Nullable(UInt32)",
    "vwap": "Nullable(Float64)", "bid_size": "Nullable(UInt32)",
    "bid_exchange": "Nullable(UInt16)", "bid": "Nullable(Float64)",
    "bid_condition": "Nullable(UInt32)", "ask_size": "Nullable(UInt32)",
    "ask_exchange": "Nullable(UInt16)", "ask": "Nullable(Float64)",
    "ask_condition": "Nullable(UInt32)", "delta": "Nullable(Float64)",
    "gamma": "Nullable(Float64)", "theta": "Nullable(Float64)",
    "vega": "Nullable(Float64)", "rho": "Nullable(Float64)",
    "underlying_time": "Nullable(DateTime64(3, 'America/New_York'))",
    "underlying_price": "Nullable(Float64)",
}

EXPECTED_PROGRESS_TYPES = {
    "symbol": "LowCardinality(String)",
    "data_date": "Date",
    "status": "Enum8('complete' = 1, 'no_data' = 2)",
    "row_count": "UInt64",
    "checked_expiration_count": "UInt32",
    "written_expiration_count": "UInt32",
    "schema_version": "UInt16",
    "completed_at": "DateTime64(3, 'UTC')",
}


def _identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"非法 ClickHouse 标识符: {value!r}")
    return value


class ClickHouseStorage:
    def __init__(
        self,
        config: ClickHouseConfig,
        notifier: Any,
        *,
        client: Any | None = None,
    ) -> None:
        self.config = config
        self.notifier = notifier
        self.database = _identifier(config.database)
        self.data_table = _identifier(config.data_table)
        self.progress_table = _identifier(config.progress_table)
        if client is None:
            from clickhouse_driver import Client

            client = Client(
                host=config.host,
                port=config.port,
                user=config.user,
                password=config.password,
                database=config.database,
                compression=True,
                connect_timeout=10,
                send_receive_timeout=900,
            )
        self.client = client

    @property
    def data_fqtn(self) -> str:
        return f"{self.database}.{self.data_table}"

    @property
    def progress_fqtn(self) -> str:
        return f"{self.database}.{self.progress_table}"

    def initialize(self, schema_path: Path) -> None:
        sql = schema_path.read_text(encoding="utf-8")
        sql = sql.replace(
            "laevitas.thetadata_options_chain_1m_progress", self.progress_fqtn
        ).replace("laevitas.thetadata_options_chain_1m", self.data_fqtn)
        for statement in (value.strip() for value in sql.split(";") if value.strip()):
            self.client.execute(statement)
        self._validate_table(self.data_fqtn, EXPECTED_DATA_TYPES)
        self._validate_table(self.progress_fqtn, EXPECTED_PROGRESS_TYPES)
        self._validate_engine(
            self.data_table,
            partition_key="toYYYYMM(data_date)",
            primary_key="symbol, data_date, expiration",
            sorting_key="symbol, data_date, expiration, right, strike, timestamp",
        )
        self._validate_engine(
            self.progress_table,
            partition_key="toYYYY(data_date)",
            primary_key="symbol, data_date",
            sorting_key="symbol, data_date",
        )

    def _validate_table(self, fqtn: str, expected: dict[str, str]) -> None:
        rows = self.client.execute(f"DESCRIBE TABLE {fqtn}")
        actual = {row[0]: row[1] for row in rows}
        if actual != expected:
            missing = sorted(set(expected) - set(actual))
            extra = sorted(set(actual) - set(expected))
            mismatched = {
                name: {"expected": expected[name], "actual": actual[name]}
                for name in expected.keys() & actual.keys()
                if expected[name] != actual[name]
            }
            raise RuntimeError(
                f"ClickHouse 表结构不兼容: table={fqtn} "
                f"missing={missing} extra={extra} mismatched={mismatched}"
            )

    def _validate_engine(
        self,
        table: str,
        *,
        partition_key: str,
        primary_key: str,
        sorting_key: str,
    ) -> None:
        rows = self.client.execute(
            "SELECT engine, partition_key, primary_key, sorting_key "
            "FROM system.tables WHERE database = %(database)s AND name = %(table)s",
            {"database": self.database, "table": table},
        )
        if len(rows) != 1:
            raise RuntimeError(f"无法读取 ClickHouse 表元数据: {self.database}.{table}")
        actual_engine, actual_partition, actual_primary, actual_sorting = rows[0]
        def normalize(value: object) -> str:
            normalized = "".join(str(value).replace("`", "").split())
            return normalized.replace("toYYYY(", "toYear(")
        expected = (
            "ReplacingMergeTree",
            normalize(partition_key),
            normalize(primary_key),
            normalize(sorting_key),
        )
        actual = (
            str(actual_engine),
            normalize(actual_partition),
            normalize(actual_primary),
            normalize(actual_sorting),
        )
        if actual != expected:
            raise RuntimeError(
                f"ClickHouse 表引擎或键不兼容: table={self.database}.{table} "
                f"expected={expected} actual={actual}"
            )

    def completed_dates(
        self, symbols: tuple[str, ...], start_date: date, end_date: date
    ) -> set[tuple[str, date]]:
        if not symbols:
            return set()
        rows = self.client.execute(
            f"SELECT symbol, data_date FROM {self.progress_fqtn} FINAL "
            "WHERE schema_version = 1 AND symbol IN %(symbols)s "
            "AND data_date BETWEEN %(start)s AND %(end)s",
            {"symbols": symbols, "start": start_date, "end": end_date},
        )
        return {(str(symbol), data_date) for symbol, data_date in rows}

    def insert_day(
        self,
        parts: Iterable[Path],
        *,
        symbol: str,
        data_date: date,
        row_count: int,
        checked_expiration_count: int,
        written_expiration_count: int,
    ) -> None:
        inserted = 0
        query = f"INSERT INTO {self.data_fqtn} ({', '.join(DATA_COLUMNS)}) VALUES"
        try:
            for path in parts:
                batches = pl.scan_ipc(path).select(DATA_COLUMNS).collect_batches(
                    chunk_size=INSERT_BATCH_SIZE,
                    engine="streaming",
                )
                for batch in batches:
                    columns = [batch.get_column(name).to_list() for name in DATA_COLUMNS]
                    self.client.execute(
                        query,
                        columns,
                        columnar=True,
                        types_check=False,
                    )
                    inserted += batch.height
                    LOGGER.info(
                        "ClickHouse 批量写入: symbol=%s date=%s batch_rows=%d inserted=%d/%d",
                        symbol,
                        data_date,
                        batch.height,
                        inserted,
                        row_count,
                    )
            if inserted != row_count:
                raise RuntimeError(
                    f"ClickHouse 写入行数不一致: expected={row_count} actual={inserted}"
                )
            self._write_progress(
                symbol=symbol,
                data_date=data_date,
                status="complete",
                row_count=row_count,
                checked_expiration_count=checked_expiration_count,
                written_expiration_count=written_expiration_count,
            )
        except Exception as exc:
            self.notifier.notify_error(
                "clickhouse_insert",
                f"symbol={symbol} data_date={data_date} inserted={inserted}",
                exc,
            )
            raise

    def write_no_data(
        self,
        *,
        symbol: str,
        data_date: date,
        checked_expiration_count: int,
    ) -> None:
        try:
            self._write_progress(
                symbol=symbol,
                data_date=data_date,
                status="no_data",
                row_count=0,
                checked_expiration_count=checked_expiration_count,
                written_expiration_count=0,
            )
        except Exception as exc:
            self.notifier.notify_error(
                "clickhouse_progress_insert",
                f"symbol={symbol} data_date={data_date} status=no_data",
                exc,
            )
            raise

    def _write_progress(
        self,
        *,
        symbol: str,
        data_date: date,
        status: str,
        row_count: int,
        checked_expiration_count: int,
        written_expiration_count: int,
    ) -> None:
        self.client.execute(
            f"INSERT INTO {self.progress_fqtn} "
            "(symbol, data_date, status, row_count, checked_expiration_count, "
            "written_expiration_count, schema_version) VALUES",
            [(
                symbol,
                data_date,
                status,
                row_count,
                checked_expiration_count,
                written_expiration_count,
                1,
            )],
            types_check=False,
        )
