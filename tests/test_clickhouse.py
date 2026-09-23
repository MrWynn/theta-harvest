from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl
import pytest

from theta_harvest.clickhouse_pipeline import prepare_clickhouse_frame
from theta_harvest.clickhouse_storage import DATA_COLUMNS, ClickHouseStorage
from theta_harvest.config import ClickHouseConfig


class RecordingNotifier:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, BaseException]] = []

    def notify_error(self, operation: str, context: str, exc: BaseException) -> None:
        self.calls.append((operation, context, exc))


class FakeClient:
    def __init__(self, *, fail_data: bool = False) -> None:
        self.fail_data = fail_data
        self.calls: list[tuple[str, object, dict[str, object]]] = []

    def execute(self, query: str, data: object = None, **kwargs: object) -> list[object]:
        self.calls.append((query, data, kwargs))
        if "thetadata_options_chain_1m (" in query and self.fail_data:
            raise RuntimeError("insert failed")
        return []


class ProgressClient(FakeClient):
    def execute(self, query: str, data: object = None, **kwargs: object) -> list[object]:
        self.calls.append((query, data, kwargs))
        return [("NVDA", date(2026, 9, 15))]


def _config() -> ClickHouseConfig:
    return ClickHouseConfig(
        database="laevitas",
        host="localhost",
        port=9000,
        user="user",
        password="secret",
        data_table="thetadata_options_chain_1m",
        progress_table="thetadata_options_chain_1m_progress",
    )


def _prepared_frame() -> pl.DataFrame:
    eastern = ZoneInfo("America/New_York")
    source = pl.DataFrame(
        {
            "symbol": ["NVDA"],
            "expiration": [date(2026, 9, 16)],
            "strike": [100.0],
            "right": ["C"],
            "timestamp": [datetime(2026, 9, 15, 9, 30, tzinfo=eastern)],
            "open": [float("nan")],
            "volume": [0],
            "delta": [0.0],
            "underlying_timestamp": [
                datetime(2026, 9, 15, 9, 30, tzinfo=eastern)
            ],
            "implied_vol": [0.25],
            "vanna": [1.0],
        }
    )
    return prepare_clickhouse_frame(source, date(2026, 9, 15))


def test_clickhouse_field_mapping_and_nulls() -> None:
    result = _prepared_frame()
    assert result.columns == DATA_COLUMNS
    row = result.to_dicts()[0]
    assert row["right"] == "CALL"
    assert row["open"] is None
    assert row["volume"] == 0
    assert row["delta"] == 0.0
    assert row["underlying_time"].hour == 9
    assert "implied_vol" not in result.columns
    assert "vanna" not in result.columns


def test_progress_is_written_only_after_columnar_data_insert(tmp_path: Path) -> None:
    path = tmp_path / "part.arrow"
    _prepared_frame().write_ipc(path)
    client = FakeClient()
    notifier = RecordingNotifier()
    storage = ClickHouseStorage(_config(), notifier, client=client)

    storage.insert_day(
        [path],
        symbol="NVDA",
        data_date=date(2026, 9, 15),
        row_count=1,
        checked_expiration_count=2,
        written_expiration_count=1,
    )

    assert len(client.calls) == 2
    assert client.calls[0][2]["columnar"] is True
    assert "_progress" not in client.calls[0][0]
    assert "_progress" in client.calls[1][0]
    assert not notifier.calls


def test_insert_failure_alerts_and_does_not_write_progress(tmp_path: Path) -> None:
    path = tmp_path / "part.arrow"
    _prepared_frame().write_ipc(path)
    client = FakeClient(fail_data=True)
    notifier = RecordingNotifier()
    storage = ClickHouseStorage(_config(), notifier, client=client)

    with pytest.raises(RuntimeError, match="insert failed"):
        storage.insert_day(
            [path],
            symbol="NVDA",
            data_date=date(2026, 9, 15),
            row_count=1,
            checked_expiration_count=1,
            written_expiration_count=1,
        )

    assert len(client.calls) == 1
    assert "_progress" not in client.calls[0][0]
    assert notifier.calls[0][0] == "clickhouse_insert"


def test_no_data_writes_only_progress() -> None:
    client = FakeClient()
    notifier = RecordingNotifier()
    storage = ClickHouseStorage(_config(), notifier, client=client)

    storage.write_no_data(
        symbol="NVDA",
        data_date=date(2026, 9, 13),
        checked_expiration_count=12,
    )

    assert len(client.calls) == 1
    assert "_progress" in client.calls[0][0]
    assert client.calls[0][1][0][2] == "no_data"


def test_completed_dates_uses_final_progress_rows() -> None:
    client = ProgressClient()
    storage = ClickHouseStorage(_config(), RecordingNotifier(), client=client)

    completed = storage.completed_dates(
        ("NVDA",), date(2026, 9, 15), date(2026, 9, 16)
    )

    assert completed == {("NVDA", date(2026, 9, 15))}
    assert " FINAL " in client.calls[0][0]
    assert "schema_version = 1" in client.calls[0][0]
