from __future__ import annotations

from collections import Counter
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime
from pathlib import Path
from threading import Event, Lock
import time
from zoneinfo import ZoneInfo

import polars as pl
import pytest
from thetadata.errors import NoDataFoundError

from theta_harvest.clickhouse_pipeline import ClickHouseThetaOptionHarvester
from theta_harvest.client_session import RefreshingThetaClient
from theta_harvest.completion import validate_completion
from theta_harvest.config import load_config
from theta_harvest.pipeline import (
    HarvestResult,
    HistoryJob,
    ThetaOptionHarvester,
    bounded_parallel_map,
)
from theta_harvest.streaming_pipeline import ThetaOptionHarvester as CsvHarvester


def test_bounded_parallel_map_reaches_but_never_exceeds_limit() -> None:
    lock = Lock()
    release = Event()
    active = 0
    peak = 0

    def operation(value: int) -> int:
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
            if active == 4:
                release.set()
        assert release.wait(timeout=2)
        time.sleep(0.01)
        with lock:
            active -= 1
        return value * 2

    results = list(bounded_parallel_map(range(12), operation, max_workers=4))

    assert peak == 4
    assert sorted(result for _, result in results) == [value * 2 for value in range(12)]


class DeterministicHarvester(ThetaOptionHarvester):
    def _fetch_batch(
        self,
        symbol: str,
        expiration: date,
        data_date: date,
        result: HarvestResult,
    ) -> pl.DataFrame:
        time.sleep((expiration.day % 3) * 0.002)
        return pl.DataFrame(
            {
                "symbol": [symbol],
                "expiration": [expiration],
                "strike": [float(expiration.day)],
                "right": ["CALL"],
                "timestamp": [datetime(2026, 9, 15, 9, 30)],
            }
        )


def test_serial_and_parallel_history_results_are_identical(tmp_path: Path) -> None:
    jobs = [
        HistoryJob("NVDA", date(2026, 9, day), date(2026, 9, 15))
        for day in range(16, 24)
    ]

    def collect(workers: int) -> list[dict[str, object]]:
        harvester = DeterministicHarvester(
            object(), tmp_path / str(workers), max_concurrent_requests=workers
        )
        rows: list[dict[str, object]] = []
        for outcome in harvester._fetch_history_parallel(jobs):
            assert outcome.frame is not None
            rows.extend(outcome.frame.to_dicts())
        return sorted(rows, key=lambda row: row["expiration"])

    assert collect(1) == collect(8)


class UnauthenticatedError(RuntimeError):
    def code(self) -> object:
        return type("Code", (), {"name": "UNAUTHENTICATED"})()


def test_concurrent_session_failures_create_only_one_new_client() -> None:
    worker_count = 4
    arrived = 0
    lock = Lock()
    release = Event()
    factory_calls = 0

    class OldClient:
        def request(self) -> None:
            nonlocal arrived
            with lock:
                arrived += 1
                if arrived == worker_count:
                    release.set()
            assert release.wait(timeout=2)
            raise UnauthenticatedError("Invalid session ID")

    class NewClient:
        def request(self) -> str:
            return "ok"

    def factory() -> NewClient:
        nonlocal factory_calls
        with lock:
            factory_calls += 1
        return NewClient()

    client = RefreshingThetaClient(OldClient(), factory)
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        failures = list(executor.map(lambda _: _capture_error(client.request), range(worker_count)))
    assert all(isinstance(error, UnauthenticatedError) for error in failures)
    assert factory_calls == 1

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        assert list(executor.map(lambda _: client.request(), range(worker_count))) == [
            "ok"
        ] * worker_count


def _capture_error(operation: Callable[[], object]) -> BaseException | None:
    try:
        operation()
    except BaseException as exc:
        return exc
    return None


class RecordingNotifier:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def notify_error(self, operation: str, context: str, exc: BaseException) -> None:
        self.calls.append(operation)


class NoDataDatesClient:
    def __init__(self, no_data_types: set[str]) -> None:
        self.no_data_types = no_data_types
        self.calls: list[str] = []

    def option_list_dates(self, *, request_type: str, **_: object) -> pl.DataFrame:
        self.calls.append(request_type)
        if request_type in self.no_data_types:
            raise NoDataFoundError(f"no {request_type} dates")
        return pl.DataFrame({"date": [date(2026, 9, 15)]})


def test_date_discovery_treats_no_data_as_empty_and_continues(tmp_path: Path) -> None:
    client = NoDataDatesClient({"trade"})
    notifier = RecordingNotifier()
    harvester = ThetaOptionHarvester(client, tmp_path, notifier=notifier)
    result = HarvestResult()

    dates = harvester._discover_dates(
        "NVDA",
        date(2026, 9, 16),
        date(2026, 9, 15),
        date(2026, 9, 15),
        result,
    )

    assert dates == [date(2026, 9, 15)]
    assert client.calls == ["trade", "quote"]
    assert not result.failures
    assert not notifier.calls


def test_date_discovery_accepts_both_request_types_without_data(tmp_path: Path) -> None:
    client = NoDataDatesClient({"trade", "quote"})
    notifier = RecordingNotifier()
    harvester = ThetaOptionHarvester(client, tmp_path, notifier=notifier)
    result = HarvestResult()

    dates = harvester._discover_dates(
        "NVDA",
        date(2026, 9, 16),
        date(2026, 9, 15),
        date(2026, 9, 15),
        result,
    )

    assert dates == []
    assert client.calls == ["trade", "quote"]
    assert not result.failures
    assert not notifier.calls


class FailingHistoryClient:
    def __init__(self) -> None:
        self.expirations = (date(2026, 9, 18), date(2026, 9, 25))
        self.data_date = date(2026, 9, 15)

    def option_list_expirations(self, **_: object) -> pl.DataFrame:
        return pl.DataFrame({"expiration": self.expirations})

    def option_list_dates(self, **_: object) -> pl.DataFrame:
        return pl.DataFrame({"date": [self.data_date]})

    def option_history_ohlc(self, **kwargs: object) -> pl.DataFrame:
        return self._frame(kwargs["expiration"], open=1.0)

    def option_history_quote(self, **kwargs: object) -> pl.DataFrame:
        return self._frame(kwargs["expiration"], bid=1.0, ask=1.1)

    def option_history_greeks_all(self, **kwargs: object) -> pl.DataFrame:
        expiration = kwargs["expiration"]
        if expiration == self.expirations[1]:
            raise RuntimeError("greeks failed")
        return self._frame(expiration, delta=0.5, underlying_price=100.0)

    def _frame(self, expiration: object, **columns: object) -> pl.DataFrame:
        values: dict[str, list[object]] = {
            "symbol": ["NVDA"],
            "expiration": [expiration],
            "strike": [100.0],
            "right": ["CALL"],
            "timestamp": [
                datetime(2026, 9, 15, 9, 30, tzinfo=ZoneInfo("America/New_York"))
            ],
        }
        values.update({name: [value] for name, value in columns.items()})
        return pl.DataFrame(values)


class RecordingStorage:
    def __init__(self) -> None:
        self.insert_calls = 0
        self.no_data_calls = 0

    def insert_day(self, *_: object, **__: object) -> None:
        self.insert_calls += 1

    def write_no_data(self, **_: object) -> None:
        self.no_data_calls += 1


class SuccessfulHistoryClient(FailingHistoryClient):
    def option_history_greeks_all(self, **kwargs: object) -> pl.DataFrame:
        return self._frame(
            kwargs["expiration"], delta=0.5, underlying_price=100.0
        )


class PartiallyEmptyHistoryClient(FailingHistoryClient):
    def __init__(self) -> None:
        super().__init__()
        self.partial_expiration = date(2026, 9, 18)
        self.empty_expiration = date(2026, 9, 25)
        self.full_expiration = date(2026, 10, 2)
        self.expirations = (
            self.partial_expiration,
            self.empty_expiration,
            self.full_expiration,
        )
        self.history_calls: list[tuple[str, date]] = []

    def option_history_ohlc(self, **kwargs: object) -> pl.DataFrame:
        expiration = kwargs["expiration"]
        self.history_calls.append(("ohlc", expiration))
        if expiration in {self.partial_expiration, self.empty_expiration}:
            raise NoDataFoundError("no ohlc")
        return self._frame(expiration, open=1.0)

    def option_history_quote(self, **kwargs: object) -> pl.DataFrame:
        expiration = kwargs["expiration"]
        self.history_calls.append(("quote", expiration))
        if expiration == self.empty_expiration:
            raise NoDataFoundError("no quote")
        return self._frame(expiration, bid=1.0, ask=1.1)

    def option_history_greeks_all(self, **kwargs: object) -> pl.DataFrame:
        expiration = kwargs["expiration"]
        self.history_calls.append(("greeks", expiration))
        if expiration == self.empty_expiration:
            raise NoDataFoundError("no greeks")
        return self._frame(expiration, delta=0.5, underlying_price=100.0)


class CapturingStorage:
    def __init__(self) -> None:
        self.insert_calls = 0
        self.no_data_calls = 0
        self.rows: list[dict[str, object]] = []

    def insert_day(self, parts: object, **_: object) -> None:
        self.insert_calls += 1
        for path in parts:
            self.rows.extend(pl.read_ipc(path).to_dicts())

    def write_no_data(self, **_: object) -> None:
        self.no_data_calls += 1


def test_history_no_data_is_empty_and_other_expirations_are_written(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("theta_harvest.pipeline.time.sleep", lambda _: None)
    client = PartiallyEmptyHistoryClient()
    storage = CapturingStorage()
    notifier = RecordingNotifier()
    harvester = ClickHouseThetaOptionHarvester(
        client,
        storage,
        completed_dates=set(),
        notifier=notifier,
        max_concurrent_requests=3,
    )

    result = harvester.run(("NVDA",), client.data_date, client.data_date)

    assert not result.failures
    assert storage.insert_calls == 1
    assert storage.no_data_calls == 0
    assert len(storage.rows) == 2
    rows_by_expiration = {row["expiration"]: row for row in storage.rows}
    assert rows_by_expiration[client.partial_expiration]["open"] is None
    assert rows_by_expiration[client.partial_expiration]["bid"] == 1.0
    assert rows_by_expiration[client.partial_expiration]["delta"] == 0.5
    assert client.empty_expiration not in rows_by_expiration
    assert Counter(client.history_calls) == Counter(
        (name, expiration)
        for expiration in client.expirations
        for name in ("ohlc", "quote", "greeks")
    )
    assert not notifier.calls


class DailyRecordingStorage:
    def __init__(self) -> None:
        self.inserted_dates: list[date] = []
        self.paths_by_date: dict[date, list[Path]] = {}

    def insert_day(self, parts: object, *, data_date: date, **_: object) -> None:
        paths = list(parts)
        assert paths and all(path.exists() for path in paths)
        self.paths_by_date[data_date] = paths
        self.inserted_dates.append(data_date)

    def write_no_data(self, *, data_date: date, **_: object) -> None:
        self.inserted_dates.append(data_date)


class DailySequencingClient:
    def __init__(self, storage: DailyRecordingStorage) -> None:
        self.storage = storage
        self.expirations = (date(2026, 9, 18), date(2026, 9, 25))
        self.data_dates = (date(2026, 9, 15), date(2026, 9, 16))
        self.next_day_started_too_early = False

    def option_list_expirations(self, **_: object) -> pl.DataFrame:
        return pl.DataFrame({"expiration": self.expirations})

    def option_list_dates(self, **_: object) -> pl.DataFrame:
        return pl.DataFrame({"date": self.data_dates})

    def option_history_ohlc(self, **kwargs: object) -> pl.DataFrame:
        self._check_day_boundary(kwargs["date"])
        return self._frame(kwargs["expiration"], kwargs["date"], open=1.0)

    def option_history_quote(self, **kwargs: object) -> pl.DataFrame:
        return self._frame(kwargs["expiration"], kwargs["date"], bid=1.0, ask=1.1)

    def option_history_greeks_all(self, **kwargs: object) -> pl.DataFrame:
        return self._frame(
            kwargs["expiration"],
            kwargs["date"],
            delta=0.5,
            underlying_price=100.0,
        )

    def _check_day_boundary(self, data_date: object) -> None:
        if data_date != self.data_dates[1]:
            return
        first_date = self.data_dates[0]
        first_paths = self.storage.paths_by_date.get(first_date, [])
        if self.storage.inserted_dates != [first_date] or any(
            path.exists() for path in first_paths
        ):
            self.next_day_started_too_early = True

    @staticmethod
    def _frame(expiration: object, data_date: object, **columns: object) -> pl.DataFrame:
        values: dict[str, list[object]] = {
            "symbol": ["NVDA"],
            "expiration": [expiration],
            "strike": [100.0],
            "right": ["CALL"],
            "timestamp": [
                datetime.combine(data_date, datetime.min.time()).replace(
                    hour=9,
                    minute=30,
                    tzinfo=ZoneInfo("America/New_York"),
                )
            ],
        }
        values.update({name: [value] for name, value in columns.items()})
        return pl.DataFrame(values)


def test_clickhouse_history_finishes_and_cleans_each_day_before_next() -> None:
    storage = DailyRecordingStorage()
    client = DailySequencingClient(storage)
    harvester = ClickHouseThetaOptionHarvester(
        client,
        storage,
        completed_dates=set(),
        notifier=RecordingNotifier(),
        max_concurrent_requests=2,
    )

    result = harvester.run(("NVDA",), client.data_dates[0], client.data_dates[1])

    assert not result.failures
    assert storage.inserted_dates == list(client.data_dates)
    assert not client.next_day_started_too_early
    assert all(
        not path.exists()
        for paths in storage.paths_by_date.values()
        for path in paths
    )


def test_failed_parallel_history_task_does_not_write_progress(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("theta_harvest.pipeline.time.sleep", lambda _: None)
    storage = RecordingStorage()
    notifier = RecordingNotifier()
    harvester = ClickHouseThetaOptionHarvester(
        FailingHistoryClient(),
        storage,
        completed_dates=set(),
        notifier=notifier,
        max_concurrent_requests=2,
    )

    result = harvester.run(("NVDA",), date(2026, 9, 15), date(2026, 9, 15))

    assert result.exit_code == 1
    assert storage.insert_calls == 0
    assert storage.no_data_calls == 0
    assert notifier.calls == ["option_history_greeks"]


def test_parallel_csv_pipeline_writes_complete_sorted_result(tmp_path: Path) -> None:
    client = SuccessfulHistoryClient()
    harvester = CsvHarvester(
        client,
        tmp_path,
        notifier=RecordingNotifier(),
        max_concurrent_requests=2,
    )

    result = harvester.run(("NVDA",), client.data_date, client.data_date)

    assert not result.failures
    output_path = tmp_path / "2026" / "09" / "15" / "NVDA.csv"
    frame = pl.read_csv(output_path, try_parse_dates=False)
    assert frame.height == 2
    assert frame.get_column("expiration").to_list() == [
        "2026-09-18",
        "2026-09-25",
    ]
    complete, reason, marker = validate_completion(
        tmp_path, "NVDA", client.data_date
    )
    assert complete, reason
    assert marker is not None
    assert marker["row_count"] == 2


def test_max_concurrent_requests_config_defaults_and_bounds(tmp_path: Path) -> None:
    base = (
        'api_key = "key"\n'
        'symbols = ["NVDA"]\n'
        'output_dir = "data"\n'
        '[lark]\nwebhook_url = "http://127.0.0.1/hook"\n'
    )
    path = tmp_path / "config.toml"
    path.write_text(base, encoding="utf-8")
    assert load_config(path).max_concurrent_requests == 1

    for valid in (1, 8):
        path.write_text(f"max_concurrent_requests = {valid}\n" + base, encoding="utf-8")
        assert load_config(path).max_concurrent_requests == valid

    for invalid in ("0", "9", "true"):
        path.write_text(f"max_concurrent_requests = {invalid}\n" + base, encoding="utf-8")
        with pytest.raises(ValueError, match="max_concurrent_requests"):
            load_config(path)
