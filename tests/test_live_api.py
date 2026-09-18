from __future__ import annotations

from datetime import date, datetime, time, timedelta
from pathlib import Path

import polars as pl
import pytest
from thetadata import ThetaClient

from theta_harvest.client_session import RefreshingThetaClient
from theta_harvest.completion import marker_path, validate_completion
from theta_harvest.config import load_config
from theta_harvest.pipeline import KEY_COLUMNS, call_with_retry
from theta_harvest.streaming_pipeline import ThetaOptionHarvester


@pytest.mark.live
def test_real_api_full_chain_is_idempotent(tmp_path: Path) -> None:
    config = load_config(Path("config.toml").resolve())
    assert config.symbols == ("NVDA", "AAPL", "CBRS", "NBIS")

    def create_client() -> ThetaClient:
        return ThetaClient(api_key=config.api_key, dataframe_type="polars")

    initial_client = call_with_retry(
        create_client,
        operation_name="ThetaClient authentication",
        context="live pytest",
    )
    client = RefreshingThetaClient(initial_client, create_client)
    harvester = ThetaOptionHarvester(client, tmp_path)
    test_date = date(2026, 9, 15)

    first = harvester.run(config.symbols, test_date, test_date)
    assert not first.failures, first.failures
    first_counts = dict(first.rows_by_file)
    file_stats = {
        path: (path.stat().st_size, path.stat().st_mtime_ns)
        for paths in first.written_files.values()
        for path in paths
    }

    for symbol in config.symbols:
        valid, reason, payload = validate_completion(tmp_path, symbol, test_date)
        assert valid, reason
        assert payload is not None
        assert payload["symbol"] == symbol
        assert payload["data_date"] == test_date.isoformat()
        assert marker_path(tmp_path, symbol, test_date).is_file()

    second = harvester.run(config.symbols, test_date, test_date)
    assert not second.failures, second.failures
    assert not second.written_files
    assert not second.rows_by_file
    for path, expected_stat in file_stats.items():
        assert (path.stat().st_size, path.stat().st_mtime_ns) == expected_stat

    forced = harvester.run(config.symbols, test_date, test_date, force=True)
    assert not forced.failures, forced.failures
    assert forced.rows_by_file == first_counts

    expected_directory = tmp_path / "2026" / "09" / "15"
    for symbol, output_paths in forced.written_files.items():
        assert output_paths == [expected_directory / f"{symbol}.csv"]
        for output_path in output_paths:
            frame = pl.read_csv(output_path, try_parse_dates=False)
            assert frame.select(KEY_COLUMNS).n_unique() == frame.height
            assert set(frame.get_column("symbol").unique()) == {symbol}
            assert set(frame.get_column("data_date").unique()) == {
                test_date.isoformat()
            }
            assert {"open", "bid", "delta", "underlying_price"}.issubset(
                frame.columns
            )

            timestamps = [
                datetime.fromisoformat(value)
                for value in frame.get_column("timestamp").unique().to_list()
            ]
            assert timestamps
            assert {value.date() for value in timestamps} == {test_date}
            assert {value.utcoffset() for value in timestamps} == {
                timedelta(hours=-4)
            }
            local_times = [value.time().replace(tzinfo=None) for value in timestamps]
            assert min(local_times) >= time(9, 30)
            assert max(local_times) <= time(16, 0)
