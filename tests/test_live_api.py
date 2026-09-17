from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest
from thetadata import ThetaClient

from theta_harvest.config import load_config
from theta_harvest.pipeline import KEY_COLUMNS, call_with_retry
from theta_harvest.streaming_pipeline import ThetaOptionHarvester


@pytest.mark.live
def test_real_api_full_chain_is_idempotent(tmp_path: Path) -> None:
    config = load_config(Path("config.toml").resolve())
    assert config.symbols == ("NVDA", "AAPL", "CBRS", "NBIS")

    client = call_with_retry(
        lambda: ThetaClient(api_key=config.api_key, dataframe_type="polars"),
        operation_name="ThetaClient authentication",
        context="live pytest",
    )
    harvester = ThetaOptionHarvester(client, tmp_path)
    test_date = date(2026, 9, 15)

    first = harvester.run(config.symbols, test_date, test_date)
    assert not first.failures, first.failures
    first_counts = dict(first.rows_by_file)

    second = harvester.run(config.symbols, test_date, test_date)
    assert not second.failures, second.failures
    assert second.rows_by_file == first_counts

    for symbol, output_paths in second.written_files.items():
        for output_path in output_paths:
            assert output_path.parent.name == "2026"
            frame = pl.read_csv(output_path, try_parse_dates=True)
            assert frame.select(KEY_COLUMNS).n_unique() == frame.height
            assert set(frame.get_column("symbol").unique()) == {symbol}
            assert set(frame.get_column("data_date").unique()) == {test_date}
            assert {"open", "bid", "delta", "underlying_price"}.issubset(frame.columns)
