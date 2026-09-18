from __future__ import annotations

from datetime import date
from pathlib import Path

from theta_harvest.cli import (
    _all_requested_dates_complete,
    _remove_requested_markers,
)
from theta_harvest.completion import marker_path, write_completion_marker


def test_complete_range_skips_and_force_invalidates_before_client(
    tmp_path: Path,
) -> None:
    symbols = ("AAPL", "NVDA")
    start_date = date(2026, 9, 12)
    end_date = date(2026, 9, 13)
    for symbol in symbols:
        for data_date in (start_date, end_date):
            write_completion_marker(
                tmp_path,
                symbol,
                data_date,
                status="no_data",
                row_count=0,
                checked_expirations=set(),
                written_expirations=set(),
            )

    assert _all_requested_dates_complete(
        tmp_path, symbols, start_date, end_date
    )

    _remove_requested_markers(tmp_path, symbols, start_date, end_date)

    assert not _all_requested_dates_complete(
        tmp_path, symbols, start_date, end_date
    )
    for symbol in symbols:
        for data_date in (start_date, end_date):
            assert not marker_path(tmp_path, symbol, data_date).exists()
