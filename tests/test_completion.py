from __future__ import annotations

from datetime import date
import json
from pathlib import Path

from theta_harvest.completion import (
    MARKER_VERSION,
    csv_path,
    marker_path,
    validate_completion,
    write_completion_marker,
)


def test_complete_marker_requires_matching_csv_size(tmp_path: Path) -> None:
    data_date = date(2026, 9, 15)
    output_csv = csv_path(tmp_path, "AAPL", data_date)
    output_csv.parent.mkdir(parents=True)
    output_csv.write_text("symbol,value\nAAPL,1\n", encoding="utf-8")

    marker = write_completion_marker(
        tmp_path,
        "AAPL",
        data_date,
        status="complete",
        row_count=1,
        checked_expirations={date(2026, 9, 18)},
        written_expirations={date(2026, 9, 18)},
    )

    valid, reason, payload = validate_completion(tmp_path, "AAPL", data_date)
    assert valid, reason
    assert payload is not None
    assert payload["marker_version"] == MARKER_VERSION
    assert payload["csv_size"] == output_csv.stat().st_size
    assert payload["row_count"] == 1
    assert payload["checked_expirations"] == ["2026-09-18"]
    assert marker == marker_path(tmp_path, "AAPL", data_date)

    output_csv.write_text("symbol,value\nAAPL,12345\n", encoding="utf-8")
    valid, reason, _ = validate_completion(tmp_path, "AAPL", data_date)
    assert not valid
    assert "大小不匹配" in reason


def test_no_data_marker_does_not_require_csv(tmp_path: Path) -> None:
    data_date = date(2026, 9, 13)
    write_completion_marker(
        tmp_path,
        "NVDA",
        data_date,
        status="no_data",
        row_count=99,
        checked_expirations={date(2026, 9, 18)},
        written_expirations=set(),
    )

    valid, reason, payload = validate_completion(tmp_path, "NVDA", data_date)
    assert valid, reason
    assert payload is not None
    assert payload["status"] == "no_data"
    assert payload["csv_file"] is None
    assert payload["csv_size"] is None
    assert payload["row_count"] == 0


def test_corrupt_or_version_mismatched_marker_is_invalid(tmp_path: Path) -> None:
    data_date = date(2026, 9, 15)
    path = marker_path(tmp_path, "AAPL", data_date)
    path.parent.mkdir(parents=True)
    path.write_text("not-json", encoding="utf-8")

    valid, reason, _ = validate_completion(tmp_path, "AAPL", data_date)
    assert not valid
    assert "无法读取" in reason

    path.write_text(
        json.dumps(
            {
                "marker_version": MARKER_VERSION + 1,
                "symbol": "AAPL",
                "data_date": data_date.isoformat(),
            }
        ),
        encoding="utf-8",
    )
    valid, reason, _ = validate_completion(tmp_path, "AAPL", data_date)
    assert not valid
    assert "marker_version" in reason
