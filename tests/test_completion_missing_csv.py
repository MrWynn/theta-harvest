from __future__ import annotations

from datetime import date
from pathlib import Path

from theta_harvest.completion import (
    csv_path,
    validate_completion,
    write_completion_marker,
)


def test_complete_marker_with_missing_csv_is_invalid(tmp_path: Path) -> None:
    data_date = date(2026, 9, 15)
    output_csv = csv_path(tmp_path, "AAPL", data_date)
    output_csv.parent.mkdir(parents=True)
    output_csv.write_text("symbol\nAAPL\n", encoding="utf-8")
    write_completion_marker(
        tmp_path,
        "AAPL",
        data_date,
        status="complete",
        row_count=1,
        checked_expirations=set(),
        written_expirations=set(),
    )

    output_csv.unlink()

    valid, reason, _ = validate_completion(tmp_path, "AAPL", data_date)
    assert not valid
    assert "CSV 文件缺失" in reason
