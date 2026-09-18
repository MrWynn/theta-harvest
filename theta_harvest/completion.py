from __future__ import annotations

from datetime import date, datetime, timezone
import json
import os
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4


MARKER_VERSION = 1
DATA_FORMAT_VERSION = 1
REQUEST_SIGNATURE = "option-chain-1m-default-et-all-strikes-both-rights-v1"

CompletionStatus = Literal["complete", "no_data"]


def day_directory(output_dir: Path, data_date: date) -> Path:
    return output_dir / f"{data_date:%Y}" / f"{data_date:%m}" / f"{data_date:%d}"


def csv_path(output_dir: Path, symbol: str, data_date: date) -> Path:
    return day_directory(output_dir, data_date) / f"{symbol}.csv"


def marker_path(output_dir: Path, symbol: str, data_date: date) -> Path:
    return day_directory(output_dir, data_date) / f".{symbol}.complete.json"


def validate_completion(
    output_dir: Path,
    symbol: str,
    data_date: date,
) -> tuple[bool, str, dict[str, Any] | None]:
    path = marker_path(output_dir, symbol, data_date)
    if not path.is_file():
        return False, "完成标记不存在", None

    try:
        marker = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        return False, f"完成标记无法读取: {type(exc).__name__}: {exc}", None

    if not isinstance(marker, dict):
        return False, "完成标记内容不是 JSON object", None
    expected = {
        "marker_version": MARKER_VERSION,
        "data_format_version": DATA_FORMAT_VERSION,
        "request_signature": REQUEST_SIGNATURE,
        "symbol": symbol,
        "data_date": data_date.isoformat(),
    }
    for field, value in expected.items():
        if marker.get(field) != value:
            return False, f"完成标记字段不匹配: {field}", marker

    status = marker.get("status")
    if status == "no_data":
        if marker.get("csv_file") is not None or marker.get("row_count") != 0:
            return False, "no_data 完成标记内容无效", marker
        return True, "已确认无数据", marker
    if status != "complete":
        return False, f"未知完成状态: {status}", marker

    expected_file = f"{symbol}.csv"
    if marker.get("csv_file") != expected_file:
        return False, "完成标记中的 CSV 文件名不匹配", marker
    path_to_csv = csv_path(output_dir, symbol, data_date)
    if not path_to_csv.is_file():
        return False, "完成标记存在但 CSV 文件缺失", marker
    expected_size = marker.get("csv_size")
    if not isinstance(expected_size, int) or expected_size < 0:
        return False, "完成标记中的 CSV 文件大小无效", marker
    actual_size = path_to_csv.stat().st_size
    if actual_size != expected_size:
        return False, f"CSV 文件大小不匹配: marker={expected_size} actual={actual_size}", marker
    row_count = marker.get("row_count")
    if not isinstance(row_count, int) or row_count < 0:
        return False, "完成标记中的 CSV 行数无效", marker
    return True, "CSV 完成标记有效", marker


def remove_completion_marker(output_dir: Path, symbol: str, data_date: date) -> bool:
    path = marker_path(output_dir, symbol, data_date)
    if not path.exists():
        return False
    path.unlink()
    return True


def write_completion_marker(
    output_dir: Path,
    symbol: str,
    data_date: date,
    *,
    status: CompletionStatus,
    row_count: int,
    checked_expirations: set[date],
    written_expirations: set[date],
) -> Path:
    directory = day_directory(output_dir, data_date)
    directory.mkdir(parents=True, exist_ok=True)
    output_csv = csv_path(output_dir, symbol, data_date)
    if status == "complete":
        if not output_csv.is_file():
            raise ValueError(f"写完成标记前 CSV 不存在: {output_csv}")
        csv_file: str | None = output_csv.name
        csv_size: int | None = output_csv.stat().st_size
    else:
        csv_file = None
        csv_size = None
        row_count = 0

    marker = {
        "marker_version": MARKER_VERSION,
        "data_format_version": DATA_FORMAT_VERSION,
        "request_signature": REQUEST_SIGNATURE,
        "symbol": symbol,
        "data_date": data_date.isoformat(),
        "status": status,
        "completed_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "csv_file": csv_file,
        "csv_size": csv_size,
        "row_count": row_count,
        "checked_expirations": sorted(value.isoformat() for value in checked_expirations),
        "written_expirations": sorted(value.isoformat() for value in written_expirations),
    }

    path = marker_path(output_dir, symbol, data_date)
    temporary_path = directory / f".{symbol}.complete.{uuid4().hex}.tmp"
    try:
        temporary_path.write_text(
            json.dumps(marker, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary_path, path)
    except Exception:
        if temporary_path.exists():
            temporary_path.unlink()
        raise
    return path
