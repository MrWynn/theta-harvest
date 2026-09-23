from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import tomllib


@dataclass(frozen=True)
class AppConfig:
    api_key: str
    symbols: tuple[str, ...]
    output_dir: Path
    clickhouse: ClickHouseConfig | None
    lark: LarkConfig


@dataclass(frozen=True)
class ClickHouseConfig:
    database: str
    host: str
    port: int
    user: str
    password: str
    data_table: str
    progress_table: str


@dataclass(frozen=True)
class LarkConfig:
    webhook_url: str


def _required_string(section: dict[str, object], key: str, section_name: str) -> str:
    value = section.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"config.toml 的 [{section_name}] 必须提供非空 {key}")
    return value.strip()


def load_config(path: Path) -> AppConfig:
    if not path.is_file():
        raise ValueError(f"配置文件不存在: {path}")

    with path.open("rb") as config_file:
        raw = tomllib.load(config_file)

    api_key = raw.get("api_key")
    if not isinstance(api_key, str) or not api_key.strip():
        raise ValueError("config.toml 中必须提供非空 api_key")

    raw_symbols = raw.get("symbols")
    if not isinstance(raw_symbols, list) or not raw_symbols:
        raise ValueError("config.toml 中 symbols 必须是非空数组")

    symbols: list[str] = []
    seen: set[str] = set()
    for value in raw_symbols:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("symbols 中的每一项都必须是非空字符串")
        symbol = value.strip().upper()
        if symbol not in seen:
            symbols.append(symbol)
            seen.add(symbol)

    raw_output_dir = raw.get("output_dir", "data")
    if not isinstance(raw_output_dir, str) or not raw_output_dir.strip():
        raise ValueError("output_dir 必须是非空字符串")
    output_dir = Path(raw_output_dir.strip())
    if not output_dir.is_absolute():
        output_dir = path.parent / output_dir

    raw_lark = raw.get("lark")
    if not isinstance(raw_lark, dict):
        raise ValueError("config.toml 必须提供 [lark] 配置")
    lark = LarkConfig(
        webhook_url=_required_string(raw_lark, "webhook_url", "lark")
    )

    raw_clickhouse = raw.get("clickhouse")
    clickhouse: ClickHouseConfig | None = None
    if raw_clickhouse is not None:
        if not isinstance(raw_clickhouse, dict):
            raise ValueError("config.toml 的 [clickhouse] 必须是配置表")
        port = raw_clickhouse.get("port")
        if not isinstance(port, int) or not 1 <= port <= 65535:
            raise ValueError("config.toml 的 [clickhouse].port 必须是有效端口")
        clickhouse = ClickHouseConfig(
            database=_required_string(raw_clickhouse, "database", "clickhouse"),
            host=_required_string(raw_clickhouse, "host", "clickhouse"),
            port=port,
            user=_required_string(raw_clickhouse, "user", "clickhouse"),
            password=_required_string(raw_clickhouse, "password", "clickhouse"),
            data_table=_required_string(raw_clickhouse, "data_table", "clickhouse"),
            progress_table=_required_string(
                raw_clickhouse, "progress_table", "clickhouse"
            ),
        )

    return AppConfig(
        api_key=api_key.strip(),
        symbols=tuple(symbols),
        output_dir=output_dir.resolve(),
        clickhouse=clickhouse,
        lark=lark,
    )
