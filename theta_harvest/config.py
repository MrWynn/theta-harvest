from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import tomllib


@dataclass(frozen=True)
class AppConfig:
    api_key: str
    symbols: tuple[str, ...]
    output_dir: Path


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

    return AppConfig(
        api_key=api_key.strip(),
        symbols=tuple(symbols),
        output_dir=output_dir.resolve(),
    )
