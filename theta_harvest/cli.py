from __future__ import annotations

import argparse
from datetime import date
import logging
from pathlib import Path
import sys
import traceback

from thetadata import ThetaClient

from .config import load_config
from .pipeline import call_with_retry
from .streaming_pipeline import ThetaOptionHarvester


def parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"日期必须使用 YYYY-MM-DD 格式: {value}"
        ) from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="采集 ThetaData 全量美股期权链")
    parser.add_argument("--start-date", required=True, type=parse_date)
    parser.add_argument("--end-date", required=True, type=parse_date)
    parser.add_argument("--config", type=Path, default=Path("config.toml"))
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s:%(lineno)d %(message)s",
    )
    logging.getLogger("thetadata").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)

    args = build_parser().parse_args(argv)
    if args.start_date > args.end_date:
        print("错误: --start-date 不能晚于 --end-date", file=sys.stderr)
        return 2

    try:
        config = load_config(args.config.resolve())
        client = call_with_retry(
            lambda: ThetaClient(api_key=config.api_key, dataframe_type="polars"),
            operation_name="ThetaClient authentication",
            context="production API",
        )
        result = ThetaOptionHarvester(client, config.output_dir).run(
            config.symbols,
            args.start_date,
            args.end_date,
        )
        return result.exit_code
    except Exception:
        print("程序发生未处理错误:", file=sys.stderr)
        traceback.print_exc()
        return 1
