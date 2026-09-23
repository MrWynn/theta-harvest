from __future__ import annotations

import argparse
from datetime import date, timedelta
import logging
from pathlib import Path
import sys
import tomllib
import traceback

from thetadata import ThetaClient

from .client_session import RefreshingThetaClient
from .completion import remove_completion_marker, validate_completion
from .config import load_config
from .clickhouse_pipeline import ClickHouseThetaOptionHarvester
from .clickhouse_storage import ClickHouseStorage
from .notifier import LarkNotifier
from .pipeline import call_with_retry
from .streaming_pipeline import ThetaOptionHarvester


LOGGER = logging.getLogger(__name__)


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
    parser.add_argument(
        "--storage",
        choices=("csv", "clickhouse"),
        default="csv",
        help="存储模式，默认 csv",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="忽略并移除请求范围内的完成标记，完整重新抓取",
    )
    return parser


def _requested_dates(start_date: date, end_date: date) -> list[date]:
    days = (end_date - start_date).days + 1
    return [start_date + timedelta(days=offset) for offset in range(days)]


def _all_requested_dates_complete(
    output_dir: Path,
    symbols: tuple[str, ...],
    start_date: date,
    end_date: date,
) -> bool:
    completed: list[tuple[str, date, str]] = []
    for symbol in symbols:
        for data_date in _requested_dates(start_date, end_date):
            is_complete, _, marker = validate_completion(
                output_dir, symbol, data_date
            )
            if not is_complete:
                return False
            completed.append((symbol, data_date, marker["status"]))

    for symbol, data_date, status in completed:
        LOGGER.info(
            "跳过已完整抓取日期: symbol=%s date=%s status=%s",
            symbol,
            data_date,
            status,
        )
    LOGGER.info("请求范围内所有 symbol/date 均已完成，不创建 ThetaData client")
    return True


def _remove_requested_markers(
    output_dir: Path,
    symbols: tuple[str, ...],
    start_date: date,
    end_date: date,
) -> None:
    for symbol in symbols:
        for data_date in _requested_dates(start_date, end_date):
            if remove_completion_marker(output_dir, symbol, data_date):
                LOGGER.info(
                    "强制重抓，已移除完成标记: symbol=%s date=%s",
                    symbol,
                    data_date,
                )


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
    if args.storage == "clickhouse" and args.force:
        print("错误: --force 仅允许用于 --storage csv", file=sys.stderr)
        return 2

    notifier: LarkNotifier | None = None
    try:
        try:
            with args.config.resolve().open("rb") as config_file:
                raw_config = tomllib.load(config_file)
            raw_lark = raw_config.get("lark")
            if isinstance(raw_lark, dict) and isinstance(raw_lark.get("webhook_url"), str):
                notifier = LarkNotifier(
                    raw_lark["webhook_url"],
                    storage=args.storage,
                    start_date=args.start_date,
                    end_date=args.end_date,
                )
        except Exception:
            pass
        config = load_config(args.config.resolve())
        notifier = LarkNotifier(
            config.lark.webhook_url,
            storage=args.storage,
            start_date=args.start_date,
            end_date=args.end_date,
            secrets=(
                config.api_key,
                config.clickhouse.password if config.clickhouse is not None else "",
            ),
        )

        storage: ClickHouseStorage | None = None
        completed_dates: set[tuple[str, date]] = set()
        if args.storage == "csv":
            if args.force:
                _remove_requested_markers(
                    config.output_dir,
                    config.symbols,
                    args.start_date,
                    args.end_date,
                )
            elif _all_requested_dates_complete(
                config.output_dir,
                config.symbols,
                args.start_date,
                args.end_date,
            ):
                return 0
        else:
            if config.clickhouse is None:
                raise ValueError("--storage clickhouse 要求 config.toml 提供 [clickhouse]")
            try:
                storage = ClickHouseStorage(config.clickhouse, notifier)
                storage.initialize(Path(__file__).resolve().parent.parent / "clickhouse_schema.sql")
                completed_dates = storage.completed_dates(
                    config.symbols, args.start_date, args.end_date
                )
            except Exception as exc:
                notifier.notify_error(
                    "clickhouse_initialize",
                    f"start={args.start_date} end={args.end_date}",
                    exc,
                )
                raise
            expected = {
                (symbol, data_date)
                for symbol in config.symbols
                for data_date in _requested_dates(args.start_date, args.end_date)
            }
            if expected <= completed_dates:
                LOGGER.info("请求范围内所有 symbol/date 均已同步，不创建 ThetaData client")
                return 0

        def create_client() -> ThetaClient:
            return ThetaClient(api_key=config.api_key, dataframe_type="polars")

        initial_client = call_with_retry(
            create_client,
            operation_name="ThetaClient authentication",
            context="production API",
            on_exhausted=notifier.notify_error,
        )
        client = RefreshingThetaClient(initial_client, create_client)
        if args.storage == "csv":
            result = ThetaOptionHarvester(client, config.output_dir, notifier).run(
                config.symbols,
                args.start_date,
                args.end_date,
                force=args.force,
            )
        else:
            assert storage is not None
            result = ClickHouseThetaOptionHarvester(
                client, storage, completed_dates, notifier
            ).run(config.symbols, args.start_date, args.end_date)
        return result.exit_code
    except Exception as exc:
        if notifier is not None:
            notifier.notify_error(
                "unhandled_exception",
                f"start={args.start_date} end={args.end_date}",
                exc,
            )
        print("程序发生未处理错误:", file=sys.stderr)
        traceback.print_exc()
        return 1
