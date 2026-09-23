from __future__ import annotations

from datetime import date
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from threading import Thread

import pytest

from theta_harvest.notifier import LarkNotifier
from theta_harvest.pipeline import RetryExhaustedError, call_with_retry


def test_lark_alert_uses_local_http_server_and_retries() -> None:
    received: list[dict[str, object]] = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            length = int(self.headers["Content-Length"])
            received.append(json.loads(self.rfile.read(length)))
            self.send_response(500 if len(received) == 1 else 200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"code":0}')

        def log_message(self, *args: object) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        notifier = LarkNotifier(
            f"http://127.0.0.1:{server.server_port}/secret-token",
            storage="clickhouse",
            start_date=date(2026, 9, 15),
            end_date=date(2026, 9, 15),
        )
        notifier.notify_error(
            "clickhouse_insert",
            "symbol=NVDA data_date=2026-09-15",
            RuntimeError("boom"),
        )
    finally:
        server.shutdown()
        thread.join()

    assert len(received) == 2
    text = received[-1]["content"]["text"]
    assert "storage mode: clickhouse" in text
    assert "symbol=NVDA" in text
    assert "RuntimeError" in text
    assert "secret-token" not in text


def test_retry_alert_callback_runs_only_after_exhaustion(monkeypatch: pytest.MonkeyPatch) -> None:
    attempts: list[int] = []
    alerts: list[tuple[str, str, BaseException]] = []

    def fail() -> None:
        attempts.append(1)
        raise RuntimeError("temporary")

    monkeypatch.setattr("theta_harvest.pipeline.time.sleep", lambda _: None)
    with pytest.raises(RetryExhaustedError):
        call_with_retry(
            fail,
            operation_name="test_operation",
            context="symbol=NVDA",
            attempts=3,
            on_exhausted=lambda operation, context, exc: alerts.append(
                (operation, context, exc)
            ),
        )

    assert len(attempts) == 3
    assert len(alerts) == 1
    assert alerts[0][0] == "test_operation"
