from __future__ import annotations

from datetime import date
import logging
import socket
import traceback
from typing import Any

import httpx


LOGGER = logging.getLogger(__name__)
MAX_MESSAGE_LENGTH = 3_500


class LarkNotifier:
    def __init__(
        self,
        webhook_url: str,
        *,
        storage: str,
        start_date: date,
        end_date: date,
        secrets: tuple[str, ...] = (),
    ) -> None:
        self.webhook_url = webhook_url
        self.storage = storage
        self.start_date = start_date
        self.end_date = end_date
        self.secrets = tuple(value for value in secrets if value)

    def notify_error(
        self,
        operation: str,
        context: str,
        exc: BaseException,
        *,
        traceback_text: str | None = None,
    ) -> None:
        if getattr(exc, "_lark_notified", False):
            return
        try:
            setattr(exc, "_lark_notified", True)
        except Exception:
            pass

        trace = traceback_text or "".join(
            traceback.format_exception(type(exc), exc, exc.__traceback__)
        )
        message = "\n".join(
            (
                "ThetaData harvest error",
                f"hostname: {socket.gethostname()}",
                f"storage mode: {self.storage}",
                f"start/end date: {self.start_date} / {self.end_date}",
                f"context: {context or 'none'}",
                f"operation: {operation}",
                f"exception type: {type(exc).__name__}",
                f"error: {exc}",
                "traceback:",
                trace,
            )
        )
        for secret in (*self.secrets, self.webhook_url):
            message = message.replace(secret, "***")
        message = message[:MAX_MESSAGE_LENGTH]

        for attempt in range(1, 4):
            try:
                response = httpx.post(
                    self.webhook_url,
                    json={"msg_type": "text", "content": {"text": message}},
                    timeout=5.0,
                )
                response.raise_for_status()
                payload: Any = response.json()
                if isinstance(payload, dict) and payload.get("code", 0) != 0:
                    raise RuntimeError(
                        f"Lark webhook returned code={payload.get('code')}"
                    )
                return
            except Exception as send_error:
                LOGGER.warning(
                    "Lark 告警发送失败，第 %d/3 次: %s: %s",
                    attempt,
                    type(send_error).__name__,
                    send_error,
                )
