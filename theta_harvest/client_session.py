from __future__ import annotations

from collections.abc import Callable
import logging
from threading import Lock
from typing import Any


LOGGER = logging.getLogger(__name__)


def is_unauthenticated_session_error(error: BaseException) -> bool:
    current: BaseException | None = error
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        code_method = getattr(current, "code", None)
        if callable(code_method):
            try:
                code = code_method()
                if getattr(code, "name", "") == "UNAUTHENTICATED":
                    return True
                if str(code).endswith("UNAUTHENTICATED"):
                    return True
            except Exception:
                pass

        message = str(current).lower()
        if "invalid session id" in message or "statuscode.unauthenticated" in message:
            return True
        current = current.__cause__ or current.__context__
    return False


class RefreshingThetaClient:
    def __init__(self, client: Any, client_factory: Callable[[], Any]) -> None:
        self._client = client
        self._client_factory = client_factory
        self._refresh_lock = Lock()

    def __getattr__(self, name: str) -> Any:
        attribute = getattr(self._client, name)
        if not callable(attribute):
            return attribute

        def request(*args: Any, **kwargs: Any) -> Any:
            client = self._client
            try:
                return getattr(client, name)(*args, **kwargs)
            except Exception as exc:
                if not is_unauthenticated_session_error(exc):
                    raise
                self._refresh_if_current(client, name)
                raise

        return request

    def _refresh_if_current(self, failed_client: Any, operation_name: str) -> None:
        with self._refresh_lock:
            if self._client is not failed_client:
                return
            LOGGER.warning(
                "ThetaData session 已失效，正在重新认证并创建 client；失败请求将在重试周期中继续: %s",
                operation_name,
            )
            self._client = self._client_factory()
            LOGGER.info("ThetaData client 重新认证成功: %s", operation_name)
