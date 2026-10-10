from __future__ import annotations

from typing import Any


class DeadlineStubProxy:
    """Apply a default deadline to ThetaData gRPC stub calls."""

    def __init__(self, stub: object, timeout_seconds: int) -> None:
        self._stub = stub
        self._timeout_seconds = timeout_seconds

    def __getattr__(self, name: str) -> Any:
        attribute = getattr(self._stub, name)
        if not callable(attribute):
            return attribute

        def request(*args: Any, **kwargs: Any) -> Any:
            if kwargs.get("timeout") is None:
                kwargs["timeout"] = self._timeout_seconds
            return attribute(*args, **kwargs)

        return request
