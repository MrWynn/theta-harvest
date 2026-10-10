from __future__ import annotations

from typing import Any

import pytest

from theta_harvest.client_session import RefreshingThetaClient
from theta_harvest.grpc_deadline import DeadlineStubProxy
from theta_harvest.pipeline import RetryExhaustedError, call_with_retry


class RecordingRpc:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def __call__(self, request: object, **kwargs: Any) -> str:
        self.calls.append(kwargs)
        return f"response:{request}"


class FakeStub:
    def __init__(self) -> None:
        self.GetOptionHistoryOhlc = RecordingRpc()
        self.name = "stub"


class UnauthenticatedError(RuntimeError):
    def code(self) -> object:
        return type("Code", (), {"name": "UNAUTHENTICATED"})()


def test_deadline_proxy_applies_default_and_preserves_explicit_timeout() -> None:
    stub = FakeStub()
    proxy = DeadlineStubProxy(stub, 300)

    assert proxy.GetOptionHistoryOhlc("first") == "response:first"
    assert proxy.GetOptionHistoryOhlc("second", timeout=45) == "response:second"
    assert proxy.name == "stub"
    assert stub.GetOptionHistoryOhlc.calls == [
        {"timeout": 300},
        {"timeout": 45},
    ]


def test_deadline_error_uses_existing_retry_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("theta_harvest.pipeline.time.sleep", lambda _: None)
    calls = 0

    def deadline_exceeded() -> None:
        nonlocal calls
        calls += 1
        raise RuntimeError("StatusCode.DEADLINE_EXCEEDED")

    with pytest.raises(RetryExhaustedError):
        call_with_retry(
            deadline_exceeded,
            operation_name="option_history_ohlc",
            context="symbol=NVDA",
            attempts=3,
        )

    assert calls == 3


def test_refreshed_client_keeps_deadline_proxy() -> None:
    class FailedRpc:
        def __call__(self, request: object, **kwargs: Any) -> str:
            raise UnauthenticatedError("Invalid session ID")

    class Client:
        def __init__(self, stub: object) -> None:
            self.stub = stub

        def request(self, value: object) -> str:
            return self.stub.GetOptionHistoryOhlc(value)

    old_stub = type("OldStub", (), {"GetOptionHistoryOhlc": FailedRpc()})()
    new_stub = FakeStub()

    def factory() -> Client:
        return Client(DeadlineStubProxy(new_stub, 300))

    client = RefreshingThetaClient(Client(old_stub), factory)
    with pytest.raises(UnauthenticatedError):
        client.request("first")

    assert client.request("second") == "response:second"
    assert new_stub.GetOptionHistoryOhlc.calls == [{"timeout": 300}]
