from __future__ import annotations

from types import SimpleNamespace

import pytest

from btc_analytics.rpc.client import BitcoinRPCClient, BitcoinRPCError


class FakeResponse:
    def __init__(self, payload: dict, status_ok: bool = True) -> None:
        self._payload = payload
        self._status_ok = status_ok

    def raise_for_status(self) -> None:
        if not self._status_ok:
            raise RuntimeError("http error")

    def json(self) -> dict:
        return self._payload


def test_rpc_retries_then_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"n": 0}

    def fake_post(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] < 3:
            raise requests.RequestException("temporary")
        return FakeResponse({"result": 7, "error": None})

    class requests:
        class RequestException(Exception):
            pass

        post = staticmethod(fake_post)

    monkeypatch.setitem(__import__("sys").modules, "requests", requests)

    client = BitcoinRPCClient("url", "user", "pass", max_retries=3, retry_backoff_sec=0)
    assert client.call("getblockcount") == 7
    assert calls["n"] == 3


def test_rpc_exhausts_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    class requests:
        class RequestException(Exception):
            pass

        @staticmethod
        def post(*args, **kwargs):
            raise requests.RequestException("down")

    monkeypatch.setitem(__import__("sys").modules, "requests", requests)

    client = BitcoinRPCClient("url", "user", "pass", max_retries=1, retry_backoff_sec=0)
    with pytest.raises(BitcoinRPCError):
        client.call("getblockcount")
