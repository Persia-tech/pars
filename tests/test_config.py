from __future__ import annotations

import pytest

from btc_analytics.config import load_settings


def test_load_settings_validates_positive_chunk_size(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CHUNK_SIZE", "0")
    with pytest.raises(ValueError):
        load_settings()


def test_load_settings_parses_retry_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CHUNK_SIZE", "10")
    monkeypatch.setenv("BTC_RPC_MAX_RETRIES", "5")
    monkeypatch.setenv("BTC_RPC_RETRY_BACKOFF_SEC", "0.5")
    settings = load_settings()
    assert settings.rpc_max_retries == 5
    assert settings.rpc_retry_backoff_sec == 0.5
