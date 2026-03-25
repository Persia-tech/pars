from __future__ import annotations

import os
from dataclasses import dataclass

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:  # pragma: no cover
    def load_dotenv() -> None:
        return None


@dataclass(frozen=True)
class Settings:
    rpc_url: str
    rpc_user: str
    rpc_password: str
    rpc_timeout: int
    rpc_max_retries: int
    rpc_retry_backoff_sec: float
    database_url: str
    start_height: int
    chunk_size: int
    parser_name: str
    log_level: str


def _get_int(name: str, default: int, minimum: int | None = None) -> int:
    raw = os.getenv(name, str(default))
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"Invalid integer for {name}: {raw}") from exc
    if minimum is not None and value < minimum:
        raise ValueError(f"{name} must be >= {minimum}, got {value}")
    return value


def _get_float(name: str, default: float, minimum: float | None = None) -> float:
    raw = os.getenv(name, str(default))
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"Invalid float for {name}: {raw}") from exc
    if minimum is not None and value < minimum:
        raise ValueError(f"{name} must be >= {minimum}, got {value}")
    return value


def load_settings() -> Settings:
    load_dotenv()
    start_height = _get_int("START_HEIGHT", 0, minimum=0)
    chunk_size = _get_int("CHUNK_SIZE", 250, minimum=1)
    return Settings(
        rpc_url=os.getenv("BTC_RPC_URL", "http://127.0.0.1:8332"),
        rpc_user=os.getenv("BTC_RPC_USER", "bitcoinrpc"),
        rpc_password=os.getenv("BTC_RPC_PASSWORD", "bitcoinrpc"),
        rpc_timeout=_get_int("BTC_RPC_TIMEOUT", 30, minimum=1),
        rpc_max_retries=_get_int("BTC_RPC_MAX_RETRIES", 3, minimum=0),
        rpc_retry_backoff_sec=_get_float("BTC_RPC_RETRY_BACKOFF_SEC", 1.0, minimum=0.0),
        database_url=os.getenv("DATABASE_URL", "postgresql://postgres:postgres@127.0.0.1:5432/btc_analytics"),
        start_height=start_height,
        chunk_size=chunk_size,
        parser_name=os.getenv("PARSER_NAME", "main"),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
    )
