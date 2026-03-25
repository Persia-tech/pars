from __future__ import annotations

from btc_analytics.config import Settings
from btc_analytics.core.pipeline import ParsePipeline


class FakeRepo:
    def __init__(self, db_hash: str | None):
        self.db_hash = db_hash

    def get_block_hash_at_height(self, height: int) -> str | None:
        return self.db_hash


class FakeRPC:
    def __init__(self, rpc_hash: str):
        self.rpc_hash = rpc_hash

    def get_block_hash(self, height: int) -> str:
        return self.rpc_hash


def _settings() -> Settings:
    return Settings(
        rpc_url="url",
        rpc_user="u",
        rpc_password="p",
        rpc_timeout=1,
        rpc_max_retries=0,
        rpc_retry_backoff_sec=0,
        database_url="postgresql://x",
        start_height=0,
        chunk_size=1,
        parser_name="main",
        log_level="INFO",
    )


def test_resume_safety_passes_when_hash_matches() -> None:
    pipeline = ParsePipeline(_settings())
    pipeline._validate_resume_point(FakeRepo("abc"), FakeRPC("abc"), 100)  # type: ignore[arg-type]


def test_resume_safety_raises_on_hash_mismatch() -> None:
    pipeline = ParsePipeline(_settings())
    try:
        pipeline._validate_resume_point(FakeRepo("abc"), FakeRPC("def"), 100)  # type: ignore[arg-type]
        assert False, "expected mismatch error"
    except ValueError as exc:
        assert "Resume safety check failed" in str(exc)
