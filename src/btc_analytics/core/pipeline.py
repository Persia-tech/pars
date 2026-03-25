from __future__ import annotations

import logging
from datetime import date

from btc_analytics.config import Settings
from btc_analytics.core.aggregator import DailyAggregator
from btc_analytics.core.parser import BlockchainParser
from btc_analytics.db.connection import get_connection, transaction
from btc_analytics.db.repository import Repository
from btc_analytics.rpc.client import BitcoinRPCClient

logger = logging.getLogger(__name__)


class ParsePipeline:
    @staticmethod
    def compute_parse_range(
        last_processed_height: int, start_height_cfg: int, chain_tip: int, end_height: int | None
    ) -> tuple[int, int] | None:
        final_height = chain_tip if end_height is None else min(end_height, chain_tip)
        start_height = max(last_processed_height + 1, start_height_cfg)
        if start_height > final_height:
            return None
        return start_height, final_height

    def __init__(self, settings: Settings, parser_name: str | None = None) -> None:
        self.settings = settings
        self.parser_name = parser_name or settings.parser_name

    def _build_rpc(self) -> BitcoinRPCClient:
        return BitcoinRPCClient(
            self.settings.rpc_url,
            self.settings.rpc_user,
            self.settings.rpc_password,
            self.settings.rpc_timeout,
            self.settings.rpc_max_retries,
            self.settings.rpc_retry_backoff_sec,
        )

    def _validate_resume_point(self, repo: Repository, rpc: BitcoinRPCClient, last_processed_height: int) -> None:
        if last_processed_height < 0:
            return
        db_hash = repo.get_block_hash_at_height(last_processed_height)
        if db_hash is None:
            raise ValueError(f"Parser state points to height {last_processed_height} but block record is missing")
        rpc_hash = rpc.get_block_hash(last_processed_height)
        if db_hash != rpc_hash:
            raise ValueError(
                f"Resume safety check failed at {last_processed_height}: db hash {db_hash} != rpc hash {rpc_hash}"
            )

    def run_parse(self, end_height: int | None = None, start_height_override: int | None = None) -> int:
        with get_connection(self.settings.database_url) as conn:
            repo = Repository(conn)
            rpc = self._build_rpc()
            parser = BlockchainParser(rpc, repo, self.parser_name)

            with transaction(conn):
                state = repo.get_parser_state(self.parser_name, for_update=True)
                self._validate_resume_point(repo, rpc, state.last_processed_height)
                chain_tip = rpc.get_block_count()
                start_cfg = self.settings.start_height if start_height_override is None else start_height_override
                parse_range = self.compute_parse_range(state.last_processed_height, start_cfg, chain_tip, end_height)
                if parse_range is None:
                    logger.info("No new blocks to parse")
                    return 0

                start_height, final_height = parse_range

            parsed = parser.parse(start_height, final_height, self.settings.chunk_size)
            logger.info("Parsed %s blocks [%s, %s]", parsed, start_height, final_height)
            return parsed

    def run_aggregate(self, from_day: date | None = None) -> int:
        with get_connection(self.settings.database_url) as conn:
            repo = Repository(conn)
            repo.ensure_parser_state(self.parser_name)
            agg = DailyAggregator(repo, self.parser_name)
            count = agg.aggregate(from_day)
            logger.info("Aggregated %s day(s)", count)
            return count

    def run_aggregate_range(self, from_day: date, to_day: date) -> int:
        with get_connection(self.settings.database_url) as conn:
            repo = Repository(conn)
            agg = DailyAggregator(repo, self.parser_name)
            count = agg.aggregate_range(from_day=from_day, to_day=to_day, update_checkpoint=False)
            logger.info("Recomputed %s day(s) from %s to %s", count, from_day, to_day)
            return count

    def run_daily_incremental_update(self) -> tuple[int, int]:
        parsed = self.run_parse(end_height=None)
        aggregated = self.run_aggregate(from_day=None)
        return parsed, aggregated
