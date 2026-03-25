from __future__ import annotations

import logging
from datetime import date, timedelta

from btc_analytics.core.metrics import compute_market_cap, compute_mvrv, compute_nupl, compute_sopr
from btc_analytics.db.connection import transaction
from btc_analytics.db.repository import Repository

logger = logging.getLogger(__name__)


class DailyAggregator:
    def __init__(self, repo: Repository, parser_name: str = "main") -> None:
        self.repo = repo
        self.parser_name = parser_name

    def aggregate(self, from_day: date | None = None) -> int:
        state = self.repo.get_parser_state(self.parser_name, for_update=True)
        latest_block_day = self.repo.get_latest_block_day()
        if latest_block_day is None:
            logger.info("No parsed blocks found; skipping aggregation")
            return 0

        if from_day is None:
            if state.last_aggregated_day is None:
                from_day = self._first_day()
            else:
                from_day = state.last_aggregated_day + timedelta(days=1)

        return self.aggregate_range(from_day=from_day, to_day=latest_block_day, update_checkpoint=True)

    def aggregate_range(self, from_day: date, to_day: date, update_checkpoint: bool = False) -> int:
        if from_day > to_day:
            return 0
        days = self.repo.get_days_in_range(from_day, to_day)
        if not days:
            return 0

        with transaction(self.repo.conn):
            for day in days:
                self._aggregate_day(day)
                if update_checkpoint:
                    self.repo.update_last_aggregated_day(day, self.parser_name)

        return len(days)

    def _first_day(self) -> date:
        with self.repo.conn.cursor() as cur:
            cur.execute("SELECT MIN(day) FROM blocks")
            row = cur.fetchone()
        if row[0] is None:
            raise ValueError("No blocks to aggregate")
        return row[0]

    def _aggregate_day(self, day: date) -> None:
        spot_price = self.repo.get_price_for_day(day)
        circulating_supply = self.repo.get_supply_through_day(day)
        market_cap = compute_market_cap(circulating_supply, spot_price)
        realized_cap = self.repo.get_realized_cap_through_day(day)
        mvrv = compute_mvrv(market_cap, realized_cap)
        nupl = compute_nupl(market_cap, realized_cap)

        spent_realized_value, spent_original_basis, cdd = self.repo.get_spent_stats_for_day(day)
        sopr = compute_sopr(spent_realized_value, spent_original_basis)

        self.repo.upsert_daily_metric(
            day=day,
            spot_price=spot_price,
            circulating_supply=circulating_supply,
            market_cap=market_cap,
            realized_cap=realized_cap,
            mvrv=mvrv,
            nupl=nupl,
            sopr=sopr,
            cdd=cdd,
        )
        logger.info("Aggregated metrics for %s", day)
