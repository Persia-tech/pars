from datetime import date
from decimal import Decimal

from btc_analytics.core.aggregator import DailyAggregator
from btc_analytics.core.pipeline import ParsePipeline


class FakeRepoAgg:
    def __init__(self) -> None:
        self.conn = self
        self._last_aggregated_day = None
        self.upserts = []

    class TxCtx:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def transaction(self):
        return self.TxCtx()

    def get_parser_state(self, parser_name: str = "main", for_update: bool = False):
        class S:
            last_aggregated_day = None
        return S()

    def get_latest_block_day(self):
        return date(2020, 1, 2)

    def get_days_with_new_data(self, from_day: date):
        return [date(2020, 1, 1), date(2020, 1, 2)]

    def get_days_in_range(self, from_day: date, to_day: date):
        return [d for d in [date(2020, 1, 1), date(2020, 1, 2)] if from_day <= d <= to_day]

    def get_price_for_day(self, day: date):
        return Decimal("10000") if day == date(2020, 1, 1) else Decimal("11000")

    def get_supply_through_day(self, day: date):
        return Decimal("50")

    def get_realized_cap_through_day(self, day: date):
        return Decimal("500000")

    def get_spent_stats_for_day(self, day: date):
        if day == date(2020, 1, 2):
            return Decimal("550000"), Decimal("500000"), Decimal("50")
        return Decimal("0"), Decimal("0"), Decimal("0")

    def upsert_daily_metric(self, **kwargs):
        self.upserts.append(kwargs)

    def update_last_aggregated_day(self, day: date, parser_name: str = "main"):
        self._last_aggregated_day = day


def test_daily_aggregation_computes_metrics() -> None:
    repo = FakeRepoAgg()
    agg = DailyAggregator(repo)  # type: ignore[arg-type]
    count = agg.aggregate(date(2020, 1, 1))
    assert count == 2
    day2 = repo.upserts[1]
    assert day2["sopr"] == Decimal("1.1")
    assert day2["cdd"] == Decimal("50")


def test_resumable_range() -> None:
    assert ParsePipeline.compute_parse_range(-1, 0, 100, None) == (0, 100)
    assert ParsePipeline.compute_parse_range(50, 0, 100, 80) == (51, 80)
    assert ParsePipeline.compute_parse_range(100, 0, 100, None) is None
