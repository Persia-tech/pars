from decimal import Decimal

from btc_analytics.core.metrics import compute_market_cap, compute_mvrv, compute_nupl, compute_sopr


def test_market_cap() -> None:
    assert compute_market_cap(Decimal("10"), Decimal("50000")) == Decimal("500000")


def test_mvrv_and_nupl() -> None:
    market_cap = Decimal("200")
    realized_cap = Decimal("100")
    assert compute_mvrv(market_cap, realized_cap) == Decimal("2")
    assert compute_nupl(market_cap, realized_cap) == Decimal("0.5")


def test_sopr_zero_guard() -> None:
    assert compute_sopr(Decimal("10"), Decimal("0")) is None
    assert compute_sopr(Decimal("12"), Decimal("10")) == Decimal("1.2")
