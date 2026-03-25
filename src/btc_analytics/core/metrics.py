from __future__ import annotations

from decimal import Decimal


def compute_market_cap(circulating_supply_btc: Decimal, spot_price_usd: Decimal) -> Decimal:
    return circulating_supply_btc * spot_price_usd


def compute_mvrv(market_cap_usd: Decimal, realized_cap_usd: Decimal) -> Decimal | None:
    if realized_cap_usd == 0:
        return None
    return market_cap_usd / realized_cap_usd


def compute_nupl(market_cap_usd: Decimal, realized_cap_usd: Decimal) -> Decimal | None:
    if market_cap_usd == 0:
        return None
    return (market_cap_usd - realized_cap_usd) / market_cap_usd


def compute_sopr(realized_value_spent: Decimal, original_cost_basis_spent: Decimal) -> Decimal | None:
    if original_cost_basis_spent == 0:
        return None
    return realized_value_spent / original_cost_basis_spent
