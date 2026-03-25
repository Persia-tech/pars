from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal


@dataclass(frozen=True)
class BlockRecord:
    height: int
    hash: str
    timestamp: datetime
    day: date


@dataclass(frozen=True)
class TxRecord:
    txid: str
    block_height: int
    block_time: datetime
    is_coinbase: bool


@dataclass(frozen=True)
class OutputRecord:
    txid: str
    vout: int
    value_btc: Decimal
    address: str | None
    block_height: int
    created_at: datetime
    created_day: date
    cost_basis_usd: Decimal


@dataclass(frozen=True)
class InputRecord:
    spending_txid: str
    vin: int
    spent_txid: str
    spent_vout: int
    block_height: int
    spent_at: datetime
    spent_day: date


@dataclass(frozen=True)
class SpentOutputDetails:
    value_btc: Decimal
    created_at: datetime
    created_day: date
    cost_basis_usd: Decimal
