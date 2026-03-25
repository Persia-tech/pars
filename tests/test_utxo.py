from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from btc_analytics.core.models import InputRecord, OutputRecord, SpentOutputDetails
from btc_analytics.core.utxo import UTXOManager


class FakeRepo:
    def __init__(self) -> None:
        self.outputs: dict[tuple[str, int], dict] = {}
        self.inputs: list[tuple[InputRecord, SpentOutputDetails]] = []

    def insert_output(self, output: OutputRecord) -> None:
        self.outputs[(output.txid, output.vout)] = {"output": output, "spent": False}

    def get_output_for_spend(self, txid: str, vout: int) -> SpentOutputDetails | None:
        row = self.outputs.get((txid, vout))
        if not row:
            return None
        output = row["output"]
        return SpentOutputDetails(output.value_btc, output.created_at, output.created_day, output.cost_basis_usd)

    def consume_output(self, txid: str, vout: int, spending_txid: str, spent_at: datetime, spent_day: date):
        row = self.outputs.get((txid, vout))
        if not row or row["spent"]:
            return None
        row["spent"] = True
        output = row["output"]
        return SpentOutputDetails(output.value_btc, output.created_at, output.created_day, output.cost_basis_usd)

    def insert_input(self, tx_input: InputRecord, spent_details: SpentOutputDetails) -> None:
        self.inputs.append((tx_input, spent_details))


def test_create_and_spend_utxo() -> None:
    repo = FakeRepo()
    utxo = UTXOManager(repo)  # type: ignore[arg-type]
    created_at = datetime(2020, 1, 1, tzinfo=timezone.utc)
    out = OutputRecord("a", 0, Decimal("1.5"), None, 100, created_at, created_at.date(), Decimal("7000"))
    utxo.create_output(out)

    txin = InputRecord("b", 0, "a", 0, 101, datetime(2020, 1, 2, tzinfo=timezone.utc), date(2020, 1, 2))
    utxo.spend_output(txin)

    assert len(repo.inputs) == 1


def test_double_spend_raises() -> None:
    repo = FakeRepo()
    utxo = UTXOManager(repo)  # type: ignore[arg-type]
    ts = datetime(2020, 1, 1, tzinfo=timezone.utc)
    out = OutputRecord("a", 0, Decimal("1"), None, 100, ts, ts.date(), Decimal("7000"))
    utxo.create_output(out)

    txin = InputRecord("b", 0, "a", 0, 101, datetime(2020, 1, 2, tzinfo=timezone.utc), date(2020, 1, 2))
    utxo.spend_output(txin)
    with pytest.raises(ValueError):
        utxo.spend_output(txin)
