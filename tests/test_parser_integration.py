from datetime import date, datetime, timezone
from decimal import Decimal

from btc_analytics.core.models import BlockRecord, InputRecord, OutputRecord, TxRecord
from btc_analytics.core.parser import BlockchainParser


class FakeRPC:
    def __init__(self, blocks: dict[int, dict]) -> None:
        self.blocks = blocks

    def get_block_hash(self, height: int) -> str:
        return self.blocks[height]["hash"]

    def get_block(self, block_hash: str, verbosity: int = 2) -> dict:
        for block in self.blocks.values():
            if block["hash"] == block_hash:
                return block
        raise KeyError(block_hash)


class FakeConn:
    class TxCtx:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def transaction(self):
        return self.TxCtx()


class FakeRepo:
    def __init__(self) -> None:
        self.conn = FakeConn()
        self.blocks: list[BlockRecord] = []
        self.block_by_height: dict[int, str] = {}
        self.txs: list[TxRecord] = []
        self.outputs: dict[tuple[str, int], OutputRecord] = {}
        self.spent: set[tuple[str, int]] = set()
        self.inputs: list[InputRecord] = []
        self.last_height = -1

    def get_price_for_day(self, day: date) -> Decimal:
        return Decimal("10000") if day <= date(2020, 1, 1) else Decimal("20000")

    def get_block_hash_at_height(self, height: int) -> str | None:
        return self.block_by_height.get(height)

    def insert_block(self, block: BlockRecord) -> None:
        self.blocks.append(block)
        self.block_by_height[block.height] = block.hash

    def insert_transaction(self, tx: TxRecord) -> None:
        self.txs.append(tx)

    def insert_output(self, output: OutputRecord) -> None:
        self.outputs[(output.txid, output.vout)] = output

    def get_output_for_spend(self, txid: str, vout: int):
        out = self.outputs.get((txid, vout))
        if out is None:
            return None
        from btc_analytics.core.models import SpentOutputDetails

        return SpentOutputDetails(out.value_btc, out.created_at, out.created_day, out.cost_basis_usd)

    def consume_output(self, txid: str, vout: int, spending_txid: str, spent_at: datetime, spent_day: date):
        key = (txid, vout)
        out = self.outputs.get(key)
        if out is None or key in self.spent:
            return None
        self.spent.add(key)
        from btc_analytics.core.models import SpentOutputDetails

        return SpentOutputDetails(out.value_btc, out.created_at, out.created_day, out.cost_basis_usd)

    def insert_input(self, tx_input: InputRecord, spent_details) -> None:
        self.inputs.append(tx_input)

    def update_last_processed_height(self, height: int, parser_name: str = "main") -> None:
        self.last_height = height


def test_parse_sample_blocks() -> None:
    block0 = {
        "hash": "h0",
        "time": int(datetime(2020, 1, 1, tzinfo=timezone.utc).timestamp()),
        "tx": [
            {
                "txid": "coinbase",
                "vin": [{"coinbase": "abcd"}],
                "vout": [{"n": 0, "value": 50.0, "scriptPubKey": {"address": "miner"}}],
            }
        ],
    }
    block1 = {
        "hash": "h1",
        "time": int(datetime(2020, 1, 2, tzinfo=timezone.utc).timestamp()),
        "tx": [
            {
                "txid": "spend",
                "vin": [{"txid": "coinbase", "vout": 0}],
                "vout": [
                    {"n": 0, "value": 49.5, "scriptPubKey": {"address": "user"}},
                    {"n": 1, "value": 0.0, "scriptPubKey": {"type": "nulldata"}},
                ],
            }
        ],
    }
    rpc = FakeRPC({0: block0, 1: block1})
    repo = FakeRepo()
    parser = BlockchainParser(rpc, repo)  # type: ignore[arg-type]

    parsed = parser.parse(0, 1, 2)

    assert parsed == 2
    assert len(repo.blocks) == 2
    assert len(repo.txs) == 2
    assert len(repo.inputs) == 1
    assert repo.last_height == 1
    assert ("spend", 1) not in repo.outputs


def test_parse_is_idempotent_for_existing_block() -> None:
    block0 = {
        "hash": "h0",
        "time": int(datetime(2020, 1, 1, tzinfo=timezone.utc).timestamp()),
        "tx": [
            {
                "txid": "coinbase",
                "vin": [{"coinbase": "abcd"}],
                "vout": [{"n": 0, "value": 50.0, "scriptPubKey": {"address": "miner"}}],
            }
        ],
    }
    rpc = FakeRPC({0: block0})
    repo = FakeRepo()
    parser = BlockchainParser(rpc, repo)  # type: ignore[arg-type]
    assert parser.parse(0, 0, 1) == 1
    assert parser.parse(0, 0, 1) == 0
    assert len(repo.blocks) == 1
