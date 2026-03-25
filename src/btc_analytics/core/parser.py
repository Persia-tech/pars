from __future__ import annotations

import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from btc_analytics.core.models import BlockRecord, InputRecord, OutputRecord, TxRecord
from btc_analytics.core.utxo import UTXOManager
from btc_analytics.db.connection import transaction
from btc_analytics.db.repository import Repository
from btc_analytics.rpc.client import BitcoinRPCClient

logger = logging.getLogger(__name__)


class BlockchainParser:
    def __init__(self, rpc: BitcoinRPCClient, repo: Repository, parser_name: str = "main") -> None:
        self.rpc = rpc
        self.repo = repo
        self.utxo = UTXOManager(repo)
        self.parser_name = parser_name

    def parse(self, start_height: int, end_height: int, chunk_size: int) -> int:
        if end_height < start_height:
            return 0
        total_new = 0
        for chunk_start in range(start_height, end_height + 1, chunk_size):
            chunk_end = min(chunk_start + chunk_size - 1, end_height)
            logger.info("Processing chunk [%s, %s]", chunk_start, chunk_end)
            total_new += self._parse_chunk(chunk_start, chunk_end)
        return total_new

    def _parse_chunk(self, start_height: int, end_height: int) -> int:
        parsed_new = 0
        with transaction(self.repo.conn):
            for height in range(start_height, end_height + 1):
                block_hash = self.rpc.get_block_hash(height)
                block = self.rpc.get_block(block_hash, 2)
                if self._parse_block(height, block):
                    parsed_new += 1
                self.repo.update_last_processed_height(height, self.parser_name)
        return parsed_new

    @staticmethod
    def _is_spendable_output(script_pub_key: dict[str, Any]) -> bool:
        script_type = script_pub_key.get("type")
        if script_type == "nulldata":
            return False
        return True

    def _parse_block(self, height: int, block: dict[str, Any]) -> bool:
        block_time = datetime.fromtimestamp(int(block["time"]), tz=timezone.utc)
        block_day = block_time.date()
        block_hash = str(block["hash"])

        existing_hash = self.repo.get_block_hash_at_height(height)
        if existing_hash is not None:
            if existing_hash != block_hash:
                raise ValueError(
                    f"Reorg detected at height {height}: stored hash {existing_hash} != rpc hash {block_hash}"
                )
            logger.info("Skipping already parsed block %s", height)
            return False

        self.repo.insert_block(BlockRecord(height=height, hash=block_hash, timestamp=block_time, day=block_day))

        try:
            spot_price = self.repo.get_price_for_day(block_day)
        except ValueError as exc:
            raise ValueError(
                f"Missing price for day {block_day.isoformat()}; load into price_history before parsing block {height}"
            ) from exc

        for tx in block["tx"]:
            txid = str(tx["txid"])
            is_coinbase = "coinbase" in tx["vin"][0]
            self.repo.insert_transaction(
                TxRecord(txid=txid, block_height=height, block_time=block_time, is_coinbase=is_coinbase)
            )

            if not is_coinbase:
                for vin_idx, vin in enumerate(tx["vin"]):
                    tx_input = InputRecord(
                        spending_txid=txid,
                        vin=vin_idx,
                        spent_txid=str(vin["txid"]),
                        spent_vout=int(vin["vout"]),
                        block_height=height,
                        spent_at=block_time,
                        spent_day=block_day,
                    )
                    self.utxo.spend_output(tx_input)

            for out in tx["vout"]:
                script_pub_key = out.get("scriptPubKey", {})
                if not self._is_spendable_output(script_pub_key):
                    continue
                addresses = script_pub_key.get("addresses") or []
                address = addresses[0] if addresses else script_pub_key.get("address")
                output = OutputRecord(
                    txid=txid,
                    vout=int(out["n"]),
                    value_btc=Decimal(str(out["value"])),
                    address=address,
                    block_height=height,
                    created_at=block_time,
                    created_day=block_day,
                    cost_basis_usd=spot_price,
                )
                self.utxo.create_output(output)

        return True
