from __future__ import annotations

from btc_analytics.core.models import InputRecord, OutputRecord
from btc_analytics.db.repository import Repository


class UTXOManager:
    def __init__(self, repository: Repository) -> None:
        self.repo = repository

    def create_output(self, output: OutputRecord) -> None:
        self.repo.insert_output(output)

    def spend_output(self, tx_input: InputRecord) -> None:
        details = self.repo.consume_output(
            tx_input.spent_txid,
            tx_input.spent_vout,
            tx_input.spending_txid,
            tx_input.spent_at,
            tx_input.spent_day,
        )
        if details is None:
            known_output = self.repo.get_output_for_spend(tx_input.spent_txid, tx_input.spent_vout)
            if known_output is None:
                raise ValueError(
                    f"Input references unknown output {tx_input.spent_txid}:{tx_input.spent_vout}"
                )
            raise ValueError(
                f"Double-spend or out-of-order spend for {tx_input.spent_txid}:{tx_input.spent_vout}"
            )
        self.repo.insert_input(tx_input, details)
