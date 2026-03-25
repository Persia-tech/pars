from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Iterable

from typing import Any

from btc_analytics.core.models import BlockRecord, InputRecord, OutputRecord, SpentOutputDetails, TxRecord


@dataclass(frozen=True)
class ParserState:
    parser_name: str
    last_processed_height: int
    last_aggregated_day: date | None


class Repository:
    def __init__(self, conn: Any) -> None:
        self.conn = conn

    def ensure_parser_state(self, parser_name: str = "main") -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO parser_state (parser_name, last_processed_height, last_aggregated_day)
                VALUES (%s, -1, NULL)
                ON CONFLICT (parser_name) DO NOTHING
                """,
                (parser_name,),
            )

    def get_parser_state(self, parser_name: str = "main", for_update: bool = False) -> ParserState:
        self.ensure_parser_state(parser_name)
        lock_clause = " FOR UPDATE" if for_update else ""
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT parser_name, last_processed_height, last_aggregated_day
                FROM parser_state
                WHERE parser_name = %s
                """ + lock_clause,
                (parser_name,),
            )
            row = cur.fetchone()
        assert row is not None
        return ParserState(parser_name=row[0], last_processed_height=row[1], last_aggregated_day=row[2])

    def update_last_processed_height(self, height: int, parser_name: str = "main") -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                UPDATE parser_state
                SET last_processed_height = GREATEST(last_processed_height, %s),
                    updated_at = NOW()
                WHERE parser_name = %s
                """,
                (height, parser_name),
            )

    def update_last_aggregated_day(self, day: date, parser_name: str = "main") -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                UPDATE parser_state
                SET last_aggregated_day = %s,
                    updated_at = NOW()
                WHERE parser_name = %s
                """,
                (day, parser_name),
            )


    def get_block_hash_at_height(self, height: int) -> str | None:
        with self.conn.cursor() as cur:
            cur.execute("SELECT block_hash FROM blocks WHERE height = %s", (height,))
            row = cur.fetchone()
        return row[0] if row else None

    def insert_block(self, block: BlockRecord) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO blocks(height, block_hash, block_time, day)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (height) DO NOTHING
                """,
                (block.height, block.hash, block.timestamp, block.day),
            )

    def insert_transaction(self, tx: TxRecord) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO transactions(txid, block_height, block_time, is_coinbase)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (txid) DO NOTHING
                """,
                (tx.txid, tx.block_height, tx.block_time, tx.is_coinbase),
            )

    def insert_output(self, output: OutputRecord) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO tx_outputs(
                    txid, vout, value_btc, address, block_height,
                    created_at, created_day, cost_basis_usd, is_spent
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, FALSE)
                ON CONFLICT (txid, vout) DO NOTHING
                """,
                (
                    output.txid,
                    output.vout,
                    output.value_btc,
                    output.address,
                    output.block_height,
                    output.created_at,
                    output.created_day,
                    output.cost_basis_usd,
                ),
            )


    def consume_output(
        self,
        txid: str,
        vout: int,
        spending_txid: str,
        spent_at: datetime,
        spent_day: date,
    ) -> SpentOutputDetails | None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                UPDATE tx_outputs
                SET is_spent = TRUE,
                    spent_by_txid = %s,
                    spent_at = %s,
                    spent_day = %s
                WHERE txid = %s
                  AND vout = %s
                  AND is_spent = FALSE
                RETURNING value_btc, created_at, created_day, cost_basis_usd
                """,
                (spending_txid, spent_at, spent_day, txid, vout),
            )
            row = cur.fetchone()
        if row is None:
            return None
        return SpentOutputDetails(value_btc=row[0], created_at=row[1], created_day=row[2], cost_basis_usd=row[3])

    def mark_output_spent(self, txid: str, vout: int, spending_txid: str, spent_at: datetime, spent_day: date) -> bool:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                UPDATE tx_outputs
                SET is_spent = TRUE,
                    spent_by_txid = %s,
                    spent_at = %s,
                    spent_day = %s
                WHERE txid = %s
                  AND vout = %s
                  AND is_spent = FALSE
                """,
                (spending_txid, spent_at, spent_day, txid, vout),
            )
            return cur.rowcount == 1

    def get_output_for_spend(self, txid: str, vout: int) -> SpentOutputDetails | None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT value_btc, created_at, created_day, cost_basis_usd
                FROM tx_outputs
                WHERE txid = %s AND vout = %s
                """,
                (txid, vout),
            )
            row = cur.fetchone()
        if row is None:
            return None
        return SpentOutputDetails(value_btc=row[0], created_at=row[1], created_day=row[2], cost_basis_usd=row[3])

    def insert_input(self, tx_input: InputRecord, spent_details: SpentOutputDetails) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO tx_inputs(
                    spending_txid, vin, spent_txid, spent_vout,
                    block_height, spent_at, spent_day,
                    value_btc, created_at, created_day, cost_basis_usd
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (spending_txid, vin) DO NOTHING
                """,
                (
                    tx_input.spending_txid,
                    tx_input.vin,
                    tx_input.spent_txid,
                    tx_input.spent_vout,
                    tx_input.block_height,
                    tx_input.spent_at,
                    tx_input.spent_day,
                    spent_details.value_btc,
                    spent_details.created_at,
                    spent_details.created_day,
                    spent_details.cost_basis_usd,
                ),
            )

    def upsert_price(self, day: date, price_usd: Decimal, source: str = "manual") -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO price_history(day, price_usd, source)
                VALUES (%s, %s, %s)
                ON CONFLICT(day)
                DO UPDATE SET price_usd = EXCLUDED.price_usd, source = EXCLUDED.source, updated_at = NOW()
                """,
                (day, price_usd, source),
            )

    def get_price_for_day(self, day: date) -> Decimal:
        with self.conn.cursor() as cur:
            cur.execute("SELECT price_usd FROM price_history WHERE day = %s", (day,))
            row = cur.fetchone()
        if row is None:
            raise ValueError(f"Missing BTC price for day {day.isoformat()}")
        return row[0]

    def get_latest_block_day(self) -> date | None:
        with self.conn.cursor() as cur:
            cur.execute("SELECT MAX(day) FROM blocks")
            row = cur.fetchone()
        return row[0]


    def get_days_in_range(self, from_day: date, to_day: date) -> list[date]:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT day
                FROM blocks
                WHERE day BETWEEN %s AND %s
                ORDER BY day
                """,
                (from_day, to_day),
            )
            rows = cur.fetchall()
        return [row[0] for row in rows]

    def get_days_with_new_data(self, from_day: date) -> list[date]:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT day
                FROM blocks
                WHERE day >= %s
                ORDER BY day
                """,
                (from_day,),
            )
            rows = cur.fetchall()
        return [row[0] for row in rows]

    def get_supply_through_day(self, day: date) -> Decimal:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT COALESCE(SUM(value_btc), 0)
                FROM tx_outputs
                WHERE created_day <= %s
                """,
                (day,),
            )
            minted = cur.fetchone()[0]
            cur.execute(
                """
                SELECT COALESCE(SUM(value_btc), 0)
                FROM tx_outputs
                WHERE is_spent = TRUE AND spent_day <= %s
                """,
                (day,),
            )
            spent = cur.fetchone()[0]
        return minted - spent

    def get_realized_cap_through_day(self, day: date) -> Decimal:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT COALESCE(SUM(value_btc * cost_basis_usd), 0)
                FROM tx_outputs
                WHERE created_day <= %s
                  AND (is_spent = FALSE OR spent_day > %s)
                """,
                (day, day),
            )
            return cur.fetchone()[0]

    def get_spent_stats_for_day(self, day: date) -> tuple[Decimal, Decimal, Decimal]:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COALESCE(SUM(value_btc * p.price_usd), 0) AS realized_value,
                    COALESCE(SUM(value_btc * cost_basis_usd), 0) AS original_basis,
                    COALESCE(SUM(value_btc * GREATEST((spent_day - created_day), 0)), 0) AS cdd
                FROM tx_inputs i
                JOIN price_history p ON p.day = i.spent_day
                WHERE i.spent_day = %s
                """,
                (day,),
            )
            row = cur.fetchone()
        return row[0], row[1], row[2]

    def upsert_daily_metric(
        self,
        day: date,
        spot_price: Decimal,
        circulating_supply: Decimal,
        market_cap: Decimal,
        realized_cap: Decimal,
        mvrv: Decimal | None,
        nupl: Decimal | None,
        sopr: Decimal | None,
        cdd: Decimal,
    ) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO daily_metrics(
                    day, spot_price_usd, circulating_supply_btc,
                    market_cap_usd, realized_cap_usd, mvrv, nupl, sopr, cdd,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT(day) DO UPDATE SET
                    spot_price_usd = EXCLUDED.spot_price_usd,
                    circulating_supply_btc = EXCLUDED.circulating_supply_btc,
                    market_cap_usd = EXCLUDED.market_cap_usd,
                    realized_cap_usd = EXCLUDED.realized_cap_usd,
                    mvrv = EXCLUDED.mvrv,
                    nupl = EXCLUDED.nupl,
                    sopr = EXCLUDED.sopr,
                    cdd = EXCLUDED.cdd,
                    updated_at = NOW()
                """,
                (day, spot_price, circulating_supply, market_cap, realized_cap, mvrv, nupl, sopr, cdd),
            )

    def get_latest_metric_day(self) -> date | None:
        with self.conn.cursor() as cur:
            cur.execute("SELECT MAX(day) FROM daily_metrics")
            return cur.fetchone()[0]

    def fetch_daily_metrics(self) -> Iterable[tuple]:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT day, spot_price_usd, circulating_supply_btc,
                       market_cap_usd, realized_cap_usd, mvrv, nupl, sopr, cdd
                FROM daily_metrics
                ORDER BY day
                """
            )
            yield from cur.fetchall()
