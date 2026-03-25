CREATE TABLE IF NOT EXISTS parser_state (
    parser_name TEXT PRIMARY KEY,
    last_processed_height INTEGER NOT NULL DEFAULT -1 CHECK (last_processed_height >= -1),
    last_aggregated_day DATE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS blocks (
    height INTEGER PRIMARY KEY CHECK (height >= 0),
    block_hash TEXT UNIQUE NOT NULL,
    block_time TIMESTAMPTZ NOT NULL,
    day DATE NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_blocks_day ON blocks(day);

CREATE TABLE IF NOT EXISTS transactions (
    txid TEXT PRIMARY KEY,
    block_height INTEGER NOT NULL REFERENCES blocks(height) ON DELETE CASCADE,
    block_time TIMESTAMPTZ NOT NULL,
    is_coinbase BOOLEAN NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_transactions_block_height ON transactions(block_height);

CREATE TABLE IF NOT EXISTS tx_outputs (
    txid TEXT NOT NULL,
    vout INTEGER NOT NULL CHECK (vout >= 0),
    value_btc NUMERIC(24, 8) NOT NULL CHECK (value_btc >= 0),
    address TEXT,
    block_height INTEGER NOT NULL REFERENCES blocks(height) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL,
    created_day DATE NOT NULL,
    cost_basis_usd NUMERIC(24, 8) NOT NULL CHECK (cost_basis_usd >= 0),
    is_spent BOOLEAN NOT NULL DEFAULT FALSE,
    spent_by_txid TEXT,
    spent_at TIMESTAMPTZ,
    spent_day DATE,
    PRIMARY KEY (txid, vout),
    FOREIGN KEY (txid) REFERENCES transactions(txid) ON DELETE CASCADE,
    FOREIGN KEY (spent_by_txid) REFERENCES transactions(txid) ON DELETE SET NULL,
    CONSTRAINT spent_metadata_consistency CHECK (
        (is_spent = FALSE AND spent_by_txid IS NULL AND spent_at IS NULL AND spent_day IS NULL)
        OR
        (is_spent = TRUE AND spent_by_txid IS NOT NULL AND spent_at IS NOT NULL AND spent_day IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS idx_tx_outputs_unspent ON tx_outputs(is_spent) WHERE is_spent = FALSE;
CREATE INDEX IF NOT EXISTS idx_tx_outputs_spent_day ON tx_outputs(spent_day) WHERE is_spent = TRUE;
CREATE INDEX IF NOT EXISTS idx_tx_outputs_created_day ON tx_outputs(created_day);
CREATE INDEX IF NOT EXISTS idx_tx_outputs_created_spent_day ON tx_outputs(created_day, spent_day);

CREATE TABLE IF NOT EXISTS tx_inputs (
    spending_txid TEXT NOT NULL REFERENCES transactions(txid) ON DELETE CASCADE,
    vin INTEGER NOT NULL CHECK (vin >= 0),
    spent_txid TEXT NOT NULL,
    spent_vout INTEGER NOT NULL CHECK (spent_vout >= 0),
    block_height INTEGER NOT NULL REFERENCES blocks(height) ON DELETE CASCADE,
    spent_at TIMESTAMPTZ NOT NULL,
    spent_day DATE NOT NULL,
    value_btc NUMERIC(24, 8) NOT NULL CHECK (value_btc >= 0),
    created_at TIMESTAMPTZ NOT NULL,
    created_day DATE NOT NULL,
    cost_basis_usd NUMERIC(24, 8) NOT NULL CHECK (cost_basis_usd >= 0),
    PRIMARY KEY (spending_txid, vin),
    FOREIGN KEY (spent_txid, spent_vout) REFERENCES tx_outputs(txid, vout)
);

CREATE INDEX IF NOT EXISTS idx_tx_inputs_spent_lookup ON tx_inputs(spent_txid, spent_vout);
CREATE INDEX IF NOT EXISTS idx_tx_inputs_spent_day ON tx_inputs(spent_day);
CREATE INDEX IF NOT EXISTS idx_tx_inputs_spent_created_day ON tx_inputs(spent_day, created_day);

CREATE TABLE IF NOT EXISTS price_history (
    day DATE PRIMARY KEY,
    price_usd NUMERIC(24, 8) NOT NULL CHECK (price_usd >= 0),
    source TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS daily_metrics (
    day DATE PRIMARY KEY,
    spot_price_usd NUMERIC(24, 8) NOT NULL CHECK (spot_price_usd >= 0),
    circulating_supply_btc NUMERIC(24, 8) NOT NULL CHECK (circulating_supply_btc >= 0),
    market_cap_usd NUMERIC(30, 8) NOT NULL CHECK (market_cap_usd >= 0),
    realized_cap_usd NUMERIC(30, 8) NOT NULL CHECK (realized_cap_usd >= 0),
    mvrv NUMERIC(24, 12),
    nupl NUMERIC(24, 12),
    sopr NUMERIC(24, 12),
    cdd NUMERIC(30, 8) NOT NULL CHECK (cdd >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_daily_metrics_day ON daily_metrics(day);
