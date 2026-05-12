"""
collectors/dune/queries.py
---------------------------
SQL queries for Dune Analytics targeting pump.fun data.
All queries use parameterized dates ({{date_start}}, {{date_end}})
for pagination in daily/weekly windows.

Key tables:
  - dex_solana.trades (project = 'pumpdotfun') — all decoded trades
  - solana.instruction_calls — raw instructions (create, migrate)
  - solana.transactions — tx-level metadata
"""

# ── 1. TOKEN CREATIONS ────────────────────────────────────────────────────
# Extracts token launches from pump.fun create instructions.
# Returns: timestamp, creator, mint, name, symbol, uri
TOKEN_CREATIONS_SQL = """
SELECT
    call_block_time                         AS block_time,
    call_block_slot                         AS block_slot,
    call_tx_signer                          AS creator,
    call_tx_id                              AS tx_signature,
    account_mint                            AS mint_address,
    -- Arguments decoded from the create instruction
    TRY_CAST(JSON_EXTRACT_SCALAR(call_data, '$.name') AS VARCHAR)   AS name,
    TRY_CAST(JSON_EXTRACT_SCALAR(call_data, '$.symbol') AS VARCHAR) AS symbol,
    TRY_CAST(JSON_EXTRACT_SCALAR(call_data, '$.uri') AS VARCHAR)    AS uri
FROM pump_fun_solana.pump_fun_call_create
WHERE call_block_time >= TIMESTAMP '{{date_start}}'
  AND call_block_time <  TIMESTAMP '{{date_end}}'
  AND call_success = true
ORDER BY call_block_time ASC
"""

# Fallback if the IDL-decoded table doesn't exist: use raw instruction_calls
TOKEN_CREATIONS_RAW_SQL = """
SELECT
    block_time,
    block_slot,
    tx_signer                               AS creator,
    tx_id                                   AS tx_signature,
    -- The mint account is typically the 1st writable account
    account_arguments[1].value              AS mint_address
FROM solana.instruction_calls
WHERE executing_account = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P'
  AND LOWER(call_name) IN ('create', 'create_v2')
  AND block_time >= TIMESTAMP '{{date_start}}'
  AND block_time <  TIMESTAMP '{{date_end}}'
  AND tx_success = true
ORDER BY block_time ASC
"""


# ── 2. TRADES (individual buy/sell events) ────────────────────────────────
# Core query for IRL: every trade with wallet, direction, amounts, price.
# dex_solana.trades already has pump.fun decoded.
TRADES_SQL = """
SELECT
    block_time,
    block_slot,
    tx_id                                   AS tx_signature,
    trader_id                               AS trader_address,
    CASE
        WHEN token_bought_mint_address = '{{mint}}'  THEN 'buy'
        WHEN token_sold_mint_address   = '{{mint}}'  THEN 'sell'
        ELSE 'unknown'
    END                                     AS trade_type,
    '{{mint}}'                              AS token_mint,
    COALESCE(token_bought_symbol, token_sold_symbol) AS token_symbol,
    CASE
        WHEN token_bought_mint_address = '{{mint}}' THEN token_bought_amount
        ELSE token_sold_amount
    END                                     AS amount_token,
    CASE
        WHEN token_bought_mint_address = '{{mint}}' THEN token_sold_amount
        ELSE token_bought_amount
    END                                     AS amount_sol,
    amount_usd,
    CASE
        WHEN token_bought_mint_address = '{{mint}}' AND token_bought_amount > 0
            THEN amount_usd / token_bought_amount
        WHEN token_sold_mint_address = '{{mint}}' AND token_sold_amount > 0
            THEN amount_usd / token_sold_amount
        ELSE NULL
    END                                     AS price_usd
FROM dex_solana.trades
WHERE project = 'pumpdotfun'
  AND (token_bought_mint_address = '{{mint}}' OR token_sold_mint_address = '{{mint}}')
  AND block_time >= TIMESTAMP '{{date_start}}'
  AND block_time <  TIMESTAMP '{{date_end}}'
ORDER BY block_slot ASC
"""

# ── 3. ALL TRADES IN A DATE WINDOW (no specific mint) ─────────────────────
# For bulk collection: all pump.fun trades in a day, to be split by mint later.
ALL_TRADES_SQL = """
SELECT
    block_time,
    block_slot,
    tx_id                                   AS tx_signature,
    trader_id                               AS trader_address,
    token_bought_mint_address,
    token_sold_mint_address,
    token_bought_symbol,
    token_sold_symbol,
    token_bought_amount,
    token_sold_amount,
    amount_usd
FROM dex_solana.trades
WHERE project = 'pumpdotfun'
  AND block_time >= TIMESTAMP '{{date_start}}'
  AND block_time <  TIMESTAMP '{{date_end}}'
ORDER BY block_slot ASC
"""


# ── 4. FIRST N BUYERS of a specific token ─────────────────────────────────
FIRST_BUYERS_SQL = """
SELECT
    block_time,
    block_slot,
    tx_id                                   AS tx_signature,
    trader_id                               AS trader_address,
    'buy'                                   AS trade_type,
    token_bought_mint_address               AS token_mint,
    token_bought_symbol                     AS token_symbol,
    token_bought_amount                     AS amount_token,
    token_sold_amount                       AS amount_sol,
    amount_usd,
    CASE WHEN token_bought_amount > 0
         THEN amount_usd / token_bought_amount
         ELSE NULL END                      AS price_usd
FROM dex_solana.trades
WHERE project = 'pumpdotfun'
  AND token_bought_mint_address = '{{mint}}'
ORDER BY block_slot ASC
LIMIT {{limit}}
"""


# ── 5. WALLET TRADES (cross-token profile for IRL) ────────────────────────
WALLET_TRADES_SQL = """
SELECT
    block_time,
    block_slot,
    tx_id                                   AS tx_signature,
    trader_id                               AS trader_address,
    token_bought_mint_address,
    token_sold_mint_address,
    token_bought_symbol,
    token_sold_symbol,
    token_bought_amount,
    token_sold_amount,
    amount_usd
FROM dex_solana.trades
WHERE project = 'pumpdotfun'
  AND trader_id = '{{wallet}}'
  AND block_time >= TIMESTAMP '{{date_start}}'
  AND block_time <  TIMESTAMP '{{date_end}}'
ORDER BY block_slot ASC
"""


# ── 6. OHLCV per minute ──────────────────────────────────────────────────
OHLCV_SQL = """
SELECT
    date_trunc('minute', block_time)        AS block_time,
    '{{mint}}'                              AS token_mint,
    FIRST_VALUE(amount_usd / NULLIF(token_bought_amount, 0))
        OVER (PARTITION BY date_trunc('minute', block_time) ORDER BY block_slot ASC)
                                            AS open,
    MAX(amount_usd / NULLIF(token_bought_amount, 0))
                                            AS high,
    MIN(amount_usd / NULLIF(token_bought_amount, 0))
                                            AS low,
    LAST_VALUE(amount_usd / NULLIF(token_bought_amount, 0))
        OVER (PARTITION BY date_trunc('minute', block_time) ORDER BY block_slot ASC
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
                                            AS close,
    SUM(token_bought_amount)                AS volume_token,
    SUM(amount_usd)                         AS volume_usd,
    COUNT(*)                                AS trade_count,
    COUNT(DISTINCT trader_id)               AS unique_traders
FROM dex_solana.trades
WHERE project = 'pumpdotfun'
  AND token_bought_mint_address = '{{mint}}'
  AND block_time >= TIMESTAMP '{{date_start}}'
  AND block_time <  TIMESTAMP '{{date_end}}'
GROUP BY 1, 2
ORDER BY 1 ASC
"""


# ── 7. MIGRATIONS (graduation to PumpSwap) ───────────────────────────────
MIGRATIONS_SQL = """
SELECT
    block_time,
    block_slot,
    tx_id                                   AS tx_signature,
    tx_signer                               AS creator
FROM solana.instruction_calls
WHERE executing_account = 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA'
  AND LOWER(call_name) = 'create_pool'
  AND block_time >= TIMESTAMP '{{date_start}}'
  AND block_time <  TIMESTAMP '{{date_end}}'
  AND tx_success = true
ORDER BY block_time ASC
"""


# ── 8. TOP CREATORS by token count ────────────────────────────────────────
TOP_CREATORS_SQL = """
SELECT
    tx_signer                               AS creator,
    COUNT(DISTINCT account_mint)            AS token_count
FROM pump_fun_solana.pump_fun_call_create
WHERE call_block_time >= TIMESTAMP '{{date_start}}'
  AND call_block_time <  TIMESTAMP '{{date_end}}'
  AND call_success = true
GROUP BY 1
ORDER BY 2 DESC
LIMIT 500
"""


# ── 9. TOKEN SUMMARY STATS (for quick overview) ──────────────────────────
TOKEN_STATS_SQL = """
SELECT
    COALESCE(token_bought_mint_address, token_sold_mint_address) AS token_mint,
    MIN(block_time)                         AS first_trade_time,
    MAX(block_time)                         AS last_trade_time,
    COUNT(*)                                AS total_trades,
    COUNT(DISTINCT trader_id)               AS unique_traders,
    SUM(amount_usd)                         AS total_volume_usd,
    MAX(amount_usd / NULLIF(token_bought_amount, 0)) AS max_price_usd
FROM dex_solana.trades
WHERE project = 'pumpdotfun'
  AND block_time >= TIMESTAMP '{{date_start}}'
  AND block_time <  TIMESTAMP '{{date_end}}'
GROUP BY 1
ORDER BY total_volume_usd DESC
"""
