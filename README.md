# polymarket-trades

![version](https://img.shields.io/badge/version-0.3.0-blue)

Python library for collecting Polymarket trade data into PostgreSQL.

Fetches trades via Polymarket's HTTP APIs (gamma-api and data-api) by paginating the `/trades` endpoint directly — each trade is stored exactly once.

## Install

```bash
pip install -e .
```

## Usage

### Python API

```python
from polymarket_trades import init_db, collect_event, collect_market

conn = init_db("postgresql://localhost/mydb")

# Collect all markets under an event
collect_event(conn, "will-israel-strike-lebanon-on", question_filter="November")

# Or collect a single market by condition ID
collect_market(conn, "0x06801ce7d3c4bd57fd90cbae5d6783040547117063046856100547fab695f56b")

conn.close()
```

### CLI

```bash
# Set DSN via env var or --dsn flag
export POLYMARKET_TRADES_DSN="postgresql://localhost/mydb"

# Collect all markets from an event
polymarket-trades --event-slug "will-israel-strike-lebanon-on" --question-filter "November"

# Collect a single market
polymarket-trades --dsn "postgresql://localhost/mydb" --condition-id "0x06801c..."
```

## Database Schema

Four tables are created automatically on first run:

**`condition_ids`** / **`proxy_wallets`** — lookup tables that normalize repeated text values into compact integer keys.

**`trades`** — one row per trade:

| Column | Type | Description |
|---|---|---|
| transaction_hash | BYTEA | Blockchain transaction hash (32 bytes) |
| condition_id | SMALLINT | FK → condition_ids |
| proxy_wallet | INTEGER | FK → proxy_wallets |
| timestamp | TIMESTAMPTZ | Trade time |
| side | BOOLEAN | true = BUY, false = SELL |
| size | NUMERIC | Number of shares |
| usdc_size | NUMERIC | USDC value |
| price | NUMERIC | Price per share |
| outcome_index | SMALLINT | Outcome index (0, 1, ...) |

Deduplicated on `(transaction_hash, condition_id, proxy_wallet, outcome_index)`.

**`collection_progress`** — tracks resume state per market. If collection is interrupted, re-running the same command picks up where it left off.

## Dependencies

- `requests`
- `psycopg2-binary`
