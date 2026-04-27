# polymarket-trades

Python library for collecting Polymarket trade data into PostgreSQL.

Fetches trades via Polymarket's HTTP APIs (gamma-api and data-api) using a two-step approach:
1. Discover unique traders from the `/trades` endpoint
2. Fetch complete trade history per trader from the `/activity` endpoint

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

Two tables are created automatically on first run:

**`trades`** — one row per trade:

| Column | Type | Description |
|---|---|---|
| transaction_hash | TEXT | Blockchain transaction hash |
| condition_id | TEXT | Market condition ID |
| proxy_wallet | TEXT | Trader's proxy wallet address |
| timestamp | TIMESTAMPTZ | Trade time |
| side | TEXT | BUY or SELL |
| size | NUMERIC | Number of shares |
| usdc_size | NUMERIC | USDC value |
| price | NUMERIC | Price per share |
| outcome | TEXT | e.g. "Yes", "No" |
| outcome_index | SMALLINT | Outcome index (0, 1, ...) |

Deduplicated on `(transaction_hash, condition_id, proxy_wallet, outcome_index)`.

**`collection_progress`** — tracks resume state per market. If collection is interrupted, re-running the same command picks up where it left off.

## Dependencies

- `requests`
- `psycopg2-binary`
