import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS trades (
    id                BIGSERIAL PRIMARY KEY,
    transaction_hash  TEXT NOT NULL,
    condition_id      TEXT NOT NULL,
    proxy_wallet      TEXT NOT NULL,
    timestamp         TIMESTAMPTZ NOT NULL,
    side              TEXT NOT NULL,
    size              NUMERIC NOT NULL,
    usdc_size         NUMERIC NOT NULL,
    price             NUMERIC NOT NULL,
    outcome           TEXT NOT NULL,
    outcome_index     SMALLINT NOT NULL,
    UNIQUE (transaction_hash, condition_id, proxy_wallet, outcome_index)
);

CREATE INDEX IF NOT EXISTS idx_trades_condition_id ON trades (condition_id);
CREATE INDEX IF NOT EXISTS idx_trades_proxy_wallet ON trades (proxy_wallet);
CREATE INDEX IF NOT EXISTS idx_trades_timestamp    ON trades (timestamp);

CREATE TABLE IF NOT EXISTS collection_progress (
    condition_id  TEXT PRIMARY KEY,
    status        TEXT NOT NULL DEFAULT 'pending',
    users_found   INTEGER DEFAULT 0,
    users_done    INTEGER DEFAULT 0,
    trades_stored INTEGER DEFAULT 0,
    updated_at    TIMESTAMPTZ DEFAULT now()
);
"""


def init_db(dsn: str):
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
    conn.autocommit = False
    return conn


def insert_trades(conn, trades: list[dict]) -> int:
    if not trades:
        return 0
    sql = """
        INSERT INTO trades (transaction_hash, condition_id, proxy_wallet,
                            timestamp, side, size, usdc_size, price,
                            outcome, outcome_index)
        VALUES %s
        ON CONFLICT DO NOTHING
    """
    rows = [
        (
            t["transaction_hash"],
            t["condition_id"],
            t["proxy_wallet"],
            datetime.fromtimestamp(t["timestamp"], tz=timezone.utc),
            t["side"],
            t["size"],
            t["usdc_size"],
            t["price"],
            t["outcome"],
            t["outcome_index"],
        )
        for t in trades
    ]
    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
        inserted = cur.rowcount
    conn.commit()
    return inserted


def get_progress(conn, condition_id: str) -> dict | None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT condition_id, status, users_found, users_done, trades_stored "
            "FROM collection_progress WHERE condition_id = %s",
            (condition_id,),
        )
        row = cur.fetchone()
    if not row:
        return None
    return {
        "condition_id": row[0],
        "status": row[1],
        "users_found": row[2],
        "users_done": row[3],
        "trades_stored": row[4],
    }


def set_progress(conn, condition_id: str, *, status: str,
                 users_found: int = 0, users_done: int = 0,
                 trades_stored: int = 0):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO collection_progress
                (condition_id, status, users_found, users_done, trades_stored, updated_at)
            VALUES (%s, %s, %s, %s, %s, now())
            ON CONFLICT (condition_id) DO UPDATE SET
                status = EXCLUDED.status,
                users_found = EXCLUDED.users_found,
                users_done = EXCLUDED.users_done,
                trades_stored = EXCLUDED.trades_stored,
                updated_at = now()
            """,
            (condition_id, status, users_found, users_done, trades_stored),
        )
    conn.commit()
