import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS condition_ids (
    id    SMALLINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    value TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS proxy_wallets (
    id    INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    value TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS trades (
    id                BIGSERIAL PRIMARY KEY,
    transaction_hash  BYTEA NOT NULL,
    condition_id      SMALLINT NOT NULL REFERENCES condition_ids(id),
    proxy_wallet      INTEGER NOT NULL REFERENCES proxy_wallets(id),
    timestamp         TIMESTAMPTZ NOT NULL,
    side              BOOLEAN NOT NULL,
    size              NUMERIC NOT NULL,
    usdc_size         NUMERIC NOT NULL,
    price             NUMERIC NOT NULL,
    outcome_index     SMALLINT NOT NULL,
    UNIQUE (transaction_hash, condition_id, proxy_wallet, outcome_index)
);

CREATE INDEX IF NOT EXISTS idx_trades_condition_id ON trades (condition_id);
CREATE INDEX IF NOT EXISTS idx_trades_proxy_wallet ON trades (proxy_wallet);
CREATE INDEX IF NOT EXISTS idx_trades_timestamp    ON trades (timestamp);

CREATE TABLE IF NOT EXISTS collection_progress (
    condition_id  SMALLINT PRIMARY KEY REFERENCES condition_ids(id),
    status        TEXT NOT NULL DEFAULT 'pending',
    users_found   INTEGER DEFAULT 0,
    users_done    INTEGER DEFAULT 0,
    trades_stored INTEGER DEFAULT 0,
    updated_at    TIMESTAMPTZ DEFAULT now()
);
"""

_condition_id_cache: dict[str, int] = {}
_proxy_wallet_cache: dict[str, int] = {}


def _get_or_create_id(cur, table: str, value: str) -> int:
    cur.execute(
        f"INSERT INTO {table} (value) VALUES (%s) "
        "ON CONFLICT (value) DO NOTHING RETURNING id",
        (value,),
    )
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute(f"SELECT id FROM {table} WHERE value = %s", (value,))
    return cur.fetchone()[0]


def _resolve_condition_id(cur, value: str) -> int:
    if value not in _condition_id_cache:
        _condition_id_cache[value] = _get_or_create_id(cur, "condition_ids", value)
    return _condition_id_cache[value]


def _resolve_proxy_wallet(cur, value: str) -> int:
    if value not in _proxy_wallet_cache:
        _proxy_wallet_cache[value] = _get_or_create_id(cur, "proxy_wallets", value)
    return _proxy_wallet_cache[value]


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
                            outcome_index)
        VALUES %s
        ON CONFLICT DO NOTHING
    """
    with conn.cursor() as cur:
        unique_cids = {t["condition_id"] for t in trades}
        unique_wallets = {t["proxy_wallet"] for t in trades}
        for cid in unique_cids:
            _resolve_condition_id(cur, cid)
        for pw in unique_wallets:
            _resolve_proxy_wallet(cur, pw)

        rows = [
            (
                bytes.fromhex(t["transaction_hash"].removeprefix("0x")),
                _condition_id_cache[t["condition_id"]],
                _proxy_wallet_cache[t["proxy_wallet"]],
                datetime.fromtimestamp(t["timestamp"], tz=timezone.utc),
                t["side"] == "BUY",
                t["size"],
                t["usdc_size"],
                t["price"],
                t["outcome_index"],
            )
            for t in trades
        ]
        execute_values(cur, sql, rows, page_size=len(rows))
        inserted = cur.rowcount
    conn.commit()
    return inserted


def get_progress(conn, condition_id: str) -> dict | None:
    with conn.cursor() as cur:
        cid_fk = _resolve_condition_id(cur, condition_id)
        cur.execute(
            "SELECT status, users_found, users_done, trades_stored "
            "FROM collection_progress WHERE condition_id = %s",
            (cid_fk,),
        )
        row = cur.fetchone()
    if not row:
        return None
    return {
        "condition_id": condition_id,
        "status": row[0],
        "pages_fetched": row[1],
        "offset_reached": row[2],
        "trades_stored": row[3],
    }


def set_progress(conn, condition_id: str, *, status: str,
                 pages_fetched: int = 0, offset_reached: int = 0,
                 trades_stored: int = 0):
    with conn.cursor() as cur:
        cid_fk = _resolve_condition_id(cur, condition_id)
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
            (cid_fk, status, pages_fetched, offset_reached, trades_stored),
        )
    conn.commit()
