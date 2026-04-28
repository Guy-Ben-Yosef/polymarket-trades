import time
from collections.abc import Generator

import requests

GAMMA_API_BASE = "https://gamma-api.polymarket.com"
DATA_API_BASE = "https://data-api.polymarket.com"

TRADE_FIELDS = (
    "transactionHash", "conditionId", "proxyWallet", "timestamp",
    "side", "size", "usdcSize", "price", "outcomeIndex",
)


def fetch_event_markets(slug: str, question_filter: str | None = None) -> list[dict]:
    url = f"{GAMMA_API_BASE}/events/slug/{slug}"
    resp = requests.get(url)
    resp.raise_for_status()
    event = resp.json()
    markets = event.get("markets", [])
    if question_filter:
        kw = question_filter.lower()
        markets = [m for m in markets if kw in m.get("question", "").lower()]
    return markets


def _extract_fields(record: dict) -> dict:
    return {
        "transaction_hash": record.get("transactionHash", ""),
        "condition_id": record.get("conditionId", ""),
        "proxy_wallet": record.get("proxyWallet", ""),
        "timestamp": record.get("timestamp", 0),
        "side": record.get("side", ""),
        "size": float(record.get("size", 0) or 0),
        "usdc_size": float(record.get("usdcSize", 0) or 0),
        "price": float(record.get("price", 0) or 0),
        "outcome_index": int(record.get("outcomeIndex", 0) or 0),
    }


def fetch_market_trades(condition_id: str, *, limit: int = 500,
                        max_pages: int = 200, page_delay: float = 0.2,
                        start_offset: int = 0) -> Generator[list[dict], None, None]:
    offset = start_offset
    page = 0

    while page < max_pages:
        params = {"market": condition_id, "limit": limit, "offset": offset}
        try:
            resp = requests.get(f"{DATA_API_BASE}/trades", params=params)
            resp.raise_for_status()
            trades = resp.json()
        except requests.exceptions.RequestException:
            break

        if not trades:
            break

        yield [_extract_fields(t) for t in trades]

        if len(trades) < limit:
            break

        offset += limit
        page += 1
        time.sleep(page_delay)
