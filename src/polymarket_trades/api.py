import time
import requests

GAMMA_API_BASE = "https://gamma-api.polymarket.com"
DATA_API_BASE = "https://data-api.polymarket.com"

TRADE_FIELDS = (
    "transactionHash", "conditionId", "proxyWallet", "timestamp",
    "side", "size", "usdcSize", "price", "outcome", "outcomeIndex",
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


def fetch_unique_users(condition_id: str, limit: int = 500,
                       max_pages: int = 100, page_delay: float = 0.2) -> list[str]:
    unique_users = set()
    offset = 0
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

        for t in trades:
            addr = t.get("proxyWallet")
            if addr:
                unique_users.add(addr)

        if len(trades) < limit:
            break

        offset += limit
        page += 1
        time.sleep(page_delay)

    return sorted(unique_users)


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
        "outcome": record.get("outcome", ""),
        "outcome_index": int(record.get("outcomeIndex", 0) or 0),
    }


def fetch_user_trades(proxy_wallet: str, condition_id: str,
                      limit: int = 500, max_pages: int = 50,
                      page_delay: float = 0.05) -> list[dict]:
    all_trades = []
    offset = 0
    page = 0

    while page < max_pages:
        params = {
            "user": proxy_wallet,
            "market": condition_id,
            "limit": limit,
            "offset": offset,
        }
        try:
            resp = requests.get(f"{DATA_API_BASE}/activity", params=params)
            resp.raise_for_status()
            activities = resp.json()
        except requests.exceptions.RequestException:
            break

        if not activities:
            break

        for a in activities:
            if a.get("type") == "TRADE":
                all_trades.append(_extract_fields(a))

        if len(activities) < limit:
            break

        offset += limit
        page += 1
        time.sleep(page_delay)

    return all_trades
