from polymarket_trades.api import fetch_event_markets, fetch_market_trades
from polymarket_trades.db import insert_trades, get_progress, set_progress


def collect_market(conn, condition_id: str, *, on_progress=None) -> int:
    progress = get_progress(conn, condition_id)

    if progress and progress["status"] == "done":
        return 0

    start_offset = 0
    total_new = 0
    pages_fetched = 0
    if progress and progress["status"] == "collecting":
        start_offset = progress["offset_reached"]
        total_new = progress["trades_stored"]
        pages_fetched = progress["pages_fetched"]

    set_progress(conn, condition_id, status="collecting",
                 pages_fetched=pages_fetched, offset_reached=start_offset,
                 trades_stored=total_new)

    for batch in fetch_market_trades(condition_id, start_offset=start_offset):
        inserted = insert_trades(conn, batch)
        total_new += inserted
        pages_fetched += 1
        start_offset += len(batch)

        set_progress(conn, condition_id, status="collecting",
                     pages_fetched=pages_fetched, offset_reached=start_offset,
                     trades_stored=total_new)

        if on_progress:
            on_progress(condition_id, pages_fetched, total_new)

    set_progress(conn, condition_id, status="done",
                 pages_fetched=pages_fetched, offset_reached=start_offset,
                 trades_stored=total_new)
    return total_new


def collect_event(conn, slug: str, *, question_filter: str | None = None,
                  on_progress=None) -> int:
    markets = fetch_event_markets(slug, question_filter=question_filter)
    total = 0
    for market in markets:
        cid = market.get("conditionId")
        if not cid:
            continue
        total += collect_market(conn, cid, on_progress=on_progress)
    return total
