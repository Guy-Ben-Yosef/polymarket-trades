from polymarket_trades.api import fetch_event_markets, fetch_unique_users, fetch_user_trades
from polymarket_trades.db import insert_trades, get_progress, set_progress


def collect_market(conn, condition_id: str, *, on_progress=None) -> int:
    progress = get_progress(conn, condition_id)

    if progress and progress["status"] == "done":
        return 0

    # Step 1: discover users
    skip_users = 0
    if progress and progress["status"] == "collecting_activity":
        skip_users = progress["users_done"]
    else:
        set_progress(conn, condition_id, status="collecting_users")

    users = fetch_unique_users(condition_id)
    total_users = len(users)

    set_progress(conn, condition_id, status="collecting_activity",
                 users_found=total_users, users_done=skip_users,
                 trades_stored=progress["trades_stored"] if progress else 0)

    # Step 2: fetch activity per user
    total_new = progress["trades_stored"] if progress else 0

    for i, wallet in enumerate(users):
        if i < skip_users:
            continue

        trades = fetch_user_trades(wallet, condition_id)
        inserted = insert_trades(conn, trades)
        total_new += inserted

        set_progress(conn, condition_id, status="collecting_activity",
                     users_found=total_users, users_done=i + 1,
                     trades_stored=total_new)

        if on_progress:
            on_progress(condition_id, i + 1, total_users)

    set_progress(conn, condition_id, status="done",
                 users_found=total_users, users_done=total_users,
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
