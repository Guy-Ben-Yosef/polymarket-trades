import argparse
import os
import sys

from polymarket_trades.db import init_db
from polymarket_trades.collector import collect_event, collect_market


def _progress(condition_id, pages_fetched, trades_stored):
    print(f"\r  [{condition_id[:10]}...] pages: {pages_fetched}, trades: {trades_stored}", end="", flush=True)


def main():
    parser = argparse.ArgumentParser(
        prog="polymarket-trades",
        description="Collect Polymarket trades into PostgreSQL.",
    )
    parser.add_argument(
        "--dsn",
        default=os.environ.get("POLYMARKET_TRADES_DSN"),
        help="PostgreSQL connection string (or set POLYMARKET_TRADES_DSN env var)",
    )

    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--event-slug", help="Polymarket event slug (collects all sub-markets)")
    source.add_argument("--condition-id", help="Single market condition ID")

    parser.add_argument("--question-filter", help="Filter event markets by keyword in question text")

    args = parser.parse_args()

    if not args.dsn:
        parser.error("--dsn is required (or set POLYMARKET_TRADES_DSN)")

    conn = init_db(args.dsn)
    try:
        if args.event_slug:
            print(f"Collecting trades for event: {args.event_slug}")
            total = collect_event(conn, args.event_slug,
                                  question_filter=args.question_filter,
                                  on_progress=_progress)
        else:
            print(f"Collecting trades for market: {args.condition_id}")
            total = collect_market(conn, args.condition_id, on_progress=_progress)

        print(f"\nDone. {total} trades stored.")
    except KeyboardInterrupt:
        print("\nInterrupted. Progress saved — re-run to resume.")
        sys.exit(1)
    finally:
        conn.close()
