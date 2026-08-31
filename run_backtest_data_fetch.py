"""Runs one "Update backtest data" job (see web/app.py's POST /api/
backtest_data_fetch) as an isolated subprocess, spawned by the dashboard -
same isolation rationale as run_backtest.py: fetch_backtest_data.py needs
its own dedicated IB Gateway client connection (IBKR_BACKTEST_CLIENT_ID)
and can run for a long time against the full S&P 500 universe, neither of
which the always-on dashboard process (which never talks to IBKR directly)
should hold itself.

Always the full S&P 500 universe at the default initial-backfill depth,
same as run_service.py's own weekly schedule - fetch_backtest_data.run_fetch
is incremental per symbol regardless (a symbol already near that depth just
gets its recent gap topped up), so this is cheap to re-run on demand between
scheduled runs, not a second full backfill.
"""
import argparse
from datetime import date
from pathlib import Path

import fetch_backtest_data
from src import db
from src.sp500_tickers import SP500_TICKERS

PROJECT_DIR = Path(__file__).resolve().parent

# SPY/QQQ ride along with the regular S&P 500 universe here (same cache,
# same incremental fetch_symbol/fetch_symbol_range - they're ordinary
# Stock contracts, nothing ES-specific about them, unlike fetch_es_
# backtest_data.py's own separate futures-contract fetch) - added for the
# Trade Telemetry Dashboard's own Market Context snapshots (see src.
# telemetry_engine.MARKET_CONTEXT_SYMBOLS), which read SPY/QQQ bars the
# exact same way it reads any traded symbol's own cache.
FETCH_UNIVERSE = list(SP500_TICKERS) + ["SPY", "QQQ"]


def run(fetch_id: int, mode: str = "paper"):
    record = db.get_backtest_data_fetch(fetch_id)
    if record is None:
        print(f"backtest data fetch {fetch_id}: not found")
        return
    db.start_backtest_data_fetch(fetch_id)
    try:
        # start_date/end_date only set for an explicit "Add Backtest Data"
        # date-range request (see db.create_backtest_data_fetch's own
        # docstring) - the ordinary "Update backtest data" button never
        # sets them, so this stays the same top-up run it always was.
        if record["start_date"] and record["end_date"]:
            summary = fetch_backtest_data.run_fetch_range(
                record["account_id"], FETCH_UNIVERSE,
                date.fromisoformat(record["start_date"]), date.fromisoformat(record["end_date"]), mode,
            )
        else:
            summary = fetch_backtest_data.run_fetch(
                record["account_id"], FETCH_UNIVERSE, fetch_backtest_data.DEFAULT_INITIAL_DURATION, mode,
            )
        db.finish_backtest_data_fetch(fetch_id, summary)
        print(
            f"backtest data fetch {fetch_id}: done "
            f"({summary['ok']} updated, {summary['up_to_date']} already current, {summary['errors']} errors)"
        )
    except Exception as exc:  # noqa: BLE001 - a bad run must record failure, not crash silently
        db.fail_backtest_data_fetch(fetch_id, f"{type(exc).__name__}: {exc}")
        print(f"backtest data fetch {fetch_id}: failed - {type(exc).__name__}: {exc}")


def main():
    db.init_db(seed_rules_path=PROJECT_DIR / "rules.json")
    parser = argparse.ArgumentParser()
    parser.add_argument("--fetch-id", type=int, required=True)
    parser.add_argument("--mode", choices=["paper", "live"], default="paper")
    args = parser.parse_args()
    run(args.fetch_id, args.mode)


if __name__ == "__main__":
    main()
