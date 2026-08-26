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
from pathlib import Path

import fetch_backtest_data
from src import db
from src.sp500_tickers import SP500_TICKERS

PROJECT_DIR = Path(__file__).resolve().parent


def run(fetch_id: int):
    record = db.get_backtest_data_fetch(fetch_id)
    if record is None:
        print(f"backtest data fetch {fetch_id}: not found")
        return
    db.start_backtest_data_fetch(fetch_id)
    try:
        summary = fetch_backtest_data.run_fetch(
            record["account_id"], list(SP500_TICKERS), fetch_backtest_data.DEFAULT_INITIAL_DURATION, "paper",
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
    args = parser.parse_args()
    run(args.fetch_id)


if __name__ == "__main__":
    main()
