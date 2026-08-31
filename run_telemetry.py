"""Runs one "Generate Telemetry" job (see web/app.py's POST /api/telemetry)
as an isolated subprocess, spawned by the dashboard - same isolation
rationale as run_backtest.py/run_optimization.py: replaying every closed
trade of an already-finished backtest against its own cached bars (see
src/telemetry_engine.py) is real pandas work for a backtest with hundreds
of trades, so it stays off the always-on dashboard process's own event
loop.
"""
import argparse
from pathlib import Path

from src import db, telemetry_engine

PROJECT_DIR = Path(__file__).resolve().parent


def run(run_id: int):
    record = db.get_telemetry_run(run_id)
    if record is None:
        print(f"telemetry run {run_id}: not found")
        return
    db.start_telemetry_run(run_id)
    try:
        summary = telemetry_engine.generate_telemetry_for_backtest(record["account_id"], record["backtest_id"])
        db.finish_telemetry_run(run_id, summary)
        print(
            f"telemetry run {run_id}: done "
            f"({summary['trades_processed']} trade(s) processed, {summary['trades_skipped']} skipped)"
        )
    except Exception as exc:  # noqa: BLE001 - a bad run must record failure, not crash silently
        db.fail_telemetry_run(run_id, f"{type(exc).__name__}: {exc}")
        print(f"telemetry run {run_id}: failed - {type(exc).__name__}: {exc}")


def main():
    db.init_db(seed_rules_path=PROJECT_DIR / "rules.json")
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", type=int, required=True)
    args = parser.parse_args()
    run(args.run_id)


if __name__ == "__main__":
    main()
