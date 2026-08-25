"""Runs one backtest (see web/app.py's POST /api/backtests) as an isolated
subprocess, spawned by the dashboard - unlike the in-process background
thread this used to run as, a subprocess's memory is fully released back
to the OS the moment it exits, regardless of how much it used getting
there. A full-universe backtest holds every cached symbol's entire
intraday history in memory for the whole simulation
(backtest_engine.simulate_strategy), which can add up to a real amount of
memory on a small server - running it in-process risked that landing
directly on the dashboard itself (the one thing everything else -
Account Holdings, trading controls, this backtest's own progress page -
depends on staying up), not a disposable one-off process. No IBKR
connection needed either way (backtest_engine.py only reads the local
historical-bar cache plus yfinance for daily bars), so there's no
client-id concern here the way there is for trade.py etc - this only
needs its own DB connection, same as every other standalone script.

Only ever runs a backtest whose execution_mode is 'local' (the default -
see api_create_backtest); a 'remote' one is never spawned like this at
all, it just sits at status='pending' until a worker claims it over HTTP
instead (see docs/worker.md, backtest_worker.py). Both paths share the
actual "run these strategies" logic via src/backtest_runner.py.

The backtest's params (symbols, date range, strategies, risk settings)
are already stored as JSON on the backtests row by db.create_backtest at
request time - this script just re-reads them via --backtest-id rather
than needing them passed on the command line.
"""
import argparse
import json
from pathlib import Path

from src import backtest_runner, db

PROJECT_DIR = Path(__file__).resolve().parent


def run_backtest(backtest_id: int):
    record = db.get_backtest(backtest_id)
    if record is None:
        print(f"backtest {backtest_id}: not found")
        return
    params = record["params"]
    db.start_backtest(backtest_id)
    try:
        strategies = {}
        for strategy_id in params["strategy_ids"]:
            strategy = db.get_strategy(strategy_id)
            if strategy:
                strategies[str(strategy_id)] = {
                    "name": strategy["name"],
                    "direction": strategy["direction"],
                    "rules": json.loads(strategy["rules_json"]),
                }
        results = backtest_runner.run_backtest_params(params, strategies)
        db.finish_backtest(backtest_id, results)
        print(f"backtest {backtest_id}: done ({len(results)} strategy result(s))")
    except Exception as exc:  # noqa: BLE001 - a bad run must record failure, not crash silently
        db.fail_backtest(backtest_id, f"{type(exc).__name__}: {exc}")
        print(f"backtest {backtest_id}: failed - {type(exc).__name__}: {exc}")


def main():
    db.init_db(seed_rules_path=PROJECT_DIR / "rules.json")
    parser = argparse.ArgumentParser()
    parser.add_argument("--backtest-id", type=int, required=True)
    args = parser.parse_args()
    run_backtest(args.backtest_id)


if __name__ == "__main__":
    main()
