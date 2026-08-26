"""One-off migration: sets exit.initial_stop_rule = "atr_2x" on every
strategy (see cycle.py's INITIAL_STOP_RULES - places the initial stop at
entry price +/- 2x ATR(14) instead of a flat 1% off the session low/high,
so a stock's actual daily volatility sets the stop distance instead of
the same fixed distance for every symbol).

Idempotent - re-running it is a no-op for a strategy that's already on
atr_2x, and safe to run any time (the live/paper trading services and
new backtests only read a strategy's rules_json at the moment they need
it, never cache it in memory across runs).

Run on the server (needs the real data), not this dev sandbox.

Usage:
    python3 apply_atr_stop_to_all_strategies.py           # apply to every strategy
    python3 apply_atr_stop_to_all_strategies.py --dry-run  # show what would change, don't write
"""
import argparse
import json
from pathlib import Path

from src import db

PROJECT_DIR = Path(__file__).resolve().parent


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Print what would change without writing to the DB")
    args = parser.parse_args()

    db.init_db(seed_rules_path=PROJECT_DIR / "rules.json")

    # Strategies are shared templates, not account-scoped (see EXTRA_STRATEGY_PRESETS'
    # own comment in src/db.py) - account_id here only drives the is_active flag
    # in list_strategies' join, irrelevant to this migration, so any account works.
    account_id = db.get_default_account_id()
    strategies = db.list_strategies(account_id)

    changed = 0
    for s in strategies:
        full = db.get_strategy(s["id"])
        rules = json.loads(full["rules_json"])
        exit_cfg = rules.setdefault("exit", {})
        old_rule = exit_cfg.get("initial_stop_rule", "(unset - was using the side's own default)")
        if old_rule == "atr_2x":
            print(f"#{s['id']} {s.get('key') or ''} {s['name']}: already atr_2x, skipping")
            continue
        exit_cfg["initial_stop_rule"] = "atr_2x"
        print(f"#{s['id']} {s.get('key') or ''} {s['name']}: {old_rule} -> atr_2x")
        if not args.dry_run:
            db.update_strategy(s["id"], rules)
        changed += 1

    verb = "would update" if args.dry_run else "updated"
    print(f"\n{verb} {changed}/{len(strategies)} strategies")


if __name__ == "__main__":
    main()
