"""One-off migration: replaces I2's "new high/low of day" default (which
turned out to rarely be true in practice - it requires the current price
to be a fresh intraday extreme at the EXACT moment checked, a narrow,
momentary condition) with a short EMA confirmation instead (see
cycle.py's I2_ema_above/I2_ema_below - price above/below EMA(9) on 5-min
bars), on every strategy that's still on the old default.

A strategy already using I2_rsi_above/I2_rsi_below (an existing
alternative, already applied to some strategies before this script
existed) is left untouched - this only replaces the specific
"I2_above_today_hod" default, not every non-EMA strategy.

Idempotent - re-running it is a no-op for a strategy that's already on
I2_ema_above/below, and safe to run any time (same reasoning as
apply_atr_stop_to_all_strategies.py).

Run on the server (needs the real data), not this dev sandbox.

Usage:
    python3 apply_ema_i2_to_all_strategies.py            # apply to every eligible strategy
    python3 apply_ema_i2_to_all_strategies.py --dry-run   # show what would change, don't write
    python3 apply_ema_i2_to_all_strategies.py --period 12 # use EMA(12) instead of the default EMA(9)
"""
import argparse
import json
from pathlib import Path

from src import db

PROJECT_DIR = Path(__file__).resolve().parent


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Print what would change without writing to the DB")
    parser.add_argument("--period", type=int, default=9, help="EMA period to use (default: 9)")
    args = parser.parse_args()

    db.init_db(seed_rules_path=PROJECT_DIR / "rules.json")
    account_id = db.get_default_account_id()
    strategies = db.list_strategies(account_id)

    changed = 0
    for s in strategies:
        full = db.get_strategy(s["id"])
        rules = json.loads(full["rules_json"])
        intraday = rules.get("intraday_filters", {})

        if "I2_rsi_above" in intraday or "I2_rsi_below" in intraday:
            print(f"#{s['id']} {s.get('key') or ''} {s['name']}: already RSI-based, skipping")
            continue
        if intraday.get("I2_ema_above") or intraday.get("I2_ema_below"):
            print(f"#{s['id']} {s.get('key') or ''} {s['name']}: already EMA-based, skipping")
            continue

        direction = s.get("direction")
        if direction == "long":
            intraday.pop("I2_above_today_hod", None)
            intraday["I2_ema_above"] = True
        elif direction == "short":
            intraday.pop("I2_above_today_hod", None)
            intraday["I2_ema_below"] = True
        else:
            print(f"#{s['id']} {s.get('key') or ''} {s['name']}: unrecognized direction {direction!r}, skipping")
            continue
        intraday["I2_ema_period"] = args.period

        print(f"#{s['id']} {s.get('key') or ''} {s['name']}: new-{'high' if direction == 'long' else 'low'}-of-day -> EMA({args.period})")
        if not args.dry_run:
            db.update_strategy(s["id"], rules)
        changed += 1

    verb = "would update" if args.dry_run else "updated"
    print(f"\n{verb} {changed}/{len(strategies)} strategies")


if __name__ == "__main__":
    main()
