"""One-shot historical backfill of the momentum detectors - see
momentum/backfill.py's module docstring for what this does and doesn't
approximate. Not a scheduled job; run by hand (or once from the
dashboard/CLI) when you want a same-night read instead of waiting for
phase 1's live scan to accumulate signals.

    python run_momentum_backfill.py                    # full NASDAQ universe, 45 days back
    python run_momentum_backfill.py --limit 300         # smaller universe, for a quick smoke test
    python run_momentum_backfill.py --days-back 30 --max-fetches 100
"""
import argparse
import json
import logging
import sys

from momentum.backfill import run_backfill
from momentum.universe import fetch_nasdaq_listed_symbols

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s",
                    handlers=[logging.StreamHandler(sys.stdout)])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days-back", type=int, default=45)
    ap.add_argument("--max-fetches", type=int, default=200)
    ap.add_argument("--limit", type=int, default=None, help="cap the NASDAQ universe size (smoke testing)")
    args = ap.parse_args()

    universe = None
    if args.limit:
        universe = fetch_nasdaq_listed_symbols()[: args.limit]

    summary = run_backfill(days_back=args.days_back, max_intraday_fetches=args.max_fetches, universe=universe)
    print(json.dumps(summary, indent=2, default=str))


if __name__ == "__main__":
    main()
