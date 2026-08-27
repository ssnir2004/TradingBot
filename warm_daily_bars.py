"""One-off maintenance script: pre-fetches and caches yfinance daily bars
(SMA200/50, D1-D3) for a whole symbol universe, so a later backtest never
has to fetch them itself.

backtest_engine.fetch_daily_bars() already caches every symbol's daily
history to disk forever - the problem this script solves is the FIRST
run against a cold cache: a backtest's own symbol loop fetches whatever
isn't cached yet inline, bounded by a per-symbol timeout and a small
worker pool (see backtest_engine._YF_TIMEOUT_SECONDS/_DAILY_FETCH_WORKERS),
but even with those safeguards, several hundred cold symbols can still
take well past 15 minutes wall-clock if Yahoo is throttling the box -
which is exactly the "Abandoned by worker" timeout a remote backtest job
is reaped at (see requeue_abandoned_worker_backtests), so a cold cache can
make a perfectly healthy worker look stuck/broken on its first few jobs.

Run this ONCE per machine (server and/or each remote worker) before
relying on it for backtests, with no job-timeout pressure attached, and
re-run occasionally to pick up new symbols in the universe (already-
cached symbols are skipped fast - see fetch_daily_bars' own freshness
check). Needs real internet access, same as build_custom_universe.py.
"""
import argparse
import concurrent.futures
import sys

from src import backtest_engine
from src.custom_universes import load_custom_universe


def _load_symbols(universe: str, symbols_file: str | None) -> list[str]:
    if symbols_file:
        with open(symbols_file) as f:
            return [line.strip() for line in f if line.strip()]
    symbols = load_custom_universe(universe)
    if not symbols:
        raise SystemExit(
            f"No cached universe found for {universe!r} on this machine "
            f"(data/universes/{universe}.json missing, empty, or stale). "
            "Either copy that file over from the server, or pass "
            "--symbols-file with a plain text list (one ticker per line)."
        )
    return symbols


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--universe", default="ixic_large_beta_buy",
                         help="Custom universe key from src/custom_universes.py (default: ixic_large_beta_buy)")
    parser.add_argument("--symbols-file",
                         help="Plain text file, one ticker per line, instead of a cached universe")
    args = parser.parse_args()

    symbols = _load_symbols(args.universe, args.symbols_file)
    print(f"Warming daily-bar cache for {len(symbols)} symbol(s)...")

    ok = 0
    failed = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=backtest_engine._DAILY_FETCH_WORKERS) as pool:
        future_to_symbol = {pool.submit(backtest_engine.fetch_daily_bars, s): s for s in symbols}
        for i, future in enumerate(concurrent.futures.as_completed(future_to_symbol), 1):
            symbol = future_to_symbol[future]
            bars = future.result()
            if bars is None:
                failed.append(symbol)
                print(f"[{i}/{len(symbols)}] {symbol}: FAILED (no daily bars)")
            else:
                ok += 1
                print(f"[{i}/{len(symbols)}] {symbol}: {len(bars)} bars cached")

    print(f"\nDone: {ok} cached, {len(failed)} failed.")
    if failed:
        print("Failed symbols:", ", ".join(failed))


if __name__ == "__main__":
    main()
