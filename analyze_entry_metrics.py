"""Statistical analysis of src/entry_metrics.py's point-in-time entry
context against trade outcomes, for one strategy pooled across every
'done' backtest that included it (same pooling/dedup as analyze_strategy.py
- reuses its find_strategy/pool_strategy_pairs/section/fmt_money helpers
rather than duplicating them).

Splits trades into Winners (Final R >= 2) and Losers (Final R <= -1) -
trades in between are excluded from the winner/loser comparison (per the
request this script implements) but still count toward each metric's own
correlation-with-Final-R, which uses the FULL diagnosable pool, not just
the two extreme groups, for more statistical power.

For every entry_metrics key: mean/median/stdev for winners and losers
separately (numeric/boolean metrics only - a bool encodes as 1.0/0.0 the
same way perf/trade_diagnostics never do elsewhere, done only here for
these stats; a categorical string metric like gap_direction gets a
category-frequency breakdown per group instead, not a fabricated numeric
stat), Pearson correlation with Final R (statistics.correlation, stdlib,
no new dependency - per this conversation's own "correlation-based, not
ML" scoping decision), ranked by |correlation| for the "which variables
most strongly predict" sections. "Recommended filters" tries a few
threshold splits per numeric/boolean metric and reports any that clear a
minimum sample size, ranked separately by win-rate/profit-factor/
expectancy improvement over the whole pool's own baseline.

Read-only: never modifies the DB. Run on the server (needs the real
data), not this dev sandbox.

Usage:
    python3 analyze_entry_metrics.py --strategy "ORB Long v2"
    python3 analyze_entry_metrics.py --strategy "ORB Short v3" --account-id 1
"""
import argparse
import statistics
from collections import Counter
from pathlib import Path

from analyze_strategy import find_strategy, fmt_money, pool_strategy_pairs, section
from src import db, entry_metrics, perf

PROJECT_DIR = Path(__file__).resolve().parent

WINNER_MIN_R = 2.0
LOSER_MAX_R = -1.0
MIN_SAMPLE = 10  # minimum trades in a group/filtered subset before its stats are reported at all


def _numeric_value(pair: dict, key: str) -> float | None:
    """None for a missing/categorical-string value; a bool encodes as
    1.0/0.0 for stats purposes only (see module docstring) - every other
    module in this codebase keeps bools as bools, this is local to this
    script's own numeric analysis."""
    v = pair.get(key)
    if v is None:
        return None
    if isinstance(v, bool):
        return 1.0 if v else 0.0
    if isinstance(v, (int, float)):
        return float(v)
    return None


def _is_categorical(pairs: list[dict], key: str) -> bool:
    return any(isinstance(p.get(key), str) for p in pairs)


def _group_stats(pairs: list[dict], key: str) -> dict | None:
    values = [v for v in (_numeric_value(p, key) for p in pairs) if v is not None]
    if len(values) < 2:
        return None
    return {"n": len(values), "mean": statistics.mean(values), "median": statistics.median(values),
            "stdev": statistics.stdev(values)}


def _category_breakdown(pairs: list[dict], key: str) -> dict:
    counts = Counter(p.get(key) for p in pairs if p.get(key) is not None)
    total = sum(counts.values())
    return {cat: (n, n / total * 100) for cat, n in counts.most_common()} if total else {}


def _correlation_with_final_r(all_pairs: list[dict], key: str) -> float | None:
    xs, ys = [], []
    for p in all_pairs:
        x, y = _numeric_value(p, key), p.get("final_r")
        if x is not None and y is not None:
            xs.append(x)
            ys.append(y)
    if len(xs) < MIN_SAMPLE or len(set(xs)) < 2:
        return None
    try:
        return statistics.correlation(xs, ys)
    except statistics.StatisticsError:
        return None


def _search_filters(all_pairs: list[dict], key: str) -> list[dict]:
    """Threshold-split candidates for one numeric/boolean metric: the
    natural True/False split for a boolean, else the 25th/50th/75th
    percentile of its own observed values - both directions (>=/<) at
    each threshold, kept only when the resulting subset clears
    MIN_SAMPLE trades."""
    values = sorted(v for v in (_numeric_value(p, key) for p in all_pairs) if v is not None)
    if len(values) < MIN_SAMPLE * 2:
        return []
    is_bool = all(v in (0.0, 1.0) for v in values)
    thresholds = [0.5] if is_bool else sorted({values[int(len(values) * q)] for q in (0.25, 0.5, 0.75)})
    out = []
    for t in thresholds:
        for op, label in ((lambda v, t=t: v >= t, f">= {t:.3g}"), (lambda v, t=t: v < t, f"< {t:.3g}")):
            subset = [p for p in all_pairs if (val := _numeric_value(p, key)) is not None and op(val)]
            if len(subset) < MIN_SAMPLE:
                continue
            agg = perf.aggregate(subset)
            final_rs = [p["final_r"] for p in subset if p.get("final_r") is not None]
            expectancy = round(statistics.mean(final_rs), 3) if final_rs else None
            out.append({
                "filter": f"{key} {label}", "n": len(subset), "win_rate_pct": agg["win_rate_pct"],
                "profit_factor": agg["profit_factor"], "expectancy_r": expectancy,
            })
    return out


def print_group_comparison(winners: list[dict], losers: list[dict], all_pairs: list[dict]) -> list[tuple]:
    """Returns [(key, |correlation| or -1 for categorical/unavailable), ...]
    for the feature-importance ranking below."""
    ranking = []
    for key in entry_metrics.ENTRY_METRICS_KEYS:
        if _is_categorical(all_pairs, key):
            w_breakdown = _category_breakdown(winners, key)
            l_breakdown = _category_breakdown(losers, key)
            if not w_breakdown and not l_breakdown:
                continue
            print(f"\n{key} (categorical):")
            cats = sorted(set(w_breakdown) | set(l_breakdown))
            for cat in cats:
                wn, wp = w_breakdown.get(cat, (0, 0.0))
                ln, lp = l_breakdown.get(cat, (0, 0.0))
                print(f"    {cat:20s}  winners {wn:4d} ({wp:5.1f}%)   losers {ln:4d} ({lp:5.1f}%)")
            continue

        w_stats, l_stats = _group_stats(winners, key), _group_stats(losers, key)
        corr = _correlation_with_final_r(all_pairs, key)
        if w_stats is None and l_stats is None and corr is None:
            continue
        ranking.append((key, abs(corr) if corr is not None else -1))
        corr_str = f"{corr:+.3f}" if corr is not None else "n/a"
        print(f"\n{key}  (correlation with Final R: {corr_str})")
        if w_stats:
            print(f"    winners (n={w_stats['n']:4d})  mean {w_stats['mean']:9.3f}  median {w_stats['median']:9.3f}  stdev {w_stats['stdev']:9.3f}")
        if l_stats:
            print(f"    losers  (n={l_stats['n']:4d})  mean {l_stats['mean']:9.3f}  median {l_stats['median']:9.3f}  stdev {l_stats['stdev']:9.3f}")
    return ranking


def print_feature_importance(ranking: list[tuple]):
    scored = sorted((r for r in ranking if r[1] >= 0), key=lambda r: -r[1])
    if not scored:
        print("No metric had enough data to compute a correlation.")
        return
    print(f"{'Metric':40s} {'|correlation|':>14s}")
    for key, abs_corr in scored[:25]:
        print(f"{key:40s} {abs_corr:14.3f}")


def print_recommended_filters(all_pairs: list[dict], baseline: dict):
    all_filters = []
    for key in entry_metrics.ENTRY_METRICS_KEYS:
        if _is_categorical(all_pairs, key):
            continue
        all_filters.extend(_search_filters(all_pairs, key))
    if not all_filters:
        print("Not enough trades yet for any filter to clear the minimum sample size "
              f"({MIN_SAMPLE} trades) - need a larger pooled backtest history.")
        return

    def _top(metric_key: str, better_fn, n: int = 8):
        candidates = [f for f in all_filters if f[metric_key] is not None]
        candidates.sort(key=lambda f: better_fn(f[metric_key]), reverse=True)
        return candidates[:n]

    print(f"\nBaseline (all {baseline['total_trades']} trades): win rate {baseline['win_rate_pct']}%, "
          f"profit factor {baseline['profit_factor']}, expectancy n/a (see per-filter expectancy_r below)\n")

    print("Top filters by WIN RATE:")
    for f in _top("win_rate_pct", lambda v: v):
        print(f"  {f['filter']:40s} n={f['n']:4d}  win rate {f['win_rate_pct']:5.1f}%  "
              f"profit factor {f['profit_factor']}  expectancy {f['expectancy_r']}R")

    print("\nTop filters by PROFIT FACTOR (numeric only, 'inf' sorts last):")
    numeric_pf = [f for f in all_filters if isinstance(f["profit_factor"], (int, float))]
    for f in sorted(numeric_pf, key=lambda f: -f["profit_factor"])[:8]:
        print(f"  {f['filter']:40s} n={f['n']:4d}  win rate {f['win_rate_pct']:5.1f}%  "
              f"profit factor {f['profit_factor']}  expectancy {f['expectancy_r']}R")

    print("\nTop filters by EXPECTANCY (avg Final R per trade):")
    for f in _top("expectancy_r", lambda v: v):
        print(f"  {f['filter']:40s} n={f['n']:4d}  win rate {f['win_rate_pct']:5.1f}%  "
              f"profit factor {f['profit_factor']}  expectancy {f['expectancy_r']}R")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--strategy", required=True, help="Substring match against strategy key or name")
    parser.add_argument("--account-id", type=int, default=None)
    args = parser.parse_args()

    db.init_db(seed_rules_path=PROJECT_DIR / "rules.json")
    account_id = args.account_id if args.account_id is not None else db.get_default_account_id()

    strategy = find_strategy(account_id, args.strategy)
    strategy_id = strategy["id"]

    backtests = db.list_done_backtest_results(account_id, include_archived=True)
    pooled = pool_strategy_pairs(backtests, strategy_id)
    all_pairs = [p for p in pooled["pairs"] if p.get("final_r") is not None]

    section(f"Entry Metrics Analysis: {strategy.get('key') or strategy_id} — {strategy['name']}")
    print(f"{len(pooled['pairs'])} pooled trade(s), {len(all_pairs)} with a computable Final R.")
    if not all_pairs:
        print("\nNo diagnosable trades (need mfe_price/mae_price, i.e. a backtest run after "
              "MFE/MAE tracking shipped) - nothing to analyze.")
        return
    if not any(p.get("rvol") is not None for p in all_pairs):
        print("\n** No entry_metrics data found on these pairs - this strategy's pooled backtests "
              "predate this feature. Re-run its backtest(s) to populate entry-time metrics. **")
        return

    winners = [p for p in all_pairs if p["final_r"] >= WINNER_MIN_R]
    losers = [p for p in all_pairs if p["final_r"] <= LOSER_MAX_R]
    print(f"Winners (Final R >= {WINNER_MIN_R}): {len(winners)}   Losers (Final R <= {LOSER_MAX_R}): {len(losers)}"
          f"   (excluded, in between: {len(all_pairs) - len(winners) - len(losers)})")
    if len(winners) < MIN_SAMPLE or len(losers) < MIN_SAMPLE:
        print(f"\n** SMALL SAMPLE WARNING: fewer than {MIN_SAMPLE} trades in one of the two groups - "
              "the comparison below is not statistically reliable yet. **")

    section("Winners vs Losers — per-metric stats and correlation with Final R")
    ranking = print_group_comparison(winners, losers, all_pairs)

    section("Feature importance (ranked by |correlation| with Final R)")
    print_feature_importance(ranking)

    section("Recommended filters")
    print_recommended_filters(all_pairs, perf.aggregate(all_pairs))


if __name__ == "__main__":
    main()
