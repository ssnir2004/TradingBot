"""V3-vs-v5-vs-v5.1 comparison and quality-score analysis for ORB Long
v5.1's two-layer entry gate (mandatory filters + a 0-5 quality score -
see src/db.py's own v5.1 comment and backtest_engine._quality_score).

Runs REAL backtests (src/backtest_runner.run_one_strategy - the same
simulation every dashboard backtest goes through) for:
  - ORB Long v3 (baseline, no quality filters at all)
  - ORB Long v5 (v5's own all-6-filters-must-pass gate, unchanged)
  - ORB Long v5.1 (the actual delivered strategy: 3 mandatory filters +
    min_score=3 of 5)
  - a DIAGNOSTIC variant of v5.1 with min_score=0 (same 3 mandatory
    filters, same 5 conditions computed and attached to every trade via
    "quality_score"/"quality_score_detail", but nothing rejected on
    score alone) - this is what the score-bucketed (0-5) table and the
    edge-validation section (win rate/PF at score>=3/>=4/==5) are built
    from, NOT the real v5.1 run.

Why a separate diagnostic run rather than just filtering v5.1's own
trades by score post-hoc: v5.1's own backtest only ever SEES the
signals that scored >=3 (everything else is rejected before ever being
sized/entered), so it has no way to tell us what a score-1 or score-2
signal would have looked like. The diagnostic run takes every
mandatory-filter-passing signal regardless of score, so slicing IT by
score (>=0, >=1, ..., ==5) covers the full 0-5 distribution the report
asks for. It is deliberately a SEPARATE real backtest from v5.1's own
(rather than mixing v5.1's own trades with the diagnostic's), because
position sizing depends on portfolio_value/concurrent-position state
that evolves along the specific sequence of trades actually taken - a
backtest that takes more signals (the diagnostic) can size later trades
differently than one that only ever saw the score>=3 subset, even for a
signal both would have taken. Same reasoning already used for the
filter-diagnostics/ablation runs in analyze_v5_ablation.py, which this
script imports _run/_stats/_trade_key from rather than re-implementing.

Read-only: never modifies the DB. Run on the server (needs the real
cached data), not this dev sandbox.

Usage:
    python3 analyze_v5_1_score.py --start-date 2024-01-01 --end-date 2024-06-30
    python3 analyze_v5_1_score.py --start-date 2024-01-01 --end-date 2024-06-30 --account-id 1
"""
import argparse
import copy
import json
import statistics
from datetime import date
from pathlib import Path

from analyze_strategy import find_strategy, fmt_money, section
from analyze_v5_ablation import _run, _stats, _trade_key, print_stats_row
from src import backtest_data, backtest_engine, db

PROJECT_DIR = Path(__file__).resolve().parent

DEFAULT_PORTFOLIO_VALUE = 100_000.0
DEFAULT_MAX_RISK_PCT = 1.0
DEFAULT_MAX_TRADES_PER_DAY = 5
DEFAULT_COMMISSION_PER_TRADE = 1.5

SCORE_CONDITION_NAMES = (
    "es_vwap_distance", "atr", "breakout_atr_ratio", "es_or_direction", "es_trend_strength",
)


def build_diagnostic_rules(v51_rules: dict) -> dict:
    """v5.1's own rules with quality_score_filters.min_score forced to 0
    - same 3 mandatory filters, same 5 scored conditions computed and
    attached to every trade, but nothing rejected on score alone (see
    this module's own docstring for why this needs to be its own
    backtest rather than a post-hoc filter of v5.1's real trades)."""
    diagnostic = copy.deepcopy(v51_rules)
    diagnostic["quality_score_filters"]["min_score"] = 0
    return diagnostic


def score_bucket_table(pairs: list[dict]) -> dict:
    """{score: _stats(...)} for score 0-5, off the diagnostic run's own
    pairs (each already carries "quality_score" via the **entry_ctx
    merge in backtest_engine.py)."""
    buckets = {score: [] for score in range(6)}
    for p in pairs:
        score = p.get("quality_score")
        if score is not None:
            buckets[score].append(p)
    return {score: _stats(bucket_pairs) for score, bucket_pairs in buckets.items()}


def edge_validation(pairs: list[dict]) -> dict:
    """win_rate_pct/profit_factor for score>=3, score>=4, score==5 -
    off the SAME diagnostic pairs as score_bucket_table (see module
    docstring for why these are cuts of the diagnostic run, not v5.1's
    own real trade set)."""
    cuts = {
        "Score >= 3": [p for p in pairs if (p.get("quality_score") or 0) >= 3],
        "Score >= 4": [p for p in pairs if (p.get("quality_score") or 0) >= 4],
        "Score == 5": [p for p in pairs if (p.get("quality_score") or 0) == 5],
    }
    return {name: _stats(cut_pairs) for name, cut_pairs in cuts.items()}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--account-id", type=int, default=None)
    parser.add_argument("--portfolio-value", type=float, default=DEFAULT_PORTFOLIO_VALUE)
    parser.add_argument("--max-risk-pct", type=float, default=DEFAULT_MAX_RISK_PCT)
    parser.add_argument("--max-trades-per-day", type=int, default=DEFAULT_MAX_TRADES_PER_DAY)
    parser.add_argument("--commission-per-trade", type=float, default=DEFAULT_COMMISSION_PER_TRADE)
    args = parser.parse_args()

    start_date = date.fromisoformat(args.start_date)
    end_date = date.fromisoformat(args.end_date)

    db.init_db(seed_rules_path=PROJECT_DIR / "rules.json")
    account_id = args.account_id if args.account_id is not None else db.get_default_account_id()

    v3_strategy = find_strategy(account_id, "ORB Long v3 (Early Profit Lock")
    v5_strategy = find_strategy(account_id, "ORB Long v5 (Quality Entry Filter)")
    v51_strategy = find_strategy(account_id, "ORB Long v5.1 (Quality Score Entry System)")
    v3_rules = json.loads(db.get_strategy(v3_strategy["id"])["rules_json"])
    v5_rules = json.loads(db.get_strategy(v5_strategy["id"])["rules_json"])
    v51_rules = json.loads(db.get_strategy(v51_strategy["id"])["rules_json"])
    direction = v51_strategy["direction"]

    symbols = backtest_data.cached_symbols(backtest_engine.BAR_SIZE)
    if not symbols:
        raise SystemExit("No symbols have cached historical bars yet - run fetch_backtest_data.py on the server first.")

    section(f"ORB Long v3 vs v5 vs v5.1 Quality Score Study — {start_date} to {end_date} "
            f"({len(symbols)} symbol(s) with cached bars)")

    def run_variant(name, rules):
        print(f"Running {name}...")
        result = _run(name, direction, rules, symbols, start_date, end_date,
                       args.portfolio_value, args.max_risk_pct, args.max_trades_per_day, args.commission_per_trade)
        return result["pairs"]

    v3_pairs = run_variant("ORB Long v3 (baseline)", v3_rules)
    v5_pairs = run_variant("ORB Long v5 (all filters must pass)", v5_rules)
    v51_pairs = run_variant("ORB Long v5.1 (mandatory + score >= 3)", v51_rules)
    diagnostic_pairs = run_variant("v5.1 diagnostic (min_score=0, full distribution)", build_diagnostic_rules(v51_rules))

    # ---------------------------------------------------------- Performance
    section("Performance Comparison: v3 vs v5 vs v5.1")
    v3_stats, v5_stats, v51_stats = _stats(v3_pairs), _stats(v5_pairs), _stats(v51_pairs)
    print_stats_row("ORB Long v3 (baseline):", v3_stats, width=32)
    print_stats_row("ORB Long v5 (all must pass):", v5_stats, width=32)
    print_stats_row("ORB Long v5.1 (score >= 3):", v51_stats, width=32)
    for label, s in (("v3", v3_stats), ("v5", v5_stats), ("v5.1", v51_stats)):
        print(f"  {label}: avg MFE R={s['avg_mfe_r']}  avg MAE R={s['avg_mae_r']}")

    # ------------------------------------------------- Quality Score Analysis
    section("Quality Score Analysis — outcome by score bucket (0-5, diagnostic run: mandatory filters only, no score threshold)")
    print(f"({len(diagnostic_pairs)} total mandatory-filter-passing trade(s) in the diagnostic run)\n")
    buckets = score_bucket_table(diagnostic_pairs)
    print(f"{'Score':<8}{'Trades':<10}{'Win Rate':<12}{'Avg R':<10}{'PF':<10}{'Net P&L':<14}")
    for score in range(6):
        s = buckets[score]
        print(f"{score:<8}{s['total_trades']:<10}{str(s['win_rate_pct']) + '%':<12}"
              f"{str(s['avg_r']):<10}{str(s['profit_factor']):<10}{fmt_money(s['net_pnl_usd']):<14}")

    # ----------------------------------------------------------- Edge Validation
    section("Edge Validation — win rate / profit factor by score cutoff (same diagnostic run)")
    cuts = edge_validation(diagnostic_pairs)
    for name, s in cuts.items():
        print(f"{name:<14} trades={s['total_trades']:4d}  win_rate={s['win_rate_pct']}%  "
              f"PF={s['profit_factor']}  net_pnl={fmt_money(s['net_pnl_usd'])}  "
              f"expectancy=${s['expectancy_usd']}  max_dd={fmt_money(s['max_drawdown_usd'])}")

    section("Interpretation notes")
    print("Read the score-bucket table top to bottom: if win rate/avg R/PF genuinely improve as")
    print("the score rises, the scoring system is capturing real signal. A table that's flat or")
    print("non-monotonic (e.g. score 2 outperforms score 4) means the score isn't actually tracking")
    print("quality in this window - a candidate for simplifying back down, or re-weighting which")
    print("conditions count. The Edge Validation section's job is to compare min_score=3 (v5.1's")
    print("own threshold) against 4 and 5 - a HIGHER threshold that keeps improving PF/expectancy")
    print("without collapsing the trade count too far is a stronger threshold than 3; one that")
    print("degrades (fewer trades, no better PF) means 3 was already close to the right cut, or")
    print("the extra selectivity is overfitting to this specific date range/symbol set.")


if __name__ == "__main__":
    main()
