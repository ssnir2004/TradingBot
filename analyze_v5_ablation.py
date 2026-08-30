"""V3-vs-V5 comparison, per-filter diagnostics, and a filter ablation
study for ORB Long v5's quality_filters gate (see src/db.py's own v5
comment and backtest_engine._quality_filters_pass).

Runs REAL backtests (src/backtest_runner.run_one_strategy - the same
simulation every dashboard backtest goes through, not a re-read of
already-pooled DB results) for:
  - ORB Long v3 (baseline, no quality filters at all)
  - ORB Long v5 (all 6 filters: entry time, pullbacks, ES direction,
    ES distance, ATR, breakout ATR ratio)
  - 5 single-filter-removed ablation variants of v5 (entry time,
    pullbacks, ES distance, ATR, breakout ATR ratio - NOT ES direction,
    which the request treats as a non-negotiable baseline gate, not
    something to ablate)

all over the SAME symbols/date range, so every comparison is apples-to-
apples. Symbols default to every symbol with cached intraday bars (same
default web/app.py's own New Backtest form uses) - each variant's own
universe_filters/custom_universe still narrows it down internally exactly
like a normal backtest.

"Number/P&L/win rate/avg R of trades removed by filter X" (the Reporting
Requirements' own Filter Diagnostics section) is answered by SET
DIFFERENCE, not a separate counterfactual simulation: a signal that
appears in "v5 without filter X"'s own trade set but NOT in full v5's own
trade set is, by construction, exactly the set filter X alone removed
(every other filter held at its normal threshold) - trades are matched by
their own (symbol, entry_timestamp) identity, unique per entry since
every strategy here is long_only (one entry model, one signal per bar).
The same 5 ablation runs also answer the Additional Analysis section's
own aggregate-level "impact of removing filter X" question - no
redundant simulation between the two sections.

"Expectancy" here is reported in DOLLAR terms (net P&L / total trades) -
deliberately NOT the same number as "Average R multiple per trade",
which is already its own separate line: report expectancy in R alongside
Average R would be mathematically identical to it (both equal win_rate *
avg_win - loss_rate * avg_loss, over the same trade set, by definition),
so a dollar-expectancy is the only reading of "Expectancy" that adds
distinct information (accounts for actual position sizing, which varies
trade to trade, unlike a size-independent R-multiple).

Read-only: never modifies the DB. Run on the server (needs the real
cached data), not this dev sandbox.

Usage:
    python3 analyze_v5_ablation.py --start-date 2024-01-01 --end-date 2024-06-30
    python3 analyze_v5_ablation.py --start-date 2024-01-01 --end-date 2024-06-30 --account-id 1
"""
import argparse
import copy
import json
import statistics
from datetime import date
from pathlib import Path

from analyze_strategy import find_strategy, fmt_money, section
from src import backtest_data, backtest_engine, backtest_runner, db, perf

PROJECT_DIR = Path(__file__).resolve().parent

DEFAULT_PORTFOLIO_VALUE = 100_000.0
DEFAULT_MAX_RISK_PCT = 1.0
DEFAULT_MAX_TRADES_PER_DAY = 5
DEFAULT_COMMISSION_PER_TRADE = 1.5


def _trade_key(pair: dict) -> tuple:
    """(symbol, entry timestamp) - unique per entry for a long_only
    strategy (every strategy this script runs is long_only), used to
    match the SAME real-world signal across different quality_filters
    configurations."""
    return (pair["symbol"], pair["buy_time"])


def _run(strategy_name: str, direction: str, rules: dict, symbols: list[str], start_date: date, end_date: date,
         portfolio_value: float, max_risk_pct: float, max_trades_per_day: int, commission_per_trade: float) -> dict:
    """direction must be the DB strategies.direction convention ("long"/
    "short" - see src/backtest_runner.run_one_strategy's own real caller),
    NOT rules["direction"] (a separate "long_only"/"short_only" convention
    used only for UI display inside rules_json)."""
    result = backtest_runner.run_one_strategy(
        strategy_name, direction, rules, symbols,
        start_date, end_date, portfolio_value, max_risk_pct, max_trades_per_day, commission_per_trade,
    )
    return result


def _stats(pairs: list[dict]) -> dict:
    """Every "Performance"/"Trade Quality" number the report needs, off
    one already-enriched pair list (trade_diagnostics.enrich_all's own
    final_r/mfe_r/mae_r/capture_pct - already present on every pair
    run_one_strategy returns, via trade_diagnostics.full_report)."""
    agg = perf.aggregate(pairs)
    diagnosable = [p for p in pairs if p.get("final_r") is not None]
    final_rs = [p["final_r"] for p in diagnosable]
    mfe_rs = [p["mfe_r"] for p in diagnosable if p.get("mfe_r") is not None]
    mae_rs = [p["mae_r"] for p in diagnosable if p.get("mae_r") is not None]
    captures = [p["capture_pct"] for p in diagnosable if p.get("capture_pct") is not None]
    wins = [p for p in pairs if p["pnl_usd"] > 0]
    losses = [p for p in pairs if p["pnl_usd"] <= 0]
    return {
        "total_trades": agg["total_trades"],
        "win_rate_pct": agg["win_rate_pct"],
        "profit_factor": agg["profit_factor"],
        "gross_pnl_usd": agg["gross_pnl_usd"],
        "net_pnl_usd": agg["net_pnl_usd"],
        "avg_r": round(statistics.mean(final_rs), 3) if final_rs else None,
        "median_r": round(statistics.median(final_rs), 3) if final_rs else None,
        "expectancy_usd": round(agg["net_pnl_usd"] / agg["total_trades"], 2) if agg["total_trades"] else None,
        "max_drawdown_usd": perf.compute_max_drawdown(pairs),
        "avg_mfe_r": round(statistics.mean(mfe_rs), 3) if mfe_rs else None,
        "avg_mae_r": round(statistics.mean(mae_rs), 3) if mae_rs else None,
        "avg_capture_pct": round(statistics.mean(captures), 1) if captures else None,
        "win_count": len(wins), "loss_count": len(losses),
    }


def print_stats_row(label: str, s: dict, width: int = 28):
    print(f"{label:{width}s} trades={s['total_trades']:4d}  win_rate={s['win_rate_pct']:5.1f}%  "
          f"PF={s['profit_factor']}  net_pnl={fmt_money(s['net_pnl_usd'])}  "
          f"avg_R={s['avg_r']}  median_R={s['median_r']}  expectancy=${s['expectancy_usd']}  "
          f"max_dd={fmt_money(s['max_drawdown_usd'])}")


def build_ablation_variants(v5_rules: dict, v3_rules: dict) -> dict:
    """{name: rules} for the 5 requested single-filter-removed variants -
    each a deep copy of v5's OWN rules with exactly one filter neutralized
    (widened back to v3's own value for the entry-time filter, or the
    corresponding quality_filters sub-key(s) simply omitted for the
    other 4) - every other filter stays at its normal v5 threshold."""
    variants = {}

    without_time = copy.deepcopy(v5_rules)
    without_time["time_filter"]["latest_entry_et"] = v3_rules["time_filter"]["latest_entry_et"]
    variants["Without Entry Time filter"] = without_time

    without_pullback = copy.deepcopy(v5_rules)
    without_pullback["quality_filters"].pop("pullbacks_max", None)
    variants["Without Pullback filter"] = without_pullback

    without_es_distance = copy.deepcopy(v5_rules)
    without_es_distance["quality_filters"].pop("es_vwap_dist_pct_min", None)
    without_es_distance["quality_filters"].pop("es_vwap_dist_pct_max", None)
    variants["Without ES VWAP Distance filter"] = without_es_distance

    without_atr = copy.deepcopy(v5_rules)
    without_atr["quality_filters"].pop("atr_pct_min", None)
    without_atr["quality_filters"].pop("atr_pct_max", None)
    variants["Without ATR filter"] = without_atr

    without_breakout_atr = copy.deepcopy(v5_rules)
    without_breakout_atr["quality_filters"].pop("breakout_atr_ratio_min", None)
    without_breakout_atr["quality_filters"].pop("breakout_atr_ratio_max", None)
    variants["Without Breakout ATR filter"] = without_breakout_atr

    return variants


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
    v3_rules = json.loads(db.get_strategy(v3_strategy["id"])["rules_json"])
    v5_rules = json.loads(db.get_strategy(v5_strategy["id"])["rules_json"])
    direction = v5_strategy["direction"]

    symbols = backtest_data.cached_symbols(backtest_engine.BAR_SIZE)
    if not symbols:
        raise SystemExit("No symbols have cached historical bars yet - run fetch_backtest_data.py on the server first.")

    section(f"ORB Long v3 vs v5 Quality Filter Study — {start_date} to {end_date} "
            f"({len(symbols)} symbol(s) with cached bars)")

    def run_variant(name, rules):
        print(f"Running {name}...")
        result = _run(name, direction, rules, symbols, start_date, end_date,
                       args.portfolio_value, args.max_risk_pct, args.max_trades_per_day, args.commission_per_trade)
        return result["pairs"]

    v3_pairs = run_variant("ORB Long v3 (baseline)", v3_rules)
    v5_pairs = run_variant("ORB Long v5 (all filters)", v5_rules)

    ablation_variants = build_ablation_variants(v5_rules, v3_rules)
    ablation_pairs = {name: run_variant(name, rules) for name, rules in ablation_variants.items()}

    # ---------------------------------------------------------- Performance
    section("Performance: ORB Long v3 vs v5")
    v3_stats, v5_stats = _stats(v3_pairs), _stats(v5_pairs)
    print_stats_row("ORB Long v3 (baseline):", v3_stats)
    print_stats_row("ORB Long v5 (all filters):", v5_stats)

    # -------------------------------------------------------- Trade Quality
    section("Trade Quality")
    pct_filtered = round((1 - v5_stats["total_trades"] / v3_stats["total_trades"]) * 100, 1) if v3_stats["total_trades"] else None
    print(f"Trades filtered out: {v3_stats['total_trades'] - v5_stats['total_trades']} "
          f"({pct_filtered}% of v3's {v3_stats['total_trades']})")
    print(f"Remaining trade count: {v5_stats['total_trades']}")
    print(f"v3 avg MFE R / avg MAE R / avg Capture %: "
          f"{v3_stats['avg_mfe_r']} / {v3_stats['avg_mae_r']} / {v3_stats['avg_capture_pct']}%")
    print(f"v5 avg MFE R / avg MAE R / avg Capture %: "
          f"{v5_stats['avg_mfe_r']} / {v5_stats['avg_mae_r']} / {v5_stats['avg_capture_pct']}%")

    # ------------------------------------------------------ Filter Diagnostics
    section("Filter Diagnostics — trades each filter alone removed from v3's own set")
    v5_keys = {_trade_key(p) for p in v5_pairs}
    for name, pairs in ablation_pairs.items():
        variant_keys = {_trade_key(p) for p in pairs}
        removed_keys = variant_keys - v5_keys
        removed_pairs = [p for p in pairs if _trade_key(p) in removed_keys]
        if not removed_pairs:
            print(f"\n{name}: removed 0 trades (either this filter never actually bound "
                  f"in this window, or every trade it would remove was already excluded "
                  f"by another filter too)")
            continue
        s = _stats(removed_pairs)
        print(f"\n{name}: removed {len(removed_pairs)} trade(s)")
        print(f"    Net P&L of removed trades:  {fmt_money(s['net_pnl_usd'])}")
        print(f"    Win rate of removed trades: {s['win_rate_pct']}%")
        print(f"    Avg R of removed trades:    {s['avg_r']}")

    # ------------------------------------------------------------ Ablation
    section("Filter Ablation Study — aggregate impact of removing each filter")
    print_stats_row("v5 (all filters):", v5_stats, width=32)
    for name, pairs in ablation_pairs.items():
        s = _stats(pairs)
        pf_delta = "n/a" if not (isinstance(s["profit_factor"], (int, float)) and isinstance(v5_stats["profit_factor"], (int, float))) \
            else round(s["profit_factor"] - v5_stats["profit_factor"], 2)
        pnl_delta = round(s["net_pnl_usd"] - v5_stats["net_pnl_usd"], 2)
        wr_delta = round(s["win_rate_pct"] - v5_stats["win_rate_pct"], 1)
        dd_delta = round(s["max_drawdown_usd"] - v5_stats["max_drawdown_usd"], 2)
        print_stats_row(f"{name}:", s, width=32)
        print(f"{'':32s} vs v5:  PF {pf_delta:+}  net_pnl {pnl_delta:+.2f}  "
              f"win_rate {wr_delta:+.1f}pp  max_dd {dd_delta:+.2f}")

    section("Interpretation notes")
    print("A filter that removed few/no trades in this window never actually bound - it isn't")
    print("doing anything here either way, positive or negative, and can't be judged from this run.")
    print("A filter whose 'Without X' ablation row shows WORSE stats than full v5 is adding real")
    print("edge (removing it hurts). A filter whose ablation row shows BETTER or near-identical")
    print("stats than full v5 is a candidate for removal - it isn't earning its keep, or is")
    print("actively overfitting to this specific date range/symbol set.")


if __name__ == "__main__":
    main()
