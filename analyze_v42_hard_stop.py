"""V4-vs-v4.1-vs-v4.2 comparison for ORB Long v4.2's "hard stop 2.5R +
early trailing" exit rework (see src/db.py's own v4.2 comment and
backtest_engine.py's "hard_stop_price" addition to the existing
"no_stop_delayed_trail" management_style v4.1 introduced).

v4.2 is v4.1 PLUS a real, fillable -2.5R stop that's live only until
trailing activates at +1.20R MFE - once trailing activates, the hard
stop is fully ignored (Trailing Priority). This script's whole point is
answering: does that hard stop actually help (catch v4.1's own worst
losers before they get even worse) more than it hurts (cut off a v4.1
trade that would have recovered into a win)?

Runs REAL backtests (src/backtest_runner.run_one_strategy) for v4, v4.1,
and v4.2 over the SAME symbols/date range, then reports:
  - Performance Metrics (v4 vs v4.1 vs v4.2)
  - Exit Breakdown per strategy (count/avg Final R/net P&L per exit
    reason each strategy can actually produce)
  - Critical Comparison - every v4.1 trade whose OWN final_r was worse
    than -2.5R, matched to v4.2's trade on the same signal (by symbol +
    buy_time, same technique as analyze_v41_no_stop.py's own)
  - Validation Questions 1-4, computed off ALL v4.2 trades that actually
    exited via the hard stop (not just the ones that were already worse
    than -2.5R under v4.1) - matching each one back to v4.1's own result
    for that signal tells us both the losses the hard stop avoided AND
    the winners it may have cut short, in the same pass.

Read-only: never modifies the DB. Run on the server (needs the real
cached data), not this dev sandbox.

Usage:
    python3 analyze_v42_hard_stop.py --start-date 2024-01-01 --end-date 2024-06-30
    python3 analyze_v42_hard_stop.py --start-date 2024-01-01 --end-date 2024-06-30 --account-id 1
"""
import argparse
import json
import statistics
from collections import Counter
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

HARD_STOP_R = -2.5


def exit_breakdown(pairs: list[dict]) -> dict:
    """{exit_reason: {"count", "avg_final_r", "net_pnl_usd"}} off
    whatever exit reasons this pair set actually contains - v4 produces
    up to 4 (initial_stop_loss/partial_profit_take/staged_trailing_stop/
    eod_close), v4.1 up to 2 (trailing_stop/eod_close), v4.2 up to 3
    (hard_stop/trailing_stop/eod_close)."""
    by_reason: dict[str, list[dict]] = {}
    for p in pairs:
        by_reason.setdefault(p.get("exit_reason"), []).append(p)
    out = {}
    for reason, group in by_reason.items():
        final_rs = [p["final_r"] for p in group if p.get("final_r") is not None]
        out[reason] = {
            "count": len(group),
            "avg_final_r": round(statistics.mean(final_rs), 3) if final_rs else None,
            "net_pnl_usd": round(sum(p["pnl_usd"] for p in group), 2),
        }
    return out


def print_exit_breakdown(label: str, breakdown: dict):
    print(f"\n{label}:")
    for reason, s in sorted(breakdown.items(), key=lambda kv: -kv[1]["count"]):
        print(f"    {reason:<20} trades={s['count']:4d}  avg_final_R={s['avg_final_r']}  net_pnl={fmt_money(s['net_pnl_usd'])}")


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

    v4_strategy = find_strategy(account_id, "ORB Long v4 (Scaled Exit")
    v41_strategy = find_strategy(account_id, "ORB Long v4.1 (No Initial Stop")
    v42_strategy = find_strategy(account_id, "ORB Long v4.2 (Hard Stop")
    v4_rules = json.loads(db.get_strategy(v4_strategy["id"])["rules_json"])
    v41_rules = json.loads(db.get_strategy(v41_strategy["id"])["rules_json"])
    v42_rules = json.loads(db.get_strategy(v42_strategy["id"])["rules_json"])
    direction = v4_strategy["direction"]

    symbols = backtest_data.cached_symbols(backtest_engine.BAR_SIZE)
    if not symbols:
        raise SystemExit("No symbols have cached historical bars yet - run fetch_backtest_data.py on the server first.")

    section(f"ORB Long v4 vs v4.1 vs v4.2 (Hard Stop 2.5R + Early Trailing) — {start_date} to {end_date} "
            f"({len(symbols)} symbol(s) with cached bars)")

    def run_variant(name, rules):
        print(f"Running {name}...")
        result = _run(name, direction, rules, symbols, start_date, end_date,
                       args.portfolio_value, args.max_risk_pct, args.max_trades_per_day, args.commission_per_trade)
        return result["pairs"]

    v4_pairs = run_variant("ORB Long v4 (baseline)", v4_rules)
    v41_pairs = run_variant("ORB Long v4.1 (no stop)", v41_rules)
    v42_pairs = run_variant("ORB Long v4.2 (hard stop 2.5R)", v42_rules)

    # ------------------------------------------------------- Performance Metrics
    section("Performance Metrics: v4 vs v4.1 vs v4.2")
    v4_stats, v41_stats, v42_stats = _stats(v4_pairs), _stats(v41_pairs), _stats(v42_pairs)
    print_stats_row("ORB Long v4 (baseline):", v4_stats, width=28)
    print_stats_row("ORB Long v4.1 (no stop):", v41_stats, width=28)
    print_stats_row("ORB Long v4.2 (hard stop):", v42_stats, width=28)

    # ----------------------------------------------------------- Exit Breakdown
    section("Exit Breakdown")
    print_exit_breakdown("v4", exit_breakdown(v4_pairs))
    print_exit_breakdown("v4.1", exit_breakdown(v41_pairs))
    print_exit_breakdown("v4.2", exit_breakdown(v42_pairs))

    # ------------------------------------------------------- Critical Comparison
    section("Critical Comparison — every v4.1 trade that lost worse than -2.5R")
    v42_by_key = {_trade_key(p): p for p in v42_pairs}
    v41_big_losers = [p for p in v41_pairs if p.get("final_r") is not None and p["final_r"] < HARD_STOP_R]
    print(f"({len(v41_big_losers)} v4.1 trade(s) with final_R < {HARD_STOP_R})\n")
    prevented_count = 0
    total_r_recovered = 0.0
    for p in v41_big_losers:
        match = v42_by_key.get(_trade_key(p))
        if match is None:
            print(f"{p['symbol']:<8} {p['buy_time']}  v4.1_R={p['final_r']:.2f}  | v4.2: NO MATCHING TRADE "
                  f"(position-sizing/re-entry path dependency - see analyze_v41_no_stop.py's own docstring)")
            continue
        improvement = match["final_r"] - p["final_r"]
        if improvement > 0:
            prevented_count += 1
        total_r_recovered += improvement
        print(f"{p['symbol']:<8} {p['buy_time']}  v4.1_R={p['final_r']:.2f}  v4.2_R={match['final_r']:.2f}  "
              f"improvement={improvement:+.2f}R")

    # ---------------------------------------------------- Validation Questions
    section("Validation Questions")
    print(f"1. v4.1 losers worse than -2.5R that v4.2 improved on: {prevented_count} of {len(v41_big_losers)}")
    print(f"2. Total R recovered from those trades: {total_r_recovered:+.2f}R")

    # Net impact of the hard stop mechanism itself: every v4.2 trade that
    # ACTUALLY exited via hard_stop, matched back to what v4.1 did with
    # the SAME signal - a positive delta means the hard stop avoided a
    # worse v4.1 outcome, negative means it cut off a trade v4.1 itself
    # did better with (a premature stop).
    v41_by_key = {_trade_key(p): p for p in v41_pairs}
    hard_stopped = [p for p in v42_pairs if p.get("exit_reason") == "hard_stop"]
    helped_r, hurt_r = 0.0, 0.0
    helped_count, hurt_count = 0, 0
    for p in hard_stopped:
        match = v41_by_key.get(_trade_key(p))
        if match is None or match.get("final_r") is None or p.get("final_r") is None:
            continue
        delta = p["final_r"] - match["final_r"]
        if delta >= 0:
            helped_r += delta
            helped_count += 1
        else:
            hurt_r += delta
            hurt_count += 1
    net_r_impact = helped_r + hurt_r
    print(f"3. Of {len(hard_stopped)} v4.2 hard-stop exits: {helped_count} avoided a worse v4.1 outcome "
          f"(+{helped_r:.2f}R total), {hurt_count} cut off a v4.1 trade that did better ({hurt_r:.2f}R total).")
    print(f"   Net R impact of the hard stop mechanism: {net_r_impact:+.2f}R "
          f"({'recovered losses OUTWEIGH premature stops' if net_r_impact > 0 else 'premature stops OUTWEIGH recovered losses' if net_r_impact < 0 else 'exactly offsetting'})")

    pf_delta = "n/a" if not (isinstance(v42_stats["profit_factor"], (int, float)) and isinstance(v41_stats["profit_factor"], (int, float))) \
        else round(v42_stats["profit_factor"] - v41_stats["profit_factor"], 2)
    pnl_delta = round(v42_stats["net_pnl_usd"] - v41_stats["net_pnl_usd"], 2)
    expectancy_delta = (
        round(v42_stats["expectancy_usd"] - v41_stats["expectancy_usd"], 2)
        if v42_stats["expectancy_usd"] is not None and v41_stats["expectancy_usd"] is not None else "n/a"
    )
    dd_delta = round(v42_stats["max_drawdown_usd"] - v41_stats["max_drawdown_usd"], 2)
    print(f"4. Net change v4.2 vs v4.1:  PF {pf_delta:+}  net_pnl {pnl_delta:+.2f}  "
          f"expectancy {expectancy_delta}  max_dd {dd_delta:+.2f}")

    section("Interpretation notes")
    print("Question 3's net R impact is the core verdict: positive means the -2.5R hard stop is net")
    print("protective (the losses it prevents outweigh the winners it occasionally cuts short) over this")
    print("window; negative means it's net costly (v4.1's own patience pays off more often than the hard")
    print("stop's protection). Question 4's dollar-based deltas should tell the same story, weighted by")
    print("actual position size rather than R-multiples alone - a mismatch between the two (e.g. R says")
    print("net positive but net_pnl is worse) usually means the hard stop's saves are on smaller/lower-ATR")
    print("positions than the winners it cuts off, or vice versa - worth checking position sizes directly")
    print("if that happens.")


if __name__ == "__main__":
    main()
