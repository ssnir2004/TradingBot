"""V4-vs-v4.1 comparison for ORB Long v4.1's "no initial stop, delayed
trailing" exit rework (see src/db.py's own v4.1 comment and
backtest_engine.py's new "no_stop_delayed_trail" management_style).

Runs REAL backtests (src/backtest_runner.run_one_strategy - the same
simulation every dashboard backtest goes through) for v4 and v4.1 over
the SAME symbols/date range, then reports:
  - Performance (v4 vs v4.1)
  - Trade Behavior (% reaching/never reaching 1.20R MFE, avg MFE/MAE/
    capture)
  - Exit Analysis (exit-reason counts per strategy)
  - Critical Validation - for every v4 trade that was actually STOPPED
    OUT (exit_reason in {"initial_stop_loss", "staged_trailing_stop"} -
    NOT its partial-take or EOD exits), the matching v4.1 trade on the
    SAME signal (by (symbol, buy_time), same technique as analyze_v5_
    ablation.py's own _trade_key) - v4's own result next to v4.1's, plus
    both trades' own MFE/MAE, to see directly whether v4's stop was
    protecting the trade or cutting off a move that later recovered.

"% reaching 1.20R" is computed the SAME way for both strategies - off
each pair's own recorded mfe_r >= 1.20, not v4's own trail_activated
flag (which reflects ITS 1.15R partial-take trigger, a different
threshold) or v4.1's own trail_activated flag (which by construction is
just this same 1.20R check restated) - using mfe_r directly keeps the
comparison apples-to-apples and independent of either strategy's own
internal bookkeeping.

Read-only: never modifies the DB. Run on the server (needs the real
cached data), not this dev sandbox.

Usage:
    python3 analyze_v41_no_stop.py --start-date 2024-01-01 --end-date 2024-06-30
    python3 analyze_v41_no_stop.py --start-date 2024-01-01 --end-date 2024-06-30 --account-id 1
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

STOPPED_OUT_REASONS = {"initial_stop_loss", "staged_trailing_stop"}
MFE_TRIGGER_R = 1.20


def trade_behavior(pairs: list[dict]) -> dict:
    reached = [p for p in pairs if p.get("mfe_r") is not None and p["mfe_r"] >= MFE_TRIGGER_R]
    never_reached = [p for p in pairs if p.get("mfe_r") is not None and p["mfe_r"] < MFE_TRIGGER_R]
    mfe_rs = [p["mfe_r"] for p in pairs if p.get("mfe_r") is not None]
    mae_rs = [p["mae_r"] for p in pairs if p.get("mae_r") is not None]
    captures = [p["capture_pct"] for p in pairs if p.get("capture_pct") is not None]
    total = len(pairs)
    return {
        "pct_reached": round(len(reached) / total * 100, 1) if total else None,
        "pct_never_reached": round(len(never_reached) / total * 100, 1) if total else None,
        "avg_mfe_r": round(statistics.mean(mfe_rs), 3) if mfe_rs else None,
        "avg_mae_r": round(statistics.mean(mae_rs), 3) if mae_rs else None,
        "avg_capture_pct": round(statistics.mean(captures), 1) if captures else None,
        "reached_count": len(reached), "never_reached_count": len(never_reached), "total": total,
    }


def exit_reason_counts(pairs: list[dict]) -> Counter:
    return Counter(p.get("exit_reason") for p in pairs)


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
    v4_rules = json.loads(db.get_strategy(v4_strategy["id"])["rules_json"])
    v41_rules = json.loads(db.get_strategy(v41_strategy["id"])["rules_json"])
    direction = v4_strategy["direction"]

    symbols = backtest_data.cached_symbols(backtest_engine.BAR_SIZE)
    if not symbols:
        raise SystemExit("No symbols have cached historical bars yet - run fetch_backtest_data.py on the server first.")

    section(f"ORB Long v4 vs v4.1 (No Initial Stop, Delayed Trailing) — {start_date} to {end_date} "
            f"({len(symbols)} symbol(s) with cached bars)")

    def run_variant(name, rules):
        print(f"Running {name}...")
        result = _run(name, direction, rules, symbols, start_date, end_date,
                       args.portfolio_value, args.max_risk_pct, args.max_trades_per_day, args.commission_per_trade)
        return result["pairs"]

    v4_pairs = run_variant("ORB Long v4 (baseline)", v4_rules)
    v41_pairs = run_variant("ORB Long v4.1 (no initial stop)", v41_rules)

    # ---------------------------------------------------------- Performance
    section("Performance: v4 vs v4.1")
    v4_stats, v41_stats = _stats(v4_pairs), _stats(v41_pairs)
    print_stats_row("ORB Long v4 (baseline):", v4_stats, width=28)
    print_stats_row("ORB Long v4.1 (no stop):", v41_stats, width=28)

    # ------------------------------------------------------- Trade Behavior
    section("Trade Behavior")
    v4_behavior, v41_behavior = trade_behavior(v4_pairs), trade_behavior(v41_pairs)
    for label, b in (("v4", v4_behavior), ("v4.1", v41_behavior)):
        print(f"{label}: reached 1.20R = {b['reached_count']}/{b['total']} ({b['pct_reached']}%)  "
              f"never reached = {b['never_reached_count']}/{b['total']} ({b['pct_never_reached']}%)")
        print(f"      avg MFE R={b['avg_mfe_r']}  avg MAE R={b['avg_mae_r']}  avg Capture %={b['avg_capture_pct']}%")

    # -------------------------------------------------------- Exit Analysis
    section("Exit Analysis — exit reason counts")
    for label, pairs in (("v4", v4_pairs), ("v4.1", v41_pairs)):
        counts = exit_reason_counts(pairs)
        print(f"{label}: " + ", ".join(f"{reason}={count}" for reason, count in sorted(counts.items(), key=lambda kv: -kv[1])))
    print(f"\n(Reached 1.20R MFE / never reached - see Trade Behavior above; v4.1 by construction can")
    print(f"only exit via 'trailing_stop' or 'eod_close' - no 'initial_stop_loss'/'partial_profit_take'.)")

    # --------------------------------------------------- Critical Validation
    section("Critical Validation — v4 trades that were STOPPED OUT vs what v4.1 did with the same signal")
    v41_by_key = {_trade_key(p): p for p in v41_pairs}
    stopped_out = [p for p in v4_pairs if p.get("exit_reason") in STOPPED_OUT_REASONS]
    print(f"({len(stopped_out)} v4 trade(s) stopped out - exit_reason in {sorted(STOPPED_OUT_REASONS)})\n")
    matched, unmatched = 0, 0
    for p in stopped_out:
        key = _trade_key(p)
        v41_match = v41_by_key.get(key)
        if v41_match is None:
            unmatched += 1
            print(f"{p['symbol']:<8} {p['buy_time']}  v4: {p['exit_reason']:<20} pnl={fmt_money(p['pnl_usd'])}  "
                  f"final_R={p.get('final_r')}  MAE_R={p.get('mae_r')}  MFE_R={p.get('mfe_r')}  "
                  f"| v4.1: NO MATCHING TRADE (see module docstring - position-sizing path dependency, "
                  f"or a v4.1-only universe/filter difference)")
            continue
        matched += 1
        print(f"{p['symbol']:<8} {p['buy_time']}")
        print(f"    v4:   {p['exit_reason']:<20} pnl={fmt_money(p['pnl_usd'])}  final_R={p.get('final_r')}  "
              f"MAE_R={p.get('mae_r')}  MFE_R={p.get('mfe_r')}")
        print(f"    v4.1: {v41_match.get('exit_reason'):<20} pnl={fmt_money(v41_match['pnl_usd'])}  "
              f"final_R={v41_match.get('final_r')}  MAE_R={v41_match.get('mae_r')}  MFE_R={v41_match.get('mfe_r')}")
    print(f"\n({matched} matched, {unmatched} unmatched)")

    section("Interpretation notes")
    print("Critical Validation is the core question this strategy exists to answer: for each v4 trade")
    print("that got stopped out, did v4.1's SAME signal (no stop) go on to a bigger MFE_R than v4 ever")
    print("captured (the stop was hurting - cutting off a move that recovered), or did it just bleed")
    print("further into a worse MAE_R with no real recovery (the stop was helping - protecting capital")
    print("from a move that kept going against it)? A consistent pattern across most matched rows answers")
    print("the research question directly; a mixed one means the stop's value is setup-dependent, not")
    print("uniform - not a case for removing it outright.")


if __name__ == "__main__":
    main()
