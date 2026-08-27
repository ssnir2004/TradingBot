"""Bird's-eye report across EVERY strategy that has at least one 'done'
backtest run, pooled the same way the dashboard's own Strategy Report
card is (src/perf.strategy_report - same dedup rules: newest run wins
for a repeated exact date range). One row per strategy, busiest first,
so it's obvious at a glance which strategies are even worth reaching for
analyze_strategy.py's much deeper single-strategy breakdown.

Read-only: never modifies the DB. Run on the server (needs the real
data), not this dev sandbox - see DEPLOY.md for how the dashboard itself
is invoked, same account/db.py wiring here.

Usage:
    python3 analyze_all_strategies.py
    python3 analyze_all_strategies.py --account-id 1
    python3 analyze_all_strategies.py --json   # full raw strategy_report() output, for feeding elsewhere
"""
import argparse
import json
from pathlib import Path

from analyze_strategy import fmt_money, print_filter_funnel, section
from src import db, perf

PROJECT_DIR = Path(__file__).resolve().parent


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--account-id", type=int, default=None)
    parser.add_argument("--json", action="store_true", help="Dump the full raw strategy_report() JSON instead of the formatted report")
    args = parser.parse_args()

    db.init_db(seed_rules_path=PROJECT_DIR / "rules.json")
    account_id = args.account_id if args.account_id is not None else db.get_default_account_id()

    # include_archived=True: an offline deep-dive shouldn't silently drop a
    # strategy's history just because a run got archived from the
    # day-to-day dashboard view - same reasoning as analyze_strategy.py's
    # own pool_strategy_pairs.
    backtests = db.list_done_backtest_results(account_id, include_archived=True)
    report = perf.strategy_report(backtests)

    if args.json:
        print(json.dumps(report, indent=2, ensure_ascii=False))
        return

    if not report:
        print("No 'done' backtests found for any strategy yet.")
        return

    section(f"All strategies with a done backtest ({len(report)})")
    header = f"{'Strategy':<40}{'Dir':<7}{'Runs':>5}{'Trades':>8}{'WinRate':>9}{'NetP&L':>13}{'PF':>8}{'Sample':>8}"
    print(header)
    print("-" * len(header))
    for entry in report:
        agg = entry["aggregate"]
        sample_flag = "LOW" if entry["low_sample"] else "ok"
        print(
            f"{entry['strategy_name']:<40}{(entry['direction'] or ''):<7}{entry['backtests_included']:>5}"
            f"{agg['total_trades']:>8}{agg['win_rate_pct']:>8.1f}%{fmt_money(agg['net_pnl_usd']):>13}"
            f"{str(agg['profit_factor']):>8}{sample_flag:>8}"
        )

    for entry in report:
        agg = entry["aggregate"]
        section(f"{entry['strategy_name']}  (direction: {entry['direction']}, {entry['backtests_included']} run(s) pooled)")
        if agg["total_trades"] < perf.STRATEGY_REPORT_LOW_SAMPLE_TRADES:
            print(f"** SMALL SAMPLE WARNING: only {agg['total_trades']} trades - not statistically reliable. **")
        print(f"Trades: {agg['total_trades']}  ({agg['wins']}W / {agg['losses']}L, {agg['win_rate_pct']}% win rate)")
        print(f"Gross P&L: {fmt_money(agg['gross_pnl_usd'])}   Commission: {fmt_money(agg['total_commission_usd'])}   Net P&L: {fmt_money(agg['net_pnl_usd'])}")
        print(f"Avg winner: {fmt_money(agg['avg_winner'])}   Avg loser: {fmt_money(agg['avg_loser'])}   Profit factor: {agg['profit_factor']}")
        print_filter_funnel(entry["filter_stats"])


if __name__ == "__main__":
    main()
