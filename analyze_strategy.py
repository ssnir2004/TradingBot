"""Diagnostic report for one strategy, pooled across every 'done' backtest
that included it - same pooling/dedup rules as src/perf.strategy_report
(exact (strategy_id, start_date, end_date) dedup, newest run wins), but
prints a much deeper breakdown than the dashboard's Strategy Report card:
exit-reason mix, entry timing (day-of-week/hour), hold-time by outcome,
per-symbol P&L, the D1-D3/I1-I3 filter pass-rate funnel, and a monthly
P&L trend - the pieces needed to actually diagnose WHY a strategy is
under-performing, not just confirm that it is.

Read-only: never modifies the DB. Run on the server (needs the real
data), not this dev sandbox - see DEPLOY.md for how the dashboard itself
is invoked, same account/db.py wiring here.

Usage:
    python3 analyze_strategy.py --strategy "L1-Reversal"
    python3 analyze_strategy.py --strategy "Long Breakout Fade" --account-id 1
"""
import argparse
import json
import statistics
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from src import db, perf

PROJECT_DIR = Path(__file__).resolve().parent


def find_strategy(account_id: int, needle: str) -> dict:
    needle_lower = needle.lower()
    strategies = db.list_strategies(account_id)
    matches = [
        s for s in strategies
        if needle_lower in (s.get("key") or "").lower() or needle_lower in (s.get("name") or "").lower()
    ]
    if not matches:
        raise SystemExit(
            f"No strategy matches {needle!r}. Available: "
            + ", ".join(f"{s.get('key') or s['id']} ({s['name']})" for s in strategies)
        )
    if len(matches) > 1:
        raise SystemExit(
            f"{needle!r} matches more than one strategy - be more specific: "
            + ", ".join(f"{s.get('key') or s['id']} ({s['name']})" for s in matches)
        )
    return matches[0]


def pool_strategy_pairs(backtests: list[dict], strategy_id) -> dict:
    """Same exact-date-range dedup as perf.strategy_report (newest created_at
    wins for a repeated range), but scoped to one strategy_id and returning
    the raw pooled pairs/filter_stats/date-ranges too, not just the
    re-aggregated summary - this needs the underlying trades for the
    breakdowns strategy_report itself doesn't compute.

    `backtests` is expected to come from db.list_done_backtest_results(...,
    include_archived=True) - unlike the dashboard's own Strategy Report
    card, this offline analysis pools a strategy's FULL history regardless
    of archive status (see that function's own docstring on why), and
    reports how many of the pooled runs were archived so that isn't
    silently invisible here either."""
    strategy_id_str = str(strategy_id)
    latest_by_range = {}  # (start_date, end_date) -> {"created_at", "result", "archived_at"}
    for bt in backtests:
        result = bt["results"].get(strategy_id_str)
        if not isinstance(result, dict) or "aggregate" not in result:
            continue
        date_key = (bt["params"].get("start_date"), bt["params"].get("end_date"))
        existing = latest_by_range.get(date_key)
        if existing is None or bt["created_at"] > existing["created_at"]:
            latest_by_range[date_key] = {"created_at": bt["created_at"], "result": result, "archived_at": bt.get("archived_at")}

    pooled_pairs = []
    pooled_filter_stats = defaultdict(int)
    date_ranges = []
    archived_count = 0
    for date_key, entry in sorted(latest_by_range.items()):
        pooled_pairs.extend(entry["result"]["pairs"])
        for cond, count in (entry["result"].get("filter_stats") or {}).items():
            pooled_filter_stats[cond] += count
        date_ranges.append((*date_key, entry["archived_at"]))
        if entry["archived_at"]:
            archived_count += 1
    return {
        "pairs": pooled_pairs, "filter_stats": dict(pooled_filter_stats),
        "date_ranges": date_ranges, "archived_count": archived_count,
    }


def fmt_money(v: float) -> str:
    sign = "-" if v < 0 else ""
    return f"{sign}${abs(v):,.2f}"


def section(title: str):
    print(f"\n{'=' * 70}\n{title}\n{'=' * 70}")


def print_aggregate(agg: dict):
    print(f"Total trades:     {agg['total_trades']}  ({agg['wins']} wins / {agg['losses']} losses)")
    print(f"Win rate:         {agg['win_rate_pct']}%")
    print(f"Gross P&L:        {fmt_money(agg['gross_pnl_usd'])}")
    print(f"Commission:       {fmt_money(agg['total_commission_usd'])}")
    print(f"Net P&L:          {fmt_money(agg['net_pnl_usd'])}")
    print(f"Avg winner:       {fmt_money(agg['avg_winner'])}")
    print(f"Avg loser:        {fmt_money(agg['avg_loser'])}")
    print(f"Profit factor:    {agg['profit_factor']}")
    if agg["total_trades"]:
        expectancy = agg["net_pnl_usd"] / agg["total_trades"]
        print(f"Expectancy/trade: {fmt_money(expectancy)}")
    if agg["largest_winner"]:
        print(f"Largest winner:   {agg['largest_winner']['symbol']} {fmt_money(agg['largest_winner']['pnl_usd'])}")
    if agg["largest_loser"]:
        print(f"Largest loser:    {agg['largest_loser']['symbol']} {fmt_money(agg['largest_loser']['pnl_usd'])}")


def print_r_histogram(pairs: list[dict]):
    r_values = perf.compute_r_multiples(pairs)
    if not r_values:
        print("(no R-multiples - every pair had a zero/invalid risk_per_share)")
        return
    for label, count, is_loss in perf.histogram(r_values):
        bar = "#" * count
        tag = "loss" if is_loss else "win "
        print(f"  {label:>12} [{tag}] {count:4d}  {bar}")
    print(f"  Avg R: {statistics.mean(r_values):.2f}   Median R: {statistics.median(r_values):.2f}")


def print_exit_reason_breakdown(pairs: list[dict]):
    by_reason = defaultdict(list)
    for p in pairs:
        by_reason[p.get("exit_reason") or "(unknown)"].append(p)
    rows = []
    for reason, group in by_reason.items():
        wins = [p for p in group if p["pnl_usd"] > 0]
        total_pnl = sum(p["pnl_usd"] for p in group)
        rows.append((reason, len(group), len(wins), total_pnl))
    rows.sort(key=lambda r: r[1], reverse=True)
    print(f"{'Exit reason':<20}{'Count':>8}{'Win rate':>12}{'Total P&L':>16}{'Avg P&L':>14}")
    for reason, count, wins, total_pnl in rows:
        win_rate = wins / count * 100 if count else 0
        avg_pnl = total_pnl / count if count else 0
        print(f"{reason:<20}{count:>8}{win_rate:>11.1f}%{fmt_money(total_pnl):>16}{fmt_money(avg_pnl):>14}")


def print_side_breakdown(pairs: list[dict]):
    by_side = defaultdict(list)
    for p in pairs:
        by_side[p["side"]].append(p)
    if len(by_side) <= 1:
        print("(strategy only ever traded one side - nothing to compare)")
        return
    for side, group in by_side.items():
        agg = perf.aggregate(group)
        print(f"-- {side} ({agg['total_trades']} trades) --")
        print(f"   Win rate {agg['win_rate_pct']}%  Net P&L {fmt_money(agg['net_pnl_usd'])}  Profit factor {agg['profit_factor']}")


def print_hold_time_breakdown(pairs: list[dict]):
    with_hold = [p for p in pairs if p.get("hold_minutes") is not None]
    if not with_hold:
        print("(no hold-time data)")
        return
    wins = [p["hold_minutes"] for p in with_hold if p["pnl_usd"] > 0]
    losses = [p["hold_minutes"] for p in with_hold if p["pnl_usd"] <= 0]
    if wins:
        print(f"Winners - avg hold: {statistics.mean(wins):.0f} min   median: {statistics.median(wins):.0f} min")
    if losses:
        print(f"Losers  - avg hold: {statistics.mean(losses):.0f} min   median: {statistics.median(losses):.0f} min")


def print_timing_breakdown(pairs: list[dict]):
    by_dow = defaultdict(list)
    by_hour = defaultdict(list)
    dow_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    for p in pairs:
        try:
            entry_time = datetime.fromisoformat(p["buy_time"] if p["side"] == "long" else p["sell_time"])
        except (ValueError, TypeError):
            continue
        by_dow[entry_time.weekday()].append(p["pnl_usd"])
        by_hour[entry_time.hour].append(p["pnl_usd"])

    print("By day of week (entry):")
    for dow in sorted(by_dow):
        pnls = by_dow[dow]
        wins = sum(1 for v in pnls if v > 0)
        print(f"  {dow_names[dow]:<4} {len(pnls):4d} trades  win rate {wins/len(pnls)*100:5.1f}%  total {fmt_money(sum(pnls))}")

    print("By entry hour (ET):")
    for hour in sorted(by_hour):
        pnls = by_hour[hour]
        wins = sum(1 for v in pnls if v > 0)
        print(f"  {hour:02d}:00 {len(pnls):4d} trades  win rate {wins/len(pnls)*100:5.1f}%  total {fmt_money(sum(pnls))}")


def print_symbol_breakdown(pairs: list[dict], top_n: int = 10):
    by_symbol = defaultdict(list)
    for p in pairs:
        by_symbol[p["symbol"]].append(p["pnl_usd"])
    rows = [(sym, len(pnls), sum(pnls)) for sym, pnls in by_symbol.items()]
    rows.sort(key=lambda r: r[2])
    print(f"Worst {top_n} symbols:")
    for sym, count, total in rows[:top_n]:
        print(f"  {sym:<8} {count:3d} trades   {fmt_money(total)}")
    print(f"Best {top_n} symbols:")
    for sym, count, total in rows[-top_n:][::-1]:
        print(f"  {sym:<8} {count:3d} trades   {fmt_money(total)}")


# Condition keys vary by strategy engine: D1-I3 for the standard D1-D3/
# I1-I3 filter strategies, or_formed/confirmed/volatility_ok for ORB (see
# src/orb.py) - same reasoning as web/templates/backtest.html's own
# FILTER_LABELS, which this mirrors so the dashboard and this offline
# script never disagree on what each key means.
FILTER_LABELS = {
    "D1": "D1: above prior day extreme", "D2": "D2: trend side of SMA200",
    "D3": "D3: gap threshold", "I1": "I1: above premarket extreme",
    "I2": "I2: new intraday high/low", "I3": "I3: relative volume",
    "or_formed": "Opening range formed", "confirmed": "5m confirmation close",
    "volatility_ok": "RVOL + ATR% filters",
}
_FILTER_STATS_META_KEYS = {"evaluations", "insufficient_data"}


def print_filter_funnel(filter_stats: dict):
    if not filter_stats:
        print("(no filter_stats recorded on these runs)")
        return
    evaluations = filter_stats.get("evaluations", 0)
    insufficient = filter_stats.get("insufficient_data", 0)
    print(f"Candidate evaluations: {evaluations}  (skipped for insufficient data: {insufficient})")
    if not evaluations:
        return
    print("Pass rate per condition (share of evaluations where this condition alone passed):")
    for cond, passed in filter_stats.items():
        if cond in _FILTER_STATS_META_KEYS:
            continue
        print(f"  {FILTER_LABELS.get(cond, cond)}: {passed:6d} / {evaluations}  ({passed/evaluations*100:5.1f}%)")


def print_monthly_trend(pairs: list[dict]):
    by_month = defaultdict(list)
    for p in pairs:
        ts = p["buy_time"] if p["side"] == "long" else p["sell_time"]
        try:
            month = datetime.fromisoformat(ts).strftime("%Y-%m")
        except (ValueError, TypeError):
            continue
        by_month[month].append(p["pnl_usd"])
    for month in sorted(by_month):
        pnls = by_month[month]
        wins = sum(1 for v in pnls if v > 0)
        print(f"  {month}  {len(pnls):4d} trades  win rate {wins/len(pnls)*100:5.1f}%  total {fmt_money(sum(pnls))}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--strategy", required=True, help="Substring match against strategy key or name")
    parser.add_argument("--account-id", type=int, default=None)
    args = parser.parse_args()

    db.init_db(seed_rules_path=PROJECT_DIR / "rules.json")
    account_id = args.account_id if args.account_id is not None else db.get_default_account_id()

    strategy = find_strategy(account_id, args.strategy)
    strategy_id = strategy["id"]
    full_strategy = db.get_strategy(strategy_id)

    backtests = db.list_done_backtest_results(account_id, include_archived=True)
    pooled = pool_strategy_pairs(backtests, strategy_id)
    pairs = pooled["pairs"]

    section(f"Strategy: {strategy.get('key') or strategy_id} — {strategy['name']}  (direction: {strategy.get('direction')})")
    archived_note = f" ({pooled['archived_count']} archived, included anyway)" if pooled["archived_count"] else ""
    print(f"Date ranges pooled ({len(pooled['date_ranges'])} backtest run(s)){archived_note}:")
    for start, end, archived_at in pooled["date_ranges"]:
        print(f"  {start} -> {end}" + ("  [archived]" if archived_at else ""))

    section("rules_json (as currently configured)")
    print(json.dumps(json.loads(full_strategy["rules_json"]), indent=2, ensure_ascii=False))

    if not pairs:
        print("\nNo closed trades found for this strategy across any 'done' backtest.")
        return

    if len(pairs) < perf.STRATEGY_REPORT_LOW_SAMPLE_TRADES:
        print(f"\n** SMALL SAMPLE WARNING: only {len(pairs)} trades - conclusions below are not statistically reliable. **")

    section("Aggregate")
    print_aggregate(perf.aggregate(pairs))

    section("R-multiple distribution")
    print_r_histogram(pairs)

    section("Exit reason breakdown")
    print_exit_reason_breakdown(pairs)

    section("Long vs short")
    print_side_breakdown(pairs)

    section("Hold time")
    print_hold_time_breakdown(pairs)

    section("Entry timing")
    print_timing_breakdown(pairs)

    section("Per-symbol P&L")
    print_symbol_breakdown(pairs)

    section("Entry filter funnel")
    print_filter_funnel(pooled["filter_stats"])

    section("Monthly trend")
    print_monthly_trend(pairs)


if __name__ == "__main__":
    main()
