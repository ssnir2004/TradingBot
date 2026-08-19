"""Scheduled job (16:05 ET): sends the daily Telegram P&L summary. The live
dashboard gets the same numbers from src/perf.py directly via the API, so
this script's only job is the Telegram push.
"""
from datetime import datetime
from zoneinfo import ZoneInfo

from src import perf
from src.notify import notify

ET = ZoneInfo("America/New_York")


def run():
    aggregates = perf.today_summary()

    if aggregates["total_trades"] == 0:
        body = "No closed trades today."
    else:
        body = (
            f"Trades: {aggregates['total_trades']} "
            f"({aggregates['wins']}W / {aggregates['losses']}L, {aggregates['win_rate_pct']}%)\n"
            f"P&L: ${aggregates['gross_pnl_usd']:+.2f}\n"
            f"Best: {aggregates['largest_winner']['symbol']} ${aggregates['largest_winner']['pnl_usd']:+.2f}\n"
            f"Worst: {aggregates['largest_loser']['symbol']} ${aggregates['largest_loser']['pnl_usd']:+.2f}\n"
            f"PF: {aggregates['profit_factor']}"
        )
    notify(f"Daily Summary {datetime.now(ET).strftime('%Y-%m-%d')}", body, "default")
    return aggregates


if __name__ == "__main__":
    print(run())
