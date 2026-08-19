"""Trade pairing and performance aggregation, shared by daily_summary.py
(the scheduled Telegram summary) and the dashboard API (live views). Reads
from the trades/positions tables in src/db.py.
"""
from collections import defaultdict
from datetime import datetime

from src import db

R_BUCKETS = [
    ("<= -2R", lambda r: r <= -2, True),
    ("-2R to -1R", lambda r: -2 < r <= -1, True),
    ("-1R to 0R", lambda r: -1 < r <= 0, True),
    ("0R to +1R", lambda r: 0 < r <= 1, False),
    ("+1R to +2R", lambda r: 1 < r <= 2, False),
    ("+2R to +3R", lambda r: 2 < r <= 3, False),
    ("> +3R", lambda r: r > 3, False),
]


def pair_trades(rows: list[dict]) -> list[dict]:
    """FIFO-pair BUY rows with SELL rows per symbol."""
    open_buys = defaultdict(list)
    pairs = []
    for row in sorted(rows, key=lambda r: (r["timestamp_iso"], r.get("id", 0))):
        symbol = row["symbol"]
        if row["side"] == "BUY":
            open_buys[symbol].append(row)
        elif row["side"] == "SELL" and open_buys[symbol]:
            buy_row = open_buys[symbol].pop(0)
            buy_price = float(buy_row["fill_price"] or 0)
            sell_price = float(row["fill_price"] or 0)
            size = min(int(buy_row["size"]), int(row["size"]))
            pnl_usd = (sell_price - buy_price) * size
            pnl_pct = ((sell_price - buy_price) / buy_price * 100) if buy_price else 0.0
            try:
                hold_minutes = (
                    datetime.fromisoformat(row["timestamp_iso"])
                    - datetime.fromisoformat(buy_row["timestamp_iso"])
                ).total_seconds() / 60
            except ValueError:
                hold_minutes = None
            pairs.append({
                "symbol": symbol,
                "buy_price": buy_price,
                "sell_price": sell_price,
                "size": size,
                "pnl_usd": pnl_usd,
                "pnl_pct": pnl_pct,
                "hold_minutes": hold_minutes,
                "buy_time": buy_row["timestamp_iso"],
                "sell_time": row["timestamp_iso"],
            })
    return pairs


def aggregate(pairs: list[dict]) -> dict:
    if not pairs:
        return {
            "total_trades": 0, "wins": 0, "losses": 0, "win_rate_pct": 0.0,
            "gross_pnl_usd": 0.0, "largest_winner": None, "largest_loser": None,
            "avg_winner": 0.0, "avg_loser": 0.0, "profit_factor": "n/a",
        }

    wins = [p for p in pairs if p["pnl_usd"] > 0]
    losses = [p for p in pairs if p["pnl_usd"] <= 0]
    gross_pnl = sum(p["pnl_usd"] for p in pairs)
    sum_wins = sum(p["pnl_usd"] for p in wins)
    sum_losses = sum(p["pnl_usd"] for p in losses)

    largest_winner = max(pairs, key=lambda p: p["pnl_usd"])
    largest_loser = min(pairs, key=lambda p: p["pnl_usd"])

    profit_factor = "inf" if (not losses or sum_losses == 0) else round(sum_wins / abs(sum_losses), 2)

    return {
        "total_trades": len(pairs),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate_pct": round(len(wins) / len(pairs) * 100, 1),
        "gross_pnl_usd": round(gross_pnl, 2),
        "largest_winner": {"symbol": largest_winner["symbol"], "pnl_usd": round(largest_winner["pnl_usd"], 2)},
        "largest_loser": {"symbol": largest_loser["symbol"], "pnl_usd": round(largest_loser["pnl_usd"], 2)},
        "avg_winner": round(sum_wins / len(wins), 2) if wins else 0.0,
        "avg_loser": round(sum_losses / len(losses), 2) if losses else 0.0,
        "profit_factor": profit_factor,
    }


def compute_r_multiples(pairs: list[dict]) -> list[float]:
    open_positions_by_symbol = {}  # closed positions won't be in the open table anymore
    r_values = []
    for p in pairs:
        stop = open_positions_by_symbol.get(p["symbol"])
        if stop is None:
            stop = p["buy_price"] * 0.99  # fallback proxy: 1% initial risk
        risk_per_share = p["buy_price"] - stop
        if risk_per_share <= 0:
            continue
        r_values.append((p["sell_price"] - p["buy_price"]) / risk_per_share)
    return r_values


def histogram(r_values: list[float]) -> list[tuple]:
    return [(label, sum(1 for r in r_values if predicate(r)), is_loss)
            for label, predicate, is_loss in R_BUCKETS]


def today_summary() -> dict:
    rows = db.get_trades(limit=5000, today_only=True)
    pairs = pair_trades(rows)
    return aggregate(pairs)
