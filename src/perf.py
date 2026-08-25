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
    """FIFO-pairs opening and closing trades per symbol. A symbol is only
    ever long XOR short at a time — cycle.py's entry_scan for either
    direction checks all currently-held symbols regardless of side before
    entering — so whichever action opens a symbol's position (BUY or SELL)
    determines that pair's side: BUY-then-SELL is a long, SELL-then-BUY is
    a short. pnl_usd = (sell_price - buy_price) * size holds unchanged for
    both — a short's SELL is its (higher, ideally) opening price and its
    BUY is its (lower, ideally) closing price, so the same subtraction
    still yields the profit."""
    open_trades = defaultdict(list)
    pairs = []
    for row in sorted(rows, key=lambda r: (r["timestamp_iso"], r.get("id", 0))):
        symbol = row["symbol"]
        pending = open_trades[symbol]
        if pending and pending[0]["side"] != row["side"]:
            open_row = pending.pop(0)
            side = "long" if open_row["side"] == "BUY" else "short"
            open_price = float(open_row["fill_price"] or 0)
            close_price = float(row["fill_price"] or 0)
            buy_price, sell_price = (open_price, close_price) if side == "long" else (close_price, open_price)
            size = min(int(open_row["size"]), int(row["size"]))
            pnl_usd = (sell_price - buy_price) * size
            move_pct = ((close_price - open_price) / open_price * 100) if open_price else 0.0
            pnl_pct = move_pct if side == "long" else -move_pct
            try:
                hold_minutes = (
                    datetime.fromisoformat(row["timestamp_iso"])
                    - datetime.fromisoformat(open_row["timestamp_iso"])
                ).total_seconds() / 60
            except ValueError:
                hold_minutes = None
            pairs.append({
                "symbol": symbol,
                "side": side,
                "buy_price": buy_price,
                "sell_price": sell_price,
                "open_price": open_price,
                "close_price": close_price,
                "size": size,
                "pnl_usd": pnl_usd,
                "pnl_pct": pnl_pct,
                "hold_minutes": hold_minutes,
                "buy_time": open_row["timestamp_iso"] if side == "long" else row["timestamp_iso"],
                "sell_time": row["timestamp_iso"] if side == "long" else open_row["timestamp_iso"],
                "exit_reason": row.get("exit_reason"),
                "initial_stop": open_row.get("initial_stop"),
            })
        else:
            pending.append(row)
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
    # closed positions won't be in the open table anymore, so this is
    # always the 1% fallback proxy, mirrored for a short's stop (above
    # entry) vs a long's (below entry).
    r_values = []
    for p in pairs:
        open_price = p["open_price"]
        if p["side"] == "short":
            stop = open_price * 1.01
            risk_per_share = stop - open_price
            move = open_price - p["close_price"]
        else:
            stop = open_price * 0.99
            risk_per_share = open_price - stop
            move = p["close_price"] - open_price
        if risk_per_share <= 0:
            continue
        r_values.append(move / risk_per_share)
    return r_values


def histogram(r_values: list[float]) -> list[tuple]:
    return [(label, sum(1 for r in r_values if predicate(r)), is_loss)
            for label, predicate, is_loss in R_BUCKETS]


def today_summary(account_id: int, mode: str) -> dict:
    rows = db.get_trades(account_id, mode, limit=5000, today_only=True)
    pairs = pair_trades(rows)
    return aggregate(pairs)
