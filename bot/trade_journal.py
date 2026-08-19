"""Append-only CSV trade journal for post-session review."""
from __future__ import annotations

import csv
import os
from datetime import datetime
from pathlib import Path

JOURNAL_PATH = Path(__file__).resolve().parent.parent / "data" / "trade_journal.csv"

FIELDS = [
    "timestamp", "symbol", "action", "quantity", "price", "stop_price",
    "take_profit_price", "reason", "realized_pnl",
]


def _ensure_file():
    JOURNAL_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not JOURNAL_PATH.exists():
        with open(JOURNAL_PATH, "w", newline="") as f:
            csv.DictWriter(f, fieldnames=FIELDS).writeheader()


def log_trade(symbol: str, action: str, quantity: int, price: float,
              stop_price: float | None = None, take_profit_price: float | None = None,
              reason: str = "", realized_pnl: float | None = None):
    _ensure_file()
    with open(JOURNAL_PATH, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writerow({
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "symbol": symbol,
            "action": action,
            "quantity": quantity,
            "price": price,
            "stop_price": stop_price,
            "take_profit_price": take_profit_price,
            "reason": reason,
            "realized_pnl": realized_pnl,
        })
