"""Minimal Telegram bot alerts via the raw Bot API (no extra SDK dependency)."""
from __future__ import annotations

import requests
from loguru import logger

import config

API_BASE = "https://api.telegram.org"


def _enabled() -> bool:
    return bool(config.TELEGRAM_BOT_TOKEN and config.TELEGRAM_CHAT_ID)


def send(message: str) -> None:
    if not _enabled():
        logger.debug("Telegram not configured, skipping alert: {}", message)
        return
    url = f"{API_BASE}/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        resp = requests.post(
            url,
            json={"chat_id": config.TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"},
            timeout=10,
        )
        if resp.status_code != 200:
            logger.warning("Telegram alert failed ({}): {}", resp.status_code, resp.text)
    except requests.RequestException as exc:
        logger.warning("Telegram alert error: {}", exc)


def bot_started(mode: str):
    send(f"🤖 TradingBot started in *{mode}* mode.")


def scan_results(symbols: list[str]):
    if symbols:
        send("🔍 Premarket scan candidates: " + ", ".join(symbols))
    else:
        send("🔍 Premarket scan: no candidates passed filters today.")


def trade_entry(symbol: str, quantity: int, entry_price: float, stop_price: float, take_profit_price: float):
    send(
        f"🟢 ENTRY {symbol} x{quantity} @ {entry_price:.2f}\n"
        f"Stop: {stop_price:.2f} | Target: {take_profit_price:.2f}"
    )


def partial_profit(symbol: str, quantity: int, price: float):
    send(f"💰 PARTIAL PROFIT {symbol} x{quantity} @ {price:.2f}")


def trade_exit(symbol: str, quantity: int, price: float, reason: str):
    send(f"🔴 EXIT {symbol} x{quantity} @ {price:.2f} ({reason})")


def force_close_all():
    send("⏰ Force-closing all open positions before market close.")


def error(message: str):
    send(f"⚠️ Error: {message}")


def daily_summary(realized_pnl: float, trades: int):
    send(f"📊 Daily summary: {trades} trades, realized P&L: {realized_pnl:+.2f}")
