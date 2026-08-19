"""Fire-and-forget push notifications: Telegram primary, ntfy.sh-style
NOTIFY_URL fallback. Never raises — a notification failure must not break
trading logic.
"""
import logging
from pathlib import Path

import requests
from dotenv import dotenv_values

PROJECT_DIR = Path(__file__).resolve().parent.parent
LOG_DIR = PROJECT_DIR / "logs"

env = dotenv_values(PROJECT_DIR / ".env")
TELEGRAM_BOT_TOKEN = env.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = env.get("TELEGRAM_CHAT_ID", "")
NOTIFY_URL = env.get("NOTIFY_URL", "")

_logger = logging.getLogger("notify")
_logger.setLevel(logging.ERROR)


def _log_error(exc: Exception, channel: str):
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    with open(LOG_DIR / "notify_errors.log", "a") as f:
        f.write(f"[{channel}] {exc}\n")


def notify(title: str, body: str, priority: str = "default") -> None:
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        try:
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": f"*{title}*\n{body}",
                    "parse_mode": "Markdown",
                },
                timeout=5,
            )
        except Exception as exc:  # noqa: BLE001 - fire-and-forget, never raise
            _log_error(exc, "telegram")

    if NOTIFY_URL:
        try:
            requests.post(
                NOTIFY_URL,
                data=body.encode("utf-8"),
                headers={"Title": title, "Priority": priority},
                timeout=5,
            )
        except Exception as exc:  # noqa: BLE001 - fire-and-forget, never raise
            _log_error(exc, "ntfy")
