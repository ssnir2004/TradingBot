"""Single SQLite database backing the trading service and the dashboard.

Replaces the earlier flat-file state (trades.csv, open_positions.json,
watchlist.txt, safety-check-log.json) so the always-on service and the
dashboard's web requests can safely read/write the same state concurrently.
WAL mode lets readers (dashboard) and the single writer (service) run at
the same time without locking each other out.
"""
import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import bcrypt

PROJECT_DIR = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_DIR / "data" / "trading_bot.db"
ET = ZoneInfo("America/New_York")

SCHEMA = """
CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp_iso TEXT NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    size INTEGER NOT NULL,
    fill_price REAL,
    order_id INTEGER,
    status TEXT
);

CREATE TABLE IF NOT EXISTS positions (
    symbol TEXT PRIMARY KEY,
    entry_price REAL NOT NULL,
    entry_time_iso TEXT NOT NULL,
    qty INTEGER NOT NULL,
    initial_stop REAL NOT NULL,
    stop_price REAL NOT NULL,
    stop_order_id INTEGER,
    state TEXT NOT NULL,
    r_multiple REAL DEFAULT 0.0
);

CREATE TABLE IF NOT EXISTS watchlist (
    symbol TEXT PRIMARY KEY,
    gap_pct REAL,
    open_price REAL,
    prev_close REAL,
    generated_at TEXT
);

CREATE TABLE IF NOT EXISTS strategies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    rules_json TEXT NOT NULL,
    is_active INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS decision_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp_iso TEXT NOT NULL,
    event TEXT NOT NULL,
    payload_json TEXT
);

CREATE TABLE IF NOT EXISTS cycle_errors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp_iso TEXT NOT NULL,
    traceback TEXT
);

CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp_iso);
CREATE INDEX IF NOT EXISTS idx_decision_log_timestamp ON decision_log(timestamp_iso);
"""

DEFAULT_SETTINGS = {
    "bot_enabled": "true",
    "flatten_now": "false",
    "last_cycle_status": "",
    "last_cycle_timestamp": "",
}


@contextmanager
def get_conn():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db(seed_rules_path: Path | None = None):
    with get_conn() as conn:
        conn.executescript(SCHEMA)

        for key, value in DEFAULT_SETTINGS.items():
            conn.execute(
                "INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", (key, value)
            )

        row = conn.execute("SELECT COUNT(*) AS c FROM strategies").fetchone()
        if row["c"] == 0 and seed_rules_path and seed_rules_path.exists():
            now = datetime.now(ET).isoformat(timespec="seconds")
            rules_json = seed_rules_path.read_text()
            conn.execute(
                "INSERT INTO strategies (name, rules_json, is_active, created_at, updated_at) "
                "VALUES (?, ?, 1, ?, ?)",
                ("Trend Join Long (default)", rules_json, now, now),
            )


# ------------------------------------------------------------- settings ---
def get_setting(key: str, default: str = "") -> str:
    with get_conn() as conn:
        row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
        return row["value"] if row else default


def set_setting(key: str, value: str):
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO settings (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )


def is_bot_enabled() -> bool:
    return get_setting("bot_enabled", "true") == "true"


def set_bot_enabled(enabled: bool):
    set_setting("bot_enabled", "true" if enabled else "false")


def request_flatten_now():
    set_setting("flatten_now", "true")


def consume_flatten_request() -> bool:
    """Returns True (and clears the flag) if a flatten-all request is pending."""
    if get_setting("flatten_now", "false") == "true":
        set_setting("flatten_now", "false")
        return True
    return False


def record_cycle_run(status: str):
    set_setting("last_cycle_status", status)
    set_setting("last_cycle_timestamp", datetime.now(ET).isoformat(timespec="seconds"))


# ---------------------------------------------------------------- trades ---
def record_trade(symbol: str, side: str, size: int, fill_price: float, order_id, status: str):
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO trades (timestamp_iso, symbol, side, size, fill_price, order_id, status) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (datetime.now(ET).isoformat(timespec="seconds"), symbol, side, size, fill_price, order_id, status),
        )


def get_trades(limit: int = 200, today_only: bool = False) -> list[dict]:
    query = "SELECT * FROM trades"
    params = ()
    if today_only:
        today = datetime.now(ET).strftime("%Y-%m-%d")
        query += " WHERE timestamp_iso LIKE ?"
        params = (f"{today}%",)
    query += " ORDER BY timestamp_iso DESC LIMIT ?"
    params = params + (limit,)
    with get_conn() as conn:
        rows = conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]


def count_todays_buys() -> int:
    today = datetime.now(ET).strftime("%Y-%m-%d")
    with get_conn() as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM trades WHERE side = 'BUY' AND timestamp_iso LIKE ?",
            (f"{today}%",),
        ).fetchone()
        return row["c"]


# ------------------------------------------------------------- positions ---
def get_open_positions() -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM positions").fetchall()
        return [dict(r) for r in rows]


def upsert_position(pos: dict):
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO positions (symbol, entry_price, entry_time_iso, qty, initial_stop, "
            "stop_price, stop_order_id, state, r_multiple) VALUES "
            "(:symbol, :entry_price, :entry_time_iso, :qty, :initial_stop, :stop_price, "
            ":stop_order_id, :state, :r_multiple) "
            "ON CONFLICT(symbol) DO UPDATE SET "
            "qty=excluded.qty, initial_stop=excluded.initial_stop, stop_price=excluded.stop_price, "
            "stop_order_id=excluded.stop_order_id, state=excluded.state, r_multiple=excluded.r_multiple",
            pos,
        )


def remove_position(symbol: str):
    with get_conn() as conn:
        conn.execute("DELETE FROM positions WHERE symbol = ?", (symbol,))


# ------------------------------------------------------------- watchlist ---
def replace_watchlist(entries: list[dict]):
    now = datetime.now(ET).isoformat(timespec="seconds")
    with get_conn() as conn:
        conn.execute("DELETE FROM watchlist")
        conn.executemany(
            "INSERT INTO watchlist (symbol, gap_pct, open_price, prev_close, generated_at) "
            "VALUES (:symbol, :gap_pct, :open_price, :prev_close, :generated_at)",
            [{**e, "generated_at": now} for e in entries],
        )


def get_watchlist() -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM watchlist ORDER BY gap_pct DESC").fetchall()
        return [dict(r) for r in rows]


# -------------------------------------------------------------- logging ---
def log_decision(event: str, **fields):
    clean = {k: (bool(v) if isinstance(v, bool) else v) for k, v in fields.items()}
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO decision_log (timestamp_iso, event, payload_json) VALUES (?, ?, ?)",
            (datetime.now(ET).isoformat(timespec="seconds"), event, json.dumps(clean, default=str)),
        )


def get_decision_log(limit: int = 200) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM decision_log ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]


def log_cycle_error(traceback_text: str):
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO cycle_errors (timestamp_iso, traceback) VALUES (?, ?)",
            (datetime.now(ET).isoformat(timespec="seconds"), traceback_text),
        )


def trim_old_rows(retention_days: int = 90):
    cutoff = datetime.now(ET).replace(hour=0, minute=0, second=0, microsecond=0)
    cutoff = cutoff.isoformat()
    with get_conn() as conn:
        conn.execute(
            "DELETE FROM decision_log WHERE timestamp_iso < date(?, ?)",
            (cutoff, f"-{retention_days} days"),
        )

    # VACUUM cannot run inside a transaction, so it needs its own autocommit connection.
    vacuum_conn = sqlite3.connect(DB_PATH, isolation_level=None, timeout=10)
    try:
        vacuum_conn.execute("VACUUM")
    finally:
        vacuum_conn.close()


# ------------------------------------------------------------ strategies ---
def list_strategies() -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute("SELECT id, name, is_active, created_at, updated_at FROM strategies ORDER BY id").fetchall()
        return [dict(r) for r in rows]


def get_active_strategy() -> dict | None:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM strategies WHERE is_active = 1 LIMIT 1").fetchone()
        return dict(row) if row else None


def get_active_rules() -> dict:
    strategy = get_active_strategy()
    if not strategy:
        raise RuntimeError("No active strategy configured")
    return json.loads(strategy["rules_json"])


def get_strategy(strategy_id: int) -> dict | None:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM strategies WHERE id = ?", (strategy_id,)).fetchone()
        return dict(row) if row else None


def create_strategy(name: str, rules: dict) -> int:
    now = datetime.now(ET).isoformat(timespec="seconds")
    with get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO strategies (name, rules_json, is_active, created_at, updated_at) "
            "VALUES (?, ?, 0, ?, ?)",
            (name, json.dumps(rules, indent=2), now, now),
        )
        return cur.lastrowid


def update_strategy(strategy_id: int, rules: dict):
    now = datetime.now(ET).isoformat(timespec="seconds")
    with get_conn() as conn:
        conn.execute(
            "UPDATE strategies SET rules_json = ?, updated_at = ? WHERE id = ?",
            (json.dumps(rules, indent=2), now, strategy_id),
        )


def activate_strategy(strategy_id: int):
    with get_conn() as conn:
        conn.execute("UPDATE strategies SET is_active = 0")
        conn.execute("UPDATE strategies SET is_active = 1 WHERE id = ?", (strategy_id,))


def delete_strategy(strategy_id: int):
    with get_conn() as conn:
        row = conn.execute("SELECT is_active FROM strategies WHERE id = ?", (strategy_id,)).fetchone()
        if row and row["is_active"]:
            raise ValueError("Cannot delete the active strategy; activate another one first")
        conn.execute("DELETE FROM strategies WHERE id = ?", (strategy_id,))


# ------------------------------------------------------------------ users ---
def create_user(username: str, password: str):
    password_hash = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO users (username, password_hash) VALUES (?, ?)",
            (username, password_hash),
        )


def verify_user(username: str, password: str) -> bool:
    with get_conn() as conn:
        row = conn.execute("SELECT password_hash FROM users WHERE username = ?", (username,)).fetchone()
        if not row:
            return False
        return bcrypt.checkpw(password.encode("utf-8"), row["password_hash"].encode("utf-8"))


def any_users_exist() -> bool:
    with get_conn() as conn:
        row = conn.execute("SELECT COUNT(*) AS c FROM users").fetchone()
        return row["c"] > 0
