"""Single SQLite database backing both trading engines (paper and live) and
the dashboard. Every table that holds mode-specific state (trades,
positions, watchlist, decision_log, cycle_errors) carries a `mode` column
('paper' or 'live') so the two engines can run at the same time against the
same DB file without stepping on each other's data. Settings that differ
per mode (enabled flag, flatten request, last cycle status, account info)
use mode-prefixed keys in the shared settings table. Strategies are shared
across both modes — there's one active strategy at a time, used by both.

WAL mode lets readers (dashboard) and the two writers (paper + live
engines) run at the same time without locking each other out.
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

MODES = ("paper", "live")


def _check_mode(mode: str):
    if mode not in MODES:
        raise ValueError(f"mode must be one of {MODES}, got {mode!r}")


SCHEMA = """
CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    mode TEXT NOT NULL DEFAULT 'paper',
    timestamp_iso TEXT NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    size INTEGER NOT NULL,
    fill_price REAL,
    order_id INTEGER,
    status TEXT
);

CREATE TABLE IF NOT EXISTS positions (
    mode TEXT NOT NULL DEFAULT 'paper',
    symbol TEXT NOT NULL,
    entry_price REAL NOT NULL,
    entry_time_iso TEXT NOT NULL,
    qty INTEGER NOT NULL,
    initial_stop REAL NOT NULL,
    stop_price REAL NOT NULL,
    stop_order_id INTEGER,
    state TEXT NOT NULL,
    r_multiple REAL DEFAULT 0.0,
    PRIMARY KEY (mode, symbol)
);

CREATE TABLE IF NOT EXISTS watchlist (
    mode TEXT NOT NULL DEFAULT 'paper',
    symbol TEXT NOT NULL,
    gap_pct REAL,
    open_price REAL,
    prev_close REAL,
    generated_at TEXT,
    PRIMARY KEY (mode, symbol)
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
    mode TEXT NOT NULL DEFAULT 'paper',
    timestamp_iso TEXT NOT NULL,
    event TEXT NOT NULL,
    payload_json TEXT
);

CREATE TABLE IF NOT EXISTS cycle_errors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    mode TEXT NOT NULL DEFAULT 'paper',
    timestamp_iso TEXT NOT NULL,
    traceback TEXT
);

CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL
);
"""

# Created after the mode-column migrations run below — an older DB's
# trades/decision_log tables won't have `mode` yet at CREATE TABLE time.
INDEXES_SCHEMA = """
CREATE INDEX IF NOT EXISTS idx_trades_mode_timestamp ON trades(mode, timestamp_iso);
CREATE INDEX IF NOT EXISTS idx_decision_log_mode_timestamp ON decision_log(mode, timestamp_iso);
"""

DEFAULT_SETTINGS_PER_MODE = {
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


def _table_columns(conn, table: str) -> set[str]:
    return {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")}


def _migrate_composite_key_table(conn, table: str, create_sql: str, columns: list[str]):
    """Rebuilds a table to add the `mode` column as part of its primary key,
    for tables created before dual paper/live support existed. Existing
    rows are tagged mode='paper' (the only mode that existed before)."""
    if "mode" in _table_columns(conn, table):
        return
    conn.execute(f"ALTER TABLE {table} RENAME TO {table}_old")
    conn.execute(create_sql)
    col_list = ", ".join(columns)
    conn.execute(
        f"INSERT INTO {table} (mode, {col_list}) "
        f"SELECT 'paper', {col_list} FROM {table}_old"
    )
    conn.execute(f"DROP TABLE {table}_old")


def _migrate_simple_mode_column(conn, table: str):
    """Adds a `mode` column defaulting to 'paper' to a table that doesn't
    need its primary key changed."""
    if "mode" not in _table_columns(conn, table):
        conn.execute(f"ALTER TABLE {table} ADD COLUMN mode TEXT NOT NULL DEFAULT 'paper'")


def _migrate_settings_to_per_mode(conn):
    """Old (single-mode) deployments stored bare keys like 'bot_enabled'.
    Copy those over to 'paper:bot_enabled' so existing state isn't lost,
    then leave the old bare key alone (harmless, unused going forward)."""
    for key in DEFAULT_SETTINGS_PER_MODE:
        old_row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
        new_key = f"paper:{key}"
        exists = conn.execute("SELECT 1 FROM settings WHERE key = ?", (new_key,)).fetchone()
        if old_row and not exists:
            conn.execute("INSERT INTO settings (key, value) VALUES (?, ?)", (new_key, old_row["value"]))
    for key in ("account_net_liquidation", "account_cash_balance", "account_buying_power", "account_updated_at"):
        old_row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
        new_key = f"paper:{key}"
        exists = conn.execute("SELECT 1 FROM settings WHERE key = ?", (new_key,)).fetchone()
        if old_row and not exists:
            conn.execute("INSERT INTO settings (key, value) VALUES (?, ?)", (new_key, old_row["value"]))


def init_db(seed_rules_path: Path | None = None):
    with get_conn() as conn:
        conn.executescript(SCHEMA)

        _migrate_simple_mode_column(conn, "trades")
        _migrate_simple_mode_column(conn, "decision_log")
        _migrate_simple_mode_column(conn, "cycle_errors")
        _migrate_composite_key_table(
            conn, "positions",
            """CREATE TABLE positions (
                mode TEXT NOT NULL DEFAULT 'paper', symbol TEXT NOT NULL,
                entry_price REAL NOT NULL, entry_time_iso TEXT NOT NULL, qty INTEGER NOT NULL,
                initial_stop REAL NOT NULL, stop_price REAL NOT NULL, stop_order_id INTEGER,
                state TEXT NOT NULL, r_multiple REAL DEFAULT 0.0, PRIMARY KEY (mode, symbol)
            )""",
            ["symbol", "entry_price", "entry_time_iso", "qty", "initial_stop",
             "stop_price", "stop_order_id", "state", "r_multiple"],
        )
        _migrate_composite_key_table(
            conn, "watchlist",
            """CREATE TABLE watchlist (
                mode TEXT NOT NULL DEFAULT 'paper', symbol TEXT NOT NULL,
                gap_pct REAL, open_price REAL, prev_close REAL, generated_at TEXT,
                PRIMARY KEY (mode, symbol)
            )""",
            ["symbol", "gap_pct", "open_price", "prev_close", "generated_at"],
        )
        _migrate_settings_to_per_mode(conn)
        conn.executescript(INDEXES_SCHEMA)

        for mode in MODES:
            for key, value in DEFAULT_SETTINGS_PER_MODE.items():
                conn.execute(
                    "INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", (f"{mode}:{key}", value)
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


def _mode_key(mode: str, key: str) -> str:
    _check_mode(mode)
    return f"{mode}:{key}"


def update_account_info(mode: str, net_liquidation: str, cash_balance: str, buying_power: str):
    set_setting(_mode_key(mode, "account_net_liquidation"), net_liquidation)
    set_setting(_mode_key(mode, "account_cash_balance"), cash_balance)
    set_setting(_mode_key(mode, "account_buying_power"), buying_power)
    set_setting(_mode_key(mode, "account_updated_at"), datetime.now(ET).isoformat(timespec="seconds"))


def get_account_info(mode: str) -> dict:
    return {
        "net_liquidation": get_setting(_mode_key(mode, "account_net_liquidation"), ""),
        "cash_balance": get_setting(_mode_key(mode, "account_cash_balance"), ""),
        "buying_power": get_setting(_mode_key(mode, "account_buying_power"), ""),
        "updated_at": get_setting(_mode_key(mode, "account_updated_at"), ""),
    }


def is_bot_enabled(mode: str) -> bool:
    return get_setting(_mode_key(mode, "bot_enabled"), "true") == "true"


def set_bot_enabled(mode: str, enabled: bool):
    set_setting(_mode_key(mode, "bot_enabled"), "true" if enabled else "false")


def request_flatten_now(mode: str):
    set_setting(_mode_key(mode, "flatten_now"), "true")


def consume_flatten_request(mode: str) -> bool:
    """Returns True (and clears the flag) if a flatten-all request is pending."""
    key = _mode_key(mode, "flatten_now")
    if get_setting(key, "false") == "true":
        set_setting(key, "false")
        return True
    return False


def record_cycle_run(mode: str, status: str):
    set_setting(_mode_key(mode, "last_cycle_status"), status)
    set_setting(_mode_key(mode, "last_cycle_timestamp"), datetime.now(ET).isoformat(timespec="seconds"))


def get_cycle_status(mode: str) -> dict:
    return {
        "last_cycle_status": get_setting(_mode_key(mode, "last_cycle_status"), ""),
        "last_cycle_timestamp": get_setting(_mode_key(mode, "last_cycle_timestamp"), ""),
        "bot_enabled": is_bot_enabled(mode),
        "flatten_pending": get_setting(_mode_key(mode, "flatten_now"), "false") == "true",
    }


# ---------------------------------------------------------------- trades ---
def record_trade(mode: str, symbol: str, side: str, size: int, fill_price: float, order_id, status: str):
    _check_mode(mode)
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO trades (mode, timestamp_iso, symbol, side, size, fill_price, order_id, status) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (mode, datetime.now(ET).isoformat(timespec="seconds"), symbol, side, size, fill_price, order_id, status),
        )


def get_trades(mode: str, limit: int = 200, today_only: bool = False) -> list[dict]:
    _check_mode(mode)
    query = "SELECT * FROM trades WHERE mode = ?"
    params = [mode]
    if today_only:
        today = datetime.now(ET).strftime("%Y-%m-%d")
        query += " AND timestamp_iso LIKE ?"
        params.append(f"{today}%")
    query += " ORDER BY timestamp_iso DESC LIMIT ?"
    params.append(limit)
    with get_conn() as conn:
        rows = conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]


def count_todays_buys(mode: str) -> int:
    _check_mode(mode)
    today = datetime.now(ET).strftime("%Y-%m-%d")
    with get_conn() as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM trades WHERE mode = ? AND side = 'BUY' AND timestamp_iso LIKE ?",
            (mode, f"{today}%"),
        ).fetchone()
        return row["c"]


# ------------------------------------------------------------- positions ---
def get_open_positions(mode: str) -> list[dict]:
    _check_mode(mode)
    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM positions WHERE mode = ?", (mode,)).fetchall()
        return [dict(r) for r in rows]


def upsert_position(mode: str, pos: dict):
    _check_mode(mode)
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO positions (mode, symbol, entry_price, entry_time_iso, qty, initial_stop, "
            "stop_price, stop_order_id, state, r_multiple) VALUES "
            "(:mode, :symbol, :entry_price, :entry_time_iso, :qty, :initial_stop, :stop_price, "
            ":stop_order_id, :state, :r_multiple) "
            "ON CONFLICT(mode, symbol) DO UPDATE SET "
            "qty=excluded.qty, initial_stop=excluded.initial_stop, stop_price=excluded.stop_price, "
            "stop_order_id=excluded.stop_order_id, state=excluded.state, r_multiple=excluded.r_multiple",
            {**pos, "mode": mode},
        )


def remove_position(mode: str, symbol: str):
    _check_mode(mode)
    with get_conn() as conn:
        conn.execute("DELETE FROM positions WHERE mode = ? AND symbol = ?", (mode, symbol))


# ------------------------------------------------------------- watchlist ---
def replace_watchlist(mode: str, entries: list[dict]):
    _check_mode(mode)
    now = datetime.now(ET).isoformat(timespec="seconds")
    with get_conn() as conn:
        conn.execute("DELETE FROM watchlist WHERE mode = ?", (mode,))
        conn.executemany(
            "INSERT INTO watchlist (mode, symbol, gap_pct, open_price, prev_close, generated_at) "
            "VALUES (:mode, :symbol, :gap_pct, :open_price, :prev_close, :generated_at)",
            [{**e, "mode": mode, "generated_at": now} for e in entries],
        )


def get_watchlist(mode: str) -> list[dict]:
    _check_mode(mode)
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM watchlist WHERE mode = ? ORDER BY gap_pct DESC", (mode,)
        ).fetchall()
        return [dict(r) for r in rows]


# -------------------------------------------------------------- logging ---
def log_decision(mode: str, event: str, **fields):
    _check_mode(mode)
    clean = {k: (bool(v) if isinstance(v, bool) else v) for k, v in fields.items()}
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO decision_log (mode, timestamp_iso, event, payload_json) VALUES (?, ?, ?, ?)",
            (mode, datetime.now(ET).isoformat(timespec="seconds"), event, json.dumps(clean, default=str)),
        )


def get_decision_log(mode: str, limit: int = 200) -> list[dict]:
    _check_mode(mode)
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM decision_log WHERE mode = ? ORDER BY id DESC LIMIT ?", (mode, limit)
        ).fetchall()
        return [dict(r) for r in rows]


def log_cycle_error(mode: str, traceback_text: str):
    _check_mode(mode)
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO cycle_errors (mode, timestamp_iso, traceback) VALUES (?, ?, ?)",
            (mode, datetime.now(ET).isoformat(timespec="seconds"), traceback_text),
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
# Strategies are shared across both modes — there is one active strategy at
# a time, and both the paper and live engines trade whichever one is active.
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
