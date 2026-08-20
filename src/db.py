"""Single SQLite database backing both trading engines (paper and live) and
the dashboard, shared across every user account. Every table that holds
mode-specific state (trades, positions, watchlist, decision_log,
cycle_errors) carries a `mode` column ('paper' or 'live') so the two engines
can run at the same time without stepping on each other's data, AND an
`account_id` column (a `users.id`) so multiple people's data never mixes —
every read/write in this module that touches such a table takes account_id
as its first argument. Settings that differ per account+mode (enabled flag,
flatten request, last cycle status, account info, risk sizing) use
account+mode-prefixed keys in the shared settings table. Strategies are
shared templates across every account (one admin curates them) — but which
strategy is *active* is per-account, tracked in account_active_strategy, not
on the strategies row itself, so one account activating a strategy never
affects another's.

Until real per-account trading engines exist, cycle.py/trade.py/bot.py/
morning_prefilter.py all operate as the single admin account — see
get_default_account_id().

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
RISK_RATINGS = ("conservative", "moderate", "aggressive")


def _check_mode(mode: str):
    if mode not in MODES:
        raise ValueError(f"mode must be one of {MODES}, got {mode!r}")


def _check_risk_rating(risk_rating: str):
    if risk_rating not in RISK_RATINGS:
        raise ValueError(f"risk_rating must be one of {RISK_RATINGS}, got {risk_rating!r}")


SCHEMA = """
CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id INTEGER NOT NULL,
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
    account_id INTEGER NOT NULL,
    mode TEXT NOT NULL DEFAULT 'paper',
    symbol TEXT NOT NULL,
    side TEXT NOT NULL DEFAULT 'long',
    entry_price REAL NOT NULL,
    entry_time_iso TEXT NOT NULL,
    qty INTEGER NOT NULL,
    initial_stop REAL NOT NULL,
    stop_price REAL NOT NULL,
    stop_order_id INTEGER,
    state TEXT NOT NULL,
    r_multiple REAL DEFAULT 0.0,
    PRIMARY KEY (account_id, mode, symbol)
);

CREATE TABLE IF NOT EXISTS watchlist (
    account_id INTEGER NOT NULL,
    mode TEXT NOT NULL DEFAULT 'paper',
    symbol TEXT NOT NULL,
    direction TEXT NOT NULL DEFAULT 'long',
    gap_pct REAL,
    open_price REAL,
    prev_close REAL,
    generated_at TEXT,
    PRIMARY KEY (account_id, mode, symbol)
);

CREATE TABLE IF NOT EXISTS strategies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    direction TEXT NOT NULL DEFAULT 'long',
    rules_json TEXT NOT NULL,
    risk_rating TEXT NOT NULL DEFAULT 'moderate',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS account_active_strategy (
    account_id INTEGER NOT NULL,
    direction TEXT NOT NULL,
    strategy_id INTEGER NOT NULL,
    PRIMARY KEY (account_id, direction)
);

CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS decision_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id INTEGER NOT NULL,
    mode TEXT NOT NULL DEFAULT 'paper',
    timestamp_iso TEXT NOT NULL,
    event TEXT NOT NULL,
    payload_json TEXT
);

CREATE TABLE IF NOT EXISTS cycle_errors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id INTEGER NOT NULL,
    mode TEXT NOT NULL DEFAULT 'paper',
    timestamp_iso TEXT NOT NULL,
    traceback TEXT
);

CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    is_admin INTEGER NOT NULL DEFAULT 0
);
"""

# Created after the mode-column migrations run below — an older DB's
# trades/decision_log tables won't have `mode` yet at CREATE TABLE time.
INDEXES_SCHEMA = """
CREATE INDEX IF NOT EXISTS idx_trades_account_mode_timestamp ON trades(account_id, mode, timestamp_iso);
CREATE INDEX IF NOT EXISTS idx_decision_log_account_mode_timestamp ON decision_log(account_id, mode, timestamp_iso);
"""

# Two extra presets seeded alongside the conservative default (rules.json),
# for switching to something less strict without hand-writing rules JSON.
# Each loosens the entry filters and/or raises the risk knobs relative to
# the default — that's what "less conservative" means here: more setups
# pass the filters, and/or a single trade can risk/hold more. Seeded once
# by unique name (INSERT OR IGNORE), so re-running init_db is a no-op if
# they already exist — including if the user has since edited or deleted
# them (delete is permanent; it will not come back on the next restart
# unless its exact name is re-inserted, which INSERT OR IGNORE only does
# while a row with that name doesn't already exist elsewhere).
#
# Each tuple is (name, rules_dict, risk_rating, direction). Strategies are
# shared templates across every account; which one is active per direction
# is tracked per-account in account_active_strategy (see activate_strategy)
# — one active strategy per direction per account, not a single global
# active strategy, so a long and a short strategy can both be active and
# trading at once, independently per account.
EXTRA_STRATEGY_PRESETS = [
    (
        "Trend Join Long - Moderate",
        {
            "strategy_name": "Trend Join Long - Moderate",
            "direction": "long_only",
            "trade_timeframe": "5m",
            "universe_filters": {"index": "S&P 500", "min_price_usd": 3.0},
            "daily_filters": {
                "D1_above_prior_day_high": True,
                "D2_prior_close_above_sma200": True,
                "D3_min_gap_pct_from_prior_close": 2.0,
            },
            "intraday_filters": {
                "I1_above_premarket_high": True,
                "I2_above_today_hod": True,
                "I3_rvol_min": 1.5,
                "I3_rvol_lookback_days": 14,
            },
            "time_filter": {"earliest_entry_et": "10:05", "latest_entry_et": "15:30", "force_close_et": "15:51"},
            "exit": {
                "initial_stop_rule": "lod_minus_1pct",
                "partial_profit_trigger_R": 1.0,
                "partial_profit_fraction": 0.3333,
                "breakeven_trigger_R": 1.25,
                "post_breakeven_trail": "swing_low_5m_2_2",
            },
            "risk": {
                "max_risk_per_trade_pct": 1.5,
                "max_position_size_pct_of_portfolio": 12,
                "max_concurrent_positions": 6,
            },
        },
        "moderate",
        "long",
    ),
    (
        "Trend Join Long - Aggressive",
        {
            "strategy_name": "Trend Join Long - Aggressive",
            "direction": "long_only",
            "trade_timeframe": "5m",
            "universe_filters": {"index": "S&P 500", "min_price_usd": 3.0},
            "daily_filters": {
                "D1_above_prior_day_high": True,
                "D2_prior_close_above_sma200": True,
                "D3_min_gap_pct_from_prior_close": 1.5,
            },
            "intraday_filters": {
                "I1_above_premarket_high": True,
                "I2_above_today_hod": True,
                "I3_rvol_min": 1.2,
                "I3_rvol_lookback_days": 10,
            },
            "time_filter": {"earliest_entry_et": "10:05", "latest_entry_et": "15:30", "force_close_et": "15:51"},
            "exit": {
                "initial_stop_rule": "lod_minus_1pct",
                "partial_profit_trigger_R": 1.5,
                "partial_profit_fraction": 0.3333,
                "breakeven_trigger_R": 2.0,
                "post_breakeven_trail": "swing_low_5m_2_2",
            },
            "risk": {
                "max_risk_per_trade_pct": 2.5,
                "max_position_size_pct_of_portfolio": 20,
                "max_concurrent_positions": 8,
            },
        },
        "aggressive",
        "long",
    ),
    (
        # Exact mirror of rules.json's long default, flipped for breakdowns
        # instead of breakouts: below the prior day's low/SMA200 instead of
        # above, gap DOWN instead of up, stop above the high of day instead
        # of below the low, profit-taking/breakeven/trailing all trigger on
        # the price falling instead of rising. Same numeric thresholds as
        # the long default, so it's the "same conservatism," mirrored.
        "Trend Break Short (default)",
        {
            "strategy_name": "Trend Break Short",
            "direction": "short_only",
            "trade_timeframe": "5m",
            "universe_filters": {"index": "S&P 500", "min_price_usd": 3.0},
            "daily_filters": {
                "D1_below_prior_day_low": True,
                "D2_prior_close_below_sma200": True,
                "D3_min_gap_pct_down_from_prior_close": 3.0,
            },
            "intraday_filters": {
                "I1_below_premarket_low": True,
                "I2_below_today_lod": True,
                "I3_rvol_min": 2.0,
                "I3_rvol_lookback_days": 14,
            },
            "time_filter": {"earliest_entry_et": "10:05", "latest_entry_et": "15:30", "force_close_et": "15:51"},
            "exit": {
                "initial_stop_rule": "hod_plus_1pct",
                "partial_profit_trigger_R": 0.75,
                "partial_profit_fraction": 0.3333,
                "breakeven_trigger_R": 1.0,
                "post_breakeven_trail": "swing_high_5m_2_2",
            },
            "risk": {
                "max_risk_per_trade_pct": 1.0,
                "max_position_size_pct_of_portfolio": 10,
                "max_concurrent_positions": 5,
            },
        },
        "conservative",
        "short",
    ),
]


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


def _migrate_add_column(conn, table: str, column: str, coldef: str):
    """Adds a column to an existing table if it isn't there yet (generic
    ALTER TABLE ADD COLUMN, for tables predating a new field)."""
    if column not in _table_columns(conn, table):
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {coldef}")


def _migrate_settings_to_per_mode(conn):
    """Old (single-mode) deployments stored bare keys like 'bot_enabled'.
    Copy those over to 'paper:bot_enabled' so existing state isn't lost,
    then leave the old bare key alone (harmless, unused going forward)."""
    for key in ("bot_enabled", "flatten_now", "last_cycle_status", "last_cycle_timestamp"):
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


def _resolve_or_create_admin(conn) -> int | None:
    """Ensures exactly one user is flagged is_admin (promoting the
    earliest-created user if none is flagged yet — the natural "you" of a
    pre-multi-account deployment) and returns their id, or None if no user
    exists yet at all (a fresh install, before initial /setup runs)."""
    row = conn.execute("SELECT id FROM users WHERE is_admin = 1 ORDER BY id LIMIT 1").fetchone()
    if row:
        return row["id"]
    row = conn.execute("SELECT MIN(id) AS id FROM users").fetchone()
    if row["id"] is None:
        return None
    conn.execute("UPDATE users SET is_admin = 1 WHERE id = ?", (row["id"],))
    return row["id"]


def _migrate_settings_to_per_account(conn, account_id: int):
    """Old (single-account) deployments stored settings keys as
    '<mode>:<key>' (risk params, bot_enabled, cached account info, cached
    watchlist/broker-position snapshots, etc). Copy those to
    '<account_id>:<mode>:<key>' — covers every such key regardless of
    suffix — so existing state isn't lost when multi-account support lands.
    Old keys are left in place, harmless."""
    for mode in MODES:
        rows = conn.execute("SELECT key, value FROM settings WHERE key LIKE ?", (f"{mode}:%",)).fetchall()
        for row in rows:
            new_key = f"{account_id}:{row['key']}"
            exists = conn.execute("SELECT 1 FROM settings WHERE key = ?", (new_key,)).fetchone()
            if not exists:
                conn.execute("INSERT INTO settings (key, value) VALUES (?, ?)", (new_key, row["value"]))


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
        _migrate_add_column(conn, "strategies", "risk_rating", "TEXT NOT NULL DEFAULT 'moderate'")
        _migrate_add_column(conn, "strategies", "direction", "TEXT NOT NULL DEFAULT 'long'")
        _migrate_add_column(conn, "positions", "side", "TEXT NOT NULL DEFAULT 'long'")
        _migrate_add_column(conn, "watchlist", "direction", "TEXT NOT NULL DEFAULT 'long'")
        # The shipped default strategy predates risk_rating and got the
        # generic 'moderate' default from the ALTER TABLE above — it's
        # actually the conservative baseline every preset above is loosened
        # from, so correct it (only while still at that generic default, so
        # a deliberate manual re-rating via the dashboard isn't clobbered).
        conn.execute(
            "UPDATE strategies SET risk_rating = 'conservative' "
            "WHERE name = 'Trend Join Long (default)' AND risk_rating = 'moderate'"
        )

        # --- multi-account migration (runs once, no-ops after) ---
        _migrate_add_column(conn, "users", "is_admin", "INTEGER NOT NULL DEFAULT 0")
        admin_id = _resolve_or_create_admin(conn)

        if "is_active" in _table_columns(conn, "strategies"):
            if admin_id is not None:
                rows = conn.execute("SELECT id, direction FROM strategies WHERE is_active = 1").fetchall()
                for row in rows:
                    conn.execute(
                        "INSERT OR IGNORE INTO account_active_strategy (account_id, direction, strategy_id) "
                        "VALUES (?, ?, ?)",
                        (admin_id, row["direction"], row["id"]),
                    )
            conn.execute("ALTER TABLE strategies DROP COLUMN is_active")

        _migrate_add_column(conn, "trades", "account_id", f"INTEGER NOT NULL DEFAULT {admin_id or 1}")
        _migrate_add_column(conn, "decision_log", "account_id", f"INTEGER NOT NULL DEFAULT {admin_id or 1}")
        _migrate_add_column(conn, "cycle_errors", "account_id", f"INTEGER NOT NULL DEFAULT {admin_id or 1}")
        if "account_id" not in _table_columns(conn, "positions"):
            conn.execute("ALTER TABLE positions RENAME TO positions_old")
            conn.execute("""CREATE TABLE positions (
                account_id INTEGER NOT NULL, mode TEXT NOT NULL DEFAULT 'paper', symbol TEXT NOT NULL,
                side TEXT NOT NULL DEFAULT 'long', entry_price REAL NOT NULL, entry_time_iso TEXT NOT NULL,
                qty INTEGER NOT NULL, initial_stop REAL NOT NULL, stop_price REAL NOT NULL,
                stop_order_id INTEGER, state TEXT NOT NULL, r_multiple REAL DEFAULT 0.0,
                PRIMARY KEY (account_id, mode, symbol)
            )""")
            conn.execute(
                "INSERT INTO positions (account_id, mode, symbol, side, entry_price, entry_time_iso, qty, "
                "initial_stop, stop_price, stop_order_id, state, r_multiple) "
                "SELECT ?, mode, symbol, side, entry_price, entry_time_iso, qty, "
                "initial_stop, stop_price, stop_order_id, state, r_multiple FROM positions_old",
                (admin_id or 1,),
            )
            conn.execute("DROP TABLE positions_old")
        if "account_id" not in _table_columns(conn, "watchlist"):
            conn.execute("ALTER TABLE watchlist RENAME TO watchlist_old")
            conn.execute("""CREATE TABLE watchlist (
                account_id INTEGER NOT NULL, mode TEXT NOT NULL DEFAULT 'paper', symbol TEXT NOT NULL,
                direction TEXT NOT NULL DEFAULT 'long', gap_pct REAL, open_price REAL, prev_close REAL,
                generated_at TEXT, PRIMARY KEY (account_id, mode, symbol)
            )""")
            conn.execute(
                "INSERT INTO watchlist (account_id, mode, symbol, direction, gap_pct, open_price, prev_close, generated_at) "
                "SELECT ?, mode, symbol, direction, gap_pct, open_price, prev_close, generated_at FROM watchlist_old",
                (admin_id or 1,),
            )
            conn.execute("DROP TABLE watchlist_old")

        if admin_id is not None:
            _migrate_settings_to_per_account(conn, admin_id)
        # --- end multi-account migration ---

        conn.execute("DROP INDEX IF EXISTS idx_trades_mode_timestamp")
        conn.execute("DROP INDEX IF EXISTS idx_decision_log_mode_timestamp")
        conn.executescript(INDEXES_SCHEMA)

        row = conn.execute("SELECT COUNT(*) AS c FROM strategies").fetchone()
        if row["c"] == 0 and seed_rules_path and seed_rules_path.exists():
            now = datetime.now(ET).isoformat(timespec="seconds")
            rules_json = seed_rules_path.read_text()
            conn.execute(
                "INSERT INTO strategies (name, direction, rules_json, risk_rating, created_at, updated_at) "
                "VALUES (?, 'long', ?, 'conservative', ?, ?)",
                ("Trend Join Long (default)", rules_json, now, now),
            )

        for name, rules, risk_rating, direction in EXTRA_STRATEGY_PRESETS:
            now = datetime.now(ET).isoformat(timespec="seconds")
            conn.execute(
                "INSERT OR IGNORE INTO strategies (name, direction, rules_json, risk_rating, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (name, direction, json.dumps(rules, indent=2), risk_rating, now, now),
            )


# -------------------------------------------------------------- settings ---
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


def _scope_key(account_id: int, mode: str, key: str) -> str:
    _check_mode(mode)
    return f"{account_id}:{mode}:{key}"


def update_account_info(account_id: int, mode: str, net_liquidation: str, cash_balance: str, buying_power: str):
    set_setting(_scope_key(account_id, mode, "account_net_liquidation"), net_liquidation)
    set_setting(_scope_key(account_id, mode, "account_cash_balance"), cash_balance)
    set_setting(_scope_key(account_id, mode, "account_buying_power"), buying_power)
    set_setting(_scope_key(account_id, mode, "account_updated_at"), datetime.now(ET).isoformat(timespec="seconds"))


def get_account_info(account_id: int, mode: str) -> dict:
    return {
        "net_liquidation": get_setting(_scope_key(account_id, mode, "account_net_liquidation"), ""),
        "cash_balance": get_setting(_scope_key(account_id, mode, "account_cash_balance"), ""),
        "buying_power": get_setting(_scope_key(account_id, mode, "account_buying_power"), ""),
        "updated_at": get_setting(_scope_key(account_id, mode, "account_updated_at"), ""),
    }


def is_bot_enabled(account_id: int, mode: str) -> bool:
    return get_setting(_scope_key(account_id, mode, "bot_enabled"), "true") == "true"


def set_bot_enabled(account_id: int, mode: str, enabled: bool):
    set_setting(_scope_key(account_id, mode, "bot_enabled"), "true" if enabled else "false")


def request_flatten_now(account_id: int, mode: str):
    set_setting(_scope_key(account_id, mode, "flatten_now"), "true")


def is_flatten_pending(account_id: int, mode: str) -> bool:
    """Cheap read-only check (doesn't clear the flag) — see
    cycle.emergency_check, which uses this to avoid opening an IBKR
    connection unless a flatten is actually pending."""
    return get_setting(_scope_key(account_id, mode, "flatten_now"), "false") == "true"


def consume_flatten_request(account_id: int, mode: str) -> bool:
    """Returns True (and clears the flag) if a flatten-all request is pending."""
    key = _scope_key(account_id, mode, "flatten_now")
    if get_setting(key, "false") == "true":
        set_setting(key, "false")
        return True
    return False


def record_cycle_run(account_id: int, mode: str, status: str):
    set_setting(_scope_key(account_id, mode, "last_cycle_status"), status)
    set_setting(_scope_key(account_id, mode, "last_cycle_timestamp"), datetime.now(ET).isoformat(timespec="seconds"))


def set_next_cycle_at(account_id: int, mode: str, next_run_iso: str):
    """Records when the scheduler will next fire the 'cycle' job for this
    account+mode (see run_service.py), so the dashboard can show a
    countdown. This is the scheduler's own next firing time, independent of
    whether that firing will actually do anything (run_cycle() self-gates
    on market hours) — it always fires every 5 minutes, so the countdown is
    accurate even outside trading hours."""
    set_setting(_scope_key(account_id, mode, "next_cycle_at"), next_run_iso)


def get_cycle_status(account_id: int, mode: str) -> dict:
    return {
        "last_cycle_status": get_setting(_scope_key(account_id, mode, "last_cycle_status"), ""),
        "last_cycle_timestamp": get_setting(_scope_key(account_id, mode, "last_cycle_timestamp"), ""),
        "next_cycle_at": get_setting(_scope_key(account_id, mode, "next_cycle_at"), ""),
        "bot_enabled": is_bot_enabled(account_id, mode),
        "flatten_pending": get_setting(_scope_key(account_id, mode, "flatten_now"), "false") == "true",
    }


# ---------------------------------------------------------------- trades ---
def record_trade(account_id: int, mode: str, symbol: str, side: str, size: int, fill_price: float, order_id, status: str):
    _check_mode(mode)
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO trades (account_id, mode, timestamp_iso, symbol, side, size, fill_price, order_id, status) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (account_id, mode, datetime.now(ET).isoformat(timespec="seconds"), symbol, side, size, fill_price, order_id, status),
        )


def get_trades(account_id: int, mode: str, limit: int = 200, today_only: bool = False) -> list[dict]:
    _check_mode(mode)
    query = "SELECT * FROM trades WHERE account_id = ? AND mode = ?"
    params = [account_id, mode]
    if today_only:
        today = datetime.now(ET).strftime("%Y-%m-%d")
        query += " AND timestamp_iso LIKE ?"
        params.append(f"{today}%")
    query += " ORDER BY timestamp_iso DESC LIMIT ?"
    params.append(limit)
    with get_conn() as conn:
        rows = conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]


def count_todays_entries(account_id: int, mode: str, side: str) -> int:
    """Counts today's *opening* trades for one side ('long' or 'short') —
    long opens with a BUY, short opens with a SELL, so this can't just
    count trades table rows by BUY/SELL action (a SELL can also be a long
    being closed). Backed by the 'entry' decision_log events cycle.py logs
    on every successful open, each tagged with its side."""
    _check_mode(mode)
    today = datetime.now(ET).strftime("%Y-%m-%d")
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT payload_json FROM decision_log WHERE account_id = ? AND mode = ? AND event = 'entry' "
            "AND timestamp_iso LIKE ?",
            (account_id, mode, f"{today}%"),
        ).fetchall()
    count = 0
    for r in rows:
        try:
            payload = json.loads(r["payload_json"])
        except (json.JSONDecodeError, TypeError):
            continue
        if payload.get("side") == side:
            count += 1
    return count


# ------------------------------------------------------------- positions ---
def get_open_positions(account_id: int, mode: str) -> list[dict]:
    _check_mode(mode)
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM positions WHERE account_id = ? AND mode = ?", (account_id, mode)
        ).fetchall()
        return [dict(r) for r in rows]


def upsert_position(account_id: int, mode: str, pos: dict):
    _check_mode(mode)
    pos = {"side": "long", **pos}  # default for callers that predate short support
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO positions (account_id, mode, symbol, side, entry_price, entry_time_iso, qty, initial_stop, "
            "stop_price, stop_order_id, state, r_multiple) VALUES "
            "(:account_id, :mode, :symbol, :side, :entry_price, :entry_time_iso, :qty, :initial_stop, :stop_price, "
            ":stop_order_id, :state, :r_multiple) "
            "ON CONFLICT(account_id, mode, symbol) DO UPDATE SET "
            "qty=excluded.qty, initial_stop=excluded.initial_stop, stop_price=excluded.stop_price, "
            "stop_order_id=excluded.stop_order_id, state=excluded.state, r_multiple=excluded.r_multiple",
            {**pos, "account_id": account_id, "mode": mode},
        )


def remove_position(account_id: int, mode: str, symbol: str):
    _check_mode(mode)
    with get_conn() as conn:
        conn.execute(
            "DELETE FROM positions WHERE account_id = ? AND mode = ? AND symbol = ?", (account_id, mode, symbol)
        )


# ------------------------------------------------------------- watchlist ---
def replace_watchlist(account_id: int, mode: str, entries: list[dict]):
    """Replaces the ENTIRE watchlist (both directions) for this account+mode
    — a caller that only scans one direction must pass the other
    direction's current entries back in too, or they'll be wiped.
    morning_prefilter.py scans both in one pass for exactly this reason."""
    _check_mode(mode)
    now = datetime.now(ET).isoformat(timespec="seconds")
    with get_conn() as conn:
        conn.execute("DELETE FROM watchlist WHERE account_id = ? AND mode = ?", (account_id, mode))
        conn.executemany(
            "INSERT INTO watchlist (account_id, mode, symbol, direction, gap_pct, open_price, prev_close, generated_at) "
            "VALUES (:account_id, :mode, :symbol, :direction, :gap_pct, :open_price, :prev_close, :generated_at)",
            [{"direction": "long", **e, "account_id": account_id, "mode": mode, "generated_at": now} for e in entries],
        )


def get_watchlist(account_id: int, mode: str, direction: str | None = None) -> list[dict]:
    _check_mode(mode)
    query = "SELECT * FROM watchlist WHERE account_id = ? AND mode = ?"
    params = [account_id, mode]
    if direction is not None:
        query += " AND direction = ?"
        params.append(direction)
    query += " ORDER BY ABS(gap_pct) DESC"
    with get_conn() as conn:
        rows = conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]


def update_watchlist_filters(account_id: int, mode: str, results: list[dict]):
    """Stores the latest per-symbol D1-D3/I1-I3 filter snapshot (see
    cycle.scan_watchlist_filters) for the dashboard's Watchlist table."""
    _check_mode(mode)
    payload = {"updated_at": datetime.now(ET).isoformat(timespec="seconds"), "results": results}
    set_setting(_scope_key(account_id, mode, "watchlist_filters_json"), json.dumps(payload))


def get_watchlist_filters(account_id: int, mode: str) -> dict:
    _check_mode(mode)
    raw = get_setting(_scope_key(account_id, mode, "watchlist_filters_json"), "")
    if not raw:
        return {"updated_at": "", "results": []}
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {"updated_at": "", "results": []}


def update_broker_positions(account_id: int, mode: str, positions: list[dict]):
    """Stores the latest raw IBKR account holdings (see
    cycle.refresh_account_info) for the dashboard's Account Holdings view —
    every real position in the account, independent of whether the bot
    opened it or is tracking it in the `positions` table."""
    _check_mode(mode)
    payload = {"updated_at": datetime.now(ET).isoformat(timespec="seconds"), "positions": positions}
    set_setting(_scope_key(account_id, mode, "broker_positions_json"), json.dumps(payload))


def get_broker_positions(account_id: int, mode: str) -> dict:
    _check_mode(mode)
    raw = get_setting(_scope_key(account_id, mode, "broker_positions_json"), "")
    if not raw:
        return {"updated_at": "", "positions": []}
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {"updated_at": "", "positions": []}


# -------------------------------------------------------------- logging ---
def log_decision(account_id: int, mode: str, event: str, **fields):
    _check_mode(mode)
    clean = {k: (bool(v) if isinstance(v, bool) else v) for k, v in fields.items()}
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO decision_log (account_id, mode, timestamp_iso, event, payload_json) VALUES (?, ?, ?, ?, ?)",
            (account_id, mode, datetime.now(ET).isoformat(timespec="seconds"), event, json.dumps(clean, default=str)),
        )


def get_decision_log(account_id: int, mode: str, limit: int = 200) -> list[dict]:
    _check_mode(mode)
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM decision_log WHERE account_id = ? AND mode = ? ORDER BY id DESC LIMIT ?",
            (account_id, mode, limit),
        ).fetchall()
        return [dict(r) for r in rows]


def log_cycle_error(account_id: int, mode: str, traceback_text: str):
    _check_mode(mode)
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO cycle_errors (account_id, mode, timestamp_iso, traceback) VALUES (?, ?, ?, ?)",
            (account_id, mode, datetime.now(ET).isoformat(timespec="seconds"), traceback_text),
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
# Strategies are shared templates across every account — one admin curates
# them. "Active" is per-account and per-direction (account_active_strategy):
# one active long strategy and one active short strategy can run at the
# same time on a given account (long and short trade independently and
# never hold the same symbol at once, since entry_scan for either checks
# all currently-held symbols regardless of side) — see activate_strategy. A
# direction with no active strategy simply doesn't trade that side for that
# account; get_active_rules returns None for it rather than raising, since
# "short trading off" is a normal, common state.
DIRECTIONS = ("long", "short")


def _check_direction(direction: str):
    if direction not in DIRECTIONS:
        raise ValueError(f"direction must be one of {DIRECTIONS}, got {direction!r}")


def list_strategies(account_id: int) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT s.id, s.name, s.direction, s.risk_rating, s.created_at, s.updated_at, "
            "(a.strategy_id IS NOT NULL) AS is_active "
            "FROM strategies s "
            "LEFT JOIN account_active_strategy a ON a.strategy_id = s.id AND a.account_id = ? "
            "ORDER BY s.direction, s.id",
            (account_id,),
        ).fetchall()
        return [dict(r) for r in rows]


def get_active_strategy(account_id: int, direction: str) -> dict | None:
    _check_direction(direction)
    with get_conn() as conn:
        row = conn.execute(
            "SELECT s.* FROM strategies s "
            "JOIN account_active_strategy a ON a.strategy_id = s.id "
            "WHERE a.account_id = ? AND a.direction = ? LIMIT 1",
            (account_id, direction),
        ).fetchone()
        return dict(row) if row else None


def get_active_rules(account_id: int, direction: str) -> dict | None:
    strategy = get_active_strategy(account_id, direction)
    return json.loads(strategy["rules_json"]) if strategy else None


def get_strategy(strategy_id: int) -> dict | None:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM strategies WHERE id = ?", (strategy_id,)).fetchone()
        return dict(row) if row else None


def create_strategy(name: str, rules: dict, direction: str, risk_rating: str = "moderate") -> int:
    _check_direction(direction)
    _check_risk_rating(risk_rating)
    now = datetime.now(ET).isoformat(timespec="seconds")
    with get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO strategies (name, direction, rules_json, risk_rating, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (name, direction, json.dumps(rules, indent=2), risk_rating, now, now),
        )
        return cur.lastrowid


def update_strategy(strategy_id: int, rules: dict, risk_rating: str | None = None):
    # direction is intentionally not editable here — it defines what the
    # rules JSON's fields even mean (D1_above_prior_day_high vs
    # D1_below_prior_day_low, etc), so changing it on an existing strategy
    # would silently invalidate its own rules rather than convert them.
    if risk_rating is not None:
        _check_risk_rating(risk_rating)
    now = datetime.now(ET).isoformat(timespec="seconds")
    with get_conn() as conn:
        if risk_rating is not None:
            conn.execute(
                "UPDATE strategies SET rules_json = ?, risk_rating = ?, updated_at = ? WHERE id = ?",
                (json.dumps(rules, indent=2), risk_rating, now, strategy_id),
            )
        else:
            conn.execute(
                "UPDATE strategies SET rules_json = ?, updated_at = ? WHERE id = ?",
                (json.dumps(rules, indent=2), now, strategy_id),
            )


def activate_strategy(account_id: int, strategy_id: int):
    """Activates this strategy for this account only, on its direction —
    replacing only this account's own prior selection for that direction
    (account_active_strategy's primary key is account_id+direction), so
    activating a strategy never touches any other account's selection, nor
    this account's selection on the OTHER direction."""
    with get_conn() as conn:
        row = conn.execute("SELECT direction FROM strategies WHERE id = ?", (strategy_id,)).fetchone()
        if not row:
            raise ValueError(f"Strategy {strategy_id} not found")
        conn.execute(
            "INSERT INTO account_active_strategy (account_id, direction, strategy_id) VALUES (?, ?, ?) "
            "ON CONFLICT(account_id, direction) DO UPDATE SET strategy_id = excluded.strategy_id",
            (account_id, row["direction"], strategy_id),
        )


def delete_strategy(strategy_id: int):
    with get_conn() as conn:
        active = conn.execute(
            "SELECT 1 FROM account_active_strategy WHERE strategy_id = ? LIMIT 1", (strategy_id,)
        ).fetchone()
        if active:
            raise ValueError("Cannot delete a strategy that's active on at least one account; deactivate it there first")
        conn.execute("DELETE FROM strategies WHERE id = ?", (strategy_id,))


# ------------------------------------------------------------------ users ---
def create_user(username: str, password: str, is_admin: bool = False) -> int:
    """Creates a new account and seeds it with a sane starting point: the
    conservative long default active (nothing active on the short side),
    mirroring what every fresh single-account deployment used to seed
    automatically before multi-account support existed."""
    password_hash = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
    with get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO users (username, password_hash, is_admin) VALUES (?, ?, ?)",
            (username, password_hash, int(is_admin)),
        )
        account_id = cur.lastrowid
        default_row = conn.execute(
            "SELECT id FROM strategies WHERE name = 'Trend Join Long (default)'"
        ).fetchone()
        if default_row:
            conn.execute(
                "INSERT OR IGNORE INTO account_active_strategy (account_id, direction, strategy_id) VALUES (?, 'long', ?)",
                (account_id, default_row["id"]),
            )
    return account_id


def verify_user(username: str, password: str) -> bool:
    with get_conn() as conn:
        row = conn.execute("SELECT password_hash FROM users WHERE username = ?", (username,)).fetchone()
        if not row:
            return False
        return bcrypt.checkpw(password.encode("utf-8"), row["password_hash"].encode("utf-8"))


def get_user_by_username(username: str) -> dict | None:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT id, username, is_admin FROM users WHERE username = ?", (username,)
        ).fetchone()
        return dict(row) if row else None


def any_users_exist() -> bool:
    with get_conn() as conn:
        row = conn.execute("SELECT COUNT(*) AS c FROM users").fetchone()
        return row["c"] > 0


def get_default_account_id() -> int:
    """The account cycle.py/trade.py/bot.py/morning_prefilter.py operate as
    until real per-account trading engines exist (see the multi-account
    plan) — the admin's own account. Raises if initial setup (creating the
    first dashboard user) hasn't happened yet, since there's no account to
    run the bot as."""
    with get_conn() as conn:
        row = conn.execute("SELECT id FROM users WHERE is_admin = 1 ORDER BY id LIMIT 1").fetchone()
    if row is None:
        raise RuntimeError(
            "No admin account exists yet — complete initial setup (create the first dashboard user) first."
        )
    return row["id"]
