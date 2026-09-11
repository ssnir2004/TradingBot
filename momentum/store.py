"""All momentum-suite persistence. Uses the same SQLite file as the rest
of TradingBot (src.db.get_conn) but owns a private set of tables, created
here with CREATE TABLE IF NOT EXISTS and never referenced from db.SCHEMA -
so this feature is fully additive and carries no migration risk for the
S&P 500 engine.

Tables
------
momentum_candidates : one row per symbol per scan cycle - the raw
    TradingView screener snapshot plus whether it passed the G1-G6 hard
    filters. This is the audit trail of "what the scanner saw".
momentum_signals    : one row per detector hit (strategy A/B/C/D). Carries
    the computed entry reference, stop and scale targets, a conviction
    tag, and a features_json blob. `outcome` stays NULL in phase 1
    (alert-only); phases 3+ fill it in.
momentum_bars_meta  : which symbol/timeframe bar files exist in
    data/momentum_bars/ and the range they cover, so the loop can decide
    whether to re-fetch.
momentum_positions  : phase 3 (momentum.live) - one row per REAL open
    position this engine placed. Distinct from the S&P engine's own
    `positions` table (never shared) - a manual/other position in the
    same broker account never appears here and this engine never touches
    positions it didn't itself open.
momentum_trades     : phase 3 - one row per REAL fill (entry, scale-out,
    stop, or close) against a momentum_positions row.
"""
import json
from datetime import datetime
from zoneinfo import ZoneInfo

from src.db import get_conn

ET = ZoneInfo("America/New_York")

SCHEMA = """
CREATE TABLE IF NOT EXISTS momentum_candidates (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    scan_iso        TEXT NOT NULL,
    symbol          TEXT NOT NULL,
    exchange        TEXT,
    price           REAL,
    change_from_open_pct REAL,
    rvol            REAL,
    float_shares    REAL,
    volume          REAL,
    avg_volume_10d  REAL,
    premarket_pct   REAL,
    passed_screener INTEGER NOT NULL DEFAULT 0,
    reject_reason   TEXT
);
CREATE INDEX IF NOT EXISTS ix_mom_cand_scan ON momentum_candidates (scan_iso);
CREATE INDEX IF NOT EXISTS ix_mom_cand_sym  ON momentum_candidates (symbol, scan_iso);

CREATE TABLE IF NOT EXISTS momentum_signals (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_iso      TEXT NOT NULL,
    trade_date      TEXT NOT NULL,           -- ET date, for per-day cooldown counting
    strategy        TEXT NOT NULL,           -- A | B | C | D
    symbol          TEXT NOT NULL,
    timeframe       TEXT NOT NULL,
    price           REAL NOT NULL,           -- last price when the signal fired
    entry_ref       REAL NOT NULL,           -- the break / trigger level
    stop_price      REAL NOT NULL,
    max_loss_price  REAL,
    first_scale_price  REAL,
    second_scale_price REAL,
    shares_hint     INTEGER,                 -- sizing at per_trade_pct, informational
    conviction      TEXT NOT NULL DEFAULT 'normal',
    above_preferred_range INTEGER NOT NULL DEFAULT 0,
    features_json   TEXT,
    mode            TEXT NOT NULL DEFAULT 'alert',   -- alert | live
    outcome         TEXT,                    -- filled/skipped/win/loss/... (phases 3+)
    outcome_json    TEXT
);
CREATE INDEX IF NOT EXISTS ix_mom_sig_sym_date ON momentum_signals (symbol, trade_date);
CREATE INDEX IF NOT EXISTS ix_mom_sig_iso ON momentum_signals (signal_iso);

CREATE TABLE IF NOT EXISTS momentum_bars_meta (
    symbol      TEXT NOT NULL,
    timeframe   TEXT NOT NULL,
    from_iso    TEXT,
    to_iso      TEXT,
    bar_count   INTEGER,
    updated_iso TEXT,
    PRIMARY KEY (symbol, timeframe)
);

CREATE TABLE IF NOT EXISTS momentum_positions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_id       INTEGER,
    strategy        TEXT NOT NULL,
    symbol          TEXT NOT NULL,
    qty             INTEGER NOT NULL,
    entry_price     REAL NOT NULL,
    entry_time_iso  TEXT NOT NULL,
    trade_date      TEXT NOT NULL,
    stop_price      REAL NOT NULL,           -- CURRENT resting stop - moves to breakeven, then trails
    initial_stop_price REAL NOT NULL,        -- captured once at entry, never touched again - the true "1R" reference
    stop_order_id   INTEGER,
    entry_order_id  INTEGER,
    state           TEXT NOT NULL DEFAULT 'open',   -- open | scaled | closed
    scaled1         INTEGER NOT NULL DEFAULT 0,
    closed_at_iso   TEXT,
    close_reason    TEXT,
    realized_pnl_usd REAL NOT NULL DEFAULT 0.0
);
CREATE INDEX IF NOT EXISTS ix_mom_pos_state ON momentum_positions (state);
CREATE INDEX IF NOT EXISTS ix_mom_pos_strat_date ON momentum_positions (strategy, trade_date);

CREATE TABLE IF NOT EXISTS momentum_trades (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    position_id     INTEGER NOT NULL,
    strategy        TEXT NOT NULL,
    symbol          TEXT NOT NULL,
    side            TEXT NOT NULL,           -- BUY | SELL
    qty             INTEGER NOT NULL,
    price           REAL NOT NULL,
    order_id        INTEGER,
    reason          TEXT NOT NULL,           -- entry | scale1 | stop | breakeven_stop | red_candle | bailout | eod | broker_reconcile
    timestamp_iso   TEXT NOT NULL,
    trade_date      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_mom_trade_strat_date ON momentum_trades (strategy, trade_date);
"""


def init_momentum_db() -> None:
    with get_conn() as conn:
        conn.executescript(SCHEMA)


# --------------------------------------------------------------- candidates ---
def record_candidates(scan_iso: str, rows: list[dict]) -> None:
    """rows: dicts with the momentum_candidates columns (minus id/scan_iso)."""
    if not rows:
        return
    cols = ("symbol", "exchange", "price", "change_from_open_pct", "rvol",
            "float_shares", "volume", "avg_volume_10d", "premarket_pct",
            "passed_screener", "reject_reason")
    with get_conn() as conn:
        conn.executemany(
            f"INSERT INTO momentum_candidates (scan_iso, {', '.join(cols)}) "
            f"VALUES (?, {', '.join('?' for _ in cols)})",
            [(scan_iso, *(r.get(c) for c in cols)) for r in rows],
        )


# ------------------------------------------------------------------ signals ---
def signals_today(symbol: str, trade_date: str) -> list[dict]:
    with get_conn() as conn:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM momentum_signals WHERE symbol = ? AND trade_date = ? "
            "ORDER BY id DESC", (symbol, trade_date),
        )]


def last_signal_at(symbol: str, strategy: str) -> datetime | None:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT signal_iso FROM momentum_signals WHERE symbol = ? AND strategy = ? "
            "ORDER BY id DESC LIMIT 1", (symbol, strategy),
        ).fetchone()
    if not row:
        return None
    try:
        return datetime.fromisoformat(row["signal_iso"])
    except ValueError:
        return None


def record_signal(sig: dict) -> int:
    cols = ("signal_iso", "trade_date", "strategy", "symbol", "timeframe", "price",
            "entry_ref", "stop_price", "max_loss_price", "first_scale_price",
            "second_scale_price", "shares_hint", "conviction",
            "above_preferred_range", "features_json", "mode")
    payload = dict(sig)
    if isinstance(payload.get("features_json"), (dict, list)):
        payload["features_json"] = json.dumps(payload["features_json"], default=str)
    with get_conn() as conn:
        cur = conn.execute(
            f"INSERT INTO momentum_signals ({', '.join(cols)}) "
            f"VALUES ({', '.join('?' for _ in cols)})",
            tuple(payload.get(c) for c in cols),
        )
        return cur.lastrowid


def set_signal_outcome(signal_id: int, outcome: str, outcome_detail: dict) -> None:
    """Phase 2 (momentum.backtest) writes its simulated result back onto
    the signal it was computed from - outcome is the short label ("win" /
    "loss" / "scratch" / "no_data"), outcome_json the full detail
    (r_multiple, exit_reason, bars_held, ...)."""
    with get_conn() as conn:
        conn.execute(
            "UPDATE momentum_signals SET outcome = ?, outcome_json = ? WHERE id = ?",
            (outcome, json.dumps(outcome_detail, default=str), signal_id),
        )


def recent_signals(limit: int = 100) -> list[dict]:
    with get_conn() as conn:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM momentum_signals ORDER BY id DESC LIMIT ?", (limit,),
        )]


# --------------------------------------------------------------- bars meta ---
def upsert_bars_meta(symbol: str, timeframe: str, from_iso: str, to_iso: str, bar_count: int) -> None:
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO momentum_bars_meta (symbol, timeframe, from_iso, to_iso, bar_count, updated_iso) "
            "VALUES (?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(symbol, timeframe) DO UPDATE SET "
            "from_iso=excluded.from_iso, to_iso=excluded.to_iso, "
            "bar_count=excluded.bar_count, updated_iso=excluded.updated_iso",
            (symbol, timeframe, from_iso, to_iso, bar_count,
             datetime.now(ET).isoformat(timespec="seconds")),
        )


def bars_meta(symbol: str, timeframe: str) -> dict | None:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM momentum_bars_meta WHERE symbol = ? AND timeframe = ?",
            (symbol, timeframe),
        ).fetchone()
    return dict(row) if row else None


def trim_old_rows(retention_days: int = 120) -> None:
    """Housekeeping - keep the candidate snapshot table from growing without
    bound (a scan every 45s over a 2.5h window is ~200 cycles/day, each
    writing every symbol it saw). Signals are kept far longer; they are the
    dataset."""
    with get_conn() as conn:
        conn.execute(
            "DELETE FROM momentum_candidates "
            "WHERE scan_iso < datetime('now', ?)",
            (f"-{retention_days} days",),
        )


# --------------------------------------------------------- phase 3: positions ---
def open_position(pos: dict) -> int:
    cols = ("signal_id", "strategy", "symbol", "qty", "entry_price", "entry_time_iso",
            "trade_date", "stop_price", "initial_stop_price", "stop_order_id", "entry_order_id", "state")
    with get_conn() as conn:
        cur = conn.execute(
            f"INSERT INTO momentum_positions ({', '.join(cols)}) VALUES ({', '.join('?' for _ in cols)})",
            tuple(pos.get(c) for c in cols),
        )
        return cur.lastrowid


def get_open_positions(strategy: str | None = None) -> list[dict]:
    with get_conn() as conn:
        if strategy:
            rows = conn.execute(
                "SELECT * FROM momentum_positions WHERE state != 'closed' AND strategy = ? ORDER BY id",
                (strategy,),
            )
        else:
            rows = conn.execute("SELECT * FROM momentum_positions WHERE state != 'closed' ORDER BY id")
        return [dict(r) for r in rows]


def update_position(position_id: int, **fields) -> None:
    if not fields:
        return
    cols = ", ".join(f"{k} = ?" for k in fields)
    with get_conn() as conn:
        conn.execute(f"UPDATE momentum_positions SET {cols} WHERE id = ?", (*fields.values(), position_id))


def close_position(position_id: int, closed_at_iso: str, close_reason: str, realized_pnl_usd: float) -> None:
    update_position(position_id, state="closed", closed_at_iso=closed_at_iso,
                    close_reason=close_reason, realized_pnl_usd=realized_pnl_usd)


def record_position_trade(trade: dict) -> int:
    cols = ("position_id", "strategy", "symbol", "side", "qty", "price",
            "order_id", "reason", "timestamp_iso", "trade_date")
    with get_conn() as conn:
        cur = conn.execute(
            f"INSERT INTO momentum_trades ({', '.join(cols)}) VALUES ({', '.join('?' for _ in cols)})",
            tuple(trade.get(c) for c in cols),
        )
        return cur.lastrowid


def count_trades_today(strategy: str, trade_date: str, reason: str = "entry") -> int:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT count(*) FROM momentum_trades WHERE strategy = ? AND trade_date = ? AND reason = ?",
            (strategy, trade_date, reason),
        ).fetchone()
        return row[0] if row else 0


def realized_pnl_today(trade_date: str, strategy: str | None = None) -> float:
    with get_conn() as conn:
        if strategy:
            row = conn.execute(
                "SELECT COALESCE(SUM(realized_pnl_usd), 0) FROM momentum_positions "
                "WHERE trade_date = ? AND strategy = ? AND state = 'closed'",
                (trade_date, strategy),
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT COALESCE(SUM(realized_pnl_usd), 0) FROM momentum_positions "
                "WHERE trade_date = ? AND state = 'closed'",
                (trade_date,),
            ).fetchone()
        return float(row[0]) if row else 0.0


def recent_position_trades(limit: int = 100) -> list[dict]:
    with get_conn() as conn:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM momentum_trades ORDER BY id DESC LIMIT ?", (limit,),
        )]
