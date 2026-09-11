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
