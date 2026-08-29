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
import hashlib
import json
import os
import secrets
import signal
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta
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
    hold_overnight INTEGER NOT NULL DEFAULT 0,
    target_price REAL,
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
    universe TEXT NOT NULL DEFAULT ',default,',
    PRIMARY KEY (account_id, mode, symbol)
);

CREATE TABLE IF NOT EXISTS strategies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    key TEXT NOT NULL DEFAULT '',
    direction TEXT NOT NULL DEFAULT 'long',
    rules_json TEXT NOT NULL,
    risk_rating TEXT NOT NULL DEFAULT 'moderate',
    description TEXT NOT NULL DEFAULT '',
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

-- role is a SEPARATE axis from is_admin: is_admin gates only the shared
-- strategy-template catalog (create/edit/delete - see require_admin in
-- web/app.py); role gates general app access. 'full' (default - every
-- account before this column existed keeps behaving exactly as before)
-- can use every page/action for its own account_id, same as always.
-- 'viewer' is new: read-only access to the Backtest page's own data
-- (history, strategy report, calendar, a single result) and nothing else
-- anywhere in the app - see require_full_access in web/app.py for the
-- enforcement side.
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    is_admin INTEGER NOT NULL DEFAULT 0,
    role TEXT NOT NULL DEFAULT 'full'
);

-- ibkr_password is Fernet-encrypted (see src/secrets_store.py) — this
-- table never holds a plaintext password. One IBKR login per account
-- serves both its paper and live Gateway (IBKR ties a paper account to
-- its parent live account under the same credentials).
CREATE TABLE IF NOT EXISTS account_ibkr_credentials (
    account_id INTEGER PRIMARY KEY,
    ibkr_username TEXT NOT NULL,
    ibkr_password_encrypted BLOB NOT NULL,
    updated_at TEXT NOT NULL
);

-- Each account's own paper/live Gateway ports, assigned once (see
-- get_or_assign_gateway_ports) so multiple accounts' Gateway processes
-- never collide. The admin's row is seeded during migration with the
-- ports the single-account deployment already used, so its running
-- Gateway is never remapped.
CREATE TABLE IF NOT EXISTS account_gateway_ports (
    account_id INTEGER PRIMARY KEY,
    paper_port INTEGER NOT NULL,
    live_port INTEGER NOT NULL
);

-- One row per backtest run (possibly covering several strategies at once,
-- for side-by-side comparison — see params_json's strategy_ids). Runs as
-- an isolated subprocess (run_backtest.py, spawned by web/app.py) rather
-- than in-process, so a memory-heavy run can't take the dashboard down
-- with it; results_json is filled in once that subprocess finishes. pid
-- is that subprocess's OS pid, recorded so a dashboard restart mid-run
-- (which kills it along with the rest of dashboard.service's cgroup) can
-- be told apart from one still genuinely computing - see
-- fail_orphaned_backtests.
CREATE TABLE IF NOT EXISTS backtests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    params_json TEXT NOT NULL,
    results_json TEXT,
    error TEXT,
    pid INTEGER,
    execution_mode TEXT NOT NULL DEFAULT 'local',
    claimed_at TEXT,
    created_at TEXT NOT NULL,
    finished_at TEXT
);

-- One row per "Update backtest data" run from the dashboard's Backtest page
-- (see run_backtest_data_fetch.py, spawned as an isolated subprocess for the
-- exact same reason as backtests above - fetch_backtest_data.py needs its
-- own IBKR Gateway client connection and can run for a long time, neither
-- of which the always-on dashboard process should hold itself). No
-- execution_mode/claimed_at - this only ever runs locally on the server
-- (it needs the server's own IB Gateway), never on a remote worker.
CREATE TABLE IF NOT EXISTS backtest_data_fetches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    mode TEXT NOT NULL DEFAULT 'paper',
    summary_json TEXT,
    error TEXT,
    pid INTEGER,
    created_at TEXT NOT NULL,
    finished_at TEXT
);

-- One row per remote backtest worker token (see docs/worker.md and
-- backtest_worker.py). Only the SHA-256 hash is ever stored - the raw
-- token is shown to the user exactly once, at creation, the same pattern
-- API keys everywhere use, so a stolen db file alone can't be used to
-- impersonate a worker. last_seen_at is updated on every successful
-- claim, purely for the dashboard's own "worker: online/offline" display.
CREATE TABLE IF NOT EXISTS worker_tokens (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id INTEGER NOT NULL,
    token_hash TEXT NOT NULL UNIQUE,
    label TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    last_seen_at TEXT
);

-- One row per Touch & Turn resting limit order attempt (see
-- cycle.touch_turn_entry_scan/check_pending_touch_turn_orders and
-- src/touch_turn.py) - unlike every other strategy here, this one places
-- a REAL broker-side limit order that sits unfilled for up to
-- time_filter.entry_window_minutes rather than buying at market the
-- instant a signal passes, so its lifecycle needs its own tracking
-- separate from `positions` (which only ever holds already-FILLED
-- entries). placed_date (an ET calendar date) is part of the primary
-- key specifically so "has this symbol already had an order attempt
-- today" (this strategy's own max-one-trade-per-symbol-per-day rule) is
-- a simple existence check regardless of that attempt's outcome, and a
-- fresh attempt is naturally allowed again the next trading day. status
-- stays 'filled'/'cancelled'/'expired' after resolution (not deleted)
-- for the same audit-trail reasoning trades/decision_log are kept.
CREATE TABLE IF NOT EXISTS pending_orders (
    account_id INTEGER NOT NULL,
    mode TEXT NOT NULL DEFAULT 'paper',
    symbol TEXT NOT NULL,
    placed_date TEXT NOT NULL,
    side TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    broker_order_id INTEGER,
    limit_price REAL NOT NULL,
    target_price REAL NOT NULL,
    initial_stop REAL NOT NULL,
    qty INTEGER NOT NULL,
    placed_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    PRIMARY KEY (account_id, mode, symbol, placed_date)
);
"""

# Created after the mode-column migrations run below — an older DB's
# trades/decision_log tables won't have `mode` yet at CREATE TABLE time.
INDEXES_SCHEMA = """
CREATE INDEX IF NOT EXISTS idx_trades_account_mode_timestamp ON trades(account_id, mode, timestamp_iso);
CREATE INDEX IF NOT EXISTS idx_decision_log_account_mode_timestamp ON decision_log(account_id, mode, timestamp_iso);
CREATE INDEX IF NOT EXISTS idx_backtests_account_created ON backtests(account_id, created_at);
CREATE INDEX IF NOT EXISTS idx_backtest_data_fetches_account_created ON backtest_data_fetches(account_id, created_at);
-- Blank ('') is the "no key set yet" default and can repeat across many
-- rows - only an actually-chosen key (e.g. "L1", "S1") needs to be unique.
CREATE UNIQUE INDEX IF NOT EXISTS idx_strategies_key ON strategies(key) WHERE key != '';
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
        # Exact match of rules.json's long default, except I2: instead of
        # requiring a new high-of-day, it requires 5m-close RSI(14) > 50
        # (see _compute_rsi / _evaluate_entry_filters in cycle.py). Every
        # other filter/exit/risk number is identical to the default on
        # purpose, so risk_rating mirrors the default's 'conservative'.
        "Long Breakout RSI Filter",
        {
            "strategy_name": "Long Breakout RSI Filter",
            "direction": "long_only",
            "trade_timeframe": "5m",
            "universe_filters": {"index": "S&P 500", "min_price_usd": 3.0},
            "daily_filters": {
                "D1_above_prior_day_high": True,
                "D2_prior_close_above_sma200": True,
                "D3_min_gap_pct_from_prior_close": 3.0,
            },
            "intraday_filters": {
                "I1_above_premarket_high": True,
                "I2_rsi_above": 50,
                "I2_rsi_period": 14,
                "I3_rvol_min": 2.0,
                "I3_rvol_lookback_days": 14,
            },
            "time_filter": {"earliest_entry_et": "10:05", "latest_entry_et": "15:30", "force_close_et": "15:51"},
            "exit": {
                "initial_stop_rule": "lod_minus_1pct",
                "breakeven_trigger_R": 1.0,
                "post_breakeven_trail": "swing_low_5m_2_2",
            },
            "risk": {
                "max_risk_per_trade_pct": 1.0,
                "max_position_size_pct_of_portfolio": 10,
                "max_concurrent_positions": 5,
            },
        },
        "conservative",
        "long",
        "## מה זה עושה\n"
        "זהה לחלוטין ל-Long Breakout Conservative בכל הפרמטרים המספריים - ההבדל היחיד הוא תנאי הכניסה I2.\n\n"
        "## ההבדל מהגרסה הבסיסית\n"
        "במקום לדרוש שיא חדש תוך-יומי (I2 בגרסה הבסיסית), האסטרטגיה הזו בודקת RSI(14) על נרות 5 דקות "
        "מעל 50 - מומנטום חיובי לפי אינדיקטור טכני, ולא רק מחיר שיא. זה יכול לתפוס כניסות מעט שונות: "
        "מניה שעדיין לא עשתה שיא חדש תוך-יומי אבל כבר מראה מומנטום חיובי לפי RSI.\n\n"
        "## תנאי כניסה\n"
        "D1: המחיר מעל השיא של אתמול\n"
        "D2: סגירת אתמול מעל הממוצע הנע 200 יום\n"
        "D3: פער של לפחות 3% מעלה\n"
        "I1: מעל השיא של המסחר המוקדם\n"
        "I2: RSI(14) על נרות 5 דקות מעל 50 (במקום שיא תוך-יומי)\n"
        "I3: RVOL פי 2 לפחות מהממוצע\n\n"
        "## יציאה וניהול פוזיציה\n"
        "זהה לגמרי לגרסה הבסיסית: סטופ 1% מתחת לשפל היום, Breakeven ב-1R, "
        "טריילינג סטופ לפי שפל נר 5 דקות.\n\n"
        "## פרופיל סיכון\n"
        "דירוג: conservative (זהה לגרסה הבסיסית)\n"
        "סיכון לעסקה: 1% | גודל פוזיציה מקס': 10% | פוזיציות בו-זמנית: עד 5",
    ),
    (
        "Long Breakout Aggressive",
        {
            "strategy_name": "Long Breakout Aggressive",
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
        "## מה זה עושה\n"
        "גרסה רופפת ומסוכנת יותר של אסטרטגיית ה-Long הבסיסית: סף כניסה נמוך יותר, כלומר יותר עסקאות "
        "פוטנציאליות - אבל גם יותר 'רעש' וסיכוי גבוה יותר לאיתותי שווא.\n\n"
        "## ההבדל מהגרסה הבסיסית\n"
        "פער מינימלי (D3) נמוך יותר: 1.5% במקום 3%\n"
        "RVOL מינימלי (I3) נמוך יותר: x1.2 במקום x2.0, על חלון של 10 ימים במקום 14\n"
        "יעד Breakeven גבוה יותר: ב-2R (במקום 1R)\n"
        "סיכון גבוה משמעותית לעסקה בודדת\n\n"
        "## תנאי כניסה\n"
        "D1: המחיר מעל השיא של אתמול\n"
        "D2: סגירת אתמול מעל הממוצע הנע 200 יום\n"
        "D3: פער של לפחות 1.5% מעלה\n"
        "I1: מעל השיא של המסחר המוקדם\n"
        "I2: שיא חדש תוך-יומי\n"
        "I3: RVOL פי 1.2 לפחות (ממוצע 10 ימים)\n\n"
        "## יציאה וניהול פוזיציה\n"
        "סטופ התחלתי: 1% מתחת לשפל היום\n"
        "Breakeven: ב-2R\n"
        "טריילינג סטופ: לפי שפל נר 5 דקות\n\n"
        "## פרופיל סיכון\n"
        "דירוג: aggressive - הפעלתה דורשת הקלדת אישור, כי היא חלה על LIVE מיידית\n"
        "סיכון לעסקה: 2.5% מהתיק\n"
        "גודל פוזיציה מקסימלי: 20% מהתיק\n"
        "פוזיציות בו-זמניות: עד 8",
    ),
    (
        # Exact mirror of rules.json's long default, flipped for breakdowns
        # instead of breakouts: below the prior day's low/SMA200 instead of
        # above, gap DOWN instead of up, stop above the high of day instead
        # of below the low, profit-taking/breakeven/trailing all trigger on
        # the price falling instead of rising. Same numeric thresholds as
        # the long default, so it's the "same conservatism," mirrored.
        "Short Breakdown Conservative",
        {
            "strategy_name": "Short Breakdown Conservative",
            "direction": "short_only",
            "trade_timeframe": "5m",
            "es_vwap_filter": True,
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
        "## מה זה עושה\n"
        "מראה מדויק והפוך-כיוון של Long Breakout Conservative: מוכרת בשורט מניות שנשברות מטה מתוך "
        "אזור צבירה, עם אישור פער בוקר כלפי מטה ונפח מסחר גבוה.\n\n"
        "## תנאי כניסה\n"
        "D1: המחיר מתחת לשפל של אתמול\n"
        "D2: סגירת אתמול מתחת לממוצע הנע 200 יום\n"
        "D3: פער של לפחות 3% מטה מסגירת אתמול\n"
        "I1: המחיר מתחת לשפל המסחר המוקדם\n"
        "I2: שפל חדש תוך-יומי\n"
        "I3: RVOL פי 2 לפחות מהממוצע\n\n"
        "## יציאה וניהול פוזיציה\n"
        "סטופ התחלתי: 1% מעל השיא של היום (לא מתחת לשפל - זו פוזיציית שורט)\n"
        "Breakeven: ב-1R\n"
        "טריילינג סטופ: לפי שיא נר 5 דקות\n\n"
        "## סינון כיוון שוק (ES VWAP)\n"
        "כניסה מתאפשרת רק כש-ES (חוזי E-mini S&P 500) נסחר **מתחת** ל-VWAP היומי שלו - \"Market first, "
        "setup second\": גם אם כל תנאי הכניסה מתקיימים, עסקה שסותרת את כיוון השוק הרחב נדחית. "
        "כבוי כברירת מחדל (דורש הרשאת נתוני CME futures + הפעלה מפורשת בעמוד Bot) - כל עוד הוא כבוי, "
        "או שאין גישה לנתוני ES, האסטרטגיה נשארת ללא סינון (fail-open), לא נחסמת.\n\n"
        "## פרופיל סיכון\n"
        "דירוג: conservative\n"
        "סיכון לעסקה: 1% | גודל פוזיציה מקס': 10% | פוזיציות בו-זמנית: עד 5\n\n"
        "## אזהרת סיכון\n"
        "בניגוד לפוזיציית Long, בפוזיציית Short אין תקרה תיאורטית להפסד - מחיר המניה יכול לעלות ללא "
        "הגבלה, וה-stop עלול 'לקפוץ מעל' (gap) במקרה של short squeeze.",
    ),
    (
        # Catches a parabolic blow-off top instead of an already-confirmed
        # downtrend: "Short Breakdown Conservative"'s D2 (prior close below
        # SMA200) can't fire near the top of a huge multi-week run-up, since
        # price is still far ABOVE the 200-day SMA at that point — by the
        # time D2 passes, most of the drop already happened. This preset
        # swaps D2 for an extension filter (prior close far ABOVE the
        # 50-day SMA = "overextended", the mirror-opposite condition) and
        # swaps I2 for an RSI-rollover confirmation (RSI(14) dropping below
        # 50 intraday = momentum has actually flipped), instead of waiting
        # for a new low-of-day. D1/D3/I1/I3 are unchanged from the
        # conservative short mirror. Rated aggressive: it's a counter-trend
        # reversal entry, not a continuation of an already-established
        # trend, so it's inherently more prone to false signals.
        "Short Parabolic Reversal",
        {
            "strategy_name": "Short Parabolic Reversal",
            "direction": "short_only",
            "trade_timeframe": "5m",
            "universe_filters": {"index": "S&P 500", "min_price_usd": 3.0},
            "daily_filters": {
                "D1_below_prior_day_low": True,
                "D2_prior_close_pct_above_sma50_min": 40.0,
                "D3_min_gap_pct_down_from_prior_close": 3.0,
            },
            "intraday_filters": {
                "I1_below_premarket_low": True,
                "I2_rsi_below": 50,
                "I2_rsi_period": 14,
                "I3_rvol_min": 2.0,
                "I3_rvol_lookback_days": 14,
            },
            "time_filter": {"earliest_entry_et": "09:35", "latest_entry_et": "15:30", "force_close_et": "15:51"},
            "exit": {
                "initial_stop_rule": "hod_plus_1pct",
                "breakeven_trigger_R": 1.0,
                "post_breakeven_trail": "swing_high_5m_2_2",
            },
            "risk": {
                "max_risk_per_trade_pct": 1.0,
                "max_position_size_pct_of_portfolio": 10,
                "max_concurrent_positions": 5,
            },
        },
        "aggressive",
        "short",
        "## מה זה עושה\n"
        "נועדה לתפוס בדיוק את המקרה שבו Short Breakdown Conservative מפספסת: מניה שעלתה פרבולית "
        "ומתחילה להתהפך בשיא, כשהיא עדיין רחוקה מעל ה-SMA200 (כך שהתנאי השמרני D2 לא היה מתמלא עד "
        "שהמניה כבר נופלת משמעותית).\n\n"
        "## ההבדל מהגרסה השמרנית\n"
        "D2 מוחלף: במקום 'סגירת אתמול מתחת ל-SMA200', דורשת שסגירת אתמול תהיה לפחות 40% מעל ה-SMA50 "
        "(סימן למתיחת יתר פרבולית)\n"
        "I2 מוחלף: במקום שפל חדש תוך-יומי, דורשת RSI(14) מתחת ל-50 תוך-יומי (אישור שהמומנטום כבר "
        "התהפך בפועל)\n"
        "זמן כניסה מוקדם יותר: 9:35 במקום 10:05, כדי לתפוס את ההיפוך מוקדם ככל האפשר\n"
        "D1, D3, I1, I3 זהים לגרסה השמרנית\n\n"
        "## תנאי כניסה\n"
        "D1: המחיר מתחת לשפל של אתמול\n"
        "D2: סגירת אתמול לפחות 40% מעל ה-SMA50\n"
        "D3: פער של לפחות 3% מטה\n"
        "I1: מתחת לשפל המסחר המוקדם\n"
        "I2: RSI(14) מתחת ל-50 תוך-יומי\n"
        "I3: RVOL פי 2 לפחות מהממוצע\n\n"
        "## יציאה, ניהול פוזיציה ופרופיל סיכון\n"
        "זהה ל-Short Breakdown Conservative: סטופ 1% מעל שיא היום, Breakeven "
        "ב-1R, טריילינג לפי שיא 5 דקות. סיכון 1% לעסקה, מקס' 10% לפוזיציה, עד 5 פוזיציות.\n\n"
        "## אזהרת סיכון\n"
        "דירוג aggressive - זו כניסה נגד המגמה שהתקיימה עד כה (mean-reversion), ולא המשך מגמה קיימת, "
        "ולכן חשופה יותר לאיתותי שווא. הפעלתה דורשת הקלדת אישור כי היא חלה על LIVE מיידית.",
    ),
    (
        # Identical numbers to the default Long Breakout Conservative
        # (D1-D3, I1-I3, exit, risk) — the only difference is the universe
        # this strategy is even allowed to consider: instead of the S&P
        # 500, it's restricted (via universe_filters.custom_universe, see
        # cycle._strategy_universe / db.get_watchlist's universe param) to
        # a fundamentals-screened slice of the NASDAQ Composite (IXIC) —
        # market cap > $1B, beta > 1.2, analyst consensus Buy or better —
        # built by build_custom_universe.py and cached in
        # data/universes/ixic_large_beta_buy.json. That cache has to exist
        # and be fresh (see src/custom_universes.py's max_staleness_days)
        # or this strategy simply finds no candidates.
        "Long Breakout NASDAQ Beta",
        {
            "strategy_name": "Long Breakout NASDAQ Beta",
            "direction": "long_only",
            "trade_timeframe": "5m",
            "universe_filters": {
                "index": "NASDAQ Composite (IXIC)",
                "min_price_usd": 3.0,
                "custom_universe": "ixic_large_beta_buy",
                "min_market_cap_usd": 1_000_000_000,
                "min_beta": 1.2,
                "min_analyst_rating": "buy",
            },
            "daily_filters": {
                "D1_above_prior_day_high": True,
                "D2_prior_close_above_sma200": True,
                "D3_min_gap_pct_from_prior_close": 3.0,
            },
            "intraday_filters": {
                "I1_above_premarket_high": True,
                "I2_above_today_hod": True,
                "I3_rvol_min": 2.0,
                "I3_rvol_lookback_days": 14,
            },
            "time_filter": {"earliest_entry_et": "10:05", "latest_entry_et": "15:30", "force_close_et": "15:51"},
            "exit": {
                "initial_stop_rule": "lod_minus_1pct",
                "breakeven_trigger_R": 1.0,
                "post_breakeven_trail": "swing_low_5m_2_2",
            },
            "risk": {
                "max_risk_per_trade_pct": 1.0,
                "max_position_size_pct_of_portfolio": 10,
                "max_concurrent_positions": 5,
            },
        },
        "conservative",
        "long",
        "## מה זה עושה\n"
        "זהה במספרים לחלוטין ל-Long Breakout Conservative - ההבדל היחיד הוא היקום שהיא בכלל מסתכלת עליו.\n\n"
        "## ההבדל מהגרסה הבסיסית\n"
        "במקום לסרוק את מדד ה-S&P 500, האסטרטגיה מוגבלת מראש לרשימת מניות NASDAQ (מדד IXIC) שעברו "
        "סינון פונדמנטלי:\n"
        "שווי שוק מעל 1 מיליארד דולר\n"
        "ביטא מעל 1.2 (תנודתיות גבוהה יותר מהשוק)\n"
        "דירוג אנליסטים ממוצע Buy ומעלה\n\n"
        "## איך הרשימה נבנית\n"
        "הרשימה הזו לא מחושבת חי בכל מחזור מסחר כמו D1-I3 - היא נבנית מראש בנפרד "
        "(build_custom_universe.py), שדורש גישה חיה לנתונים פונדמנטליים, ומתעדכנת אוטומטית פעם "
        "בשבוע. אם הרשימה השמורה בשרת ישנה או לא קיימת, האסטרטגיה פשוט לא תמצא מועמדים באותו יום - "
        "זו לא תקלה, זו הגנה מפני מסחר על נתונים מיושנים.\n\n"
        "## תנאי כניסה\n"
        "זהה לגמרי לגרסה הבסיסית: D1 מעל שיא אתמול, D2 מעל SMA200, D3 פער 3%+, I1 מעל שיא פרה-מרקט, "
        "I2 שיא תוך-יומי, I3 RVOL x2.0.\n\n"
        "## יציאה, ניהול פוזיציה ופרופיל סיכון\n"
        "זהה לגמרי לגרסה הבסיסית: סטופ 1% מתחת לשפל היום, Breakeven ב-1R, "
        "טריילינג לפי שפל 5 דקות. דירוג conservative, סיכון 1% לעסקה, מקס' 10% לפוזיציה, עד 5 פוזיציות.",
    ),
    (
        # Experimental "fade" pair, born from a real conversation: a
        # single day's backtest showed Long Breakout Conservative and
        # Short Breakdown Conservative BOTH losing on every trade, and the
        # question was "what if we took the opposite side of the exact
        # same signal instead." That's a genuinely different, unvalidated
        # thesis (bet against continuation instead of on it) - NOT a
        # statistically sound conclusion from one bad day (9 trades) for
        # two trend-following strategies, which is explicitly called out
        # in both descriptions below and is exactly why risk_rating is
        # 'aggressive' (requires the typed-confirmation speed bump before
        # either can go live, on top of extensive backtesting the
        # descriptions insist on first).
        #
        # signal_side (new top-level rules field, see cycle._evaluate_
        # filters_from_bars) is what makes this possible without
        # duplicating or forking the shared filter-evaluation engine: D1-
        # D3/I1-I3 here are LITERALLY Long Breakout Conservative's own
        # long-style definitions, copied verbatim (same signal, unchanged)
        # - only `direction` (short) and the exit block's stop/trail
        # mechanics (hod_plus_1pct / swing_high, a short's own, not
        # Long Breakout's) describe the actual trade being placed.
        "Long Breakout Fade (Short)",
        {
            "strategy_name": "Long Breakout Fade (Short)",
            "direction": "short_only",
            "trade_timeframe": "5m",
            "universe_filters": {"index": "S&P 500", "min_price_usd": 3.0},
            "signal_side": "long",
            "daily_filters": {
                "D1_above_prior_day_high": True,
                "D2_prior_close_above_sma200": True,
                "D3_min_gap_pct_from_prior_close": 3.0,
            },
            "intraday_filters": {
                "I1_above_premarket_high": True,
                "I2_above_today_hod": True,
                "I3_rvol_min": 2.0,
                "I3_rvol_lookback_days": 14,
            },
            "time_filter": {"earliest_entry_et": "10:05", "latest_entry_et": "15:30", "force_close_et": "15:51"},
            "exit": {
                "initial_stop_rule": "hod_plus_1pct",
                "breakeven_trigger_R": 1.0,
                "post_breakeven_trail": "swing_high_5m_2_2",
            },
            "risk": {
                "max_risk_per_trade_pct": 1.0,
                "max_position_size_pct_of_portfolio": 10,
                "max_concurrent_positions": 5,
            },
        },
        "aggressive",
        "short",
        "## מה זה עושה\n"
        "אסטרטגיית מחקר ניסיונית: מזהה בדיוק את אותו איתות של Long Breakout Conservative (פריצה "
        "כלפי מעלה עם נפח גבוה), אבל **מוכרת בשורט** נגד הפריצה במקום לקנות איתה - הימור שהפריצה "
        "תיכשל ותתהפך, לא שהיא תמשיך.\n\n"
        "## תנאי כניסה (זהים לחלוטין ל-Long Breakout Conservative)\n"
        "D1: המחיר מעל השיא של אתמול\n"
        "D2: סגירת אתמול מעל הממוצע הנע 200 יום\n"
        "D3: פער של לפחות 3% מעלה מסגירת אתמול\n"
        "I1: המחיר מעל שיא המסחר המוקדם\n"
        "I2: שיא חדש תוך-יומי\n"
        "I3: RVOL פי 2 לפחות מהממוצע\n\n"
        "## יציאה וניהול פוזיציה (מותאם לפוזיציית שורט, לא ללונג)\n"
        "סטופ התחלתי: 1% מעל השיא של היום (לא מתחת לשפל - זו פוזיציית שורט)\n"
        "Breakeven: ב-1R | טריילינג סטופ: לפי שיא נר 5 דקות\n\n"
        "## פרופיל סיכון\n"
        "דירוג: aggressive | סיכון לעסקה: 1% | גודל פוזיציה מקס': 10% | פוזיציות בו-זמנית: עד 5\n\n"
        "## אזהרת סיכון - קרא לפני שאתה שוקל להפעיל\n"
        "זו לא אסטרטגיה שאומתה - היא נולדה משאלת מחקר על סמך יום מסחר בודד שבו Long Breakout "
        "Conservative הפסידה בכל עסקה. הפיכת כיוון על סמך יום אחד (9 עסקאות) היא בדיוק סוג הטעות "
        "הסטטיסטית ש-overfitting נראה כמוה - זה לא מוכיח יתרון אמיתי וחוזר בשוק. אל תפעיל LIVE לפני "
        "בדיקה מקיפה על פני תקופה ארוכה בהרבה (שבועות-חודשים, מאות עסקאות). בנוסף, בפוזיציית Short "
        "אין תקרה תיאורטית להפסד.",
    ),
    (
        # Mirror-opposite of the fade above: Short Breakdown Conservative's
        # own short-style D1-D3/I1-I3, unchanged, but traded LONG (betting
        # a breakdown reverses instead of continues). Same signal_side
        # mechanism, same 'aggressive' rating for the same reason - see the
        # comment on "Long Breakout Fade (Short)" above.
        "Short Breakdown Fade (Long)",
        {
            "strategy_name": "Short Breakdown Fade (Long)",
            "direction": "long_only",
            "trade_timeframe": "5m",
            "universe_filters": {"index": "S&P 500", "min_price_usd": 3.0},
            "signal_side": "short",
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
                "initial_stop_rule": "lod_minus_1pct",
                "breakeven_trigger_R": 1.0,
                "post_breakeven_trail": "swing_low_5m_2_2",
            },
            "risk": {
                "max_risk_per_trade_pct": 1.0,
                "max_position_size_pct_of_portfolio": 10,
                "max_concurrent_positions": 5,
            },
        },
        "aggressive",
        "long",
        "## מה זה עושה\n"
        "אסטרטגיית מחקר ניסיונית: מזהה בדיוק את אותו איתות של Short Breakdown Conservative (שבירה "
        "כלפי מטה עם נפח גבוה), אבל **קונה בלונג** נגד השבירה במקום למכור בשורט איתה - הימור שהשבירה "
        "תיכשל ותתהפך כלפי מעלה, לא שהיא תמשיך.\n\n"
        "## תנאי כניסה (זהים לחלוטין ל-Short Breakdown Conservative)\n"
        "D1: המחיר מתחת לשפל של אתמול\n"
        "D2: סגירת אתמול מתחת לממוצע הנע 200 יום\n"
        "D3: פער של לפחות 3% מטה מסגירת אתמול\n"
        "I1: המחיר מתחת לשפל המסחר המוקדם\n"
        "I2: שפל חדש תוך-יומי\n"
        "I3: RVOL פי 2 לפחות מהממוצע\n\n"
        "## יציאה וניהול פוזיציה (מותאם לפוזיציית לונג, לא לשורט)\n"
        "סטופ התחלתי: 1% מתחת לשפל של היום (לא מעל השיא - זו פוזיציית לונג)\n"
        "Breakeven: ב-1R | טריילינג סטופ: לפי שפל נר 5 דקות\n\n"
        "## פרופיל סיכון\n"
        "דירוג: aggressive | סיכון לעסקה: 1% | גודל פוזיציה מקס': 10% | פוזיציות בו-זמנית: עד 5\n\n"
        "## אזהרת סיכון - קרא לפני שאתה שוקל להפעיל\n"
        "זו לא אסטרטגיה שאומתה - היא נולדה משאלת מחקר על סמך יום מסחר בודד שבו Short Breakdown "
        "Conservative הפסידה בכל עסקה. הפיכת כיוון על סמך יום אחד (9 עסקאות) היא בדיוק סוג הטעות "
        "הסטטיסטית ש-overfitting נראה כמוה - זה לא מוכיח יתרון אמיתי וחוזר בשוק. אל תפעיל LIVE לפני "
        "בדיקה מקיפה על פני תקופה ארוכה בהרבה (שבועות-חודשים, מאות עסקאות).",
    ),
    (
        # Opening Range Breakout - a genuinely different engine from every
        # strategy above: no daily_filters/D1-D3 at all (no "yesterday"
        # bias - see docs/orb_strategy_spec.md), and dispatched to
        # src/orb.py's own evaluate_orb_entry instead of cycle._evaluate_
        # filters_from_bars entirely (see cycle.entry_scan's "opening_range"
        # in rules check). Two entry models (breakout, retest) off a
        # 15-minute opening range confirmed on a 5-minute candle close -
        # the video's own third step ("drop to 1-minute for entries") is
        # evaluated on 5-minute bars instead, a deliberate compromise since
        # the backtest data pipeline (fetch_backtest_data.py/backtest_
        # engine.py) only caches 5-minute bars; see the spec doc for what
        # that costs in entry precision. Exit is a fixed 1:2 R:R target,
        # not the breakeven+trailing mechanism every other strategy here
        # uses (exit.management_style: "fixed_target_no_trail" - see
        # cycle.manage_position's dedicated branch).
        "ORB Long (Opening Range Breakout)",
        {
            "strategy_name": "ORB Long (Opening Range Breakout)",
            "direction": "long_only",
            "es_vwap_filter": True,
            "opening_range": {
                "or_timeframe": "15m",
                "confirm_timeframe": "5m",
                "entry_timeframe": "5m",
                "session": "new_york",
                "session_open_et": "09:30",
            },
            "universe_filters": {
                "index": "S&P 500",
                "min_price_usd": 3.0,
                "custom_universe": "sp500_marketcap_1b",
            },
            "volatility_filters": {
                "V1_rvol_min": 2.0,
                "V1_rvol_lookback_days": 14,
                "V2_atr_period": 14,
                "V2_atr_pct_tiers": [
                    {"price_min": 3.0, "price_max": 20.0, "atr_pct_min": 4.0},
                    {"price_min": 20.0, "price_max": 50.0, "atr_pct_min": 3.0},
                    {"price_min": 50.0, "price_max": 100.0, "atr_pct_min": 2.0},
                    {"price_min": 100.0, "price_max": None, "atr_pct_min": 1.5},
                ],
            },
            "entry_models": {
                "breakout": {"enabled": True, "target_rr": 2.0},
                "retest": {"enabled": True, "target_rr": 2.0},
            },
            "time_filter": {"earliest_entry_et": "09:50", "latest_entry_et": "11:30", "force_close_et": "15:51"},
            "exit": {"management_style": "fixed_target_no_trail"},
            "risk": {
                "max_risk_per_trade_pct": 1.0,
                "max_position_size_pct_of_portfolio": 10,
                "max_concurrent_positions": 5,
                "min_stop_distance_pct": 0.25,
            },
        },
        "aggressive",
        "long",
        "## מה זה עושה\n"
        "אסטרטגיה מבוססת Opening Range Breakout (ORB): לא בודקת דעה מקדימה מהיום הקודם (אין "
        "daily_filters בכלל) - כל יום מתחיל מאפס. סוחרת רק את הנר הראשון של פתיחת המסחר בניו יורק "
        "(9:30 ET), מחכה לאישור פריצה, ואז מחפשת כניסה להמשך התנועה. מקור: תמלול סרטון YouTube "
        "(bITIVwysCzM) - ראו docs/orb_strategy_spec.md למפרט המלא ולתהליך ההגדרה.\n\n"
        "## יקום\n"
        "S&P 500 בלבד, מסונן מראש למניות עם Market Cap מעל $1B (custom_universe: "
        "sp500_marketcap_1b, נבנה על ידי build_custom_universe.py - כמו Long Breakout NASDAQ Beta) "
        "ומחיר מינימלי $3.\n\n"
        "## מנגנון ה-Opening Range\n"
        "1. סימון High/Low של 3 נרות 5 דקות ראשונים מ-9:30 ET (= 'נר' 15 דקות) - זה ה-Opening Range.\n"
        "2. אישור: נר 5 דקות שנסגר מעל ה-OR High.\n"
        "3. כניסה: על אותה מסגרת 5 דקות (**לא 1 דקה כמו בסרטון המקורי** - פשרה כי אין נתוני 1 דקה "
        "בתשתית ה-backtest הקיימת, ראו הערה בקובץ המפרט).\n\n"
        "## פילטרים לפני כניסה\n"
        "RVOL מעל 2.0 (חלון 14 ימים) ו-ATR% (יחסי למחיר, לא אבסולוטי) לפי מדרגת מחיר: "
        "$3-20 מעל 4%, $20-50 מעל 3%, $50-100 מעל 2%, מעל $100 מעל 1.5%.\n\n"
        "## מודלי כניסה (2 מתוך 3 בסרטון המקורי - Reversal הוסר מהיקף)\n"
        "**Breakout**: רק על נר האישור עצמו, ורק אם יש 'gap' (displacement) בינו לנר הקודם - כניסה "
        "בסגירת הנר, סטופ בשפל/שיא אותו נר.\n"
        "**Retest**: נר כלשהו אחרי האישור שנוגע בחזרה ברמת ה-OR ונסגר בחזרה בכיוון הפריצה - כניסה "
        "בסגירת הנר, סטופ בשפל/שיא אותו נר.\n\n"
        "## יציאה וניהול פוזיציה (שונה מכל שאר האסטרטגיות בפרויקט)\n"
        "אין breakeven flip ואין טריילינג סטופ - הסטופ ההתחלתי (משלב הכניסה) נשאר קבוע כל הפוזיציה. "
        "יעד קבוע R:R = 1:2: יציאה מלאה ביעד או בסטופ, מה שמגיע קודם.\n\n"
        "## חלון כניסות\n"
        "09:50-11:30 ET בלבד (השעתיים הראשונות של המסחר, כפי שממליץ הסרטון) - force close רגיל "
        "ב-15:51 ET לכל פוזיציה שעדיין פתוחה.\n\n"
        "## סינון כיוון שוק (ES VWAP)\n"
        "כניסה מתאפשרת רק כש-ES (חוזי E-mini S&P 500) נסחר **מעל** ל-VWAP היומי שלו - \"Market "
        "first, setup second\": גם אם כל תנאי הכניסה מתקיימים, עסקה שסותרת את כיוון השוק הרחב "
        "נדחית. כבוי כברירת מחדל (דורש הרשאת נתוני CME futures + הפעלה מפורשת בעמוד Bot) - כל עוד "
        "הוא כבוי, או שאין גישה לנתוני ES, האסטרטגיה נשארת ללא סינון (fail-open), לא נחסמת.\n\n"
        "## רצפת מרחק סטופ מינימלית\n"
        "הסטופ הטכני (שפל/שיא נר האישור) לפעמים קרוב מדי לכניסה (נר כמעט ללא צל) - זה מייצר "
        "גודל פוזיציה מנופח מדי וערכי R מנופחים מדי שלא משקפים סיכון אמיתי (ראו חקירה בהיסטוריית "
        "השיחה). לכן מרחק הסטופ מהכניסה מוגבל מלמטה ל-0.25% ממחיר הכניסה "
        "(min_stop_distance_pct) - אם הסטופ הטכני קרוב יותר, הוא מורחב לרצפה הזו; סטופ טכני "
        "שכבר רחוק מספיק לא משתנה כלל.\n\n"
        "## פרופיל סיכון\n"
        "דירוג: aggressive - אסטרטגיה חדשה שלא נבדקה (לא backtest, לא paper trading) - הפעלתה "
        "דורשת הקלדת אישור כי היא חלה על LIVE מיידית. סיכון לעסקה: 1% | גודל פוזיציה מקס': 10% | "
        "פוזיציות בו-זמנית: עד 5\n\n"
        "## אזהרת סיכון - קרא לפני שאתה שוקל להפעיל\n"
        "זו לא אסטרטגיה שאומתה בשום צורה - יש להריץ backtest מקיף (שבועות-חודשים, מאות עסקאות) "
        "ולבחון paper trading ממושך לפני כל שיקול להעלות ל-LIVE. שימו לב גם לפשרת 1 דקה→5 דקות "
        "בכניסה: הדיוק בפועל נמוך יותר ממה שהסרטון המקורי מתאר, וה-R:R בפועל עלול להיות שונה מהמתוכנן.",
    ),
    (
        # Exact mirror of ORB Long - see its own comment above for the full
        # engine explanation, not repeated here.
        "ORB Short (Opening Range Breakdown)",
        {
            "strategy_name": "ORB Short (Opening Range Breakdown)",
            "direction": "short_only",
            "es_vwap_filter": True,
            "opening_range": {
                "or_timeframe": "15m",
                "confirm_timeframe": "5m",
                "entry_timeframe": "5m",
                "session": "new_york",
                "session_open_et": "09:30",
            },
            "universe_filters": {
                "index": "S&P 500",
                "min_price_usd": 3.0,
                "custom_universe": "sp500_marketcap_1b",
            },
            "volatility_filters": {
                "V1_rvol_min": 2.0,
                "V1_rvol_lookback_days": 14,
                "V2_atr_period": 14,
                "V2_atr_pct_tiers": [
                    {"price_min": 3.0, "price_max": 20.0, "atr_pct_min": 4.0},
                    {"price_min": 20.0, "price_max": 50.0, "atr_pct_min": 3.0},
                    {"price_min": 50.0, "price_max": 100.0, "atr_pct_min": 2.0},
                    {"price_min": 100.0, "price_max": None, "atr_pct_min": 1.5},
                ],
            },
            "entry_models": {
                "breakout": {"enabled": True, "target_rr": 2.0},
                "retest": {"enabled": True, "target_rr": 2.0},
            },
            "time_filter": {"earliest_entry_et": "09:50", "latest_entry_et": "11:30", "force_close_et": "15:51"},
            "exit": {"management_style": "fixed_target_no_trail"},
            "risk": {
                "max_risk_per_trade_pct": 1.0,
                "max_position_size_pct_of_portfolio": 10,
                "max_concurrent_positions": 5,
                "min_stop_distance_pct": 0.25,
            },
        },
        "aggressive",
        "short",
        "## מה זה עושה\n"
        "מראה הפוכה מדויקת של ORB Long (Opening Range Breakout) - ראו את התיאור המלא שם. כאן: "
        "אישור על נר 5 דקות שנסגר מתחת ל-OR Low, breakout/retest בכיוון ירידה, סטופ מעל שפל/שיא "
        "הנר הרלוונטי, יעד קבוע R:R 1:2 כלפי מטה.\n\n"
        "## יקום, פילטרים, חלון כניסות\n"
        "זהה לחלוטין ל-ORB Long: S&P 500 עם Market Cap מעל $1B, מחיר מינימלי $3, RVOL מעל 2.0, "
        "ATR% מדורג לפי מחיר, חלון כניסות 09:50-11:30 ET.\n\n"
        "## סינון כיוון שוק (ES VWAP)\n"
        "כניסה מתאפשרת רק כש-ES (חוזי E-mini S&P 500) נסחר **מתחת** ל-VWAP היומי שלו - \"Market "
        "first, setup second\": גם אם כל תנאי הכניסה מתקיימים, עסקה שסותרת את כיוון השוק הרחב "
        "נדחית. כבוי כברירת מחדל (דורש הרשאת נתוני CME futures + הפעלה מפורשת בעמוד Bot) - כל עוד "
        "הוא כבוי, או שאין גישה לנתוני ES, האסטרטגיה נשארת ללא סינון (fail-open), לא נחסמת.\n\n"
        "## רצפת מרחק סטופ מינימלית\n"
        "הסטופ הטכני (שפל/שיא נר האישור) לפעמים קרוב מדי לכניסה (נר כמעט ללא צל) - זה מייצר "
        "גודל פוזיציה מנופח מדי וערכי R מנופחים מדי שלא משקפים סיכון אמיתי (ראו חקירה בהיסטוריית "
        "השיחה). לכן מרחק הסטופ מהכניסה מוגבל מלמטה ל-0.25% ממחיר הכניסה "
        "(min_stop_distance_pct) - אם הסטופ הטכני קרוב יותר, הוא מורחב לרצפה הזו; סטופ טכני "
        "שכבר רחוק מספיק לא משתנה כלל.\n\n"
        "## פרופיל סיכון\n"
        "דירוג: aggressive - אסטרטגיה חדשה שלא נבדקה. סיכון לעסקה: 1% | גודל פוזיציה מקס': 10% | "
        "פוזיציות בו-זמנית: עד 5\n\n"
        "## אזהרת סיכון - קרא לפני שאתה שוקל להפעיל\n"
        "בנוסף לכל אזהרות ORB Long (לא נבדקה, פשרת 1m→5m): בפוזיציית Short אין תקרה תיאורטית "
        "להפסד - מחיר המניה יכול לעלות ללא הגבלה, וה-stop עלול 'לקפוץ מעל' (gap) במקרה של short "
        "squeeze. אל תפעיל LIVE לפני בדיקה מקיפה.",
    ),
    (
        # v2 of ORB Long - a genuinely different variant (new entry
        # confluence filters + a whole different exit mechanism), so this
        # is a NEW preset rather than an in-place edit of "ORB Long
        # (Opening Range Breakout)" above: overwriting that one's
        # rules_json would silently mix its already-pooled fixed-target
        # backtest history together with this staged-trail variant's
        # future runs under the same strategy_id, corrupting both
        # perf.strategy_report's pooling and analyze_strategy.py's
        # diagnostics (same reasoning every other "aggressive"/"fade"
        # variant preset above already follows).
        #
        # Two changes from v1:
        # 1. entry_confluence (see orb._trend_confluence_ok): on top of
        #    the opening-range breakout/retest signal itself, also
        #    requires RSI(14) rising for the last 3 bars AND (EMA(20) on
        #    5m rising OR price above session VWAP) - two independent
        #    momentum/trend confirmations layered onto the ORB entry.
        # 2. exit.management_style: "staged_trail" instead of
        #    "fixed_target_no_trail" - no fixed R:R target at all
        #    (entry_models omit target_rr on purpose). Original stop
        #    stays untouched up to breakeven_trigger_R (2R), then flips
        #    to breakeven; trailing only starts once trailing_trigger_R
        #    (3R) is also cleared, trailing below the low of the last 2
        #    5-minute bars (see cycle.manage_position's staged_trail
        #    branch / orb.low_of_last_n_bars).
        "ORB Long v2 (RSI/Trend Confluence, Staged Trail)",
        {
            "strategy_name": "ORB Long v2 (RSI/Trend Confluence, Staged Trail)",
            "direction": "long_only",
            "es_vwap_filter": True,
            "opening_range": {
                "or_timeframe": "15m",
                "confirm_timeframe": "5m",
                "entry_timeframe": "5m",
                "session": "new_york",
                "session_open_et": "09:30",
            },
            "universe_filters": {
                "index": "S&P 500",
                "min_price_usd": 3.0,
                "custom_universe": "sp500_marketcap_1b",
            },
            "volatility_filters": {
                "V1_rvol_min": 2.0,
                "V1_rvol_lookback_days": 14,
                "V2_atr_period": 14,
                "V2_atr_pct_tiers": [
                    {"price_min": 3.0, "price_max": 20.0, "atr_pct_min": 4.0},
                    {"price_min": 20.0, "price_max": 50.0, "atr_pct_min": 3.0},
                    {"price_min": 50.0, "price_max": 100.0, "atr_pct_min": 2.0},
                    {"price_min": 100.0, "price_max": None, "atr_pct_min": 1.5},
                ],
            },
            "entry_confluence": {
                "rsi_period": 14,
                "rsi_rising_bars": 3,
                "ema_period": 20,
            },
            "entry_models": {
                "breakout": {"enabled": True},
                "retest": {"enabled": True},
            },
            "time_filter": {"earliest_entry_et": "09:50", "latest_entry_et": "11:30", "force_close_et": "15:51"},
            "exit": {
                "management_style": "staged_trail",
                "breakeven_trigger_R": 2.0,
                "trailing_trigger_R": 3.0,
                "profit_lock_offset_R": 0.25,
            },
            "risk": {
                "max_risk_per_trade_pct": 1.0,
                "max_position_size_pct_of_portfolio": 10,
                "max_concurrent_positions": 5,
                "min_stop_distance_pct": 0.25,
            },
        },
        "aggressive",
        "long",
        "## מה זה עושה\n"
        "גרסה שנייה (v2) של ORB Long - שומרת על אותו מנגנון Opening Range Breakout (OR 15 דקות, "
        "אישור 5 דקות, breakout/retest) אבל עם שני שינויים משמעותיים: פילטרים נוספים לפני כניסה, "
        "ומנגנון יציאה שונה לגמרי. **נשמרת כאסטרטגיה נפרדת מ-ORB Long המקורית** (לא דריסה במקום) "
        "כדי לא לערבב את היסטוריית הבקטסטים של שתיהן תחת אותה זהות.\n\n"
        "## פילטר כניסה נוסף: RSI + מגמה\n"
        "בנוסף לכל תנאי ה-ORB המקוריים (OR, אישור, RVOL+ATR%), נדרש גם: RSI(14) עולה על פני 3 נרות "
        "רצופים אחרונים, **וגם** (EMA(20) על 5 דקות עולה **או** המחיר מעל ה-VWAP של היום). כל התנאים "
        "האלה חייבים להתקיים באותו נר שבו נכנסים.\n\n"
        "## יציאה: Staged Trail (במקום יעד קבוע)\n"
        "אין יותר יעד R:R קבוע - הסטופ ההתחלתי נשאר קבוע עד שה-MFE (השיא שהמחיר בפועל נגע בו, לא "
        "רק מחיר הסגירה) מגיע ל-2R, ואז עובר ל-**Profit Lock: 0.25R** (לא ל-Breakeven שטוח) - "
        "כלומר גם עסקה שנגעה ב-2R תוך-יומית וחזרה אחורה, ננעלת עם רווח קטן במקום להסתכן בחזרה "
        "לסטופ המקורי. כשמגיעים ל-3R (סגירת נר), מתחיל טריילינג סטופ מתחת לשפל של שני הנרות "
        "האחרונים (5 דקות), ומתעדכן כל עוד הוא משתפר. הפוזיציה יכולה לרוץ הרבה מעבר ל-2R אם המניה "
        "ממשיכה.\n\n"
        "## יקום, פילטרי תנודתיות, חלון כניסות\n"
        "זהה ל-ORB Long המקורית: S&P 500 עם Market Cap מעל $1B, מחיר מינימלי $3, RVOL מעל 2.0, "
        "ATR% מדורג לפי מחיר, חלון כניסות 09:50-11:30 ET.\n\n"
        "## סינון כיוון שוק (ES VWAP)\n"
        "כניסה מתאפשרת רק כש-ES (חוזי E-mini S&P 500) נסחר **מעל** ל-VWAP היומי שלו - שים לב: "
        "זה נפרד לגמרי מ-VWAP הסימבול עצמו שכבר מופיע למעלה כחלק מ-RSI+EMA/VWAP (זה בודק את המניה "
        "הספציפית, זה בודק את השוק הרחב). \"Market first, setup second\": גם אם כל תנאי הכניסה "
        "מתקיימים, עסקה שסותרת את כיוון השוק הרחב נדחית. כבוי כברירת מחדל (דורש הרשאת נתוני CME "
        "futures + הפעלה מפורשת בעמוד Bot) - כל עוד הוא כבוי, או שאין גישה לנתוני ES, האסטרטגיה "
        "נשארת ללא סינון (fail-open), לא נחסמת.\n\n"
        "## רצפת מרחק סטופ מינימלית\n"
        "הסטופ הטכני (שפל/שיא נר האישור) לפעמים קרוב מדי לכניסה (נר כמעט ללא צל) - זה מייצר "
        "גודל פוזיציה מנופח מדי וערכי R מנופחים מדי שלא משקפים סיכון אמיתי (ראו חקירה בהיסטוריית "
        "השיחה). לכן מרחק הסטופ מהכניסה מוגבל מלמטה ל-0.25% ממחיר הכניסה "
        "(min_stop_distance_pct) - אם הסטופ הטכני קרוב יותר, הוא מורחב לרצפה הזו; סטופ טכני "
        "שכבר רחוק מספיק לא משתנה כלל.\n\n"
        "## פרופיל סיכון\n"
        "דירוג: aggressive - אסטרטגיה חדשה לגמרי שלא נבדקה כלל (v1 המקורית לפחות עברה בקטסט "
        "ראשוני - זו עוד לא). סיכון לעסקה: 1% | גודל פוזיציה מקס': 10% | פוזיציות בו-זמנית: עד 5\n\n"
        "## אזהרת סיכון - קרא לפני שאתה שוקל להפעיל\n"
        "אין לזה שום היסטוריית בקטסט עדיין - כל אזהרות ORB Long המקורית תקפות כאן במלואן, "
        "ובנוסף: הפילטרים הנוספים (RSI+EMA/VWAP) מצמצמים עוד יותר את מספר העסקאות הפוטנציאליות, "
        "וה-Staged Trail טרם נבדק כלל מול הנתונים ההיסטוריים. הרץ בקטסט מקיף (שבועות-חודשים) לפני "
        "כל שיקול נוסף.",
    ),
    (
        # Exact mirror of ORB Long v2 - see its own comment above for the
        # full explanation, not repeated here.
        "ORB Short v2 (RSI/Trend Confluence, Staged Trail)",
        {
            "strategy_name": "ORB Short v2 (RSI/Trend Confluence, Staged Trail)",
            "direction": "short_only",
            "es_vwap_filter": True,
            "opening_range": {
                "or_timeframe": "15m",
                "confirm_timeframe": "5m",
                "entry_timeframe": "5m",
                "session": "new_york",
                "session_open_et": "09:30",
            },
            "universe_filters": {
                "index": "S&P 500",
                "min_price_usd": 3.0,
                "custom_universe": "sp500_marketcap_1b",
            },
            "volatility_filters": {
                "V1_rvol_min": 2.0,
                "V1_rvol_lookback_days": 14,
                "V2_atr_period": 14,
                "V2_atr_pct_tiers": [
                    {"price_min": 3.0, "price_max": 20.0, "atr_pct_min": 4.0},
                    {"price_min": 20.0, "price_max": 50.0, "atr_pct_min": 3.0},
                    {"price_min": 50.0, "price_max": 100.0, "atr_pct_min": 2.0},
                    {"price_min": 100.0, "price_max": None, "atr_pct_min": 1.5},
                ],
            },
            "entry_confluence": {
                "rsi_period": 14,
                "rsi_rising_bars": 3,
                "ema_period": 20,
            },
            "entry_models": {
                "breakout": {"enabled": True},
                "retest": {"enabled": True},
            },
            "time_filter": {"earliest_entry_et": "09:50", "latest_entry_et": "11:30", "force_close_et": "15:51"},
            "exit": {
                "management_style": "staged_trail",
                "breakeven_trigger_R": 2.0,
                "trailing_trigger_R": 3.0,
                "profit_lock_offset_R": 0.25,
            },
            "risk": {
                "max_risk_per_trade_pct": 1.0,
                "max_position_size_pct_of_portfolio": 10,
                "max_concurrent_positions": 5,
                "min_stop_distance_pct": 0.25,
            },
        },
        "aggressive",
        "short",
        "## מה זה עושה\n"
        "מראה הפוכה מדויקת של ORB Long v2 - ראו את התיאור המלא שם. כאן: RSI(14) יורד על פני 3 "
        "נרות רצופים, וגם (EMA(20) יורד או המחיר מתחת ל-VWAP). סטופ קבוע עד MFE 2R (השיא/שפל "
        "שהמחיר בפועל נגע בו, לא רק סגירה), ואז Profit Lock 0.25R, טריילינג מ-3R מעל השיא של שני "
        "הנרות האחרונים.\n\n"
        "## יקום, פילטרים, חלון כניסות\n"
        "זהה לחלוטין ל-ORB Short המקורית ול-ORB Long v2.\n\n"
        "## סינון כיוון שוק (ES VWAP)\n"
        "כניסה מתאפשרת רק כש-ES (חוזי E-mini S&P 500) נסחר **מתחת** ל-VWAP היומי שלו - שים לב: "
        "זה נפרד לגמרי מ-VWAP הסימבול עצמו שכבר מופיע למעלה. \"Market first, setup second\": גם "
        "אם כל תנאי הכניסה מתקיימים, עסקה שסותרת את כיוון השוק הרחב נדחית. כבוי כברירת מחדל "
        "(דורש הרשאת נתוני CME futures + הפעלה מפורשת בעמוד Bot) - כל עוד הוא כבוי, או שאין גישה "
        "לנתוני ES, האסטרטגיה נשארת ללא סינון (fail-open), לא נחסמת.\n\n"
        "## רצפת מרחק סטופ מינימלית\n"
        "הסטופ הטכני (שפל/שיא נר האישור) לפעמים קרוב מדי לכניסה (נר כמעט ללא צל) - זה מייצר "
        "גודל פוזיציה מנופח מדי וערכי R מנופחים מדי שלא משקפים סיכון אמיתי (ראו חקירה בהיסטוריית "
        "השיחה). לכן מרחק הסטופ מהכניסה מוגבל מלמטה ל-0.25% ממחיר הכניסה "
        "(min_stop_distance_pct) - אם הסטופ הטכני קרוב יותר, הוא מורחב לרצפה הזו; סטופ טכני "
        "שכבר רחוק מספיק לא משתנה כלל.\n\n"
        "## פרופיל סיכון\n"
        "דירוג: aggressive - אסטרטגיה חדשה לגמרי שלא נבדקה כלל. סיכון לעסקה: 1% | גודל פוזיציה "
        "מקס': 10% | פוזיציות בו-זמנית: עד 5\n\n"
        "## אזהרת סיכון - קרא לפני שאתה שוקל להפעיל\n"
        "כל אזהרות ORB Long v2 תקפות כאן, ובנוסף: בפוזיציית Short אין תקרה תיאורטית להפסד - מחיר "
        "המניה יכול לעלות ללא הגבלה, וה-stop עלול 'לקפוץ מעל' (gap) במקרה של short squeeze. "
        "אל תפעיל LIVE לפני בדיקה מקיפה.",
    ),
    (
        # v3 of ORB Long v2 - EXACT same entry logic/filters as v2 (see its
        # own comment above, not repeated here: opening_range/universe_
        # filters/volatility_filters/entry_confluence/entry_models/
        # time_filter/es_vwap_filter/risk are all byte-for-byte copies).
        # Only exit_cfg's threshold constants change - same staged_trail
        # algorithm/code path (cycle._profit_lock_decision/_trailing_stop_
        # decision, backtest_engine.py's _staged_trail_exit_reason - none
        # of those were touched, only the numbers this rules_json feeds
        # them):
        #   MFE >= 2.0R -> Profit Lock +0.25R, MFE >= 3.0R -> trail  (v2)
        #   MFE >= 1.5R -> Profit Lock +0.50R, MFE >= 2.5R -> trail  (v3)
        # Motivation (see the "more aggressive profit-protection" request
        # this conversation implemented): plenty of v2 trades touch
        # 2R-3R intraday and give back most of the move before the flat
        # 2R/+0.25R lock ever catches them - v3 locks in a bigger profit
        # earlier while keeping the exact same trailing mechanism for a
        # real trend trade to keep running on. Kept as its own strategy_id
        # (v2 untouched) so backtest history never mixes and the two pool/
        # compare independently - see /api/strategies/compare.
        "ORB Long v3 (Early Profit Lock, Staged Trail)",
        {
            "strategy_name": "ORB Long v3 (Early Profit Lock, Staged Trail)",
            "direction": "long_only",
            "es_vwap_filter": True,
            "opening_range": {
                "or_timeframe": "15m",
                "confirm_timeframe": "5m",
                "entry_timeframe": "5m",
                "session": "new_york",
                "session_open_et": "09:30",
            },
            "universe_filters": {
                "index": "S&P 500",
                "min_price_usd": 3.0,
                "custom_universe": "sp500_marketcap_1b",
            },
            "volatility_filters": {
                "V1_rvol_min": 2.0,
                "V1_rvol_lookback_days": 14,
                "V2_atr_period": 14,
                "V2_atr_pct_tiers": [
                    {"price_min": 3.0, "price_max": 20.0, "atr_pct_min": 4.0},
                    {"price_min": 20.0, "price_max": 50.0, "atr_pct_min": 3.0},
                    {"price_min": 50.0, "price_max": 100.0, "atr_pct_min": 2.0},
                    {"price_min": 100.0, "price_max": None, "atr_pct_min": 1.5},
                ],
            },
            "entry_confluence": {
                "rsi_period": 14,
                "rsi_rising_bars": 3,
                "ema_period": 20,
            },
            "entry_models": {
                "breakout": {"enabled": True},
                "retest": {"enabled": True},
            },
            "time_filter": {"earliest_entry_et": "09:50", "latest_entry_et": "11:30", "force_close_et": "15:51"},
            "exit": {
                "management_style": "staged_trail",
                "breakeven_trigger_R": 1.5,
                "trailing_trigger_R": 2.5,
                "profit_lock_offset_R": 0.50,
            },
            "risk": {
                "max_risk_per_trade_pct": 1.0,
                "max_position_size_pct_of_portfolio": 10,
                "max_concurrent_positions": 5,
                "min_stop_distance_pct": 0.25,
            },
        },
        "aggressive",
        "long",
        "## מה זה עושה\n"
        "גרסה שלישית (v3) של ORB Long - תנאי כניסה זהים לחלוטין ל-ORB Long v2 (OR breakout/"
        "retest, RSI+EMA/VWAP, RVOL+ATR%, ES VWAP, אותו יקום וחלון כניסות). ההבדל היחיד: מודל "
        "הגנת הרווח (trade management) מגן על רווח מוקדם יותר ובכמות גדולה יותר. **נשמרת "
        "כאסטרטגיה נפרדת מ-v2** (לא דריסה) כדי לא לערבב את היסטוריות הבקטסט של שתיהן.\n\n"
        "## יציאה: Profit Lock ו-Trail מוקדמים יותר\n"
        "כש-MFE (השיא שהמחיר בפועל נגע בו תוך-יומית, לא רק סגירה) מגיע ל-**1.5R** (במקום 2R "
        "ב-v2), הסטופ עובר ל-**Profit Lock +0.50R** (במקום +0.25R ב-v2) - נעילת רווח גדולה יותר, "
        "מוקדם יותר. כש-MFE מגיע ל-**2.5R** (במקום 3R ב-v2), מתחיל טריילינג סטופ - **אותו "
        "אלגוריתם בדיוק כמו ב-v2** (מתחת לשפל של שני נרות 5 הדקות האחרונים, ללא שינוי), רק סף "
        "ההפעלה שונה. המטרה: בהרבה עסקאות v2 המחיר נוגע ב-2R-3R תוך-יומית וחוזר אחורה כמעט עד "
        "הסטופ לפני שהנעילה השטוחה של 2R/+0.25R תופסת אותו - v3 נועדה לנעול רווח משמעותי מוקדם "
        "יותר, בלי לפגוע ביכולת לרכב על עסקת מגמה אמיתית.\n\n"
        "## סיווג סיבת יציאה\n"
        "בדיוק כמו ב-v2: Initial stop loss / Profit-lock stop / Staged trailing stop / End of "
        "day - מסווג אוטומטית לפי אותו מנגנון (profit_lock_offset_R קיים ב-exit_cfg).\n\n"
        "## יקום, פילטרי תנודתיות, חלון כניסות\n"
        "זהה לחלוטין ל-ORB Long v2.\n\n"
        "## פרופיל סיכון\n"
        "דירוג: aggressive - אסטרטגיה חדשה לגמרי שלא נבדקה כלל, בדיוק כמו v2 בזמנו. סיכון "
        "לעסקה: 1% | גודל פוזיציה מקס': 10% | פוזיציות בו-זמנית: עד 5\n\n"
        "## אזהרת סיכון - קרא לפני שאתה שוקל להפעיל\n"
        "אין לזה שום היסטוריית בקטסט עדיין. כל אזהרות ORB Long v2 תקפות כאן במלואן - הרץ בקטסט "
        "מקיף (שבועות-חודשים, מאות עסקאות) והשווה מול v2 (ראו כלי ההשוואה בעמוד Backtest) לפני "
        "כל שיקול נוסף.",
    ),
    (
        # v3 of ORB Short v2 - exact mirror of ORB Long v3 above, see its
        # own comment for the full explanation, not repeated here.
        "ORB Short v3 (Early Profit Lock, Staged Trail)",
        {
            "strategy_name": "ORB Short v3 (Early Profit Lock, Staged Trail)",
            "direction": "short_only",
            "es_vwap_filter": True,
            "opening_range": {
                "or_timeframe": "15m",
                "confirm_timeframe": "5m",
                "entry_timeframe": "5m",
                "session": "new_york",
                "session_open_et": "09:30",
            },
            "universe_filters": {
                "index": "S&P 500",
                "min_price_usd": 3.0,
                "custom_universe": "sp500_marketcap_1b",
            },
            "volatility_filters": {
                "V1_rvol_min": 2.0,
                "V1_rvol_lookback_days": 14,
                "V2_atr_period": 14,
                "V2_atr_pct_tiers": [
                    {"price_min": 3.0, "price_max": 20.0, "atr_pct_min": 4.0},
                    {"price_min": 20.0, "price_max": 50.0, "atr_pct_min": 3.0},
                    {"price_min": 50.0, "price_max": 100.0, "atr_pct_min": 2.0},
                    {"price_min": 100.0, "price_max": None, "atr_pct_min": 1.5},
                ],
            },
            "entry_confluence": {
                "rsi_period": 14,
                "rsi_rising_bars": 3,
                "ema_period": 20,
            },
            "entry_models": {
                "breakout": {"enabled": True},
                "retest": {"enabled": True},
            },
            "time_filter": {"earliest_entry_et": "09:50", "latest_entry_et": "11:30", "force_close_et": "15:51"},
            "exit": {
                "management_style": "staged_trail",
                "breakeven_trigger_R": 1.5,
                "trailing_trigger_R": 2.5,
                "profit_lock_offset_R": 0.50,
            },
            "risk": {
                "max_risk_per_trade_pct": 1.0,
                "max_position_size_pct_of_portfolio": 10,
                "max_concurrent_positions": 5,
                "min_stop_distance_pct": 0.25,
            },
        },
        "aggressive",
        "short",
        "## מה זה עושה\n"
        "מראה הפוכה מדויקת של ORB Long v3 - ראו את התיאור המלא שם. כאן: תנאי כניסה זהים "
        "לחלוטין ל-ORB Short v2 (RSI יורד, EMA יורד/מחיר מתחת ל-VWAP, אותם פילטרים ויקום). "
        "אותו שינוי ניהול פוזיציה: MFE >= 1.5R -> Profit Lock -0.50R (במקום 2R -> -0.25R ב-v2), "
        "MFE >= 2.5R -> טריילינג (במקום 3R) - אותו אלגוריתם טריילינג בדיוק, רק סף ההפעלה "
        "שונה.\n\n"
        "## סיווג סיבת יציאה\n"
        "בדיוק כמו ב-v2: Initial stop loss / Profit-lock stop / Staged trailing stop / End of "
        "day.\n\n"
        "## יקום, פילטרים, חלון כניסות\n"
        "זהה לחלוטין ל-ORB Short v2 ול-ORB Long v3.\n\n"
        "## פרופיל סיכון\n"
        "דירוג: aggressive - אסטרטגיה חדשה לגמרי שלא נבדקה כלל. סיכון לעסקה: 1% | גודל פוזיציה "
        "מקס': 10% | פוזיציות בו-זמנית: עד 5\n\n"
        "## אזהרת סיכון - קרא לפני שאתה שוקל להפעיל\n"
        "כל אזהרות ORB Long v3 ו-ORB Short v2 תקפות כאן, ובנוסף: בפוזיציית Short אין תקרה "
        "תיאורטית להפסד - מחיר המניה יכול לעלות ללא הגבלה, וה-stop עלול 'לקפוץ מעל' (gap) "
        "במקרה של short squeeze. אל תפעיל LIVE לפני בדיקה מקיפה.",
    ),
    (
        # Experimental "fade" pair for ORB v2, same signal_side mechanism
        # (and same research motivation) as "Long Breakout Fade (Short)"/
        # "Short Breakdown Fade (Long)" above, extended to
        # orb.evaluate_orb_entry (see its own "signal_side decouples..."
        # docstring paragraph) rather than duplicating the whole ORB v2
        # engine. daily/opening_range/universe_filters/volatility_filters/
        # entry_confluence/entry_models/time_filter here are LITERALLY ORB
        # Long v2's own definitions, copied verbatim (same signal,
        # detected via signal_side="long") - only `direction` (short) and
        # the exit block's stop/trail mechanics (already side-driven, not
        # signal_side-driven - see evaluate_orb_entry's docstring) describe
        # the actual trade being placed. Kept as its own strategy_id
        # (not an edit of ORB Long v2) for the same backtest-history-
        # pooling reason every other fade/aggressive variant here follows.
        "ORB Long v2 Fade (Short)",
        {
            "strategy_name": "ORB Long v2 Fade (Short)",
            "direction": "short_only",
            "signal_side": "long",
            "opening_range": {
                "or_timeframe": "15m",
                "confirm_timeframe": "5m",
                "entry_timeframe": "5m",
                "session": "new_york",
                "session_open_et": "09:30",
            },
            "universe_filters": {
                "index": "S&P 500",
                "min_price_usd": 3.0,
                "custom_universe": "sp500_marketcap_1b",
            },
            "volatility_filters": {
                "V1_rvol_min": 2.0,
                "V1_rvol_lookback_days": 14,
                "V2_atr_period": 14,
                "V2_atr_pct_tiers": [
                    {"price_min": 3.0, "price_max": 20.0, "atr_pct_min": 4.0},
                    {"price_min": 20.0, "price_max": 50.0, "atr_pct_min": 3.0},
                    {"price_min": 50.0, "price_max": 100.0, "atr_pct_min": 2.0},
                    {"price_min": 100.0, "price_max": None, "atr_pct_min": 1.5},
                ],
            },
            "entry_confluence": {
                "rsi_period": 14,
                "rsi_rising_bars": 3,
                "ema_period": 20,
            },
            "entry_models": {
                "breakout": {"enabled": True},
                "retest": {"enabled": True},
            },
            "time_filter": {"earliest_entry_et": "09:50", "latest_entry_et": "11:30", "force_close_et": "15:51"},
            "exit": {
                "management_style": "staged_trail",
                "breakeven_trigger_R": 2.0,
                "trailing_trigger_R": 3.0,
            },
            "risk": {
                "max_risk_per_trade_pct": 1.0,
                "max_position_size_pct_of_portfolio": 10,
                "max_concurrent_positions": 5,
                "min_stop_distance_pct": 0.25,
            },
        },
        "aggressive",
        "short",
        "## מה זה עושה\n"
        "אסטרטגיית מחקר ניסיונית: מזהה בדיוק את אותו איתות של ORB Long v2 (פריצת opening range "
        "כלפי מעלה, עם אישור RVOL/ATR% ו-RSI+מגמה), אבל **מוכרת בשורט** נגד הפריצה במקום לקנות "
        "איתה - הימור שהפריצה תיכשל ותתהפך, לא שהיא תמשיך.\n\n"
        "## תנאי כניסה (זהים לחלוטין ל-ORB Long v2)\n"
        "Opening Range 15 דקות, אישור פריצה כלפי מעלה על נר 5 דקות, RVOL מעל 2.0, ATR% מדורג לפי "
        "מחיר, RSI(14) עולה על פני 3 נרות, וגם (EMA(20) עולה או מחיר מעל VWAP).\n\n"
        "## יציאה וניהול פוזיציה (מותאם לפוזיציית שורט, לא ללונג)\n"
        "סטופ התחלתי: שפל/שיא נר האישור (מעל הכניסה - זו פוזיציית שורט, לא מתחתיה). Staged Trail: "
        "נשאר קבוע עד 2R, Breakeven ב-2R, טריילינג מ-3R מעל השיא של שני הנרות האחרונים (5 דקות).\n\n"
        "## רצפת מרחק סטופ מינימלית\n"
        "הסטופ הטכני (שפל/שיא נר האישור) לפעמים קרוב מדי לכניסה (נר כמעט ללא צל) - זה מייצר "
        "גודל פוזיציה מנופח מדי וערכי R מנופחים מדי שלא משקפים סיכון אמיתי (ראו חקירה בהיסטוריית "
        "השיחה). לכן מרחק הסטופ מהכניסה מוגבל מלמטה ל-0.25% ממחיר הכניסה "
        "(min_stop_distance_pct) - אם הסטופ הטכני קרוב יותר, הוא מורחב לרצפה הזו; סטופ טכני "
        "שכבר רחוק מספיק לא משתנה כלל.\n\n"
        "## פרופיל סיכון\n"
        "דירוג: aggressive | סיכון לעסקה: 1% | גודל פוזיציה מקס': 10% | פוזיציות בו-זמנית: עד 5\n\n"
        "## אזהרת סיכון - קרא לפני שאתה שוקל להפעיל\n"
        "זו לא אסטרטגיה שאומתה - היא ממש את אותו רעיון ניסיוני של Long Breakout Fade (Short) "
        "(הימור שפריצה תיכשל ותתהפך), מיושם על ORB v2 במקום המודל הקלאסי. אל תפעיל LIVE לפני "
        "בדיקה מקיפה על פני תקופה ארוכה (שבועות-חודשים, מאות עסקאות). בנוסף, בפוזיציית Short "
        "אין תקרה תיאורטית להפסד.",
    ),
    (
        # Exact mirror of ORB Long v2 Fade (Short) - see its own comment
        # above for the full explanation, not repeated here.
        "ORB Short v2 Fade (Long)",
        {
            "strategy_name": "ORB Short v2 Fade (Long)",
            "direction": "long_only",
            "signal_side": "short",
            "opening_range": {
                "or_timeframe": "15m",
                "confirm_timeframe": "5m",
                "entry_timeframe": "5m",
                "session": "new_york",
                "session_open_et": "09:30",
            },
            "universe_filters": {
                "index": "S&P 500",
                "min_price_usd": 3.0,
                "custom_universe": "sp500_marketcap_1b",
            },
            "volatility_filters": {
                "V1_rvol_min": 2.0,
                "V1_rvol_lookback_days": 14,
                "V2_atr_period": 14,
                "V2_atr_pct_tiers": [
                    {"price_min": 3.0, "price_max": 20.0, "atr_pct_min": 4.0},
                    {"price_min": 20.0, "price_max": 50.0, "atr_pct_min": 3.0},
                    {"price_min": 50.0, "price_max": 100.0, "atr_pct_min": 2.0},
                    {"price_min": 100.0, "price_max": None, "atr_pct_min": 1.5},
                ],
            },
            "entry_confluence": {
                "rsi_period": 14,
                "rsi_rising_bars": 3,
                "ema_period": 20,
            },
            "entry_models": {
                "breakout": {"enabled": True},
                "retest": {"enabled": True},
            },
            "time_filter": {"earliest_entry_et": "09:50", "latest_entry_et": "11:30", "force_close_et": "15:51"},
            "exit": {
                "management_style": "staged_trail",
                "breakeven_trigger_R": 2.0,
                "trailing_trigger_R": 3.0,
            },
            "risk": {
                "max_risk_per_trade_pct": 1.0,
                "max_position_size_pct_of_portfolio": 10,
                "max_concurrent_positions": 5,
                "min_stop_distance_pct": 0.25,
            },
        },
        "aggressive",
        "long",
        "## מה זה עושה\n"
        "מראה הפוכה מדויקת של ORB Long v2 Fade (Short) - ראו את התיאור המלא שם. כאן: מזהה בדיוק "
        "את אותו איתות של ORB Short v2 (פריצת opening range כלפי מטה, RSI יורד, EMA יורד/מחיר "
        "מתחת ל-VWAP), אבל **קונה בלונג** נגד השבירה במקום למכור בשורט איתה.\n\n"
        "## יקום, פילטרים, חלון כניסות\n"
        "זהה לחלוטין ל-ORB Short v2 ול-ORB Long v2 Fade (Short).\n\n"
        "## יציאה וניהול פוזיציה (מותאם לפוזיציית לונג)\n"
        "סטופ התחלתי: שפל נר האישור (מתחת לכניסה - זו פוזיציית לונג). Staged Trail: נשאר קבוע עד "
        "2R, Breakeven ב-2R, טריילינג מ-3R מתחת לשפל של שני הנרות האחרונים.\n\n"
        "## רצפת מרחק סטופ מינימלית\n"
        "הסטופ הטכני (שפל/שיא נר האישור) לפעמים קרוב מדי לכניסה (נר כמעט ללא צל) - זה מייצר "
        "גודל פוזיציה מנופח מדי וערכי R מנופחים מדי שלא משקפים סיכון אמיתי (ראו חקירה בהיסטוריית "
        "השיחה). לכן מרחק הסטופ מהכניסה מוגבל מלמטה ל-0.25% ממחיר הכניסה "
        "(min_stop_distance_pct) - אם הסטופ הטכני קרוב יותר, הוא מורחב לרצפה הזו; סטופ טכני "
        "שכבר רחוק מספיק לא משתנה כלל.\n\n"
        "## פרופיל סיכון\n"
        "דירוג: aggressive | סיכון לעסקה: 1% | גודל פוזיציה מקס': 10% | פוזיציות בו-זמנית: עד 5\n\n"
        "## אזהרת סיכון - קרא לפני שאתה שוקל להפעיל\n"
        "כל אזהרות ORB Long v2 Fade (Short) תקפות כאן - זו לא אסטרטגיה שאומתה. אל תפעיל LIVE לפני "
        "בדיקה מקיפה על פני תקופה ארוכה.",
    ),
    (
        # Touch & Turn Scalper - a THIRD, distinct engine from both the
        # classic D1-D3/I1-I3 model and ORB: no daily "yesterday" bias
        # filters, no continuous per-tick "does it pass right now" signal
        # either - the opening N-minute candle is evaluated ONCE (right
        # after it closes) to decide whether today qualifies at all
        # (Liquidity Candle: opening_range >= ATR(14) x atr_multiplier)
        # and which way to fade it (bias: a red/bearish candle fades UP,
        # a green/bullish one fades DOWN - see src/touch_turn.py's own
        # docstring for the exact math, including the doji tie-break).
        # Dispatched to src/touch_turn.py's own evaluate_touch_turn_entry
        # (see cycle.entry_scan's "opening_candle" in rules check) instead
        # of either the classic or ORB pure logic.
        #
        # Unlike every other strategy here, a pass doesn't buy/sell at
        # market - it places a REAL resting IBKR limit order (a Buy Limit
        # at the opening candle's low for this Long variant, waiting for
        # price to retest/"touch" it and reverse/"turn") that sits in the
        # market for time_filter.entry_window_minutes (90) before IBKR's
        # own GTD time-in-force auto-cancels it unfilled - see cycle.
        # touch_turn_entry_scan/check_pending_touch_turn_orders and the
        # pending_orders table. Direction fits the one-active-strategy-
        # per-side model exactly: THIS variant only ever fires on a red
        # opening candle (bias must equal this strategy's own "long")
        # - activate the Short variant on the other side to cover green-
        # candle days too, each side only ever having a live setup on the
        # days its own bias actually occurs.
        #
        # Exit is a fixed Fibonacci target with R:R computed as
        # reward/reward_risk_ratio (2.0, i.e. Risk = Reward/2, matching
        # the spec's literal "2:1" wording) - same exit.management_style:
        # "fixed_target_no_trail" ORB already uses (no breakeven flip, no
        # trailing - orb.fixed_target_decision is fully generic despite
        # living in orb.py, so it's reused verbatim, not duplicated).
        "Touch & Turn Scalper - Long",
        {
            "strategy_name": "Touch & Turn Scalper - Long",
            "direction": "long_only",
            "es_vwap_filter": True,
            "opening_candle": {"timeframe_minutes": 15, "session_open_et": "09:30"},
            "universe_filters": {"index": "S&P 500", "min_price_usd": 3.0},
            "liquidity_filter": {"atr_period": 14, "atr_multiplier": 0.25},
            "fib_targets": {"long_target_pct": 38.2, "short_target_pct": 61.8},
            "reward_risk_ratio": 2.0,
            "time_filter": {"entry_window_minutes": 90},
            "exit": {"management_style": "fixed_target_no_trail"},
            "risk": {
                "max_risk_per_trade_pct": 1.0,
                "max_position_size_pct_of_portfolio": 10,
                "max_concurrent_positions": 5,
            },
        },
        "aggressive",
        "long",
        "## מה זה עושה\n"
        "אסטרטגיית סקאלפינג יומית: נר הפתיחה הראשון (15 דקות, נבנה מ-3 נרות 5 דקות: 9:30-9:45 ET) "
        "לפעמים מייצג 'Liquidity Candle' - מהלך חריג ביחס לתנודתיות הרגילה של המניה. כשזה קורה, "
        "המחיר נוטה לחזור ולגעת ('Touch') בקצה הטווח של אותו נר ואז להתהפך ('Turn') בחזרה למרכז. "
        "וריאציה זו (Long) פועלת רק בימים שבהם נר הפתיחה **אדום** (בשפל).\n\n"
        "## שלב 1: זיהוי Liquidity Candle\n"
        "טווח נר הפתיחה (High-Low) מושווה ל-ATR(14) היומי × 0.25. אם הטווח קטן מהסף - אין עסקה "
        "היום בכלל, לא רק בסימבול הזה.\n\n"
        "## שלב 2: קביעת כיוון (Bias)\n"
        "נר אדום (סגירה ≤ פתיחה) → Bias=Long (וריאציה זו פועלת). נר ירוק → Bias=Short (וריאציה זו "
        "מדלגת - הפעילו את Touch & Turn Scalper - Short בצד ה-Short כדי לכסות גם ימים כאלה).\n\n"
        "## שלב 3: כניסה - הזמנת Limit אמיתית\n"
        "**שונה מכל אסטרטגיה אחרת כאן**: לא קונה בשוק ברגע שהתנאים מתקיימים - מציבה הזמנת Buy "
        "Limit אמיתית ב-IBKR בשפל נר הפתיחה, וממתינה. ההזמנה משתמשת ב-GTD (Good-Till-Date) של "
        "IBKR עצמו כדי להתבטל אוטומטית אחרי 90 דקות (11:00 ET) אם לא מולאה - הבוט גם בודק זאת "
        "בעצמו כגיבוי. עד שהיא נמלאת או מתבטלת, זו לא פוזיציה עוקבת (stop/target) - רק הזמנה "
        "רדומה בשוק. מקסימום ניסיון אחד לסימבול ביום.\n\n"
        "## יעד ה-Take Profit וה-Stop\n"
        "יעד: רמת Fibonacci 38.2% (נמדד מהשיא של נר הפתיחה כלפי השפל). הסיכון מחושב **מהיעד**, לא "
        "מרמה טכנית: Reward = מרחק מהכניסה ליעד, Risk = Reward ÷ 2 (יחס 2:1), Stop = כניסה פחות "
        "Risk. לאחר מילוי - יעד קבוע בלבד, ללא breakeven וללא טריילינג (management_style: "
        "fixed_target_no_trail, כמו ORB).\n\n"
        "## חלון זמן\n"
        "רק ב-90 הדקות הראשונות של המסחר (9:30-11:00 ET) - גם להצבת ההזמנה וגם לתוקף שלה.\n\n"
        "## סינון כיוון שוק (ES VWAP)\n"
        "כניסה מתאפשרת רק כש-ES (חוזי E-mini S&P 500) נסחר **מעל** ל-VWAP היומי שלו - \"Market "
        "first, setup second\": גם אם כל תנאי הכניסה מתקיימים, עסקה שסותרת את כיוון השוק הרחב "
        "נדחית. כבוי כברירת מחדל (דורש הרשאת נתוני CME futures + הפעלה מפורשת בעמוד Bot) - כל עוד "
        "הוא כבוי, או שאין גישה לנתוני ES, האסטרטגיה נשארת ללא סינון (fail-open), לא נחסמת.\n\n"
        "## פרופיל סיכון\n"
        "דירוג: aggressive - אסטרטגיה חדשה שלא נבדקה, ומשתמשת במנגנון הזמנה שלא קיים באף אסטרטגיה "
        "אחרת (Limit אמיתי, לא Market). סיכון לעסקה: 1% | גודל פוזיציה מקס': 10% | פוזיציות "
        "בו-זמנית: עד 5\n\n"
        "## אזהרת סיכון - קרא לפני שאתה שוקל להפעיל\n"
        "זו לא אסטרטגיה שאומתה בשום צורה - יש להריץ backtest מקיף (שבועות-חודשים, מאות עסקאות) "
        "ולבחון paper trading ממושך לפני כל שיקול להעלות ל-LIVE. שימו לב שה-Stop כאן נגזר מיחס "
        "הסיכוי/סיכון ולא מרמה טכנית קונקרטית - זה עלול להניח סטופ קרוב מדי או רחוק מדי ביחס "
        "לתנודתיות בפועל של המניה.",
    ),
    (
        # Exact mirror of Touch & Turn Scalper - Long, see its own comment
        # above for the full engine explanation, not repeated here.
        "Touch & Turn Scalper - Short",
        {
            "strategy_name": "Touch & Turn Scalper - Short",
            "direction": "short_only",
            "es_vwap_filter": True,
            "opening_candle": {"timeframe_minutes": 15, "session_open_et": "09:30"},
            "universe_filters": {"index": "S&P 500", "min_price_usd": 3.0},
            "liquidity_filter": {"atr_period": 14, "atr_multiplier": 0.25},
            "fib_targets": {"long_target_pct": 38.2, "short_target_pct": 61.8},
            "reward_risk_ratio": 2.0,
            "time_filter": {"entry_window_minutes": 90},
            "exit": {"management_style": "fixed_target_no_trail"},
            "risk": {
                "max_risk_per_trade_pct": 1.0,
                "max_position_size_pct_of_portfolio": 10,
                "max_concurrent_positions": 5,
            },
        },
        "aggressive",
        "short",
        "## מה זה עושה\n"
        "מראה הפוכה מדויקת של Touch & Turn Scalper - Long - ראו את התיאור המלא שם. כאן: פועלת רק "
        "בימים שבהם נר הפתיחה **ירוק** (Bias=Short), מציבה Sell Limit אמיתי ב-IBKR בשיא נר הפתיחה "
        "וממתינה למגע חוזר, יעד ב-Fibonacci 61.8% (קרוב יותר לשפל), Stop = כניסה + Reward÷2.\n\n"
        "## יקום, זיהוי Liquidity Candle, חלון זמן\n"
        "זהה לחלוטין ל-Touch & Turn Scalper - Long: S&P 500, ATR(14)×0.25, חלון 9:30-11:00 ET, "
        "מקסימום ניסיון אחד לסימבול ביום, הזמנת Limit אמיתית עם GTD.\n\n"
        "## סינון כיוון שוק (ES VWAP)\n"
        "כניסה מתאפשרת רק כש-ES (חוזי E-mini S&P 500) נסחר **מתחת** ל-VWAP היומי שלו - \"Market "
        "first, setup second\": גם אם כל תנאי הכניסה מתקיימים, עסקה שסותרת את כיוון השוק הרחב "
        "נדחית. כבוי כברירת מחדל (דורש הרשאת נתוני CME futures + הפעלה מפורשת בעמוד Bot) - כל עוד "
        "הוא כבוי, או שאין גישה לנתוני ES, האסטרטגיה נשארת ללא סינון (fail-open), לא נחסמת.\n\n"
        "## פרופיל סיכון\n"
        "דירוג: aggressive - אסטרטגיה חדשה שלא נבדקה. סיכון לעסקה: 1% | גודל פוזיציה מקס': 10% | "
        "פוזיציות בו-זמנית: עד 5\n\n"
        "## אזהרת סיכון - קרא לפני שאתה שוקל להפעיל\n"
        "כל אזהרות Touch & Turn Scalper - Long תקפות כאן, ובנוסף: בפוזיציית Short אין תקרה "
        "תיאורטית להפסד - מחיר המניה יכול לעלות ללא הגבלה. אל תפעיל LIVE לפני בדיקה מקיפה.",
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
        _migrate_add_column(conn, "strategies", "description", "TEXT NOT NULL DEFAULT ''")
        _migrate_add_column(conn, "strategies", "key", "TEXT NOT NULL DEFAULT ''")
        _migrate_add_column(conn, "positions", "side", "TEXT NOT NULL DEFAULT 'long'")
        _migrate_add_column(conn, "positions", "hold_overnight", "INTEGER NOT NULL DEFAULT 0")
        # NULL for every non-ORB position (the vast majority) - only ever set
        # by cycle.entry_scan for a "fixed_target_no_trail" strategy (see
        # src/orb.py), read back by cycle.manage_position's ORB branch to
        # know when to close the whole position at its fixed R:R target.
        _migrate_add_column(conn, "positions", "target_price", "REAL")
        _migrate_add_column(conn, "watchlist", "universe", "TEXT NOT NULL DEFAULT ',default,'")
        _migrate_add_column(conn, "watchlist", "direction", "TEXT NOT NULL DEFAULT 'long'")
        # NULL for trades recorded the old way (trade.py/close_position.py's
        # own immediate record_trade call, before this column existed) and
        # for any row recorded since without a matching IBKR execution (e.g.
        # a failed/cancelled order) - only ever set for a real fill, and
        # only ever by cycle.sync_broker_fills, which uses it to avoid
        # re-inserting a fill it has already synced. See record_trade.
        _migrate_add_column(conn, "trades", "exec_id", "TEXT")
        _migrate_add_column(conn, "backtests", "pid", "INTEGER")
        _migrate_add_column(conn, "backtests", "execution_mode", "TEXT NOT NULL DEFAULT 'local'")
        _migrate_add_column(conn, "backtests", "claimed_at", "TEXT")
        _migrate_add_column(conn, "backtests", "archived_at", "TEXT")
        _migrate_add_column(conn, "backtests", "archive_folder", "TEXT NOT NULL DEFAULT ''")
        _migrate_add_column(conn, "backtest_data_fetches", "mode", "TEXT NOT NULL DEFAULT 'paper'")
        # The shipped default strategy predates risk_rating and got the
        # generic 'moderate' default from the ALTER TABLE above — it's
        # actually the conservative baseline every preset above is loosened
        # from, so correct it (only while still at that generic default, so
        # a deliberate manual re-rating via the dashboard isn't clobbered).
        conn.execute(
            "UPDATE strategies SET risk_rating = 'conservative' "
            "WHERE name = 'Trend Join Long (default)' AND risk_rating = 'moderate'"
        )

        # One-time rename to clearer, ≤4-word strategy names (old installs
        # only — INSERT OR IGNORE below is a no-op once a row already has
        # the new name, so this only fires the first time each old name is
        # still present).
        _STRATEGY_RENAMES = {
            "Trend Join Long (default)": "Long Breakout Conservative",
            "Trend Join Long - Moderate": "Long Breakout RSI Filter",
            "Trend Join Long - Aggressive": "Long Breakout Aggressive",
            "Trend Break Short (default)": "Short Breakdown Conservative",
        }
        for old_name, new_name in _STRATEGY_RENAMES.items():
            conn.execute("UPDATE strategies SET name = ? WHERE name = ?", (new_name, old_name))

        # The renamed "Long Breakout RSI Filter" strategy also changed
        # content (I2 swapped from new-high-of-day to RSI>50, and every
        # other number brought in line with the default) — replace its
        # rules_json/risk_rating too, but only while it still has the old
        # I2 key, so a deliberate manual edit made after this migration
        # already ran once isn't clobbered on a later restart.
        row = conn.execute(
            "SELECT id, rules_json FROM strategies WHERE name = 'Long Breakout RSI Filter'"
        ).fetchone()
        if row and "I2_above_today_hod" in row["rules_json"]:
            rsi_preset = next(p for p in EXTRA_STRATEGY_PRESETS if p[0] == "Long Breakout RSI Filter")
            conn.execute(
                "UPDATE strategies SET rules_json = ?, risk_rating = ?, updated_at = ? WHERE id = ?",
                (json.dumps(rsi_preset[1], indent=2), rsi_preset[2],
                 datetime.now(ET).isoformat(timespec="seconds"), row["id"]),
            )

        # "Short Parabolic Reversal" moved its own earliest entry time to
        # 09:35 (5 min after the actual 9:30 open, now that entry_scan
        # actually reads a strategy's own time_filter — see
        # _within_entry_window in cycle.py) instead of the shared 10:05
        # default. Only touch rows still at the old value, so a deliberate
        # manual re-edit after this migration already ran once isn't
        # clobbered on a later restart.
        row = conn.execute(
            "SELECT id, rules_json FROM strategies WHERE name = 'Short Parabolic Reversal'"
        ).fetchone()
        if row and '"earliest_entry_et": "10:05"' in row["rules_json"]:
            parabolic_preset = next(p for p in EXTRA_STRATEGY_PRESETS if p[0] == "Short Parabolic Reversal")
            conn.execute(
                "UPDATE strategies SET rules_json = ?, updated_at = ? WHERE id = ?",
                (json.dumps(parabolic_preset[1], indent=2),
                 datetime.now(ET).isoformat(timespec="seconds"), row["id"]),
            )

        # --- multi-account migration (runs once, no-ops after) ---
        _migrate_add_column(conn, "users", "is_admin", "INTEGER NOT NULL DEFAULT 0")
        _migrate_add_column(conn, "users", "role", "TEXT NOT NULL DEFAULT 'full'")
        admin_id = _resolve_or_create_admin(conn)

        # The admin's Gateway is already running on the ports the
        # single-account deployment has always used (env-driven — see
        # mode_config.ibkr_port) — seed that as their row so
        # get_or_assign_gateway_ports never hands them out to anyone else,
        # without actually remapping the admin's already-running Gateway.
        if admin_id is not None:
            conn.execute(
                "INSERT OR IGNORE INTO account_gateway_ports (account_id, paper_port, live_port) VALUES (?, ?, ?)",
                (admin_id, 4002, 4001),
            )

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

        default_strategy_description = (
            "## מה זה עושה\n"
            "אסטרטגיית הפריצה (breakout) הבסיסית והשמרנית ביותר של הבוט: מחפשת מניות שפורצות "
            "מעלה מתוך אזור צבירה, עם אישור פער בוקר ונפח מסחר גבוה מהרגיל.\n\n"
            "## תנאי כניסה\n"
            "D1: המחיר מעל השיא של אתמול\n"
            "D2: סגירת אתמול מעל הממוצע הנע 200 יום\n"
            "D3: פער (gap) של לפחות 3% מעלה מסגירת אתמול\n"
            "I1: המחיר מעל השיא של המסחר המוקדם (פרה-מרקט)\n"
            "I2: שיא חדש תוך-יומי\n"
            "I3: מחזור מסחר (RVOL) פי 2 לפחות מהממוצע של 14 הימים האחרונים\n\n"
            "## יציאה וניהול פוזיציה\n"
            "סטופ התחלתי: 1% מתחת לשפל היום\n"
            "העברת סטופ ל-Breakeven: ב-1R רווח\n"
            "לאחר מכן: טריילינג סטופ לפי שפל נר 5 דקות (2 נרות אחורה)\n\n"
            "## סינון כיוון שוק (ES VWAP)\n"
            "כניסה מתאפשרת רק כש-ES (חוזי E-mini S&P 500) נסחר **מעל** ל-VWAP היומי שלו - \"Market "
            "first, setup second\": גם אם כל תנאי הכניסה מתקיימים, עסקה שסותרת את כיוון השוק הרחב "
            "נדחית. כבוי כברירת מחדל (דורש הרשאת נתוני CME futures + הפעלה מפורשת בעמוד Bot) - כל עוד "
            "הוא כבוי, או שאין גישה לנתוני ES, האסטרטגיה נשארת ללא סינון (fail-open), לא נחסמת.\n\n"
            "## פרופיל סיכון\n"
            "דירוג: conservative\n"
            "סיכון לעסקה: 1% מהתיק\n"
            "גודל פוזיציה מקסימלי: 10% מהתיק\n"
            "פוזיציות בו-זמניות: עד 5"
        )
        row = conn.execute("SELECT COUNT(*) AS c FROM strategies").fetchone()
        if row["c"] == 0 and seed_rules_path and seed_rules_path.exists():
            now = datetime.now(ET).isoformat(timespec="seconds")
            rules_json = seed_rules_path.read_text()
            conn.execute(
                "INSERT INTO strategies (name, direction, rules_json, risk_rating, description, created_at, updated_at) "
                "VALUES (?, 'long', ?, 'conservative', ?, ?, ?)",
                ("Long Breakout Conservative", rules_json, default_strategy_description, now, now),
            )

        # Backfill the description for the default seeded strategy on an
        # old install that predates this column - only while it's still
        # empty, so a deliberate manual edit isn't clobbered on restart.
        conn.execute(
            "UPDATE strategies SET description = ? WHERE name = 'Long Breakout Conservative' AND description = ''",
            (default_strategy_description,),
        )

        for name, rules, risk_rating, direction, description in EXTRA_STRATEGY_PRESETS:
            now = datetime.now(ET).isoformat(timespec="seconds")
            conn.execute(
                "INSERT OR IGNORE INTO strategies (name, direction, rules_json, risk_rating, description, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (name, direction, json.dumps(rules, indent=2), risk_rating, description, now, now),
            )

        # Backfill the description for a strategy that already existed
        # before this column did (old install) - only while it's still the
        # empty default, so a deliberate manual edit made after this
        # migration already ran once isn't clobbered on a later restart.
        for preset_name, _rules, _risk, _direction, preset_description in EXTRA_STRATEGY_PRESETS:
            conn.execute(
                "UPDATE strategies SET description = ? WHERE name = ? AND description = ''",
                (preset_description, preset_name),
            )

        # One-time upgrade from the original single-paragraph descriptions
        # to the new "## heading" sectioned format (see bot.html's strategy
        # info modal, which parses "## " lines into headings) - matched on
        # the EXACT old text, not just "non-empty", so an install that
        # already auto-seeded the old prose gets the new format too, while
        # a real manual edit (which won't match this literal old text
        # either) is left alone either way.
        _OLD_TO_NEW_DESCRIPTIONS = {
            "Long Breakout Conservative": (
                "האסטרטגיה הבסיסית - זיהוי פריצה (breakout) שמרנית: (D1) המחיר מעל השיא של אתמול, "
                "(D2) סגירת אתמול מעל הממוצע הנע 200 יום, (D3) פער (gap) של לפחות 3% מעלה, "
                "(I1) מעל השיא של המסחר המוקדם, (I2) שיא חדש תוך-יומי, (I3) מחזור מסחר פי 2 לפחות "
                "מהממוצע (RVOL x2.0). Stop 1% מתחת לשפל היום, Partial profit ב-1R, Breakeven ב-1.5R, "
                "טריילינג סטופ אחרי זה. סיכון שמרני: 1% מהתיק לעסקה, מקס' 10% מהתיק לפוזיציה, "
                "עד 5 פוזיציות בו-זמנית.",
                default_strategy_description,
            ),
            "Long Breakout RSI Filter": (
                "זהה לחלוטין ל-Long Breakout Conservative בכל הפרמטרים המספריים - D1-D3, I1, I3, "
                "ניהול הסיכון והיציאה. ההבדל היחיד הוא תנאי I2: במקום לדרוש שיא חדש תוך-יומי, בודקת "
                "RSI(14) על נרות 5 דקות מעל 50 (מומנטום חיובי לפי אינדיקטור, ולא רק מחיר שיא).",
                None,
            ),
            "Long Breakout Aggressive": (
                "גרסה רופפת יותר של אסטרטגיית ה-Long: פער מינימלי נמוך יותר (1.5% במקום 3%), "
                "ו-RVOL מינימלי נמוך יותר (x1.2 במקום x2.0) - יותר עסקאות פוטנציאליות, אבל גם יותר "
                "'רעש' וסיכוי לאיתותי שווא. סיכון גבוה משמעותית: 2.5% מהתיק לעסקה, מקס' 20% מהתיק "
                "לפוזיציה, עד 8 פוזיציות בו-זמנית. Partial profit ב-1.5R, Breakeven ב-2R - יעדי רווח "
                "גבוהים יותר, תואמים לסיכון הגבוה יותר.",
                None,
            ),
            "Short Breakdown Conservative": (
                "מראה מדויק והפוך-כיוון של Long Breakout Conservative: (D1) המחיר מתחת לשפל של "
                "אתמול, (D2) סגירת אתמול מתחת לממוצע נע 200 יום, (D3) פער למטה של לפחות 3%, "
                "(I1) מתחת לשפל המסחר המוקדם, (I2) שפל חדש תוך-יומי, (I3) מחזור מסחר פי 2 מהממוצע. "
                "Stop 1% מעל השיא היום, אותם יעדי partial/breakeven/trailing כמו הגרסה הארוכה - רק "
                "הפוכים בכיוון. אותו פרופיל סיכון (1% לעסקה, מקס' 10%, עד 5 פוזיציות).",
                None,
            ),
            "Short Parabolic Reversal": (
                "נועדה לתפוס בדיוק את המקרה שבו Short Breakdown Conservative מפספסת: מניה שעלתה "
                "פרבולית ומתחילה להתהפך בשיא, כשהיא עדיין הרבה מעל ה-SMA200 (D2 השמרני לא יתמלא עד "
                "שהמניה כבר נפלה משמעותית). שני שינויים מהגרסה השמרנית: D2 דורש שסגירת אתמול תהיה "
                "לפחות 40% מעל ה-SMA50 (מתיחת יתר פרבולית, לא 'כבר מתחת ל-SMA200'), ו-I2 דורש "
                "RSI(14) מתחת ל-50 תוך-יומי (אישור שהמומנטום התהפך בפועל, לא שפל יומי חדש). זמן "
                "הכניסה המוקדם ביותר שלה גם מוקדם יותר - 9:35 במקום 10:05, כדי לתפוס את ההיפוך "
                "מוקדם ככל האפשר. זו כניסה נגד המגמה שהתקיימה עד כה (mean-reversion), ולכן חשופה "
                "יותר לאיתותי שווא.",
                None,
            ),
        }
        _new_by_name = {name: description for name, _r, _rr, _d, description in EXTRA_STRATEGY_PRESETS}
        for _name, (_old_text, _new_text) in _OLD_TO_NEW_DESCRIPTIONS.items():
            conn.execute(
                "UPDATE strategies SET description = ? WHERE name = ? AND description = ?",
                (_new_text if _new_text is not None else _new_by_name[_name], _name, _old_text),
            )

        # One-time upgrade removing stale "מימוש חלקי" (partial-profit) claims
        # from descriptions seeded before the exit logic dropped that stage (see
        # cycle._breakeven_decision's own docstring) - same exact-old-text-match
        # convention as _OLD_TO_NEW_DESCRIPTIONS above, so a real manual edit is
        # left alone either way.
        _STALE_PARTIAL_PROFIT_DESCRIPTIONS = {
            'Long Breakout RSI Filter': (
                "## מה זה עושה\nזהה לחלוטין ל-Long Breakout Conservative בכל הפרמטרים המספריים - ההבדל היחיד הוא תנאי הכניסה I2.\n\n## ההבדל מהגרסה הבסיסית\nבמקום לדרוש שיא חדש תוך-יומי (I2 בגרסה הבסיסית), האסטרטגיה הזו בודקת RSI(14) על נרות 5 דקות מעל 50 - מומנטום חיובי לפי אינדיקטור טכני, ולא רק מחיר שיא. זה יכול לתפוס כניסות מעט שונות: מניה שעדיין לא עשתה שיא חדש תוך-יומי אבל כבר מראה מומנטום חיובי לפי RSI.\n\n## תנאי כניסה\nD1: המחיר מעל השיא של אתמול\nD2: סגירת אתמול מעל הממוצע הנע 200 יום\nD3: פער של לפחות 3% מעלה\nI1: מעל השיא של המסחר המוקדם\nI2: RSI(14) על נרות 5 דקות מעל 50 (במקום שיא תוך-יומי)\nI3: RVOL פי 2 לפחות מהממוצע\n\n## יציאה וניהול פוזיציה\nזהה לגמרי לגרסה הבסיסית: סטופ 1% מתחת לשפל היום, מימוש חלקי ב-0.75R, Breakeven ב-1R, טריילינג סטופ לפי שפל נר 5 דקות.\n\n## פרופיל סיכון\nדירוג: conservative (זהה לגרסה הבסיסית)\nסיכון לעסקה: 1% | גודל פוזיציה מקס': 10% | פוזיציות בו-זמנית: עד 5",
                "## מה זה עושה\nזהה לחלוטין ל-Long Breakout Conservative בכל הפרמטרים המספריים - ההבדל היחיד הוא תנאי הכניסה I2.\n\n## ההבדל מהגרסה הבסיסית\nבמקום לדרוש שיא חדש תוך-יומי (I2 בגרסה הבסיסית), האסטרטגיה הזו בודקת RSI(14) על נרות 5 דקות מעל 50 - מומנטום חיובי לפי אינדיקטור טכני, ולא רק מחיר שיא. זה יכול לתפוס כניסות מעט שונות: מניה שעדיין לא עשתה שיא חדש תוך-יומי אבל כבר מראה מומנטום חיובי לפי RSI.\n\n## תנאי כניסה\nD1: המחיר מעל השיא של אתמול\nD2: סגירת אתמול מעל הממוצע הנע 200 יום\nD3: פער של לפחות 3% מעלה\nI1: מעל השיא של המסחר המוקדם\nI2: RSI(14) על נרות 5 דקות מעל 50 (במקום שיא תוך-יומי)\nI3: RVOL פי 2 לפחות מהממוצע\n\n## יציאה וניהול פוזיציה\nזהה לגמרי לגרסה הבסיסית: סטופ 1% מתחת לשפל היום, Breakeven ב-1R, טריילינג סטופ לפי שפל נר 5 דקות.\n\n## פרופיל סיכון\nדירוג: conservative (זהה לגרסה הבסיסית)\nסיכון לעסקה: 1% | גודל פוזיציה מקס': 10% | פוזיציות בו-זמנית: עד 5",
            ),
            'Long Breakout Aggressive': (
                "## מה זה עושה\nגרסה רופפת ומסוכנת יותר של אסטרטגיית ה-Long הבסיסית: סף כניסה נמוך יותר, כלומר יותר עסקאות פוטנציאליות - אבל גם יותר 'רעש' וסיכוי גבוה יותר לאיתותי שווא.\n\n## ההבדל מהגרסה הבסיסית\nפער מינימלי (D3) נמוך יותר: 1.5% במקום 3%\nRVOL מינימלי (I3) נמוך יותר: x1.2 במקום x2.0, על חלון של 10 ימים במקום 14\nיעדי רווח גבוהים יותר: מימוש חלקי ב-1.5R (במקום 0.75R), Breakeven ב-2R (במקום 1R)\nסיכון גבוה משמעותית לעסקה בודדת\n\n## תנאי כניסה\nD1: המחיר מעל השיא של אתמול\nD2: סגירת אתמול מעל הממוצע הנע 200 יום\nD3: פער של לפחות 1.5% מעלה\nI1: מעל השיא של המסחר המוקדם\nI2: שיא חדש תוך-יומי\nI3: RVOL פי 1.2 לפחות (ממוצע 10 ימים)\n\n## יציאה וניהול פוזיציה\nסטופ התחלתי: 1% מתחת לשפל היום\nמימוש חלקי: ב-1.5R\nBreakeven: ב-2R\nטריילינג סטופ: לפי שפל נר 5 דקות\n\n## פרופיל סיכון\nדירוג: aggressive - הפעלתה דורשת הקלדת אישור, כי היא חלה על LIVE מיידית\nסיכון לעסקה: 2.5% מהתיק\nגודל פוזיציה מקסימלי: 20% מהתיק\nפוזיציות בו-זמניות: עד 8",
                "## מה זה עושה\nגרסה רופפת ומסוכנת יותר של אסטרטגיית ה-Long הבסיסית: סף כניסה נמוך יותר, כלומר יותר עסקאות פוטנציאליות - אבל גם יותר 'רעש' וסיכוי גבוה יותר לאיתותי שווא.\n\n## ההבדל מהגרסה הבסיסית\nפער מינימלי (D3) נמוך יותר: 1.5% במקום 3%\nRVOL מינימלי (I3) נמוך יותר: x1.2 במקום x2.0, על חלון של 10 ימים במקום 14\nיעד Breakeven גבוה יותר: ב-2R (במקום 1R)\nסיכון גבוה משמעותית לעסקה בודדת\n\n## תנאי כניסה\nD1: המחיר מעל השיא של אתמול\nD2: סגירת אתמול מעל הממוצע הנע 200 יום\nD3: פער של לפחות 1.5% מעלה\nI1: מעל השיא של המסחר המוקדם\nI2: שיא חדש תוך-יומי\nI3: RVOL פי 1.2 לפחות (ממוצע 10 ימים)\n\n## יציאה וניהול פוזיציה\nסטופ התחלתי: 1% מתחת לשפל היום\nBreakeven: ב-2R\nטריילינג סטופ: לפי שפל נר 5 דקות\n\n## פרופיל סיכון\nדירוג: aggressive - הפעלתה דורשת הקלדת אישור, כי היא חלה על LIVE מיידית\nסיכון לעסקה: 2.5% מהתיק\nגודל פוזיציה מקסימלי: 20% מהתיק\nפוזיציות בו-זמניות: עד 8",
            ),
            'Short Breakdown Conservative': (
                "## מה זה עושה\nמראה מדויק והפוך-כיוון של Long Breakout Conservative: מוכרת בשורט מניות שנשברות מטה מתוך אזור צבירה, עם אישור פער בוקר כלפי מטה ונפח מסחר גבוה.\n\n## תנאי כניסה\nD1: המחיר מתחת לשפל של אתמול\nD2: סגירת אתמול מתחת לממוצע הנע 200 יום\nD3: פער של לפחות 3% מטה מסגירת אתמול\nI1: המחיר מתחת לשפל המסחר המוקדם\nI2: שפל חדש תוך-יומי\nI3: RVOL פי 2 לפחות מהממוצע\n\n## יציאה וניהול פוזיציה\nסטופ התחלתי: 1% מעל השיא של היום (לא מתחת לשפל - זו פוזיציית שורט)\nמימוש חלקי: ב-0.75R\nBreakeven: ב-1R\nטריילינג סטופ: לפי שיא נר 5 דקות\n\n## פרופיל סיכון\nדירוג: conservative\nסיכון לעסקה: 1% | גודל פוזיציה מקס': 10% | פוזיציות בו-זמנית: עד 5\n\n## אזהרת סיכון\nבניגוד לפוזיציית Long, בפוזיציית Short אין תקרה תיאורטית להפסד - מחיר המניה יכול לעלות ללא הגבלה, וה-stop עלול 'לקפוץ מעל' (gap) במקרה של short squeeze.",
                "## מה זה עושה\nמראה מדויק והפוך-כיוון של Long Breakout Conservative: מוכרת בשורט מניות שנשברות מטה מתוך אזור צבירה, עם אישור פער בוקר כלפי מטה ונפח מסחר גבוה.\n\n## תנאי כניסה\nD1: המחיר מתחת לשפל של אתמול\nD2: סגירת אתמול מתחת לממוצע הנע 200 יום\nD3: פער של לפחות 3% מטה מסגירת אתמול\nI1: המחיר מתחת לשפל המסחר המוקדם\nI2: שפל חדש תוך-יומי\nI3: RVOL פי 2 לפחות מהממוצע\n\n## יציאה וניהול פוזיציה\nסטופ התחלתי: 1% מעל השיא של היום (לא מתחת לשפל - זו פוזיציית שורט)\nBreakeven: ב-1R\nטריילינג סטופ: לפי שיא נר 5 דקות\n\n## פרופיל סיכון\nדירוג: conservative\nסיכון לעסקה: 1% | גודל פוזיציה מקס': 10% | פוזיציות בו-זמנית: עד 5\n\n## אזהרת סיכון\nבניגוד לפוזיציית Long, בפוזיציית Short אין תקרה תיאורטית להפסד - מחיר המניה יכול לעלות ללא הגבלה, וה-stop עלול 'לקפוץ מעל' (gap) במקרה של short squeeze.",
            ),
            'Short Parabolic Reversal': (
                "## מה זה עושה\nנועדה לתפוס בדיוק את המקרה שבו Short Breakdown Conservative מפספסת: מניה שעלתה פרבולית ומתחילה להתהפך בשיא, כשהיא עדיין רחוקה מעל ה-SMA200 (כך שהתנאי השמרני D2 לא היה מתמלא עד שהמניה כבר נופלת משמעותית).\n\n## ההבדל מהגרסה השמרנית\nD2 מוחלף: במקום 'סגירת אתמול מתחת ל-SMA200', דורשת שסגירת אתמול תהיה לפחות 40% מעל ה-SMA50 (סימן למתיחת יתר פרבולית)\nI2 מוחלף: במקום שפל חדש תוך-יומי, דורשת RSI(14) מתחת ל-50 תוך-יומי (אישור שהמומנטום כבר התהפך בפועל)\nזמן כניסה מוקדם יותר: 9:35 במקום 10:05, כדי לתפוס את ההיפוך מוקדם ככל האפשר\nD1, D3, I1, I3 זהים לגרסה השמרנית\n\n## תנאי כניסה\nD1: המחיר מתחת לשפל של אתמול\nD2: סגירת אתמול לפחות 40% מעל ה-SMA50\nD3: פער של לפחות 3% מטה\nI1: מתחת לשפל המסחר המוקדם\nI2: RSI(14) מתחת ל-50 תוך-יומי\nI3: RVOL פי 2 לפחות מהממוצע\n\n## יציאה, ניהול פוזיציה ופרופיל סיכון\nזהה ל-Short Breakdown Conservative: סטופ 1% מעל שיא היום, מימוש חלקי ב-0.75R, Breakeven ב-1R, טריילינג לפי שיא 5 דקות. סיכון 1% לעסקה, מקס' 10% לפוזיציה, עד 5 פוזיציות.\n\n## אזהרת סיכון\nדירוג aggressive - זו כניסה נגד המגמה שהתקיימה עד כה (mean-reversion), ולא המשך מגמה קיימת, ולכן חשופה יותר לאיתותי שווא. הפעלתה דורשת הקלדת אישור כי היא חלה על LIVE מיידית.",
                "## מה זה עושה\nנועדה לתפוס בדיוק את המקרה שבו Short Breakdown Conservative מפספסת: מניה שעלתה פרבולית ומתחילה להתהפך בשיא, כשהיא עדיין רחוקה מעל ה-SMA200 (כך שהתנאי השמרני D2 לא היה מתמלא עד שהמניה כבר נופלת משמעותית).\n\n## ההבדל מהגרסה השמרנית\nD2 מוחלף: במקום 'סגירת אתמול מתחת ל-SMA200', דורשת שסגירת אתמול תהיה לפחות 40% מעל ה-SMA50 (סימן למתיחת יתר פרבולית)\nI2 מוחלף: במקום שפל חדש תוך-יומי, דורשת RSI(14) מתחת ל-50 תוך-יומי (אישור שהמומנטום כבר התהפך בפועל)\nזמן כניסה מוקדם יותר: 9:35 במקום 10:05, כדי לתפוס את ההיפוך מוקדם ככל האפשר\nD1, D3, I1, I3 זהים לגרסה השמרנית\n\n## תנאי כניסה\nD1: המחיר מתחת לשפל של אתמול\nD2: סגירת אתמול לפחות 40% מעל ה-SMA50\nD3: פער של לפחות 3% מטה\nI1: מתחת לשפל המסחר המוקדם\nI2: RSI(14) מתחת ל-50 תוך-יומי\nI3: RVOL פי 2 לפחות מהממוצע\n\n## יציאה, ניהול פוזיציה ופרופיל סיכון\nזהה ל-Short Breakdown Conservative: סטופ 1% מעל שיא היום, Breakeven ב-1R, טריילינג לפי שיא 5 דקות. סיכון 1% לעסקה, מקס' 10% לפוזיציה, עד 5 פוזיציות.\n\n## אזהרת סיכון\nדירוג aggressive - זו כניסה נגד המגמה שהתקיימה עד כה (mean-reversion), ולא המשך מגמה קיימת, ולכן חשופה יותר לאיתותי שווא. הפעלתה דורשת הקלדת אישור כי היא חלה על LIVE מיידית.",
            ),
            'Long Breakout NASDAQ Beta': (
                "## מה זה עושה\nזהה במספרים לחלוטין ל-Long Breakout Conservative - ההבדל היחיד הוא היקום שהיא בכלל מסתכלת עליו.\n\n## ההבדל מהגרסה הבסיסית\nבמקום לסרוק את מדד ה-S&P 500, האסטרטגיה מוגבלת מראש לרשימת מניות NASDAQ (מדד IXIC) שעברו סינון פונדמנטלי:\nשווי שוק מעל 1 מיליארד דולר\nביטא מעל 1.2 (תנודתיות גבוהה יותר מהשוק)\nדירוג אנליסטים ממוצע Buy ומעלה\n\n## איך הרשימה נבנית\nהרשימה הזו לא מחושבת חי בכל מחזור מסחר כמו D1-I3 - היא נבנית מראש בנפרד (build_custom_universe.py), שדורש גישה חיה לנתונים פונדמנטליים, ומתעדכנת אוטומטית פעם בשבוע. אם הרשימה השמורה בשרת ישנה או לא קיימת, האסטרטגיה פשוט לא תמצא מועמדים באותו יום - זו לא תקלה, זו הגנה מפני מסחר על נתונים מיושנים.\n\n## תנאי כניסה\nזהה לגמרי לגרסה הבסיסית: D1 מעל שיא אתמול, D2 מעל SMA200, D3 פער 3%+, I1 מעל שיא פרה-מרקט, I2 שיא תוך-יומי, I3 RVOL x2.0.\n\n## יציאה, ניהול פוזיציה ופרופיל סיכון\nזהה לגמרי לגרסה הבסיסית: סטופ 1% מתחת לשפל היום, מימוש חלקי ב-0.75R, Breakeven ב-1R, טריילינג לפי שפל 5 דקות. דירוג conservative, סיכון 1% לעסקה, מקס' 10% לפוזיציה, עד 5 פוזיציות.",
                "## מה זה עושה\nזהה במספרים לחלוטין ל-Long Breakout Conservative - ההבדל היחיד הוא היקום שהיא בכלל מסתכלת עליו.\n\n## ההבדל מהגרסה הבסיסית\nבמקום לסרוק את מדד ה-S&P 500, האסטרטגיה מוגבלת מראש לרשימת מניות NASDAQ (מדד IXIC) שעברו סינון פונדמנטלי:\nשווי שוק מעל 1 מיליארד דולר\nביטא מעל 1.2 (תנודתיות גבוהה יותר מהשוק)\nדירוג אנליסטים ממוצע Buy ומעלה\n\n## איך הרשימה נבנית\nהרשימה הזו לא מחושבת חי בכל מחזור מסחר כמו D1-I3 - היא נבנית מראש בנפרד (build_custom_universe.py), שדורש גישה חיה לנתונים פונדמנטליים, ומתעדכנת אוטומטית פעם בשבוע. אם הרשימה השמורה בשרת ישנה או לא קיימת, האסטרטגיה פשוט לא תמצא מועמדים באותו יום - זו לא תקלה, זו הגנה מפני מסחר על נתונים מיושנים.\n\n## תנאי כניסה\nזהה לגמרי לגרסה הבסיסית: D1 מעל שיא אתמול, D2 מעל SMA200, D3 פער 3%+, I1 מעל שיא פרה-מרקט, I2 שיא תוך-יומי, I3 RVOL x2.0.\n\n## יציאה, ניהול פוזיציה ופרופיל סיכון\nזהה לגמרי לגרסה הבסיסית: סטופ 1% מתחת לשפל היום, Breakeven ב-1R, טריילינג לפי שפל 5 דקות. דירוג conservative, סיכון 1% לעסקה, מקס' 10% לפוזיציה, עד 5 פוזיציות.",
            ),
            'Long Breakout Fade (Short)': (
                "## מה זה עושה\nאסטרטגיית מחקר ניסיונית: מזהה בדיוק את אותו איתות של Long Breakout Conservative (פריצה כלפי מעלה עם נפח גבוה), אבל **מוכרת בשורט** נגד הפריצה במקום לקנות איתה - הימור שהפריצה תיכשל ותתהפך, לא שהיא תמשיך.\n\n## תנאי כניסה (זהים לחלוטין ל-Long Breakout Conservative)\nD1: המחיר מעל השיא של אתמול\nD2: סגירת אתמול מעל הממוצע הנע 200 יום\nD3: פער של לפחות 3% מעלה מסגירת אתמול\nI1: המחיר מעל שיא המסחר המוקדם\nI2: שיא חדש תוך-יומי\nI3: RVOL פי 2 לפחות מהממוצע\n\n## יציאה וניהול פוזיציה (מותאם לפוזיציית שורט, לא ללונג)\nסטופ התחלתי: 1% מעל השיא של היום (לא מתחת לשפל - זו פוזיציית שורט)\nמימוש חלקי: ב-0.75R | Breakeven: ב-1R | טריילינג סטופ: לפי שיא נר 5 דקות\n\n## פרופיל סיכון\nדירוג: aggressive | סיכון לעסקה: 1% | גודל פוזיציה מקס': 10% | פוזיציות בו-זמנית: עד 5\n\n## אזהרת סיכון - קרא לפני שאתה שוקל להפעיל\nזו לא אסטרטגיה שאומתה - היא נולדה משאלת מחקר על סמך יום מסחר בודד שבו Long Breakout Conservative הפסידה בכל עסקה. הפיכת כיוון על סמך יום אחד (9 עסקאות) היא בדיוק סוג הטעות הסטטיסטית ש-overfitting נראה כמוה - זה לא מוכיח יתרון אמיתי וחוזר בשוק. אל תפעיל LIVE לפני בדיקה מקיפה על פני תקופה ארוכה בהרבה (שבועות-חודשים, מאות עסקאות). בנוסף, בפוזיציית Short אין תקרה תיאורטית להפסד.",
                "## מה זה עושה\nאסטרטגיית מחקר ניסיונית: מזהה בדיוק את אותו איתות של Long Breakout Conservative (פריצה כלפי מעלה עם נפח גבוה), אבל **מוכרת בשורט** נגד הפריצה במקום לקנות איתה - הימור שהפריצה תיכשל ותתהפך, לא שהיא תמשיך.\n\n## תנאי כניסה (זהים לחלוטין ל-Long Breakout Conservative)\nD1: המחיר מעל השיא של אתמול\nD2: סגירת אתמול מעל הממוצע הנע 200 יום\nD3: פער של לפחות 3% מעלה מסגירת אתמול\nI1: המחיר מעל שיא המסחר המוקדם\nI2: שיא חדש תוך-יומי\nI3: RVOL פי 2 לפחות מהממוצע\n\n## יציאה וניהול פוזיציה (מותאם לפוזיציית שורט, לא ללונג)\nסטופ התחלתי: 1% מעל השיא של היום (לא מתחת לשפל - זו פוזיציית שורט)\nBreakeven: ב-1R | טריילינג סטופ: לפי שיא נר 5 דקות\n\n## פרופיל סיכון\nדירוג: aggressive | סיכון לעסקה: 1% | גודל פוזיציה מקס': 10% | פוזיציות בו-זמנית: עד 5\n\n## אזהרת סיכון - קרא לפני שאתה שוקל להפעיל\nזו לא אסטרטגיה שאומתה - היא נולדה משאלת מחקר על סמך יום מסחר בודד שבו Long Breakout Conservative הפסידה בכל עסקה. הפיכת כיוון על סמך יום אחד (9 עסקאות) היא בדיוק סוג הטעות הסטטיסטית ש-overfitting נראה כמוה - זה לא מוכיח יתרון אמיתי וחוזר בשוק. אל תפעיל LIVE לפני בדיקה מקיפה על פני תקופה ארוכה בהרבה (שבועות-חודשים, מאות עסקאות). בנוסף, בפוזיציית Short אין תקרה תיאורטית להפסד.",
            ),
            'Short Breakdown Fade (Long)': (
                "## מה זה עושה\nאסטרטגיית מחקר ניסיונית: מזהה בדיוק את אותו איתות של Short Breakdown Conservative (שבירה כלפי מטה עם נפח גבוה), אבל **קונה בלונג** נגד השבירה במקום למכור בשורט איתה - הימור שהשבירה תיכשל ותתהפך כלפי מעלה, לא שהיא תמשיך.\n\n## תנאי כניסה (זהים לחלוטין ל-Short Breakdown Conservative)\nD1: המחיר מתחת לשפל של אתמול\nD2: סגירת אתמול מתחת לממוצע הנע 200 יום\nD3: פער של לפחות 3% מטה מסגירת אתמול\nI1: המחיר מתחת לשפל המסחר המוקדם\nI2: שפל חדש תוך-יומי\nI3: RVOL פי 2 לפחות מהממוצע\n\n## יציאה וניהול פוזיציה (מותאם לפוזיציית לונג, לא לשורט)\nסטופ התחלתי: 1% מתחת לשפל של היום (לא מעל השיא - זו פוזיציית לונג)\nמימוש חלקי: ב-0.75R | Breakeven: ב-1R | טריילינג סטופ: לפי שפל נר 5 דקות\n\n## פרופיל סיכון\nדירוג: aggressive | סיכון לעסקה: 1% | גודל פוזיציה מקס': 10% | פוזיציות בו-זמנית: עד 5\n\n## אזהרת סיכון - קרא לפני שאתה שוקל להפעיל\nזו לא אסטרטגיה שאומתה - היא נולדה משאלת מחקר על סמך יום מסחר בודד שבו Short Breakdown Conservative הפסידה בכל עסקה. הפיכת כיוון על סמך יום אחד (9 עסקאות) היא בדיוק סוג הטעות הסטטיסטית ש-overfitting נראה כמוה - זה לא מוכיח יתרון אמיתי וחוזר בשוק. אל תפעיל LIVE לפני בדיקה מקיפה על פני תקופה ארוכה בהרבה (שבועות-חודשים, מאות עסקאות).",
                "## מה זה עושה\nאסטרטגיית מחקר ניסיונית: מזהה בדיוק את אותו איתות של Short Breakdown Conservative (שבירה כלפי מטה עם נפח גבוה), אבל **קונה בלונג** נגד השבירה במקום למכור בשורט איתה - הימור שהשבירה תיכשל ותתהפך כלפי מעלה, לא שהיא תמשיך.\n\n## תנאי כניסה (זהים לחלוטין ל-Short Breakdown Conservative)\nD1: המחיר מתחת לשפל של אתמול\nD2: סגירת אתמול מתחת לממוצע הנע 200 יום\nD3: פער של לפחות 3% מטה מסגירת אתמול\nI1: המחיר מתחת לשפל המסחר המוקדם\nI2: שפל חדש תוך-יומי\nI3: RVOL פי 2 לפחות מהממוצע\n\n## יציאה וניהול פוזיציה (מותאם לפוזיציית לונג, לא לשורט)\nסטופ התחלתי: 1% מתחת לשפל של היום (לא מעל השיא - זו פוזיציית לונג)\nBreakeven: ב-1R | טריילינג סטופ: לפי שפל נר 5 דקות\n\n## פרופיל סיכון\nדירוג: aggressive | סיכון לעסקה: 1% | גודל פוזיציה מקס': 10% | פוזיציות בו-זמנית: עד 5\n\n## אזהרת סיכון - קרא לפני שאתה שוקל להפעיל\nזו לא אסטרטגיה שאומתה - היא נולדה משאלת מחקר על סמך יום מסחר בודד שבו Short Breakdown Conservative הפסידה בכל עסקה. הפיכת כיוון על סמך יום אחד (9 עסקאות) היא בדיוק סוג הטעות הסטטיסטית ש-overfitting נראה כמוה - זה לא מוכיח יתרון אמיתי וחוזר בשוק. אל תפעיל LIVE לפני בדיקה מקיפה על פני תקופה ארוכה בהרבה (שבועות-חודשים, מאות עסקאות).",
            ),
            'Long Breakout Conservative': (
                '## מה זה עושה\nאסטרטגיית הפריצה (breakout) הבסיסית והשמרנית ביותר של הבוט: מחפשת מניות שפורצות מעלה מתוך אזור צבירה, עם אישור פער בוקר ונפח מסחר גבוה מהרגיל.\n\n## תנאי כניסה\nD1: המחיר מעל השיא של אתמול\nD2: סגירת אתמול מעל הממוצע הנע 200 יום\nD3: פער (gap) של לפחות 3% מעלה מסגירת אתמול\nI1: המחיר מעל השיא של המסחר המוקדם (פרה-מרקט)\nI2: שיא חדש תוך-יומי\nI3: מחזור מסחר (RVOL) פי 2 לפחות מהממוצע של 14 הימים האחרונים\n\n## יציאה וניהול פוזיציה\nסטופ התחלתי: 1% מתחת לשפל היום\nמימוש חלקי (1/3 מהפוזיציה): ב-0.75R רווח\nהעברת סטופ ל-Breakeven: ב-1R רווח\nלאחר מכן: טריילינג סטופ לפי שפל נר 5 דקות (2 נרות אחורה)\n\n## פרופיל סיכון\nדירוג: conservative\nסיכון לעסקה: 1% מהתיק\nגודל פוזיציה מקסימלי: 10% מהתיק\nפוזיציות בו-זמניות: עד 5',
                '## מה זה עושה\nאסטרטגיית הפריצה (breakout) הבסיסית והשמרנית ביותר של הבוט: מחפשת מניות שפורצות מעלה מתוך אזור צבירה, עם אישור פער בוקר ונפח מסחר גבוה מהרגיל.\n\n## תנאי כניסה\nD1: המחיר מעל השיא של אתמול\nD2: סגירת אתמול מעל הממוצע הנע 200 יום\nD3: פער (gap) של לפחות 3% מעלה מסגירת אתמול\nI1: המחיר מעל השיא של המסחר המוקדם (פרה-מרקט)\nI2: שיא חדש תוך-יומי\nI3: מחזור מסחר (RVOL) פי 2 לפחות מהממוצע של 14 הימים האחרונים\n\n## יציאה וניהול פוזיציה\nסטופ התחלתי: 1% מתחת לשפל היום\nהעברת סטופ ל-Breakeven: ב-1R רווח\nלאחר מכן: טריילינג סטופ לפי שפל נר 5 דקות (2 נרות אחורה)\n\n## פרופיל סיכון\nדירוג: conservative\nסיכון לעסקה: 1% מהתיק\nגודל פוזיציה מקסימלי: 10% מהתיק\nפוזיציות בו-זמניות: עד 5',
            ),
        }
        for _name, (_old_text, _new_text) in _STALE_PARTIAL_PROFIT_DESCRIPTIONS.items():
            conn.execute(
                "UPDATE strategies SET description = ? WHERE name = ? AND description = ?",
                (_new_text, _name, _old_text),
            )

        # One-time migration adding "es_vwap_filter": true to the 8 named
        # strategies the ES VWAP directional filter applies to (see
        # src/es_filter.py) - a fresh install already gets this from
        # EXTRA_STRATEGY_PRESETS/rules.json above, but an existing DB's
        # row was seeded before that key existed and INSERT OR IGNORE
        # never touches it again. Uses SQLite's own JSON1 functions
        # (json_set/json_extract) rather than an exact-text match (the
        # _STALE_PARTIAL_PROFIT_DESCRIPTIONS convention above) since
        # rules_json's exact whitespace/key-order isn't something this
        # migration should have to match byte-for-byte - only touches a
        # row that doesn't already have the key set, so it's a no-op on
        # every later restart once applied, and never overwrites a
        # deliberate manual "es_vwap_filter": false a user might set to
        # opt a specific strategy back out.
        _ES_VWAP_FILTER_STRATEGY_NAMES = (
            "Long Breakout Conservative", "Short Breakdown Conservative",
            "ORB Long (Opening Range Breakout)", "ORB Short (Opening Range Breakdown)",
            "ORB Long v2 (RSI/Trend Confluence, Staged Trail)", "ORB Short v2 (RSI/Trend Confluence, Staged Trail)",
            "Touch & Turn Scalper - Long", "Touch & Turn Scalper - Short",
        )
        for _name in _ES_VWAP_FILTER_STRATEGY_NAMES:
            conn.execute(
                "UPDATE strategies SET rules_json = json_set(rules_json, '$.es_vwap_filter', json('true')) "
                "WHERE name = ? AND json_extract(rules_json, '$.es_vwap_filter') IS NULL",
                (_name,),
            )

        # One-time migration adding "profit_lock_offset_R": 0.25 to
        # exit_json.exit for ORB Long v2 / ORB Short v2 ONLY (not their
        # Fade siblings, which keep the old flat-breakeven-on-Close
        # behavior unchanged - see cycle._profit_lock_decision's own
        # docstring for the full rationale). Same JSON1-not-exact-text
        # convention as the es_vwap_filter migration just above, and same
        # no-op-once-applied / never-overwrites-a-manual-value guarantee.
        _PROFIT_LOCK_STRATEGY_NAMES = (
            "ORB Long v2 (RSI/Trend Confluence, Staged Trail)", "ORB Short v2 (RSI/Trend Confluence, Staged Trail)",
        )
        for _name in _PROFIT_LOCK_STRATEGY_NAMES:
            conn.execute(
                "UPDATE strategies SET rules_json = json_set(rules_json, '$.exit.profit_lock_offset_R', 0.25) "
                "WHERE name = ? AND json_extract(rules_json, '$.exit.profit_lock_offset_R') IS NULL",
                (_name,),
            )

        # One-time migration adding "min_stop_distance_pct": 0.25 to
        # risk_json.risk for all 6 ORB strategies (v1 Long/Short, v2 Long/
        # Short, and both v2 Fade variants - orb._apply_min_stop_distance
        # already defaults to 0.25% even without this key present, so this
        # migration is belt-and-suspenders for visibility/tunability in the
        # Strategy edit UI, not required for the floor to take effect on an
        # unmigrated row. Same JSON1-not-exact-text convention as the two
        # migrations just above.
        _MIN_STOP_DISTANCE_STRATEGY_NAMES = (
            "ORB Long (Opening Range Breakout)", "ORB Short (Opening Range Breakdown)",
            "ORB Long v2 (RSI/Trend Confluence, Staged Trail)", "ORB Short v2 (RSI/Trend Confluence, Staged Trail)",
            "ORB Long v2 Fade (Short)", "ORB Short v2 Fade (Long)",
        )
        for _name in _MIN_STOP_DISTANCE_STRATEGY_NAMES:
            conn.execute(
                "UPDATE strategies SET rules_json = json_set(rules_json, '$.risk.min_stop_distance_pct', 0.25) "
                "WHERE name = ? AND json_extract(rules_json, '$.risk.min_stop_distance_pct') IS NULL",
                (_name,),
            )

        # One-time migration inserting the new "## סינון כיוון שוק (ES VWAP)"
        # description section into the same 8 strategies' description text
        # (see _ES_VWAP_FILTER_STRATEGY_NAMES migration above, which only touches
        # rules_json, not description) - same exact-old-text-match convention as
        # _STALE_PARTIAL_PROFIT_DESCRIPTIONS above, so a real manual edit is left
        # alone either way.
        _ES_VWAP_FILTER_DESCRIPTIONS = {
            'Long Breakout Conservative': (
                '## מה זה עושה\nאסטרטגיית הפריצה (breakout) הבסיסית והשמרנית ביותר של הבוט: מחפשת מניות שפורצות מעלה מתוך אזור צבירה, עם אישור פער בוקר ונפח מסחר גבוה מהרגיל.\n\n## תנאי כניסה\nD1: המחיר מעל השיא של אתמול\nD2: סגירת אתמול מעל הממוצע הנע 200 יום\nD3: פער (gap) של לפחות 3% מעלה מסגירת אתמול\nI1: המחיר מעל השיא של המסחר המוקדם (פרה-מרקט)\nI2: שיא חדש תוך-יומי\nI3: מחזור מסחר (RVOL) פי 2 לפחות מהממוצע של 14 הימים האחרונים\n\n## יציאה וניהול פוזיציה\nסטופ התחלתי: 1% מתחת לשפל היום\nהעברת סטופ ל-Breakeven: ב-1R רווח\nלאחר מכן: טריילינג סטופ לפי שפל נר 5 דקות (2 נרות אחורה)\n\n## פרופיל סיכון\nדירוג: conservative\nסיכון לעסקה: 1% מהתיק\nגודל פוזיציה מקסימלי: 10% מהתיק\nפוזיציות בו-זמניות: עד 5',
                '## מה זה עושה\nאסטרטגיית הפריצה (breakout) הבסיסית והשמרנית ביותר של הבוט: מחפשת מניות שפורצות מעלה מתוך אזור צבירה, עם אישור פער בוקר ונפח מסחר גבוה מהרגיל.\n\n## תנאי כניסה\nD1: המחיר מעל השיא של אתמול\nD2: סגירת אתמול מעל הממוצע הנע 200 יום\nD3: פער (gap) של לפחות 3% מעלה מסגירת אתמול\nI1: המחיר מעל השיא של המסחר המוקדם (פרה-מרקט)\nI2: שיא חדש תוך-יומי\nI3: מחזור מסחר (RVOL) פי 2 לפחות מהממוצע של 14 הימים האחרונים\n\n## יציאה וניהול פוזיציה\nסטופ התחלתי: 1% מתחת לשפל היום\nהעברת סטופ ל-Breakeven: ב-1R רווח\nלאחר מכן: טריילינג סטופ לפי שפל נר 5 דקות (2 נרות אחורה)\n\n## סינון כיוון שוק (ES VWAP)\nכניסה מתאפשרת רק כש-ES (חוזי E-mini S&P 500) נסחר **מעל** ל-VWAP היומי שלו - "Market first, setup second": גם אם כל תנאי הכניסה מתקיימים, עסקה שסותרת את כיוון השוק הרחב נדחית. כבוי כברירת מחדל (דורש הרשאת נתוני CME futures + הפעלה מפורשת בעמוד Bot) - כל עוד הוא כבוי, או שאין גישה לנתוני ES, האסטרטגיה נשארת ללא סינון (fail-open), לא נחסמת.\n\n## פרופיל סיכון\nדירוג: conservative\nסיכון לעסקה: 1% מהתיק\nגודל פוזיציה מקסימלי: 10% מהתיק\nפוזיציות בו-זמניות: עד 5',
            ),
            'Short Breakdown Conservative': (
                "## מה זה עושה\nמראה מדויק והפוך-כיוון של Long Breakout Conservative: מוכרת בשורט מניות שנשברות מטה מתוך אזור צבירה, עם אישור פער בוקר כלפי מטה ונפח מסחר גבוה.\n\n## תנאי כניסה\nD1: המחיר מתחת לשפל של אתמול\nD2: סגירת אתמול מתחת לממוצע הנע 200 יום\nD3: פער של לפחות 3% מטה מסגירת אתמול\nI1: המחיר מתחת לשפל המסחר המוקדם\nI2: שפל חדש תוך-יומי\nI3: RVOL פי 2 לפחות מהממוצע\n\n## יציאה וניהול פוזיציה\nסטופ התחלתי: 1% מעל השיא של היום (לא מתחת לשפל - זו פוזיציית שורט)\nBreakeven: ב-1R\nטריילינג סטופ: לפי שיא נר 5 דקות\n\n## פרופיל סיכון\nדירוג: conservative\nסיכון לעסקה: 1% | גודל פוזיציה מקס': 10% | פוזיציות בו-זמנית: עד 5\n\n## אזהרת סיכון\nבניגוד לפוזיציית Long, בפוזיציית Short אין תקרה תיאורטית להפסד - מחיר המניה יכול לעלות ללא הגבלה, וה-stop עלול 'לקפוץ מעל' (gap) במקרה של short squeeze.",
                '## מה זה עושה\nמראה מדויק והפוך-כיוון של Long Breakout Conservative: מוכרת בשורט מניות שנשברות מטה מתוך אזור צבירה, עם אישור פער בוקר כלפי מטה ונפח מסחר גבוה.\n\n## תנאי כניסה\nD1: המחיר מתחת לשפל של אתמול\nD2: סגירת אתמול מתחת לממוצע הנע 200 יום\nD3: פער של לפחות 3% מטה מסגירת אתמול\nI1: המחיר מתחת לשפל המסחר המוקדם\nI2: שפל חדש תוך-יומי\nI3: RVOL פי 2 לפחות מהממוצע\n\n## יציאה וניהול פוזיציה\nסטופ התחלתי: 1% מעל השיא של היום (לא מתחת לשפל - זו פוזיציית שורט)\nBreakeven: ב-1R\nטריילינג סטופ: לפי שיא נר 5 דקות\n\n## סינון כיוון שוק (ES VWAP)\nכניסה מתאפשרת רק כש-ES (חוזי E-mini S&P 500) נסחר **מתחת** ל-VWAP היומי שלו - "Market first, setup second": גם אם כל תנאי הכניסה מתקיימים, עסקה שסותרת את כיוון השוק הרחב נדחית. כבוי כברירת מחדל (דורש הרשאת נתוני CME futures + הפעלה מפורשת בעמוד Bot) - כל עוד הוא כבוי, או שאין גישה לנתוני ES, האסטרטגיה נשארת ללא סינון (fail-open), לא נחסמת.\n\n## פרופיל סיכון\nדירוג: conservative\nסיכון לעסקה: 1% | גודל פוזיציה מקס\': 10% | פוזיציות בו-זמנית: עד 5\n\n## אזהרת סיכון\nבניגוד לפוזיציית Long, בפוזיציית Short אין תקרה תיאורטית להפסד - מחיר המניה יכול לעלות ללא הגבלה, וה-stop עלול \'לקפוץ מעל\' (gap) במקרה של short squeeze.',
            ),
            'ORB Long (Opening Range Breakout)': (
                "## מה זה עושה\nאסטרטגיה מבוססת Opening Range Breakout (ORB): לא בודקת דעה מקדימה מהיום הקודם (אין daily_filters בכלל) - כל יום מתחיל מאפס. סוחרת רק את הנר הראשון של פתיחת המסחר בניו יורק (9:30 ET), מחכה לאישור פריצה, ואז מחפשת כניסה להמשך התנועה. מקור: תמלול סרטון YouTube (bITIVwysCzM) - ראו docs/orb_strategy_spec.md למפרט המלא ולתהליך ההגדרה.\n\n## יקום\nS&P 500 בלבד, מסונן מראש למניות עם Market Cap מעל $1B (custom_universe: sp500_marketcap_1b, נבנה על ידי build_custom_universe.py - כמו Long Breakout NASDAQ Beta) ומחיר מינימלי $3.\n\n## מנגנון ה-Opening Range\n1. סימון High/Low של 3 נרות 5 דקות ראשונים מ-9:30 ET (= 'נר' 15 דקות) - זה ה-Opening Range.\n2. אישור: נר 5 דקות שנסגר מעל ה-OR High.\n3. כניסה: על אותה מסגרת 5 דקות (**לא 1 דקה כמו בסרטון המקורי** - פשרה כי אין נתוני 1 דקה בתשתית ה-backtest הקיימת, ראו הערה בקובץ המפרט).\n\n## פילטרים לפני כניסה\nRVOL מעל 2.0 (חלון 14 ימים) ו-ATR% (יחסי למחיר, לא אבסולוטי) לפי מדרגת מחיר: $3-20 מעל 4%, $20-50 מעל 3%, $50-100 מעל 2%, מעל $100 מעל 1.5%.\n\n## מודלי כניסה (2 מתוך 3 בסרטון המקורי - Reversal הוסר מהיקף)\n**Breakout**: רק על נר האישור עצמו, ורק אם יש 'gap' (displacement) בינו לנר הקודם - כניסה בסגירת הנר, סטופ בשפל/שיא אותו נר.\n**Retest**: נר כלשהו אחרי האישור שנוגע בחזרה ברמת ה-OR ונסגר בחזרה בכיוון הפריצה - כניסה בסגירת הנר, סטופ בשפל/שיא אותו נר.\n\n## יציאה וניהול פוזיציה (שונה מכל שאר האסטרטגיות בפרויקט)\nאין breakeven flip ואין טריילינג סטופ - הסטופ ההתחלתי (משלב הכניסה) נשאר קבוע כל הפוזיציה. יעד קבוע R:R = 1:2: יציאה מלאה ביעד או בסטופ, מה שמגיע קודם.\n\n## חלון כניסות\n09:50-11:30 ET בלבד (השעתיים הראשונות של המסחר, כפי שממליץ הסרטון) - force close רגיל ב-15:51 ET לכל פוזיציה שעדיין פתוחה.\n\n## פרופיל סיכון\nדירוג: aggressive - אסטרטגיה חדשה שלא נבדקה (לא backtest, לא paper trading) - הפעלתה דורשת הקלדת אישור כי היא חלה על LIVE מיידית. סיכון לעסקה: 1% | גודל פוזיציה מקס': 10% | פוזיציות בו-זמנית: עד 5\n\n## אזהרת סיכון - קרא לפני שאתה שוקל להפעיל\nזו לא אסטרטגיה שאומתה בשום צורה - יש להריץ backtest מקיף (שבועות-חודשים, מאות עסקאות) ולבחון paper trading ממושך לפני כל שיקול להעלות ל-LIVE. שימו לב גם לפשרת 1 דקה→5 דקות בכניסה: הדיוק בפועל נמוך יותר ממה שהסרטון המקורי מתאר, וה-R:R בפועל עלול להיות שונה מהמתוכנן.",
                '## מה זה עושה\nאסטרטגיה מבוססת Opening Range Breakout (ORB): לא בודקת דעה מקדימה מהיום הקודם (אין daily_filters בכלל) - כל יום מתחיל מאפס. סוחרת רק את הנר הראשון של פתיחת המסחר בניו יורק (9:30 ET), מחכה לאישור פריצה, ואז מחפשת כניסה להמשך התנועה. מקור: תמלול סרטון YouTube (bITIVwysCzM) - ראו docs/orb_strategy_spec.md למפרט המלא ולתהליך ההגדרה.\n\n## יקום\nS&P 500 בלבד, מסונן מראש למניות עם Market Cap מעל $1B (custom_universe: sp500_marketcap_1b, נבנה על ידי build_custom_universe.py - כמו Long Breakout NASDAQ Beta) ומחיר מינימלי $3.\n\n## מנגנון ה-Opening Range\n1. סימון High/Low של 3 נרות 5 דקות ראשונים מ-9:30 ET (= \'נר\' 15 דקות) - זה ה-Opening Range.\n2. אישור: נר 5 דקות שנסגר מעל ה-OR High.\n3. כניסה: על אותה מסגרת 5 דקות (**לא 1 דקה כמו בסרטון המקורי** - פשרה כי אין נתוני 1 דקה בתשתית ה-backtest הקיימת, ראו הערה בקובץ המפרט).\n\n## פילטרים לפני כניסה\nRVOL מעל 2.0 (חלון 14 ימים) ו-ATR% (יחסי למחיר, לא אבסולוטי) לפי מדרגת מחיר: $3-20 מעל 4%, $20-50 מעל 3%, $50-100 מעל 2%, מעל $100 מעל 1.5%.\n\n## מודלי כניסה (2 מתוך 3 בסרטון המקורי - Reversal הוסר מהיקף)\n**Breakout**: רק על נר האישור עצמו, ורק אם יש \'gap\' (displacement) בינו לנר הקודם - כניסה בסגירת הנר, סטופ בשפל/שיא אותו נר.\n**Retest**: נר כלשהו אחרי האישור שנוגע בחזרה ברמת ה-OR ונסגר בחזרה בכיוון הפריצה - כניסה בסגירת הנר, סטופ בשפל/שיא אותו נר.\n\n## יציאה וניהול פוזיציה (שונה מכל שאר האסטרטגיות בפרויקט)\nאין breakeven flip ואין טריילינג סטופ - הסטופ ההתחלתי (משלב הכניסה) נשאר קבוע כל הפוזיציה. יעד קבוע R:R = 1:2: יציאה מלאה ביעד או בסטופ, מה שמגיע קודם.\n\n## חלון כניסות\n09:50-11:30 ET בלבד (השעתיים הראשונות של המסחר, כפי שממליץ הסרטון) - force close רגיל ב-15:51 ET לכל פוזיציה שעדיין פתוחה.\n\n## סינון כיוון שוק (ES VWAP)\nכניסה מתאפשרת רק כש-ES (חוזי E-mini S&P 500) נסחר **מעל** ל-VWAP היומי שלו - "Market first, setup second": גם אם כל תנאי הכניסה מתקיימים, עסקה שסותרת את כיוון השוק הרחב נדחית. כבוי כברירת מחדל (דורש הרשאת נתוני CME futures + הפעלה מפורשת בעמוד Bot) - כל עוד הוא כבוי, או שאין גישה לנתוני ES, האסטרטגיה נשארת ללא סינון (fail-open), לא נחסמת.\n\n## פרופיל סיכון\nדירוג: aggressive - אסטרטגיה חדשה שלא נבדקה (לא backtest, לא paper trading) - הפעלתה דורשת הקלדת אישור כי היא חלה על LIVE מיידית. סיכון לעסקה: 1% | גודל פוזיציה מקס\': 10% | פוזיציות בו-זמנית: עד 5\n\n## אזהרת סיכון - קרא לפני שאתה שוקל להפעיל\nזו לא אסטרטגיה שאומתה בשום צורה - יש להריץ backtest מקיף (שבועות-חודשים, מאות עסקאות) ולבחון paper trading ממושך לפני כל שיקול להעלות ל-LIVE. שימו לב גם לפשרת 1 דקה→5 דקות בכניסה: הדיוק בפועל נמוך יותר ממה שהסרטון המקורי מתאר, וה-R:R בפועל עלול להיות שונה מהמתוכנן.',
            ),
            'ORB Short (Opening Range Breakdown)': (
                "## מה זה עושה\nמראה הפוכה מדויקת של ORB Long (Opening Range Breakout) - ראו את התיאור המלא שם. כאן: אישור על נר 5 דקות שנסגר מתחת ל-OR Low, breakout/retest בכיוון ירידה, סטופ מעל שפל/שיא הנר הרלוונטי, יעד קבוע R:R 1:2 כלפי מטה.\n\n## יקום, פילטרים, חלון כניסות\nזהה לחלוטין ל-ORB Long: S&P 500 עם Market Cap מעל $1B, מחיר מינימלי $3, RVOL מעל 2.0, ATR% מדורג לפי מחיר, חלון כניסות 09:50-11:30 ET.\n\n## פרופיל סיכון\nדירוג: aggressive - אסטרטגיה חדשה שלא נבדקה. סיכון לעסקה: 1% | גודל פוזיציה מקס': 10% | פוזיציות בו-זמנית: עד 5\n\n## אזהרת סיכון - קרא לפני שאתה שוקל להפעיל\nבנוסף לכל אזהרות ORB Long (לא נבדקה, פשרת 1m→5m): בפוזיציית Short אין תקרה תיאורטית להפסד - מחיר המניה יכול לעלות ללא הגבלה, וה-stop עלול 'לקפוץ מעל' (gap) במקרה של short squeeze. אל תפעיל LIVE לפני בדיקה מקיפה.",
                '## מה זה עושה\nמראה הפוכה מדויקת של ORB Long (Opening Range Breakout) - ראו את התיאור המלא שם. כאן: אישור על נר 5 דקות שנסגר מתחת ל-OR Low, breakout/retest בכיוון ירידה, סטופ מעל שפל/שיא הנר הרלוונטי, יעד קבוע R:R 1:2 כלפי מטה.\n\n## יקום, פילטרים, חלון כניסות\nזהה לחלוטין ל-ORB Long: S&P 500 עם Market Cap מעל $1B, מחיר מינימלי $3, RVOL מעל 2.0, ATR% מדורג לפי מחיר, חלון כניסות 09:50-11:30 ET.\n\n## סינון כיוון שוק (ES VWAP)\nכניסה מתאפשרת רק כש-ES (חוזי E-mini S&P 500) נסחר **מתחת** ל-VWAP היומי שלו - "Market first, setup second": גם אם כל תנאי הכניסה מתקיימים, עסקה שסותרת את כיוון השוק הרחב נדחית. כבוי כברירת מחדל (דורש הרשאת נתוני CME futures + הפעלה מפורשת בעמוד Bot) - כל עוד הוא כבוי, או שאין גישה לנתוני ES, האסטרטגיה נשארת ללא סינון (fail-open), לא נחסמת.\n\n## פרופיל סיכון\nדירוג: aggressive - אסטרטגיה חדשה שלא נבדקה. סיכון לעסקה: 1% | גודל פוזיציה מקס\': 10% | פוזיציות בו-זמנית: עד 5\n\n## אזהרת סיכון - קרא לפני שאתה שוקל להפעיל\nבנוסף לכל אזהרות ORB Long (לא נבדקה, פשרת 1m→5m): בפוזיציית Short אין תקרה תיאורטית להפסד - מחיר המניה יכול לעלות ללא הגבלה, וה-stop עלול \'לקפוץ מעל\' (gap) במקרה של short squeeze. אל תפעיל LIVE לפני בדיקה מקיפה.',
            ),
            'ORB Long v2 (RSI/Trend Confluence, Staged Trail)': (
                "## מה זה עושה\nגרסה שנייה (v2) של ORB Long - שומרת על אותו מנגנון Opening Range Breakout (OR 15 דקות, אישור 5 דקות, breakout/retest) אבל עם שני שינויים משמעותיים: פילטרים נוספים לפני כניסה, ומנגנון יציאה שונה לגמרי. **נשמרת כאסטרטגיה נפרדת מ-ORB Long המקורית** (לא דריסה במקום) כדי לא לערבב את היסטוריית הבקטסטים של שתיהן תחת אותה זהות.\n\n## פילטר כניסה נוסף: RSI + מגמה\nבנוסף לכל תנאי ה-ORB המקוריים (OR, אישור, RVOL+ATR%), נדרש גם: RSI(14) עולה על פני 3 נרות רצופים אחרונים, **וגם** (EMA(20) על 5 דקות עולה **או** המחיר מעל ה-VWAP של היום). כל התנאים האלה חייבים להתקיים באותו נר שבו נכנסים.\n\n## יציאה: Staged Trail (במקום יעד קבוע)\nאין יותר יעד R:R קבוע - הסטופ ההתחלתי נשאר קבוע עד 2R, ואז עובר ל-Breakeven. כשמגיעים ל-3R, מתחיל טריילינג סטופ מתחת לשפל של שני הנרות האחרונים (5 דקות), ומתעדכן כל עוד הוא משתפר. הפוזיציה יכולה לרוץ הרבה מעבר ל-2R אם המניה ממשיכה.\n\n## יקום, פילטרי תנודתיות, חלון כניסות\nזהה ל-ORB Long המקורית: S&P 500 עם Market Cap מעל $1B, מחיר מינימלי $3, RVOL מעל 2.0, ATR% מדורג לפי מחיר, חלון כניסות 09:50-11:30 ET.\n\n## פרופיל סיכון\nדירוג: aggressive - אסטרטגיה חדשה לגמרי שלא נבדקה כלל (v1 המקורית לפחות עברה בקטסט ראשוני - זו עוד לא). סיכון לעסקה: 1% | גודל פוזיציה מקס': 10% | פוזיציות בו-זמנית: עד 5\n\n## אזהרת סיכון - קרא לפני שאתה שוקל להפעיל\nאין לזה שום היסטוריית בקטסט עדיין - כל אזהרות ORB Long המקורית תקפות כאן במלואן, ובנוסף: הפילטרים הנוספים (RSI+EMA/VWAP) מצמצמים עוד יותר את מספר העסקאות הפוטנציאליות, וה-Staged Trail טרם נבדק כלל מול הנתונים ההיסטוריים. הרץ בקטסט מקיף (שבועות-חודשים) לפני כל שיקול נוסף.",
                '## מה זה עושה\nגרסה שנייה (v2) של ORB Long - שומרת על אותו מנגנון Opening Range Breakout (OR 15 דקות, אישור 5 דקות, breakout/retest) אבל עם שני שינויים משמעותיים: פילטרים נוספים לפני כניסה, ומנגנון יציאה שונה לגמרי. **נשמרת כאסטרטגיה נפרדת מ-ORB Long המקורית** (לא דריסה במקום) כדי לא לערבב את היסטוריית הבקטסטים של שתיהן תחת אותה זהות.\n\n## פילטר כניסה נוסף: RSI + מגמה\nבנוסף לכל תנאי ה-ORB המקוריים (OR, אישור, RVOL+ATR%), נדרש גם: RSI(14) עולה על פני 3 נרות רצופים אחרונים, **וגם** (EMA(20) על 5 דקות עולה **או** המחיר מעל ה-VWAP של היום). כל התנאים האלה חייבים להתקיים באותו נר שבו נכנסים.\n\n## יציאה: Staged Trail (במקום יעד קבוע)\nאין יותר יעד R:R קבוע - הסטופ ההתחלתי נשאר קבוע עד 2R, ואז עובר ל-Breakeven. כשמגיעים ל-3R, מתחיל טריילינג סטופ מתחת לשפל של שני הנרות האחרונים (5 דקות), ומתעדכן כל עוד הוא משתפר. הפוזיציה יכולה לרוץ הרבה מעבר ל-2R אם המניה ממשיכה.\n\n## יקום, פילטרי תנודתיות, חלון כניסות\nזהה ל-ORB Long המקורית: S&P 500 עם Market Cap מעל $1B, מחיר מינימלי $3, RVOL מעל 2.0, ATR% מדורג לפי מחיר, חלון כניסות 09:50-11:30 ET.\n\n## סינון כיוון שוק (ES VWAP)\nכניסה מתאפשרת רק כש-ES (חוזי E-mini S&P 500) נסחר **מעל** ל-VWAP היומי שלו - שים לב: זה נפרד לגמרי מ-VWAP הסימבול עצמו שכבר מופיע למעלה כחלק מ-RSI+EMA/VWAP (זה בודק את המניה הספציפית, זה בודק את השוק הרחב). "Market first, setup second": גם אם כל תנאי הכניסה מתקיימים, עסקה שסותרת את כיוון השוק הרחב נדחית. כבוי כברירת מחדל (דורש הרשאת נתוני CME futures + הפעלה מפורשת בעמוד Bot) - כל עוד הוא כבוי, או שאין גישה לנתוני ES, האסטרטגיה נשארת ללא סינון (fail-open), לא נחסמת.\n\n## פרופיל סיכון\nדירוג: aggressive - אסטרטגיה חדשה לגמרי שלא נבדקה כלל (v1 המקורית לפחות עברה בקטסט ראשוני - זו עוד לא). סיכון לעסקה: 1% | גודל פוזיציה מקס\': 10% | פוזיציות בו-זמנית: עד 5\n\n## אזהרת סיכון - קרא לפני שאתה שוקל להפעיל\nאין לזה שום היסטוריית בקטסט עדיין - כל אזהרות ORB Long המקורית תקפות כאן במלואן, ובנוסף: הפילטרים הנוספים (RSI+EMA/VWAP) מצמצמים עוד יותר את מספר העסקאות הפוטנציאליות, וה-Staged Trail טרם נבדק כלל מול הנתונים ההיסטוריים. הרץ בקטסט מקיף (שבועות-חודשים) לפני כל שיקול נוסף.',
            ),
            'ORB Short v2 (RSI/Trend Confluence, Staged Trail)': (
                "## מה זה עושה\nמראה הפוכה מדויקת של ORB Long v2 - ראו את התיאור המלא שם. כאן: RSI(14) יורד על פני 3 נרות רצופים, וגם (EMA(20) יורד או המחיר מתחת ל-VWAP). סטופ קבוע עד 2R, Breakeven ב-2R, טריילינג מ-3R מעל השיא של שני הנרות האחרונים.\n\n## יקום, פילטרים, חלון כניסות\nזהה לחלוטין ל-ORB Short המקורית ול-ORB Long v2.\n\n## פרופיל סיכון\nדירוג: aggressive - אסטרטגיה חדשה לגמרי שלא נבדקה כלל. סיכון לעסקה: 1% | גודל פוזיציה מקס': 10% | פוזיציות בו-זמנית: עד 5\n\n## אזהרת סיכון - קרא לפני שאתה שוקל להפעיל\nכל אזהרות ORB Long v2 תקפות כאן, ובנוסף: בפוזיציית Short אין תקרה תיאורטית להפסד - מחיר המניה יכול לעלות ללא הגבלה, וה-stop עלול 'לקפוץ מעל' (gap) במקרה של short squeeze. אל תפעיל LIVE לפני בדיקה מקיפה.",
                '## מה זה עושה\nמראה הפוכה מדויקת של ORB Long v2 - ראו את התיאור המלא שם. כאן: RSI(14) יורד על פני 3 נרות רצופים, וגם (EMA(20) יורד או המחיר מתחת ל-VWAP). סטופ קבוע עד 2R, Breakeven ב-2R, טריילינג מ-3R מעל השיא של שני הנרות האחרונים.\n\n## יקום, פילטרים, חלון כניסות\nזהה לחלוטין ל-ORB Short המקורית ול-ORB Long v2.\n\n## סינון כיוון שוק (ES VWAP)\nכניסה מתאפשרת רק כש-ES (חוזי E-mini S&P 500) נסחר **מתחת** ל-VWAP היומי שלו - שים לב: זה נפרד לגמרי מ-VWAP הסימבול עצמו שכבר מופיע למעלה. "Market first, setup second": גם אם כל תנאי הכניסה מתקיימים, עסקה שסותרת את כיוון השוק הרחב נדחית. כבוי כברירת מחדל (דורש הרשאת נתוני CME futures + הפעלה מפורשת בעמוד Bot) - כל עוד הוא כבוי, או שאין גישה לנתוני ES, האסטרטגיה נשארת ללא סינון (fail-open), לא נחסמת.\n\n## פרופיל סיכון\nדירוג: aggressive - אסטרטגיה חדשה לגמרי שלא נבדקה כלל. סיכון לעסקה: 1% | גודל פוזיציה מקס\': 10% | פוזיציות בו-זמנית: עד 5\n\n## אזהרת סיכון - קרא לפני שאתה שוקל להפעיל\nכל אזהרות ORB Long v2 תקפות כאן, ובנוסף: בפוזיציית Short אין תקרה תיאורטית להפסד - מחיר המניה יכול לעלות ללא הגבלה, וה-stop עלול \'לקפוץ מעל\' (gap) במקרה של short squeeze. אל תפעיל LIVE לפני בדיקה מקיפה.',
            ),
            'Touch & Turn Scalper - Long': (
                "## מה זה עושה\nאסטרטגיית סקאלפינג יומית: נר הפתיחה הראשון (15 דקות, נבנה מ-3 נרות 5 דקות: 9:30-9:45 ET) לפעמים מייצג 'Liquidity Candle' - מהלך חריג ביחס לתנודתיות הרגילה של המניה. כשזה קורה, המחיר נוטה לחזור ולגעת ('Touch') בקצה הטווח של אותו נר ואז להתהפך ('Turn') בחזרה למרכז. וריאציה זו (Long) פועלת רק בימים שבהם נר הפתיחה **אדום** (בשפל).\n\n## שלב 1: זיהוי Liquidity Candle\nטווח נר הפתיחה (High-Low) מושווה ל-ATR(14) היומי × 0.25. אם הטווח קטן מהסף - אין עסקה היום בכלל, לא רק בסימבול הזה.\n\n## שלב 2: קביעת כיוון (Bias)\nנר אדום (סגירה ≤ פתיחה) → Bias=Long (וריאציה זו פועלת). נר ירוק → Bias=Short (וריאציה זו מדלגת - הפעילו את Touch & Turn Scalper - Short בצד ה-Short כדי לכסות גם ימים כאלה).\n\n## שלב 3: כניסה - הזמנת Limit אמיתית\n**שונה מכל אסטרטגיה אחרת כאן**: לא קונה בשוק ברגע שהתנאים מתקיימים - מציבה הזמנת Buy Limit אמיתית ב-IBKR בשפל נר הפתיחה, וממתינה. ההזמנה משתמשת ב-GTD (Good-Till-Date) של IBKR עצמו כדי להתבטל אוטומטית אחרי 90 דקות (11:00 ET) אם לא מולאה - הבוט גם בודק זאת בעצמו כגיבוי. עד שהיא נמלאת או מתבטלת, זו לא פוזיציה עוקבת (stop/target) - רק הזמנה רדומה בשוק. מקסימום ניסיון אחד לסימבול ביום.\n\n## יעד ה-Take Profit וה-Stop\nיעד: רמת Fibonacci 38.2% (נמדד מהשיא של נר הפתיחה כלפי השפל). הסיכון מחושב **מהיעד**, לא מרמה טכנית: Reward = מרחק מהכניסה ליעד, Risk = Reward ÷ 2 (יחס 2:1), Stop = כניסה פחות Risk. לאחר מילוי - יעד קבוע בלבד, ללא breakeven וללא טריילינג (management_style: fixed_target_no_trail, כמו ORB).\n\n## חלון זמן\nרק ב-90 הדקות הראשונות של המסחר (9:30-11:00 ET) - גם להצבת ההזמנה וגם לתוקף שלה.\n\n## פרופיל סיכון\nדירוג: aggressive - אסטרטגיה חדשה שלא נבדקה, ומשתמשת במנגנון הזמנה שלא קיים באף אסטרטגיה אחרת (Limit אמיתי, לא Market). סיכון לעסקה: 1% | גודל פוזיציה מקס': 10% | פוזיציות בו-זמנית: עד 5\n\n## אזהרת סיכון - קרא לפני שאתה שוקל להפעיל\nזו לא אסטרטגיה שאומתה בשום צורה - יש להריץ backtest מקיף (שבועות-חודשים, מאות עסקאות) ולבחון paper trading ממושך לפני כל שיקול להעלות ל-LIVE. שימו לב שה-Stop כאן נגזר מיחס הסיכוי/סיכון ולא מרמה טכנית קונקרטית - זה עלול להניח סטופ קרוב מדי או רחוק מדי ביחס לתנודתיות בפועל של המניה.",
                '## מה זה עושה\nאסטרטגיית סקאלפינג יומית: נר הפתיחה הראשון (15 דקות, נבנה מ-3 נרות 5 דקות: 9:30-9:45 ET) לפעמים מייצג \'Liquidity Candle\' - מהלך חריג ביחס לתנודתיות הרגילה של המניה. כשזה קורה, המחיר נוטה לחזור ולגעת (\'Touch\') בקצה הטווח של אותו נר ואז להתהפך (\'Turn\') בחזרה למרכז. וריאציה זו (Long) פועלת רק בימים שבהם נר הפתיחה **אדום** (בשפל).\n\n## שלב 1: זיהוי Liquidity Candle\nטווח נר הפתיחה (High-Low) מושווה ל-ATR(14) היומי × 0.25. אם הטווח קטן מהסף - אין עסקה היום בכלל, לא רק בסימבול הזה.\n\n## שלב 2: קביעת כיוון (Bias)\nנר אדום (סגירה ≤ פתיחה) → Bias=Long (וריאציה זו פועלת). נר ירוק → Bias=Short (וריאציה זו מדלגת - הפעילו את Touch & Turn Scalper - Short בצד ה-Short כדי לכסות גם ימים כאלה).\n\n## שלב 3: כניסה - הזמנת Limit אמיתית\n**שונה מכל אסטרטגיה אחרת כאן**: לא קונה בשוק ברגע שהתנאים מתקיימים - מציבה הזמנת Buy Limit אמיתית ב-IBKR בשפל נר הפתיחה, וממתינה. ההזמנה משתמשת ב-GTD (Good-Till-Date) של IBKR עצמו כדי להתבטל אוטומטית אחרי 90 דקות (11:00 ET) אם לא מולאה - הבוט גם בודק זאת בעצמו כגיבוי. עד שהיא נמלאת או מתבטלת, זו לא פוזיציה עוקבת (stop/target) - רק הזמנה רדומה בשוק. מקסימום ניסיון אחד לסימבול ביום.\n\n## יעד ה-Take Profit וה-Stop\nיעד: רמת Fibonacci 38.2% (נמדד מהשיא של נר הפתיחה כלפי השפל). הסיכון מחושב **מהיעד**, לא מרמה טכנית: Reward = מרחק מהכניסה ליעד, Risk = Reward ÷ 2 (יחס 2:1), Stop = כניסה פחות Risk. לאחר מילוי - יעד קבוע בלבד, ללא breakeven וללא טריילינג (management_style: fixed_target_no_trail, כמו ORB).\n\n## חלון זמן\nרק ב-90 הדקות הראשונות של המסחר (9:30-11:00 ET) - גם להצבת ההזמנה וגם לתוקף שלה.\n\n## סינון כיוון שוק (ES VWAP)\nכניסה מתאפשרת רק כש-ES (חוזי E-mini S&P 500) נסחר **מעל** ל-VWAP היומי שלו - "Market first, setup second": גם אם כל תנאי הכניסה מתקיימים, עסקה שסותרת את כיוון השוק הרחב נדחית. כבוי כברירת מחדל (דורש הרשאת נתוני CME futures + הפעלה מפורשת בעמוד Bot) - כל עוד הוא כבוי, או שאין גישה לנתוני ES, האסטרטגיה נשארת ללא סינון (fail-open), לא נחסמת.\n\n## פרופיל סיכון\nדירוג: aggressive - אסטרטגיה חדשה שלא נבדקה, ומשתמשת במנגנון הזמנה שלא קיים באף אסטרטגיה אחרת (Limit אמיתי, לא Market). סיכון לעסקה: 1% | גודל פוזיציה מקס\': 10% | פוזיציות בו-זמנית: עד 5\n\n## אזהרת סיכון - קרא לפני שאתה שוקל להפעיל\nזו לא אסטרטגיה שאומתה בשום צורה - יש להריץ backtest מקיף (שבועות-חודשים, מאות עסקאות) ולבחון paper trading ממושך לפני כל שיקול להעלות ל-LIVE. שימו לב שה-Stop כאן נגזר מיחס הסיכוי/סיכון ולא מרמה טכנית קונקרטית - זה עלול להניח סטופ קרוב מדי או רחוק מדי ביחס לתנודתיות בפועל של המניה.',
            ),
            'Touch & Turn Scalper - Short': (
                "## מה זה עושה\nמראה הפוכה מדויקת של Touch & Turn Scalper - Long - ראו את התיאור המלא שם. כאן: פועלת רק בימים שבהם נר הפתיחה **ירוק** (Bias=Short), מציבה Sell Limit אמיתי ב-IBKR בשיא נר הפתיחה וממתינה למגע חוזר, יעד ב-Fibonacci 61.8% (קרוב יותר לשפל), Stop = כניסה + Reward÷2.\n\n## יקום, זיהוי Liquidity Candle, חלון זמן\nזהה לחלוטין ל-Touch & Turn Scalper - Long: S&P 500, ATR(14)×0.25, חלון 9:30-11:00 ET, מקסימום ניסיון אחד לסימבול ביום, הזמנת Limit אמיתית עם GTD.\n\n## פרופיל סיכון\nדירוג: aggressive - אסטרטגיה חדשה שלא נבדקה. סיכון לעסקה: 1% | גודל פוזיציה מקס': 10% | פוזיציות בו-זמנית: עד 5\n\n## אזהרת סיכון - קרא לפני שאתה שוקל להפעיל\nכל אזהרות Touch & Turn Scalper - Long תקפות כאן, ובנוסף: בפוזיציית Short אין תקרה תיאורטית להפסד - מחיר המניה יכול לעלות ללא הגבלה. אל תפעיל LIVE לפני בדיקה מקיפה.",
                '## מה זה עושה\nמראה הפוכה מדויקת של Touch & Turn Scalper - Long - ראו את התיאור המלא שם. כאן: פועלת רק בימים שבהם נר הפתיחה **ירוק** (Bias=Short), מציבה Sell Limit אמיתי ב-IBKR בשיא נר הפתיחה וממתינה למגע חוזר, יעד ב-Fibonacci 61.8% (קרוב יותר לשפל), Stop = כניסה + Reward÷2.\n\n## יקום, זיהוי Liquidity Candle, חלון זמן\nזהה לחלוטין ל-Touch & Turn Scalper - Long: S&P 500, ATR(14)×0.25, חלון 9:30-11:00 ET, מקסימום ניסיון אחד לסימבול ביום, הזמנת Limit אמיתית עם GTD.\n\n## סינון כיוון שוק (ES VWAP)\nכניסה מתאפשרת רק כש-ES (חוזי E-mini S&P 500) נסחר **מתחת** ל-VWAP היומי שלו - "Market first, setup second": גם אם כל תנאי הכניסה מתקיימים, עסקה שסותרת את כיוון השוק הרחב נדחית. כבוי כברירת מחדל (דורש הרשאת נתוני CME futures + הפעלה מפורשת בעמוד Bot) - כל עוד הוא כבוי, או שאין גישה לנתוני ES, האסטרטגיה נשארת ללא סינון (fail-open), לא נחסמת.\n\n## פרופיל סיכון\nדירוג: aggressive - אסטרטגיה חדשה שלא נבדקה. סיכון לעסקה: 1% | גודל פוזיציה מקס\': 10% | פוזיציות בו-זמנית: עד 5\n\n## אזהרת סיכון - קרא לפני שאתה שוקל להפעיל\nכל אזהרות Touch & Turn Scalper - Long תקפות כאן, ובנוסף: בפוזיציית Short אין תקרה תיאורטית להפסד - מחיר המניה יכול לעלות ללא הגבלה. אל תפעיל LIVE לפני בדיקה מקיפה.',
            ),
        }
        for _name, (_old_text, _new_text) in _ES_VWAP_FILTER_DESCRIPTIONS.items():
            conn.execute(
                "UPDATE strategies SET description = ? WHERE name = ? AND description = ?",
                (_new_text, _name, _old_text),
            )

        # One-time migration updating ORB Long v2 / ORB Short v2's own
        # description text for the MFE-based profit-lock change above (see
        # _PROFIT_LOCK_STRATEGY_NAMES migration, which only touches
        # rules_json, not description) - same exact-old-text-match
        # convention as the other description migrations above.
        _PROFIT_LOCK_DESCRIPTIONS = {
            'ORB Long v2 (RSI/Trend Confluence, Staged Trail)': (
                '## מה זה עושה\nגרסה שנייה (v2) של ORB Long - שומרת על אותו מנגנון Opening Range Breakout (OR 15 דקות, אישור 5 דקות, breakout/retest) אבל עם שני שינויים משמעותיים: פילטרים נוספים לפני כניסה, ומנגנון יציאה שונה לגמרי. **נשמרת כאסטרטגיה נפרדת מ-ORB Long המקורית** (לא דריסה במקום) כדי לא לערבב את היסטוריית הבקטסטים של שתיהן תחת אותה זהות.\n\n## פילטר כניסה נוסף: RSI + מגמה\nבנוסף לכל תנאי ה-ORB המקוריים (OR, אישור, RVOL+ATR%), נדרש גם: RSI(14) עולה על פני 3 נרות רצופים אחרונים, **וגם** (EMA(20) על 5 דקות עולה **או** המחיר מעל ה-VWAP של היום). כל התנאים האלה חייבים להתקיים באותו נר שבו נכנסים.\n\n## יציאה: Staged Trail (במקום יעד קבוע)\nאין יותר יעד R:R קבוע - הסטופ ההתחלתי נשאר קבוע עד 2R, ואז עובר ל-Breakeven. כשמגיעים ל-3R, מתחיל טריילינג סטופ מתחת לשפל של שני הנרות האחרונים (5 דקות), ומתעדכן כל עוד הוא משתפר. הפוזיציה יכולה לרוץ הרבה מעבר ל-2R אם המניה ממשיכה.\n\n## יקום, פילטרי תנודתיות, חלון כניסות\nזהה ל-ORB Long המקורית: S&P 500 עם Market Cap מעל $1B, מחיר מינימלי $3, RVOL מעל 2.0, ATR% מדורג לפי מחיר, חלון כניסות 09:50-11:30 ET.\n\n## סינון כיוון שוק (ES VWAP)\nכניסה מתאפשרת רק כש-ES (חוזי E-mini S&P 500) נסחר **מעל** ל-VWAP היומי שלו - שים לב: זה נפרד לגמרי מ-VWAP הסימבול עצמו שכבר מופיע למעלה כחלק מ-RSI+EMA/VWAP (זה בודק את המניה הספציפית, זה בודק את השוק הרחב). "Market first, setup second": גם אם כל תנאי הכניסה מתקיימים, עסקה שסותרת את כיוון השוק הרחב נדחית. כבוי כברירת מחדל (דורש הרשאת נתוני CME futures + הפעלה מפורשת בעמוד Bot) - כל עוד הוא כבוי, או שאין גישה לנתוני ES, האסטרטגיה נשארת ללא סינון (fail-open), לא נחסמת.\n\n## פרופיל סיכון\nדירוג: aggressive - אסטרטגיה חדשה לגמרי שלא נבדקה כלל (v1 המקורית לפחות עברה בקטסט ראשוני - זו עוד לא). סיכון לעסקה: 1% | גודל פוזיציה מקס\': 10% | פוזיציות בו-זמנית: עד 5\n\n## אזהרת סיכון - קרא לפני שאתה שוקל להפעיל\nאין לזה שום היסטוריית בקטסט עדיין - כל אזהרות ORB Long המקורית תקפות כאן במלואן, ובנוסף: הפילטרים הנוספים (RSI+EMA/VWAP) מצמצמים עוד יותר את מספר העסקאות הפוטנציאליות, וה-Staged Trail טרם נבדק כלל מול הנתונים ההיסטוריים. הרץ בקטסט מקיף (שבועות-חודשים) לפני כל שיקול נוסף.',
                '## מה זה עושה\nגרסה שנייה (v2) של ORB Long - שומרת על אותו מנגנון Opening Range Breakout (OR 15 דקות, אישור 5 דקות, breakout/retest) אבל עם שני שינויים משמעותיים: פילטרים נוספים לפני כניסה, ומנגנון יציאה שונה לגמרי. **נשמרת כאסטרטגיה נפרדת מ-ORB Long המקורית** (לא דריסה במקום) כדי לא לערבב את היסטוריית הבקטסטים של שתיהן תחת אותה זהות.\n\n## פילטר כניסה נוסף: RSI + מגמה\nבנוסף לכל תנאי ה-ORB המקוריים (OR, אישור, RVOL+ATR%), נדרש גם: RSI(14) עולה על פני 3 נרות רצופים אחרונים, **וגם** (EMA(20) על 5 דקות עולה **או** המחיר מעל ה-VWAP של היום). כל התנאים האלה חייבים להתקיים באותו נר שבו נכנסים.\n\n## יציאה: Staged Trail (במקום יעד קבוע)\nאין יותר יעד R:R קבוע - הסטופ ההתחלתי נשאר קבוע עד שה-MFE (השיא שהמחיר בפועל נגע בו, לא רק מחיר הסגירה) מגיע ל-2R, ואז עובר ל-**Profit Lock: 0.25R** (לא ל-Breakeven שטוח) - כלומר גם עסקה שנגעה ב-2R תוך-יומית וחזרה אחורה, ננעלת עם רווח קטן במקום להסתכן בחזרה לסטופ המקורי. כשמגיעים ל-3R (סגירת נר), מתחיל טריילינג סטופ מתחת לשפל של שני הנרות האחרונים (5 דקות), ומתעדכן כל עוד הוא משתפר. הפוזיציה יכולה לרוץ הרבה מעבר ל-2R אם המניה ממשיכה.\n\n## יקום, פילטרי תנודתיות, חלון כניסות\nזהה ל-ORB Long המקורית: S&P 500 עם Market Cap מעל $1B, מחיר מינימלי $3, RVOL מעל 2.0, ATR% מדורג לפי מחיר, חלון כניסות 09:50-11:30 ET.\n\n## סינון כיוון שוק (ES VWAP)\nכניסה מתאפשרת רק כש-ES (חוזי E-mini S&P 500) נסחר **מעל** ל-VWAP היומי שלו - שים לב: זה נפרד לגמרי מ-VWAP הסימבול עצמו שכבר מופיע למעלה כחלק מ-RSI+EMA/VWAP (זה בודק את המניה הספציפית, זה בודק את השוק הרחב). "Market first, setup second": גם אם כל תנאי הכניסה מתקיימים, עסקה שסותרת את כיוון השוק הרחב נדחית. כבוי כברירת מחדל (דורש הרשאת נתוני CME futures + הפעלה מפורשת בעמוד Bot) - כל עוד הוא כבוי, או שאין גישה לנתוני ES, האסטרטגיה נשארת ללא סינון (fail-open), לא נחסמת.\n\n## פרופיל סיכון\nדירוג: aggressive - אסטרטגיה חדשה לגמרי שלא נבדקה כלל (v1 המקורית לפחות עברה בקטסט ראשוני - זו עוד לא). סיכון לעסקה: 1% | גודל פוזיציה מקס\': 10% | פוזיציות בו-זמנית: עד 5\n\n## אזהרת סיכון - קרא לפני שאתה שוקל להפעיל\nאין לזה שום היסטוריית בקטסט עדיין - כל אזהרות ORB Long המקורית תקפות כאן במלואן, ובנוסף: הפילטרים הנוספים (RSI+EMA/VWAP) מצמצמים עוד יותר את מספר העסקאות הפוטנציאליות, וה-Staged Trail טרם נבדק כלל מול הנתונים ההיסטוריים. הרץ בקטסט מקיף (שבועות-חודשים) לפני כל שיקול נוסף.',
            ),
            'ORB Short v2 (RSI/Trend Confluence, Staged Trail)': (
                '## מה זה עושה\nמראה הפוכה מדויקת של ORB Long v2 - ראו את התיאור המלא שם. כאן: RSI(14) יורד על פני 3 נרות רצופים, וגם (EMA(20) יורד או המחיר מתחת ל-VWAP). סטופ קבוע עד 2R, Breakeven ב-2R, טריילינג מ-3R מעל השיא של שני הנרות האחרונים.\n\n## יקום, פילטרים, חלון כניסות\nזהה לחלוטין ל-ORB Short המקורית ול-ORB Long v2.\n\n## סינון כיוון שוק (ES VWAP)\nכניסה מתאפשרת רק כש-ES (חוזי E-mini S&P 500) נסחר **מתחת** ל-VWAP היומי שלו - שים לב: זה נפרד לגמרי מ-VWAP הסימבול עצמו שכבר מופיע למעלה. "Market first, setup second": גם אם כל תנאי הכניסה מתקיימים, עסקה שסותרת את כיוון השוק הרחב נדחית. כבוי כברירת מחדל (דורש הרשאת נתוני CME futures + הפעלה מפורשת בעמוד Bot) - כל עוד הוא כבוי, או שאין גישה לנתוני ES, האסטרטגיה נשארת ללא סינון (fail-open), לא נחסמת.\n\n## פרופיל סיכון\nדירוג: aggressive - אסטרטגיה חדשה לגמרי שלא נבדקה כלל. סיכון לעסקה: 1% | גודל פוזיציה מקס\': 10% | פוזיציות בו-זמנית: עד 5\n\n## אזהרת סיכון - קרא לפני שאתה שוקל להפעיל\nכל אזהרות ORB Long v2 תקפות כאן, ובנוסף: בפוזיציית Short אין תקרה תיאורטית להפסד - מחיר המניה יכול לעלות ללא הגבלה, וה-stop עלול \'לקפוץ מעל\' (gap) במקרה של short squeeze. אל תפעיל LIVE לפני בדיקה מקיפה.',
                '## מה זה עושה\nמראה הפוכה מדויקת של ORB Long v2 - ראו את התיאור המלא שם. כאן: RSI(14) יורד על פני 3 נרות רצופים, וגם (EMA(20) יורד או המחיר מתחת ל-VWAP). סטופ קבוע עד MFE 2R (השיא/שפל שהמחיר בפועל נגע בו, לא רק סגירה), ואז Profit Lock 0.25R, טריילינג מ-3R מעל השיא של שני הנרות האחרונים.\n\n## יקום, פילטרים, חלון כניסות\nזהה לחלוטין ל-ORB Short המקורית ול-ORB Long v2.\n\n## סינון כיוון שוק (ES VWAP)\nכניסה מתאפשרת רק כש-ES (חוזי E-mini S&P 500) נסחר **מתחת** ל-VWAP היומי שלו - שים לב: זה נפרד לגמרי מ-VWAP הסימבול עצמו שכבר מופיע למעלה. "Market first, setup second": גם אם כל תנאי הכניסה מתקיימים, עסקה שסותרת את כיוון השוק הרחב נדחית. כבוי כברירת מחדל (דורש הרשאת נתוני CME futures + הפעלה מפורשת בעמוד Bot) - כל עוד הוא כבוי, או שאין גישה לנתוני ES, האסטרטגיה נשארת ללא סינון (fail-open), לא נחסמת.\n\n## פרופיל סיכון\nדירוג: aggressive - אסטרטגיה חדשה לגמרי שלא נבדקה כלל. סיכון לעסקה: 1% | גודל פוזיציה מקס\': 10% | פוזיציות בו-זמנית: עד 5\n\n## אזהרת סיכון - קרא לפני שאתה שוקל להפעיל\nכל אזהרות ORB Long v2 תקפות כאן, ובנוסף: בפוזיציית Short אין תקרה תיאורטית להפסד - מחיר המניה יכול לעלות ללא הגבלה, וה-stop עלול \'לקפוץ מעל\' (gap) במקרה של short squeeze. אל תפעיל LIVE לפני בדיקה מקיפה.',
            ),
        }
        for _name, (_old_text, _new_text) in _PROFIT_LOCK_DESCRIPTIONS.items():
            conn.execute(
                "UPDATE strategies SET description = ? WHERE name = ? AND description = ?",
                (_new_text, _name, _old_text),
            )

        # One-time migration inserting the new "## רצפת מרחק סטופ מינימלית"
        # description section into all 6 ORB strategies' description text (see
        # _MIN_STOP_DISTANCE_STRATEGY_NAMES migration above, which only touches
        # rules_json, not description) - same exact-old-text-match convention as
        # the other description migrations above.
        _MIN_STOP_DISTANCE_DESCRIPTIONS = {
            'ORB Long (Opening Range Breakout)': (
                '## מה זה עושה\nאסטרטגיה מבוססת Opening Range Breakout (ORB): לא בודקת דעה מקדימה מהיום הקודם (אין daily_filters בכלל) - כל יום מתחיל מאפס. סוחרת רק את הנר הראשון של פתיחת המסחר בניו יורק (9:30 ET), מחכה לאישור פריצה, ואז מחפשת כניסה להמשך התנועה. מקור: תמלול סרטון YouTube (bITIVwysCzM) - ראו docs/orb_strategy_spec.md למפרט המלא ולתהליך ההגדרה.\n\n## יקום\nS&P 500 בלבד, מסונן מראש למניות עם Market Cap מעל $1B (custom_universe: sp500_marketcap_1b, נבנה על ידי build_custom_universe.py - כמו Long Breakout NASDAQ Beta) ומחיר מינימלי $3.\n\n## מנגנון ה-Opening Range\n1. סימון High/Low של 3 נרות 5 דקות ראשונים מ-9:30 ET (= \'נר\' 15 דקות) - זה ה-Opening Range.\n2. אישור: נר 5 דקות שנסגר מעל ה-OR High.\n3. כניסה: על אותה מסגרת 5 דקות (**לא 1 דקה כמו בסרטון המקורי** - פשרה כי אין נתוני 1 דקה בתשתית ה-backtest הקיימת, ראו הערה בקובץ המפרט).\n\n## פילטרים לפני כניסה\nRVOL מעל 2.0 (חלון 14 ימים) ו-ATR% (יחסי למחיר, לא אבסולוטי) לפי מדרגת מחיר: $3-20 מעל 4%, $20-50 מעל 3%, $50-100 מעל 2%, מעל $100 מעל 1.5%.\n\n## מודלי כניסה (2 מתוך 3 בסרטון המקורי - Reversal הוסר מהיקף)\n**Breakout**: רק על נר האישור עצמו, ורק אם יש \'gap\' (displacement) בינו לנר הקודם - כניסה בסגירת הנר, סטופ בשפל/שיא אותו נר.\n**Retest**: נר כלשהו אחרי האישור שנוגע בחזרה ברמת ה-OR ונסגר בחזרה בכיוון הפריצה - כניסה בסגירת הנר, סטופ בשפל/שיא אותו נר.\n\n## יציאה וניהול פוזיציה (שונה מכל שאר האסטרטגיות בפרויקט)\nאין breakeven flip ואין טריילינג סטופ - הסטופ ההתחלתי (משלב הכניסה) נשאר קבוע כל הפוזיציה. יעד קבוע R:R = 1:2: יציאה מלאה ביעד או בסטופ, מה שמגיע קודם.\n\n## חלון כניסות\n09:50-11:30 ET בלבד (השעתיים הראשונות של המסחר, כפי שממליץ הסרטון) - force close רגיל ב-15:51 ET לכל פוזיציה שעדיין פתוחה.\n\n## סינון כיוון שוק (ES VWAP)\nכניסה מתאפשרת רק כש-ES (חוזי E-mini S&P 500) נסחר **מעל** ל-VWAP היומי שלו - "Market first, setup second": גם אם כל תנאי הכניסה מתקיימים, עסקה שסותרת את כיוון השוק הרחב נדחית. כבוי כברירת מחדל (דורש הרשאת נתוני CME futures + הפעלה מפורשת בעמוד Bot) - כל עוד הוא כבוי, או שאין גישה לנתוני ES, האסטרטגיה נשארת ללא סינון (fail-open), לא נחסמת.\n\n## פרופיל סיכון\nדירוג: aggressive - אסטרטגיה חדשה שלא נבדקה (לא backtest, לא paper trading) - הפעלתה דורשת הקלדת אישור כי היא חלה על LIVE מיידית. סיכון לעסקה: 1% | גודל פוזיציה מקס\': 10% | פוזיציות בו-זמנית: עד 5\n\n## אזהרת סיכון - קרא לפני שאתה שוקל להפעיל\nזו לא אסטרטגיה שאומתה בשום צורה - יש להריץ backtest מקיף (שבועות-חודשים, מאות עסקאות) ולבחון paper trading ממושך לפני כל שיקול להעלות ל-LIVE. שימו לב גם לפשרת 1 דקה→5 דקות בכניסה: הדיוק בפועל נמוך יותר ממה שהסרטון המקורי מתאר, וה-R:R בפועל עלול להיות שונה מהמתוכנן.',
                '## מה זה עושה\nאסטרטגיה מבוססת Opening Range Breakout (ORB): לא בודקת דעה מקדימה מהיום הקודם (אין daily_filters בכלל) - כל יום מתחיל מאפס. סוחרת רק את הנר הראשון של פתיחת המסחר בניו יורק (9:30 ET), מחכה לאישור פריצה, ואז מחפשת כניסה להמשך התנועה. מקור: תמלול סרטון YouTube (bITIVwysCzM) - ראו docs/orb_strategy_spec.md למפרט המלא ולתהליך ההגדרה.\n\n## יקום\nS&P 500 בלבד, מסונן מראש למניות עם Market Cap מעל $1B (custom_universe: sp500_marketcap_1b, נבנה על ידי build_custom_universe.py - כמו Long Breakout NASDAQ Beta) ומחיר מינימלי $3.\n\n## מנגנון ה-Opening Range\n1. סימון High/Low של 3 נרות 5 דקות ראשונים מ-9:30 ET (= \'נר\' 15 דקות) - זה ה-Opening Range.\n2. אישור: נר 5 דקות שנסגר מעל ה-OR High.\n3. כניסה: על אותה מסגרת 5 דקות (**לא 1 דקה כמו בסרטון המקורי** - פשרה כי אין נתוני 1 דקה בתשתית ה-backtest הקיימת, ראו הערה בקובץ המפרט).\n\n## פילטרים לפני כניסה\nRVOL מעל 2.0 (חלון 14 ימים) ו-ATR% (יחסי למחיר, לא אבסולוטי) לפי מדרגת מחיר: $3-20 מעל 4%, $20-50 מעל 3%, $50-100 מעל 2%, מעל $100 מעל 1.5%.\n\n## מודלי כניסה (2 מתוך 3 בסרטון המקורי - Reversal הוסר מהיקף)\n**Breakout**: רק על נר האישור עצמו, ורק אם יש \'gap\' (displacement) בינו לנר הקודם - כניסה בסגירת הנר, סטופ בשפל/שיא אותו נר.\n**Retest**: נר כלשהו אחרי האישור שנוגע בחזרה ברמת ה-OR ונסגר בחזרה בכיוון הפריצה - כניסה בסגירת הנר, סטופ בשפל/שיא אותו נר.\n\n## יציאה וניהול פוזיציה (שונה מכל שאר האסטרטגיות בפרויקט)\nאין breakeven flip ואין טריילינג סטופ - הסטופ ההתחלתי (משלב הכניסה) נשאר קבוע כל הפוזיציה. יעד קבוע R:R = 1:2: יציאה מלאה ביעד או בסטופ, מה שמגיע קודם.\n\n## חלון כניסות\n09:50-11:30 ET בלבד (השעתיים הראשונות של המסחר, כפי שממליץ הסרטון) - force close רגיל ב-15:51 ET לכל פוזיציה שעדיין פתוחה.\n\n## סינון כיוון שוק (ES VWAP)\nכניסה מתאפשרת רק כש-ES (חוזי E-mini S&P 500) נסחר **מעל** ל-VWAP היומי שלו - "Market first, setup second": גם אם כל תנאי הכניסה מתקיימים, עסקה שסותרת את כיוון השוק הרחב נדחית. כבוי כברירת מחדל (דורש הרשאת נתוני CME futures + הפעלה מפורשת בעמוד Bot) - כל עוד הוא כבוי, או שאין גישה לנתוני ES, האסטרטגיה נשארת ללא סינון (fail-open), לא נחסמת.\n\n## רצפת מרחק סטופ מינימלית\nהסטופ הטכני (שפל/שיא נר האישור) לפעמים קרוב מדי לכניסה (נר כמעט ללא צל) - זה מייצר גודל פוזיציה מנופח מדי וערכי R מנופחים מדי שלא משקפים סיכון אמיתי (ראו חקירה בהיסטוריית השיחה). לכן מרחק הסטופ מהכניסה מוגבל מלמטה ל-0.25% ממחיר הכניסה (min_stop_distance_pct) - אם הסטופ הטכני קרוב יותר, הוא מורחב לרצפה הזו; סטופ טכני שכבר רחוק מספיק לא משתנה כלל.\n\n## פרופיל סיכון\nדירוג: aggressive - אסטרטגיה חדשה שלא נבדקה (לא backtest, לא paper trading) - הפעלתה דורשת הקלדת אישור כי היא חלה על LIVE מיידית. סיכון לעסקה: 1% | גודל פוזיציה מקס\': 10% | פוזיציות בו-זמנית: עד 5\n\n## אזהרת סיכון - קרא לפני שאתה שוקל להפעיל\nזו לא אסטרטגיה שאומתה בשום צורה - יש להריץ backtest מקיף (שבועות-חודשים, מאות עסקאות) ולבחון paper trading ממושך לפני כל שיקול להעלות ל-LIVE. שימו לב גם לפשרת 1 דקה→5 דקות בכניסה: הדיוק בפועל נמוך יותר ממה שהסרטון המקורי מתאר, וה-R:R בפועל עלול להיות שונה מהמתוכנן.',
            ),
            'ORB Short (Opening Range Breakdown)': (
                '## מה זה עושה\nמראה הפוכה מדויקת של ORB Long (Opening Range Breakout) - ראו את התיאור המלא שם. כאן: אישור על נר 5 דקות שנסגר מתחת ל-OR Low, breakout/retest בכיוון ירידה, סטופ מעל שפל/שיא הנר הרלוונטי, יעד קבוע R:R 1:2 כלפי מטה.\n\n## יקום, פילטרים, חלון כניסות\nזהה לחלוטין ל-ORB Long: S&P 500 עם Market Cap מעל $1B, מחיר מינימלי $3, RVOL מעל 2.0, ATR% מדורג לפי מחיר, חלון כניסות 09:50-11:30 ET.\n\n## סינון כיוון שוק (ES VWAP)\nכניסה מתאפשרת רק כש-ES (חוזי E-mini S&P 500) נסחר **מתחת** ל-VWAP היומי שלו - "Market first, setup second": גם אם כל תנאי הכניסה מתקיימים, עסקה שסותרת את כיוון השוק הרחב נדחית. כבוי כברירת מחדל (דורש הרשאת נתוני CME futures + הפעלה מפורשת בעמוד Bot) - כל עוד הוא כבוי, או שאין גישה לנתוני ES, האסטרטגיה נשארת ללא סינון (fail-open), לא נחסמת.\n\n## פרופיל סיכון\nדירוג: aggressive - אסטרטגיה חדשה שלא נבדקה. סיכון לעסקה: 1% | גודל פוזיציה מקס\': 10% | פוזיציות בו-זמנית: עד 5\n\n## אזהרת סיכון - קרא לפני שאתה שוקל להפעיל\nבנוסף לכל אזהרות ORB Long (לא נבדקה, פשרת 1m→5m): בפוזיציית Short אין תקרה תיאורטית להפסד - מחיר המניה יכול לעלות ללא הגבלה, וה-stop עלול \'לקפוץ מעל\' (gap) במקרה של short squeeze. אל תפעיל LIVE לפני בדיקה מקיפה.',
                '## מה זה עושה\nמראה הפוכה מדויקת של ORB Long (Opening Range Breakout) - ראו את התיאור המלא שם. כאן: אישור על נר 5 דקות שנסגר מתחת ל-OR Low, breakout/retest בכיוון ירידה, סטופ מעל שפל/שיא הנר הרלוונטי, יעד קבוע R:R 1:2 כלפי מטה.\n\n## יקום, פילטרים, חלון כניסות\nזהה לחלוטין ל-ORB Long: S&P 500 עם Market Cap מעל $1B, מחיר מינימלי $3, RVOL מעל 2.0, ATR% מדורג לפי מחיר, חלון כניסות 09:50-11:30 ET.\n\n## סינון כיוון שוק (ES VWAP)\nכניסה מתאפשרת רק כש-ES (חוזי E-mini S&P 500) נסחר **מתחת** ל-VWAP היומי שלו - "Market first, setup second": גם אם כל תנאי הכניסה מתקיימים, עסקה שסותרת את כיוון השוק הרחב נדחית. כבוי כברירת מחדל (דורש הרשאת נתוני CME futures + הפעלה מפורשת בעמוד Bot) - כל עוד הוא כבוי, או שאין גישה לנתוני ES, האסטרטגיה נשארת ללא סינון (fail-open), לא נחסמת.\n\n## רצפת מרחק סטופ מינימלית\nהסטופ הטכני (שפל/שיא נר האישור) לפעמים קרוב מדי לכניסה (נר כמעט ללא צל) - זה מייצר גודל פוזיציה מנופח מדי וערכי R מנופחים מדי שלא משקפים סיכון אמיתי (ראו חקירה בהיסטוריית השיחה). לכן מרחק הסטופ מהכניסה מוגבל מלמטה ל-0.25% ממחיר הכניסה (min_stop_distance_pct) - אם הסטופ הטכני קרוב יותר, הוא מורחב לרצפה הזו; סטופ טכני שכבר רחוק מספיק לא משתנה כלל.\n\n## פרופיל סיכון\nדירוג: aggressive - אסטרטגיה חדשה שלא נבדקה. סיכון לעסקה: 1% | גודל פוזיציה מקס\': 10% | פוזיציות בו-זמנית: עד 5\n\n## אזהרת סיכון - קרא לפני שאתה שוקל להפעיל\nבנוסף לכל אזהרות ORB Long (לא נבדקה, פשרת 1m→5m): בפוזיציית Short אין תקרה תיאורטית להפסד - מחיר המניה יכול לעלות ללא הגבלה, וה-stop עלול \'לקפוץ מעל\' (gap) במקרה של short squeeze. אל תפעיל LIVE לפני בדיקה מקיפה.',
            ),
            'ORB Long v2 (RSI/Trend Confluence, Staged Trail)': (
                '## מה זה עושה\nגרסה שנייה (v2) של ORB Long - שומרת על אותו מנגנון Opening Range Breakout (OR 15 דקות, אישור 5 דקות, breakout/retest) אבל עם שני שינויים משמעותיים: פילטרים נוספים לפני כניסה, ומנגנון יציאה שונה לגמרי. **נשמרת כאסטרטגיה נפרדת מ-ORB Long המקורית** (לא דריסה במקום) כדי לא לערבב את היסטוריית הבקטסטים של שתיהן תחת אותה זהות.\n\n## פילטר כניסה נוסף: RSI + מגמה\nבנוסף לכל תנאי ה-ORB המקוריים (OR, אישור, RVOL+ATR%), נדרש גם: RSI(14) עולה על פני 3 נרות רצופים אחרונים, **וגם** (EMA(20) על 5 דקות עולה **או** המחיר מעל ה-VWAP של היום). כל התנאים האלה חייבים להתקיים באותו נר שבו נכנסים.\n\n## יציאה: Staged Trail (במקום יעד קבוע)\nאין יותר יעד R:R קבוע - הסטופ ההתחלתי נשאר קבוע עד שה-MFE (השיא שהמחיר בפועל נגע בו, לא רק מחיר הסגירה) מגיע ל-2R, ואז עובר ל-**Profit Lock: 0.25R** (לא ל-Breakeven שטוח) - כלומר גם עסקה שנגעה ב-2R תוך-יומית וחזרה אחורה, ננעלת עם רווח קטן במקום להסתכן בחזרה לסטופ המקורי. כשמגיעים ל-3R (סגירת נר), מתחיל טריילינג סטופ מתחת לשפל של שני הנרות האחרונים (5 דקות), ומתעדכן כל עוד הוא משתפר. הפוזיציה יכולה לרוץ הרבה מעבר ל-2R אם המניה ממשיכה.\n\n## יקום, פילטרי תנודתיות, חלון כניסות\nזהה ל-ORB Long המקורית: S&P 500 עם Market Cap מעל $1B, מחיר מינימלי $3, RVOL מעל 2.0, ATR% מדורג לפי מחיר, חלון כניסות 09:50-11:30 ET.\n\n## סינון כיוון שוק (ES VWAP)\nכניסה מתאפשרת רק כש-ES (חוזי E-mini S&P 500) נסחר **מעל** ל-VWAP היומי שלו - שים לב: זה נפרד לגמרי מ-VWAP הסימבול עצמו שכבר מופיע למעלה כחלק מ-RSI+EMA/VWAP (זה בודק את המניה הספציפית, זה בודק את השוק הרחב). "Market first, setup second": גם אם כל תנאי הכניסה מתקיימים, עסקה שסותרת את כיוון השוק הרחב נדחית. כבוי כברירת מחדל (דורש הרשאת נתוני CME futures + הפעלה מפורשת בעמוד Bot) - כל עוד הוא כבוי, או שאין גישה לנתוני ES, האסטרטגיה נשארת ללא סינון (fail-open), לא נחסמת.\n\n## פרופיל סיכון\nדירוג: aggressive - אסטרטגיה חדשה לגמרי שלא נבדקה כלל (v1 המקורית לפחות עברה בקטסט ראשוני - זו עוד לא). סיכון לעסקה: 1% | גודל פוזיציה מקס\': 10% | פוזיציות בו-זמנית: עד 5\n\n## אזהרת סיכון - קרא לפני שאתה שוקל להפעיל\nאין לזה שום היסטוריית בקטסט עדיין - כל אזהרות ORB Long המקורית תקפות כאן במלואן, ובנוסף: הפילטרים הנוספים (RSI+EMA/VWAP) מצמצמים עוד יותר את מספר העסקאות הפוטנציאליות, וה-Staged Trail טרם נבדק כלל מול הנתונים ההיסטוריים. הרץ בקטסט מקיף (שבועות-חודשים) לפני כל שיקול נוסף.',
                '## מה זה עושה\nגרסה שנייה (v2) של ORB Long - שומרת על אותו מנגנון Opening Range Breakout (OR 15 דקות, אישור 5 דקות, breakout/retest) אבל עם שני שינויים משמעותיים: פילטרים נוספים לפני כניסה, ומנגנון יציאה שונה לגמרי. **נשמרת כאסטרטגיה נפרדת מ-ORB Long המקורית** (לא דריסה במקום) כדי לא לערבב את היסטוריית הבקטסטים של שתיהן תחת אותה זהות.\n\n## פילטר כניסה נוסף: RSI + מגמה\nבנוסף לכל תנאי ה-ORB המקוריים (OR, אישור, RVOL+ATR%), נדרש גם: RSI(14) עולה על פני 3 נרות רצופים אחרונים, **וגם** (EMA(20) על 5 דקות עולה **או** המחיר מעל ה-VWAP של היום). כל התנאים האלה חייבים להתקיים באותו נר שבו נכנסים.\n\n## יציאה: Staged Trail (במקום יעד קבוע)\nאין יותר יעד R:R קבוע - הסטופ ההתחלתי נשאר קבוע עד שה-MFE (השיא שהמחיר בפועל נגע בו, לא רק מחיר הסגירה) מגיע ל-2R, ואז עובר ל-**Profit Lock: 0.25R** (לא ל-Breakeven שטוח) - כלומר גם עסקה שנגעה ב-2R תוך-יומית וחזרה אחורה, ננעלת עם רווח קטן במקום להסתכן בחזרה לסטופ המקורי. כשמגיעים ל-3R (סגירת נר), מתחיל טריילינג סטופ מתחת לשפל של שני הנרות האחרונים (5 דקות), ומתעדכן כל עוד הוא משתפר. הפוזיציה יכולה לרוץ הרבה מעבר ל-2R אם המניה ממשיכה.\n\n## יקום, פילטרי תנודתיות, חלון כניסות\nזהה ל-ORB Long המקורית: S&P 500 עם Market Cap מעל $1B, מחיר מינימלי $3, RVOL מעל 2.0, ATR% מדורג לפי מחיר, חלון כניסות 09:50-11:30 ET.\n\n## סינון כיוון שוק (ES VWAP)\nכניסה מתאפשרת רק כש-ES (חוזי E-mini S&P 500) נסחר **מעל** ל-VWAP היומי שלו - שים לב: זה נפרד לגמרי מ-VWAP הסימבול עצמו שכבר מופיע למעלה כחלק מ-RSI+EMA/VWAP (זה בודק את המניה הספציפית, זה בודק את השוק הרחב). "Market first, setup second": גם אם כל תנאי הכניסה מתקיימים, עסקה שסותרת את כיוון השוק הרחב נדחית. כבוי כברירת מחדל (דורש הרשאת נתוני CME futures + הפעלה מפורשת בעמוד Bot) - כל עוד הוא כבוי, או שאין גישה לנתוני ES, האסטרטגיה נשארת ללא סינון (fail-open), לא נחסמת.\n\n## רצפת מרחק סטופ מינימלית\nהסטופ הטכני (שפל/שיא נר האישור) לפעמים קרוב מדי לכניסה (נר כמעט ללא צל) - זה מייצר גודל פוזיציה מנופח מדי וערכי R מנופחים מדי שלא משקפים סיכון אמיתי (ראו חקירה בהיסטוריית השיחה). לכן מרחק הסטופ מהכניסה מוגבל מלמטה ל-0.25% ממחיר הכניסה (min_stop_distance_pct) - אם הסטופ הטכני קרוב יותר, הוא מורחב לרצפה הזו; סטופ טכני שכבר רחוק מספיק לא משתנה כלל.\n\n## פרופיל סיכון\nדירוג: aggressive - אסטרטגיה חדשה לגמרי שלא נבדקה כלל (v1 המקורית לפחות עברה בקטסט ראשוני - זו עוד לא). סיכון לעסקה: 1% | גודל פוזיציה מקס\': 10% | פוזיציות בו-זמנית: עד 5\n\n## אזהרת סיכון - קרא לפני שאתה שוקל להפעיל\nאין לזה שום היסטוריית בקטסט עדיין - כל אזהרות ORB Long המקורית תקפות כאן במלואן, ובנוסף: הפילטרים הנוספים (RSI+EMA/VWAP) מצמצמים עוד יותר את מספר העסקאות הפוטנציאליות, וה-Staged Trail טרם נבדק כלל מול הנתונים ההיסטוריים. הרץ בקטסט מקיף (שבועות-חודשים) לפני כל שיקול נוסף.',
            ),
            'ORB Short v2 (RSI/Trend Confluence, Staged Trail)': (
                '## מה זה עושה\nמראה הפוכה מדויקת של ORB Long v2 - ראו את התיאור המלא שם. כאן: RSI(14) יורד על פני 3 נרות רצופים, וגם (EMA(20) יורד או המחיר מתחת ל-VWAP). סטופ קבוע עד MFE 2R (השיא/שפל שהמחיר בפועל נגע בו, לא רק סגירה), ואז Profit Lock 0.25R, טריילינג מ-3R מעל השיא של שני הנרות האחרונים.\n\n## יקום, פילטרים, חלון כניסות\nזהה לחלוטין ל-ORB Short המקורית ול-ORB Long v2.\n\n## סינון כיוון שוק (ES VWAP)\nכניסה מתאפשרת רק כש-ES (חוזי E-mini S&P 500) נסחר **מתחת** ל-VWAP היומי שלו - שים לב: זה נפרד לגמרי מ-VWAP הסימבול עצמו שכבר מופיע למעלה. "Market first, setup second": גם אם כל תנאי הכניסה מתקיימים, עסקה שסותרת את כיוון השוק הרחב נדחית. כבוי כברירת מחדל (דורש הרשאת נתוני CME futures + הפעלה מפורשת בעמוד Bot) - כל עוד הוא כבוי, או שאין גישה לנתוני ES, האסטרטגיה נשארת ללא סינון (fail-open), לא נחסמת.\n\n## פרופיל סיכון\nדירוג: aggressive - אסטרטגיה חדשה לגמרי שלא נבדקה כלל. סיכון לעסקה: 1% | גודל פוזיציה מקס\': 10% | פוזיציות בו-זמנית: עד 5\n\n## אזהרת סיכון - קרא לפני שאתה שוקל להפעיל\nכל אזהרות ORB Long v2 תקפות כאן, ובנוסף: בפוזיציית Short אין תקרה תיאורטית להפסד - מחיר המניה יכול לעלות ללא הגבלה, וה-stop עלול \'לקפוץ מעל\' (gap) במקרה של short squeeze. אל תפעיל LIVE לפני בדיקה מקיפה.',
                '## מה זה עושה\nמראה הפוכה מדויקת של ORB Long v2 - ראו את התיאור המלא שם. כאן: RSI(14) יורד על פני 3 נרות רצופים, וגם (EMA(20) יורד או המחיר מתחת ל-VWAP). סטופ קבוע עד MFE 2R (השיא/שפל שהמחיר בפועל נגע בו, לא רק סגירה), ואז Profit Lock 0.25R, טריילינג מ-3R מעל השיא של שני הנרות האחרונים.\n\n## יקום, פילטרים, חלון כניסות\nזהה לחלוטין ל-ORB Short המקורית ול-ORB Long v2.\n\n## סינון כיוון שוק (ES VWAP)\nכניסה מתאפשרת רק כש-ES (חוזי E-mini S&P 500) נסחר **מתחת** ל-VWAP היומי שלו - שים לב: זה נפרד לגמרי מ-VWAP הסימבול עצמו שכבר מופיע למעלה. "Market first, setup second": גם אם כל תנאי הכניסה מתקיימים, עסקה שסותרת את כיוון השוק הרחב נדחית. כבוי כברירת מחדל (דורש הרשאת נתוני CME futures + הפעלה מפורשת בעמוד Bot) - כל עוד הוא כבוי, או שאין גישה לנתוני ES, האסטרטגיה נשארת ללא סינון (fail-open), לא נחסמת.\n\n## רצפת מרחק סטופ מינימלית\nהסטופ הטכני (שפל/שיא נר האישור) לפעמים קרוב מדי לכניסה (נר כמעט ללא צל) - זה מייצר גודל פוזיציה מנופח מדי וערכי R מנופחים מדי שלא משקפים סיכון אמיתי (ראו חקירה בהיסטוריית השיחה). לכן מרחק הסטופ מהכניסה מוגבל מלמטה ל-0.25% ממחיר הכניסה (min_stop_distance_pct) - אם הסטופ הטכני קרוב יותר, הוא מורחב לרצפה הזו; סטופ טכני שכבר רחוק מספיק לא משתנה כלל.\n\n## פרופיל סיכון\nדירוג: aggressive - אסטרטגיה חדשה לגמרי שלא נבדקה כלל. סיכון לעסקה: 1% | גודל פוזיציה מקס\': 10% | פוזיציות בו-זמנית: עד 5\n\n## אזהרת סיכון - קרא לפני שאתה שוקל להפעיל\nכל אזהרות ORB Long v2 תקפות כאן, ובנוסף: בפוזיציית Short אין תקרה תיאורטית להפסד - מחיר המניה יכול לעלות ללא הגבלה, וה-stop עלול \'לקפוץ מעל\' (gap) במקרה של short squeeze. אל תפעיל LIVE לפני בדיקה מקיפה.',
            ),
            'ORB Long v2 Fade (Short)': (
                "## מה זה עושה\nאסטרטגיית מחקר ניסיונית: מזהה בדיוק את אותו איתות של ORB Long v2 (פריצת opening range כלפי מעלה, עם אישור RVOL/ATR% ו-RSI+מגמה), אבל **מוכרת בשורט** נגד הפריצה במקום לקנות איתה - הימור שהפריצה תיכשל ותתהפך, לא שהיא תמשיך.\n\n## תנאי כניסה (זהים לחלוטין ל-ORB Long v2)\nOpening Range 15 דקות, אישור פריצה כלפי מעלה על נר 5 דקות, RVOL מעל 2.0, ATR% מדורג לפי מחיר, RSI(14) עולה על פני 3 נרות, וגם (EMA(20) עולה או מחיר מעל VWAP).\n\n## יציאה וניהול פוזיציה (מותאם לפוזיציית שורט, לא ללונג)\nסטופ התחלתי: שפל/שיא נר האישור (מעל הכניסה - זו פוזיציית שורט, לא מתחתיה). Staged Trail: נשאר קבוע עד 2R, Breakeven ב-2R, טריילינג מ-3R מעל השיא של שני הנרות האחרונים (5 דקות).\n\n## פרופיל סיכון\nדירוג: aggressive | סיכון לעסקה: 1% | גודל פוזיציה מקס': 10% | פוזיציות בו-זמנית: עד 5\n\n## אזהרת סיכון - קרא לפני שאתה שוקל להפעיל\nזו לא אסטרטגיה שאומתה - היא ממש את אותו רעיון ניסיוני של Long Breakout Fade (Short) (הימור שפריצה תיכשל ותתהפך), מיושם על ORB v2 במקום המודל הקלאסי. אל תפעיל LIVE לפני בדיקה מקיפה על פני תקופה ארוכה (שבועות-חודשים, מאות עסקאות). בנוסף, בפוזיציית Short אין תקרה תיאורטית להפסד.",
                "## מה זה עושה\nאסטרטגיית מחקר ניסיונית: מזהה בדיוק את אותו איתות של ORB Long v2 (פריצת opening range כלפי מעלה, עם אישור RVOL/ATR% ו-RSI+מגמה), אבל **מוכרת בשורט** נגד הפריצה במקום לקנות איתה - הימור שהפריצה תיכשל ותתהפך, לא שהיא תמשיך.\n\n## תנאי כניסה (זהים לחלוטין ל-ORB Long v2)\nOpening Range 15 דקות, אישור פריצה כלפי מעלה על נר 5 דקות, RVOL מעל 2.0, ATR% מדורג לפי מחיר, RSI(14) עולה על פני 3 נרות, וגם (EMA(20) עולה או מחיר מעל VWAP).\n\n## יציאה וניהול פוזיציה (מותאם לפוזיציית שורט, לא ללונג)\nסטופ התחלתי: שפל/שיא נר האישור (מעל הכניסה - זו פוזיציית שורט, לא מתחתיה). Staged Trail: נשאר קבוע עד 2R, Breakeven ב-2R, טריילינג מ-3R מעל השיא של שני הנרות האחרונים (5 דקות).\n\n## רצפת מרחק סטופ מינימלית\nהסטופ הטכני (שפל/שיא נר האישור) לפעמים קרוב מדי לכניסה (נר כמעט ללא צל) - זה מייצר גודל פוזיציה מנופח מדי וערכי R מנופחים מדי שלא משקפים סיכון אמיתי (ראו חקירה בהיסטוריית השיחה). לכן מרחק הסטופ מהכניסה מוגבל מלמטה ל-0.25% ממחיר הכניסה (min_stop_distance_pct) - אם הסטופ הטכני קרוב יותר, הוא מורחב לרצפה הזו; סטופ טכני שכבר רחוק מספיק לא משתנה כלל.\n\n## פרופיל סיכון\nדירוג: aggressive | סיכון לעסקה: 1% | גודל פוזיציה מקס': 10% | פוזיציות בו-זמנית: עד 5\n\n## אזהרת סיכון - קרא לפני שאתה שוקל להפעיל\nזו לא אסטרטגיה שאומתה - היא ממש את אותו רעיון ניסיוני של Long Breakout Fade (Short) (הימור שפריצה תיכשל ותתהפך), מיושם על ORB v2 במקום המודל הקלאסי. אל תפעיל LIVE לפני בדיקה מקיפה על פני תקופה ארוכה (שבועות-חודשים, מאות עסקאות). בנוסף, בפוזיציית Short אין תקרה תיאורטית להפסד.",
            ),
            'ORB Short v2 Fade (Long)': (
                "## מה זה עושה\nמראה הפוכה מדויקת של ORB Long v2 Fade (Short) - ראו את התיאור המלא שם. כאן: מזהה בדיוק את אותו איתות של ORB Short v2 (פריצת opening range כלפי מטה, RSI יורד, EMA יורד/מחיר מתחת ל-VWAP), אבל **קונה בלונג** נגד השבירה במקום למכור בשורט איתה.\n\n## יקום, פילטרים, חלון כניסות\nזהה לחלוטין ל-ORB Short v2 ול-ORB Long v2 Fade (Short).\n\n## יציאה וניהול פוזיציה (מותאם לפוזיציית לונג)\nסטופ התחלתי: שפל נר האישור (מתחת לכניסה - זו פוזיציית לונג). Staged Trail: נשאר קבוע עד 2R, Breakeven ב-2R, טריילינג מ-3R מתחת לשפל של שני הנרות האחרונים.\n\n## פרופיל סיכון\nדירוג: aggressive | סיכון לעסקה: 1% | גודל פוזיציה מקס': 10% | פוזיציות בו-זמנית: עד 5\n\n## אזהרת סיכון - קרא לפני שאתה שוקל להפעיל\nכל אזהרות ORB Long v2 Fade (Short) תקפות כאן - זו לא אסטרטגיה שאומתה. אל תפעיל LIVE לפני בדיקה מקיפה על פני תקופה ארוכה.",
                "## מה זה עושה\nמראה הפוכה מדויקת של ORB Long v2 Fade (Short) - ראו את התיאור המלא שם. כאן: מזהה בדיוק את אותו איתות של ORB Short v2 (פריצת opening range כלפי מטה, RSI יורד, EMA יורד/מחיר מתחת ל-VWAP), אבל **קונה בלונג** נגד השבירה במקום למכור בשורט איתה.\n\n## יקום, פילטרים, חלון כניסות\nזהה לחלוטין ל-ORB Short v2 ול-ORB Long v2 Fade (Short).\n\n## יציאה וניהול פוזיציה (מותאם לפוזיציית לונג)\nסטופ התחלתי: שפל נר האישור (מתחת לכניסה - זו פוזיציית לונג). Staged Trail: נשאר קבוע עד 2R, Breakeven ב-2R, טריילינג מ-3R מתחת לשפל של שני הנרות האחרונים.\n\n## רצפת מרחק סטופ מינימלית\nהסטופ הטכני (שפל/שיא נר האישור) לפעמים קרוב מדי לכניסה (נר כמעט ללא צל) - זה מייצר גודל פוזיציה מנופח מדי וערכי R מנופחים מדי שלא משקפים סיכון אמיתי (ראו חקירה בהיסטוריית השיחה). לכן מרחק הסטופ מהכניסה מוגבל מלמטה ל-0.25% ממחיר הכניסה (min_stop_distance_pct) - אם הסטופ הטכני קרוב יותר, הוא מורחב לרצפה הזו; סטופ טכני שכבר רחוק מספיק לא משתנה כלל.\n\n## פרופיל סיכון\nדירוג: aggressive | סיכון לעסקה: 1% | גודל פוזיציה מקס': 10% | פוזיציות בו-זמנית: עד 5\n\n## אזהרת סיכון - קרא לפני שאתה שוקל להפעיל\nכל אזהרות ORB Long v2 Fade (Short) תקפות כאן - זו לא אסטרטגיה שאומתה. אל תפעיל LIVE לפני בדיקה מקיפה על פני תקופה ארוכה.",
            ),
        }
        for _name, (_old_text, _new_text) in _MIN_STOP_DISTANCE_DESCRIPTIONS.items():
            conn.execute(
                "UPDATE strategies SET description = ? WHERE name = ? AND description = ?",
                (_new_text, _name, _old_text),
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


# Off by default (unlike bot_enabled) - src/es_filter.py needs real CME
# futures market-data entitlement on this account's IBKR connection to do
# anything useful; enabling it before that's confirmed just means every
# gated strategy fails open on every scan (harmless, per es_filter.check's
# own fail-open design, but pointless). See cycle.entry_scan/touch_turn_
# entry_scan for where this actually gates a trade.
def is_es_vwap_filter_enabled(account_id: int, mode: str) -> bool:
    return get_setting(_scope_key(account_id, mode, "es_vwap_filter_enabled"), "false") == "true"


def set_es_vwap_filter_enabled(account_id: int, mode: str, enabled: bool):
    set_setting(_scope_key(account_id, mode, "es_vwap_filter_enabled"), "true" if enabled else "false")


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
def record_trade(account_id: int, mode: str, symbol: str, side: str, size: int, fill_price: float, order_id, status: str,
                  exec_id: str | None = None, timestamp_iso: str | None = None):
    """exec_id (IBKR's own execution id) is optional - trade.py/
    close_position.py pass it when they have it (letting cycle's periodic
    broker-fills sync recognize the same fill later and skip it, instead
    of double-logging), but it's fine to omit for a row with no matching
    IBKR execution (e.g. an order that never filled). timestamp_iso lets
    a synced-after-the-fact broker fill (see cycle.sync_broker_fills) use
    the execution's own real fill time instead of whenever the sync
    happened to run; omit it for "now" (the normal case, an immediate
    trade.py/close_position.py call)."""
    _check_mode(mode)
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO trades (account_id, mode, timestamp_iso, symbol, side, size, fill_price, order_id, status, exec_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (account_id, mode, timestamp_iso or datetime.now(ET).isoformat(timespec="seconds"),
             symbol, side, size, fill_price, order_id, status, exec_id),
        )


def trade_exec_id_exists(exec_id: str) -> bool:
    """Whether a trade with this IBKR execution id has already been
    recorded - IBKR's execId is globally unique, so this needs no
    account/mode scoping. Lets cycle.sync_broker_fills skip an execution
    it has already logged on an earlier periodic sync, or one trade.py/
    close_position.py already recorded immediately when they placed the
    order themselves, instead of double-counting the same fill."""
    with get_conn() as conn:
        row = conn.execute("SELECT 1 FROM trades WHERE exec_id = ?", (exec_id,)).fetchone()
        return row is not None


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
    # target_price defaults to None for every non-ORB caller (predates this
    # field, same reasoning as the side default just below).
    pos = {"side": "long", "target_price": None, **pos}
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO positions (account_id, mode, symbol, side, entry_price, entry_time_iso, qty, initial_stop, "
            "stop_price, stop_order_id, state, r_multiple, target_price) VALUES "
            "(:account_id, :mode, :symbol, :side, :entry_price, :entry_time_iso, :qty, :initial_stop, :stop_price, "
            ":stop_order_id, :state, :r_multiple, :target_price) "
            "ON CONFLICT(account_id, mode, symbol) DO UPDATE SET "
            "qty=excluded.qty, initial_stop=excluded.initial_stop, stop_price=excluded.stop_price, "
            "stop_order_id=excluded.stop_order_id, state=excluded.state, r_multiple=excluded.r_multiple, "
            "target_price=excluded.target_price",
            {**pos, "account_id": account_id, "mode": mode},
        )


def remove_position(account_id: int, mode: str, symbol: str):
    _check_mode(mode)
    with get_conn() as conn:
        conn.execute(
            "DELETE FROM positions WHERE account_id = ? AND mode = ? AND symbol = ?", (account_id, mode, symbol)
        )


def has_pending_order_today(account_id: int, mode: str, symbol: str, placed_date: str) -> bool:
    """Touch & Turn's max-one-attempt-per-symbol-per-day gate (see
    pending_orders' own schema comment) - True regardless of that
    attempt's outcome (pending/filled/cancelled/expired)."""
    _check_mode(mode)
    with get_conn() as conn:
        row = conn.execute(
            "SELECT 1 FROM pending_orders WHERE account_id = ? AND mode = ? AND symbol = ? AND placed_date = ?",
            (account_id, mode, symbol, placed_date),
        ).fetchone()
        return row is not None


def create_pending_order(account_id: int, mode: str, order: dict) -> bool:
    """Returns whether a new row was actually inserted - False (a no-op,
    not an error) if this exact (account_id, mode, symbol, placed_date)
    already has an attempt on record, so a caller that raced past
    has_pending_order_today's own check (two cycle ticks close together)
    still can't double-place. `order` needs symbol, placed_date, side,
    limit_price, target_price, initial_stop, qty, placed_at, expires_at."""
    _check_mode(mode)
    with get_conn() as conn:
        cur = conn.execute(
            "INSERT OR IGNORE INTO pending_orders (account_id, mode, symbol, placed_date, side, "
            "limit_price, target_price, initial_stop, qty, placed_at, expires_at) VALUES "
            "(:account_id, :mode, :symbol, :placed_date, :side, :limit_price, :target_price, "
            ":initial_stop, :qty, :placed_at, :expires_at)",
            {**order, "account_id": account_id, "mode": mode},
        )
        return cur.rowcount > 0


def set_pending_order_broker_id(account_id: int, mode: str, symbol: str, placed_date: str, broker_order_id: int):
    _check_mode(mode)
    with get_conn() as conn:
        conn.execute(
            "UPDATE pending_orders SET broker_order_id = ? "
            "WHERE account_id = ? AND mode = ? AND symbol = ? AND placed_date = ?",
            (broker_order_id, account_id, mode, symbol, placed_date),
        )


def get_pending_orders(account_id: int, mode: str, status: str = "pending") -> list[dict]:
    _check_mode(mode)
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM pending_orders WHERE account_id = ? AND mode = ? AND status = ?",
            (account_id, mode, status),
        ).fetchall()
        return [dict(r) for r in rows]


def resolve_pending_order(account_id: int, mode: str, symbol: str, placed_date: str, status: str):
    """status: 'filled', 'cancelled', or 'expired' - the row is kept (not
    deleted), both as an audit trail and so has_pending_order_today keeps
    blocking a same-day re-attempt after resolution."""
    _check_mode(mode)
    with get_conn() as conn:
        conn.execute(
            "UPDATE pending_orders SET status = ? WHERE account_id = ? AND mode = ? AND symbol = ? AND placed_date = ?",
            (status, account_id, mode, symbol, placed_date),
        )


def set_hold_overnight(account_id: int, mode: str, symbol: str, value: bool):
    """One-shot opt-out of today's EOD force-close for a single position —
    cycle.py's force_close_all resets this back to False the moment it
    skips the position, so it only ever applies to the next close, never
    silently forever."""
    _check_mode(mode)
    with get_conn() as conn:
        conn.execute(
            "UPDATE positions SET hold_overnight = ? WHERE account_id = ? AND mode = ? AND symbol = ?",
            (int(value), account_id, mode, symbol),
        )


# ------------------------------------------------------------- watchlist ---
def replace_watchlist(account_id: int, mode: str, entries: list[dict]):
    """Replaces the ENTIRE watchlist (both directions) for this account+mode
    — a caller that only scans one direction must pass the other
    direction's current entries back in too, or they'll be wiped.
    morning_prefilter.py scans both in one pass for exactly this reason.

    Each entry may carry a "universes" list (e.g. ["default"] or
    ["default", "ixic_large_beta_buy"] for a symbol that qualifies for more
    than one strategy universe) - stored as a comma-delimited, comma-wrapped
    tag string (",default,ixic_large_beta_buy,") so get_watchlist's universe
    filter can do a simple LIKE containment check without a join table.
    Entries with no "universes" key default to just ["default"], the
    S&P 500 scan's own tag."""
    _check_mode(mode)
    now = datetime.now(ET).isoformat(timespec="seconds")
    with get_conn() as conn:
        conn.execute("DELETE FROM watchlist WHERE account_id = ? AND mode = ?", (account_id, mode))
        rows = []
        for e in entries:
            e = dict(e)
            universes = e.pop("universes", None) or ["default"]
            e["universe"] = "," + ",".join(universes) + ","
            rows.append({"direction": "long", **e, "account_id": account_id, "mode": mode, "generated_at": now})
        conn.executemany(
            "INSERT INTO watchlist (account_id, mode, symbol, direction, gap_pct, open_price, prev_close, generated_at, universe) "
            "VALUES (:account_id, :mode, :symbol, :direction, :gap_pct, :open_price, :prev_close, :generated_at, :universe)",
            rows,
        )


def get_watchlist(account_id: int, mode: str, direction: str | None = None, universe: str | None = None) -> list[dict]:
    """universe, when given, restricts results to rows tagged with that
    universe key (see replace_watchlist) - e.g. a strategy whose
    universe_filters.custom_universe is "ixic_large_beta_buy" only ever
    sees symbols tagged with that key, never plain S&P 500 survivors, and
    vice versa a strategy with no custom_universe only sees "default"-tagged
    symbols."""
    _check_mode(mode)
    query = "SELECT * FROM watchlist WHERE account_id = ? AND mode = ?"
    params = [account_id, mode]
    if direction is not None:
        query += " AND direction = ?"
        params.append(direction)
    if universe is not None:
        query += " AND universe LIKE ?"
        params.append(f"%,{universe},%")
    query += " ORDER BY ABS(gap_pct) DESC"
    with get_conn() as conn:
        rows = conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]


def update_watchlist_filters(account_id: int, mode: str, results: list[dict]):
    """Stores the latest per-symbol filter snapshot (classic D1-D3/I1-I3
    or ORB, per row's own "model" - see cycle.scan_watchlist_filters) for
    the dashboard's Watchlist table."""
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


def update_broker_orders(account_id: int, mode: str, orders: list[dict]):
    """Stores the latest raw IBKR open orders (see
    cycle.refresh_account_info) for the dashboard's Account Holdings view —
    every resting stop/limit order in the account, independent of which
    client ID placed it (the bot's own cycle, a manual TWS/Mobile order,
    etc.), so a holding's real protective orders are visible even when the
    bot itself never touched that symbol."""
    _check_mode(mode)
    payload = {"updated_at": datetime.now(ET).isoformat(timespec="seconds"), "orders": orders}
    set_setting(_scope_key(account_id, mode, "broker_orders_json"), json.dumps(payload))


def get_broker_orders(account_id: int, mode: str) -> dict:
    _check_mode(mode)
    raw = get_setting(_scope_key(account_id, mode, "broker_orders_json"), "")
    if not raw:
        return {"updated_at": "", "orders": []}
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {"updated_at": "", "orders": []}


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
            "SELECT s.id, s.name, s.key, s.direction, s.risk_rating, s.description, s.created_at, s.updated_at, "
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


def create_strategy(name: str, rules: dict, direction: str, risk_rating: str = "moderate", description: str = "", key: str = "") -> int:
    _check_direction(direction)
    _check_risk_rating(risk_rating)
    now = datetime.now(ET).isoformat(timespec="seconds")
    with get_conn() as conn:
        try:
            cur = conn.execute(
                "INSERT INTO strategies (name, key, direction, rules_json, risk_rating, description, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (name, key, direction, json.dumps(rules, indent=2), risk_rating, description, now, now),
            )
        except sqlite3.IntegrityError as exc:
            if "strategies.key" in str(exc):
                raise ValueError(f"Key '{key}' is already used by another strategy") from exc
            raise
        return cur.lastrowid


def update_strategy(strategy_id: int, rules: dict, risk_rating: str | None = None, description: str | None = None, key: str | None = None):
    # direction is intentionally not editable here — it defines what the
    # rules JSON's fields even mean (D1_above_prior_day_high vs
    # D1_below_prior_day_low, etc), so changing it on an existing strategy
    # would silently invalidate its own rules rather than convert them.
    if risk_rating is not None:
        _check_risk_rating(risk_rating)
    now = datetime.now(ET).isoformat(timespec="seconds")
    with get_conn() as conn:
        sets = ["rules_json = ?", "updated_at = ?"]
        params: list = [json.dumps(rules, indent=2), now]
        if risk_rating is not None:
            sets.append("risk_rating = ?")
            params.append(risk_rating)
        if description is not None:
            sets.append("description = ?")
            params.append(description)
        if key is not None:
            sets.append("key = ?")
            params.append(key)
        params.append(strategy_id)
        try:
            conn.execute(f"UPDATE strategies SET {', '.join(sets)} WHERE id = ?", params)
        except sqlite3.IntegrityError as exc:
            if "strategies.key" in str(exc):
                raise ValueError(f"Key '{key}' is already used by another strategy") from exc
            raise


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


def deactivate_strategy(account_id: int, strategy_id: int):
    """Turns off this account's active strategy for this strategy's
    direction - the bot then simply skips that direction going forward
    (cycle.py's `if rules is None: continue`), same as if nothing had ever
    been activated there. Scoped to strategy_id as well as direction so a
    stale "Deactivate" click can't clear a different strategy that became
    active in the meantime; only this account's own row is touched, same
    isolation as activate_strategy."""
    with get_conn() as conn:
        row = conn.execute("SELECT direction FROM strategies WHERE id = ?", (strategy_id,)).fetchone()
        if not row:
            raise ValueError(f"Strategy {strategy_id} not found")
        conn.execute(
            "DELETE FROM account_active_strategy WHERE account_id = ? AND direction = ? AND strategy_id = ?",
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


# -------------------------------------------------------------- backtests ---
def create_backtest(account_id: int, params: dict, execution_mode: str = "local") -> int:
    """execution_mode 'local' (default, unchanged behavior) has web/app.py
    spawn run_backtest.py itself right after this returns. 'remote' leaves
    the row at status='pending' for a worker to pick up via
    claim_next_backtest - see docs/worker.md."""
    if execution_mode not in ("local", "remote"):
        raise ValueError("execution_mode must be 'local' or 'remote'")
    now = datetime.now(ET).isoformat(timespec="seconds")
    with get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO backtests (account_id, status, params_json, execution_mode, created_at) "
            "VALUES (?, 'pending', ?, ?, ?)",
            (account_id, json.dumps(params), execution_mode, now),
        )
        return cur.lastrowid


def start_backtest(backtest_id: int):
    with get_conn() as conn:
        conn.execute("UPDATE backtests SET status = 'running' WHERE id = ?", (backtest_id,))


def set_backtest_pid(backtest_id: int, pid: int):
    """Records the OS pid of the subprocess web/app.py just spawned for this
    backtest (before it's necessarily reached 'running' - run_backtest.py
    sets that itself once it's actually started). Only used to tell a
    genuinely still-running backtest apart from an orphaned one on the next
    dashboard startup - see fail_orphaned_backtests."""
    with get_conn() as conn:
        conn.execute("UPDATE backtests SET pid = ? WHERE id = ?", (pid, backtest_id))


def finish_backtest(backtest_id: int, results: dict):
    now = datetime.now(ET).isoformat(timespec="seconds")
    with get_conn() as conn:
        conn.execute(
            "UPDATE backtests SET status = 'done', results_json = ?, finished_at = ? WHERE id = ?",
            (json.dumps(results), now, backtest_id),
        )


def fail_backtest(backtest_id: int, error: str):
    now = datetime.now(ET).isoformat(timespec="seconds")
    with get_conn() as conn:
        conn.execute(
            "UPDATE backtests SET status = 'failed', error = ?, finished_at = ? WHERE id = ?",
            (error, now, backtest_id),
        )


def fail_orphaned_backtests():
    """Called once from web/app.py's startup handler: a backtest still
    marked 'running' from BEFORE this dashboard process started can no
    longer be trusted - systemd's default KillMode=control-group kills
    every process in dashboard.service's cgroup on stop/restart, including
    any subprocess.Popen'd run_backtest.py, not just the dashboard's own
    pid, so a `systemctl restart dashboard.service` while a backtest is
    running silently kills it without ever reaching its own except
    handler (the one thing that would otherwise call fail_backtest) -
    leaving the row stuck at 'running' forever with nothing left tracking
    it. os.kill(pid, 0) (a real signal is never sent - see the os.kill
    docs) tells a genuinely still-running backtest (survived because
    KillMode=process was set, or this dashboard start wasn't a restart at
    all) apart from one that's actually gone.

    Scoped to execution_mode = 'local' - a 'remote' row has no pid on this
    machine at all (it's computing on someone's own PC via
    backtest_worker.py), so the os.kill check below would always read it
    as dead and fail it out from under a worker that's still happily
    computing. requeue_abandoned_worker_backtests is the equivalent check
    for those, using claimed_at's timeout instead of a pid."""
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT id, pid FROM backtests WHERE status = 'running' AND execution_mode = 'local'"
        ).fetchall()
    for row in rows:
        alive = False
        if row["pid"]:
            try:
                os.kill(row["pid"], 0)
                alive = True
            except ProcessLookupError:
                alive = False
            except PermissionError:
                alive = True  # pid exists, just not ours to signal - treat as still running rather than guess
        if not alive:
            fail_backtest(
                row["id"],
                "Orphaned - the dashboard restarted while this backtest was still running, which killed it. Re-run it.",
            )


def get_backtest(backtest_id: int) -> dict | None:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM backtests WHERE id = ?", (backtest_id,)).fetchone()
        if not row:
            return None
        result = dict(row)
        result["params"] = json.loads(result.pop("params_json"))
        raw_results = result.pop("results_json")
        result["results"] = json.loads(raw_results) if raw_results else None
        return result


def delete_backtest(backtest_id: int, account_id: int) -> bool:
    """Scoped to account_id so one account can't delete another's backtest.
    Returns whether a row was actually deleted. Deleting a still-running
    backtest does NOT stop it - the row just disappears out from under a
    subprocess that has no idea it happened, which keeps computing until
    it finishes (or crashes) and then finds nothing left to write its
    result into. See cancel_backtest for what actually stops it."""
    with get_conn() as conn:
        cur = conn.execute(
            "DELETE FROM backtests WHERE id = ? AND account_id = ?", (backtest_id, account_id)
        )
        return cur.rowcount > 0


def cancel_backtest(backtest_id: int, account_id: int) -> bool:
    """Actually stops a still-running (or not-yet-started) backtest,
    unlike delete_backtest - kills the subprocess by its recorded pid
    (see set_backtest_pid) and marks the row failed, instead of leaving
    an orphaned process to compute for nothing. Scoped to account_id like
    delete_backtest. Returns False if there's no matching row for this
    account in a cancellable state ('pending'/'running') - already-
    finished runs have nothing to cancel."""
    with get_conn() as conn:
        row = conn.execute(
            "SELECT pid, status FROM backtests WHERE id = ? AND account_id = ?", (backtest_id, account_id)
        ).fetchone()
    if row is None or row["status"] not in ("pending", "running"):
        return False
    if row["pid"]:
        try:
            os.kill(row["pid"], signal.SIGTERM)
        except ProcessLookupError:
            pass  # already exited on its own - still fine to mark it failed below
    fail_backtest(backtest_id, "Cancelled by user")
    return True


def cancel_backtests(account_id: int, backtest_ids: list[int]) -> int:
    """Bulk cancel_backtest for a multi-select action - loops the same
    single-row logic (kill the recorded pid, mark failed) since cancelling
    needs a live status/pid check per row, not a single bulk UPDATE like
    archive_backtests. Same silent-skip behavior on an id that doesn't
    belong to this account or isn't in a cancellable state."""
    return sum(1 for bid in backtest_ids if cancel_backtest(bid, account_id))


def delete_backtests(account_id: int, backtest_ids: list[int]) -> int:
    """Bulk delete_backtest for a multi-select action. Excludes any row
    currently 'pending'/'running', same as the single-row UI (which only
    ever offers Delete on an already-finished run) - deleting a
    still-running backtest's row would abandon its subprocess with
    nothing left to write its result into (see delete_backtest's own
    docstring). Silently skips those rather than failing the whole batch;
    cancel_backtests first if they need to go away right now."""
    if not backtest_ids:
        return 0
    with get_conn() as conn:
        placeholders = ",".join("?" for _ in backtest_ids)
        cur = conn.execute(
            f"DELETE FROM backtests WHERE account_id = ? AND id IN ({placeholders}) "
            f"AND status NOT IN ('pending', 'running')",
            (account_id, *backtest_ids),
        )
        return cur.rowcount


def _backtest_row_to_summary(row) -> dict:
    """Shared by list_backtests/list_archived_backtests: turns one raw
    `backtests` row into a summary dict carrying total_pnl_usd, a
    lightweight sum of every strategy's own aggregate.net_pnl_usd (falling
    back to the older gross_pnl_usd for a result stored before commission
    modeling added that field) within that run's results, so a list can
    mark a run profitable/unprofitable - after realistic transaction
    costs, not just on paper - at a glance, without the caller pulling
    every row's full trade log just to find that out. results_json itself
    (which can be large, with several strategies' full trade logs) is
    parsed here to compute that sum but never included in the returned
    dict - only the derived total."""
    result = dict(row)
    result["params"] = json.loads(result.pop("params_json"))
    raw_results = result.pop("results_json")
    result["total_pnl_usd"] = None
    if raw_results:
        try:
            parsed = json.loads(raw_results)
            pnls = [
                s["aggregate"].get("net_pnl_usd", s["aggregate"].get("gross_pnl_usd"))
                for s in parsed.values()
                if isinstance(s, dict) and "aggregate" in s
            ]
            pnls = [p for p in pnls if p is not None]
            if pnls:
                result["total_pnl_usd"] = round(sum(pnls), 2)
        except (json.JSONDecodeError, KeyError, TypeError):
            pass  # malformed/unexpected results_json - leave total_pnl_usd as None rather than fail the whole list
    return result


def list_backtests(account_id: int, limit: int = 100) -> list[dict]:
    """Summary rows for the history list (fetch a single backtest's full
    detail, including its per-trade pairs, via get_backtest) - see
    _backtest_row_to_summary for what each row carries.

    Archived rows (archived_at set - see archive_backtests) are excluded:
    an archived run has no business cluttering the day-to-day History
    list it was deliberately filed away out of - list_archived_backtests
    is the separate view for those."""
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT id, account_id, status, params_json, results_json, error, execution_mode, created_at, claimed_at, finished_at FROM backtests "
            "WHERE account_id = ? AND archived_at IS NULL ORDER BY created_at DESC LIMIT ?",
            (account_id, limit),
        ).fetchall()
        return [_backtest_row_to_summary(row) for row in rows]


def archive_backtests(account_id: int, backtest_ids: list[int], folder: str) -> int:
    """Files the given backtests away into `folder` (a short free-text
    label explaining why, e.g. "old universe, before the I3 fix") -
    archived_at is set to now, archive_folder to the given label. Excludes
    them from the History list, the Backtest Calendar, and the Strategy
    Report (see those functions' own archived_at IS NULL filters) until
    explicitly restored via unarchive_backtests. Scoped to account_id like
    delete_backtest/cancel_backtest. Returns how many rows actually
    matched and were archived (silently skips any id that doesn't belong
    to this account rather than failing the whole batch over one bad id -
    a multi-select bulk action in the UI shouldn't need to pre-validate
    every id itself)."""
    if not backtest_ids:
        return 0
    now = datetime.now(ET).isoformat(timespec="seconds")
    with get_conn() as conn:
        placeholders = ",".join("?" for _ in backtest_ids)
        cur = conn.execute(
            f"UPDATE backtests SET archived_at = ?, archive_folder = ? "
            f"WHERE account_id = ? AND id IN ({placeholders})",
            (now, folder, account_id, *backtest_ids),
        )
        return cur.rowcount


def unarchive_backtests(account_id: int, backtest_ids: list[int]) -> int:
    """Restores the given backtests to the ordinary History/Calendar/
    Strategy Report views - clears both archived_at and archive_folder (a
    restored run has no folder anymore; archiving it again later starts
    fresh). Same account-scoping/silent-skip behavior as
    archive_backtests."""
    if not backtest_ids:
        return 0
    with get_conn() as conn:
        placeholders = ",".join("?" for _ in backtest_ids)
        cur = conn.execute(
            f"UPDATE backtests SET archived_at = NULL, archive_folder = '' "
            f"WHERE account_id = ? AND id IN ({placeholders})",
            (account_id, *backtest_ids),
        )
        return cur.rowcount


def list_archived_backtests(account_id: int) -> list[dict]:
    """Every archived backtest, same summary shape as list_backtests (see
    _backtest_row_to_summary) plus archived_at/archive_folder, for the
    Archive view - grouped by folder client-side (a run's folder is just a
    text label on the row, not a separate table, so "the folders" are
    whatever distinct values are currently in use, computed on the fly)."""
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT id, account_id, status, params_json, results_json, error, execution_mode, "
            "created_at, claimed_at, finished_at, archived_at, archive_folder FROM backtests "
            "WHERE account_id = ? AND archived_at IS NOT NULL ORDER BY archived_at DESC",
            (account_id,),
        ).fetchall()
        return [_backtest_row_to_summary(row) for row in rows]


def list_done_backtest_results(account_id: int, include_archived: bool = False) -> list[dict]:
    """Every 'done' backtest's params + full results, for src/perf.py's
    strategy_report to pool trade pairs across every run of the same
    strategy - unlike list_backtests' summary rows (results_json parsed
    only far enough to sum one total_pnl_usd figure, never returned),
    this needs the actual per-strategy trade pairs back out, so it's a
    separate query rather than a mode on the existing one. Returns
    [{"id", "created_at", "archived_at", "params", "results"}, ...],
    oldest first (so a caller pooling by date range can just keep
    whichever entry it sees last for a given key to prefer the newest
    re-run).

    Archived rows are excluded by default - same reasoning as
    list_backtests: an archived run must stay out of the dashboard's
    Strategy Report card until explicitly restored (see
    archive_backtests), not just out of the History list. A caller doing
    OFFLINE analysis across a strategy's full history regardless of
    archive status (e.g. analyze_strategy.py) passes include_archived=True
    instead - archiving is a display/organization concept for the
    day-to-day dashboard views, not a reason to hide data from a
    deliberate deep-dive."""
    with get_conn() as conn:
        archived_clause = "" if include_archived else "AND archived_at IS NULL "
        rows = conn.execute(
            "SELECT id, params_json, results_json, created_at, archived_at FROM backtests "
            f"WHERE account_id = ? AND status = 'done' AND results_json IS NOT NULL {archived_clause}"
            "ORDER BY created_at ASC",
            (account_id,),
        ).fetchall()
    out = []
    for row in rows:
        try:
            params = json.loads(row["params_json"])
            results = json.loads(row["results_json"])
        except (json.JSONDecodeError, TypeError):
            continue  # malformed row - skip rather than fail the whole report
        out.append({
            "id": row["id"], "created_at": row["created_at"], "archived_at": row["archived_at"],
            "params": params, "results": results,
        })
    return out


def list_backtest_calendar_entries(account_id: int) -> list[dict]:
    """Every backtest's date range + strategy_ids + status, for the
    Backtest page's calendar view - no limit (unlike list_backtests, tuned
    for the recency-capped History table) and no results_json parsing at
    all, since the calendar only ever needs "which days, which strategies,
    what state" - never P&L.

    Archived rows are excluded - same reasoning as list_backtests: an
    archived run's dot must disappear from the calendar until explicitly
    restored (see archive_backtests)."""
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT id, status, params_json FROM backtests WHERE account_id = ? AND archived_at IS NULL ORDER BY created_at ASC",
            (account_id,),
        ).fetchall()
    out = []
    for row in rows:
        try:
            params = json.loads(row["params_json"])
        except (json.JSONDecodeError, TypeError):
            continue  # malformed row - skip rather than fail the whole calendar
        out.append({
            "id": row["id"],
            "status": row["status"],
            "start_date": params.get("start_date"),
            "end_date": params.get("end_date"),
            "strategy_ids": params.get("strategy_ids") or [],
        })
    return out


# ----------------------------------------------------- backtest data fetch ---
# Mirrors the backtests table's own create/set_pid/start/finish/fail/
# fail_orphaned lifecycle (see above) exactly, for the same reason: an
# isolated subprocess (run_backtest_data_fetch.py) that the always-on
# dashboard process spawns and tracks rather than running fetch_backtest_
# data.py's long, IBKR-connected fetch in-process.
def create_backtest_data_fetch(account_id: int, mode: str = "paper") -> int:
    now = datetime.now(ET).isoformat(timespec="seconds")
    with get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO backtest_data_fetches (account_id, status, mode, created_at) VALUES (?, 'pending', ?, ?)",
            (account_id, mode, now),
        )
        return cur.lastrowid


def set_backtest_data_fetch_pid(fetch_id: int, pid: int):
    with get_conn() as conn:
        conn.execute("UPDATE backtest_data_fetches SET pid = ? WHERE id = ?", (pid, fetch_id))


def start_backtest_data_fetch(fetch_id: int):
    with get_conn() as conn:
        conn.execute("UPDATE backtest_data_fetches SET status = 'running' WHERE id = ?", (fetch_id,))


def finish_backtest_data_fetch(fetch_id: int, summary: dict):
    now = datetime.now(ET).isoformat(timespec="seconds")
    with get_conn() as conn:
        conn.execute(
            "UPDATE backtest_data_fetches SET status = 'done', summary_json = ?, finished_at = ? WHERE id = ?",
            (json.dumps(summary), now, fetch_id),
        )


def fail_backtest_data_fetch(fetch_id: int, error: str):
    now = datetime.now(ET).isoformat(timespec="seconds")
    with get_conn() as conn:
        conn.execute(
            "UPDATE backtest_data_fetches SET status = 'failed', error = ?, finished_at = ? WHERE id = ?",
            (error, now, fetch_id),
        )


def cancel_backtest_data_fetch(fetch_id: int, account_id: int) -> bool:
    """Same pattern as cancel_backtest: kills the subprocess by its
    recorded pid and marks the row failed, rather than leaving it to run
    (and time out through IBKR's own retry/backoff loop, symbol by
    symbol) for nothing after the user has already given up on it.
    Returns False if there's no matching row for this account in a
    cancellable state ('pending'/'running')."""
    with get_conn() as conn:
        row = conn.execute(
            "SELECT pid, status FROM backtest_data_fetches WHERE id = ? AND account_id = ?", (fetch_id, account_id)
        ).fetchone()
    if row is None or row["status"] not in ("pending", "running"):
        return False
    if row["pid"]:
        try:
            os.kill(row["pid"], signal.SIGTERM)
        except ProcessLookupError:
            pass  # already exited on its own - still fine to mark it failed below
    fail_backtest_data_fetch(fetch_id, "Cancelled by user")
    return True


def fail_orphaned_backtest_data_fetches():
    """Called once from web/app.py's startup handler, right alongside
    fail_orphaned_backtests - same os.kill(pid, 0) reasoning: a dashboard
    restart kills this subprocess along with the rest of dashboard.
    service's cgroup without it ever reaching its own except handler,
    leaving the row stuck at 'running' forever otherwise."""
    with get_conn() as conn:
        rows = conn.execute("SELECT id, pid FROM backtest_data_fetches WHERE status IN ('pending', 'running')").fetchall()
    for row in rows:
        alive = False
        if row["pid"]:
            try:
                os.kill(row["pid"], 0)
                alive = True
            except ProcessLookupError:
                alive = False
            except PermissionError:
                alive = True
        if not alive:
            fail_backtest_data_fetch(
                row["id"],
                "Orphaned - the dashboard restarted while this update was still running, which killed it. Re-run it.",
            )


def _backtest_data_fetch_row_to_dict(row) -> dict:
    result = dict(row)
    raw = result.pop("summary_json")
    result["summary"] = json.loads(raw) if raw else None
    return result


def get_backtest_data_fetch(fetch_id: int) -> dict | None:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM backtest_data_fetches WHERE id = ?", (fetch_id,)).fetchone()
    return _backtest_data_fetch_row_to_dict(row) if row else None


def get_latest_backtest_data_fetch(account_id: int) -> dict | None:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM backtest_data_fetches WHERE account_id = ? ORDER BY created_at DESC LIMIT 1",
            (account_id,),
        ).fetchone()
    return _backtest_data_fetch_row_to_dict(row) if row else None


# --------------------------------------------------------- backtest worker ---
# A remote backtest worker (see docs/worker.md, backtest_worker.py) is a
# script running on the user's OWN machine, polling this dashboard over
# HTTP instead of being spawned as a local subprocess (see run_backtest.py/
# api_create_backtest) - moving the CPU/memory cost of a backtest off the
# small always-on server entirely. It authenticates with a bearer token
# instead of a browser session cookie (see require_worker_token in
# web/app.py), so it needs its own token lifecycle here.
def _hash_worker_token(raw_token: str) -> str:
    return hashlib.sha256(raw_token.encode()).hexdigest()


def create_worker_token(account_id: int, label: str = "") -> tuple[int, str]:
    """Returns (token_id, raw_token) - raw_token is shown to the caller
    exactly once, right here; only its hash is ever persisted, the same
    pattern every API-key system uses, so a stolen db file alone can't be
    used to impersonate a worker. Generated with secrets.token_urlsafe
    (256 bits) rather than bcrypt-hashed like user passwords - this is a
    high-entropy random token, not a human-chosen low-entropy password,
    so it doesn't need bcrypt's deliberate slowness (which would also
    needlessly cost real CPU on every single claim poll)."""
    raw_token = secrets.token_urlsafe(32)
    now = datetime.now(ET).isoformat(timespec="seconds")
    with get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO worker_tokens (account_id, token_hash, label, created_at) VALUES (?, ?, ?, ?)",
            (account_id, _hash_worker_token(raw_token), label, now),
        )
        return cur.lastrowid, raw_token


def list_worker_tokens(account_id: int) -> list[dict]:
    """Never returns token_hash - there's no legitimate reason for the
    dashboard UI to need it, and it costs nothing to just not send it."""
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT id, label, created_at, last_seen_at FROM worker_tokens WHERE account_id = ? ORDER BY created_at DESC",
            (account_id,),
        ).fetchall()
        return [dict(r) for r in rows]


def delete_worker_token(token_id: int, account_id: int) -> bool:
    with get_conn() as conn:
        cur = conn.execute(
            "DELETE FROM worker_tokens WHERE id = ? AND account_id = ?", (token_id, account_id)
        )
        return cur.rowcount > 0


def verify_worker_token(raw_token: str) -> int | None:
    """Returns the owning account_id, or None if the token doesn't match
    any live row. Updates last_seen_at on every successful verification -
    purely for the dashboard's own "worker last seen" display, not used
    for any access-control decision."""
    now = datetime.now(ET).isoformat(timespec="seconds")
    with get_conn() as conn:
        row = conn.execute(
            "SELECT id, account_id FROM worker_tokens WHERE token_hash = ?",
            (_hash_worker_token(raw_token),),
        ).fetchone()
        if not row:
            return None
        conn.execute("UPDATE worker_tokens SET last_seen_at = ? WHERE id = ?", (now, row["id"]))
        return row["account_id"]


def claim_next_backtest(account_id: int) -> dict | None:
    """Atomically finds the oldest still-pending remote-mode backtest for
    this account and marks it claimed (status='running', claimed_at=now)
    in one UPDATE ... WHERE status='pending' - so two workers polling at
    once can't both claim the same row (whichever's UPDATE lands first
    changes status out from under the other, and rowcount tells the loser
    it got nothing). Returns the full params PLUS each requested
    strategy's own resolved rules/direction (a worker has no direct DB
    access of its own - everything it needs to actually run
    backtest_engine.simulate_strategy has to come back in this one
    response) - or None if there's nothing to claim."""
    now = datetime.now(ET).isoformat(timespec="seconds")
    with get_conn() as conn:
        row = conn.execute(
            "SELECT id FROM backtests WHERE account_id = ? AND status = 'pending' AND execution_mode = 'remote' "
            "ORDER BY created_at ASC LIMIT 1",
            (account_id,),
        ).fetchone()
        if not row:
            return None
        backtest_id = row["id"]
        cur = conn.execute(
            "UPDATE backtests SET status = 'running', claimed_at = ? WHERE id = ? AND status = 'pending'",
            (now, backtest_id),
        )
        if cur.rowcount == 0:
            return None  # lost the race to another worker's claim between the SELECT and this UPDATE
        backtest_row = conn.execute("SELECT * FROM backtests WHERE id = ?", (backtest_id,)).fetchone()
        params = json.loads(backtest_row["params_json"])
        strategies = {}
        for strategy_id in params["strategy_ids"]:
            strategy_row = conn.execute("SELECT * FROM strategies WHERE id = ?", (strategy_id,)).fetchone()
            if strategy_row:
                strategies[str(strategy_id)] = {
                    "name": strategy_row["name"],
                    "direction": strategy_row["direction"],
                    "rules": json.loads(strategy_row["rules_json"]),
                }
        return {"id": backtest_id, "params": params, "strategies": strategies}


def submit_worker_result(backtest_id: int, account_id: int, results: dict) -> bool:
    """Scoped to account_id so one account's worker can't write into
    another's backtest row. Returns False (does nothing) for a row that
    doesn't belong to this account, or isn't actually in the 'running'
    state a worker's own claim would have left it in (e.g. it was already
    cancelled, or requeue_abandoned_worker_backtests already gave up on
    it and a slow worker is now reporting in too late)."""
    with get_conn() as conn:
        row = conn.execute(
            "SELECT status FROM backtests WHERE id = ? AND account_id = ? AND execution_mode = 'remote'",
            (backtest_id, account_id),
        ).fetchone()
        if not row or row["status"] != "running":
            return False
    finish_backtest(backtest_id, results)
    return True


def fail_worker_backtest(backtest_id: int, account_id: int, error: str) -> bool:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT status FROM backtests WHERE id = ? AND account_id = ? AND execution_mode = 'remote'",
            (backtest_id, account_id),
        ).fetchone()
        if not row or row["status"] != "running":
            return False
    fail_backtest(backtest_id, error)
    return True


def requeue_abandoned_worker_backtests(timeout_minutes: int = 45):
    """Called periodically from web/app.py (a background asyncio task, not
    just at startup - a worker can go quiet at any time, not only across a
    dashboard restart). A remote backtest claimed more than timeout_minutes
    ago and still sitting at 'running' means the worker that claimed it
    went away (crashed, lost network, closed the laptop) without ever
    reporting a result - mirrors fail_orphaned_backtests' role for a local
    subprocess's dead pid, just detected by a time budget instead of
    os.kill, since a remote worker's liveness isn't something this process
    can check directly at all."""
    cutoff = (datetime.now(ET) - timedelta(minutes=timeout_minutes)).isoformat(timespec="seconds")
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT id FROM backtests WHERE execution_mode = 'remote' AND status = 'running' "
            "AND claimed_at IS NOT NULL AND claimed_at < ?",
            (cutoff,),
        ).fetchall()
    for row in rows:
        fail_backtest(row["id"], f"Abandoned by worker - no result within {timeout_minutes} minutes of being claimed. Re-run it.")


# ------------------------------------------------------------------ users ---
VALID_USER_ROLES = ("full", "viewer")


def create_user(username: str, password: str, is_admin: bool = False, role: str = "full") -> int:
    """Creates a new account and seeds it with a sane starting point: the
    conservative long default active (nothing active on the short side),
    mirroring what every fresh single-account deployment used to seed
    automatically before multi-account support existed."""
    if role not in VALID_USER_ROLES:
        raise ValueError(f"role must be one of {VALID_USER_ROLES}")
    password_hash = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
    with get_conn() as conn:
        try:
            cur = conn.execute(
                "INSERT INTO users (username, password_hash, is_admin, role) VALUES (?, ?, ?, ?)",
                (username, password_hash, int(is_admin), role),
            )
        except sqlite3.IntegrityError as exc:
            raise ValueError(f"Username '{username}' is already taken") from exc
        account_id = cur.lastrowid
        default_row = conn.execute(
            "SELECT id FROM strategies WHERE name = 'Long Breakout Conservative'"
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
            "SELECT id, username, is_admin, role FROM users WHERE username = ?", (username,)
        ).fetchone()
        return dict(row) if row else None


def any_users_exist() -> bool:
    with get_conn() as conn:
        row = conn.execute("SELECT COUNT(*) AS c FROM users").fetchone()
        return row["c"] > 0


# ------------------------------------------------------- user management ---
# Admin-only "Users" page (web/app.py's /users + /api/users*) - the only
# user-creation path before this was /setup, which only ever runs once and
# always creates a single is_admin=True account (see DEPLOY.md). These let
# an admin add more accounts afterward, in particular role='viewer' ones -
# read-only access to the Backtest page and nothing else (see
# require_full_access in web/app.py for the enforcement side).
def list_users() -> list[dict]:
    """No password_hash - this is for the admin-only Users page, which
    never needs it and should never even transit it over HTTP."""
    with get_conn() as conn:
        return [dict(r) for r in conn.execute("SELECT id, username, is_admin, role FROM users ORDER BY id")]


def set_user_role(user_id: int, role: str):
    if role not in VALID_USER_ROLES:
        raise ValueError(f"role must be one of {VALID_USER_ROLES}")
    with get_conn() as conn:
        conn.execute("UPDATE users SET role = ? WHERE id = ?", (role, user_id))


def delete_user(user_id: int):
    """Refuses to delete the last remaining admin - would leave the whole
    app with nobody able to reach the Users page (require_admin-gated) or
    the strategy-template catalog (also require_admin) to fix it."""
    with get_conn() as conn:
        row = conn.execute("SELECT is_admin FROM users WHERE id = ?", (user_id,)).fetchone()
        if row is None:
            return
        if row["is_admin"]:
            admin_count = conn.execute("SELECT COUNT(*) AS c FROM users WHERE is_admin = 1").fetchone()["c"]
            if admin_count <= 1:
                raise ValueError("Cannot delete the last remaining admin account")
        conn.execute("DELETE FROM users WHERE id = ?", (user_id,))


def list_account_ids() -> list[int]:
    """Every account that exists — used by morning_prefilter's gap scan
    (shared market data, run once, but written into every account's own
    watchlist rows) to know who to write to."""
    with get_conn() as conn:
        return [r["id"] for r in conn.execute("SELECT id FROM users ORDER BY id")]


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


# ----------------------------------------------------------- IBKR creds ---
# Values here are opaque to db.py — ibkr_password_encrypted is a
# Fernet-encrypted blob (see src/secrets_store.py), never a plaintext
# password. db.py only stores/retrieves it; it never decrypts it.
def set_ibkr_credentials(account_id: int, ibkr_username: str, ibkr_password_encrypted: bytes):
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO account_ibkr_credentials (account_id, ibkr_username, ibkr_password_encrypted, updated_at) "
            "VALUES (?, ?, ?, ?) "
            "ON CONFLICT(account_id) DO UPDATE SET "
            "ibkr_username = excluded.ibkr_username, "
            "ibkr_password_encrypted = excluded.ibkr_password_encrypted, "
            "updated_at = excluded.updated_at",
            (account_id, ibkr_username, ibkr_password_encrypted, datetime.now(ET).isoformat(timespec="seconds")),
        )


def get_ibkr_credentials(account_id: int) -> dict | None:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM account_ibkr_credentials WHERE account_id = ?", (account_id,)
        ).fetchone()
        return dict(row) if row else None


def has_ibkr_credentials(account_id: int) -> bool:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT 1 FROM account_ibkr_credentials WHERE account_id = ?", (account_id,)
        ).fetchone()
        return row is not None


# ------------------------------------------------------------- gateways ---
# Base/step for assigning fresh port pairs to accounts beyond the admin
# (whose row is seeded during migration with the ports it already runs
# on — see init_db). Chosen well clear of 4001/4002 so a handful of
# accounts can never collide with the admin's Gateway.
_GATEWAY_PORT_BASE = 4101
_GATEWAY_PORT_STEP = 10


def get_or_assign_gateway_ports(account_id: int) -> dict:
    """Returns this account's {paper_port, live_port}, assigning a fresh,
    unused pair on first call. Ports are permanent once assigned — an
    account's Gateway config always points at the same ports."""
    with get_conn() as conn:
        row = conn.execute(
            "SELECT paper_port, live_port FROM account_gateway_ports WHERE account_id = ?", (account_id,)
        ).fetchone()
        if row:
            return dict(row)

        taken = {r["live_port"] for r in conn.execute("SELECT live_port FROM account_gateway_ports")}
        live_port = _GATEWAY_PORT_BASE
        while live_port in taken:
            live_port += _GATEWAY_PORT_STEP
        paper_port = live_port + 1

        conn.execute(
            "INSERT INTO account_gateway_ports (account_id, paper_port, live_port) VALUES (?, ?, ?)",
            (account_id, paper_port, live_port),
        )
        return {"paper_port": paper_port, "live_port": live_port}
