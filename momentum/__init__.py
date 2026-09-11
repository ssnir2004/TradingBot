"""Momentum day-trading suite — a self-contained add-on to TradingBot.

This package is deliberately isolated from cycle.py / the S&P 500 engine:
it has its own scanner (TradingView public screener), its own intraday bar
cache (data/momentum_bars/), its own SQLite tables (all created by
momentum.store, never added to db.SCHEMA), and its own always-on process
(run_momentum.py -> deploy/momentum-scan.service). It only *reuses* the
shared low-level helpers: src.db.get_conn (same DB file), src.ibkr_client
(same live Gateway, read-only in alert mode), src.notify (Telegram).

Phase 1 scope (see docs/momentum_strategy_spec.md): scan + detect + alert,
NO order placement. Every strategy also archives the intraday bars of each
candidate it fires on, so a "poor backtest" can be run later against real,
pre-screened data.
"""
