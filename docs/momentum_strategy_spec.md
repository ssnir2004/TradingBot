# Momentum Day-Trading Suite — Spec

Source of truth for the four momentum setups. All trading logic here is
extracted from a day-trading course chapter ("Warrior Trading — Ch. 6:
Momentum Trading Strategies, Part 2: Mid-Day Momentum"). Where the source
was silent on a number it is exposed as a config parameter with the
default agreed during planning (the "G1–G19" decisions below), never a
guessed hardcode.

This suite is **isolated** from `cycle.py` / the S&P 500 engine. It shares
only the SQLite file, `src.ibkr_client` (phase 3+), and `src.notify`.

---

## Architecture

```
momentum/
  scanner.py          TradingView public screener (scanner.tradingview.com) → Candidate list
  bars.py             yfinance 5m/1m/1d bars, archived per-symbol to data/momentum_bars/
  features.py         pure helpers: EMA, fractal swings, volume spike, overhead resistance, round levels
  signals.py          Signal dataclass + shared Exit/Risk math (G7–G12) + cooldown (G15)
  strategies/
    base.py           Detector ABC + DetectContext
    whole_half_dollar.py   A
    bull_flag.py           B
    ma_pullback.py         C
    setup_1234.py          D
  alert.py            Signal → momentum_signals row + decision_log + Telegram
  loop.py             one scan cycle: scan → shortlist → bars → G2/G6 → detectors → alert
  config.py           DEFAULTS (all G-decisions) + settings-table override (momentum:config_json)
  store.py            private tables: momentum_candidates / momentum_signals / momentum_bars_meta
  selftest.py         `python -m momentum.selftest [--live]`

run_momentum.py             entrypoint: `--exec alert` (phase 1) | `--once`
deploy/momentum-scan.service systemd unit (no IBKR dependency in phase 1)
```

### Data sources

| Need | Phase 1 source | Notes |
|---|---|---|
| Universe + float + intraday RVOL + change-from-open | TradingView public scanner endpoint | no auth, no MCP; poll ≥30s |
| Intraday 5m/1m bars | yfinance (same as `cycle.py`) | ~1–3 min delay, fine for alerting on closed candles |
| Daily bars (overhead resistance) | yfinance | |
| Real-time bars for entry timing | *(phase 3)* IBKR `reqRealTimeBars` | not needed for scan+alert |

OTC / pink-sheet symbols are dropped (`allowed_exchanges` = NASDAQ/NYSE/AMEX):
untradeable bars, wide spreads, hard borrows.

---

## The four setups

All four: entry timeframe **5m** (`patterns.entry_timeframe`), long only,
break confirmation per **G14** (`buffer` = 3¢ through the level by default).

- **A — WholeHalfDollarBreak** — surging stock breaks a whole/half-dollar
  level that sits within 2% of the session HOD. Entry ref = the level.
- **B — BullFlagFlatTop** — 3–6 mostly-green impulse candles, then a
  2–4-candle flat-top pullback (highs within 0.5%, retrace ≤50%), then a
  green candle makes a new high and breaks the flat top. Entry ref = flat top.
- **C — MAPullback9ema** — a flag that went sideways ≥4 candles into the
  9 EMA instead of breaking out; entry on the first new-high candle after
  the EMA tap, or the flat-top break after a fakeout. Always tagged
  `conviction: low` (never dropped — G18 note).
- **D — Setup1234** — fractal swing structure: low(1) → pivot high(2) →
  higher low(3) → break of the pivot(4). Entry ref = the pivot.

### Explicitly out of scope for v1
Micro Pullback (1m) setup; Breaking-News & Halt setup; Level-2 / Time-&-Sales
discretionary exits (all per the source / require feeds we don't have).

---

## G1–G19 — the gap decisions

### Screener (G1–G6)
| G | Parameter | Default |
|---|---|---|
| G1 | `screener.rvol_min` | 5.0 |
| G2 | `screener.volume_spike_mult` / `..._lookback_min` | 2.0× / 5 min (computed from 1m bars; **annotate-only** in v1) |
| G3 | `screener.price_preferred_min/max` · `price_hard_min/max` | 1.50–10.00 preferred · 1.00–20.00 hard; >10 flagged `above_preferred_range`, 5m signals only |
| G4 | `screener.entry_window_et` · `premarket_enabled` · `afternoon_enabled` | 09:30–12:00 ET · false · false |
| G5 | `screener.require_catalyst` | false (no news classification in v1) |
| G6 | `screener.resistance_filter_mode` · `_lookback_days` · `_headroom_pct` | `annotate` · 60 · 10% |

### Exit / Risk (G7–G12)
| G | Parameter | Default |
|---|---|---|
| G7 | `exit.stop_mode` · `stop_cents` · `max_loss_cents` | `fixed_cents` · 10 · 20 (risk normalised via sizing, not by scaling the stop) |
| G8 | `exit.red_candle_exit_enabled` · `red_candle_hold_min_r` | true · 2.0R |
| G9 | `risk.per_trade_pct` · `max_concurrent_positions` · `max_position_notional_usd` · `daily_max_loss_usd` · `daily_max_trades` | 0.5% · 1 · 3000 · 200 · 5 |
| G10 | `exit.bailout_minutes` · `bailout_exit_type` | 5 · `market` |
| G11 | `exit.first_scale_r` / `second_scale_r` · `breakeven_after_first_scale` · `runner_trail` | 1.0R / 2.0R · true · `prior_5m_low` |
| G12 | `strategy_D.stop_mode` | `shared` (inherits the 10¢ stop; `pivot_low` available) |

*Phase 1 computes these levels and shows them in the alert; nothing is
managed until phase 3.*

### Pattern primitives (G13–G15)
| G | Parameter | Default |
|---|---|---|
| G13 | `patterns.entry_timeframe` · `allow_1m` | 5m · false |
| G14 | `patterns.break_confirm_mode` · `break_buffer_cents` | `buffer` · 3 |
| G15 | `patterns.new_high_ref` · `swing_fractal_bars` · `signal_cooldown_min` · `max_signals_per_symbol_per_day` | `prior_candle` · 2 · 15 · 3 |

### Per-strategy (G16–G19)
| G | Parameters (defaults) |
|---|---|
| G16 (A) | `levels` [whole, half] · `arm_distance_cents` 15 · `require_within_hod_pct` 2.0 |
| G17 (B) | `impulse_min/max_candles` 3/6 · `impulse_max_counter_candles` 1 · `pullback_min/max_candles` 2/4 · `flat_top_tol_pct` 0.5 · `pullback_max_retrace_pct` 50 |
| G18 (C) | `ma_period` 9 · `ma_timeframe` 5m · `sideways_min_candles` 4 · `tap_tol_cents` 5 · `conviction` low |
| G19 (D) | `swing_fractal_bars` 2 · `require_higher_low` true · `max_base_candles` 15 · `stop_mode` shared |

---

## Phased delivery

| # | Deliverable | Done when |
|---|---|---|
| **1** ← *current* | Scanner + 4 detectors, **alert-only**, bar archival | signals in Telegram + `momentum_signals`; runs ~3 weeks |
| 2 | "Poor backtest" over the archived bars | per-pattern PnL report on real pre-screened data |
| 3 | Strategy **A** end-to-end + Shared Exit Engine | A live with circuit breakers |
| 4 | B, C, D live | all four live |

## Phase 1 caveats / known limits

- Detectors B/C/D pattern-matching is a first cut — will be tuned against
  real signals collected in phase 1.
- yfinance intraday lag means alerts are informational, not fill-accurate.
- G2 (volume spike) and G6 (overhead resistance) are **annotate-only** —
  they never reject a candidate in v1, just tag the signal.
- No orders are placed. `run_momentum.py --exec live` exits with an error.

## Operating

```bash
# one-off dry run (no Telegram): 
MOMENTUM_ALERT_DRYRUN=1 .venv/bin/python run_momentum.py --once

# the service (after `systemctl enable --now momentum-scan`):
journalctl -u momentum-scan -f
```

Config overrides: write JSON to the `momentum:config_json` settings row
(deep-merged over `DEFAULTS`).
