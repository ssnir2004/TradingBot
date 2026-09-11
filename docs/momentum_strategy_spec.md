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
  universe.py         NASDAQ symbol list (reuses build_custom_universe) + cached float lookups
  backfill.py          historical reconstruction - see "Backfill / same-night validation" below
  selftest.py         `python -m momentum.selftest [--live]`

run_momentum.py             entrypoint: `--exec alert` (phase 1) | `--once`
run_momentum_backfill.py    one-shot historical reconstruction (see below) - not scheduled
deploy/momentum-scan.service systemd unit (no IBKR dependency in phase 1)
web/templates/momentum.html  dashboard page: master + per-strategy on/off switches, signal feed
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
| G11 | `exit.first_scale_r` · `use_second_scale` · `breakeven_after_first_scale` · `runner_trail` | 1.0R · **false** (changed 2026-09-11 - see Phase 2's exit-tuning note; the other 50% rides past first_scale_r instead of a fixed second scale-out) · true · `prior_5m_low` |
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
| G18 (C) | `ma_period` 9 · `ma_timeframe` 5m · `sideways_min_candles` 4 · `sideways_max_range_pct` 3.0 · `impulse_lookback_candles` 8 · `min_impulse_pct` 5.0 · `tap_tol_cents` 3 · `conviction` low |
| G19 (D) | `swing_fractal_bars` 2 · `require_higher_low` true · `max_base_candles` 15 · `stop_mode` shared |

---

## Phased delivery

| # | Deliverable | Done when |
|---|---|---|
| **1** ← *current* | Scanner + 4 detectors, **alert-only**, bar archival | signals in Telegram + `momentum_signals`; runs ~3 weeks |
| 2 | "Poor backtest" over the archived bars | per-pattern PnL report on real pre-screened data |
| 3 | Strategy **A** end-to-end + Shared Exit Engine | A live with circuit breakers |
| 4 | B, C, D live | all four live |

## Backfill / same-night validation

`momentum/backfill.py` (entrypoint `run_momentum_backfill.py`) exists because
waiting ~3 weeks for phase 1 to accumulate live signals is too slow to
validate the detectors against. It substitutes a proxy screen built from
data already fully accessible (see the module's own docstring for the full
reasoning and caveats): bulk yfinance DAILY bars over the NASDAQ-listed
universe approximate the G1/G3 screener, real yfinance 5-minute bars for
the surviving (symbol, day) pairs get replayed candle-by-candle with no
lookahead through the same four detectors, restricted to the real entry
window (not the full pre/post-market session). Results land in
`momentum_signals` tagged `mode="backfill"`.

**This server has under 1GB RAM and runs the live trading engine on the
same box** - `stream_daily_candidates` deliberately processes the universe
in small (25-symbol) chunks, screening and discarding each chunk's bars
immediately, rather than a single `yf.download` batch that would hold the
whole ~4300-symbol universe in memory at once (confirmed by testing: this
was a genuine risk before the fix, not a theoretical one). Always launch
a backfill under a memory cap so a bug here can never touch the live
engine's own cgroup:

```bash
sudo systemd-run --uid=tradingbot --gid=tradingbot --unit=momentum-backfill --scope \
  -p MemoryMax=220M -p MemoryHigh=190M -p CPUWeight=20 \
  bash -c 'cd /opt/tradingbot && .venv/bin/python run_momentum_backfill.py --days-back 45 --max-fetches 300'
```

A full run (~4300 symbols) takes ~20-25 minutes and holds under 200MB RSS
throughout (measured 2026-09-10/11). `--limit N` caps the universe for a
quick smoke test.

## Post-backfill tuning log

- **2026-09-10, Strategy C over-firing:** a 4328-symbol backfill (see below)
  showed C at 71% of all signals (347/488) - "stalled flag" was matching
  any 4 candles that simply didn't make a new high, scored against
  "the impulse" = any prior bar in the session. Added `sideways_max_range_pct`
  (the stall must actually be tight) and `min_impulse_pct`/
  `impulse_lookback_candles` (a real, recent, bounded run-up before it),
  tightened the EMA tap to the most recent candle only. Re-validated on a
  2500-symbol backfill: C dropped to 14/142 (10%), A/D now dominate (55%/33%),
  B stayed rare (2%, expected - the most specific of the four patterns).

## Phase 2 — backtest report (2026-09-11)

`momentum/backtest.py` (entrypoint `run_momentum_backtest.py`) replays every
`momentum_signals` row (`mode='backfill'`) forward through real 5-minute
RTH bars, applying the Shared Exit Engine (G7-G12) exactly as documented -
no lookahead. Reports **gross** (optimistic: fill at `entry_ref`, no
commissions) and **net** (realistic: `execution.slippage_cents` worse fill
+ the broker's own `commissions` schedule, both in `momentum/config.py`)
side by side, always - never just one number. Writes `outcome`/
`outcome_json` (both figures) back onto each signal row; shown on the
`/momentum` dashboard page too.

**Gross headline (155 signals, fill-at-entry_ref, no costs, original
two-scale exit):** 69.7% win rate, avg +0.82R, PF 4.7, ≈$7,650 nominal.
**Net (realistic: 3¢ slippage + commissions):** 55.5% win rate, avg
+0.23R, PF 1.75, ≈$2,155 nominal, $1,583 in commissions - about a quarter
of the optimistic number. This first net pass is what motivated the
exit-rule tuning below.

### Exit-rule tuning: dropping the second scale (2026-09-11)

The commission investigation above led to a real question: is locking in
25% of the position at exactly `second_scale_r` (2.0R) actually better
than letting that whole remaining 50% ride the trailing stop past
`first_scale_r`? Tested both directly on the same 155 signals (see
`momentum.config`'s `exit.use_second_scale`):

| variant | win rate | avg R (net) | PF (net) | commission |
|---|---|---|---|---|
| original (2 scales) | 58.1% | +0.24 | 1.81 | $1,583 |
| **drop 2nd scale (adopted)** | 52.3% | **+0.44** | **2.39** | $1,552 |
| widen stop to 20c (2 scales) | 52.3% | +0.19 | 1.78 | $959 |
| widen stop to 20c + drop 2nd scale | 52.3% | +0.27 | 2.14 | $905 |

**Dropping the second scale roughly doubled net avg R for essentially the
same commission** - the fixed 2R take-profit was cutting real winners
short more than it was protecting against reversals. Widening the stop to
20c was tried too (it does cut commission ~40%, since position size scales
inversely with stop distance) but **made results worse**, not better - the
proportionally farther targets became harder to reach before red-candle/
bailout closed the trade. **Rejected - do not revisit without new
evidence.** `exit.use_second_scale` is now `False` by default;
`second_scale_r`/`second_scale_price` are still computed and shown
(informational) but no longer acted on by the exit engine or the backtest.

**Current net headline (155 signals, second scale dropped):** 52.3% win
rate, avg **+0.44R**, total +67.4R, PF **2.39**, ≈**$4,043** nominal,
$1,552 in commissions - roughly double the net profitability of the
original exit rule, at essentially the same cost.

Two commission-specific notes that remain true regardless of exit rule:
(1) every leg of a trade - the entry buy, each scale-out sell, the final
exit - is its own order and its own fee; (2) with a fixed-cents stop, the
commission-as-%-of-risk ratio is structurally ~`2 x per_share_fee /
stop_dollars` (here, ~17-20%) **independent of account size** - the only
ways to change that ratio are a different stop distance (tried, rejected
above), fewer legs (the change adopted above), or a different broker fee
schedule (not investigated here - worth checking against alternate
commission plans separately, no code change needed either way).

Other reasons not to over-trust even the net figure:

1. **Outlier concentration.** A handful of trades (esp. one, D/ZSTK,
   +31.57R net) are a large share of total net R. A "poor backtest" this
   small is not resilient to a handful of extreme prints.
2. **Only 103 independent (symbol, day) events behind 155 signals** - up
   to 5 signals fired on the same symbol on the same day, so the true
   sample of independent market events is smaller than 155 suggests.
3. Sizing is illustrative ($12k nominal account, 0.5%/trade) - see
   momentum.backfill's own equity_usd=None note.

**By strategy (net, second scale dropped) - this changed the phase-3
ordering:** D is by far the strongest (win 52.8%, **PF 4.1**, +$2,175), A
solid (win 54.7%, PF 2.08, +$1,959), **C is a net LOSER once costs are
included** (PF 0.78, -$73 - it looked profitable gross), B stayed too
small a sample to read (n=2). **D, not A, is the stronger phase-3
candidate** - the original "A first" plan (chosen before this report
existed) should be revisited.

**Conclusion: encouraging, not a green light.** The strategies aren't
obviously broken (A and D stay net positive under realistic costs, and
meaningfully improved by the exit-rule fix), but this backtest cannot size
a live-capital decision on its own - the sample is small, correlated, and
outlier-driven, and real execution quality on 10-cent-stop penny-stock
breakouts is the single biggest unknown it can't answer (the 3¢ slippage
assumption is itself a guess, not measured). Before phase 3: keep phase
1's live alert-only scan running to accumulate genuinely independent,
forward (not backfilled) signals, and treat any live-execution numbers
phase 3 eventually produces as the real test - not this report.

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
