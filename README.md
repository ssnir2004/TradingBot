# TradingBot

An automated **paper-trading** bot for Interactive Brokers, built following
Humbled Trader's ["How to Build an AI Trading Bot with Claude Code and
Interactive Brokers"](https://www.humbledtrader.com/blog/ai-trading-bot-claude-ibkr/)
(Part 2 of a 3-part series — Part 1 built the "Trend Join Long" strategy and
backtested it on TradingView; Part 3 covers an AI premarket analyst).

Every morning it scans the S&P 500 for gappers, trades a long-only 5-minute
breakout strategy defined in `rules.json`, manages stops/partial
profits/trailing stops, force-closes everything before the close, sends
Telegram alerts, and publishes an HTML performance dashboard — on a schedule,
with no manual intervention.

## ⚠️ Safety first

- **Defaults to paper trading.** `.env.example` ships with `IBKR_PORT=7497`
  (TWS paper) and `PAPER_TRADING=true`. `bot.py`, `trade.py`, and `cycle.py`
  all hard-abort if `PAPER_TRADING` and `IBKR_PORT` ever disagree (paper flag
  + live port, or vice versa) — this refusal is intentional, don't remove it.
- This is a **paper-trading learning project, not a finished trading
  system**. The backtested numbers from a TradingView/Pine backtest don't
  perfectly transfer to live execution — manual judgment isn't fully
  codified into `rules.json`, and gap risk / halt risk / partial fills are
  real in live markets but invisible in a backtest.
- **Paper trade for at least 2 weeks** before ever pointing this at a live
  account. Watch the daily Telegram summaries and the dashboard; confirm
  `open_positions.json` matches TWS's positions panel exactly and that
  force-close reliably fires at 15:51 ET.
- Not financial advice. Trading involves risk of loss.

## Architecture

```
rules.json                # the strategy, in one file: filters, exits, risk
.env.example               # copy to .env: IBKR connection, sizing, Telegram

test_connect.py             # step-3 sanity check: can Python talk to TWS?
buy_one.py / close_one.py    # one-off manual test: buy/sell 1 share of MU

src/ibkr_client.py            # IBKRClient: connect, place_order, disconnect
strategy.py                    # single-symbol dev tool: time-gate + dedupe only
bot.py                           # CLI: evaluate one symbol, hand off to trade.py
trade.py                          # order execution (own IBKR client id)

src/sp500_tickers.py               # hardcoded S&P 500 universe (IBKR format)
morning_prefilter.py                # yfinance gap scanner -> watchlist.txt
cycle.py                             # the autonomous 5-minute trading cycle
src/notify.py                         # Telegram (+ optional ntfy) alerts

compute_perf.py                       # daily summary + dashboard/index.html
rotate_logs.py                         # log/trade-history rotation

setup_schedule.py                      # registers 11 Windows Task Scheduler jobs
cleanup_schedule.py                     # tears them all down
```

Runtime files (git-ignored, created automatically): `trades.csv`,
`open_positions.json`, `safety-check-log.json`, `watchlist.txt`,
`logs/`, `dashboard/index.html`.

## The strategy: "Trend Join Long"

Defined entirely in `rules.json` — long only, 5-minute chart, six filters
split daily/intraday. All six must pass for `cycle.py` to take a trade:

**Daily:**
- **D1** — price above yesterday's daily high
- **D2** — yesterday's close above the 200-day SMA (trade with the longer trend)
- **D3** — gap of at least 3% from the previous close

**Intraday:**
- **I1** — price above today's premarket high
- **I2** — price above today's high-so-far (joining strength, not a fade)
- **I3** — relative volume at least 2x the 14-day average

**Exit:** initial stop at low-of-day − 1%. At +0.75R, sell 1/3 and move the
stop to entry × 0.99. At +1R (if the partial hasn't already fired), move the
stop to breakeven. After breakeven, the stop trails the latest confirmed
5-minute swing low (a bar whose low is below the 2 bars before *and* the 2
bars after it), minus a cent — stops only ever ratchet up. Everything force-
closes at 15:51 ET.

Want a different strategy? Edit `rules.json` — the bot doesn't care what's
in there as long as the keys match. Adding a genuinely new filter type means
also updating the filter evaluation in `cycle.py` (and `strategy.py` if you
want it in the single-symbol dev tool too).

## Setup

### 1. Interactive Brokers

1. Install [TWS](https://linktw.in/IBKR-HT) (or IB Gateway) and log into a
   **paper trading** account — a separate paper sub-account is recommended
   so this doesn't interfere with anything else in your real account.
2. **File → Global Configuration → API → Settings**:
   - Check "Enable ActiveX and Socket Clients"
   - Check "Allow connections from localhost only"
   - Set Socket Port = `7497` (paper trading)
   - Uncheck "Read-Only API"
   - Confirm `127.0.0.1` is under Trusted IPs
3. Restart TWS. Leave it open and logged in whenever the bot runs.

### 2. Python environment

Requires Python 3.12+.

```bash
python -m venv .venv
.venv\Scripts\activate          # Windows; use `source .venv/bin/activate` on Mac/Linux
pip install -r requirements.txt
```

### 3. Prove the connection works

```bash
python test_connect.py
```

Expect `Connected: True` and your paper account ID (`DU#######`). If not,
see Troubleshooting below.

### 4. Configuration

```bash
cp .env.example .env
```

`.env` is git-ignored (it's local machine config, not a secret store you'd
publish) — fill in your Telegram token/chat id and adjust
`PORTFOLIO_VALUE_USD` / `MAX_TRADE_SIZE_USD` / `MAX_TRADES_PER_DAY` to taste.
Leave `IBKR_PORT=7497` and `PAPER_TRADING=true` until you've read the
"Going live" section below.

### 5. Telegram alerts (optional but recommended)

1. Message [@BotFather](https://t.me/BotFather) → `/newbot` → save the token
   into `TELEGRAM_BOT_TOKEN`.
2. Message [@userinfobot](https://t.me/userinfobot) → `/start` → save your
   chat id into `TELEGRAM_CHAT_ID`.
3. Test it: `python -c "from src.notify import notify; notify('Test', 'Hello from IBKR bot')"`

Leave both blank to disable Telegram — `notify()` becomes a silent no-op.

### 6. Manual smoke test (market hours only)

```bash
python buy_one.py     # buys 1 share of MU on paper
python close_one.py   # closes it again
```

Confirms the whole Claude → Python → IBKR chain before you trust it with the
real strategy logic.

```bash
python bot.py --symbol NVDA --check-only   # dry run: time-gate + price, no order
python bot.py --symbol NVDA                # places a paper order if the gate is open
```

### 7. Build today's watchlist

```bash
python morning_prefilter.py --dry-run   # preview, doesn't write watchlist.txt
python morning_prefilter.py             # writes watchlist.txt for real
```

On a flat day with no 3%+ gappers, `watchlist.txt` will be empty — lower
`--min-gap` (e.g. `--min-gap 1.0`) to get survivors while testing.

### 8. Run one trading cycle by hand

```bash
python cycle.py
```

Outside market hours this exits in under a second (`weekend` / `too_early` /
`closed`) — that's correct. During market hours it does the full 9-step
cycle: reconcile stop-outs, manage open positions, scan for entries (or
force-close near the bell).

### 9. Schedule everything (Windows)

```bash
python setup_schedule.py
```

Registers 11 Task Scheduler jobs: log rotation (09:25 ET), keep-awake
(09:30 ET), 7x premarket prefilter scans (09:55–12:55 ET), the trading cycle
every 5 minutes starting 10:00 ET, and the dashboard/summary at 16:05 ET.
All run in your user context — no admin elevation, no SYSTEM account.

```bash
python cleanup_schedule.py   # tear it all down when you're done
```

**Not on Windows?** Same Python code and `rules.json` work unchanged on Mac
(`launchd`) or Linux/a cheap VPS with IB Gateway + `cron` — you'd translate
the 11 jobs above into `launchd` plists or `cron` entries yourself;
`setup_schedule.py`/`cleanup_schedule.py` as written are Windows-only
(`schtasks`/`powercfg`).

### 10. Check performance

```bash
python compute_perf.py
```

Sends a Telegram daily summary and (re)writes `dashboard/index.html` — open
it directly in a browser. Shows today's P&L/win-rate, an R-multiple
histogram, open positions, and the last 20 closed trades.

## Troubleshooting

**`Connected: False`** — TWS isn't running, wrong API port, Trusted IPs
missing, or API access isn't enabled in TWS settings.

**Orders rejected** — missing market data subscription, trading permissions
not enabled on the account, or wrong account type.

**Scheduled tasks not running** — the machine is asleep, the user is logged
out, or it's a Task Scheduler permissions issue. `HT_KeepAwake` handles
sleep on AC power; it does not help if you're on battery or the machine is
fully shut down.

## Going live (do this last, and carefully)

1. Paper-trade for at least 2 weeks and confirm the bot's behavior matches
   your expectations every day.
2. Read `cycle.py` end-to-end — know exactly what it will do with real money.
3. In `.env`, set `IBKR_PORT=7496` (TWS live) and `PAPER_TRADING=false`
   together — the guard refuses to start if they disagree, which is
   intentional. Adjust `PORTFOLIO_VALUE_USD`/`MAX_TRADE_SIZE_USD` to your
   real account.
4. Prefer **IB Gateway** over TWS for unattended production runs — no GUI,
   less memory, more reliable for long-running automation. Same code, just
   change `IBKR_PORT` to `4001` (live) / `4002` (paper).
5. Start small. Watch the first few live sessions closely.

## FAQ

- **Paid Claude subscription needed?** Claude Code requires Pro or Max.
- **Paid IBKR market data?** Not for paper trading US stocks — IBKR includes
  free delayed data on paper accounts.
- **IBKR Lite vs Pro?** The API is identical either way.
- **Different strategy?** Edit `rules.json`. New filter *types* need
  matching code changes in `cycle.py`.
- **Short selling / options / futures?** Not implemented — `cycle.py` and
  `trade.py` assume `Stock` contracts and long-only. Treat as a fork.
- **Other brokers?** Not without rewriting `src/ibkr_client.py` for that
  broker's API — most retail brokers don't have one as mature as IBKR's.
- **Updating the S&P 500 list?** `src/sp500_tickers.py` drifts as the index
  changes (additions, removals, spinoffs, ticker changes) — re-generate it
  every 2-3 months against a current source. A stale list causes yfinance
  lookup errors on delisted symbols in `morning_prefilter.py`.
