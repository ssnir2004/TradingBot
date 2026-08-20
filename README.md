# TradingBot

An automated **paper-trading** bot for Interactive Brokers, built following
Humbled Trader's ["How to Build an AI Trading Bot with Claude Code and
Interactive Brokers"](https://www.humbledtrader.com/blog/ai-trading-bot-claude-ibkr/)
(Part 2 of a 3-part series — Part 1 built the "Trend Join Long" strategy and
backtested it on TradingView; Part 3 covers an AI premarket analyst).

It scans the S&P 500 for gappers every morning, trades a long-only 5-minute
breakout strategy, manages stops/partial profits/trailing stops, force-closes
everything before the close, and sends Telegram alerts — running as an
always-on service (see [DEPLOY.md](DEPLOY.md) for running it on a free cloud
server instead of a machine that has to stay on), controlled from a web
dashboard with login, live positions/trades, and a strategy switcher.

## ⚠️ Safety first

- **Defaults to paper trading.** `.env.example` ships with `IBKR_PORT=7497`
  (TWS paper) and `PAPER_TRADING=true`. `bot.py`, `trade.py`, and `cycle.py`
  all hard-abort if `PAPER_TRADING` and `IBKR_PORT` ever disagree (paper flag
  + live port, or vice versa) — this refusal is intentional, don't remove it.
- This is a **paper-trading learning project, not a finished trading
  system**. The backtested numbers from a TradingView/Pine backtest don't
  perfectly transfer to live execution — manual judgment isn't fully
  codified into a strategy's rules, and gap risk / halt risk / partial fills
  are real in live markets but invisible in a backtest.
- **Paper trade for at least 2 weeks** before ever pointing this at a live
  account. Watch the daily Telegram summaries and the dashboard; confirm the
  dashboard's open positions match TWS's positions panel exactly and that
  force-close reliably fires at 15:51 ET.
- The dashboard can start/stop trading and flatten every position — put a
  real password on it (the `/setup` first-run flow requires one) and run it
  behind HTTPS (see DEPLOY.md) before it's reachable from the internet.
- Not financial advice. Trading involves risk of loss.

## Architecture

```
rules.json                # the DEFAULT "Long Breakout Conservative" strategy, seeded into
                           # the DB on first run — after that, the DB (not this
                           # file) is the source of truth; edit strategies from
                           # the dashboard instead of this file post-setup
.env.example               # copy to .env: IBKR connection, sizing, Telegram

src/db.py                   # SQLite: trades, positions, strategies, settings,
                             # decision log — shared by every process below
src/perf.py                  # trade pairing / win-rate / R-multiple math

test_connect.py                # manual sanity check: can Python talk to TWS?
buy_one.py / close_one.py       # one-off manual test: buy/sell 1 share of MU
src/ibkr_client.py               # IBKRClient: connect, place_order, disconnect
strategy.py                       # single-symbol dev tool: time-gate + dedupe only
bot.py                              # CLI: evaluate one symbol, hand off to trade.py
trade.py                             # order execution (own IBKR client id)

src/sp500_tickers.py               # hardcoded S&P 500 universe (IBKR format)
morning_prefilter.py                # yfinance gap scanner -> DB watchlist
cycle.py                             # one tick of the trading cycle (see below)
daily_summary.py                      # Telegram daily P&L summary
src/notify.py                          # Telegram (+ optional ntfy) alerts

run_service.py                # the always-on trading engine process: an
                               # internal scheduler runs cycle.py/
                               # morning_prefilter.py/daily_summary.py on
                               # their cadences. Talks to IBKR. This is what
                               # deploy/trading-bot.service runs.
run_dashboard.py              # the dashboard process (FastAPI, web/app.py).
                               # Only reads/writes the DB — never touches
                               # IBKR — so it's safe to run as a separate
                               # process. This is what deploy/dashboard.service runs.
web/                           # dashboard backend (auth, API) + templates

deploy/                        # systemd units, Caddy reverse-proxy config,
                                # and IBC (headless IB Gateway login) config
DEPLOY.md                      # step-by-step: deploy all of the above to a
                                # free cloud server
```

Runtime data lives in `data/trading_bot.db` (SQLite, git-ignored) and
`logs/` (a couple of small fire-and-forget error logs) — nothing else on
disk carries state.

## Running it locally first

Before deploying to a server, run everything on your own machine against
paper trading to make sure it behaves the way you expect — the dashboard,
the strategy switcher, and the trading logic are all identical between local
and server deployment; only *how the three processes are kept running*
changes (systemd instead of you leaving three terminals open).

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
python -c "import secrets; print(secrets.token_hex(32))"   # paste into SESSION_SECRET
```

`.env` is git-ignored — fill in `SESSION_SECRET` (required, the dashboard
refuses to start without it), your Telegram token/chat id, and adjust
`PORTFOLIO_VALUE_USD` / `MAX_TRADES_PER_DAY` to taste.
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

### 7. Run the trading engine and the dashboard

```bash
python run_service.py     # terminal 1: the always-on trading engine
python run_dashboard.py   # terminal 2: the dashboard, http://127.0.0.1:8000
```

Open `http://127.0.0.1:8000` — first visit redirects to `/setup` to create
your dashboard login (this is the only account; there's no self-registration
after that). From there you get the live dashboard: enable/pause/flatten
controls, open positions, trade history, an R-multiple histogram, a
watchlist view, and the strategy switcher.

`run_service.py`'s internal scheduler handles everything Windows Task
Scheduler used to: the premarket prefilter scan (09:55–12:55 ET), the
trading cycle every 5 minutes (self-gates outside 10:00–16:00 ET), an
emergency-flatten check every 20s, and the daily summary at 16:05 ET — all
in one process, so there's nothing else to schedule separately.

You can still run any piece by hand for testing:

```bash
python morning_prefilter.py --dry-run   # preview the scan without writing the watchlist
python cycle.py                          # run exactly one tick of the trading cycle
python daily_summary.py                  # send the Telegram summary on demand
```

## The strategy: "Long Breakout Conservative"

The default strategy (seeded from `rules.json` into the DB on first run) —
long only, 5-minute chart, six filters split daily/intraday. All six must
pass for `cycle.py` to take a trade:

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

**Changing the strategy day to day is a dashboard action, not a file edit**:
the Strategies card lets you create/edit/activate/delete named strategies
(each one is just this same JSON shape); `cycle.py` always reads whichever
one is currently active from the DB. Adding a genuinely new filter *type*
(not just new thresholds) still means editing the filter evaluation code in
`cycle.py`.

## Deploying to a server

See **[DEPLOY.md](DEPLOY.md)** for a full walkthrough of running this on a
free Oracle Cloud Always Free instance: headless IB Gateway via IBC, three
systemd services, and Caddy for HTTPS in front of the dashboard. It also
covers the real limitation worth knowing up front: IBKR's 2FA can't be
fully eliminated for unattended login, only made rare.

## Troubleshooting

**`Connected: False`** — TWS/Gateway isn't running, wrong API port, Trusted
IPs missing, or API access isn't enabled in its settings.

**Orders rejected** — missing market data subscription, trading permissions
not enabled on the account, or wrong account type.

**Dashboard shows "no cycle data yet" / a stale last-cycle timestamp** —
`run_service.py` isn't running, or it can't reach IBKR (check
`ibgateway.service` on a server deployment — a 2FA prompt waiting for your
approval is the most common cause).

## Going live (do this last, and carefully)

1. Paper-trade for at least 2 weeks and confirm the bot's behavior matches
   your expectations every day.
2. Read `cycle.py` end-to-end — know exactly what it will do with real money.
3. In `.env`, set `IBKR_PORT=7496` (TWS live) and `PAPER_TRADING=false`
   together — the guard refuses to start if they disagree, which is
   intentional. Adjust `PORTFOLIO_VALUE_USD` to your real account.
4. Prefer **IB Gateway** over TWS for unattended production runs — no GUI,
   less memory, more reliable for long-running automation. Same code, just
   change `IBKR_PORT` to `4001` (live) / `4002` (paper).
5. Start small. Watch the first few live sessions closely.

## FAQ

- **Paid Claude subscription needed?** Claude Code requires Pro or Max.
- **Paid IBKR market data?** Not for paper trading US stocks — IBKR includes
  free delayed data on paper accounts.
- **IBKR Lite vs Pro?** The API is identical either way.
- **Different strategy?** Use the dashboard's Strategies card. New filter
  *types* need matching code changes in `cycle.py`.
- **Short selling / options / futures?** Not implemented — `cycle.py` and
  `trade.py` assume `Stock` contracts and long-only. Treat as a fork.
- **Other brokers?** Not without rewriting `src/ibkr_client.py` for that
  broker's API — most retail brokers don't have one as mature as IBKR's.
- **Updating the S&P 500 list?** `src/sp500_tickers.py` drifts as the index
  changes (additions, removals, spinoffs, ticker changes) — re-generate it
  every 2-3 months against a current source. A stale list causes yfinance
  lookup errors on delisted symbols in `morning_prefilter.py`.
- **What does "pause" do to open positions?** Nothing unsafe — pausing only
  stops new entries. Stop-loss, breakeven, partial-profit, and trailing-stop
  management on anything already open keeps running regardless of the
  enabled flag; only the emergency "flatten all now" button closes positions
  outright.
