# TradingBot

An automated trading bot for Interactive Brokers, built following Humbled
Trader's ["How to Build an AI Trading Bot with Claude Code and Interactive
Brokers"](https://www.humbledtrader.com/blog/ai-trading-bot-claude-ibkr/)
(Part 2 of a 3-part series — Part 1 built the "Trend Join Long" strategy and
backtested it on TradingView; Part 3 covers an AI premarket analyst), and
substantially extended since.

It scans the S&P 500 (plus a few fundamentals-screened custom universes) for
setups every morning, runs several concurrent strategies — breakout,
Opening Range Breakout, and Touch & Turn scalping models, long and short —
manages stops/partial profits/trailing stops per strategy, force-closes
everything before the close, and sends Telegram alerts — running as an
always-on service (see [DEPLOY.md](DEPLOY.md) for running it on a free cloud
server instead of a machine that has to stay on), controlled from a web
dashboard with login, live positions/trades, a strategy switcher, a
backtest engine, and a decision-observability view.

## ⚠️ Safety first

- **This trades your real IBKR account. There is no paper/simulation mode**
  — paper trading was removed from this project entirely (see
  `src/db.py`'s own `MODES` comment). Every order this bot places is real
  money the moment `LIVE_PORTFOLIO_VALUE_USD` (or the dashboard's Risk
  Settings "Portfolio Value") is set above 0 — it starts at 0 by design, so
  position sizing comes out to zero shares and no real order can be placed
  until you deliberately raise it.
- **This is not a finished, fully-validated trading system.** Some
  strategies are backtested extensively against real historical intraday
  data (see the dashboard's Backtest page); others are newer and marked
  "aggressive" in the dashboard, meaning they haven't been. Backtested
  numbers don't perfectly transfer to live execution — gap risk, halt risk,
  and partial fills are real in live markets but invisible in a backtest.
- **Test before trusting a strategy with size.** Use the dashboard's
  Backtest page against real historical data first, and consider running a
  strategy in **Dry Run** mode (logs "would have entered" instead of
  placing a real order) for a while before activating it live. See the
  in-dashboard guide (`/guide`) for the fuller walkthrough.
- The dashboard can start/stop trading and flatten every position — put a
  real password on it (the `/setup` first-run flow requires one) and run it
  behind HTTPS (see DEPLOY.md) before it's reachable from the internet.
- Not financial advice. Trading involves risk of loss.

## Architecture

```
src/db.py                   # SQLite: accounts, users, trades, positions,
                             # strategies, strategy_runs, settings, decision
                             # log - shared by every process below. The DB,
                             # not any file on disk, is the source of truth
                             # for strategies once the dashboard is running.
src/mode_config.py          # per-account Gateway port / risk-param lookup
src/perf.py                 # trade pairing / win-rate / R-multiple math
src/ibkr_client.py          # IBKRClient + shared IBKR connection helpers
                             # (account-wide order lookup, cross-client
                             # cancel/modify - see its own docstrings)

src/sp500_tickers.py        # hardcoded S&P 500 universe (IBKR format)
src/custom_universes.py     # fundamentals-screened universes (market cap,
                             # beta, analyst rating) some strategies use
                             # instead of the full S&P 500
build_custom_universe.py    # builds/refreshes a custom universe's ticker
                             # list (scheduled weekly, see DEPLOY.md)
morning_prefilter.py        # yfinance gap scanner -> DB watchlist
cycle.py                    # one tick of the trading cycle: evaluates
                             # every active strategy's entry filters,
                             # manages stops/partials/trailing/force-close
                             # on everything already open
daily_summary.py            # Telegram daily P&L summary
src/notify.py                # Telegram (+ optional ntfy) alerts

src/backtest_engine.py       # replays a strategy against cached historical
                              # bars (entry timing + exit management)
src/backtest_runner.py       # shared glue between the CLI and dashboard
                              # backtest paths, so they can't drift apart
fetch_backtest_data.py       # IBKR intraday (5-min) bar cache builder,
                              # scheduled weekly (see DEPLOY.md)
src/es_filter.py             # optional ES-futures VWAP directional filter
                              # some strategies can require (see /guide)

run_service.py               # the always-on trading engine process: an
                              # internal scheduler runs cycle.py/
                              # morning_prefilter.py/daily_summary.py/
                              # fetch_backtest_data.py/build_custom_universe.py
                              # on their cadences. Talks to IBKR. This is
                              # what deploy/trading-bot-live.service runs.
run_dashboard.py             # the dashboard process (FastAPI, web/app.py).
                              # Only reads/writes the DB - never touches
                              # IBKR directly - so it's safe to run as a
                              # separate process. This is what
                              # deploy/dashboard.service runs.
web/                          # dashboard backend (auth, API) + templates:
                               # Strategies (/bot), Trading, Backtest,
                               # Decision Center, Telemetry, in-app Guide

deploy/                       # systemd units, Caddy reverse-proxy config,
                               # and IBC (headless IB Gateway login) config
DEPLOY.md                     # step-by-step: deploy all of the above to a
                               # free cloud server
```

There's also a handful of one-off manual sanity-check scripts from early
development (`test_connect.py`, `buy_one.py`/`close_one.py`) and dev/
research tools (`analyze_*.py`, `audit_*.py`, `run_optimization.py`,
`run_telemetry.py`) not covered above - none of them run as part of the
scheduled engine.

Runtime data lives in `data/trading_bot.db` (SQLite, git-ignored),
`data/backtest_bars/` (cached IBKR intraday bars), and `logs/` (a couple of
small fire-and-forget error logs) — nothing else on disk carries state.

## Running it locally first

Before deploying to a server, run everything on your own machine to make
sure it behaves the way you expect — the dashboard, the strategy switcher,
and the trading logic are all identical between local and server
deployment; only *how the two processes are kept running* changes (systemd
instead of you leaving two terminals open). Since there's no paper mode,
"trying it out" locally means using the dashboard's Backtest page and Dry
Run mode rather than placing real orders — see "⚠️ Safety first" above.

### 1. Interactive Brokers

1. Install [TWS](https://linktw.in/IBKR-HT) or (recommended for unattended
   runs) IB Gateway, and log into your real account.
2. **File → Global Configuration → API → Settings**:
   - Check "Enable ActiveX and Socket Clients"
   - Check "Allow connections from localhost only"
   - Set Socket Port = `7496` (TWS live) or `4001` (IB Gateway live)
   - Uncheck "Read-Only API"
   - Confirm `127.0.0.1` is under Trusted IPs
3. Restart TWS/Gateway. Leave it open and logged in whenever the bot runs.

### 2. Python environment

Requires Python 3.12+.

```bash
python -m venv .venv
.venv\Scripts\activate          # Windows; use `source .venv/bin/activate` on Mac/Linux
pip install -r requirements.txt
```

### 3. Configuration

```bash
cp .env.example .env
python -c "import secrets; print(secrets.token_hex(32))"   # paste into SESSION_SECRET
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"   # paste into CREDENTIALS_ENCRYPTION_KEY
```

`.env` is git-ignored — fill in `SESSION_SECRET` and `CREDENTIALS_ENCRYPTION_KEY`
(both required, the dashboard refuses to start without `SESSION_SECRET`),
your Telegram token/chat id, and confirm `LIVE_IBKR_PORT` matches whatever
you set in TWS/Gateway above. **Leave `LIVE_PORTFOLIO_VALUE_USD=0`** until
you've read "⚠️ Safety first" above and are genuinely ready — at 0 the
engine can only ever size a position to zero shares, so real orders are
physically impossible until you deliberately set a real number there
(from `.env` or from the dashboard's Risk Settings screen, which takes
precedence once you've saved a value there).

### 4. Telegram alerts (optional but recommended)

1. Message [@BotFather](https://t.me/BotFather) → `/newbot` → save the token
   into `TELEGRAM_BOT_TOKEN`.
2. Message [@userinfobot](https://t.me/userinfobot) → `/start` → save your
   chat id into `TELEGRAM_CHAT_ID`.
3. Test it: `python -c "from src.notify import notify; notify('Test', 'Hello from IBKR bot')"`

Leave both blank to disable Telegram — `notify()` becomes a silent no-op.

### 5. Run the trading engine and the dashboard

```bash
python run_service.py     # terminal 1: the always-on trading engine
python run_dashboard.py   # terminal 2: the dashboard, http://127.0.0.1:8000
```

Open `http://127.0.0.1:8000` — first visit redirects to `/setup` to create
your dashboard admin login. From there you get the live dashboard:
enable/pause/flatten controls, per-strategy positions and virtual trade
history, a watchlist view, the Strategies switcher, and a link to Trading
(manual order entry against the real account), Backtest, Decision Center,
and the in-app Guide (`/guide`) for a fuller walkthrough of every screen.

`run_service.py`'s internal scheduler handles everything a cron/Task
Scheduler setup used to: the premarket prefilter scan, the trading cycle
every 5 minutes (self-gates to market hours), an emergency-flatten check
every ~20s, the daily summary, and the weekly backtest-data/custom-universe
refresh jobs — all in one process, so there's nothing else to schedule
separately.

You can still run some pieces by hand for testing:

```bash
python morning_prefilter.py --dry-run   # preview the scan without writing the watchlist
python cycle.py                          # run exactly one tick of the trading cycle
python daily_summary.py                  # send the Telegram summary on demand
```

## Strategies

Multiple strategies can run concurrently, each independently switched
between **Off** / **Virtual** (evaluated and logged, no real order) /
**Live** from the dashboard's Strategies page — `cycle.py` evaluates every
active strategy's own entry filters each cycle and manages whatever's
already open according to that strategy's own exit/management style.
Strategy definitions (entry filters, exit rules, risk profile, and
human-readable notes) live in the DB, seeded on first run — **changing a
strategy day to day is a dashboard action, not a file edit**. See the
in-dashboard Guide (`/guide`) and each strategy's own notes (shown when you
open it in the Strategies editor) for exactly what each one does and its
current parameters, rather than this README, which would just drift out of
sync with them.

Broadly, the built-in strategies fall into a few families: gap-and-breakout
models (long and short, several risk variants), Opening Range Breakout,
and Touch & Turn liquidity-candle scalping — plus an optional ES-futures
VWAP directional filter any strategy can require. Adding a genuinely new
filter *type* (not just new thresholds on an existing one) means editing
the filter evaluation code in `cycle.py`.

## Backtesting

The dashboard's Backtest page (`/backtest`) replays a strategy (or several,
for comparison) against real historical bars and reports the same
Win rate / Profit factor / R-histogram breakdown as the live dashboard's
own performance card — see DEPLOY.md's "Backtest engine" section for how
the underlying data cache works and how to seed it on a fresh deploy.

## Deploying to a server

See **[DEPLOY.md](DEPLOY.md)** for a full walkthrough of running this on a
free Oracle Cloud Always Free instance: headless IB Gateway via IBC, three
systemd services, and Caddy for HTTPS in front of the dashboard. It also
covers the real limitation worth knowing up front: IBKR's 2FA can't be
fully eliminated for unattended login, only made rare.

## Troubleshooting

**Dashboard's Gateway status shows disconnected** — TWS/Gateway isn't
running, wrong API port, Trusted IPs missing, or API access isn't enabled
in its settings.

**Orders rejected** — missing market data subscription, trading permissions
not enabled on the account, or wrong contract/account type.

**Dashboard shows "no cycle data yet" / a stale last-cycle timestamp** —
`run_service.py` isn't running, or it can't reach IBKR (check
`ibgateway-live.service` on a server deployment — a 2FA prompt waiting for
your approval is the most common cause).

## FAQ

- **Paid Claude subscription needed?** Claude Code requires Pro or Max.
- **IBKR Lite vs Pro?** The API is identical either way.
- **Different strategy?** Use the dashboard's Strategies card. New filter
  *types* need matching code changes in `cycle.py`.
- **Short selling?** Supported — several built-in strategies are short-only
  (see the dashboard). **Options / futures?** Not implemented — `cycle.py`
  and `trade.py` assume `Stock` contracts. Treat as a fork.
- **Other brokers?** Not without rewriting `src/ibkr_client.py` for that
  broker's API — most retail brokers don't have one as mature as IBKR's.
- **Updating the S&P 500 list?** `src/sp500_tickers.py` drifts as the index
  changes (additions, removals, spinoffs, ticker changes) — re-generate it
  every 2-3 months against a current source. A stale list causes yfinance
  lookup errors on delisted symbols in `morning_prefilter.py`.
- **What does "Pause new entries" do to open positions?** Nothing unsafe —
  it only stops new entries. Stop-loss, breakeven, partial-profit, and
  trailing-stop management on anything already open keeps running
  regardless; only the emergency "Flatten all now" button closes positions
  outright.
