# TradingBot

An automated **paper-trading** bot that connects [Claude Code](https://claude.com/claude-code)-style
development to Interactive Brokers: it scans a watchlist premarket, applies a
long-only, trend-following 5-minute strategy ("Trend Join Long"), places
bracket orders through the IBKR API, manages stops/partial profits/trailing
stops, force-closes everything before the close, and sends you Telegram
alerts along the way.

> **Provenance note:** this was built based on the publicly described shape of
> [Humbled Trader's "Build an AI Trading Bot with Claude + Interactive
> Brokers"](https://www.humbledtrader.com/blog/ai-trading-bot-claude-ibkr/)
> (part 2 of a 3-part series: TradingView strategy → IBKR execution bot →
> premarket analyst). The article itself wasn't reachable from this
> environment's network, so the exact filter thresholds, code, and prompts in
> the original post are **not** reproduced here — this is an independent,
> best-effort implementation of the same idea (premarket scan → trend
> strategy → bracket orders → risk management → Telegram alerts → paper
> trading on IBKR). Treat every threshold in `.env.example` as a starting
> point to tune against your own backtests, not as a transcription of the
> source article.

## ⚠️ Safety first

- **Defaults to IBKR's paper-trading port (7497) and `DRY_RUN=true`.** In dry
  run, the bot logs and Telegram-alerts every order it *would* place but never
  actually sends it to IBKR — use this to validate the scanner/strategy/risk
  logic end-to-end with zero risk.
- `config.py` **refuses to start** if `IBKR_PORT` is a live port (7496/4001)
  unless you explicitly set `ALLOW_LIVE_TRADING=true` in `.env`. Don't flip
  that switch until you've run extensively on paper and understand every line
  of `bot/strategy.py` and `bot/risk_manager.py`.
- This is educational software, not financial advice. Markets involve risk of
  loss; past backtest performance does not guarantee future results.

## Architecture

```
main.py                 # orchestration loop (scan -> entries -> manage -> force-close)
config.py                # env-driven configuration + live-trading safety switch
bot/
  ibkr_client.py          # ib_async wrapper: connect, historical bars, bracket orders
  scanner.py               # premarket gap/RVOL/price scanner (yfinance, no paid feed)
  strategy.py               # "Trend Join Long" signal logic (daily + 5-min filters)
  risk_manager.py            # position sizing, daily loss halt, partials, trailing stop
  telegram_alerts.py          # Telegram Bot API notifications
  trade_journal.py             # CSV trade log for post-session review
  market_hours.py               # US/Eastern market-hours helpers
tests/test_strategy.py    # unit tests for the strategy signal logic
data/                      # trade_journal.csv + bot.log written here at runtime
```

## Strategy: "Trend Join Long"

Long-only, evaluated on 5-minute bars, six filters split daily/intraday:

**Daily (context, must all hold):**
1. Price above the daily 50-SMA
2. Price above the daily 200-SMA

**Intraday, 5-min bars (must all hold on the signal bar):**
3. Price above VWAP
4. 9-EMA above 20-EMA, and price above the 9-EMA
5. Relative volume on the signal bar ≥ 1.5× the 20-bar average
6. Close breaks above the high of the prior 6 bars (~30-min opening range)

**Entry:** market order on bar-close when all six filters pass.
**Initial stop:** signal-bar low, minus a small ATR buffer.
**Partial profit:** take `PARTIAL_PROFIT_SIZE_PCT` off at
`entry + PARTIAL_PROFIT_R_MULTIPLE × risk`.
**Trailing stop:** remainder trails the highest price seen by
`TRAILING_STOP_ATR_MULT × ATR`.
**Force close:** any open position is flattened at `FORCE_CLOSE_TIME`
(default 15:55 ET), before the close.

All of the above thresholds are configurable in `.env` — tune them to match
whatever you've actually backtested (e.g. in TradingView, per part 1 of the
source series) rather than trusting the defaults blindly.

## Setup

### 1. Interactive Brokers

1. Install [Trader Workstation (TWS)](https://www.interactivebrokers.com/en/trading/tws.php)
   or IB Gateway, and log into a **paper trading** account (IBKR Lite or Pro —
   the API is identical either way).
2. In TWS: **File → Global Configuration → API → Settings**
   - Check "Enable ActiveX and Socket Clients"
   - Note the socket port (paper TWS defaults to `7497`; Gateway paper is
     `4002`)
   - Add `127.0.0.1` to "Trusted IPs" if running the bot on the same machine
   - Uncheck "Read-Only API" (required to place orders)
3. Leave TWS/Gateway running and logged in whenever the bot runs — it talks
   to IBKR over that local socket, it does not use REST/cloud credentials.

### 2. Python environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3. Configuration

```bash
cp .env.example .env
```

Fill in at least: IBKR host/port (leave the paper defaults unless you know
you want live), your Telegram bot token/chat id (optional, see below), your
scan universe, and risk parameters.

### 4. Telegram alerts (optional)

1. Message [@BotFather](https://t.me/BotFather) on Telegram, run `/newbot`,
   copy the token into `TELEGRAM_BOT_TOKEN`.
2. Send your new bot any message, then open
   `https://api.telegram.org/bot<token>/getUpdates` in a browser and copy the
   `chat.id` value into `TELEGRAM_CHAT_ID`.
3. Leave both blank to disable Telegram alerts entirely (the bot logs to
   console/`data/bot.log` regardless).

### 5. Run it

With TWS/Gateway running and logged into paper trading:

```bash
python3 main.py
```

The bot will:
- Connect to IBKR and report your paper account's net liquidation value
- Run the premarket scan once markets reach `PREMARKET_SCAN_TIME`
- Evaluate entries against the strategy while the market is open
- Manage stops/partials/trailing stops on any open position
- Force-close everything at `FORCE_CLOSE_TIME`
- Log every action to `data/bot.log` and `data/trade_journal.csv`, and alert
  Telegram if configured

Stop it any time with `Ctrl+C` — it disconnects from IBKR cleanly.

### 6. Tests

```bash
python3 -m pytest tests/ -v
```

## Going live (do this last, and carefully)

1. Paper-trade for long enough to trust the strategy, sizing, and alerts.
2. Read `bot/risk_manager.py` and `bot/strategy.py` end-to-end — know exactly
   what the bot will do with real money.
3. In `.env`, set `IBKR_PORT` to a live port (7496 TWS / 4001 Gateway),
   `ALLOW_LIVE_TRADING=true`, and `DRY_RUN=false`.
4. Start small. Watch the first few sessions closely.
