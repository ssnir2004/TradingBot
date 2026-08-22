# Deploying to a free server (Oracle Cloud Always Free)

This turns the bot from "runs on my laptop via Task Scheduler" into "runs
24/7 on a server, controlled from a web dashboard." Paper and live run as
two entirely separate, simultaneous pipelines — each is its own IB Gateway
process plus its own trading engine, five systemd services in total:

- **`ibgateway-paper.service`** / **`ibgateway-live.service`** — two separate IB Gateway processes, headless, each kept logged into its own trading mode by IBC
- **`trading-bot-paper.service`** / **`trading-bot-live.service`** — two separate `run_service.py --mode paper|live` engines, each talking only to its own Gateway
- **`dashboard.service`** — `run_dashboard.py`, the FastAPI web dashboard (a Paper/Live tab selector; only reads/writes the shared DB, never touches IBKR)

All five read/write the same SQLite DB at `data/trading_bot.db`, tagged by
mode, so the dashboard sees whatever either engine is doing in near-real-time
without any direct connection between the processes.

Running both modes at once roughly doubles the resource footprint (two IB
Gateway JVMs) — see the RAM note in Step 1 before picking a shape.

**Do this whole setup on paper trading first.** Nothing here is safer just
because it's "on a server" — the same paper-first, 2-week soak, then-go-live
guidance from the main README still applies.

## 1. Create the server

1. Sign up for [Oracle Cloud](https://www.oracle.com/cloud/free/) (needs a
   card for verification, but the Always Free tier genuinely never charges).
2. Create a compute instance:
   - Shape: **VM.Standard.A1.Flex** (Ampere ARM, Always Free) — 2-4 OCPUs /
     12-24 GB RAM is comfortably enough for two IB Gateway processes + both
     bot engines + the dashboard. If you hit "Out of host capacity" (a known
     Oracle free-tier issue in busy regions), retry in a different
     availability domain/region, or fall back to the smaller
     **VM.Standard.E2.1.Micro** (AMD, also Always Free, 1 GB RAM). Running
     BOTH paper and live Gateway processes on 1 GB is tight — each Gateway
     JVM alone runs ~450-550 MB — expect to lean on swap heavily and to see
     real memory pressure. It's workable but not comfortable; if you only
     want one mode running, skip the `-live` services below entirely and
     you're back to the single-Gateway footprint.
   - Image: Ubuntu 24.04 (or 22.04).
   - Add your SSH key during creation.
3. In the instance's **Virtual Cloud Network → Security List**, add ingress
   rules for TCP 80 and 443 (0.0.0.0/0) — Caddy needs these for HTTPS. Leave
   everything else closed; the dashboard itself binds to `127.0.0.1` only,
   so it's never reachable except through Caddy.
4. Note the instance's public IP.

## 2. Point a free hostname at it

Caddy's automatic HTTPS needs a real domain name (not a bare IP). If you
don't already have one, [DuckDNS](https://www.duckdns.org/) gives you a free
`yourname.duckdns.org` subdomain — sign in, create a subdomain, point it at
your instance's public IP. Update it whenever the IP changes (Oracle Always
Free instances keep a fixed public IP unless you delete and recreate them).

## 3. Base setup on the server

```bash
ssh ubuntu@<your-instance-ip>

sudo apt update && sudo apt install -y python3.12 python3.12-venv git \
    xvfb unzip curl

# Caddy (official repo)
sudo apt install -y debian-keyring debian-archive-keyring apt-transport-https
curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/gpg.key' | \
    sudo gpg --dearmor -o /usr/share/keyrings/caddy-stable-archive-keyring.gpg
curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/debian.deb.txt' | \
    sudo tee /etc/apt/sources.list.d/caddy-stable.list
sudo apt update && sudo apt install -y caddy

sudo useradd -m -s /bin/bash tradingbot
```

## 4. Install IB Gateway + IBC

These are third-party downloads, not part of this repo (IBKR's own
installer, and the IBC automation project) — get the current versions
yourself and double check the install steps against their own docs, since
both change over time:

```bash
sudo -iu tradingbot
mkdir -p /tmp/ibinstall && cd /tmp/ibinstall

# IB Gateway (get the current Linux offline installer link from
# https://www.interactivebrokers.com/en/trading/ibgateway-stable.php)
curl -O <ibgateway-linux-installer-url>
chmod +x ibgateway-*-standalone-linux-x64.sh
sudo ./ibgateway-*-standalone-linux-x64.sh   # installs to /opt/ibgateway by default

# IBC (get the current release from https://github.com/IbcAlpha/IBC/releases)
curl -L -o ibc.zip <ibc-release-zip-url>
sudo mkdir -p /opt/ibc && sudo unzip ibc.zip -d /opt/ibc
sudo chmod +x /opt/ibc/scripts/*.sh
```

Adjust `TWS_PATH`/`IBC_PATH` in `deploy/ibc/start-gateway.sh` if your install
paths differ from `/opt/ibgateway` and `/opt/ibc`. One IB Gateway install is
shared by both modes — `start-gateway.sh paper`/`start-gateway.sh live` each
make their own copy of IBC's `gatewaystart.sh` and point it at separate
settings/log directories, so the two running instances never collide.

## 5. Deploy the bot's code

```bash
sudo -iu tradingbot
git clone <your-fork-url> /opt/tradingbot
cd /opt/tradingbot
git checkout claude/ai-trading-bot-ibkr-haqc10   # or main, once merged

python3.12 -m venv .venv
.venv/bin/pip install -r requirements.txt

cp .env.example .env
python3 -c "import secrets; print(secrets.token_hex(32))"   # paste into SESSION_SECRET in .env
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"   # paste into CREDENTIALS_ENCRYPTION_KEY
nano .env   # fill in Telegram token/chat id, SESSION_SECRET, CREDENTIALS_ENCRYPTION_KEY; leave
            # LIVE_PORTFOLIO_VALUE_USD at 0 until you've read "Going live" in README.md — at 0
            # the live engine can only ever size a position to zero shares, so live orders are physically
            # impossible until you deliberately set a real number there.

cp deploy/ibc/config-paper.ini.example deploy/ibc/config-paper.ini
nano deploy/ibc/config-paper.ini   # your real IBKR username/password
chmod 600 deploy/ibc/config-paper.ini

cp deploy/ibc/config-live.ini.example deploy/ibc/config-live.ini
nano deploy/ibc/config-live.ini    # verify whether your paper account shares the live login or has
                                    # its own separate one (see config-paper.ini.example) - don't assume; TradingMode=live
chmod 600 deploy/ibc/config-live.ini
```

Only setting up paper for now? Skip the `config-live.ini` step and don't
enable the `-live` services below — everything else works the same with
just the paper half running.

## 6. Install and start the services

```bash
sudo cp /opt/tradingbot/deploy/*.service /etc/systemd/system/
sudo systemctl daemon-reload

sudo systemctl enable --now ibgateway-paper.service
# Watch it come up — the FIRST login needs you to approve the 2FA push on
# your phone. Tail the log and wait for it:
sudo journalctl -u ibgateway-paper.service -f
```

Once paper's Gateway is logged in and stable, bring up live's the same way
(skip this if you're only running paper):

```bash
sudo systemctl enable --now ibgateway-live.service
sudo journalctl -u ibgateway-live.service -f   # separate 2FA approval, same as paper
```

Then the engines and the dashboard:

```bash
sudo systemctl enable --now trading-bot-paper.service
sudo systemctl enable --now trading-bot-live.service   # skip if not running live
sudo systemctl enable --now dashboard.service

sudo journalctl -u trading-bot-paper.service -f   # should show the scheduler starting its jobs
sudo journalctl -u dashboard.service -f           # should show uvicorn listening on 127.0.0.1:8000
```

Optional: the dashboard's "Gateway Connection" control (Disconnect/Reconnect
per mode, so you can log into TWS or IBKR Mobile with the bot's account
without SSH) needs a narrowly-scoped sudo rule for the `tradingbot` user —
it grants start/stop on exactly the 4 service units above, nothing else:

```bash
which systemctl   # confirm this matches the path in the file below; edit it first if not
sudo cp /opt/tradingbot/deploy/sudoers-tradingbot /etc/sudoers.d/tradingbot
sudo chmod 440 /etc/sudoers.d/tradingbot
sudo visudo -c   # validates syntax
```

Skip this if you don't want that control — the dashboard works fine
without it, that one feature just returns an error until installed.

## 7. Wire up Caddy

```bash
sudo cp /opt/tradingbot/deploy/Caddyfile /etc/caddy/Caddyfile
sudo nano /etc/caddy/Caddyfile   # replace your-subdomain.duckdns.org with your real hostname
sudo systemctl restart caddy
```

Caddy fetches its own HTTPS certificate automatically on first request —
give it a minute.

## 8. First login

Open `https://yoursubdomain.duckdns.org/` — it redirects to `/setup` since
no dashboard account exists yet. Create your admin username/password there
(this is the only account; there's no self-registration after that). You're
now looking at the live dashboard: a PAPER/LIVE tab at the top selects
which engine's status, positions, trades, R-histogram, and enable/pause/
flatten controls you're looking at — they're independent (pausing one
doesn't touch the other). Strategies are shared across both tabs.

## Day-to-day operations

```bash
# Logs (repeat with -live for the live half)
sudo journalctl -u trading-bot-paper.service -f
sudo journalctl -u ibgateway-paper.service -f
sudo journalctl -u dashboard.service -f

# Restart after a config or code change
sudo systemctl restart trading-bot-paper.service trading-bot-live.service dashboard.service

# Deploy an update
sudo -iu tradingbot bash -c "cd /opt/tradingbot && git pull && .venv/bin/pip install -r requirements.txt"
sudo systemctl restart trading-bot-paper.service trading-bot-live.service dashboard.service
```

### Custom-universe strategies (e.g. "Long Breakout NASDAQ Beta")

A strategy can restrict itself to a fundamentals-screened universe (market
cap, beta, analyst rating) narrower than the default S&P 500 scan — see
`src/custom_universes.py`. `build_custom_universe.py` builds/refreshes the
ticker list for one of these; `trading-bot-paper.service`'s scheduler
already runs it automatically every Sunday 08:00 ET for every universe
defined there, so this is normally hands-off. The one time it needs a
manual run is right after this feature is first deployed — the cache
starts out empty, and a strategy pinned to an empty/stale universe just
finds no candidates rather than erroring, so it fails silently until the
first scheduled run (up to a week away):

```bash
sudo -iu tradingbot bash -c "cd /opt/tradingbot && .venv/bin/python build_custom_universe.py --universe ixic_large_beta_buy"
```

Takes a while by design — one yfinance fundamentals lookup per NASDAQ-listed
candidate (several thousand tickers), deliberately throttled (2 workers,
paced submissions) so it doesn't look like scraping to Yahoo and get the
session's crumb rejected wholesale. Let it run in the background
(`... &` or a `screen`/`tmux` session) rather than waiting on it; a run
over the full NASDAQ-listed universe can take 30-60+ minutes.

If the JSON result shows `"survivors_count": 0` with a wall of
`Invalid Crumb` / `HTTP Error 401` lines, that's Yahoo temporarily
rate-limiting this server's IP after too many fundamentals requests too
fast (it happened after an early version of this script ran 8 workers with
no pacing) — retrying immediately just repeats it. Wait 30-60 minutes and
run it again once; don't loop-retry.

### Backtest engine

The dashboard's Backtest page (`/backtest`) replays a strategy against
real historical bars — daily bars come from yfinance (unlimited-ish
history, same as the live bot), but the intraday (5-minute) bars it needs
for entry timing and exit management come from IBKR's own historical-data
API instead of yfinance, since yfinance only keeps ~60 days of intraday
history while IBKR gives months per request. `fetch_backtest_data.py`
pulls and locally caches those bars (`data/backtest_bars/`) — once a bar
is fetched it's kept forever, so the usable backtest window only grows
over time. `trading-bot-paper.service`'s scheduler already runs it
automatically every Sunday 09:00 ET for the full S&P 500 universe, but —
same as the custom-universe builder — it needs one manual run right after
this feature is first deployed, or the Backtest page has nothing to test
against for up to a week:

```bash
sudo -iu tradingbot bash -c "cd /opt/tradingbot && .venv/bin/python fetch_backtest_data.py"
```

Unlike the other two background jobs, this one needs a live IB Gateway
connection (its own dedicated client ID, `IBKR_BACKTEST_CLIENT_ID` in
`.env` — add it if upgrading from before this feature existed, see
`.env.example`), so run it while `ibgateway-paper.service` is up.

IBKR takes paper accounts offline for extended weekend maintenance —
observed starting right around Friday's session close (~00:00 ET
Saturday) and not reliably back until Sunday evening or Monday. During
that window every request hangs/fails with `Error 1100: Connectivity
between IBKR and Trader Workstation has been lost` (and the IBC log under
`~/ibc-logs-paper/` shows a "No Internet connection" dialog) no matter how
many times the Gateway is restarted — this is expected and not a bug in
this script or the Gateway config. Don't run (or debug failures of) this
job over the weekend; retry once markets are back.

IBKR silently rejects (or hangs on) a single `reqHistoricalData` request
for 5-minute bars spanning more than a few days, even though its own docs
advertise "months per request" — so a symbol's first-ever backfill is
paged backward in small (`CHUNK_DAYS`, currently 5-day) requests, paced
2 seconds apart, rather than one big ask. That makes an initial full S&P
500 backfill (6 months of history per symbol) genuinely slow — expect it
to run for several hours, not minutes — so always run it in the
background (`nohup ... &` or `screen`/`tmux`) rather than waiting on it,
and check on it later via its per-symbol progress output. Re-running it
later (by hand or via the weekly schedule) is much faster since it's
always incremental — it only fetches the gap since each symbol's last
cached bar (typically 1-2 chunk requests), never re-downloads what's
already cached.

If `ibgateway-paper.service` or `ibgateway-live.service` restarts (nightly
`AutoRestartTime`, a crash, a server reboot) and IBKR forces a fresh 2FA
challenge, the matching `trading-bot-*.service` will simply fail to connect
until you approve it on your phone — that's why Telegram alerts and the
dashboard's "last cycle" timestamp (per tab) matter: a long gap with no
cycle activity on one tab is your signal to go check that mode's
`journalctl -u ibgateway-<mode>.service`. The two config files' staggered
`AutoRestartTime` (11:50pm vs 11:55pm live) means both don't demand 2FA at
the exact same moment.
