# Deploying to a free server (Oracle Cloud Always Free)

This turns the bot from "runs on my laptop via Task Scheduler" into "runs
24/7 on a server, controlled from a web dashboard." Three long-running
processes, managed by systemd:

- **`ibgateway.service`** — IB Gateway itself, headless, kept logged in by IBC
- **`trading-bot.service`** — `run_service.py`, the trading engine (talks to IBKR)
- **`dashboard.service`** — `run_dashboard.py`, the FastAPI web dashboard (only reads/writes the shared DB, never touches IBKR)

All three read/write the same SQLite DB at `data/trading_bot.db`, so the
dashboard sees whatever the engine is doing in near-real-time without any
direct connection between the two processes.

**Do this whole setup on paper trading first.** Nothing here is safer just
because it's "on a server" — the same paper-first, 2-week soak, then-go-live
guidance from the main README still applies.

## 1. Create the server

1. Sign up for [Oracle Cloud](https://www.oracle.com/cloud/free/) (needs a
   card for verification, but the Always Free tier genuinely never charges).
2. Create a compute instance:
   - Shape: **VM.Standard.A1.Flex** (Ampere ARM, Always Free) — 2-4 OCPUs /
     12-24 GB RAM is comfortably enough for IB Gateway + the bot + the
     dashboard. If you hit "Out of host capacity" (a known Oracle free-tier
     issue in busy regions), retry in a different availability domain/region,
     or fall back to the smaller **VM.Standard.E2.1.Micro** (AMD, also
     Always Free, 1 GB RAM — tighter but workable with a swap file).
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
paths differ from `/opt/ibgateway` and `/opt/ibc`.

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
nano .env   # fill in Telegram token/chat id, portfolio sizing, SESSION_SECRET

cp deploy/ibc/config.ini.example deploy/ibc/config.ini
nano deploy/ibc/config.ini   # your real IBKR username/password, TradingMode=paper
chmod 600 deploy/ibc/config.ini
```

Leave `IBKR_PORT=4002` in `.env` if you're running IB Gateway (paper) rather
than TWS — Gateway's paper port differs from TWS's. Update
`IBKR_HOST`/`IBKR_PORT` in `.env` to match whichever you installed.

## 6. Install and start the services

```bash
sudo cp /opt/tradingbot/deploy/*.service /etc/systemd/system/
sudo systemctl daemon-reload

sudo systemctl enable --now ibgateway.service
# Watch it come up — the FIRST login needs you to approve the 2FA push on
# your phone. Tail the log and wait for it:
sudo journalctl -u ibgateway.service -f
```

Once IB Gateway is logged in and stable:

```bash
sudo systemctl enable --now trading-bot.service
sudo systemctl enable --now dashboard.service

sudo journalctl -u trading-bot.service -f    # should show the scheduler starting its jobs
sudo journalctl -u dashboard.service -f      # should show uvicorn listening on 127.0.0.1:8000
```

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
now looking at the live dashboard: status, positions, trades, R-histogram,
strategy switcher, and the enable/pause/flatten controls.

## Day-to-day operations

```bash
# Logs
sudo journalctl -u trading-bot.service -f
sudo journalctl -u dashboard.service -f
sudo journalctl -u ibgateway.service -f

# Restart after a config or code change
sudo systemctl restart trading-bot.service dashboard.service

# Deploy an update
sudo -iu tradingbot bash -c "cd /opt/tradingbot && git pull && .venv/bin/pip install -r requirements.txt"
sudo systemctl restart trading-bot.service dashboard.service
```

If `ibgateway.service` restarts (nightly `AutoRestartTime`, a crash, a
server reboot) and IBKR forces a fresh 2FA challenge, `trading-bot.service`
will simply fail to connect until you approve it on your phone — that's why
Telegram alerts and the dashboard's "last cycle" timestamp matter: a long
gap with no cycle activity is your signal to go check `journalctl -u
ibgateway.service`.
