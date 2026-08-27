# Remote backtest worker

Runs a backtest's actual computation on your own computer instead of the
server — the server just queues the request and stores the result. Useful
because the server is a small always-on box already running two IB
Gateways and both trading engines around the clock, with very little
memory headroom left over for a CPU/memory-heavy backtest on top of that.

Nothing changes about how you use the dashboard — you still create and
view backtests from the Backtest page. The only difference is a checkbox:
**"Run on remote worker"** on the New Backtest form. Leave it unchecked
and everything behaves exactly as before (the server runs it locally).
Check it, and the backtest sits at "pending" until a worker (a script
running on your machine) picks it up.

## 1. Set up your local machine

You need a local clone of this repo, on the same branch as the server, with
its own Python environment:

```bash
git clone <this repo's URL>
cd TradingBot
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

Keep it updated the same way you'd deploy to the server — `git pull` before
running the worker, so it's never computing against stale filter/exit logic.

## 2. Sync the historical bar cache

`backtest_engine.py` only ever reads from the local `data/backtest_bars/`
cache — it never talks to IBKR directly. Your machine needs its own copy
of whatever the server has cached (built there via `fetch_backtest_data.py`).
For now this is a manual step — copy it over with `rsync` or `scp`:

```bash
rsync -avz your-server:/opt/tradingbot/data/backtest_bars/ ./data/backtest_bars/
```

Re-run this after fetching new symbols/date ranges on the server. A
backtest that requests a symbol or date range your local cache doesn't
have will just come back with that symbol skipped ("no cached intraday
bars") — same as it would on the server without the fetch.

That rsync only covers *intraday* bars. Daily bars (SMA200/50, D1-D3) are
fetched from yfinance directly by `backtest_engine.py` itself and cached
separately, the first time each symbol is needed — on a cold cache, a
few hundred symbols can take well past this dashboard's 15-minute
"abandoned by worker" timeout to fetch, even with the built-in per-symbol
timeout and worker pool, which can make a perfectly healthy worker look
stuck on its first few jobs. Run this once (no job-timeout pressure
attached) before relying on the worker for real:

```bash
python3 warm_daily_bars.py --universe ixic_large_beta_buy
```

(needs `data/universes/ixic_large_beta_buy.json` on this machine too —
copy it from the server the same way as the bars cache above, or pass
`--symbols-file` with a plain ticker list instead.) Already-cached
symbols are skipped fast, so it's safe to re-run occasionally to pick up
new additions to the universe.

## 3. Generate a worker token

On the dashboard's Backtest page, under the **Remote Worker** card, type a
label (e.g. "home laptop") and click **Generate token**. The raw token is
shown exactly once, right there — copy it immediately. The server only
ever stores its hash, so if you lose it, generate a new one; there's no
way to retrieve the old value again.

## 4. Configure and run the worker

Create a `.env` file in your local clone (same directory as `backtest_worker.py`):

```
BACKTEST_SERVER_URL=https://your-dashboard-domain
BACKTEST_WORKER_TOKEN=<the token from step 3>
```

Then run it:

```bash
python3 backtest_worker.py
```

It polls the server every 10 seconds (configurable via `--poll-interval`)
for a pending remote backtest, runs it locally using the exact same
`backtest_engine.py`/`perf.py` logic the server's own `run_backtest.py`
uses (shared via `src/backtest_runner.py`, so results are identical
either way — this is not an approximation), and reports the result back.
Leave it running in a terminal (or under something like `tmux`/`screen`)
for as long as you want it available to claim work.

You'll see output like:

```
[worker] polling https://your-dashboard-domain every 10.0s (Ctrl+C to stop)
[worker] claimed backtest 42 (2026-08-01 -> 2026-08-05, 494 symbol(s), 2 strateg(y/ies))
[worker] backtest 42: done (2 strategy result(s))
```

## Notes and limitations (v1)

- **Bar cache sync is manual.** There's no automatic "fetch what's
  missing" endpoint yet — that's a natural future enhancement, but for now
  it's on you to `rsync` after adding new cached data on the server.
- **A worker must be running when you check the box.** A remote backtest
  just waits at "pending" — it isn't retried or escalated to local
  execution automatically. If no worker ever claims it, it sits there
  until you cancel it.
- **Cancelling a remote backtest from the dashboard doesn't reach across
  the network to stop your worker mid-computation** — it immediately
  marks the row failed on the server's side (so you can start a fresh one
  right away), but the worker keeps computing locally and its eventual
  result submission is simply rejected (the row's no longer "running") and
  discarded. Harmless, just wasted local compute time.
- **An abandoned claim (worker crashes, loses network, or you close the
  laptop mid-run) is automatically detected and marked failed** after 15
  minutes with no result — the dashboard checks for this once a minute in
  the background. Just re-run it once your worker's back.
- **Stopping the worker (Ctrl+C) to `git pull` an update triggers this same
  "abandoned claim" failure if it currently has a job claimed** — the
  backtest it was working on gets marked failed after 15 minutes, same as a
  crash. Check the worker's terminal output before stopping it: if the last
  line is `[worker] claimed backtest N ...` with no matching "done"/result
  line yet, it's still mid-job. Either wait for it to finish that job first,
  or accept that you'll need to re-run (via the dashboard's Retry button)
  the backtest it was in the middle of.
- **One token per worker machine is the simplest setup**, but nothing
  stops you from running the same token on two machines - they'll both
  poll and race for whichever job is claimed first, which is a harmless
  (if slightly wasteful) way to add more capacity.
