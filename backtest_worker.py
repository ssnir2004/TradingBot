"""Remote backtest worker - runs on YOUR OWN computer, not the server.
Polls the dashboard over HTTP for a pending backtest created with
execution_mode="remote" (see web/app.py's api_create_backtest and the
"Run on remote worker" checkbox in the New Backtest form), computes it
locally using the exact same backtest_engine/perf logic the server's own
run_backtest.py uses (shared via src/backtest_runner.py, so the two paths
can never quietly drift apart), and reports the result back. Moves the
CPU/memory cost of a backtest off the small always-on server entirely -
that server also runs two IB Gateways and the live/paper trading engines
around the clock, and has very little headroom to spare for a compute-
heavy backtest on top of that.

Needs its own local copy of data/backtest_bars - backtest_engine.py only
ever reads from that local cache (never IBKR directly), so a symbol/date
range this worker doesn't have cached will just come back "no cached
intraday bars", the same as on the server if fetch_backtest_data.py
hadn't been run there. See docs/worker.md for how to sync that cache
from the server (rsync/scp) and for the full setup walkthrough.

Configure via a local .env file (BACKTEST_SERVER_URL, BACKTEST_WORKER_TOKEN
- the token is generated once from the dashboard's Backtest page and
never shown again) or environment variables / CLI flags directly. Never
commit a real token - keep .env out of version control (it already is,
see .gitignore).
"""
import argparse
import os
import sys
import time
import traceback
from pathlib import Path

import requests
from dotenv import load_dotenv

from src import backtest_runner

PROJECT_DIR = Path(__file__).resolve().parent
DEFAULT_POLL_INTERVAL_SECONDS = 10
# Generous but bounded - claiming should be near-instant server-side, but
# a slow/overloaded server (or a flaky connection) shouldn't hang this
# forever with no feedback.
_CLAIM_TIMEOUT_SECONDS = 30
_SUBMIT_TIMEOUT_SECONDS = 60


def _prevent_system_sleep():
    """Windows only: tell the OS not to sleep (which drops networking, and
    with it this worker's poll loop / in-flight submissions) while this
    process is running. Deliberately does NOT pass ES_DISPLAY_REQUIRED, so
    screen lock / screensaver still behave normally - only full system
    sleep is blocked. Works per-process via the Win32 API, so it applies
    even when a stricter power plan (e.g. locked down by IT policy) can't
    be changed through Settings. No-op on other platforms; the setting is
    automatically released when this process exits."""
    if sys.platform != "win32":
        return
    import ctypes
    ES_CONTINUOUS = 0x80000000
    ES_SYSTEM_REQUIRED = 0x00000001
    ctypes.windll.kernel32.SetThreadExecutionState(ES_CONTINUOUS | ES_SYSTEM_REQUIRED)


def _headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def claim_job(server_url: str, token: str) -> dict | None:
    resp = requests.post(f"{server_url}/api/worker/claim", headers=_headers(token), timeout=_CLAIM_TIMEOUT_SECONDS)
    resp.raise_for_status()
    return resp.json()  # None (JSON null) when there's nothing pending right now


def submit_result(server_url: str, token: str, backtest_id: int, results: dict):
    resp = requests.post(
        f"{server_url}/api/worker/backtests/{backtest_id}/result",
        headers=_headers(token), json={"results": results}, timeout=_SUBMIT_TIMEOUT_SECONDS,
    )
    resp.raise_for_status()


def submit_failure(server_url: str, token: str, backtest_id: int, error: str):
    try:
        requests.post(
            f"{server_url}/api/worker/backtests/{backtest_id}/fail",
            headers=_headers(token), json={"error": error}, timeout=_CLAIM_TIMEOUT_SECONDS,
        )
    except requests.RequestException:
        # Best-effort - if even reporting the failure doesn't reach the
        # server, the row stays 'running' until the server's own
        # requeue_abandoned_worker_backtests times it out and marks it
        # failed on its own, same end state either way.
        pass


def run_once(server_url: str, token: str) -> bool:
    """Returns whether a job was actually claimed and processed, so the
    caller's poll loop can skip its sleep and immediately check for more
    work instead of idling right after finishing one."""
    try:
        job = claim_job(server_url, token)
    except requests.RequestException as exc:
        print(f"[worker] could not reach server: {exc}")
        return False
    if job is None:
        return False

    backtest_id = job["id"]
    print(f"[worker] claimed backtest {backtest_id} "
          f"({job['params']['start_date']} -> {job['params']['end_date']}, "
          f"{len(job['params']['symbols'])} symbol(s), {len(job['strategies'])} strateg(y/ies))")
    try:
        results = backtest_runner.run_backtest_params(job["params"], job["strategies"])
        submit_result(server_url, token, backtest_id, results)
        print(f"[worker] backtest {backtest_id}: done ({len(results)} strategy result(s))")
    except Exception as exc:  # noqa: BLE001 - a bad run must report failure to the server, not crash this worker's loop
        error = f"{type(exc).__name__}: {exc}"
        print(f"[worker] backtest {backtest_id}: failed - {error}")
        traceback.print_exc()
        submit_failure(server_url, token, backtest_id, error)
    return True


def main():
    load_dotenv(PROJECT_DIR / ".env")
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--server-url", default=os.environ.get("BACKTEST_SERVER_URL"),
                         help="e.g. https://your-dashboard-domain (no trailing slash needed)")
    parser.add_argument("--token", default=os.environ.get("BACKTEST_WORKER_TOKEN"),
                         help="Generated once from the dashboard's Backtest page")
    parser.add_argument("--poll-interval", type=float, default=DEFAULT_POLL_INTERVAL_SECONDS,
                         help=f"Seconds between claim attempts when idle (default {DEFAULT_POLL_INTERVAL_SECONDS})")
    args = parser.parse_args()

    if not args.server_url or not args.token:
        raise SystemExit(
            "Missing server URL or token - set BACKTEST_SERVER_URL and BACKTEST_WORKER_TOKEN "
            "in a local .env file (see docs/worker.md), or pass --server-url/--token directly."
        )
    server_url = args.server_url.rstrip("/")

    _prevent_system_sleep()
    print(f"[worker] polling {server_url} every {args.poll_interval}s (Ctrl+C to stop)")
    while True:
        did_work = run_once(server_url, args.token)
        if not did_work:
            time.sleep(args.poll_interval)


if __name__ == "__main__":
    main()
