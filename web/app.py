"""Dashboard web app: login, bot start/stop/emergency-flatten controls, live
positions/trades/performance views, and the multi-strategy switcher — for
BOTH the paper and live engines at once, selected per-request via a `mode`
query param (the frontend has a Paper/Live tab). Reads and writes the same
SQLite DB the two trading services (run_service.py --mode paper/live) use;
it never talks to IBKR directly, so it can safely run as a separate process.
"""
import asyncio
import json
import subprocess
import sys
from datetime import date
from pathlib import Path

from dotenv import dotenv_values
from fastapi import Depends, FastAPI, Form, Header, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

import cycle
import morning_prefilter
from src import backtest_data, backtest_engine, db, gateway_provisioning, mode_config, perf, secrets_store
from src.sp500_tickers import SP500_TICKERS
from web import gateway_control
from web.auth import COOKIE_NAME, make_session_cookie, read_session, require_user

PROJECT_DIR = Path(__file__).resolve().parent.parent
templates = Jinja2Templates(directory=str(Path(__file__).resolve().parent / "templates"))

app = FastAPI(title="TradingBot Dashboard")
app.mount("/static", StaticFiles(directory=str(Path(__file__).resolve().parent / "static")), name="static")

# Typed into the confirmation modal before any LIVE risk-sizing value can be
# changed from the dashboard — a deliberate speed bump since these numbers
# directly control how much real money a single live order can risk.
LIVE_RISK_CONFIRM_PHRASE = "ok"

# Typed into the confirmation modal before disconnecting LIVE's Gateway —
# while disconnected, nothing manages open positions or force-closes at
# end of day, so this is a deliberate, confirmed action too.
DISCONNECT_LIVE_CONFIRM_PHRASE = "ok"

# Typed into the confirmation modal before closing a LIVE position from the
# Account Holdings panel — this fires a real market order against a real
# account holding, independent of anything the bot itself is tracking.
CLOSE_LIVE_CONFIRM_PHRASE = "ok"

SUBPROCESS_TIMEOUT = 40  # margin above ibkr_client.SETTLED_STATUSES_TIMEOUT (20s) plus connect/qualify/disconnect overhead


def _env() -> dict:
    return dotenv_values(PROJECT_DIR / ".env")


async def _requeue_abandoned_worker_backtests_loop():
    """A remote worker can go quiet at ANY time (crash, closed laptop,
    lost network) - unlike a local subprocess's dead pid, this process has
    no way to directly check whether a worker that claimed a job is still
    alive, so it just checks periodically whether too much time has
    passed since the claim. Runs for the lifetime of the dashboard
    process as a background asyncio task (not just once at startup, since
    a worker can vanish hours into an already-running session) - errors
    are swallowed and retried next tick rather than letting one bad pass
    kill the loop for good."""
    while True:
        try:
            db.requeue_abandoned_worker_backtests()
        except Exception:  # noqa: BLE001 - a bad pass must not silently end this background loop
            pass
        await asyncio.sleep(60)


@app.on_event("startup")
async def on_startup():
    db.init_db(seed_rules_path=PROJECT_DIR / "rules.json")
    # A backtest subprocess spawned by the PREVIOUS dashboard process (see
    # api_create_backtest) doesn't survive a `systemctl restart
    # dashboard.service` - systemd's default KillMode=control-group kills
    # every process in the service's cgroup on stop, this subprocess
    # included, without ever reaching its own except handler. Left alone
    # that leaves the backtest's row stuck at 'running' forever; this
    # reconciles that on every startup instead.
    db.fail_orphaned_backtests()
    db.fail_orphaned_backtest_data_fetches()  # same reconciliation, same reason - see its own docstring
    asyncio.create_task(_requeue_abandoned_worker_backtests_loop())


def require_mode(mode: str = Query(...)) -> str:
    if mode not in db.MODES:
        raise HTTPException(status_code=400, detail=f"mode must be one of {db.MODES}")
    return mode


def require_account(user: str = Depends(require_user)) -> int:
    """Resolves the logged-in session's account_id fresh on every request
    (rather than baking it into the cookie) — every account-scoped read/
    write in this file goes through this, so one account's data never
    leaks into another's."""
    account = db.get_user_by_username(user)
    if account is None:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return account["id"]


def require_admin(user: str = Depends(require_user)) -> str:
    """Strategy *templates* are curated by one admin; every account can
    only choose which template to activate (api_activate_strategy), not
    create/edit/delete the shared catalog."""
    account = db.get_user_by_username(user)
    if account is None or not account.get("is_admin"):
        raise HTTPException(status_code=403, detail="Admin only")
    return user


def require_full_access(user: str = Depends(require_user)) -> str:
    """A role='viewer' account (see the Users page, admin-only) gets
    read-only access to the Backtest page's own data and nothing else -
    every route anywhere else in this file that reads or changes anything
    (trading controls, positions/orders, Gateway/IBKR credentials, risk
    params, worker tokens, running/deleting/cancelling a backtest,
    activating a strategy, updating backtest data, ...) depends on this
    instead of plain require_user. Deliberately separate from is_admin -
    that axis is "can manage the shared strategy-template catalog",
    orthogonal to "can this account touch anything beyond a read-only
    backtest view.\""""
    account = db.get_user_by_username(user)
    if account is None:
        raise HTTPException(status_code=401, detail="Not authenticated")
    if account.get("role") == "viewer":
        raise HTTPException(status_code=403, detail="Your account has read-only backtest access")
    return user


def require_worker_token(authorization: str | None = Header(default=None)) -> int:
    """A remote backtest worker (see docs/worker.md) isn't a browser with a
    session cookie - it authenticates every request with a long-lived
    bearer token instead (Authorization: Bearer <token>), created via
    POST /api/worker_tokens by a real logged-in user and never shown
    again after that. Returns the owning account_id, resolved fresh on
    every call exactly like require_account does for cookie sessions -
    every worker-facing endpoint below is scoped through this, so one
    account's worker can never see or touch another account's backtests."""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or malformed Authorization header")
    token = authorization.removeprefix("Bearer ").strip()
    account_id = db.verify_worker_token(token)
    if account_id is None:
        raise HTTPException(status_code=401, detail="Invalid worker token")
    return account_id


# ------------------------------------------------------------------ pages ---
@app.get("/setup", response_class=HTMLResponse)
def setup_page(request: Request):
    if db.any_users_exist():
        return RedirectResponse("/login", status_code=303)
    return templates.TemplateResponse(request, "setup.html", {"error": None})


@app.post("/setup")
def setup_submit(request: Request, username: str = Form(...), password: str = Form(...)):
    if db.any_users_exist():
        return RedirectResponse("/login", status_code=303)
    if len(password) < 8:
        return templates.TemplateResponse(
            request, "setup.html", {"error": "Password must be at least 8 characters."}
        )
    db.create_user(username, password, is_admin=True)
    response = RedirectResponse("/", status_code=303)
    response.set_cookie(COOKIE_NAME, make_session_cookie(username), httponly=True, samesite="lax")
    return response


@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    if not db.any_users_exist():
        return RedirectResponse("/setup", status_code=303)
    return templates.TemplateResponse(request, "login.html", {"error": None})


@app.post("/login")
def login_submit(request: Request, username: str = Form(...), password: str = Form(...)):
    if not db.verify_user(username, password):
        return templates.TemplateResponse(
            request, "login.html", {"error": "Invalid username or password."}, status_code=401
        )
    response = RedirectResponse("/", status_code=303)
    response.set_cookie(COOKIE_NAME, make_session_cookie(username), httponly=True, samesite="lax")
    return response


@app.post("/logout")
def logout():
    response = RedirectResponse("/login", status_code=303)
    response.delete_cookie(COOKIE_NAME)
    return response


def _is_viewer(username: str) -> bool:
    account = db.get_user_by_username(username)
    return bool(account) and account.get("role") == "viewer"


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    if not db.any_users_exist():
        return RedirectResponse("/setup", status_code=303)
    username = read_session(request)
    if not username:
        return RedirectResponse("/login", status_code=303)
    # A viewer has no access to /bot at all (see bot_page/trading_page below) -
    # land them straight on the one page they can actually use.
    return RedirectResponse("/backtest" if _is_viewer(username) else "/bot", status_code=303)


@app.get("/bot", response_class=HTMLResponse)
def bot_page(request: Request):
    if not db.any_users_exist():
        return RedirectResponse("/setup", status_code=303)
    username = read_session(request)
    if not username:
        return RedirectResponse("/login", status_code=303)
    account = db.get_user_by_username(username)
    if account and account.get("role") == "viewer":
        return RedirectResponse("/backtest", status_code=303)
    return templates.TemplateResponse(request, "bot.html", {
        "active_page": "bot", "is_admin": bool(account and account.get("is_admin")),
        "default_commission_per_trade": DEFAULT_BACKTEST_COMMISSION_PER_TRADE,
    })


@app.get("/trading", response_class=HTMLResponse)
def trading_page(request: Request):
    if not db.any_users_exist():
        return RedirectResponse("/setup", status_code=303)
    username = read_session(request)
    if not username:
        return RedirectResponse("/login", status_code=303)
    account = db.get_user_by_username(username)
    if account and account.get("role") == "viewer":
        return RedirectResponse("/backtest", status_code=303)
    return templates.TemplateResponse(request, "trading.html", {"active_page": "trading", "is_admin": bool(account and account.get("is_admin"))})


@app.get("/backtest", response_class=HTMLResponse)
def backtest_page(request: Request):
    if not db.any_users_exist():
        return RedirectResponse("/setup", status_code=303)
    username = read_session(request)
    if not username:
        return RedirectResponse("/login", status_code=303)
    account = db.get_user_by_username(username)
    return templates.TemplateResponse(request, "backtest.html", {
        "active_page": "backtest",
        "is_admin": bool(account and account.get("is_admin")),
        "is_viewer": bool(account and account.get("role") == "viewer"),
    })


@app.get("/guide", response_class=HTMLResponse)
def guide(request: Request):
    if not read_session(request):
        return RedirectResponse("/login", status_code=303)
    return templates.TemplateResponse(request, "guide.html", {})


@app.get("/users", response_class=HTMLResponse)
def users_page(request: Request):
    """Admin-only - add/remove dashboard accounts, in particular
    role='viewer' ones (read-only Backtest-page access). Gated by
    require_admin the same as the strategy-template catalog, not
    require_full_access - a role check has nothing to do with who may
    manage OTHER accounts' roles."""
    if not db.any_users_exist():
        return RedirectResponse("/setup", status_code=303)
    username = read_session(request)
    if not username:
        return RedirectResponse("/login", status_code=303)
    account = db.get_user_by_username(username)
    if not account or not account.get("is_admin"):
        return RedirectResponse("/bot", status_code=303)
    return templates.TemplateResponse(request, "users.html", {"active_page": "users", "is_admin": True})


# -------------------------------------------------------------------- API ---
@app.get("/api/me")
def api_me(account_id: int = Depends(require_account), user: str = Depends(require_user)):
    account = db.get_user_by_username(user)
    return {"username": user, "is_admin": bool(account["is_admin"]), "role": account["role"]}


@app.get("/api/status")
def api_status(mode: str = Depends(require_mode), account_id: int = Depends(require_account), user: str = Depends(require_full_access)):
    return db.get_cycle_status(account_id, mode)


@app.post("/api/control/enable")
def api_enable(mode: str = Depends(require_mode), account_id: int = Depends(require_account), user: str = Depends(require_full_access)):
    db.set_bot_enabled(account_id, mode, True)
    db.log_decision(account_id, mode, "dashboard_control", action="enable", user=user)
    return {"bot_enabled": True}


@app.post("/api/control/disable")
def api_disable(mode: str = Depends(require_mode), account_id: int = Depends(require_account), user: str = Depends(require_full_access)):
    db.set_bot_enabled(account_id, mode, False)
    db.log_decision(account_id, mode, "dashboard_control", action="disable", user=user)
    return {"bot_enabled": False}


@app.post("/api/control/flatten")
def api_flatten(mode: str = Depends(require_mode), account_id: int = Depends(require_account), user: str = Depends(require_full_access)):
    db.request_flatten_now(account_id, mode)
    db.log_decision(account_id, mode, "dashboard_control", action="flatten_now", user=user)
    return {"flatten_pending": True}


@app.get("/api/account")
def api_account(mode: str = Depends(require_mode), account_id: int = Depends(require_account), user: str = Depends(require_full_access)):
    return db.get_account_info(account_id, mode)


@app.get("/api/risk_params")
def api_get_risk_params(mode: str = Depends(require_mode), account_id: int = Depends(require_account), user: str = Depends(require_full_access)):
    return mode_config.risk_params(_env(), account_id, mode)


@app.post("/api/risk_params")
async def api_set_risk_params(request: Request, mode: str = Depends(require_mode), account_id: int = Depends(require_account), user: str = Depends(require_full_access)):
    body = await request.json()

    if mode == "live" and body.get("confirm") != LIVE_RISK_CONFIRM_PHRASE:
        raise HTTPException(
            status_code=400,
            detail=f"Type '{LIVE_RISK_CONFIRM_PHRASE}' to confirm changing LIVE risk settings.",
        )

    updates: dict[str, float | int] = {}
    for key, (_, cast, _default) in mode_config.RISK_PARAM_SPECS.items():
        if key not in body or body[key] is None or body[key] == "":
            continue
        try:
            value = cast(body[key])
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail=f"Invalid value for {key}")
        if value < 0:
            raise HTTPException(status_code=400, detail=f"{key} must be >= 0")
        updates[key] = value

    if not updates:
        raise HTTPException(status_code=400, detail="No values provided")

    for key, value in updates.items():
        db.set_setting(f"{account_id}:{mode}:risk:{key}", str(value))
    db.log_decision(account_id, mode, "dashboard_control", user=user, action="update_risk_params",
                     **{k: str(v) for k, v in updates.items()})
    return mode_config.risk_params(_env(), account_id, mode)


# ------------------------------------------------------- IBKR credentials ---
# Self-service — every account enters and manages its OWN IBKR login here,
# never through the admin. The password is Fernet-encrypted before it ever
# touches the DB (see src/secrets_store.py) and this API never echoes it
# back, only whether credentials are on file and for which username.
@app.get("/api/ibkr_credentials")
def api_get_ibkr_credentials(account_id: int = Depends(require_account), user: str = Depends(require_full_access)):
    creds = db.get_ibkr_credentials(account_id)
    if not creds:
        return {"configured": False, "ibkr_username": None, "updated_at": None}
    return {"configured": True, "ibkr_username": creds["ibkr_username"], "updated_at": creds["updated_at"]}


@app.post("/api/ibkr_credentials")
async def api_set_ibkr_credentials(request: Request, account_id: int = Depends(require_account), user: str = Depends(require_full_access)):
    body = await request.json()
    ibkr_username = (body.get("ibkr_username") or "").strip()
    ibkr_password = body.get("ibkr_password") or ""
    if not ibkr_username or not ibkr_password:
        raise HTTPException(status_code=400, detail="ibkr_username and ibkr_password are required")

    try:
        encrypted = secrets_store.encrypt(ibkr_password)
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    db.set_ibkr_credentials(account_id, ibkr_username, encrypted)
    _log_account_action(account_id, user, action="set_ibkr_credentials", ibkr_username=ibkr_username)
    return {"ok": True}


# --------------------------------------------------- my gateway (non-admin) ---
# Self-service Gateway control for every account EXCEPT the admin, who
# already has a working Gateway on the fixed units below (Gateway
# control section) — gateway_provisioning guards against the admin
# accidentally starting a second, conflicting session on their own login.
def _provisioning_error_response(exc: Exception) -> HTTPException:
    if isinstance(exc, gateway_provisioning.AdminNotAllowedError):
        return HTTPException(status_code=403, detail=str(exc))
    if isinstance(exc, gateway_provisioning.CredentialsNotSetError):
        return HTTPException(status_code=400, detail=str(exc))
    return HTTPException(status_code=500, detail=str(exc))


@app.get("/api/my_gateway/status")
def api_my_gateway_status(account_id: int = Depends(require_account), user: str = Depends(require_full_access)):
    try:
        return gateway_provisioning.status(account_id)
    except Exception as exc:
        raise _provisioning_error_response(exc)


@app.post("/api/my_gateway/connect")
def api_my_gateway_connect(account_id: int = Depends(require_account), user: str = Depends(require_full_access)):
    try:
        gateway_provisioning.provision_and_connect(account_id)
    except Exception as exc:
        raise _provisioning_error_response(exc)
    _log_account_action(account_id, user, action="my_gateway_connect")
    return {"ok": True}


@app.post("/api/my_gateway/resume")
def api_my_gateway_resume(account_id: int = Depends(require_account), user: str = Depends(require_full_access)):
    try:
        gateway_provisioning.resume_engines(account_id)
    except Exception as exc:
        raise _provisioning_error_response(exc)
    _log_account_action(account_id, user, action="my_gateway_resume")
    return {"ok": True}


@app.post("/api/my_gateway/disconnect")
def api_my_gateway_disconnect(account_id: int = Depends(require_account), user: str = Depends(require_full_access)):
    try:
        gateway_provisioning.disconnect(account_id)
    except Exception as exc:
        raise _provisioning_error_response(exc)
    _log_account_action(account_id, user, action="my_gateway_disconnect")
    return {"ok": True}


# --------------------------------------------------------- gateway control ---
# Lets you free up an IBKR session for a manual TWS/IBKR Mobile login,
# without SSH — see web/gateway_control.py for the systemd/sudo mechanics.
@app.get("/api/gateway/status")
def api_gateway_status(mode: str = Depends(require_mode), user: str = Depends(require_full_access)):
    return gateway_control.status(mode, _env())


@app.post("/api/gateway/disconnect")
async def api_gateway_disconnect(request: Request, mode: str = Depends(require_mode), account_id: int = Depends(require_account), user: str = Depends(require_full_access)):
    positions = db.get_open_positions(account_id, mode)
    if positions:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Cannot disconnect: {len(positions)} open {mode} position(s) would be left "
                "completely unmanaged (no stop monitoring, no end-of-day close) while the "
                "Gateway is down. Flatten them first."
            ),
        )
    if mode == "live":
        body = await request.json()
        if body.get("confirm") != DISCONNECT_LIVE_CONFIRM_PHRASE:
            raise HTTPException(
                status_code=400,
                detail=f"Type '{DISCONNECT_LIVE_CONFIRM_PHRASE}' to confirm disconnecting LIVE.",
            )
    try:
        gateway_control.disconnect(mode)
    except gateway_control.GatewayControlError as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    db.log_decision(account_id, mode, "dashboard_control", user=user, action="gateway_disconnect")
    return {"ok": True}


@app.post("/api/gateway/reconnect")
def api_gateway_reconnect(mode: str = Depends(require_mode), account_id: int = Depends(require_account), user: str = Depends(require_full_access)):
    try:
        gateway_control.reconnect_gateway(mode)
    except gateway_control.GatewayControlError as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    db.log_decision(account_id, mode, "dashboard_control", user=user, action="gateway_reconnect")
    return {"ok": True}


@app.post("/api/gateway/resume_engine")
def api_gateway_resume_engine(mode: str = Depends(require_mode), account_id: int = Depends(require_account), user: str = Depends(require_full_access)):
    try:
        gateway_control.resume_engine(mode)
    except gateway_control.GatewayControlError as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    db.log_decision(account_id, mode, "dashboard_control", user=user, action="gateway_resume_engine")
    return {"ok": True}


# Placing a real LIVE order to open a brand-new position is the biggest
# single action available from this screen, so it gets the same speed
# bump as every other LIVE-affecting control.
OPEN_POSITION_LIVE_CONFIRM_PHRASE = "ok"


@app.post("/api/positions/open")
async def api_open_position(request: Request, mode: str = Depends(require_mode), account_id: int = Depends(require_account), user: str = Depends(require_full_access)):
    """Manual LIMIT entry with an ATR-based native IBKR trailing stop
    attached as a bracket child - see open_position.py. This is a plain
    broker action: the resulting position is never written to the bot's
    own positions/trades table, isn't subject to force_close_et, and isn't
    managed by cycle.py at all going forward - both orders live entirely
    in TWS/IBKR from here, same as if placed there by hand."""
    body = await request.json()
    symbol = str(body.get("symbol", "")).strip().upper()
    side = body.get("side")
    if not symbol or side not in ("BUY", "SELL"):
        raise HTTPException(status_code=400, detail="symbol and side ('BUY' or 'SELL') are required")
    try:
        qty = int(body.get("qty"))
        limit_price = float(body.get("limit_price"))
        atr_multiplier = float(body.get("atr_multiplier"))
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="qty, limit_price, and atr_multiplier must be numbers")
    if qty <= 0 or limit_price <= 0 or atr_multiplier <= 0:
        raise HTTPException(status_code=400, detail="qty, limit_price, and atr_multiplier must be positive")
    atr_period = int(body.get("atr_period", 14))

    if mode == "live" and body.get("confirm") != OPEN_POSITION_LIVE_CONFIRM_PHRASE:
        raise HTTPException(
            status_code=400,
            detail=f"Type '{OPEN_POSITION_LIVE_CONFIRM_PHRASE}' to confirm placing a real LIVE order.",
        )

    proc = subprocess.run(
        [sys.executable, str(PROJECT_DIR / "open_position.py"), "--mode", mode,
         "--account-id", str(account_id), "--symbol", symbol, "--side", side, "--qty", str(qty),
         "--limit-price", str(limit_price), "--atr-period", str(atr_period), "--atr-multiplier", str(atr_multiplier)],
        capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT,
    )
    db.log_decision(account_id, mode, "dashboard_control", user=user, action="open_position",
                     symbol=symbol, side=side, qty=qty, limit_price=limit_price, atr_multiplier=atr_multiplier,
                     stdout=proc.stdout, returncode=proc.returncode)
    if proc.returncode != 0:
        raise HTTPException(status_code=400, detail=proc.stdout.strip().splitlines()[-1] if proc.stdout.strip() else "Order failed")
    return {"ok": True, "stdout": proc.stdout}


@app.get("/api/positions")
def api_positions(mode: str = Depends(require_mode), account_id: int = Depends(require_account), user: str = Depends(require_full_access)):
    positions = db.get_open_positions(account_id, mode)
    for pos in positions:
        price = None
        try:
            price = cycle._current_price(pos["symbol"])
        except Exception:
            pass
        side = pos.get("side", "long")
        if side == "short":
            risk_per_share = pos["initial_stop"] - pos["entry_price"]
            move = (pos["entry_price"] - price) if price is not None else None
        else:
            risk_per_share = pos["entry_price"] - pos["initial_stop"]
            move = (price - pos["entry_price"]) if price is not None else None
        pos["current_price"] = price
        pos["unrealized_r"] = (move / risk_per_share) if move is not None and risk_per_share > 0 else None
    return positions


# Moving a LIVE stop cancels the order actually protecting the position and
# replaces it, so it gets the same speed bump as other LIVE-affecting
# actions - there's a real (if brief) window where the position is unprotected.
MODIFY_STOP_LIVE_CONFIRM_PHRASE = "ok"


@app.put("/api/positions/{symbol}/stop")
async def api_modify_stop(symbol: str, request: Request, mode: str = Depends(require_mode), account_id: int = Depends(require_account), user: str = Depends(require_full_access)):
    body = await request.json()
    try:
        stop_price = float(body.get("stop_price"))
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="stop_price must be a number")
    if stop_price <= 0:
        raise HTTPException(status_code=400, detail="stop_price must be positive")

    if mode == "live" and body.get("confirm") != MODIFY_STOP_LIVE_CONFIRM_PHRASE:
        raise HTTPException(
            status_code=400,
            detail=f"Type '{MODIFY_STOP_LIVE_CONFIRM_PHRASE}' to confirm moving a LIVE stop order.",
        )

    symbol = symbol.strip().upper()
    if not any(p["symbol"] == symbol for p in db.get_open_positions(account_id, mode)):
        raise HTTPException(status_code=404, detail="No open position for that symbol")

    proc = subprocess.run(
        [sys.executable, str(PROJECT_DIR / "modify_stop.py"), "--mode", mode,
         "--account-id", str(account_id), "--symbol", symbol, "--stop-price", str(stop_price)],
        capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT,
    )
    db.log_decision(account_id, mode, "dashboard_control", user=user, action="modify_stop",
                     symbol=symbol, stop_price=stop_price, stdout=proc.stdout, returncode=proc.returncode)
    if proc.returncode != 0:
        raise HTTPException(status_code=400, detail=proc.stdout.strip().splitlines()[-1] if proc.stdout.strip() else "Modify failed")
    return {"ok": True, "stdout": proc.stdout}


@app.get("/api/candles")
def api_candles(symbol: str, interval: str = "5m", mode: str = Depends(require_mode), account_id: int = Depends(require_account), user: str = Depends(require_full_access)):
    """Candles (plus volume, RSI, the SMA50/SMA200 reference levels D2
    actually checks against, and full MA20/MA200 moving-average lines)
    for the dashboard's chart modal, at the requested interval (one of
    cycle.CHART_INTERVAL_PERIODS) - pure yfinance, no IBKR connection
    needed (mode/account_id are only here so the endpoint follows the
    same auth/scoping shape as everything else)."""
    if interval not in cycle.CHART_INTERVAL_PERIODS:
        raise HTTPException(status_code=400, detail=f"interval must be one of {sorted(cycle.CHART_INTERVAL_PERIODS)}")
    symbol = symbol.strip().upper()
    bars = cycle.get_chart_bars(symbol, interval)
    if bars is None:
        return {
            "candles": [], "volume": [], "rsi": [], "sma50": None, "sma200": None,
            "sma20_series": [], "sma200_series": [],
        }
    candles = [
        {
            "time": int(ts.timestamp()),
            "open": round(float(row.Open), 4),
            "high": round(float(row.High), 4),
            "low": round(float(row.Low), 4),
            "close": round(float(row.Close), 4),
        }
        for ts, row in bars.iterrows()
    ]
    ma = cycle.get_chart_ma_series(symbol, bars, interval)
    return {
        "candles": candles,
        "volume": cycle.get_chart_volume(bars),
        "rsi": cycle.get_chart_rsi(bars),
        "sma50": cycle.get_sma(symbol, 50),
        "sma200": cycle.get_sma(symbol, 200),
        "sma20_series": ma["sma20_series"],
        "sma200_series": ma["sma200_series"],
    }


# Opting a LIVE position out of today's automatic EOD close is real overnight
# gap risk taken on deliberately, so it gets the same speed bump as other
# LIVE-affecting actions; turning it back off just restores the normal safe
# behavior, so that direction needs no confirmation.
HOLD_OVERNIGHT_LIVE_CONFIRM_PHRASE = "ok"


@app.put("/api/positions/{symbol}/hold_overnight")
async def api_set_hold_overnight(symbol: str, request: Request, mode: str = Depends(require_mode), account_id: int = Depends(require_account), user: str = Depends(require_full_access)):
    body = await request.json()
    hold = bool(body.get("hold_overnight"))
    if mode == "live" and hold and body.get("confirm") != HOLD_OVERNIGHT_LIVE_CONFIRM_PHRASE:
        raise HTTPException(
            status_code=400,
            detail=f"Type '{HOLD_OVERNIGHT_LIVE_CONFIRM_PHRASE}' to confirm holding a LIVE position past today's close.",
        )

    symbol = symbol.strip().upper()
    if not any(p["symbol"] == symbol for p in db.get_open_positions(account_id, mode)):
        raise HTTPException(status_code=404, detail="No open position for that symbol")

    db.set_hold_overnight(account_id, mode, symbol, hold)
    db.log_decision(account_id, mode, "dashboard_control", user=user, action="hold_overnight", symbol=symbol, value=hold)
    return {"ok": True}


@app.post("/api/account/refresh")
def api_refresh_account(mode: str = Depends(require_mode), account_id: int = Depends(require_account), user: str = Depends(require_full_access)):
    """On-demand account/positions/orders sync - the same thing
    run_service.py's scheduler already does every 5 minutes for this mode,
    triggered right now instead of waiting for the next tick. Spawned as a
    subprocess (like every other IBKR-touching dashboard action) rather
    than calling cycle.refresh_account_info in-process - the dashboard
    process itself never talks to IBKR directly."""
    proc = subprocess.run(
        [sys.executable, str(PROJECT_DIR / "refresh_account.py"), "--mode", mode, "--account-id", str(account_id)],
        capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT,
    )
    db.log_decision(account_id, mode, "dashboard_control", user=user, action="refresh_account_now",
                     stdout=proc.stdout, returncode=proc.returncode)
    if proc.returncode != 0:
        raise HTTPException(
            status_code=400,
            detail=proc.stdout.strip().splitlines()[-1] if proc.stdout.strip() else "Refresh failed",
        )
    return {"ok": True}


@app.get("/api/broker_positions")
def api_broker_positions(mode: str = Depends(require_mode), account_id: int = Depends(require_account), user: str = Depends(require_full_access)):
    """Every real IBKR holding in this mode's account, independent of
    whether the bot opened it or is tracking it — refreshed every 5 minutes
    by cycle.refresh_account_info alongside the account summary, or
    on-demand via POST /api/account/refresh."""
    data = db.get_broker_positions(account_id, mode)
    orders_by_symbol = {}
    for o in db.get_broker_orders(account_id, mode)["orders"]:
        orders_by_symbol.setdefault(o["symbol"], []).append(o)

    for pos in data["positions"]:
        price = None
        prior_close = None
        try:
            price = cycle._current_price(pos["symbol"])
        except Exception:
            pass
        try:
            prior_close = cycle.get_prior_close(pos["symbol"])
        except Exception:
            pass
        pos["current_price"] = price
        pos["unrealized_pnl"] = ((price - pos["avg_cost"]) * pos["qty"]) if price is not None else None
        pos["daily_pnl"] = ((price - prior_close) * pos["qty"]) if price is not None and prior_close is not None else None

        try:
            eh = cycle.get_extended_hours_quote(pos["symbol"])
        except Exception:
            eh = {"session": None, "price": None, "change_pct": None, "ref_price": None}
        # eh["change_pct"] is the SYMBOL's raw move, direction-agnostic - a
        # rising price is meaningless as "gain" or "loss" on its own for a
        # short (pos["qty"] is already signed, negative for a short), so
        # the dollar figure show here must go through qty like every other
        # P&L column, not just mirror the raw price direction.
        eh["pnl"] = (
            (eh["price"] - eh["ref_price"]) * pos["qty"]
            if eh.get("price") is not None and eh.get("ref_price") is not None else None
        )
        pos["extended_hours"] = eh

        try:
            lp = cycle.get_last_price_quote(pos["symbol"])
        except Exception:
            lp = {"price": None, "change_pct": None, "ref_price": None}
        # Same direction-aware treatment as extended_hours above - lp's
        # change_pct is the symbol's raw move, so the dollar figure shown
        # must go through the signed qty, not the raw price direction.
        lp["pnl"] = (
            (lp["price"] - lp["ref_price"]) * pos["qty"]
            if lp.get("price") is not None and lp.get("ref_price") is not None else None
        )
        pos["last_price_quote"] = lp

        # A stop/take-profit that actually protects/exits THIS position
        # must trade in the closing direction (SELL for a long, BUY for a
        # short) - an STP/TRAIL/LMT order on the same symbol going the
        # other way isn't a stop or take-profit at all (e.g. a separate
        # limit buy order averaging into a long), and showing it as one
        # would be actively misleading about what protects the position.
        # STP and TRAIL both count as "stop" here - a TRAIL is just a stop
        # that follows price instead of sitting at a fixed level.
        closing_action = "SELL" if pos["qty"] > 0 else "BUY"
        symbol_orders = orders_by_symbol.get(pos["symbol"], [])
        pos["stop_orders"] = [o for o in symbol_orders if o["order_type"] in ("STP", "TRAIL") and o["action"] == closing_action]
        pos["take_profit_orders"] = [o for o in symbol_orders if o["order_type"] == "LMT" and o["action"] == closing_action]
    return data


# Placing/cancelling a LIVE stop or take-profit for a real holding is a
# real, immediate change to what protects/exits that position, so it gets
# the same speed bump as every other LIVE-affecting action.
MODIFY_BROKER_ORDER_LIVE_CONFIRM_PHRASE = "ok"


ORDER_TYPE_LABELS = {"stop": "stop", "take_profit": "take-profit", "atr_trailing_stop": "ATR trailing stop"}


@app.post("/api/broker_positions/{symbol}/order")
async def api_add_broker_order(symbol: str, request: Request, mode: str = Depends(require_mode), account_id: int = Depends(require_account), user: str = Depends(require_full_access)):
    """Adds a NEW stop, take-profit, or ATR trailing stop order for
    `symbol` — a position can carry several of each at once, each
    covering its own slice (scaling out); this never touches an existing
    order. An atr_trailing_stop is a real IBKR TRAIL order sized from a
    fresh ATR read (same math open_position.py uses for a brand-new
    entry), computed by modify_broker_order.py itself rather than here.
    See the DELETE endpoint below to cancel one."""
    body = await request.json()
    order_type = body.get("order_type")
    if order_type not in ORDER_TYPE_LABELS:
        raise HTTPException(status_code=400, detail="order_type must be 'stop', 'take_profit', or 'atr_trailing_stop'")
    label = ORDER_TYPE_LABELS[order_type]
    try:
        qty = int(body.get("qty"))
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="qty must be a number")
    if qty <= 0:
        raise HTTPException(status_code=400, detail="qty must be positive")

    price = None
    atr_period = 14
    atr_multiplier = None
    if order_type == "atr_trailing_stop":
        try:
            atr_multiplier = float(body.get("atr_multiplier"))
            if body.get("atr_period") is not None:
                atr_period = int(body.get("atr_period"))
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail="atr_multiplier (and atr_period, if given) must be numbers")
        if atr_multiplier <= 0 or atr_period <= 1:
            raise HTTPException(status_code=400, detail="atr_multiplier must be positive and atr_period must be at least 2")
    else:
        try:
            price = float(body.get("price"))
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail="price must be a number")
        if price <= 0:
            raise HTTPException(status_code=400, detail="price must be positive")

    if mode == "live" and body.get("confirm") != MODIFY_BROKER_ORDER_LIVE_CONFIRM_PHRASE:
        raise HTTPException(
            status_code=400,
            detail=f"Type '{MODIFY_BROKER_ORDER_LIVE_CONFIRM_PHRASE}' to confirm adding a LIVE {label} order.",
        )

    symbol = symbol.strip().upper()

    # Fail fast against the last-refreshed cache (up to ~5 min stale)
    # before spawning a subprocess — modify_broker_order.py itself
    # re-checks against a live connection right before placing, so this is
    # just an early, clear error for the obvious case.
    positions = {p["symbol"]: p for p in db.get_broker_positions(account_id, mode)["positions"]}
    pos = positions.get(symbol)
    if pos is None:
        raise HTTPException(status_code=404, detail="No open position for that symbol")
    held_qty = abs(pos["qty"])
    # A stop and a take-profit for the SAME shares is the normal bracket
    # pattern (protect the whole position both ways at once — if one
    # fills, the other is simply left to cancel manually since these
    # aren't OCO-linked), so only orders of the SAME class compete for the
    # share count: two stops together can't cover more than what's held,
    # but a full stop plus a full take-profit both can. A plain stop and
    # an ATR trailing stop are the same class (STP and TRAIL both just
    # protect the position) and compete against each other too.
    ibkr_order_types = ("STP", "TRAIL") if order_type in ("stop", "atr_trailing_stop") else ("LMT",)
    allocated = sum(
        o["qty"] for o in db.get_broker_orders(account_id, mode)["orders"]
        if o["symbol"] == symbol and o["order_type"] in ibkr_order_types
    )
    if allocated + qty > held_qty:
        raise HTTPException(
            status_code=400,
            detail=f"{allocated} share(s) already allocated across existing {label} orders for {symbol} — "
                   f"adding {qty} more would exceed the {held_qty} actually held.",
        )

    args = [sys.executable, str(PROJECT_DIR / "modify_broker_order.py"), "--mode", mode,
            "--account-id", str(account_id), "--symbol", symbol, "--action", "add",
            "--order-type", order_type, "--qty", str(qty)]
    if order_type == "atr_trailing_stop":
        args += ["--atr-multiplier", str(atr_multiplier), "--atr-period", str(atr_period)]
    else:
        args += ["--price", str(price)]

    proc = subprocess.run(args, capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT)
    db.log_decision(account_id, mode, "dashboard_control", user=user, action="add_broker_order",
                     symbol=symbol, order_type=order_type, price=price, qty=qty,
                     atr_period=atr_period if order_type == "atr_trailing_stop" else None,
                     atr_multiplier=atr_multiplier, stdout=proc.stdout, returncode=proc.returncode)
    if proc.returncode != 0:
        raise HTTPException(status_code=400, detail=proc.stdout.strip().splitlines()[-1] if proc.stdout.strip() else "Add failed")
    return {"ok": True, "stdout": proc.stdout}


@app.delete("/api/broker_positions/{symbol}/order/{order_id}")
def api_cancel_broker_order(symbol: str, order_id: int, confirm: str | None = None, mode: str = Depends(require_mode), account_id: int = Depends(require_account), user: str = Depends(require_full_access)):
    if mode == "live" and confirm != MODIFY_BROKER_ORDER_LIVE_CONFIRM_PHRASE:
        raise HTTPException(
            status_code=400,
            detail=f"Pass ?confirm={MODIFY_BROKER_ORDER_LIVE_CONFIRM_PHRASE} to confirm cancelling a LIVE order.",
        )

    symbol = symbol.strip().upper()
    proc = subprocess.run(
        [sys.executable, str(PROJECT_DIR / "modify_broker_order.py"), "--mode", mode,
         "--account-id", str(account_id), "--symbol", symbol, "--action", "cancel", "--order-id", str(order_id)],
        capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT,
    )
    db.log_decision(account_id, mode, "dashboard_control", user=user, action="cancel_broker_order",
                     symbol=symbol, order_id=order_id, stdout=proc.stdout, returncode=proc.returncode)
    if proc.returncode != 0:
        raise HTTPException(status_code=400, detail=proc.stdout.strip().splitlines()[-1] if proc.stdout.strip() else "Cancel failed")
    return {"ok": True, "stdout": proc.stdout}


@app.get("/api/orders")
def api_list_orders(mode: str = Depends(require_mode), account_id: int = Depends(require_account), user: str = Depends(require_full_access)):
    """Every real resting order in this mode's account, independent of
    order type or whether it's tied to a currently-held position - unlike
    /api/broker_positions' stop_orders/take_profit_orders (which only
    surface STP/LMT orders in the closing direction of an already-open
    position), this also shows a not-yet-filled entry order (e.g. the
    LIMIT half of open_position.py's bracket) and TRAIL stops, neither of
    which appear anywhere else in the dashboard today."""
    return db.get_broker_orders(account_id, mode)


@app.put("/api/orders/{order_id}")
async def api_edit_order(order_id: int, request: Request, mode: str = Depends(require_mode), account_id: int = Depends(require_account), user: str = Depends(require_full_access)):
    """Modifies an existing resting order in place (same order ID - IBKR
    treats this as a live modification, not cancel+replace). Works for any
    order type (LMT entry, STP, TRAIL, take-profit)."""
    body = await request.json()
    symbol = str(body.get("symbol", "")).strip().upper()
    if not symbol:
        raise HTTPException(status_code=400, detail="symbol is required")
    try:
        price = float(body.get("price"))
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="price must be a number")
    if price <= 0:
        raise HTTPException(status_code=400, detail="price must be positive")
    qty = body.get("qty")
    if qty is not None:
        try:
            qty = int(qty)
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail="qty must be a whole number")
        if qty <= 0:
            raise HTTPException(status_code=400, detail="qty must be positive")

    if mode == "live" and body.get("confirm") != MODIFY_BROKER_ORDER_LIVE_CONFIRM_PHRASE:
        raise HTTPException(
            status_code=400,
            detail=f"Type '{MODIFY_BROKER_ORDER_LIVE_CONFIRM_PHRASE}' to confirm editing a LIVE order.",
        )

    cmd = [sys.executable, str(PROJECT_DIR / "modify_broker_order.py"), "--mode", mode,
           "--account-id", str(account_id), "--symbol", symbol, "--action", "edit",
           "--order-id", str(order_id), "--price", str(price)]
    if qty is not None:
        cmd += ["--qty", str(qty)]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT)
    db.log_decision(account_id, mode, "dashboard_control", user=user, action="edit_order",
                     symbol=symbol, order_id=order_id, price=price, qty=qty, stdout=proc.stdout, returncode=proc.returncode)
    if proc.returncode != 0:
        raise HTTPException(status_code=400, detail=proc.stdout.strip().splitlines()[-1] if proc.stdout.strip() else "Edit failed")
    return {"ok": True, "stdout": proc.stdout}


@app.delete("/api/orders/{order_id}")
def api_cancel_order(order_id: int, symbol: str, confirm: str | None = None, mode: str = Depends(require_mode), account_id: int = Depends(require_account), user: str = Depends(require_full_access)):
    """Cancels any resting order by ID, independent of order type or
    whether it's tied to a currently-held position."""
    if mode == "live" and confirm != MODIFY_BROKER_ORDER_LIVE_CONFIRM_PHRASE:
        raise HTTPException(
            status_code=400,
            detail=f"Pass ?confirm={MODIFY_BROKER_ORDER_LIVE_CONFIRM_PHRASE} to confirm cancelling a LIVE order.",
        )
    symbol = symbol.strip().upper()
    proc = subprocess.run(
        [sys.executable, str(PROJECT_DIR / "modify_broker_order.py"), "--mode", mode,
         "--account-id", str(account_id), "--symbol", symbol, "--action", "cancel", "--order-id", str(order_id)],
        capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT,
    )
    db.log_decision(account_id, mode, "dashboard_control", user=user, action="cancel_order",
                     symbol=symbol, order_id=order_id, stdout=proc.stdout, returncode=proc.returncode)
    if proc.returncode != 0:
        raise HTTPException(status_code=400, detail=proc.stdout.strip().splitlines()[-1] if proc.stdout.strip() else "Cancel failed")
    return {"ok": True, "stdout": proc.stdout}


@app.post("/api/broker_positions/close")
async def api_broker_positions_close(request: Request, mode: str = Depends(require_mode), account_id: int = Depends(require_account), user: str = Depends(require_full_access)):
    body = await request.json()
    symbol = (body.get("symbol") or "").strip().upper()
    if not symbol:
        raise HTTPException(status_code=400, detail="symbol is required")

    if mode == "live" and body.get("confirm") != CLOSE_LIVE_CONFIRM_PHRASE:
        raise HTTPException(
            status_code=400,
            detail=f"Type '{CLOSE_LIVE_CONFIRM_PHRASE}' to confirm closing a LIVE position.",
        )

    qty = body.get("qty")
    cmd = [sys.executable, str(PROJECT_DIR / "close_position.py"), "--mode", mode,
           "--account-id", str(account_id), "--symbol", symbol]
    if qty:
        cmd += ["--qty", str(int(qty))]

    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT)
    db.log_decision(account_id, mode, "dashboard_control", user=user, action="close_broker_position",
                     symbol=symbol, qty=qty, stdout=proc.stdout, returncode=proc.returncode)
    if proc.returncode != 0:
        raise HTTPException(status_code=400, detail=proc.stdout.strip().splitlines()[-1] if proc.stdout.strip() else "Close failed")
    return {"ok": True, "stdout": proc.stdout}


# A manual buy/sell fires a real market order immediately and independent
# of any strategy - it never gets a bot-managed stop (use the Stop/Take
# Profit columns in Account Holdings for that once it shows up there), so
# it gets the same speed bump as every other LIVE-affecting action.
MANUAL_ORDER_LIVE_CONFIRM_PHRASE = "ok"


@app.post("/api/trading/order")
async def api_place_manual_order(request: Request, mode: str = Depends(require_mode), account_id: int = Depends(require_account), user: str = Depends(require_full_access)):
    body = await request.json()
    symbol = (body.get("symbol") or "").strip().upper()
    side = body.get("side")
    if not symbol:
        raise HTTPException(status_code=400, detail="symbol is required")
    if side not in ("BUY", "SELL"):
        raise HTTPException(status_code=400, detail="side must be 'BUY' or 'SELL'")
    try:
        qty = int(body.get("qty"))
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="qty must be a whole number")
    if qty <= 0:
        raise HTTPException(status_code=400, detail="qty must be positive")

    if mode == "live" and body.get("confirm") != MANUAL_ORDER_LIVE_CONFIRM_PHRASE:
        raise HTTPException(
            status_code=400,
            detail=f"Type '{MANUAL_ORDER_LIVE_CONFIRM_PHRASE}' to confirm placing a LIVE market order.",
        )

    proc = subprocess.run(
        [sys.executable, str(PROJECT_DIR / "trade.py"), "--mode", mode,
         "--account-id", str(account_id), "--symbol", symbol, "--side", side, "--size", str(qty)],
        capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT,
    )
    db.log_decision(account_id, mode, "dashboard_control", user=user, action="manual_order",
                     symbol=symbol, side=side, qty=qty, stdout=proc.stdout, returncode=proc.returncode)
    if proc.returncode != 0:
        raise HTTPException(status_code=400, detail=proc.stdout.strip().splitlines()[-1] if proc.stdout.strip() else "Order failed")
    return {"ok": True, "stdout": proc.stdout}


@app.get("/api/trades")
def api_trades(limit: int = 100, mode: str = Depends(require_mode), account_id: int = Depends(require_account), user: str = Depends(require_full_access)):
    return db.get_trades(account_id, mode, limit=limit)


@app.get("/api/performance")
def api_performance(mode: str = Depends(require_mode), account_id: int = Depends(require_account), user: str = Depends(require_full_access)):
    rows = db.get_trades(account_id, mode, limit=5000, today_only=True)
    pairs = perf.pair_trades(rows)
    aggregates = perf.aggregate(pairs)
    r_values = perf.compute_r_multiples(pairs)
    return {
        "aggregates": aggregates,
        "histogram": [{"label": l, "count": c, "is_loss": loss} for l, c, loss in perf.histogram(r_values)],
    }


@app.get("/api/watchlist")
def api_watchlist(mode: str = Depends(require_mode), account_id: int = Depends(require_account), user: str = Depends(require_full_access)):
    return db.get_watchlist(account_id, mode)


@app.get("/api/watchlist_filters")
def api_watchlist_filters(mode: str = Depends(require_mode), account_id: int = Depends(require_account), user: str = Depends(require_full_access)):
    return db.get_watchlist_filters(account_id, mode)


@app.post("/api/prefilter/run")
def api_run_prefilter(account_id: int = Depends(require_account), user: str = Depends(require_full_access)):
    """On-demand gap scan — the same scan the scheduler runs at :25/:55 past
    the hour (9:25-12:55 ET), triggered right now instead of waiting for
    the next scheduled slot. Mode-agnostic like the scan itself: writes the
    same watchlist to both paper and live. Takes a while (scans the whole
    S&P 500 via yfinance) — the request blocks until it's done."""
    result = morning_prefilter.run_scan(morning_prefilter.DEFAULT_MIN_GAP_PCT, morning_prefilter.DEFAULT_MIN_PRICE, False)
    if result.get("success"):
        cycle.scan_watchlist_filters(account_id)
    _log_account_action(account_id, user, action="run_prefilter_now", success=result.get("success"),
                          up=result.get("up_survivors_count"), down=result.get("down_survivors_count"))
    return result


@app.get("/api/decision_log")
def api_decision_log(limit: int = 100, mode: str = Depends(require_mode), account_id: int = Depends(require_account), user: str = Depends(require_full_access)):
    rows = db.get_decision_log(account_id, mode, limit=limit)
    for r in rows:
        try:
            r["payload"] = json.loads(r["payload_json"])
        except (json.JSONDecodeError, TypeError):
            r["payload"] = {}
    return rows


# ------------------------------------------------------------- strategies ---
# Strategy templates are shared across every account and both modes — no
# `mode` param here. Actions are logged into both modes' activity logs
# (for this account) so either tab shows them.
def _log_account_action(account_id: int, user: str, **fields):
    for m in db.MODES:
        db.log_decision(account_id, m, "dashboard_control", user=user, **fields)


@app.get("/api/strategies")
def api_list_strategies(account_id: int = Depends(require_account), user: str = Depends(require_user)):
    return db.list_strategies(account_id)


@app.get("/api/strategies/{strategy_id}")
def api_get_strategy(strategy_id: int, user: str = Depends(require_user)):
    strategy = db.get_strategy(strategy_id)
    if not strategy:
        raise HTTPException(status_code=404, detail="Strategy not found")
    strategy["rules"] = json.loads(strategy["rules_json"])
    return strategy


@app.post("/api/strategies")
async def api_create_strategy(request: Request, account_id: int = Depends(require_account), user: str = Depends(require_admin)):
    body = await request.json()
    name = body.get("name")
    rules = body.get("rules")
    direction = body.get("direction", "long")
    risk_rating = body.get("risk_rating", "moderate")
    description = body.get("description", "") or ""
    key = (body.get("key") or "").strip()
    if not name or not isinstance(rules, dict):
        raise HTTPException(status_code=400, detail="name and rules are required")
    if direction not in db.DIRECTIONS:
        raise HTTPException(status_code=400, detail=f"direction must be one of {db.DIRECTIONS}")
    if risk_rating not in db.RISK_RATINGS:
        raise HTTPException(status_code=400, detail=f"risk_rating must be one of {db.RISK_RATINGS}")
    try:
        strategy_id = db.create_strategy(name, rules, direction, risk_rating, description, key)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    _log_account_action(account_id, user, action="create_strategy", name=name, direction=direction, risk_rating=risk_rating)
    return {"id": strategy_id}


@app.put("/api/strategies/{strategy_id}")
async def api_update_strategy(strategy_id: int, request: Request, account_id: int = Depends(require_account), user: str = Depends(require_admin)):
    if not db.get_strategy(strategy_id):
        raise HTTPException(status_code=404, detail="Strategy not found")
    body = await request.json()
    rules = body.get("rules")
    risk_rating = body.get("risk_rating")
    description = body.get("description")
    key = body.get("key")
    if key is not None:
        key = key.strip()
    if not isinstance(rules, dict):
        raise HTTPException(status_code=400, detail="rules is required")
    if risk_rating is not None and risk_rating not in db.RISK_RATINGS:
        raise HTTPException(status_code=400, detail=f"risk_rating must be one of {db.RISK_RATINGS}")
    try:
        db.update_strategy(strategy_id, rules, risk_rating, description, key)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    _log_account_action(account_id, user, action="update_strategy", strategy_id=strategy_id)
    return {"ok": True}


# Strategies are shared across paper AND live — activating one takes effect
# on the live engine immediately too. A typed confirmation is required only
# for the highest tier (aggressive), matching the same speed-bump pattern
# used for editing LIVE risk sizing.
ACTIVATE_AGGRESSIVE_CONFIRM_PHRASE = "ok"


@app.post("/api/strategies/{strategy_id}/activate")
async def api_activate_strategy(strategy_id: int, request: Request, account_id: int = Depends(require_account), user: str = Depends(require_full_access)):
    strategy = db.get_strategy(strategy_id)
    if not strategy:
        raise HTTPException(status_code=404, detail="Strategy not found")
    if strategy["risk_rating"] == "aggressive":
        try:
            body = await request.json()
        except Exception:
            body = {}
        if body.get("confirm") != ACTIVATE_AGGRESSIVE_CONFIRM_PHRASE:
            raise HTTPException(
                status_code=400,
                detail=f"Type '{ACTIVATE_AGGRESSIVE_CONFIRM_PHRASE}' to confirm activating an aggressive strategy.",
            )
    db.activate_strategy(account_id, strategy_id)
    _log_account_action(account_id, user, action="activate_strategy", strategy_id=strategy_id, name=strategy["name"])
    return {"ok": True}


@app.post("/api/strategies/{strategy_id}/deactivate")
async def api_deactivate_strategy(strategy_id: int, account_id: int = Depends(require_account), user: str = Depends(require_full_access)):
    strategy = db.get_strategy(strategy_id)
    if not strategy:
        raise HTTPException(status_code=404, detail="Strategy not found")
    db.deactivate_strategy(account_id, strategy_id)
    _log_account_action(account_id, user, action="deactivate_strategy", strategy_id=strategy_id, name=strategy["name"])
    return {"ok": True}


@app.delete("/api/strategies/{strategy_id}")
def api_delete_strategy(strategy_id: int, account_id: int = Depends(require_account), user: str = Depends(require_admin)):
    try:
        db.delete_strategy(strategy_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    _log_account_action(account_id, user, action="delete_strategy", strategy_id=strategy_id)
    return {"ok": True}


# ------------------------------------------------------------------ users ---
# Admin-only account management - the only user-creation path before this
# was /setup, which runs at most once and always creates a single
# is_admin=True account (see DEPLOY.md). This is how an admin adds more
# accounts afterward, in particular role='viewer' ones.
@app.get("/api/users")
def api_list_users(user: str = Depends(require_admin)):
    return db.list_users()


@app.post("/api/users")
async def api_create_user(request: Request, user: str = Depends(require_admin)):
    body = await request.json()
    username = (body.get("username") or "").strip()
    password = body.get("password") or ""
    role = body.get("role", "full")
    is_admin = bool(body.get("is_admin", False))
    if not username:
        raise HTTPException(status_code=400, detail="username is required")
    if len(password) < 8:
        raise HTTPException(status_code=400, detail="Password must be at least 8 characters.")
    if role not in db.VALID_USER_ROLES:
        raise HTTPException(status_code=400, detail=f"role must be one of {db.VALID_USER_ROLES}")
    try:
        new_id = db.create_user(username, password, is_admin=is_admin, role=role)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"id": new_id}


@app.put("/api/users/{user_id}/role")
async def api_set_user_role(user_id: int, request: Request, user: str = Depends(require_admin)):
    body = await request.json()
    role = body.get("role")
    if role not in db.VALID_USER_ROLES:
        raise HTTPException(status_code=400, detail=f"role must be one of {db.VALID_USER_ROLES}")
    target = db.get_user_by_username(user)
    # Not a hard security boundary (require_admin still lets an admin fix
    # this via direct DB access if truly needed) - just avoiding the
    # confusing, easy-to-trigger-by-accident state of an admin locking
    # themselves out of /bot and /trading with one click.
    if target and target["id"] == user_id and role == "viewer":
        raise HTTPException(status_code=400, detail="Cannot set your own account to viewer")
    db.set_user_role(user_id, role)
    return {"ok": True}


@app.delete("/api/users/{user_id}")
def api_delete_user(user_id: int, user: str = Depends(require_admin)):
    requester = db.get_user_by_username(user)
    if requester and requester["id"] == user_id:
        raise HTTPException(status_code=400, detail="Cannot delete your own account")
    try:
        db.delete_user(user_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"ok": True}


# --------------------------------------------------------------- backtests ---
# Spawned as an isolated subprocess (run_backtest.py), like every
# IBKR-touching endpoint elsewhere in this file — not for a client-id
# concern (backtest_engine.py only reads the local historical-bar cache
# plus yfinance for daily bars, never talks to IBKR at run time), but for
# memory isolation: a full-universe backtest holds every cached symbol's
# entire intraday history in memory for the whole simulation, which used
# to run as a background thread INSIDE this dashboard process — so that
# memory pressure landed directly on the one process everything else
# (Account Holdings, trading controls, this backtest's own progress)
# depends on staying up. A subprocess's memory is fully released back to
# the OS the moment it exits, win or lose, regardless of how much it used.
DEFAULT_BACKTEST_PORTFOLIO_VALUE = 100_000.0
DEFAULT_BACKTEST_MAX_RISK_PCT = 1.0
DEFAULT_BACKTEST_MAX_TRADES_PER_DAY = 5
# Real per-fill commission (e.g. IBKR's own rate) - modeled per execution,
# not per round-trip, so a partial-profit trade (entry + partial close +
# final close = 3 fills) correctly costs 3x, not 2x. Small position sizes
# on a small portfolio can show a "profitable" gross P&L that's actually a
# net loser once this is subtracted - see simulate_strategy/pair_trades/
# aggregate for where it's actually applied.
DEFAULT_BACKTEST_COMMISSION_PER_TRADE = 1.5


def _start_backtest(account_id: int, user: str, params: dict, execution_mode: str) -> int:
    """Shared by api_create_backtest and api_retry_backtest: creates a new
    backtests row for `params` and actually starts it (local subprocess,
    or left 'pending' for a remote worker to claim). The retry path reuses
    this to re-run a failed backtest's exact params as a brand-new row -
    it never resets/reuses the failed row itself, so the original failure
    stays in History as its own record instead of being overwritten."""
    backtest_id = db.create_backtest(account_id, params, execution_mode=execution_mode)
    _log_account_action(
        account_id, user, action="create_backtest", backtest_id=backtest_id,
        strategy_ids=params["strategy_ids"], execution_mode=execution_mode,
    )
    if execution_mode == "local":
        # Fire-and-forget (Popen, not run) - the run itself can take a
        # while for a wide date range or many symbols, and this request
        # must return immediately with the new backtest's id so the
        # dashboard can start polling GET /api/backtests/{id} for
        # progress.
        proc = subprocess.Popen([sys.executable, str(PROJECT_DIR / "run_backtest.py"), "--backtest-id", str(backtest_id)])
        db.set_backtest_pid(backtest_id, proc.pid)
    # execution_mode == "remote": the row stays 'pending' with no local
    # process at all - a worker picks it up via POST /api/worker/claim
    # whenever it next polls (see docs/worker.md).
    return backtest_id


@app.post("/api/backtests")
async def api_create_backtest(request: Request, account_id: int = Depends(require_account), user: str = Depends(require_full_access)):
    body = await request.json()
    strategy_ids = body.get("strategy_ids")
    if not isinstance(strategy_ids, list) or not strategy_ids:
        raise HTTPException(status_code=400, detail="strategy_ids (a non-empty list) is required")
    try:
        strategy_ids = [int(sid) for sid in strategy_ids]
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="strategy_ids must be integers")

    try:
        start_date = date.fromisoformat(body.get("start_date", ""))
        end_date = date.fromisoformat(body.get("end_date", ""))
    except ValueError:
        raise HTTPException(status_code=400, detail="start_date/end_date must be YYYY-MM-DD")
    if start_date > end_date:
        raise HTTPException(status_code=400, detail="start_date must not be after end_date")

    symbols = body.get("symbols") or backtest_data.cached_symbols(backtest_engine.BAR_SIZE)
    if not symbols:
        raise HTTPException(
            status_code=400,
            detail="No symbols have cached historical bars yet - run fetch_backtest_data.py on the server first.",
        )

    execution_mode = body.get("execution_mode", "local")
    if execution_mode not in ("local", "remote"):
        raise HTTPException(status_code=400, detail="execution_mode must be 'local' or 'remote'")

    params = {
        "strategy_ids": strategy_ids,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "symbols": symbols,
        "portfolio_value": float(body.get("portfolio_value", DEFAULT_BACKTEST_PORTFOLIO_VALUE)),
        "max_risk_pct": float(body.get("max_risk_pct", DEFAULT_BACKTEST_MAX_RISK_PCT)),
        "max_trades_per_day": int(body.get("max_trades_per_day", DEFAULT_BACKTEST_MAX_TRADES_PER_DAY)),
        "commission_per_trade": float(body.get("commission_per_trade", DEFAULT_BACKTEST_COMMISSION_PER_TRADE)),
    }
    backtest_id = _start_backtest(account_id, user, params, execution_mode)
    return {"id": backtest_id}


@app.get("/api/backtests")
def api_list_backtests(account_id: int = Depends(require_account), user: str = Depends(require_user)):
    return db.list_backtests(account_id)


# Registered ahead of /api/backtests/{backtest_id} - Starlette matches routes
# in registration order and {backtest_id} (typed int only via FastAPI/
# Pydantic validation AFTER the path already matched, not by the router
# itself) would otherwise swallow this path first and 422 on "strategy_report"
# failing to parse as an int, never reaching this route at all.
@app.get("/api/backtests/strategy_report")
def api_backtests_strategy_report(account_id: int = Depends(require_account), user: str = Depends(require_user)):
    return perf.strategy_report(db.list_done_backtest_results(account_id))


# Same registration-order reason as strategy_report above.
@app.get("/api/backtests/calendar")
def api_backtests_calendar(account_id: int = Depends(require_account), user: str = Depends(require_user)):
    return db.list_backtest_calendar_entries(account_id)


# Same registration-order reason as strategy_report above - GET here, not
# under /api/backtests/{backtest_id}, so it must be registered first too.
@app.get("/api/backtests/archive")
def api_list_archived_backtests(account_id: int = Depends(require_account), user: str = Depends(require_user)):
    return db.list_archived_backtests(account_id)


@app.post("/api/backtests/archive")
async def api_archive_backtests(request: Request, account_id: int = Depends(require_account), user: str = Depends(require_full_access)):
    body = await request.json()
    backtest_ids = body.get("backtest_ids") or []
    folder = (body.get("folder") or "").strip()
    if not backtest_ids:
        raise HTTPException(status_code=400, detail="backtest_ids is required")
    if not folder:
        raise HTTPException(status_code=400, detail="folder (a short reason) is required")
    count = db.archive_backtests(account_id, backtest_ids, folder)
    _log_account_action(account_id, user, action="archive_backtests", backtest_ids=backtest_ids, folder=folder)
    return {"archived": count}


@app.post("/api/backtests/unarchive")
async def api_unarchive_backtests(request: Request, account_id: int = Depends(require_account), user: str = Depends(require_full_access)):
    body = await request.json()
    backtest_ids = body.get("backtest_ids") or []
    if not backtest_ids:
        raise HTTPException(status_code=400, detail="backtest_ids is required")
    count = db.unarchive_backtests(account_id, backtest_ids)
    _log_account_action(account_id, user, action="unarchive_backtests", backtest_ids=backtest_ids)
    return {"restored": count}


# Registered ahead of /api/backtests/{backtest_id}/cancel for the same
# registration-order reason as archive/unarchive above (a fixed path must
# come before a path-parameter one it could otherwise collide with).
@app.post("/api/backtests/cancel")
async def api_cancel_backtests(request: Request, account_id: int = Depends(require_account), user: str = Depends(require_full_access)):
    body = await request.json()
    backtest_ids = body.get("backtest_ids") or []
    if not backtest_ids:
        raise HTTPException(status_code=400, detail="backtest_ids is required")
    count = db.cancel_backtests(account_id, backtest_ids)
    _log_account_action(account_id, user, action="cancel_backtests", backtest_ids=backtest_ids)
    return {"cancelled": count}


@app.post("/api/backtests/delete")
async def api_delete_backtests(request: Request, account_id: int = Depends(require_account), user: str = Depends(require_full_access)):
    body = await request.json()
    backtest_ids = body.get("backtest_ids") or []
    if not backtest_ids:
        raise HTTPException(status_code=400, detail="backtest_ids is required")
    count = db.delete_backtests(account_id, backtest_ids)
    _log_account_action(account_id, user, action="delete_backtests", backtest_ids=backtest_ids)
    return {"deleted": count}


# ----------------------------------------------------- backtest data fetch ---
# "Update backtest data" button on the Backtest page - refreshes the local
# intraday-bars cache from IBKR (see fetch_backtest_data.py). Spawned the
# same way as a local backtest (subprocess.Popen, tracked via its own DB
# row) since it needs a live IB Gateway connection and can run a long time -
# neither of which this always-on dashboard process should hold itself.

# Registered ahead of /api/backtest_data_fetch/{fetch_id} for the same
# registration-order reason as strategy_report/calendar above.
@app.get("/api/backtest_data_fetch/status")
def api_backtest_data_fetch_status(
    mode: str = Query("paper"), account_id: int = Depends(require_account), user: str = Depends(require_user)
):
    if mode not in ("paper", "live"):
        raise HTTPException(status_code=400, detail="mode must be 'paper' or 'live'.")
    return {
        "gateway": gateway_control.status(mode, _env()),
        "latest": db.get_latest_backtest_data_fetch(account_id),
    }


# Reads every cached symbol's full bars file (see cache_coverage_summary's
# own docstring on why) - meant for page load / right after an update
# finishes, never the 5s poll api_backtest_data_fetch_status serves, so it's
# kept as its own endpoint rather than folded into that one.
@app.get("/api/backtest_data_fetch/coverage")
def api_backtest_data_fetch_coverage(user: str = Depends(require_user)):
    return {
        "coverage": backtest_data.cache_coverage_summary(backtest_engine.BAR_SIZE),
        "symbols_total_expected": len(SP500_TICKERS),
    }


@app.post("/api/backtest_data_fetch")
async def api_create_backtest_data_fetch(request: Request, account_id: int = Depends(require_account), user: str = Depends(require_full_access)):
    body = await request.json() if await request.body() else {}
    mode = body.get("mode", "paper")
    if mode not in ("paper", "live"):
        raise HTTPException(status_code=400, detail="mode must be 'paper' or 'live'.")
    gw = gateway_control.status(mode, _env())
    if not (gw["gateway_active"] and gw["port_listening"]):
        raise HTTPException(
            status_code=400,
            detail=f"IBKR {mode} Gateway isn't connected - check Gateway Connection before updating backtest data.",
        )
    latest = db.get_latest_backtest_data_fetch(account_id)
    if latest and latest["status"] in ("pending", "running"):
        raise HTTPException(status_code=409, detail=f"An update (#{latest['id']}) is already running.")
    fetch_id = db.create_backtest_data_fetch(account_id, mode)
    proc = subprocess.Popen([sys.executable, str(PROJECT_DIR / "run_backtest_data_fetch.py"), "--fetch-id", str(fetch_id), "--mode", mode])
    db.set_backtest_data_fetch_pid(fetch_id, proc.pid)
    _log_account_action(account_id, user, action="backtest_data_fetch_start", fetch_id=fetch_id, fetch_mode=mode)
    return {"id": fetch_id}


@app.get("/api/backtest_data_fetch/{fetch_id}")
def api_get_backtest_data_fetch(fetch_id: int, account_id: int = Depends(require_account), user: str = Depends(require_user)):
    record = db.get_backtest_data_fetch(fetch_id)
    if not record or record["account_id"] != account_id:
        raise HTTPException(status_code=404, detail="Not found")
    return record


@app.post("/api/backtest_data_fetch/{fetch_id}/cancel")
def api_cancel_backtest_data_fetch(fetch_id: int, account_id: int = Depends(require_account), user: str = Depends(require_full_access)):
    if not db.cancel_backtest_data_fetch(fetch_id, account_id):
        raise HTTPException(status_code=404, detail="Update not found, or already finished")
    _log_account_action(account_id, user, action="cancel_backtest_data_fetch", fetch_id=fetch_id)
    return {"ok": True}


@app.get("/api/backtests/{backtest_id}")
def api_get_backtest(backtest_id: int, account_id: int = Depends(require_account), user: str = Depends(require_user)):
    result = db.get_backtest(backtest_id)
    if not result or result["account_id"] != account_id:
        raise HTTPException(status_code=404, detail="Backtest not found")
    return result


@app.delete("/api/backtests/{backtest_id}")
def api_delete_backtest(backtest_id: int, account_id: int = Depends(require_account), user: str = Depends(require_full_access)):
    if not db.delete_backtest(backtest_id, account_id):
        raise HTTPException(status_code=404, detail="Backtest not found")
    return {"ok": True}


@app.post("/api/backtests/{backtest_id}/cancel")
def api_cancel_backtest(backtest_id: int, account_id: int = Depends(require_account), user: str = Depends(require_full_access)):
    if not db.cancel_backtest(backtest_id, account_id):
        raise HTTPException(status_code=404, detail="Backtest not found, or already finished")
    _log_account_action(account_id, user, action="cancel_backtest", backtest_id=backtest_id)
    return {"ok": True}


@app.post("/api/backtests/{backtest_id}/retry")
def api_retry_backtest(backtest_id: int, account_id: int = Depends(require_account), user: str = Depends(require_full_access)):
    record = db.get_backtest(backtest_id)
    if not record or record["account_id"] != account_id:
        raise HTTPException(status_code=404, detail="Backtest not found")
    if record["status"] != "failed":
        raise HTTPException(status_code=400, detail="Only a failed backtest can be retried")
    new_id = _start_backtest(account_id, user, record["params"], record["execution_mode"])
    _log_account_action(account_id, user, action="retry_backtest", backtest_id=backtest_id, new_backtest_id=new_id)
    return {"id": new_id}


@app.get("/api/backtest_universe")
def api_backtest_universe(user: str = Depends(require_user)):
    symbols = backtest_data.cached_symbols(backtest_engine.BAR_SIZE)
    return {"symbols": symbols, "count": len(symbols)}


# --------------------------------------------------------- backtest worker ---
# Token management is browser-session-authenticated (a real logged-in user
# creates/revokes these), like everything else in this file up to here.
# The /api/worker/* endpoints below it are different: a remote worker has
# no session cookie at all, so those go through require_worker_token
# (Authorization: Bearer <token>) instead of require_account/require_user.
# See docs/worker.md and backtest_worker.py.
@app.post("/api/worker_tokens")
async def api_create_worker_token(request: Request, account_id: int = Depends(require_account), user: str = Depends(require_full_access)):
    body = await request.json()
    label = (body.get("label") or "").strip()
    token_id, raw_token = db.create_worker_token(account_id, label)
    _log_account_action(account_id, user, action="create_worker_token", token_id=token_id, label=label)
    # raw_token is returned ONLY on this one response - the db never
    # stores it, just its hash (see create_worker_token), so there is no
    # way to retrieve it again later. Losing it means generating a new one.
    return {"id": token_id, "token": raw_token}


@app.get("/api/worker_tokens")
def api_list_worker_tokens(account_id: int = Depends(require_account), user: str = Depends(require_full_access)):
    return db.list_worker_tokens(account_id)


@app.delete("/api/worker_tokens/{token_id}")
def api_delete_worker_token(token_id: int, account_id: int = Depends(require_account), user: str = Depends(require_full_access)):
    if not db.delete_worker_token(token_id, account_id):
        raise HTTPException(status_code=404, detail="Worker token not found")
    _log_account_action(account_id, user, action="delete_worker_token", token_id=token_id)
    return {"ok": True}


@app.post("/api/worker/claim")
def api_worker_claim(account_id: int = Depends(require_worker_token)):
    job = db.claim_next_backtest(account_id)
    return job  # None (-> JSON null) when there's nothing pending - the worker's own poll loop treats that as "nothing to do, sleep and retry"


@app.post("/api/worker/backtests/{backtest_id}/result")
async def api_worker_submit_result(backtest_id: int, request: Request, account_id: int = Depends(require_worker_token)):
    body = await request.json()
    results = body.get("results")
    if not isinstance(results, dict):
        raise HTTPException(status_code=400, detail="results (an object) is required")
    if not db.submit_worker_result(backtest_id, account_id, results):
        raise HTTPException(status_code=404, detail="Backtest not found, not claimed by this account, or no longer 'running' (already cancelled/timed out/finished)")
    return {"ok": True}


@app.post("/api/worker/backtests/{backtest_id}/fail")
async def api_worker_fail(backtest_id: int, request: Request, account_id: int = Depends(require_worker_token)):
    body = await request.json()
    error = body.get("error") or "Worker reported failure with no error message"
    if not db.fail_worker_backtest(backtest_id, account_id, str(error)):
        raise HTTPException(status_code=404, detail="Backtest not found, not claimed by this account, or no longer 'running'")
    return {"ok": True}
