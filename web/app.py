"""Dashboard web app: login, bot start/stop/emergency-flatten controls, live
positions/trades/performance views, and the multi-strategy switcher — for
BOTH the paper and live engines at once, selected per-request via a `mode`
query param (the frontend has a Paper/Live tab). Reads and writes the same
SQLite DB the two trading services (run_service.py --mode paper/live) use;
it never talks to IBKR directly, so it can safely run as a separate process.
"""
import json
import subprocess
import sys
from pathlib import Path

from dotenv import dotenv_values
from fastapi import Depends, FastAPI, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

import cycle
import morning_prefilter
from src import db, gateway_provisioning, mode_config, perf, secrets_store
from web import gateway_control
from web.auth import COOKIE_NAME, make_session_cookie, read_session, require_user

PROJECT_DIR = Path(__file__).resolve().parent.parent
templates = Jinja2Templates(directory=str(Path(__file__).resolve().parent / "templates"))

app = FastAPI(title="TradingBot Dashboard")

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


@app.on_event("startup")
def on_startup():
    db.init_db(seed_rules_path=PROJECT_DIR / "rules.json")


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


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    if not db.any_users_exist():
        return RedirectResponse("/setup", status_code=303)
    if not read_session(request):
        return RedirectResponse("/login", status_code=303)
    return templates.TemplateResponse(request, "index.html", {})


@app.get("/guide", response_class=HTMLResponse)
def guide(request: Request):
    if not read_session(request):
        return RedirectResponse("/login", status_code=303)
    return templates.TemplateResponse(request, "guide.html", {})


# -------------------------------------------------------------------- API ---
@app.get("/api/me")
def api_me(account_id: int = Depends(require_account), user: str = Depends(require_user)):
    account = db.get_user_by_username(user)
    return {"username": user, "is_admin": bool(account["is_admin"])}


@app.get("/api/status")
def api_status(mode: str = Depends(require_mode), account_id: int = Depends(require_account), user: str = Depends(require_user)):
    return db.get_cycle_status(account_id, mode)


@app.post("/api/control/enable")
def api_enable(mode: str = Depends(require_mode), account_id: int = Depends(require_account), user: str = Depends(require_user)):
    db.set_bot_enabled(account_id, mode, True)
    db.log_decision(account_id, mode, "dashboard_control", action="enable", user=user)
    return {"bot_enabled": True}


@app.post("/api/control/disable")
def api_disable(mode: str = Depends(require_mode), account_id: int = Depends(require_account), user: str = Depends(require_user)):
    db.set_bot_enabled(account_id, mode, False)
    db.log_decision(account_id, mode, "dashboard_control", action="disable", user=user)
    return {"bot_enabled": False}


@app.post("/api/control/flatten")
def api_flatten(mode: str = Depends(require_mode), account_id: int = Depends(require_account), user: str = Depends(require_user)):
    db.request_flatten_now(account_id, mode)
    db.log_decision(account_id, mode, "dashboard_control", action="flatten_now", user=user)
    return {"flatten_pending": True}


@app.get("/api/account")
def api_account(mode: str = Depends(require_mode), account_id: int = Depends(require_account), user: str = Depends(require_user)):
    return db.get_account_info(account_id, mode)


@app.get("/api/risk_params")
def api_get_risk_params(mode: str = Depends(require_mode), account_id: int = Depends(require_account), user: str = Depends(require_user)):
    return mode_config.risk_params(_env(), account_id, mode)


@app.post("/api/risk_params")
async def api_set_risk_params(request: Request, mode: str = Depends(require_mode), account_id: int = Depends(require_account), user: str = Depends(require_user)):
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
def api_get_ibkr_credentials(account_id: int = Depends(require_account), user: str = Depends(require_user)):
    creds = db.get_ibkr_credentials(account_id)
    if not creds:
        return {"configured": False, "ibkr_username": None, "updated_at": None}
    return {"configured": True, "ibkr_username": creds["ibkr_username"], "updated_at": creds["updated_at"]}


@app.post("/api/ibkr_credentials")
async def api_set_ibkr_credentials(request: Request, account_id: int = Depends(require_account), user: str = Depends(require_user)):
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
def api_my_gateway_status(account_id: int = Depends(require_account), user: str = Depends(require_user)):
    try:
        return gateway_provisioning.status(account_id)
    except Exception as exc:
        raise _provisioning_error_response(exc)


@app.post("/api/my_gateway/connect")
def api_my_gateway_connect(account_id: int = Depends(require_account), user: str = Depends(require_user)):
    try:
        gateway_provisioning.provision_and_connect(account_id)
    except Exception as exc:
        raise _provisioning_error_response(exc)
    _log_account_action(account_id, user, action="my_gateway_connect")
    return {"ok": True}


@app.post("/api/my_gateway/resume")
def api_my_gateway_resume(account_id: int = Depends(require_account), user: str = Depends(require_user)):
    try:
        gateway_provisioning.resume_engines(account_id)
    except Exception as exc:
        raise _provisioning_error_response(exc)
    _log_account_action(account_id, user, action="my_gateway_resume")
    return {"ok": True}


@app.post("/api/my_gateway/disconnect")
def api_my_gateway_disconnect(account_id: int = Depends(require_account), user: str = Depends(require_user)):
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
def api_gateway_status(mode: str = Depends(require_mode), user: str = Depends(require_user)):
    return gateway_control.status(mode, _env())


@app.post("/api/gateway/disconnect")
async def api_gateway_disconnect(request: Request, mode: str = Depends(require_mode), account_id: int = Depends(require_account), user: str = Depends(require_user)):
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
def api_gateway_reconnect(mode: str = Depends(require_mode), account_id: int = Depends(require_account), user: str = Depends(require_user)):
    try:
        gateway_control.reconnect_gateway(mode)
    except gateway_control.GatewayControlError as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    db.log_decision(account_id, mode, "dashboard_control", user=user, action="gateway_reconnect")
    return {"ok": True}


@app.post("/api/gateway/resume_engine")
def api_gateway_resume_engine(mode: str = Depends(require_mode), account_id: int = Depends(require_account), user: str = Depends(require_user)):
    try:
        gateway_control.resume_engine(mode)
    except gateway_control.GatewayControlError as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    db.log_decision(account_id, mode, "dashboard_control", user=user, action="gateway_resume_engine")
    return {"ok": True}


@app.get("/api/positions")
def api_positions(mode: str = Depends(require_mode), account_id: int = Depends(require_account), user: str = Depends(require_user)):
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
async def api_modify_stop(symbol: str, request: Request, mode: str = Depends(require_mode), account_id: int = Depends(require_account), user: str = Depends(require_user)):
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
def api_candles(symbol: str, interval: str = "5m", mode: str = Depends(require_mode), account_id: int = Depends(require_account), user: str = Depends(require_user)):
    """Candles for the dashboard's chart modal at the requested interval
    (one of cycle.CHART_INTERVAL_PERIODS) - pure yfinance, no IBKR
    connection needed (mode/account_id are only here so the endpoint follows
    the same auth/scoping shape as everything else)."""
    if interval not in cycle.CHART_INTERVAL_PERIODS:
        raise HTTPException(status_code=400, detail=f"interval must be one of {sorted(cycle.CHART_INTERVAL_PERIODS)}")
    bars = cycle.get_chart_bars(symbol.strip().upper(), interval)
    if bars is None:
        return {"candles": []}
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
    return {"candles": candles}


# Opting a LIVE position out of today's automatic EOD close is real overnight
# gap risk taken on deliberately, so it gets the same speed bump as other
# LIVE-affecting actions; turning it back off just restores the normal safe
# behavior, so that direction needs no confirmation.
HOLD_OVERNIGHT_LIVE_CONFIRM_PHRASE = "ok"


@app.put("/api/positions/{symbol}/hold_overnight")
async def api_set_hold_overnight(symbol: str, request: Request, mode: str = Depends(require_mode), account_id: int = Depends(require_account), user: str = Depends(require_user)):
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


@app.get("/api/broker_positions")
def api_broker_positions(mode: str = Depends(require_mode), account_id: int = Depends(require_account), user: str = Depends(require_user)):
    """Every real IBKR holding in this mode's account, independent of
    whether the bot opened it or is tracking it — refreshed every 5 minutes
    by cycle.refresh_account_info alongside the account summary."""
    data = db.get_broker_positions(account_id, mode)
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
    return data


@app.post("/api/broker_positions/close")
async def api_broker_positions_close(request: Request, mode: str = Depends(require_mode), account_id: int = Depends(require_account), user: str = Depends(require_user)):
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


@app.get("/api/trades")
def api_trades(limit: int = 100, mode: str = Depends(require_mode), account_id: int = Depends(require_account), user: str = Depends(require_user)):
    return db.get_trades(account_id, mode, limit=limit)


@app.get("/api/performance")
def api_performance(mode: str = Depends(require_mode), account_id: int = Depends(require_account), user: str = Depends(require_user)):
    rows = db.get_trades(account_id, mode, limit=5000, today_only=True)
    pairs = perf.pair_trades(rows)
    aggregates = perf.aggregate(pairs)
    r_values = perf.compute_r_multiples(pairs)
    return {
        "aggregates": aggregates,
        "histogram": [{"label": l, "count": c, "is_loss": loss} for l, c, loss in perf.histogram(r_values)],
    }


@app.get("/api/watchlist")
def api_watchlist(mode: str = Depends(require_mode), account_id: int = Depends(require_account), user: str = Depends(require_user)):
    return db.get_watchlist(account_id, mode)


@app.get("/api/watchlist_filters")
def api_watchlist_filters(mode: str = Depends(require_mode), account_id: int = Depends(require_account), user: str = Depends(require_user)):
    return db.get_watchlist_filters(account_id, mode)


@app.post("/api/prefilter/run")
def api_run_prefilter(account_id: int = Depends(require_account), user: str = Depends(require_user)):
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
def api_decision_log(limit: int = 100, mode: str = Depends(require_mode), account_id: int = Depends(require_account), user: str = Depends(require_user)):
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
    if not name or not isinstance(rules, dict):
        raise HTTPException(status_code=400, detail="name and rules are required")
    if direction not in db.DIRECTIONS:
        raise HTTPException(status_code=400, detail=f"direction must be one of {db.DIRECTIONS}")
    if risk_rating not in db.RISK_RATINGS:
        raise HTTPException(status_code=400, detail=f"risk_rating must be one of {db.RISK_RATINGS}")
    strategy_id = db.create_strategy(name, rules, direction, risk_rating)
    _log_account_action(account_id, user, action="create_strategy", name=name, direction=direction, risk_rating=risk_rating)
    return {"id": strategy_id}


@app.put("/api/strategies/{strategy_id}")
async def api_update_strategy(strategy_id: int, request: Request, account_id: int = Depends(require_account), user: str = Depends(require_admin)):
    if not db.get_strategy(strategy_id):
        raise HTTPException(status_code=404, detail="Strategy not found")
    body = await request.json()
    rules = body.get("rules")
    risk_rating = body.get("risk_rating")
    if not isinstance(rules, dict):
        raise HTTPException(status_code=400, detail="rules is required")
    if risk_rating is not None and risk_rating not in db.RISK_RATINGS:
        raise HTTPException(status_code=400, detail=f"risk_rating must be one of {db.RISK_RATINGS}")
    db.update_strategy(strategy_id, rules, risk_rating)
    _log_account_action(account_id, user, action="update_strategy", strategy_id=strategy_id)
    return {"ok": True}


# Strategies are shared across paper AND live — activating one takes effect
# on the live engine immediately too. A typed confirmation is required only
# for the highest tier (aggressive), matching the same speed-bump pattern
# used for editing LIVE risk sizing.
ACTIVATE_AGGRESSIVE_CONFIRM_PHRASE = "ok"


@app.post("/api/strategies/{strategy_id}/activate")
async def api_activate_strategy(strategy_id: int, request: Request, account_id: int = Depends(require_account), user: str = Depends(require_user)):
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


@app.delete("/api/strategies/{strategy_id}")
def api_delete_strategy(strategy_id: int, account_id: int = Depends(require_account), user: str = Depends(require_admin)):
    try:
        db.delete_strategy(strategy_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    _log_account_action(account_id, user, action="delete_strategy", strategy_id=strategy_id)
    return {"ok": True}
