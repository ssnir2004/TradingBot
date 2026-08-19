"""Dashboard web app: login, bot start/stop/emergency-flatten controls, live
positions/trades/performance views, and the multi-strategy switcher — for
BOTH the paper and live engines at once, selected per-request via a `mode`
query param (the frontend has a Paper/Live tab). Reads and writes the same
SQLite DB the two trading services (run_service.py --mode paper/live) use;
it never talks to IBKR directly, so it can safely run as a separate process.
"""
import json
from pathlib import Path

from dotenv import dotenv_values
from fastapi import Depends, FastAPI, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

import cycle
from src import db, mode_config, perf
from web.auth import COOKIE_NAME, make_session_cookie, read_session, require_user

PROJECT_DIR = Path(__file__).resolve().parent.parent
templates = Jinja2Templates(directory=str(Path(__file__).resolve().parent / "templates"))

app = FastAPI(title="TradingBot Dashboard")

# Typed into the confirmation modal before any LIVE risk-sizing value can be
# changed from the dashboard — a deliberate speed bump since these numbers
# directly control how much real money a single live order can risk.
LIVE_RISK_CONFIRM_PHRASE = "CHANGE LIVE RISK"


def _env() -> dict:
    return dotenv_values(PROJECT_DIR / ".env")


@app.on_event("startup")
def on_startup():
    db.init_db(seed_rules_path=PROJECT_DIR / "rules.json")


def require_mode(mode: str = Query(...)) -> str:
    if mode not in db.MODES:
        raise HTTPException(status_code=400, detail=f"mode must be one of {db.MODES}")
    return mode


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
    db.create_user(username, password)
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
@app.get("/api/status")
def api_status(mode: str = Depends(require_mode), user: str = Depends(require_user)):
    return db.get_cycle_status(mode)


@app.post("/api/control/enable")
def api_enable(mode: str = Depends(require_mode), user: str = Depends(require_user)):
    db.set_bot_enabled(mode, True)
    db.log_decision(mode, "dashboard_control", action="enable", user=user)
    return {"bot_enabled": True}


@app.post("/api/control/disable")
def api_disable(mode: str = Depends(require_mode), user: str = Depends(require_user)):
    db.set_bot_enabled(mode, False)
    db.log_decision(mode, "dashboard_control", action="disable", user=user)
    return {"bot_enabled": False}


@app.post("/api/control/flatten")
def api_flatten(mode: str = Depends(require_mode), user: str = Depends(require_user)):
    db.request_flatten_now(mode)
    db.log_decision(mode, "dashboard_control", action="flatten_now", user=user)
    return {"flatten_pending": True}


@app.get("/api/account")
def api_account(mode: str = Depends(require_mode), user: str = Depends(require_user)):
    return db.get_account_info(mode)


@app.get("/api/risk_params")
def api_get_risk_params(mode: str = Depends(require_mode), user: str = Depends(require_user)):
    return mode_config.risk_params(_env(), mode)


@app.post("/api/risk_params")
async def api_set_risk_params(request: Request, mode: str = Depends(require_mode), user: str = Depends(require_user)):
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
        db.set_setting(f"{mode}:risk:{key}", str(value))
    db.log_decision(mode, "dashboard_control", user=user, action="update_risk_params",
                     **{k: str(v) for k, v in updates.items()})
    return mode_config.risk_params(_env(), mode)


@app.get("/api/positions")
def api_positions(mode: str = Depends(require_mode), user: str = Depends(require_user)):
    positions = db.get_open_positions(mode)
    for pos in positions:
        price = None
        try:
            price = cycle._current_price(pos["symbol"])
        except Exception:
            pass
        risk_per_share = pos["entry_price"] - pos["initial_stop"]
        pos["current_price"] = price
        pos["unrealized_r"] = (
            (price - pos["entry_price"]) / risk_per_share
            if price is not None and risk_per_share > 0 else None
        )
    return positions


@app.get("/api/trades")
def api_trades(limit: int = 100, mode: str = Depends(require_mode), user: str = Depends(require_user)):
    return db.get_trades(mode, limit=limit)


@app.get("/api/performance")
def api_performance(mode: str = Depends(require_mode), user: str = Depends(require_user)):
    rows = db.get_trades(mode, limit=5000, today_only=True)
    pairs = perf.pair_trades(rows)
    aggregates = perf.aggregate(pairs)
    r_values = perf.compute_r_multiples(pairs)
    return {
        "aggregates": aggregates,
        "histogram": [{"label": l, "count": c, "is_loss": loss} for l, c, loss in perf.histogram(r_values)],
    }


@app.get("/api/watchlist")
def api_watchlist(mode: str = Depends(require_mode), user: str = Depends(require_user)):
    return db.get_watchlist(mode)


@app.get("/api/watchlist_filters")
def api_watchlist_filters(mode: str = Depends(require_mode), user: str = Depends(require_user)):
    return db.get_watchlist_filters(mode)


@app.get("/api/decision_log")
def api_decision_log(limit: int = 100, mode: str = Depends(require_mode), user: str = Depends(require_user)):
    rows = db.get_decision_log(mode, limit=limit)
    for r in rows:
        try:
            r["payload"] = json.loads(r["payload_json"])
        except (json.JSONDecodeError, TypeError):
            r["payload"] = {}
    return rows


# ------------------------------------------------------------- strategies ---
# Strategies are shared across both modes — no `mode` param here. Actions
# are logged into both modes' activity logs so either tab shows them.
def _log_strategy_action(user: str, **fields):
    for m in db.MODES:
        db.log_decision(m, "dashboard_control", user=user, **fields)


@app.get("/api/strategies")
def api_list_strategies(user: str = Depends(require_user)):
    return db.list_strategies()


@app.get("/api/strategies/{strategy_id}")
def api_get_strategy(strategy_id: int, user: str = Depends(require_user)):
    strategy = db.get_strategy(strategy_id)
    if not strategy:
        raise HTTPException(status_code=404, detail="Strategy not found")
    strategy["rules"] = json.loads(strategy["rules_json"])
    return strategy


@app.post("/api/strategies")
async def api_create_strategy(request: Request, user: str = Depends(require_user)):
    body = await request.json()
    name = body.get("name")
    rules = body.get("rules")
    risk_rating = body.get("risk_rating", "moderate")
    if not name or not isinstance(rules, dict):
        raise HTTPException(status_code=400, detail="name and rules are required")
    if risk_rating not in db.RISK_RATINGS:
        raise HTTPException(status_code=400, detail=f"risk_rating must be one of {db.RISK_RATINGS}")
    strategy_id = db.create_strategy(name, rules, risk_rating)
    _log_strategy_action(user, action="create_strategy", name=name, risk_rating=risk_rating)
    return {"id": strategy_id}


@app.put("/api/strategies/{strategy_id}")
async def api_update_strategy(strategy_id: int, request: Request, user: str = Depends(require_user)):
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
    _log_strategy_action(user, action="update_strategy", strategy_id=strategy_id)
    return {"ok": True}


# Strategies are shared across paper AND live — activating one takes effect
# on the live engine immediately too. A typed confirmation is required only
# for the highest tier (aggressive), matching the same speed-bump pattern
# used for editing LIVE risk sizing.
ACTIVATE_AGGRESSIVE_CONFIRM_PHRASE = "ACTIVATE AGGRESSIVE"


@app.post("/api/strategies/{strategy_id}/activate")
async def api_activate_strategy(strategy_id: int, request: Request, user: str = Depends(require_user)):
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
    db.activate_strategy(strategy_id)
    _log_strategy_action(user, action="activate_strategy", strategy_id=strategy_id, name=strategy["name"])
    return {"ok": True}


@app.delete("/api/strategies/{strategy_id}")
def api_delete_strategy(strategy_id: int, user: str = Depends(require_user)):
    try:
        db.delete_strategy(strategy_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    _log_strategy_action(user, action="delete_strategy", strategy_id=strategy_id)
    return {"ok": True}
