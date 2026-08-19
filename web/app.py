"""Dashboard web app: login, bot start/stop/emergency-flatten controls, live
positions/trades/performance views, and the multi-strategy switcher — for
BOTH the paper and live engines at once, selected per-request via a `mode`
query param (the frontend has a Paper/Live tab). Reads and writes the same
SQLite DB the two trading services (run_service.py --mode paper/live) use;
it never talks to IBKR directly, so it can safely run as a separate process.
"""
import json
from pathlib import Path

from fastapi import Depends, FastAPI, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

import cycle
from src import db, perf
from web.auth import COOKIE_NAME, make_session_cookie, read_session, require_user

PROJECT_DIR = Path(__file__).resolve().parent.parent
templates = Jinja2Templates(directory=str(Path(__file__).resolve().parent / "templates"))

app = FastAPI(title="TradingBot Dashboard")


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
    if not name or not isinstance(rules, dict):
        raise HTTPException(status_code=400, detail="name and rules are required")
    strategy_id = db.create_strategy(name, rules)
    _log_strategy_action(user, action="create_strategy", name=name)
    return {"id": strategy_id}


@app.put("/api/strategies/{strategy_id}")
async def api_update_strategy(strategy_id: int, request: Request, user: str = Depends(require_user)):
    if not db.get_strategy(strategy_id):
        raise HTTPException(status_code=404, detail="Strategy not found")
    body = await request.json()
    rules = body.get("rules")
    if not isinstance(rules, dict):
        raise HTTPException(status_code=400, detail="rules is required")
    db.update_strategy(strategy_id, rules)
    _log_strategy_action(user, action="update_strategy", strategy_id=strategy_id)
    return {"ok": True}


@app.post("/api/strategies/{strategy_id}/activate")
def api_activate_strategy(strategy_id: int, user: str = Depends(require_user)):
    if not db.get_strategy(strategy_id):
        raise HTTPException(status_code=404, detail="Strategy not found")
    db.activate_strategy(strategy_id)
    _log_strategy_action(user, action="activate_strategy", strategy_id=strategy_id)
    return {"ok": True}


@app.delete("/api/strategies/{strategy_id}")
def api_delete_strategy(strategy_id: int, user: str = Depends(require_user)):
    try:
        db.delete_strategy(strategy_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    _log_strategy_action(user, action="delete_strategy", strategy_id=strategy_id)
    return {"ok": True}
