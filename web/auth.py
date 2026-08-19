"""Signed-cookie session auth. No dependency on a session store — the cookie
itself carries the username, signed with SESSION_SECRET so it can't be
forged, and expires after MAX_AGE_SECONDS.
"""
import os
from pathlib import Path

from dotenv import dotenv_values
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer
from fastapi import HTTPException, Request

PROJECT_DIR = Path(__file__).resolve().parent.parent
_env = dotenv_values(PROJECT_DIR / ".env")
SECRET = _env.get("SESSION_SECRET") or os.environ.get("SESSION_SECRET")
if not SECRET:
    raise RuntimeError(
        "SESSION_SECRET is not set. Add a long random value to .env before "
        "running the dashboard (e.g. `python -c \"import secrets; print(secrets.token_hex(32))\"`)."
    )

COOKIE_NAME = "tb_session"
MAX_AGE_SECONDS = 7 * 24 * 60 * 60

_serializer = URLSafeTimedSerializer(SECRET)


def make_session_cookie(username: str) -> str:
    return _serializer.dumps({"username": username})


def read_session(request: Request) -> str | None:
    cookie = request.cookies.get(COOKIE_NAME)
    if not cookie:
        return None
    try:
        data = _serializer.loads(cookie, max_age=MAX_AGE_SECONDS)
    except (BadSignature, SignatureExpired):
        return None
    return data.get("username")


def require_user(request: Request) -> str:
    """FastAPI dependency for API routes: 401s if not logged in."""
    username = read_session(request)
    if not username:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return username
