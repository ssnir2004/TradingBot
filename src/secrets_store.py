"""Encrypts/decrypts each account's own IBKR credentials at rest, so a DB
file (or a backup of it) never holds a plaintext broker password — only
db.py's account_ibkr_credentials table and web/app.py's credentials
endpoints touch this. Symmetric (Fernet/AES-128-CBC + HMAC) since the same
process that encrypts also has to decrypt it later, to hand the plaintext
to IBC at Gateway login time — there's no way around IBC needing a
plaintext config.ini to log in with, only around who has to see or handle
that password to get it there.

The key is loaded lazily (only when encrypt/decrypt is actually called),
not at import time — this module is imported by web/app.py alongside
everything else the dashboard serves, so a deploy that hasn't set
CREDENTIALS_ENCRYPTION_KEY yet must not crash the whole dashboard, only
the (currently unused) credentials feature.
"""
from functools import lru_cache
from pathlib import Path

from cryptography.fernet import Fernet, InvalidToken
from dotenv import dotenv_values

PROJECT_DIR = Path(__file__).resolve().parent.parent


@lru_cache(maxsize=1)
def _get_fernet() -> Fernet:
    env = dotenv_values(PROJECT_DIR / ".env")
    key = env.get("CREDENTIALS_ENCRYPTION_KEY")
    if not key:
        raise RuntimeError(
            "CREDENTIALS_ENCRYPTION_KEY is not set. Add one to .env before storing IBKR "
            "credentials (e.g. `python -c \"from cryptography.fernet import Fernet; "
            "print(Fernet.generate_key().decode())\"`)."
        )
    return Fernet(key.encode("utf-8"))


def encrypt(plaintext: str) -> bytes:
    return _get_fernet().encrypt(plaintext.encode("utf-8"))


def decrypt(ciphertext: bytes) -> str:
    try:
        return _get_fernet().decrypt(ciphertext).decode("utf-8")
    except InvalidToken as exc:
        raise ValueError("Could not decrypt — CREDENTIALS_ENCRYPTION_KEY may have changed.") from exc
