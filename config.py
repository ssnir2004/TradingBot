"""Central configuration loaded from environment variables (.env)."""
import os
from dotenv import load_dotenv

load_dotenv()


def _bool(name: str, default: bool) -> bool:
    return os.getenv(name, str(default)).strip().lower() in ("1", "true", "yes", "on")


def _float(name: str, default: float) -> float:
    return float(os.getenv(name, default))


def _int(name: str, default: int) -> int:
    return int(os.getenv(name, default))


# ---- IBKR connection ----
IBKR_HOST = os.getenv("IBKR_HOST", "127.0.0.1")
IBKR_PORT = _int("IBKR_PORT", 7497)
IBKR_CLIENT_ID = _int("IBKR_CLIENT_ID", 17)

LIVE_PORTS = (7496, 4001)
PAPER_PORTS = (7497, 4002)

ALLOW_LIVE_TRADING = _bool("ALLOW_LIVE_TRADING", False)
DRY_RUN = _bool("DRY_RUN", True)

if IBKR_PORT in LIVE_PORTS and not ALLOW_LIVE_TRADING:
    raise SystemExit(
        f"Refusing to start: IBKR_PORT={IBKR_PORT} is a LIVE trading port but "
        "ALLOW_LIVE_TRADING is not 'true'. Set IBKR_PORT to a paper port "
        f"({PAPER_PORTS}) or explicitly opt in to live trading in .env."
    )

IS_PAPER_ACCOUNT = IBKR_PORT in PAPER_PORTS

# ---- Telegram ----
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# ---- Risk / money management ----
RISK_PER_TRADE_PCT = _float("RISK_PER_TRADE_PCT", 1.0)
MAX_CONCURRENT_POSITIONS = _int("MAX_CONCURRENT_POSITIONS", 3)
MAX_DAILY_LOSS_PCT = _float("MAX_DAILY_LOSS_PCT", 3.0)
PARTIAL_PROFIT_R_MULTIPLE = _float("PARTIAL_PROFIT_R_MULTIPLE", 1.0)
PARTIAL_PROFIT_SIZE_PCT = _float("PARTIAL_PROFIT_SIZE_PCT", 50)
TRAILING_STOP_ATR_MULT = _float("TRAILING_STOP_ATR_MULT", 1.5)

# ---- Scanner filters ----
MIN_PRICE = _float("MIN_PRICE", 5)
MAX_PRICE = _float("MAX_PRICE", 100)
MIN_GAP_PCT = _float("MIN_GAP_PCT", 2.0)
MIN_RELATIVE_VOLUME = _float("MIN_RELATIVE_VOLUME", 2.0)
MIN_AVG_DOLLAR_VOLUME = _float("MIN_AVG_DOLLAR_VOLUME", 5_000_000)
SCAN_UNIVERSE = [s.strip().upper() for s in os.getenv(
    "SCAN_UNIVERSE", "AAPL,MSFT,NVDA,AMD,TSLA,AMZN,META,GOOGL"
).split(",") if s.strip()]

# ---- Schedule (US/Eastern, "HH:MM") ----
PREMARKET_SCAN_TIME = os.getenv("PREMARKET_SCAN_TIME", "08:00")
MARKET_OPEN_TIME = os.getenv("MARKET_OPEN_TIME", "09:30")
MARKET_CLOSE_TIME = os.getenv("MARKET_CLOSE_TIME", "16:00")
FORCE_CLOSE_TIME = os.getenv("FORCE_CLOSE_TIME", "15:55")

TIMEZONE = "America/New_York"
