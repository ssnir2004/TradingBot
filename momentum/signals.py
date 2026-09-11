"""The Signal object every detector returns, plus the shared Exit/Risk
math (G7-G12) that turns a raw entry level into stop + scale targets +
a size hint. In phase 1 nothing is *managed* - these numbers are computed,
logged and alerted so we can judge them against real outcomes later.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from momentum import store

ET = ZoneInfo("America/New_York")


@dataclass
class Signal:
    strategy: str                 # "A" | "B" | "C" | "D"
    symbol: str
    price: float                  # last price at fire time
    entry_ref: float              # the break / trigger level
    timeframe: str = "5m"
    conviction: str = "normal"    # "normal" | "low"
    above_preferred_range: bool = False
    features: dict = field(default_factory=dict)
    # filled by finalize()
    stop_price: float = 0.0
    max_loss_price: float = 0.0
    first_scale_price: float = 0.0
    second_scale_price: float = 0.0
    shares_hint: int = 0

    def finalize(self, cfg: dict, equity_usd: float | None) -> "Signal":
        ex = cfg["exit"]
        risk = cfg["risk"]
        stop_dollars = ex["stop_cents"] / 100.0
        max_loss_dollars = ex["max_loss_cents"] / 100.0

        if ex["stop_mode"] == "pct":
            stop_dollars = self.entry_ref * (ex["stop_cents"] / 100.0)  # reuse cents field as pct
        # (atr mode is a phase-3 concern; fixed_cents is the agreed default)

        self.stop_price = round(self.entry_ref - stop_dollars, 4)
        self.max_loss_price = round(self.entry_ref - max_loss_dollars, 4)
        r = self.entry_ref - self.stop_price
        self.first_scale_price = round(self.entry_ref + ex["first_scale_r"] * r, 4)
        self.second_scale_price = round(self.entry_ref + ex["second_scale_r"] * r, 4)

        if equity_usd and r > 0:
            risk_usd = equity_usd * (risk["per_trade_pct"] / 100.0)
            shares = int(risk_usd / r)
            notional_cap = int(risk["max_position_notional_usd"] / max(self.entry_ref, 0.01))
            self.shares_hint = max(0, min(shares, notional_cap))
        return self

    def to_store_row(self, mode: str = "alert") -> dict:
        now = datetime.now(ET)
        return {
            "signal_iso": now.isoformat(timespec="seconds"),
            "trade_date": now.date().isoformat(),
            "strategy": self.strategy,
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "price": self.price,
            "entry_ref": self.entry_ref,
            "stop_price": self.stop_price,
            "max_loss_price": self.max_loss_price,
            "first_scale_price": self.first_scale_price,
            "second_scale_price": self.second_scale_price,
            "shares_hint": self.shares_hint,
            "conviction": self.conviction,
            "above_preferred_range": int(self.above_preferred_range),
            "features_json": self.features,
            "mode": mode,
        }


def cooldown_ok(sig: Signal, cfg: dict) -> tuple[bool, str]:
    """G15: no re-fire for the same symbol+strategy within
    signal_cooldown_min, and no more than max_signals_per_symbol_per_day
    across all strategies."""
    p = cfg["patterns"]
    today = datetime.now(ET).date().isoformat()

    todays = store.signals_today(sig.symbol, today)
    if len(todays) >= p["max_signals_per_symbol_per_day"]:
        return False, f"{sig.symbol}: {len(todays)} signals today >= cap"

    last = store.last_signal_at(sig.symbol, sig.strategy)
    if last is not None:
        age_min = (datetime.now(ET) - last).total_seconds() / 60.0
        if age_min < p["signal_cooldown_min"]:
            return False, f"{sig.symbol}/{sig.strategy}: {age_min:.0f}m < {p['signal_cooldown_min']}m cooldown"
    return True, ""
