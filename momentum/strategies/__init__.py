"""Detector registry. `enabled_detectors(cfg)` returns the Detector
instances whose strategy_X.enabled flag is set - phase 1 runs all four,
alert-only."""
from momentum.strategies.base import DetectContext, Detector
from momentum.strategies.bull_flag import BullFlagFlatTop
from momentum.strategies.ma_pullback import MAPullback9ema
from momentum.strategies.setup_1234 import Setup1234
from momentum.strategies.whole_half_dollar import WholeHalfDollarBreak

ALL_DETECTORS: dict[str, Detector] = {
    "A": WholeHalfDollarBreak(),
    "B": BullFlagFlatTop(),
    "C": MAPullback9ema(),
    "D": Setup1234(),
}


def enabled_detectors(cfg: dict) -> list[Detector]:
    return [d for k, d in ALL_DETECTORS.items()
            if cfg.get(f"strategy_{k}", {}).get("enabled", False)]


__all__ = ["DetectContext", "Detector", "ALL_DETECTORS", "enabled_detectors"]
