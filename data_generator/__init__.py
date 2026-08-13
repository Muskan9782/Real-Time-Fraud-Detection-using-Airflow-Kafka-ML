"""Synthetic data generation for the real-time fraud detection & risk engine.
"""

from .config import DEFAULTS, load_config
from .pipeline import generate_phase1

__all__ = ["DEFAULTS", "load_config", "generate_phase1"]
