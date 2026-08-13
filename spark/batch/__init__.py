"""Batch PySpark jobs.

The Bronze/Silver/Gold medallion - ``bronze.py`` ingests the
raw files into Delta, ``silver.py`` deduplicates / conforms / enriches
(labels, reference data, Phase 13 model probabilities), ``gold.py`` builds
analytics-ready aggregates. Run with ``python run_phase15.py``.
"""

from __future__ import annotations
