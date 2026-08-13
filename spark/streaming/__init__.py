"""Structured Streaming jobs (Phases 4-17).

Phase 4 - DONE: Kafka -> Spark console consumer (explicit schema + checkpoint).
Phase 5 - DONE: tumbling (2 min) + sliding (2 min / 30 s) windows, count/sum/avg.
Phase 6 - DONE: watermarks (append mode) + late events.
Phase 7 - DONE: velocity detector (>5 tx / 2 min, append mode, per-window).
Phase 8 - DONE: stateful impossible travel (transformWithState + RocksDB,
        update mode, per-customer previous-event state).
Phase 9 - DONE: stream-stream join (logins x transactions, watermarks +
        append mode, finalization-on-watermark-advance).
Phase 10 - DONE: card-testing detector (failed payments / 60 s window,
        append mode, per-window semantics).
Phase 11 - DONE: historical anomaly (7d/30d rolling averages, >5x,
        transformWithState + ValueState history blob; ListState crashes on
        PySpark 4.2.0 task-closure deserialization).
Phase 12 - DONE: risk engine (ALERTS_SCHEMA envelopes -> per-(customer, window)
        total points + LOW/MEDIUM/HIGH/CRITICAL, append mode).
Phase 14 - DONE: streaming ML inference (foreachBatch driver-side scoring with
        the Phase 13 XGBoost model; transformWithState's Python state server
        crashes on PySpark 4.2.0 - the Phase 11 bug family, not ListState-only).
Phase 15 - DONE: Delta Bronze/Silver/Gold (spark/batch/).
Phase 16 - DONE: reliability - malformed records quarantined to dead_letter
        (raw key+value preserved, never crashes), withWatermark +
        dropDuplicatesWithinWatermark dedup by transaction_id, late events
        dropped deterministically by the watermark.
Phase 17 - DONE: checkpoint/state recovery - the same stateful job stopped
        after lifecycle A and restarted on the same checkpoint (fresh
        driver): offsets resume exactly, batch ids continue, RocksDB dedup
        state + the watermark are restored (no data loss, no re-read).
"""

from __future__ import annotations
