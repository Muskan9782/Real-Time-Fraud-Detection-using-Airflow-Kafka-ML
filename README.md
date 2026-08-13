# Real-Time Fraud Detection & Risk Engine

Event-driven fraud platform: Python generators -> Kafka -> PySpark Structured
Streaming -> Rules + XGBoost risk engine -> `fraud_alerts` + Delta Lake,
with Airflow for batch jobs (training / quality / backfills).

This is the **long-term working document**. Build locally first, then move
selected components to GCP. Do **not** start with cloud infrastructure.

```
    PYTHON EVENT GENERATORS
             |
             v
           KAFKA
    /       |       \
   v        v        v
transactions  logins  payments  customer_locations
             |
             v
  SPARK STRUCTURED STREAMING
      windows / watermarks
      stateful processing
      stream-stream joins
      feature engineering
             |
      +------+------+
      |             |
    Rules       XGBoost
      |             |
      +------+------+
             |
         Risk Engine
             |
     +------+------+
     |             |
 fraud_alerts   Delta Lake -> GCS -> Power BI

 AIRFLOW (separate): training / data quality / backfills
```

## Status

| Phase | Milestone | Status |
|-------|-----------|--------|
| 1 | Python generators produce normal + fraudulent events | **DONE** |
| 2 | Docker + Kafka, 1,000 events Python -> Kafka -> consumer | **DONE** |
| 3 | Topics, partitions, keys, offsets, consumer groups | **DONE** |
| 4 | Kafka -> PySpark streaming (explicit schema + checkpoint) | **DONE** |
| 5 | Tumbling + sliding windows (2 min / 30 s slide) | **DONE** |
| 6 | Watermarks + late events | **DONE** |
| 7 | Velocity detector (>5 tx / 2 min) | **DONE** |
| 8 | Stateful impossible travel (Haversine speed) | **DONE** |
| 9 | Stream-stream join: login -> transaction (5 min) | **DONE** |
| 10 | Card-testing detector (>10 failed payments / 60 s) | **DONE** |
| 11 | Historical anomaly (7d / 30d averages, >5x) | **DONE** |
| 12 | Risk engine: points -> LOW/MEDIUM/HIGH/CRITICAL | **DONE** |
| 13 | XGBoost offline model + evaluation | **DONE** |
| 14 | Streaming ML inference | **DONE** |
| 15 | Delta Lake Bronze/Silver/Gold | **DONE** |
| 16 | Reliability: dead-letter, dedup, late events | **DONE** |
| 17 | Checkpoint/state recovery | **DONE** |
| 18 | Airflow DAGs (training / quality / backfill) | **DONE** |
| 19 | Power BI dashboard | **DONE** |
| 20 | GCP deployment + observability | **DONE** |

## Repository layout

```
├── README.md               this file
├── requirements.txt        dependencies (Phase 1 is stdlib-only)
├── docker-compose.yml      Phase 2: Kafka + Phase 18: Airflow
├── Dockerfile.airflow      Phase 18: Airflow image (+ pyspark/delta/xgboost)
├── config/phase1.json      Phase 1 run configuration
├── data_generator/         Phase 1: synthetic event generation
│   ├── customer_generator.py
│   ├── merchant_generator.py
│   ├── transaction_generator.py
│   ├── login_generator.py
│   ├── payment_generator.py
│   ├── location_generator.py
│   └── fraud_scenarios.py  injects the five fraud behaviors
├── kafka/                  Phase 2/3: topics + producers + consumer
│   ├── config.py           bootstrap servers, topic partitions, event routes
│   ├── common.py           producer/consumer factories (confluent-kafka)
│   ├── create_topics.py    creates all 6 topics
│   ├── producers.py        generic JSONL -> topic producer
│   ├── consumer.py         consume a topic and report counts/samples
│   ├── inspect.py          topics/partitions/end+committed offsets/lag
│   ├── transaction_producer.py / login / payment / location
│   └── __init__.py
├── spark/                  Phases 4-15: streaming + batch jobs
│   ├── common.py           JDK/winutils detection + SparkSession factory
│   ├── schemas/            explicit StructType for every event type
│   ├── batch/              Phase 15: Delta bronze.py / silver.py / gold.py
│   └── streaming/
│       ├── kafka_to_console.py   Phase 4 job (explicit schema -> console)
│       ├── windowed_aggregations.py  Phase 5 job (count/sum/avg in windows)
│       ├── watermarks.py         Phase 6 job (append mode + withWatermark)
│       ├── velocity.py           Phase 7 job (velocity detector, append mode)
│       ├── impossible_travel.py  Phase 8 job (stateful Haversine detector)
│       ├── login_txn_join.py     Phase 9 job (stream-stream join)
│       ├── payment_attack.py     Phase 10 job (card-testing detector)
│       ├── high_value.py         Phase 11 job (historical anomaly detector)
│       ├── risk_engine.py        Phase 12 job (alert points -> risk levels)
│       ├── ml_inference.py       Phase 14 job (online XGBoost scoring)
│       └── <later phases>        joins, ...
├── ml/                     Phase 13-14: XGBoost train/evaluate
│   ├── features.py         labeled dataset + per-event engineered features
│   ├── train.py            train XGBoost, save model + held-out test set
│   ├── evaluate.py         precision/recall/F1, ROC-AUC, PR-AUC on test set
│   └── model/              xgb_model.json, test_set.parquet, metadata/evaluation
├── dags/                   Phase 18: Airflow DAGs (training/quality/backfill)
├── sql/                    analytics queries
├── dashboard/              Phase 19 (Power BI)
├── tests/                  Phase 1 milestone tests
└── docker/                 Dockerfile / helper files
```

Generated data (gitignored) lands in `data/`.

## Phase 4 (DONE) - Kafka -> PySpark streaming

Milestone: **Spark reads the `transactions` topic from Kafka, parses each
message with an explicit schema, converts `event_time` to a timestamp, prints
to the console, and checkpoints.**

The historical milestone runner has been retired. Run the streaming job
directly when Kafka is available:

The runner proves, with numbers:

- **Backlog + live data**: Spark reads everything already on the topic, and
  picks up 10 transactions produced *while the query is running* (live).
- **Explicit schema**: `spark/schemas/__init__.py` declares `StructType` for
  every event type, matching `data_generator/schemas.py` - Spark never guesses
  field types (a guessed `amount` would be a string and `amount > 200` would
  compare lexically). Verified via a zero bad-rows check.
- **Real timestamps**: `event_time` (string `YYYY-MM-DDTHH:MM:SS`) is converted
  to a `event_ts` timestamp column with an explicit format.
- **Checkpointing**: the second run reuses the same checkpoint directory and
  consumes ~0 messages - it resumes at committed offsets (no data lost, none
  re-read). Checkpoints live under `spark/checkpoints/` (gitignored).

Running the job on its own:

```powershell
python spark/streaming/kafka_to_console.py --topic transactions --duration 30
```

Local Spark prerequisites handled by `spark/common.py`: JDK 21 (Temurin) via
`JAVA_HOME` and `winutils.exe` under `.tools/winutils` via `HADOOP_HOME`. The
first run downloads the `spark-sql-kafka` connector jar (Spark 4.2 / Scala 2.13).

## Phase 3 (DONE) - Kafka concepts

Milestone: **understand topics, partitions, keys, offsets and groups.**

The implementation is available through `kafka/inspect.py` and the Kafka
commands described below.

What it demonstrates (all against the running local Kafka):

- **Topics / partitions** - all 6 topics; the four event topics have 3
  partitions each, `fraud_alerts`/`dead_letter` have 1.
- **End offsets** - the high watermark (next offset to be written) per
  partition for `transactions`.
- **Two consumer groups** (`phase3-alpha-*`, `phase3-beta-*`) - each reads the
  whole topic independently; offsets are tracked **per group**.
- **Offsets advance** - after 5 new transactions are produced, both groups
  consume exactly the 5 new messages and never replay old ones.
- **Offsets + lag** - committed offset per partition and lag (= end - committed)
  reported for each group via `kafka/inspect.py`.
- **Keys** - `customer_id` keys: 10 transactions for C001 all land in the same
  partition; across 199 sampled customers every key maps to exactly one
  partition (Kafka guarantees ordering within a partition per key, not
  globally).

## Phase 2 (DONE) - Docker + Kafka

Milestone: **Python -> Kafka -> Python consumer; send and consume 1,000 events.**

```powershell
# 1. Start Kafka (Docker Desktop must be running)
docker compose up -d                 # wait until `docker compose ps` shows healthy

# 2. Create the six topics (transactions has 3 partitions)
python -m kafka.create_topics

# 3. Produce all Phase 1 events (keyed by customer_id) and consume them back
# The current producer/consumer commands are listed below.

# ad-hoc checks
python -m kafka.transaction_producer --limit 100     # smoke test
python -m kafka.consumer --topic transactions        # read everything back
```

Observed run: 1,111 transactions / 610 logins / 549 payments / 790 locations
all round-tripped (sent == consumed), spread across the 3 partitions of each
topic. `transactions` uses 3 partitions; the message key is `customer_id`.

Client: `confluent-kafka` (imported as `confluent_kafka`) -- chosen over
`kafka-python` because this project's own `kafka/` package would shadow that
module name. Stop/wipe Kafka with `docker compose down` / `docker compose down -v`.

## Phase 1 (DONE) - Python generators

Milestone: **Python generates valid normal and fraudulent events.**

### Run

```powershell
# from E:\FraudDetection
python -m unittest discover -s tests -v   # 15 milestone tests
```

Generator options are documented in `data_generator/` and `config/phase1.json`.
Config overrides live in `config/phase1.json`.

### Output

```
data/
├── raw/
│   ├── customers.csv           customer reference data
│   ├── merchants.csv           merchant reference data
│   ├── transactions.jsonl      normal + injected transactions
│   ├── logins.jsonl
│   ├── payments.jsonl
│   └── customer_locations.jsonl
└── labels/
    ├── labels.jsonl            event_id -> injected fraud scenario
    └── summary.json            counts + seed (manifest)
```

Raw event files are **clean** (no label leakage). Labels are stored in a
sidecar so later ML training can use them while the streaming pipeline stays
label-free, exactly like real production feeds.

### Injected fraud scenarios

| Scenario | Alert type | Rule | Points |
|----------|-----------|------|--------|
| Velocity | `HIGH_TRANSACTION_VELOCITY` | >5 transactions / 2 min | +25 |
| Impossible travel | `IMPOSSIBLE_TRAVEL` | implied speed > 800 km/h | +30 |
| Login + transaction | `LOGIN_TRANSACTION_CORRELATION` | tx within 5 min of login | +20 |
| Payment attack | `CARD_TESTING_ATTACK` | >10 failed payments / 60 s | +30 |
| High-value anomaly | `HIGH_VALUE_ANOMALY` | amount > 5x historical avg | +25 |

Labels are **behavior-driven**: the generator injects the behavior (e.g. a
burst of 6-8 transactions in 2 minutes), it never randomly flips rows.

### Data contracts

- Customer: `customer_id, age, country, currency, avg_transaction, home_city, home_lat, home_lon`
- Transaction: `transaction_id, customer_id, event_time, amount, currency, merchant_id, payment_method, location, lat, lon, device_id, status`
- Event times use `YYYY-MM-DDTHH:MM:SS` (UTC). Schemas are centralized in
  `data_generator/schemas.py` for reuse by PySpark (Phase 4) and Delta (Phase 15).

## Phase 5 (DONE) - Windowed aggregations

`transactions` are grouped with `window()` + `groupBy` into:

- **Tumbling** windows (2 min, non-overlapping): verified with 5 controlled
  events injected at `2026-08-13T00:00:10-00:00:50` UTC (amounts 100-500).
  Result: exactly one window `00:00:00 -> 00:02:00` with `count=5`,
  `sum=1500`, `avg=300`; `sum(tx_count)` matches the topic; all window starts
  sit on the even 2-minute grid.
- **Sliding** windows (2 min length / 30 s slide, overlapping): the same 5
  events land in the 4 overlapping windows that cover their time span
  (starts `00:00:00`, `00:00:30`, `00:01:00`, `00:01:30`), each showing
  `count=5` / `sum=1500`.

Each event sits deep inside the window so all 4 overlapping sliding windows
capture it (events at 90-110 s into a 120 s window). Verifier
The Phase 5 implementation compares **epoch seconds**, immune to display timezone.
These window features feed the velocity detector in Phase 7.

The controlled topic `phase5_controlled` is deleted/recreated every run, so
the milestone check is repeatable.

## Phase 6 (DONE) - Watermarks + late events

The Phase 5 aggregation is extended with `withWatermark("event_ts", "60
seconds")` and `append` output mode. Controlled events on a dedicated
`phase6_controlled` topic (staged in real time on a 2-min tumbling window)
verify the append-mode watermark semantics:

- **Deferred emission** - window `00:00:00 -> 00:02:00` was not emitted until
  a later batch advanced the watermark (max event time - 60 s) past its end.
- **Late events included while the window is open** - an event at `00:00:20`
  arriving after the `00:00:10-00:00:50` batch was still counted
  (`count=4, sum=1000, avg=250`).
- **Late events discarded after finalization** - events at `00:00:40` and
  `00:01:10` arriving after window `00:00:00` was emitted did not change the
  result (count stayed 4), and no second row appeared.
- **Second window finalizes later** - `00:02:00 -> 00:04:00` emitted only
  `count=1, sum=1500` (just the `00:03:30` event) after a later batch moved
  the watermark past its end; `00:04:00` stayed open (not emitted).

Key finding: in append mode, a late record is dropped when **its window has
already been finalized** (state evicted), not merely when its event time is
older than the watermark - records for still-open windows are counted so the
final emission is complete. The streaming job waits for each
produced stage to be consumed before advancing the watermark, making the
milestone deterministic.

## Phase 7 (DONE) - Velocity detector (>5 tx / 2 min)

Milestone: **a streaming job flags a customer when their transaction count in
a 2-minute event-time window exceeds 5, emitting a
`HIGH_TRANSACTION_VELOCITY` alert (25 risk points).**

The velocity detector is implemented in `spark/streaming/velocity.py`.

The job (`spark/streaming/velocity.py`) reads the `transactions` schema,
groups by `customer_id` + `window(event_ts, "120 seconds")`, counts, keeps only
windows where `count > 5`, and enriches each alert with `alert_type` +
`risk_points`. It runs in **append mode with a 120 s watermark**, so every
finalized window emits its alert exactly once (no duplicate alarms as a burst
grows).

The runner proves, with numbers (27 controlled transactions, 6 customers):

- **C700 (7 tx in one window)** -> flagged, `tx_count` 7, `amount_sum` 1400.
- **C702 (6 tx in one window, exactly threshold+1)** -> flagged, `count` 6,
  `sum` 810.
- **C701 (4 tx) / C703 (2 tx)** -> **no** alert.
- **C705 (6 tx split 3 + 3 across two windows)** -> **no** alert: velocity is
  per window, not cumulative.
- Alerts land on the 2-minute grid (epoch == window anchor), `alert_type ==
  HIGH_TRANSACTION_VELOCITY`, `risk_points == 25`; all 27 events consumed,
  exactly 2 alerts emitted.

## Phase 8 (DONE) - Stateful impossible travel (Haversine speed)

Milestone: **a streaming job keeps, per customer, the previous transaction's
location/time in state, and flags a transaction when the implied travel speed
from that previous transaction exceeds 800 km/h, emitting an
`IMPOSSIBLE_TRAVEL` alert (30 risk points).**

The stateful detector is implemented in `spark/streaming/impossible_travel.py`.

The job (`spark/streaming/impossible_travel.py`) reads the `transactions`
schema, groups by `customer_id`, and runs a `transformWithState` stateful
processor (`update` mode, `ProcessingTime`) that keeps the last event's
`(lat, lon, ts_ms)` in a `ValueState` and computes Haversine distance / time
gap / speed for each incoming event. Event times travel as epoch milliseconds
so no time zone conversion can interfere.

Spark 4.2 removed the legacy `flatMapGroupsWithState` API; the new
`transformWithState` API requires `pyarrow` + `google.protobuf` (added to
`requirements.txt`) and **only supports the RocksDB state store backend**, so
`spark/common.py` now sets
`spark.sql.streaming.stateStore.providerClass=RocksDBStateStoreProvider` (the
recommended provider for windowed aggregations too).

The runner proves, with numbers (13 controlled transactions, 6 customers,
exactly 3 alerts):

- **C800 Bangalore -> London in 5 min** -> flagged: 8035.1 km in 300 s =
  96,421.7 km/h.
- **C801 Bangalore -> Delhi in 3 h** -> **no** alert (implied ~580 km/h).
- **C802 New York -> New York in 1 min** -> **no** alert (distance 0).
- **C803 Bangalore -> Mumbai (2 h) -> London (5 min)** -> exactly **one**
  alert, for the Mumbai -> London leg (7191.7 km in 300 s = 86,300.2 km/h);
  the plausible Bangalore -> Mumbai leg correctly does not alert, proving the
  stateful chain carries the previous event, not just the first one.
- **Boundary pair** - Bangalore -> Singapore with the gap tuned to ~600 km/h
  (C805, **no** alert) vs ~1200.1 km/h (C806, alert), bracketing the 800 km/h
  threshold with a same-origin/destination pair.
- Alerts carry `alert_type == IMPOSSIBLE_TRAVEL`, `risk_points == 30`, the
  exact prev/cur coordinates, Haversine distance, gap and speed, and the
  alerting event's epoch-ms timestamp; all 13 events consumed, exactly 3
  alerts in memory.

## Phase 9 (DONE) - Stream-stream join: login -> transaction (5 min)

Milestone: **two live Kafka streams (`logins`, `transactions`) are joined *as
streams* (no batch materialization): a transaction that follows a successful
login within 5 minutes emits a `LOGIN_TRANSACTION_CORRELATION` alert (20 risk
points).**

The stream join is implemented in `spark/streaming/login_txn_join.py`.

The job (`spark/streaming/login_txn_join.py`) filters to successful logins,
watermarks both sides (`event_ts`, 300 s delay -- the join range must be at
least as large as each side's watermark delay so state can be bounded and
evicted), and inner-joins on `customer_id` with the event-time range condition
`login_ts <= tx_ts <= login_ts + 300 s`. Output is **append mode**: a matched
pair is emitted exactly once, only after both watermarks advance past the
point where that pair could still gain new matches (standard stream-stream
join finalization).

The runner proves, with numbers (14 controlled events, 7 customers, exactly 4
alert rows; all times travel as epoch milliseconds):

- **C900 login 00:00:00 + tx 00:02:00 (same device)** -> alert, gap 120 s.
- **C902 login + tx at exactly 00:05:00** -> alert (boundary is inclusive:
  gap 300 s accepted).
- **C901 login + tx at 00:05:05 (5 s past)** -> **no** alert: the 300 s window
  is a hard cutoff.
- **C903 tx with no login / C904 login with no tx** -> **no** alert: the join
  needs both sides.
- **C905 two logins (00:00:00, 00:03:00) + one tx at 00:04:00** -> **two**
  alerts: the transaction matches both logins (gap 240 s and 60 s), proving
  the join is many-to-many, not a dedup.
- **C906 *failed* login + tx** -> **no** alert (only successful logins
  correlate, matching the injected scenario).
- Each alert carries `login_id`, `login_ts_ms` / `tx_ts_ms` epoch ms, `gap`,
  `alert_type`, `risk_points`; all 14 events consumed, exactly 4 rows in
  memory.

A pair is only emitted after the watermark guarantees no more matches, so the
runner injects a high-event-time "advancer" transaction (00:15:00) that moves
the watermark past the last join window (login 00:03:00 + 300 s) and finalizes
the pairs deterministically.

## Phase 10 (DONE) - Card-testing detector (>10 failed payments / 60 s)

Milestone: **a streaming job counts *failed* payments per customer inside a
60-second event-time window and emits a `CARD_TESTING_ATTACK` alert (30 risk
points) when the count exceeds 10.**

The payment attack detector is implemented in `spark/streaming/payment_attack.py`.

The job (`spark/streaming/payment_attack.py`) filters the `payments` stream to
`status == 'FAILED'`, groups by `customer_id` + `window(event_ts, "60
seconds")`, counts failures, keeps only windows where `failure_count > 10`, and
enriches each alert with `alert_type` + `risk_points`. Append mode + 60 s
watermark, so every finalized window emits its alert exactly once.

The runner proves, with numbers (58 controlled payments, 6 customers):

- **C700 (12 failed payments in one window)** -> flagged, `failure_count` 12,
  `amount_sum` 780, window `00:00:00 -> 00:01:00`.
- **C702 (11 failed, exactly threshold+1)** -> flagged, `count` 11, `sum` 330,
  window `00:01:00 -> 00:02:00`.
- **C701 (8 failed) / C703 (3 failed)** -> **no** alert.
- **C705 (12 failed split 6 + 6 across two windows)** -> **no** alert:
  card-testing is per window, not cumulative (same lesson as Phase 7).
- **C706 (12 SUCCESS payments in one window)** -> **no** alert: only *failed*
  attempts count toward the attack.
- Alerts land on the 60-second grid (epoch == window anchor), `alert_type ==
  CARD_TESTING_ATTACK`, `risk_points == 30`; all 58 events consumed, exactly 2
  alerts emitted.

## Phase 11 (DONE) - Historical anomaly (7d / 30d averages, >5x)

Milestone: **a stateful streaming job keeps each customer's trailing-30-day
transaction history and flags a new transaction as a `HIGH_VALUE_ANOMALY` (25
risk points) when its amount exceeds 5x the customer's rolling-average baseline,
where baseline = max(avg_7d, avg_30d).**

The historical anomaly detector is implemented in `spark/streaming/high_value.py`.

The job (`spark/streaming/high_value.py`) uses Spark 4's `transformWithState`
(`StatefulProcessor`, ProcessingTime, update mode). The trailing history is kept
per customer as a `ValueState` holding a packed `"ts_ms,amount;..."` blob (pruned
to 30 days on every event). Note: `ListState` is the natural fit, but on PySpark
4.2.0 (latest release) *any* `transformWithState` query declaring a `ListState`
crashes before its first batch with `java.io.OptionalDataException` while the JVM
deserializes the task closure (reproduced with a minimal rate-source query); the
packed-`ValueState` design avoids that broken code path.

The runner proves, with numbers (29 controlled transactions across 6 customers,
spread over a 31-day span):

- **C1100** - history 5 x 100 over 30 days, current **600** -> flagged: `avg_7d
  100`, `avg_30d 100`, baseline 100, **ratio 6.0** (current tx excluded from its
  own average).
- **C1101** - consistently high spender (history 5 x 600, current 600) -> **no**
  alert: proportional, same absolute amount as C1100.
- **C1102** - 600 twenty days ago + 3 x 100 recently, current **550** -> **no**
  alert: `avg_7d 100` alone would alert (550 > 5x100), but `avg_30d 225` keeps
  the baseline at 1125 (550 < 1125) - proof the 30-day window is really used.
- **C1103** - history 4 x 100, current **500** -> **no** alert (exactly 5x, the
  rule is strictly greater).
- **C1104** - history 4 x 100, current **501** -> flagged: **ratio 5.01** (one
  dollar over the cutoff).
- **C1105** - only 1 prior transaction, current 600 -> **no** alert (needs >= 2
  prior transactions in the window).
- All 29 events consumed (2 batches), exactly **2 alerts** emitted, milestone
  verified.

## Phase 12 (DONE) - Risk engine: points -> LOW/MEDIUM/HIGH/CRITICAL

Milestone: **the risk engine consumes the detectors' alerts and, per customer
and window, sums the risk points and classifies the combined score into
`LOW` / `MEDIUM` / `HIGH` / `CRITICAL` - the `fraud_alerts` output. A customer
hit by several detectors is scored on the *combination*, not on each alert in
isolation.**

The risk engine is implemented in `spark/streaming/risk_engine.py`.

The job (`spark/streaming/risk_engine.py`) reads unified alert envelopes
(`ALERTS_SCHEMA`: `alert_id`, `customer_id`, `event_time`, `alert_type`,
`risk_points`, ...) - the shape every detector emits - and combines alerts for
the same customer inside a 5-minute event-time window (append mode + watermark,
so each finalized window emits exactly once). Bands
(`config/phase1.json` `risk_levels`): **CRITICAL >= 76, HIGH >= 51, MEDIUM >=
26, else LOW**; `alert_types` lists the distinct detectors that fired (sorted).

The runner proves, with numbers (16 controlled alert envelopes, 8 customers,
all within `[00:00, 00:05)` UTC; per-detector points velocity 25, impossible
travel 30, login->tx 20, card testing 30, high value 25):

- **C1201** - 1 x velocity (25) -> **LOW**.
- **C1202** - 1 x impossible travel (30) -> **MEDIUM** (a single 30-point alert
  outranks the LOW band).
- **C1203** - velocity + high value (**50**) -> **MEDIUM** (band boundary: 50 is
  the top of MEDIUM, not HIGH).
- **C1204** - impossible travel + card testing (**60**) -> **HIGH**.
- **C1205** - velocity + high value + login (**70**) -> **HIGH**.
- **C1206** - impossible travel + card testing + velocity (**85**) ->
  **CRITICAL** (3 overlapping detectors).
- **C1207** - impossible travel + high value + login (**75**) -> **HIGH** (just
  one point under CRITICAL).
- **C1200** pacer (00:11:00, advances the watermark to finalize the scored
  window) lives in a never-finalized window -> **no** record; **C1299** with no
  alerts -> no record.
- All 16 events consumed (2 batches), exactly **7 risk records** emitted, all on
  the 300-second grid, milestone verified.

## Phase 13 (DONE) - XGBoost offline model + evaluation

Milestone: **an offline XGBoost fraud model is trained on behavior-driven
features from the labeled synthetic events and its generalization is measured
on a held-out test set the model never saw during training.**

Train and evaluate the model with `ml/train.py` and `ml/evaluate.py`.

The pipeline (`ml/features.py` -> `ml/train.py` -> `ml/evaluate.py`) builds one
row per transaction/payment (1660 events: 1111 transactions + 549 payments)
and labels each row 1 iff its id is in `data/labels/labels.jsonl` (230 fraud
events: 101 transaction fraud + 129 card-testing payments; every other event
is label 0). The 14 features per event - amount vs the customer's historical
average, log-amount, hour/day, distance from the customer's home city
(Haversine km), transaction count in the trailing 120 s, failed-payment count
in 60 s, total event count in 300 s, seconds since the customer's previous
event, new-device / new-merchant flags, payment status/type - mirror the same
windows the streaming detectors use. Every temporal/novelty feature is computed
from same-customer events that happened **strictly before** the current event
(leakage discipline: the current event joins the history only after its
features are read), so the training set looks exactly like what an online
scorer will see.

Training: stratified 80/20 split (seed 42, 1328 train / 332 test, 184/46
positives), `XGBClassifier` with `scale_pos_weight` ~6.2 for the 230/1430
class imbalance. Held-out test results (measured, not invented):

- **precision 0.915, recall 0.935, F1 0.925** (decision threshold 0.5)
- **ROC-AUC 0.989, PR-AUC 0.971**
- confusion: 43 TP / 4 FP / 3 FN / 282 TN
- artifacts persisted under `ml/model/`: `xgb_model.json`, `test_set.parquet`,
  `metadata.json`, `evaluation.json` (the model is consumed in Phase 14).

## Phase 14 (DONE) - Streaming ML inference

Milestone: **the Phase 13 XGBoost model is loaded into the streaming path and
every incoming transaction / payment is scored online with the same 14
features the model was trained on, producing a `fraud_probability` and an
`ml_prediction` per event.**

Streaming inference is implemented in `spark/streaming/ml_inference.py`.

The job (`spark/streaming/ml_inference.py`) consumes the `transactions` +
`payments` Kafka streams and scores each event in a driver-side
`foreachBatch` callback: the callback appends the batch's events to the
per-customer history and re-applies `ml.features._customer_features` over the
full history (strictly-prior events, exactly like the offline dataset), so the
online feature values reproduce the training-time values, then predicts all
accumulated rows in one `predict_proba` call and emits each event exactly once.
The model, the customer reference table and the feature order are loaded once
on the driver.

Two platform notes (measured on this box):
- PySpark 4.2.0's `transformWithState`/`applyInPandasWithState` **Python state
  server crashes on any query** - even a minimal rate-source repro - with
  `java.io.OptionalDataException` + "No more data to read from the socket"
  (the same bug family that forced the packed-`ValueState` design in Phase 11;
  it turned out not to be `ListState`-specific). Phase 14 therefore uses
  `foreachBatch`, which needs no state server.
- `foreachBatch` runs a Spark job from inside the streaming thread, whose
  await-path recursion overflows the default 512K thread stack
  (`StackOverflowError`); `spark/common.py` sets `-Xss32m` on driver and
  executors, which fixes it.

The runner proves, with numbers (27 controlled events, 4 real customers,
dedicated `phase14_ml_tx`/`phase14_ml_pay` topics, all times on the training
day 2026-08-12 so hour/dow stay in distribution):

- **C001 London** - 2 normal tx + 1 high-value tx at 15x the customer's
  average (`amount_vs_customer_avg` == 15) -> high-value flagged with
  **fraud_probability 0.9993**.
- **C002 Chicago** - normal, then a 7-tx velocity burst in 120 s
  (`tx_count_120s` climbs 0..6) -> burst events predicted fraud.
- **C003 Mumbai** - normal payment, then 12 FAILED payments in 60 s
  (`failed_pay_count_60s` climbs 0..11) -> attack predicted fraud.
- **C004 Bangalore** - 3 normals, 3 h apart (`tx_count_120s` == 0) -> all
  predicted normal.
- One scored row per input event, state-counts reproduce the training windows
  exactly; **online accuracy 0.963, fraud recall 0.95, precision 1.0**
  (confusion 19 TP / 0 FP / 1 FN / 7 TN), milestone verified.
- The single miss is **TX00005**, the *leading edge* of the C002 burst: with
  no prior event in the 120 s window and a below-average amount, it looks
  exactly like a normal transaction (probability 0.0006) - the model cannot
  flag the first event of a burst before any burst signal exists. That is what
  the Phase 7/10/12 rules layer is for; the detectors fire on the burst, and
  the risk engine combines both signals.

## Phase 15 (DONE) - Delta Lake Bronze/Silver/Gold

Milestone: **the raw event files are ingested into a three-layer Delta Lake -
Bronze (untouched raw appends), Silver (deduplicated / conformed / enriched),
Gold (analytics-ready aggregates) - and Delta's ACID guarantees are verified.**

Rebuild the Delta tables through the Phase 18 Airflow backfill DAG, or use an
existing `data/lake/gold` directory before exporting dashboard data.

The pipeline (`spark/batch/bronze.py` -> `silver.py` -> `gold.py`) rebuilds
`data/lake` (gitignored) from the Phase 1 files:

- **Bronze** (`data/lake/bronze/`, 6 tables) - every raw source appended
  untouched with an explicit schema + a `_ingestion_ts` processing-time column:
  transactions 1111, logins 610, payments 549, customer_locations 790,
  customers 200, merchants 50. Verified against the raw files line-by-line.
- **Silver** (`data/lake/silver/`, 5 tables) - per-type tables plus a unified
  `events` table (3060 rows) that deduplicates by id, parses `event_time` into
  a real `event_ts` timestamp, and enriches with the fraud labels (all 230
  from `labels.jsonl`: 101 transaction + 129 payment), the customer / merchant
  reference tables, and the Phase 13 XGBoost model re-scored offline on the
  exact same behavior-driven features the streaming scorer uses. The 230
  labeled events have mean fraud_probability **0.9829**; the 1660 scored rows
  match transactions+payments exactly.
- **Gold** (`data/lake/gold/`, 3 tables) - `customer_risk_summary` (200 rows,
  event mix, fraud counts, summed risk points + the Phase 12 risk band),
  `merchant_fraud_summary` (50 rows, fraud rate + amounts), `fraud_events`
  (230 curated fraud rows with merchant/category/scenario/probability).

Delta mechanics are verified on this box (all with measured numbers):

- **Schema enforcement** - a bad-type append to `bronze/transactions`
  (`amount` as a string) is rejected; the table count stays 1111 (atomic, no
  partial data).
- **MERGE** - a "manual review" correction upserts one real event (TX00001)
  from unlabeled to labeled: fraud count goes **230 -> 231**.
- **Time travel** - `versionAsOf 0` reads the pre-merge state (230 labeled);
  `DESCRIBE HISTORY` shows the commits `(0, WRITE) -> (1, MERGE)`.

Delta connector note: the `delta-spark` *wheel* pins `pyspark<=4.1.1` and would
downgrade this box's pyspark 4.2.0, so it is not pip-installed. Instead
`spark/common.py` resolves the jar via `spark.jars.packages`
(`io.delta:delta-spark_4.1_2.13:4.3.1` - the newest Delta build; there is no
Spark-4.2-specific artifact yet, and this 4.1 build is verified working on
Spark 4.2.0 here). The Gold tables reflect the 230 labels at build time;
rebuilding Gold after Silver changes is a scheduled Airflow concern (Phase 18).

## Phase 16 (DONE) - Reliability: dead-letter, dedup, late events

Milestone: **the streaming consumer never crashes on bad input and never
emits a duplicate or out-of-order event - malformed records are quarantined,
duplicates collapse, late events are handled deterministically by the
watermark.**

Reliability logic is implemented in `spark/streaming/reliability.py`.

The job (`spark/streaming/reliability.py`) keeps the raw Kafka `key`/`value`
columns, then splits the stream in two:

- **Dead-letter** - `from_json` with the transactions schema; a record is
  unparseable when the struct or its `transaction_id` is null (Spark 4.2
  `from_json` returns a *non-null* struct whose fields are null for
  malformed non-empty JSON - a plain `IS NULL` check alone catches nothing).
  Unparseable records go to the `dead_letter` topic with key and value
  preserved **byte-for-byte**, and the rest of the stream keeps flowing.
- **Dedup + late events** - valid events get a real `event_ts` and flow
  through `withWatermark("event_ts", "30 seconds")` +
  `dropDuplicatesWithinWatermark(["transaction_id"])` (Spark 4.2 signature:
  `subset` only - the dedup window *is* the watermark delay), collected
  driver-side with `foreachBatch`.

Controlled run (measured, 11/11 checks PASS, exit 0): 9 records in
(7 valid + 2 malformed) produced in two stages over a fresh per-run topic.
Stage 1 (before the query starts): TX-0001@0s, TX-0002@10s, TX-0001@15s
(duplicate), TX-9000@45s (the watermark pacer -> watermark = 45-30 = **15s**),
and `K-BAD1 -> "this is not json #####"`. Stage 2 (after batch 1, when the
watermark is already 15s): TX-0003@20s (accepted, 20 > 15), TX-0002@22s
(duplicate), TX-0004@5s (**late**, 5 < 15 -> dropped), and `K-BAD2 ->
'{"transaction_id": 42, } broken'`. Result:

- Clean output = exactly `{TX-0001, TX-0002, TX-0003, TX-9000}`, each once;
  the two duplicates collapsed without leaking a second row.
- TX-0004 (late, 5s < 15s watermark) absent from the output.
- `dead_letter` = exactly 2 records, keys `K-BAD1`/`K-BAD2`, raw values
  byte-identical; both queries alive the whole run (job never crashed).
- Note: Spark 4.2's dedup may additionally emit all-null placeholder rows for
  dropped duplicates - an execution-dependent artifact, measured 0 this run,
  **not** part of the contract.

Reliability learnings baked into the runner: fresh topic + fresh checkpoint
per run (reused checkpoints made Spark resume the *old* topic/offsets and
threw `offset was changed from 9 to 5` races when a topic was deleted and
recreated); `kafka.consumer` crashes on non-JSON values, so dead-letter
verification uses a raw-bytes consumer.

## Phase 17 (DONE) - Checkpoint / state recovery

Milestone: **a stateful streaming job survives a driver restart - the
checkpoint directory restores exactly what was lost (Kafka source offsets,
the RocksDB state store, the streaming watermark, the batch counter), so the
restarted job resumes where the old one stopped: no data loss, no duplicate
re-processing, no lost state.**

Checkpoint recovery is implemented in `spark/streaming/checkpoint_recovery.py`.

The job (`spark/streaming/checkpoint_recovery.py`) reuses the Phase 16
watermarked dedup operator (`withWatermark` + `dropDuplicatesWithinWatermark`,
RocksDB-backed keyed state) as the stateful probe. The runner runs it on one
controlled topic against ONE checkpoint directory across TWO query lifecycles
- lifecycle A runs to batch 1 and stops cleanly, 3 more records are produced
during the "downtime", then lifecycle B restarts with a fresh SparkSession
(i.e. a driver restart) on the same checkpoint.

Controlled run (measured, 12/12 checks PASS, exit 0). Stage A (before
lifecycle A): TX-0001@0s, TX-0002@10s, TX-0001@15s (duplicate), TX-9000@50s
(the pacer -> watermark 50-30 = **20s**). Stage B (during downtime):
TX-0003@35s (new, 35 > 20 -> accepted), TX-0002@38s (duplicate of a
**pre-restart** key), TX-0004@15s (**late** vs the *restored* watermark).
Result:

- **Offsets recovered** - lifecycle B's first batch started at offset 4, run
  A's last committed offset (contiguous: no overlap, no gap). Consumed 4 + 3
  = 7 = the topic high watermark: nothing lost, nothing re-read.
- **Batch counter recovered** - batch ids continued `0,1 -> 2,3` (not reset).
- **RocksDB state recovered** - TX-0002@38s, a duplicate of a transaction
  seen *before* the restart, was still suppressed (TX-0002 appears exactly
  once across both lifecycles) - only possible if the dedup state was
  restored from the checkpoint.
- **Watermark recovered** - lifecycle B dropped TX-0004@15s and reported the
  restored watermark `2026-08-12T00:00:20Z` (= 20s). A fresh watermark would
  have been 38-30 = 8s and would have *accepted* TX-0004 - the drop proves
  the watermark itself is checkpointed, not recomputed.
- Lifecycle A emitted exactly `{TX-0001, TX-0002, TX-9000}`, lifecycle B
  exactly `{TX-0003}`; the checkpoint dir shows `offsets/`, `commits/`,
  `sources/` and RocksDB `state/` snapshots.

Note (Spark 4.2 API): kafka source `startOffset`/`endOffset` in
`StreamingQueryProgress` are topic-nested (`{"topic": {"0": 4}}`) and the
watermark is an ISO string (`...T00:00:20.000Z`), not a millisecond long.

## Phase 18 (DONE) - Airflow DAGs (training / quality / backfill)

Milestone: **Airflow runs the repo's batch pipeline inside its own container
image - the training DAG refreshes the XGBoost model, the backfill DAG
rebuilds the Bronze/Silver/Gold Delta lake, and the quality DAG gates the
rebuild - and the milestone runner (30/30 checks PASS, exit 0) watches each
run reach success and verifies the artifacts they publish under
`data/airflow/`.**

Start the Airflow service with `docker compose up -d airflow`, then trigger the
training, quality, and backfill DAGs from the Airflow UI.

The image (`Dockerfile.airflow` -> `fraud-airflow:2.10.2`) is
`apache/airflow:2.10.2` plus openjdk-17-jre-headless and the repo's compute
stack (pyspark 4.2.0, delta-spark, xgboost, pandas 3.0.5, numpy 2.5.2,
pyarrow, scikit-learn), with `HADOOP_HOME=/opt/hadoop`. The compose service
runs `airflow standalone` on http://localhost:8080 (admin/admin) with
`DAGS_FOLDER=/repo/dags`, `PYTHONPATH=/repo`, the repo bind-mounted at
`/repo`, and the SequentialExecutor. Airflow stays out of the real-time
Kafka -> Spark path (the streaming jobs keep running from Phase 17); the
DAGs are batch-only and schedule daily.

The three DAGs in `dags/`:

- `fraud_training_dag` - train (`ml/train.py`) -> evaluate
  (`ml/evaluate.py`) -> publish `training_metrics.json`.
- `fraud_backfill_dag` - clear the lake -> rebuild Bronze -> Silver -> Gold
  (`spark/batch/`) -> publish `backfill_manifest.json`.
- `fraud_quality_dag` - quality gates over the rebuilt lake
  (`spark/batch/quality.py`) -> publish `quality_report.json`.

Verified run (measured): all three runs reached **success**.
`backfill_manifest.json`: Bronze 1111/610/549/790/200/50 rows
(transactions/logins/payments/customer_locations/customers/merchants),
Silver `events` 3060 rows (all distinct event_ids), 230 labeled, 1660 scored,
Gold `customer_risk_summary` 200 / `merchant_fraud_summary` 50 /
`fraud_events` 230. `training_metrics.json` (n_test 332, 46 positive):
roc_auc **0.990**, f1 **0.915**, precision 0.896, recall 0.935 - above the
runner's gates (roc_auc > 0.9, f1 > 0.8). `quality_report.json` `ok` with
the silver/gold checks green. The model file referenced by the report
(`/repo/ml/model/xgb_model.json` in-container) exists on the host at
`ml/model/xgb_model.json`.

Notes that cost real debugging time:

- Airflow 2.10 creates DAGs **paused**; runs triggered on a paused DAG stay
  `queued` forever. The DAGs set `is_paused_upon_creation=False` and the
  runner also unpauses them explicitly.
- `airflow dags list` parses the DAG files on demand, but `airflow dags
  trigger` goes through the scheduler's DB (DagModel) - on a fresh container
  the DB is only populated after the scheduler's own DagBag parse (~3 min).
  The runner waits for `is_paused` to be populated in `dags list -o json`
  (DB-synced) before triggering, and retries the trigger.
- PySpark 4.2.0 needs `pandas >= 2.2.0`; the stock Airflow image ships
  pandas 2.1.4, so the Dockerfile pins `pandas>=2.2.0` (image now pandas
  3.0.5).

## Phase 19 (DONE) - Power BI dashboard

Milestone: **the Gold Delta layer is exposed as refreshable Power BI tables
without making Power BI depend on Delta transaction-log internals.**

Generate the dashboard extract after the Phase 15/18 backfill:

```powershell
python dashboard/export_powerbi.py
```

The exporter (`dashboard/export_powerbi.py`) reads Gold Delta and creates
`data/dashboard/` presentation tables for KPI cards, alerts by hour, alerts by
type, top risky customers, and alert details. `dashboard/README.md` contains
the Power BI folder-connection steps, report layout, and DAX measures. The
export is intentionally a snapshot: run it again after an Airflow backfill,
then refresh the Power BI folder source.

## Phase 20 (DONE) - GCP lake and demonstration deployment

Milestone: **the Delta lake and Power BI extract can be published to GCS, and
an optional lightweight Compute Engine demonstration can run the existing
Airflow batch service with least-privilege access and Cloud Monitoring.**

The scripts under `gcp/` are deliberately credential-free:

```powershell
$env:PROJECT_ID = "your-project-id"
$env:BUCKET = "your-project-id-fraud-lake"
$env:REGION = "us-central1"
.\gcp\bootstrap.ps1
python dashboard\export_powerbi.py
.\gcp\sync_gcs.ps1
```

`bootstrap.ps1` enables the required APIs, creates a uniform-access bucket,
and grants the `fraud-runtime` service account only bucket object-viewer plus
logging/monitoring writer permissions. It never creates or downloads a key.
`sync_gcs.ps1` uploads complete Bronze/Silver/Gold Delta directories, including
`_delta_log`, and the Power BI extracts. `deploy_compute_engine.ps1` is an
optional, explicitly sized demo VM path; it starts Airflow, not a pretend
production Kafka/Spark cluster. See `gcp/README.md` for cleanup and monitoring
instructions.

No cloud throughput or latency numbers are claimed here. Measure events/sec,
Kafka lag, alert P50/P95/P99, late events, dead-letter count, and recovery time
on the selected runtime before adding benchmark results.

### Complete GCP deployment steps

The following is the supported low-cost deployment path for this repository.
It publishes the Delta lake and Power BI extracts to Google Cloud Storage. The
Kafka and Spark streaming learning environment remains local unless a managed
Kafka provider and a cloud Spark runtime are configured separately.

#### Prerequisites

1. Install the Google Cloud CLI and restart PowerShell after installation.
2. Enable billing for the selected GCP project and create a budget alert.
3. Authenticate without creating a service-account key:

```powershell
gcloud auth login
```

#### Set the project variables

Use the project ID, not the project number. The bucket name must be globally
unique:

```powershell
cd E:\FraudDetection
$env:PROJECT_ID = "your-project-id"
$env:BUCKET = "your-project-id-fraud-lake"
$env:REGION = "us-central1"
gcloud config set project $env:PROJECT_ID
gcloud config get-value project
```

If a billing account must be linked from the CLI:

```powershell
$env:BILLING_ACCOUNT_ID = "000000-000000-000000"
  --billing-account=$env:BILLING_ACCOUNT_ID
```

#### Build the local artifacts first

The Gold Delta tables must exist before they can be uploaded. If the lake is
already present, skip the rebuild:

```powershell
Test-Path data\lake\gold
```

Export stable Power BI CSV filenames:

```powershell
python dashboard\export_powerbi.py
Get-ChildItem data\dashboard -Recurse -Filter *.csv
```

The exporter creates `kpis\kpis.csv`, `alert_details\alert_details.csv`,
`alerts_by_hour\alerts_by_hour.csv`, `alerts_by_type\alerts_by_type.csv`,
and `top_risky_customers\top_risky_customers.csv`. Power BI should load these
files individually with **Get data -> Text/CSV**.

#### Create cloud resources

```powershell
.\gcp\bootstrap.ps1
```

This enables the required APIs, creates the uniform-access GCS bucket, creates
the `fraud-runtime` service account, and grants it bucket object-viewer plus
Cloud Logging and Monitoring writer permissions. The script is safe to rerun
and stops if a native `gcloud` command fails.

#### Upload the lake and dashboard extracts

```powershell
.\gcp\sync_gcs.ps1
gcloud storage ls --recursive "gs://$env:BUCKET"
```

Expected cloud prefixes:

```text
```

#### Optional Compute Engine demonstration

This is optional and can create charges. It starts the Airflow batch service;
it does not create a production Kafka/Spark cluster. The repository must be
available from a Git hosting URL:

```powershell
$env:REPO_URL = "https://github.com/your-user/your-repository.git"
$env:ZONE = "us-central1-a"
.\gcp\deploy_compute_engine.ps1
```

Check the VM service:

```powershell
  --command="sudo docker compose -f /opt/fraud-engine/docker-compose.yml ps"
```

#### Clean up after the demonstration

```powershell
```

Delete the bucket only when its data is no longer needed:

```powershell
```

For a genuinely online analytics dashboard, add BigQuery between GCS and
Power BI. For a genuinely online streaming platform, add managed Kafka and a
properly sized cloud Spark runtime. Do not claim those components until they
are deployed and measured.

## Notes / rules

- Never invent benchmark numbers; measure and put real values in the README.
- Do not commit credentials (see `.gitignore`).
- Airflow stays out of the Kafka -> Spark real-time path.
- Keep ML secondary; the streaming/reliability engineering is the value.
