# Real-Time Fraud Detection & Risk Engine

Event-driven fraud platform: Python generators -> Kafka -> PySpark Structured
Streaming -> Rules + XGBoost risk engine -> `fraud_alerts` + Delta Lake,
with Airflow for batch jobs (training / quality / backfills).

Workflow: 

Someone makes a payment
          |
          v
The payment is placed in the event queue
          |
          v
The system checks the time, customer, amount, device, and location
          |
          v
It compares the activity with warning signs and past behavior
          |
          v
The system creates a risk score and explanation
          |
          v
The alert is saved and displayed for review


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
| 1 | Python generators produce normal + fraudulent events
| 2 | Docker + Kafka, 1,000 events Python -> Kafka -> consumer
| 3 | Topics, partitions, keys, offsets, consumer groups
| 4 | Kafka -> PySpark streaming (explicit schema + checkpoint)
| 5 | Tumbling + sliding windows (2 min / 30 s slide)
| 6 | Watermarks + late events
| 7 | Velocity detector (>5 tx / 2 min)
| 8 | Stateful impossible travel (Haversine speed)
| 9 | Stream-stream join: login -> transaction (5 min)
| 10 | Card-testing detector (>10 failed payments / 60 s)
| 11 | Historical anomaly (7d / 30d averages, >5x) 
| 12 | Risk engine: points -> LOW/MEDIUM/HIGH/CRITICAL
| 13 | XGBoost offline model + evaluation
| 14 | Streaming ML inference
| 15 | Delta Lake Bronze/Silver/Gold
| 16 | Reliability: dead-letter, dedup, late events
| 17 | Checkpoint/state recovery
| 18 | Airflow DAGs (training / quality / backfill)
| 19 | Power BI dashboard
| 20 | GCP deployment + observability

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