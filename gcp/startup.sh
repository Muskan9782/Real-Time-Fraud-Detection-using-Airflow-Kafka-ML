#!/usr/bin/env bash
set -euo pipefail

apt-get update
apt-get install -y docker.io docker-compose-plugin git google-cloud-ops-agent
systemctl enable --now docker google-cloud-ops-agent

install -d -o root -g root /opt/fraud-engine
if [ ! -d /opt/fraud-engine/.git ]; then
  git clone "${REPO_URL}" /opt/fraud-engine
fi
cd /opt/fraud-engine

# Airflow is the selected batch demonstration. Kafka and Spark streaming stay
# local until a managed Kafka endpoint and a sized Spark runtime are configured.
docker compose build airflow
docker compose up -d airflow
echo "Fraud engine Airflow started on port 8080" > /var/log/fraud-engine-startup.log
