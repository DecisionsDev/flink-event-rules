#!/usr/bin/env bash

#
# Copyright IBM Corp. 2026
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# deploy.sh
# Submits the AgentBasedDataStreamJob to a Flink cluster running in Docker
# for the Airline Agent use case.
#
# The job reads the airline agent descriptor from the path specified by
# AGENT_DESCRIPTOR_PATH.  The samples/ directory is volume-mounted into the
# Flink containers by docker-compose.yml.
#
# Usage:
#   ./deploy.sh [bootstrap-servers] [descriptor-relative-path]
#
# Defaults:
#   bootstrap-servers  : kafka:29092   (internal Docker network listener)
#   descriptor-path    : samples/airline-agent/agents.json
#
# Environment variables (override defaults):
#   KAFKA_BOOTSTRAP_SERVERS            (default: kafka:29092)
#   AGENT_DESCRIPTOR_PATH              (default: samples/airline-agent/agents.json)
#   FLINK_CONTAINER                    (default: event-rule-backend-jobmanager)
#   FLINK_JOBMANAGER_URL               (default: http://localhost:8081)
#   FLINK_PARALLELISM                  (default: 1)
#   FLINK_JOB_NAME                     (default: Airline Agent)
#   AGENT_ERROR_TOPIC                  (default: agent-errors)
#
# Volume mounts (defined in docker-compose.yml):
#   ./event-rule-backend/target  → /opt/flink/usrlib     (FAT JAR)
#   ./samples                    → /opt/flink/samples     (descriptors + rule archives)

set -euo pipefail

USECASE="Airline Agent"

# ── Local paths (used only for pre-flight checks on the host) ─────────────────
LOCAL_DESCRIPTOR="${AGENT_DESCRIPTOR_PATH:-${2:-samples/airline-agent/agents.json}}"

# ── Container-internal paths (volumes defined in docker-compose.yml) ──────────
#   ./event-rule-backend/target  → /opt/flink/usrlib
#   ./samples                    → /opt/flink/samples
CONTAINER_JAR="/opt/flink/usrlib/event-rule-backend-1.0-SNAPSHOT.jar"
# Strip the leading "samples/" prefix to make the path relative to /opt/flink/samples
DESCRIPTOR_REL="${LOCAL_DESCRIPTOR#samples/}"
CONTAINER_DESCRIPTOR="/opt/flink/samples/${DESCRIPTOR_REL}"

# ── Tunables ──────────────────────────────────────────────────────────────────
# Flink containers communicate on the internal Docker network (kafka:29092).
# The host-side listener (localhost:9092) is not reachable from within containers.
BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-${1:-kafka:29092}}"
JOBMANAGER="${FLINK_JOBMANAGER_URL:-http://localhost:8081}"
FLINK_CONTAINER="${FLINK_CONTAINER:-event-rule-backend-jobmanager}"
export FLINK_JOB_NAME="${FLINK_JOB_NAME:-Airline Agent}"
export AGENT_ERROR_TOPIC="${AGENT_ERROR_TOPIC:-agent-errors}"
export FLINK_PARALLELISM="${FLINK_PARALLELISM:-1}"

echo "========================================================"
echo " Airline Agent Job  (Docker)"
echo "========================================================"
echo "  Bootstrap servers  : ${BOOTSTRAP}"
echo "  Descriptor (host)  : ${LOCAL_DESCRIPTOR}"
echo "  Descriptor (ctr)   : ${CONTAINER_DESCRIPTOR}"
echo "  JobManager URL     : ${JOBMANAGER}"
echo "  Flink container    : ${FLINK_CONTAINER}"
echo "  JAR (container)    : ${CONTAINER_JAR}"
echo "  Job name           : ${FLINK_JOB_NAME}"
echo "  Error topic        : ${AGENT_ERROR_TOPIC}"
echo "  Parallelism        : ${FLINK_PARALLELISM}"
echo "========================================================"

# ── Pre-flight: verify the host-side descriptor exists ────────────────────────
if [[ ! -f "$LOCAL_DESCRIPTOR" ]]; then
  echo "ERROR: Descriptor not found on host: ${LOCAL_DESCRIPTOR}"
  exit 1
fi

# ── Pre-flight: verify the Flink JobManager container is running ───────────────
if ! docker inspect "${FLINK_CONTAINER}" > /dev/null 2>&1; then
  echo "ERROR: Container '${FLINK_CONTAINER}' not found."
  echo "       Run: docker compose up -d"
  exit 1
fi

# ── Wait for Flink TaskManager to register with the JobManager ────────────────
echo "Waiting for Flink cluster to be ready..."
until curl -sf "${JOBMANAGER}/overview" 2>/dev/null | grep -q '"taskmanagers":[1-9]'; do
  echo "  Flink not ready, retrying in 5s..."
  sleep 5
done
echo "  Flink cluster ready."

# ── Pre-create Kafka topics (Flink Kafka source requires topics to exist) ─────
# Kafka auto.create.topics.enable is true but the Flink source connector calls
# AdminClient.listTopicPartitions() before any producer triggers auto-creation,
# which causes an UnknownTopicOrPartitionException and immediate job failure.
# Pre-creating the topics here avoids that race condition.
KAFKA_CONTAINER="${KAFKA_CONTAINER:-kafka}"
TOPIC_PARTITIONS="${TOPIC_PARTITIONS:-1}"
TOPIC_REPLICATION="${TOPIC_REPLICATION:-1}"
AIRLINE_TOPICS=(
  "agent-flight-events"
  "agent-passenger-events"
  "agent-catering-events"
  "agent-flight-notifications"
  "agent-passenger-notifications"
  "agent-catering-status-notifications"
  "agent-airline-errors"
  "agent-airline-archive-control"
  "agent-flight-entity-changelog"
)
echo "Pre-creating Kafka topics for airline use-case..."
for topic in "${AIRLINE_TOPICS[@]}"; do
  docker exec "${KAFKA_CONTAINER}" \
    kafka-topics --bootstrap-server localhost:9092 \
    --create --if-not-exists \
    --topic "${topic}" \
    --partitions "${TOPIC_PARTITIONS}" \
    --replication-factor "${TOPIC_REPLICATION}" 2>&1 | grep -v "^$" | sed "s/^/  [kafka] /" || true
done
echo "  Kafka topics ready."

# ── Submit the job via docker exec ────────────────────────────────────────────
echo "  Deploying job in Flink for ${USECASE}...."

docker exec \
  -e KAFKA_BOOTSTRAP_SERVERS="${BOOTSTRAP}" \
  -e AGENT_DESCRIPTOR_PATH="${CONTAINER_DESCRIPTOR}" \
  -e FLINK_JOB_NAME="${FLINK_JOB_NAME}" \
  -e AGENT_ERROR_TOPIC="${AGENT_ERROR_TOPIC}" \
  -e FLINK_PARALLELISM="${FLINK_PARALLELISM}" \
  "${FLINK_CONTAINER}" \
  /opt/flink/bin/flink run -d \
    -m localhost:8081 \
    -c com.ibm.di.event.agent.AgentBasedDataStreamJob \
    "${CONTAINER_JAR}" \
    "${BOOTSTRAP}" \
    "${CONTAINER_DESCRIPTOR}"

echo ""
echo "${USECASE} job submitted. Monitor at ${JOBMANAGER}"
echo "  Flink UI : ${JOBMANAGER}/#/overview"
