#!/bin/bash

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
# broadcast-reload.sh
#
# Triggers a live Decision Intelligence archive reload on a Flink broadcast-state pipeline
# by publishing an ArchiveControlCmd JSON message to the domain archive-control
# Kafka topic.  Supports PREFETCH, LOAD, ROLLBACK, and NOOP commands.
#
# Supports the following use cases:
#   banking-fraud-agent   StatefulAgentProcessor — Banking Fraud Detection Agent
#
# Usage:
#   ./broadcast-reload.sh --use-case <case> --jar <path> [OPTIONS]
#
# Required (for LOAD / PREFETCH):
#   --use-case <name>       Domain: banking-fraud-agent
#   --jar     <path>        Path to the JAR on the HOST machine.
#                           The script computes SHA-256 and resolves the container URL.
#
# Options:
#   --version    <label>    Human-readable version label (default: derived from jar filename)
#   --command    <type>     LOAD | PREFETCH | ROLLBACK | NOOP  (default: LOAD)
#   --request-id <id>       Correlation ID for audit logs (default: auto-generated)
#   --topic      <name>     Override the archive-control Kafka topic name
#   --bootstrap  <addr>     Kafka bootstrap server (default: localhost:9092)
#   --container  <name>     Kafka Docker container name (default: kafka)
#   --samples-dir <dir>     Samples directory mounted into Flink containers (default: ./samples)
#   --container-mount <p>   Container path where samples/ is mounted (default: /opt/flink/samples)
#   --archive-url <url>     Full archive URL for ROLLBACK (skips jar-based URL resolution)
#   --sha256 <hex>          SHA-256 hex for ROLLBACK (skips local computation)
#   --wait                  After LOAD/ROLLBACK, tail TaskManager logs until reload is
#                           confirmed or timeout (requires --tm-container)
#   --tm-container <name>   Flink TaskManager container name for --wait
#                           (default: event-rule-backend-taskmanager)
#   --wait-timeout <secs>   Seconds to wait for reload confirmation (default: 120)
#   --dry-run               Print the command that would be published; do not publish
#
# Examples:
#   # Banking fraud agent: deploy a new fraud-detection JAR
#   ./broadcast-reload.sh --use-case banking-fraud-agent \
#       --jar samples/banking-fraud-agent/FraudDetectionService_1.3.1.jar
#
#   # Prefetch, then load (minimises checkpoint-barrier blocking window)
#   ./broadcast-reload.sh --use-case banking-fraud-agent \
#       --jar samples/banking-fraud-agent/FraudDetectionService_1.3.1.jar --command PREFETCH
#   ./broadcast-reload.sh --use-case banking-fraud-agent \
#       --jar samples/banking-fraud-agent/FraudDetectionService_1.3.1.jar --command LOAD --wait
#
#   # Rollback to a previous version
#   ./broadcast-reload.sh --use-case banking-fraud-agent --command ROLLBACK \
#       --archive-url file:///opt/flink/samples/banking-fraud-agent/old.jar \
#       --sha256 3f9a1b2c... --version v1.0.1
#
#   # Preview message without publishing
#   ./broadcast-reload.sh --use-case banking-fraud-agent \
#       --jar samples/banking-fraud-agent/FraudDetectionService_1.3.1.jar --dry-run

set -euo pipefail

# ── Colour helpers ─────────────────────────────────────────────────────────────
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; CYAN='\033[0;36m'; NC='\033[0m'
info()    { echo -e "${GREEN}[INFO]${NC}   $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}   $*"; }
error()   { echo -e "${RED}[ERROR]${NC}  $*" >&2; }
section() { echo -e "\n${CYAN}── $* ──${NC}"; }

# ── Defaults ───────────────────────────────────────────────────────────────────
USE_CASE=""
NEW_JAR=""
COMMAND="LOAD"
VERSION=""
REQUEST_ID="broadcast-reload-$(date +%s)"
TOPIC_OVERRIDE=""
BOOTSTRAP="localhost:9092"
KAFKA_CONTAINER="kafka"
SAMPLES_DIR="./samples"
CONTAINER_MOUNT="/opt/flink/samples"
ARCHIVE_URL_OVERRIDE=""
SHA256_OVERRIDE=""
WAIT=false
TM_CONTAINER="event-rule-backend-taskmanager"
WAIT_TIMEOUT=120
DRY_RUN=false

# ── Usage ──────────────────────────────────────────────────────────────────────
usage() {
    sed -n '3,57p' "$0" | sed 's/^# \?//'
    exit 1
}

# ── Argument parsing ───────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --use-case)        USE_CASE="$2";          shift 2 ;;
        --jar)             NEW_JAR="$2";            shift 2 ;;
        --command)         COMMAND="$(echo "$2" | tr '[:lower:]' '[:upper:]')"; shift 2 ;;
        --version)         VERSION="$2";            shift 2 ;;
        --request-id)      REQUEST_ID="$2";         shift 2 ;;
        --topic)           TOPIC_OVERRIDE="$2";     shift 2 ;;
        --bootstrap)       BOOTSTRAP="$2";          shift 2 ;;
        --container)       KAFKA_CONTAINER="$2";    shift 2 ;;
        --samples-dir)     SAMPLES_DIR="$2";        shift 2 ;;
        --container-mount) CONTAINER_MOUNT="$2";    shift 2 ;;
        --archive-url)     ARCHIVE_URL_OVERRIDE="$2"; shift 2 ;;
        --sha256)          SHA256_OVERRIDE="$2";    shift 2 ;;
        --wait)            WAIT=true;               shift   ;;
        --tm-container)    TM_CONTAINER="$2";       shift 2 ;;
        --wait-timeout)    WAIT_TIMEOUT="$2";       shift 2 ;;
        --dry-run)         DRY_RUN=true;            shift   ;;
        --help|-h)         usage ;;
        *) error "Unknown argument: $1"; usage ;;
    esac
done

# ── Validate use-case and derive defaults ──────────────────────────────────────
[[ -z "$USE_CASE" ]] && { error "--use-case is required (banking-fraud-agent)"; usage; }

# DEFAULT_TOPIC: Kafka archive-control topic for this use case
# SUCCESS_PATTERN: grep pattern in TaskManager logs indicating a successful reload
case "$USE_CASE" in
    banking-fraud-agent)
        DEFAULT_TOPIC="agent-banking-archive-control"
        SUCCESS_PATTERN="StatefulAgentProcessor\[fraud-detection-agent\].*Archive reloaded.*v="
        ;;
    *)
        error "--use-case must be: banking-fraud-agent (got: '$USE_CASE')"
        exit 1
        ;;
esac

TOPIC="${TOPIC_OVERRIDE:-$DEFAULT_TOPIC}"

# ── Validate command ───────────────────────────────────────────────────────────
case "$COMMAND" in
    LOAD|PREFETCH|ROLLBACK|NOOP) ;;
    *) error "--command must be LOAD | PREFETCH | ROLLBACK | NOOP (got: '$COMMAND')"; exit 1 ;;
esac

# ── NOOP fast path ─────────────────────────────────────────────────────────────
if [[ "$COMMAND" == "NOOP" ]]; then
    PAYLOAD="{\"version\":\"noop\",\"commandType\":\"NOOP\",\"requestId\":\"${REQUEST_ID}\"}"
    section "NOOP command — $USE_CASE"
    info "Topic      : $TOPIC"
    info "Payload    : $PAYLOAD"
    if [[ "$DRY_RUN" == "true" ]]; then
        warn "[DRY-RUN] No message published."
        exit 0
    fi
    if ! docker ps --format '{{.Names}}' | grep -qx "$KAFKA_CONTAINER"; then
        error "Kafka container '$KAFKA_CONTAINER' is not running."
        exit 1
    fi
    echo "$PAYLOAD" | docker exec -i "$KAFKA_CONTAINER" \
        kafka-console-producer --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC"
    info "✓ NOOP published to $TOPIC"
    exit 0
fi

# ── Resolve archive URL and SHA-256 ───────────────────────────────────────────
#
# Two paths:
#   1. --jar provided: derive container URL from samples-dir mapping, compute SHA-256 locally
#   2. --archive-url + --sha256 provided: used directly (ROLLBACK to a prior version)

if [[ -n "$ARCHIVE_URL_OVERRIDE" ]]; then
    if [[ -z "$SHA256_OVERRIDE" ]]; then
        error "--sha256 is required when --archive-url is provided."
        exit 1
    fi
    ARCHIVE_URL="$ARCHIVE_URL_OVERRIDE"
    SHA256="$SHA256_OVERRIDE"
    if [[ -z "$VERSION" ]]; then
        VERSION="$(basename "$ARCHIVE_URL" .jar)"
    fi
else
    if [[ -z "$NEW_JAR" ]]; then
        error "--jar is required for $COMMAND (or supply --archive-url + --sha256 for ROLLBACK)."
        exit 1
    fi
    if [[ ! -f "$NEW_JAR" ]]; then
        error "JAR not found: $NEW_JAR"
        exit 1
    fi
    if [[ "$NEW_JAR" != *.jar ]]; then
        error "File does not have a .jar extension: $NEW_JAR"
        exit 1
    fi

    JAR_SIZE=$(stat -f%z "$NEW_JAR" 2>/dev/null || stat -c%s "$NEW_JAR" 2>/dev/null || echo 0)
    if [[ "$JAR_SIZE" -eq 0 ]]; then
        error "JAR is empty (0 bytes): $NEW_JAR"
        exit 1
    fi

    # ── Compute SHA-256 ──────────────────────────────────────────────────────
    section "Computing SHA-256"
    if command -v shasum &>/dev/null; then
        SHA256="$(shasum -a 256 "$NEW_JAR" | awk '{print $1}')"
    elif command -v sha256sum &>/dev/null; then
        SHA256="$(sha256sum "$NEW_JAR" | awk '{print $1}')"
    else
        error "Neither 'shasum' (macOS) nor 'sha256sum' (Linux) is available."
        exit 1
    fi
    info "SHA-256 : $SHA256"

    # ── Resolve container-side file:// URL ───────────────────────────────────
    # The samples/ directory on the host is mounted inside Flink containers at
    # CONTAINER_MOUNT (default: /opt/flink/samples).  Convert the host JAR path
    # to the equivalent absolute container path.
    HOST_SAMPLES_ABS="$(cd "$SAMPLES_DIR" && pwd)"
    JAR_ABS="$(cd "$(dirname "$NEW_JAR")" && pwd)/$(basename "$NEW_JAR")"

    if [[ "$JAR_ABS" == "$HOST_SAMPLES_ABS"* ]]; then
        JAR_REL="${JAR_ABS#"$HOST_SAMPLES_ABS"/}"
        ARCHIVE_URL="file://${CONTAINER_MOUNT}/${JAR_REL}"
    else
        warn "JAR is outside the samples directory ($SAMPLES_DIR)."
        warn "The Flink containers can only reach files under the volume mount."
        warn "Copy the JAR into $SAMPLES_DIR/<use-case>/ first, then re-run."
        warn "Falling back to host path in URL — this will FAIL at runtime."
        ARCHIVE_URL="file://${JAR_ABS}"
    fi

    if [[ -z "$VERSION" ]]; then
        VERSION="$(basename "$NEW_JAR" .jar)"
    fi
fi

# ── Build the JSON payload ─────────────────────────────────────────────────────
PAYLOAD="{\"version\":\"${VERSION}\",\"archiveUrl\":\"${ARCHIVE_URL}\",\"sha256\":\"${SHA256}\",\"commandType\":\"${COMMAND}\",\"requestId\":\"${REQUEST_ID}\"}"

# ── Print plan ─────────────────────────────────────────────────────────────────
section "Broadcast archive reload — $USE_CASE ($COMMAND)"
info "Topic          : $TOPIC"
info "Version        : $VERSION"
info "Command        : $COMMAND"
info "Archive URL    : $ARCHIVE_URL"
info "SHA-256        : $SHA256"
info "Request ID     : $REQUEST_ID"
info "Bootstrap      : $BOOTSTRAP"
info "Kafka container: $KAFKA_CONTAINER"
if [[ "$WAIT" == "true" ]]; then
    info "Wait for reload: yes (timeout=${WAIT_TIMEOUT}s, container=$TM_CONTAINER)"
fi
echo ""
info "Payload:"
echo "  $PAYLOAD"
echo ""

if [[ "$DRY_RUN" == "true" ]]; then
    warn "[DRY-RUN] No message published. Command that would run:"
    echo ""
    echo "  echo '$PAYLOAD' | docker exec -i $KAFKA_CONTAINER \\"
    echo "      kafka-console-producer --bootstrap-server $BOOTSTRAP --topic $TOPIC"
    echo ""
    exit 0
fi

# ── Pre-flight: Kafka container check ─────────────────────────────────────────
if ! docker ps --format '{{.Names}}' | grep -qx "$KAFKA_CONTAINER"; then
    error "Kafka container '$KAFKA_CONTAINER' is not running."
    error "Start it with: ./run.sh up  (or: docker compose up -d kafka)"
    exit 1
fi

# ── Verify the topic exists (create if missing) ────────────────────────────────
section "Verifying topic: $TOPIC"
if docker exec "$KAFKA_CONTAINER" \
       kafka-topics --bootstrap-server "$BOOTSTRAP" --list 2>/dev/null \
       | grep -qx "$TOPIC"; then
    info "Topic '$TOPIC' exists."
else
    warn "Topic '$TOPIC' not found — creating with 1 partition (control topics must be single-partition for ordered delivery)."
    docker exec "$KAFKA_CONTAINER" \
        kafka-topics --bootstrap-server "$BOOTSTRAP" --create \
            --topic "$TOPIC" --partitions 1 --replication-factor 1
    info "Topic '$TOPIC' created."
fi

# ── Publish the control message ────────────────────────────────────────────────
section "Publishing $COMMAND command"
echo "$PAYLOAD" | docker exec -i "$KAFKA_CONTAINER" \
    kafka-console-producer --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC"

info "✓ Command published to '$TOPIC'"
echo ""

# ── Post-publish guidance ──────────────────────────────────────────────────────
case "$COMMAND" in
    PREFETCH)
        info "PREFETCH dispatched. The archive will be downloaded and pre-warmed in the"
        info "TaskManager's local cache (/tmp/flink-archive-cache/) without activating it."
        info "When ready, run again with --command LOAD to activate."
        ;;
    LOAD|ROLLBACK)
        info "The Flink TaskManager will:"
        info "  1. Download the archive (up to 120 s download timeout)"
        info "  2. Initialise a new RuleEngineRunner (URLClassLoader + DecisionRunner)"
        info "  3. Run a domain-specific smoke test"
        info "  4. Update broadcast state to activate the new version"
        info "  5. Evict the previous URLClassLoader"
        echo ""
        info "Monitor TaskManager logs:"
        echo ""
        echo "  docker compose logs -f $TM_CONTAINER | grep -E 'Archive reloaded|SMOKE|SHA-256|reload'"
        echo ""

        # StatefulAgentProcessor log markers
        info "Expected success log:"
        echo "  [StatefulAgentProcessor[fraud-detection-agent]] ✓ Archive reloaded: v=${VERSION} requestId=${REQUEST_ID}"
        echo ""
        info "On failure the error is logged at ERROR level on the TaskManager — there is"
        info "no DLQ topic.  The previous version stays active."
        info "Check logs for:"
        echo "  [StatefulAgentProcessor[fraud-detection-agent]] ✗ Archive reload failed for requestId=${REQUEST_ID}"
        echo ""
        info "To inspect raw TaskManager logs:"
        echo ""
        echo "  docker logs $TM_CONTAINER 2>&1 | grep -E 'Archive|SMOKE|SHA-256'"
        echo ""
        ;;
    NOOP)
        info "NOOP published. No archive change was triggered."
        ;;
esac

# ── Optional: tail TaskManager logs until reload confirmation ──────────────────
if [[ "$WAIT" == "true" && ( "$COMMAND" == "LOAD" || "$COMMAND" == "ROLLBACK" ) ]]; then
    section "Waiting for reload confirmation (timeout: ${WAIT_TIMEOUT}s)"

    if ! docker ps --format '{{.Names}}' | grep -qx "$TM_CONTAINER"; then
        warn "TaskManager container '$TM_CONTAINER' is not running — skipping log wait."
        exit 0
    fi

    FAILURE_PATTERN="SHA-256 mismatch|smoke.test failed|reload failed|Download failed"
    COMBINED_PATTERN="${SUCCESS_PATTERN}|${FAILURE_PATTERN}"

    info "Tailing '$TM_CONTAINER' logs…"
    info "Success pattern : $SUCCESS_PATTERN"
    info "Failure pattern : $FAILURE_PATTERN"
    echo ""

    ELAPSED=0
    INTERVAL=2
    MATCHED=""

    while [[ $ELAPSED -lt $WAIT_TIMEOUT ]]; do
        MATCHED=$(docker logs --tail 300 "$TM_CONTAINER" 2>&1 \
            | grep -E "$COMBINED_PATTERN" | tail -5 || true)

        if [[ -n "$MATCHED" ]]; then
            echo ""
            if echo "$MATCHED" | grep -qE "$SUCCESS_PATTERN"; then
                info "✓ Reload confirmed:"
                echo "$MATCHED" | sed "s/^/  /"
                echo ""
                exit 0
            else
                error "✗ Reload failed (error detected in logs):"
                echo "$MATCHED" | sed "s/^/  /"
                echo ""
                exit 1
            fi
        fi

        printf "  Waiting... %ds elapsed\r" "$ELAPSED"
        sleep "$INTERVAL"
        ELAPSED=$(( ELAPSED + INTERVAL ))
    done

    echo ""
    warn "Timeout after ${WAIT_TIMEOUT}s — reload not yet confirmed."
    warn "The download may still be in progress. Check logs manually:"
    echo ""
    echo "  docker compose logs --tail 100 $TM_CONTAINER | grep -E 'Archive|SMOKE|reload'"
    echo ""
    exit 1
fi
