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
# ccloud-publish-ruleset.sh
# Publishes a Decision Intelligence ruleset archive directly to the archive-control
# Kafka topic in Confluent Cloud, enabling zero-mount dynamic ruleset updates.
#
# Supports single-message base64 payloads and chunking for archives larger than 1 MB.
#
# Usage:
#   ./scripts/ccloud/ccloud-publish-ruleset.sh <use-case> --jar <path> [OPTIONS]
#
# Options:
#   --jar <path>        Path to the ruleset archive (.jar) on host
#   --command <type>    LOAD | PREFETCH | ROLLBACK | NOOP  (default: LOAD)
#   --version <label>   Version label (default: derived from JAR filename)
#   --request-id <id>   Correlation ID (default: auto-generated)
#   --topic <name>      Override the archive-control topic name
#   --chunk-size <n>    Max chunk payload size in bytes (default: 786432 ~768KB base64 chunk)
#   --cluster <id>      Kafka Cluster ID (overrides context/env)
#   --environment <id>  Environment ID (overrides context/env)
#   --dry-run           Print message payload and chunk plan without sending
#   -h, --help          Show help message
#
# Examples:
#   ./scripts/ccloud/ccloud-publish-ruleset.sh banking-fraud-agent \
#       --jar samples/banking-fraud-agent/Fraud_Detection_Service_1.6.0.jar
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
# shellcheck source=scripts/ccloud/ccloud-common.sh
source "${SCRIPT_DIR}/ccloud-common.sh"

USE_CASE_ARG=""
NEW_JAR=""
COMMAND="LOAD"
VERSION=""
REQUEST_ID="ccloud-reload-$(date +%s)"
TOPIC_OVERRIDE=""
CHUNK_SIZE=786432 # 768 KB raw chunk size (~1MB base64)
DRY_RUN=false
CLUSTER_OVERRIDE=""
ENV_OVERRIDE=""

usage() {
    cat <<EOF
Usage: $0 <use-case> --jar <path> [OPTIONS]

Options:
  --jar <path>        Path to the ruleset archive (.jar) on host
  --command <type>    LOAD | PREFETCH | ROLLBACK | NOOP  (default: LOAD)
  --version <label>   Version label (default: derived from JAR filename)
  --request-id <id>   Correlation ID (default: auto-generated)
  --topic <name>      Override the archive-control topic name
  --chunk-size <n>    Max chunk payload size in bytes (default: 786432)
  --cluster <id>      Kafka Cluster ID (overrides context/env)
  --environment <id>  Environment ID (overrides context/env)
  --dry-run           Print message payload and chunk plan without sending
  -h, --help          Show help message

Examples:
  $0 banking-fraud-agent --jar samples/banking-fraud-agent/Fraud_Detection_Service_1.6.0.jar
  $0 retail-agent --jar samples/retail-agent/RetailPromotionService_1.6.0.jar --command PREFETCH
EOF
    exit "${1:-0}"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --jar)              NEW_JAR="$2";            shift 2 ;;
        --command)          COMMAND="$(echo "$2" | tr '[:lower:]' '[:upper:]')"; shift 2 ;;
        --version)          VERSION="$2";            shift 2 ;;
        --request-id)       REQUEST_ID="$2";         shift 2 ;;
        --topic)            TOPIC_OVERRIDE="$2";     shift 2 ;;
        --chunk-size)       CHUNK_SIZE="$2";         shift 2 ;;
        --cluster)          CLUSTER_OVERRIDE="$2";   shift 2 ;;
        --environment)      ENV_OVERRIDE="$2";       shift 2 ;;
        --dry-run)          DRY_RUN=true;            shift   ;;
        -h|--help)          usage 0 ;;
        -*)                 error "Unknown option: $1"; usage 1 ;;
        *)
            if [[ -z "$USE_CASE_ARG" ]]; then
                USE_CASE_ARG="$1"
                shift
            else
                error "Unexpected argument: $1"
                usage 1
            fi
            ;;
    esac
done

if [[ -z "$USE_CASE_ARG" ]]; then
    error "No use case specified."
    usage 1
fi

resolve_use_case "$USE_CASE_ARG"
validate_ccloud_context

if [[ "$DRY_RUN" == "false" ]]; then
    check_required_tools confluent jq base64
fi

[[ -n "$CLUSTER_OVERRIDE" ]] && CCLOUD_CLUSTER_ID="$CLUSTER_OVERRIDE"
[[ -n "$ENV_OVERRIDE" ]]     && CCLOUD_ENV_ID="$ENV_OVERRIDE"

section "Publishing Ruleset Archive to Confluent Cloud — ${USE_CASE_NAME}"
print_ccloud_context

if [[ -z "$CCLOUD_CLUSTER_ID" || -z "$CCLOUD_ENV_ID" ]]; then
    error "Kafka Cluster ID and Environment ID are required."
    exit 1
fi

# ── Resolve Topic ──────────────────────────────────────────────────────────────
CONTROL_TOPIC="$TOPIC_OVERRIDE"
if [[ -z "$CONTROL_TOPIC" ]]; then
    CONTROL_TOPIC="$(jq -r '[.agents[].ruleset.archiveControlTopic // empty][0] // empty' "$DESCRIPTOR_PATH" 2>/dev/null || true)"
fi
if [[ -z "$CONTROL_TOPIC" ]]; then
    CONTROL_TOPIC="agent-${USE_CASE_NAME}-archive-control"
fi

info "Archive Control Topic: ${CONTROL_TOPIC}"
info "Command Type          : ${COMMAND}"
info "Request ID            : ${REQUEST_ID}"

# ── NOOP Handler ───────────────────────────────────────────────────────────────
if [[ "$COMMAND" == "NOOP" ]]; then
    NOOP_JSON="{\"commandType\":\"NOOP\",\"version\":\"noop\",\"requestId\":\"${REQUEST_ID}\"}"
    if [[ "$DRY_RUN" == "true" ]]; then
        info "[DRY-RUN] Would publish NOOP payload:"
        echo "$NOOP_JSON"
        exit 0
    fi
    info "Publishing NOOP command..."
    echo "$NOOP_JSON" | confluent kafka topic produce "$CONTROL_TOPIC" \
        --cluster "$CCLOUD_CLUSTER_ID" \
        --environment "$CCLOUD_ENV_ID"
    success "✓ NOOP published to ${CONTROL_TOPIC}"
    exit 0
fi

# ── Validate JAR Path ──────────────────────────────────────────────────────────
if [[ -z "$NEW_JAR" ]]; then
    # Try default ruleset from agents.json
    DECLARED_JAR="$(jq -r '[.agents[].ruleset.path // empty][0] // empty' "$DESCRIPTOR_PATH" 2>/dev/null || true)"
    if [[ -n "$DECLARED_JAR" && -f "${PROJECT_ROOT}/${DECLARED_JAR}" ]]; then
        NEW_JAR="${PROJECT_ROOT}/${DECLARED_JAR}"
        info "Inferred ruleset JAR from descriptor: ${NEW_JAR}"
    elif [[ -n "$DECLARED_JAR" && -f "$DECLARED_JAR" ]]; then
        NEW_JAR="$DECLARED_JAR"
    fi
fi

if [[ -z "$NEW_JAR" || ! -f "$NEW_JAR" ]]; then
    error "JAR not found: ${NEW_JAR:-<unspecified>}"
    error "Provide a valid JAR with --jar <path>"
    exit 1
fi

if [[ -z "$VERSION" ]]; then
    VERSION="$(basename "$NEW_JAR" .jar)"
fi

# ── Compute SHA-256 ────────────────────────────────────────────────────────────
if command -v shasum &>/dev/null; then
    SHA256="$(shasum -a 256 "$NEW_JAR" | awk '{print $1}')"
elif command -v sha256sum &>/dev/null; then
    SHA256="$(sha256sum "$NEW_JAR" | awk '{print $1}')"
else
    error "Neither shasum nor sha256sum is available."
    exit 1
fi

JAR_SIZE=$(stat -f%z "$NEW_JAR" 2>/dev/null || stat -c%s "$NEW_JAR" 2>/dev/null || echo 0)
info "JAR File   : ${NEW_JAR} (${JAR_SIZE} bytes)"
info "Version    : ${VERSION}"
info "SHA-256    : ${SHA256}"
echo ""

# ── Single Message vs Chunking ─────────────────────────────────────────────────
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

if [[ "$JAR_SIZE" -le "$CHUNK_SIZE" ]]; then
    section "Encoding Single-Record Payload"
    B64_DATA="$(base64 < "$NEW_JAR" | tr -d '\r\n')"
    PAYLOAD_JSON="$(jq -n \
        --arg cmd "$COMMAND" \
        --arg ver "$VERSION" \
        --arg req "$REQUEST_ID" \
        --arg sha "$SHA256" \
        --arg b64 "$B64_DATA" \
        '{commandType: $cmd, version: $ver, requestId: $req, sha256: $sha, payloadBase64: $b64}')"

    if [[ "$DRY_RUN" == "true" ]]; then
        info "[DRY-RUN] Single message payload (size: ${#PAYLOAD_JSON} chars):"
        echo "$PAYLOAD_JSON" | jq '{commandType, version, requestId, sha256, payloadBase64: (.payloadBase64[:32] + "... [truncated]")}'
        exit 0
    fi

    info "Publishing single-record ruleset to topic '${CONTROL_TOPIC}'..."
    echo "$PAYLOAD_JSON" | confluent kafka topic produce "$CONTROL_TOPIC" \
        --cluster "$CCLOUD_CLUSTER_ID" \
        --environment "$CCLOUD_ENV_ID"

    success "✓ Ruleset archive published to ${CONTROL_TOPIC} (${COMMAND}, v=${VERSION})"
else
    section "Chunking Large Archive (> ${CHUNK_SIZE} bytes)"
    CHUNK_ID="chunk-${REQUEST_ID}"
    
    # Split file into raw chunks
    split -b "$CHUNK_SIZE" "$NEW_JAR" "${TMP_DIR}/part_"
    
    CHUNKS=()
    for f in "${TMP_DIR}"/part_*; do
        [[ -f "$f" ]] && CHUNKS+=("$f")
    done
    TOTAL_CHUNKS=${#CHUNKS[@]}
    info "Split archive into ${TOTAL_CHUNKS} chunk(s) of max ${CHUNK_SIZE} bytes."
    echo ""

    if [[ "$DRY_RUN" == "true" ]]; then
        info "[DRY-RUN] Would produce ${TOTAL_CHUNKS} chunks with chunkId: ${CHUNK_ID}"
        exit 0
    fi

    for idx in "${!CHUNKS[@]}"; do
        CHUNK_FILE="${CHUNKS[$idx]}"
        CHUNK_B64="$(base64 < "$CHUNK_FILE" | tr -d '\r\n')"
        CHUNK_JSON="$(jq -n \
            --arg cmd "$COMMAND" \
            --arg ver "$VERSION" \
            --arg req "$REQUEST_ID" \
            --arg sha "$SHA256" \
            --arg cid "$CHUNK_ID" \
            --argjson cidx "$idx" \
            --argjson tot "$TOTAL_CHUNKS" \
            --arg data "$CHUNK_B64" \
            '{commandType: $cmd, version: $ver, requestId: $req, sha256: $sha, chunkId: $cid, chunkIndex: $cidx, totalChunks: $tot, chunkData: $data}')"

        info "Publishing chunk [$((idx + 1))/$TOTAL_CHUNKS] (${#CHUNK_B64} base64 chars)..."
        echo "$CHUNK_JSON" | confluent kafka topic produce "$CONTROL_TOPIC" \
            --cluster "$CCLOUD_CLUSTER_ID" \
            --environment "$CCLOUD_ENV_ID"
    done

    echo ""
    success "✓ All ${TOTAL_CHUNKS} chunks published to ${CONTROL_TOPIC} (chunkId: ${CHUNK_ID}, v=${VERSION})"
fi
