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
# validate-deployment.sh
#
# Validates all prerequisites for a Flink job deployment.
# When called with no option, validates the banking-fraud-agent use case.
#
# Usage:
#   bash scripts/validate-deployment.sh                          # validates banking-fraud-agent
#   bash scripts/validate-deployment.sh --banking-fraud-agent   # same, explicit
#
# Exit codes:
#   0  All required checks passed for every validated use case
#   1  One or more required checks failed

set -uo pipefail
# Note: -e (errexit) is intentionally omitted.  Validation scripts probe things
# that are expected to fail; every failure path is handled explicitly via || guards
# and the OVERALL_FAILED flag.  Using -e would cause the script to exit silently
# mid-run whenever a tested service is not yet running.

# ── Colour helpers ─────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
success() { echo -e "${GREEN}✓${NC} $1"; }
error()   { echo -e "${RED}✗${NC} $1"; SECTION_FAILED=true; OVERALL_FAILED=true; }
warning() { echo -e "${YELLOW}⚠${NC} $1"; }
info()    { echo -e "  ${CYAN}ℹ${NC} $1"; }
section() { echo ""; echo "$1"; printf '%.0s-' {1..50}; echo ""; }
header()  { echo ""; echo "=========================================="; echo " $1"; echo "=========================================="; }

# ── Argument parsing ───────────────────────────────────────────────────────────
USE_CASES=()
for arg in "$@"; do
    case "$arg" in
        --banking-fraud-agent) USE_CASES+=("banking-fraud-agent") ;;
        --help|-h)
            echo "Usage: $0 [--banking-fraud-agent]"
            echo "  No options = validate banking-fraud-agent use case."
            exit 0 ;;
        *) echo "Unknown option: $arg"; echo "Usage: $0 [--banking-fraud-agent]"; exit 1 ;;
    esac
done

# Default: banking-fraud-agent when no flag is provided
if [[ ${#USE_CASES[@]} -eq 0 ]]; then
    USE_CASES=("banking-fraud-agent")
fi

# ── Fixed infrastructure constants ────────────────────────────────────────────
KAFKA_CONTAINER="kafka"
# kafka:29092 is the container-internal listener (used inside docker exec).
# localhost:9092 is the host-side listener (not reachable from within containers).
KAFKA_BOOTSTRAP="kafka:29092"
FLINK_JOBMANAGER="event-rule-backend-jobmanager"
FLINK_TASKMANAGER="event-rule-backend-taskmanager"
BFF_CONTAINER="event-rule-bff"

# ── Global failure flag (accumulates across all use cases) ────────────────────
OVERALL_FAILED=false

# ── Helper: parse a single field from a descriptor JSON ───────────────────────
parse_descriptor() {
    local file="$1" query="$2"
    if command -v jq &>/dev/null; then
        jq -r "$query" "$file" 2>/dev/null || echo ""
    elif command -v python3 &>/dev/null; then
        python3 -c "
import json
data = json.load(open('$file'))
keys = '$query'.lstrip('.').split('.')
v = data
for k in keys:
    v = v.get(k, '') if isinstance(v, dict) else ''
print(v if v is not None else '')
" 2>/dev/null || echo ""
    else
        echo ""
    fi
}

# ── Shared: infrastructure checks (run once, before per-use-case checks) ──────
validate_infrastructure() {
    header "Shared Infrastructure Checks"

    # ── Containers ────────────────────────────────────────────────────────────
    section "Docker containers"
    SECTION_FAILED=false

    check_container() {
        local name="$1" label="$2" required="${3:-true}"
        if docker ps --format '{{.Names}}' 2>/dev/null | grep -q "^${name}$"; then
            success "$label ($name) is running"
        elif [[ "$required" == "true" ]]; then
            error "$label ($name) is not running"
        else
            warning "$label ($name) is not running (optional)"
        fi
    }

    check_container "$KAFKA_CONTAINER"    "Kafka broker"
    check_container "$FLINK_JOBMANAGER"   "Flink JobManager"
    check_container "$FLINK_TASKMANAGER"  "Flink TaskManager"
    check_container "$BFF_CONTAINER"      "BFF server"       false
    check_container "event-rule-frontend" "Frontend"         false

    # ── Kafka health ──────────────────────────────────────────────────────────
    section "Kafka broker health"
    SECTION_FAILED=false

    if docker ps --filter "name=${KAFKA_CONTAINER}" --format '{{.Status}}' 2>/dev/null \
            | grep -q "healthy"; then
        success "Kafka container health: healthy"
    else
        warning "Kafka container not yet reporting healthy (may still be starting)"
    fi

    if docker ps --format '{{.Names}}' 2>/dev/null | grep -q "^${KAFKA_CONTAINER}$"; then
        if docker exec "${KAFKA_CONTAINER}" kafka-broker-api-versions \
                --bootstrap-server "${KAFKA_BOOTSTRAP}" &>/dev/null 2>&1; then
            success "Kafka broker API reachable (${KAFKA_BOOTSTRAP})"
        else
            error "Kafka broker API not reachable (${KAFKA_BOOTSTRAP})"
        fi
    fi

    # ── Flink cluster ─────────────────────────────────────────────────────────
    section "Flink cluster"
    SECTION_FAILED=false

    FLINK_OVERVIEW=""
    if docker ps --format '{{.Names}}' 2>/dev/null | grep -q "^${FLINK_JOBMANAGER}$"; then
        FLINK_OVERVIEW=$(docker exec "${FLINK_JOBMANAGER}" \
            curl -sf http://localhost:8081/overview 2>/dev/null || echo "")
    fi

    if [[ -n "$FLINK_OVERVIEW" ]]; then
        success "Flink JobManager REST API reachable (port 8081)"
        TM_COUNT=$(echo "$FLINK_OVERVIEW" | grep -o '"taskmanagers":[0-9]*'  | cut -d: -f2 || echo "0")
        SLOTS_TOTAL=$(echo "$FLINK_OVERVIEW" | grep -o '"slots-total":[0-9]*' | cut -d: -f2 || echo "0")
        SLOTS_AVAIL=$(echo "$FLINK_OVERVIEW" | grep -o '"slots-available":[0-9]*' | cut -d: -f2 || echo "0")
        FLINK_VER=$(echo "$FLINK_OVERVIEW" | grep -o '"flink-version":"[^"]*"' | cut -d'"' -f4 || echo "?")
        info "Flink version    : $FLINK_VER"
        if [[ "${TM_COUNT:-0}" -gt 0 ]]; then
            success "TaskManagers registered: $TM_COUNT"
        else
            error "No TaskManagers registered with JobManager"
        fi
        info "Slots total      : ${SLOTS_TOTAL:-0}  |  available: ${SLOTS_AVAIL:-0}"
        if [[ "${SLOTS_AVAIL:-0}" -eq 0 ]]; then
            warning "No task slots available (a job may already occupy all slots)"
        fi
    else
        error "Flink JobManager REST API not reachable (http://localhost:8081)"
        info "Hint: docker compose up -d jobmanager taskmanager"
    fi

    # Expose FLINK_OVERVIEW to the per-use-case validator
    export FLINK_OVERVIEW
}

# ── Per-use-case validation ────────────────────────────────────────────────────
#
# Each use case is described by a single agents.json file: the agents themselves
# (loaded by AgentBasedDataStreamJob via AGENT_DESCRIPTOR_PATH) plus the top-level
# BFF metadata (name, flinkJobName, topics, outboundTopics — used by the BFF for
# tile-visibility and topic discovery).
# Reload is handled via broadcast-reload.sh (Kafka broadcast state).

validate_use_case() {
    local use_case="$1"
    local descriptor_path="samples/${use_case}/agents.json"
    SECTION_FAILED=false

    header "Use Case: $(echo "$use_case" | tr '[:lower:]' '[:upper:]')"

    # ── Step 1: Descriptor integrity ──────────────────────────────────────────
    section "Step 1: Descriptor integrity"
    SECTION_FAILED=false

    if [[ ! -f "$descriptor_path" ]]; then
        error "Descriptor not found: $descriptor_path"
        return
    fi
    success "Descriptor found: $descriptor_path"

    local display_name flink_job_name consumer_group
    display_name=$(parse_descriptor   "$descriptor_path" ".name")
    flink_job_name=$(parse_descriptor "$descriptor_path" ".flinkJobName")
    consumer_group=$(parse_descriptor "$descriptor_path" ".consumerGroupId")

    # Inbound topics (bash 3 compatible — no mapfile)
    local inbound_topics=()
    if command -v jq &>/dev/null; then
        while IFS= read -r t; do [[ -n "$t" ]] && inbound_topics+=("$t"); done \
            < <(jq -r '.topics[].name' "$descriptor_path" 2>/dev/null)
    elif command -v python3 &>/dev/null; then
        while IFS= read -r t; do [[ -n "$t" ]] && inbound_topics+=("$t"); done \
            < <(python3 -c "
import json
data = json.load(open('$descriptor_path'))
for t in data.get('topics', []):
    print(t['name'])
" 2>/dev/null)
    fi

    # Outbound topics (top-level outboundTopics array in agents.json)
    local outbound_topics=()
    if command -v jq &>/dev/null; then
        while IFS= read -r t; do [[ -n "$t" ]] && outbound_topics+=("$t"); done \
            < <(jq -r '.outboundTopics[]?.name' "$descriptor_path" 2>/dev/null)
    fi

    if [[ -n "$display_name" ]]; then
        success "Display name     : $display_name"
    else
        error "Descriptor missing 'name' field"
    fi

    if [[ -n "$flink_job_name" && "$flink_job_name" != "null" ]]; then
        success "Flink job name   : $flink_job_name"
    else
        warning "No 'flinkJobName' field — BFF tile-visibility probe will not filter this use case"
    fi

    if [[ ${#inbound_topics[@]} -gt 0 ]]; then
        success "Inbound topics   : ${inbound_topics[*]}"
    else
        error "No inbound topics defined in descriptor"
    fi

    if [[ ${#outbound_topics[@]} -gt 0 ]]; then
        success "Outbound topics  : ${outbound_topics[*]}"
    else
        warning "No outbound topics in descriptor (outboundTopics array missing or empty)"
    fi

    info "Consumer group   : ${consumer_group:-<not set>}"

    # ── Step 2: Agent descriptor (agents.json) ────────────────────────────────
    section "Step 2: Agent descriptor (agents.json)"
    SECTION_FAILED=false
    local agents_json="samples/${use_case}/agents.json"
    if [[ -f "$agents_json" ]]; then
        success "agents.json found: $agents_json"
    else
        error "agents.json not found: $agents_json"
        info "The AgentBasedDataStreamJob reads this file via AGENT_DESCRIPTOR_PATH"
    fi

    # ── Step 3: Kafka topics ──────────────────────────────────────────────────
    section "Step 3: Kafka topics"
    SECTION_FAILED=false
    local required_topics=("${inbound_topics[@]}")
    for ot in "${outbound_topics[@]}"; do required_topics+=("$ot"); done
    local existing_topics=""

    if docker ps --format '{{.Names}}' 2>/dev/null | grep -q "^${KAFKA_CONTAINER}$"; then
        existing_topics=$(docker exec "${KAFKA_CONTAINER}" \
            kafka-topics --bootstrap-server "${KAFKA_BOOTSTRAP}" --list 2>/dev/null || echo "")
    fi

    if [[ -z "$existing_topics" ]]; then
        error "Cannot retrieve topic list from Kafka"
    else
        local missing_topics=()
        for topic in "${required_topics[@]}"; do
            if echo "$existing_topics" | grep -qxF "$topic"; then
                success "Topic exists: $topic"
            else
                error "Topic missing: $topic"
                missing_topics+=("$topic")
            fi
        done
        if [[ ${#missing_topics[@]} -gt 0 ]]; then
            info "Create missing topics: bash scripts/create-topics.sh ${missing_topics[*]}"
        fi
        if [[ -n "$consumer_group" ]]; then
            local cg_state
            cg_state=$(docker exec "${KAFKA_CONTAINER}" \
                kafka-consumer-groups --bootstrap-server "${KAFKA_BOOTSTRAP}" \
                --describe --group "${consumer_group}" 2>/dev/null | head -3 || echo "")
            if echo "$cg_state" | grep -q "TOPIC"; then
                info "Consumer group '${consumer_group}' has committed offsets"
            else
                info "Consumer group '${consumer_group}' has no committed offsets yet"
            fi
        fi
    fi

    # ── Step 4: Running Flink job ─────────────────────────────────────────────
    section "Step 4: Running Flink job"
    SECTION_FAILED=false

    # Match against flinkJobName when present, otherwise fall back to display name
    local match_name="${flink_job_name:-$display_name}"

    if [[ -z "${FLINK_OVERVIEW:-}" ]]; then
        warning "Flink cluster unreachable — skipping job check"
    else
        local jobs_json
        jobs_json=$(docker exec "${FLINK_JOBMANAGER}" \
            curl -sf http://localhost:8081/jobs 2>/dev/null || echo "")

        if [[ -z "$jobs_json" ]]; then
            warning "Could not query Flink jobs API"
        else
            local running_count failed_count total_count
            running_count=$(echo "$jobs_json" | grep -o '"status":"RUNNING"' | wc -l | tr -d ' ')
            failed_count=$(echo "$jobs_json"  | grep -o '"status":"FAILED"'  | wc -l | tr -d ' ')
            total_count=$(echo "$jobs_json"   | grep -o '"id":"'             | wc -l | tr -d ' ')
            info "Jobs — total: ${total_count}  running: ${running_count}  failed: ${failed_count}"

            if [[ "${running_count:-0}" -gt 0 ]]; then
                success "Flink job(s) running"
                local jobs_detail
                jobs_detail=$(docker exec "${FLINK_JOBMANAGER}" \
                    curl -sf http://localhost:8081/jobs/overview 2>/dev/null || echo "")
                if [[ -n "$jobs_detail" && -n "$match_name" ]]; then
                    if echo "$jobs_detail" | grep -qF "$match_name"; then
                        success "Active job matches: \"$match_name\""
                    else
                        warning "No running job matched: \"$match_name\""
                        info "Running jobs:"
                        echo "$jobs_detail" | grep -o '"name":"[^"]*"' | cut -d'"' -f4 | \
                            while IFS= read -r jn; do info "  $jn"; done
                    fi
                fi
            elif [[ "${failed_count:-0}" -gt 0 ]]; then
                error "One or more Flink jobs are in FAILED state"
                info "Check: docker logs ${FLINK_JOBMANAGER} --tail 50"
            else
                info "No running Flink job yet"
                info "Submit with: bash samples/${use_case}/scripts/deploy.sh"
            fi
        fi
    fi

    # ── Step 5: Dynamic reload ────────────────────────────────────────────────
    section "Step 5: Dynamic archive reload"
    SECTION_FAILED=false

    success "Reload mechanism : Kafka broadcast (broadcast-reload.sh)"
    info "Deploy a new archive at runtime:"
    info "  bash scripts/broadcast-reload.sh --use-case ${use_case} --jar /path/to/new.jar"
}

# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════
echo ""
echo "=========================================="
echo " Flink Deployment Validation"
echo " Validating: ${USE_CASES[*]}"
echo "=========================================="

# Shared infrastructure checks run once regardless of the number of use cases
validate_infrastructure

# Per-use-case checks
for uc in "${USE_CASES[@]}"; do
    validate_use_case "$uc"
done

# ── Final summary ──────────────────────────────────────────────────────────────
echo ""
echo "=========================================="
echo " Validation Summary — ${USE_CASES[*]}"
echo "=========================================="

if [[ "$OVERALL_FAILED" == "false" ]]; then
    echo -e "${GREEN}"
    echo " ✓ All required checks passed!"
    echo -e "${NC}"
    for uc in "${USE_CASES[@]}"; do
        echo "   Start ${uc}  :  bash samples/${uc}/scripts/deploy.sh"
    done
    echo "   Logs          :  bash run.sh logs"
    echo "   Stop          :  bash run.sh down"
    exit 0
else
    echo -e "${RED}"
    echo " ✗ One or more required checks failed — see errors above."
    echo -e "${NC}"
    echo " Common fixes:"
    echo "   bash scripts/create-topics.sh <missing-topic>   (create missing topics)"
    echo "   docker compose ps                                (check container health)"
    echo "   bash run.sh logs                                 (view logs)"
    echo ""
    exit 1
fi

