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
# ccloud-launch-demo-scenario.sh
# Reads samples/<use-case>/demo-scenarios.json and publishes events to Confluent Cloud Kafka.
#
# Usage:
#   ./scripts/ccloud/ccloud-launch-demo-scenario.sh <use-case> [scenario-id] [OPTIONS]
#
# Options:
#   --delay <secs>      Seconds to wait between sending events (default: 2)
#   --cluster <id>      Kafka Cluster ID (overrides context/env)
#   --environment <id>  Environment ID (overrides context/env)
#   --dry-run           Print events that would be sent without publishing
#   -h, --help          Show help message
#
# Examples:
#   ./scripts/ccloud/ccloud-launch-demo-scenario.sh banking-fraud-agent
#   ./scripts/ccloud/ccloud-launch-demo-scenario.sh banking-fraud-agent account-creation
#   ./scripts/ccloud/ccloud-launch-demo-scenario.sh retail-agent high-value-purchase --delay 1
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
# shellcheck source=scripts/ccloud/ccloud-common.sh
source "${SCRIPT_DIR}/ccloud-common.sh"

USE_CASE_ARG=""
SCENARIO_ID=""
DELAY=2
DRY_RUN=false
CLUSTER_OVERRIDE=""
ENV_OVERRIDE=""

usage() {
    cat <<EOF
Usage: $0 <use-case> [scenario-id] [OPTIONS]

Options:
  --delay <secs>      Seconds to wait between sending events (default: 2)
  --cluster <id>      Kafka Cluster ID (overrides context/env)
  --environment <id>  Environment ID (overrides context/env)
  --dry-run           Print events that would be sent without publishing
  -h, --help          Show help message

Examples:
  $0 banking-fraud-agent
  $0 banking-fraud-agent account-creation
  $0 retail-agent high-value-purchase --delay 1
EOF
    exit "${1:-0}"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --delay)            DELAY="$2";              shift 2 ;;
        --cluster)          CLUSTER_OVERRIDE="$2";   shift 2 ;;
        --environment)      ENV_OVERRIDE="$2";       shift 2 ;;
        --dry-run)          DRY_RUN=true;            shift   ;;
        -h|--help)          usage 0 ;;
        -*)                 error "Unknown option: $1"; usage 1 ;;
        *)
            if [[ -z "$USE_CASE_ARG" ]]; then
                USE_CASE_ARG="$1"
                shift
            elif [[ -z "$SCENARIO_ID" ]]; then
                SCENARIO_ID="$1"
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
    check_required_tools confluent jq
fi

[[ -n "$CLUSTER_OVERRIDE" ]] && CCLOUD_CLUSTER_ID="$CLUSTER_OVERRIDE"
[[ -n "$ENV_OVERRIDE" ]]     && CCLOUD_ENV_ID="$ENV_OVERRIDE"

SCENARIOS_FILE="${SAMPLE_DIR}/demo-scenarios.json"
if [[ ! -f "$SCENARIOS_FILE" ]]; then
    error "demo-scenarios.json not found in ${SAMPLE_DIR}"
    exit 1
fi

section "Launching Demo Scenarios on Confluent Cloud — ${USE_CASE_NAME}"
print_ccloud_context

if [[ -z "$CCLOUD_CLUSTER_ID" || -z "$CCLOUD_ENV_ID" ]]; then
    error "Kafka Cluster ID and Environment ID are required."
    exit 1
fi

# Topic resolver helper
resolve_topic() {
    local event_type="$1"
    local topic=""

    # 1. Top-level topics
    if [[ -f "$DESCRIPTOR_PATH" ]]; then
        topic="$(jq -r --arg type "$event_type" '.topics[]? | select(.type == $type) | .name' "$DESCRIPTOR_PATH" 2>/dev/null || true)"
    fi

    # 2. Subscriptions
    if [[ -z "$topic" && -f "$DESCRIPTOR_PATH" ]]; then
        topic="$(jq -r --arg type "$event_type" '.agents[]?.subscriptions[]? | select(.eventType == $type) | .topic' "$DESCRIPTOR_PATH" 2>/dev/null | head -n 1 || true)"
    fi

    echo "$topic"
}

run_scenario() {
    local id="$1"
    local label
    local desc
    label="$(jq -r --arg id "$id" '.[] | select(.id == $id) | .label // $id' "$SCENARIOS_FILE")"
    desc="$(jq -r --arg id "$id" '.[] | select(.id == $id) | .description // ""' "$SCENARIOS_FILE")"

    echo "========================================================================="
    echo -e "🚀 ${BOLD}Scenario:${NC} ${label} (${id})"
    [[ -n "$desc" ]] && echo -e "   ${CYAN}Description:${NC} ${desc}"
    echo "========================================================================="
    echo ""

    local has_events
    has_events="$(jq --arg id "$id" '.[] | select(.id == $id) | .events? | length' "$SCENARIOS_FILE" 2>/dev/null || echo "0")"
    if [[ "$has_events" == "0" || -z "$has_events" ]]; then
        warn "No events found in scenario '$id'."
        return 0
    fi

    local count=0
    while IFS= read -r event_json; do
        [[ -z "$event_json" ]] && continue
        count=$((count + 1))

        local event_type
        local event_topic
        local event_payload
        event_type="$(echo "$event_json" | jq -r '.type // empty')"
        event_topic="$(echo "$event_json" | jq -r '.topic // empty')"
        event_payload="$(echo "$event_json" | jq -c '.payload // empty')"

        if [[ -z "$event_topic" && -n "$event_type" ]]; then
            event_topic="$(resolve_topic "$event_type")"
        fi

        if [[ -z "$event_topic" ]]; then
            error "Could not resolve Kafka topic for event type '${event_type}'"
            echo "  Payload: ${event_payload}"
            continue
        fi

        info "[$count] Sending '${event_type}' to topic '${event_topic}'..."

        if [[ "$DRY_RUN" == "true" ]]; then
            echo -e "  [DRY-RUN] ${event_payload}"
        else
            echo "$event_payload" | confluent kafka topic produce "$event_topic" \
                --cluster "$CCLOUD_CLUSTER_ID" \
                --environment "$CCLOUD_ENV_ID"
            success "  ✓ Sent: ${event_payload}"
        fi

        if [[ "$DRY_RUN" == "false" && "$DELAY" -gt 0 ]]; then
            sleep "$DELAY"
        fi
    done < <(jq -c -r --arg id "$id" '.[] | select(.id == $id) | .events[]' "$SCENARIOS_FILE")

    echo ""
    success "🎉 Scenario '${id}' completed!"
    echo ""
}

# Determine scenario(s) to run
if [[ -n "$SCENARIO_ID" && "$SCENARIO_ID" != "all" ]]; then
    EXISTS="$(jq -r --arg id "$SCENARIO_ID" '.[] | select(.id == $id) | .id' "$SCENARIOS_FILE")"
    if [[ -z "$EXISTS" ]]; then
        error "Scenario '${SCENARIO_ID}' not found in ${SCENARIOS_FILE}"
        echo "Available scenarios:"
        jq -r '.[] | "  - \(.id) (\(.label // ""))"' "$SCENARIOS_FILE"
        exit 1
    fi
    run_scenario "$SCENARIO_ID"
else
    ALL_SCENARIOS="$(jq -r '.[].id' "$SCENARIOS_FILE")"
    if [[ -z "$ALL_SCENARIOS" ]]; then
        warn "No scenarios found in ${SCENARIOS_FILE}"
        exit 0
    fi
    info "Executing all demo scenarios from ${SCENARIOS_FILE}..."
    echo ""
    for s_id in $ALL_SCENARIOS; do
        run_scenario "$s_id"
    done
    success "All demo scenarios executed successfully!"
fi
