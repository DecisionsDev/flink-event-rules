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
# ccloud-job-status.sh
# Inspects statement execution status, CFU utilization, and error diagnostics
# for Confluent Cloud Flink pipelines.
#
# Usage:
#   ./scripts/ccloud/ccloud-job-status.sh <use-case> [OPTIONS]
#
# Options:
#   --all               List all statements in the compute pool (ignores use-case filter)
#   --details           Display full statement JSON output
#   --environment <id>  Environment ID (overrides context/env)
#   --compute-pool <id> Compute Pool ID (overrides context/env)
#   -h, --help          Show help message
#
# Examples:
#   ./scripts/ccloud/ccloud-job-status.sh banking-fraud-agent
#   ./scripts/ccloud/ccloud-job-status.sh --all
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
# shellcheck source=scripts/ccloud/ccloud-common.sh
source "${SCRIPT_DIR}/ccloud-common.sh"

USE_CASE_ARG=""
SHOW_ALL=false
SHOW_DETAILS=false
ENV_OVERRIDE=""
POOL_OVERRIDE=""

usage() {
    cat <<EOF
Usage: $0 [use-case] [OPTIONS]

Options:
  --all               List all statements in the compute pool (ignores use-case filter)
  --details           Display full statement JSON output
  --environment <id>  Environment ID (overrides context/env)
  --compute-pool <id> Compute Pool ID (overrides context/env)
  -h, --help          Show help message

Examples:
  $0 banking-fraud-agent
  $0 samples/retail-agent
  $0 --all
EOF
    exit "${1:-0}"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --all)              SHOW_ALL=true;           shift   ;;
        --details)          SHOW_DETAILS=true;       shift   ;;
        --environment)      ENV_OVERRIDE="$2";       shift 2 ;;
        --compute-pool)     POOL_OVERRIDE="$2";      shift 2 ;;
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

if [[ "$SHOW_ALL" == "false" && -z "$USE_CASE_ARG" ]]; then
    error "No use case specified. Specify a use case or use --all."
    usage 1
fi

if [[ -n "$USE_CASE_ARG" ]]; then
    resolve_use_case "$USE_CASE_ARG"
fi

validate_ccloud_context
check_required_tools jq

[[ -n "$ENV_OVERRIDE" ]]  && CCLOUD_ENV_ID="$ENV_OVERRIDE"
[[ -n "$POOL_OVERRIDE" ]] && CCLOUD_COMPUTE_POOL_ID="$POOL_OVERRIDE"

section "Confluent Cloud Flink Status"
print_ccloud_context

if [[ -z "$CCLOUD_ENV_ID" || -z "$CCLOUD_COMPUTE_POOL_ID" ]]; then
    error "Environment ID and Compute Pool ID are required."
    exit 1
fi

# ── Compute Pool Health ────────────────────────────────────────────────────────
section "Compute Pool Summary"
POOL_JSON="$(confluent flink compute-pool describe "$CCLOUD_COMPUTE_POOL_ID" \
    --environment "$CCLOUD_ENV_ID" \
    -o json 2>/dev/null || true)"

if [[ -n "$POOL_JSON" ]]; then
    POOL_NAME="$(echo "$POOL_JSON" | jq -r '.name // .display_name // "n/a"')"
    POOL_STATUS="$(echo "$POOL_JSON" | jq -r '.status // .phase // "n/a"')"
    POOL_CFU="$(echo "$POOL_JSON" | jq -r '.current_cfu // .cfu // "n/a"')"
    POOL_MAX_CFU="$(echo "$POOL_JSON" | jq -r '.max_cfu // "n/a"')"

    echo "  Pool ID       : ${CCLOUD_COMPUTE_POOL_ID}"
    echo "  Pool Name     : ${POOL_NAME}"
    echo "  Status        : ${POOL_STATUS}"
    echo "  Current CFU   : ${POOL_CFU} / max ${POOL_MAX_CFU}"
else
    warn "Could not fetch details for compute pool ${CCLOUD_COMPUTE_POOL_ID}"
fi
echo ""

# ── Query Statements ───────────────────────────────────────────────────────────
section "Flink Statements"
STATEMENTS_JSON="$(confluent flink statement list \
    --compute-pool "$CCLOUD_COMPUTE_POOL_ID" \
    --environment "$CCLOUD_ENV_ID" \
    -o json 2>/dev/null || true)"

if [[ -z "$STATEMENTS_JSON" || "$STATEMENTS_JSON" == "[]" ]]; then
    info "No statements found in compute pool ${CCLOUD_COMPUTE_POOL_ID}."
    exit 0
fi

# Filter statements
FILTERED_JSON="$STATEMENTS_JSON"
if [[ "$SHOW_ALL" == "false" && -n "${USE_CASE_NAME:-}" ]]; then
    FILTERED_JSON="$(echo "$STATEMENTS_JSON" | jq -c --arg prefix "$USE_CASE_NAME" '[.[] | select((.name // .statement_name // "") | startswith($prefix))]')"
fi

STMT_COUNT="$(echo "$FILTERED_JSON" | jq 'length')"
if [[ "$STMT_COUNT" -eq 0 ]]; then
    warn "No matching statements found for use case '${USE_CASE_NAME}'."
    echo "All statements in pool:"
    echo "$STATEMENTS_JSON" | jq -r '.[] | "  - \(.name // .statement_name // .id)"'
    exit 0
fi

info "Found ${STMT_COUNT} statement(s):"
echo ""

for i in $(seq 0 $((STMT_COUNT - 1))); do
    STMT="$(echo "$FILTERED_JSON" | jq -c ".[$i]")"
    STMT_NAME="$(echo "$STMT" | jq -r '.name // .statement_name // .id')"
    STMT_PHASE="$(echo "$STMT" | jq -r '.status.phase // .status // .phase // "UNKNOWN"')"

    case "$STMT_PHASE" in
        RUNNING|COMPLETED)
            echo -e "  [${GREEN}${STMT_PHASE}${NC}] ${BOLD}${STMT_NAME}${NC}"
            ;;
        PENDING|STARTING)
            echo -e "  [${YELLOW}${STMT_PHASE}${NC}] ${BOLD}${STMT_NAME}${NC}"
            ;;
        FAILED|STOPPED)
            echo -e "  [${RED}${STMT_PHASE}${NC}] ${BOLD}${STMT_NAME}${NC}"
            ;;
        *)
            echo -e "  [${CYAN}${STMT_PHASE}${NC}] ${BOLD}${STMT_NAME}${NC}"
            ;;
    esac

    if [[ "$SHOW_DETAILS" == "true" ]]; then
        echo "$STMT" | jq . | sed 's/^/    /'
    else
        DETAIL="$(confluent flink statement describe "$STMT_NAME" \
            --compute-pool "$CCLOUD_COMPUTE_POOL_ID" \
            --environment "$CCLOUD_ENV_ID" \
            -o json 2>/dev/null || true)"
        if [[ -n "$DETAIL" ]]; then
            DETAIL_MSG="$(echo "$DETAIL" | jq -r '.status.detail // .status.message // empty')"
            [[ -n "$DETAIL_MSG" ]] && echo "      Detail : $DETAIL_MSG"
        fi
    fi
    echo ""
done
