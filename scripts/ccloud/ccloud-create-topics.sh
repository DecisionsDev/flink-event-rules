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
# ccloud-create-topics.sh
# Parses agents.json for a use case and creates all required Kafka topics in Confluent Cloud.
#
# Usage:
#   ./scripts/ccloud/ccloud-create-topics.sh <use-case> [OPTIONS]
#
# Options:
#   --partitions <n>    Default partition count for data topics (default: 3)
#   --cluster <id>      Kafka Cluster ID (overrides context/env)
#   --environment <id>  Environment ID (overrides context/env)
#   --dry-run           Display topics that would be created without creating them
#   -h, --help          Show help message
#
# Examples:
#   ./scripts/ccloud/ccloud-create-topics.sh banking-fraud-agent
#   ./scripts/ccloud/ccloud-create-topics.sh retail-agent --partitions 6
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
# shellcheck source=scripts/ccloud/ccloud-common.sh
source "${SCRIPT_DIR}/ccloud-common.sh"

USE_CASE_ARG=""
DEFAULT_PARTITIONS=3
DRY_RUN=false
CLUSTER_OVERRIDE=""
ENV_OVERRIDE=""

usage() {
    cat <<EOF
Usage: $0 <use-case> [OPTIONS]

Options:
  --partitions <n>    Default partition count for data topics (default: 3)
  --cluster <id>      Kafka Cluster ID (overrides context/env)
  --environment <id>  Environment ID (overrides context/env)
  --dry-run           Display topics that would be created without creating them
  -h, --help          Show this help message

Examples:
  $0 banking-fraud-agent
  $0 samples/retail-agent --partitions 6
EOF
    exit "${1:-0}"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --partitions)       DEFAULT_PARTITIONS="$2"; shift 2 ;;
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
    check_required_tools confluent jq
fi

[[ -n "$CLUSTER_OVERRIDE" ]] && CCLOUD_CLUSTER_ID="$CLUSTER_OVERRIDE"
[[ -n "$ENV_OVERRIDE" ]]     && CCLOUD_ENV_ID="$ENV_OVERRIDE"

section "Creating Confluent Cloud Kafka Topics — ${USE_CASE_NAME}"
print_ccloud_context

if [[ -z "$CCLOUD_CLUSTER_ID" || -z "$CCLOUD_ENV_ID" ]]; then
    error "Kafka Cluster ID and Environment ID are required."
    error "Set CCLOUD_CLUSTER_ID and CCLOUD_ENV_ID in .env.ccloud or pass via --cluster / --environment"
    exit 1
fi

# ── Extract Topics from Descriptor ─────────────────────────────────────────────
TOPICS_JSON="$(jq -c -r '
  [
    (.topics[]?.name // empty),
    (.agents[]?.subscriptions[]?.topic // empty),
    (.agents[]?.subscriptions[]?.topics[]? // empty),
    (.agents[]?.ruleset.archiveControlTopic // empty),
    (.agents[]?.outputs[]?.topic // empty),
    (.agents[]?.stateConfig.changeLogTopic // empty),
    (.agents[]?.errorHandling.deadLetterTopic // empty)
  ] | unique | map(select(length > 0))
' "$DESCRIPTOR_PATH")"

TOPICS=()
while IFS= read -r t; do
    [[ -n "$t" ]] && TOPICS+=("$t")
done < <(echo "$TOPICS_JSON" | jq -r '.[]')

if [[ ${#TOPICS[@]} -eq 0 ]]; then
    warn "No topics found in ${DESCRIPTOR_PATH}"
    exit 0
fi

info "Discovered ${#TOPICS[@]} topic(s) in descriptor:"
for t in "${TOPICS[@]}"; do
    echo "  - $t"
done
echo ""

if [[ "$DRY_RUN" == "true" ]]; then
    warn "[DRY-RUN] Would create the following topics in cluster ${CCLOUD_CLUSTER_ID}:"
    for t in "${TOPICS[@]}"; do
        if [[ "$t" == *"archive-control"* ]]; then
            echo "  - $t (partitions: 1 [control stream])"
        else
            echo "  - $t (partitions: ${DEFAULT_PARTITIONS})"
        fi
    done
    exit 0
fi

# ── Fetch Existing Topics ──────────────────────────────────────────────────────
info "Fetching existing topics in cluster ${CCLOUD_CLUSTER_ID}..."
EXISTING_TOPICS_LIST="$(confluent kafka topic list \
    --cluster "$CCLOUD_CLUSTER_ID" \
    --environment "$CCLOUD_ENV_ID" \
    -o json 2>/dev/null | jq -r '.[].name // empty' 2>/dev/null || true)"

# ── Create Topics ──────────────────────────────────────────────────────────────
CREATED_COUNT=0
SKIPPED_COUNT=0

for topic in "${TOPICS[@]}"; do
    if echo "$EXISTING_TOPICS_LIST" | grep -qx "$topic"; then
        info "Topic '$topic' already exists — skipping."
        SKIPPED_COUNT=$((SKIPPED_COUNT + 1))
        continue
    fi

    local_partitions="$DEFAULT_PARTITIONS"
    # Archive control topics require single partition for total ordering
    if [[ "$topic" == *"archive-control"* ]]; then
        local_partitions=1
        info "Control topic '$topic' will be created with 1 partition for ordered delivery."
    fi

    info "Creating topic '$topic' (partitions: ${local_partitions})..."
    if confluent kafka topic create "$topic" \
        --cluster "$CCLOUD_CLUSTER_ID" \
        --environment "$CCLOUD_ENV_ID" \
        --partitions "$local_partitions"; then
        CREATED_COUNT=$((CREATED_COUNT + 1))
        success "Topic '$topic' created."
    else
        warn "Could not create topic '$topic' (may already exist or insufficient permissions)."
    fi
    echo ""
done

section "Topic Creation Summary"
success "Done. Created: ${CREATED_COUNT}, Skipped (already existed): ${SKIPPED_COUNT}"
