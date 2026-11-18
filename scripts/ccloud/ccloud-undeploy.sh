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
# ccloud-undeploy.sh
# Stops and deletes all statements for a use case in Confluent Cloud Flink,
# and optionally cleans up the uploaded Flink artifact.
#
# Usage:
#   ./scripts/ccloud/ccloud-undeploy.sh <use-case> [OPTIONS]
#
# Options:
#   --delete-artifact   Also delete the uploaded Flink JAR artifact
#   --dry-run           Print statements that would be deleted without deleting
#   --environment <id>  Environment ID (overrides context/env)
#   --compute-pool <id> Compute Pool ID (overrides context/env)
#   --cloud <provider>  Cloud provider (default: aws)
#   --region <region>   Cloud region (default: us-east-1)
#   -h, --help          Show help message
#
# Examples:
#   ./scripts/ccloud/ccloud-undeploy.sh banking-fraud-agent
#   ./scripts/ccloud/ccloud-undeploy.sh retail-agent --delete-artifact
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
# shellcheck source=scripts/ccloud/ccloud-common.sh
source "${SCRIPT_DIR}/ccloud-common.sh"

USE_CASE_ARG=""
DELETE_ARTIFACT=false
DRY_RUN=false
ENV_OVERRIDE=""
POOL_OVERRIDE=""
CLOUD_OVERRIDE=""
REGION_OVERRIDE=""

usage() {
    cat <<EOF
Usage: $0 <use-case> [OPTIONS]

Options:
  --delete-artifact   Also delete the uploaded Flink JAR artifact
  --dry-run           Print statements that would be deleted without deleting
  --environment <id>  Environment ID (overrides context/env)
  --compute-pool <id> Compute Pool ID (overrides context/env)
  --cloud <provider>  Cloud provider (default: aws)
  --region <region>   Cloud region (default: us-east-1)
  -h, --help          Show help message

Examples:
  $0 banking-fraud-agent
  $0 samples/retail-agent --delete-artifact
EOF
    exit "${1:-0}"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --delete-artifact)  DELETE_ARTIFACT=true;    shift   ;;
        --dry-run)          DRY_RUN=true;            shift   ;;
        --environment)      ENV_OVERRIDE="$2";       shift 2 ;;
        --compute-pool)     POOL_OVERRIDE="$2";      shift 2 ;;
        --cloud)            CLOUD_OVERRIDE="$2";     shift 2 ;;
        --region)           REGION_OVERRIDE="$2";    shift 2 ;;
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
    check_required_tools confluent jq sed
fi

[[ -n "$ENV_OVERRIDE" ]]   && CCLOUD_ENV_ID="$ENV_OVERRIDE"
[[ -n "$POOL_OVERRIDE" ]]  && CCLOUD_COMPUTE_POOL_ID="$POOL_OVERRIDE"
[[ -n "$CLOUD_OVERRIDE" ]] && CCLOUD_CLOUD="$CLOUD_OVERRIDE"
[[ -n "$REGION_OVERRIDE" ]]&& CCLOUD_REGION="$REGION_OVERRIDE"

section "Undeploying Confluent Cloud Flink Pipeline — ${USE_CASE_NAME}"
print_ccloud_context

if [[ -z "$CCLOUD_ENV_ID" || -z "$CCLOUD_COMPUTE_POOL_ID" ]]; then
    error "Environment ID and Compute Pool ID are required."
    exit 1
fi

# ── Step 1: Discover Statements to Delete ──────────────────────────────────────
STATEMENTS_TO_DELETE=()

# 1a. If manifest exists, add statements in reverse order
if [[ -f "$MANIFEST_PATH" ]]; then
    STMT_COUNT=$(jq '.statements | length' "$MANIFEST_PATH")
    for (( i=STMT_COUNT-1; i>=0; i-- )); do
        STMT_OBJ="$(jq -c ".statements[$i]" "$MANIFEST_PATH")"
        ORDER="$(echo "$STMT_OBJ" | jq -r '.order')"
        KIND="$(echo "$STMT_OBJ" | jq -r '.kind')"
        NAME="$(echo "$STMT_OBJ" | jq -r '.name')"
        
        STMT_NAME="${USE_CASE_NAME}-${ORDER}-${KIND}-${NAME}"
        STMT_NAME="$(echo "$STMT_NAME" | sed -E 's/[^a-zA-Z0-9_-]+/-/g' | sed -E 's/^-+|-+$//g')"
        STATEMENTS_TO_DELETE+=("$STMT_NAME")
    done
fi

# 1b. Also query active statements matching prefix to ensure no orphans
ACTIVE_STMTS_JSON="$(confluent flink statement list \
    --compute-pool "$CCLOUD_COMPUTE_POOL_ID" \
    --environment "$CCLOUD_ENV_ID" \
    -o json 2>/dev/null || true)"

if [[ -n "$ACTIVE_STMTS_JSON" && "$ACTIVE_STMTS_JSON" != "[]" ]]; then
    MATCHED_NAMES=()
    while IFS= read -r s_name; do
        [[ -n "$s_name" ]] && MATCHED_NAMES+=("$s_name")
    done < <(echo "$ACTIVE_STMTS_JSON" | jq -r --arg prefix "$USE_CASE_NAME" '.[] | select((.name // .statement_name // "") | startswith($prefix)) | (.name // .statement_name // .id)')

    for m in "${MATCHED_NAMES[@]}"; do
        # Avoid duplicate entries
        ALREADY=false
        for existing in "${STATEMENTS_TO_DELETE[@]}"; do
            if [[ "$existing" == "$m" ]]; then
                ALREADY=true
                break
            fi
        done
        [[ "$ALREADY" == "false" ]] && STATEMENTS_TO_DELETE+=("$m")
    done
fi

if [[ ${#STATEMENTS_TO_DELETE[@]} -eq 0 ]]; then
    info "No active statements found for use case '${USE_CASE_NAME}'."
else
    info "Identified ${#STATEMENTS_TO_DELETE[@]} statement(s) to remove:"
    for s in "${STATEMENTS_TO_DELETE[@]}"; do
        echo "  - $s"
    done
    echo ""

    if [[ "$DRY_RUN" == "true" ]]; then
        warn "[DRY-RUN] Would delete statements listed above."
    else
        for s in "${STATEMENTS_TO_DELETE[@]}"; do
            info "Deleting statement: $s..."
            confluent flink statement delete "$s" \
                --compute-pool "$CCLOUD_COMPUTE_POOL_ID" \
                --environment "$CCLOUD_ENV_ID" \
                --force 2>/dev/null || true
            success "✓ Deleted: $s"
        done
    fi
fi

# ── Step 2: Delete Artifact if Requested ───────────────────────────────────────
if [[ "$DELETE_ARTIFACT" == "true" ]]; then
    section "Deleting Flink Artifact"
    if [[ "$DRY_RUN" == "true" ]]; then
        warn "[DRY-RUN] Would delete artifact '${USE_CASE_NAME}' from ${CCLOUD_CLOUD}:${CCLOUD_REGION}"
    else
        info "Deleting artifact '${USE_CASE_NAME}'..."
        confluent flink artifact delete "$USE_CASE_NAME" \
            --cloud "$CCLOUD_CLOUD" \
            --region "$CCLOUD_REGION" \
            --environment "$CCLOUD_ENV_ID" \
            --force 2>/dev/null || warn "Artifact '${USE_CASE_NAME}' not found or already deleted."
        success "✓ Artifact '${USE_CASE_NAME}' deleted."
    fi
fi

echo ""
success "Undeploy completed for '${USE_CASE_NAME}'."
