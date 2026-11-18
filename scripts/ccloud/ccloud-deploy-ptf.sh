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
# ccloud-deploy-ptf.sh
# Uploads the artifact, registers functions/views, and submits the Flink SQL pipeline
# to Confluent Cloud for Apache Flink.
#
# Usage:
#   ./scripts/ccloud/ccloud-deploy-ptf.sh <use-case> [OPTIONS]
#
# Options:
#   --build             Force rebuild before deploying
#   --dry-run           Print SQL statements and plan without submitting
#   --environment <id>  Confluent Environment ID
#   --cluster <id>      Kafka Cluster ID (Flink database)
#   --compute-pool <id> Flink Compute Pool ID
#   --cloud <provider>  Cloud provider (default: aws)
#   --region <region>   Cloud region (default: us-east-1)
#   --artifact-id <id>  Use an existing artifact ID instead of uploading
#   -h, --help          Show help message
#
# Examples:
#   ./scripts/ccloud/ccloud-deploy-ptf.sh banking-fraud-agent
#   ./scripts/ccloud/ccloud-deploy-ptf.sh retail-agent --build
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
# shellcheck source=scripts/ccloud/ccloud-common.sh
source "${SCRIPT_DIR}/ccloud-common.sh"

USE_CASE_ARG=""
FORCE_BUILD=false
DRY_RUN=false
ENV_OVERRIDE=""
CLUSTER_OVERRIDE=""
POOL_OVERRIDE=""
CLOUD_OVERRIDE=""
REGION_OVERRIDE=""
ARTIFACT_ID_OVERRIDE=""

usage() {
    cat <<EOF
Usage: $0 <use-case> [OPTIONS]

Options:
  --build             Force rebuild before deploying
  --dry-run           Print SQL statements and plan without submitting
  --environment <id>  Confluent Environment ID (overrides context/env)
  --cluster <id>      Kafka Cluster ID / Flink Database (overrides context/env)
  --compute-pool <id> Flink Compute Pool ID (overrides context/env)
  --cloud <provider>  Cloud provider (default: aws)
  --region <region>   Cloud region (default: us-east-1)
  --artifact-id <id>  Use an existing artifact ID instead of uploading
  -h, --help          Show help message

Examples:
  $0 banking-fraud-agent
  $0 samples/retail-agent --build
EOF
    exit "${1:-0}"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --build)            FORCE_BUILD=true;        shift   ;;
        --dry-run)          DRY_RUN=true;            shift   ;;
        --environment)      ENV_OVERRIDE="$2";       shift 2 ;;
        --cluster)          CLUSTER_OVERRIDE="$2";   shift 2 ;;
        --compute-pool)     POOL_OVERRIDE="$2";      shift 2 ;;
        --cloud)            CLOUD_OVERRIDE="$2";     shift 2 ;;
        --region)           REGION_OVERRIDE="$2";    shift 2 ;;
        --artifact-id)      ARTIFACT_ID_OVERRIDE="$2"; shift 2 ;;
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

[[ -n "$ENV_OVERRIDE" ]]         && CCLOUD_ENV_ID="$ENV_OVERRIDE"
[[ -n "$CLUSTER_OVERRIDE" ]]     && CCLOUD_CLUSTER_ID="$CLUSTER_OVERRIDE"
[[ -n "$POOL_OVERRIDE" ]]        && CCLOUD_COMPUTE_POOL_ID="$POOL_OVERRIDE"
[[ -n "$CLOUD_OVERRIDE" ]]       && CCLOUD_CLOUD="$CLOUD_OVERRIDE"
[[ -n "$REGION_OVERRIDE" ]]      && CCLOUD_REGION="$REGION_OVERRIDE"

section "Deploying to Confluent Cloud Flink — ${USE_CASE_NAME}"
print_ccloud_context

if [[ -z "$CCLOUD_ENV_ID" || -z "$CCLOUD_CLUSTER_ID" || -z "$CCLOUD_COMPUTE_POOL_ID" ]]; then
    error "Missing required Confluent Cloud parameters:"
    echo "  Environment ID    : ${CCLOUD_ENV_ID:-MISSING}"
    echo "  Kafka Cluster ID  : ${CCLOUD_CLUSTER_ID:-MISSING}"
    echo "  Flink Pool ID     : ${CCLOUD_COMPUTE_POOL_ID:-MISSING}"
    error "Configure these in .env.ccloud or pass as CLI arguments."
    exit 1
fi

# ── Step 1: Build if needed or requested ───────────────────────────────────────
if [[ "$FORCE_BUILD" == "true" || ! -f "$JAR_PATH" || ! -f "$MANIFEST_PATH" ]]; then
    info "Running build for ${USE_CASE_NAME}..."
    "${SCRIPT_DIR}/ccloud-build.sh" "$USE_CASE_NAME"
    echo ""
fi

# ── Step 2: Upload Flink Artifact ──────────────────────────────────────────────
ARTIFACT_ID="$ARTIFACT_ID_OVERRIDE"
ARTIFACT_VERSION_ID=""

if [[ -z "$ARTIFACT_ID" ]]; then
    if [[ "$DRY_RUN" == "true" ]]; then
        ARTIFACT_ID="mock-artifact-id"
        ARTIFACT_VERSION_ID="mock-version"
        info "[DRY-RUN] Simulating artifact upload for: ${JAR_PATH}"
    else
        section "1. Uploading Flink Artifact"
        info "Uploading ${JAR_PATH} to ${CCLOUD_CLOUD}:${CCLOUD_REGION} (env: ${CCLOUD_ENV_ID})..."

        # Create or upload artifact
        UPLOAD_OUTPUT="$(confluent flink artifact create "$USE_CASE_NAME" \
            --artifact-file "$JAR_PATH" \
            --cloud "$CCLOUD_CLOUD" \
            --region "$CCLOUD_REGION" \
            --environment "$CCLOUD_ENV_ID" \
            -o json 2>&1 || true)"

        ARTIFACT_ID="$(echo "$UPLOAD_OUTPUT" | jq -r '.id // .artifact_id // empty' 2>/dev/null || true)"
        ARTIFACT_VERSION_ID="$(echo "$UPLOAD_OUTPUT" | jq -r '.version // .version_id // empty' 2>/dev/null || true)"

        if [[ -z "$ARTIFACT_ID" ]]; then
            # Artifact might already exist; check existing artifacts
            warn "Artifact creation output did not contain an ID directly. Checking existing artifacts..."
            ARTIFACT_ID="$(confluent flink artifact list \
                --cloud "$CCLOUD_CLOUD" \
                --region "$CCLOUD_REGION" \
                --environment "$CCLOUD_ENV_ID" \
                -o json 2>/dev/null | jq -r --arg name "$USE_CASE_NAME" '.[]? | select(.name == $name or .id == $name) | .id' | head -n 1 || true)"
        fi

        if [[ -z "$ARTIFACT_ID" ]]; then
            error "Failed to resolve Artifact ID from Confluent Cloud."
            echo "Output: $UPLOAD_OUTPUT"
            exit 1
        fi

        success "Artifact ready: ID=${ARTIFACT_ID} ${ARTIFACT_VERSION_ID:+(version: ${ARTIFACT_VERSION_ID})}"
    fi
else
    info "Using specified Artifact ID: ${ARTIFACT_ID}"
fi

# Determine artifact URI
if [[ -n "$ARTIFACT_VERSION_ID" && "$ARTIFACT_VERSION_ID" != "null" ]]; then
    ARTIFACT_URI="confluent-artifact://${ARTIFACT_ID}/${ARTIFACT_VERSION_ID}"
else
    ARTIFACT_URI="confluent-artifact://${ARTIFACT_ID}"
fi
info "Resolved Artifact URI: ${ARTIFACT_URI}"
echo ""

# ── Step 3: Execute SQL Statements from Manifest ───────────────────────────────
section "2. Submitting Flink SQL Statements"

URI_PLACEHOLDER="$(jq -r '.artifact.uriPlaceholder // "confluent-artifact://<artifact-id>/<version-id>"' "$MANIFEST_PATH")"
STATEMENT_COUNT=$(jq '.statements | length' "$MANIFEST_PATH")

info "Found ${STATEMENT_COUNT} statement(s) in manifest."
echo ""

for i in $(seq 0 $((STATEMENT_COUNT - 1))); do
    STMT_OBJ="$(jq -c ".statements[$i]" "$MANIFEST_PATH")"
    ORDER="$(echo "$STMT_OBJ" | jq -r '.order')"
    KIND="$(echo "$STMT_OBJ" | jq -r '.kind')"
    NAME="$(echo "$STMT_OBJ" | jq -r '.name')"
    FILE="$(echo "$STMT_OBJ" | jq -r '.file')"

    STMT_FILE="${TARGET_CCLOUD_DIR}/${FILE}"
    if [[ ! -f "$STMT_FILE" ]]; then
        error "Statement file not found: ${STMT_FILE}"
        exit 1
    fi

    # Read SQL and substitute placeholder
    SQL_CONTENT="$(cat "$STMT_FILE")"
    SQL_SUBSTITUTED="${SQL_CONTENT//${URI_PLACEHOLDER}/${ARTIFACT_URI}}"
    # Also handle the short placeholder if present
    SQL_SUBSTITUTED="${SQL_SUBSTITUTED//confluent-artifact:\/\/<artifact-id>/${ARTIFACT_URI}}"

    STMT_NAME="${USE_CASE_NAME}-${ORDER}-${KIND}-${NAME}"
    # Sanitize statement name for Confluent CLI (alphanumeric, dashes, underscores)
    STMT_NAME="$(echo "$STMT_NAME" | sed -E 's/[^a-zA-Z0-9_-]+/-/g' | sed -E 's/^-+|-+$//g')"

    info "[$ORDER/$STATEMENT_COUNT] Submitting statement: ${STMT_NAME} (${KIND})"

    if [[ "$DRY_RUN" == "true" ]]; then
        echo -e "${BLUE}--- SQL (${FILE}) ---${NC}"
        echo "$SQL_SUBSTITUTED"
        echo -e "${BLUE}----------------------${NC}\n"
        continue
    fi

    # Check if statement already exists with this name; if so, delete or replace
    confluent flink statement delete "$STMT_NAME" \
        --compute-pool "$CCLOUD_COMPUTE_POOL_ID" \
        --environment "$CCLOUD_ENV_ID" \
        --force 2>/dev/null || true

    # Create and submit statement
    confluent flink statement create "$STMT_NAME" \
        --sql "$SQL_SUBSTITUTED" \
        --compute-pool "$CCLOUD_COMPUTE_POOL_ID" \
        --database "$CCLOUD_CLUSTER_ID" \
        --environment "$CCLOUD_ENV_ID" \
        --wait

    success "✓ Statement '${STMT_NAME}' created."
    echo ""
done

section "Deployment Summary"
success "Use case '${USE_CASE_NAME}' successfully deployed to Confluent Cloud Flink!"
echo ""
echo "Monitor execution status:"
echo "  ${SCRIPT_DIR}/ccloud-job-status.sh ${USE_CASE_NAME}"
echo ""
echo "Publish events with:"
echo "  ${SCRIPT_DIR}/ccloud-launch-demo-scenario.sh ${USE_CASE_NAME}"
echo ""
echo "Undeploy with:"
echo "  ${SCRIPT_DIR}/ccloud-undeploy.sh ${USE_CASE_NAME}"
echo ""
