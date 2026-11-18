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
# ccloud-common.sh
# Shared environment loading, Confluent CLI validation, and utility functions
# for Confluent Cloud operations.
#

set -euo pipefail

# ── Color Output Helpers ───────────────────────────────────────────────────────
if [[ -t 1 ]]; then
    GREEN='\033[0;32m'
    YELLOW='\033[1;33m'
    RED='\033[0;31m'
    CYAN='\033[0;36m'
    BLUE='\033[0;34m'
    BOLD='\033[1m'
    NC='\033[0m'
else
    GREEN=''
    YELLOW=''
    RED=''
    CYAN=''
    BLUE=''
    BOLD=''
    NC=''
fi

info()    { echo -e "${GREEN}[INFO]${NC}   $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}   $*"; }
error()   { echo -e "${RED}[ERROR]${NC}  $*" >&2; }
success() { echo -e "${GREEN}${BOLD}[SUCCESS]${NC} $*"; }
section() { echo -e "\n${CYAN}${BOLD}── $* ──${NC}"; }

# ── Project Directory Discovery ────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd -P)"

# ── Load Environment Configuration ─────────────────────────────────────────────
# Search order: .env.ccloud.local, .env.ccloud, .env.local, .env
load_env_file() {
    local file="$1"
    if [[ -f "$file" ]]; then
        info "Loading environment variables from: $(basename "$file")"
        set -a
        # shellcheck disable=SC1090
        source "$file"
        set +a
    fi
}

load_env_file "${PROJECT_ROOT}/.env.ccloud.local"
load_env_file "${PROJECT_ROOT}/.env.ccloud"
load_env_file "${PROJECT_ROOT}/.env.local"
load_env_file "${PROJECT_ROOT}/.env"

# ── Pre-flight: Required Tools Check ───────────────────────────────────────────
check_required_tools() {
    local missing=()
    for tool in "$@"; do
        if ! command -v "$tool" &>/dev/null; then
            missing+=("$tool")
        fi
    done
    if [[ ${#missing[@]} -gt 0 ]]; then
        error "Required tools missing: ${missing[*]}"
        error "Please install the missing tools and ensure they are on your PATH."
        exit 1
    fi
}

# ── Resolve Use Case Paths ─────────────────────────────────────────────────────
# Sets: USE_CASE_NAME, SAMPLE_DIR, DESCRIPTOR_PATH, TARGET_CCLOUD_DIR, MANIFEST_PATH, JAR_PATH
resolve_use_case() {
    local raw_target="${1:-}"

    if [[ -z "$raw_target" ]]; then
        error "No use case specified."
        error "Usage: $0 <use-case-name-or-path> [OPTIONS]"
        error "Examples: banking-fraud-agent, retail-agent, samples/airline-agent"
        exit 1
    fi

    # Strip trailing slash and path prefixes if given as name
    local clean_name="${raw_target%/}"
    clean_name="${clean_name##*/}"

    SAMPLE_DIR=""
    if [[ -d "$raw_target" ]]; then
        SAMPLE_DIR="$(cd "$raw_target" && pwd -P)"
    elif [[ -d "${PROJECT_ROOT}/samples/${raw_target}" ]]; then
        SAMPLE_DIR="$(cd "${PROJECT_ROOT}/samples/${raw_target}" && pwd -P)"
    elif [[ -d "${PROJECT_ROOT}/samples/${clean_name}" ]]; then
        SAMPLE_DIR="$(cd "${PROJECT_ROOT}/samples/${clean_name}" && pwd -P)"
    elif [[ -f "$raw_target" && "$(basename "$raw_target")" == "agents.json" ]]; then
        SAMPLE_DIR="$(cd "$(dirname "$raw_target")" && pwd -P)"
    else
        error "Use case directory not found: '$raw_target'"
        error "Available samples in ${PROJECT_ROOT}/samples:"
        find "${PROJECT_ROOT}/samples" -mindepth 1 -maxdepth 1 -type d ! -name ".*" -exec basename {} \; 2>/dev/null | sort | sed 's/^/  - /' >&2
        exit 1
    fi

    USE_CASE_NAME="$(basename "$SAMPLE_DIR")"
    DESCRIPTOR_PATH="${SAMPLE_DIR}/agents.json"

    if [[ ! -f "$DESCRIPTOR_PATH" ]]; then
        error "Descriptor agents.json not found in: ${SAMPLE_DIR}"
        exit 1
    fi

    TARGET_CCLOUD_DIR="${PROJECT_ROOT}/event-rule-backend/target/ccloud"
    MANIFEST_PATH="${TARGET_CCLOUD_DIR}/manifest.json"
    JAR_PATH="${TARGET_CCLOUD_DIR}/${USE_CASE_NAME}.jar"
}

# ── Confluent Cloud Context Discovery & Validation ─────────────────────────────
# Resolves:
#   CCLOUD_ENV_ID          (Confluent Environment ID, e.g. env-xxxx)
#   CCLOUD_CLUSTER_ID      (Kafka Cluster ID / Flink Database, e.g. lkc-xxxx)
#   CCLOUD_COMPUTE_POOL_ID (Flink Compute Pool ID, e.g. lfcp-xxxx)
#   CCLOUD_CLOUD           (Cloud provider, e.g. aws)
#   CCLOUD_REGION          (Cloud region, e.g. us-east-1)
validate_ccloud_context() {
    # If confluent is missing, check if required variables are already provided or if dry-run
    if ! command -v confluent &>/dev/null; then
        warn "The 'confluent' CLI is not found on PATH. Commands requiring Confluent Cloud will fail unless --dry-run is used or CLI is installed."
    fi
    check_required_tools jq

    # 1. Environment ID
    CCLOUD_ENV_ID="${CCLOUD_ENV_ID:-${CONFLUENT_ENVIRONMENT_ID:-}}"
    if [[ -z "$CCLOUD_ENV_ID" ]]; then
        # Try to resolve active environment from confluent cli
        CCLOUD_ENV_ID="$(confluent environment list -o json 2>/dev/null | jq -r '(.[] | select(.is_current == true or .current == true) | .id) // (.[0].id // empty)' 2>/dev/null || true)"
    fi

    # 2. Kafka Cluster ID (used as Flink Database name)
    CCLOUD_CLUSTER_ID="${CCLOUD_CLUSTER_ID:-${CONFLUENT_KAFKA_CLUSTER_ID:-${CCLOUD_DATABASE:-}}}"
    if [[ -z "$CCLOUD_CLUSTER_ID" ]]; then
        if [[ -n "$CCLOUD_ENV_ID" ]]; then
            CCLOUD_CLUSTER_ID="$(confluent kafka cluster list --environment "$CCLOUD_ENV_ID" -o json 2>/dev/null | jq -r '(.[] | select(.is_current == true or .current == true) | .id) // (.[0].id // empty)' 2>/dev/null || true)"
        else
            CCLOUD_CLUSTER_ID="$(confluent kafka cluster list -o json 2>/dev/null | jq -r '(.[] | select(.is_current == true or .current == true) | .id) // (.[0].id // empty)' 2>/dev/null || true)"
        fi
    fi

    # 3. Flink Compute Pool ID
    CCLOUD_COMPUTE_POOL_ID="${CCLOUD_COMPUTE_POOL_ID:-${CONFLUENT_COMPUTE_POOL_ID:-${CCLOUD_POOL_ID:-}}}"
    if [[ -z "$CCLOUD_COMPUTE_POOL_ID" ]]; then
        if [[ -n "$CCLOUD_ENV_ID" ]]; then
            CCLOUD_COMPUTE_POOL_ID="$(confluent flink compute-pool list --environment "$CCLOUD_ENV_ID" -o json 2>/dev/null | jq -r '(.[] | select(.is_current == true or .current == true) | .id) // (.[0].id // empty)' 2>/dev/null || true)"
        else
            CCLOUD_COMPUTE_POOL_ID="$(confluent flink compute-pool list -o json 2>/dev/null | jq -r '(.[] | select(.is_current == true or .current == true) | .id) // (.[0].id // empty)' 2>/dev/null || true)"
        fi
    fi

    # 4. Cloud Provider & Region
    CCLOUD_CLOUD="${CCLOUD_CLOUD:-aws}"
    CCLOUD_REGION="${CCLOUD_REGION:-us-east-1}"

    # If compute pool is known, query pool details for cloud and region
    if [[ -n "$CCLOUD_COMPUTE_POOL_ID" && -n "$CCLOUD_ENV_ID" ]]; then
        local pool_json
        pool_json="$(confluent flink compute-pool describe "$CCLOUD_COMPUTE_POOL_ID" --environment "$CCLOUD_ENV_ID" -o json 2>/dev/null || true)"
        if [[ -n "$pool_json" ]]; then
            local p_cloud p_region
            p_cloud="$(echo "$pool_json" | jq -r '.cloud // empty')"
            p_region="$(echo "$pool_json" | jq -r '.region // empty')"
            [[ -n "$p_cloud" ]] && CCLOUD_CLOUD="$p_cloud"
            [[ -n "$p_region" ]] && CCLOUD_REGION="$p_region"
        fi
    fi
}

print_ccloud_context() {
    info "Confluent Cloud Context:"
    echo "  Environment ID    : ${CCLOUD_ENV_ID:-<not set / auto-detect>}"
    echo "  Kafka Cluster ID  : ${CCLOUD_CLUSTER_ID:-<not set / auto-detect>}"
    echo "  Flink Pool ID     : ${CCLOUD_COMPUTE_POOL_ID:-<not set / auto-detect>}"
    echo "  Cloud / Region    : ${CCLOUD_CLOUD} / ${CCLOUD_REGION}"
    echo ""
}
