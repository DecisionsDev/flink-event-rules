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
# get-dsa.sh
#
# Downloads a Decision Service Archive (.jar) from an IBM Decision Intelligence (DI)
# runtime (embedded or named deployment space) using Zen API key authentication.
#
# Supports any sample located in the samples directory (e.g. airline-agent,
# airtraffic-agent, banking-fraud-agent, retail-agent, or custom use cases),
# as well as downloading by explicit archive name and target output directory.
#
# Execution & Working Directory:
#   This script can be executed from ANY directory — project root, scripts/ directory,
#   or from within a sample's scripts/ directory.
#
# Usage:
#   # Using named flags:
#   ./scripts/get-dsa.sh --use-case <name> --version <version> [OPTIONS]
#   ./scripts/get-dsa.sh --archive <archive-name> --version <version> --output <dir> [OPTIONS]
#
#   # Positional shorthand:
#   ./scripts/get-dsa.sh <use-case-or-archive> <version> [output-dir]
#
# Environment variables:
#   DI_ZEN_HOSTNAME     Base URL of Zen REST API (e.g. https://cpd-namespace.apps.example.com)
#   DI_ZEN_USERNAME     Zen user name the API key belongs to
#   DI_ZEN_API_KEY      Zen API key; sent as "Authorization: ZenApiKey base64(<username>:<apikey>)"
#   DI_SKIP_TLS_VERIFY  Set to 'true' to disable TLS certificate verification (default: true)
#   DI_DEPLOYMENT_SPACE Deployment space name (default: embedded)
#
# Examples:
#   # Download for airline-agent use case:
#   ./scripts/get-dsa.sh --use-case airline-agent --version 1.2.5
#
#   # Download with explicit archive name and output directory:
#   ./scripts/get-dsa.sh --archive Fraud_Detection_Service --version 1.5.2 --output samples/banking-fraud-agent
#
#   # Positional shorthand:
#   ./scripts/get-dsa.sh banking-fraud-agent 1.5.2
#

set -euo pipefail

# ── Directory anchors ──────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
SAMPLES_DIR="${PROJECT_ROOT}/samples"

# ── Colour helpers ─────────────────────────────────────────────────────────────
if [[ -t 1 ]]; then
    GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; CYAN='\033[0;36m'; NC='\033[0m'
else
    GREEN=''; YELLOW=''; RED=''; CYAN=''; NC=''
fi

info()    { echo -e "${GREEN}[INFO]${NC}   $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}   $*"; }
error()   { echo -e "${RED}[ERROR]${NC}  $*" >&2; }
section() { echo -e "\n${CYAN}── $* ──${NC}"; }

# ── Defaults ───────────────────────────────────────────────────────────────────
USE_CASE=""
DECISION_SERVICE_ARCHIVE=""
VERSION=""
OUTPUT_TARGET=""
DEPLOYMENT_SPACE="${DI_DEPLOYMENT_SPACE:-embedded}"

DI_ZEN_HOSTNAME="${DI_ZEN_HOSTNAME:-${DI_URL:-}}"
DI_ZEN_USERNAME="${DI_ZEN_USERNAME:-${DI_USERNAME:-}}"
DI_ZEN_API_KEY="${DI_ZEN_API_KEY:-${DI_API_KEY:-}}"
SKIP_TLS="${DI_SKIP_TLS_VERIFY:-true}"

# ── Usage ──────────────────────────────────────────────────────────────────────
usage() {
    local code="${1:-1}"
    cat <<'EOF'
Usage:
  ./scripts/get-dsa.sh --use-case <name> --version <version> [OPTIONS]
  ./scripts/get-dsa.sh --archive <name> --version <version> --output <dir> [OPTIONS]
  ./scripts/get-dsa.sh <use-case-or-archive> <version> [output-dir]

Options:
  --use-case, -u <name>     Sample / use-case name (e.g. airline-agent, airtraffic-agent,
                            banking-fraud-agent, retail-agent)
  --archive, -a <name>      Decision service archive name (e.g. AirlineDecisionService,
                            Air_Traffic_Control_Service, Fraud_Detection_Service, RetailPromotionService)
  --version, -v <version>   Version string of the decision archive to retrieve (e.g. 1.0.15)
  --output, -o <path>       Output directory or destination .jar file path
  --hostname, --di-url <h>  Base URL or hostname of the Zen REST API
  --username <user>         Zen user name
  --api-key <key>           Zen API key
  --deployment-space <s>    Deployment space name (default: embedded)
  --skip-tls                Disable TLS certificate verification (default: true)
  --no-skip-tls             Enable strict TLS certificate verification
  --help, -h                Show this help message

Environment variables:
  DI_ZEN_HOSTNAME   Base URL of the Zen REST API (e.g. https://cpd-namespace.apps.example.com)
  DI_ZEN_USERNAME   Zen user name the API key belongs to
  DI_ZEN_API_KEY    Zen API key
  DI_DEPLOYMENT_SPACE Deployment space (default: embedded)
  DI_SKIP_TLS_VERIFY  Set to 'false' for strict TLS verification (default: true)
EOF
    exit "$code"
}

# ── Argument parsing ───────────────────────────────────────────────────────────
POSITIONAL_ARGS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --use-case|-u)
            USE_CASE="$2"; shift 2 ;;
        --archive|--archive-name|--dsa|-a)
            DECISION_SERVICE_ARCHIVE="$2"; shift 2 ;;
        --version|-v)
            VERSION="$2"; shift 2 ;;
        --output|--output-dir|--output-file|-o)
            OUTPUT_TARGET="$2"; shift 2 ;;
        --hostname|--di-url)
            DI_ZEN_HOSTNAME="$2"; shift 2 ;;
        --username)
            DI_ZEN_USERNAME="$2"; shift 2 ;;
        --api-key)
            DI_ZEN_API_KEY="$2"; shift 2 ;;
        --deployment-space|--space|-s)
            DEPLOYMENT_SPACE="$2"; shift 2 ;;
        --skip-tls)
            SKIP_TLS="true"; shift ;;
        --no-skip-tls)
            SKIP_TLS="false"; shift ;;
        --help|-h)
            usage 0 ;;
        -*)
            error "Unknown option: $1"
            usage 1 ;;
        *)
            POSITIONAL_ARGS+=("$1"); shift ;;
    esac
done

# Handle positional arguments if flags were omitted
if [[ ${#POSITIONAL_ARGS[@]} -gt 0 ]]; then
    if [[ -z "$USE_CASE" && -z "$DECISION_SERVICE_ARCHIVE" && -z "$VERSION" ]]; then
        # Check if first arg looks like a version (all digits/dots) or a name
        if [[ "${POSITIONAL_ARGS[0]}" =~ ^[0-9]+(\.[0-9]+)*.*$ ]]; then
            VERSION="${POSITIONAL_ARGS[0]}"
            if [[ ${#POSITIONAL_ARGS[@]} -ge 2 ]]; then
                OUTPUT_TARGET="${POSITIONAL_ARGS[1]}"
            fi
        else
            FIRST_ARG="${POSITIONAL_ARGS[0]}"
            if [[ -d "${SAMPLES_DIR}/${FIRST_ARG}" || -d "${PROJECT_ROOT}/samples/${FIRST_ARG}" || "${FIRST_ARG}" == *-agent* ]]; then
                USE_CASE="${FIRST_ARG}"
            else
                DECISION_SERVICE_ARCHIVE="${FIRST_ARG}"
            fi
            if [[ ${#POSITIONAL_ARGS[@]} -ge 2 ]]; then
                VERSION="${POSITIONAL_ARGS[1]}"
            fi
            if [[ ${#POSITIONAL_ARGS[@]} -ge 3 ]]; then
                OUTPUT_TARGET="${POSITIONAL_ARGS[2]}"
            fi
        fi
    elif [[ -z "$VERSION" && ${#POSITIONAL_ARGS[@]} -ge 1 ]]; then
        VERSION="${POSITIONAL_ARGS[0]}"
        if [[ -z "$OUTPUT_TARGET" && ${#POSITIONAL_ARGS[@]} -ge 2 ]]; then
            OUTPUT_TARGET="${POSITIONAL_ARGS[1]}"
        fi
    elif [[ -z "$OUTPUT_TARGET" && ${#POSITIONAL_ARGS[@]} -ge 1 ]]; then
        OUTPUT_TARGET="${POSITIONAL_ARGS[0]}"
    fi
fi

# ── Validate version ───────────────────────────────────────────────────────────
if [[ -z "$VERSION" ]]; then
    error "Version is required."
    usage 1
fi

# ── Resolve use case & sample directory ───────────────────────────────────────
SAMPLE_DIR=""
if [[ -n "$USE_CASE" ]]; then
    USE_CASE_CLEAN="${USE_CASE%/}"
    USE_CASE_CLEAN="${USE_CASE_CLEAN##*/}"

    if [[ -d "$USE_CASE" ]]; then
        SAMPLE_DIR="$(cd "$USE_CASE" && pwd)"
    elif [[ -d "$SAMPLES_DIR/$USE_CASE" ]]; then
        SAMPLE_DIR="$SAMPLES_DIR/$USE_CASE"
    elif [[ -d "$SAMPLES_DIR/$USE_CASE_CLEAN" ]]; then
        SAMPLE_DIR="$SAMPLES_DIR/$USE_CASE_CLEAN"
    elif [[ -d "${PROJECT_ROOT}/samples/$USE_CASE_CLEAN" ]]; then
        SAMPLE_DIR="${PROJECT_ROOT}/samples/$USE_CASE_CLEAN"
    else
        MATCHED_DIR=$(find "$SAMPLES_DIR" -mindepth 1 -maxdepth 1 -type d ! -name ".*" -name "*${USE_CASE_CLEAN#agent-}*" 2>/dev/null | head -n 1 || true)
        if [[ -n "$MATCHED_DIR" && -d "$MATCHED_DIR" ]]; then
            SAMPLE_DIR="$MATCHED_DIR"
        fi
    fi
fi

# ── Infer archive name if not explicitly specified ────────────────────────────
if [[ -z "$DECISION_SERVICE_ARCHIVE" && -n "$SAMPLE_DIR" ]]; then
    DESCRIPTOR_FILE="$SAMPLE_DIR/agents.json"
    if [[ -f "$DESCRIPTOR_FILE" && -x "$(command -v jq 2>/dev/null)" ]]; then
        JAR_NAME=$(jq -r '[.agents[].ruleset.path // empty][0] // empty' "$DESCRIPTOR_FILE" 2>/dev/null || true)
        if [[ -n "$JAR_NAME" ]]; then
            # Strip _<version>.jar or .jar suffix
            DECISION_SERVICE_ARCHIVE="$(echo "$JAR_NAME" | sed -E 's/(_[0-9]+(\.[0-9]+)*.*)?\.jar$//')"
        fi
    fi

    # Check di-project/*.zip
    if [[ -z "$DECISION_SERVICE_ARCHIVE" && -d "$SAMPLE_DIR/di-project" ]]; then
        ZIP_FILE=$(find "$SAMPLE_DIR/di-project" -maxdepth 1 -name "*.zip" | head -n 1 || true)
        if [[ -n "$ZIP_FILE" ]]; then
            ZIP_BASE="$(basename "$ZIP_FILE" .zip)"
            DECISION_SERVICE_ARCHIVE="${ZIP_BASE// /_}"
        fi
    fi

    # Check known sample folder names as fallback
    if [[ -z "$DECISION_SERVICE_ARCHIVE" ]]; then
        case "$(basename "$SAMPLE_DIR")" in
            airline-agent)       DECISION_SERVICE_ARCHIVE="AirlineDecisionService" ;;
            airtraffic-agent)    DECISION_SERVICE_ARCHIVE="Air_Traffic_Control_Service" ;;
            banking-fraud-agent) DECISION_SERVICE_ARCHIVE="Fraud_Detection_Service" ;;
            retail-agent)        DECISION_SERVICE_ARCHIVE="RetailPromotionService" ;;
        esac
    fi
fi

if [[ -z "$DECISION_SERVICE_ARCHIVE" ]]; then
    error "Decision service archive name could not be determined. Please specify --archive <name> or --use-case <name>."
    usage 1
fi

# ── Resolve output file path ───────────────────────────────────────────────────
OUTPUT_FILE=""
if [[ -n "$OUTPUT_TARGET" ]]; then
    if [[ "$OUTPUT_TARGET" == *.jar ]]; then
        OUTPUT_FILE="$OUTPUT_TARGET"
        OUTPUT_DIR="$(dirname "$OUTPUT_FILE")"
        mkdir -p "$OUTPUT_DIR"
    else
        OUTPUT_DIR="$OUTPUT_TARGET"
        mkdir -p "$OUTPUT_DIR"
        OUTPUT_DIR="$(cd "$OUTPUT_DIR" && pwd)"
        OUTPUT_FILE="${OUTPUT_DIR}/${DECISION_SERVICE_ARCHIVE}_${VERSION}.jar"
    fi
elif [[ -n "$SAMPLE_DIR" ]]; then
    OUTPUT_FILE="${SAMPLE_DIR}/${DECISION_SERVICE_ARCHIVE}_${VERSION}.jar"
else
    OUTPUT_FILE="$(pwd)/${DECISION_SERVICE_ARCHIVE}_${VERSION}.jar"
fi

# ── Validate environment / credentials ─────────────────────────────────────────
if [[ -z "$DI_ZEN_HOSTNAME" ]] || [[ -z "$DI_ZEN_USERNAME" ]] || [[ -z "$DI_ZEN_API_KEY" ]]; then
    error "DI_ZEN_HOSTNAME, DI_ZEN_USERNAME, and DI_ZEN_API_KEY must be set (via options or environment variables)."
    echo ""
    echo "  Example:"
    echo "    export DI_ZEN_HOSTNAME=https://cpd-namespace.apps.example.com"
    echo "    export DI_ZEN_USERNAME=cpadmin"
    echo "    export DI_ZEN_API_KEY=xxxxxxxx"
    echo ""
    exit 1
fi

# Normalize hostname
if [[ "$DI_ZEN_HOSTNAME" != http://* && "$DI_ZEN_HOSTNAME" != https://* ]]; then
    DI_ZEN_HOSTNAME="https://${DI_ZEN_HOSTNAME}"
fi
DI_ZEN_HOSTNAME="${DI_ZEN_HOSTNAME%/}"
DI_ZEN_HOSTNAME="${DI_ZEN_HOSTNAME%/ads}"

ZEN_AUTH=$(printf '%s:%s' "${DI_ZEN_USERNAME}" "${DI_ZEN_API_KEY}" | base64 | tr -d '\n')

CURL_FLAGS=()
if [[ "$SKIP_TLS" == "true" ]]; then
    CURL_FLAGS+=("-k")
fi

# ── Fetch decisions list ───────────────────────────────────────────────────────
section "Retrieving list of decisions from deployment space '${DEPLOYMENT_SPACE}'"
info "Host           : ${DI_ZEN_HOSTNAME}"
info "Archive search : ${DECISION_SERVICE_ARCHIVE}"
info "Version        : ${VERSION}"
info "Target output  : ${OUTPUT_FILE}"

DECISIONS_URL="${DI_ZEN_HOSTNAME}/ads/runtime/api/v1/deploymentSpaces/${DEPLOYMENT_SPACE}/decisions"

DECISIONS_JSON=$(curl -s "${CURL_FLAGS[@]+"${CURL_FLAGS[@]}"}" "${DECISIONS_URL}" \
    -H 'accept: application/json' \
    -H "Authorization: ZenApiKey ${ZEN_AUTH}")

# Check for authentication or network errors
if [[ -z "$DECISIONS_JSON" ]]; then
    error "Failed to retrieve decisions list from ${DECISIONS_URL} (empty response)."
    exit 1
fi

if echo "$DECISIONS_JSON" | grep -qiE "Unauthorized|Invalid token|Access Denied|401|403" && ! echo "$DECISIONS_JSON" | grep -q '\[.*\]'; then
    error "Authentication failed when querying DI runtime:"
    echo "$DECISIONS_JSON"
    exit 1
fi

# Extract matching DSA ID using jq if available
DSA_ID=""
if command -v jq &>/dev/null; then
    DSA_ID=$(echo "${DECISIONS_JSON}" | jq -r \
      --arg ver "${VERSION}" \
      --arg dsa "${DECISION_SERVICE_ARCHIVE}" '
      (if type == "array" then .
       elif type == "object" then (.decisionArchiveIds // .decisions // .items // [])
       else [] end)
      | map(if type == "string" then . else (.decisionArchiveId // .decisionServiceArchiveId // .decisionId // .id // "") end)
      | map(select((test($dsa; "i")) and (split("/")[-1] | test("^" + ($ver | gsub("\\."; "\\.")) + "(-|$)"))))
      | sort | reverse | .[0] // empty
    ' 2>/dev/null || true)
fi

# Fallback if jq failed or is not installed
if [[ -z "${DSA_ID}" ]]; then
    DSA_ID=$(echo "${DECISIONS_JSON}" | grep -i "${DECISION_SERVICE_ARCHIVE}" | grep -E "/${VERSION//./\\.}(-[^/]+)?$" | sort -r | head -n 1 || true)
fi

if [[ -z "${DSA_ID}" ]]; then
    error "No decision archive matching '${DECISION_SERVICE_ARCHIVE}' with version '${VERSION}' found in deployment space '${DEPLOYMENT_SPACE}'."
    echo ""
    echo "Available decisions:"
    if command -v jq &>/dev/null; then
        echo "${DECISIONS_JSON}" | jq -r '
          (if type == "array" then .
           elif type == "object" then (.decisionArchiveIds // .decisions // .items // [])
           else [] end)
          | map(if type == "string" then . else (.decisionArchiveId // .decisionServiceArchiveId // .decisionId // .id // "") end)
          | .[]' 2>/dev/null || echo "${DECISIONS_JSON}"
    else
        echo "${DECISIONS_JSON}"
    fi
    exit 1
fi

info "✓ Found Decision Service Archive ID: ${DSA_ID}"
DSA_ID_ENCODED="${DSA_ID//\//%2F}"

# ── Download archive ───────────────────────────────────────────────────────────
section "Downloading Decision Service Archive for version ${VERSION}"
DOWNLOAD_URL="${DI_ZEN_HOSTNAME}/ads/runtime/api/v1/deploymentSpaces/${DEPLOYMENT_SPACE}/decisions/${DSA_ID_ENCODED}/archive"

HTTP_CODE=$(curl -X "GET" "${CURL_FLAGS[@]+"${CURL_FLAGS[@]}"}" -s -w "%{http_code}" "${DOWNLOAD_URL}" \
    -H 'accept: */*' \
    -H 'Content-Type: application/octet-stream' \
    -H "Authorization: ZenApiKey ${ZEN_AUTH}" \
    --output "${OUTPUT_FILE}")

if [[ "$HTTP_CODE" != "200" ]]; then
    error "Download failed with HTTP ${HTTP_CODE} from ${DOWNLOAD_URL}"
    rm -f "${OUTPUT_FILE}"
    exit 1
fi

if [[ ! -s "${OUTPUT_FILE}" ]]; then
    error "Downloaded file is empty (0 bytes): ${OUTPUT_FILE}"
    rm -f "${OUTPUT_FILE}"
    exit 1
fi

FILE_SIZE=$(du -h "${OUTPUT_FILE}" | cut -f1)
info "✓ Decision Service Archive successfully saved to:"
echo "  ${OUTPUT_FILE} (${FILE_SIZE})"
echo ""
