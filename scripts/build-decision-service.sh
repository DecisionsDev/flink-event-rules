#!/usr/bin/env bash
# =============================================================================
# Copyright IBM Corp. 2026
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# =============================================================================


# build-decision-service.sh
#
# Builds an IBM Decision Intelligence (DI) Decision Service archive
# from a Decision Project ZIP or directory.
#
# The script:
#   1. Validates prerequisites (Java, Maven, curl, python3).
#   2. Discovers the required IBM DI Maven artifacts from the DI instance
#      by querying /ads/index.json.
#   3. Installs those artifacts into an isolated local Maven repository so the
#      user's global ~/.m2 is not modified.
#   4. Generates an isolated Maven settings file that points at the DI
#      Maven repository (https://<host>/ads/static/maven-releases).
#   5. Extracts the Decision Project if a ZIP was given.
#   6. Runs "mvn package -DskipTests" against the project.
#   7. Locates the generated Decision Service archive (.jar) and copies it to
#      the requested output directory.
#
# Authentication (DI / CP4BA Zen API key):
#   The DI Maven repository requires authentication via the IBM CP4BA Zen
#   API key mechanism:
#
#     Authorization: ZenApiKey base64(<username>:<apikey>)
#
#   Provide credentials via environment variables:
#     DI_USERNAME      — CP4BA / Zen platform username
#     DI_API_KEY       — Zen API key (plain text; generate from CP4BA UI:
#                         profile icon → "Generate API key")
#
#   Or via --username / --api-key flags (NOT recommended for CI; prefer env).
#
# Usage:
#   ./build-decision-service.sh \
#       --project  /path/to/decision-project-or.zip \
#       --output   /path/to/output \
#       --di-url  https://cpd-cp4a.apps.ads-dev.cp.fyre.ibm.com/ads
#
# Full option reference: run with --help.
# =============================================================================

set -euo pipefail

# ─── Colour helpers ───────────────────────────────────────────────────────────
# Assign colours only when stdout is a terminal; otherwise use empty strings.
if [[ -t 1 ]]; then
  GREEN=$'\033[0;32m';  YELLOW=$'\033[1;33m'
  RED=$'\033[0;31m';    CYAN=$'\033[0;36m'
  BOLD=$'\033[1m';      NC=$'\033[0m'
else
  GREEN=''; YELLOW=''; RED=''; CYAN=''; BOLD=''; NC=''
fi

info()    { echo "${GREEN}[INFO]${NC}  $*";  }
warn()    { echo "${YELLOW}[WARN]${NC}  $*"; }
error()   { echo "${RED}[ERROR]${NC} $*" >&2; }
section() { echo; echo "${CYAN}${BOLD}── $* ──${NC}"; }
debug()   { [[ "${VERBOSE:-false}" == "true" ]] && echo "  [debug] $*" || true; }
die()     { error "$*"; exit 1; }

# ─── Script metadata ──────────────────────────────────────────────────────────
SCRIPT_VERSION="1.0.0"
SCRIPT_NAME="$(basename "${BASH_SOURCE[0]}")"

# ─── Usage ────────────────────────────────────────────────────────────────────
usage() {
  cat <<EOF
${BOLD}${SCRIPT_NAME}${NC} v${SCRIPT_VERSION}
Build an IBM Automation Decision Services Decision Service archive from a Decision Project.

${BOLD}Usage:${NC}
  ${SCRIPT_NAME} --project <path> --output <dir> --di-url <url> [options]

${BOLD}Required:${NC}
  --project   <path>   Path to the Decision Project directory or ZIP file.
  --output    <dir>    Output directory for the generated archive.
  --di-url   <url>    Base URL of the DI instance, e.g.
                       https://cpd-cp4a.apps.ads-dev.cp.fyre.ibm.com/ads

${BOLD}Authentication (ADS Zen API key — required):${NC}
  Set via environment variables (preferred — keeps credentials out of history):
    DI_USERNAME=<username>
    DI_API_KEY=<plain-text-api-key>

  Or via flags (use only for testing; visible in process list):
    --username  <username>
    --api-key   <apikey>

  The script computes: Authorization: ZenApiKey base64(<username>:<apikey>)
  Generate your API key: CP4BA Platform UI → profile icon → "Generate API key"

${BOLD}Optional:${NC}
  --mvn       <path>   Path to Maven executable (default: first 'mvn' on PATH).
  --version   <ver>    Override the Decision Service version string
                       (default: LATEST-SNAPSHOT from the project POM).
  --skip-tls           Disable TLS certificate verification (development only).
  --local-repo <dir>   Isolated Maven local repository (default: temp dir).
                       Persists across runs when set explicitly.
  --verbose            Enable debug output.
  --help               Show this help and exit.

${BOLD}Examples:${NC}
  # Minimal — credentials from environment:
  export DI_USERNAME=cpadmin
  export DI_API_KEY="\$(cat my-api-key.txt)"
  ${SCRIPT_NAME} \\
    --project ./my-decision-project \\
    --output  ./dist \\
    --di-url https://cpd-cp4a.apps.ads-dev.cp.fyre.ibm.com/ads

  # ZIP input, custom version, verbose:
  ${SCRIPT_NAME} \\
    --project  "Fraud Detection Service.zip" \\
    --output   ./dist \\
    --di-url  https://cpd-cp4a.apps.ads-dev.cp.fyre.ibm.com/ads \\
    --version  1.5.2 \\
    --verbose

  # Self-signed TLS certificate (development only):
  ${SCRIPT_NAME} \\
    --project  ./my-project \\
    --output   ./dist \\
    --di-url  https://cpd-cp4a.apps.example.com/ads \\
    --skip-tls

  # Persistent local Maven repo (speeds up repeated builds):
  ${SCRIPT_NAME} \\
    --project    ./my-project \\
    --output     ./dist \\
    --di-url    https://cpd-cp4a.apps.example.com/ads \\
    --local-repo ~/.m2-ads
EOF
  exit 0
}

# ─── Defaults ─────────────────────────────────────────────────────────────────
PROJECT_PATH=""
OUTPUT_DIR=""
DI_URL=""
DI_USERNAME="${DI_USERNAME:-${DI_ZEN_USERNAME:-}}"
DI_API_KEY="${DI_API_KEY:-${DI_ZEN_API_KEY:-}}"
MVN_CMD=""
VERSION_OVERRIDE=""
SKIP_TLS=false
LOCAL_REPO_ARG=""
VERBOSE=false

# These arrays must be initialized before any use (set -u compliance)
CURL_TLS_FLAGS=()
MVN_TLS_FLAGS=()

# ─── Argument parsing ─────────────────────────────────────────────────────────
[[ $# -eq 0 ]] && usage

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project)   PROJECT_PATH="$2";   shift 2 ;;
    --output)    OUTPUT_DIR="$2";     shift 2 ;;
    --di-url)   DI_URL="$2";         shift 2 ;;
    --username)  DI_USERNAME="$2";   shift 2 ;;
    --api-key)   DI_API_KEY="$2";    shift 2 ;;
    --mvn)       MVN_CMD="$2";        shift 2 ;;
    --version)   VERSION_OVERRIDE="$2"; shift 2 ;;
    --skip-tls)  SKIP_TLS=true;       shift   ;;
    --local-repo) LOCAL_REPO_ARG="$2"; shift 2 ;;
    --verbose)   VERBOSE=true;        shift   ;;
    --help|-h)   usage ;;
    *)           die "Unknown option: $1. Run with --help for usage." ;;
  esac
done

# ─── Temp directory + cleanup ─────────────────────────────────────────────────
WORK_DIR=""
cleanup() {
  local rc=$?
  if [[ -n "${WORK_DIR:-}" && -d "${WORK_DIR}" ]]; then
    # Remove the work directory; keep the local repo if the user specified one
    rm -rf "${WORK_DIR}"
  fi
  exit $rc
}
trap cleanup EXIT INT TERM

WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/ads-build-XXXXXX")"
debug "Work directory: ${WORK_DIR}"

# ─── Header ───────────────────────────────────────────────────────────────────
echo
echo "${BOLD}╔══════════════════════════════════════════════════╗${NC}"
echo "${BOLD}║  IBM DI Decision Service Builder  v${SCRIPT_VERSION}      ║${NC}"
echo "${BOLD}╚══════════════════════════════════════════════════╝${NC}"

# ═════════════════════════════════════════════════════════════════════════════
# Section 1: Validate required arguments
# ═════════════════════════════════════════════════════════════════════════════
section "Step 1: Validating arguments"

[[ -z "${PROJECT_PATH}" ]] && die "--project is required."
[[ -z "${OUTPUT_DIR}"   ]] && die "--output is required."
[[ -z "${DI_URL}"      ]] && die "--di-url is required."

# Normalise: strip trailing slash from DI URL
DI_URL="${DI_URL%/}"

# Validate project exists
[[ -e "${PROJECT_PATH}" ]] || die "Decision Project not found: ${PROJECT_PATH}"

info "Decision Project: ${PROJECT_PATH}"
info "Output directory: ${OUTPUT_DIR}"
info "DI URL:          ${DI_URL}"

# ═════════════════════════════════════════════════════════════════════════════
# Section 2: Prerequisite checks
# ═════════════════════════════════════════════════════════════════════════════
section "Step 2: Checking prerequisites"

# ── Java ──────────────────────────────────────────────────────────────────────
if ! command -v java &>/dev/null; then
  die "Java is not installed or not on PATH. Install JDK 11+ and retry."
fi
JAVA_VERSION=$(java -version 2>&1 | awk -F'"' 'NR==1{print $2}')
info "Java: $(command -v java)  (version: ${JAVA_VERSION})"

# ── Maven ─────────────────────────────────────────────────────────────────────
if [[ -z "${MVN_CMD}" ]]; then
  if ! command -v mvn &>/dev/null; then
    die "Maven is not installed or not on PATH. Install Maven 3.6+ and retry, or pass --mvn /path/to/mvn."
  fi
  MVN_CMD="$(command -v mvn)"
fi
[[ -x "${MVN_CMD}" ]] || die "Maven executable not found or not executable: ${MVN_CMD}"
MVN_VERSION=$("${MVN_CMD}" --version 2>&1 | head -1)
info "Maven: ${MVN_CMD}  (${MVN_VERSION})"

# ── curl ──────────────────────────────────────────────────────────────────────
if ! command -v curl &>/dev/null; then
  die "curl is not installed or not on PATH. Install curl and retry."
fi
info "curl: $(command -v curl)"

# ── python3 ───────────────────────────────────────────────────────────────────
if ! command -v python3 &>/dev/null; then
  die "python3 is not installed or not on PATH. Install Python 3 and retry."
fi
info "python3: $(command -v python3)"

# ── unzip (only needed if project is a ZIP) ───────────────────────────────────
if [[ -f "${PROJECT_PATH}" && "${PROJECT_PATH}" == *.zip ]]; then
  if ! command -v unzip &>/dev/null; then
    die "unzip is not installed but is required to extract a ZIP project."
  fi
  info "unzip: $(command -v unzip)"
fi

# ═════════════════════════════════════════════════════════════════════════════
# Section 3: Authentication
# ═════════════════════════════════════════════════════════════════════════════
section "Step 3: Configuring authentication"

if [[ -z "${DI_USERNAME}" || -z "${DI_API_KEY}" ]]; then
  error "DI credentials are required."
  cat >&2 <<EOF

Set the following environment variables:
  export DI_USERNAME=<your-cp4ba-username>
  export DI_API_KEY=<your-zen-api-key>

Generate your Zen API key from the CP4BA Platform UI:
  Profile icon → "Generate API key"

Or pass --username and --api-key flags (not recommended in CI).
EOF
  exit 1
fi

# Build the ZenApiKey header value: base64(<username>:<apikey>)
DI_API_KEY_B64=$(printf '%s:%s' "${DI_USERNAME}" "${DI_API_KEY}" | \
  python3 -c "import sys,base64; print(base64.b64encode(sys.stdin.buffer.read()).decode().strip())")

AUTH_HEADER="ZenApiKey ${DI_API_KEY_B64}"
# Mask in logs: show first 8 chars only
AUTH_MASKED="ZenApiKey ${DI_API_KEY_B64:0:8}…"
info "Auth header computed: ${AUTH_MASKED}"

# TLS configuration
if [[ "${SKIP_TLS}" == "true" ]]; then
  warn "TLS certificate verification is DISABLED (--skip-tls). Use only in development."
  CURL_TLS_FLAGS=("-k")
  # Maven 3.9+ uses maven-resolver's native HTTP transport by default, which does
  # NOT honour maven.wagon.http.ssl.* properties.  Force the Wagon transport so
  # those flags are actually respected by the HTTP client that resolves artifacts.
  MVN_TLS_FLAGS=(
    "-Dmaven.resolver.transport=wagon"
    "-Dmaven.wagon.http.ssl.insecure=true"
    "-Dmaven.wagon.http.ssl.allowall=true"
    "-Dmaven.wagon.http.ssl.peer.unverified=true"
  )
fi

# ═════════════════════════════════════════════════════════════════════════════
# Section 4: Probe the DI instance and discover artifacts
# ═════════════════════════════════════════════════════════════════════════════
section "Step 4: Discovering DI artifacts from ${DI_URL}/index.json"

INDEX_URL="${DI_URL}/index.json"
MAVEN_REPO_URL="${DI_URL}/static/maven-releases"

info "Querying: ${INDEX_URL}"

# --fail causes curl to exit non-zero on HTTP 4xx/5xx, giving an earlier and
# cleaner failure than waiting for the empty-body / JSON-parse check below.
INDEX_JSON=$(curl "${CURL_TLS_FLAGS[@]+"${CURL_TLS_FLAGS[@]}"}" \
  --silent --fail --max-time 30 \
  -H "Authorization: ${AUTH_HEADER}" \
  "${INDEX_URL}") || {
    error "Failed to reach ${INDEX_URL} (network error or HTTP error status)."
    cat >&2 <<EOF

Possible causes:
  • DI instance is unreachable (check --di-url).
  • TLS certificate error — retry with --skip-tls (dev/CI only) or import the CA.
  • Authentication failed — check DI_USERNAME / DI_API_KEY.

HTTP diagnostic:
  curl ${CURL_TLS_FLAGS[@]+"${CURL_TLS_FLAGS[@]}"} -v \\
    -H "Authorization: ${AUTH_MASKED}" \\
    "${INDEX_URL}"
EOF
    exit 1
  }

# Detect authentication failure (unauthenticated requests redirect to OIDC login page)
if printf '%s\n' "${INDEX_JSON}" | grep -qE "loginRedirectUrl|OIDC|login\.jsp|<!DOCTYPE html"; then
  error "DI authentication failed."
  cat >&2 <<EOF

The server returned an OIDC login page instead of index.json.
This means the Zen API key was rejected.

Troubleshooting:
  1. DI_API_KEY must be a Zen API key generated from the CP4BA UI
     (Profile icon → "Generate API key"). It is NOT your account password.
  2. DI_USERNAME must exactly match your username in the CP4BA UI.
  3. The API key may have expired — regenerate it in the UI.
  4. If using --skip-tls, ensure the hostname is correct.

Manual test:
  B64=\$(printf '\${DI_USERNAME}:\${DI_API_KEY}' | base64)
  curl -s -H "Authorization: ZenApiKey \$B64" "${INDEX_URL}" | head -20
EOF
  exit 1
fi

if ! printf '%s\n' "${INDEX_JSON}" | python3 -c "import json,sys; json.load(sys.stdin)" &>/dev/null; then
  error "Failed to parse response from ${INDEX_URL} as JSON."
  cat >&2 <<EOF

Possible causes:
  • The endpoint path is wrong (expected /ads/index.json).
  • The DI instance returned an unexpected response.

HTTP diagnostic:
  curl ${CURL_TLS_FLAGS[@]+"${CURL_TLS_FLAGS[@]}"} -v \\
    -H "Authorization: ${AUTH_MASKED}" \\
    "${INDEX_URL}"
EOF
  exit 1
fi

info "index.json retrieved successfully."

# ── Parse the build plugin and foundation coordinates ─────────────────────────
# The index.json uses well-known fixed keys for each artifact role:
# annotations_maven_plugin      → com.ibm.decision:annotations
# build_command_maven_plugin    → com.ibm.decision:build-maven-plugin
# foundation_maven_plugin       → com.ibm.decision:foundation  (required by build plugin)
# engine_maven_plugin           → com.ibm.decision:engine-compact-runtime
# engine_de_api_maven_plugin    → com.ibm.decision:engine-de-api
# execution_api_maven_plugin    → com.ibm.decision.execution:execution-api
# ml_integration_maven_plugin   → com.ibm.decision:ml-integration-runtime
# ml_predictor_maven_plugin     → com.ibm.decision:decision-ml-predictor
#
# import-maven-plugin ships at the same version as build-maven-plugin and is
# required by external-library-ads.  It is not always listed in index.json, so
# we derive its Maven repository path from the build-plugin path.

# Write index JSON to a temp file to avoid pipe/heredoc conflicts under set -e
INDEX_JSON_FILE="${WORK_DIR}/index.json"
printf '%s' "${INDEX_JSON}" > "${INDEX_JSON_FILE}"

_read_artifact() {
  local key="$1"
  python3 - "${key}" "${INDEX_JSON_FILE}" <<'PYEOF'
import json, sys

key      = sys.argv[1]
idx_file = sys.argv[2]

with open(idx_file) as fh:
    idx = json.load(fh)

# DI 16.7+ wraps all artifact entries under a top-level "resources" key:
#   {"format": 1, "resources": { "build_command_maven_plugin": {...}, ... }}
# Earlier versions returned a flat object.  Support both.
if "resources" in idx and isinstance(idx["resources"], dict):
    idx = idx["resources"]

entry = idx.get(key)
if not entry:
    print("MISSING", flush=True)
    sys.exit(0)

# Flatten nested maven_coordinates if present (seen in some DI versions)
mc = entry.get("maven_coordinates", {})
if isinstance(mc, dict) and "maven_coordinates" in mc:
    mc = mc["maven_coordinates"]

path     = entry.get("path", "")
group    = mc.get("groupId", "")
artifact = mc.get("artifactId", "")
version  = mc.get("version", entry.get("version", ""))
pkg      = mc.get("packaging", "jar")
print(f"{group}:{artifact}:{version}:{pkg}:{path}", flush=True)
PYEOF
}

ANNOTATIONS_COORDS=$(_read_artifact "annotations_maven_plugin")
BUILD_PLUGIN_COORDS=$(_read_artifact "build_command_maven_plugin")
FOUNDATION_COORDS=$(_read_artifact "foundation_maven_plugin")
ENGINE_RT_COORDS=$(_read_artifact "engine_maven_plugin")
ENGINE_API_COORDS=$(_read_artifact "engine_de_api_maven_plugin")
EXEC_API_COORDS=$(_read_artifact "execution_api_maven_plugin")
ML_INTEG_COORDS=$(_read_artifact "ml_integration_maven_plugin")
ML_PRED_COORDS=$(_read_artifact "ml_predictor_maven_plugin")
# import-maven-plugin: try index.json first; fall back to deriving from build-plugin path.
IMPORT_PLUGIN_COORDS=$(_read_artifact "import_maven_plugin")

for name_val in \
  "build_command_maven_plugin:${BUILD_PLUGIN_COORDS}" \
  "foundation_maven_plugin:${FOUNDATION_COORDS}" \
  "engine_maven_plugin:${ENGINE_RT_COORDS}" \
  "engine_de_api_maven_plugin:${ENGINE_API_COORDS}"; do
  key="${name_val%%:*}"
  val="${name_val#*:}"
  if [[ "${val}" == "MISSING" || -z "${val}" ]]; then
    error "Required artifact '${key}' not found in ${INDEX_URL}."
    cat >&2 <<EOF

This usually means the DI instance is an older version that does not publish
the build plugin via index.json.  Verify that the DI instance version is 16.7+
and that the Maven repository at ${MAVEN_REPO_URL} is accessible.

Available keys in this index.json:
$(python3 -c "
import json, sys
with open('${INDEX_JSON_FILE}') as fh:
    d = json.load(fh)
idx = d.get('resources', d)
for k in sorted(idx.keys()):
    print('  ' + k)
" 2>/dev/null || echo "  (could not parse index.json)")
EOF
    exit 1
  fi
done

# Extract fields: groupId:artifactId:version:packaging:path
_field() { echo "${1}" | cut -d: -f"${2}"; }

ANNOTATIONS_GROUP=$(_field "${ANNOTATIONS_COORDS}" 1)
ANNOTATIONS_ARTIFACT=$(_field "${ANNOTATIONS_COORDS}" 2)
ANNOTATIONS_VERSION=$(_field "${ANNOTATIONS_COORDS}" 3)
ANNOTATIONS_PATH=$(_field "${ANNOTATIONS_COORDS}" 5)

BUILD_PLUGIN_GROUP=$(_field "${BUILD_PLUGIN_COORDS}" 1)
BUILD_PLUGIN_ARTIFACT=$(_field "${BUILD_PLUGIN_COORDS}" 2)
BUILD_PLUGIN_VERSION=$(_field "${BUILD_PLUGIN_COORDS}" 3)
BUILD_PLUGIN_PKG=$(_field "${BUILD_PLUGIN_COORDS}" 4)
BUILD_PLUGIN_PATH=$(_field "${BUILD_PLUGIN_COORDS}" 5)

FOUNDATION_GROUP=$(_field "${FOUNDATION_COORDS}" 1)
FOUNDATION_ARTIFACT=$(_field "${FOUNDATION_COORDS}" 2)
FOUNDATION_VERSION=$(_field "${FOUNDATION_COORDS}" 3)
FOUNDATION_PATH=$(_field "${FOUNDATION_COORDS}" 5)

ENGINE_RT_GROUP=$(_field "${ENGINE_RT_COORDS}" 1)
ENGINE_RT_ARTIFACT=$(_field "${ENGINE_RT_COORDS}" 2)
ENGINE_RT_VERSION=$(_field "${ENGINE_RT_COORDS}" 3)
ENGINE_RT_PATH=$(_field "${ENGINE_RT_COORDS}" 5)

ENGINE_API_GROUP=$(_field "${ENGINE_API_COORDS}" 1)
ENGINE_API_ARTIFACT=$(_field "${ENGINE_API_COORDS}" 2)
ENGINE_API_VERSION=$(_field "${ENGINE_API_COORDS}" 3)
ENGINE_API_PATH=$(_field "${ENGINE_API_COORDS}" 5)

EXEC_API_GROUP=$(_field "${EXEC_API_COORDS}" 1)
EXEC_API_ARTIFACT=$(_field "${EXEC_API_COORDS}" 2)
EXEC_API_VERSION=$(_field "${EXEC_API_COORDS}" 3)
EXEC_API_PATH=$(_field "${EXEC_API_COORDS}" 5)

ML_INTEG_GROUP=$(_field "${ML_INTEG_COORDS}" 1)
ML_INTEG_ARTIFACT=$(_field "${ML_INTEG_COORDS}" 2)
ML_INTEG_VERSION=$(_field "${ML_INTEG_COORDS}" 3)
ML_INTEG_PATH=$(_field "${ML_INTEG_COORDS}" 5)

ML_PRED_GROUP=$(_field "${ML_PRED_COORDS}" 1)
ML_PRED_ARTIFACT=$(_field "${ML_PRED_COORDS}" 2)
ML_PRED_VERSION=$(_field "${ML_PRED_COORDS}" 3)
ML_PRED_PATH=$(_field "${ML_PRED_COORDS}" 5)

# import-maven-plugin: if not in index.json, derive coordinates from build-plugin.
# The import plugin always ships at the same version as the build plugin and
# lives at the same path prefix (just swap the artifactId).
if [[ "${IMPORT_PLUGIN_COORDS}" == "MISSING" || -z "${IMPORT_PLUGIN_COORDS}" ]]; then
  IMPORT_PLUGIN_GROUP="${BUILD_PLUGIN_GROUP}"
  IMPORT_PLUGIN_ARTIFACT="import-maven-plugin"
  IMPORT_PLUGIN_VERSION="${BUILD_PLUGIN_VERSION}"
  # Derive path by substituting build-maven-plugin → import-maven-plugin in the build path
  IMPORT_PLUGIN_PATH="${BUILD_PLUGIN_PATH/build-maven-plugin/import-maven-plugin}"
else
  IMPORT_PLUGIN_GROUP=$(_field "${IMPORT_PLUGIN_COORDS}" 1)
  IMPORT_PLUGIN_ARTIFACT=$(_field "${IMPORT_PLUGIN_COORDS}" 2)
  IMPORT_PLUGIN_VERSION=$(_field "${IMPORT_PLUGIN_COORDS}" 3)
  IMPORT_PLUGIN_PATH=$(_field "${IMPORT_PLUGIN_COORDS}" 5)
fi

if [[ "${ANNOTATIONS_COORDS}" != "MISSING" && -n "${ANNOTATIONS_COORDS}" ]]; then
  info "Annotations:   ${ANNOTATIONS_GROUP}:${ANNOTATIONS_ARTIFACT}:${ANNOTATIONS_VERSION}"
fi
info "Build plugin:  ${BUILD_PLUGIN_GROUP}:${BUILD_PLUGIN_ARTIFACT}:${BUILD_PLUGIN_VERSION}"
info "Import plugin: ${IMPORT_PLUGIN_GROUP}:${IMPORT_PLUGIN_ARTIFACT}:${IMPORT_PLUGIN_VERSION}"
info "Foundation:    ${FOUNDATION_GROUP}:${FOUNDATION_ARTIFACT}:${FOUNDATION_VERSION}"
info "Engine RT:     ${ENGINE_RT_GROUP}:${ENGINE_RT_ARTIFACT}:${ENGINE_RT_VERSION}"
info "Engine API:    ${ENGINE_API_GROUP}:${ENGINE_API_ARTIFACT}:${ENGINE_API_VERSION}"
if [[ "${EXEC_API_COORDS}" != "MISSING" && -n "${EXEC_API_COORDS}" ]]; then
  info "Execution API: ${EXEC_API_GROUP}:${EXEC_API_ARTIFACT}:${EXEC_API_VERSION}"
fi
if [[ "${ML_INTEG_COORDS}" != "MISSING" && -n "${ML_INTEG_COORDS}" ]]; then
  info "ML Integration: ${ML_INTEG_GROUP}:${ML_INTEG_ARTIFACT}:${ML_INTEG_VERSION}"
fi

# ═════════════════════════════════════════════════════════════════════════════
# Section 5: Download and install IBM DI artifacts into an isolated local repo
# ═════════════════════════════════════════════════════════════════════════════
section "Step 5: Installing DI artifacts into isolated Maven repository"

# Use caller-specified local repo or a directory inside WORK_DIR
if [[ -n "${LOCAL_REPO_ARG}" ]]; then
  LOCAL_REPO="${LOCAL_REPO_ARG}"
  mkdir -p "${LOCAL_REPO}"
  info "Using persistent local Maven repository: ${LOCAL_REPO}"
else
  LOCAL_REPO="${WORK_DIR}/local-repo"
  mkdir -p "${LOCAL_REPO}"
  debug "Isolated local Maven repository: ${LOCAL_REPO}"
fi

# ── Download helper ───────────────────────────────────────────────────────────
# Downloads an artifact from the DI /ads/<path> endpoint and installs it
# into the local Maven repository. Also installs the POM from Maven repo.
install_di_artifact() {
  local group="$1"
  local artifact="$2"
  local version="$3"
  local packaging="$4"       # jar, pom, zip
  local ads_path="$5"        # relative path from index.json (starts with static/...)
  local pom_xml="$6"         # XML string for POM (empty = fetch from repo)
  local install_as_plugin="${7:-false}"  # true = maven-plugin packaging

  local m2_group_path="${group//.//}"
  local m2_dir="${LOCAL_REPO}/${m2_group_path}/${artifact}/${version}"

  mkdir -p "${m2_dir}"

  # ── Purge negative-cache markers ──────────────────────────────────────────
  # Maven writes .lastUpdated files when a remote fetch fails. If such a file
  # exists alongside a valid artifact, Maven still reports the artifact as
  # "(present, but unavailable)" and refuses to use it — even if the bytes are
  # correct on disk.  This happens when:
  #   • The repo ID in _remote.repositories doesn't match any configured repo
  #     (e.g. artifact was installed from "ads-release" but our settings use
  #     "ads-maven"), causing Maven to attempt re-validation from "ads-maven".
  #   • The re-validation request fails (TLS, auth, timeout), writing a fresh
  #     .lastUpdated failure entry.
  # We remove all .lastUpdated files for this artifact/version directory before
  # writing anything, so they cannot block the subsequent Maven build.
  find "${m2_dir}" -maxdepth 1 -name "*.lastUpdated" -delete 2>/dev/null || true
  debug "  Cleared any .lastUpdated markers in ${m2_dir}"

  # Skip re-downloading if the artifact file is already present.
  # We still fall through to write _remote.repositories and the POM so that
  # the repo ID is always recorded as "ads-maven" (matching our settings.xml),
  # which prevents Maven from attempting remote re-validation on future runs.
  local already_installed=false
  if [[ -f "${m2_dir}/${artifact}-${version}.${packaging}" ]]; then
    already_installed=true
    debug "  Artifact already present: ${group}:${artifact}:${version}"
  fi

  if [[ "${already_installed}" == "false" ]]; then
    info "  Downloading ${group}:${artifact}:${version}..."

    local jar_dest="${m2_dir}/${artifact}-${version}.${packaging}"

    # Download the artifact JAR/ZIP
    local download_url="${DI_URL}/${ads_path}"
    local http_code
    http_code=$(curl "${CURL_TLS_FLAGS[@]+"${CURL_TLS_FLAGS[@]}"}" --silent --max-time 120 \
      -H "Authorization: ${AUTH_HEADER}" \
      -o "${jar_dest}" \
      -w "%{http_code}" \
      "${download_url}")

    if [[ "${http_code}" != "200" ]]; then
      rm -f "${jar_dest}"
      error "Failed to download ${artifact}-${version}.${packaging} (HTTP ${http_code})."
      cat >&2 <<EOF

Download URL: ${download_url}
Possible causes:
  • Authentication expired — regenerate the API key.
  • Artifact path in index.json is incorrect (DI version mismatch).
  • Network timeout — retry or check connectivity.

Manual test:
  curl ${CURL_TLS_FLAGS[@]+"${CURL_TLS_FLAGS[@]}"} -o /tmp/${artifact}.${packaging} \\
    -H "Authorization: ${AUTH_MASKED}" \\
    "${download_url}"
EOF
      return 1
    fi
  fi

  # Write or fetch the POM (always, even when the artifact was already present,
  # so the POM reflects the correct content and repo provenance).
  local pom_dest="${m2_dir}/${artifact}-${version}.pom"
  if [[ -n "${pom_xml}" ]]; then
    # Use provided POM string (needed for plugin POM with dependencies)
    printf '%s\n' "${pom_xml}" > "${pom_dest}"
    debug "  Wrote synthetic POM for ${artifact}-${version}"
  elif [[ "${already_installed}" == "false" ]]; then
    # Attempt to fetch POM from the DI Maven repo (only when JAR was just downloaded)
    local pom_path="${ads_path%.*}.pom"
    local pom_http
    pom_http=$(curl "${CURL_TLS_FLAGS[@]+"${CURL_TLS_FLAGS[@]}"}" --silent --max-time 30 \
      -H "Authorization: ${AUTH_HEADER}" \
      -o "${pom_dest}" \
      -w "%{http_code}" \
      "${DI_URL}/${pom_path}")

    if [[ "${pom_http}" != "200" ]]; then
      # Fall back to minimal generated POM
      cat > "${pom_dest}" <<POMEOF
<?xml version="1.0" encoding="UTF-8"?>
<project>
  <modelVersion>4.0.0</modelVersion>
  <groupId>${group}</groupId>
  <artifactId>${artifact}</artifactId>
  <version>${version}</version>
  <packaging>${packaging}</packaging>
</project>
POMEOF
      debug "  POM not available from server; wrote minimal POM for ${artifact}-${version}"
    fi
  fi

  # ── Write _remote.repositories ─────────────────────────────────────────────
  # This file tells Maven which repository each artifact came from.  Writing it
  # with "ads-maven" (the repo ID in our generated settings.xml) prevents Maven
  # from treating the artifacts as "locally installed" with an unknown origin and
  # attempting to re-validate them from the remote repository on every build.
  # Without this, artifacts originally installed from a differently-named repo
  # (e.g. "ads-release" from an Artifactory mirror) cause Maven to contact
  # "ads-maven" for re-validation, fail, and write a .lastUpdated negative-cache
  # entry — producing the "(present, but unavailable)" error.
  local remote_repos_file="${m2_dir}/_remote.repositories"
  {
    printf '#NOTE: This is a Maven Resolver internal implementation file, its format can be changed without prior notice.\n'
    printf '%s-%s.%s>ads-maven=\n' "${artifact}" "${version}" "${packaging}"
    printf '%s-%s.pom>ads-maven=\n' "${artifact}" "${version}"
  } > "${remote_repos_file}"
  debug "  Wrote _remote.repositories for ${group}:${artifact}:${version} (repo: ads-maven)"

  debug "  Installed ${artifact}-${version}.${packaging} → ${m2_dir}"
}

# ── Build plugin POM (must declare foundation dependency for plugin classloader) ──
# Written via heredoc to avoid embedding XML special characters in a shell variable.
BUILD_PLUGIN_POM_FILE="${WORK_DIR}/build-plugin.pom"
cat > "${BUILD_PLUGIN_POM_FILE}" <<PLUGINPOM
<?xml version="1.0" encoding="UTF-8"?>
<project>
  <modelVersion>4.0.0</modelVersion>
  <groupId>${BUILD_PLUGIN_GROUP}</groupId>
  <artifactId>${BUILD_PLUGIN_ARTIFACT}</artifactId>
  <version>${BUILD_PLUGIN_VERSION}</version>
  <packaging>maven-plugin</packaging>
  <dependencies>
    <dependency>
      <groupId>${FOUNDATION_GROUP}</groupId>
      <artifactId>${FOUNDATION_ARTIFACT}</artifactId>
      <version>${FOUNDATION_VERSION}</version>
    </dependency>
  </dependencies>
</project>
PLUGINPOM
BUILD_PLUGIN_POM="$(<"${BUILD_PLUGIN_POM_FILE}")"

# ── Import plugin POM (same foundation dependency as build plugin) ─────────────
# The import-maven-plugin is required by external-library-ads (import:import goal).
# Installing it here guarantees it is available in the isolated local repo so that
# the external-library-ads Maven invocation never needs to contact ads-maven for it.
IMPORT_PLUGIN_POM_FILE="${WORK_DIR}/import-plugin.pom"
cat > "${IMPORT_PLUGIN_POM_FILE}" <<IMPORTPLUGPOM
<?xml version="1.0" encoding="UTF-8"?>
<project>
  <modelVersion>4.0.0</modelVersion>
  <groupId>${IMPORT_PLUGIN_GROUP}</groupId>
  <artifactId>${IMPORT_PLUGIN_ARTIFACT}</artifactId>
  <version>${IMPORT_PLUGIN_VERSION}</version>
  <packaging>maven-plugin</packaging>
  <dependencies>
    <dependency>
      <groupId>${FOUNDATION_GROUP}</groupId>
      <artifactId>${FOUNDATION_ARTIFACT}</artifactId>
      <version>${FOUNDATION_VERSION}</version>
    </dependency>
  </dependencies>
</project>
IMPORTPLUGPOM
IMPORT_PLUGIN_POM="$(<"${IMPORT_PLUGIN_POM_FILE}")"

# Install foundation first (required by build plugin classloader)
install_di_artifact \
  "${FOUNDATION_GROUP}" "${FOUNDATION_ARTIFACT}" "${FOUNDATION_VERSION}" \
  "jar" "${FOUNDATION_PATH}" "" false

# Install build plugin with its dependency-aware POM
install_di_artifact \
  "${BUILD_PLUGIN_GROUP}" "${BUILD_PLUGIN_ARTIFACT}" "${BUILD_PLUGIN_VERSION}" \
  "jar" "${BUILD_PLUGIN_PATH}" "${BUILD_PLUGIN_POM}" true

# Install import plugin with its dependency-aware POM.
# This is required by external-library-ads (import:import goal) and must be
# present in the isolated LOCAL_REPO before external-library-ads is built so
# that Maven does not need to resolve it from the ads-maven remote repository.
# Without this, Maven may find a .lastUpdated negative-cache marker from a
# prior failed resolution attempt and report the POM as "(present, but
# unavailable)", causing the external-library-ads build to fail even though
# the manual `mvn install -f external-library-ads/pom.xml` would succeed.
install_di_artifact \
  "${IMPORT_PLUGIN_GROUP}" "${IMPORT_PLUGIN_ARTIFACT}" "${IMPORT_PLUGIN_VERSION}" \
  "jar" "${IMPORT_PLUGIN_PATH}" "${IMPORT_PLUGIN_POM}" true

# Install engine-compact-runtime
install_di_artifact \
  "${ENGINE_RT_GROUP}" "${ENGINE_RT_ARTIFACT}" "${ENGINE_RT_VERSION}" \
  "jar" "${ENGINE_RT_PATH}" "" false

# Install engine-de-api
install_di_artifact \
  "${ENGINE_API_GROUP}" "${ENGINE_API_ARTIFACT}" "${ENGINE_API_VERSION}" \
  "jar" "${ENGINE_API_PATH}" "" false

# Install annotations (optional; transitive dep of foundation on some DI versions)
if [[ "${ANNOTATIONS_COORDS}" != "MISSING" && -n "${ANNOTATIONS_COORDS}" ]]; then
  install_di_artifact \
    "${ANNOTATIONS_GROUP}" "${ANNOTATIONS_ARTIFACT}" "${ANNOTATIONS_VERSION}" \
    "jar" "${ANNOTATIONS_PATH}" "" false
fi

# Install execution-api (optional; needed if pom.xml references it)
if [[ "${EXEC_API_COORDS}" != "MISSING" && -n "${EXEC_API_COORDS}" ]]; then
  install_di_artifact \
    "${EXEC_API_GROUP}" "${EXEC_API_ARTIFACT}" "${EXEC_API_VERSION}" \
    "jar" "${EXEC_API_PATH}" "" false
fi

# Install ml-integration-runtime (optional; needed if pom.xml references it)
if [[ "${ML_INTEG_COORDS}" != "MISSING" && -n "${ML_INTEG_COORDS}" ]]; then
  install_di_artifact \
    "${ML_INTEG_GROUP}" "${ML_INTEG_ARTIFACT}" "${ML_INTEG_VERSION}" \
    "jar" "${ML_INTEG_PATH}" "" false
fi

# Install decision-ml-predictor (optional; needed if pom.xml references it)
if [[ "${ML_PRED_COORDS}" != "MISSING" && -n "${ML_PRED_COORDS}" ]]; then
  install_di_artifact \
    "${ML_PRED_GROUP}" "${ML_PRED_ARTIFACT}" "${ML_PRED_VERSION}" \
    "jar" "${ML_PRED_PATH}" "" false
fi

info "DI artifacts installed into local Maven repository."

# ═════════════════════════════════════════════════════════════════════════════
# Section 6: Generate isolated Maven settings.xml
# ═════════════════════════════════════════════════════════════════════════════
section "Step 6: Generating isolated Maven settings"

MVN_SETTINGS="${WORK_DIR}/settings.xml"

# The settings file configures:
#   • An isolated local repository (won't pollute ~/.m2/repository).
#   • The DI static maven-releases endpoint as both a dependency and plugin repo.
#   • The Zen API key auth header for that repository.
cat > "${MVN_SETTINGS}" <<SETTINGS_EOF
<?xml version="1.0" encoding="UTF-8"?>
<settings xmlns="http://maven.apache.org/SETTINGS/1.2.0"
          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
          xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.2.0
            http://maven.apache.org/xsd/settings-1.2.0.xsd">

  <!-- Use the isolated local repo supplied to the script.
       This prevents modification of the user's global ~/.m2/repository. -->
  <localRepository>${LOCAL_REPO}</localRepository>

  <profiles>
    <profile>
      <id>ads-build</id>
      <repositories>
        <repository>
          <id>ads-maven</id>
          <name>ADS Maven repository</name>
          <url>${MAVEN_REPO_URL}</url>
          <snapshots><enabled>false</enabled></snapshots>
          <releases><updatePolicy>never</updatePolicy></releases>
        </repository>
      </repositories>
      <pluginRepositories>
        <pluginRepository>
          <id>ads-maven</id>
          <name>ADS Maven repository (plugins)</name>
          <url>${MAVEN_REPO_URL}</url>
          <snapshots><enabled>false</enabled></snapshots>
          <releases><updatePolicy>never</updatePolicy></releases>
        </pluginRepository>
      </pluginRepositories>
    </profile>
  </profiles>

  <activeProfiles>
    <activeProfile>ads-build</activeProfile>
  </activeProfiles>

  <servers>
    <!-- Zen API key authentication for the DI Maven repository.
         Header format: Authorization: ZenApiKey base64(username:apikey) -->
    <server>
      <id>ads-maven</id>
      <configuration>
        <connectTimeout>10000</connectTimeout>
        <readTimeout>60000</readTimeout>
        <httpHeaders>
          <property>
            <name>Authorization</name>
            <value>ZenApiKey ${DI_API_KEY_B64}</value>
          </property>
        </httpHeaders>
      </configuration>
    </server>
  </servers>

</settings>
SETTINGS_EOF

debug "Maven settings written to: ${MVN_SETTINGS}"

# ═════════════════════════════════════════════════════════════════════════════
# Section 7: Prepare the Decision Project
# ═════════════════════════════════════════════════════════════════════════════
section "Step 7: Preparing Decision Project"

PROJECT_DIR=""

if [[ -f "${PROJECT_PATH}" && "${PROJECT_PATH}" == *.zip ]]; then
  # ── ZIP input: extract into work directory ──────────────────────────────────
  info "Extracting ZIP: ${PROJECT_PATH}"
  EXTRACT_DIR="${WORK_DIR}/project-extract"
  mkdir -p "${EXTRACT_DIR}"

  unzip -q "${PROJECT_PATH}" -d "${EXTRACT_DIR}"

  # Locate the Decision Project root inside the extracted ZIP.
  #
  # Three layouts are supported:
  #
  #   (A) Project at root:
  #         extract/
  #         ├── pom.xml
  #         └── .decisionservice/pom.xml
  #
  #   (B) Project inside a single top-level directory:
  #         extract/
  #         └── My Decision Project/
  #             ├── pom.xml
  #             └── .decisionservice/pom.xml
  #
  #   (C) Project inside a top-level directory alongside unrelated ZIP metadata
  #       (e.g. signed JARs add META-INF/ at root):
  #         extract/
  #         ├── META-INF/          ← signing metadata, not a Decision Project
  #         └── FraudDetectionService/
  #             ├── pom.xml
  #             └── .decisionservice/pom.xml
  #
  # Detection algorithm:
  #   1. If extract root is itself a valid project (has pom.xml + .decisionservice/pom.xml) → use it.
  #   2. Otherwise scan every immediate subdirectory for the same signature.
  #      Accept the first (and only) subdir that qualifies; fail if zero or more than one match.
  #
  # This is deliberately structure-driven rather than name-driven so that it works
  # for any Decision Project name without hard-coding specific paths.

  _is_decision_project() {
    local dir="$1"
    [[ -f "${dir}/pom.xml" && -f "${dir}/.decisionservice/pom.xml" ]]
  }

  PROJECT_DIR=""

  if _is_decision_project "${EXTRACT_DIR}"; then
    # Layout (A): project is at the ZIP root
    PROJECT_DIR="${EXTRACT_DIR}"
    debug "ZIP root is the Decision Project"
  else
    # Layouts (B) and (C): scan immediate subdirectories
    CANDIDATE_DIRS=()
    for candidate in "${EXTRACT_DIR}"/*/; do
      [[ -d "${candidate}" ]] || continue
      if _is_decision_project "${candidate%/}"; then
        CANDIDATE_DIRS+=("${candidate%/}")
      fi
    done

    if [[ ${#CANDIDATE_DIRS[@]} -eq 1 ]]; then
      PROJECT_DIR="${CANDIDATE_DIRS[0]}"
      debug "ZIP top-level project directory: ${PROJECT_DIR}"
    elif [[ ${#CANDIDATE_DIRS[@]} -eq 0 ]]; then
      die "Cannot locate a Decision Project inside the extracted ZIP. Expected a directory with pom.xml and .decisionservice/pom.xml."
    else
      die "Multiple Decision Project candidates found inside the ZIP: ${CANDIDATE_DIRS[*]}. The ZIP must contain exactly one Decision Project."
    fi
  fi
elif [[ -d "${PROJECT_PATH}" ]]; then
  # ── Directory input ─────────────────────────────────────────────────────────
  PROJECT_DIR="$(cd "${PROJECT_PATH}" && pwd)"
else
  die "Decision Project must be a directory or a .zip file: ${PROJECT_PATH}"
fi

info "Project directory: ${PROJECT_DIR}"

# ── Validate required files ──────────────────────────────────────────────────
# A valid DI Decision Project must have:
#   pom.xml                        — parent reactor POM
#   .decisionservice/pom.xml       — Decision Service assembly module
#   .decisionservice/META-INF/decisionServiceArchive.json — metadata template
#   At least one rule project subdir containing pom.xml + .ruleproject

for required in \
  "pom.xml" \
  ".decisionservice/pom.xml" \
  ".decisionservice/META-INF/decisionServiceArchive.json"; do
  if [[ ! -f "${PROJECT_DIR}/${required}" ]]; then
    error "Invalid Decision Project: missing required file: ${required}"
    cat >&2 <<EOF

Expected structure:
  <project-root>/
  ├── pom.xml                                 (parent reactor)
  ├── .decisionservice/
  │   ├── pom.xml                             (assembly module)
  │   └── META-INF/
  │       └── decisionServiceArchive.json
  ├── <Rule Project 1>/
  │   ├── pom.xml
  │   ├── .ruleproject
  │   └── deployment/
  │       ├── deployment.dep
  │       └── *.dop
  └── <Rule Project N>/
      └── ...
EOF
    exit 1
  fi
done

# Confirm at least one rule project exists
RULE_PROJECTS=()
while IFS= read -r -d '' rp; do
  RULE_PROJECTS+=("$(dirname "${rp}")")
done < <(find "${PROJECT_DIR}" -maxdepth 2 -name ".ruleproject" -print0 2>/dev/null)

if [[ ${#RULE_PROJECTS[@]} -eq 0 ]]; then
  die "No rule projects found (.ruleproject marker missing). Invalid Decision Project."
fi
info "Rule projects found: ${#RULE_PROJECTS[@]}"
for rp in "${RULE_PROJECTS[@]}"; do
  debug "  $(basename "${rp}")"
done

# ── Parse project metadata ────────────────────────────────────────────────────
PROJECT_POM="${PROJECT_DIR}/pom.xml"
DS_POM="${PROJECT_DIR}/.decisionservice/pom.xml"

SERVICE_NAME=$(python3 -c "
import xml.etree.ElementTree as ET, sys
ns = {'m': 'http://maven.apache.org/POM/4.0.0'}
root = ET.parse('${PROJECT_POM}').getroot()
name = root.findtext('m:name', namespaces=ns) or root.findtext('m:artifactId', namespaces=ns) or 'unknown'
print(name)
" 2>/dev/null || echo "unknown")

DS_ARTIFACT=$(python3 -c "
import xml.etree.ElementTree as ET, sys
ns = {'m': 'http://maven.apache.org/POM/4.0.0'}
root = ET.parse('${DS_POM}').getroot()
print(root.findtext('m:artifactId', namespaces=ns) or 'decisionservice')
" 2>/dev/null || echo "decisionservice")

# Project version: use override if given, else read from POM
if [[ -n "${VERSION_OVERRIDE}" ]]; then
  PROJECT_VERSION="${VERSION_OVERRIDE}"
  info "Using version override: ${PROJECT_VERSION}"
else
  PROJECT_VERSION=$(python3 -c "
import xml.etree.ElementTree as ET, sys
ns = {'m': 'http://maven.apache.org/POM/4.0.0'}
root = ET.parse('${PROJECT_POM}').getroot()
ver = root.findtext('m:version', namespaces=ns)
print(ver or 'LATEST-SNAPSHOT')
" 2>/dev/null || echo "LATEST-SNAPSHOT")
  info "Project version from POM: ${PROJECT_VERSION}"
fi

info "Service name: ${SERVICE_NAME}"
info "Assembly artifactId: ${DS_ARTIFACT}"

# ═════════════════════════════════════════════════════════════════════════════
# Section 8: Synchronise DI artifact versions with Designer
# ═════════════════════════════════════════════════════════════════════════════
section "Step 8: Synchronising DI artifact versions"

# All DI artifact versions (build plugin, engine runtime, engine API) were
# determined in Step 4 from index.json and are the single source of truth.
# The decision project's root pom.xml stores these as Maven properties:
#
#   <ads-mojo.version>           — build-maven-plugin version
#   <engine-compact-runtime.version> — engine runtime version
#   <engine-api.version>         — engine API version
#
# If any property differs from the Designer value, patch it before building.
# Only these three DI-managed properties are touched; every other version,
# dependency, or project file is left unchanged.
# The operation is idempotent: properties that already match are not rewritten.

_sync_di_versions() {
  local pom="$1"              # path to the project root pom.xml
  local plugin_ver="$2"       # build-maven-plugin version from Designer
  local engine_rt_ver="$3"    # engine-compact-runtime version from Designer
  local engine_api_ver="$4"   # engine-de-api version from Designer

  # Read all three current property values from the POM in one python3 call
  local current_values
  current_values=$(python3 - "${pom}" <<'PYEOF'
import xml.etree.ElementTree as ET, sys
pom = sys.argv[1]
ns = {'m': 'http://maven.apache.org/POM/4.0.0'}
try:
    root = ET.parse(pom).getroot()
    props = root.find('m:properties', namespaces=ns)
    def get(name):
        if props is None:
            return ''
        el = props.find('m:' + name, namespaces=ns)
        return (el.text or '').strip() if el is not None else ''
    print(get('ads-mojo.version'))
    print(get('engine-compact-runtime.version'))
    print(get('engine-api.version'))
except Exception:
    print(''); print(''); print('')
PYEOF
) || true

  local cur_plugin cur_engine_rt cur_engine_api
  cur_plugin=$(    echo "${current_values}" | sed -n '1p')
  cur_engine_rt=$( echo "${current_values}" | sed -n '2p')
  cur_engine_api=$(echo "${current_values}" | sed -n '3p')

  if [[ -z "${cur_plugin}" && -z "${cur_engine_rt}" && -z "${cur_engine_api}" ]]; then
    warn "  ${pom}: no DI version properties found — skipping version sync."
    return 0
  fi

  # Determine which properties need patching
  local need_patch=false
  [[ -n "${cur_plugin}"     && "${cur_plugin}"     != "${plugin_ver}"    ]] && need_patch=true
  [[ -n "${cur_engine_rt}"  && "${cur_engine_rt}"  != "${engine_rt_ver}" ]] && need_patch=true
  [[ -n "${cur_engine_api}" && "${cur_engine_api}" != "${engine_api_ver}" ]] && need_patch=true

  if [[ "${need_patch}" == "false" ]]; then
    info "  All DI versions already match Designer — no patch needed."
    return 0
  fi

  info "  Patching ${pom}"
  [[ -n "${cur_plugin}"     && "${cur_plugin}"     != "${plugin_ver}"     ]] && \
    info "    ads-mojo.version:                ${cur_plugin} → ${plugin_ver}"
  [[ -n "${cur_engine_rt}"  && "${cur_engine_rt}"  != "${engine_rt_ver}"  ]] && \
    info "    engine-compact-runtime.version:  ${cur_engine_rt} → ${engine_rt_ver}"
  [[ -n "${cur_engine_api}" && "${cur_engine_api}" != "${engine_api_ver}" ]] && \
    info "    engine-api.version:              ${cur_engine_api} → ${engine_api_ver}"

  # Rewrite only the three DI-managed property elements, preserving everything else.
  # Uses a single regex pass per property so whitespace and attribute ordering
  # in the rest of the file are never touched.
  python3 - "${pom}" "${plugin_ver}" "${engine_rt_ver}" "${engine_api_ver}" \
            "${cur_plugin}" "${cur_engine_rt}" "${cur_engine_api}" <<'PYEOF'
import re, sys
pom_path       = sys.argv[1]
plugin_ver     = sys.argv[2]
engine_rt_ver  = sys.argv[3]
engine_api_ver = sys.argv[4]
cur_plugin     = sys.argv[5]
cur_engine_rt  = sys.argv[6]
cur_engine_api = sys.argv[7]

with open(pom_path, 'r', encoding='utf-8') as fh:
    content = fh.read()

def patch_prop(text, tag, new_val, old_val):
    """Replace <tag>old_val</tag> with <tag>new_val</tag> (first occurrence)."""
    if not old_val or old_val == new_val:
        return text
    pattern = r'(<' + re.escape(tag) + r'>)[^<]*(</\s*' + re.escape(tag) + r'>)'
    return re.sub(pattern, r'\g<1>' + new_val + r'\g<2>', text, count=1)

content = patch_prop(content, 'ads-mojo.version',
                     plugin_ver,     cur_plugin)
content = patch_prop(content, 'engine-compact-runtime.version',
                     engine_rt_ver,  cur_engine_rt)
content = patch_prop(content, 'engine-api.version',
                     engine_api_ver, cur_engine_api)

with open(pom_path, 'w', encoding='utf-8') as fh:
    fh.write(content)
PYEOF

  info "  Patch applied successfully."
}

# Fail clearly if the Designer versions were not determined
if [[ -z "${BUILD_PLUGIN_VERSION}" ]]; then
  die "build-maven-plugin version could not be determined from Designer. Cannot proceed."
fi

info "Designer versions:"
info "  build-maven-plugin:           ${BUILD_PLUGIN_VERSION}"
info "  engine-compact-runtime:       ${ENGINE_RT_VERSION}"
info "  engine-de-api:                ${ENGINE_API_VERSION}"
_sync_di_versions "${PROJECT_DIR}/pom.xml" \
  "${BUILD_PLUGIN_VERSION}" "${ENGINE_RT_VERSION}" "${ENGINE_API_VERSION}"

# ═════════════════════════════════════════════════════════════════════════════
# Section 9: Run Maven build
# ═════════════════════════════════════════════════════════════════════════════
section "Step 9: Building Decision Service archive"

echo
echo "${BOLD}Decision Project: ${SERVICE_NAME}${NC}"
echo "${BOLD}DI URL:          ${DI_URL}${NC}"
echo "${BOLD}Maven:            ${MVN_CMD}${NC}"
echo "${BOLD}DI build plugin: ${BUILD_PLUGIN_GROUP}:${BUILD_PLUGIN_ARTIFACT}:${BUILD_PLUGIN_VERSION}${NC}"
echo "${BOLD}Build started...${NC}"
echo

MVN_LOG="${WORK_DIR}/mvn-build.log"

_print_build_failure() {
  local log="$1"
  echo
  echo "${YELLOW}Likely failure reason:${NC}"
  grep -E "^\[ERROR\]" "${log}" 2>/dev/null | head -20 || true
}

# Build the Maven command
# -U forces Maven to re-check remote repositories for any dependency whose
# local cache entry is a recorded failure (negative cache hit).  This is
# necessary when the project was generated against an older DI instance and
# previously resolved — or attempted to resolve — engine artifacts at a
# version that no longer exists on the current DI Maven repository.
MVN_ARGS=(
  package
  -DskipTests
  -U
  -f "${PROJECT_DIR}/pom.xml"
  -s "${MVN_SETTINGS}"
  "-Dmaven.repo.local=${LOCAL_REPO}"
  "-Dlocalm2=${LOCAL_REPO}"
  --no-transfer-progress
  # Version override: sets project.version and parent.version for the reactor
)

# Apply version override — update the pom.xml using Maven versions plugin
if [[ -n "${VERSION_OVERRIDE}" ]]; then
  MVN_ARGS+=("-Drevision=${VERSION_OVERRIDE}")
fi

# Append TLS flags if needed
if [[ "${#MVN_TLS_FLAGS[@]}" -gt 0 ]]; then
  MVN_ARGS+=("${MVN_TLS_FLAGS[@]}")
fi

# Run the Maven build.
# In verbose mode, tee output to the log AND the terminal.
# Use a FIFO + explicit wait to correctly capture Maven's exit code through
# the tee pipeline (pipefail only works in simple pipelines, not compound
# commands behind `if !`).
MVN_EXIT=0
if [[ "${VERBOSE}" == "true" ]]; then
  debug "Maven command: ${MVN_CMD} ${MVN_ARGS[*]}"
  "${MVN_CMD}" "${MVN_ARGS[@]}" 2>&1 | tee "${MVN_LOG}" || MVN_EXIT=${PIPESTATUS[0]}
else
  "${MVN_CMD}" "${MVN_ARGS[@]}" > "${MVN_LOG}" 2>&1 || MVN_EXIT=$?
fi

if [[ "${MVN_EXIT}" -ne 0 ]]; then
  echo
  error "Maven build FAILED (exit ${MVN_EXIT})."
  if [[ "${VERBOSE}" != "true" ]]; then
    echo
    echo "${YELLOW}Last 40 lines of build log:${NC}"
    tail -40 "${MVN_LOG}" >&2
  fi
  # Copy the log out of WORK_DIR before cleanup removes it
  SAVED_LOG="${OUTPUT_DIR}/mvn-build-failure.log"
  mkdir -p "${OUTPUT_DIR}"
  cp "${MVN_LOG}" "${SAVED_LOG}" 2>/dev/null || true
  echo
  error "Full build log saved to: ${SAVED_LOG}"
  _print_build_failure "${MVN_LOG}"
  exit 1
fi

echo
echo "${GREEN}${BOLD}Build completed successfully.${NC}"

# ═════════════════════════════════════════════════════════════════════════════
# Section 10: Locate and copy the generated archive
# ═════════════════════════════════════════════════════════════════════════════
section "Step 10: Locating and copying the Decision Service archive"

# The archive is produced by the .decisionservice module (maven-shade-plugin)
# in: <project>/.decisionservice/target/<artifactId>-<version>.jar
# The shade plugin also produces original-*.jar (pre-shade); we want the main jar.
DS_TARGET_DIR="${PROJECT_DIR}/.decisionservice/target"

# Glob: match <artifactId>-<version>.jar, exclude original- prefix
GENERATED_JARS=()
while IFS= read -r -d '' jar; do
  # Exclude the original- prefix (pre-shade artifact)
  [[ "$(basename "${jar}")" == original-* ]] && continue
  GENERATED_JARS+=("${jar}")
done < <(find "${DS_TARGET_DIR}" -maxdepth 1 -name "${DS_ARTIFACT}-*.jar" -print0 2>/dev/null)

if [[ ${#GENERATED_JARS[@]} -eq 0 ]]; then
  error "No Decision Service archive found in: ${DS_TARGET_DIR}"
  cat >&2 <<EOF

Expected: ${DS_ARTIFACT}-<version>.jar

Troubleshooting:
  1. The build log may show a failure after "BUILD SUCCESS" for a specific module.
     Check: ${SAVED_LOG:-${MVN_LOG}}
  2. The .decisionservice/pom.xml packaging must be 'jar' and must include
     the maven-shade-plugin bound to the 'package' phase.
  3. Run with --verbose for full Maven output.
EOF
  exit 1
fi

# If multiple (shouldn't happen but be safe), take the most recent
GENERATED_JAR="${GENERATED_JARS[0]}"
if [[ ${#GENERATED_JARS[@]} -gt 1 ]]; then
  GENERATED_JAR=$(ls -t "${GENERATED_JARS[@]}" | head -1)
  warn "Multiple archives found; using most recent: $(basename "${GENERATED_JAR}")"
fi

ARCHIVE_BASENAME=$(basename "${GENERATED_JAR}")
info "Archive found: ${ARCHIVE_BASENAME}  ($(du -h "${GENERATED_JAR}" | cut -f1))"

# ── Copy to output directory ───────────────────────────────────────────────────
mkdir -p "${OUTPUT_DIR}"

# Name the output file after the service name + version for clarity
# Replace spaces with underscores in service name
SAFE_NAME="${SERVICE_NAME// /_}"
OUTPUT_FILENAME="${SAFE_NAME}_${PROJECT_VERSION}.jar"
OUTPUT_PATH="${OUTPUT_DIR}/${OUTPUT_FILENAME}"

cp "${GENERATED_JAR}" "${OUTPUT_PATH}"

# ─── Final report ─────────────────────────────────────────────────────────────
echo
echo "${BOLD}╔══════════════════════════════════════════════════╗${NC}"
echo "${BOLD}║  Build Summary                                   ║${NC}"
echo "${BOLD}╚══════════════════════════════════════════════════╝${NC}"
echo
echo "  ${BOLD}Decision Project :${NC} ${SERVICE_NAME}"
echo "  ${BOLD}Version          :${NC} ${PROJECT_VERSION}"
echo "  ${BOLD}DI URL          :${NC} ${DI_URL}"
echo "  ${BOLD}Build plugin     :${NC} ${BUILD_PLUGIN_GROUP}:${BUILD_PLUGIN_ARTIFACT}:${BUILD_PLUGIN_VERSION}"
echo "  ${BOLD}Engine runtime   :${NC} ${ENGINE_RT_GROUP}:${ENGINE_RT_ARTIFACT}:${ENGINE_RT_VERSION}"
echo
echo "  ${GREEN}${BOLD}Decision Service archive:${NC}"
echo "  ${BOLD}${OUTPUT_PATH}${NC}"
echo "  Size: $(du -h "${OUTPUT_PATH}" | cut -f1)"
echo

# Compute and print SHA-256 for integrity verification
if command -v sha256sum &>/dev/null; then
  SHA256=$(sha256sum "${OUTPUT_PATH}" | awk '{print $1}')
elif command -v shasum &>/dev/null; then
  SHA256=$(shasum -a 256 "${OUTPUT_PATH}" | awk '{print $1}')
else
  SHA256="(shasum/sha256sum not available)"
fi
echo "  SHA-256: ${SHA256}"
echo

exit 0
