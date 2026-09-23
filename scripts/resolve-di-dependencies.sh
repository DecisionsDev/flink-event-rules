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
# resolve-di-dependencies.sh
#
# Configures a locally installed IBM Automation Decision Services (ADS)
# Decision Designer instance as a Maven repository and resolves the four
# classpath items required by the Execution Java API:
#
#   - execution-api.jar
#   - engine-de-api.jar
#   - jackson-databind (version from pom.xml)
#   - jackson-datatype-jsr310 (version from pom.xml)
#
# The script queries <DECISION_DESIGNER_SERVER_URL>/ads/index.json
# to discover the exact artifact versions published by the local installation,
# then runs 'mvn dependency:resolve' against the maven-designer Maven profile
# defined in settings.xml.  No versions are assumed or hard-coded.
#
# Prerequisites:
#   - curl
#   - python3  (for JSON parsing and base64 encoding)
#   - mvn      (Apache Maven, any recent version)
#   - DI_ZEN_HOSTNAME   env var: Decision Designer hostname (e.g. cpd-cp4a.mycompany.com)
#   - DI_ZEN_USERNAME   env var: CP4BA / Zen platform username
#   - DI_ZEN_API_KEY    env var: Zen API key (plain text, not base64-encoded)
#                       Obtained from the CP4BA Platform UI:
#                       profile icon → "Generate API key"
#                       The script computes the required header:
#                       Authorization: ZenApiKey base64(username:apikey)
#
# Optional:
#   - DI_SKIP_TLS_VERIFY=true   Disable TLS certificate verification (dev/CI only)
#   - DI_SETTINGS_FILE          Path to a custom Maven settings file (default: ./settings.xml)
#
# Usage:
#   ./scripts/resolve-di-dependencies.sh
#
# Example (with self-signed certificate):
#   export DI_ZEN_HOSTNAME="cpd-cp4a.mycompany.com"
#   export DI_ZEN_USERNAME="cpadmin"
#   export DI_ZEN_API_KEY="my-zen-api-key"
#   export DI_SKIP_TLS_VERIFY=true
#   ./scripts/resolve-di-dependencies.sh
#
# What it does:
#   1. Validates required environment variables and tools
#   2. Computes DI_ZEN_API_KEY_B64 = base64(username:apikey) for the Zen header
#   3. Queries /ads/index.json to discover execution-api and engine-de-api versions
#   4. Validates the Maven repository endpoint is reachable with the resolved auth header
#   5. Verifies the versions in pom.xml match index.json (warns on mismatch)
#   6. Runs mvn dependency:resolve -P maven-designer to download all required JARs
#   7. Prints a summary of what was resolved

set -uo pipefail
# Note: -e (errexit) is intentionally omitted so every check can be reported
# before the script exits.  Failures are accumulated and reported at the end.

# ── Resolve project root (script can be run from any directory) ────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

# ── Colour helpers (matching project conventions) ──────────────────────────────
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; CYAN='\033[0;36m'; NC='\033[0m'
info()    { echo -e "${GREEN}[INFO]${NC}   $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}   $*"; }
error()   { echo -e "${RED}[ERROR]${NC}  $*" >&2; FAILED=true; }
section() { echo -e "\n${CYAN}── $* ──${NC}"; }

FAILED=false

# ── Configuration ──────────────────────────────────────────────────────────────
DI_ZEN_HOSTNAME="${DI_ZEN_HOSTNAME:-}"
DI_ZEN_USERNAME="${DI_ZEN_USERNAME:-}"
DI_ZEN_API_KEY="${DI_ZEN_API_KEY:-}"
DI_SKIP_TLS_VERIFY="${DI_SKIP_TLS_VERIFY:-false}"
DI_SETTINGS_FILE="${DI_SETTINGS_FILE:-${PROJECT_ROOT}/settings.xml}"

# Derived — initialised here so they are always bound under set -u,
# even when the index.json fetch fails and the else-branch is skipped.
DESIGNER_URL=""
AUTH_HEADER=""
INDEX_URL=""
CURL_TLS_FLAG=""
MVN_TLS_FLAG=""
EXEC_API_VERSION=""
ENGINE_DE_API_VERSION=""
POM_EXEC_VERSION=""
POM_ENGINE_VERSION=""
POM_JACKSON_VERSION=""
DI_ZEN_API_KEY_B64=""  # exported for Maven settings.xml interpolation

# ── Usage ──────────────────────────────────────────────────────────────────────
usage() {
    sed -n '3,67p' "$0" | sed 's/^# \?//'
    exit 1
}

for arg in "$@"; do
    case "$arg" in
        --help|-h) usage ;;
        *) error "Unknown argument: $arg"; usage ;;
    esac
done

# ══════════════════════════════════════════════════════════════════════════════
echo ""
echo "=========================================="
echo " DI Dependency Resolver"
echo " Local Decision Designer → Maven"
echo "=========================================="

# ── Step 1: Validate required tools ───────────────────────────────────────────
section "Step 1: Required tools"

for tool in curl python3 mvn; do
    if command -v "$tool" &>/dev/null; then
        info "✓ $tool found: $(command -v "$tool")"
    else
        error "Required tool not found: $tool"
        info "  Install $tool before running this script."
    fi
done

[[ "$FAILED" == "true" ]] && { echo ""; error "Aborting — required tools missing."; exit 1; }

# ── Step 2: Validate required environment variables ───────────────────────────
section "Step 2: Environment variables"

missing_vars=()
[[ -z "$DI_ZEN_HOSTNAME" ]]  && missing_vars+=("DI_ZEN_HOSTNAME")
[[ -z "$DI_ZEN_USERNAME" ]]  && missing_vars+=("DI_ZEN_USERNAME")
[[ -z "$DI_ZEN_API_KEY"  ]]  && missing_vars+=("DI_ZEN_API_KEY")

if [[ ${#missing_vars[@]} -gt 0 ]]; then
    for v in "${missing_vars[@]}"; do
        error "Required environment variable not set: $v"
    done
    echo ""
    echo "  Set the missing variables and re-run:"
    echo ""
    echo "    export DI_ZEN_HOSTNAME=\"cpd-cp4a.mycompany.com\""
    echo "    export DI_ZEN_USERNAME=\"cpadmin\""
    echo "    export DI_ZEN_API_KEY=\"<api-key-from-cp4ba-ui>\""
    echo ""
    echo "  To get the API key: CP4BA UI → profile icon → Generate API key"
    echo ""
    exit 1
fi

info "DI_ZEN_HOSTNAME   : $DI_ZEN_HOSTNAME"
info "DI_ZEN_USERNAME   : $DI_ZEN_USERNAME"
info "DI_ZEN_API_KEY    : ${DI_ZEN_API_KEY:0:8}… (truncated)"
info "DI_SKIP_TLS_VERIFY: $DI_SKIP_TLS_VERIFY"
info "Settings file      : $DI_SETTINGS_FILE"

if [[ ! -f "$DI_SETTINGS_FILE" ]]; then
    error "Maven settings file not found: $DI_SETTINGS_FILE"
    exit 1
fi
info "✓ Settings file found"

# ── TLS configuration ─────────────────────────────────────────────────────────
if [[ "$DI_SKIP_TLS_VERIFY" == "true" ]]; then
    warn "TLS certificate verification is DISABLED (DI_SKIP_TLS_VERIFY=true)."
    warn "This is only acceptable for development or CI environments."
    CURL_TLS_FLAG="-k"
    # Force the legacy Wagon HTTP transport.  Maven 3.9+ defaults to the native resolver
    # (httpclient/OkHttp) which:
    #   (a) ignores maven.wagon.http.ssl.* properties, and
    #   (b) mishandles chunked transfer-encoding from some enterprise gateways (CP4BA /
    #       OpenShift routes), producing "Premature end of chunk coded message body".
    # Wagon does not have this chunked-encoding bug and respects the ssl.* flags.
    # ssl.ignore.validity.dates=true is required when the server certificate has expired.
    MVN_TLS_FLAG="-Dmaven.resolver.transport=wagon -Dmaven.wagon.http.ssl.insecure=true -Dmaven.wagon.http.ssl.allowall=true -Dmaven.wagon.http.ssl.ignore.validity.dates=true"
else
    CURL_TLS_FLAG=""
    MVN_TLS_FLAG=""
    info "TLS verification: enabled"
    info "If you see certificate errors, either:"
    info "  - Import the Zen CA:  openssl s_client -connect ${DI_ZEN_HOSTNAME}:443 -showcerts </dev/null 2>/dev/null | openssl x509 -outform PEM > zen-ca.pem"
    info "    then: keytool -import -alias zen-local -file zen-ca.pem -keystore \"\$JAVA_HOME/lib/security/cacerts\" -storepass changeit -noprompt"
    info "  - Or set DI_SKIP_TLS_VERIFY=true (dev/CI only)"
fi

DESIGNER_URL="https://${DI_ZEN_HOSTNAME}"
INDEX_URL="${DESIGNER_URL}/ads/index.json"
MAVEN_REPO_URL="${DESIGNER_URL}/ads/static/maven-releases"

# ── Step 3: Computing Zen API key header ──────────────────────────────────────
# Header format (official IBM CP4BA 26.0.0 docs):
#   Authorization: ZenApiKey base64(<username>:<apikey>)
section "Step 3: Computing Zen API key header"

DI_ZEN_API_KEY_B64="$(printf '%s:%s' "$DI_ZEN_USERNAME" "$DI_ZEN_API_KEY" | python3 -c "
import sys, base64
print(base64.b64encode(sys.stdin.buffer.read()).decode().strip())
")"
export DI_ZEN_API_KEY_B64
AUTH_HEADER="ZenApiKey ${DI_ZEN_API_KEY_B64}"
info "Auth header computed: ZenApiKey ${DI_ZEN_API_KEY_B64:0:16}…"

# ── Step 4: Verify authentication and index reachability ──────────────────────
# The ADS Maven repository at /ads/static/maven-releases does not expose a
# directory listing (returns 404 on the root).  We verify auth by probing
# /ads/index.json (which we will also parse in Step 5).
section "Step 4: Verifying authentication and index reachability"
info "Repository URL: $MAVEN_REPO_URL"
info "Index URL     : $INDEX_URL"

HTTP_CODE=$(curl $CURL_TLS_FLAG -s -o /dev/null -w "%{http_code}" \
    -H "Authorization: $AUTH_HEADER" \
    "$INDEX_URL" 2>/dev/null || echo "000")

case "$HTTP_CODE" in
    2*)
        info "✓ Authentication accepted — index reachable (HTTP $HTTP_CODE)" ;;
    302)
        error "HTTP 302 — request redirected to OIDC login page; Zen API key not accepted."
        error "The Zen API key was not recognised by the CP4BA gateway."
        info  "  1. DI_ZEN_API_KEY must be a Zen API key generated from the CP4BA Platform UI"
        info  "     (profile icon → Generate API key). It is NOT your password."
        info  "  2. DI_ZEN_USERNAME must match the username in the CP4BA UI exactly."
        info  "  3. The API key may have expired — regenerate it in the UI."
        FAILED=true ;;
    401)
        error "HTTP 401 Unauthorized — authentication failed."
        error "The Zen API key was rejected by the CPD gateway."
        info  "  1. DI_ZEN_API_KEY must be a Zen API key (profile icon → Generate API key)."
        info  "     It is NOT your IBM w3id password."
        info  "  2. DI_ZEN_USERNAME must match the username in the CP4BA UI exactly."
        info  "  3. The API key may have expired — regenerate it."
        info  "  Test manually:"
        info  "    curl $CURL_TLS_FLAG -H 'Authorization: ZenApiKey \$(printf \"username:apikey\" | base64)' $INDEX_URL"
        FAILED=true ;;
    403)
        error "HTTP 403 Forbidden — the user does not have access to Decision Designer."
        FAILED=true ;;
    000)
        error "Could not connect to $INDEX_URL"
        error "Check DI_ZEN_HOSTNAME, network connectivity, and TLS configuration."
        FAILED=true ;;
    *)
        warn "Index endpoint returned HTTP $HTTP_CODE — proceeding, but this may indicate a problem."
        ;;
esac

# ── Step 5: Query index.json ───────────────────────────────────────────────────
section "Step 5: Querying index.json for artifact versions"
info "Endpoint: $INDEX_URL"

INDEX_JSON=$(curl $CURL_TLS_FLAG -s \
    -H "Authorization: $AUTH_HEADER" \
    "$INDEX_URL" 2>/dev/null || echo "")

if [[ -z "$INDEX_JSON" ]] || ! echo "$INDEX_JSON" | python3 -c "import sys,json; json.load(sys.stdin)" &>/dev/null; then
    error "Failed to fetch or parse index.json from $INDEX_URL"
    error "Verify the URL, credentials, and network connectivity."
    echo ""
    echo "  Manual check:"
    echo "    curl $CURL_TLS_FLAG -s -H 'Authorization: $AUTH_HEADER' $INDEX_URL | python3 -m json.tool"
    echo ""
    FAILED=true
else
    info "✓ index.json retrieved successfully"

    # /ads/index.json format: {"format":1, "resources": {"key": {"version":..., "maven_coordinates":{...}, "path":...}}}
    VERSIONS=$(echo "$INDEX_JSON" | python3 -c "
import json, sys
data = json.load(sys.stdin)
wanted = {'execution-api', 'engine-de-api', 'foundation'}
resources = data.get('resources', {})
for _key, info in resources.items():
    mc  = info.get('maven_coordinates', {})
    aid = mc.get('artifactId','')
    if aid in wanted:
        gid = mc.get('groupId','?')
        ver = mc.get('version', info.get('version','?'))
        print(f'{gid}:{aid}:{ver}')
" 2>/dev/null || echo "")

    if [[ -z "$VERSIONS" ]]; then
        warn "No execution-api or engine-de-api entries found in index.json."
        warn "Full index.json resources:"
        echo "$INDEX_JSON" | python3 -c "
import json,sys
d=json.load(sys.stdin)
for k,v in d.get('resources',{}).items():
    mc=v.get('maven_coordinates',{})
    print(f'  {mc.get(\"groupId\")}:{mc.get(\"artifactId\")}:{mc.get(\"version\")}')
" 2>/dev/null | head -20
    else
        info "Artifacts available from Decision Designer:"
        echo ""
        while IFS= read -r coord; do
            echo "  $coord"
        done <<< "$VERSIONS"
        echo ""
    fi

    EXEC_API_VERSION=$(echo "$INDEX_JSON" | python3 -c "
import json, sys
data = json.load(sys.stdin)
for _k, info in data.get('resources', {}).items():
    mc = info.get('maven_coordinates', {})
    if mc.get('artifactId') == 'execution-api':
        print(mc.get('version', info.get('version','')))
        break
" 2>/dev/null || echo "")

    ENGINE_DE_API_VERSION=$(echo "$INDEX_JSON" | python3 -c "
import json, sys
data = json.load(sys.stdin)
for _k, info in data.get('resources', {}).items():
    mc = info.get('maven_coordinates', {})
    if mc.get('artifactId') == 'engine-de-api':
        print(mc.get('version', info.get('version','')))
        break
" 2>/dev/null || echo "")

    if [[ -n "$EXEC_API_VERSION" ]]; then
        info "execution-api version from index.json : $EXEC_API_VERSION"
    else
        warn "execution-api not found in index.json — it may be listed under a different artifactId."
    fi

    if [[ -n "$ENGINE_DE_API_VERSION" ]]; then
        info "engine-de-api version from index.json : $ENGINE_DE_API_VERSION"
    else
        warn "engine-de-api not found in index.json — it may be listed under a different artifactId."
        warn "engine-de-api is typically a transitive dependency of execution-api."
    fi
fi

# ── Step 6: Cross-check versions against pom.xml ──────────────────────────────
# Properties may live in the child (event-rule-backend/pom.xml) or in the parent
# (pom.xml).  Search the child first; fall back to the parent if not found there.
section "Step 6: Cross-checking pom.xml against index.json"

BACKEND_POM="${PROJECT_ROOT}/event-rule-backend/pom.xml"
PARENT_POM="${PROJECT_ROOT}/pom.xml"

if [[ ! -f "$BACKEND_POM" ]]; then
    warn "event-rule-backend/pom.xml not found — skipping version cross-check."
else
    # Helper: search a property in child pom first, then parent pom
    pom_property() {
        local tag="$1"
        local val
        val=$(python3 -c "
import re, sys
pattern = r'<${tag}>(.*?)</${tag}>'
for path in sys.argv[1:]:
    try:
        m = re.search(pattern, open(path).read())
        if m:
            print(m.group(1))
            sys.exit(0)
    except Exception:
        pass
" "$BACKEND_POM" "$PARENT_POM" 2>/dev/null || echo "")
        echo "$val"
    }

    POM_EXEC_VERSION=$(pom_property   "execution\\.api\\.version")
    POM_ENGINE_VERSION=$(pom_property "engine\\.de\\.api\\.version")
    POM_JACKSON_VERSION=$(pom_property "jackson\\.version")

    info "pom.xml execution.api.version  : ${POM_EXEC_VERSION:-(not found)}"
    info "pom.xml engine.de.api.version  : ${POM_ENGINE_VERSION:-(not found)}"
    info "pom.xml jackson.version        : ${POM_JACKSON_VERSION:-(not found)}"

    if [[ -n "$EXEC_API_VERSION" && -n "$POM_EXEC_VERSION" ]]; then
        if [[ "$EXEC_API_VERSION" == "$POM_EXEC_VERSION" ]]; then
            info "✓ execution-api version matches index.json ($POM_EXEC_VERSION)"
        else
            warn "Version mismatch for execution-api:"
            warn "  pom.xml    : $POM_EXEC_VERSION"
            warn "  index.json : $EXEC_API_VERSION"
            warn "Update <execution.api.version> in pom.xml to: $EXEC_API_VERSION"
        fi
    fi

    if [[ -n "$ENGINE_DE_API_VERSION" && -n "$POM_ENGINE_VERSION" ]]; then
        if [[ "$ENGINE_DE_API_VERSION" == "$POM_ENGINE_VERSION" ]]; then
            info "✓ engine-de-api version matches index.json ($POM_ENGINE_VERSION)"
        else
            warn "Version mismatch for engine-de-api:"
            warn "  pom.xml    : $POM_ENGINE_VERSION"
            warn "  index.json : $ENGINE_DE_API_VERSION"
            warn "Update <engine.de.api.version> in pom.xml to: $ENGINE_DE_API_VERSION"
        fi
    fi

    if [[ -n "$POM_JACKSON_VERSION" ]]; then
        info "✓ jackson.version : $POM_JACKSON_VERSION"
    fi
fi

# ── Abort if pre-flight failed ─────────────────────────────────────────────────
if [[ "$FAILED" == "true" ]]; then
    echo ""
    error "Pre-flight checks failed — Maven resolution not attempted."
    error "Fix the errors above and re-run this script."
    echo ""
    exit 1
fi

# ── Step 7: Run Maven dependency resolution ────────────────────────────────────
section "Step 7: Resolving dependencies via Maven (maven-designer profile)"

info "Settings file : $DI_SETTINGS_FILE"
info "Profile       : maven-designer"
info "Project       : event-rule-backend"
info "Repository    : ${MAVEN_REPO_URL}"
echo ""

# Export so Maven can read them from settings.xml interpolation.
# For ZenApiKey mode: settings.xml uses ${env.DI_ZEN_HOSTNAME} and ${env.DI_ZEN_API_KEY_B64}
# For Bearer mode:    settings.xml uses ${env.DI_ZEN_HOSTNAME}; the Authorization header
#                     is overridden via -Dhttp.requestheader.Authorization below.
export DI_ZEN_HOSTNAME
export DI_ZEN_API_KEY_B64

MVN_CMD=(
    mvn dependency:resolve
    -s "$DI_SETTINGS_FILE"
    -P maven-designer
    -pl event-rule-backend
    --no-transfer-progress
)

# Append TLS flags only if non-empty
if [[ -n "$MVN_TLS_FLAG" ]]; then
    # shellcheck disable=SC2206
    MVN_CMD+=($MVN_TLS_FLAG)
fi

info "Running: mvn dependency:resolve -s <settings> -P maven-designer -pl event-rule-backend${MVN_TLS_FLAG:+ $MVN_TLS_FLAG}"
echo ""

if "${MVN_CMD[@]}"; then
    echo ""
    info "✓ Maven dependency resolution succeeded."
else
    MVN_EXIT=$?
    echo ""
    error "Maven dependency resolution failed (exit code $MVN_EXIT)."
    echo ""
    echo "  Common causes:"
    echo "  - Artifact not present in the Decision Designer Maven repository."
    echo "    Verify: curl $CURL_TLS_FLAG -s -H 'Authorization: $AUTH_HEADER' \\"
    echo "      '${MAVEN_REPO_URL}/com/ibm/decision/execution/execution-api/maven-metadata.xml'"
    echo ""
    echo "  - TLS certificate error."
    echo "    Set DI_SKIP_TLS_VERIFY=true (dev/CI only) or import the Zen CA into your JVM truststore."
    echo ""
    echo "  - The maven-designer profile is missing from $DI_SETTINGS_FILE."
    echo "    Check that the profile exists and DI_ZEN_HOSTNAME is set correctly."
    echo ""
    exit 1
fi

# ── Step 8: Verify the downloaded JARs ────────────────────────────────────────
section "Step 8: Verifying downloaded JARs"

M2_REPO="${HOME}/.m2/repository"
JARS_FOUND=0
JARS_MISSING=0

check_jar() {
    local gid="$1" aid="$2" ver="$3"
    local path="${M2_REPO}/$(echo "$gid" | tr '.' '/')/${aid}/${ver}/${aid}-${ver}.jar"
    if [[ -f "$path" ]]; then
        local size
        size=$(du -h "$path" | cut -f1)
        info "✓ $aid-$ver.jar  ($size)  $path"
        JARS_FOUND=$(( JARS_FOUND + 1 ))
    else
        warn "✗ $aid-$ver.jar not found in local Maven repository: $path"
        warn "  It may be resolved under a different groupId/artifactId."
        JARS_MISSING=$(( JARS_MISSING + 1 ))
    fi
}

[[ -n "$EXEC_API_VERSION" ]]       && check_jar "com.ibm.decision.execution" "execution-api" "$EXEC_API_VERSION"
[[ -n "$ENGINE_DE_API_VERSION" ]]  && check_jar "com.ibm.decision"           "engine-de-api" "$ENGINE_DE_API_VERSION"

if [[ -n "$POM_JACKSON_VERSION" ]]; then
    check_jar "com.fasterxml.jackson.core"     "jackson-databind"        "$POM_JACKSON_VERSION"
    check_jar "com.fasterxml.jackson.datatype" "jackson-datatype-jsr310" "$POM_JACKSON_VERSION"
else
    warn "jackson.version not found in pom.xml — skipping Jackson JAR verification."
fi

# ── Final summary ──────────────────────────────────────────────────────────────
echo ""
echo "=========================================="
echo " Resolution Summary"
echo "=========================================="
echo ""
info "JARs verified  : $JARS_FOUND found, $JARS_MISSING not verified"

if [[ -n "$EXEC_API_VERSION" && "$EXEC_API_VERSION" != "${POM_EXEC_VERSION:-}" ]]; then
    echo ""
    warn "Action required: update execution.api.version in pom.xml"
    warn "  Current : ${POM_EXEC_VERSION:-(not found)}"
    warn "  Required: $EXEC_API_VERSION  (from index.json)"
    echo ""
    echo "  Edit pom.xml:"
    echo "    <execution.api.version>${EXEC_API_VERSION}</execution.api.version>"
    echo ""
fi

echo ""
info "To build the project with the local Decision Designer repository:"
echo ""
echo "  mvn clean package -s settings.xml -P maven-designer -pl event-rule-backend"
if [[ "$DI_SKIP_TLS_VERIFY" == "true" ]]; then
    echo "    -Dmaven.wagon.http.ssl.insecure=true -Dmaven.wagon.http.ssl.allowall=true"
fi
echo ""
info "To verify all four required classpath items are present in the dependency tree:"
echo ""
echo "  mvn dependency:tree -s settings.xml -P maven-designer -pl event-rule-backend \\"
echo "    | grep -E 'execution-api|engine-de-api|jackson-databind|jackson-datatype-jsr310'"
echo ""
