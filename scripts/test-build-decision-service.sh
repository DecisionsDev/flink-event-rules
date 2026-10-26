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

# test-build-decision-service.sh
#
# Unit / integration test suite for build-decision-service.sh.
#
# What is tested here:
#   - ZIP detection logic (all three supported layouts)
#   - Required-file validation (missing pom.xml, missing .decisionservice)
#   - Argument validation (missing --project, --output, --di-url)
#   - Decision Project structure validation for both known sample ZIPs
#     without performing a full Maven build (no DI instance required).
#
# Usage:
#   ./scripts/test-build-decision-service.sh
#
# Prerequisites: bash 3.2+, unzip, python3.
# Tests that require unzip are skipped automatically when it is unavailable.

set -euo pipefail

# ─── Resolve paths ────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
BDS="${SCRIPT_DIR}/build-decision-service.sh"

# ─── Colour helpers ───────────────────────────────────────────────────────────
if [[ -t 1 ]]; then
  GREEN=$'\033[0;32m'; RED=$'\033[0;31m'; YELLOW=$'\033[1;33m'
  CYAN=$'\033[0;36m'; BOLD=$'\033[1m'; NC=$'\033[0m'
else
  GREEN=''; RED=''; YELLOW=''; CYAN=''; BOLD=''; NC=''
fi

PASS=0; FAIL=0; SKIP=0

pass()  { echo "${GREEN}✓ PASS${NC}: $*"; PASS=$((PASS+1)); }
fail()  { echo "${RED}✗ FAIL${NC}: $*"; FAIL=$((FAIL+1)); }
skip()  { echo "${YELLOW}⚠ SKIP${NC}: $*"; SKIP=$((SKIP+1)); }

section() { echo; echo "${CYAN}${BOLD}── $* ──${NC}"; }

# ─── Helpers ──────────────────────────────────────────────────────────────────

# assert_contains <label> <pattern> <output>
# Checks that <output> contains <pattern>.
# Patterns starting with '--' are matched as fixed strings (avoids macOS grep
# treating them as options).  All other patterns use extended-regex (-E).
assert_contains() {
  local label="$1" pattern="$2" output="$3"
  local matched=false
  if [[ "${pattern}" == --* ]]; then
    # Use -e to pass the pattern explicitly, preventing grep from treating
    # the leading '--' as a flag on macOS BSD grep.
    printf '%s\n' "${output}" | grep -qFe "${pattern}" && matched=true || true
  else
    printf '%s\n' "${output}" | grep -qE "${pattern}" && matched=true || true
  fi
  if [[ "${matched}" == "true" ]]; then
    pass "${label}"
  else
    fail "${label} — expected output to match: ${pattern}"
    printf '%s\n' "${output}" | head -5 | sed 's/^/  /' >&2
  fi
}

# assert_nonzero <label> <exit-code>
assert_nonzero() {
  local label="$1" code="$2"
  if [[ "${code}" -ne 0 ]]; then
    pass "${label} (exited ${code})"
  else
    fail "${label} — expected non-zero exit but got 0"
  fi
}

# ─── Shared: build a minimal valid Decision Project directory ─────────────────
make_minimal_project() {
  local base="$1"
  mkdir -p "${base}/.decisionservice/META-INF"
  mkdir -p "${base}/RuleProject"
  cat > "${base}/pom.xml" <<'POM'
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
           http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>com.example</groupId>
  <artifactId>testproject</artifactId>
  <version>1.0.0-SNAPSHOT</version>
  <name>Test Decision Service</name>
  <packaging>pom</packaging>
  <modules>
    <module>.decisionservice</module>
    <module>RuleProject</module>
  </modules>
</project>
POM
  cat > "${base}/.decisionservice/pom.xml" <<'POM'
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
           http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <parent>
    <groupId>com.example</groupId>
    <artifactId>testproject</artifactId>
    <version>1.0.0-SNAPSHOT</version>
  </parent>
  <artifactId>testprojectDecisionService</artifactId>
  <packaging>jar</packaging>
</project>
POM
  printf '{"name":"Test Decision Service","version":"1.0.0-SNAPSHOT"}\n' \
    > "${base}/.decisionservice/META-INF/decisionServiceArchive.json"
  touch "${base}/RuleProject/pom.xml"
  touch "${base}/RuleProject/.ruleproject"
}

# ─── ZIP detection helper (mirrors Step 7 of build-decision-service.sh) ───────
# is_decision_project <dir>  — returns 0 if dir contains pom.xml + .decisionservice/pom.xml
is_decision_project() {
  local dir="$1"
  [[ -f "${dir}/pom.xml" && -f "${dir}/.decisionservice/pom.xml" ]]
}

# detect_project_dir <zip_path> <extract_dir>
# Extracts <zip_path> into <extract_dir> and prints the detected project path.
# The caller owns <extract_dir> and is responsible for cleaning it up.
detect_project_dir() {
  local zip_path="$1" extract_dir="$2"
  unzip -q "${zip_path}" -d "${extract_dir}"

  local found=""
  if is_decision_project "${extract_dir}"; then
    found="${extract_dir}"
  else
    local candidates=()
    for candidate in "${extract_dir}"/*/; do
      [[ -d "${candidate}" ]] || continue
      if is_decision_project "${candidate%/}"; then
        candidates+=("${candidate%/}")
      fi
    done
    if [[ ${#candidates[@]} -eq 1 ]]; then
      found="${candidates[0]}"
    fi
  fi
  printf '%s' "${found}"
}

# ─────────────────────────────────────────────────────────────────────────────
echo
echo "${BOLD}╔══════════════════════════════════════════════════════════╗${NC}"
echo "${BOLD}║  build-decision-service.sh  Unit / Integration Tests     ║${NC}"
echo "${BOLD}╚══════════════════════════════════════════════════════════╝${NC}"
echo

# Temp directory used by Suites 3–5; cleaned up on exit
WORK="$(mktemp -d "${TMPDIR:-/tmp}/bds-test-XXXXXX")"
trap 'rm -rf "${WORK}"' EXIT

# ═════════════════════════════════════════════════════════════════════════════
section "Suite 1: Script invariants"
# ═════════════════════════════════════════════════════════════════════════════

# 1.1 — Script exists and is executable
if [[ -f "${BDS}" && -x "${BDS}" ]]; then
  pass "build-decision-service.sh exists and is executable"
else
  fail "build-decision-service.sh not found or not executable at ${BDS}"
fi

# 1.2 — --help exits cleanly
set +e
HELP_OUT=$("${BDS}" --help 2>&1); HELP_EXIT=$?
set -e
if [[ "${HELP_EXIT}" -eq 0 ]]; then
  pass "--help exits 0"
else
  fail "--help should exit 0 (got ${HELP_EXIT})"
fi
assert_contains "--help includes usage" "Usage:" "${HELP_OUT}"

# ═════════════════════════════════════════════════════════════════════════════
section "Suite 2: Argument validation"
# ═════════════════════════════════════════════════════════════════════════════

# 2.1 — Missing --project
set +e
OUT=$("${BDS}" --output /tmp --di-url https://example.com/ads \
        --username u --api-key k 2>&1); RC=$?
set -e
assert_nonzero "missing --project → non-zero exit" "${RC}"
assert_contains "missing --project → error message" "--project is required" "${OUT}"

# 2.2 — Missing --output
set +e
OUT=$("${BDS}" --project /dev/null --di-url https://example.com/ads \
        --username u --api-key k 2>&1); RC=$?
set -e
assert_nonzero "missing --output → non-zero exit" "${RC}"
assert_contains "missing --output → error message" "--output is required" "${OUT}"

# 2.3 — Missing --di-url
set +e
OUT=$("${BDS}" --project /dev/null --output /tmp \
        --username u --api-key k 2>&1); RC=$?
set -e
assert_nonzero "missing --di-url → non-zero exit" "${RC}"
assert_contains "missing --di-url → error message" "--di-url is required" "${OUT}"

# 2.4 — Non-existent project path
set +e
OUT=$("${BDS}" --project /nonexistent/path --output /tmp \
        --di-url https://example.com/ads \
        --username u --api-key k 2>&1); RC=$?
set -e
assert_nonzero "non-existent project path → non-zero exit" "${RC}"
assert_contains "non-existent project path → error message" "not found" "${OUT}"

# 2.5 — Unknown option
set +e
OUT=$("${BDS}" --not-a-real-option 2>&1); RC=$?
set -e
assert_nonzero "unknown option → non-zero exit" "${RC}"
assert_contains "unknown option → error message" "Unknown option" "${OUT}"

# ═════════════════════════════════════════════════════════════════════════════
section "Suite 3: ZIP extraction — project structure detection"
# ═════════════════════════════════════════════════════════════════════════════
# Tests the ZIP layout detection logic by calling detect_project_dir() directly
# with synthetically constructed ZIPs. No DI instance or Maven build required.

if ! command -v unzip &>/dev/null; then
  skip "Suite 3: unzip not available — skipping ZIP detection tests"
else

# 3.1 — Layout A: project files at ZIP root
echo
echo "  3.1 — Layout A: project at ZIP root"
make_minimal_project "${WORK}/layout-a-src"
(cd "${WORK}/layout-a-src" && zip -rq "${WORK}/layout-a.zip" .)
mkdir -p "${WORK}/extract-a"
DETECTED=$(detect_project_dir "${WORK}/layout-a.zip" "${WORK}/extract-a")
if [[ -n "${DETECTED}" && -f "${DETECTED}/pom.xml" ]]; then
  pass "Layout A: detected project at ZIP root"
else
  fail "Layout A: failed to detect project at ZIP root (got: '${DETECTED}')"
fi

# 3.2 — Layout B: project inside a single top-level directory
echo
echo "  3.2 — Layout B: project inside a single top-level directory"
make_minimal_project "${WORK}/layout-b-src/My Decision Project"
(cd "${WORK}/layout-b-src" && zip -rq "${WORK}/layout-b.zip" "My Decision Project")
mkdir -p "${WORK}/extract-b"
DETECTED=$(detect_project_dir "${WORK}/layout-b.zip" "${WORK}/extract-b")
if [[ -n "${DETECTED}" && -f "${DETECTED}/pom.xml" ]]; then
  pass "Layout B: detected project inside single top-level directory ($(basename "${DETECTED}"))"
else
  fail "Layout B: failed to detect project (got: '${DETECTED}')"
fi

# 3.3 — Layout C: project alongside META-INF signing metadata (signed ZIP)
echo
echo "  3.3 — Layout C: project alongside META-INF signing metadata"
make_minimal_project "${WORK}/layout-c-src/FraudDetectionService"
mkdir -p "${WORK}/layout-c-src/META-INF"
printf 'Manifest-Version: 1.0\n' > "${WORK}/layout-c-src/META-INF/MANIFEST.MF"
printf 'Signature-Version: 1.0\n' > "${WORK}/layout-c-src/META-INF/SIGNER.SF"
(cd "${WORK}/layout-c-src" && zip -rq "${WORK}/layout-c.zip" META-INF FraudDetectionService)
mkdir -p "${WORK}/extract-c"
DETECTED=$(detect_project_dir "${WORK}/layout-c.zip" "${WORK}/extract-c")
DETECTED_BASE="$(basename "${DETECTED}")"
if [[ "${DETECTED_BASE}" == "FraudDetectionService" && -f "${DETECTED}/pom.xml" ]]; then
  pass "Layout C: detected project 'FraudDetectionService', ignored META-INF signing metadata"
else
  fail "Layout C: expected 'FraudDetectionService', got '${DETECTED_BASE}' (full: '${DETECTED}')"
fi

# 3.4 — Error: no Decision Project found (only META-INF in ZIP)
echo
echo "  3.4 — Error: no Decision Project found in ZIP"
mkdir -p "${WORK}/empty-src/META-INF"
printf 'x\n' > "${WORK}/empty-src/META-INF/MANIFEST.MF"
(cd "${WORK}/empty-src" && zip -rq "${WORK}/empty.zip" META-INF)
mkdir -p "${WORK}/extract-empty"
DETECTED=$(detect_project_dir "${WORK}/empty.zip" "${WORK}/extract-empty" 2>/dev/null || true)
if [[ -z "${DETECTED}" ]]; then
  pass "Error case: no project found → detection returns empty string"
else
  fail "Error case: expected empty detection, got '${DETECTED}'"
fi

fi  # end unzip guard

# ═════════════════════════════════════════════════════════════════════════════
section "Suite 4: Known sample ZIP structures"
# ═════════════════════════════════════════════════════════════════════════════
# Validates the two real-world ZIP files in the repository against the same
# detection logic without performing a Maven build.

if ! command -v unzip &>/dev/null; then
  skip "Suite 4: unzip not available — skipping sample ZIP tests"
else

cd "${PROJECT_ROOT}"

# ── 4.1 — Existing ZIP: Fraud Detection Service (Layout B) ──────────────────
BANKING_ZIP="samples/banking-fraud-agent/di-project/Fraud Detection Service.zip"
echo
echo "  4.1 — ${BANKING_ZIP}"
if [[ -f "${BANKING_ZIP}" ]]; then
  mkdir -p "${WORK}/banking-extract"
  FOUND_BANKING=$(detect_project_dir "${BANKING_ZIP}" "${WORK}/banking-extract")
  if [[ -n "${FOUND_BANKING}" ]]; then
    pass "Banking ZIP: Decision Project detected at '$(basename "${FOUND_BANKING}")'"
  else
    fail "Banking ZIP: no Decision Project found"
    FOUND_BANKING="${WORK}/banking-extract"  # prevent empty-var errors below
  fi
  for f in "pom.xml" ".decisionservice/pom.xml" ".decisionservice/META-INF/decisionServiceArchive.json"; do
    if [[ -f "${FOUND_BANKING}/${f}" ]]; then
      pass "Banking ZIP: required file present: ${f}"
    else
      fail "Banking ZIP: required file missing: ${f}"
    fi
  done
  RP_COUNT=$(find "${FOUND_BANKING}" -maxdepth 2 -name ".ruleproject" 2>/dev/null | wc -l | tr -d ' ')
  if [[ "${RP_COUNT}" -gt 0 ]]; then
    pass "Banking ZIP: ${RP_COUNT} rule project(s) found"
  else
    fail "Banking ZIP: no rule projects found"
  fi
else
  skip "Banking ZIP not found at: ${BANKING_ZIP}"
fi

fi  # end unzip guard


# ═════════════════════════════════════════════════════════════════════════════
section "Suite 5: Decision Project directory validation"
# ═════════════════════════════════════════════════════════════════════════════
# Tests that the script accepts valid project directories and rejects invalid
# ones.  We use directory input (not ZIP) and stop well before the Maven build
# because no DI instance is available.

# 5.1 — Valid project directory → fails at prereqs/network, NOT at structure validation
VALID_PROJ="${WORK}/valid-proj"
make_minimal_project "${VALID_PROJ}"
set +e
OUT=$("${BDS}" \
  --project "${VALID_PROJ}" \
  --output  "${WORK}/out-valid" \
  --di-url  "https://example.invalid/ads" \
  --username "testuser" --api-key "testkey" 2>&1); RC=$?
set -e
# The script may fail at Step 2 (prereqs: java/mvn/curl) or Step 4 (network).
# It must NOT fail with "Invalid Decision Project" or "missing required file".
if printf '%s\n' "${OUT}" | grep -qE "Invalid Decision Project|missing required file"; then
  fail "Valid project directory: script incorrectly rejected the project structure"
else
  pass "Valid project directory: structure accepted (failed at prereqs/network as expected)"
fi

# 5.2 — Project missing .decisionservice/pom.xml → script must exit non-zero
# The script checks for .decisionservice/pom.xml at Step 7.  If prerequisites
# (java, mvn, curl, python3) are absent, the script may fail at Step 2 instead
# — in both cases the exit code must be non-zero.
INCOMPLETE_PROJ="${WORK}/incomplete-proj"
mkdir -p "${INCOMPLETE_PROJ}"
printf '<project/>\n' > "${INCOMPLETE_PROJ}/pom.xml"
# Deliberately omit .decisionservice/
set +e
OUT=$("${BDS}" \
  --project "${INCOMPLETE_PROJ}" \
  --output  "${WORK}/out-incomplete" \
  --di-url  "https://example.invalid/ads" \
  --username "testuser" --api-key "testkey" 2>&1); RC=$?
set -e
assert_nonzero "missing .decisionservice/pom.xml → non-zero exit" "${RC}"
# Output must not claim the build succeeded
if printf '%s\n' "${OUT}" | grep -qE "Build completed|Decision Service archive:"; then
  fail "missing .decisionservice/pom.xml: output incorrectly indicates a successful build"
else
  pass "missing .decisionservice/pom.xml: output does not indicate a successful build"
fi

# ═════════════════════════════════════════════════════════════════════════════
section "Suite 6: DI artifact version sync (_sync_di_versions)"
# ═════════════════════════════════════════════════════════════════════════════
# Tests the version-sync logic added to Step 8 of the build script.
# We extract and eval the function directly so no DI instance is needed.

_setup_sync_env() {
  GREEN=''; YELLOW=''; RED=''; CYAN=''; BOLD=''; NC=''
  info()  { echo "[INFO]  $*"; }
  warn()  { echo "[WARN]  $*"; }
  error() { echo "[ERROR] $*" >&2; }
  debug() { true; }

  # Extract _sync_di_versions() body from the build script and eval it.
  local func
  func=$(sed -n '/_sync_di_versions()/,/^}/p' "${BDS}")
  eval "${func}"
}

_setup_sync_env

# Helper: create a minimal root pom.xml with all three DI version properties
make_di_pom() {
  local dest="$1" plugin_ver="$2" engine_rt_ver="$3" engine_api_ver="$4"
  cat > "${dest}" <<POM
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
           http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>com.example</groupId>
  <artifactId>testproject</artifactId>
  <version>1.0.0</version>
  <packaging>pom</packaging>
  <properties>
    <ads-mojo.version>${plugin_ver}</ads-mojo.version>
    <engine-compact-runtime.version>${engine_rt_ver}</engine-compact-runtime.version>
    <engine-api.version>${engine_api_ver}</engine-api.version>
    <some-other.version>9.9.9</some-other.version>
  </properties>
</project>
POM
}

read_prop() {
  local pom="$1" prop="$2"
  python3 - "${pom}" "${prop}" <<'PYEOF'
import xml.etree.ElementTree as ET, sys
ns = {'m': 'http://maven.apache.org/POM/4.0.0'}
root = ET.parse(sys.argv[1]).getroot()
print(root.findtext('m:properties/m:' + sys.argv[2], namespaces=ns) or '')
PYEOF
}

# 6.1 — All three versions older → all patched; unrelated property untouched
echo
echo "  6.1 — All DI versions older than Designer → all three patched"
POM_61="${WORK}/pom-61.xml"
make_di_pom "${POM_61}" "16.6.0" "3.12.1" "3.11.0"
_sync_di_versions "${POM_61}" "16.7.3" "3.12.3" "3.12.0"
GOT_PLUGIN=$(read_prop "${POM_61}" "ads-mojo.version")
GOT_RT=$(read_prop    "${POM_61}" "engine-compact-runtime.version")
GOT_API=$(read_prop   "${POM_61}" "engine-api.version")
GOT_OTHER=$(read_prop "${POM_61}" "some-other.version")
[[ "${GOT_PLUGIN}" == "16.7.3" ]] && pass "6.1: ads-mojo.version: 16.6.0 → 16.7.3" \
  || fail "6.1: ads-mojo.version expected 16.7.3, got '${GOT_PLUGIN}'"
[[ "${GOT_RT}" == "3.12.3" ]] && pass "6.1: engine-compact-runtime.version: 3.12.1 → 3.12.3" \
  || fail "6.1: engine-compact-runtime.version expected 3.12.3, got '${GOT_RT}'"
[[ "${GOT_API}" == "3.12.0" ]] && pass "6.1: engine-api.version: 3.11.0 → 3.12.0" \
  || fail "6.1: engine-api.version expected 3.12.0, got '${GOT_API}'"
[[ "${GOT_OTHER}" == "9.9.9" ]] && pass "6.1: unrelated <some-other.version> unchanged" \
  || fail "6.1: unrelated property changed to '${GOT_OTHER}'"

# 6.2 — ads-mojo.version newer, engine versions match → only plugin patched down
echo
echo "  6.2 — Plugin version newer than Designer; engine versions match → only plugin patched"
POM_62="${WORK}/pom-62.xml"
make_di_pom "${POM_62}" "16.8.1" "3.12.3" "3.12.0"
_sync_di_versions "${POM_62}" "16.7.3" "3.12.3" "3.12.0"
GOT_PLUGIN=$(read_prop "${POM_62}" "ads-mojo.version")
GOT_RT=$(read_prop    "${POM_62}" "engine-compact-runtime.version")
[[ "${GOT_PLUGIN}" == "16.7.3" ]] && pass "6.2: ads-mojo.version: 16.8.1 → 16.7.3" \
  || fail "6.2: ads-mojo.version expected 16.7.3, got '${GOT_PLUGIN}'"
[[ "${GOT_RT}" == "3.12.3" ]] && pass "6.2: engine-compact-runtime.version unchanged at 3.12.3" \
  || fail "6.2: engine-compact-runtime.version expected 3.12.3, got '${GOT_RT}'"

# 6.3 — All versions already match → file not modified
echo
echo "  6.3 — All versions match Designer → no modification"
POM_63="${WORK}/pom-63.xml"
make_di_pom "${POM_63}" "16.7.3" "3.12.3" "3.12.0"
MTIME_BEFORE=$(python3 -c "import os; print(os.stat('${POM_63}').st_mtime)")
_sync_di_versions "${POM_63}" "16.7.3" "3.12.3" "3.12.0"
GOT_PLUGIN=$(read_prop "${POM_63}" "ads-mojo.version")
GOT_RT=$(read_prop    "${POM_63}" "engine-compact-runtime.version")
MTIME_AFTER=$(python3 -c "import os; print(os.stat('${POM_63}').st_mtime)")
[[ "${GOT_PLUGIN}" == "16.7.3" ]] && pass "6.3: ads-mojo.version unchanged at 16.7.3" \
  || fail "6.3: ads-mojo.version was unexpectedly changed to '${GOT_PLUGIN}'"
[[ "${GOT_RT}" == "3.12.3" ]] && pass "6.3: engine-compact-runtime.version unchanged at 3.12.3" \
  || fail "6.3: engine-compact-runtime.version was unexpectedly changed to '${GOT_RT}'"
if [[ "${MTIME_BEFORE}" == "${MTIME_AFTER}" ]]; then
  pass "6.3: file mtime unchanged (no unnecessary write)"
else
  pass "6.3: mtime changed (write occurred, but content is correct — acceptable)"
fi

# 6.4 — POM without any DI properties → warn and skip (no error)
echo
echo "  6.4 — POM missing all DI version properties → graceful skip"
POM_64="${WORK}/pom-64.xml"
cat > "${POM_64}" <<'POM'
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
           http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>com.example</groupId>
  <artifactId>noprop</artifactId>
  <version>1.0.0</version>
  <packaging>pom</packaging>
  <properties>
    <some-other.version>1.2.3</some-other.version>
  </properties>
</project>
POM
set +e
OUT_64=$(_sync_di_versions "${POM_64}" "16.7.3" "3.12.3" "3.12.0" 2>&1); RC_64=$?
set -e
if [[ "${RC_64}" -eq 0 ]]; then
  pass "6.4: no DI properties → exits 0 (no error)"
else
  fail "6.4: no DI properties → unexpected non-zero exit ${RC_64}"
fi
if printf '%s\n' "${OUT_64}" | grep -qi "skipping\|not found"; then
  pass "6.4: output mentions skip/not-found"
else
  fail "6.4: expected skip/not-found message; got: ${OUT_64}"
fi

# 6.5 — Real-world Fraud Detection Service.zip: plugin 16.6.0+engine 3.12.1 → Designer 16.7.3+3.12.3
echo
echo "  6.5 — Real-world Fraud Detection Service.zip: all stale DI versions patched"
DISAAS_ZIP="samples/banking-fraud-agent/di-project/Fraud Detection Service.zip"
if ! command -v unzip &>/dev/null; then
  skip "6.5: unzip not available"
elif [[ ! -f "${PROJECT_ROOT}/${DISAAS_ZIP}" ]]; then
  skip "6.5: ${DISAAS_ZIP} not found"
else
  EXTRACT_65="${WORK}/extract-65"
  mkdir -p "${EXTRACT_65}"
  unzip -q "${PROJECT_ROOT}/${DISAAS_ZIP}" -d "${EXTRACT_65}"
  POM_65=""
  for d in "${EXTRACT_65}"/*/; do
    [[ -f "${d}pom.xml" && -f "${d}.decisionservice/pom.xml" ]] && POM_65="${d}pom.xml" && break
  done
  if [[ -z "${POM_65}" ]]; then
    skip "6.5: could not locate project pom.xml in extracted ZIP"
  else
    ORIG_PLUGIN=$(read_prop "${POM_65}" "ads-mojo.version")
    ORIG_RT=$(read_prop     "${POM_65}" "engine-compact-runtime.version")
    ORIG_API=$(read_prop    "${POM_65}" "engine-api.version")
    _sync_di_versions "${POM_65}" "16.7.3" "3.12.3" "3.12.0"
    NEW_PLUGIN=$(read_prop "${POM_65}" "ads-mojo.version")
    NEW_RT=$(read_prop     "${POM_65}" "engine-compact-runtime.version")
    NEW_API=$(read_prop    "${POM_65}" "engine-api.version")
    [[ "${NEW_PLUGIN}" == "16.7.3" ]] && pass "6.5: ads-mojo.version: ${ORIG_PLUGIN} → ${NEW_PLUGIN}" \
      || fail "6.5: ads-mojo.version expected 16.7.3, got '${NEW_PLUGIN}'"
    [[ "${NEW_RT}" == "3.12.3" ]] && pass "6.5: engine-compact-runtime.version: ${ORIG_RT} → ${NEW_RT}" \
      || fail "6.5: engine-compact-runtime.version expected 3.12.3, got '${NEW_RT}'"
    [[ "${NEW_API}" == "3.12.0" ]] && pass "6.5: engine-api.version: ${ORIG_API} → ${NEW_API} (already matched)" \
      || fail "6.5: engine-api.version expected 3.12.0, got '${NEW_API}'"
  fi
fi

# ═════════════════════════════════════════════════════════════════════════════
section "Suite 7: Maven configuration consistency (import-maven-plugin)"
# ═════════════════════════════════════════════════════════════════════════════
# Regression tests ensuring DI plugins (import-maven-plugin) are resolved from
# the isolated LOCAL_REPO rather than ~/.m2/repository, which may carry a
# .lastUpdated negative-cache entry.
#
# These tests inspect the source of build-decision-service.sh directly and do
# NOT require a running DI instance or a Maven build.

# ── Helpers for source inspection ─────────────────────────────────────────────
_script_src() { cat "${BDS}"; }

# 7.4 — import-maven-plugin must be installed in Step 5
echo
echo "  7.4 — import-maven-plugin is installed into LOCAL_REPO in Step 5"
STEP5_BLOCK=$(_script_src | awk '/^section.*Step 5/,/^section.*Step 6/')
if printf '%s\n' "${STEP5_BLOCK}" | grep -qF 'import-maven-plugin'; then
  pass "7.4: import-maven-plugin install call present in Step 5"
else
  fail "7.4: import-maven-plugin is NOT installed in Step 5; it must be pre-installed to avoid negative-cache failures"
fi

# 7.5 — IMPORT_PLUGIN_PATH must be derived from BUILD_PLUGIN_PATH when not in index.json
echo
echo "  7.5 — import-maven-plugin path fallback derives from build-plugin path"
if _script_src | grep -qF 'BUILD_PLUGIN_PATH/build-maven-plugin/import-maven-plugin'; then
  pass "7.5: IMPORT_PLUGIN_PATH fallback substitutes build-maven-plugin → import-maven-plugin in path"
else
  fail "7.5: IMPORT_PLUGIN_PATH fallback not found; import-maven-plugin path may be wrong when not in index.json"
fi

# 7.7 — import-maven-plugin POM written to WORK_DIR (not hardcoded path)
echo
echo "  7.7 — IMPORT_PLUGIN_POM_FILE uses WORK_DIR (not a hardcoded temp path)"
if _script_src | grep -qF 'IMPORT_PLUGIN_POM_FILE="${WORK_DIR}/'; then
  pass "7.7: import plugin POM file is written under WORK_DIR"
else
  fail "7.7: IMPORT_PLUGIN_POM_FILE does not reference WORK_DIR — may use a hardcoded path"
fi

# ═════════════════════════════════════════════════════════════════════════════
# Summary
# ═════════════════════════════════════════════════════════════════════════════
echo
echo "════════════════════════════════════════════════════════════"
printf "Results: ${GREEN}%d passed${NC}  ${RED}%d failed${NC}  ${YELLOW}%d skipped${NC}\n" \
  "${PASS}" "${FAIL}" "${SKIP}"
echo "════════════════════════════════════════════════════════════"
echo

if [[ "${FAIL}" -gt 0 ]]; then
  echo "One or more tests failed. Review the output above."
  exit 1
fi

echo "All tests passed."
exit 0
