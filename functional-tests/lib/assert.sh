#!/usr/bin/env bash
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
# lib/assert.sh — Lightweight assertion and reporting helpers.
#
# Usage: source lib/assert.sh before any test step.
#
# Variables set by this library (read externally):
#   TESTS_RUN    — number of assertions executed
#   TESTS_PASSED — number of assertions that passed
#   TESTS_FAILED — number of assertions that failed
#   FAILURES[]   — array of failure description strings

TESTS_RUN=0
TESTS_PASSED=0
TESTS_FAILED=0
declare -a FAILURES=()

# ── Colour / formatting ───────────────────────────────────────────────────────

# Only emit colour codes when stdout is a real terminal.
if [[ -t 1 ]]; then
    _GREEN='\033[0;32m'
    _YELLOW='\033[1;33m'
    _RED='\033[0;31m'
    _CYAN='\033[0;36m'
    _BOLD='\033[1m'
    _NC='\033[0m'
else
    _GREEN='' _YELLOW='' _RED='' _CYAN='' _BOLD='' _NC=''
fi

log_info()    { echo -e "${_CYAN}[INFO]${_NC}  $*"; }
log_pass()    { echo -e "${_GREEN}[PASS]${_NC}  $*"; }
log_warn()    { echo -e "${_YELLOW}[WARN]${_NC}  $*"; }
log_error()   { echo -e "${_RED}[ERROR]${_NC} $*" >&2; }
log_section() { echo -e "\n${_BOLD}━━━ $* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${_NC}"; }
log_step()    { echo -e "${_BOLD}  ▶ $*${_NC}"; }

# ── Assertion primitives ──────────────────────────────────────────────────────

# Record a named test result.
# Usage: _record_result <name> <passed:0|1> [failure_detail]
_record_result() {
    local name="$1"
    local passed="$2"
    local detail="${3:-}"
    (( TESTS_RUN++ ))
    if [[ "$passed" == "0" ]]; then
        (( TESTS_PASSED++ ))
        log_pass "$name"
    else
        (( TESTS_FAILED++ ))
        log_error "FAIL: $name"
        [[ -n "$detail" ]] && log_error "      $detail"
        FAILURES+=("$name${detail:+ — $detail}")
    fi
}

# Assert that two values are equal.
# Usage: assert_eq <name> <expected> <actual>
assert_eq() {
    local name="$1" expected="$2" actual="$3"
    if [[ "$expected" == "$actual" ]]; then
        _record_result "$name" 0
    else
        _record_result "$name" 1 "expected='$expected'  actual='$actual'"
    fi
}

# Assert that an integer is greater than or equal to a minimum.
# Usage: assert_ge <name> <min> <actual>
assert_ge() {
    local name="$1" min="$2" actual="$3"
    if [[ "$actual" =~ ^[0-9]+$ ]] && (( actual >= min )); then
        _record_result "$name" 0
    else
        _record_result "$name" 1 "expected >= $min, got '$actual'"
    fi
}

# Assert that a string is non-empty.
# Usage: assert_nonempty <name> <value>
assert_nonempty() {
    local name="$1" value="$2"
    if [[ -n "$value" ]]; then
        _record_result "$name" 0
    else
        _record_result "$name" 1 "value was empty"
    fi
}

# Assert that a string contains a substring.
# Usage: assert_contains <name> <substring> <string>
assert_contains() {
    local name="$1" sub="$2" str="$3"
    if [[ "$str" == *"$sub"* ]]; then
        _record_result "$name" 0
    else
        _record_result "$name" 1 "expected to contain '$sub', got: $str"
    fi
}

# Assert that a JSON field (via jq path) equals an expected value.
# Usage: assert_json_eq <name> <jq_path> <expected> <json_string>
assert_json_eq() {
    local name="$1" path="$2" expected="$3" json="$4"
    local actual
    actual=$(echo "$json" | jq -r "$path" 2>/dev/null)
    assert_eq "$name" "$expected" "$actual"
}

# Assert that a JSON field (via jq path) is truthy (not false/null/0/"").
# Usage: assert_json_true <name> <jq_path> <json_string>
assert_json_true() {
    local name="$1" path="$2" json="$3"
    local actual
    actual=$(echo "$json" | jq -r "$path" 2>/dev/null)
    if [[ "$actual" != "false" && "$actual" != "null" && "$actual" != "0" && -n "$actual" ]]; then
        _record_result "$name" 0
    else
        _record_result "$name" 1 "expected truthy at '$path', got '$actual'"
    fi
}

# Assert that an HTTP status code equals the expected value.
# Usage: assert_http_status <name> <expected_status> <actual_status> [response_body]
assert_http_status() {
    local name="$1" expected="$2" actual="$3" body="${4:-}"
    if [[ "$expected" == "$actual" ]]; then
        _record_result "$name" 0
    else
        _record_result "$name" 1 "HTTP $actual (expected $expected)${body:+  body: ${body:0:200}}"
    fi
}

# ── Summary report ────────────────────────────────────────────────────────────

# Print the test summary and exit with 0 (all passed) or 1 (some failed).
# Usage: print_summary_and_exit
print_summary_and_exit() {
    log_section "Test Summary"
    echo "  Total : $TESTS_RUN"
    echo -e "  ${_GREEN}Passed${_NC}: $TESTS_PASSED"
    if [[ $TESTS_FAILED -gt 0 ]]; then
        echo -e "  ${_RED}Failed${_NC}: $TESTS_FAILED"
        echo ""
        echo -e "${_RED}Failed assertions:${_NC}"
        for f in "${FAILURES[@]}"; do
            echo -e "  ${_RED}✗${_NC} $f"
        done
        echo ""
        exit 1
    else
        echo -e "  ${_GREEN}All assertions passed.${_NC}"
        echo ""
        exit 0
    fi
}

# ── HTTP helpers ──────────────────────────────────────────────────────────────
#
# IMPORTANT: http_get / http_post / http_delete set two global variables:
#   HTTP_STATUS  — the HTTP response code (string, e.g. "200")
#   HTTP_BODY    — the full response body (string)
#
# Callers MUST NOT invoke these functions inside a command-substitution subshell
# (i.e. do NOT write:  body=$(http_get ...)).  Subshells cannot export variables
# to their parent, so HTTP_STATUS would be lost.
#
# Correct usage pattern:
#   http_get "/some/path"
#   # now read $HTTP_STATUS and $HTTP_BODY

BFF_BASE_URL="${BFF_URL:-http://127.0.0.1:8080}"

# Temp file for response body — PID-scoped so parallel runs don't collide.
_FT_RESP_BODY="/tmp/ft_response_body_$$"

# Initialise globals so nounset (-u) doesn't fire on first reference.
HTTP_STATUS=""
HTTP_BODY=""

# Issue GET. Populates HTTP_STATUS and HTTP_BODY globals.
# Usage: http_get <path> [timeout_seconds]
http_get() {
    local path="$1"
    local timeout="${2:-15}"
    HTTP_STATUS=""
    HTTP_BODY=""
    local code
    code=$(curl -s -o "$_FT_RESP_BODY" -w "%{http_code}" \
        --connect-timeout 5 --max-time "$timeout" \
        "$BFF_BASE_URL$path" 2>/dev/null) || { HTTP_STATUS="CURL_ERROR"; return 1; }
    HTTP_STATUS="$code"
    HTTP_BODY=$(cat "$_FT_RESP_BODY" 2>/dev/null || true)
}

# Issue POST with JSON body. Populates HTTP_STATUS and HTTP_BODY globals.
# Usage: http_post <path> <json_body> [timeout_seconds]
http_post() {
    local path="$1" body="$2"
    local timeout="${3:-20}"
    HTTP_STATUS=""
    HTTP_BODY=""
    local code
    code=$(curl -s -o "$_FT_RESP_BODY" -w "%{http_code}" \
        --connect-timeout 5 --max-time "$timeout" \
        -H "Content-Type: application/json" \
        -d "$body" \
        "$BFF_BASE_URL$path" 2>/dev/null) || { HTTP_STATUS="CURL_ERROR"; return 1; }
    HTTP_STATUS="$code"
    HTTP_BODY=$(cat "$_FT_RESP_BODY" 2>/dev/null || true)
}

# Issue DELETE. Populates HTTP_STATUS and HTTP_BODY globals.
# Usage: http_delete <path> [timeout_seconds]
http_delete() {
    local path="$1"
    local timeout="${2:-60}"
    HTTP_STATUS=""
    HTTP_BODY=""
    local code
    code=$(curl -s -o "$_FT_RESP_BODY" -w "%{http_code}" \
        --connect-timeout 5 --max-time "$timeout" \
        -X DELETE \
        "$BFF_BASE_URL$path" 2>/dev/null) || { HTTP_STATUS="CURL_ERROR"; return 1; }
    HTTP_STATUS="$code"
    HTTP_BODY=$(cat "$_FT_RESP_BODY" 2>/dev/null || true)
}

# ── Service readiness helpers ─────────────────────────────────────────────────

# Poll GET /health until the BFF responds with 200 or timeout.
# Usage: wait_for_bff [timeout_seconds]
wait_for_bff() {
    local timeout="${1:-60}"
    local deadline=$(( $(date +%s) + timeout ))
    log_info "Waiting up to ${timeout}s for BFF at $BFF_BASE_URL/health ..."
    while [[ $(date +%s) -lt $deadline ]]; do
        local status
        status=$(curl -s -o /dev/null -w "%{http_code}" \
            --connect-timeout 3 --max-time 5 \
            "$BFF_BASE_URL/health" 2>/dev/null)
        if [[ "$status" == "200" ]]; then
            log_pass "BFF is reachable."
            return 0
        fi
        sleep 3
    done
    log_error "BFF did not become reachable at $BFF_BASE_URL within ${timeout}s"
    return 1
}

# Poll GET /api/flink/status until the named job appears in the running jobs list.
# Usage: wait_for_flink_job <job_name> [timeout_seconds]
wait_for_flink_job() {
    local job_name="$1"
    local timeout="${2:-120}"
    local deadline=$(( $(date +%s) + timeout ))
    log_info "Waiting up to ${timeout}s for Flink job '$job_name' to be RUNNING..."
    while [[ $(date +%s) -lt $deadline ]]; do
        http_get "/api/flink/status" 10 || true
        if [[ "$HTTP_STATUS" == "200" ]]; then
            if echo "$HTTP_BODY" | jq -e --arg name "$job_name" '.jobs[] | select(. == $name)' >/dev/null 2>&1; then
                log_pass "Flink job '$job_name' is RUNNING."
                return 0
            fi
        fi
        sleep 5
    done
    log_error "Flink job '$job_name' did not appear in running jobs within ${timeout}s"
    http_get "/api/flink/status" 5 || true
    log_error "Current Flink status: $HTTP_BODY"
    return 1
}

# Poll the agent-fraud-alerts Kafka topic via BFF events endpoint until an
# event containing <account_number> in its key or body appears.
# Usage: wait_for_fraud_alert <use_case_id> <account_number> [timeout_seconds]
wait_for_fraud_alert() {
    local use_case_id="$1"
    local account_number="$2"
    local timeout="${3:-60}"
    local deadline=$(( $(date +%s) + timeout ))
    log_info "Waiting up to ${timeout}s for fraud alert for account '$account_number'..."
    while [[ $(date +%s) -lt $deadline ]]; do
        http_get "/api/use-cases/${use_case_id}/events" 15 || true
        if [[ "$HTTP_STATUS" == "200" ]]; then
            # Check if agent-fraud-alerts topic has any events for this account
            local alerts
            alerts=$(echo "$HTTP_BODY" | jq -r \
                --arg acct "$account_number" \
                '."agent-fraud-alerts" // [] | map(select(tostring | contains($acct))) | length' \
                2>/dev/null)
            if [[ "$alerts" =~ ^[0-9]+$ ]] && (( alerts > 0 )); then
                log_pass "Fraud alert found for account '$account_number' ($alerts alert(s))."
                return 0
            fi
        fi
        sleep 5
    done
    log_error "No fraud alert for account '$account_number' within ${timeout}s"
    http_get "/api/use-cases/${use_case_id}/events" 10 || true
    log_error "Current events snapshot (truncated): ${HTTP_BODY:0:500}"
    return 1
}

# Poll the entity-state endpoint until the account entity appears.
# Usage: wait_for_entity_state <use_case_id> <agent_id> <entity_id> [timeout_seconds]
wait_for_entity_state() {
    local use_case_id="$1"
    local agent_id="$2"
    local entity_id="$3"
    local timeout="${4:-60}"
    local deadline=$(( $(date +%s) + timeout ))
    log_info "Waiting up to ${timeout}s for entity state '$entity_id' in agent '$agent_id'..."
    while [[ $(date +%s) -lt $deadline ]]; do
        http_get "/api/agents/${use_case_id}/entity-state?agentId=${agent_id}&entityId=${entity_id}" 10 || true
        if [[ "$HTTP_STATUS" == "200" ]]; then
            local count
            count=$(echo "$HTTP_BODY" | jq -r '.entities | length' 2>/dev/null)
            if [[ "$count" =~ ^[0-9]+$ ]] && (( count > 0 )); then
                log_pass "Entity state found for '$entity_id' ($count snapshot(s))."
                return 0
            fi
        fi
        sleep 5
    done
    log_error "Entity state for '$entity_id' not found within ${timeout}s"
    return 1
}
