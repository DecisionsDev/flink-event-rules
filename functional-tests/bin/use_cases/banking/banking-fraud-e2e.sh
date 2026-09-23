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
# banking-fraud-e2e.sh — Banking Fraud Detection Agent functional end-to-end test.
#
# What this test covers
# ─────────────────────
# 1.  Docker Compose stack starts successfully.
# 2.  All required containers (Kafka, Flink JobManager, Flink TaskManager, BFF)
#     are running and healthy.
# 3.  BFF REST API is reachable and responds with a valid /health response.
# 4.  The banking-fraud-agent use-case descriptor is discoverable via the API.
# 5.  The Flink job is running (Banking Fraud Detection Agent).
# 6.  The banking use-case workflow is exercised step-by-step:
#       a) Inject an AccountEvent (OPENING) to establish entity state.
#       b) Verify intermediate state: no fraud alerts for the test account yet.
#       c) Inject a large Transaction ($50 000, cross-border) that triggers
#          RULE-001 (large amount) and RULE-002a (cross-border, score >= 45 -> PENDING_REVIEW).
#       d) Wait for the Flink pipeline to process and emit an Alert on
#          the agent-fraud-alerts topic.
#       e) Validate the final state: fraud alert present for the account.
# 7.  A second scenario validates the BLOCKED path (suspended account).
# 8.  API input validation: bad requests return the correct HTTP error codes.
#
# Usage
# ─────
#   # Run from the project root (Compose not yet started — test manages lifecycle):
#   make functional-test-banking
#
#   # Skip Compose start/stop (stack already running):
#   SKIP_COMPOSE=true make functional-test-banking
#
# Environment variables (all optional)
# ──────────────────────────────────────
#   BFF_URL                       BFF base URL (default: http://127.0.0.1:8080)
#   SKIP_COMPOSE                  true -> skip docker compose up/down
#   COMPOSE_CLEAN_VOLUMES         true -> pass -v to compose down (removes Kafka data)
#   CONTAINER_HEALTHY_TIMEOUT     seconds to wait for a container to become healthy (default: 180)
#   FLINK_JOB_TIMEOUT             seconds to wait for Flink job to be RUNNING (default: 180)
#   ALERT_WAIT_TIMEOUT            seconds to wait for fraud alert on Kafka (default: 90)
#   INTER_EVENT_DELAY_MS          milliseconds between injected events sent to BFF (default: 1000)
#
# Exit codes
# ──────────
#   0  All tests passed.
#   1  One or more assertions failed, or the environment could not be set up.

set -euo pipefail

# ── Resolve script and repo root ─────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# banking-fraud-e2e.sh lives at: functional-tests/bin/use_cases/banking/
# FUNCTIONAL_TEST_ROOT is:       functional-tests/
FUNCTIONAL_TEST_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)/functional-tests"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"

export REPO_ROOT

# ── Source shared libraries ───────────────────────────────────────────────────

# shellcheck source=lib/assert.sh
source "${FUNCTIONAL_TEST_ROOT}/lib/assert.sh"
# shellcheck source=lib/compose.sh
source "${FUNCTIONAL_TEST_ROOT}/lib/compose.sh"

# ── Configuration ─────────────────────────────────────────────────────────────

BFF_BASE_URL="${BFF_URL:-http://127.0.0.1:8080}"
SKIP_COMPOSE="${SKIP_COMPOSE:-false}"
CONTAINER_HEALTHY_TIMEOUT="${CONTAINER_HEALTHY_TIMEOUT:-180}"
FLINK_JOB_TIMEOUT="${FLINK_JOB_TIMEOUT:-180}"
ALERT_WAIT_TIMEOUT="${ALERT_WAIT_TIMEOUT:-90}"
INTER_EVENT_DELAY_MS="${INTER_EVENT_DELAY_MS:-1000}"

USE_CASE_ID="banking-fraud-agent"
FLINK_JOB_NAME="Banking Fraud Detection Agent"

# Unique account number per test run so repeated runs don't interfere.
TEST_RUN_ID="FT-$(date +%s)"
ACCOUNT_HAPPY="${TEST_RUN_ID}-HAPPY"
ACCOUNT_BLOCKED="${TEST_RUN_ID}-BLOCKED"

# ── Cleanup trap ──────────────────────────────────────────────────────────────

_cleanup() {
    local exit_code=$?
    log_section "Cleanup"
    if [[ "${SKIP_COMPOSE}" != "true" ]]; then
        compose_down || true
    fi
    if [[ $exit_code -ne 0 ]] && [[ "${DUMP_LOGS_ON_FAILURE:-true}" == "true" ]]; then
        log_warn "Test failed — dumping container logs for diagnostics..."
        dump_container_logs "kafka" 30 || true
        dump_container_logs "event-rule-backend-jobmanager" 50 || true
        dump_container_logs "event-rule-bff" 50 || true
    fi
    rm -f "/tmp/ft_response_body_$$" || true
}
trap _cleanup EXIT

# ── Prerequisite checks ───────────────────────────────────────────────────────

check_prerequisites() {
    log_section "Prerequisite Checks"
    local failed=0

    # docker
    if ! command -v docker >/dev/null 2>&1; then
        log_error "docker is not installed or not on PATH."
        (( failed++ ))
    else
        log_pass "docker is available."
    fi

    # docker compose
    if [[ -z "$COMPOSE_CMD" ]]; then
        log_error "docker compose (or docker-compose) is not available."
        (( failed++ ))
    else
        log_pass "$COMPOSE_CMD is available."
    fi

    # curl
    if ! command -v curl >/dev/null 2>&1; then
        log_error "curl is not installed or not on PATH."
        (( failed++ ))
    else
        log_pass "curl is available."
    fi

    # jq
    if ! command -v jq >/dev/null 2>&1; then
        log_error "jq is not installed or not on PATH."
        log_error "Install with: brew install jq  (macOS)  or  apt-get install jq  (Linux)"
        (( failed++ ))
    else
        log_pass "jq is available."
    fi

    if [[ $failed -gt 0 ]]; then
        log_error "Prerequisites not met — cannot run functional tests."
        exit 1
    fi
}

# ── Docker Compose lifecycle ───────────────────────────────────────────────────

start_compose_stack() {
    if [[ "${SKIP_COMPOSE}" == "true" ]]; then
        log_info "SKIP_COMPOSE=true — skipping docker compose up."
        return 0
    fi
    log_section "Starting Docker Compose Stack"
    compose_up || exit 1
}

validate_compose_stack() {
    log_section "Step 1 — Docker Compose: All Required Services Running & Healthy"
    assert_all_services_healthy

    # Count total running containers from compose
    local running_count
    running_count=$(compose ps --filter "status=running" -q 2>/dev/null | wc -l | tr -d ' ')
    assert_ge "At least 4 compose services are running" 4 "$running_count"
}

# ── Step 2 — BFF Health ───────────────────────────────────────────────────────

validate_bff_health() {
    log_section "Step 2 — BFF Health Check"

    # Wait for BFF to become ready (it needs Kafka to be up first)
    wait_for_bff 90 || {
        dump_container_logs "event-rule-bff" 80
        log_error "BFF is not reachable — check container logs above."
        exit 1
    }

    # GET /health -> 200, status=ok
    http_get "/health" 10
    assert_http_status "GET /health returns 200" "200" "$HTTP_STATUS" "$HTTP_BODY"
    assert_json_eq    "GET /health body.status = ok" ".status" "ok" "$HTTP_BODY"
    assert_json_true  "GET /health body has activeUseCases field" ".activeUseCases != null" "$HTTP_BODY"
    assert_json_true  "GET /health body has useCases object" ".useCases | type == \"object\"" "$HTTP_BODY"
}

# ── Step 3 — Use-Case Descriptor ─────────────────────────────────────────────

validate_use_case_descriptor() {
    log_section "Step 3 — Banking Use-Case Descriptor is Available"

    http_get "/api/use-cases/${USE_CASE_ID}/descriptor" 10
    assert_http_status "GET /api/use-cases/${USE_CASE_ID}/descriptor returns 200" "200" "$HTTP_STATUS" "$HTTP_BODY"
    assert_nonempty    "Descriptor has a non-blank name" "$(echo "$HTTP_BODY" | jq -r '.name' 2>/dev/null)"
    assert_nonempty    "Descriptor has a non-blank flinkJobName" "$(echo "$HTTP_BODY" | jq -r '.flinkJobName' 2>/dev/null)"

    # Topics declared
    local topic_count
    topic_count=$(echo "$HTTP_BODY" | jq '.topics | length' 2>/dev/null)
    assert_ge "Descriptor declares at least 2 inbound topics" 2 "$topic_count"

    # Verify the two inbound topics are present
    local has_transactions
    has_transactions=$(echo "$HTTP_BODY" | jq -r '[.topics[] | select(.name == "agent-transactions")] | length' 2>/dev/null)
    assert_ge "Descriptor includes agent-transactions topic" 1 "$has_transactions"

    local has_account_events
    has_account_events=$(echo "$HTTP_BODY" | jq -r '[.topics[] | select(.name == "agent-account-events")] | length' 2>/dev/null)
    assert_ge "Descriptor includes agent-account-events topic" 1 "$has_account_events"

    # Outbound topics
    local outbound_count
    outbound_count=$(echo "$HTTP_BODY" | jq '.outboundTopics | length' 2>/dev/null)
    assert_ge "Descriptor declares at least 1 outbound topic (fraud alerts)" 1 "$outbound_count"

    local has_fraud_alerts
    has_fraud_alerts=$(echo "$HTTP_BODY" | jq -r '[.outboundTopics[] | select(.name == "agent-fraud-alerts")] | length' 2>/dev/null)
    assert_ge "Descriptor includes agent-fraud-alerts outbound topic" 1 "$has_fraud_alerts"

    # Agents list endpoint
    http_get "/api/agents" 10
    assert_http_status "GET /api/agents returns 200" "200" "$HTTP_STATUS" "$HTTP_BODY"
    local found
    found=$(echo "$HTTP_BODY" | jq -r \
        --arg id "$USE_CASE_ID" '[.[] | select(.id == $id)] | length' 2>/dev/null)
    assert_ge "banking-fraud-agent appears in /api/agents list" 1 "$found"
}

# ── Step 4 — Flink Job Running ────────────────────────────────────────────────

validate_flink_job_running() {
    log_section "Step 4 — Flink Job is RUNNING"
    wait_for_flink_job "$FLINK_JOB_NAME" "$FLINK_JOB_TIMEOUT" || {
        log_error "Banking Fraud Detection Agent Flink job is not running."
        log_error "Make sure the stack is started and the Flink job was deployed."
        log_error "To deploy: ./samples/banking-fraud-agent/scripts/deploy.sh"
        dump_container_logs "event-rule-backend-jobmanager" 60
        exit 1
    }

    # Also verify via the BFF /api/flink/status endpoint
    http_get "/api/flink/status" 10
    assert_http_status "GET /api/flink/status returns 200" "200" "$HTTP_STATUS" "$HTTP_BODY"
    local job_count
    job_count=$(echo "$HTTP_BODY" | jq -r '.jobs | length' 2>/dev/null)
    assert_ge "At least one Flink job is listed as running" 1 "$job_count"
    local job_found
    job_found=$(echo "$HTTP_BODY" | jq -r \
        --arg name "$FLINK_JOB_NAME" '[.jobs[] | select(. == $name)] | length' 2>/dev/null)
    assert_ge "Banking Fraud Detection Agent job is listed" 1 "$job_found"

    # Wait for Flink Kafka consumers to fully subscribe to partitions.
    # The job status transitions to RUNNING before the Kafka consumer groups have
    # assigned all partitions. Events injected too early may be missed.
    log_info "Waiting 10s for Flink Kafka consumers to finish partition assignment..."
    sleep 10
}

# ── Helper: inject events via BFF ─────────────────────────────────────────────

# Inject a list of events via POST /api/use-cases/:id/events
# Arguments: JSON array string in the format the BFF expects:
#   [{"topic":"<t>","payload":{...}}, ...]
# After calling, $HTTP_STATUS and $HTTP_BODY are set.
inject_events() {
    local events_json="$1"
    local delay_ms="${2:-$INTER_EVENT_DELAY_MS}"
    local body="{\"events\":${events_json},\"delayMs\":${delay_ms}}"
    http_post "/api/use-cases/${USE_CASE_ID}/events" "$body" 30
}

# ── Step 5 — Happy Path: Legitimate Account + Large Transaction ───────────────
# This exercises:
#   - AccountEvent (OPENING) -> entity state created
#   - Large Transaction ($50 000, cross-border) -> RULE-001 + RULE-002a fire
#   - Expected decision: PENDING_REVIEW or BLOCKED (score >= 45)
#   - Fraud alert appears on agent-fraud-alerts topic

banking_happy_path() {
    log_section "Step 5 — Banking Workflow: Account Opening + Large Transaction"

    local now
    now=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    # Use a fixed past date for account creation so age-based rules behave consistently
    local account_open_ts="2026-01-01T00:00:00Z"

    # ── Step 5a: inject AccountEvent ────────────────────────────────────────────

    log_step "Step 5a — Inject AccountEvent (OPENING) for account ${ACCOUNT_HAPPY}"

    local account_event_payload
    account_event_payload=$(jq -n \
        --arg acct "$ACCOUNT_HAPPY" \
        --arg ts "$account_open_ts" \
        '{
            accountNumber: $acct,
            accountType: "CHECKING",
            balance: 10000,
            currency: "DOLLAR",
            description: "Functional test account",
            overdraftLimit: 15000,
            owner: "Functional Test User",
            ownerCountry: "USA",
            status: "ACTIVE",
            timestamp: $ts,
            type: "OPENING"
        }')

    local events_json
    events_json=$(jq -n \
        --arg topic "agent-account-events" \
        --argjson payload "$account_event_payload" \
        '[{"topic": $topic, "payload": $payload}]')

    inject_events "$events_json" 0
    assert_http_status "Step 5a: POST events (AccountEvent) returns 200" "200" "$HTTP_STATUS" "$HTTP_BODY"
    local published
    published=$(echo "$HTTP_BODY" | jq -r '.published' 2>/dev/null)
    assert_eq "Step 5a: published=1 (AccountEvent sent)" "1" "$published"

    log_info "Waiting 8s for Flink to process AccountEvent..."
    sleep 8

    # ── Step 5b: verify no fraud alert yet (intermediate state validation) ───────

    log_step "Step 5b — Intermediate state: no fraud alerts yet for this account"

    http_get "/api/use-cases/${USE_CASE_ID}/events" 15
    assert_http_status "Step 5b: GET /events returns 200" "200" "$HTTP_STATUS" "$HTTP_BODY"

    # agent-fraud-alerts should have 0 alerts for this account at this point
    local alert_count_now
    alert_count_now=$(echo "$HTTP_BODY" | jq -r \
        --arg acct "$ACCOUNT_HAPPY" \
        '."agent-fraud-alerts" // [] | map(select(tostring | contains($acct))) | length' 2>/dev/null)
    assert_eq "Step 5b: No fraud alert after AccountEvent only (intermediate state)" \
        "0" "$alert_count_now"

    log_pass "Step 5b: No premature fraud alert — intermediate state is clean."

    # ── Step 5c: inject large cross-border Transaction ────────────────────────

    log_step "Step 5c — Inject large cross-border Transaction for account ${ACCOUNT_HAPPY}"
    # Amount $50 000 -> triggers RULE-001 (large amount, +30)
    # Country UK != ownerCountry USA -> triggers RULE-002a (cross-border, +15)
    # Total risk: 45 -> PENDING_REVIEW

    local transaction_payload
    transaction_payload=$(jq -n \
        --arg acct "$ACCOUNT_HAPPY" \
        --arg ts "$now" \
        --arg txid "FT-TX-${ACCOUNT_HAPPY}" \
        '{
            accountNumber: $acct,
            amount: 50000,
            channel: "ONLINE",
            country: "GBR",
            currency: "DOLLAR",
            merchant: "International Transfer",
            merchantCategory: "OTHER",
            timestamp: $ts,
            transactionId: $txid,
            type: "DEBIT"
        }')

    local tx_events_json
    tx_events_json=$(jq -n \
        --arg topic "agent-transactions" \
        --argjson payload "$transaction_payload" \
        '[{"topic": $topic, "payload": $payload}]')

    inject_events "$tx_events_json" 0
    assert_http_status "Step 5c: POST events (Transaction) returns 200" "200" "$HTTP_STATUS" "$HTTP_BODY"
    local tx_published
    tx_published=$(echo "$HTTP_BODY" | jq -r '.published' 2>/dev/null)
    assert_eq "Step 5c: published=1 (Transaction sent)" "1" "$tx_published"

    # ── Step 5d: wait for fraud alert and validate ──────────────────────────────

    log_step "Step 5d — Wait for fraud alert on agent-fraud-alerts topic"

    wait_for_fraud_alert "$USE_CASE_ID" "$ACCOUNT_HAPPY" "$ALERT_WAIT_TIMEOUT" || {
        dump_container_logs "event-rule-backend-taskmanager" 60
        log_error "No fraud alert produced for account '$ACCOUNT_HAPPY'."
        log_error "RULE-001 (amount=50000 > 10000) should have triggered."
        _record_result "Step 5d: Fraud alert produced for large cross-border transaction" 1 \
            "No alert found on agent-fraud-alerts within ${ALERT_WAIT_TIMEOUT}s"
        return 0   # record failure but continue to step 6
    }

    # Validate alert content
    http_get "/api/use-cases/${USE_CASE_ID}/events" 15
    assert_http_status "Step 5d: GET /events returns 200 (final state)" "200" "$HTTP_STATUS" "$HTTP_BODY"

    local alert_count
    alert_count=$(echo "$HTTP_BODY" | jq -r \
        --arg acct "$ACCOUNT_HAPPY" \
        '."agent-fraud-alerts" // [] | map(select(tostring | contains($acct))) | length' 2>/dev/null)
    assert_ge "Step 5d: At least 1 fraud alert for account (final state)" 1 "$alert_count"

    # The alert should contain the account number
    local first_alert_raw
    first_alert_raw=$(echo "$HTTP_BODY" | jq -r \
        --arg acct "$ACCOUNT_HAPPY" \
        '."agent-fraud-alerts" // [] | map(select(tostring | contains($acct))) | .[0]' 2>/dev/null)
    assert_contains "Step 5d: Alert body references the account number" \
        "$ACCOUNT_HAPPY" "$first_alert_raw"

    log_pass "Step 5: Banking happy-path scenario validated end-to-end."
}

# ── Step 6 — Suspended Account: RULE-006 (BLOCKED) ───────────────────────────
# Validates the BLOCKED decision path:
#   - AccountEvent with status=SUSPENDED
#   - Any transaction -> RULE-006 fires (score=100, BLOCKED)

banking_suspended_account_scenario() {
    log_section "Step 6 — Banking Workflow: Transaction on Suspended Account (BLOCKED)"

    local now
    now=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

    # ── Step 6a: inject AccountEvent for SUSPENDED account ────────────────────

    log_step "Step 6a — Inject AccountEvent (SUSPENDED) for account ${ACCOUNT_BLOCKED}"

    local suspended_account_payload
    suspended_account_payload=$(jq -n \
        --arg acct "$ACCOUNT_BLOCKED" \
        --arg ts "2026-01-01T00:00:00Z" \
        '{
            accountNumber: $acct,
            accountType: "CHECKING",
            balance: 10000,
            currency: "DOLLAR",
            description: "Suspended account functional test",
            overdraftLimit: 15000,
            owner: "Suspended Test User",
            ownerCountry: "USA",
            status: "SUSPENDED",
            timestamp: $ts,
            type: "OPENING"
        }')

    local susp_events
    susp_events=$(jq -n \
        --arg topic "agent-account-events" \
        --argjson payload "$suspended_account_payload" \
        '[{"topic": $topic, "payload": $payload}]')

    inject_events "$susp_events" 0
    assert_http_status "Step 6a: POST events (SUSPENDED AccountEvent) returns 200" "200" "$HTTP_STATUS" "$HTTP_BODY"
    local susp_published
    susp_published=$(echo "$HTTP_BODY" | jq -r '.published' 2>/dev/null)
    assert_eq "Step 6a: published=1 (SUSPENDED AccountEvent sent)" "1" "$susp_published"

    log_info "Waiting 8s for Flink to process SUSPENDED AccountEvent..."
    sleep 8

    # ── Step 6b: inject transaction on suspended account ─────────────────────

    log_step "Step 6b — Inject Transaction on SUSPENDED account ${ACCOUNT_BLOCKED}"
    # RULE-006: status=SUSPENDED -> score=100, BLOCKED, short-circuits all other rules

    local suspended_tx_payload
    suspended_tx_payload=$(jq -n \
        --arg acct "$ACCOUNT_BLOCKED" \
        --arg ts "$now" \
        --arg txid "FT-TX-${ACCOUNT_BLOCKED}" \
        '{
            accountNumber: $acct,
            amount: 100,
            channel: "POS",
            country: "USA",
            currency: "DOLLAR",
            merchant: "Test Merchant",
            merchantCategory: "OTHER",
            timestamp: $ts,
            transactionId: $txid,
            type: "DEBIT"
        }')

    local susp_tx_events
    susp_tx_events=$(jq -n \
        --arg topic "agent-transactions" \
        --argjson payload "$suspended_tx_payload" \
        '[{"topic": $topic, "payload": $payload}]')

    inject_events "$susp_tx_events" 0
    assert_http_status "Step 6b: POST events (Transaction on SUSPENDED account) returns 200" \
        "200" "$HTTP_STATUS" "$HTTP_BODY"
    local susp_tx_published
    susp_tx_published=$(echo "$HTTP_BODY" | jq -r '.published' 2>/dev/null)
    assert_eq "Step 6b: published=1 (Transaction on suspended account sent)" "1" "$susp_tx_published"

    # ── Step 6c: wait for BLOCKED alert ──────────────────────────────────────

    log_step "Step 6c — Wait for BLOCKED fraud alert for suspended account"

    wait_for_fraud_alert "$USE_CASE_ID" "$ACCOUNT_BLOCKED" "$ALERT_WAIT_TIMEOUT" || {
        dump_container_logs "event-rule-backend-taskmanager" 40
        log_error "No fraud alert produced for suspended account '$ACCOUNT_BLOCKED'."
        log_error "RULE-006 (status=SUSPENDED -> score=100) should have triggered."
        _record_result "Step 6c: BLOCKED alert for suspended account" 1 \
            "No alert within ${ALERT_WAIT_TIMEOUT}s"
        return 0
    }

    # Validate
    http_get "/api/use-cases/${USE_CASE_ID}/events" 15
    local blocked_count
    blocked_count=$(echo "$HTTP_BODY" | jq -r \
        --arg acct "$ACCOUNT_BLOCKED" \
        '."agent-fraud-alerts" // [] | map(select(tostring | contains($acct))) | length' 2>/dev/null)
    assert_ge "Step 6c: At least 1 BLOCKED alert for suspended account" 1 "$blocked_count"

    log_pass "Step 6: Suspended-account scenario validated — RULE-006 fires correctly."
}

# ── Step 7 — API Input Validation ─────────────────────────────────────────────
# Verifies that bad requests are rejected with the correct HTTP status codes,
# catching regressions in the API's request validation layer.

validate_api_input_validation() {
    log_section "Step 7 — API Input Validation (Regression Guard)"

    # Unknown use-case -> 404
    http_post "/api/use-cases/no-such-use-case-xyz/events" \
        '{"events":[{"topic":"t","payload":{}}]}' 10
    assert_http_status "Unknown use-case -> 404" "404" "$HTTP_STATUS" "$HTTP_BODY"

    # Empty events array -> 400
    http_post "/api/use-cases/${USE_CASE_ID}/events" \
        '{"events":[]}' 10
    assert_http_status "Empty events array -> 400" "400" "$HTTP_STATUS" "$HTTP_BODY"

    # Missing events field -> 400
    http_post "/api/use-cases/${USE_CASE_ID}/events" \
        '{"topic":"agent-transactions"}' 10
    assert_http_status "Missing events field -> 400" "400" "$HTTP_STATUS" "$HTTP_BODY"

    # Unknown topic for use-case -> 400 with knownTopics
    http_post "/api/use-cases/${USE_CASE_ID}/events" \
        '{"events":[{"topic":"no-such-topic","payload":{}}]}' 10
    assert_http_status "Unknown topic for use-case -> 400" "400" "$HTTP_STATUS" "$HTTP_BODY"
    assert_contains   "Unknown topic error includes knownTopics" "knownTopics" "$HTTP_BODY"

    # GET /api/use-cases/:id/descriptor for unknown use-case -> 404
    http_get "/api/use-cases/does-not-exist/descriptor" 10
    assert_http_status "Unknown descriptor use-case -> 404" "404" "$HTTP_STATUS" "$HTTP_BODY"
}

# ── Main ──────────────────────────────────────────────────────────────────────

main() {
    log_section "Banking Fraud Agent — Functional End-to-End Tests"
    echo "  Run ID     : $TEST_RUN_ID"
    echo "  BFF URL    : $BFF_BASE_URL"
    echo "  Use-case   : $USE_CASE_ID"
    echo "  Flink job  : $FLINK_JOB_NAME"
    echo "  Skip Compose: $SKIP_COMPOSE"
    echo ""

    check_prerequisites

    start_compose_stack

    validate_compose_stack

    validate_bff_health

    validate_use_case_descriptor

    validate_flink_job_running

    banking_happy_path

    banking_suspended_account_scenario

    validate_api_input_validation

    print_summary_and_exit
}

main "$@"
