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
# retail-agent-e2e.sh — Retail Promotion Agent scenario tests.
#
# Tests every scenario defined in samples/retail-agent/demo-scenarios.json.
# For each scenario the test:
#   1. Resets entity state (POST /api/use-cases/retail-agent/reset-entities).
#   2. Injects the scenario events (POST /api/use-cases/retail-agent/events).
#   3. Polls the output topic (GET /api/use-cases/retail-agent/events) until the
#      expected Promotion for the scenario's customer appears.
#   4. Asserts the Promotion payload (discount, message, product category) and the
#      customer entity changelog (loyalty status) match the expected values.
#   5. Asserts no processing errors were reported for the customer.
#
# Design notes
# ────────────
# - Every scenario uses its own customerId (JS, TS, WM, EC, LB), and all output is
#   filtered by that customerId. Promotions scheduled by an earlier scenario (for
#   example the delayed "upgraded 7 days ago" promotion) can therefore never be
#   mistaken for the output of a later scenario.
# - Promotions are matched by message content as well as customer, because one
#   scenario can legitimately emit several promotions for the same customer.
# - Entity changelog records are emitted on every entity update; the last record
#   (highest sequence) carries the final loyalty status.
#
# Usage (stack already running, job already deployed):
#   SKIP_COMPOSE=true bash functional-tests/bin/use_cases/retail/retail-agent-e2e.sh
#
# Full lifecycle (bring up stack and deploy job):
#   bash functional-tests/bin/use_cases/retail/retail-agent-e2e.sh
#
# Environment variables (all optional):
#   BFF_URL                   default: http://127.0.0.1:8080
#   SKIP_COMPOSE              true → skip docker compose up/down
#   FLINK_JOB_TIMEOUT         seconds (default: 180)
#   OUTPUT_WAIT_TIMEOUT       seconds to wait for a Promotion on the topic (default: 60)
#   INTER_EVENT_DELAY_MS      ms between events within one scenario (default: 2000)
#
# Exit codes:
#   0  All scenario assertions passed.
#   1  One or more assertions failed, or the environment could not be set up.

set -euo pipefail

# ── Resolve paths ─────────────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FUNCTIONAL_TEST_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"
export REPO_ROOT

# ── Source shared libraries ───────────────────────────────────────────────────

source "${FUNCTIONAL_TEST_ROOT}/lib/assert.sh"
source "${FUNCTIONAL_TEST_ROOT}/lib/compose.sh"

# ── Configuration ─────────────────────────────────────────────────────────────

BFF_BASE_URL="${BFF_URL:-http://127.0.0.1:8080}"
SKIP_COMPOSE="${SKIP_COMPOSE:-false}"
FLINK_JOB_TIMEOUT="${FLINK_JOB_TIMEOUT:-180}"
# The browse-events source declares a 30s idleness, which can delay the watermark
# that releases scheduled (delayed) promotions — keep the wait comfortably above it.
OUTPUT_WAIT_TIMEOUT="${OUTPUT_WAIT_TIMEOUT:-60}"
INTER_EVENT_DELAY_MS="${INTER_EVENT_DELAY_MS:-2000}"

USE_CASE_ID="retail-agent"
FLINK_JOB_NAME="Retail Promotion Agent"
PROMOTION_TOPIC="agent-promotions"
ERROR_TOPIC="agent-retail-errors"
CHANGELOG_TOPIC="agent-customer-entity-changelog"
SCENARIOS_FILE="${REPO_ROOT}/samples/retail-agent/demo-scenarios.json"

# ── Cleanup trap ──────────────────────────────────────────────────────────────

_cleanup() {
    local exit_code=$?
    if [[ "${SKIP_COMPOSE}" != "true" ]]; then
        compose_down || true
    fi
    rm -f "/tmp/ft_response_body_$$" || true
}
trap _cleanup EXIT

# ── Helpers ───────────────────────────────────────────────────────────────────

# Inject events for a scenario via the BFF.
# Usage: inject_scenario_events <scenario_id>
inject_scenario_events() {
    local scenario_id="$1"

    local events_array
    events_array=$(jq -c --arg id "$scenario_id" \
        '[.[] | select(.id == $id) | .events[] | {"topic": .topic, "payload": .payload}]' \
        "$SCENARIOS_FILE")

    local body="{\"events\":${events_array},\"delayMs\":${INTER_EVENT_DELAY_MS}}"
    http_post "/api/use-cases/${USE_CASE_ID}/events" "$body" 60
}

# Reset entity state for a scenario before running it.
# Usage: reset_scenario <scenario_id>
reset_scenario() {
    local scenario_id="$1"
    local body="{\"scenarioId\":\"${scenario_id}\"}"
    http_post "/api/use-cases/${USE_CASE_ID}/reset-entities" "$body" 20
    # Give Flink time to process the sentinel before the scenario events arrive
    sleep 2
}

# Poll the Promotion topic until a Promotion for <customer_id> whose message
# contains <message_fragment> is present, or until OUTPUT_WAIT_TIMEOUT elapses.
# Sets global PROMOTIONS to the JSON array of matching promotion payloads
# (all promotions for the customer if none match at timeout).
# Usage: wait_for_promotion <customer_id> <message_fragment>
PROMOTIONS="[]"
_load_promotions() {
    local customer_id="$1" fragment="$2"
    PROMOTIONS=$(echo "$HTTP_BODY" | jq -c \
        --arg cid "$customer_id" --arg frag "$fragment" \
        '(.["'"${PROMOTION_TOPIC}"'"] // [])
         | map(.payload | select(.customerId == $cid and ((.message // "") | contains($frag))))' \
        2>/dev/null || echo "[]")
}
wait_for_promotion() {
    local customer_id="$1"
    local fragment="$2"
    local deadline=$(( $(date +%s) + OUTPUT_WAIT_TIMEOUT ))

    while [[ $(date +%s) -lt $deadline ]]; do
        http_get "/api/use-cases/${USE_CASE_ID}/events" 15 || true
        if [[ "$HTTP_STATUS" == "200" ]]; then
            _load_promotions "$customer_id" "$fragment"
            local count
            count=$(echo "$PROMOTIONS" | jq 'length' 2>/dev/null || echo 0)
            if [[ "$count" =~ ^[0-9]+$ ]] && (( count >= 1 )); then
                return 0
            fi
        fi
        sleep 3
    done
    return 1
}

# Count error records for <customer_id> on the error topic (uses the last fetched snapshot).
count_errors_for() {
    echo "$HTTP_BODY" | jq -r --arg cid "$1" \
        '(.["'"${ERROR_TOPIC}"'"] // []) | map(select(tostring | contains($cid))) | length' \
        2>/dev/null || echo 0
}

# Final loyaltyStatus recorded for <customer_id> in the entity changelog
# (uses the last fetched snapshot).
final_loyalty_status_for() {
    echo "$HTTP_BODY" | jq -r --arg cid "$1" \
        '(.["'"${CHANGELOG_TOPIC}"'"] // [])
         | map(select(.entityId == $cid)) | (.[-1].payload.entity.loyaltyStatus // "")' \
        2>/dev/null || echo ""
}

# Common assertions run after a Promotion has been received.
# Usage: assert_promotion <label> <expected_discount> <message_fragment> <expected_category|""> <customer_id> <expected_final_status>
assert_promotion() {
    local label="$1" discount="$2" fragment="$3" category="$4" customer_id="$5" status="$6"

    # Most recent matching promotion
    local promo
    promo=$(echo "$PROMOTIONS" | jq -c '.[-1]' 2>/dev/null || echo "null")
    log_info "Actual promotion: $(echo "$promo" | python3 -m json.tool 2>/dev/null || echo "$promo")"

    assert_json_eq   "${label}: promotion.customerId = ${customer_id}" ".customerId" "$customer_id" "$promo"
    assert_json_eq   "${label}: promotion.discount = ${discount}"      ".discount"   "$discount"    "$promo"
    assert_contains  "${label}: promotion.message contains '${fragment}'" "$fragment" \
        "$(echo "$promo" | jq -r '.message // ""')"
    if [[ -n "$category" ]]; then
        assert_json_eq "${label}: promotion.productCategory = ${category}" ".productCategory" "$category" "$promo"
    fi
    assert_nonempty  "${label}: promotion.timestamp is set" "$(echo "$promo" | jq -r '.timestamp // ""')"

    # Snapshot used for the error / changelog checks must be fresh and include
    # the entity update that produced the promotion.
    http_get "/api/use-cases/${USE_CASE_ID}/events" 15 || true
    assert_eq "${label}: no errors on ${ERROR_TOPIC} for ${customer_id}" "0" "$(count_errors_for "$customer_id")"
    assert_eq "${label}: final loyaltyStatus in entity changelog = ${status}" "$status" \
        "$(final_loyalty_status_for "$customer_id")"
}

# ── Environment validation ─────────────────────────────────────────────────────

check_prerequisites() {
    log_section "Prerequisite Checks"
    for tool in docker curl jq; do
        if command -v "$tool" >/dev/null 2>&1; then
            log_pass "$tool is available."
        else
            log_error "$tool is not installed or not on PATH."
            exit 1
        fi
    done
}

# ── Stack readiness ────────────────────────────────────────────────────────────

validate_stack() {
    log_section "Stack Readiness"
    wait_for_bff 90 || { log_error "BFF not reachable."; exit 1; }
    http_get "/health" 10
    assert_http_status "GET /health → 200" "200" "$HTTP_STATUS" "$HTTP_BODY"
    assert_json_eq "BFF status = ok" ".status" "ok" "$HTTP_BODY"
}

validate_descriptor() {
    log_section "Use-Case Descriptor and Topics"
    http_get "/api/use-cases/${USE_CASE_ID}/descriptor" 10
    assert_http_status "GET descriptor → 200" "200" "$HTTP_STATUS" "$HTTP_BODY"
    assert_json_eq "Descriptor flinkJobName = ${FLINK_JOB_NAME}" ".flinkJobName" "$FLINK_JOB_NAME" "$HTTP_BODY"

    local snapshot="$HTTP_BODY" t count
    for t in agent-purchases agent-customer-events agent-browse-events; do
        count=$(echo "$snapshot" | jq -r --arg n "$t" '[.topics[] | select(.name == $n)] | length' 2>/dev/null)
        assert_ge "Descriptor declares inbound topic: $t" 1 "$count"
    done
    for t in "$PROMOTION_TOPIC" "$ERROR_TOPIC" "$CHANGELOG_TOPIC"; do
        count=$(echo "$snapshot" | jq -r --arg n "$t" '[.outboundTopics[] | select(.name == $n)] | length' 2>/dev/null)
        assert_ge "Descriptor declares outbound topic: $t" 1 "$count"
    done
}

validate_flink_job() {
    log_section "Flink Job Readiness"
    wait_for_flink_job "$FLINK_JOB_NAME" "$FLINK_JOB_TIMEOUT" || {
        log_error "Flink job '${FLINK_JOB_NAME}' is not running."
        log_error "Deploy with: ./samples/retail-agent/scripts/deploy.sh"
        exit 1
    }
    log_info "Waiting 8s for Kafka consumers to finish partition assignment..."
    sleep 8
}

# ══════════════════════════════════════════════════════════════════════════════
# SCENARIO TEST FUNCTIONS
# Each function resets entity state, injects the scenario, waits for the expected
# Promotion and asserts its content.
# ══════════════════════════════════════════════════════════════════════════════

# Run the common reset / inject / wait steps.
# Usage: run_scenario <number> <scenario_id> <expected_published> <customer_id> <message_fragment>
# Returns 1 (after recording a failure) when the expected promotion never appears.
run_scenario() {
    local number="$1" scenario_id="$2" published="$3" customer_id="$4" fragment="$5"

    reset_scenario "$scenario_id"
    inject_scenario_events "$scenario_id"
    assert_http_status "Scenario ${number}: events injected → 200" "200" "$HTTP_STATUS" "$HTTP_BODY"
    assert_json_eq "Scenario ${number}: published = ${published}" ".published" "$published" "$HTTP_BODY"

    log_info "Waiting for Promotion for ${customer_id} on ${PROMOTION_TOPIC}..."
    if ! wait_for_promotion "$customer_id" "$fragment"; then
        _record_result "Scenario ${number}: Promotion for ${customer_id} appeared on ${PROMOTION_TOPIC}" 1 \
            "No promotion with message containing '${fragment}' within ${OUTPUT_WAIT_TIMEOUT}s"
        return 1
    fi
    _record_result "Scenario ${number}: Promotion for ${customer_id} appeared on ${PROMOTION_TOPIC}" 0
}

# ── Scenario 1: upgrade-customer-to-gold ──────────────────────────────────────
# Silver customer with 3 purchases (5 800 total) → upgraded to Gold; the upgrade
# promotion (10%) is released by the scheduled follow-up event.
test_scenario_01_upgrade_to_gold() {
    log_section "Scenario 1 — upgrade-customer-to-gold: Silver → Gold"
    log_info "Expected: Promotion discount=10 'upgraded to Gold' | final loyaltyStatus=Gold"
    run_scenario 1 "upgrade-customer-to-gold" 5 "JS" "upgraded to Gold" || return 0
    assert_promotion "Scenario 1" 10 "10% discount" "" "JS" "Gold"
}

# ── Scenario 2: upgrade-customer-to-platinum ──────────────────────────────────
test_scenario_02_upgrade_to_platinum() {
    log_section "Scenario 2 — upgrade-customer-to-platinum: Gold → Platinum"
    log_info "Expected: Promotion discount=12 'upgraded to Platinum' | final loyaltyStatus=Platinum"
    run_scenario 2 "upgrade-customer-to-platinum" 7 "TS" "upgraded to Platinum" || return 0
    assert_promotion "Scenario 2" 12 "12% discount" "" "TS" "Platinum"
}

# ── Scenario 3: large-purchase-reward ─────────────────────────────────────────
# Gold customer, the last purchase (SPORT) triggers the large-purchase reward (10%).
# The customer also reaches Platinum through the cumulated purchases.
test_scenario_03_large_purchase_reward() {
    log_section "Scenario 3 — large-purchase-reward: Large Purchase → 10% Promotion"
    log_info "Expected: Promotion discount=10 'Thanks for your recent purchase' | productCategory=SPORT"
    run_scenario 3 "large-purchase-reward" 5 "WM" "Thanks for your recent purchase" || return 0
    assert_promotion "Scenario 3" 10 "10% discount" "SPORT" "WM" "Platinum"
}

# ── Scenario 4: repeat-purchase-reward ────────────────────────────────────────
# Silver customer, 3 ELECTRONICS purchases within 48h, then enters the store → 5%.
test_scenario_04_repeat_purchase_reward() {
    log_section "Scenario 4 — repeat-purchase-reward: Repeat Purchases → 5% Promotion"
    log_info "Expected: Promotion discount=5 'multiple purchases' | productCategory=ELECTRONICS | status stays Silver"
    run_scenario 4 "repeat-purchase-reward" 5 "EC" "multiple purchases" || return 0
    assert_promotion "Scenario 4" 5 "5% discount" "ELECTRONICS" "EC" "Silver"
}

# ── Scenario 5: clear-purchase-intent-reward ──────────────────────────────────
# Silver customer, 3 ELECTRONICS browse events within 48h, then enters the store → 3%.
test_scenario_05_clear_purchase_intent_reward() {
    log_section "Scenario 5 — clear-purchase-intent-reward: Clear Purchase Intent → 3% Promotion"
    log_info "Expected: Promotion discount=3 'interested by this category' | productCategory=ELECTRONICS | status stays Silver"
    run_scenario 5 "clear-purchase-intent-reward" 5 "LB" "interested by this category" || return 0
    assert_promotion "Scenario 5" 3 "3% discount" "ELECTRONICS" "LB" "Silver"
}

# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

main() {
    log_section "Retail Promotion Agent — Scenario Tests"
    echo "  BFF URL          : $BFF_BASE_URL"
    echo "  Use-case         : $USE_CASE_ID"
    echo "  Flink job        : $FLINK_JOB_NAME"
    echo "  Output topic     : $PROMOTION_TOPIC"
    echo "  Output wait (s)  : $OUTPUT_WAIT_TIMEOUT"
    echo "  Inter-event delay: ${INTER_EVENT_DELAY_MS}ms"
    echo "  Skip Compose     : $SKIP_COMPOSE"
    echo ""

    check_prerequisites

    if [[ "${SKIP_COMPOSE}" != "true" ]]; then
        log_section "Starting Docker Compose Stack"
        compose_up || exit 1
    fi

    validate_stack
    validate_descriptor
    validate_flink_job

    # Reset all topics to start from a clean slate
    log_info "Resetting all retail-agent topics before test run..."
    http_post "/api/use-cases/${USE_CASE_ID}/reset-topics" '{}' 30 || true
    assert_http_status "POST reset-topics → 200" "200" "$HTTP_STATUS" "$HTTP_BODY"
    sleep 3

    # Eagerly initialize BFF Kafka consumer group to prevent race conditions in Scenario 1
    log_info "Eagerly warming up BFF Kafka consumer group..."
    http_get "/api/use-cases/${USE_CASE_ID}/events" 10
    sleep 6

    test_scenario_01_upgrade_to_gold
    test_scenario_02_upgrade_to_platinum
    test_scenario_03_large_purchase_reward
    test_scenario_04_repeat_purchase_reward
    test_scenario_05_clear_purchase_intent_reward

    print_summary_and_exit
}

main "$@"
