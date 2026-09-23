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
# run-functional-tests.sh — Entry point for the functional test suite.
#
# This script:
#   1. Ensures a .env file exists (copies from .env.example if absent).
#   2. Starts the Docker Compose stack (unless SKIP_COMPOSE=true).
#   3. Waits for all containers to be healthy.
#   4. Deploys the selected use case's Flink job (unless SKIP_FLINK_DEPLOY=true).
#   5. Runs the selected use-case E2E test script(s).
#   6. Tears down the stack on exit (unless SKIP_COMPOSE=true).
#
# Usage
# ─────
#   ./functional-tests/run-functional-tests.sh                 # all use cases
#   ./functional-tests/run-functional-tests.sh banking         # banking only
#   ./functional-tests/run-functional-tests.sh aircraft        # aircraft only
#   SKIP_COMPOSE=true ./functional-tests/run-functional-tests.sh aircraft
#
# Environment variables
# ─────────────────────
#   SKIP_COMPOSE          true → skip docker compose up/down (default: false)
#   SKIP_FLINK_DEPLOY     true → skip Flink job deployment (default: false)
#   COMPOSE_CLEAN_VOLUMES true → pass -v to compose down   (default: false)
#   BFF_URL               BFF base URL (default: http://127.0.0.1:8080)
#   CONTAINER_HEALTHY_TIMEOUT  seconds to wait for containers (default: 180)
#   FLINK_JOB_TIMEOUT          seconds to wait for Flink job (default: 180)
#   ALERT_WAIT_TIMEOUT         seconds to wait for Kafka alert (default: 90)
#   REPORT_DIR                 directory for test reports (default: functional-tests/reports)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
FUNCTIONAL_TEST_ROOT="$SCRIPT_DIR"

export REPO_ROOT

# ── Source shared libraries ───────────────────────────────────────────────────

# shellcheck source=lib/assert.sh
source "${FUNCTIONAL_TEST_ROOT}/lib/assert.sh"
# shellcheck source=lib/compose.sh
source "${FUNCTIONAL_TEST_ROOT}/lib/compose.sh"

# ── Configuration ─────────────────────────────────────────────────────────────

SKIP_COMPOSE="${SKIP_COMPOSE:-false}"
SKIP_FLINK_DEPLOY="${SKIP_FLINK_DEPLOY:-false}"
BFF_URL="${BFF_URL:-http://127.0.0.1:8080}"
REPORT_DIR="${REPORT_DIR:-${FUNCTIONAL_TEST_ROOT}/reports}"
CONTAINER_HEALTHY_TIMEOUT="${CONTAINER_HEALTHY_TIMEOUT:-180}"
FLINK_JOB_TIMEOUT="${FLINK_JOB_TIMEOUT:-180}"

# Which use cases to run: "all", "banking", "aircraft"
USE_CASE="${1:-all}"

# Return the deploy script path for a given use-case name.
# Plain function instead of associative array — compatible with bash 3.2 (macOS default).
deploy_script_for() {
    case "$1" in
        banking)  echo "${REPO_ROOT}/samples/banking-fraud-agent/scripts/deploy.sh" ;;
        aircraft) echo "${REPO_ROOT}/samples/airtraffic-agent/scripts/deploy.sh" ;;
        *)        echo "" ;;
    esac
}

# Return the E2E test script path for a given use-case name.
test_script_for() {
    case "$1" in
        banking)  echo "${FUNCTIONAL_TEST_ROOT}/bin/use_cases/banking/banking-fraud-e2e.sh" ;;
        aircraft) echo "${FUNCTIONAL_TEST_ROOT}/bin/use_cases/aircraft/aircraft-e2e.sh" ;;
        *)        echo "" ;;
    esac
}

# ── Setup ─────────────────────────────────────────────────────────────────────

mkdir -p "$REPORT_DIR"

# ── Prerequisite checks ───────────────────────────────────────────────────────

preflight_check() {
    local failed=0

    command -v docker >/dev/null 2>&1 || { log_error "docker not found"; (( failed++ )); }
    command -v curl   >/dev/null 2>&1 || { log_error "curl not found"; (( failed++ )); }
    command -v jq     >/dev/null 2>&1 || { log_error "jq not found"; (( failed++ )); }

    if [[ $failed -gt 0 ]]; then
        log_error "Prerequisites not met. Install missing tools and retry."
        exit 1
    fi

    # Fat JAR must exist (required by Flink job submission)
    local fat_jar="${REPO_ROOT}/event-rule-backend/target/event-rule-backend-1.0-SNAPSHOT.jar"
    if [[ ! -f "$fat_jar" ]]; then
        log_warn "Fat JAR not found: $fat_jar"
        log_warn "Build with: ./build.sh  or  mvn package -pl event-rule-backend --settings settings.xml -DskipTests"
        if [[ "${SKIP_FLINK_DEPLOY:-false}" != "true" ]]; then
            log_error "Flink job cannot be deployed without the fat JAR."
            log_error "Set SKIP_FLINK_DEPLOY=true to skip deployment and test a pre-running job."
            exit 1
        fi
    fi
}

# ── Compose lifecycle ─────────────────────────────────────────────────────────

_compose_started_by_runner=false

cleanup() {
    if [[ "$_compose_started_by_runner" == "true" ]]; then
        log_info "Stopping Docker Compose stack (runner cleanup)..."
        COMPOSE_STARTED=true compose_down || true
    fi
}
trap cleanup EXIT

start_stack() {
    if [[ "$SKIP_COMPOSE" == "true" ]]; then
        log_info "SKIP_COMPOSE=true — assuming stack is already running."
        return 0
    fi

    compose_up || exit 1
    _compose_started_by_runner=true

    log_section "Waiting for all services to become healthy..."
    assert_all_services_healthy || {
        log_error "One or more services failed health checks — aborting."
        exit 1
    }
}

# ── Flink job deployment ──────────────────────────────────────────────────────

deploy_flink_job() {
    local use_case="$1"
    if [[ "$SKIP_FLINK_DEPLOY" == "true" ]]; then
        log_info "SKIP_FLINK_DEPLOY=true — skipping Flink job deployment for '$use_case'."
        return 0
    fi

    local deploy_script
    deploy_script=$(deploy_script_for "$use_case")
    if [[ -z "$deploy_script" ]]; then
        log_warn "No deploy script registered for use case '$use_case' — skipping."
        return 0
    fi

    log_section "Deploying Flink Job for use case: $use_case"

    if [[ ! -f "$deploy_script" ]]; then
        log_error "Deploy script not found: $deploy_script"
        exit 1
    fi

    bash "$deploy_script" || {
        log_error "Flink job deployment failed for '$use_case'."
        dump_container_logs "event-rule-backend-jobmanager" 60
        exit 1
    }

    log_pass "Flink job deployed for '$use_case'."
}

# ── Run a single use-case test ────────────────────────────────────────────────

run_use_case() {
    local use_case="$1"
    local test_script
    test_script=$(test_script_for "$use_case")

    if [[ -z "$test_script" ]]; then
        log_error "No test script registered for use case '$use_case'."
        log_error "Available use cases: banking, aircraft"
        return 1
    fi
    if [[ ! -f "$test_script" ]]; then
        log_error "Test script not found: $test_script"
        return 1
    fi

    local report_file="${REPORT_DIR}/${use_case}-$(date +%Y%m%d-%H%M%S).log"
    log_section "Running use case: $use_case"
    log_info "Report: $report_file"

    deploy_flink_job "$use_case"

    # Pass SKIP_COMPOSE=true to the test script — stack is already managed here.
    local script_exit=0
    SKIP_COMPOSE=true \
    BFF_URL="$BFF_URL" \
    CONTAINER_HEALTHY_TIMEOUT="$CONTAINER_HEALTHY_TIMEOUT" \
    FLINK_JOB_TIMEOUT="$FLINK_JOB_TIMEOUT" \
    ALERT_WAIT_TIMEOUT="${ALERT_WAIT_TIMEOUT:-90}" \
    bash "$test_script" 2>&1 | tee "$report_file" || script_exit=$?

    return "$script_exit"
}

# ── Resolve which use cases to run ───────────────────────────────────────────

resolve_use_cases() {
    local selection="$1"
    case "$selection" in
        all)     echo "banking aircraft" ;;
        banking) echo "banking" ;;
        aircraft) echo "aircraft" ;;
        *)
            log_error "Unknown use case: '$selection'. Valid values: all, banking, aircraft"
            exit 1
            ;;
    esac
}

# ── Main ──────────────────────────────────────────────────────────────────────

main() {
    local use_cases_to_run
    use_cases_to_run=$(resolve_use_cases "$USE_CASE")

    log_section "Event Rule — Functional Test Runner"
    echo "  Repo root      : $REPO_ROOT"
    echo "  BFF URL        : $BFF_URL"
    echo "  Use cases      : $use_cases_to_run"
    echo "  Skip Compose   : $SKIP_COMPOSE"
    echo "  Skip Deploy    : $SKIP_FLINK_DEPLOY"
    echo "  Report dir     : $REPORT_DIR"
    echo ""

    preflight_check
    start_stack

    local overall_exit=0
    for uc in $use_cases_to_run; do
        run_use_case "$uc" || overall_exit=1
    done

    if [[ $overall_exit -ne 0 ]]; then
        log_error "One or more use-case test suites FAILED."
        exit 1
    fi
    log_pass "All selected use-case test suites PASSED."
}

main "$@"
