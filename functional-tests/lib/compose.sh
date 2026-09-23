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
# lib/compose.sh — Docker Compose lifecycle helpers for the functional test suite.
#
# Sources: source lib/compose.sh from test scripts.
# Requires: REPO_ROOT must be set before sourcing this file.

# ── Detect docker compose command ────────────────────────────────────────────

_detect_compose_cmd() {
    if docker compose version >/dev/null 2>&1; then
        echo "docker compose"
    elif command -v docker-compose >/dev/null 2>&1; then
        echo "docker-compose"
    else
        echo ""
    fi
}

COMPOSE_CMD="${COMPOSE_CMD:-$(_detect_compose_cmd)}"
COMPOSE_FILE="${REPO_ROOT}/docker-compose.yml"
COMPOSE_ENV_FILE="${REPO_ROOT}/.env"

# ── Compose wrappers ──────────────────────────────────────────────────────────

compose() {
    # shellcheck disable=SC2086
    $COMPOSE_CMD -f "$COMPOSE_FILE" "$@"
}

# ── Stack lifecycle ───────────────────────────────────────────────────────────

# Start the Docker Compose stack detached.
# Exports COMPOSE_STARTED=true so the cleanup trap can stop it.
compose_up() {
    log_info "Starting Docker Compose stack..."

    if [[ -z "$COMPOSE_CMD" ]]; then
        log_error "docker compose (or docker-compose) is not available."
        log_error "Install Docker Desktop or the Docker Compose plugin."
        return 1
    fi

    if [[ ! -f "$COMPOSE_FILE" ]]; then
        log_error "Compose file not found: $COMPOSE_FILE"
        return 1
    fi

    # Ensure a .env file exists (create from .env.example if missing)
    if [[ ! -f "$COMPOSE_ENV_FILE" ]]; then
        if [[ -f "${REPO_ROOT}/.env.example" ]]; then
            log_info ".env not found — creating from .env.example"
            cp "${REPO_ROOT}/.env.example" "$COMPOSE_ENV_FILE"
        fi
    fi

    compose up -d 2>&1 | sed 's/^/  [compose] /'
    local exit_code=${PIPESTATUS[0]}
    if [[ $exit_code -ne 0 ]]; then
        log_error "docker compose up failed (exit $exit_code)"
        return 1
    fi

    COMPOSE_STARTED=true
    export COMPOSE_STARTED
    log_info "Docker Compose stack started."
}

# Stop and remove the Docker Compose stack.
# Removes volumes only if COMPOSE_CLEAN_VOLUMES=true.
compose_down() {
    if [[ "${COMPOSE_STARTED:-false}" != "true" ]]; then
        return 0
    fi
    log_info "Tearing down Docker Compose stack..."
    if [[ "${COMPOSE_CLEAN_VOLUMES:-false}" == "true" ]]; then
        compose down -v --remove-orphans 2>&1 | sed 's/^/  [compose] /'
    else
        compose down --remove-orphans 2>&1 | sed 's/^/  [compose] /'
    fi
    COMPOSE_STARTED=false
}

# ── Container health checks ───────────────────────────────────────────────────

# Verify that a named container exists and is in the "running" state.
# Usage: assert_container_running <container_name>
assert_container_running() {
    local name="$1"
    local state
    state=$(docker inspect --format='{{.State.Running}}' "$name" 2>/dev/null)
    if [[ "$state" != "true" ]]; then
        log_error "Container '$name' is not running (State.Running=$state)"
        return 1
    fi
    log_pass "Container '$name' is running."
}

# Wait until a container's Docker healthcheck reports 'healthy', or
# until TIMEOUT_SECONDS elapses.
# Usage: wait_for_healthy <container_name> [timeout_seconds]
wait_for_healthy() {
    local name="$1"
    local timeout="${2:-${CONTAINER_HEALTHY_TIMEOUT:-120}}"
    local deadline=$(( $(date +%s) + timeout ))

    log_info "Waiting up to ${timeout}s for container '$name' to become healthy..."
    while [[ $(date +%s) -lt $deadline ]]; do
        local health
        health=$(docker inspect --format='{{.State.Health.Status}}' "$name" 2>/dev/null)
        case "$health" in
            healthy)
                log_pass "Container '$name' is healthy."
                return 0
                ;;
            unhealthy)
                log_error "Container '$name' is UNHEALTHY."
                docker inspect --format='{{range .State.Health.Log}}  {{.Output}}{{end}}' "$name" 2>/dev/null || true
                return 1
                ;;
            ""|starting|null)
                # no healthcheck or still starting — keep polling
                ;;
        esac
        sleep 3
    done

    log_error "Timed out (${timeout}s) waiting for '$name' to become healthy."
    local last_health
    last_health=$(docker inspect --format='{{.State.Health.Status}}' "$name" 2>/dev/null)
    log_error "Final health status: $last_health"
    return 1
}

# Assert that a container exists, is running, and (if it has a healthcheck) is healthy.
# Usage: assert_service_healthy <container_name> [timeout_seconds]
assert_service_healthy() {
    local name="$1"
    local timeout="${2:-${CONTAINER_HEALTHY_TIMEOUT:-120}}"

    # 1. Container must exist and be running
    assert_container_running "$name" || return 1

    # 2. If a healthcheck is defined, wait for it to pass
    local has_healthcheck
    has_healthcheck=$(docker inspect --format='{{if .Config.Healthcheck}}yes{{else}}no{{end}}' "$name" 2>/dev/null)
    if [[ "$has_healthcheck" == "yes" ]]; then
        wait_for_healthy "$name" "$timeout" || return 1
    else
        log_info "  Container '$name' has no healthcheck — running state is sufficient."
    fi
}

# Validate all required containers are running and healthy.
# Usage: assert_all_services_healthy
assert_all_services_healthy() {
    local -a services=("kafka" "event-rule-backend-jobmanager" "event-rule-backend-taskmanager" "event-rule-bff")
    local failed=0

    log_section "Container Health Validation"
    for svc in "${services[@]}"; do
        assert_service_healthy "$svc" "${CONTAINER_HEALTHY_TIMEOUT:-180}" || (( failed++ ))
    done

    if [[ $failed -gt 0 ]]; then
        log_error "$failed service(s) failed health checks."
        return 1
    fi
    log_pass "All required containers are running and healthy."
}

# ── Container name lookup ─────────────────────────────────────────────────────

# Return the running container name for a Compose service.
# Falls back to the service name if none found.
compose_container_name() {
    local service="$1"
    compose ps -q "$service" 2>/dev/null | head -1
}

# Print the last N lines of a container's logs (useful on failure).
dump_container_logs() {
    local name="$1"
    local lines="${2:-50}"
    log_info "--- Last $lines lines of logs for '$name' ---"
    docker logs --tail="$lines" "$name" 2>&1 | sed "s/^/  [$name] /" || true
    log_info "--- end ---"
}
