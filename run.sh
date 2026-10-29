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

# run.sh - Manages the event-rule application with Docker Compose.
#
# Usage: ./run.sh [COMMAND]
#
# Commands:
#   up           Start all infrastructure services (Zookeeper, Kafka, Flink, BFF, Frontend)
#   down         Stop and remove all services
#   restart      Restart all services
#   logs         Follow logs from all services
#   log          Follow Flink TaskManager logs only
#   build        Build or rebuild Docker images
#   clean        Stop services and remove volumes
#   clean-state  Stop services, remove volumes, checkpoints, and reset job state (steps 1-4)
#   status       Show container status
#
# After starting with 'up', submit a Flink job with:
#   ./samples/banking-fraud-agent/scripts/deploy.sh   # Banking fraud detection

set -e

# Resolve project root directory
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_ROOT"

COMPOSE_FILE="docker-compose.yml"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Print colored messages
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Detect the available Docker Compose command (plugin v2 or standalone v1)
check_docker_compose() {
    if ! command -v docker &> /dev/null; then
        print_error "Docker not found. Please install Docker."
        exit 1
    fi

    if command -v docker-compose &> /dev/null; then
        DOCKER_COMPOSE_CMD="docker-compose"
    elif docker compose version &> /dev/null; then
        DOCKER_COMPOSE_CMD="docker compose"
    else
        print_error "docker-compose or docker compose plugin not found. Please install Docker Compose."
        exit 1
    fi
}

# Remove stale buildx builder container that can conflict with compose builds
cleanup_buildx_conflict() {
    local buildx_container="buildx_buildkit_default"

    if docker ps -a --format '{{.Names}}' 2>/dev/null | grep -Fxq "$buildx_container"; then
        print_warn "Detected stale Docker Buildx container: $buildx_container"
        print_info "Removing stale Buildx container to avoid build conflicts..."
        docker rm -f "$buildx_container" > /dev/null 2>&1 || true
    fi
}

# Execute runtime state cleanup (steps 1 to 4 of full cleanup)
perform_clean_state() {
    local errors=0
    print_info "============================================================"
    print_info "Starting runtime state cleanup (steps 1-4)..."
    print_info "============================================================"

    # 1. Stop containers and remove Compose-managed volumes and orphans
    print_info "1/4 Tearing down Docker Compose containers and volumes..."
    if [ -n "$DOCKER_COMPOSE_CMD" ]; then
        if [ -f "$COMPOSE_FILE" ]; then
            $DOCKER_COMPOSE_CMD -f "$COMPOSE_FILE" down -v --remove-orphans 2>&1 || {
                print_warn "Docker Compose down for $COMPOSE_FILE encountered an issue"
            }
        fi
        if [ -f "docker-compose-dev.yml" ]; then
            $DOCKER_COMPOSE_CMD -f "docker-compose-dev.yml" down -v --remove-orphans 2>&1 || {
                print_warn "Docker Compose down for docker-compose-dev.yml encountered an issue"
            }
        fi
    fi

    # 2. Clean up any remaining project-specific Docker containers, volumes, and networks
    print_info "2/4 Cleaning up project Docker containers, volumes, and networks..."
    local project_containers=(
        "kafka"
        "event-rule-backend-jobmanager"
        "event-rule-backend-taskmanager"
        "event-rule-bff"
        "event-rule-frontend"
        "buildx_buildkit_default"
    )
    for container in "${project_containers[@]}"; do
        if docker ps -a --format '{{.Names}}' 2>/dev/null | grep -Fxq "$container"; then
            print_info "  Removing project container: $container"
            docker rm -f "$container" >/dev/null 2>&1 || {
                print_warn "Failed to remove container: $container"
                errors=$((errors + 1))
            }
        fi
    done

    local project_volumes=(
        "kafka-data"
        "kafka-dev-data"
        "event-rule_kafka-data"
        "event-rule_kafka-dev-data"
        "event-rule-network_kafka-data"
        "event-rule-network_kafka-dev-data"
    )
    for vol in "${project_volumes[@]}"; do
        if docker volume ls -q 2>/dev/null | grep -Fxq "$vol"; then
            print_info "  Removing project volume: $vol"
            docker volume rm -f "$vol" >/dev/null 2>&1 || {
                print_warn "Failed to remove volume: $vol"
                errors=$((errors + 1))
            }
        fi
    done

    local project_networks=(
        "event-rule-network"
        "event-rule_event-rule-network"
    )
    for net in "${project_networks[@]}"; do
        if docker network ls --format '{{.Name}}' 2>/dev/null | grep -Fxq "$net"; then
            print_info "  Removing project network: $net"
            docker network rm "$net" >/dev/null 2>&1 || true
        fi
    done

    # 3. Destroy all savepoints and checkpoints
    print_info "3/4 Destroying all savepoints, checkpoints, and recovery state..."
    rm -rf "$PROJECT_ROOT/checkpoints" "$PROJECT_ROOT/savepoints"
    mkdir -p "$PROJECT_ROOT/checkpoints" "$PROJECT_ROOT/savepoints"
    chmod 775 "$PROJECT_ROOT/checkpoints" "$PROJECT_ROOT/savepoints" 2>/dev/null || true
    find "$PROJECT_ROOT" -name node_modules -prune -o -type d -name ".ipynb_checkpoints" -exec rm -rf {} + 2>/dev/null || true
    print_info "  ✓ Destroyed checkpoints and savepoints; reset directories to clean initial state (mode 775)"

    # 4. Reset application and BFF runtime job state
    print_info "4/4 Resetting persisted application and agent job state..."
    find "$PROJECT_ROOT/samples" -type f -name ".job-state.json" -exec rm -f {} + 2>/dev/null || true
    find "$PROJECT_ROOT/samples" -type f -name ".job-state*" -exec rm -f {} + 2>/dev/null || true
    print_info "  ✓ Removed all persisted job state files in samples/"

    print_info "============================================================"
    if [ $errors -gt 0 ]; then
        print_error "Clean-state completed with $errors warning(s)/error(s)."
        return 1
    else
        print_info "Clean-state completed successfully! Runtime state reset."
        print_info "============================================================"
        return 0
    fi
}

# Execute complete, irreversible cleanup of all project artifacts, state, and resources
perform_cleanup() {
    local errors=0
    print_info "============================================================"
    print_info "Starting full and irreversible project cleanup..."
    print_info "============================================================"

    # 1. Stop containers and remove Compose-managed volumes and orphans
    print_info "1/7 Tearing down Docker Compose containers and volumes..."
    if [ -n "$DOCKER_COMPOSE_CMD" ]; then
        if [ -f "$COMPOSE_FILE" ]; then
            $DOCKER_COMPOSE_CMD -f "$COMPOSE_FILE" down -v --remove-orphans 2>&1 || {
                print_warn "Docker Compose down for $COMPOSE_FILE encountered an issue"
            }
        fi
        if [ -f "docker-compose-dev.yml" ]; then
            $DOCKER_COMPOSE_CMD -f "docker-compose-dev.yml" down -v --remove-orphans 2>&1 || {
                print_warn "Docker Compose down for docker-compose-dev.yml encountered an issue"
            }
        fi
    fi

    # 2. Clean up any remaining project-specific Docker containers, volumes, and networks
    print_info "2/7 Cleaning up project Docker containers, volumes, and networks..."
    local project_containers=(
        "kafka"
        "event-rule-backend-jobmanager"
        "event-rule-backend-taskmanager"
        "event-rule-bff"
        "event-rule-frontend"
        "buildx_buildkit_default"
    )
    for container in "${project_containers[@]}"; do
        if docker ps -a --format '{{.Names}}' 2>/dev/null | grep -Fxq "$container"; then
            print_info "  Removing project container: $container"
            docker rm -f "$container" >/dev/null 2>&1 || {
                print_warn "Failed to remove container: $container"
                errors=$((errors + 1))
            }
        fi
    done

    local project_volumes=(
        "kafka-data"
        "kafka-dev-data"
        "event-rule_kafka-data"
        "event-rule_kafka-dev-data"
        "event-rule-network_kafka-data"
        "event-rule-network_kafka-dev-data"
    )
    for vol in "${project_volumes[@]}"; do
        if docker volume ls -q 2>/dev/null | grep -Fxq "$vol"; then
            print_info "  Removing project volume: $vol"
            docker volume rm -f "$vol" >/dev/null 2>&1 || {
                print_warn "Failed to remove volume: $vol"
                errors=$((errors + 1))
            }
        fi
    done

    local project_networks=(
        "event-rule-network"
        "event-rule_event-rule-network"
    )
    for net in "${project_networks[@]}"; do
        if docker network ls --format '{{.Name}}' 2>/dev/null | grep -Fxq "$net"; then
            print_info "  Removing project network: $net"
            docker network rm "$net" >/dev/null 2>&1 || true
        fi
    done

    # 3. Destroy all savepoints and checkpoints
    print_info "3/7 Destroying all savepoints, checkpoints, and recovery state..."
    rm -rf "$PROJECT_ROOT/checkpoints" "$PROJECT_ROOT/savepoints"
    mkdir -p "$PROJECT_ROOT/checkpoints" "$PROJECT_ROOT/savepoints"
    chmod 775 "$PROJECT_ROOT/checkpoints" "$PROJECT_ROOT/savepoints" 2>/dev/null || true
    find "$PROJECT_ROOT" -name node_modules -prune -o -type d -name ".ipynb_checkpoints" -exec rm -rf {} + 2>/dev/null || true
    print_info "  ✓ Destroyed checkpoints and savepoints; reset directories to clean initial state (mode 775)"

    # 4. Reset application and BFF runtime job state
    print_info "4/7 Resetting persisted application and agent job state..."
    find "$PROJECT_ROOT/samples" -type f -name ".job-state.json" -exec rm -f {} + 2>/dev/null || true
    find "$PROJECT_ROOT/samples" -type f -name ".job-state*" -exec rm -f {} + 2>/dev/null || true
    print_info "  ✓ Removed all persisted job state files in samples/"

    # 5. Delete build outputs and compiled artifacts
    print_info "5/7 Removing build outputs and compiled artifacts..."
    find "$PROJECT_ROOT" -name node_modules -prune -o -type d -name "target" -exec rm -rf {} + 2>/dev/null || true
    rm -rf \
        "$PROJECT_ROOT/event-rule-frontend/build" \
        "$PROJECT_ROOT/event-rule-frontend/dist" \
        "$PROJECT_ROOT/event-rule-frontend/.eslintcache" \
        "$PROJECT_ROOT/event-rule-frontend/bff/build" \
        "$PROJECT_ROOT/event-rule-frontend/bff/dist"
    print_info "  ✓ Removed Maven target directories and Frontend build outputs"

    # 6. Delete test reports and test run artifacts
    print_info "6/7 Removing test reports and execution artifacts..."
    rm -rf "$PROJECT_ROOT/functional-tests/reports"
    print_info "  ✓ Removed functional test reports"

    # 7. Delete caches, bytecode, and log files
    print_info "7/7 Removing temporary files, caches, and logs..."
    find "$PROJECT_ROOT" -name node_modules -prune -o -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
    find "$PROJECT_ROOT" -name node_modules -prune -o -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
    find "$PROJECT_ROOT" -name node_modules -prune -o -type f \( -name "*.pyc" -o -name "*.pyo" -o -name "*.pyd" \) -exec rm -f {} + 2>/dev/null || true
    rm -rf \
        "$PROJECT_ROOT/.coverage" \
        "$PROJECT_ROOT"/.coverage.* \
        "$PROJECT_ROOT/htmlcov" \
        "$PROJECT_ROOT/coverage.xml" \
        "$PROJECT_ROOT/.hypothesis" \
        "$PROJECT_ROOT/.mypy_cache" \
        "$PROJECT_ROOT/.ruff_cache" \
        "$PROJECT_ROOT/.tox" \
        "$PROJECT_ROOT/.nox"

    # Log files
    rm -f "$PROJECT_ROOT"/'${sys:log.file}'* 2>/dev/null || true
    find "$PROJECT_ROOT" -maxdepth 3 -type f \( -name "*.log" -o -name "*.log.*" -o -name "npm-debug.log*" -o -name "yarn-debug.log*" -o -name "yarn-error.log*" \) -exec rm -f {} + 2>/dev/null || true
    print_info "  ✓ Removed bytecode caches, coverage artifacts, and log files"

    print_info "============================================================"
    if [ $errors -gt 0 ]; then
        print_error "Cleanup completed with $errors warning(s)/error(s)."
        return 1
    else
        print_info "Cleanup completed successfully! Environment reset to clean initial state."
        print_info "============================================================"
        return 0
    fi
}

# Display usage information
usage() {
    echo "Usage: $0 [COMMAND] [OPTIONS]"
    echo ""
    echo "Commands:"
    echo "  up          Start all infrastructure services (Zookeeper, Kafka, Flink, BFF, Frontend)"
    echo "  down        Stop and remove all services"
    echo "  restart     Restart all services"
    echo "  logs        Follow logs from all services"
    echo "  log         Follow Flink TaskManager logs only"
    echo "  log-jobmanager         Follow Flink JobkManager logs only"
    echo "  build       Build or rebuild Docker images"
    echo "  clean       Completely and irreversibly clean all project artifacts, state, checkpoints, volumes, and caches"
    echo "  clean-state Reset runtime state: stop services, remove volumes/checkpoints, and reset job state (steps 1-4)"
    echo "  status      Show status of services"
    echo ""
    echo "Options:"
    echo "  -f, --force, -y, --yes   Skip confirmation prompt (e.g. for clean or clean-state)"
    echo "  -h, --help               Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 up                        # Start all infrastructure services"
    echo "  $0 logs                      # Follow all service logs"
    echo "  $0 log                       # Follow taskmanager service logs"
    echo "  $0 down                      # Stop all services"
    echo "  $0 clean                     # Clean all project resources (interactive confirmation)"
    echo "  $0 clean --force             # Clean all project resources without confirmation"
    echo "  $0 clean-state               # Reset runtime state and volumes (interactive confirmation)"
    echo "  $0 clean-state --force       # Reset runtime state and volumes without confirmation"
    echo ""
    echo "To submit a Flink job after starting infrastructure:"
    echo "  ./samples/banking-fraud-agent/scripts/deploy.sh   # Banking fraud detection"
}

# ── Parse command line arguments ──────────────────────────────────────────────
FORCE=false
COMMAND=""

for arg in "$@"; do
    case $arg in
        --help|-h)
            usage
            exit 0
            ;;
        --force|-f|--yes|-y)
            FORCE=true
            ;;
        up|down|restart|logs|log|log-jobmanager|build|clean|clean-state|status)
            COMMAND="$arg"
            ;;
    esac
done

COMMAND="${COMMAND:-up}"

# ── Runtime state ─────────────────────────────────────────────────────────────
check_docker_compose
cleanup_buildx_conflict

# Ensure BuildKit remains enabled for multi-arch support
unset DOCKER_BUILDKIT
unset COMPOSE_DOCKER_CLI_BUILD

# ── Command dispatch ──────────────────────────────────────────────────────────
case $COMMAND in
    up)
        print_info "Starting event-rule application (Zookeeper, Kafka, Flink, BFF, Frontend)..."
        print_info "Ensuring runtime directories exist..."
        mkdir -p "$PROJECT_ROOT/checkpoints" "$PROJECT_ROOT/savepoints"
        chmod 775 "$PROJECT_ROOT/checkpoints" "$PROJECT_ROOT/savepoints" 2>/dev/null || true

        print_info "After services are healthy, submit a Flink job with:"
        print_info "  ./samples/banking-fraud-agent/scripts/deploy.sh   # Banking fraud detection"

        # shellcheck disable=SC2086
        $DOCKER_COMPOSE_CMD -f $COMPOSE_FILE up -d
        print_info "Services started successfully!"
        print_info "Frontend:          http://localhost:3010"
        print_info "BFF WebSocket:     ws://localhost:8080"
        print_info "Flink UI:          http://localhost:8081"
        print_info "Kafka:             localhost:9092"
        ;;
    down)
        print_info "Stopping event-rule application..."
        $DOCKER_COMPOSE_CMD -f $COMPOSE_FILE down --remove-orphans
        print_info "Services stopped successfully!"
        ;;
    restart)
        print_info "Restarting event-rule application..."
        $DOCKER_COMPOSE_CMD -f $COMPOSE_FILE restart
        print_info "Services restarted successfully!"
        ;;
    logs)
        print_info "Following logs (Ctrl+C to exit)..."
        $DOCKER_COMPOSE_CMD -f $COMPOSE_FILE logs -f
        ;;
    log)
        print_info "Following Flink TaskManager logs (Ctrl+C to exit)..."
        $DOCKER_COMPOSE_CMD -f $COMPOSE_FILE logs -f taskmanager
        ;;
    log-jobmanager)
        print_info "Following Flink JobManager logs (Ctrl+C to exit)..."
        $DOCKER_COMPOSE_CMD -f $COMPOSE_FILE logs -f jobmanager
        ;;
    build)
        print_info "Building Docker images..."
        print_info "Using classic Docker builder (BuildKit disabled)"
        $DOCKER_COMPOSE_CMD -f $COMPOSE_FILE build
        print_info "Build completed successfully!"
        ;;
    clean)
        if [ "$FORCE" = "true" ] || [ "${CLEAN_FORCE:-false}" = "true" ] || [ "${FORCE:-false}" = "true" ]; then
            perform_cleanup
        else
            print_warn "============================================================"
            print_warn "WARNING: This will perform a complete, irreversible cleanup:"
            print_warn "  • Stop all containers and delete all project Docker volumes"
            print_warn "  • Destroy all checkpoints, savepoints, and recovery state"
            print_warn "  • Reset persisted agent job state"
            print_warn "  • Delete all build outputs (JARs, target directories, builds)"
            print_warn "  • Delete test reports, temporary caches, and logs"
            print_warn "============================================================"
            echo -n "Are you sure you want to proceed with full cleanup? (y/N): "
            read -r response
            if [[ "$response" =~ ^([yY][eE][sS]|[yY])$ ]]; then
                perform_cleanup
            else
                print_info "Cleanup cancelled."
            fi
        fi
        ;;
    clean-state)
        if [ "$FORCE" = "true" ] || [ "${CLEAN_FORCE:-false}" = "true" ] || [ "${FORCE:-false}" = "true" ]; then
            perform_clean_state
        else
            print_warn "============================================================"
            print_warn "WARNING: This will reset runtime state and remove containers/volumes:"
            print_warn "  • Stop all containers and delete all project Docker volumes"
            print_warn "  • Destroy all checkpoints, savepoints, and recovery state"
            print_warn "  • Reset persisted agent job state"
            print_warn "============================================================"
            echo -n "Are you sure you want to proceed with clean-state? (y/N): "
            read -r response
            if [[ "$response" =~ ^([yY][eE][sS]|[yY])$ ]]; then
                perform_clean_state
            else
                print_info "Clean-state cancelled."
            fi
        fi
        ;;
    status)
        print_info "Service status:"
        $DOCKER_COMPOSE_CMD -f $COMPOSE_FILE ps
        ;;
    *)
        print_error "Unknown command: $COMMAND"
        usage
        exit 1
        ;;
esac

