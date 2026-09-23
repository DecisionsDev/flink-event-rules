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
#   up       Start all infrastructure services (Zookeeper, Kafka, Flink, BFF, Frontend)
#   down     Stop and remove all services
#   restart  Restart all services
#   logs     Follow logs from all services
#   log      Follow Flink TaskManager logs only
#   build    Build or rebuild Docker images
#   clean    Stop services and remove volumes
#   status   Show container status
#
# After starting with 'up', submit a Flink job with:
#   ./samples/banking-fraud-agent/scripts/deploy.sh   # Banking fraud detection

set -e

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

    if docker ps -a --format '{{.Names}}' | grep -Fxq "$buildx_container"; then
        print_warn "Detected stale Docker Buildx container: $buildx_container"
        print_info "Removing stale Buildx container to avoid build conflicts..."
        docker rm -f "$buildx_container" > /dev/null
    fi
}

# Display usage information
usage() {
    echo "Usage: $0 [COMMAND]"
    echo ""
    echo "Commands:"
    echo "  up          Start all infrastructure services (Zookeeper, Kafka, Flink, BFF, Frontend)"
    echo "  down        Stop and remove all services"
    echo "  restart     Restart all services"
    echo "  logs        Follow logs from all services"
    echo "  log         Follow Flink TaskManager logs only"
    echo "  build       Build or rebuild Docker images"
    echo "  clean       Stop services and remove volumes"
    echo "  status      Show status of services"
    echo ""
    echo "Examples:"
    echo "  $0 up                        # Start all infrastructure services"
    echo "  $0 logs                      # Follow all service logs"
    echo "  $0 down                      # Stop all services"
    echo "  $0 clean                     # Stop and remove volumes"
    echo ""
    echo "To submit a Flink job after starting infrastructure:"
    echo "  ./samples/banking-fraud-agent/scripts/deploy.sh   # Banking fraud detection"
}

# ── Parse command line arguments ──────────────────────────────────────────────
COMMAND="${1:-up}"

for arg in "$@"; do
    case $arg in
        --help|-h)
            usage
            exit 0
            ;;
    esac
done

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
    build)
        print_info "Building Docker images..."
        print_info "Using classic Docker builder (BuildKit disabled)"
        $DOCKER_COMPOSE_CMD -f $COMPOSE_FILE build
        print_info "Build completed successfully!"
        ;;
    clean)
        print_warn "This will stop all services and remove volumes. Continue? (y/N)"
        read -r response
        if [[ "$response" =~ ^([yY][eE][sS]|[yY])$ ]]; then
            print_info "Cleaning up event-rule application..."
            $DOCKER_COMPOSE_CMD -f $COMPOSE_FILE down -v --remove-orphans
            print_info "Cleanup completed successfully!"
        else
            print_info "Cleanup cancelled."
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

