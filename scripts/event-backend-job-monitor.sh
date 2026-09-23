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
# event-backend-job-monitor.sh
#
# Interactive Flink job monitor and diagnostic tool.
# Connects to the Flink REST API and optionally searches container logs to
# help diagnose job lifecycle issues (e.g. NotFoundException after a JobManager restart).
#
# Usage:
#   ./scripts/event-backend-job-monitor.sh <command> [options]
#
# Commands:
#   list              List all Flink jobs with IDs and statuses
#   check <job-id>    Inspect a specific job and display any recorded exceptions
#   monitor           Continuously refresh job and cluster status every 5 s (Ctrl+C to stop)
#   history           Search JobManager container logs for job submission/completion entries
#   diagnose <job-id> Combined check + log search for a specific job ID
#   help              Show this help message
#
# Environment variables:
#   FLINK_REST_URL    Flink REST API base URL (default: http://localhost:8081)
#   BACKEND_CONTAINER JobManager container name used for log searches
#                     (default: event-rule-backend-jobmanager)
#
# Dependencies: curl, jq, Docker

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Configuration
FLINK_REST_URL="${FLINK_REST_URL:-http://localhost:8081}"
BACKEND_CONTAINER="${BACKEND_CONTAINER:-event-rule-backend-jobmanager}"
CHECK_INTERVAL=5
MAX_RETRIES=5

# Print functions
print_header() {
    echo -e "${CYAN}╔════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║   ${NC} ${BLUE}Event backend Job Monitor & Diagnostic Tool ${NC}            ${CYAN}║${NC}"
    echo -e "${CYAN}╚════════════════════════════════════════════════════════════╝${NC}"
    echo ""
}

print_section() {
    echo ""
    echo -e "${CYAN}═══════════════════════════════════════════════════════════${NC}"
    echo -e "${CYAN}  $1${NC}"
    echo -e "${CYAN}═══════════════════════════════════════════════════════════${NC}"
    echo ""
}

print_info() {
    echo -e "${BLUE}[INFO]${NC} $(date '+%H:%M:%S') - $1"
}

print_success() {
    echo -e "${GREEN}[✓]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[⚠]${NC} $1"
}

print_error() {
    echo -e "${RED}[✗]${NC} $1"
}

# Verify curl and jq are available before proceeding
check_dependencies() {
    local missing_deps=()

    if ! command -v curl &> /dev/null; then
        missing_deps+=("curl")
    fi

    if ! command -v jq &> /dev/null; then
        missing_deps+=("jq")
    fi

    if [ ${#missing_deps[@]} -gt 0 ]; then
        print_error "Missing required dependencies: ${missing_deps[*]}"
        echo "Please install them:"
        echo "  macOS: brew install ${missing_deps[*]}"
        echo "  Linux: apt-get install ${missing_deps[*]}"
        exit 1
    fi
}

# Check Flink REST API connectivity; print cluster overview on success
check_flink_api() {
    print_section "Checking Flink REST API"

    local retries=0
    while [ $retries -lt $MAX_RETRIES ]; do
        if curl -s -f --max-time 5 "$FLINK_REST_URL/overview" > /dev/null 2>&1; then
            print_success "Flink REST API is accessible at $FLINK_REST_URL"

            # Get cluster overview
            local overview=$(curl -s "$FLINK_REST_URL/overview")
            echo ""
            echo "Cluster Overview:"
            echo "$overview" | jq '.'
            return 0
        fi

        retries=$((retries + 1))
        if [ $retries -lt $MAX_RETRIES ]; then
            print_warning "Cannot reach Flink API (attempt $retries/$MAX_RETRIES)"
            sleep 2
        fi
    done

    print_error "Flink REST API is not accessible at $FLINK_REST_URL"
    print_info "Checking if backend container is running..."

    if docker ps --format '{{.Names}}' | grep -q "^${BACKEND_CONTAINER}$"; then
        print_info "Container is running. Checking logs..."
        docker logs --tail 20 "$BACKEND_CONTAINER"
    else
        print_error "Backend container '$BACKEND_CONTAINER' is not running!"
        print_info "Start it with: docker compose up -d"
    fi

    return 1
}

# List all jobs with IDs, statuses, and per-job details
list_jobs() {
    print_section "Listing All Flink Jobs"

    local jobs=$(curl -s "$FLINK_REST_URL/jobs")

    if [ -z "$jobs" ]; then
        print_warning "No response from Flink API"
        return 1
    fi

    local job_count=$(echo "$jobs" | jq '.jobs | length')

    if [ "$job_count" -eq 0 ]; then
        print_warning "No jobs found in Flink cluster"
        echo ""
        echo "Possible reasons:"
        echo "  1. No jobs have been submitted yet"
        echo "  2. All jobs have completed and been cleaned up"
        echo "  3. JobManager was restarted"
        echo ""
        echo "To submit a job, run:"
        echo "  ./samples/banking-fraud-agent/scripts/deploy.sh"
        return 0
    fi

    print_success "Found $job_count job(s)"
    echo ""

    echo "$jobs" | jq -r '.jobs[] | "Job ID: \(.id)\nStatus:  \(.status)\n"'

    # Fetch detailed info for each job
    echo "$jobs" | jq -r '.jobs[].id' | while read -r job_id; do
        echo "─────────────────────────────────────────────────────────────"
        echo "Detailed info for job: $job_id"
        echo ""

        local job_details=$(curl -s "$FLINK_REST_URL/jobs/$job_id")
        echo "$job_details" | jq '{
            name: .name,
            state: .state,
            "start-time": .["start-time"],
            "end-time": .["end-time"],
            duration: .duration,
            vertices: .vertices | length
        }'
        echo ""
    done
}

# Inspect a specific job by ID and display any recorded exceptions
check_job() {
    local job_id=$1

    print_section "Checking Job: $job_id"

    local response=$(curl -s -w "\n%{http_code}" "$FLINK_REST_URL/jobs/$job_id")
    local http_code=$(echo "$response" | tail -n1)
    local body=$(echo "$response" | sed '$d')

    if [ "$http_code" = "200" ]; then
        print_success "Job found!"
        echo ""
        echo "$body" | jq '.'

        # Check for recorded exceptions
        print_info "Checking for exceptions..."
        local exceptions=$(curl -s "$FLINK_REST_URL/jobs/$job_id/exceptions")
        local exception_count=$(echo "$exceptions" | jq '.["all-exceptions"] | length')

        if [ "$exception_count" -gt 0 ]; then
            print_warning "Found $exception_count exception(s)"
            echo "$exceptions" | jq '.["all-exceptions"][] | {
                timestamp: .timestamp,
                exception: .exception,
                task: .task
            }'
        else
            print_success "No exceptions found"
        fi

    elif [ "$http_code" = "404" ]; then
        print_error "Job not found (HTTP 404)"
        echo ""
        echo "This job ID does not exist in the Flink cluster."
        echo ""
        echo "Possible reasons:"
        echo "  1. Job ID is incorrect or contains a typo"
        echo "  2. Job has completed and been cleaned up"
        echo "  3. JobManager was restarted, losing job metadata"
        echo "  4. Job exists in a different Flink cluster"
        echo ""
        echo "Available jobs:"
        list_jobs

    else
        print_error "Unexpected HTTP status: $http_code"
        echo "$body"
    fi
}

# Continuously refresh cluster overview and job list every CHECK_INTERVAL seconds
monitor_jobs() {
    print_section "Continuous Job Monitoring"
    print_info "Monitoring Flink jobs every ${CHECK_INTERVAL}s (Press Ctrl+C to stop)"
    echo ""

    while true; do
        clear
        print_header
        echo "Last update: $(date '+%Y-%m-%d %H:%M:%S')"
        echo ""

        # Cluster overview
        local overview=$(curl -s "$FLINK_REST_URL/overview" 2>/dev/null)
        if [ -n "$overview" ]; then
            echo "Cluster Status:"
            echo "$overview" | jq '{
                "jobs-running": .["jobs-running"],
                "jobs-finished": .["jobs-finished"],
                "jobs-cancelled": .["jobs-cancelled"],
                "jobs-failed": .["jobs-failed"],
                "taskmanagers": .taskmanagers,
                "slots-available": .["slots-available"]
            }'
            echo ""
        fi

        # Job list
        local jobs=$(curl -s "$FLINK_REST_URL/jobs" 2>/dev/null)
        if [ -n "$jobs" ]; then
            local job_count=$(echo "$jobs" | jq '.jobs | length')

            if [ "$job_count" -gt 0 ]; then
                echo "Active Jobs:"
                echo "$jobs" | jq -r '.jobs[] | "  • \(.id) - \(.status)"'
            else
                echo "No active jobs"
            fi
        fi

        sleep $CHECK_INTERVAL
    done
}

# Search JobManager container logs for job submission and completion entries
check_job_history() {
    print_section "Checking Job History in Logs"

    if ! docker ps --format '{{.Names}}' | grep -q "^${BACKEND_CONTAINER}$"; then
        print_error "Backend container '$BACKEND_CONTAINER' is not running"
        return 1
    fi

    print_info "Searching for job submissions in logs..."
    echo ""

    docker logs "$BACKEND_CONTAINER" 2>&1 | grep -i "job.*submitted\|job.*started\|job.*finished" | tail -20

    echo ""
    print_info "Searching for job IDs in logs..."
    echo ""

    docker logs "$BACKEND_CONTAINER" 2>&1 | grep -oE '[0-9a-f]{32}' | sort -u | tail -10
}

# Full diagnostic for a specific job: REST check + log search
diagnose_job() {
    local job_id=$1

    print_section "Diagnosing Job: $job_id"

    # Check via REST API
    check_job "$job_id"

    # Search for the job ID in container logs
    print_info "Searching for job ID in container logs..."
    if docker ps --format '{{.Names}}' | grep -q "^${BACKEND_CONTAINER}$"; then
        local log_matches=$(docker logs "$BACKEND_CONTAINER" 2>&1 | grep -c "$job_id" || true)
        if [ "$log_matches" -gt 0 ]; then
            print_success "Found $log_matches reference(s) in logs"
            echo ""
            docker logs "$BACKEND_CONTAINER" 2>&1 | grep "$job_id" | tail -10
        else
            print_warning "Job ID not found in logs"
        fi
    fi
}

# Show usage information
show_usage() {
    print_header

    echo "USAGE:"
    echo "  $0 [command] [options]"
    echo ""
    echo "COMMANDS:"
    echo "  ${GREEN}list${NC}              - List all Flink jobs"
    echo "  ${GREEN}check <job-id>${NC}    - Check specific job by ID"
    echo "  ${GREEN}monitor${NC}           - Continuously monitor all jobs"
    echo "  ${GREEN}history${NC}           - Show job history from logs"
    echo "  ${GREEN}diagnose <job-id>${NC} - Full diagnostic for specific job"
    echo "  ${GREEN}help${NC}              - Show this help message"
    echo ""
    echo "ENVIRONMENT VARIABLES:"
    echo "  ${YELLOW}FLINK_REST_URL${NC}      - Flink REST API URL (default: http://localhost:8081)"
    echo "  ${YELLOW}BACKEND_CONTAINER${NC}   - JobManager container name (default: event-rule-backend-jobmanager)"
    echo ""
    echo "EXAMPLES:"
    echo "  # List all jobs"
    echo "  $0 list"
    echo ""
    echo "  # Check specific job"
    echo "  $0 check 11d84800813e78953ca13529208a697d"
    echo ""
    echo "  # Monitor jobs continuously"
    echo "  $0 monitor"
    echo ""
    echo "  # Diagnose job not found issue"
    echo "  $0 diagnose 11d84800813e78953ca13529208a697d"
    echo ""
}

# Main execution
main() {
    case "${1:-help}" in
        list)
            print_header
            check_dependencies
            if check_flink_api; then
                list_jobs
            fi
            ;;

        check)
            if [ -z "$2" ]; then
                print_error "Job ID required"
                echo "Usage: $0 check <job-id>"
                exit 1
            fi
            print_header
            check_dependencies
            if check_flink_api; then
                check_job "$2"
            fi
            ;;

        monitor)
            check_dependencies
            if check_flink_api; then
                monitor_jobs
            fi
            ;;

        history)
            print_header
            check_job_history
            ;;

        diagnose)
            if [ -z "$2" ]; then
                print_error "Job ID required"
                echo "Usage: $0 diagnose <job-id>"
                exit 1
            fi
            print_header
            check_dependencies
            if check_flink_api; then
                diagnose_job "$2"
            fi
            ;;

        help|--help|-h)
            show_usage
            ;;

        *)
            print_error "Unknown command: $1"
            echo ""
            show_usage
            exit 1
            ;;
    esac
}

# Run main function
main "$@"

