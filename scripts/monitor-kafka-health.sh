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
# monitor-kafka-health.sh
#
# One-shot health report for the Kafka broker (and Zookeeper when present).
# Reports: container running status, resource usage, broker API reachability,
# topic list, and a scan of the last 50 lines of logs for ERRORs and timeouts.
#
# Usage:
#   ./scripts/monitor-kafka-health.sh
#
# To re-run automatically every 30 seconds:
#   watch -n 30 ./scripts/monitor-kafka-health.sh

# Do not exit on error — continue checking all aspects even if some fail
set +e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

KAFKA_CONTAINER="kafka"
ZOOKEEPER_CONTAINER="zookeeper"

print_header() {
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}========================================${NC}"
}

print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

# Check if a container is running and report its health status
check_container_status() {
    local container=$1
    if docker ps --format '{{.Names}}' | grep -q "^${container}$"; then
        local status=$(docker inspect --format='{{.State.Health.Status}}' $container 2>/dev/null || echo "no-healthcheck")
        if [ "$status" = "healthy" ]; then
            print_success "$container is running and healthy"
            return 0
        elif [ "$status" = "no-healthcheck" ]; then
            print_warning "$container is running (no healthcheck configured)"
            return 0
        else
            print_warning "$container is running but status: $status"
            return 1
        fi
    else
        print_error "$container is not running"
        return 1
    fi
}

# Show CPU and memory resource usage for a container
check_resources() {
    local container=$1
    print_header "Resource Usage: $container"

    docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}" $container

    # Warn on elevated memory usage
    local mem_perc=$(docker stats --no-stream --format "{{.MemPerc}}" $container | sed 's/%//')

    if (( $(echo "$mem_perc > 80" | bc -l) )); then
        print_error "High memory usage: ${mem_perc}%"
    elif (( $(echo "$mem_perc > 60" | bc -l) )); then
        print_warning "Moderate memory usage: ${mem_perc}%"
    else
        print_success "Memory usage is healthy: ${mem_perc}%"
    fi
}

# Scan recent Kafka logs for ERROR and timeout messages
check_kafka_logs() {
    print_header "Recent Kafka Issues (last 50 lines)"

    local error_count=$(docker logs $KAFKA_CONTAINER --tail 50 2>&1 | grep -c "ERROR" || echo "0")
    local warn_count=$(docker logs $KAFKA_CONTAINER --tail 50 2>&1 | grep -c "WARN" || echo "0")
    local timeout_count=$(docker logs $KAFKA_CONTAINER --tail 50 2>&1 | grep -c "timeout" || echo "0")

    echo "Errors: $error_count | Warnings: $warn_count | Timeouts: $timeout_count"

    if [ "$error_count" -gt 0 ]; then
        print_error "Found $error_count errors in recent logs"
        echo "Recent errors:"
        docker logs $KAFKA_CONTAINER --tail 50 2>&1 | grep "ERROR" | tail -5
    fi

    if [ "$timeout_count" -gt 0 ]; then
        print_warning "Found $timeout_count timeout messages in recent logs"
        echo "Recent timeouts:"
        docker logs $KAFKA_CONTAINER --tail 50 2>&1 | grep -i "timeout" | tail -3
    fi

    if [ "$error_count" -eq 0 ] && [ "$timeout_count" -eq 0 ]; then
        print_success "No critical issues found in recent logs"
    fi
}

# Verify Kafka broker API is reachable and check for recent Zookeeper session activity
check_zookeeper_connection() {
    print_header "Zookeeper Connection Status"

    if docker exec $KAFKA_CONTAINER kafka-broker-api-versions --bootstrap-server localhost:9092 &>/dev/null; then
        print_success "Kafka broker is responding"
    else
        print_error "Kafka broker is not responding"
        return 1
    fi

    # Surface the most recent Zookeeper session log line if present
    local session_info=$(docker logs $KAFKA_CONTAINER --tail 20 2>&1 | grep -i "session" | tail -1)
    if [ -n "$session_info" ]; then
        echo "Latest session info: $session_info"
    fi
}

# List all Kafka topics
check_topics() {
    print_header "Kafka Topics"

    if docker exec $KAFKA_CONTAINER kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null; then
        print_success "Successfully listed topics"
    else
        print_error "Failed to list topics"
        return 1
    fi
}

# Main monitoring function
main() {
    echo -e "${BLUE}Kafka Health Monitor - $(date)${NC}"
    echo ""

    # Container running status
    print_header "Container Status"
    check_container_status $ZOOKEEPER_CONTAINER
    check_container_status $KAFKA_CONTAINER
    echo ""

    # Resource usage
    check_resources $ZOOKEEPER_CONTAINER
    echo ""
    check_resources $KAFKA_CONTAINER
    echo ""

    # Broker API / Zookeeper connectivity
    check_zookeeper_connection
    echo ""

    # Topic list
    check_topics
    echo ""

    # Log scan
    check_kafka_logs
    echo ""

    print_header "Summary"
    echo "Monitoring complete. Check above for any warnings or errors."
    echo ""
    echo "To continuously monitor, run: watch -n 30 $0"
}

# Run main function
main

