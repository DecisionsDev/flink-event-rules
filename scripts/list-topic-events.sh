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
# list-topic-events.sh
#
# Consume and print events from a Kafka topic using kcat (kafkacat).
# Runs directly on the host — no docker exec required.
#
# Install kcat:
#   brew install kcat          (macOS)
#   apt-get install kafkacat   (Debian/Ubuntu)
#
# Usage:
#   ./scripts/list-topic-events.sh <topic>
#
# Options (environment variables):
#   FROM_BEGINNING=false   Tail new messages only (default: true)
#   MAX_MESSAGES=<n>       Exit after n messages (default: unlimited)
#   PRINT_KEY=true         Print message key before each value
#
# Examples:
#   ./scripts/list-topic-events.sh agent-account-events
#   MAX_MESSAGES=10 ./scripts/list-topic-events.sh agent-transactions
#   FROM_BEGINNING=false ./scripts/list-topic-events.sh agent-fraud-alerts
#   PRINT_KEY=true ./scripts/list-topic-events.sh agent-transactions

set -e

BOOTSTRAP_SERVER="localhost:9092"

# --- defaults (override via environment) ---
FROM_BEGINNING="${FROM_BEGINNING:-true}"
MAX_MESSAGES="${MAX_MESSAGES:-}"
PRINT_KEY="${PRINT_KEY:-false}"

# ----------------------------------------

usage() {
    echo "Usage: $0 <topic>"
    echo ""
    echo "Options (environment variables):"
    echo "  FROM_BEGINNING=false   Tail new messages only (default: true)"
    echo "  MAX_MESSAGES=<n>       Exit after n messages"
    echo "  PRINT_KEY=true         Print message key before value"
    exit 1
}

if [ -z "$1" ]; then
    usage
fi

TOPIC="$1"

# Check kcat is installed
if ! command -v kcat &>/dev/null; then
    echo "Error: kcat is not installed." >&2
    echo "" >&2
    echo "Install it with:" >&2
    echo "  brew install kcat          (macOS)" >&2
    echo "  apt-get install kafkacat   (Debian/Ubuntu)" >&2
    exit 1
fi

# Build kcat arguments
# -C = consumer mode
# -b = broker address
# -t = topic
# -e = exit when end of topic is reached (used with -o beginning)
ARGS=(-C -b "$BOOTSTRAP_SERVER" -t "$TOPIC")

if [ "$FROM_BEGINNING" = "true" ]; then
    ARGS+=(-o beginning -e)
fi

if [ -n "$MAX_MESSAGES" ]; then
    ARGS+=(-c "$MAX_MESSAGES")
fi

if [ "$PRINT_KEY" = "true" ]; then
    # -K: print key followed by the separator before the value
    ARGS+=(-K ": ")
fi

echo "Consuming from topic: ${TOPIC}"
echo "Bootstrap server:     ${BOOTSTRAP_SERVER}"
echo "From beginning:       ${FROM_BEGINNING}"
[ -n "$MAX_MESSAGES" ] && echo "Max messages:         ${MAX_MESSAGES}"
echo "---"

kcat "${ARGS[@]}"
