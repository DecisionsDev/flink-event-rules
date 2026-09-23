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
# submit-events.sh
#
# Reads a JSON array file and publishes each element as a separate Kafka message.
#
# Usage:
#   ./scripts/submit-events.sh <topic> <json-file>
#
# Example:
#   ./scripts/submit-events.sh agent-transactions samples/banking-fraud-agent/demo-scenarios.json

set -e

KAFKA_CONTAINER="kafka"
BOOTSTRAP_SERVER="localhost:9092"

# Require exactly two arguments: topic and JSON file
if [ $# -ne 2 ]; then
    echo "Error: Invalid number of arguments"
    echo "Usage: $0 <topic> <json-file>"
    echo "Example: $0 agent-transactions samples/banking-fraud-agent/demo-scenarios.json"
    exit 1
fi

TOPIC=$1
JSON_FILE=$2

echo "Starting event submission to Kafka..."
echo ""

# Publish each element from the JSON array as a separate Kafka message.
# Requires jq to parse the array.
submit_events() {
    local file=$1
    local topic=$2

    echo "Submitting events from $file to topic: $topic"

    if [ ! -f "$file" ]; then
        echo "Error: File $file not found!"
        return 1
    fi

    # Extract each JSON object from the array and publish it individually
    cat "$file" | jq -c '.[]' | while IFS= read -r event; do
        echo "$event" | docker exec -i $KAFKA_CONTAINER kafka-console-producer \
            --bootstrap-server $BOOTSTRAP_SERVER \
            --topic $topic
        echo "  ✓ Sent: $event"
    done

    echo "✓ Completed submitting events"
    echo ""
}

# Check if jq is installed
if ! command -v jq &> /dev/null; then
    echo "Error: jq is required but not installed."
    echo "Please install jq: brew install jq (macOS) or apt-get install jq (Linux)"
    exit 1
fi

# Check if Kafka container is running
if ! docker ps --format '{{.Names}}' | grep -qx "$KAFKA_CONTAINER"; then
    echo "Error: Kafka container '$KAFKA_CONTAINER' is not running!"
    echo "Please start the stack with: ./run.sh up"
    exit 1
fi

# Submit events
submit_events "$JSON_FILE" "$TOPIC"

echo "=========================================="
echo "All events submitted successfully!"
echo "=========================================="
echo ""
echo "To verify, consume from the topic:"
echo "  docker exec -it $KAFKA_CONTAINER kafka-console-consumer --bootstrap-server $BOOTSTRAP_SERVER --topic $TOPIC --from-beginning --max-messages 5"

