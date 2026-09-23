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
# launch-demo-scenario.sh
#
# Reads samples/<use-case>/demo-scenarios.json and publishes the events from one
# or all named scenarios to their configured Kafka topics.
#
# After each scenario completes, a __ENTITY_RESET__ sentinel is published for every
# entity (account) touched by that scenario.  The sentinel travels through the same
# event topic as normal events, is routed by Flink's keyBy to the exact entity
# partition, and clears only that entity's state and event history — leaving every
# other entity completely untouched.
#
# The entity key field and the first subscribed event topic are read from
# agents.json (.agents[].entityKeyExtractor and .agents[].subscriptions[].topic).
#
# Pass --no-reset to skip the per-entity reset step.
#
# Topic resolution order per event:
#   1. Explicit "topic" field on the event object (highest priority)
#   2. Matching entry in samples/<use-case>/descriptor.json .topics[]
#   3. Matching entry in samples/<use-case>/agents.json .agents[].subscriptions[].topics[0]
#
# Usage:
#   ./scripts/launch-demo-scenario.sh <use-case> [scenario-id] [--no-reset]
#
# Examples:
#   ./scripts/launch-demo-scenario.sh banking-fraud-agent                          # all scenarios, reset after each
#   ./scripts/launch-demo-scenario.sh banking-fraud-agent account-creation         # one scenario, reset after
#   ./scripts/launch-demo-scenario.sh banking-fraud-agent account-creation --no-reset  # skip reset

set -e

# Default configurations (overridable via environment variables)
KAFKA_CONTAINER="${KAFKA_CONTAINER:-kafka}"
BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-localhost:9092}"

# Check if jq is installed
if ! command -v jq &> /dev/null; then
    echo "Error: jq is required but not installed."
    echo "Please install jq: brew install jq (macOS) or apt-get install jq (Linux)"
    exit 1
fi

# Parse arguments
USE_CASE_DIR=$1
SCENARIO_ID=""
RESET_STATE=true

shift || true
for arg in "$@"; do
    case "$arg" in
        --no-reset) RESET_STATE=false ;;
        *)          SCENARIO_ID="$arg" ;;
    esac
done

if [ -z "$USE_CASE_DIR" ]; then
    echo "Error: Use case directory/name not specified."
    echo "Usage: $0 <use-case> [scenario-id]"
    echo "Example: $0 banking-fraud-agent account-creation"
    echo "Example (all scenarios): $0 banking-fraud-agent"
    exit 1
fi

# Resolve use case directory: accept both a direct path and a name under samples/
if [ ! -d "$USE_CASE_DIR" ] && [ -d "samples/$USE_CASE_DIR" ]; then
    USE_CASE_DIR="samples/$USE_CASE_DIR"
fi

if [ ! -d "$USE_CASE_DIR" ]; then
    echo "Error: Use-case directory '$USE_CASE_DIR' does not exist."
    exit 1
fi

# Define paths to metadata and scenario files
SCENARIOS_FILE="$USE_CASE_DIR/demo-scenarios.json"
AGENTS_FILE="$USE_CASE_DIR/agents.json"
DESCRIPTOR_FILE="$USE_CASE_DIR/descriptor.json"

if [ ! -f "$SCENARIOS_FILE" ]; then
    echo "Error: Scenarios file '$SCENARIOS_FILE' not found."
    exit 1
fi

# Check if Kafka container is running
if ! docker ps --format '{{.Names}}' | grep -q "^${KAFKA_CONTAINER}$"; then
    echo "Error: Kafka container '$KAFKA_CONTAINER' is not running!"
    echo "Please start the stack with: ./run.sh up"
    exit 1
fi

# Resolve the entity key field name from agents.json (strips leading '$.' from extractor).
# e.g. "$.accountNumber" → "accountNumber"
resolve_entity_key_field() {
    if [ ! -f "$AGENTS_FILE" ]; then
        echo ""
        return
    fi
    local extractor
    extractor=$(jq -r '.agents[0].entityKeyExtractor // empty' "$AGENTS_FILE" 2>/dev/null)
    if [ -z "$extractor" ]; then
        echo ""
        return
    fi
    # Strip leading "$." if present so we get a plain field name
    echo "${extractor#\$.}"
}

# Resolve the Kafka topic for the reset sentinel for a given scenario.
#
# The sentinel MUST travel on the same topic as the scenario's first event so
# that Flink's per-partition ordering guarantees the sentinel is processed before
# that event.  Resolution order (most- to least-specific):
#   1. Explicit "topic" field on the first event in the scenario
#   2. agents.json subscription whose eventType matches the first event's type
#   3. agents.json subscriptions[0].topic  (fallback)
resolve_sentinel_topic() {
    local scenario_id=$1

    if [ ! -f "$AGENTS_FILE" ] || [ ! -f "$SCENARIOS_FILE" ]; then
        echo ""
        return
    fi

    # Try the explicit topic field on the first event
    local explicit_topic
    explicit_topic=$(jq -r --arg id "$scenario_id" '
        .[] | select(.id == $id) | .events[0].topic // empty
    ' "$SCENARIOS_FILE" 2>/dev/null)
    if [ -n "$explicit_topic" ]; then
        echo "$explicit_topic"
        return
    fi

    # Match by the first event's type against agents.json subscriptions
    local first_event_type
    first_event_type=$(jq -r --arg id "$scenario_id" '
        .[] | select(.id == $id) | .events[0].type // empty
    ' "$SCENARIOS_FILE" 2>/dev/null)
    if [ -n "$first_event_type" ]; then
        local matched_topic
        matched_topic=$(jq -r --arg type "$first_event_type" '
            .agents[0].subscriptions[] | select(.eventType == $type) | .topic // empty
        ' "$AGENTS_FILE" 2>/dev/null | head -1)
        if [ -n "$matched_topic" ]; then
            echo "$matched_topic"
            return
        fi
    fi

    # Fallback: first subscribed topic
    jq -r '.agents[0].subscriptions[0].topic // empty' "$AGENTS_FILE" 2>/dev/null
}

# Extract all distinct values of the entity key field from a scenario's event payloads.
# Prints one entity ID per line.
extract_entity_ids() {
    local scenario_id=$1
    local key_field=$2
    jq -r --arg id "$scenario_id" --arg key "$key_field" '
        .[] | select(.id == $id) | .events[].payload[$key] // empty
    ' "$SCENARIOS_FILE" 2>/dev/null | sort -u
}

# Publish a __ENTITY_RESET__ sentinel for a single entity.
# The sentinel travels through the normal event topic so Flink's keyBy routes it
# to exactly that entity's keyed partition — clearing only that entity's state
# and event history, leaving every other entity untouched.
reset_entity() {
    local entity_id=$1
    local key_field=$2
    local topic=$3

    local payload
    payload=$(printf '{"__eventType":"__ENTITY_RESET__","%s":"%s"}' "$key_field" "$entity_id")

    echo "$payload" | docker exec -i "$KAFKA_CONTAINER" \
        kafka-console-producer \
            --bootstrap-server "$BOOTSTRAP_SERVER" \
            --topic "$topic"
    echo "  ✓ Reset sentinel sent for entity $key_field=$entity_id"
}

# Reset every entity touched by a scenario by publishing __ENTITY_RESET__ sentinels.
# Skips gracefully if agents.json is missing or incomplete.
reset_scenario_entities() {
    local scenario_id=$1

    local key_field
    key_field=$(resolve_entity_key_field)
    if [ -z "$key_field" ]; then
        echo "⚠️  Could not resolve entityKeyExtractor from $AGENTS_FILE — skipping entity reset."
        echo ""
        return 0
    fi

    local event_topic
    event_topic=$(resolve_sentinel_topic "$scenario_id")
    if [ -z "$event_topic" ]; then
        echo "⚠️  Could not resolve a sentinel topic for scenario '$scenario_id' — skipping entity reset."
        echo ""
        return 0
    fi

    local entity_ids
    entity_ids=$(extract_entity_ids "$scenario_id" "$key_field")
    if [ -z "$entity_ids" ]; then
        echo "⚠️  No entity IDs found for scenario '$scenario_id' — skipping entity reset."
        echo ""
        return 0
    fi

    echo "🔄 Resetting entity state for scenario '$scenario_id' (topic=$event_topic)..."
    while IFS= read -r entity_id; do
        reset_entity "$entity_id" "$key_field" "$event_topic"
    done <<< "$entity_ids"
    echo "  ✓ Entity reset complete."
    echo ""
}

# Resolve the Kafka topic for a given event type.
# Checks descriptor.json first, then agents.json as a fallback.
resolve_topic() {
    local event_type=$1
    local topic=""

    # 1. Attempt to resolve topic from descriptor.json first
    if [ -f "$DESCRIPTOR_FILE" ]; then
        topic=$(jq -r --arg type "$event_type" '.topics[]? | select(.type == $type) | .name' "$DESCRIPTOR_FILE" 2>/dev/null)
    fi

    # 2. Fallback to resolving topic from agents.json
    if [ -z "$topic" ] && [ -f "$AGENTS_FILE" ]; then
        topic=$(jq -r --arg type "$event_type" '.agents[]?.subscriptions[]? | select(.eventType == $type) | .topics[0]' "$AGENTS_FILE" 2>/dev/null)
    fi

    echo "$topic"
}

# Launch a single scenario by its ID: publishes all events in declaration order.
launch_scenario() {
    local id=$1
    local label=$(jq -r --arg id "$id" '.[] | select(.id == $id) | .label' "$SCENARIOS_FILE")
    local desc=$(jq -r --arg id "$id" '.[] | select(.id == $id) | .description' "$SCENARIOS_FILE")

    echo "========================================================================="
    echo "🚀 Launching Scenario: $label ($id)"
    echo "Description: $desc"
    echo "========================================================================="
    echo ""

    # Skip scenarios with no events
    local has_events=$(jq --arg id "$id" '.[] | select(.id == $id) | .events? | length' "$SCENARIOS_FILE" 2>/dev/null || echo "0")
    if [ "$has_events" = "0" ] || [ -z "$has_events" ]; then
        echo "No events found in scenario '$id'."
        echo ""
        return 0
    fi

    # Iterate events in order; each line from jq is one compact JSON object
    jq -c -r --arg id "$id" '.[] | select(.id == $id) | .events[]' "$SCENARIOS_FILE" | while IFS= read -r event_json; do
        if [ -z "$event_json" ]; then
            continue
        fi

        # Extract type, topic, and payload from the event object
        local event_type=$(echo "$event_json" | jq -r '.type // empty')
        local event_topic=$(echo "$event_json" | jq -r '.topic // empty')
        local event_payload=$(echo "$event_json" | jq -c '.payload // empty')

        # If topic is not explicitly defined in the scenario event, resolve it from metadata files
        if [ -z "$event_topic" ]; then
            if [ -n "$event_type" ]; then
                event_topic=$(resolve_topic "$event_type")
            fi
        fi

        if [ -z "$event_topic" ]; then
            echo "❌ Error: Could not resolve Kafka topic for event type '$event_type'"
            echo "  Event skipped: $event_payload"
            echo ""
            continue
        fi

        echo "Sending '$event_type' to topic '$event_topic'..."

        # Publish the event payload to Kafka
        echo "$event_payload" | docker exec -i "$KAFKA_CONTAINER" kafka-console-producer \
            --bootstrap-server "$BOOTSTRAP_SERVER" \
            --topic "$event_topic"

        echo "  ✓ Sent: $event_payload"
        echo ""

        # Wait between events so the Flink pipeline can process each one
        # before the next event arrives on any topic.
        sleep 2
    done

    echo "🎉 Scenario '$id' completed successfully!"
    echo "========================================================================="
    echo ""

    if [ "$RESET_STATE" = "true" ]; then
        reset_scenario_entities "$id"
    fi
}

# Determine whether to run a single named scenario or all scenarios
if [ -n "$SCENARIO_ID" ] && [ "$SCENARIO_ID" != "all" ]; then
    # Verify the scenario exists before attempting to run it
    EXISTS=$(jq -r --arg id "$SCENARIO_ID" '.[] | select(.id == $id) | .id' "$SCENARIOS_FILE")
    if [ -z "$EXISTS" ]; then
        echo "Error: Scenario '$SCENARIO_ID' not found in $SCENARIOS_FILE."
        echo "Available scenarios:"
        jq -r '.[] | "  - \(.id) (\(.label))"' "$SCENARIOS_FILE"
        exit 1
    fi
    launch_scenario "$SCENARIO_ID"
else
    # Execute all scenarios in the order they appear in the file
    SCENARIOS=$(jq -r '.[] | .id' "$SCENARIOS_FILE")
    if [ -z "$SCENARIOS" ]; then
        echo "No scenarios found in $SCENARIOS_FILE."
        exit 0
    fi

    echo "Starting execution of all demo scenarios from $SCENARIOS_FILE..."
    echo ""
    for id in $SCENARIOS; do
        launch_scenario "$id"
    done
    echo "All demo scenarios executed successfully!"
fi

