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
# launch-demo-scenario-ptf.sh
#
# PTF-runtime variant of launch-demo-scenario.sh.
#
# Reads samples/<use-case>/demo-scenarios.json and publishes the events from one
# or all named scenarios to their configured Kafka topics.
#
# Instead of publishing __ENTITY_RESET__ sentinels, the Flink job (Process Table
# Function runtime, AgentBasedPTFJob) is restarted from scratch before EVERY scenario:
#
#   1. Cancel the job if it is running (no savepoint is taken).
#   2. Move the job's Kafka consumer-group offsets to the end of the topics, so the
#      fresh job does not replay events from a previous scenario.
#   3. Submit the job again with samples/<use-case>/scripts/deploy-ptf.sh
#      (no savepoint restore => empty state).
#   4. Wait for the job to be RUNNING, then publish the scenario events.
#
# Topic resolution order per event:
#   1. Explicit "topic" field on the event object (highest priority)
#   2. Matching entry in samples/<use-case>/agents.json .topics[]
#   3. Matching entry in samples/<use-case>/agents.json .agents[].subscriptions[].topics[0]
#
# Usage:
#   ./scripts/launch-demo-scenario-ptf.sh <use-case> [scenario-id]
#
# Examples:
#   ./scripts/launch-demo-scenario-ptf.sh banking-fraud-agent                  # all scenarios, restart before each
#   ./scripts/launch-demo-scenario-ptf.sh banking-fraud-agent account-creation # one scenario
#
# Environment variables (all optional):
#   KAFKA_CONTAINER       (default: kafka)
#   BOOTSTRAP_SERVER      (default: localhost:9092, as seen from inside the Kafka container)
#   FLINK_JOBMANAGER_URL  (default: http://localhost:8081)
#   FLINK_JOB_NAME        (default: "<agents.json .name> (PTF)")
#   STARTUP_WAIT          (default: 5)   seconds to wait after the job is RUNNING
#   JOB_TIMEOUT           (default: 120) seconds to wait for cancel / running

set -e

# Always run from the project root so relative paths resolve
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

# Default configurations (overridable via environment variables)
KAFKA_CONTAINER="${KAFKA_CONTAINER:-kafka}"
BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-localhost:9092}"
JOBMANAGER="${FLINK_JOBMANAGER_URL:-http://localhost:8081}"
STARTUP_WAIT="${STARTUP_WAIT:-5}"
JOB_TIMEOUT="${JOB_TIMEOUT:-120}"

# Check required tools
for tool in jq curl docker; do
    if ! command -v "$tool" &> /dev/null; then
        echo "Error: $tool is required but not installed."
        exit 1
    fi
done

# Parse arguments
USE_CASE_DIR=$1
SCENARIO_ID=""

shift || true
for arg in "$@"; do
    SCENARIO_ID="$arg"
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
DESCRIPTOR_FILE="$USE_CASE_DIR/agents.json"
DEPLOY_SCRIPT="$USE_CASE_DIR/scripts/deploy-ptf.sh"

if [ ! -f "$SCENARIOS_FILE" ]; then
    echo "Error: Scenarios file '$SCENARIOS_FILE' not found."
    exit 1
fi

if [ ! -f "$AGENTS_FILE" ]; then
    echo "Error: Agents descriptor '$AGENTS_FILE' not found."
    exit 1
fi

if [ ! -x "$DEPLOY_SCRIPT" ]; then
    echo "Error: PTF deploy script '$DEPLOY_SCRIPT' not found or not executable."
    exit 1
fi

# Job name used to find the running job; also passed to deploy-ptf.sh so both agree.
if [ -z "$FLINK_JOB_NAME" ]; then
    FLINK_JOB_NAME="$(jq -r '.name // empty' "$AGENTS_FILE") (PTF)"
fi
export FLINK_JOB_NAME

# Check if Kafka container is running
if ! docker ps --format '{{.Names}}' | grep -q "^${KAFKA_CONTAINER}$"; then
    echo "Error: Kafka container '$KAFKA_CONTAINER' is not running!"
    echo "Please start the stack with: ./run.sh up"
    exit 1
fi

# Print the IDs of all non-terminal Flink jobs with the configured job name.
active_job_ids() {
    curl -sf "$JOBMANAGER/jobs/overview" 2>/dev/null | jq -r --arg name "$FLINK_JOB_NAME" '
        .jobs[]?
        | select(.name == $name)
        | select(.state | IN("CREATED","INITIALIZING","RUNNING","RESTARTING","RECONCILING","CANCELLING","FAILING","SUSPENDED"))
        | .jid
    ' 2>/dev/null || true
}

# Print the IDs of all RUNNING Flink jobs with the configured job name.
running_job_ids() {
    curl -sf "$JOBMANAGER/jobs/overview" 2>/dev/null | jq -r --arg name "$FLINK_JOB_NAME" '
        .jobs[]? | select(.name == $name and .state == "RUNNING") | .jid
    ' 2>/dev/null || true
}

# Cancel the job if it is running. No savepoint is taken.
cancel_job() {
    local ids
    ids=$(active_job_ids)
    if [ -z "$ids" ]; then
        echo "  No active job named '$FLINK_JOB_NAME' — nothing to cancel."
        return 0
    fi

    local jid
    for jid in $ids; do
        echo "  Cancelling job $jid (no savepoint)..."
        curl -sf -X PATCH "$JOBMANAGER/jobs/$jid?mode=cancel" > /dev/null || true
    done

    local waited=0
    while [ -n "$(active_job_ids)" ]; do
        if [ "$waited" -ge "$JOB_TIMEOUT" ]; then
            echo "❌ Error: job '$FLINK_JOB_NAME' did not terminate within ${JOB_TIMEOUT}s."
            exit 1
        fi
        sleep 2
        waited=$((waited + 2))
    done
    echo "  ✓ Job cancelled."
}

# Move the consumer-group offsets of every agent to the end of its topics so the
# restarted job does not re-read events of the previous scenario.
# The group id convention is "agent-<agentId>" (see AgentSqlPipelineBuilder).
reset_consumer_offsets() {
    local agent_id
    for agent_id in $(jq -r '.agents[]?.agentId // empty' "$AGENTS_FILE"); do
        docker exec "$KAFKA_CONTAINER" kafka-consumer-groups \
            --bootstrap-server "$BOOTSTRAP_SERVER" \
            --group "agent-$agent_id" \
            --reset-offsets --to-latest --all-topics --execute > /dev/null 2>&1 \
            && echo "  ✓ Consumer group agent-$agent_id moved to latest offsets." \
            || echo "  ⚠️  Could not reset consumer group agent-$agent_id (may not exist yet)."
    done
}

# Wait until the job is RUNNING.
wait_for_job_running() {
    local waited=0
    while [ -z "$(running_job_ids)" ]; do
        if [ "$waited" -ge "$JOB_TIMEOUT" ]; then
            echo "❌ Error: job '$FLINK_JOB_NAME' not RUNNING after ${JOB_TIMEOUT}s."
            exit 1
        fi
        sleep 2
        waited=$((waited + 2))
    done
    echo "  ✓ Job is RUNNING."
}

# Restart the PTF job from scratch (cancel if running, then submit without a savepoint).
restart_job() {
    echo "♻️  Restarting PTF job '$FLINK_JOB_NAME'..."
    cancel_job
    reset_consumer_offsets
    echo "  Submitting job with $DEPLOY_SCRIPT ..."
    "$DEPLOY_SCRIPT"
    wait_for_job_running
    echo "  Waiting ${STARTUP_WAIT}s for the Kafka sources to initialise..."
    sleep "$STARTUP_WAIT"
    echo ""
}

# Resolve the Kafka topic for a given event type.
# Checks the top-level topics of agents.json first, then the agent subscriptions as a fallback.
resolve_topic() {
    local event_type=$1
    local topic=""

    # 1. Attempt to resolve topic from the top-level topics of agents.json first
    if [ -f "$DESCRIPTOR_FILE" ]; then
        topic=$(jq -r --arg type "$event_type" '.topics[]? | select(.type == $type) | .name' "$DESCRIPTOR_FILE" 2>/dev/null)
    fi

    # 2. Fallback to resolving topic from agents.json
    if [ -z "$topic" ] && [ -f "$AGENTS_FILE" ]; then
        topic=$(jq -r --arg type "$event_type" '.agents[]?.subscriptions[]? | select(.eventType == $type) | .topic' "$AGENTS_FILE" 2>/dev/null | head -1)
    fi

    echo "$topic"
}

# Launch a single scenario by its ID: restarts the job, then publishes all events in declaration order.
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

    # Fresh job (and therefore fresh state) for every scenario
    restart_job

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
