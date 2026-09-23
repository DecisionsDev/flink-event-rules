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

# run-datastream.sh - Runs AgentBasedDataStreamJob locally with Maven dependencies on the classpath.
#
# This script is intended for local development and testing outside of Docker/Flink.
# For the production Docker deployment, use:
#   ./samples/banking-fraud-agent/scripts/deploy.sh
#
# Usage: ./run-datastream.sh [bootstrap-servers] [agents-json-path]
#
# Defaults:
#   bootstrap-servers  : localhost:9092
#   agents-json-path   : ../samples/banking-fraud-agent/agents.json
#
# Example:
#   ./run-datastream.sh localhost:9092 ../samples/banking-fraud-agent/agents.json

set -e

# Resolve script and project root directories
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Change to event-rule-backend directory so Maven resolves its POM
cd "$SCRIPT_DIR"

echo "Building Maven classpath..."

# Build the classpath from all Maven dependencies.
# The 'dev' profile includes Flink (normally 'provided') in compile scope
# so the job can run without a Flink cluster on the host machine.
MAVEN_CLASSPATH=$(mvn -Pdev dependency:build-classpath -DincludeScope=compile -q -Dmdep.outputFile=/dev/stdout)

if [ -z "$MAVEN_CLASSPATH" ]; then
    echo "Error: Failed to build Maven classpath"
    exit 1
fi

# Add compiled classes and test classes
CLASSPATH="target/classes:target/test-classes:$MAVEN_CLASSPATH"

# Add the project root so descriptor files can be resolved relative to it
CLASSPATH="$CLASSPATH:$PROJECT_ROOT"

echo "Classpath configured with Maven dependencies"

# Default arguments (overridable via positional parameters)
BOOTSTRAP_SERVERS="${1:-localhost:9092}"
AGENTS_JSON_PATH="${2:-../samples/banking-fraud-agent/agents.json}"

echo "Starting AgentBasedDataStreamJob..."
echo "Bootstrap Servers: $BOOTSTRAP_SERVERS"
echo "Agents JSON Path:  $AGENTS_JSON_PATH"
echo ""

# Launch the Flink job in local (mini-cluster) mode
java -cp "$CLASSPATH" \
    -Dlog4j.configurationFile=src/main/resources/log4j2.properties \
    com.ibm.di.event.agent.AgentBasedDataStreamJob \
    "$BOOTSTRAP_SERVERS" \
    "$AGENTS_JSON_PATH"

