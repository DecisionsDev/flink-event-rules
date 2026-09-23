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
# create-topics.sh
#
# Creates one or more Kafka topics in the running Kafka container.
# Topics are created with 3 partitions and replication-factor 1.
# Safe to re-run: uses --if-not-exists so already-existing topics are skipped.
#
# Usage:
#   ./scripts/create-topics.sh <topic1> [topic2] ...
#
# Example:
#   ./scripts/create-topics.sh agent-transactions agent-fraud-alerts agent-errors

set -e

KAFKA_CONTAINER="kafka"
BOOTSTRAP_SERVER="localhost:9092"
PARTITIONS=3
REPLICATION_FACTOR=1

# Require at least one topic argument
if [ $# -eq 0 ]; then
    echo "Error: No topics specified"
    echo "Usage: $0 <topic1> [topic2] [topic3] ..."
    echo "Example: $0 agent-transactions agent-fraud-alerts agent-errors"
    exit 1
fi

echo "Creating Kafka topics..."
echo ""

# Create each topic provided as an argument
for topic in "$@"; do
    echo "Creating topic: $topic"
    docker exec $KAFKA_CONTAINER kafka-topics \
      --create \
      --bootstrap-server $BOOTSTRAP_SERVER \
      --topic "$topic" \
      --partitions $PARTITIONS \
      --replication-factor $REPLICATION_FACTOR \
      --if-not-exists
    echo ""
done

echo "Listing all topics:"
docker exec $KAFKA_CONTAINER kafka-topics \
  --list \
  --bootstrap-server $BOOTSTRAP_SERVER

echo ""
echo "Topics created successfully!"

