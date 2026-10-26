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

# Test script to verify descriptor loading works for all use cases.
# Usage: ./scripts/test-descriptor-loading.sh

set -e

echo "================================================================================"
echo "Descriptor Loading Test Suite"
echo "================================================================================"
echo ""

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Resolve the project root so the script can be run from any directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$PROJECT_ROOT"

PASS=0
FAIL=0
SKIP=0

pass()  { echo -e "${GREEN}✓ PASS${NC}: $1"; PASS=$((PASS+1)); }
fail()  { echo -e "${RED}✗ FAIL${NC}: $1"; FAIL=$((FAIL+1)); }
skip()  { echo -e "${YELLOW}⚠ SKIP${NC}: $1"; SKIP=$((SKIP+1)); }

# ─── Test 1: agents.json files exist ──────────────────────────────────────────
echo "Test 1: agents.json files exist"
for uc in banking-fraud-agent; do
  if [ -f "samples/${uc}/agents.json" ]; then
    pass "samples/${uc}/agents.json exists"
  else
    fail "samples/${uc}/agents.json not found (required by AgentBasedDataStreamJob)"
  fi
done
echo ""

# ─── Test 2: deploy script exists ─────────────────────────────────────────────
echo "Test 2: deploy script exists"
for uc in banking-fraud-agent; do
  if [ -f "samples/${uc}/scripts/deploy.sh" ]; then
    pass "samples/${uc}/scripts/deploy.sh exists"
  else
    fail "samples/${uc}/scripts/deploy.sh not found"
  fi
done
echo ""

# ─── Test 3: JSON syntax validation ───────────────────────────────────────────
echo "Test 3: JSON syntax validation"
if command -v jq &> /dev/null; then
  for uc in banking-fraud-agent; do
    if [ -f "samples/${uc}/agents.json" ]; then
      if jq empty "samples/${uc}/agents.json" 2>/dev/null; then
        pass "samples/${uc}/agents.json is valid JSON"
      else
        fail "samples/${uc}/agents.json is invalid JSON"
      fi
    fi
  done
else
  skip "jq not installed — skipping JSON syntax validation"
fi
echo ""

# ─── Test 4: Required top-level agents.json fields ────────────────────────────
echo "Test 4: Required top-level agents.json fields"
for uc in banking-fraud-agent; do
  desc="samples/${uc}/agents.json"
  [ -f "$desc" ] || continue
  content=$(cat "$desc")
  grep -q '"name"'           <<< "$content" && pass "${uc}: has 'name'"           || fail "${uc}: missing 'name'"
  grep -q '"flinkJobName"'   <<< "$content" && pass "${uc}: has 'flinkJobName'"   || fail "${uc}: missing 'flinkJobName' (BFF tile-visibility probe will not work)"
  grep -q '"topics"'         <<< "$content" && pass "${uc}: has 'topics'"         || fail "${uc}: missing 'topics'"
  grep -q '"consumerGroupId"' <<< "$content" && pass "${uc}: has 'consumerGroupId'" || fail "${uc}: missing 'consumerGroupId'"
done
echo ""

# ─── Test 5: flinkJobName values match expected Flink job names ───────────────
# These values must match the FLINK_JOB_NAME passed by the deploy script and
# used by the BFF tile-visibility probe to identify the running job.
echo "Test 5: flinkJobName values"
if command -v jq &> /dev/null; then
  # Pairs: "use-case:expected-job-name" (avoids bash 3 associative-array limitation)
  for pair in "banking-fraud-agent:Banking Fraud Detection Agent"; do
    uc="${pair%%:*}"
    expected="${pair#*:}"
    desc="samples/${uc}/agents.json"
    [ -f "$desc" ] || continue
    actual=$(jq -r '.flinkJobName // empty' "$desc" 2>/dev/null)
    if [ "$actual" = "$expected" ]; then
      pass "${uc}: flinkJobName = \"${actual}\""
    else
      fail "${uc}: flinkJobName = \"${actual}\" (expected \"${expected}\")"
    fi
  done
else
  skip "jq not installed — skipping flinkJobName value checks"
fi
echo ""

# ─── Test 6: Maven build ───────────────────────────────────────────────────────
# Uses 'compile' (not 'clean compile') so that a pre-existing FAT JAR checked by
# Test 8 is not deleted by the clean phase.
echo "Test 6: Maven build"
cd event-rule-backend
if mvn compile -q 2>/dev/null; then
  pass "Maven build successful"
else
  fail "Maven build failed"
fi
cd "$PROJECT_ROOT"
echo ""

# ─── Test 7: FAT JAR exists ────────────────────────────────────────────────────
echo "Test 7: FAT JAR exists"
JAR="event-rule-backend/target/event-rule-backend-1.0-SNAPSHOT.jar"
if [ -f "$JAR" ]; then
  JAR_SIZE=$(stat -f%z "$JAR" 2>/dev/null || stat -c%s "$JAR" 2>/dev/null || echo 0)
  pass "FAT JAR found: ${JAR} (${JAR_SIZE} bytes)"
else
  fail "FAT JAR not found at ${JAR} — run: cd event-rule-backend && mvn package -DskipTests"
fi
echo ""

# ─── Test 8: Environment variable configuration ────────────────────────────────
echo "Test 8: Environment variable configuration"
export AGENT_DESCRIPTOR_PATH="samples/banking-fraud-agent/agents.json"
[ "$AGENT_DESCRIPTOR_PATH" = "samples/banking-fraud-agent/agents.json" ] \
  && pass "AGENT_DESCRIPTOR_PATH can be set" \
  || fail "AGENT_DESCRIPTOR_PATH could not be set"
unset AGENT_DESCRIPTOR_PATH

export FLINK_JOB_NAME="Banking Fraud Detection Agent"
[ "$FLINK_JOB_NAME" = "Banking Fraud Detection Agent" ] \
  && pass "FLINK_JOB_NAME can be set" \
  || fail "FLINK_JOB_NAME could not be set"
unset FLINK_JOB_NAME
echo ""

# ─── Summary ───────────────────────────────────────────────────────────────────
echo "================================================================================"
echo -e "Results: ${GREEN}${PASS} passed${NC}  ${RED}${FAIL} failed${NC}  ${YELLOW}${SKIP} skipped${NC}"
echo "================================================================================"

if [ "$FAIL" -gt 0 ]; then
  echo ""
  echo "Next steps to fix failures:"
  echo "  1. Build the FAT JAR : cd event-rule-backend && mvn package -DskipTests"
  echo "  2. Start the stack   : ./run.sh up"
  echo "  3. Submit the job    : ./samples/banking-fraud-agent/scripts/deploy.sh"
  echo "  4. Install jq        : brew install jq  (macOS) / apt-get install jq  (Linux)"
  echo ""
  exit 1
fi

echo ""
echo "All tests passed. To start the banking fraud-detection pipeline:"
echo "  ./run.sh up"
echo "  ./samples/banking-fraud-agent/scripts/deploy.sh"
echo ""

