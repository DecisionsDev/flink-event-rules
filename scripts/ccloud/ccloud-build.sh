#!/usr/bin/env bash

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
# ccloud-build.sh
# Builds the Confluent Cloud deployable artifact JAR and SQL program for a use case
# using the Maven 'ccloud' profile.
#
# Usage:
#   ./scripts/ccloud/ccloud-build.sh <use-case> [maven-flags...]
#
# Examples:
#   ./scripts/ccloud/ccloud-build.sh banking-fraud-agent
#   ./scripts/ccloud/ccloud-build.sh samples/retail-agent -DskipTests
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
# shellcheck source=scripts/ccloud/ccloud-common.sh
source "${SCRIPT_DIR}/ccloud-common.sh"

check_required_tools mvn jq

if [[ $# -eq 0 ]]; then
    error "No use case specified."
    echo "Usage: $0 <use-case> [maven-options...]"
    echo "Example: $0 banking-fraud-agent"
    exit 1
fi

USE_CASE_ARG="$1"
shift || true

resolve_use_case "$USE_CASE_ARG"

section "Building Confluent Cloud Artifact — ${USE_CASE_NAME}"
info "Descriptor        : ${DESCRIPTOR_PATH}"
info "Base directory    : ${PROJECT_ROOT}"
info "Output directory  : ${TARGET_CCLOUD_DIR}"
info "Target JAR        : ${JAR_PATH}"
echo ""

mvn -f "${PROJECT_ROOT}/event-rule-backend" package -DskipTests -Pccloud \
    "-Dccloud.descriptor=${DESCRIPTOR_PATH}" \
    "-Dccloud.name=${USE_CASE_NAME}" \
    "-Dccloud.baseDir=${PROJECT_ROOT}" \
    "-Dccloud.classesDir=${PROJECT_ROOT}/event-rule-backend/target/ccloud/classes" \
    "-Dccloud.outDir=${TARGET_CCLOUD_DIR}" \
    "-Dccloud.jar=${USE_CASE_NAME}.jar" \
    "$@"

echo ""
if [[ ! -f "$JAR_PATH" ]]; then
    error "Build failed: expected JAR not found at ${JAR_PATH}"
    exit 1
fi

if [[ ! -f "$MANIFEST_PATH" ]]; then
    error "Build failed: expected manifest not found at ${MANIFEST_PATH}"
    exit 1
fi

JAR_SIZE=$(stat -f%z "$JAR_PATH" 2>/dev/null || stat -c%s "$JAR_PATH" 2>/dev/null || echo 0)
JAR_SIZE_MB=$(awk "BEGIN {printf \"%.2f\", $JAR_SIZE/1024/1024}")

section "Build Summary"
success "Artifact created successfully:"
echo "  JAR file   : ${JAR_PATH} (${JAR_SIZE_MB} MB)"
echo "  Manifest   : ${MANIFEST_PATH}"
echo "  SQL Script : ${TARGET_CCLOUD_DIR}/pipeline.sql"
echo ""

info "Bundled Runtime Files:"
jq -r '.bundled[]? | "  - \(.)"' "$MANIFEST_PATH" || echo "  (none)"
echo ""

info "Generated SQL Statements:"
jq -r '.statements[]? | "  [\(.order)] \(.kind): \(.name) (\(.file))"' "$MANIFEST_PATH"
echo ""
success "Ready for deployment with: ${SCRIPT_DIR}/ccloud-deploy-ptf.sh ${USE_CASE_NAME}"
