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
# Get Decision Service Archive from DI CMS Runtime
#
# Usage:
#   ./get-dsa.sh <version>
# Example:
#   ./get-dsa.sh 1.0.15
#
# Environment variables:
#   DI_ZEN_HOSTNAME  Base URL of the Zen REST API (e.g. https://cpd-namespace.apps.example.com)
#   DI_ZEN_USERNAME  Zen user name the API key belongs to
#   DI_ZEN_API_KEY   Zen API key; sent as "Authorization: ZenApiKey base64(<username>:<apikey>)"

set -euo pipefail

USECASE="banking-fraud-agent"
DECISION_SERVICE_ARCHIVE="Fraud_Detection_Service"

if [ -z "${1:-}" ]; then
    echo "USAGE: ${0} <version>"
    echo "Example: ${0} 1.0.15"
    exit 1
fi

VERSION="${1}"
shift || true

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

exec "${SCRIPT_DIR}/../../../scripts/get-dsa.sh" \
    --use-case "${USECASE}" \
    --archive "${DECISION_SERVICE_ARCHIVE}" \
    --version "${VERSION}" \
    --output "${SCRIPT_DIR}/.." \
    "$@"
