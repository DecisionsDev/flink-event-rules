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

# Get Airline Decision Service Archive from DI CMS Runtime
#
# Usage:
#   ./get-dsa.sh <version>
# Example:
#   ./get-dsa.sh 1.0.15
#
# Environment variables:
#   DI_CMS_URL    Base URL of the DI CMS runtime (default: https://localhost:3000)
#   DI_TOKEN_URL  Base URL of the token service (default: https://localhost:18082)

set -e

if [ -z "${1}" ]; then
    echo "USAGE: ${0} <version>"
    echo "Example: ${0} 1.0.15"
    exit 1
fi

VERSION="${1}"
DI_CMS_URL="${DI_CMS_URL:-https://localhost:3000}"
DI_CMS_URL="${DI_CMS_URL%/}"
DI_TOKEN_URL="${DI_TOKEN_URL:-https://localhost:18082}"
DI_TOKEN_URL="${DI_TOKEN_URL%/}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUTPUT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
OUTPUT_FILE="${OUTPUT_DIR}/AirlineDecisionService_${VERSION}.jar"

# get access token for rtsadmin
# Credentials must be supplied via RTSADMIN_USER / RTSADMIN_PASSWORD environment variables.
if [ -z "${RTSADMIN_USER}" ] || [ -z "${RTSADMIN_PASSWORD}" ]; then
    echo "ERROR: RTSADMIN_USER and RTSADMIN_PASSWORD environment variables must be set"
    exit 1
fi

ACCESS_TOKEN=$(curl -s -k "${DI_TOKEN_URL}/token" \
    -H "Content-Type: application/x-www-form-urlencoded" \
    --data-urlencode "client_id=OIDC_Test" \
    --data-urlencode "client_secret=secret" \
    --data-urlencode "grant_type=password" \
    --data-urlencode "scope=openid" \
    --data-urlencode "username=${RTSADMIN_USER}" \
    --data-urlencode "password=${RTSADMIN_PASSWORD}" | jq -r .access_token)

if [ -z "${ACCESS_TOKEN}" ] || [ "${ACCESS_TOKEN}" = "null" ]; then
    echo "ERROR: Failed to retrieve access token"
    exit 1
fi

echo "Retrieving list of decisions from embedded deployment space..."
DECISIONS_JSON=$(curl -s -k "${DI_CMS_URL}/ads/runtime/api/v1/deploymentSpaces/embedded/decisions" \
    -H 'accept: application/json' \
    -H "x-ads-internal-service-id: 58AEE13AFFCB2AC937C3048DE3578B97B5797414E228D7D0A0C378F2D4255ACE" \
    -H "Authorization: Bearer ${ACCESS_TOKEN}")

DSA_ID=$(echo "${DECISIONS_JSON}" | jq -r --arg ver "${VERSION}" '
  (if type == "array" then .
   elif type == "object" then (.decisionArchiveIds // .decisions // .items // [])
   else [] end)
  | map(if type == "string" then . else (.decisionArchiveId // .decisionServiceArchiveId // .decisionId // .id // "") end)
  | map(select((test("airlinedecisionservice"; "i")) and (split("/")[-1] | test("^" + ($ver | gsub("\\."; "\\.")) + "(-|$)"))))
  | sort | reverse | .[0] // empty
' 2>/dev/null || true)

# Fallback in case response is newline-delimited text instead of JSON
if [ -z "${DSA_ID}" ] && [ -n "${DECISIONS_JSON}" ]; then
    DSA_ID=$(echo "${DECISIONS_JSON}" | grep -i "airlinedecisionservice" | grep -E "/${VERSION//./\\.}(-[^/]+)?$" | sort -r | head -n 1 || true)
fi

if [ -z "${DSA_ID}" ]; then
    echo "ERROR: No decision archive matching 'airlinedecisionservice' with version '${VERSION}' found."
    echo "Available decisions:"
    echo "${DECISIONS_JSON}" | jq -r '
      (if type == "array" then .
       elif type == "object" then (.decisionArchiveIds // .decisions // .items // [])
       else [] end)
      | map(if type == "string" then . else (.decisionArchiveId // .decisionServiceArchiveId // .decisionId // .id // "") end)
      | .[]' 2>/dev/null || echo "${DECISIONS_JSON}"
    exit 1
fi

echo "Found Decision Service Archive ID: ${DSA_ID}"
DSA_ID_ENCODED="${DSA_ID//\//%2F}"

echo "Downloading Decision Service Archive for version ${VERSION}..."
curl -X "GET" -k "${DI_CMS_URL}/ads/runtime/api/v1/deploymentSpaces/embedded/decisions/${DSA_ID_ENCODED}/archive" \
    -H 'accept: */*' \
    -H 'Content-Type: application/octet-stream' \
    -H "x-ads-internal-service-id: 58AEE13AFFCB2AC937C3048DE3578B97B5797414E228D7D0A0C378F2D4255ACE" \
    -H "Authorization: Bearer ${ACCESS_TOKEN}" \
    --output "${OUTPUT_FILE}"

echo "Decision Service Archive saved to ${OUTPUT_FILE}"
