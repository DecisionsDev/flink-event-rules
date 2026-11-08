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
# Build decision service archive from the decision service project
#
# Usage:
#   ./build-decision-service.sh <jar-version>
#

set -euo pipefail

USECASE="airline-agent"
USECASE_PROJECT="AirTrafficControlService"

if [ -z "${1}" ]; then
    echo "USAGE: ${0} <jar-version>"
    echo "Example: ${0} 1.0.15"
    exit 1
fi

VERSION="${1}"

../../../scripts/build-decision-service.sh --project ../di-project/${USECASE_PROJECT}.zip --api-key ${DI_ZEN_API_KEY} --di-url ${DI_ZEN_HOSTNAME}/ads --username ${DI_ZEN_USERNAME} --output .. --version ${VERSION}