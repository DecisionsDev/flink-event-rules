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
# Broadcast a new version of the Decision Archive to Flink
#
# Usage:
#   ./broadcast-reload.sh <jar-version>
#

set -euo pipefail

USECASE="airline-agent"
USECASE_JAR="AirlineDecisionService"

if [ -z "${1}" ]; then
    echo "USAGE: ${0} <jar-version>"
    echo "Example: ${0} 1.0.15"
    exit 1
fi

VERSION="${1}"

../../../scripts/broadcast-reload.sh --use-case ${USECASE} --jar ../${USECASE_JAR}_${VERSION}.jar
