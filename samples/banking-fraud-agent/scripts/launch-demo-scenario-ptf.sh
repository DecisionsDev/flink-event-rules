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
# launch-demo-scenario-ptf.sh
#
# Convenience wrapper around <PROJECT_ROOT>/scripts/launch-demo-scenario-ptf.sh
# scoped to the banking-fraud-agent use case.
#
# The PTF job is cancelled (no savepoint) and resubmitted before each scenario.
#
# Usage:
#   ./scripts/launch-demo-scenario-ptf.sh [scenario-id]
#
# Examples:
#   ./scripts/launch-demo-scenario-ptf.sh                   # all scenarios, job restarted before each
#   ./scripts/launch-demo-scenario-ptf.sh account-creation  # one scenario

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

cd "${PROJECT_ROOT}"
exec "${PROJECT_ROOT}/scripts/launch-demo-scenario-ptf.sh" banking-fraud-agent "$@"
