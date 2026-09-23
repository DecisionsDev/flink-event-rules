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

# build.sh - Builds all components of the event-rule application and runs all test suites.
#
# Builds and tests in order:
#   1. Backend  — Maven tests + package (produces the FAT JAR used by Flink)
#   2. Frontend — npm install, npm test (Vitest), npm run build (React app)
#   3. BFF      — npm install, npm test (Jest unit tests)
#   4. Docker images — docker compose build
#
# Usage: ./build.sh [--force-rebuild]
#   --force-rebuild   Pass --no-cache to docker compose build (full layer rebuild)

set -e  # Exit on error
set -o pipefail  # Propagate failure through pipes

FORCE_REBUILD=false
for arg in "$@"; do
    case "$arg" in
        --force-rebuild) FORCE_REBUILD=true ;;
    esac
done

echo "=========================================="
echo "Building Event Rule Application"
echo "=========================================="
echo ""

# Resolve the project root (directory containing this script)
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ── 1. Backend ────────────────────────────────────────────────────────────────
echo "=========================================="
echo "Building & Testing Backend (Apache Flink)"
echo "=========================================="
cd "$PROJECT_ROOT/event-rule-backend"

# Suppress JVM unsafe memory access warnings (e.g. Java 23+)
export MAVEN_OPTS="${MAVEN_OPTS:-} --sun-misc-unsafe-memory-access=allow"

MVN_EXTRA_FLAGS=""
if [ "${DI_SKIP_TLS_VERIFY:-false}" = "true" ]; then
    MVN_EXTRA_FLAGS="-Dmaven.wagon.http.ssl.insecure=true -Dmaven.wagon.http.ssl.allowall=true"
fi

# Run Maven clean package (which compiles, runs all unit/regression tests, and packages the fat JAR)
# shellcheck disable=SC2086
mvn clean package \
    --settings "$PROJECT_ROOT/settings.xml" \
    -P maven-designer \
    -DskipTests=false \
    $MVN_EXTRA_FLAGS

echo "✓ Backend build and tests complete"
echo ""

# ── 2. Frontend ───────────────────────────────────────────────────────────────
echo "=========================================="
echo "Building & Testing Frontend (React)"
echo "=========================================="
cd "$PROJECT_ROOT/event-rule-frontend"
npm install
npm test
npm run build
echo "✓ Frontend build and tests complete"
echo ""

# ── 3. BFF ────────────────────────────────────────────────────────────────────
echo "=========================================="
echo "Building & Testing BFF (Backend-for-Frontend)"
echo "=========================================="
cd "$PROJECT_ROOT/event-rule-frontend/bff"
npm install
npm test
echo "✓ BFF build and tests complete"
echo ""

# Return to project root before building Docker images
cd "$PROJECT_ROOT"

# ── 4. Docker images ──────────────────────────────────────────────────────────
echo "=========================================="
echo "Building Docker Images"
echo "=========================================="
echo ""
echo "Detecting platform architecture..."
ARCH=$(uname -m)
if [ "$ARCH" = "arm64" ] || [ "$ARCH" = "aarch64" ]; then
    PLATFORM="linux/arm64"
    echo "✓ Detected ARM64 architecture"
elif [ "$ARCH" = "x86_64" ]; then
    PLATFORM="linux/amd64"
    echo "✓ Detected AMD64 architecture"
else
    PLATFORM="linux/amd64"
    echo "⚠ Unknown architecture ($ARCH), defaulting to linux/amd64"
fi
echo ""

# Use the Docker Compose plugin (v2); fall back to standalone docker-compose (v1)
if docker compose version &> /dev/null; then
    COMPOSE_CMD="docker compose"
else
    COMPOSE_CMD="docker-compose"
fi

DOCKER_BUILD_FLAGS=""
if [ "$FORCE_REBUILD" = "true" ]; then
    DOCKER_BUILD_FLAGS="--no-cache"
    echo "Force rebuild: --no-cache enabled"
fi

echo "Building Docker images for platform: $PLATFORM"
# shellcheck disable=SC2086
DOCKER_DEFAULT_PLATFORM=$PLATFORM $COMPOSE_CMD build $DOCKER_BUILD_FLAGS
echo "✓ Docker images built successfully"
echo ""

echo "=========================================="
echo "Build Complete!"
echo "=========================================="
echo ""
echo "All components and tests have passed successfully:"
echo "  ✓ Backend tests & JAR: event-rule-backend/target/event-rule-backend-1.0-SNAPSHOT.jar"
echo "  ✓ Frontend tests & build: event-rule-frontend/build/"
echo "  ✓ BFF tests & dependencies: event-rule-frontend/bff/node_modules/"
echo "  ✓ Docker images: event-rule-backend, event-rule-bff, event-rule-frontend"
echo "  ✓ Platform: $PLATFORM"
echo ""
echo "Next steps:"
echo "  ./run.sh up                                        # Start all services"
echo "  ./samples/banking-fraud-agent/scripts/deploy.sh   # Submit the banking fraud-detection job"
echo ""
