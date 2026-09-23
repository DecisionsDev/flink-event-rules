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
# Makefile — top-level convenience targets for the Event Rule project.

.PHONY: functional-tests functional-test-aircraft functional-test-banking \
        functional-test-skip-compose functional-test-clean help

# ── Variables ─────────────────────────────────────────────────────────────────

SHELL := /bin/bash

# BFF URL override (e.g. make functional-tests BFF_URL=http://localhost:8080)
BFF_URL ?= http://127.0.0.1:8080

# Seconds to wait for containers to become healthy
CONTAINER_HEALTHY_TIMEOUT ?= 180

# Seconds to wait for the Flink job to be RUNNING
FLINK_JOB_TIMEOUT ?= 180

# Seconds to wait for an output event (alert/tracked) to appear on Kafka
ALERT_WAIT_TIMEOUT ?= 90

# Seconds to wait for a TrackedAircraftEvent on the aircraft topic
TRACKED_WAIT_TIMEOUT ?= 60

# Set to true to skip docker compose up/down (stack already running)
SKIP_COMPOSE ?= false

# Set to true to skip Flink job deployment (job already running)
SKIP_FLINK_DEPLOY ?= false

# ── Common env passthrough ────────────────────────────────────────────────────

_ENV = BFF_URL=$(BFF_URL) \
       CONTAINER_HEALTHY_TIMEOUT=$(CONTAINER_HEALTHY_TIMEOUT) \
       FLINK_JOB_TIMEOUT=$(FLINK_JOB_TIMEOUT) \
       ALERT_WAIT_TIMEOUT=$(ALERT_WAIT_TIMEOUT) \
       TRACKED_WAIT_TIMEOUT=$(TRACKED_WAIT_TIMEOUT) \
       SKIP_COMPOSE=$(SKIP_COMPOSE) \
       SKIP_FLINK_DEPLOY=$(SKIP_FLINK_DEPLOY)

# ── Targets ───────────────────────────────────────────────────────────────────

## Run ALL use-case functional tests (banking + aircraft).
## Starts Docker Compose, deploys Flink jobs, runs tests, tears down.
## Prerequisites: docker, curl, jq, and a pre-built fat JAR (./build.sh).
## Uses COMPOSE_CLEAN_VOLUMES=true to ensure a clean Kafka state on each run.
functional-tests:
	@COMPOSE_CLEAN_VOLUMES=true $(_ENV) bash functional-tests/run-functional-tests.sh all

## Run only the Air Traffic Control (aircraft) functional tests.
## Starts Docker Compose, deploys the ATC Flink job, runs tests, tears down.
## Prerequisites: docker, curl, jq, and a pre-built fat JAR (./build.sh).
## Uses COMPOSE_CLEAN_VOLUMES=true to ensure a clean Kafka state on each run.
functional-test-aircraft:
	@COMPOSE_CLEAN_VOLUMES=true $(_ENV) bash functional-tests/run-functional-tests.sh aircraft

## Run only the Banking Fraud Detection functional tests.
## Starts Docker Compose, deploys the banking Flink job, runs tests, tears down.
## Prerequisites: docker, curl, jq, and a pre-built fat JAR (./build.sh).
## Uses COMPOSE_CLEAN_VOLUMES=true to ensure a clean Kafka state on each run.
functional-test-banking:
	@COMPOSE_CLEAN_VOLUMES=true $(_ENV) bash functional-tests/run-functional-tests.sh banking

## Run ALL functional tests against an already-running stack (skip Compose + deploy).
## Useful for rapid re-runs during development when the stack is already up.
functional-test-skip-compose:
	@BFF_URL=$(BFF_URL) \
	SKIP_COMPOSE=true \
	SKIP_FLINK_DEPLOY=true \
	CONTAINER_HEALTHY_TIMEOUT=$(CONTAINER_HEALTHY_TIMEOUT) \
	FLINK_JOB_TIMEOUT=$(FLINK_JOB_TIMEOUT) \
	ALERT_WAIT_TIMEOUT=$(ALERT_WAIT_TIMEOUT) \
	TRACKED_WAIT_TIMEOUT=$(TRACKED_WAIT_TIMEOUT) \
	bash functional-tests/run-functional-tests.sh all

## Remove functional test report files.
functional-test-clean:
	@echo "Removing functional-tests/reports/ ..."
	@rm -rf functional-tests/reports/
	@echo "Done."

## Show this help message.
help:
	@echo ""
	@echo "Event Rule — available make targets:"
	@echo ""
	@echo "  functional-tests              Run ALL use-case functional tests (banking + aircraft)"
	@echo "  functional-test-aircraft     Run only the Air Traffic Control functional tests"
	@echo "  functional-test-banking      Run only the Banking Fraud Detection functional tests"
	@echo "  functional-test-skip-compose Run all tests against an already-running stack"
	@echo "  functional-test-clean        Remove generated test reports"
	@echo ""
	@echo "Usage examples:"
	@echo "  make functional-tests"
	@echo "  make functional-test-aircraft"
	@echo "  make functional-test-banking"
	@echo "  make functional-test SKIP_FLINK_DEPLOY=true"
	@echo "  make functional-test-skip-compose"
	@echo "  make functional-test-aircraft ALERT_WAIT_TIMEOUT=120"
	@echo "  make functional-test-clean"
	@echo ""
