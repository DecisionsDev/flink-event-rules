# Functional Test Suite

End-to-end regression tests for the Event Rule project.

The suite manages the full Docker Compose lifecycle automatically—it starts the
stack, deploys Flink jobs, runs the tests, and tears everything down on exit (even
on failure).

Three use cases are covered: **Banking Fraud Detection**, **Air Traffic Control**, and
**Retail Promotion**.

---

## Quick start

```bash
# Run all use-case tests (Banking + Air Traffic Control + Retail)
make functional-tests

# Run only the banking use case
make functional-test-banking

# Run only the ATC use case
make functional-test-aircraft

# Run only the retail use case
make functional-test-retail

# Run against an already-running stack (skip Compose + Flink deploy)
make functional-test-skip-compose
```

---

## Prerequisites

| Tool | Purpose |
|---|---|
| `docker` + Docker Compose plugin | Starts/stops the application stack |
| `curl` | HTTP calls to the BFF REST API |
| `jq` | JSON parsing in assertions |
| Pre-built fat JAR | `event-rule-backend/target/event-rule-backend-1.0-SNAPSHOT.jar` |

Build the fat JAR once before running tests:

```bash
./build.sh          # or
mvn package -pl event-rule-backend --settings settings.xml -DskipTests
```

---

## Structure

```
functional-tests/
├── README.md                           # this file
├── run-functional-tests.sh             # top-level runner (manages Compose + Flink)
├── lib/
│   ├── assert.sh                       # assertion helpers, HTTP helpers, readiness probes
│   └── compose.sh                      # Docker Compose lifecycle helpers
├── bin/use_cases/
│   ├── banking/
│   │   └── banking-fraud-e2e.sh        # banking fraud detection E2E test (38 assertions)
│   ├── aircraft/
│   │   └── aircraft-e2e.sh             # air traffic control E2E test (50 assertions)
│   └── retail/
│       └── retail-agent-e2e.sh         # retail promotion E2E test (5 scenarios)
├── fixtures/
│   ├── banking/                        # reference JSON payloads for banking
│   └── aircraft/                       # reference JSON payloads for ATC
└── reports/                            # per-run log files (generated at runtime)
```

---

## Docker Compose lifecycle

1. `run-functional-tests.sh` calls `docker compose up -d`.
2. It then waits for all four required containers to become healthy:
   - `kafka` (KRaft broker)
   - `event-rule-backend-jobmanager` (Flink JobManager)
   - `event-rule-backend-taskmanager` (Flink TaskManager)
   - `event-rule-bff` (BFF / REST API)
3. For each use case, the appropriate Flink deploy script is run (see
   `samples/<use-case>/scripts/deploy.sh`). The deploy script:
   - Pre-creates the required Kafka topics (avoids a race where the Flink Kafka
     source connector fails because topics do not yet exist).
   - Submits the Flink job via `docker exec`.
4. The E2E test script is run with `SKIP_COMPOSE=true` (the runner owns the stack).
5. On exit (success or failure) `docker compose down` is called automatically.

To skip Compose management when the stack is already running:

```bash
SKIP_COMPOSE=true make functional-test
SKIP_FLINK_DEPLOY=true make functional-test    # skip both compose and deploy
```

---

## Banking fraud detection use case

**Use-case ID:** `banking-fraud-agent`
**Flink job:** `Banking Fraud Detection Agent`
**Assertion count:** 38

### Workflow covered

| Step | Description |
|---|---|
| 1 | All required containers are running and healthy |
| 2 | BFF `/health` returns `{status: "ok"}` |
| 3 | Use-case descriptor is available with all expected topics |
| 4 | Flink job is RUNNING (verified via BFF `/api/flink/status`) |
| 5a | Inject `AccountEvent (OPENING)` → entity state created |
| 5b | **Intermediate state**: no fraud alert yet (regression guard) |
| 5c | Inject large cross-border transaction ($50 000, GBR) |
| 5d | Wait for `Alert` on `agent-fraud-alerts` (RULE-001 + RULE-002a) |
| 6a | Inject `AccountEvent (SUSPENDED)` for a different account |
| 6b | Inject any transaction on the suspended account |
| 6c | Wait for `BLOCKED` alert (RULE-006 fires immediately) |
| 7 | API input validation: unknown use-case → 404, bad body → 400 |

### Rules exercised

| Rule | Trigger | Expected decision |
|---|---|---|
| RULE-001 | `amount ≥ 10 000` (+30) | contributes to PENDING_REVIEW |
| RULE-002a | cross-border transaction (+15) | total ≥ 45 → PENDING_REVIEW |
| RULE-006 | `status = SUSPENDED` | BLOCKED (short-circuits) |

---

## Air Traffic Control use case

**Use-case ID:** `airtraffic-agent`
**Flink job:** `Air Traffic Control`
**Assertion count:** 50

### Workflow covered

| Step | Description |
|---|---|
| 1 | All required containers are running and healthy |
| 2 | BFF `/health` returns `{status: "ok"}` |
| 3 | Descriptor has all 5 inbound + 4 outbound topics; both agents have `chainingEnabled: true` |
| 4 | Flink job is RUNNING |
| 5a | Initialize CDG airport entity via `AirportInitializationEvent` |
| 5b | Initialize NCE airport entity |
| 5c | Initialize aircraft entity with `originAirport=CDG`, `destinationAirport=NCE` |
| 6 | API input validation (cross-use-case topic → 400, etc.) |
| 7a | **Intermediate state**: `TAXIING` event → no `TrackedAircraftEvent` yet |
| 7b | `IN_FLIGHT` event (ascending near CDG) → aircraft status transitions |
| 7c | `TrackedAircraftEvent` appears on `agent-tracked-aircraft-events` topic |
| 8a | Descend toward NCE airport |
| 8a-2 | Touch down at NCE elevation → LANDING/TAXIING state |
| 8b | Immediately ascend again (within 2 minutes) |
| 8c | `TOUCH_AND_GO` alert on `agent-atc-alerts` with `aircraftId` + airport code |

### Architecture validated

- **Agent chaining**: Aircraft Agent → TrackedAircraftEvent → Airport Agent (in-process, no Kafka hop)
- Airport entity key: `$.airport` (e.g. `CDG`)
- Aircraft entity key: `$.aircraftId`

---

## Retail Promotion use case

**Use-case ID:** `retail-agent`
**Flink job:** `Retail Promotion Agent`
**Scenario count:** 5

The retail test drives every scenario in `samples/retail-agent/demo-scenarios.json`
through the BFF and asserts the `Promotion` emitted on `agent-promotions`, the absence of
errors on `agent-retail-errors`, and the final `loyaltyStatus` recorded on
`agent-customer-entity-changelog`. Each scenario uses its own `customerId`, and all
assertions are filtered by that id so scheduled promotions from earlier scenarios cannot
leak into later ones.

### Scenarios covered

| Scenario | Customer | Description | Expected output |
|---|---|---|---|
| 1 | `JS` | Silver customer with 3 purchases | Upgraded to Gold, `discount=10` |
| 2 | `TS` | Gold customer with 5 purchases | Upgraded to Platinum, `discount=12` |
| 3 | `WM` | Gold customer, large purchase (SPORT) | `discount=10`, `productCategory=SPORT` |
| 4 | `EC` | 3 ELECTRONICS purchases in 48h, then store visit | `discount=5`, status stays Silver |
| 5 | `LB` | 3 ELECTRONICS browse events in 48h, then store visit | `discount=3`, status stays Silver |

### Key environment variable

| Variable | Default | Description |
|---|---|---|
| `OUTPUT_WAIT_TIMEOUT` | `60` | Seconds to wait for a `Promotion` on `agent-promotions` |
| `INTER_EVENT_DELAY_MS` | `2000` | Milliseconds between injected events within one scenario |

---

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `BFF_URL` | `http://127.0.0.1:8080` | BFF base URL |
| `SKIP_COMPOSE` | `false` | Skip `docker compose up/down` |
| `SKIP_FLINK_DEPLOY` | `false` | Skip Flink job deployment |
| `COMPOSE_CLEAN_VOLUMES` | `false` | Pass `-v` to `compose down` (removes Kafka data) |
| `CONTAINER_HEALTHY_TIMEOUT` | `180` | Seconds to wait for containers to become healthy |
| `FLINK_JOB_TIMEOUT` | `180` | Seconds to wait for a Flink job to be RUNNING |
| `ALERT_WAIT_TIMEOUT` | `90` | Seconds to wait for a fraud/ATC alert |
| `TRACKED_WAIT_TIMEOUT` | `60` | Seconds to wait for a `TrackedAircraftEvent` |
| `OUTPUT_WAIT_TIMEOUT` | `60` | Seconds to wait for a `Promotion` event (retail) |
| `INTER_EVENT_DELAY_MS` | `2000` | Milliseconds between events in one retail scenario |

---

## Running in CI

```yaml
# GitHub Actions example
- name: Build fat JAR
  run: mvn package -pl event-rule-backend --settings settings.xml -DskipTests

- name: Run functional tests
  run: make functional-tests
  timeout-minutes: 30
```

The suite is deterministic: each test run uses unique entity/account IDs
(`FT-<unix-timestamp>`) to prevent interference between runs.

---

## Troubleshooting

### Stack does not start

```bash
docker compose logs kafka
docker compose ps
```

Check that no other process is bound to ports `9092`, `8080`, or `8081`.

### Flink job fails immediately after submission

```bash
curl -s http://localhost:8081/jobs | python3 -m json.tool
JOB_ID=$(curl -s http://localhost:8081/jobs | jq -r '.jobs[0].id')
curl -s "http://localhost:8081/jobs/$JOB_ID/exceptions" | python3 -m json.tool | head -30
```

The most common cause on a fresh stack is **Kafka topics not yet created**.
The deploy scripts now pre-create all required topics, but if you submit a job
manually without using the deploy script you may see
`UnknownTopicOrPartitionException`. Create topics with:

```bash
docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic <topic-name> --partitions 1 --replication-factor 1
```

### Events injected but no alerts produced

This usually means the Flink Kafka consumer was not fully subscribed when the
events were published. After the Flink job transitions to RUNNING, the tests wait
an additional 10–20 seconds for partition assignment to complete. If you still see
this problem, increase `FLINK_JOB_TIMEOUT` or check:

```bash
docker logs event-rule-backend-taskmanager 2>&1 | grep "distance\|undefined\|ERROR"
```

### Fraud alert not produced for happy-path transaction

The rules require the account entity to exist before the transaction is processed.
The test waits 8 seconds after injecting `AccountEvent` before injecting the
transaction. If the Flink worker is under load, increase this delay:

```bash
INTER_EVENT_DELAY_MS=10000 make functional-test-banking
```

### Test timeouts or slow startup on Apple Silicon

By default, Docker Compose pulls images matching the host platform, but if you are running an emulated `amd64` container on Apple Silicon (using Rosetta 2 or QEMU), JVM startup and class loading can be extremely slow. This can cause Flink job registration or first-event processing to exceed default timeouts (such as the 60-second wait for a `TrackedAircraftEvent`).

To resolve this, ensure you configure a native `arm64` image in your `.env` file:
```bash
FLINK_IMAGE=flink:2.2.0-scala_2.12-java21
```
This forces the Docker host to run the container natively, reducing warm-up latency from several minutes to under 20 seconds.

### Report files

Each run produces a timestamped log in `functional-tests/reports/`:

```bash
ls -lt functional-tests/reports/
cat functional-tests/reports/banking-*.log | grep -E "PASS|FAIL"
```

---

## Design notes

### Why bash, not JUnit/pytest?

The functional tests are intentionally written in bash so they have zero build
dependencies beyond the tools already required for the project (`docker`, `curl`,
`jq`). No Maven credentials, no Python environment, no Node.js.

### HTTP helper pattern

`http_get` / `http_post` / `http_delete` in `lib/assert.sh` set two global
variables (`HTTP_STATUS`, `HTTP_BODY`) rather than printing to stdout. This avoids
the bash subshell trap where variables set inside `$(...)` are lost in the parent
shell. All test scripts call these functions directly and read the globals:

```bash
http_get "/health" 10
assert_http_status "GET /health returns 200" "200" "$HTTP_STATUS" "$HTTP_BODY"
```

### Kafka consumer warm-up

The Flink job transitions to RUNNING state before all Kafka consumer group members
have completed partition assignment. The tests add a fixed warm-up sleep (10 s for
banking, 20 s for ATC which has more operators) after the job is detected as
RUNNING, before injecting any test events.
