# Backend-for-Frontend (BFF) Service

Node.js service that bridges Kafka, Flink, and the React frontend — consuming events from all use-case topics, streaming them in real-time via WebSocket, and managing the full agent lifecycle (descriptor persistence, Flink job submission, JAR upload, sample-input generation).

## Architecture

```
Kafka Topics → BFF (KafkaJS consumer) → WebSocket → React Frontend
                        ↑
              POST /api/use-cases/:id/events   (event injection from UI)

samples/<id>/agents.json ←→ BFF ←→ Flink REST API (FLINK_URL)
                                       ↑
                         POST /api/agents/:id/start  (submit job)
                         POST /api/agents/:id/stop   (cancel job)
                         GET  /api/agents/:id/status (poll state)
```

The BFF service:
1. Discovers available use cases by scanning the `samples/` directory for `agents.json` files
2. On the first WebSocket client for a use case, starts a dedicated Kafka consumer subscribed to all inbound and outbound topics
3. Maintains a ring-buffer in-memory cache of the last 50 events per topic type
4. Broadcasts each new Kafka message to all connected WebSocket clients watching that use case
5. Stops the Kafka consumer automatically when the last client for a use case disconnects
6. Persists agent job state to `samples/<id>/.job-state.json` so it survives restarts
7. Auto-creates all Kafka topics declared in every `agents.json` at startup (idempotent)

## Features

- **Dynamic use-case discovery**: Any `samples/<name>/agents.json` appears automatically
- **Real-time event streaming**: Events pushed to the frontend immediately via WebSocket
- **In-memory ring buffer**: 50-event cap per topic type per use case (no unbounded growth)
- **Per-use-case Kafka consumers**: Isolated consumer groups (`event-rule-bff-<useCase>`)
- **Event injection**: `POST /api/use-cases/:useCase/events` publishes events to Kafka from the browser
- **Demo scenarios**: `GET /api/use-cases/:useCase/scenarios` serves `demo-scenarios.json`
- **Topic auto-creation**: All descriptor-referenced topics are created at startup
- **Agent management**: Save, start, stop, and delete agent descriptors via REST
- **Flink job lifecycle**: Submit and cancel Flink jobs through the Flink REST API
- **JAR upload**: Upload Decision Intelligence ruleset JARs via multipart POST
- **Sample-input generation**: Invoke the fat JAR's `SampleInputGenerator` to produce example event payloads
- **Persisted job state**: Agent Flink job IDs survive BFF container restarts
- **Health endpoint**: Reports connected clients, active consumers, and per-topic event counts

## Configuration

| Variable                   | Description                                                                             | Default                                          |
|----------------------------|-----------------------------------------------------------------------------------------|--------------------------------------------------|
| `KAFKA_BOOTSTRAP_SERVERS`  | Kafka broker address                                                                    | `localhost:9092`                                 |
| `PORT`                     | Server port                                                                             | `8080`                                           |
| `SAMPLES_DIR`              | Absolute path to the `samples/` directory                                               | `../../samples` relative to `server.js`          |
| `FLINK_URL`                | Flink JobManager REST URL (e.g. `http://jobmanager:8081`)                               | `null` — Flink endpoints return `503` when unset |
| `JAR_PATH`                 | Explicit path to the fat JAR; if unset, auto-resolved from `event-rule-backend/target/` | auto-resolved                                    |
| `TOPIC_PARTITIONS`         | Number of partitions for auto-created topics                                            | `3`                                              |
| `TOPIC_REPLICATION_FACTOR` | Replication factor for auto-created topics                                              | `1`                                              |
| `DI_RUNTIMES`              | JSON array of IBM Decision Intelligence runtime connections used by the Descriptor Wizard's "Fetch from DI Runtime" path. See [`README.md §6`](../../README.md#6-descriptor-wizard) for the full format. | unset — DI fetch feature disabled |

## API Reference

### Endpoint Summary

| Method   | Path                                            | Description                                                                    |
|----------|-------------------------------------------------|--------------------------------------------------------------------------------|
| `WS`     | `/`                                             | Real-time event stream; client sends `selectUseCase` to subscribe              |
| `GET`    | `/health`                                       | Server health, connected clients, active consumers, and per-topic event counts |
| `GET`    | `/api/use-cases`                                | List all discovered use cases                                                  |
| `GET`    | `/api/use-cases/:useCase/descriptor`            | Use-case descriptor (topics, job name, ...) derived from `agents.json`         |
| `GET`    | `/api/use-cases/:useCase/scenarios`             | Pre-built demo scenarios for a use case                                        |
| `POST`   | `/api/use-cases/:useCase/events`                | Publish one or more events to a Kafka topic                                    |
| `POST`   | `/api/use-cases/:useCase/reset-entities`        | Reset Flink entity state for all entities touched by a scenario                |
| `GET`    | `/api/flink/status`                             | Running Flink job names from the JobManager                                    |
| `GET`    | `/api/agents`                        | List all saved agent descriptors                                               |
| `POST`   | `/api/agents`                        | Create or replace an agent descriptor                                          |
| `GET`    | `/api/agents/:id`                    | Read back `agents.json` for an agent                                           |
| `POST`   | `/api/agents/:id/start`              | Submit the Flink job for an agent                                              |
| `GET`    | `/api/agents/:id/status`             | Poll current Flink job state for an agent                                      |
| `POST`   | `/api/agents/:id/stop`               | Cancel the running Flink job for an agent                                      |
| `DELETE` | `/api/agents/:id`                    | Stop job, delete Kafka topics, and remove the agent files                      |
| `POST`   | `/api/agents/:id/jar`                | Upload a Decision Intelligence ruleset JAR (multipart `jar` field)             |
| `GET`    | `/api/agents/:id/sample-input`       | Generate example event payloads from the ruleset archive                       |

---

### WebSocket — `ws://localhost:8080`

**Client → Server** (after connection, must be the first message):
```json
{ "type": "selectUseCase", "useCase": "banking-fraud-agent" }
```

**Server → Client — initial data snapshot:**
```json
{
  "type": "initial",
  "descriptor": {
    "name": "Banking Fraud Detection Agent",
    "topics": [ { "name": "agent-account-events", "type": "AccountEvent", "label": "Account Event" } ],
    "outboundTopic": { "name": "agent-fraud-alerts", "label": "Fraud Alerts" }
  },
  "data": {
    "AccountEvent": [ { "accountNumber": "ACC123", "type": "account-created", ... } ],
    "outbound":     [ { "decision": "APPROVED", "riskScore": 0, ... } ]
  }
}
```

**Server → Client — live event:**
```json
{ "type": "event", "topicType": "outbound", "data": { "decision": "BLOCKED", ... } }
```

`topicType` is set to the `type` field from the descriptor for inbound topics (e.g., `"transaction"`, `"account-created"`), or `"outbound"` for any outbound topic (all entries in `outboundTopics[]` share the `"outbound"` type value).

**Server → Client — error:**
```json
{ "type": "error", "message": "Use case 'unknown' not found" }
```

---

### REST Endpoints

#### `GET /api/use-cases`
Returns all discovered use cases.

```json
[
  { "id": "banking-fraud-agent", "name": "Banking Fraud Detection Agent", "topics": [ ... ] }
]
```

#### `GET /api/use-cases/:useCase/descriptor`
Returns the use-case descriptor, built from the use case's `agents.json`. Top-level `name`, `flinkJobName`, `consumerGroupId`, `description`, `icon`, `topics` and `outboundTopics` in `agents.json` are used when present; otherwise they are derived from the agents' `subscriptions[].topic` and `outputs[].topic`.

#### `GET /api/use-cases/:useCase/scenarios`
Returns the pre-built demo scenarios from `samples/<useCase>/demo-scenarios.json`, or `[]` if the file does not exist.

```json
[
  {
    "id": "large-transaction",
    "label": "Large Transaction (RULE-001)",
    "description": "Single transaction over $10 000 ...",
    "accountEvents": [],
    "transactionEvents": [ { "transactionId": "DEMO-LARGE-001", "amount": "28500.00", ... } ]
  }
]
```

#### `POST /api/use-cases/:useCase/events`
Publishes one or more events to a Kafka topic. Used by the frontend **Send Events** panel.

**Request body:**
```json
{ "topic": "transactions", "events": [ { "transactionId": "TXN-X", "amount": "99.00", ... } ] }
```

**Response:**
```json
{ "published": 1, "topic": "transactions" }
```

Returns `400` if `topic` is not declared in the descriptor, or if `events` is empty.

#### `POST /api/use-cases/:useCase/reset-entities`
Resets Flink entity state for all entities touched by a specific demo scenario.  Used by the
frontend **Send Events** panel to guarantee each scenario starts from a clean state.

Reads `agents.json` to determine the entity key field and target topic, then reads
`demo-scenarios.json` to find the scenario.  Publishes one `__ENTITY_RESET__` sentinel
event per distinct entity key value found in the scenario's event payloads.  Sentinels travel
through the normal Kafka event topic; Flink routes each one to the correct keyed partition via
`keyBy(entityId)` and clears only that entity's state.

**Request body:**
```json
{ "scenarioId": "transaction-on-suspended-account" }
```

**Response:**
```json
{ "reset": 1, "entityIds": ["111"] }
```

Returns `400` if `scenarioId` is missing, `404` if `agents.json` or the scenario is not found.
Reset sentinels are **not** forwarded to the dashboard or added to the event ring buffer —
they appear in server logs only.

#### `GET /health`
```json
{
  "status": "ok",
  "connectedClients": 2,
  "activeUseCases": 1,
  "useCases": {
    "banking-fraud-agent": {
      "AccountEvent": 5,
      "outbound": 48
    }
  }
}
```

---

### Flink Endpoints

#### `GET /api/flink/status`
Queries the Flink JobManager for currently running jobs (by name).

```json
{ "jobs": ["Banking Fraud Detection Agent"] }
```

Returns `{ "jobs": [] }` when `FLINK_URL` is not configured or Flink is unreachable.

---

### Agent Endpoints

Agents are AI-driven pipelines defined by an `agents.json` descriptor. The BFF persists these descriptors under `samples/<id>/` and drives the full Flink job lifecycle via the Flink REST API.

#### `POST /api/agents`
Creates or replaces the agent descriptor. Writes `agents.json` (the topic lists are derived from the agents) and ensures all referenced Kafka topics exist.

**Request body:**
```json
{
  "useCaseId": "banking-fraud-agent",
  "descriptor": {
    "agents": [
      {
        "name": "Fraud Detection Agent",
        "agentId": "fraud-agent-v1",
        "subscriptions": [
          {
            "eventType": "transaction",
            "topic": "transactions",
            "operation": "FraudDetectionService_transaction_op"
          }
        ],
        "ruleset": { "path": "samples/banking-fraud-agent/rules.jar" }
      }
    ]
  }
}
```

**Response `201`:**
```json
{ "saved": true, "useCaseId": "banking-fraud-agent", "path": "samples/banking-fraud-agent/agents.json" }
```

Returns `409` if the agent is currently running (protected use cases only).

#### `GET /api/agents`
Lists all saved agent descriptors.

```json
[
  { "id": "banking-fraud-agent", "name": "Banking Fraud Detection Agent", "agentId": "fraud-detection-agent" }
]
```

#### `GET /api/agents/:id`
Returns the raw `agents.json` for the agent.

Returns `404` if no `agents.json` exists for `id`.

#### `POST /api/agents/:id/start`
Submits the Flink job for the agent using the configured fat JAR.

Passes `[KAFKA_BROKER, samples/<id>/agents.json, <id>-job]` as `programArgsList` and sets `pipeline.name` to `<id>-job` in `flinkConfiguration`.

**Response `202`:**
```json
{ "started": true, "jobId": "abc123", "agentId": "banking-fraud-agent" }
```

Returns `503` if `FLINK_URL` is not configured, `404` if `agents.json` does not exist, `409` if already running (protected use cases).

#### `GET /api/agents/:id/status`
Polls Flink for the current job state.

```json
{ "agentId": "banking-fraud-agent", "jobId": "abc123", "state": "RUNNING", "name": "Banking Fraud Detection Agent", "startedAt": 1700000000000 }
```

Returns `{ "state": "NOT_STARTED" }` when no job has been submitted. Returns `{ "state": "UNKNOWN" }` when `FLINK_URL` is not configured. Clears stale state automatically if Flink returns `404` for the stored job ID.

#### `POST /api/agents/:id/stop`
Cancels the running Flink job. Resolves the job ID from BFF state first; falls back to a Flink `/jobs/overview` lookup by `descriptor.flinkJobName` to handle jobs started outside the BFF.

**Response:**
```json
{ "stopped": true, "jobId": "abc123", "agentId": "banking-fraud-agent" }
```

Returns `404` if no running job is found.

#### `DELETE /api/agents/:id`
Cancels any running Flink job (best-effort), deletes owned Kafka topics, removes `agents.json`, `.job-state.json`, and any co-located `.jar` files. Removes the directory if empty.

**Response:**
```json
{ "deleted": true, "agentId": "banking-fraud-agent", "removed": ["agents.json"], "topicsDeleted": ["agent-account-events", "agent-fraud-alerts"] }
```

Returns `409` if the agent is currently running (protected use cases), `404` if no files were found.

#### `POST /api/agents/:id/jar`
Uploads a Decision Intelligence ruleset JAR via multipart form-data (field name: `jar`). Saves the file to `samples/<id>/<originalname>`.

**Response `201`:**
```json
{ "saved": true, "filename": "FraudDetectionService_1.5.2.jar", "path": "samples/banking-fraud-agent/FraudDetectionService_1.5.2.jar" }
```

Use the returned `path` as `ruleset.path` in the `agents.json` descriptor.

Returns `400` if no file is included.

#### `GET /api/agents/:id/sample-input`
Invokes `com.ibm.di.event.rule.SampleInputGenerator` from the fat JAR as a Java child process. Calls `DecisionRunner.createInputExample()` (public Decision Intelligence API) on the agent's ruleset archive and returns the unwrapped per-topic raw event payloads.

```json
{
  "topic": "transactions",
  "sample": { "transactionId": "TXN-EXAMPLE", "amount": "100.00", ... },
  "samplesByTopic": {
    "transactions": { "transactionId": "TXN-EXAMPLE", ... }
  },
  "topics": [ { "name": "transactions", "type": "transaction", "label": "transaction" } ]
}
```

Returns `503` if the fat JAR is not found (run `mvn package -DskipTests` in `event-rule-backend/` first), `400` if `ruleset.path` is missing from `agents.json`.

---

## Running Locally

### Prerequisites
- Node.js 20.x or higher
- Running Kafka instance
- Java 21 (only required for `GET /api/agents/:id/sample-input`)

### Install Dependencies
```bash
npm install
```

### Start the Server
```bash
npm start
```

### Development Mode (with auto-reload)
```bash
npm run dev
```

## Running with Docker

The BFF service is included in the Docker Compose setup. The Docker image bundles OpenJDK 21 (headless) for sample-input generation.

```bash
docker compose up bff
```

## Troubleshooting

### WebSocket connection fails
- Verify the BFF is running and port 8080 is reachable
- Check logs: `docker logs event-rule-bff`

### No events received from Kafka
- Confirm topics exist and contain data:
  ```bash
  docker exec -it kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic transactions --from-beginning --max-messages 5
  ```
- Verify `KAFKA_BOOTSTRAP_SERVERS` is correct

### Event injection returns 400
- The requested `topic` must be known to the use case (declared in `topics` of `agents.json`, or used by an agent subscription)
- `events` must be a non-empty JSON array

### Agent start returns 503
- `FLINK_URL` must be set to the Flink JobManager REST address (e.g. `http://jobmanager:8081`)

### Sample-input returns 503
- Build the fat JAR first: `mvn package -DskipTests` in `event-rule-backend/`
- Or set `JAR_PATH` explicitly to point at the built JAR

### Agent start returns 409
- A job for this agent is already running; call `POST /api/agents/:id/stop` first

