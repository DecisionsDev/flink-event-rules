# Tutorial: Building a New Use Case

This tutorial walks through building a new Agent-based use case from scratch, using the Banking
Fraud Agent as a reference implementation. By the end, you will have a working `agents.json`
and Decision Intelligence archive wired into a running Flink job and dashboard.

Before starting, read [`docs/AGENT_PATTERN_DESIGN.md`](docs/AGENT_PATTERN_DESIGN.md) for the
descriptor schema and conceptual model — this tutorial assumes familiarity with Agents,
subscriptions, and horizons.

## Table of Contents

1. [Required Project Structure](#1-required-project-structure)
2. [Create a New Use-Case Directory](#2-create-a-new-use-case-directory)
3. [Define `agents.json`](#3-define-agentsjson)
4. [Add Agent Logic — the Decision Intelligence Archive](#4-add-agent-logic--the-decision-intelligence-archive)
5. [Configure Dependencies and Environment Variables](#5-configure-dependencies-and-environment-variables)
6. [Add a Deploy Script and Demo Scenarios](#6-add-a-deploy-script-and-demo-scenarios)
7. [Build and Test](#7-build-and-test)
8. [Run and Validate](#8-run-and-validate)
9. [Troubleshooting](#9-troubleshooting)
10. [Reference: The Banking Fraud Agent](#10-reference-the-banking-fraud-agent)

---

## 1. Required Project Structure

A use case is a self-contained directory under `samples/`. At minimum it needs:

```
samples/<your-use-case>/
├── README.md              # Use-case documentation (rules, event shapes, how to run)
├── agents.json             # Agent descriptor + use-case properties — drives the Flink job and the dashboard
├── demo-scenarios.json     # (optional) pre-built events for the Send Events panel
├── YourService.jar         # Decision Intelligence archive with your rules
├── di-project/             # (optional) Decision Intelligence project source, for reference
└── scripts/
    └── deploy.sh           # Submits the Flink job for this use case
```

Compare this to the reference implementation at
[`samples/banking-fraud-agent/`](samples/banking-fraud-agent/).

## 2. Create a New Use-Case Directory

```bash
mkdir -p samples/my-usecase/scripts
```

Choose a short, hyphenated directory name — it becomes part of Kafka consumer group IDs and
default topic names.

## 3. Define `agents.json`

This single file is the execution contract consumed by `AgentBasedDataStreamJob` and the source the
BFF/dashboard uses to discover the use case. A minimal single-agent descriptor, with the optional
use-case properties at the top:

```json
{
  "name": "My Use Case",
  "flinkJobName": "My Use Case Agent",
  "consumerGroupId": "event-rule-bff-my-usecase",
  "topics": [
    { "name": "my-usecase-events", "type": "MyEvent", "label": "My Events" }
  ],
  "outboundTopics": [
    { "name": "my-usecase-results", "type": "MyResult", "label": "Results" }
  ],
  "agents": [
    {
      "agentId": "my-agent",
      "name": "My Use Case Agent",
      "entityType": "MyEntity",
      "entityKeyExtractor": "$.id",
      "subscriptions": [
        {
          "eventType": "my-event",
          "topic": "my-usecase-events",
          "operation": "MyOperation",
          "horizon": { "type": "count", "value": 50, "description": "Keep last 50 events" }
        }
      ],
      "ruleset": {
        "path": "samples/my-usecase/MyService.jar",
        "archiveControlTopic": "my-usecase-archive-control",
        "operations": [
          {
            "operation": "MyOperation",
            "entityField": "myEntity",
            "currentEventField": "myEvent",
            "history": [ { "eventType": "my-event", "field": "pastMyEvents" } ],
            "outbounds": [
              { "field": "myOutputEvents", "eventType": "MyResult" }
            ]
          }
        ]
      },
      "outputs": [
        { "eventType": "MyResult", "topic": "my-usecase-results", "description": "Processed output" },
        { "eventType": "agent-error", "topic": "my-usecase-errors", "description": "Processing errors" }
      ],
      "stateConfig": { "ttl": "30d", "backend": "rocksdb", "changeLogTopic": "my-usecase-entity-changelog" },
      "parallelism": 1
    }
  ]
}
```

The top-level properties (`name`, `flinkJobName`, `consumerGroupId`, `topics`, `outboundTopics`)
are optional: when omitted, the BFF derives them from the agents (topics from `subscriptions[].topic`
and `outputs[].topic`). Declare them to control labels and to set a per-topic `idleness`.
If you declare them:

- `flinkJobName` must match the job name the Flink job registers under (see Step 6).
- `topics[].name` must match a `subscriptions[].topic` in an agent.
- `outboundTopics[].name` must match an `outputs[].topic` in an agent.

See [`docs/AGENT_PATTERN_DESIGN.md` §4](docs/AGENT_PATTERN_DESIGN.md#4-agentsjson--use-case-properties)
for the full field reference.

> **Shortcut:** if you use the BFF's `POST /api/agents` endpoint to create your agent, the topics are
> derived from the agents automatically. See
> [`event-rule-frontend/bff/README.md`](event-rule-frontend/bff/README.md).

Key decisions to make at this step:

- **`entityType` / `entityKeyExtractor`** — what business entity does your Agent track, and what
  JSONPath extracts its ID from incoming events?
- **`operation`** — the exact operation name your Decision Intelligence archive will export
  (case-sensitive). Use `""` for any subscription that should only feed history/context.
- **`horizon`** — how much history does each event type need? See
  [`docs/AGENT_PATTERN_DESIGN.md` §6](docs/AGENT_PATTERN_DESIGN.md#6-state-management).

For multi-agent pipelines, chaining, or fan-out patterns, see
[`docs/ADVANCED_PATTERNS.md`](docs/ADVANCED_PATTERNS.md).

## 4. Add Agent Logic — the Decision Intelligence Archive

Business logic lives in an IBM Decision Intelligence archive, not in Java code:

1. Open IBM Decision Designer and create a new decision service.
2. Define an operation matching the `operation` name in your `agents.json` subscription (e.g.
   `MyOperation`).
3. Define input parameters matching the **generic rule signature**: `entity`, `currentEvent`,
   `now`, `processingTime`, and one array parameter per `history[]` mapping — using the
   domain-specific aliases you configured in `ruleset.operations[]` (e.g. `myEntity`, `myEvent`,
   `pastMyEvents`).
4. Write your business rules against those inputs. Have rules write the updated entity back to
   the `entityField` name and any output events to the field names declared in `outbounds[].field`.
5. Build → **Export as JAR** (Decision Service Archive).
6. Place the exported JAR at the path declared in `ruleset.path` (e.g.
   `samples/my-usecase/MyService.jar`).

See [`docs/AGENT_PATTERN_DESIGN.md` §7](docs/AGENT_PATTERN_DESIGN.md#7-rule-execution-context) for
the exact context shape passed to your rules, and
[`docs/RULE_ENGINE.md`](docs/RULE_ENGINE.md) for how the runtime loads
and executes the archive.

## 5. Configure Dependencies and Environment Variables

No Java dependencies are needed for a new use case — only the Decision Intelligence archive and
the JSON descriptor. Relevant runtime environment variables (see [`.env.example`](.env.example)):

| Variable                  | Purpose                                                                     |
|---------------------------|-----------------------------------------------------------------------------|
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker address visible to the Flink job                               |
| `AGENT_DESCRIPTOR_PATH`   | Path to your `agents.json` (used when running the job directly, see Step 7) |
| `AGENT_ERROR_TOPIC_<ID>`  | Per-agent DLQ topic override, if you don't declare an `agent-error` output  |

## 6. Add a Deploy Script and Demo Scenarios

Copy the reference deploy script and update the descriptor path and job name:

```bash
cp samples/banking-fraud-agent/scripts/deploy.sh samples/my-usecase/scripts/deploy.sh
```

Edit the copied script so it points at your descriptor and job name, e.g.:

```bash
LOCAL_DESCRIPTOR="${AGENT_DESCRIPTOR_PATH:-${2:-samples/my-usecase/agents.json}}"
export FLINK_JOB_NAME="${FLINK_JOB_NAME:-My Use Case Agent}"
```

Optionally add `demo-scenarios.json` with pre-built event payloads so your use case appears in the
dashboard's **Send Events** panel — see
[`samples/banking-fraud-agent/demo-scenarios.json`](samples/banking-fraud-agent/demo-scenarios.json)
for the format.

## 7. Build and Test

```bash
# Build the backend fat JAR (includes all agent logic — no code changes needed per use case)
cd event-rule-backend && mvn package -DskipTests && cd ..

# Run backend unit tests
cd event-rule-backend && mvn test && cd ..

# Validate your descriptor without starting Flink
./scripts/test-descriptor-loading.sh
```

`test-descriptor-loading.sh` checks `agents.json` presence, JSON syntax, required fields, and
confirms the Maven build produced a fat JAR. See [`scripts/README.md`](scripts/README.md) for all
available validation scripts.

## 8. Run and Validate

```bash
# 1. Start the infrastructure (if not already running)
./run.sh up

# 2. Submit your Flink job
./samples/my-usecase/scripts/deploy.sh

# 3. Validate the full deployment
./scripts/validate-deployment.sh

# 4. Open the dashboard and select your use case
open http://localhost:3010

# 5. Send a test event (or run a demo scenario)
./scripts/launch-demo-scenario.sh my-usecase
```

`validate-deployment.sh` checks descriptor integrity, Kafka topic existence, and that your Flink
job is `RUNNING`. See [`scripts/README.md`](scripts/README.md) for details.

## 9. Troubleshooting

| Problem                                | What to check                                                                                                                 |
|----------------------------------------|-------------------------------------------------------------------------------------------------------------------------------|
| `Could not find decision runner` error | The `operation` in each subscription must match the operation name exported by IBM Decision Designer exactly (case-sensitive) |
| Use case not showing in dashboard      | Confirm `agents.json` exists and is valid JSON; the BFF only discovers use cases with an `agents.json`                        |
| Rules not firing                       | Check TaskManager logs for smoke-test warnings; verify `ruleset.path` points to an existing JAR                               |
| Entity state not building up           | Ensure any context-only events (`"operation": ""`) arrive before the events that trigger rule execution                       |
| No events appearing in Kafka           | Verify topic names match exactly between the agents and the top-level `topics` of `agents.json`, and what your producer publishes to |

For infrastructure-level troubleshooting (Kafka, Flink, Docker), see
[`README.md#troubleshooting`](README.md) and [`DOCKER.md`](DOCKER.md).

## 10. Reference: The Banking Fraud Agent

Use [`samples/banking-fraud-agent/`](samples/banking-fraud-agent/) as a complete, working
reference implementation:

- [`agents.json`](samples/banking-fraud-agent/agents.json) — a two-operation Agent (transaction
  evaluation + account-event handling) with a hybrid horizon, plus the dashboard wiring (topics)
- [`README.md`](samples/banking-fraud-agent/README.md) — rules, event shapes, and an end-to-end
  walkthrough
- [`scripts/deploy.sh`](samples/banking-fraud-agent/scripts/deploy.sh) — the deploy script to copy

