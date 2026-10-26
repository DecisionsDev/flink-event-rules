# Scripts

This directory contains operational scripts for managing Kafka topics, submitting events, deploying Flink jobs, hot-reloading Decision Intelligence archives, and monitoring the running stack.

## Files

| Script                                                                       | Purpose                                                                                                                                                  |
|------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|
| [`create-topics.sh`](create-topics.sh)                                       | Create one or more Kafka topics (3 partitions, replication-factor 1)                                                                                     |
| [`submit-events.sh`](submit-events.sh)                                       | Submit every object in a JSON array file as a separate Kafka message                                                                                     |
| [`launch-demo-scenario.sh`](launch-demo-scenario.sh)                         | Run all or one named demo scenario from `samples/<use-case>/demo-scenarios.json`                                                                         |
| [`broadcast-reload.sh`](broadcast-reload.sh)                                 | Publish an `ArchiveControlCmd` to trigger a live archive reload via Kafka broadcast state; supports `LOAD`, `PREFETCH`, `ROLLBACK`, and `NOOP`           |
| [`build-decision-service.sh`](build-decision-service.sh)                     | Build an IBM Decision Intelligence Decision Project directory or `.zip` file against a Decision Designer instance; outputs a ready-to-deploy archive JAR |
| [`validate-deployment.sh`](validate-deployment.sh)                           | Validate the full deployment — checks infrastructure, descriptor integrity, Kafka topics, running Flink jobs, and reload configuration for each use case |
| [`test-descriptor-loading.sh`](test-descriptor-loading.sh)                   | Run a lightweight test suite: checks `agents.json` presence, JSON syntax, required fields, `flinkJobName` values, Maven build, and FAT JAR              |
| [`event-backend-job-monitor.sh`](event-backend-job-monitor.sh)               | Interactive Flink job monitor — list, inspect, continuously monitor, or diagnose a specific job ID                                                       |
| [`monitor-kafka-health.sh`](monitor-kafka-health.sh)                         | One-shot Kafka health check — container status, resource usage, broker API reachability, topics, and recent log errors                                   |
| [`resolve-di-dependencies.sh`](resolve-di-dependencies.sh)                   | Resolve `execution-api`, `engine-de-api`, and required Jackson JARs from a locally installed Decision Designer Maven repository                          |

---

## Prerequisites

| Tool                    | Used by                                                                                                    | Install                                                                                              |
|-------------------------|------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------|
| Docker & Docker Compose | all scripts except `resolve-di-dependencies.sh`                                                           | [docs.docker.com](https://docs.docker.com)                                                           |
| `jq`                    | `submit-events.sh`, `event-backend-job-monitor.sh`, `validate-deployment.sh`, `test-descriptor-loading.sh` | macOS: `brew install jq` · Linux: `apt-get install jq`                                               |
| `curl`                  | `event-backend-job-monitor.sh`, `resolve-di-dependencies.sh`                                              | usually pre-installed                                                                                |
| `python3`               | `resolve-di-dependencies.sh`                                                                              | usually pre-installed; macOS: `brew install python3`                                                 |
| `shasum` / `sha256sum`  | `broadcast-reload.sh`                                                                                      | pre-installed on macOS / Linux respectively                                                          |
| `flink` CLI             | `samples/banking-fraud-agent/scripts/deploy.sh`                                                            | available inside the Flink Docker image or locally from [flink.apache.org](https://flink.apache.org) |
| `mvn`                   | `test-descriptor-loading.sh` (Test 7), `resolve-di-dependencies.sh`                                       | [maven.apache.org](https://maven.apache.org)                                                         |

---

## Usage

### Starting the stack

```bash
docker compose up -d kafka
```

Wait a few seconds for Kafka to be ready before running any other scripts.

---

### `create-topics.sh` — create Kafka topics

```bash
./scripts/create-topics.sh <topic1> [topic2] ...
```

Creates topics with 3 partitions and replication-factor 1. Safe to run multiple times (`--if-not-exists`). Prints the full topic list after creation.

**Example:**
```bash
./scripts/create-topics.sh agent-transactions agent-fraud-alerts agent-errors
```

---

### `submit-events.sh` — publish events from a JSON file

```bash
./scripts/submit-events.sh <topic> <json-file>
```

The JSON file must contain a top-level array of objects. Each object is sent as a separate Kafka message.

**Example:**
```bash
./scripts/submit-events.sh agent-transactions samples/banking-fraud-agent/demo-scenarios.json
```

---

### `launch-demo-scenario.sh` — run demo scenarios

Reads `samples/<use-case>/demo-scenarios.json` and publishes each event to its configured Kafka topic.

After each scenario completes, a `__ENTITY_RESET__` sentinel is published for every entity
(account) touched by that scenario.  The sentinel clears only that entity's Flink state and
event history — all other entities are unaffected.  This guarantees that running scenarios
back-to-back produces independent results.

```bash
# Run all scenarios for a use case (reset after each — default)
./scripts/launch-demo-scenario.sh banking-fraud-agent

# Run a single named scenario (reset after — default)
./scripts/launch-demo-scenario.sh banking-fraud-agent transaction-on-suspended-account

# Run without the post-scenario entity reset
./scripts/launch-demo-scenario.sh banking-fraud-agent transaction-on-suspended-account --no-reset
```

The entity key field and reset target topic are read from `agents.json`
(`.agents[0].entityKeyExtractor` and `.agents[0].subscriptions[0].topic`).  If `agents.json` is
absent or incomplete the reset step is skipped with a warning — the scenario events are still
published.

---

### `broadcast-reload.sh` — live archive reload via Kafka broadcast

Publishes an `ArchiveControlCmd` JSON message to the archive-control Kafka topic. The running Flink job receives it on every subtask simultaneously via broadcast state, downloads the JAR, smoke-tests it, and swaps it in atomically — no filesystem change or job restart needed.

```bash
# Banking fraud agent: deploy a new fraud-detection JAR
./scripts/broadcast-reload.sh --use-case agent-banking \
    --jar samples/banking-fraud-agent/new-fraud-rules.jar

# Prefetch then load (minimises checkpoint-barrier blocking window)
./scripts/broadcast-reload.sh --use-case agent-banking \
    --jar samples/banking-fraud-agent/new-fraud-rules.jar --command PREFETCH
./scripts/broadcast-reload.sh --use-case agent-banking \
    --jar samples/banking-fraud-agent/new-fraud-rules.jar --command LOAD --wait

# Rollback to a prior version (no JAR on disk needed)
./scripts/broadcast-reload.sh --use-case agent-banking --command ROLLBACK \
    --archive-url file:///opt/flink/samples/banking-fraud-agent/old.jar \
    --sha256 3f9a1b2c... --version v1.0.1

# Preview the Kafka message without publishing
./scripts/broadcast-reload.sh --use-case agent-banking \
    --jar samples/banking-fraud-agent/new-fraud-rules.jar --dry-run
```

**Required flags (for LOAD / PREFETCH / ROLLBACK):**

| Flag                | Description                                                                                                           |
|---------------------|-----------------------------------------------------------------------------------------------------------------------|
| `--use-case <name>` | Use-case name (e.g. `agent-banking`); determines the default archive-control topic                                    |
| `--jar <path>`      | Host-side JAR path. SHA-256 is computed locally; container `file://` URL is derived from the `samples/` volume mount. |

**All options:**

| Flag                    | Default                          | Description                                                                    |
|-------------------------|----------------------------------|--------------------------------------------------------------------------------|
| `--command <type>`      | `LOAD`                           | `LOAD` \| `PREFETCH` \| `ROLLBACK` \| `NOOP`                                   |
| `--version <label>`     | JAR basename                     | Human-readable version label embedded in the control message                   |
| `--request-id <id>`     | auto-generated                   | Correlation ID for audit logs                                                  |
| `--topic <name>`        | per use-case default             | Override the archive-control topic name                                        |
| `--bootstrap <addr>`    | `localhost:9092`                 | Kafka bootstrap server                                                         |
| `--container <name>`    | `kafka`                          | Kafka Docker container name                                                    |
| `--samples-dir <dir>`   | `./samples`                      | Host samples directory (used for URL resolution)                               |
| `--container-mount <p>` | `/opt/flink/samples`             | Container path where `samples/` is mounted                                     |
| `--archive-url <url>`   | —                                | Full `file://` URL for ROLLBACK (skips JAR-based resolution)                   |
| `--sha256 <hex>`        | —                                | Pre-computed SHA-256 for ROLLBACK                                              |
| `--wait`                | off                              | Tail TaskManager logs after LOAD/ROLLBACK until reload is confirmed or timeout |
| `--tm-container <name>` | `event-rule-backend-taskmanager` | TaskManager container name (required with `--wait`)                            |
| `--wait-timeout <secs>` | `120`                            | How long to wait for reload confirmation                                       |
| `--dry-run`             | off                              | Print the Kafka command that would run; do not publish                         |

**Default Kafka topic for the banking use case:**

| Use case        | Default archive-control topic   |
|-----------------|---------------------------------|
| `agent-banking` | `agent-banking-archive-control` |

Override the topic for any use case with `--topic <name>`. Control topics are created
automatically with 1 partition (ordered delivery) if they do not exist.

**How it works:**
1. Computes SHA-256 of the JAR locally
2. Resolves the container-side `file://` URL from the volume mount (`./samples → /opt/flink/samples`)
3. Creates the Kafka topic if it does not yet exist (1 partition for ordered delivery)
4. Publishes the `ArchiveControlCmd` JSON payload
5. With `--wait`: polls TaskManager logs until the reload success marker or an error line appears

**Reload log markers:**
```
[StatefulAgentProcessor[fraud-detection-agent]] ✓ Archive reloaded: v=<version> requestId=<id>
[StatefulAgentProcessor[personalized-promotions-agent]] ✓ Archive reloaded: v=<version> requestId=<id>
```

Agent pipeline errors are logged at ERROR level on the TaskManager (no DLQ topic). The previous version stays active on failure.

---

### `validate-deployment.sh` — full deployment validator

Validates prerequisites for one or both agent use cases in a single pass.

```bash
# Validate all deployed use cases
bash scripts/validate-deployment.sh

# Validate a single use case
bash scripts/validate-deployment.sh --agent-banking
```

**Checks performed per use case:**

| Step                     | Description                                                              |
|--------------------------|--------------------------------------------------------------------------|
| 1 – Descriptor integrity | `agents.json` present; required fields (`name`, `flinkJobName`, `topics`, `outboundTopic`, `consumerGroupId`) |
| 2 – agents.json          | Agent descriptor file present                                            |
| 3 – Kafka topics         | All inbound + outbound topics exist in the broker                        |
| 4 – Running Flink job    | Job matching `flinkJobName` is RUNNING                                   |
| 5 – Dynamic reload       | Reports broadcast reload as active                                       |

Shared infrastructure checks (run once, before per-use-case checks): Docker containers (`kafka`, `jobmanager`, `taskmanager`, `bff`, `frontend`), Kafka broker API reachability, Flink REST API, TaskManager registration.

**Exit codes:** `0` = all required checks passed; `1` = one or more required checks failed.

---

### `event-backend-job-monitor.sh` — Flink job monitor

Interactive monitor and diagnostic tool for the Flink REST API.

```bash
./scripts/event-backend-job-monitor.sh <command> [options]
```

| Command             | Description                                                                    |
|---------------------|--------------------------------------------------------------------------------|
| `list`              | List all Flink jobs with their IDs and statuses                                |
| `check <job-id>`    | Inspect a specific job and display any recorded exceptions                     |
| `monitor`           | Continuously refresh job and cluster status every 5 s (Ctrl+C to stop)         |
| `history`           | Search the JobManager container logs for job submission and completion entries |
| `diagnose <job-id>` | Combined check + log search for a specific job ID                              |
| `help`              | Display the usage message                                                      |

**Environment variables:**

| Variable            | Default                         | Description                                     |
|---------------------|---------------------------------|-------------------------------------------------|
| `FLINK_REST_URL`    | `http://localhost:8081`         | Flink REST API base URL                         |
| `BACKEND_CONTAINER` | `event-rule-backend-jobmanager` | JobManager container name used for log searches |

**Dependencies:** `curl`, `jq`, Docker.

---

### `monitor-kafka-health.sh` — Kafka health check

One-shot health report for the Kafka broker (and Zookeeper container when present).

```bash
./scripts/monitor-kafka-health.sh

# Re-run every 30 seconds
watch -n 30 ./scripts/monitor-kafka-health.sh
```

**Checks performed:**
- Container running status and Docker health (`kafka`, `zookeeper`)
- CPU and memory resource usage via `docker stats`
- Broker API reachability (`kafka-broker-api-versions`)
- Full topic list (`kafka-topics --list`)
- Recent log scan: counts of `ERROR`, `WARN`, and `timeout` entries (last 50 lines)

---

## Use case samples

Event samples are organised by use case in [`../samples/`](../samples/). Each use case directory contains:
- An `agents.json` descriptor, also used by the BFF for use-case discovery
- A `demo-scenarios.json` for the UI send-events panel
- A `scripts/` subdirectory with a `deploy.sh` script for submitting the Flink job

### Available use cases

| Use case                                                         | Pipeline                | Description                                          |
|------------------------------------------------------------------|-------------------------|------------------------------------------------------|
| [`banking-fraud-agent`](../samples/banking-fraud-agent/)         | AgentBasedDataStreamJob | Agent-based fraud detection                          |
| [`airtraffic-agent`](../samples/airtraffic-agent/)               | AgentBasedDataStreamJob | Cross-domain agent chaining (Air Traffic Control)    |

---

## Verifying published events

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic <topic-name> \
  --from-beginning \
  --max-messages 10
```

---

### `resolve-di-dependencies.sh` — resolve IBM ADS JARs from a local Decision Designer

Downloads `execution-api.jar`, `engine-de-api.jar`, `jackson-databind 2.16.2`, and `jackson-datatype-jsr310 2.16.2` directly from a locally installed IBM Decision Intelligence Decision Designer instance.

This script resolves the IBM Decision Intelligence JARs required to build the backend (`execution-api`, `engine-de-api`, `foundation`, and Jackson) from a locally installed IBM Decision Intelligence Decision Designer instance. It uses the `maven-designer` Maven profile defined in [`settings.xml`](../settings.xml).

#### Prerequisites

| Requirement | Details                                                                                                  |
|---|----------------------------------------------------------------------------------------------------------|
| Local Decision Designer | A running IBM Decision Intelligence Decision Designer instance (CP4BA or IBM Decision Intelligence Client Managed Software) |
| Zen API key | Generated from the CP4BA Platform UI: profile icon → **Generate API key**                                |
| `curl`, `python3`, `mvn` | See Prerequisites table above                                                                            |

#### Required environment variables

| Variable | Description | Example |
|---|---|---|
| `DI_ZEN_HOSTNAME` | Hostname of your Decision Designer installation (no `https://`) | `cpd-cp4a.mycompany.com` |
| `DI_ZEN_USERNAME` | Zen platform username | `admin` |
| `DI_ZEN_API_KEY` | Zen API key (plain text, not base64) | `abc123…` |

#### Optional environment variables

| Variable | Default | Description |
|---|---|---|
| `DI_SKIP_TLS_VERIFY` | `false` | Set to `true` to disable TLS verification (dev/CI only — not for production) |
| `DI_SETTINGS_FILE` | `./settings.xml` | Path to a custom Maven settings file |

#### Basic usage

```bash
export DI_ZEN_HOSTNAME="cpd-cp4a.mycompany.com"
export DI_ZEN_USERNAME="admin"
export DI_ZEN_API_KEY="<api-key-from-cp4ba-ui>"

./scripts/resolve-di-dependencies.sh
```

#### With a self-signed TLS certificate (development only)

```bash
export DI_ZEN_HOSTNAME="cpd-cp4a.mycompany.com"
export DI_ZEN_USERNAME="admin"
export DI_ZEN_API_KEY="<api-key>"
export DI_SKIP_TLS_VERIFY=true

./scripts/resolve-di-dependencies.sh
```

#### What the script does

1. Validates required tools (`curl`, `python3`, `mvn`) and environment variables
2. Computes `DI_ZEN_API_KEY_B64 = base64(username:apikey)` for the Zen `Authorization` header
3. Verifies that the Decision Designer Maven repository endpoint is reachable (`HTTP 2xx/3xx`)
4. Queries `https://<DI_ZEN_HOSTNAME>/ads/index.json` to discover the exact versions of `execution-api` and `engine-de-api` published by your installation
5. Cross-checks the discovered versions against `event-rule-backend/pom.xml` and warns on any mismatch
6. Runs `mvn dependency:resolve -P maven-designer -pl event-rule-backend` to download all four required JARs into the local Maven repository (`~/.m2`)
7. Verifies the downloaded JARs are present in `~/.m2/repository` and prints a build command

#### After running

Build the project using the same profile:

```bash
mvn clean package -s settings.xml -P maven-designer -pl event-rule-backend
```

Verify all four required classpath items are in the dependency tree:

```bash
mvn dependency:tree -s settings.xml -P maven-designer -pl event-rule-backend \
  | grep -E 'execution-api|engine-de-api|jackson-databind|jackson-datatype-jsr310'
```

---

## Troubleshooting

### Kafka container not running
```bash
docker compose up -d kafka
```

### `jq` not installed
```bash
brew install jq          # macOS
sudo apt-get install jq  # Ubuntu/Debian
sudo yum install jq      # CentOS/RHEL
```

### Topics already exist
[`create-topics.sh`](create-topics.sh) uses `--if-not-exists`, so it is safe to re-run.

### Permission denied
```bash
chmod +x scripts/*.sh
```

### FAT JAR missing (deploy.sh)
```bash
cd event-rule-backend && mvn package -DskipTests
```

### Reload not confirmed after `broadcast-reload.sh --wait`
```bash
docker compose logs --tail 100 event-rule-backend-taskmanager \
  | grep -E 'Archive|SMOKE|reload'
```

---

