# Running AgentBasedPTFJob from Command Line

This guide explains how to run the `AgentBasedPTFJob` application (the Flink Process Table Function
backend, see [README §5.2](../README.md#52-ptf-implementation)) from the command line using the
provided script. It is the PTF counterpart of [`RUN_DATASTREAM.md`](RUN_DATASTREAM.md); the same
`agents.json` runs on both backends.

## Prerequisites

- Java 21 or higher
- Maven 3.6 or higher
- Kafka running (default: localhost:9092)

## Quick Start

1. **Compile the project** (if not already compiled):
   ```bash
   cd event-rule-backend
   mvn clean compile
   ```

2. **Run with default settings**:
   ```bash
   ./run-ptf.sh
   ```

   This uses:
   - Bootstrap servers: `localhost:9092`
   - Agents JSON: `../samples/banking-fraud-agent/agents.json`

3. **Run with custom settings**:
   ```bash
   ./run-ptf.sh <bootstrap-servers> <agents-json-path>
   ```

   Example:
   ```bash
   ./run-ptf.sh localhost:9092 ../samples/airline-agent/agents.json
   ```

The job runs in a local Flink mini-cluster inside the JVM; stop it with `Ctrl+C`. Logs go to the
console and to `target/ptf-local.log`.

## What the Script Does

The `run-ptf.sh` script:

1. **Builds Maven classpath**: Uses `mvn -Pdev dependency:build-classpath` so that the Flink
   dependencies, normally `provided`, are on the classpath.

2. **Adds to classpath**:
   - `target/classes` - Compiled application classes
   - `target/test-classes` - Test classes
   - Maven dependencies
   - The project root, so descriptor files can be resolved relative to it

3. **Runs AgentBasedPTFJob**: Executes `com.ibm.di.event.agent.ptf.AgentBasedPTFJob` with the
   bootstrap servers and descriptor path as arguments.

## Configuration

The positional arguments are `[bootstrap-servers] [agents-json-path]`. The environment variables
below override them and are read the same way as by the DataStream job:

| Variable | Default | Effect |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka bootstrap servers |
| `AGENT_DESCRIPTOR_PATH` | argument 2 | Path of `agents.json` |
| `FLINK_JOB_NAME` | `Agent-Based Event Processor (PTF)` | Job name |
| `FLINK_PARALLELISM` | `1` | Used when the descriptor has no top-level `parallelism` |
| `CHECKPOINT_INTERVAL_MS` | `60000` | Checkpoint interval |
| `WATERMARK_OUT_OF_ORDER_MS` | `5000` | Out-of-orderness bound of the watermark |
| `CHAINING_ENABLED` | `true` | Global switch for agent chaining |
| `AGENT_ERROR_TOPIC[_<AGENT>]` | | Error topic, globally or per agent |
| `PTF_PRINT_SQL` | `false` | `true` prints the generated SQL script and exits; nothing is submitted. `json` prints `{"script", "statements"}` with each statement attributed to its agent (`kind`, `agentId`, `sql`); the dashboard SQL View uses it |
| `PTF_DIALECT` | open source | `confluent` generates the Confluent Cloud flavour (only with `PTF_PRINT_SQL=true`) |

Example:

```bash
FLINK_PARALLELISM=2 CHECKPOINT_INTERVAL_MS=10000 \
  ./run-ptf.sh localhost:9092 ../samples/retail-agent/agents.json
```

## Printing the Generated SQL

The PTF backend turns each agent into a Flink SQL pipeline. To inspect it without running anything
(Kafka is not needed):

```bash
PTF_PRINT_SQL=true ./run-ptf.sh localhost:9092 ../samples/airtraffic-agent/agents.json
```

For Confluent Cloud for Apache Flink, generate the script and run it in the SQL workspace:

```bash
PTF_PRINT_SQL=true PTF_DIALECT=confluent ./run-ptf.sh localhost:9092 ../samples/airtraffic-agent/agents.json
```

The script also prints the Maven classpath messages before the SQL. Redirect the output and drop
those lines if you want a clean file. See
[README §5.2](../README.md#confluent-cloud-for-apache-flink) for the Confluent Cloud constraints
and the deployment steps.

## Dashboard: deployment mode and SQL View

The dashboard shows a badge with the runtime a job was **deployed** with (`PTF` or `DataStream`).
It comes from the `runtime` the BFF stores with the job (`samples/<id>/.job-state.json`) and serves
in `GET /api/agents/<id>/status`; it is not the "Flink Runtime" toggle, which only chooses the
runtime of the next deployment. An agent that is not deployed has no badge, and jobs stored before
the selector existed count as DataStream.

Only PTF deployments have a **SQL View** button (it is absent, not disabled, for DataStream or
undeployed agents). When the BFF submits a PTF job it runs `PTF_PRINT_SQL=json` and stores the result
in `samples/<id>/.ptf-sql.json`, so the view shows the SQL the job was started with even if
`agents.json` is edited later (a notice says so). It offers two presentations of the same statements:

- **Raw SQL**: the statements as executed, highlighted and grouped by agent, with Copy and Download.
- **Graphical**: a data-flow diagram, one lane per agent: Kafka sources, input view, Process Table
  Function, Kafka sinks. Select a box or a label for a plain-language description and its SQL, or
  jump to the statement in the raw view. A wide diagram opens scaled to fit the window (never below
  60%, so the text stays readable); the zoom controls (− / + / Fit / 100%, 20–200%) change that. Statements outside the grammar the generator emits are not
  guessed at: they are listed with the reason and their SQL.

The BFF needs `java`, the fat JAR (`JAR_PATH`) and the same `CHAINING_ENABLED` as the Flink
containers, because chaining changes the generated SQL. Jobs started outside the BFF (for example
with `deploy-ptf.sh`) have no stored SQL.

## Differences from the DataStream Backend

- No ruleset hot reload: `archiveControlTopic` is ignored (a warning is logged).
- No `sqlEnrichment`: it is ignored with a warning. Use `references` + `enrichment` instead.
- Timers are event-time: an entity expires only when the watermark of its partition advances.

The full comparison is in [README §5](../README.md#5-flink-backends).

## Troubleshooting

### Classpath Issues

If you encounter classpath-related errors:

1. Ensure Maven dependencies are downloaded:
   ```bash
   mvn dependency:resolve
   ```

2. Verify the classpath is built correctly:
   ```bash
   mvn -Pdev dependency:build-classpath
   ```

### Agents JSON Not Found

If the agents descriptor cannot be loaded:

1. Check the agents JSON path is correct. Relative paths are resolved from `event-rule-backend`,
   where the script changes directory.
2. Verify the file exists (e.g. `samples/banking-fraud-agent/agents.json`)

### Kafka Connection Issues

If Kafka connection fails:

1. Verify Kafka is running: `docker ps` or check your Kafka service
2. Check the bootstrap servers address is correct
3. Ensure topics defined in `agents.json` subscriptions exist in Kafka

### Decision Archive Not Found

`ruleset.path` is resolved on the machine running the job. Check that the archive exists at that
path, relative to `event-rule-backend` or absolute.

## Development Profile

The script uses Maven's `dev` profile which changes Flink dependencies from `provided` to `compile`
scope. This ensures Flink libraries are included in the classpath for local execution.

## Alternative: Using Maven Exec Plugin

You can also run using Maven directly:

```bash
mvn -Pdev compile exec:java \
  -Dexec.mainClass="com.ibm.di.event.agent.ptf.AgentBasedPTFJob" \
  -Dexec.args="localhost:9092 ../samples/banking-fraud-agent/agents.json" \
  -Dexec.classpathScope=compile
```

## Alternative: Using the Shaded JAR

The shaded JAR's main class, `AgentJobLauncher`, selects the backend with `EVENT_RULE_RUNTIME`
(`datastream` by default, or `ptf`). On a Flink cluster:

```bash
flink run -c com.ibm.di.event.agent.ptf.AgentBasedPTFJob \
  event-rule-backend/target/event-rule-backend-1.0-SNAPSHOT.jar \
  kafka:29092 /opt/flink/samples/airline-agent/agents.json
```

With Docker, `samples/airline-agent/scripts/deploy-ptf.sh` submits the job to the Flink container.
