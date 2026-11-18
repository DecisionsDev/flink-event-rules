# Deploying Event Rules on Confluent Cloud for Apache Flink

This document describes how an Event Rules use case is built, deployed and operated on **Confluent
Cloud for Apache Flink** as implemented in this repository. The use case runs as a Process Table
Function (PTF): one agent per function call, with the ruleset archive and the descriptor bundled in
the function JAR.

For the local PTF runner see [`RUN_PTF.md`](RUN_PTF.md); for the descriptor schema see
[`AGENT_PATTERN_DESIGN.md`](AGENT_PATTERN_DESIGN.md).

---

## 1. Constraints this implementation is built for

- **No filesystem.** Confluent Cloud Flink runs statements on managed workers. There is no volume
  mount, so `agents.json` and the ruleset archives are carried inside the function JAR.
- **Artifacts.** A function is registered with `CREATE FUNCTION ... AS '<class>' USING JAR
  '<artifact-uri>'`. The JAR is uploaded with the Confluent CLI and referenced by its artifact URI.
- **The function is created with a public no-argument constructor.** The descriptor is therefore
  not passed to the constructor on Confluent Cloud.
- **No per-function job parameters.** The function cannot be configured with Flink job parameters
  per function instance. The descriptor selection is done with SQL call arguments only.
- **No broadcast input.** The Flink runtime used here gives a PTF one keyed input table. Ruleset
  updates are therefore not received through Kafka at runtime (see §7).

---

## 2. What is produced by the build

One Maven build (`-Pccloud`) produces two outputs from a use case's `agents.json`:

| Output | Content | Used by |
|---|---|---|
| `target/ccloud/<name>.jar` | Backend classes, the ruleset archives and reference CSV files named in the descriptor, and the `agents.json` itself | `confluent flink artifact create` |
| `target/ccloud/pipeline.sql`, `manifest.json`, `sql/*.sql` | The SQL program: function registrations, one input and one output view per agent, one statement set for the sinks | `ccloud-deploy-ptf.sh`, or pasted in the SQL workspace |

```
event-rule-backend/target/ccloud/
├── <name>.jar                              function artifact
├── pipeline.sql                            whole program, one script
├── manifest.json                           statements in execution order, artifact, bundled files
└── sql/
    ├── 001-function-event_rule_agent.sql
    ├── 002-function-EVENT_FIELD.sql
    ├── 003-function-EVENT_ENRICH.sql       only when an agent uses enrichment
    ├── 004-view-<agent>_in.sql
    ├── 005-view-<agent>_out.sql
    └── 006-statement-set.sql               all sink INSERTs, one statement
```

The statement files are numbered in execution order, and the statement-set file comes last. Each
statement file contains exactly one statement, because the Confluent CLI submits one statement at a
time.

### 2.1 Build command

From the repository root:

```bash
scripts/ccloud/ccloud-build.sh <use-case> [maven-flags...]
```

This runs, from `event-rule-backend`:

```bash
mvn package -DskipTests -Pccloud \
    -Dccloud.descriptor=samples/<use-case>/agents.json \
    -Dccloud.name=<use-case> \
    -Dccloud.baseDir=<repository root> \
    -Dccloud.classesDir=event-rule-backend/target/ccloud/classes \
    -Dccloud.outDir=event-rule-backend/target/ccloud \
    -Dccloud.jar=<use-case>.jar
```

The script checks that the JAR and `manifest.json` exist after the build.

Maven properties of the `ccloud` profile:

| Property | Default | Meaning |
|---|---|---|
| `ccloud.descriptor` | *(required)* | The `agents.json` to package. The build fails if it is missing. |
| `ccloud.name` | `event-rule-ccloud` | Name of the JAR (`<name>.jar`). |
| `ccloud.baseDir` | repository root | Directory that the paths in the descriptor (`ruleset.path`, `references[].file`) are relative to. |
| `ccloud.artifactUri` | *(empty)* | When set, it replaces the placeholder in the `USING JAR` clauses. |

The output directory is `event-rule-backend/target/ccloud/`. The standard `target/` output and the
standard fat JAR are not changed.

### 2.2 What the `ccloud` profile changes

1. **Flink, connectors and Log4j are `provided`.** Confluent Cloud supplies them.
2. **A reduced set of libraries is bundled.** The shade plugin uses its own `artifactSet`:
   `execution-api`, `engine-de-api` (with `annotations`), `jackson-databind` (with `jackson-core`
   and `jackson-annotations`), `jackson-datatype-jsr310`, JsonPath, and the agent classes.
   - `foundation` (about 80 MB) is not bundled. The class the runtime needs from it is loaded from the
     ruleset archive, which contains it. When `foundation` is on the classpath (standard build),
     `RuleEngineRunner` finds it there.
   - `ml-integration-runtime` (about 36 MB, with the AWS SDK) is excluded. A ruleset that scores a
     machine-learning model needs it: the exclusion must then be removed in the `ccloud` profile.
     None of the sample rulesets contains ML classes.
3. **The runtime files are copied into the JAR** (`ConfluentCloudPackager`, run by
   `exec-maven-plugin` in the `process-classes` phase), at the path that the descriptor gives:
   - each ruleset archive and each reference CSV file, at its path relative to `ccloud.baseDir`,
     for example `samples/<use-case>/<ruleset>.jar`;
   - the `agents.json`, at its path relative to `ccloud.baseDir`, for example
     `samples/<use-case>/agents.json`.

   A file that is missing fails the build and is named in the error. A path outside
   `ccloud.baseDir` also fails the build.
4. **The SQL is generated by the same code as the PTF runner**: `ConfluentCloudPackager` calls
   `AgentSqlPipelineBuilder` with the `CONFLUENT` dialect. The SQL therefore matches
   `PTF_PRINT_SQL=true PTF_DIALECT=confluent`.

Environment variables read by the packager, as for `AgentBasedPTFJob`: `CHAINING_ENABLED` (default
`true`), and the `AGENT_ERROR_TOPIC` / `AGENT_ERROR_TOPIC_<AGENT>` variables.

---

## 3. Generated SQL

### 3.1 Function registration

Every program registers the agent function `event_rule_agent` (the `StatefulAgentProcessTableFunction`
class), and `EVENT_FIELD`; `EVENT_ENRICH` is registered only when an agent uses enrichment. Each one is
`USING JAR` the artifact:

```sql
-- Register once; the artifact id comes from the Confluent Cloud artifact upload
CREATE FUNCTION `event_rule_agent` AS 'com.ibm.di.event.agent.ptf.StatefulAgentProcessTableFunction'
    USING JAR 'confluent-artifact://<artifact-id>/<version-id>';
```

The URI is the placeholder `confluent-artifact://<artifact-id>/<version-id>` unless
`-Dccloud.artifactUri` was given at build time. The deployment script replaces the placeholder
(§5).

### 3.2 Per agent: input view, function call, output view

For each agent, the generator creates:

- **One source per subscribed topic**: a `SELECT` over the Kafka topic (`FROM `<topic>``), which
  reads the raw value and produces the envelope fields (`entity_id`, `event_type`, `source_topic`,
  `payload`, `event_ts`) with the `EVENT_FIELD` function. Event types received through an agent
  chain are read from the upstream agent's `<upstream>_out` view instead, filtered on `out_type`, so
  they are not read from Kafka a second time.
- **`<agent>_in`**: a `CREATE VIEW` that `UNION ALL`s the sources (and chained inputs) of the agent.
- **`<agent>_out`**: a `CREATE VIEW` that calls the function:

```sql
CREATE VIEW `aircraft-agent_out` AS SELECT * FROM `event_rule_agent`(
    events => TABLE `aircraft-agent_in` PARTITION BY `entity_id`,
    on_time => DESCRIPTOR(`event_ts`),
    agents_file => 'samples/airtraffic-agent/agents.json',
    agent_id => 'aircraft-agent',
    uid => 'agent-processor-aircraft-agent')
```

The call arguments are:

| Argument | Meaning |
|---|---|
| `events` | The input view, partitioned by `entity_id` |
| `on_time` | `event_ts`, the event-time column |
| `agents_file` | Classpath path of the bundled `agents.json` (the same path as in the descriptor's location under `ccloud.baseDir`) |
| `agent_id` | The agent in that file |
| `uid` | `agent-processor-<agentId>`, the operator uid |

The function does not receive the descriptor in the SQL. It opens the bundled file, parses it, and
selects the agent with `agent_id`.

### 3.3 Sinks: one statement set

Output sinks, the error topic and the entity changelog are written with `INSERT INTO <topic>
(key, val)` inside a single `EXECUTE STATEMENT SET`, which is statement `006` in the file list.
Sinks are created only for topics that already exist (see §4), because Confluent Cloud topics are
tables of the environment's Kafka cluster and no Kafka DDL is generated.

### 3.4 Differences from the open-source dialect

- **Views are persistent** (`CREATE VIEW`, not `CREATE TEMPORARY VIEW`). Confluent Cloud has no
  temporary views, and the next statement must see the view. A view stores no data.
- **No Kafka DDL.** Topics are not created in the SQL.
- **The descriptor is not inlined.** The open-source dialect and the `agent_config` argument pass the
  whole descriptor JSON; the Confluent dialect passes `agents_file` and `agent_id` instead.
  `agent_config` is still accepted by the function when given, and takes precedence.

### 3.5 `manifest.json`

`manifest.json` is the contract with the deployment script:

```json
{
  "descriptor": "samples/banking-fraud-agent/agents.json",
  "artifact": {
    "file": "banking-fraud-agent.jar",
    "uri": "confluent-artifact://<artifact-id>/<version-id>",
    "uriPlaceholder": "confluent-artifact://<artifact-id>/<version-id>"
  },
  "bundled": [
    "samples/banking-fraud-agent/agents.json",
    "samples/banking-fraud-agent/Fraud_Detection_Service_1.6.0.jar"
  ],
  "statements": [
    { "order": 1, "kind": "function", "name": "event_rule_agent", "agentId": null,
      "file": "sql/001-function-event_rule_agent.sql" }
  ],
  "script": "pipeline.sql"
}
```

`kind` is `function`, `view` or `statement-set`. `order` is the execution order. `agentId` is set for
the statements of one agent and is `null` for shared ones.

---

## 4. Topics

`scripts/ccloud/ccloud-create-topics.sh <use-case>` creates the topics that the descriptor names:
subscription topics, output topics, `archiveControlTopic`, the state changelog topic and the dead-letter
topic. Existing topics are skipped.

- Data topics get `--partitions` partitions (default `3`).
- Topics whose name contains `archive-control` get **1 partition**.
- `--dry-run` prints the topics without creating them.

The topics must exist before the statements are submitted.

---

## 5. Deployment

`scripts/ccloud/ccloud-deploy-ptf.sh <use-case>` deploys the use case. It runs the build first when
`target/ccloud/<name>.jar` or `manifest.json` is missing, or with `--build`.

### 5.1 Environment variables

The Confluent Cloud scripts (`scripts/ccloud/*`) automatically source environment configuration files in the following order:
`.env.ccloud.local`, `.env.ccloud`, `.env.local`, `.env`.

Copy those variables from `.env.example` into the. local environment file you choose. 

| Variable | Description | CLI Option Override | Default / Auto-detection |
|---|---|---|---|
| `CCLOUD_ENV_ID` | Confluent Cloud Environment ID (e.g. `env-abc1234`) | `--environment <id>` | Active context in `confluent` CLI |
| `CCLOUD_CLUSTER_ID` | Kafka Cluster ID / Flink Database (e.g. `lkc-xyz5678`) | `--cluster <id>` | Active Kafka cluster in `confluent` CLI |
| `CCLOUD_COMPUTE_POOL_ID` | Flink Compute Pool ID (e.g. `lfcp-def9012`) | `--compute-pool <id>` | First available compute pool in env |
| `CCLOUD_CLOUD` | Cloud provider hosting Confluent Cloud resources | `--cloud <provider>` | `aws` (or resolved from compute pool) |
| `CCLOUD_REGION` | Cloud region where Confluent Cloud resources reside | `--region <region>` | `us-east-1` (or resolved from compute pool) |

### 5.2 Deployment options and steps

Options for `ccloud-deploy-ptf.sh`:

| Option | Meaning |
|---|---|
| `--environment <id>` | Confluent environment (overrides environment variables and current CLI context) |
| `--cluster <id>` | Kafka cluster, used as the Flink database |
| `--compute-pool <id>` | Flink compute pool |
| `--cloud <provider>` | Default `aws` |
| `--region <region>` | Default `us-east-1` |
| `--artifact-id <id>` | Reuse an uploaded artifact instead of uploading |
| `--build` | Rebuild before deploying |
| `--dry-run` | Print the SQL that would be submitted; nothing is uploaded or submitted |

Steps:

1. **Upload the artifact** with `confluent flink artifact create <use-case> --artifact-file
   target/ccloud/<name>.jar`, unless `--artifact-id` is given. If the output has no id, the script
   looks for an artifact with the use-case name. The URI is `confluent-artifact://<artifact-id>/<version-id>`
   when a version id is returned, otherwise `confluent-artifact://<artifact-id>`.
2. **Substitute the placeholder** in each statement file with that URI.
3. **Submit the statements** in `manifest.json` order. For each statement, the script deletes any
   statement with the same name (`confluent flink statement delete --force`, errors ignored), then runs
   `confluent flink statement create --compute-pool ... --database <cluster> --environment ... --wait`.

   The statement name is `<use-case>-<order>-<kind>-<name>`, with characters other than letters,
   digits, `_` and `-` replaced by `-`.

Requirements: the environment, Kafka cluster and compute pool must be set (by CLI option, environment variable, or in
`.env.ccloud` / `.env`), and the topics must exist (§4).

After deployment the script prints the commands for status, demo events and undeploy.

### Updating a deployment

The descriptor and the rulesets are part of the artifact. To change either, run the build, then the
deploy script again. The deploy script replaces statements that have the same name. The
artifact URI changes with each upload, so the statements are re-created with the new URI.

### Monitoring and publishing

- `scripts/ccloud/ccloud-job-status.sh <use-case>`: statement status, compute pool usage and error
  output.
- `scripts/ccloud/ccloud-launch-demo-scenario.sh <use-case>`: publishes the events of
  `demo-scenarios.json` to the input topics.

### Undeploy

`scripts/ccloud/ccloud-undeploy.sh <use-case> [--delete-artifact] [--dry-run]` deletes the statements
of the use case in reverse order (the manifest's statements, plus any active statement whose name
starts with the use-case name), then optionally deletes the artifact.

### Sample wrappers

Each sample under `samples/<use-case>/scripts/ccloud/` provides wrappers for these scripts:
`build.sh`, `create-topics.sh`, `deploy-ptf.sh`, `publish-ruleset.sh`, `job-status.sh`,
`undeploy.sh`, `launch-demo-scenario.sh`.

---

## 6. Runtime behaviour in the function

### 6.1 Descriptor selection

`StatefulAgentProcessTableFunction` takes the descriptor from the first source that applies:

| Source | When it is used |
|---|---|
| Constructor argument | Open-source Flink (`createTemporarySystemFunction`). Not used on Confluent Cloud. |
| `agent_config` argument | The descriptor JSON is given in the call. |
| `agents_file` + `agent_id` arguments | The bundled `agents.json`. This is the generated call on Confluent Cloud. Without `agent_id`, the file must contain exactly one agent. |

The descriptor is read on the first `eval()` of a function instance and kept for the lifetime of that
instance. The `open()` method reads only the job parameter `event-rule.agent-config`, which the
Confluent Cloud deployment does not set (see §1).

### 6.2 Ruleset

The ruleset is resolved by `RulesetResourceResolver`: the path is looked up on the filesystem first,
then on the classpath. A ruleset found on the classpath is copied to a temporary file
(`/tmp/ruleset-extracted-*.jar`) and loaded with a `URLClassLoader`. `RulesetRunnerProvider` warms
the runner up when the processor is created.

### 6.3 Timers

The function registers named event-time timers:

| Timer | Registered for | Effect when it fires |
|---|---|---|
| `sched-<epochMs>` | each pending schedule time point | Fires the schedules due at that time |
| `cleanup` | a time horizon configured on a subscription | Evicts events older than the horizon |
| `ttl` | `stateConfig.ttl` | Clears the entity state; the schedules are kept |

Timers are event-time: they fire when the watermark of the partition passes them.

When a schedule timer fires and the function instance has not yet received an event (for example
after a restart, when Flink restores the timers before any event arrives), the function has no
descriptor. It then **drops the timer and logs a warning**. The pending schedules and the entity
state are kept, and the next event on that entity registers its timers again. The schedules that were
due in that window are not processed. The `ttl` timer does not need the descriptor and is always
processed.

---

## 7. Known limitations

- **Restored schedule timers can be dropped after a restart.** Behaviour is described in §6.3. The
  descriptor cannot be provided by a job parameter on Confluent Cloud, so the loss can only be avoided
  by receiving an event before the timer fires.
- **No ruleset hot reload.** The PTF engine has no broadcast input. `archiveControlTopic` is ignored
  (a warning is logged). `scripts/ccloud/ccloud-publish-ruleset.sh` publishes LOAD, PREFETCH,
  ROLLBACK and NOOP commands, and single or chunked (`chunkId`, `chunkIndex`, `totalChunks`) payloads,
  to the `archive-control` topic. **The PTF engine does not read that topic**, so these commands have no
  effect on a deployed PTF function. To change the ruleset, rebuild the artifact and redeploy (§5).
  A future version of Flink (2.4) brings support for broadcast input, which, when supported in Confluent Cloud, would bring back this capability. 
- **No `sqlEnrichment`.** It is ignored with a warning. Use `references` with `enrichment`.
- **Artifact size.** The function JAR of `banking-fraud-agent` is about 7 MB compressed (13 MB
  uncompressed). The size of other samples differs by their ruleset archive and CSV files.
- **Artifact URI form.** The generated SQL uses `confluent-artifact://<artifact-id>/<version-id>`. The
  deployment script uses the version form only when the upload output contains a version id. This
  depends on what the Confluent CLI returns, which has not been verified against a live environment.
