# Agent Pattern Design

Authoritative reference for the Agent pattern used by Event Rule: how `agents.json` is
structured (agents plus use-case metadata) and how it drives execution inside the Apache Flink
job and the BFF/dashboard.

> For step-by-step tutorials, see [`../TUTORIAL.md`](../TUTORIAL.md).
> For chaining, multi-input, parallel-processing, and other non-core patterns, see
> [`ADVANCED_PATTERNS.md`](ADVANCED_PATTERNS.md).

## Table of Contents

1. [Overview](#1-overview)
2. [Conceptual Model](#2-conceptual-model)
3. [`agents.json` — Agent Descriptor Schema](#3-agentsjson--agent-descriptor-schema)
4. [`agents.json` — Use-Case Properties](#4-agentsjson--use-case-properties)
5. [How the Job and the BFF Use `agents.json`](#5-how-the-job-and-the-bff-use-agentsjson)
6. [State Management](#6-state-management)
7. [Rule Execution Context](#7-rule-execution-context)
   - 7.1 [The `entityField` Contract — Input and Output](#71-the-entityfield-contract--input-and-output)
   - 7.2 [The `outbounds` Contract — Data Model JSON Names](#72-the-outbounds-contract--data-model-json-names)
   - 7.3 [Decision Service Design Guide](#73-decision-service-design-guide)
   - 7.4 [Scheduling](#74-scheduling)
8. [Flink DAG Architecture](#8-flink-dag-architecture)
9. [Agent Descriptor Field Reference](#9-agent-descriptor-field-reference)
10. [Best Practices](#10-best-practices)
11. [Further Reading](#11-further-reading)

---

## 1. Overview

Event Rule processes Kafka event streams through **Agents**: stateful Flink processing units
configured entirely by JSON, with no application code required per use case. A single descriptor
file drives everything:

| Part of `agents.json`                                  | Consumed by        | Purpose                                                                          |
|---------------------------------------------------------|--------------------|-----------------------------------------------------------------------------------|
| `agents[]`                                              | Flink (backend)    | Defines the Agents: subscriptions, state, ruleset wiring, and outputs            |
| Top-level properties (`name`, `topics[]`, `outboundTopics[]`, …) | BFF (frontend API), Flink | Use-case metadata: how the dashboard names and labels the topics, and per-topic source idleness |

The file lives under `samples/<use-case>/agents.json`. See
[`samples/banking-fraud-agent/`](../samples/banking-fraud-agent/) for a complete working example.

## 2. Conceptual Model

### Agent

An **Agent** is a stateful processing unit that:

- Subscribes to one or more event types, each on a single Kafka topic
- Maintains state for a specific entity type (account, customer, product, etc.), keyed by an
  extracted entity ID
- Keeps a bounded event history per subscription, governed by a configurable *horizon*
- Applies a ruleset (an IBM Decision Intelligence archive) to process events with full context
- Generates outbound events, routed to designated Kafka topics by event type

### Event Rule

An **Event Rule** is a business rule, authored in IBM Decision Designer, that decides what to do
with an event given everything the Agent already knows about the related entity. Rules are
packaged into a Decision Service Archive (a `.jar`) referenced by `agents.json`. The Agent itself
is a neutral execution engine — it does not interpret rule logic, it only builds the input context
and applies the archive.

---

## 3. `agents.json` — Agent Descriptor Schema

`agents.json` is a JSON document (either a bare array of agents or an object with a top-level
`agents` array) consumed by `AgentBasedDataStreamJob`, the Flink entry point. It defines one or
more Agents.

> **Note:** The abbreviated example below uses generic field names (`$.accountId`,
> `account-updates`) to illustrate the schema shape. The canonical banking fraud reference
> implementation uses `$.accountNumber` and topic `agent-account-events`. Refer to
> [`../samples/banking-fraud-agent/agents.json`](../samples/banking-fraud-agent/agents.json) for
> the real field names.

The `operation` field is **required** on every subscription. It names the IBM Decision
Intelligence operation that processes events of that type. Use an **empty string** (`""`) when an
event should be added to the Agent's history for context but must not trigger any rule-engine
processing (see *Subscribe-for-context pattern* below).

```json
{
  "agents": [
    {
      "agentId": "fraud-detection-agent",
      "name": "Fraud Detection Agent",
      "entityType": "account",
      "entityKeyExtractor": "$.accountId",
      "subscriptions": [
        {
          "eventType": "transaction",
          "topic": "transactions",
          "operation": "EvaluateTransaction",
          "horizon": {
            "type": "hybrid",
            "countLimit": 100,
            "timeLimit": "24h",
            "description": "Keep last 100 transactions or 24 hours"
          }
        },
        {
          "eventType": "account-update",
          "topic": "account-updates",
          "operation": "",
          "horizon": {
            "type": "count",
            "value": 1,
            "description": "Keep only latest account update (context only — no processing)"
          }
        }
      ],
      "ruleset": {
        "path": "/opt/rulesets/fraud-detection.jar",
        "archiveControlTopic": "agent-banking-archive-control",
        "operations": [
          {
            "operation": "EvaluateTransaction",
            "entityField": "theAccount",
            "currentEventField": "theTransaction",
            "history": [
              { "eventType": "transaction", "field": "thePastTransactions" }
            ],
            "outbounds": [
              { "field": "theFraudAlerts", "eventType": "Alert" }
            ]
          }
        ]
      },
      "outputs": [
        { "eventType": "Alert", "topic": "fraud-alerts" },
        { "eventType": "approved-transaction", "topic": "approved-transactions" }
      ],
      "stateConfig": {
        "ttl": "30d",
        "backend": "rocksdb"
      },
      "parallelism": 8
    }
  ]
}
```

### Subscription field reference

| Field       | Required | Description                                                                                                                                                                                    |
|-------------|----------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `eventType` | yes      | Logical event-type label; used as the history-map key.                                                                                                                                       |
| `topic`     | yes      | Single Kafka topic whose events belong to this subscription.                                                                                                                                  |
| `operation` | **yes**  | IBM Decision Intelligence operation name to invoke. Set to `""` (or any blank string) to subscribe for context only — the rule engine is **not** invoked and no output event is emitted.     |
| `horizon`   | no       | Controls how many historical events are retained per entity. Defaults to `{"type":"count","value":0}` (no history).                                                                          |

### Subscribe-for-context pattern

An agent may subscribe to an event type purely for context — the event must be present in
history when a subsequent event triggers processing, but its own arrival should not invoke the
rule engine. Set `"operation": ""` to achieve this:

```json
{
  "eventType": "account-created",
  "topic": "account-created-events",
  "operation": "",
  "horizon": { "type": "count", "value": 1, "description": "Keep latest account record" }
}
```

When an `account-created` event arrives:

1. The event is added to the agent's history for the `account-created` bucket.
2. Horizon eviction is applied normally.
3. The rule engine is **not** invoked.
4. No output event is emitted.

The accumulated history is then available as context when a later event (e.g. `transaction`)
arrives and triggers the configured operation.

### One operation per event type

Each operation in a Decision Service processes exactly one event type. If a Decision Service must
handle several event types, declare one subscription per event type, each with its own distinct
`operation` name. Assigning the same operation name to two subscriptions within the same agent is
invalid and is rejected at startup.

---

## 4. `agents.json` — Use-Case Properties

Besides the `agents` array, the top level of `agents.json` accepts optional **use-case
properties**. They are read by the BFF (`event-rule-frontend/bff/server.js`) to auto-discover a
use case and drive the dashboard: which topics to subscribe to, how to label them, and where to
route decisions. The job also reads `topics[].idleness` to configure the event sources.

There is no separate descriptor file: every property below lives next to `agents` in the same
`agents.json`. **All of them are optional.** When a property is missing, the BFF derives it from
the agents (see *Defaults* below).

```json
{
  "name": "Banking — Fraud Detection Agent",
  "flinkJobName": "Banking Fraud Detection Agent",
  "description": "Detect potentially fraudulent transactions.",
  "consumerGroupId": "event-rule-bff-banking-fraud-detection-agent",
  "topics": [
    { "name": "agent-transactions",   "type": "Transaction",  "label": "Transaction Events" },
    { "name": "agent-account-events", "type": "AccountEvent", "label": "Account Events", "idleness": "30s" }
  ],
  "outboundTopics": [
    { "name": "agent-fraud-alerts",   "type": "Alert",       "label": "Fraud Alerts" },
    { "name": "agent-banking-errors", "type": "agent-error", "label": "Agent Errors" }
  ],
  "agents": [ … ]
}
```

### Field reference

| Field                       | Required | Description                                                                                                             |
|------------------------------|----------|---------------------------------------------------------------------------------------------------------------------------|
| `name`                       | no       | Human-readable use-case name, shown in the use-case selector.                                                          |
| `description`                | no       | Short description shown on the use-case tile.                                                                          |
| `icon`                       | no       | Icon shown on the use-case tile.                                                                                       |
| `flinkJobName`               | no       | Must match the Flink job display name so the BFF can detect whether the job is running.                                |
| `consumerGroupId`            | no       | Kafka consumer group ID for this use case.                                                                             |
| `topics[].name`              | yes (if `topics` is set) | Kafka topic to consume (an inbound/input topic — mirrors a `subscriptions[].topic` in `agents`).       |
| `topics[].type`              | yes (if `topics` is set) | Logical event-type label shown in the UI and used as the WebSocket `topicType`.                        |
| `topics[].label`             | yes (if `topics` is set) | Display label in the dashboard.                                                                         |
| `topics[].idleness`          | no       | Flink watermark idleness of this topic's source, e.g. `"5mn"`. Default `1mn`. Same format as `horizon.timeLimit` (`m` = months, use `mn` for minutes). Needed so quiet topics do not hold back event-time schedule timers. Read by the job, so it requires `topics` to be declared. See [§7.4](#74-scheduling). |
| `outboundTopics[].name`      | yes (if `outboundTopics` is set) | Kafka topic carrying Agent output — mirrors an `outputs[].topic` in `agents`.                  |
| `outboundTopics[].type`      | yes (if `outboundTopics` is set) | Logical event-type label for the output.                                                       |
| `outboundTopics[].label`     | yes (if `outboundTopics` is set) | Display label in the dashboard.                                                                |

### Defaults

When a use-case property is omitted, the BFF computes it from `agents` (the same computation is
applied to agents created through the wizard / REST API):

| Property          | Default                                                                                                           |
|-------------------|--------------------------------------------------------------------------------------------------------------------|
| `name`            | The agent's `name` for a single-agent use case, otherwise the title-cased use-case folder name.                    |
| `flinkJobName`    | `<use-case>-job`                                                                                                   |
| `consumerGroupId` | `event-rule-bff-<use-case>`                                                                                        |
| `topics`          | Every `subscriptions[].topic` of every agent (de-duplicated), labelled with its `eventType`.                       |
| `outboundTopics`  | Every `outputs[].topic` plus every `stateConfig.changeLogTopic` (typed `entity-changelog`).                         |

Declaring `topics` / `outboundTopics` explicitly lets you set friendlier labels, and — for
`topics` — the source `idleness`. When an agent is added, replaced or removed through the BFF REST
API, the declared `topics` / `outboundTopics` are dropped from the file (they would be stale) and
derived again; re-add them by hand if you need custom labels or idleness.

See [`event-rule-frontend/bff/README.md`](../event-rule-frontend/bff/README.md) for the full BFF REST API that reads
and writes this file.

---

## 5. How the Job and the BFF Use `agents.json`

```
samples/<use-case>/agents.json
├── agents[]                 ──► AgentBasedDataStreamJob (Flink) ──► reads Kafka topics, applies rules,
│                                                                    writes Kafka output topics
└── name, topics[], …        ──► BFF server.js ──► same Kafka topics (by name) ──► WebSocket ──► Dashboard
    (use-case properties)    ──► AgentBasedDataStreamJob: topics[].idleness (source idleness)
```

- `agents` is the **execution** contract: it tells Flink what to subscribe to, how much
  history to keep, which Decision Service operation to invoke, and where to publish results.
- The top-level use-case properties are the **presentation** contract: they tell the BFF/dashboard
  which of those same Kafka topics to display, and how to label them.
- Topic names declared in `topics[]` / `outboundTopics[]` must agree with those used by the
  agents. The agents are the source of truth for what actually runs; when the use-case properties
  are omitted the BFF derives them from the agents.
- A single event's journey: **Kafka → Flink Agent (`agents`-driven) → Decision Service
  archive → Kafka output topic → BFF (`topics` / `outboundTopics`-driven) → WebSocket → Dashboard.**

---

## 6. State Management

### Entity Type

- Defines the business entity the agent manages (e.g., `account`, `customer`, `order`, `product`)
- Used as the keying strategy for Flink state partitioning
- All events for the same entity are processed by the same agent instance, ensuring consistent
  state access and updates

### Entity Key Extractor

A JSONPath expression (`entityKeyExtractor`) used to extract the entity ID from each event, e.g.:

- `$.accountId` — extract the `accountId` field
- `$.customer.customerId` — extract a nested `customerId`
- `$.order.orderId` — extract `orderId` from an `order` object

It can be set at agent level (applies to every subscription) and overridden per subscription
(`subscriptions[].entityKeyExtractor`) when topics carry the entity ID at different paths. A
subscription without its own value inherits the agent-level one; every non-`Schedule`
subscription must resolve to a value (`""` is the explicit single-partition opt-in). All
subscriptions must yield the **same entity ID** for the same real-world entity, because events
are keyed after the subscription streams are merged.

### Event History Horizon

Controls how many events are kept in history per subscription.

**Count-based:**
```json
{ "horizon": { "type": "count", "value": 100, "description": "Keep last 100 events" } }
```
Maintains a fixed-size deque; when a new event arrives and size exceeds the limit, the oldest
event is evicted. O(1) insertion and eviction.

**Time-based:**
```json
{ "horizon": { "type": "time", "timeLimit": "24h", "description": "Keep events from last 24 hours" } }
```
Supported time units: `s` (seconds), `mn` (minutes), `h` (hours), `d` (days), `m` (**months**,
30 days). Note that `m` is months, not minutes. Events older than the
horizon are evicted using the current event's timestamp as the reference.

**Hybrid:**
```json
{ "horizon": { "type": "hybrid", "countLimit": 1000, "timeLimit": "7d", "description": "Keep up to 1000 events or 7 days" } }
```
Combines both limits, preventing unbounded state growth. **Recommended for production.**

**Default:** `{"type": "count", "value": 0}` (no history retained).

### Agent State Structure

Each Agent persists one `AgentState` object per entity in Flink keyed state (RocksDB):

- **Entity data** — the mutable business object (e.g. `Account`), read/written via the
  `entityField` name configured in `ruleset.operations[]`
- **Event histories** — a map of event-type → bounded deque of historical payloads
- **Metadata** — entity ID, entity type, creation/update timestamps, event count

---

## 7. Rule Execution Context

The `generic` context format is the standard execution contract between an Agent and the Decision
Service. It is generic by design — the same signature works for any domain (banking, retail,
IoT, etc.) without modification. Domain-specific field names are exposed via aliases configured
per-operation in `ruleset.operations[]`.

| Parameter               | Type            | Direction | Description                                                                                                                                       |
|--------------------------|-----------------|-----------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
| `entity`                 | Object          | Input     | Nullable correlated object (e.g. account profile). `null` for new entities. Placed in the input context under the alias from `entityField` (e.g. `"theAccount"`; `"entity"` when unset). After execution the framework reads the updated entity from the result under `outboundEntityField` if set, otherwise under `entityField`, and persists it. See [§7.1](#71-the-entityfield-contract--input-and-output). |
| `currentEvent`           | Object          | Input     | The event that triggered this processing cycle. Placed in the input context under the alias from `currentEventField` (e.g. `"theTransaction"`).  |
| `now`                    | ISO-8601 String | Input     | Timestamp of the triggering event, derived from the Kafka payload. Deterministic; checkpoint-safe.                                                |
| `processingTime`         | ISO-8601 String | Input     | Wall-clock time at which the Decision Service executes. May differ from `now` under late arrival or replay.                                       |
| `<historyField>`         | Object[]        | Input     | Per-event-type history arrays, named by `history[].field` mappings (e.g. `thePastTransactions`). Present in the input context; not read from the result. |
| `<outbounds[n].field>`   | Object[]        | Output    | Events produced by the Decision Service. The framework reads each field declared in `outbounds[].field` from the **result object** — these fields are never pre-populated in the input. Defaults to `"outputEvents"` when blank. See [§7.2](#72-the-outbounds-contract--data-model-json-names). |

**Example input context** for the banking `EvaluateTransaction` operation
(`entityField: "theAccount"`, `currentEventField: "theTransaction"`,
history mappings: `thePastTransactions`, `thePastAccountEvents`):

```json
{
  "now":                  "2025-01-15T14:22:00Z",
  "processingTime":       "2025-01-15T14:22:00.050Z",
  "theAccount":           { "accountNumber": "ACC-42", "balance": 4000.00, "status": "ACTIVE" },
  "theTransaction":       { "amount": 3500.00, "country": "Germany", "channel": "ONLINE" },
  "thePastTransactions":  [ { "amount": 200.00 } ],
  "thePastAccountEvents": []
}
```

The `theFraudAlerts` field is **not** present in the input context — it is a field in the
Decision Service result object, read by the framework after execution (see [§7.2](#72-the-outbounds-contract--data-model-json-names)).

The rule engine returns a result object. The framework reads back the entity field (updated
entity state persisted to RocksDB) and the outbound-events field (emitted to Kafka as separate
events). The Agent does not interpret the rule logic — it is a neutral execution engine.

### 7.1 The `entityField` Contract — Input and Output

Two optional properties of `ruleset.operations[]` connect the entity in the Decision Service
input with the entity in the Decision Service output:

| Property              | Direction | Role                                                                                         |
|-----------------------|-----------|-------------------------------------------------------------------------------------------------|
| `entityField`         | Input (and Output by default) | Name under which the entity is placed in the input context. Also the name read from the result unless `outboundEntityField` is set. |
| `outboundEntityField` | Output    | Optional. Name under which the Decision Service result carries the updated entity. When unset or blank, `entityField` is used. |

By default the **same field name** is used in both directions; set `outboundEntityField` when the
Decision Service data model uses a different name for the entity in its result type than in its
input.

**Input — building the context:**

Before invoking the Decision Service, `GenericContextBuilder` reads the entity object from
`AgentState.entityData` (the value persisted from the previous processing cycle, or `null` for a
new entity) and places it in the input context under the field name configured by `entityField`.
If `entityField` is unset the alias `"entity"` is used; if it is explicitly blank (`""`) no entity
is injected.

**Output — reading the result:**

After the Decision Service returns, `StatefulAgentProcessor.updateEntityState()` looks for the
entity in the result object under `outboundEntityField` when set (non-blank), otherwise under
`entityField`. If the field is present and contains a JSON object, its value replaces
`AgentState.entityData` and is persisted to RocksDB. If the field is not a JSON object it is
ignored with a warning.

If the result does not contain the entity, the runtime falls back to the field named
`entityField` in the **inbound event payload**: when that is a JSON object, it is persisted as the
new entity state. This supports context-accumulation operations whose decision output is not the
entity itself. When neither source yields an object, entity state is left unchanged.

**Example with different input and output names** — `entityField: "theCustomer"`,
`outboundEntityField: "customer"` (from the retail sample):

```json
{
  "operation": "ProcessPurchaseEvent",
  "entityField": "theCustomer",
  "outboundEntityField": "customer",
  "currentEventField": "thePurchase"
}
```

The entity is sent to the Decision Service as `theCustomer`, and the updated entity is read back
from `result["customer"]`.

**Concrete example** — `entityField: "account"` (no `outboundEntityField`, so `account` is used
in both directions):

*`agents.json` configuration:*
```json
{
  "operation": "HandleAccountEvent",
  "entityField": "account",
  "currentEventField": "accountEvent",
  "history": [
    { "eventType": "AccountEvent", "field": "pastAccountEvents" }
  ],
  "outbounds": [
    { "field": "outboundEvents", "eventType": "AccountAlert" }
  ]
}
```

*Decision Service input (assembled by the rule engine):*
```json
{
  "now": "2025-06-01T10:00:00Z",
  "processingTime": "2025-06-01T10:00:00.012Z",
  "account": {
    "accountNumber": "ACC-001",
    "balance": 8000,
    "status": "ACTIVE"
  },
  "accountEvent": {
    "accountNumber": "ACC-001",
    "type": "BALANCE_UPDATE",
    "balance": 9500
  },
  "pastAccountEvents": []
}
```

*Decision Service output — entity updated, outbound events produced:*
```json
{
  "account": {
    "accountNumber": "ACC-001",
    "balance": 9500,
    "status": "ACTIVE"
  },
  "outboundEvents": [
    {
      "alertType": "BALANCE_CHANGE",
      "previousBalance": 8000,
      "newBalance": 9500
    }
  ]
}
```

The framework reads `result["account"]` → persists updated entity state (`entityField` is used
because `outboundEntityField` is not set).
The framework reads `result["outboundEvents"]` → emits each element as an outbound Kafka event.

**Allowed return combinations:**

The Decision Service result may contain the entity, outbound events, both, or neither. All
combinations are valid:

| Result contains                                          | Entity state | Kafka output   |
|----------------------------------------------------------|--------------|----------------|
| Entity field only (`outboundEntityField` or `entityField`) | Updated    | None emitted   |
| `outbounds` fields only                                  | Unchanged¹   | Events emitted |
| Both fields                                              | Updated      | Events emitted |
| Neither field                                            | Unchanged¹   | None emitted   |

¹ Unless the inbound event payload carries a JSON object under `entityField`, in which case that
object is persisted (payload fallback described above).

---

### 7.2 The `outbounds` Contract — Data Model JSON Names

`outbounds` is a list of entries, each mapping one **result-field name** in the serialized
Decision Service result to the **event type** that will be assigned to every element emitted from
that array. An operation can declare multiple `outbounds` entries when it returns several
distinct event-type arrays in a single rule invocation.

Each `outbounds` entry has two fields:

| Field       | Description                                                                                         |
|-------------|------------------------------------------------------------------------------------------------------|
| `field`     | **JSON name** of the data-model attribute in the Decision Service result that holds the events array |
| `eventType` | Event type label assigned to every element emitted from that array                                   |

The `field` value must match the **JSON name** of the corresponding attribute — not the
attribute's display name, and not necessarily its Java property name.

#### How Decision Designer names become JSON field names

IBM Decision Designer lets you assign two distinct names to a data model attribute:

- **Display name** (or attribute name) — the human-readable label used inside Decision Designer
  (e.g. `the fraud alert`). This name is used when writing rules.
- **JSON name** — the field name that appears in the serialized JSON payload when the Decision
  Service runs. This is the name the rule engine sees when it reads the result object.

These two names are not required to be identical. When configuring `outbounds[].field`, you
must use the **JSON name**.

#### Banking sample walkthrough — `EvaluateTransaction` and `FraudAssessment`

In the banking fraud detection sample, the `EvaluateTransaction` operation returns a
`FraudAssessment` result object. Inside the `FraudAssessment` data model, there is an attribute
that holds the list of fraud alerts produced by the rules:

```
Data model attribute
  Display name:  "the fraud alert"       ← used when writing rules in Decision Designer
  JSON name:     "theFraudAlerts"        ← appears in the serialized result JSON
```

Because the JSON name is `theFraudAlerts`, the serialized `FraudAssessment` result looks like:

```json
{
  "theAccount":    { "accountNumber": "ACC-42", "balance": 3500.00, "status": "ACTIVE" },
  "theFraudAlerts": [
    {
      "alertType": "LARGE_TRANSACTION",
      "riskScore": 30,
      "decision":  "REQUIRES_VERIFICATION"
    }
  ]
}
```

The descriptor must therefore reference this JSON name in the `outbounds` array:

```json
{
  "operation":         "EvaluateTransaction",
  "entityField":       "theAccount",
  "currentEventField": "theTransaction",
  "history": [
    { "eventType": "Transaction",   "field": "thePastTransactions" },
    { "eventType": "AccountEvent",  "field": "thePastAccountEvents" }
  ],
  "outbounds": [
    { "field": "theFraudAlerts", "eventType": "Alert" }
  ]
}
```

An operation that returns two distinct event-type arrays would declare two entries:

```json
"outbounds": [
  { "field": "fraudAlerts",   "eventType": "FraudAlert" },
  { "field": "approvedTxns",  "eventType": "ApprovedTransaction" }
]
```

The complete flow for each `outbounds` entry is:

```
EvaluateTransaction(...)
       ↓ returns
FraudAssessment
       ↓ serialized JSON field (JSON name of the data-model attribute)
FraudAssessment.theFraudAlerts
       ↓ referenced by
outbounds[0].field = "theFraudAlerts"
       ↓
StatefulAgentProcessor reads result["theFraudAlerts"] and emits each element
as an outbound Kafka event with eventType = outbounds[0].eventType = "Alert"
```

`StatefulAgentProcessor.buildOutputEvents()` iterates every `outbounds` entry and performs the
lookup `ruleResult.get(entry.field)`. If the field name does not exactly match the JSON name on
the result object, the lookup returns `null`, no events are emitted for that entry, and no error
is raised. When `outbounds` is absent or empty, the processor falls back to reading a single
`"outputEvents"` array with event type `"object"`.

> **Key rule:** `outbounds[].field` must match the **JSON name** of the data-model attribute
> — the name that appears in the serialized result, not the display name used inside
> Decision Designer and not a Java property name derived from it.

---

### 7.3 Decision Service Design Guide

This section explains how to design a Decision Service operation so that it contracts correctly
with the `agents.json` configuration.

#### What an operation receives

The Decision Service operation receives an input object assembled by `GenericContextBuilder`. For
the operation to work with the entity, it must declare an input parameter whose name matches
`entityField`. All other input fields (`currentEventField`, history arrays, `now`,
`processingTime`) follow the same aliasing pattern, using the names configured for each in
`ruleset.operations[]`.

#### What an operation must return

An operation **must** return a result object. That result may contain:

- The entity, exposed under `outboundEntityField` if configured, otherwise under the **same field
  name** as `entityField` — the framework reads this field to persist the updated entity back to
  Flink state.
- One or more outbound events arrays, each described by an `outbounds` entry — the framework
  iterates every entry, reads `result[entry.field]`, and emits each array element as a separate
  Kafka event with `eventType = entry.eventType`.
- Both of the above, for operations that both mutate the entity and emit events.
- Neither, for operations whose only effect is read-only rule evaluation with no side effects
  (though this is unusual in practice).

> **Key rule:** When the operation returns an entity, it must be exposed under the field name
> configured by `outboundEntityField` (or `entityField` when `outboundEntityField` is not set).
> When the operation emits events, each array must be under the
> field name configured in the matching `outbounds[].field` entry. Using a different name in
> either case means the framework will not find the value, and the entity state or the outbound
> events will be silently ignored.

#### How the data model connects to `agents.json`

The overall data flow from Decision Designer to the rule engine is:

```
Data model attribute
   ↓  JSON name (set in Decision Designer)
Decision Service result object field
   ↓  referenced by
agents.json ruleset.operations[] field
   ↓  used by
StatefulAgentProcessor to extract entity state and outbound events
```

| `agents.json` field                              | Decision Service equivalent                                              |
|--------------------------------------------------|---------------------------------------------------------------------------|
| `ruleset.operations[].operation`                 | Operation name in `META-INF/decisions/*.json`                             |
| `ruleset.operations[].entityField`               | JSON name of the input attribute carrying the entity (also the result attribute unless `outboundEntityField` is set) |
| `ruleset.operations[].outboundEntityField`       | JSON name of the result attribute carrying the updated entity (optional; defaults to `entityField`) |
| `ruleset.operations[].currentEventField`         | JSON name of the input attribute carrying the triggering event             |
| `ruleset.operations[].history[].field`           | JSON name of the input attribute carrying that event type's history array  |
| `ruleset.operations[].outbounds[].field`         | JSON name of the result attribute carrying one outbound events array       |
| `ruleset.operations[].outbounds[].eventType`     | Event type label assigned to each element of that array                    |

#### Banking sample walkthrough — `HandleAccountEvent`

In the banking fraud detection sample, the `HandleAccountEvent` operation is configured as:

```json
{
  "operation": "HandleAccountEvent",
  "entityField": "account",
  "currentEventField": "accountEvent",
  "history": [
    { "eventType": "AccountEvent", "field": "pastAccountEvents" }
  ]
}
```

The Decision Service operation `handleAccountEvent` is therefore designed to:

1. Receive the current `Account` entity under the input parameter named `account`.
2. Receive the triggering `AccountEvent` under the input parameter named `accountEvent`.
3. Apply rules to update the account from the event (e.g. update balance, status, profile fields).
4. Return a result object (`AccountEventProcessingResult`) that exposes the updated account under
   a JSON field also named `account`.

Here `account` is used both as the input parameter name and as the result field name because no
`outboundEntityField` is configured, so the `entityField` name applies in both directions (see
[§7.1](#71-the-entityfield-contract--input-and-output)).
This operation has no outbound events — the `outbounds` array is omitted — so the result type
need not include any events array.

For the complementary case that does emit events, see the `EvaluateTransaction` walkthrough in
[§7.2](#72-the-outbounds-contract--data-model-json-names): its result type
(`FraudAssessment`) exposes the entity under `theAccount` (`entityField`, also used for the result) and the alerts under
`theFraudAlerts` (`outbounds[0].field`).

#### Checklist for a correctly designed Decision Service operation

- [ ] The operation name matches `ruleset.operations[].operation` exactly (case-sensitive).
- [ ] The entity input parameter JSON name matches `entityField`.
- [ ] If the operation can modify the entity, the result object's JSON field name for the entity
      matches `outboundEntityField` (or `entityField` when `outboundEntityField` is not set).
- [ ] If the operation emits events, each array's JSON field name matches the corresponding
      `outbounds[].field` entry (use the JSON name from the data model, not the display name).
- [ ] Each element of every outbound events array is a plain object matching the schema expected
      by downstream consumers of the corresponding `outbounds[].eventType`.
- [ ] If the operation does not emit events, omit `outbounds` (or set it to `[]`) in `agents.json`
      and the result type need not include any events array.

---

### 7.4 Scheduling

A rule can ask the Agent to be woken up later for the current entity. The rule returns a list of
**`ScheduleRequest`** objects; when a requested time is reached, the Agent injects a **`Schedule`**
event for the same entity and runs the matching operation. Use it for reminders, timeouts,
follow-ups, and expiry checks.

#### Data model

The Decision Service data model must contain these two types.

`ScheduleRequest` — returned by a rule:

| Field         | Type   | Description                                                                                       |
|---------------|--------|-----------------------------------------------------------------------------------------------------|
| `id`          | String | Identifier, unique per entity. Required for `START` and `CANCEL`.                                 |
| `description` | String | Free text, handed back unchanged in the `Schedule` event.                                         |
| `timepoint`   | String | ISO-8601 UTC date-time, e.g. `2026-10-18T23:29:16Z`. Required for `START`.                        |
| `status`      | String | `START`, `CANCEL` or `CANCELALL`.                                                                  |

`Schedule` — the event received when a schedule fires:

| Field         | Type   | Description                                                                                       |
|---------------|--------|-----------------------------------------------------------------------------------------------------|
| `timestamp`   | String | The timer timestamp, ISO-8601 UTC, e.g. `2026-10-18T23:29:16Z`.                                   |
| `id`          | String | Id of the `ScheduleRequest` that fired.                                                            |
| `description` | String | Description of the `ScheduleRequest` that fired.                                                   |

#### Configuration

Scheduling is wired in `agents.json` with two additions. Neither exists unless you add it, so
existing agents are unaffected.

```json
{
  "subscriptions": [
    { "eventType": "Order", "topic": "orders", "operation": "HandleOrder",
      "horizon": { "type": "count", "value": 10 } },
    { "eventType": "Schedule", "operation": "HandleSchedule" }
  ],
  "ruleset": {
    "operations": [
      {
        "operation": "HandleOrder",
        "entityField": "order",
        "currentEventField": "orderEvent",
        "scheduleRequestsField": "scheduleRequests"
      },
      {
        "operation": "HandleSchedule",
        "entityField": "order",
        "currentEventField": "schedule"
      }
    ]
  }
}
```

- `ruleset.operations[].scheduleRequestsField` names the attribute of the operation's result that
  holds the `ScheduleRequest` list. Any operation may return schedule requests, including the
  operation that processes `Schedule` events (to re-arm a timer).
- The `Schedule` subscription declares **no `topic` and no `horizon`**. No Kafka source is created
  for it; the events are produced by the Agent itself. Its `operation` names the decision
  operation that processes the schedule. Specifying a `topic` on it is rejected at startup.

A decision operation such as `HandleSchedule` receives the `Schedule` event under
`currentEventField` and the entity under `entityField`, like any other operation, and can update
the entity, emit outbound events, or return further `ScheduleRequest` objects.

#### Part 1 — handling `ScheduleRequest` results

After the rule engine returns, `StatefulAgentProcessor` reads `scheduleRequestsField` from the result and
processes each request in order. Invalid requests (unknown status, missing `id` for `START` or
`CANCEL`, missing or unparsable `timepoint` for `START`, non-string `description`) are logged at
`WARN` and skipped; they never fail the event.

| Status      | Behaviour                                                                                                                       |
|-------------|-----------------------------------------------------------------------------------------------------------------------------------|
| `START`     | Registers a Flink **event-time** timer at `timepoint` for the current entity. If a request with the same `id` exists, it is cancelled first and replaced. |
| `CANCEL`    | If a request with that `id` exists for the entity, it is removed; otherwise nothing happens.                                    |
| `CANCELALL` | Removes every schedule requests registered for the current entity.                                                                        |

#### Part 2 — firing

When Flink calls `onTimer()` for an event-time timer, the Agent takes every `ScheduleRequest`
registered at that timestamp, removes it from state, builds a `Schedule` event for each one
(`timestamp` is the timestamp passed to `onTimer()`; `id` and `description` come from the request)
and calls `processElement()` with it. From there the event follows the normal path: operation
lookup through the `Schedule` subscription, rule execution, state update, output emission, and
any new `ScheduleRequest`s.

#### Watermarks and idleness

Event-time timers fire only when the operator's watermark passes the timepoint. The watermark is
the minimum over all inputs, so an input that never advances (a quiet topic) would hold timers
back indefinitely. Each Kafka source therefore marks itself **idle** after a period without
events, letting the watermark advance with the other inputs. The archive-control broadcast input is
always idle.

The idleness is set per topic in the top-level `topics[]` of `agents.json`, default **1 minute**:

```json
"topics": [
  { "name": "orders",  "type": "Order",  "label": "Orders",  "idleness": "5mn" },
  { "name": "returns", "type": "Return", "label": "Returns" }
]
```

The format is the same as `horizon.timeLimit`: `s`, `mn`, `h`, `d`. Note that `m` means
**months**, so one minute is `1mn`, not `1m`. A missing `topics` section or `idleness` falls back
to the default; an invalid or non-positive value fails startup.

Because event time comes from the event timestamps, a timer fires when events (on any non-idle
input) carry the watermark past its timepoint. A schedule set far in the future therefore fires
when the stream's event time reaches it, not when the wall clock does.

To make schedules more efficient, each time an event is received, the agent processor checks if some overdue timer should be triggered. If some are found, they fire immediately, anticipating the watermark. 

#### State and timer semantics

- Requests are kept in keyed state, per entity: by `id`, and by timepoint.
- Several requests may share a timepoint. They share one Flink timer. The timer is deleted
  (`deleteEventTimeTimer`) **only when no request is left at that timepoint**, whether through
  `CANCEL`, replacement by a `START` with a new timepoint, `CANCELALL`, or firing.
- A fired request is removed from state, so it fires once.
- An entity reset (the BFF reset sentinel) also cancels all schedules of that entity.
- The existing **processing-time** timer that enforces the event horizon is unchanged. The two
  timers are told apart by their time domain in `onTimer()`.

#### Checklist

- [ ] The data model has `ScheduleRequest` and `Schedule` types, with the JSON names above.
- [ ] The operation result exposes the request list under the name set in `scheduleRequestsField`.
- [ ] `agents.json` has a `Schedule` subscription with an `operation` and no `topic`.
- [ ] A decision operation for that name exists and is declared in `ruleset.operations[]`.
- [ ] `timepoint` values are UTC, in the `2026-10-18T23:29:16Z` form.
- [ ] Topics that can stay quiet have a suitable `idleness` in the top-level `topics[]` of `agents.json`.

---

## 8. Flink DAG Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Kafka Topics                            │
│  [transactions] [account-events] [orders] [product-views]       │
└────────────┬────────────────┬──────────────┬────────────────────┘
             │                │              │
             ▼                ▼              ▼
    ┌────────────────┐ ┌──────────────┐ ┌──────────────┐
    │ Kafka Source 1 │ │Kafka Source 2│ │Kafka Source 3│
    └────────┬───────┘ └──────┬───────┘ └──────┬───────┘
             │                │                 │
             ▼                ▼                 ▼
    ┌─────────────────────────────────────────────────────┐
    │   Union Stream (per-agent) + optional chained edge  │
    └────────────────────┬────────────────────────────────┘
                         ▼
    ┌─────────────────────────────────────────────────────┐
    │         KeyBy Entity ID (entityKeyExtractor)        │
    └────────────────────┬────────────────────────────────┘
                         ▼
    ┌───────────────────────────────────────-----────────────────────┐
    │   StatefulAgentProcessor (KeyedBroadcastProcessFunction)       │
    │   ┌─────────────────────────────────────────────────────────┐  │
    │   │ RocksDB keyed state: entity data + history + schedules  │  │
    │   └─────────────────────────────────────────────────────────┘  │
    │   1. Load/init state        2. Fire due schedules              │
    │   3. Resolve operation      4. Evict expired events            │
    │   5. Build rule context     6. Execute rules                   │
    │   7. Update entity state    8. Emit changelog output           │
    │   9. Apply schedule reqs   10. Update event history            │
    │  11. Persist state         12. Emit output events              │
    │  13. Register cleanup timer                                    │
    └────────┬───────────────────────────────────────────────────────┘
             ▼
    ┌────────────────────────────────────────────────────┐
    │   Output Router — filter by declared outputs[]     │
    └─────┬──────────────┬──────────────┬────────────────┘
          ▼              ▼              ▼
    [fraud-alerts] [risk-scores] [approved-transactions]
```

Key points:

- One Kafka source per subscription (no shared global consumer).
- `StatefulAgentProcessor` connects the keyed event stream with a **broadcast stream** of
  `ArchiveControlCmd` messages, enabling live archive hot-swap without job restart.
- Agent chaining (an Agent's output feeding another Agent's input, in-process) is resolved by
  `AgentChainBuilder.resolveTopology()`. Each agent is wired exactly once in a single pass;
  the chain topology (edges, entity-key normalisation, Kafka source suppression) is computed
  from descriptor metadata before any stream is created. See
  [`ADVANCED_PATTERNS.md`](ADVANCED_PATTERNS.md) for the full mechanics and examples.
- Checkpointing: `CHECKPOINT_INTERVAL_MS` env var (default `60000` ms), `EXACTLY_ONCE` mode.
- Job-level parallelism: top-level `"parallelism"` field in the descriptor, or `FLINK_PARALLELISM`.

---

## 9. Agent Descriptor Field Reference

```json
{
  "parallelism": 4,
  "agents": [
    {
      "agentId":            "my-agent",
      "name":               "My Agent (human-readable label)",
      "entityType":         "MyEntity",
      "entityKeyExtractor": "$.entityId",
      "parallelism":        0,
      "chainingEnabled":    true,
      "subscriptions": [
        {
          "eventType": "my-event",
          "topic":     "my-topic",
          "operation": "MyOperation",
          "horizon": {
            "type":        "hybrid",
            "countLimit":  100,
            "timeLimit":   "24h",
            "description": "Keep last 100 events or 24 hours"
          }
        },
        {
          "eventType": "Schedule",
          "operation": "HandleSchedule"
        }
      ],
      "ruleset": {
        "path":                "samples/my-use-case/MyService.jar",
        "archiveControlTopic": "my-archive-control",
        "operations": [
          {
            "operation":           "MyOperation",
            "entityField":         "myEntity",
            "outboundEntityField": "updatedMyEntity",
            "currentEventField":   "myEvent",
            "history": [
              { "eventType": "my-event", "field": "pastMyEvents" }
            ],
            "outbounds": [
              { "field": "myOutputEvents", "eventType": "MyOutput" }
            ],
            "scheduleRequestsField": "scheduleRequests"
          },
          {
            "operation":         "HandleSchedule",
            "entityField":       "myEntity",
            "currentEventField": "schedule"
          }
        ]
      },
      "outputs": [
        { "eventType": "MyOutput",    "topic": "output-topic", "description": "Processed output" },
        { "eventType": "agent-error", "topic": "error-topic",  "description": "Processing errors" }
      ],
      "stateConfig": {
        "ttl":                "30d",
        "backend":            "rocksdb",
        "checkpointInterval": "1m",
        "changeLogTopic":     "my-agent-entity-changelog"
      }
    }
  ]
}
```

| Field                                      | Required            | Description                                                                                     |
|--------------------------------------------|---------------------|---------------------------------------------------------------------------------------------------|
| `parallelism` (top-level)                  | No                  | Job-level Flink parallelism; overrides `FLINK_PARALLELISM` env var. Only read when the file is an object with an `agents` array (not a bare array). |
| `agentId`                                  | **Yes**             | Unique Agent identifier. Used as the Flink operator UID (`agent-processor-<agentId>`). Must be unique across all agents in the same job. |
| `name`                                     | No                  | Human-readable label for Flink operator display names and logs                                  |
| `entityType`                               | No                  | Informational entity label (e.g. `Account`, `Customer`)                                         |
| `entityKeyExtractor`                       | Conditional         | JSONPath (Jayway JsonPath) to extract the entity key from each event (e.g. `$.accountNumber`). Default for all subscriptions; required unless every subscription sets its own. |
| `parallelism` (agent-level)                | No                  | Flink task parallelism for this agent; `0` = inherit job default                                |
| `chainingEnabled`                          | No                  | Default `true`. When `false` the agent always reads from Kafka and its output is never forwarded in-process to another agent; no chain edges are created for it. See [`ADVANCED_PATTERNS.md`](ADVANCED_PATTERNS.md). |
| `subscriptions[].eventType`                | **Yes**             | Logical event-type label — used as the history-map key                                          |
| `subscriptions[].topic`                    | **Yes** (except `Schedule`) | Single Kafka topic to consume for this event type. Must be omitted on the `Schedule` subscription. |
| `subscriptions[].operation`                | **Yes**             | IBM Decision Intelligence operation name to invoke; `""` for history-only (no rule invocation)  |
| `subscriptions[].entityKeyExtractor`       | No                  | Overrides the agent-level `entityKeyExtractor` for this subscription's events. Omit to inherit. |
| `subscriptions[].horizon`                  | No                  | History retention. Defaults to `{"type":"count","value":0}` (no history retained)               |
| `subscriptions[].horizon.type`             | No                  | `count` (default), `time`, or `hybrid`                                                          |
| `subscriptions[].horizon.value`            | For `count`         | Max events to keep                                                                              |
| `subscriptions[].horizon.countLimit`       | For `hybrid`        | Max event count                                                                                 |
| `subscriptions[].horizon.timeLimit`        | For `time`/`hybrid` | Max age: number + unit `s` (seconds), `mn` (minutes), `h` (hours), `d` (days) or `m` (**months**, 30 days), e.g. `30s`, `5mn`, `24h`, `7d`, `1m` |
| `subscriptions[].horizon.description`      | No                  | Human-readable description for observability / logging                                          |
| `ruleset.path`                             | **Yes**             | Path to the Decision Intelligence archive JAR                                                   |
| `ruleset.archiveControlTopic`              | No                  | Kafka topic for zero-downtime archive reload; omit to disable                                   |
| `ruleset.operations[].operation`           | **Yes** (per op)    | IBM Decision Intelligence operation name this mapping applies to                                |
| `ruleset.operations[].entityField`         | No                  | Field name used for the entity in the Decision Service **input**. Also the field read from the result to persist updated entity state, unless `outboundEntityField` is set. Unset → input alias defaults to `"entity"`; blank (`""`) → no entity is injected into the input. See [§7.1](#71-the-entityfield-contract--input-and-output). |
| `ruleset.operations[].outboundEntityField` | No                  | Field name under which the Decision Service **result** carries the updated entity, when it differs from the input name (`entityField`). Unset or blank → `entityField` is used for the result too. See [§7.1](#71-the-entityfield-contract--input-and-output). |
| `ruleset.operations[].currentEventField`   | No                  | Domain-specific field name for the current event in the rule input (e.g. `"theTransaction"`). Unset → `"currentEvent"`; blank (`""`) → **spread mode**: every field of the event payload is merged directly into the root of the input context. |
| `ruleset.operations[].history[].eventType` | No                  | Event type whose history is mapped to a domain field                                            |
| `ruleset.operations[].history[].field`     | No                  | Domain field name for the per-type history array (e.g. `"thePastTransactions"`)                 |
| `ruleset.operations[].outbounds`           | No                  | List of outbound event descriptors. Each entry maps one result-field name to an event type. An operation may declare multiple entries to emit several distinct event types. Omit (or use `[]`) when the operation emits no events. See [§7.2](#72-the-outbounds-contract--data-model-json-names). |
| `ruleset.operations[].outbounds[].field`   | No                  | JSON name of the data-model attribute in the Decision Service result that carries one outbound events array. Must match the JSON name exactly (not the display name). Defaults to `"outputEvents"` when blank. |
| `ruleset.operations[].outbounds[].eventType` | No                | Event type assigned to every element emitted from that array. Defaults to `"object"` when blank. |
| `outputs[].eventType`                      | **Yes**             | Event type to route to this Kafka topic                                                         |
| `outputs[].topic`                          | **Yes**             | Kafka topic to publish matching output events to                                                |
| `outputs[].description`                    | No                  | Human-readable description for logging and monitoring                                           |
| `stateConfig.ttl`                          | No                  | How long to retain entity state after last event (e.g. `"30d"`, `"24h"`). Default: no TTL (state retained indefinitely). |
| `stateConfig.backend`                      | No                  | Preferred state backend hint: `"rocksdb"` or `"heap"`. Informational — actual backend is determined by the Flink job configuration. |
| `stateConfig.checkpointInterval`           | No                  | Preferred checkpoint interval hint (same format as `ttl`, e.g. `"1m"`). Informational — the global `CHECKPOINT_INTERVAL_MS` env var always takes precedence. |
| `stateConfig.changeLogTopic`               | No                  | Kafka topic name for entity-state change events. When set, the Flink job publishes a side-output record to this topic every time the rule engine returns a non-null, updated entity. Each record is a JSON object: `{ "entityId": "...", "entityType": "...", "timestamp": <epoch-ms>, "entity": { ... } }`. The Kafka record key is the entity ID (UTF-8). When absent or blank, no changelog is published. See [§10](#10-best-practices). |
| `ruleset.operations[].scheduleRequestsField` | No               | JSON name of the Decision Service result attribute carrying the list of `ScheduleRequest` objects returned by the rule. Omit to disable scheduling for the operation. See [§7.4](#74-scheduling). |
| `subscriptions[]` with `eventType: "Schedule"` | No             | Declares the event synthesised when a schedule fires. Must have **no** `topic` (no Kafka source is created) and needs no `horizon`. See [§7.4](#74-scheduling). |

The top-level use-case properties of `agents.json` (`name`, `topics[]`, `outboundTopics[]`, …) are
described in [§4](#4-agentsjson--use-case-properties); they include an optional
`topics[].idleness` (duration, default `1mn`) that sets the Flink watermark idleness of that
topic's source. See also [§7.4](#74-scheduling).



---

## 10. Best Practices

1. **Horizon selection** — count-based for fixed-size patterns, time-based for temporal patterns,
   hybrid for safety (prevents unbounded growth).
2. **Entity design** — choose an entity type that aligns with business logic; ensure the entity ID
   is always present in events.
3. **State management** — always configure TTL; use RocksDB for production; enable incremental
   checkpoints.
4. **Rule design** — access history efficiently, update entity state incrementally, and return the
   updated entity under the configured `entityField` so the framework persists it as new state.
5. **Testing** — test with realistic event volumes, verify horizon eviction, and test state
   recovery from checkpoints.
6. **Event type naming** — use clear, hierarchical naming (e.g. `banking.transaction.credit`).
7. **Agent granularity** — keep agents focused on single responsibilities.
8. **Error handling** — always define error output topics for failed processing.
9. **Idempotency** — design rulesets to be idempotent for exactly-once processing.
10. **Entity changelog** — add `stateConfig.changeLogTopic` to any agent whose entity state
    should be observable downstream or auditable outside of Flink. The changelog stream allows
    other systems to consume the evolving entity state without reading Flink checkpoints. Name
    the topic clearly (e.g. `<agentId>-entity-changelog`) and ensure it exists in Kafka before
    starting the job (the BFF auto-creates it when the agent is managed via the REST API).

---

## 11. Further Reading

| Document                                                                  | Contents                                                                    |
|-----------------------------------------------------------------------------|------------------------------------------------------------------------------|
| [`ADVANCED_PATTERNS.md`](ADVANCED_PATTERNS.md)                              | Agent chaining, multi-input agents, parallel processing, worked examples    |
| [`../TUTORIAL.md`](../TUTORIAL.md)                                          | Step-by-step guide to building a new use case                               |
| [`RULE_ENGINE.md`](RULE_ENGINE.md)                                          | IBM Decision Intelligence `RuleEngineRunner` internals, archive hot-swap    |
| [`../README.md`](../README.md)                                              | Project overview, quick start, architecture                                 |

