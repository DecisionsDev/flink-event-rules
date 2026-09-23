# Agent Pattern Design

Authoritative reference for the Agent pattern used by Event Rule: how `agents.json` and
`descriptor.json` are structured, how they relate to each other, and how they drive execution
inside the Apache Flink job and the BFF/dashboard.

> For step-by-step tutorials, see [`../TUTORIAL.md`](../TUTORIAL.md).
> For chaining, multi-input, parallel-processing, and other non-core patterns, see
> [`ADVANCED_PATTERNS.md`](ADVANCED_PATTERNS.md).

## Table of Contents

1. [Overview](#1-overview)
2. [Conceptual Model](#2-conceptual-model)
3. [`agents.json` — Agent Descriptor Schema](#3-agentsjson--agent-descriptor-schema)
4. [`descriptor.json` — BFF Use-Case Descriptor](#4-descriptorjson--bff-use-case-descriptor)
5. [How They Work Together](#5-how-they-work-together)
6. [State Management](#6-state-management)
7. [Rule Execution Context](#7-rule-execution-context)
   - 7.1 [The `entityField` Contract — Input and Output](#71-the-entityfield-contract--input-and-output)
   - 7.2 [The `outbounds` Contract — Data Model JSON Names](#72-the-outbounds-contract--data-model-json-names)
   - 7.3 [Decision Service Design Guide](#73-decision-service-design-guide)
8. [Flink DAG Architecture](#8-flink-dag-architecture)
9. [Agent Descriptor Field Reference](#9-agent-descriptor-field-reference)
10. [Best Practices](#10-best-practices)
11. [Further Reading](#11-further-reading)

---

## 1. Overview

Event Rule processes Kafka event streams through **Agents**: stateful Flink processing units
configured entirely by JSON, with no application code required per use case. Two descriptor
files drive everything:

| File              | Consumed by       | Purpose                                                                          |
|--------------------|--------------------|-----------------------------------------------------------------------------------|
| `agents.json`      | Flink (backend)    | Defines the Agents: subscriptions, state, ruleset wiring, and outputs            |
| `descriptor.json`  | BFF (frontend API) | Tells the dashboard which topics to monitor/display and how to label them       |

Both files live together under `samples/<use-case>/`. See
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

## 4. `descriptor.json` — BFF Use-Case Descriptor

`descriptor.json` is a JSON document read by the BFF (`event-rule-frontend/bff/server.js`) to auto-discover a
use case and drive the dashboard: which topics to subscribe to, how to label them, and where to
route decisions. It is independent of Flink — the BFF never reads `agents.json` directly for
display purposes (though the two must reference the **same topic names**).

```json
{
  "name": "Banking — Fraud Detection Agent",
  "flinkJobName": "Banking Fraud Detection Agent",
  "consumerGroupId": "event-rule-bff-banking-fraud-detection-agent",
  "topics": [
    { "name": "agent-transactions",   "type": "Transaction",  "label": "Transaction Events" },
    { "name": "agent-account-events", "type": "AccountEvent", "label": "Account Events" }
  ],
  "outboundTopics": [
    { "name": "agent-fraud-alerts",   "type": "Alert",       "label": "Fraud Alerts" },
    { "name": "agent-banking-errors", "type": "agent-error", "label": "Agent Errors" }
  ]
}
```

### Field reference

| Field                       | Required | Description                                                                                                             |
|------------------------------|----------|---------------------------------------------------------------------------------------------------------------------------|
| `name`                       | yes      | Human-readable use-case name, shown in the use-case selector.                                                          |
| `flinkJobName`               | yes      | Must match the Flink job display name so the BFF can detect whether the job is running.                                |
| `consumerGroupId`            | yes      | Kafka consumer group ID used by the BFF's dedicated consumer for this use case.                                        |
| `topics[].name`              | yes      | Kafka topic to consume (an inbound/input topic — mirrors a `subscriptions[].topic` in `agents.json`).                 |
| `topics[].type`              | yes      | Logical event-type label shown in the UI and used as the WebSocket `topicType`.                                       |
| `topics[].label`             | yes      | Display label in the dashboard.                                                                                        |
| `outboundTopics[].name`      | yes      | Kafka topic carrying Agent output — mirrors an `outputs[].topic` in `agents.json`.                                    |
| `outboundTopics[].type`      | yes      | Logical event-type label for the output.                                                                                |
| `outboundTopics[].label`     | yes      | Display label in the dashboard.                                                                                        |

See [`event-rule-frontend/bff/README.md`](../event-rule-frontend/bff/README.md) for the full BFF REST API that reads
and writes this file.

---

## 5. How They Work Together

```
samples/<use-case>/
├── agents.json         ──► AgentBasedDataStreamJob (Flink) ──► reads Kafka topics, applies rules,
│                                                                writes Kafka output topics
└── descriptor.json      ──► BFF server.js ──► same Kafka topics (by name) ──► WebSocket ──► Dashboard
```

- `agents.json` is the **execution** contract: it tells Flink what to subscribe to, how much
  history to keep, which Decision Service operation to invoke, and where to publish results.
- `descriptor.json` is the **presentation** contract: it tells the BFF/dashboard which of those
  same Kafka topics to display, and how to label them.
- The two files must agree on topic names. `agents.json` is the source of truth for what actually
  runs; `descriptor.json` is derived from it for UI purposes (the BFF can generate a stub
  `descriptor.json` automatically from `agents.json` when an agent is created via its REST API).
- A single event's journey: **Kafka → Flink Agent (`agents.json`-driven) → Decision Service
  archive → Kafka output topic → BFF (`descriptor.json`-driven) → WebSocket → Dashboard.**

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
Supported time units: `s` (seconds), `m` (minutes), `h` (hours), `d` (days). Events older than the
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
| `entity`                 | Object          | Input     | Nullable correlated object (e.g. account profile). `null` for new entities. Placed in the input context under the alias from `entityField` (e.g. `"theAccount"`). After execution the framework also reads the result under the same field name to persist updated entity state. See [§7.1](#71-the-entityfield-contract--input-and-output). |
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

`entityField` is the single configuration property that connects the entity in the Decision
Service input with the entity in the Decision Service output. The **same field name** is used in
both directions.

**Input — building the context:**

Before invoking the Decision Service, `GenericContextBuilder` reads the entity object from
`AgentState.entityData` (the value persisted from the previous processing cycle, or `null` for a
new entity) and places it in the input context under the field name configured by `entityField`.

**Output — reading the result:**

After the Decision Service returns, `StatefulAgentProcessor.updateEntityState()` looks for the
same field name in the result object. If the field is present and contains a JSON object, its
value replaces `AgentState.entityData` and is persisted to RocksDB. If the field is absent or
`null`, entity state is left unchanged.

Because there is only one `entityField` property per operation, **the input field name and the
output field name are always identical**. This is a deliberate constraint, not an accidental
naming convention: the same name is used to carry the entity into the Decision Service and to
carry the (potentially modified) entity back out.

**Concrete example** — `entityField: "account"`:

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

The framework reads `result["account"]` → persists updated entity state.
The framework reads `result["outboundEvents"]` → emits each element as an outbound Kafka event.

**Allowed return combinations:**

The Decision Service result may contain the entity, outbound events, both, or neither. All
combinations are valid:

| Result contains           | Entity state | Kafka output   |
|---------------------------|--------------|----------------|
| `entityField` only        | Updated      | None emitted   |
| `outbounds` fields only   | Unchanged    | Events emitted |
| Both fields               | Updated      | Events emitted |
| Neither field             | Unchanged    | None emitted   |

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

- The entity, exposed under the **same field name** as `entityField` — the framework reads this
  field to persist the updated entity back to Flink state.
- One or more outbound events arrays, each described by an `outbounds` entry — the framework
  iterates every entry, reads `result[entry.field]`, and emits each array element as a separate
  Kafka event with `eventType = entry.eventType`.
- Both of the above, for operations that both mutate the entity and emit events.
- Neither, for operations whose only effect is read-only rule evaluation with no side effects
  (though this is unusual in practice).

> **Key rule:** When the operation returns an entity, it must be exposed under the field name
> configured by `entityField`. When the operation emits events, each array must be under the
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
| `ruleset.operations[].entityField`               | JSON name of the result attribute carrying the entity (input AND output)  |
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

The fact that `account` is used both as the input parameter name and as the result field name is
**intentional and required by the `entityField` contract** (see [§7.1](#71-the-entityfield-contract--input-and-output)).
This operation has no outbound events — the `outbounds` array is omitted — so the result type
need not include any events array.

For the complementary case that does emit events, see the `EvaluateTransaction` walkthrough in
[§7.2](#72-the-outbounds-contract--data-model-json-names): its result type
(`FraudAssessment`) exposes the entity under `theAccount` (`entityField`) and the alerts under
`theFraudAlerts` (`outbounds[0].field`).

#### Checklist for a correctly designed Decision Service operation

- [ ] The operation name matches `ruleset.operations[].operation` exactly (case-sensitive).
- [ ] The entity input parameter JSON name matches `entityField`.
- [ ] If the operation can modify the entity, the result object's JSON field name for the entity
      matches `entityField`.
- [ ] If the operation emits events, each array's JSON field name matches the corresponding
      `outbounds[].field` entry (use the JSON name from the data model, not the display name).
- [ ] Each element of every outbound events array is a plain object matching the schema expected
      by downstream consumers of the corresponding `outbounds[].eventType`.
- [ ] If the operation does not emit events, omit `outbounds` (or set it to `[]`) in `agents.json`
      and the result type need not include any events array.

---

## 8. Flink DAG Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Kafka Topics                             │
│  [transactions] [account-events] [orders] [product-views]       │
└────────────┬────────────────┬──────────────┬────────────────────┘
             │                │              │
             ▼                ▼              ▼
    ┌────────────────┐ ┌──────────────┐ ┌──────────────┐
    │ Kafka Source 1 │ │Kafka Source 2│ │Kafka Source 3│
    └────────┬───────┘ └──────┬───────┘ └──────┬───────┘
             │                │                 │
             ▼                ▼                 ▼
    ┌────────────────────────────────────────────────────┐
    │   Union Stream (per-agent) + optional chained edge  │
    └────────────────────┬───────────────────────────────┘
                         ▼
    ┌────────────────────────────────────────────────────┐
    │         KeyBy Entity ID (entityKeyExtractor)        │
    └────────────────────┬───────────────────────────────┘
                         ▼
    ┌─────────────────────────────────────────────────────┐
    │   StatefulAgentProcessor (KeyedBroadcastProcessFunction) │
    │   ┌───────────────────────────────────────────┐      │
    │   │ RocksDB keyed state: entity data + history │      │
    │   └───────────────────────────────────────────┘      │
    │   1. Load/init state   2. Resolve operation           │
    │   3. Evict expired     4. Build context               │
    │   5. Execute rules     6. Update entity state          │
    │   7. Append history    8. Persist state                │
    │   9. Emit outputs      10. Register cleanup timer       │
    └────────┬────────────────────────────────────────────┘
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
        }
      ],
      "ruleset": {
        "path":                "samples/my-use-case/MyService.jar",
        "archiveControlTopic": "my-archive-control",
        "operations": [
          {
            "operation":         "MyOperation",
            "entityField":       "myEntity",
            "currentEventField": "myEvent",
            "history": [
              { "eventType": "my-event", "field": "pastMyEvents" }
            ],
            "outbounds": [
              { "field": "myOutputEvents", "eventType": "MyOutput" }
            ]
          }
        ]
      },
      "outputs": [
        { "eventType": "MyOutput",    "topic": "output-topic", "description": "Processed output" },
        { "eventType": "agent-error", "topic": "error-topic",  "description": "Processing errors" }
      ],
      "stateConfig": {
        "ttl":            "30d",
        "backend":        "rocksdb",
        "changeLogTopic": "my-agent-entity-changelog"
      }
    }
  ]
}
```

| Field                                      | Required            | Description                                                                                     |
|--------------------------------------------|---------------------|---------------------------------------------------------------------------------------------------|
| `parallelism` (top-level)                  | No                  | Job-level Flink parallelism; overrides `FLINK_PARALLELISM` env var                              |
| `agentId`                                  | **Yes**             | Unique Agent identifier. Used as the Flink operator UID (`agent-processor-<agentId>`). Must be unique across all agents in the same job. |
| `name`                                     | No                  | Human-readable label for Flink operator display names and logs                                  |
| `entityType`                               | No                  | Informational entity label (e.g. `Account`, `Customer`)                                         |
| `entityKeyExtractor`                       | No                  | JSONPath to extract the entity key from each event (e.g. `$.accountNumber`)                     |
| `parallelism` (agent-level)                | No                  | Flink task parallelism for this agent; `0` = inherit job default                                |
| `subscriptions[].eventType`                | **Yes**             | Logical event-type label — used as the history-map key                                          |
| `subscriptions[].topic`                    | **Yes**             | Single Kafka topic to consume for this event type                                               |
| `subscriptions[].operation`                | **Yes**             | IBM Decision Intelligence operation name to invoke; `""` for history-only (no rule invocation)  |
| `subscriptions[].horizon.type`             | **Yes**             | `count`, `time`, or `hybrid`                                                                    |
| `subscriptions[].horizon.value`            | For `count`         | Max events to keep                                                                              |
| `subscriptions[].horizon.countLimit`       | For `hybrid`        | Max event count                                                                                 |
| `subscriptions[].horizon.timeLimit`        | For `time`/`hybrid` | Max age: e.g. `30s`, `5m`, `24h`, `7d`                                                           |
| `ruleset.path`                             | **Yes**             | Path to the Decision Intelligence archive JAR                                                   |
| `ruleset.archiveControlTopic`              | No                  | Kafka topic for zero-downtime archive reload; omit to disable                                   |
| `ruleset.operations[].operation`           | **Yes** (per op)    | IBM Decision Intelligence operation name this mapping applies to                                |
| `ruleset.operations[].entityField`         | No                  | Field name used for the entity in **both** the Decision Service input and the Decision Service result. The rule engine places the entity in the input context under this name; after execution it reads the result under the same name to persist updated entity state. See [§7.1](#71-the-entityfield-contract--input-and-output). |
| `ruleset.operations[].currentEventField`   | No                  | Domain-specific field name for the current event in the rule input (e.g. `"theTransaction"`)    |
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

