# Advanced Agent and Event Rule Patterns

This document covers agent and event-rule patterns that go beyond a single agent processing a
single event type — chaining, multi-input agents, parallel/fan-out processing, and a design
pattern catalogue with worked examples.

> For the core descriptor schema (`agents.json`, `descriptor.json`) see
> [`AGENT_PATTERN_DESIGN.md`](AGENT_PATTERN_DESIGN.md). For a step-by-step build guide see
> [`../TUTORIAL.md`](../TUTORIAL.md).

> **Note on worked examples:** The Air Traffic Control example (§5) and the Banking
> multi-agent pipeline example (§6) are backed by runnable samples in this repository
> (see [`../samples/`](../samples/)). The Retail (§7) and IoT (§8) examples are
> **illustrative only** — they demonstrate valid descriptor patterns but are not included
> as runnable samples. Run only §5 and §6 against the live stack.

## Table of Contents

1. [Agent Chaining](#1-agent-chaining)
2. [Multi-Input Agents](#2-multi-input-agents)
3. [Parallel Processing](#3-parallel-processing)
4. [Broadcast State for Dynamic Configuration](#4-broadcast-state-for-dynamic-configuration)
5. [Worked Example: Air Traffic Control — Cross-Domain Chaining](#5-worked-example-air-traffic-control--cross-domain-chaining)
6. [Worked Example: Banking — Multi-Agent Pipeline](#6-worked-example-banking--multi-agent-pipeline)
7. [Worked Example: Retail — Order Fulfillment Pipeline](#7-worked-example-retail--order-fulfillment-pipeline)
8. [Worked Example: IoT — Anomaly Detection Pipeline](#8-worked-example-iot--anomaly-detection-pipeline)
9. [Design Pattern Catalogue](#9-design-pattern-catalogue)
10. [Best Practices](#10-best-practices)

---

## 1. Agent Chaining

Agents in the same `agents.json` file can be wired together: the output stream of one Agent
becomes part of the input stream of another — **without a round-trip through Kafka**.

`AgentChainBuilder` auto-detects chains at job startup by comparing every agent's declared
`outputs[].eventType` against every other agent's `subscriptions[].eventType`. When a match is
found the two agents are connected by a chain edge. The complete chain graph is topologically
sorted (Kahn's algorithm) before any stream is wired, so upstream agents are always wired before
their downstream consumers.

### How it works end-to-end

```
Kafka ──► [Agent A]
              │
              │  emits EventType-X
              │
         ┌────┴──────────────────────────────────────┐
         │  filter(eventType == EventType-X)          │  no Kafka hop
         │  map(re-key to downstream entityKey,       │
         │      set sourceTopic = downstream topic)   │
         └────────────────┬──────────────────────────┘
                          │
                          ▼
Kafka ──► [Agent B]  (union of Kafka sources + chain stream)
              │
              │  processes EventType-X with full entity state
              ▼
         Kafka sinks
```

**Four things happen automatically for every chain edge:**

1. **Edge detection** — `AgentChainBuilder.resolveTopology()` scans descriptor metadata and
   records all `upstream → downstream` pairs before any Flink stream is created.

2. **Single-pass wiring** — each agent is wired exactly once. The downstream agent's
   `wireAgent()` call receives the already-wired upstream output stream as an additional inbound
   argument. This guarantees that the Flink operator UID `agent-processor-<agentId>` is assigned
   to exactly one operator per job, which is required for Flink state management and redeployment.

3. **Envelope normalisation** — before the upstream stream reaches the downstream agent, a `map()`
   step re-extracts the entity ID from the payload using the downstream's own `entityKeyExtractor`
   (e.g. `$.airportCode`) and corrects `sourceTopic` to the downstream's configured subscription
   topic. This is essential when the two agents manage entities in different key spaces.

4. **Kafka source suppression** — the downstream agent's Kafka consumer for any event type
   already covered by a chain edge is suppressed. This prevents each event from being processed
   twice: once via the fast in-memory chain and again after the Kafka round-trip.

### Descriptor for a chained pipeline

```json
{
  "agents": [
    {
      "agentId": "fraud-detection-agent",
      "entityKeyExtractor": "$.accountId",
      "subscriptions": [
        { "eventType": "transaction", "topic": "transactions", "operation": "EvaluateTransaction" }
      ],
      "outputs": [
        { "eventType": "approved-transaction", "topic": "approved-transactions" },
        { "eventType": "fraud-alert",          "topic": "fraud-alerts" }
      ]
    },
    {
      "agentId": "risk-scoring-agent",
      "entityKeyExtractor": "$.accountId",
      "subscriptions": [
        {
          "eventType": "approved-transaction",
          "topic": "approved-transactions",
          "operation": "ScoreRisk",
          "horizon": { "type": "count", "value": 20 }
        }
      ],
      "outputs": [
        { "eventType": "risk-score", "topic": "risk-scores" }
      ]
    }
  ]
}
```

`AgentChainBuilder` detects that `risk-scoring-agent` subscribes to `approved-transaction`, which
`fraud-detection-agent` emits, and wires the edge automatically — no extra configuration required.

### Visualising the pipeline

```
[transactions] ──► Fraud Detector ──► approved-transaction ──► Risk Scorer ──► risk-score
                       │                   (in-process,              │
                       │                    no Kafka hop)            ▼
                       ▼                                        [risk-scores] topic
                  [fraud-alerts] topic
```

The `approved-transactions` Kafka topic still exists as a sink for the Fraud Detector.
External consumers (monitoring, replay, audit) can still read from it. But the Risk Scorer
receives events via the in-process chain, not from that topic.

### Cross-domain chaining (different entity key spaces)

When the two agents manage **different entity types** (e.g. Aircraft keyed by `$.aircraftId` and
Airport keyed by `$.airportCode`), the envelope normalisation step is critical.

Without it, the downstream agent's `keyBy(entityId)` would route events to the wrong partition
(the aircraft partition instead of the airport partition). The framework handles this
automatically: no extra configuration is needed beyond declaring the correct `entityKeyExtractor`
on each agent.

See the [Air Traffic Control example](#5-worked-example-air-traffic-control--cross-domain-chaining)
for a complete worked example.

### Constraints

> **Cycles are not supported.** Do not create descriptors where Agent A depends on output from
> Agent B and Agent B depends on output from Agent A. `AgentChainBuilder.resolveTopology()` runs
> Kahn's topological sort and throws `IllegalStateException` on any cycle, including self-loops
> and multi-hop cycles (A→B→C→A).

> **Declaration order for clarity.** Agents are always wired in the order they appear in
> `agents.json`. For readable descriptors, declare upstream agents before their downstream
> consumers.

---

## 2. Multi-Input Agents

A single Agent can subscribe to multiple event types, each with its own topic and horizon. This
enables building a comprehensive entity view from multiple data sources:

```json
{
  "agentId": "customer-360",
  "entityType": "customer",
  "entityKeyExtractor": "$.customerId",
  "subscriptions": [
    { "eventType": "transaction",   "topic": "transactions",   "operation": "EvaluateCustomer", "horizon": { "type": "hybrid", "countLimit": 50,  "timeLimit": "30d" } },
    { "eventType": "account-event", "topic": "account-events", "operation": "",                 "horizon": { "type": "count",  "value": 1 } },
    { "eventType": "product-view",  "topic": "product-views",  "operation": "",                 "horizon": { "type": "hybrid", "countLimit": 200, "timeLimit": "48h" } },
    { "eventType": "order",         "topic": "orders",         "operation": "",                 "horizon": { "type": "hybrid", "countLimit": 20,  "timeLimit": "7d"  } }
  ],
  "ruleset": { "path": "/opt/rulesets/customer-360.jar" },
  "outputs": [
    { "eventType": "customer-profile-update", "topic": "customer-profiles" },
    { "eventType": "customer-segment-change", "topic": "customer-segments" }
  ]
}
```

Each event type has its own operation; use `""` for types that should be stored for context but
not trigger rule processing (see *Subscribe-for-context pattern* in
[`AGENT_PATTERN_DESIGN.md`](AGENT_PATTERN_DESIGN.md#subscribe-for-context-pattern)).

---

## 3. Parallel Processing

Multiple Agents can read from the **same** Kafka topics independently, applying different rule
sets:

```json
{
  "agents": [
    {
      "agentId": "fraud-detector",
      "subscriptions": [{ "eventType": "transaction", "topic": "transactions", "operation": "EvaluateFraud" }],
      "ruleset": { "path": "/opt/rulesets/fraud.jar" },
      "outputs": [{ "eventType": "fraud-alert", "topic": "fraud-alerts" }],
      "parallelism": 8
    },
    {
      "agentId": "compliance-checker",
      "subscriptions": [{ "eventType": "transaction", "topic": "transactions", "operation": "CheckCompliance" }],
      "ruleset": { "path": "/opt/rulesets/compliance.jar" },
      "outputs": [{ "eventType": "compliance-violation", "topic": "compliance-violations" }],
      "parallelism": 4
    },
    {
      "agentId": "analytics-aggregator",
      "subscriptions": [{ "eventType": "transaction", "topic": "transactions", "operation": "AggregateAnalytics" }],
      "ruleset": { "path": "/opt/rulesets/analytics.jar" },
      "outputs": [{ "eventType": "analytics-metric", "topic": "analytics-metrics" }],
      "parallelism": 4
    }
  ]
}
```

Each transaction is evaluated simultaneously by all three Agents: fraud detection, compliance
checking, and analytics — each with its own independent rule set and output stream.

---

## 4. Broadcast State for Dynamic Configuration

Every Agent's Decision Service archive can be hot-swapped at runtime via a Kafka
`ArchiveControlCmd` message delivered through Flink broadcast state, without restarting the job:

```java
BroadcastStream<ArchiveControlCmd> configStream = env
    .addSource(new ArchiveControlSource())
    .broadcast(archiveControlDescriptor);

DataStream<EventEnvelope> processedStream = eventStream
    .keyBy(EventEnvelope::getEntityId)
    .connect(configStream)
    .process(new StatefulAgentProcessor(agent));
```

For the full reload sequence, supported archive URL schemes, message format, command types, and
the Deployment REST API, see
[`RULE_ENGINE.md`](RULE_ENGINE.md) and
[`../event-rule-frontend/bff/README.md`](../event-rule-frontend/bff/README.md).

---

## 5. Worked Example: Air Traffic Control — Cross-Domain Chaining

The Air Traffic Control sample (`samples/airtraffic-agent/`) is the canonical example of
cross-domain agent chaining: the Aircraft Agent and the Airport Agent manage **different entity
types** with **different key spaces** (`$.aircraftId` vs. `$.airportCode`).

The chain works as follows: the Aircraft Agent emits `TrackedAircraftEvent` events that carry
`$.airportCode` in their payload. At the chain boundary, a `map()` step re-extracts
`$.airportCode` and sets it as the new `entityId`, so Flink's `keyBy(entityId)` routes each
event to the correct airport partition rather than the aircraft partition. The Airport Agent's
Kafka consumer for `agent-tracked-aircraft-events` is suppressed while the chain is active,
preventing duplicate processing.

See [`samples/airtraffic-agent/README.md`](../samples/airtraffic-agent/README.md) for the
complete architecture diagram, data model, agent descriptors, and demo scenarios.

---

## 6. Worked Example: Banking — Multi-Agent Pipeline

A four-agent pipeline: fraud detection feeds risk scoring and account validation feeds risk
scoring for context.

```json
{
  "agents": [
    {
      "agentId": "fraud-detector",
      "name": "Fraud Detection Agent",
      "entityType": "account",
      "entityKeyExtractor": "$.accountId",
      "subscriptions": [
        {
          "eventType": "transaction",
          "topic": "transactions",
          "operation": "FraudDetectionService_transaction_op",
          "horizon": { "type": "hybrid", "countLimit": 100, "timeLimit": "24h" }
        }
      ],
      "ruleset": { "path": "/opt/rulesets/fraud-detection.jar" },
      "outputs": [
        { "eventType": "fraud-alert",       "topic": "fraud-alerts" },
        { "eventType": "clean-transaction", "topic": "clean-transactions" }
      ],
      "stateConfig": { "ttl": "30d", "backend": "rocksdb" },
      "parallelism": 8
    },
    {
      "agentId": "account-validator",
      "name": "Account Validation Agent",
      "entityType": "customer",
      "entityKeyExtractor": "$.customerId",
      "subscriptions": [
        {
          "eventType": "account-created",
          "topic": "account-created-events",
          "operation": "AccountValidationService_accountCreated_op",
          "horizon": { "type": "count", "value": 10 }
        }
      ],
      "ruleset": { "path": "/opt/rulesets/account-validation.jar" },
      "outputs": [
        { "eventType": "validated-account", "topic": "validated-accounts" },
        { "eventType": "invalid-account",   "topic": "invalid-accounts" }
      ],
      "stateConfig": { "ttl": "90d", "backend": "rocksdb" },
      "parallelism": 4
    },
    {
      "agentId": "risk-scorer",
      "name": "Risk Scoring Agent",
      "entityType": "account",
      "entityKeyExtractor": "$.accountId",
      "subscriptions": [
        {
          "eventType": "clean-transaction",
          "topic": "clean-transactions",
          "operation": "RiskScoringService_cleanTransaction_op",
          "horizon": { "type": "time", "value": "7d" }
        },
        {
          "eventType": "validated-account",
          "topic": "validated-accounts",
          "operation": "",
          "horizon": { "type": "count", "value": 1, "description": "Context only" }
        }
      ],
      "ruleset": { "path": "/opt/rulesets/risk-scoring.jar" },
      "outputs": [{ "eventType": "risk-score", "topic": "risk-scores" }],
      "stateConfig": { "ttl": "30d", "backend": "rocksdb" },
      "parallelism": 4
    },
    {
      "agentId": "compliance-checker",
      "name": "Compliance Checking Agent",
      "entityType": "account",
      "entityKeyExtractor": "$.accountId",
      "subscriptions": [
        {
          "eventType": "transaction",
          "topic": "transactions",
          "operation": "ComplianceService_transaction_op",
          "horizon": { "type": "time", "value": "1d" }
        },
        {
          "eventType": "account-created",
          "topic": "account-created-events",
          "operation": "",
          "horizon": { "type": "count", "value": 1, "description": "Context only" }
        }
      ],
      "ruleset": { "path": "/opt/rulesets/compliance.jar" },
      "outputs": [
        { "eventType": "compliance-violation", "topic": "compliance-violations" },
        { "eventType": "compliant-event",      "topic": "compliant-events" }
      ],
      "stateConfig": { "ttl": "7d", "backend": "rocksdb" },
      "parallelism": 4
    }
  ]
}
```

`clean-transaction` flows from `fraud-detector` into `risk-scorer` in-process via
`AgentChainBuilder`; `validated-account` is subscribed for context only.

---

## 7. Worked Example: Retail — Order Fulfillment Pipeline

A five-agent chained pipeline covering inventory validation, pricing, recommendations, loyalty,
and shipping.

```json
{
  "agents": [
    {
      "agentId": "inventory-validator",
      "name": "Inventory Validation Agent",
      "entityType": "product",
      "entityKeyExtractor": "$.productId",
      "subscriptions": [
        {
          "eventType": "order",
          "topic": "orders",
          "operation": "InventoryValidationService_order_op",
          "horizon": { "type": "time", "value": "1h" }
        }
      ],
      "ruleset": { "path": "/opt/rulesets/inventory-validation.jar" },
      "outputs": [
        { "eventType": "validated-order", "topic": "validated-orders" },
        { "eventType": "out-of-stock",     "topic": "out-of-stock-alerts" }
      ],
      "stateConfig": { "ttl": "7d", "backend": "rocksdb" },
      "parallelism": 6
    },
    {
      "agentId": "pricing-calculator",
      "name": "Pricing and Discount Agent",
      "entityType": "customer",
      "entityKeyExtractor": "$.customerId",
      "subscriptions": [
        {
          "eventType": "validated-order",
          "topic": "validated-orders",
          "operation": "PricingService_validatedOrder_op",
          "horizon": { "type": "count", "value": 20 }
        }
      ],
      "ruleset": { "path": "/opt/rulesets/pricing.jar" },
      "outputs": [{ "eventType": "priced-order", "topic": "priced-orders" }],
      "stateConfig": { "ttl": "90d", "backend": "rocksdb" },
      "parallelism": 4
    },
    {
      "agentId": "recommendation-engine",
      "name": "Product Recommendation Agent",
      "entityType": "customer",
      "entityKeyExtractor": "$.customerId",
      "subscriptions": [
        {
          "eventType": "product-view",
          "topic": "product-views",
          "operation": "RecommendationService_productView_op",
          "horizon": { "type": "hybrid", "countLimit": 50, "timeLimit": "7d" }
        }
      ],
      "ruleset": { "path": "/opt/rulesets/recommendations.jar" },
      "outputs": [{ "eventType": "recommendation", "topic": "product-recommendations" }],
      "stateConfig": { "ttl": "30d", "backend": "rocksdb" },
      "parallelism": 8
    },
    {
      "agentId": "loyalty-points",
      "name": "Loyalty Points Agent",
      "entityType": "customer",
      "entityKeyExtractor": "$.customerId",
      "subscriptions": [
        {
          "eventType": "priced-order",
          "topic": "priced-orders",
          "operation": "LoyaltyService_pricedOrder_op",
          "horizon": { "type": "time", "value": "365d" }
        }
      ],
      "ruleset": { "path": "/opt/rulesets/loyalty.jar" },
      "outputs": [{ "eventType": "loyalty-event", "topic": "loyalty-events" }],
      "stateConfig": { "ttl": "730d", "backend": "rocksdb" },
      "parallelism": 4
    },
    {
      "agentId": "shipping-router",
      "name": "Shipping Method Agent",
      "subscriptions": [
        { "eventType": "priced-order", "topic": "priced-orders", "operation": "ShippingService_pricedOrder_op" }
      ],
      "ruleset": { "path": "/opt/rulesets/shipping.jar" },
      "outputs": [{ "eventType": "shipping-assignment", "topic": "shipping-assignments" }],
      "parallelism": 4
    }
  ]
}
```

> **Illustrative only.** The Retail (this section) and IoT (§8) examples below demonstrate
> valid descriptor patterns but are not included as runnable samples in this repository.
> See [`../samples/README.md`](../samples/README.md) for the samples that ship with the project.

---

## 8. Worked Example: IoT — Anomaly Detection Pipeline

Three agents covering anomaly detection, predictive maintenance, and energy optimization.

> **Illustrative only.** See the note at the end of §7. This example also demonstrates that each
> subscription within the same agent must declare a **distinct** operation name — the three sensor
> subscriptions below use different operation names (`…_temperature_op`, `…_pressure_op`,
> `…_vibration_op`) because `AGENT_PATTERN_DESIGN.md §3` requires unique operation names per
> agent. (A single operation that handles all three sensor types via input-event dispatch is a valid
> Decision Service design, but each subscription must still name it distinctly; if they are truly
> identical, the runtime will reject the descriptor.)

```json
{
  "agents": [
    {
      "agentId": "anomaly-detector",
      "name": "IoT Anomaly Detection Agent",
      "subscriptions": [
        { "eventType": "temperature-reading", "topic": "temperature-sensors", "operation": "AnomalyDetectionService_temperature_op" },
        { "eventType": "pressure-reading",    "topic": "pressure-sensors",    "operation": "AnomalyDetectionService_pressure_op" },
        { "eventType": "vibration-reading",   "topic": "vibration-sensors",   "operation": "AnomalyDetectionService_vibration_op" }
      ],
      "ruleset": { "path": "/opt/rulesets/anomaly-detection.jar" },
      "outputs": [
        { "eventType": "anomaly-alert",  "topic": "anomaly-alerts" },
        { "eventType": "normal-reading", "topic": "normal-readings" }
      ],
      "parallelism": 12
    },
    {
      "agentId": "predictive-maintenance",
      "name": "Predictive Maintenance Agent",
      "subscriptions": [
        { "eventType": "anomaly-alert", "topic": "anomaly-alerts", "operation": "MaintenancePredictionService_anomalyAlert_op" },
        {
          "eventType": "normal-reading",
          "topic": "normal-readings",
          "operation": "",
          "horizon": { "type": "count", "value": 20, "description": "Baseline context only" }
        }
      ],
      "ruleset": { "path": "/opt/rulesets/predictive-maintenance.jar" },
      "outputs": [{ "eventType": "maintenance-required", "topic": "maintenance-alerts" }],
      "parallelism": 6
    },
    {
      "agentId": "energy-optimizer",
      "name": "Energy Optimization Agent",
      "subscriptions": [
        { "eventType": "temperature-reading", "topic": "temperature-sensors", "operation": "EnergyOptimizationService_sensorReading_op" },
        { "eventType": "power-reading",       "topic": "power-sensors",       "operation": "EnergyOptimizationService_sensorReading_op" }
      ],
      "ruleset": { "path": "/opt/rulesets/energy-optimization.jar" },
      "outputs": [{ "eventType": "optimization-command", "topic": "optimization-commands" }],
      "parallelism": 4
    }
  ]
}
```

`anomaly-detector`'s output (`anomaly-alert`) chains automatically into
`predictive-maintenance`'s input.

---

## 9. Design Pattern Catalogue

| Pattern                              | Description                                                                                           |
|----------------------------------------|--------------------------------------------------------------------------------------------------------|
| **Sequential Processing (Pipeline)**   | Events flow through agents in sequence, each agent's output feeding the next agent's input.           |
| **Parallel Processing (Fan-out)**      | Multiple agents process the same event independently, each producing different outputs.               |
| **Aggregation (Fan-in)**               | Multiple event types converge into a single agent for combined processing (multi-input agent).        |
| **Conditional Routing**                | Agents produce different output event types based on rule results, enabling dynamic downstream routing. |
| **Stateful Processing**                | Agents maintain state across events (e.g., customer profiles, running totals, time windows).           |
| **Subscribe-for-Context**              | An event type is retained in history but does not trigger rule execution (`"operation": ""`).         |
| **Cross-Domain Chaining**              | Two agents managing different entity types are chained; the framework re-keys envelopes at the boundary so each agent's `keyBy` partitions correctly. |

---

## 10. Best Practices

For foundational best practices (horizon selection, entity design, state management, rule
design, error handling, idempotency, and entity changelog), see
[`AGENT_PATTERN_DESIGN.md` §10](AGENT_PATTERN_DESIGN.md#10-best-practices).

The following practices are specific to multi-agent pipelines and advanced patterns:

1. **Parallelism tuning** — set parallelism based on event volume, processing complexity, and
   available resources. Agents in the same pipeline can have different parallelism values.
2. **Monitoring** — include correlation metadata (e.g. `requestId`, `entityId`) in output
   events for end-to-end tracing across a chained pipeline.
3. **Context-only subscriptions** — use `"operation": ""` for event types that must be present
   in history but should not trigger rule processing. This avoids creating a no-op operation in
   the Decision Intelligence archive.
4. **Cross-domain chaining** — when chaining agents that manage different entity types, ensure
   each agent declares the correct `entityKeyExtractor` for its own entity. The framework
   automatically re-keys envelopes at the chain boundary; no manual mapping is needed.
5. **Shared archive-control topics** — when multiple agents share the same JAR, they may share
   an `archiveControlTopic`. A single reload command then reloads all agents simultaneously.
   If agents ever diverge to use different JARs, assign each its own control topic to avoid
   cross-agent reload interference.

---

## 11. Known Limitations and Trade-offs

### 11.1 Stream-union watermark and cross-source event ordering

When an agent subscribes to **more than one event type** the Flink job unions all per-subscription
Kafka sources before the `keyBy` step:

```
Kafka[account-events] ──┐
                         ├─ union ──► keyBy(entityId) ──► StatefulAgentProcessor
Kafka[transactions]    ──┘
```

Flink's `union()` operator advances watermarks to the **minimum** watermark across all inputs,
which is correct and prevents premature horizon eviction.  However, because the two Kafka source
partitions are consumed independently by separate Flink source operators, an event produced on
`transactions` *after* an event on `account-events` may still arrive at the processor *before*
it due to:

* Kafka consumer group re-balancing or partition-leader election creating a temporary lag on one
  source.
* The bounded out-of-orderness watermark window (`WATERMARK_OUT_OF_ORDER_MS`, default 5 s)
  absorbing the arrival delta but not guaranteeing causal ordering.

**Impact:** For the Banking Fraud Agent, if a `Transaction` is processed before the preceding
`AccountEvent` that set the account balance, the rule engine sees a stale entity state.  In
practice the out-of-orderness window is small enough that real-world inter-event gaps (order of
seconds to minutes) are handled correctly.

**Mitigation options:**

| Option | Description | Trade-off |
|--------|-------------|-----------|
| Increase `WATERMARK_OUT_OF_ORDER_MS` | Tolerate larger reordering windows | Higher latency; state grows |
| Sentinel "context loaded" event | Account agent emits a lightweight event after setup is complete; Transaction agent waits for it | Requires an additional operation in the Decision Service |
| Single Kafka topic per agent | Merge event types into one topic with a type discriminator field | Simpler ordering; harder to produce from upstream systems |
| `CoProcessFunction` for two inputs | Flink-native two-input ordered join | High implementation complexity; no current Flink abstraction for N inputs |

The recommended approach for new use cases with strict ordering requirements is to use a single
Kafka topic with a `__eventType` discriminator field so all events for an entity are delivered
in the same partition and in exactly the order they were produced.

---

### 11.2 Delivery guarantee: AT_LEAST_ONCE

All Kafka output sinks use `DeliveryGuarantee.AT_LEAST_ONCE` with unique transactional ID
prefixes (one per agent/event-type pair).  This means:

* Under normal operation every outbound event is delivered **exactly once**.
* After a Flink checkpoint failure that occurs *after* Kafka produce but *before* the Flink
  checkpoint record is committed, the affected Flink task is restarted from the last checkpoint
  and the uncommitted Kafka records are replayed.  This can result in **duplicate outbound
  events** (e.g. a fraud alert sent twice for the same transaction).

**Upgrading to EXACTLY_ONCE:**

Switch to `DeliveryGuarantee.EXACTLY_ONCE_BETA` in
`AgentBasedDataStreamJob.buildKafkaSink()`.  This wraps produce calls in Kafka transactions and
requires:

1. `transaction.timeout.ms` on the Kafka broker ≥ the Flink checkpoint interval (set
   `transaction.max.timeout.ms=900000` if using the default 60 s checkpoint interval).
2. All downstream consumers to read with `isolation.level=read_committed`.
3. The `transactionalIdPrefix` to remain stable across job restarts (it already is — it is
   derived from `agentId + "-" + eventType`).

**Idempotency alternative (recommended for demos):**

Add a `dedupKey` field (e.g. `entityId + "-" + eventTimestamp + "-" + agentId`) to every
outbound envelope and implement deduplication at the consumer side (e.g. a Redis SET with TTL).
This is simpler to operate than Kafka transactions and decouples the guarantee from Flink
internals.
