# Banking Fraud Agent

A stateful fraud-detection pipeline built on the Event Rule Agent pattern: transactions are
evaluated in real time against an account's full history using rules authored in IBM Decision
Intelligence.

## Overview

| | |
|---|---|
| **Agent** | `fraud-detection-agent` |
| **Entity tracked** | `Account` (keyed by `$.accountNumber`) |
| **Decision archive** | [`FraudDetectionService_1.5.2.jar`](FraudDetectionService_1.5.2.jar) |
| **State TTL** | 30 days (RocksDB) |
| **Descriptor** | [`agents.json`](agents.json) / [`descriptor.json`](descriptor.json) |

## Table of Contents

1. [Subscriptions and Operations](#1-subscriptions-and-operations)
2. [Event Shapes](#2-event-shapes)
3. [Output Topics](#3-output-topics)
4. [Fraud Detection Rules](#4-fraud-detection-rules)
5. [Running This Use Case](#5-running-this-use-case)
6. [Demo Scenarios](#6-demo-scenarios)
7. [End-to-End Example](#7-end-to-end-example)
8. [Entity State Persistence](#8-entity-state-persistence)
9. [Troubleshooting](#9-troubleshooting)

---

## 1. Subscriptions and Operations

| Subscription   | Topic                  | Operation             | History horizon          |
|----------------|-------------------------|------------------------|---------------------------|
| `Transaction`  | `agent-transactions`   | `EvaluateTransaction` | Hybrid: last 100 or 24 h |
| `AccountEvent` | `agent-account-events` | `HandleAccountEvent`  | Count: last 100          |

### `EvaluateTransaction` — domain field mappings

| `agents.json` key              | Domain field in archive | Notes                                            |
|--------------------------------|--------------------------|---------------------------------------------------|
| `entityField`                  | `theAccount`             | The `Account` object persisted in Flink state    |
| `currentEventField`            | `theTransaction`         | The `Transaction` that triggered this evaluation |
| `history[Transaction].field`   | `thePastTransactions`    | Array of past `Transaction` payloads             |
| `history[AccountEvent].field`  | `thePastAccountEvents`   | Array of past `AccountEvent` payloads            |
| `outbounds[0].field`           | `theFraudAlerts`         | Array of `Alert` objects emitted by rules        |
| `outbounds[0].eventType`       | `Alert`                  | Routed to `agent-fraud-alerts`                   |

### `HandleAccountEvent` — domain field mappings

| `agents.json` key             | Domain field in archive | Notes                                                               |
|-------------------------------|--------------------------|-----------------------------------------------------------------------|
| `entityField`                 | `account`                | The `Account` object persisted in Flink state                       |
| `currentEventField`           | `accountEvent`           | The `AccountEvent` that triggered this operation                    |
| `history[AccountEvent].field` | `pastAccountEvents`      | Array of past `AccountEvent` payloads                                |
| `outbounds`                   | *(none)*                 | This operation updates entity state only; it emits no output events |

See [`../../docs/AGENT_PATTERN_DESIGN.md`](../../docs/AGENT_PATTERN_DESIGN.md) for the generic
rule signature and full descriptor field reference.

---

## 2. Event Shapes

**AccountEvent** (topic: `agent-account-events`):
```json
{
  "accountNumber": "111",
  "accountType": "CHECKING",
  "balance": 10000,
  "currency": "DOLLAR",
  "overdraftLimit": 15000,
  "owner": "John Smith",
  "ownerCountry": "USA",
  "status": "ACTIVE",
  "timestamp": "2026-08-23T20:29:16Z",
  "type": "OPENING"
}
```

**Transaction** (topic: `agent-transactions`):
```json
{
  "accountNumber": "111",
  "amount": 11000,
  "channel": "POS",
  "country": "USA",
  "currency": "DOLLAR",
  "merchant": "Amazon",
  "merchantCategory": "OTHER",
  "timestamp": "2026-08-22T20:42:01Z",
  "transactionId": "1",
  "type": "DEBIT"
}
```

---

## 3. Output Topics

| Event type    | Topic                  | When                                              |
|---------------|-------------------------|-----------------------------------------------------|
| `Alert`       | `agent-fraud-alerts`   | Any alert emitted by `EvaluateTransaction` rules  |
| `agent-error` | `agent-banking-errors` | Rule engine or processing errors                  |

### Entity changelog

When `stateConfig.changeLogTopic` is set (this agent uses `agent-account-entity-changelog`), the
Flink job publishes a side-output record to that topic every time the rule engine returns an
updated `Account` entity. Each record has the form:

```json
{
  "entityId":   "ACC-42",
  "entityType": "Account",
  "timestamp":  1750000000000,
  "entity":     { "accountNumber": "ACC-42", "balance": 3500.00, "status": "ACTIVE" }
}
```

The Kafka record key is the entity ID (UTF-8), consistent with all other agent output topics.
Downstream systems can consume this topic to track the live state of every account without
reading Flink checkpoints directly.

**Decision thresholds** (cumulative risk score → decision):

| Cumulative score | Decision                | Output topic                  |
|-------------------|--------------------------|---------------------------------|
| ≥ 70              | `BLOCKED`               | `agent-fraud-alerts`          |
| 50 – 69           | `PENDING_REVIEW`        | `agent-fraud-alerts`          |
| 30 – 49           | `REQUIRES_VERIFICATION` | `agent-fraud-alerts`          |
| < 30              | `APPROVED`              | `agent-approved-transactions` |

---

## 4. Fraud Detection Rules

All rules live in the Decision Intelligence archive (`FraudDetectionService`):

| Rule                                     | Trigger                                                | Risk added | Short-circuits? |
|--------------------------------------------|----------------------------------------------------------|--------------|-------------------|
| RULE-001 Large Transaction               | Amount > $10,000                                       | +30        | No              |
| RULE-002a Cross-Border (account profile) | Transaction country ≠ account home country             | +15        | No              |
| RULE-002b Cross-Border (cardholder)      | Transaction country ≠ `cardHolderCountry` on the event | +15        | No              |
| RULE-003 Suspicious Small Amount         | Amount < $1 (card testing probe)                        | +20        | No              |
| RULE-004 Velocity Check                  | > 5 transactions in current history window              | +35        | No              |
| RULE-005 High-Risk Merchant              | Merchant name contains a high-risk keyword              | +25        | No              |
| RULE-006 Frozen / Suspended Account      | Account status = FROZEN or SUSPENDED                    | → 100      | **Yes**         |
| RULE-007 Overdraft Risk                  | Amount > balance (no overdraft facility)                | +40        | No              |
| RULE-008 New Account High-Value          | Account age < 30 days and amount > $1,000               | +30        | No              |

RULE-006 immediately sets the score to 100 and returns `BLOCKED`, skipping all other rules.

> **Note on RULE-002:** RULE-002a uses the stored account profile (`account.country`) from the
> `account-created` history. RULE-002b uses `cardHolderCountry` directly from the transaction
> event — no account-created event is required for RULE-002b to fire.
>
> **Note on RULE-007 and RULE-008:** These rules require account context. If no
> `account-created` event has been received for an account, the rules produce no risk
> contribution for that entity.

---

## 5. Running This Use Case

**From the project root**, after the infrastructure is up (see the
[main README Quick Start](../../README.md#4-quick-start)):

```bash
# 1. Build the project (if not already built)
./build.sh

# 2. Start the infrastructure
./run.sh up

# 3. Deploy this use case
./samples/banking-fraud-agent/scripts/deploy.sh

# 4. Open the dashboard
open http://localhost:3010
```

Click **Agent Banking** in the use-case selector, then click any scenario button in the
**Send Events** panel.

To deploy a **new or updated** archive while the system is running (zero-downtime hot-swap), see
[`../README.md#creating-a-new-use-case`](../README.md) and
[`../../docs/AGENT_PATTERN_DESIGN.md`](../../docs/AGENT_PATTERN_DESIGN.md).

---

## 6. Demo Scenarios

See [`demo-scenarios.json`](demo-scenarios.json). Click these in the **Send Events** panel to see
each rule fire:

| Scenario                               | Rule triggered           | Expected decision       |
|-------------------------------------------|-----------------------------|----------------------------|
| Normal Approval                        | None                     | `APPROVED`              |
| Large Transaction                      | RULE-001                 | `REQUIRES_VERIFICATION` |
| Cross-Border Cardholder Mismatch       | RULE-002b                | `APPROVED` (with alert) |
| Cross-Border Account Profile Mismatch  | RULE-002a                | `APPROVED` (with alert) |
| Suspicious Small Amount — Card Testing | RULE-003                 | `APPROVED` (with alert) |
| Velocity Limit Exceeded                | RULE-004 (6 events sent) | `REQUIRES_VERIFICATION` |
| High-Risk Merchant                     | RULE-005                 | `APPROVED` (with alert) |
| Frozen Account                         | RULE-006                 | `BLOCKED`               |
| Overdraft — No Overdraft Facility      | RULE-007                 | `REQUIRES_VERIFICATION` |
| New Account High-Value Transaction     | RULE-008                 | `REQUIRES_VERIFICATION` |
| Multi-Rule Escalation                  | RULE-001 + RULE-007      | `BLOCKED`               |
| `account-creation`                     | —                         | Creates an `Account` entity (type `OPENING`) |
| `transaction-on-suspended-account`     | RULE-006                 | Creates a `SUSPENDED` account, then blocks a transaction |

> **Velocity scenario:** This sends 6 transactions in sequence for the same account. The Agent
> accumulates them in its history buffer; on the 6th transaction `recentTransactionCount = 6 > 5`
> triggers RULE-004.

You can also run scenarios from the command line:

```bash
# Run all scenarios
../../scripts/launch-demo-scenario.sh banking-fraud-agent

# Run a specific scenario
../../scripts/launch-demo-scenario.sh banking-fraud-agent transaction-on-suspended-account
```

---

## 7. End-to-End Example

A new account (created 2 days ago) attempts a $2,500 online payment.

### Step 1 — Account profile stored

An `AccountEvent` (type `OPENING`) arrives on `agent-account-events`:

```json
{
  "accountNumber": "ACC-NEW-001",
  "accountType": "CHECKING",
  "balance": 8000,
  "currency": "DOLLAR",
  "overdraftLimit": 15000,
  "owner": "Nina Newbie",
  "ownerCountry": "USA",
  "status": "ACTIVE",
  "timestamp": "2026-06-24T10:00:00Z",
  "type": "OPENING"
}
```

The Agent stores this as `AccountEvent` history for `ACC-NEW-001` and invokes
`HandleAccountEvent`, which persists the `Account` entity state. No fraud alert is generated from
account events.

### Step 2 — A transaction arrives

```json
{
  "accountNumber": "ACC-NEW-001",
  "amount": 2500,
  "channel": "ONLINE",
  "country": "USA",
  "currency": "DOLLAR",
  "merchant": "Electronics Mega Store",
  "merchantCategory": "OTHER",
  "timestamp": "2026-06-26T10:45:00Z",
  "transactionId": "AGENT-NEW-001",
  "type": "DEBIT"
}
```

### Step 3 — Agent assembles the full context

```json
{
  "now":               "2026-06-26T10:45:00Z",
  "processingTime":    "2026-06-26T10:45:00.042Z",
  "theAccount": {
    "accountNumber": "ACC-NEW-001",
    "ownerCountry":  "USA",
    "balance":       8000,
    "status":        "ACTIVE"
  },
  "theTransaction": {
    "transactionId": "AGENT-NEW-001",
    "amount":        2500,
    "country":       "USA",
    "merchant":      "Electronics Mega Store"
  },
  "thePastTransactions":  [],
  "thePastAccountEvents": [
    { "accountNumber": "ACC-NEW-001", "status": "ACTIVE", "type": "OPENING" }
  ],
  "theFraudAlerts": []
}
```

### Step 4 — Rules evaluate

| Rule                             | Condition met?                            | Risk delta |
|-------------------------------------|-----------------------------------------------|--------------|
| RULE-001 Large Transaction       | ❌ $2,500 < $10,000                        | 0          |
| RULE-002 Cross-Border            | ❌ USA = USA                               | 0          |
| RULE-003 Suspicious Small Amount | ❌ $2,500 ≥ $1                             | 0          |
| RULE-004 Velocity Check          | ❌ 1 transaction ≤ 5                       | 0          |
| RULE-005 High-Risk Merchant      | ❌ Electronics not in high-risk list       | 0          |
| RULE-006 Frozen Account          | ❌ Status = ACTIVE                         | 0          |
| RULE-007 Overdraft Risk          | ❌ $2,500 ≤ $8,000 balance                 | 0          |
| RULE-008 New Account High-Value  | ✅ Age 2 days < 30, amount $2,500 > $1,000 | **+30**    |

**Cumulative risk score: 30** → **REQUIRES_VERIFICATION** (30–49 range)

### Step 5 — Decision Intelligence returns the structured decision

```json
{
  "decision": "REQUIRES_VERIFICATION",
  "riskScore": 30.0,
  "triggeredRules": [
    {
      "ruleId": "RULE-008",
      "ruleName": "New Account High-Value",
      "severity": "HIGH",
      "message": "New account risk: account age 2 days with transaction amount $2500 exceeds $1000 threshold"
    }
  ],
  "alerts": [
    {
      "alertType": "SUSPICIOUS_ACTIVITY",
      "severity": "HIGH",
      "title": "New Account Risk",
      "message": "High-value transaction on recently opened account"
    }
  ],
  "actions": [
    {
      "actionType": "REQUEST_VERIFICATION",
      "priority": "HIGH",
      "description": "Request customer verification for high-risk transaction"
    }
  ]
}
```

### Step 6 — Result routed and displayed

Decision is not `APPROVED` → routed to `agent-fraud-alerts` → BFF forwards via WebSocket →
dashboard shows a result card with the decision, risk score, triggered rules, and recommended
actions.

---

## 8. Entity State Persistence

The `Account` object is persisted in Flink keyed state (`AgentState.entityData`) and updated by
both operations on every event cycle.

**Read path — event arrives → rules:**

1. `GenericContextBuilder` places `AgentState.entityData` (the stored `Account` JSON) in the
   context under the operation's `entityField` name (`theAccount` or `account`).
2. The Decision Intelligence archive's setup rules hydrate a typed `Account` object from that
   field.
3. Rules evaluate against the typed `Account` alongside the current event and past history.

**Write path — rules → Flink state:**

1. Rules write the updated `Account` back to the result object under the `entityField` name
   (`theAccount` for `EvaluateTransaction`; `account` for `HandleAccountEvent`).
2. `StatefulAgentProcessor.updateEntityState()` reads that field from the rule result and
   replaces `AgentState.entityData`.
3. Flink persists the updated state to RocksDB at the next checkpoint.

**Entity changelog (`stateConfig.changeLogTopic`):**

This agent declares `"changeLogTopic": "agent-account-entity-changelog"` in its `stateConfig`.
Whenever step 2 above produces a new (non-null) entity value, Flink emits a side-output record to
that Kafka topic in addition to persisting the update internally. See
[§3 Entity changelog](#entity-changelog) above for the record shape.

The fact that the entity field name is the same in the input context and in the result object is
a direct consequence of the `entityField` contract: a single property in `agents.json`
configures both directions. See
[`docs/AGENT_PATTERN_DESIGN.md` §7.1](../../docs/AGENT_PATTERN_DESIGN.md#71-the-entityfield-contract--input-and-output)
for a detailed explanation,
[§7.2](../../docs/AGENT_PATTERN_DESIGN.md#72-the-outbounds-contract--data-model-json-names)
for the `outbounds` contract, and
[§7.3](../../docs/AGENT_PATTERN_DESIGN.md#73-decision-service-design-guide) for the full
Decision Service design guide.

---

## 9. Troubleshooting

| Problem                                                 | What to check                                                                                                                       |
|------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------|
| Entity state not building up (rules can't see account) | An `AccountEvent` (type `OPENING`) must arrive **before** transactions — `HandleAccountEvent` persists the `Account` entity that `EvaluateTransaction` reads |
| Velocity scenario not triggering                        | All transactions must share the same `accountNumber` and arrive before the horizon evicts them                                       |
| Rules not firing                                        | Check for smoke-test warnings in TaskManager logs; verify `ruleset.path` points to an existing JAR                                    |

For infrastructure-level troubleshooting (Kafka, Flink, dashboard connectivity), see the
[main README Troubleshooting section](../../README.md#troubleshooting).

