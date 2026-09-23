# Rule Engine Internals

This document covers the IBM Decision Intelligence integration layer inside the Event Rule
backend: how archives are loaded and isolated, how the rule engine is invoked, how live
archive hot-swap works, and how to monitor and debug rule execution.

> For the rule execution context, field-name contracts (`entityField`, `outbounds`), and
> worked examples of Decision Service design, see
> [`AGENT_PATTERN_DESIGN.md` §7](AGENT_PATTERN_DESIGN.md#7-rule-execution-context).
> For step-by-step agent and Decision Service setup, see [`../TUTORIAL.md`](../TUTORIAL.md).

## Table of contents

1. [Class hierarchy](#1-class-hierarchy)
2. [Archive loading and isolation](#2-archive-loading-and-isolation)
3. [Rule execution](#3-rule-execution)
4. [Archive hot-swap](#4-archive-hot-swap)
5. [Custom rule observer](#5-custom-rule-observer)
6. [Monitoring and debugging](#6-monitoring-and-debugging)
7. [Performance notes](#7-performance-notes)
8. [Error handling](#8-error-handling)
9. [Troubleshooting](#9-troubleshooting)

---

## 1. Class hierarchy

```
StatefulAgentProcessor           (Flink KeyedBroadcastProcessFunction)
    └── RuleEngineRunner         (IBM Decision Intelligence adapter)
            ├── URLClassLoader   (per-archive isolation)
            └── DecisionRunner   (IBM Decision Intelligence API entry point)
```

`RuleEngineRunner` is the sole boundary between the Flink agent pipeline and IBM Decision
Intelligence. It wraps the `DecisionRunner` API and owns the archive lifecycle (load, verify,
run, swap).

`StatefulAgentProcessor` drives two paths through `RuleEngineRunner`:

- **Normal event processing** — calls `execute(operationName, context)` and reads the result.
- **Broadcast control** — receives `ArchiveControlCmd` messages on the broadcast stream and
  delegates to `loadArchive()` / `rollback()` / `prefetchArchive()`.

---

## 2. Archive loading and isolation

Each archive version gets its own `URLClassLoader` scoped to the JAR file. This means:

- Multiple archive versions can coexist in the same JVM without class collisions.
- A hot-swapped archive takes effect immediately for the next event after the checkpoint
  barrier completes — the old `URLClassLoader` remains active until that barrier.
- A failed load leaves the previous `URLClassLoader` active; no events are dropped.

### Smoke test on load

After loading a new archive, `RuleEngineRunner` executes a smoke-test invocation with an
empty context. This verifies that the `DecisionServiceProvider` registration inside
`META-INF/services/com.ibm.decision.run.DecisionServiceProvider` is valid and that the
configured operation names are resolvable before any real event is processed.

If the smoke test fails, the archive is rejected, the old version continues to serve events,
and the failure is logged at `ERROR` level with the operation name and exception.

### SHA-256 integrity verification

Before a JAR is loaded, `ArchiveDownloader` computes its SHA-256 and compares it against the
value embedded in the `ArchiveControlCmd` message. A mismatch causes the archive to be
silently rejected — the previous version stays active.

---

## 3. Rule execution

`RuleEngineRunner.execute(operationName, context)` performs:

1. Looks up the operation by name in the loaded `DecisionRunner` (case-sensitive).
2. Serialises the `context` map to JSON and sets it as the input for the operation.
3. Invokes the operation and deserialises the result JSON to a `Map<String, Object>`.
4. Returns the result map, or `null` if the invocation throws.

The input `context` map is assembled by `GenericContextBuilder` from the agent state and
current event. For the complete context shape, field-name aliasing rules, `entityField`
contract, and `outbounds` list contract, see
[`AGENT_PATTERN_DESIGN.md` §7](AGENT_PATTERN_DESIGN.md#7-rule-execution-context).

### Operation lookup

The operation name in `agents.json` `subscriptions[].operation` must match the operation
name in `META-INF/decisions/*.json` inside the archive **exactly** (case-sensitive). If no
matching operation is found, `RuleEngineRunner` logs a warning and returns `null` — the
agent emits an `agent-error` event and skips state updates for that event.

---

## 4. Archive hot-swap

The running Flink job can receive a new archive without restart via the archive-control
Kafka broadcast topic configured in `ruleset.archiveControlTopic`.

### Command types

| Command      | Description                                                                      |
|--------------|----------------------------------------------------------------------------------|
| `LOAD`       | Download (if URL-based), verify SHA-256, smoke-test, then atomically swap in     |
| `PREFETCH`   | Download and verify SHA-256 into the JVM cache; do not swap yet                  |
| `ROLLBACK`   | Revert to the immediately preceding archive version                              |
| `NOOP`       | No-op; used to confirm broadcast connectivity without triggering a swap          |

### Reload sequence (LOAD)

```
Broadcast stream delivers ArchiveControlCmd to every subtask
        │
        ▼
ArchiveDownloader.download(url)
        │  ← checks JVM cache first (URL + SHA-256 key)
        ▼
SHA-256 verification
        │  ← mismatch → reject; keep current version
        ▼
URLClassLoader instantiation + service discovery
        │
        ▼
Smoke-test invocation
        │  ← failure → reject; keep current version
        ▼
Atomic swap: new URLClassLoader replaces current
        │
        ▼
Log: [StatefulAgentProcessor[<agentId>]] ✓ Archive reloaded: v=<version> requestId=<id>
```

All subtasks receive the broadcast command before the next Flink checkpoint completes
(`EXACTLY_ONCE` semantics), so every parallel task swaps to the same version at the same
checkpoint boundary.

### Triggering a reload

Use [`scripts/broadcast-reload.sh`](../scripts/broadcast-reload.sh) to publish an
`ArchiveControlCmd` from the command line, or call the Deployment REST API via the BFF.

---

## 5. Custom rule observer

Extend `RuleEngineObserver` to hook into individual rule execution events:

```java
public class CustomRuleObserver implements RuleEngineObserver {
    @Override
    public void ruleExecutionStarted(Observable obs, RuleInstance rule) {
        // e.g. emit a metric, trace the rule name
    }

    @Override
    public void ruleExecutionFinished(Observable obs, RuleInstance rule) {
        // e.g. record elapsed time
    }
}
```

Register the observer on the `RuleEngineRunner` instance during `StatefulAgentProcessor`
initialisation (in `open()`).

---

## 6. Monitoring and debugging

### Rule execution log lines

`RuleEngineRunner` logs at `DEBUG` level by default:

```
[IBM DI] ▶ Invoking operation='FraudDetectionService-EvaluateTransaction' eventId=evt-123
[IBM DI]   → Rule started:  RULE-001 Large Transaction
[IBM DI]   ← Rule finished: RULE-001 Large Transaction
[IBM DI] ◀ Complete  eventId=evt-123 elapsedMs=8 firedRules=1
```

Enable `DEBUG` logging for `com.ibm.di.event.rule` in your Flink log4j configuration to see
individual rule fire/finish events.

### Archive reload confirmation

A successful reload logs at `INFO` on the TaskManager:

```
[StatefulAgentProcessor[fraud-detection-agent]] ✓ Archive reloaded: v=1.3.1 requestId=req-abc
```

A rejected archive (SHA-256 mismatch, smoke-test failure, or missing operation) logs at
`ERROR`:

```
[StatefulAgentProcessor[fraud-detection-agent]] ✗ Archive load failed: <reason> — keeping current version
```

### Flink Web UI

The Flink Web UI at `http://localhost:8081` shows:

- Per-operator throughput and latency in the job DAG view.
- Checkpoint history and duration (target: ≤ 60 s; prolonged stalls may indicate a blocked
  archive load).
- Backpressure indicators per task (high backpressure during a reload is transient and
  expected; it resolves after the smoke test completes).

---

## 7. Performance notes

| Concern              | Detail                                                                                  |
|----------------------|----------------------------------------------------------------------------------------|
| **Startup cost**     | Archives are loaded once per Flink subtask at job start (or on hot-swap). First event   per subtask bears the `URLClassLoader` initialisation overhead (~100–300 ms).            |
| **Per-event cost**   | Rule execution: ~8 ms per event under typical banking fraud workloads (single archive, ≤ 10 rules fired). Context serialisation adds ~1–2 ms.                                   |
| **Parallelism**      | Each Flink task slot runs an independent `RuleEngineRunner`. Scale parallelism to match event volume; each instance is fully isolated.                                            |
| **Hot-swap window**  | The `PREFETCH` → `LOAD` two-step minimises the checkpoint-barrier blocking window. Use `PREFETCH` first during high-throughput periods, then `LOAD` during a quieter window.     |

---

## 8. Error handling

If `RuleEngineRunner.execute()` throws or returns `null`:

- The error is logged at `ERROR` level with `agentId`, `entityId`, and `operationName`.
- A structured `agent-error` envelope is emitted to the configured error output topic.
  The envelope payload contains: `agentId`, `entityId`, `errorReason`, and the original
  event payload.
- Entity state and event history are **not** updated.
- Stream processing continues — the failure is isolated to the single event that caused it.

---

## 9. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `Could not find decision runner for operation: <name>` | Operation name mismatch or missing `META-INF/services/com.ibm.decision.run.DecisionServiceProvider` entry | Verify `subscriptions[].operation` matches the operation name in the archive exactly (case-sensitive). Rebuild the archive if the service entry is missing. |
| Smoke-test failure on load | Archive compiled against a different IBM Decision Intelligence runtime version | Ensure the archive is built with the same DI runtime version as declared in `event-rule-backend/pom.xml`. |
| No output events despite rules firing | `outbounds[].field` does not match the JSON name of the result attribute | Use the JSON name from the Decision Designer data model — not the display name. See [`AGENT_PATTERN_DESIGN.md` §7.2](AGENT_PATTERN_DESIGN.md#72-the-outbounds-contract--data-model-json-names). |
| Entity state not updating | `entityField` in `ruleset.operations[]` does not match the result field name | The same field name must be used in both input context and Decision Service result. See [`AGENT_PATTERN_DESIGN.md` §7.1](AGENT_PATTERN_DESIGN.md#71-the-entityfield-contract--input-and-output). |
| Archive reload not confirmed after `broadcast-reload.sh --wait` | Reload confirmation log line not appearing within timeout | Check TaskManager logs: `docker compose logs --tail 100 event-rule-backend-taskmanager \| grep -E 'Archive\|SMOKE\|reload'` |
