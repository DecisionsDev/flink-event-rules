/*
 *
 *   Copyright IBM Corp. 2026
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 */
package com.ibm.di.event.agent;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ibm.di.event.agent.context.ContextBuilderRegistry;
import com.ibm.di.event.rule.RuleEngineRunner;
import com.ibm.di.event.rule.broadcast.ArchiveControlCmd;
import com.ibm.di.event.rule.broadcast.ArchiveDownloader;
import com.ibm.di.event.rule.broadcast.ArchiveEntry;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serial;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Stateful Agent rule processor with broadcast-state archive reload.
 *
 * <p>Extends {@link KeyedBroadcastProcessFunction}: the keyed side processes entity events
 * while the broadcast side receives {@link ArchiveControlCmd} messages that hot-swap the
 * Decision Intelligence archive JAR across all parallel subtasks without restarting the job.
 *
 * <p>{@link #ARCHIVE_STATE_DESC} must be passed to both the {@code .broadcast()} call in
 * {@link AgentBasedDataStreamJob#wireAgent} and used inside this operator.  Only the small
 * {@link ArchiveEntry} (~200 bytes) lives in broadcast state; the {@link RuleEngineRunner}
 * is {@code transient} and is re-materialised from the checkpointed URL on the first event
 * after a restore.
 *
 * <p>For the full event-processing lifecycle, archive-reload protocol, and fault-tolerance
 * behaviour see {@code README.md} §5 (How Event Rules Work) and
 * {@code docs/ADVANCED_PATTERNS.md} (zero-downtime archive reload).
 */
public class StatefulAgentProcessor
        extends KeyedBroadcastProcessFunction<String, EventEnvelope, ArchiveControlCmd, EventEnvelope> {

    private static final Logger LOG = LoggerFactory.getLogger(StatefulAgentProcessor.class);

    @Serial
    private static final long serialVersionUID = 2L;

    // ── Broadcast state descriptor ─────────────────────────────────────────────

    /**
     * Single-entry broadcast state: key {@code "ACTIVE"} → currently active {@link ArchiveEntry}.
     * Must be passed to both the {@code .broadcast()} call site in
     * {@link AgentBasedDataStreamJob#wireAgent} and inside this operator.
     */
    public static final MapStateDescriptor<String, ArchiveEntry> ARCHIVE_STATE_DESC =
        new MapStateDescriptor<>(
            "agent-archive-state",
            Types.STRING,
            TypeInformation.of(ArchiveEntry.class)
        );

    /** Broadcast-state key under which the active entry is stored. */
    private static final String ACTIVE_KEY = "ACTIVE";

    /**
     * Per-instance side-output tag for entity-state changelog records.
     *
     * <p>Emitted after every event that causes the entity data to change (rule engine
     * returned a non-null entity that differs from the one sent as input).  Only active
     * when {@link StateConfig#hasChangeLogTopic()} returns {@code true}; the
     * {@link AgentBasedDataStreamJob} wires it to a Kafka sink in that case.
     *
     * <p>The tag id is {@code "entity-changelog-<agentId>"} so that each agent's
     * changelog side-output is distinct.  Using a single shared (static) tag id would
     * cause Flink to route all agents' changelog records to every consumer that calls
     * {@code getSideOutput} with that tag, broadcasting records across all changelog topics.
     *
     * <p>The side-output carries a plain {@link EventEnvelope} whose {@code eventType} is
     * {@code "entity-changelog"}, {@code entityId} is the entity key, {@code timestamp} is
     * the processing-time of the triggering event, and {@code payload} is a JSON object:
     * <pre>
     * {
     *   "entityId":   "...",
     *   "entityType": "...",
     *   "timestamp":  1234567890,
     *   "entity":     { ... }
     * }
     * </pre>
     */
    private final OutputTag<EventEnvelope> changelogTag;

    /**
     * Sentinel event type that clears the keyed state for a single entity when received
     * on the event stream.
     *
     * <p>Publish an event with this type to any of the agent's subscribed topics, with the
     * entity key field set to the target entity ID.  The event is routed by Flink's
     * {@code keyBy(entityId)} to exactly that entity's partition, where
     * {@link #processElement} will clear its {@link AgentState} and return without
     * invoking the rule engine or emitting any output.
     *
     * <p>This provides targeted per-entity reset without disturbing any other entity.
     * Use {@link ArchiveControlCmd.CommandType#RESET_STATE} (broadcast) only when a
     * global wipe of all entities is required.
     */
    public static final String ENTITY_RESET_EVENT_TYPE = "__ENTITY_RESET__";

    // ── Configuration (serialised with the operator) ───────────────────────────

    private final AgentDescriptor descriptor;

    // ── Transient runtime fields ───────────────────────────────────────────────

    private transient ValueState<AgentState> agentStateHandle;
    private transient ObjectMapper           objectMapper;

    /**
     * Keeps the entity's {@link ScheduleRequest}s and their Flink event-time timers in sync.
     * {@code null} when the descriptor uses no scheduling (no operation has a {@code schedule}
     * section and there is no {@code Schedule} subscription), in which case no schedule state
     * is created at all.
     */
    private transient ScheduleManager scheduleManager;

    /**
     * Per-subtask runner cache: archive version label → initialised {@link RuleEngineRunner}.
     * Field-initialised so unit-test accessors are safe before {@link #open} is called.
     */
    // ConcurrentHashMap is required because the async archive-reload path (M-10)
    // can write to this map from a CompletableFuture completion while the Flink
    // operator thread concurrently reads from it.
    private transient Map<String, RuleEngineRunner> runnerCache = new ConcurrentHashMap<>();

    /**
     * Lazily-built bootstrap entry used when broadcast state is empty (cold start or no
     * checkpoint).
     */
    private transient ArchiveEntry bootstrapEntry;

    /**
     * Background future that drives the bootstrap runner pre-warm.
     *
     * <p>Pre-warming the IBM DI rule engine (class loading, DRL compilation, JIT warm-up)
     * can take multiple minutes on environments where the JVM runs under QEMU emulation
     * (e.g. an {@code amd64} Docker image on Apple Silicon).  Running it synchronously
     * inside {@link #open} would block the Flink task thread and prevent the task from
     * transitioning from {@code INITIALIZING} to {@code RUNNING}, causing every checkpoint
     * that fires during warm-up to abort with "Not all required tasks are currently running".
     *
     * <p>Instead, {@link #open} starts the warm-up on a dedicated daemon thread and returns
     * immediately.  The task reaches {@code RUNNING} within milliseconds.  The first incoming
     * event that needs the rule engine calls {@link #awaitWarmUp()} which blocks only that
     * single processing thread until the background work completes — at which point it is
     * guaranteed to be ready for all subsequent events with zero additional wait.
     *
     * <p>Failures during background warm-up are surfaced as a {@link RuntimeException} on
     * the first {@link #processElement} call rather than at open-time, preserving the
     * existing error-propagation contract.
     */
    private transient volatile CompletableFuture<Void> warmUpFuture;

    /**
     * Dedicated single-thread executor for the background pre-warm.
     * Shut down in {@link #close()} to release the daemon thread promptly.
     */
    private transient ExecutorService warmUpExecutor;

    // ── Flink metrics ──────────────────────────────────────────────────────────

    private transient Counter   eventsProcessedCounter;
    private transient Counter   processingErrorsCounter;
    private transient Counter   newEntityCounter;
    private transient Counter   horizonEvictionsCounter;
    private transient Histogram ruleLatencyHistogram;

    // ── Construction ───────────────────────────────────────────────────────────

    /**
     * Creates the processor.
     *
     * @param descriptor the fully populated {@link AgentDescriptor};
     *                   {@code ruleset.path} must be non-blank.
     *                   Subscriptions may omit {@code operation} — those events are stored
     *                   in history and their horizon is applied, but the rule engine is
     *                   never invoked and no output event is emitted (history-only mode).
     *                   All subscriptions may be history-only; no subscription is required
     *                   to carry an operation.
     */
    public StatefulAgentProcessor(AgentDescriptor descriptor) {
        if (descriptor == null) {
            throw new IllegalArgumentException("AgentDescriptor must not be null");
        }
        if (descriptor.getRuleset() == null
                || descriptor.getRuleset().getPath() == null
                || descriptor.getRuleset().getPath().isBlank()) {
            throw new IllegalArgumentException(
                "AgentDescriptor.ruleset.path must not be blank");
        }
        this.descriptor = descriptor;
        // Per-agent tag id prevents Flink from cross-routing changelog side-outputs when
        // multiple agents share the same job (each agent must fan out only to its own topic).
        this.changelogTag = new OutputTag<>(
            "entity-changelog-" + descriptor.getAgentId(),
            TypeInformation.of(EventEnvelope.class));
    }

    /**
     * Returns the per-agent changelog {@link OutputTag}.
     *
     * <p>Used by {@link AgentBasedDataStreamJob#wireAgent} to subscribe to this agent's
     * changelog side-output via {@code SingleOutputStreamOperator#getSideOutput}.
     */
    public OutputTag<EventEnvelope> getChangelogTag() {
        return changelogTag;
    }

    // ── Flink lifecycle ────────────────────────────────────────────────────────

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);

        objectMapper   = new ObjectMapper();
        runnerCache    = new ConcurrentHashMap<>();
        bootstrapEntry = null;

        // ── Keyed state descriptor ─────────────────────────────────────────
        ValueStateDescriptor<AgentState> stateDesc = new ValueStateDescriptor<>(
            "agent-state-" + descriptor.getAgentId(),
            TypeInformation.of(AgentState.class));

        if (descriptor.getStateConfig() != null) {
            Duration ttl = descriptor.getStateConfig().ttlDuration();
            if (ttl != null) {
                StateTtlConfig ttlConfig = StateTtlConfig
                    .newBuilder(ttl)
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build();
                stateDesc.enableTimeToLive(ttlConfig);
                LOG.info("[{}] State TTL configured: {}", agentTag(), ttl);
            }
        }

        agentStateHandle = getRuntimeContext().getState(stateDesc);

        // ── Schedule state (only when the descriptor uses scheduling) ──────
        // Deliberately no TTL: an expired schedule would silently never fire.
        if (usesScheduling()) {
            MapState<String, ScheduleRequest> byId = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("agent-schedule-by-id-" + descriptor.getAgentId(),
                    Types.STRING, TypeInformation.of(ScheduleRequest.class)));
            MapState<Long, List<String>> idsByTime = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("agent-schedule-by-time-" + descriptor.getAgentId(),
                    Types.LONG, Types.LIST(Types.STRING)));
            scheduleManager = new ScheduleManager(byId, idsByTime, agentTag());
            LOG.info("[{}] Scheduling enabled", agentTag());
        }

        // ── Metrics ────────────────────────────────────────────────────────
        MetricGroup mg = getRuntimeContext().getMetricGroup()
            .addGroup("agent", descriptor.getAgentId());
        eventsProcessedCounter  = mg.counter("eventsProcessed");
        processingErrorsCounter = mg.counter("processingErrors");
        newEntityCounter        = mg.counter("newEntities");
        horizonEvictionsCounter = mg.counter("horizonEvictions");
        ruleLatencyHistogram    = mg.histogram("ruleLatencyMs",
            new DescriptiveStatisticsHistogram(1_000));

        long opCount = descriptor.getSubscriptions() == null ? 0 :
            descriptor.getSubscriptions().stream()
                .filter(EventSubscription::hasOperation).count();
        long subCount = descriptor.getSubscriptions() == null ? 0 :
            descriptor.getSubscriptions().size();

        LOG.info("[{}] Initialized. subtask={}/{} broadcastReload={} operations={}/{}",
            agentTag(),
            getRuntimeContext().getTaskInfo().getIndexOfThisSubtask() + 1,
            getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks(),
            descriptor.getRuleset().hasBroadcastReload(),
            opCount, subCount);

        if (opCount == 0 && subCount > 0) {
            LOG.info("[{}] All {} subscription(s) are history-only (no operation configured) — "
                + "rule engine will never be invoked; events are accumulated for context only.",
                agentTag(), subCount);
        }

        // ── Background bootstrap runner pre-warm ───────────────────────────
        // IBM DI runner initialisation (class loading, DRL compilation, JIT warm-up)
        // is CPU-intensive and can block the Flink task thread for several minutes on
        // slow or emulated environments (QEMU x86-64 on Apple Silicon being the worst
        // case).  Running it here synchronously would keep the task stuck in INITIALIZING
        // and cause every checkpoint that fires during warm-up to abort.
        //
        // Solution: submit the work to a daemon thread and return immediately.  The task
        // reaches RUNNING in milliseconds.  The first processElement/processBroadcastElement
        // call that needs a runner will block on awaitWarmUp() — by then the warm-up is
        // almost always complete, so the wait is negligible in steady state.
        String rulesetPath = descriptor.getRuleset().getPath();
        java.net.URI rulesetUri = rulesetPath.contains("://")
            ? java.net.URI.create(rulesetPath)
            : java.nio.file.Paths.get(rulesetPath).toUri();

        List<String> opsToWarm = descriptor.getSubscriptions() == null
            ? java.util.Collections.emptyList()
            : descriptor.getSubscriptions().stream()
                .filter(EventSubscription::hasOperation)
                .map(EventSubscription::getOperation)
                .distinct()
                .collect(java.util.stream.Collectors.toList());

        if (opsToWarm.isEmpty()) {
            // No operations to pre-warm; create a completed future so awaitWarmUp() is a no-op.
            warmUpFuture = CompletableFuture.completedFuture(null);
        } else {
            warmUpExecutor = Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, "agent-prewarm-" + descriptor.getAgentId());
                t.setDaemon(true);
                return t;
            });
            LOG.info("[{}] Launching background pre-warm for {} operation(s): {}",
                agentTag(), opsToWarm.size(), opsToWarm);
            warmUpFuture = CompletableFuture.runAsync(() -> {
                for (String op : opsToWarm) {
                    try {
                        RuleEngineRunner warmRunner = new RuleEngineRunner(op, rulesetUri);
                        warmRunner.initializeEagerly();
                        synchronized (runnerCache) {
                            runnerCache.put(bootstrapKey(op), warmRunner);
                        }
                        LOG.info("[{}] Bootstrap runner pre-warmed. operation={}", agentTag(), op);
                    } catch (Exception e) {
                        LOG.warn("[{}] Bootstrap runner pre-warm failed for operation={} "
                            + "(will retry lazily on first event): {}",
                            agentTag(), op, e.getMessage());
                    }
                }
                LOG.info("[{}] Background pre-warm complete for all {} operation(s).",
                    agentTag(), opsToWarm.size());
            }, warmUpExecutor);
        }
    }

    @Override
    public void close() throws Exception {
        // Cancel any in-progress warm-up to avoid a thread leak.
        if (warmUpFuture != null) {
            warmUpFuture.cancel(true);
        }
        if (warmUpExecutor != null) {
            warmUpExecutor.shutdownNow();
        }
        closeAllRunners();
        super.close();
    }

    /**
     * Blocks the calling thread until the background bootstrap pre-warm completes.
     *
     * <p>Must be called at the start of every entry point that touches the runner cache
     * ({@link #processElement} and {@link #processBroadcastElement}).  After the first
     * successful call, the future is already completed and subsequent calls return
     * immediately without any synchronization overhead.
     *
     * <p>Warm-up failures (e.g., missing archive JAR) are re-thrown here as a
     * {@link RuntimeException} so Flink can fail the task and trigger a restart, rather
     * than silently proceeding with an uninitialised runner.
     */
    private void awaitWarmUp() {
        if (warmUpFuture == null || warmUpFuture.isDone()) return;
        try {
            LOG.info("[{}] First event arrived while background pre-warm is still running — "
                + "waiting for completion before processing.", agentTag());
            warmUpFuture.join(); // blocks until done or cancelled
        } catch (java.util.concurrent.CancellationException e) {
            // Cancelled during close() — treat as a non-fatal warning; the task is shutting down.
            LOG.warn("[{}] Background pre-warm was cancelled (task is closing).", agentTag());
        } catch (java.util.concurrent.CompletionException e) {
            // Unwrap so the root cause propagates up to Flink's task error handler.
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            throw new RuntimeException("Bootstrap pre-warm failed: " + cause.getMessage(), cause);
        }
    }

    // ── Broadcast side: archive-control commands ───────────────────────────────

    /**
     * Handles an incoming {@link ArchiveControlCmd} from the archive-control Kafka topic.
     *
     * <ul>
     *   <li>{@code NOOP}     — logged, ignored.</li>
     *   <li>{@code PREFETCH} — downloads + SHA-256 verifies + pre-warms a new runner without activating it.</li>
     *   <li>{@code LOAD} / {@code ROLLBACK} — download → verify SHA-256 → initialise → smoke-test →
     *       update broadcast state → evict stale runners.</li>
     * </ul>
     *
     * <p>Failed reloads leave broadcast state unchanged and are logged at ERROR level.
     */
    @Override
    public void processBroadcastElement(ArchiveControlCmd cmd,
                                        Context ctx,
                                        Collector<EventEnvelope> out) {
        // Ensure the bootstrap pre-warm has completed before touching the runner cache.
        awaitWarmUp();

        if (cmd == null) {
            LOG.warn("[{}] Received null archive-control command — ignoring", agentTag());
            return;
        }
        LOG.info("[{}] Archive-control command: {}", agentTag(), cmd);

        switch (cmd.getCommandType()) {
            case NOOP:
                LOG.info("[{}] NOOP command — no action", agentTag());
                return;
            case PREFETCH:
                handlePrefetch(cmd);
                return;
            case RESET_STATE:
                handleResetState(cmd, ctx);
                return;
            default:
                break; // LOAD / ROLLBACK fall through
        }

        if (!validateCmd(cmd)) return;

        RuleEngineRunner newRunner = null;
        try {
            Path localJar = ArchiveDownloader.downloadAndVerify(
                cmd.getArchiveUrl(), cmd.getSha256());

            // Build one runner per distinct operation from the new archive
            java.util.List<String> distinctOps = descriptor.getSubscriptions() == null
                ? java.util.List.of()
                : descriptor.getSubscriptions().stream()
                    .filter(EventSubscription::hasOperation)
                    .map(EventSubscription::getOperation)
                    .distinct()
                    .collect(java.util.stream.Collectors.toList());

            java.util.List<RuleEngineRunner> newRunners = new ArrayList<>();
            for (String op : distinctOps) {
                RuleEngineRunner opRunner = new RuleEngineRunner(op, localJar.toUri());
                opRunner.initializeEagerly();
                newRunners.add(opRunner);
            }

            // Smoke-test using the first operation (representative check)
            if (!newRunners.isEmpty()) {
                runSmokeTest(newRunners.get(0));
            }

            // Add all new runners to cache before updating broadcast state
            for (int i = 0; i < distinctOps.size(); i++) {
                runnerCache.put(versionedKey(cmd.getVersion(), distinctOps.get(i)), newRunners.get(i));
            }
            newRunner = newRunners.isEmpty() ? null : newRunners.get(0); // sentinel for finally block

            ctx.getBroadcastState(ARCHIVE_STATE_DESC).put(ACTIVE_KEY, new ArchiveEntry(
                cmd.getVersion(), cmd.getArchiveUrl(), cmd.getSha256(),
                System.currentTimeMillis()));

            newRunner = null; // ownership fully transferred — do not close in finally

            evictStaleRunners(cmd.getVersion());

            LOG.info("[{}] ✓ Archive reloaded: v={} requestId={}",
                agentTag(), cmd.getVersion(), cmd.getRequestId());

        } catch (com.ibm.di.event.rule.broadcast.ArchiveVerificationException ave) {
            LOG.error("[{}] ✗ SHA-256 mismatch for requestId={}: {}",
                agentTag(), cmd.getRequestId(), ave.getMessage());
            ArchiveDownloader.evict(cmd.getSha256());
        } catch (Exception e) {
            LOG.error("[{}] ✗ Archive reload failed for requestId={} v={}: {}",
                agentTag(), cmd.getRequestId(), cmd.getVersion(), e.getMessage());
            ArchiveDownloader.evict(cmd.getSha256());
        } finally {
            if (newRunner != null) {
                // Runners were never fully activated — remove all versioned cache entries
                // for this version, then close each to release their URLClassLoaders.
                if (descriptor.getSubscriptions() != null) {
                    descriptor.getSubscriptions().stream()
                        .filter(EventSubscription::hasOperation)
                        .map(EventSubscription::getOperation)
                        .distinct()
                        .forEach(op -> {
                            RuleEngineRunner r = runnerCache.remove(versionedKey(cmd.getVersion(), op));
                            if (r != null) {
                                try { r.close(); } catch (Exception ex) {
                                    LOG.warn("[{}] Error closing failed new runner op={}: {}",
                                        agentTag(), op, ex.getMessage());
                                }
                            }
                        });
                }
            }
        }
    }

    // ── Non-broadcast side: event processing ──────────────────────────────────

    /**
     * 13-step processing pipeline for a single incoming event.
     */
    @Override
    public void processElement(EventEnvelope event,
                               ReadOnlyContext ctx,
                               Collector<EventEnvelope> out) throws Exception {
        // Ensure the bootstrap pre-warm has completed before touching the runner cache.
        awaitWarmUp();

        if (event == null) {
            LOG.warn("[{}] Null event received — skipping", agentTag());
            return;
        } else {
            LOG.debug("[{}] Processing event: {}. Current timestamp: {} [Watermark: {}]", agentTag(), event.toString(), ctx.timestamp(), ctx.currentWatermark());
        }

        // ── Sentinel: targeted per-entity state reset ─────────────────────
        // Clear this entity's keyed state and return without rule execution.
        // The event is routed here by keyBy(entityId), so only this partition
        // is affected — all other entities are completely untouched.
        if (ENTITY_RESET_EVENT_TYPE.equals(event.getEventType())) {
            agentStateHandle.clear();
            // A reset entity must not receive Schedule events registered before the reset.
            if (scheduleManager != null) scheduleManager.cancelAll(ctx.timerService());
            LOG.info("[{}] Entity state cleared for entityId={}", agentTag(), event.getEntityId());
            return;
        }

        // ── Step 1: get or init keyed state ──────────────────────────────
        AgentState state = agentStateHandle.value();
        if (state == null) {
            state = AgentState.init(event.getEntityId(),
                descriptor.getEntityType(), event.getTimestamp());
            if (newEntityCounter != null) newEntityCounter.inc();
            LOG.debug("[{}] New entity state created for entityId={} of type {}", agentTag(), event.getEntityId(), descriptor.getEntityType());
        }

        // ── Step 2: trigger schedule requests that are due at this event time ──────────────────────────────
        // Guard: only fire accumulated past schedules when this event is NOT itself a Schedule event.
        // Schedule events are produced by fireSchedules() (called from fireAllSchedules() or onTimer())
        // and recursively arrive here via processElement().  If fireAllSchedules() were called again for a
        // Schedule event, each recursive processElement() call would invoke fireAllSchedules() once more,
        // creating an unbounded recursion stack — and an infinite loop whenever the rule engine returns new
        // ScheduleRequests for a timepoint ≤ the current Schedule event's timestamp.
        // Schedule events must never seed another round of fireAllSchedules: they are already the product
        // of that round, and any new ScheduleRequests they produce are for FUTURE timepoints that will be
        // handled correctly by the Flink event-time timer in onTimer().
        if (!(ctx instanceof OnTimerContext) && !ScheduleManager.SCHEDULE_EVENT_TYPE.equals(event.getEventType())) {
            // timers are event-time timers that are triggered by Watermark. The watermark of this processor
            // is the lowest watermark of all incoming sources, which delays the execution of the timers if one
            // or more sources are idle. This is solved by idleness attached to the source, but we anticipate
            // the watermark when we receive an event by triggering all timers due at this event time.
            fireAllSchedules(event.getTimestamp(), ctx, out);
        }


        // ── Step 3: look up the operation for this event's subscription ───
        String operation = resolveOperation(event);
        if (operation == null) {
            LOG.debug("[{}] no operation registered for {}", agentTag(), event.getEventType());
        }

        // -- Step 4: evict events from horizon based on the current event time
        state.evictExpiredEvents(event.getTimestamp());

        JsonNode ruleResult = null;
        if (operation != null) {
            // ── Step 5: prepare rule execution context ────────────────────────
            if (state.getEntityData() == null) {
                LOG.debug("[{}] entity is null for entityId={} currentEventType={} — "
                    + "null entity will be passed to DI as input.entity; "
                    + "rules may create and populate it from the current event.",
                    agentTag(), event.getEntityId(), event.getEventType());
            }

            JsonNode ruleContext = buildRuleContext(state, event);

            // ── Step 6: resolve active runner for this operation and execute ──
            try {
                RuleEngineRunner runner = resolveRunnerForOperation(operation, ctx);
                long t0 = System.currentTimeMillis();
                ruleResult = runner.execute(ruleContext);
                long latencyMs = System.currentTimeMillis() - t0;
                if (ruleLatencyHistogram != null) ruleLatencyHistogram.update(latencyMs);
                LOG.debug("[{}] Rule execution complete entityId={} eventType={} operation={} latencyMs={}",
                    agentTag(), event.getEntityId(), event.getEventType(), operation, latencyMs);
            } catch (Exception e) {
                LOG.error("[{}] Rule engine error entityId={} operation={}: {}",
                    agentTag(), event.getEntityId(), operation, e.getMessage());
            }

            // ── Step 7: update entity state from rule result ──────────────────
            JsonNode entityBefore = state.getEntityData() != null
                ? state.getEntityData().deepCopy() : null;
            if (ruleResult != null) {
                updateEntityState(state, ruleResult, operation, event);
            }

            // ── Step 8: emit changelog side-output when entity changed ────────
            if (descriptor.getStateConfig() != null
                    && descriptor.getStateConfig().hasChangeLogTopic()) {
                JsonNode entityAfter = state.getEntityData();
                if (entityAfter != null && !entityAfter.equals(entityBefore)) {
                    ctx.output(changelogTag, buildChangeLogEnvelope(event, entityAfter));
                }
            }

            // ── Step 9: apply schedule requests returned by the rules ────────
            processScheduleRequests(ruleResult, operation, ctx);
        }

        // ── Step 10: update event history  ────────────
        long historyBefore = countHistorySize(state);
        updateEventHistory(state, event);
        long evicted = historyBefore + 1 - countHistorySize(state);
        if (evicted > 0 && horizonEvictionsCounter != null) {
            horizonEvictionsCounter.inc(evicted);
        }

        // ── Step 11: persist state ─────────────────────────────────────────
        agentStateHandle.update(state);

        if (operation != null) {
            // ── Step 12: emit output events (zero or more) ─────────────────────
            List<EventEnvelope> outputs = buildOutputEvents(event, ruleResult, operation);

            if (!outputs.isEmpty()) {
                for (EventEnvelope output : outputs) {
                    LOG.debug("Send outbound event: [" + output.getEventType() + "," + output.getPayload().toPrettyString() + "]");

                    out.collect(output);
                    logOutputEvent(event, output);
                    if ("agent-error".equals(output.getEventType())
                        && processingErrorsCounter != null) {
                        processingErrorsCounter.inc();
                    }
                }
            } else {
                LOG.debug("No outbound events to send");    
            }
        }
        if (eventsProcessedCounter != null) eventsProcessedCounter.inc();

        // ── Step 13: register cleanup timer for time-based horizons ────────
        registerCleanupTimer(ctx, state);
        LOG.debug("[{}] Event processing completed: {}. Current timestamp: {} [Watermark: {}]", agentTag(), event.toString(), ctx.timestamp(), ctx.currentWatermark());
    }

    /**
     * Timer callback, shared by two kinds of timer:
     * <ul>
     *   <li><b>processing time</b>: evict time-expired events from idle entities (event horizon);</li>
     *   <li><b>event time</b>: a schedule registered through a {@link ScheduleRequest} is due.
     *       A {@code Schedule} event is built for every request registered at {@code timestamp}
     *       and fed to {@link #processElement}.</li>
     * </ul>
     */
    @Override
    public void onTimer(long timestamp,
                        OnTimerContext ctx,
                        Collector<EventEnvelope> out) throws Exception {

        LOG.debug("Timer triggered at {} [Watermark: {}]", ctx.timestamp(), ctx.currentWatermark() );
        if (ctx.timeDomain() == TimeDomain.EVENT_TIME) {
            fireSchedules(timestamp, ctx, out);
            return;
        }
        AgentState state = agentStateHandle.value();
        if (state == null) return;
        state.evictExpiredEvents(timestamp);
        agentStateHandle.update(state);
        LOG.debug("[{}] onTimer eviction entityId={}", agentTag(),
            state.getMetadata().getEntityId());
    }

    // ── Scheduling ─────────────────────────────────────────────────────────────

    /** True when an operation has a {@code schedule} section or a Schedule subscription exists. */
    private boolean usesScheduling() {
        boolean opSchedules = descriptor.getRuleset().getOperations() != null
            && descriptor.getRuleset().getOperations().stream()
                .anyMatch(op -> op.scheduleRequestsField() != null);
        boolean scheduleSub = descriptor.getSubscriptions() != null
            && descriptor.getSubscriptions().stream()
                .anyMatch(EventSubscription::isScheduleSubscription);
        return opSchedules || scheduleSub;
    }

    /**
     * Reads the {@link ScheduleRequest} list from the rule result (field named by the
     * operation's {@code scheduleRequestsField}), validates it, and applies each request
     * to the entity's timers. Invalid requests are logged and skipped.
     */
    private void processScheduleRequests(JsonNode ruleResult, String operation,
                                       ReadOnlyContext ctx) throws Exception {
        if (scheduleManager == null || ruleResult == null) return;
        RulesetConfig.OperationConfig opConfig = descriptor.getRuleset().findOperation(operation);
        String field = opConfig != null ? opConfig.scheduleRequestsField() : null;
        if (field == null) return;

        List<ScheduleRequest> requests =
            ScheduleManager.parseRequests(ruleResult.get(field), agentTag());
        if (!requests.isEmpty()) {
            scheduleManager.apply(requests, ctx.timerService());
        }
    }

    /**
     * Handles an event-time timer: removes the schedules due at {@code timestamp} from state
     * and processes one {@code Schedule} event for each of them.
     */
    private void fireSchedules(long timestamp, ReadOnlyContext ctx,
                               Collector<EventEnvelope> out) throws Exception {
        if (scheduleManager == null) return;
        List<EventEnvelope> events = scheduleManager.fire(timestamp, ctx.getCurrentKey());
        LOG.debug("[{}] Schedule timer fired timestamp={} entityId={} schedules={}",
            agentTag(), timestamp, ctx.getCurrentKey(), events.size());
        if (!(ctx instanceof OnTimerContext)) {
            // if not called in the context of a timer, cancel the timer planned for this timestamp
            // if called in the context of a timer, Flink removes it 
            ctx.timerService().deleteEventTimeTimer(timestamp);
        }
        for (EventEnvelope event : events) {
            processElement(event, ctx, out);
        }
    }


    /**
     * Handle all event-time timers that are due at this {@code timestamp} and before. Schedules are executed 
     * in time order starting with the oldest. Returns the collection of outbound events generated by the processing 
     * of the schedules. 
     */
    private void fireAllSchedules(long timestamp, ReadOnlyContext ctx,
                                  Collector<EventEnvelope> out) throws Exception {
        if (scheduleManager == null) return;
        List<Long> allTimestamps = scheduleManager.getAllScheduledTimestamps(timestamp, ctx.getCurrentKey());
        for (Long ts : allTimestamps) {
            fireSchedules(ts, ctx, out);
        }

    }



    // ── Runner resolution ──────────────────────────────────────────────────────

    /**
     * Returns the operation name for the incoming event by finding the matching subscription.
     *
     * @return the non-blank operation name, or {@code null} if no subscription has an operation
     *         configured for this event (history-only subscription)
     */
    private String resolveOperation(EventEnvelope event) {
        if (descriptor.getSubscriptions() == null) return null;
        for (EventSubscription sub : descriptor.getSubscriptions()) {
            boolean topicMatch = sub.getTopic() != null
                && sub.getTopic().equals(event.getSourceTopic());
            boolean typeMatch  = sub.getEventType() != null
                && sub.getEventType().equals(event.getEventType());
            if (topicMatch || typeMatch) {
                return sub.hasOperation() ? sub.getOperation() : null;
            }
        }
        return null;
    }

    /**
     * Returns the active {@link RuleEngineRunner} for {@code operation}, reading
     * {@link #ACTIVE_KEY} from broadcast state and looking up (or creating on first use) the
     * runner from {@link #runnerCache}.  Falls back to the bootstrap runner when broadcast
     * state is empty (cold start).
     *
     * <p>Cache keys are {@code "<version>:<operationName>"} so multiple operations from the
     * same archive version each get their own runner.
     */
    private RuleEngineRunner resolveRunnerForOperation(String operation,
                                                       ReadOnlyContext ctx) throws Exception {
        ReadOnlyBroadcastState<String, ArchiveEntry> broadcastState =
            ctx.getBroadcastState(ARCHIVE_STATE_DESC);
        ArchiveEntry entry = broadcastState.get(ACTIVE_KEY);

        if (entry == null) {
            // Cold start or no reload yet — use bootstrap runner
            if (bootstrapEntry == null) {
                bootstrapEntry = new ArchiveEntry(
                    "bootstrap", null, null, System.currentTimeMillis());
            }
            entry = bootstrapEntry;
            LOG.debug("[{}] Broadcast state empty — using bootstrap runner for op={}", agentTag(), operation);
        }

        String cacheKey = versionedKey(entry.getVersion(), operation);
        RuleEngineRunner cached = runnerCache.get(cacheKey);
        if (cached != null) return cached;

        // Post-restore re-materialisation: re-download and re-initialise
        LOG.info("[{}] Re-initialising runner for v={} op={} (restore or cold-start)",
            agentTag(), entry.getVersion(), operation);
        java.net.URI jarUri;
        if (entry.getArchiveUrl() != null && !entry.getArchiveUrl().isBlank()
                && entry.getSha256() != null && !entry.getSha256().isBlank()) {
            jarUri = ArchiveDownloader
                .downloadAndVerify(entry.getArchiveUrl(), entry.getSha256()).toUri();
        } else {
            // Bootstrap entry — use local path
            String rulesetPath = descriptor.getRuleset().getPath();
            jarUri = rulesetPath.contains("://")
                ? java.net.URI.create(rulesetPath)
                : java.nio.file.Paths.get(rulesetPath).toUri();
        }
        RuleEngineRunner runner = new RuleEngineRunner(operation, jarUri);
        runner.initializeEagerly();
        runnerCache.put(cacheKey, runner);
        return runner;
    }

    // ── PREFETCH handling ──────────────────────────────────────────────────────

    private void handlePrefetch(ArchiveControlCmd cmd) {
        if (cmd.getArchiveUrl() == null || cmd.getArchiveUrl().isBlank()
                || cmd.getSha256() == null || cmd.getSha256().isBlank()) {
            LOG.warn("[{}] PREFETCH missing url/sha256 — ignoring (requestId={})",
                agentTag(), cmd.getRequestId());
            return;
        }
        try {
            Path localJar = ArchiveDownloader.downloadAndVerify(
                cmd.getArchiveUrl(), cmd.getSha256());
            LOG.info("[{}] ✓ PREFETCH download complete v={}", agentTag(), cmd.getVersion());

            String version = cmd.getVersion();
            if (version != null && !version.isBlank() && descriptor.getSubscriptions() != null) {
                descriptor.getSubscriptions().stream()
                    .filter(EventSubscription::hasOperation)
                    .map(EventSubscription::getOperation)
                    .distinct()
                    .forEach(op -> {
                        String cacheKey = versionedKey(version, op);
                        if (!runnerCache.containsKey(cacheKey)) {
                            RuleEngineRunner r = null;
                            try {
                                r = new RuleEngineRunner(op, localJar.toUri());
                                r.initializeEagerly();
                                runnerCache.put(cacheKey, r);
                                r = null;
                                LOG.info("[{}] ✓ PREFETCH runner pre-warmed v={} op={}", agentTag(), version, op);
                            } catch (Exception e) {
                                LOG.error("[{}] ✗ PREFETCH runner failed v={} op={}: {}", agentTag(), version, op, e.getMessage());
                                if (r != null) { try { r.close(); } catch (Exception ex) { /* ignore */ } }
                            }
                        }
                    });
            }
        } catch (com.ibm.di.event.rule.broadcast.ArchiveVerificationException ave) {
            LOG.error("[{}] ✗ PREFETCH SHA-256 mismatch v={}: {}", agentTag(),
                cmd.getVersion(), ave.getMessage());
            ArchiveDownloader.evict(cmd.getSha256());
        } catch (Exception e) {
            LOG.error("[{}] ✗ PREFETCH failed v={}: {}", agentTag(),
                cmd.getVersion(), e.getMessage());
            ArchiveDownloader.evict(cmd.getSha256());
        }
    }

    // ── RESET_STATE handling ───────────────────────────────────────────────────

    /**
     * Handles a {@link ArchiveControlCmd.CommandType#RESET_STATE} command by clearing the
     * {@link AgentState} for every entity currently held in keyed state via
     * {@link Context#applyToKeyedState}.
     *
     * <p>Both the entity data and the complete event-history are erased.  After this call
     * the next event for any entity will be treated as if the entity is brand-new, which
     * guarantees that subsequent test scenarios start from a clean slate.
     *
     * <p>The archive reload state (broadcast state) and the runner cache are deliberately
     * preserved — only per-entity keyed state is cleared.
     */
    private void handleResetState(ArchiveControlCmd cmd, Context ctx) {
        LOG.info("[{}] RESET_STATE — clearing all keyed entity state (requestId={})",
            agentTag(), cmd.getRequestId());
        try {
            ctx.applyToKeyedState(
                new org.apache.flink.api.common.state.ValueStateDescriptor<>(
                    "agent-state-" + descriptor.getAgentId(),
                    org.apache.flink.api.common.typeinfo.TypeInformation.of(AgentState.class)),
                (key, state) -> {
                    state.clear();
                    LOG.debug("[{}] RESET_STATE cleared entity state for key={}", agentTag(), key);
                });
            LOG.info("[{}] RESET_STATE complete", agentTag());
        } catch (Exception e) {
            LOG.error("[{}] RESET_STATE failed (requestId={}): {}",
                agentTag(), cmd.getRequestId(), e.getMessage());
        }
    }

    // ── Smoke test ─────────────────────────────────────────────────────────────

    /**
     * Validates a newly loaded archive by routing a representative probe event through it.
     *
     * <p>The probe is generated from the archive's own schema via
     * {@link RuleEngineRunner#generateSampleInput()} so it matches the exact field names
     * and types the rule engine expects — an empty payload would cause most operations to
     * return null or throw, making the validation unreliable.
     *
     * @throws RuntimeException to abort the archive reload on failure
     */
    private void runSmokeTest(RuleEngineRunner runner) {
        try {
            JsonNode probe = runner.generateSampleInput();
            JsonNode result = runner.execute(probe);
            if (result == null || result.isNull() || result.isEmpty()) {
                LOG.warn("[{}] [SMOKE TEST] Empty result — archive loaded but produced no output", agentTag());
            } else {
                LOG.info("[{}] [SMOKE TEST] OK", agentTag());
            }
        } catch (Exception e) {
            throw new RuntimeException("[SMOKE TEST] Failed: " + e.getMessage(), e);
        }
    }

    // ── Command validation ─────────────────────────────────────────────────────

    private boolean validateCmd(ArchiveControlCmd cmd) {
        if (cmd.getVersion() == null || cmd.getVersion().isBlank()) {
            LOG.error("[{}] Archive-control command missing version requestId={}",
                agentTag(), cmd.getRequestId());
            return false;
        }
        if (cmd.getArchiveUrl() == null || cmd.getArchiveUrl().isBlank()) {
            LOG.error("[{}] Archive-control command missing archiveUrl requestId={}",
                agentTag(), cmd.getRequestId());
            return false;
        }
        if (cmd.getSha256() == null || cmd.getSha256().isBlank()) {
            LOG.error("[{}] Archive-control command missing sha256 requestId={}",
                agentTag(), cmd.getRequestId());
            return false;
        }
        return true;
    }

    // ── Runner cache management ────────────────────────────────────────────────

    private void evictStaleRunners(String activeVersion) {
        runnerCache.entrySet().removeIf(e -> {
            // Keys are "<version>:<op>" — keep entries whose version segment matches activeVersion,
            // plus the legacy "bootstrap" key so warm-up runners survive their first use.
            String key = e.getKey();
            if (key.equals("bootstrap") || key.startsWith(activeVersion + ":")) return false;
            try { e.getValue().close(); } catch (Exception ex) {
                LOG.warn("[{}] Error closing stale runner key={}: {}",
                    agentTag(), key, ex.getMessage());
            }
            LOG.info("[{}] Evicted stale runner key={}", agentTag(), key);
            return true;
        });
    }

    // ── Cache key helpers ──────────────────────────────────────────────────────

    /** Cache key for a bootstrap (pre-broadcast) runner: {@code "bootstrap:<op>"}. */
    private static String bootstrapKey(String operation) {
        return "bootstrap:" + operation;
    }

    /** Cache key for a broadcast-reloaded runner: {@code "<version>:<op>"}. */
    private static String versionedKey(String version, String operation) {
        return version + ":" + operation;
    }

    private void closeAllRunners() {
        if (runnerCache == null) return;
        runnerCache.forEach((version, runner) -> {
            try { runner.close(); } catch (Exception ex) {
                LOG.warn("[{}] Error closing runner v={}: {}",
                    agentTag(), version, ex.getMessage());
            }
        });
        runnerCache.clear();
    }

    // ── Private event-processing helpers ──────────────────────────────────────

    private void updateEventHistory(AgentState state, EventEnvelope event) {
        if (descriptor.getSubscriptions() == null) return;
        for (EventSubscription sub : descriptor.getSubscriptions()) {
            boolean topicMatch = sub.getTopic() != null
                && sub.getTopic().equals(event.getSourceTopic());
            boolean typeMatch  = sub.getEventType() != null
                && sub.getEventType().equals(event.getEventType());
            if (!topicMatch && !typeMatch) continue;

            HorizonConfig horizon = sub.getHorizon();
            if (horizon == null) horizon = HorizonConfig.count(0);
            if ("count".equals(horizon.getType()) && horizon.effectiveCountLimit() <= 0) break;

            AgentState.HistoricalEvent he = new AgentState.HistoricalEvent(event.getTimestamp(), event.getPayload());
            state.addEvent(sub.getEventType(), he, horizon, event.getTimestamp());
            break;
        }
    }

    /**
     * Builds the JSON context for the Decision Intelligence rule engine by delegating to
     * {@link ContextBuilderRegistry}.  New context formats can be added by registering a
     * {@link com.ibm.di.event.agent.context.ContextBuilder} without changing this class.
     */
    private JsonNode buildRuleContext(AgentState state, EventEnvelope event) {
        return ContextBuilderRegistry
            .get("generic")
            .build(descriptor, state, event, objectMapper);
    }

    /**
     * Builds an {@link EventEnvelope} for the changelog side-output.
     *
     * <p>The payload carries the four standard changelog fields so consumers do not need
     * to look at envelope metadata to reconstruct the entity identity and snapshot time.
     */
    private EventEnvelope buildChangeLogEnvelope(EventEnvelope inbound, JsonNode entityAfter) {
        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("entityId",   safeString(inbound.getEntityId()));
        payload.put("entityType", safeString(descriptor.getEntityType()));
        payload.put("timestamp",  inbound.getTimestamp());
        payload.set("entity",     entityAfter);

        EventEnvelope env = new EventEnvelope();
        env.setEventType("entity-changelog");
        env.setEntityId(inbound.getEntityId());
        env.setSourceTopic(inbound.getSourceTopic());
        env.setTimestamp(inbound.getTimestamp());
        env.setPayload(payload);
        return env;
    }

    /**
     * Applies the updated entity state from the rule result to {@link AgentState#getEntityData()}.
     *
     * <p><b>Primary path</b>: reads {@code outboundEntityField} (or {@code entityField} if
     * {@code outboundEntityField} is not set) from the rule result.  For operations where the
     * Decision Intelligence archive returns the entity object under the configured field key,
     * this is the primary path that fires.
     *
     * <p><b>Payload fallback</b>: when the rule result does not contain the entity field
     * (e.g. context-enrichment operations whose archive returns a decision output rather than
     * the entity), the runtime falls back to reading the same-named field from the inbound
     * event's payload.  The fallback fires silently — no
     * warning is emitted — because the absent entity field in the result is the expected
     * behaviour for these context-accumulation operations.
     *
     * @param state      current per-entity state to mutate
     * @param ruleResult result returned by the Decision Intelligence archive
     * @param operation  operation name used to locate the {@link RulesetConfig.OperationConfig}
     * @param event      triggering inbound event whose payload is used for the fallback
     */
    private void updateEntityState(AgentState state, JsonNode ruleResult, String operation,
                                   EventEnvelope event) {
        JsonNode update = null;
        String entityField = null;

        // ── Primary path: outboundEntityField / entityField in the rule result ───
        RulesetConfig.OperationConfig opConfig =
            descriptor.getRuleset().findOperation(operation);
        if (opConfig != null) {
            String outboundEntityField = opConfig.getOutboundEntityField();
            String resultField = (outboundEntityField != null && !outboundEntityField.isBlank())
                ? outboundEntityField
                : opConfig.getEntityField();
            if (resultField != null && !resultField.isBlank()) {
                update = ruleResult.get(resultField);
            }
            entityField = opConfig.getEntityField();
        }

        // ── Payload fallback: entityField absent in result → read from payload ─
        // Fires when the archive returns a decision output rather
        // than the entity itself.  Context-accumulation operations use this path to
        // persist the relevant context fragment
        // from the event payload into entity state without requiring a JAR change.
        if ((update == null || update.isNull())
                && entityField != null && !entityField.isBlank()
                && event != null && event.getPayload() != null) {
            JsonNode payloadField = event.getPayload().get(entityField);
            if (payloadField != null && !payloadField.isNull() && payloadField.isObject()) {
                update = payloadField;
                LOG.debug("[{}] Entity state fallback: persisting payload.{} for op={}",
                    agentTag(), entityField, operation);
            }
        }

        if (update == null || update.isNull()) return;
        if (!update.isObject()) {
            LOG.warn("[{}] entity state source is not a JSON object — ignoring", agentTag());
            return;
        }

        if (update.equals(state.getEntityData())) {
            LOG.debug("Entity has not been modified. Don't save it.");    
            return;
        }

        LOG.debug("Entity has been modified. Save it.");
        state.setEntityData(update.deepCopy());
    }

    /**
     * Wraps a single rule-result node into an {@link EventEnvelope} tagged with {@code eventType}.
     *
     * <p>The envelope carries the inbound event's {@code entityId}, {@code sourceTopic}, and
     * {@code timestamp} so that:
     * <ul>
     *   <li>The Kafka sink's {@link AgentBasedDataStreamJob.EnvelopeKeySerializer} can emit the
     *       entity ID as the Kafka record key, enabling consumers to partition by entity.</li>
     *   <li>The BFF event store can identify which entity an outbound event belongs to when
     *       building the entity-state snapshot for the UI.</li>
     *   <li>Downstream agents in a chain receive a correctly-keyed envelope and can partition
     *       by the same entity ID without re-extracting it from the payload.</li>
     * </ul>
     */
    private EventEnvelope toEnvelope(EventEnvelope inbound, String eventType, JsonNode resultNode) {
        EventEnvelope out = new EventEnvelope();
        out.setEventType(eventType);
        out.setPayload(resultNode);
        // Propagate entity correlation fields from the triggering inbound event so
        // downstream consumers (Kafka sinks, BFF, chained agents) can identify the entity.
        out.setEntityId(inbound.getEntityId());
        out.setSourceTopic(inbound.getSourceTopic());
        out.setTimestamp(inbound.getTimestamp());
        return out;
    }

    /**
     * Builds the list of output envelopes from a rule result.
     *
     * <p>The rule result is an object that may contain one or more outbound-events arrays,
     * each described by a {@link RulesetConfig.OutboundConfig} entry in
     * {@link RulesetConfig.OperationConfig#getOutbounds()}.  Every entry specifies the
     * result-field name ({@code field}) that holds the array and the event type
     * ({@code eventType}) to assign to each element.  When no {@code outbounds} list is
     * configured the processor falls back to reading a single array from the conventional
     * field name {@code "outputEvents"} with event type {@code "object"}.
     *
     * <p>When {@code ruleResult} is {@code null} (rule engine error), exactly one
     * {@code "agent-error"} envelope is returned.
     *
     * @param inbound    the original inbound event
     * @param ruleResult rule evaluation result, or {@code null} on error
     * @param operation  the operation name used to look up the {@link RulesetConfig.OperationConfig}
     * @return           list of envelopes to emit (may be empty when no outbound arrays are found)
     */
    private List<EventEnvelope> buildOutputEvents(EventEnvelope inbound, JsonNode ruleResult,
                                                  String operation) {
        List<EventEnvelope> outputs = new ArrayList<>();

        if (ruleResult == null) {
            // Rule engine error path — emit a single agent-error envelope
            EventEnvelope errEnv = new EventEnvelope();
            errEnv.setEntityId(inbound.getEntityId());
            errEnv.setTimestamp(System.currentTimeMillis());
            errEnv.setSourceTopic(inbound.getSourceTopic());
            errEnv.setEventType("agent-error");
            ObjectNode errPayload = objectMapper.createObjectNode();
            errPayload.put("agentId",     descriptor.getAgentId());
            errPayload.put("entityId",    safeString(inbound.getEntityId()));
            errPayload.put("errorReason", "decision service returned null or threw exception");
            errPayload.set("originalEvent", inbound.getPayload() != null
                ? inbound.getPayload() : objectMapper.createObjectNode());
            errEnv.setPayload(errPayload);
            outputs.add(errEnv);
            return outputs;
        }

        if (!ruleResult.isObject()) return outputs;

        // Look up per-operation outbounds configuration.
        List<RulesetConfig.OutboundConfig> outboundsCfg = null;
        if (operation != null && descriptor.getRuleset() != null) {
            RulesetConfig.OperationConfig opCfg = descriptor.getRuleset().findOperation(operation);
            if (opCfg != null) {
                outboundsCfg = opCfg.getOutbounds();
            }
        }

        if (outboundsCfg != null && !outboundsCfg.isEmpty()) {
            // Configured path: iterate every outbound descriptor and collect matching array items.
            for (RulesetConfig.OutboundConfig outbound : outboundsCfg) {
                String fieldName = (outbound.getField() != null && !outbound.getField().isBlank())
                    ? outbound.getField() : "outputEvents";
                String eventType = (outbound.getEventType() != null && !outbound.getEventType().isBlank())
                    ? outbound.getEventType() : "object";
                JsonNode eventsNode = ruleResult.get(fieldName);
                if (eventsNode != null && eventsNode.isArray()) {
                    for (JsonNode eventNode : eventsNode) {
                        outputs.add(toEnvelope(inbound, eventType, eventNode));
                    }
                }
            }
        } else {
            // Fallback path: no outbounds list configured — read from the default "outputEvents" field.
            JsonNode eventsNode = ruleResult.get("outputEvents");
            if (eventsNode != null && eventsNode.isArray()) {
                for (JsonNode eventNode : eventsNode) {
                    outputs.add(toEnvelope(inbound, "object", eventNode));
                }
            }
        }
        return outputs;
    }

    /** Logs a single emitted output event at INFO level. */
    private void logOutputEvent(EventEnvelope inbound, EventEnvelope output) {
        if (!LOG.isInfoEnabled()) return;
        LOG.info("[{}] ⟶ OUTPUT eventType={} entityId={} inEventType={} inTopic={}",
            agentTag(),
            output.getEventType(),
            output.getEntityId(),
            inbound.getEventType(),
            inbound.getSourceTopic());
    }

    private void registerCleanupTimer(ReadOnlyContext ctx, AgentState state) {
        long minTimeLimitMs = Long.MAX_VALUE;
        if (descriptor.getSubscriptions() == null) return;
        for (EventSubscription sub : descriptor.getSubscriptions()) {
            HorizonConfig h = sub.getHorizon();
            if (h == null) continue;
            long tlMs = h.timeLimitMs();
            if (tlMs > 0 && tlMs < minTimeLimitMs) minTimeLimitMs = tlMs;
        }
        if (minTimeLimitMs == Long.MAX_VALUE) return;
        long timerTs = ctx.timerService().currentProcessingTime() + minTimeLimitMs;
        ctx.timerService().registerProcessingTimeTimer(timerTs);
        LOG.debug("[{}] Cleanup timer registered entityId={} fireAt={}",
            agentTag(), state.getMetadata().getEntityId(), timerTs);
    }

    private static long countHistorySize(AgentState state) {
        if (state == null || state.getEventHistories() == null) return 0L;
        long total = 0L;
        for (AgentState.EventHistory h : state.getEventHistories().values()) {
            total += h.size();
        }
        return total;
    }

    // ── Testing / introspection accessors ──────────────────────────────────────

    /** Returns the descriptor this processor was constructed with. */
    public AgentDescriptor getDescriptor() { return descriptor; }

    /**
     * Injects a pre-configured {@link RuleEngineRunner} into the runner cache under the
     * given {@code version} key, bypassing DI classloader initialisation.
     * Safe to call before {@link #open} because {@link #runnerCache} is field-initialised.
     */
    public void injectRunner(String version, RuleEngineRunner engine) {
        runnerCache.put(version, engine);
    }

    /**
     * Exposes the runner cache for test assertions.
     * Returns the live map — callers must not modify it.
     */
    public Map<String, RuleEngineRunner> getRunnerCache() { return runnerCache; }

    /**
     * Invokes {@link #processBroadcastElement} with a minimal in-memory context that
     * captures broadcast-state writes into {@code broadcastStateStore}.
     *
     * <p>This bypasses the Flink runtime so unit tests can verify LOAD/PREFETCH/ROLLBACK/NOOP
     * dispatch, runner-cache mutation, and broadcast-state updates without a live cluster.
     *
     * @param cmd                the archive-control command to process
     * @param broadcastStateStore mutable map representing the single-entry broadcast state
     *                            ({@code "ACTIVE" → ArchiveEntry}); may be pre-populated to
     *                            simulate a checkpoint-restore scenario
     * @return this processor (for fluent assertions)
     */
    public StatefulAgentProcessor invokeProcessBroadcastForTest(
            ArchiveControlCmd cmd,
            java.util.Map<String, ArchiveEntry> broadcastStateStore) {
        if (objectMapper == null) objectMapper = new ObjectMapper();
        processBroadcastElement(cmd, buildBroadcastTestContext(broadcastStateStore, null), null);
        return this;
    }

    /**
     * Invokes {@link #processBroadcastElement} with a context that, in addition to the
     * broadcast state, wires {@code applyToKeyedState} to call the given function on each
     * entry in {@code keyedStateStore}.
     *
     * <p>Use this overload when testing commands that iterate keyed state (e.g.
     * {@link ArchiveControlCmd.CommandType#RESET_STATE}).
     *
     * @param cmd                 the archive-control command to process
     * @param broadcastStateStore mutable map representing the broadcast state
     * @param keyedStateStore     mutable map of {@code entityId → AgentState} representing
     *                            the keyed state that {@code applyToKeyedState} will visit;
     *                            entries cleared by a {@code RESET_STATE} command are removed
     *                            from this map
     * @return this processor (for fluent assertions)
     */
    public StatefulAgentProcessor invokeProcessBroadcastForTest(
            ArchiveControlCmd cmd,
            java.util.Map<String, ArchiveEntry> broadcastStateStore,
            java.util.Map<String, AgentState> keyedStateStore) {
        if (objectMapper == null) objectMapper = new ObjectMapper();
        processBroadcastElement(
            cmd, buildBroadcastTestContext(broadcastStateStore, keyedStateStore), null);
        return this;
    }

    /**
     * Convenience test helper: sends a {@link ArchiveControlCmd.CommandType#RESET_STATE}
     * command and applies it to {@code keyedStateStore}.
     *
     * <p>Each {@link AgentState} value in the map whose state is cleared by the command will
     * have {@link org.apache.flink.api.common.state.State#clear()} called on a thin wrapper,
     * which removes the entry from {@code keyedStateStore}.
     *
     * @param requestId       opaque correlation ID for the reset command (may be {@code null})
     * @param keyedStateStore mutable map of {@code entityId → AgentState} to be cleared
     * @return this processor (for fluent assertions)
     */
    public StatefulAgentProcessor invokeResetForTest(
            String requestId,
            java.util.Map<String, AgentState> keyedStateStore) {
        ArchiveControlCmd resetCmd = new ArchiveControlCmd();
        resetCmd.setCommandType(ArchiveControlCmd.CommandType.RESET_STATE);
        resetCmd.setRequestId(requestId);
        return invokeProcessBroadcastForTest(resetCmd, new java.util.HashMap<>(), keyedStateStore);
    }

    /**
     * Builds a minimal {@link Context} stub whose {@code getBroadcastState} delegates to
     * {@code broadcastStore}, and whose {@code applyToKeyedState} iterates {@code keyedStore}
     * (when non-null), applying the given function to a thin in-memory
     * {@link org.apache.flink.api.common.state.ValueState} wrapper for each entry.
     *
     * <p>Defined as a named method (not a lambda) because {@code Context} is an abstract
     * inner class of {@link KeyedBroadcastProcessFunction}: the compiler requires the
     * anonymous subclass to be created in a method where {@code this} is already a
     * {@code KeyedBroadcastProcessFunction} instance — which is true here.
     *
     * @param broadcastStore mutable map representing the single-entry broadcast state
     * @param keyedStore     mutable map of {@code entityId → AgentState} to be visited by
     *                       {@code applyToKeyedState}; {@code null} means the operation is a no-op
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private Context buildBroadcastTestContext(java.util.Map<String, ArchiveEntry> broadcastStore,
                                              java.util.Map<String, AgentState> keyedStore) {
        return new Context() {
            @Override
            public <K, V> org.apache.flink.api.common.state.BroadcastState<K, V>
                    getBroadcastState(
                            org.apache.flink.api.common.state.MapStateDescriptor<K, V> desc) {
                // Thin in-memory BroadcastState backed by `broadcastStore`.
                // Flink 2.2.0 BroadcastState interface methods: put, putAll, remove,
                // iterator, entries (from BroadcastState) + get, contains,
                // immutableEntries (from ReadOnlyBroadcastState) + clear (from State).
                return new org.apache.flink.api.common.state.BroadcastState<>() {
                    @Override
                    public V get(K k) {
                        return (V) broadcastStore.get(k);
                    }

                    @Override
                    public boolean contains(K k) {
                        return broadcastStore.containsKey(k);
                    }

                    @Override
                    public Iterable<java.util.Map.Entry<K, V>> immutableEntries() {
                        return (Iterable) Map.copyOf(broadcastStore).entrySet();
                    }

                    @Override
                    public void put(K k, V v) {
                        broadcastStore.put((String) k, (ArchiveEntry) v);
                    }

                    @Override
                    public void putAll(java.util.Map<K, V> m) {
                        m.forEach((k, v) -> broadcastStore.put((String) k, (ArchiveEntry) v));
                    }

                    @Override
                    public void remove(K k) {
                        broadcastStore.remove(k);
                    }

                    @Override
                    public java.util.Iterator<java.util.Map.Entry<K, V>> iterator() {
                        return (java.util.Iterator<java.util.Map.Entry<K, V>>) (java.util.Iterator<?>) broadcastStore.entrySet().iterator();
                    }

                    @Override
                    public Iterable<java.util.Map.Entry<K, V>> entries() {
                        return (Iterable) new java.util.HashMap<>(broadcastStore).entrySet();
                    }

                    @Override
                    public void clear() {
                        broadcastStore.clear();
                    }
                };
            }

            @Override
            @SuppressWarnings("unchecked")
            public <VS, S extends org.apache.flink.api.common.state.State> void
                    applyToKeyedState(
                            org.apache.flink.api.common.state.StateDescriptor<S, VS> stateDesc,
                            org.apache.flink.runtime.state.KeyedStateFunction<String, S> func) {
                if (keyedStore == null) return;
                // Wrap each AgentState in a minimal ValueState stub and call func.
                // A snapshot of the key set is taken so the lambda can safely remove entries
                // (via clear()) without ConcurrentModificationException.
                for (String key : new java.util.ArrayList<>(keyedStore.keySet())) {
                    AgentState agentState = keyedStore.get(key);
                    if (agentState == null) continue;
                    org.apache.flink.api.common.state.ValueState<AgentState> valueStateStub =
                        new org.apache.flink.api.common.state.ValueState<>() {
                            @Override public AgentState value()         { return agentState; }
                            @Override public void update(AgentState s)  { keyedStore.put(key, s); }
                            @Override public void clear()               { keyedStore.remove(key); }
                        };
                    try {
                        func.process(key, (S) valueStateStub);
                    } catch (Exception e) {
                        LOG.warn("[{}] applyToKeyedState test stub: error for key={}: {}",
                            agentTag(), key, e.getMessage());
                    }
                }
            }

            @Override public Long timestamp()             { return null; }
            @Override public long currentProcessingTime() { return System.currentTimeMillis(); }
            @Override public long currentWatermark()      { return Long.MIN_VALUE; }
            @Override
            public <X> void output(org.apache.flink.util.OutputTag<X> tag, X value) {
                // DLQ outputs are swallowed in unit tests; callers inspect runnerCache directly
            }
        };
    }

    /**
     * Injects a {@link RuleEngineRunner} under all bootstrap keys for each operation
     * in the descriptor's subscriptions.  Provides a single-call convenience for tests
     * that use one stub engine for all operations.
     */
    public void injectRuleEngine(RuleEngineRunner engine) {
        if (descriptor.getSubscriptions() != null) {
            descriptor.getSubscriptions().stream()
                .filter(EventSubscription::hasOperation)
                .map(EventSubscription::getOperation)
                .distinct()
                .forEach(op -> injectRunner(bootstrapKey(op), engine));
        }
        // Also register under the legacy "bootstrap" key so invokeForTest still works.
        injectRunner("bootstrap", engine);
    }

    /**
     * Injects a {@link ScheduleManager} for unit tests that exercise the scheduling path.
     * Allows tests to verify scheduling behaviour without a live Flink state backend.
     */
    public void injectScheduleManagerForTest(ScheduleManager mgr) { this.scheduleManager = mgr; }

    /**
     * Injects an {@link ObjectMapper} for unit tests.
     */
    public void injectObjectMapper(ObjectMapper mapper) { this.objectMapper = mapper; }

    /**
     * Directly invokes the processing pipeline steps 1–7 without the Flink harness.
     *
     * <p>Uses the bootstrap runner from {@link #runnerCache} (key {@code "bootstrap"}).
     * Timer registration (step 8) is skipped — no {@link ReadOnlyContext} is available.
     *
     * @param event        inbound event
     * @param currentState current entity state, or {@code null} for a new entity
     * @return the output event that would be emitted, or {@code null} when the matched
     *         subscription has no operation (history-only) — mirroring {@link #processElement}
     *         which persists state and emits nothing in that case
     */
    public EventEnvelope invokeForTest(EventEnvelope event, AgentState currentState) {
        if (objectMapper == null) objectMapper = new ObjectMapper();

        // Mirror the sentinel path from processElement: clear the state and return null
        // (no output is emitted for a reset event, and no state is created when null).
        if (ENTITY_RESET_EVENT_TYPE.equals(event.getEventType())) {
            if (currentState != null) {
                currentState.setEntityData(null);
                currentState.setEventHistories(new java.util.HashMap<>());
            }
            LOG.info("[{}] invokeForTest: entity state cleared for entityId={}",
                agentTag(), event.getEntityId());
            return null;
        }

        AgentState state = currentState;
        if (state == null) {
            state = AgentState.init(event.getEntityId(),
                descriptor.getEntityType(), event.getTimestamp());
        }
        updateEventHistory(state, event);

        // Resolve per-subscription operation; fall back to legacy "bootstrap" key for tests
        // that inject a runner without per-operation subscriptions configured.
        String op = resolveOperation(event);
        RuleEngineRunner runner = op != null
            ? runnerCache.getOrDefault(bootstrapKey(op), runnerCache.get("bootstrap"))
            : runnerCache.get("bootstrap");

        if (op == null) {
            // History-only subscription — return null, mirroring the real processElement
            // path which persists state and emits nothing for operation-free subscriptions.
            // History is already accumulated into `state` by updateEventHistory above.
            return null;
        }

        JsonNode ruleContext = buildRuleContext(state, event);
        JsonNode ruleResult  = null;
        if (runner != null) {
            try { ruleResult = runner.execute(ruleContext); }
            catch (Exception e) {
                LOG.warn("[{}] invokeForTest: rule engine error: {}", agentTag(), e.getMessage());
            }
        }
        if (ruleResult != null) updateEntityState(state, ruleResult, op, event);
        List<EventEnvelope> outs = buildOutputEvents(event, ruleResult, op);
        return outs.isEmpty() ? null : outs.getFirst();
    }

    /**
     * Exposes {@link #buildRuleContext} for unit tests.
     */
    public JsonNode buildRuleContextForTest(AgentState state, EventEnvelope event) {
        if (objectMapper == null) objectMapper = new ObjectMapper();
        return buildRuleContext(state, event);
    }

    /**
     * Exposes {@link #runSmokeTest} for unit tests.
     * Allows tests to verify the smoke-test probe/execute/result logic using a stub runner
     * without requiring a real Decision Intelligence JAR or the full archive-reload path.
     *
     * @throws RuntimeException (re-thrown from {@link #runSmokeTest}) when the probe fails
     */
    public void runSmokeTestForTest(RuleEngineRunner runner) {
        if (objectMapper == null) objectMapper = new ObjectMapper();
        runSmokeTest(runner);
    }

    // ── Utility ────────────────────────────────────────────────────────────────

    private String agentTag() {
        return "StatefulAgentProcessor[" + descriptor.getAgentId() + "]";
    }

    private static String safeString(String s) {
        return s != null ? s : "";
    }
}

