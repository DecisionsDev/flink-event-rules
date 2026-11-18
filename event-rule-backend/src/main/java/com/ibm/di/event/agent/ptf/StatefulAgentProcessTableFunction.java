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
package com.ibm.di.event.agent.ptf;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.di.event.agent.AgentDescriptor;
import com.ibm.di.event.agent.AgentDescriptorLoader;
import com.ibm.di.event.agent.EventEnvelope;
import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.ArgumentTrait;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.StateHint;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Process Table Function that runs one agent: the Flink Table API counterpart of
 * {@code StatefulAgentProcessor}.
 *
 * <h3>Call</h3>
 * <pre>
 * SELECT * FROM agent_fn(
 *     events   =&gt; TABLE agent_input PARTITION BY entity_id,
 *     on_time  =&gt; DESCRIPTOR(event_ts),
 *     uid      =&gt; 'agent-processor-my-agent')
 * </pre>
 * The input table has the columns {@code entity_id, event_type, source_topic, payload (JSON
 * string), event_ts}. The output has {@code entity_id} (the partition key, added by Flink),
 * {@code out_type, source_topic, out_ts, payload}.
 *
 * <h3>State and timers</h3>
 * The entity state is one JSON string ({@link AgentPtfState}) and the pending schedules a second
 * state ({@link SchedulePtfState}). Event-time timers, all named so that registering again
 * replaces them: {@code cleanup} (horizon eviction of an idle entity), {@code ttl} (emulates
 * {@code stateConfig.ttl}) and {@code sched-<epochMs>} (one per pending schedule timepoint).
 *
 * <h3>Configuration</h3>
 * The descriptor is given either to the constructor (open source Flink,
 * {@code createTemporarySystemFunction(name, instance)}) or, for a function created with
 * {@code CREATE FUNCTION ... USING JAR} on Confluent Cloud (public no-argument constructor
 * required), by one of:
 * <ul>
 *   <li>the optional scalar argument {@code agent_config}, the descriptor serialized as JSON;</li>
 *   <li>the optional scalar arguments {@code agents_file} and {@code agent_id}: the agent is looked
 *       up in the use case's {@code agents.json}, bundled in the function JAR at the path
 *       {@code agents_file} (for example {@code samples/my-use-case/agents.json}, next to the
 *       ruleset archive), so the SQL stays small; without {@code agent_id}, the file must hold a
 *       single agent.</li>
 * </ul>
 *
 * <h3>Not supported compared to the DataStream engine</h3>
 * Ruleset hot reload through {@code archiveControlTopic} (no broadcast semantics in Process Table
 * Functions) and SQL enrichment.
 */
@DataTypeHint("ROW<out_type STRING, envelope STRING>")
public class StatefulAgentProcessTableFunction extends ProcessTableFunction<Row> {

    private static final Logger LOG = LoggerFactory.getLogger(StatefulAgentProcessTableFunction.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Name of the horizon cleanup timer. */
    static final String TIMER_CLEANUP = "cleanup";
    /** Name of the state time-to-live timer. */
    static final String TIMER_TTL = "ttl";
    /** Prefix of the schedule timers; the suffix is the timepoint in epoch milliseconds. */
    static final String TIMER_SCHEDULE_PREFIX = "sched-";

    /** Job parameter holding the descriptor JSON, read by {@link #open}. */
    static final String CONFIG_JOB_PARAMETER = "event-rule.agent-config";

    private AgentDescriptor descriptor;
    private AgentEventProcessor.RunnerProvider runnerProvider;

    private transient AgentEventProcessor processor;
    private transient RulesetRunnerProvider ownedRunners;
    private transient long ttlMs;

    /** Required by Confluent Cloud: the descriptor then comes from the {@code agent_config} argument. */
    public StatefulAgentProcessTableFunction() {}

    public StatefulAgentProcessTableFunction(AgentDescriptor descriptor) {
        this.descriptor = descriptor;
    }

    /** Tests: use {@code runnerProvider} instead of loading the ruleset archive. */
    public StatefulAgentProcessTableFunction(AgentDescriptor descriptor,
                                             AgentEventProcessor.RunnerProvider runnerProvider) {
        this.descriptor = descriptor;
        this.runnerProvider = runnerProvider;
    }

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    @Override
    public void open(FunctionContext context) throws Exception {
        if (descriptor == null) {
            String json = context.getJobParameter(CONFIG_JOB_PARAMETER, null);
            if (json != null) descriptor = parseDescriptor(json);
        }
        if (descriptor != null) initialize();
    }

    private static AgentDescriptor parseDescriptor(String json) {
        return AgentDescriptors.fromJson(json);
    }

    @Override
    public void close() {
        if (ownedRunners != null) ownedRunners.close();
    }

    private void initialize() {
        if (processor != null) return;
        AgentEventProcessor.RunnerProvider provider = runnerProvider;
        if (provider == null) {
            ownedRunners = new RulesetRunnerProvider(descriptor);
            ownedRunners.warmUp();
            provider = ownedRunners;
        }
        processor = new AgentEventProcessor(descriptor, provider);
        Duration ttl = descriptor.getStateConfig() != null ? descriptor.getStateConfig().ttlDuration() : null;
        ttlMs = ttl != null ? ttl.toMillis() : 0L;
        if (descriptor.getRuleset() != null && descriptor.getRuleset().hasBroadcastReload()) {
            LOG.warn("[{}] archiveControlTopic is ignored by the Process Table Function engine: "
                + "it has no broadcast semantics, the ruleset is fixed at deployment.",
                descriptor.getAgentId());
        }
    }

    private void initialize(String agentConfig, String agentsFile, String agentId) {
        if (processor != null) return;
        if (descriptor == null) {
            if (agentConfig != null && !agentConfig.isBlank()) {
                descriptor = parseDescriptor(agentConfig);
            } else {
                descriptor = bundledAgent(StatefulAgentProcessTableFunction.class.getClassLoader(),
                    agentsFile, agentId);
            }
        }
        initialize();
    }

    /**
     * The agent {@code agentId} of the {@code agents.json} bundled in the function JAR at the
     * classpath path {@code agentsFile}; when no id is given, the only agent of that file.
     */
    static AgentDescriptor bundledAgent(ClassLoader classLoader, String agentsFile, String agentId) {
        if (agentsFile == null || agentsFile.isBlank()) {
            throw new IllegalStateException("No agent descriptor: pass it to the constructor, set the job "
                + "parameter " + CONFIG_JOB_PARAMETER
                + ", or give the 'agent_config' argument or the 'agents_file' argument");
        }
        String resource = agentsFile.replace('\\', '/').replaceFirst("^/+", "");
        List<AgentDescriptor> agents;
        try (java.io.InputStream in = classLoader.getResourceAsStream(resource)) {
            if (in == null) {
                throw new IllegalStateException("The agents file '" + agentsFile
                    + "' is not bundled in the function JAR");
            }
            agents = AgentDescriptorLoader.parseFileDescriptor(
                new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8), resource).agents();
        } catch (java.io.IOException e) {
            throw new IllegalStateException("Cannot read the bundled " + agentsFile + ": " + e.getMessage(), e);
        }
        if (agentId == null || agentId.isBlank()) {
            if (agents.size() == 1) return agents.get(0);
            throw new IllegalStateException(agentsFile + " holds " + agents.size()
                + " agents: give the 'agent_id' argument");
        }
        return agents.stream().filter(a -> agentId.equals(a.getAgentId())).findFirst()
            .orElseThrow(() -> new IllegalStateException("Agent '" + agentId + "' is not in " + agentsFile));
    }

    // ── Event ─────────────────────────────────────────────────────────────────

    /**
     * Processes one event row of an entity.
     *
     * @param ctx         Flink context (timers)
     * @param agent       entity state
     * @param schedules   pending schedules
     * @param events      the input row
     * @param agentConfig optional descriptor JSON, needed only when the constructor gave none
     * @param agentsFile  optional classpath path of the bundled {@code agents.json}
     * @param agentId     optional id of the agent in that file
     */
    public void eval(Context ctx,
                     @StateHint(name = "agent") AgentPtfState agent,
                     @StateHint(name = "schedules") SchedulePtfState schedules,
                     @ArgumentHint(value = {ArgumentTrait.SET_SEMANTIC_TABLE, ArgumentTrait.REQUIRE_ON_TIME},
                                   name = "events") Row events,
                     @ArgumentHint(value = ArgumentTrait.SCALAR, name = "agent_config",
                                   isOptional = true) String agentConfig,
                     @ArgumentHint(value = ArgumentTrait.SCALAR, name = "agents_file",
                                   isOptional = true) String agentsFile,
                     @ArgumentHint(value = ArgumentTrait.SCALAR, name = "agent_id",
                                   isOptional = true) String agentId) throws Exception {
        initialize(agentConfig, agentsFile, agentId);

        EventEnvelope event = toEnvelope(ctx, events);
        AgentEventProcessor.Session session = newSession(agent, schedules);
        processor.process(session, event);
        finish(ctx, session, agent, schedules, null);
    }

    /**
     * Fires the schedules due at the timer time and evicts the time-expired events. A timer restored
     * after a restart can fire before any {@code eval} has supplied the agent; it is then dropped.
     */
    public void onTimer(OnTimerContext ctx,
                        @StateHint(name = "agent") AgentPtfState agent,
                        @StateHint(name = "schedules") SchedulePtfState schedules) throws Exception {
        String timer = ctx.currentTimer();
        if (TIMER_TTL.equals(timer)) {
            // The entity has been idle for the whole time-to-live: drop its state, keep its schedules.
            agent.data = null;
            return;
        }
        if (descriptor == null) {
            LOG.warn("Timer {} dropped: no agent descriptor known yet, the function has not been called "
                + "since it started", timer);
            return;
        }
        initialize();
        long timestamp = ctx.timeContext(Instant.class).time().toEpochMilli();
        AgentEventProcessor.Session session = newSession(agent, schedules);
        processor.onTimer(session, timestamp);
        finish(ctx, session, agent, schedules, timer);
    }

    // ── Mapping between Flink and the processor ───────────────────────────────

    private AgentEventProcessor.Session newSession(AgentPtfState agent, SchedulePtfState schedules) {
        return new AgentEventProcessor.Session(
            AgentStateCodec.decode(agent.data, descriptor), new ScheduleBook(schedules));
    }

    private EventEnvelope toEnvelope(Context ctx, Row row) throws Exception {
        String payload = row.getFieldAs("payload");
        JsonNode node = payload != null ? MAPPER.readTree(payload) : MAPPER.createObjectNode();
        Instant ts = ctx.timeContext(Instant.class).time();
        return EventEnvelope.from(row.getFieldAs("event_type"), row.getFieldAs("source_topic"),
            ts.toEpochMilli(), node, row.getFieldAs("entity_id"));
    }

    /** Writes state, rows and timers back after the processor ran. */
    private void finish(Context ctx, AgentEventProcessor.Session session, AgentPtfState agent,
                        SchedulePtfState schedules, String firedTimer) {
        agent.data = AgentStateCodec.encode(session.state);
        for (EventEnvelope out : session.outputs) {
            collect(Row.of(out.getEventType(), EnvelopeJson.write(out)));
        }

        var time = ctx.timeContext(Instant.class);
        if (session.reset) {
            ctx.clearAllTimers();
            return;
        }
        for (Long ms : session.timersToClear()) {
            String name = TIMER_SCHEDULE_PREFIX + ms;
            if (!name.equals(firedTimer)) time.clearTimer(name);
        }
        for (Long ms : session.timersToRegister()) {
            time.registerOnTime(TIMER_SCHEDULE_PREFIX + ms, Instant.ofEpochMilli(ms));
        }
        Long cleanupAt = processor.cleanupTimerAt(session);
        if (cleanupAt != null && session.state != null) {
            time.registerOnTime(TIMER_CLEANUP, Instant.ofEpochMilli(cleanupAt));
        }
        if (ttlMs > 0 && session.state != null && session.lastEventTs() != Long.MIN_VALUE) {
            time.registerOnTime(TIMER_TTL, Instant.ofEpochMilli(session.lastEventTs() + ttlMs));
        }
    }
}
