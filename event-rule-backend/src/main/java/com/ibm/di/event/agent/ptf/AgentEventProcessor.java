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
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ibm.di.event.agent.AgentDescriptor;
import com.ibm.di.event.agent.AgentState;
import com.ibm.di.event.agent.EventEnvelope;
import com.ibm.di.event.agent.EventSubscription;
import com.ibm.di.event.agent.HorizonConfig;
import com.ibm.di.event.agent.RulesetConfig;
import com.ibm.di.event.agent.ScheduleRequests;
import com.ibm.di.event.agent.ScheduleRequest;
import com.ibm.di.event.agent.context.ContextBuilderRegistry;
import com.ibm.di.event.rule.RuleEngineRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * The per-event pipeline of an agent, independent of any Flink API.
 *
 * <p>It is the Process Table Function counterpart of
 * {@code StatefulAgentProcessor.processElement}/{@code onTimer} and applies the same steps in the
 * same order: entity reset, state initialisation, firing of due schedules, operation lookup,
 * horizon eviction, rule execution, entity update, changelog, schedule requests, history update and
 * output construction. The Process Table Function only maps its inputs to a {@link Session}, calls
 * this class, and maps the {@link Session} back to rows, state and timers.
 *
 * <p>Instances are thread-confined; the {@link RunnerProvider} must be safe for the same use.
 */
public final class AgentEventProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(AgentEventProcessor.class);

    /** Event type of the sentinel that clears the state of an entity. */
    public static final String ENTITY_RESET_EVENT_TYPE = "__ENTITY_RESET__";

    /** Event type of the envelopes written to the changelog topic. */
    public static final String CHANGELOG_EVENT_TYPE = "entity-changelog";

    /** Event type of the envelopes written to the error topic. */
    public static final String ERROR_EVENT_TYPE = "agent-error";

    /** Supplies the rule engine runner of an operation. */
    @FunctionalInterface
    public interface RunnerProvider extends java.io.Serializable {
        RuleEngineRunner runnerFor(String operation) throws Exception;
    }

    /**
     * Everything one invocation reads and produces. The caller creates it from the Flink state,
     * and reads {@link #state}, {@link #book}, {@link #outputs} and the timer deltas afterwards.
     */
    public static final class Session {

        /** Entity state; {@code null} when the entity has none, or after a reset. */
        public AgentState state;

        /** Pending schedules of the entity. */
        public final ScheduleBook book;

        /** Events to emit, in order: agent output, errors and changelog. */
        public final List<EventEnvelope> outputs = new ArrayList<>();

        /** True when the entity was reset during this invocation. */
        public boolean reset;

        private final Set<Long> timepointsBefore;
        private long lastEventTs = Long.MIN_VALUE;

        public Session(AgentState state, ScheduleBook book) {
            this.state = state;
            this.book = book;
            this.timepointsBefore = new TreeSet<>(book.timepoints());
        }

        /** Timepoints that need a timer after this invocation and had none before it. */
        public Set<Long> timersToRegister() {
            Set<Long> result = new TreeSet<>(book.timepoints());
            result.removeAll(timepointsBefore);
            return result;
        }

        /** Timepoints that had a timer before this invocation and have no pending schedule now. */
        public Set<Long> timersToClear() {
            Set<Long> result = new TreeSet<>(timepointsBefore);
            result.removeAll(book.timepoints());
            return result;
        }

        /** Largest event time seen by this invocation, or {@code Long.MIN_VALUE}. */
        public long lastEventTs() {
            return lastEventTs;
        }
    }

    private final AgentDescriptor descriptor;
    private final RunnerProvider runners;
    private final ObjectMapper mapper = new ObjectMapper();
    private final long minTimeLimitMs;

    public AgentEventProcessor(AgentDescriptor descriptor, RunnerProvider runners) {
        this.descriptor = descriptor;
        this.runners = runners;
        this.minTimeLimitMs = minTimeLimitMs(descriptor);
    }

    // ── Entry points ──────────────────────────────────────────────────────────

    /** Processes one inbound event. */
    public void process(Session s, EventEnvelope event) throws Exception {
        if (event == null) {
            LOG.warn("[{}] Null event received — skipping", descriptor.getAgentId());
            return;
        }
        s.book.setEntityId(event.getEntityId());

        // ── Sentinel: targeted per-entity state reset ─────────────────────
        if (isEntityResetEvent(event)) {
            s.state = null;
            s.book.cancelAll();
            s.reset = true;
            LOG.info("[{}] Entity state cleared for entityId={}",
                descriptor.getAgentId(), event.getEntityId());
            return;
        }

        // ── Step 1: get or init the state ─────────────────────────────────
        if (s.state == null) {
            s.state = AgentState.init(event.getEntityId(), descriptor.getEntityType(),
                event.getTimestamp());
        }
        s.lastEventTs = Math.max(s.lastEventTs, event.getTimestamp());

        // ── Step 2: fire the schedules due at this event time ─────────────
        // Never for a Schedule event: it is itself the product of this round, and a rule that
        // starts a schedule for a time <= now would otherwise recurse forever.
        if (!ScheduleRequests.SCHEDULE_EVENT_TYPE.equals(event.getEventType())) {
            fireDueSchedules(s, event.getTimestamp());
        }

        // ── Steps 3 and 4: operation lookup and horizon eviction ──────────
        String operation = resolveOperation(event);
        s.state.evictExpiredEvents(event.getTimestamp());

        JsonNode ruleResult = null;
        if (operation != null) {
            JsonNode ruleContext = ContextBuilderRegistry.get("generic")
                .build(descriptor, s.state, event, mapper);

            // ── Steps 5 and 6: rule execution ─────────────────────────────
            try {
                ruleResult = runners.runnerFor(operation).execute(ruleContext);
            } catch (Exception e) {
                LOG.error("[{}] Rule engine error entityId={} operation={}: {}",
                    descriptor.getAgentId(), event.getEntityId(), operation, e.getMessage());
            }

            // ── Step 7: entity update ─────────────────────────────────────
            JsonNode entityBefore = s.state.getEntityData() != null
                ? s.state.getEntityData().deepCopy() : null;
            if (ruleResult != null) {
                updateEntityState(s.state, ruleResult, operation, event);
            }

            // ── Step 8: changelog ─────────────────────────────────────────
            if (descriptor.getStateConfig() != null
                    && descriptor.getStateConfig().hasChangeLogTopic()) {
                JsonNode entityAfter = s.state.getEntityData();
                if (entityAfter != null && !entityAfter.equals(entityBefore)) {
                    s.outputs.add(buildChangeLogEnvelope(event, entityAfter));
                }
            }

            // ── Step 9: schedule requests ─────────────────────────────────
            processScheduleRequests(s, ruleResult, operation);
        }

        // ── Step 10: history ──────────────────────────────────────────────
        updateEventHistory(s.state, event);

        // ── Step 12: outputs ──────────────────────────────────────────────
        if (operation != null) {
            s.outputs.addAll(buildOutputEvents(event, ruleResult, operation));
        }
    }

    /**
     * Processes the timer of {@code timestamp}: fires the schedules due at that time and evicts the
     * time-expired events. Both are always done, because the horizon cleanup timer and a schedule
     * timer may share a timestamp.
     */
    public void onTimer(Session s, long timestamp) throws Exception {
        fireSchedules(s, timestamp);
        if (s.state != null) {
            s.state.evictExpiredEvents(timestamp);
        }
    }

    /**
     * Time at which the horizon cleanup timer must fire, or {@code null} when no subscription has a
     * time horizon: the newest event seen plus the shortest time horizon, plus one millisecond.
     */
    public Long cleanupTimerAt(Session s) {
        if (minTimeLimitMs == Long.MAX_VALUE || s.lastEventTs == Long.MIN_VALUE) return null;
        return s.lastEventTs + minTimeLimitMs + 1;
    }

    // ── Scheduling ────────────────────────────────────────────────────────────

    /** Fires every timepoint {@code <= timestamp}, oldest first. */
    private void fireDueSchedules(Session s, long timestamp) throws Exception {
        for (Long ts : s.book.dueTimepoints(timestamp)) {
            fireSchedules(s, ts);
        }
    }

    /** Removes the requests of {@code timestamp} and processes one Schedule event per request. */
    private void fireSchedules(Session s, long timestamp) throws Exception {
        for (ScheduleRequest r : s.book.fire(timestamp)) {
            process(s, ScheduleRequests.buildEvent(r, timestamp, s.book.getEntityId()));
        }
    }

    private void processScheduleRequests(Session s, JsonNode ruleResult, String operation) {
        if (ruleResult == null) return;
        RulesetConfig.OperationConfig opConfig = descriptor.getRuleset().findOperation(operation);
        String field = opConfig != null ? opConfig.scheduleRequestsField() : null;
        if (field == null) return;
        List<ScheduleRequest> requests =
            ScheduleRequests.parseRequests(ruleResult.get(field), descriptor.getAgentId());
        s.book.apply(requests);
    }

    // ── Helpers ported from StatefulAgentProcessor ────────────────────────────

    static boolean isEntityResetEvent(EventEnvelope event) {
        if (ENTITY_RESET_EVENT_TYPE.equals(event.getEventType())) return true;
        if (event.getPayload() != null && event.getPayload().has("__eventType")) {
            return ENTITY_RESET_EVENT_TYPE.equals(event.getPayload().get("__eventType").asText(null));
        }
        return false;
    }

    private String resolveOperation(EventEnvelope event) {
        if (descriptor.getSubscriptions() == null) return null;
        for (EventSubscription sub : descriptor.getSubscriptions()) {
            boolean topicMatch = sub.getTopic() != null && sub.getTopic().equals(event.getSourceTopic());
            boolean typeMatch  = sub.getEventType() != null && sub.getEventType().equals(event.getEventType());
            if (topicMatch || typeMatch) {
                return sub.hasOperation() ? sub.getOperation() : null;
            }
        }
        return null;
    }

    private void updateEventHistory(AgentState state, EventEnvelope event) {
        if (descriptor.getSubscriptions() == null) return;
        for (EventSubscription sub : descriptor.getSubscriptions()) {
            boolean topicMatch = sub.getTopic() != null && sub.getTopic().equals(event.getSourceTopic());
            boolean typeMatch  = sub.getEventType() != null && sub.getEventType().equals(event.getEventType());
            if (!topicMatch && !typeMatch) continue;

            HorizonConfig horizon = sub.getHorizon();
            if (horizon == null) horizon = HorizonConfig.count(0);
            if ("count".equals(horizon.getType()) && horizon.effectiveCountLimit() <= 0) break;

            state.addEvent(sub.getEventType(),
                new AgentState.HistoricalEvent(event.getTimestamp(), event.getPayload()),
                horizon, event.getTimestamp());
            break;
        }
    }

    private void updateEntityState(AgentState state, JsonNode ruleResult, String operation,
                                   EventEnvelope event) {
        JsonNode update = null;
        String entityField = null;

        RulesetConfig.OperationConfig opConfig = descriptor.getRuleset().findOperation(operation);
        if (opConfig != null) {
            String outboundEntityField = opConfig.getOutboundEntityField();
            String resultField = (outboundEntityField != null && !outboundEntityField.isBlank())
                ? outboundEntityField : opConfig.getEntityField();
            if (resultField != null && !resultField.isBlank()) {
                update = ruleResult.get(resultField);
            }
            entityField = opConfig.getEntityField();
        }

        // Payload fallback: the archive returned a decision output rather than the entity.
        if ((update == null || update.isNull())
                && entityField != null && !entityField.isBlank()
                && event != null && event.getPayload() != null) {
            JsonNode payloadField = event.getPayload().get(entityField);
            if (payloadField != null && !payloadField.isNull() && payloadField.isObject()) {
                update = payloadField;
            }
        }

        if (update == null || update.isNull()) return;
        if (!update.isObject()) {
            LOG.warn("[{}] entity state source is not a JSON object — ignoring", descriptor.getAgentId());
            return;
        }
        if (update.equals(state.getEntityData())) return;
        state.setEntityData(update.deepCopy());
    }

    private EventEnvelope buildChangeLogEnvelope(EventEnvelope inbound, JsonNode entityAfter) {
        ObjectNode payload = mapper.createObjectNode();
        payload.put("entityId",   safe(inbound.getEntityId()));
        payload.put("entityType", safe(descriptor.getEntityType()));
        payload.put("timestamp",  inbound.getTimestamp());
        payload.set("entity",     entityAfter);

        EventEnvelope env = new EventEnvelope();
        env.setEventType(CHANGELOG_EVENT_TYPE);
        env.setEntityId(inbound.getEntityId());
        env.setSourceTopic(inbound.getSourceTopic());
        env.setTimestamp(inbound.getTimestamp());
        env.setPayload(payload);
        return env;
    }

    private EventEnvelope toEnvelope(EventEnvelope inbound, String eventType, JsonNode resultNode) {
        EventEnvelope out = new EventEnvelope();
        out.setEventType(eventType);
        out.setPayload(resultNode);
        out.setEntityId(inbound.getEntityId());
        out.setSourceTopic(inbound.getSourceTopic());
        out.setTimestamp(inbound.getTimestamp());
        return out;
    }

    private List<EventEnvelope> buildOutputEvents(EventEnvelope inbound, JsonNode ruleResult,
                                                  String operation) {
        List<EventEnvelope> outputs = new ArrayList<>();

        if (ruleResult == null) {
            EventEnvelope err = new EventEnvelope();
            err.setEntityId(inbound.getEntityId());
            err.setTimestamp(System.currentTimeMillis());
            err.setSourceTopic(inbound.getSourceTopic());
            err.setEventType(ERROR_EVENT_TYPE);
            ObjectNode payload = mapper.createObjectNode();
            payload.put("agentId",     descriptor.getAgentId());
            payload.put("entityId",    safe(inbound.getEntityId()));
            payload.put("errorReason", "decision service returned null or threw exception");
            payload.set("originalEvent", inbound.getPayload() != null
                ? inbound.getPayload() : mapper.createObjectNode());
            err.setPayload(payload);
            outputs.add(err);
            return outputs;
        }

        if (!ruleResult.isObject()) return outputs;

        List<RulesetConfig.OutboundConfig> outboundsCfg = null;
        if (descriptor.getRuleset() != null) {
            RulesetConfig.OperationConfig opCfg = descriptor.getRuleset().findOperation(operation);
            if (opCfg != null) outboundsCfg = opCfg.getOutbounds();
        }

        if (outboundsCfg != null && !outboundsCfg.isEmpty()) {
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
            JsonNode eventsNode = ruleResult.get("outputEvents");
            if (eventsNode != null && eventsNode.isArray()) {
                for (JsonNode eventNode : eventsNode) {
                    outputs.add(toEnvelope(inbound, "object", eventNode));
                }
            }
        }
        return outputs;
    }

    private static long minTimeLimitMs(AgentDescriptor descriptor) {
        long min = Long.MAX_VALUE;
        if (descriptor.getSubscriptions() == null) return min;
        for (EventSubscription sub : descriptor.getSubscriptions()) {
            HorizonConfig h = sub.getHorizon();
            if (h == null) continue;
            long ms = h.timeLimitMs();
            if (ms > 0 && ms < min) min = ms;
        }
        return min;
    }

    private static String safe(String s) {
        return s == null ? "" : s;
    }
}
