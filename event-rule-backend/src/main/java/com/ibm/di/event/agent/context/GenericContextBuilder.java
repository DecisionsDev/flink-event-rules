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
package com.ibm.di.event.agent.context;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ibm.di.event.agent.AgentDescriptor;
import com.ibm.di.event.agent.AgentState;
import com.ibm.di.event.agent.AgentState.FlatHistoryEntry;
import com.ibm.di.event.agent.EventSubscription;
import com.ibm.di.event.agent.EventEnvelope;
import com.ibm.di.event.agent.RulesetConfig;
import com.ibm.di.event.agent.RulesetConfig.HistoryFieldMapping;
import com.ibm.di.event.agent.RulesetConfig.OperationConfig;
import java.time.Instant;
import java.util.List;

/**
 * Builds the {@code "generic"} Decision Intelligence input context — the default format.
 *
 * <p>The context contains: {@code now}, {@code processingTime}, the entity object (under its
 * canonical name and optional domain alias), the current event (similarly aliased), a
 * flat timestamp-sorted {@code history} array, and per-event-type history arrays when
 * {@link com.ibm.di.event.agent.RulesetConfig.OperationConfig#getHistory() history mappings}
 * are configured.  For the full field contract and semantics see
 * {@code event-rule-backend/RULE_ENGINE.md} §Generic Rule Signature.
 */
public class GenericContextBuilder implements ContextBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(GenericContextBuilder.class);

    @Override
    public JsonNode build(AgentDescriptor descriptor,
                          AgentState state,
                          EventEnvelope event,
                          ObjectMapper mapper) {
        RulesetConfig ruleset = descriptor != null ? descriptor.getRuleset() : null;
        OperationConfig opConfig = null;
        if (ruleset != null && descriptor.getSubscriptions() != null) {
            for (EventSubscription sub : descriptor.getSubscriptions()) {
                boolean topicMatch = sub.getTopic() != null
                    && sub.getTopic().equals(event.getSourceTopic());
                boolean typeMatch = sub.getEventType() != null
                    && sub.getEventType().equals(event.getEventType());
                if ((topicMatch || typeMatch) && sub.hasOperation()) {
                    opConfig = ruleset.findOperation(sub.getOperation());
                    break;
                }
            }
        }
        ObjectNode inputPayload = buildInner(state, event, mapper, opConfig);
        return inputPayload;
    }

    /**
     * Builds the inner {@code GenericInput} fields without the {@code "input"} wrapper.
     * Exposed package-private for direct use in tests that assert on field contents.
     *
     * @param state     current per-entity state
     * @param event     the inbound event being processed
     * @param mapper    shared ObjectMapper
     * @param opConfig  optional per-operation field-name mappings (may be null)
     */
    static ObjectNode buildInner(AgentState state, EventEnvelope event, ObjectMapper mapper,
                                 OperationConfig opConfig) {
        String entityField0      = opConfig != null ? opConfig.getEntityField() : null;
        // Blank (empty-string) entityField is a deliberate opt-out: the operation does not need
        // entity state injected into the context (e.g. spread-mode operations where the event
        // payload carries every required context fragment directly).
        // A *null* entityField (no opConfig, or entityField unset) falls back to the default
        // "entity" alias — preserving the historical behaviour for operations that do not
        // configure an explicit entityField.
        boolean entityFieldExplicitlyBlank = entityField0 != null && entityField0.isBlank();
        boolean injectEntity     = !entityFieldExplicitlyBlank;
        String entityAlias       = (entityField0 != null && !entityField0.isBlank())
            ? entityField0 : "entity";
        String currentEventField = opConfig != null ? opConfig.getCurrentEventField() : null;
        // Spread mode: empty/blank currentEventField means every field of the event payload is
        // merged directly into the root context object rather than being nested under a single key.
        boolean spreadMode       = currentEventField != null && currentEventField.isBlank();
        String currentEventAlias = (!spreadMode && currentEventField != null)
            ? currentEventField : "currentEvent";
        List<HistoryFieldMapping> historyMappings = opConfig != null ? opConfig.getHistory() : null;
        ObjectNode ctx = mapper.createObjectNode();

        // ── Temporal fields ───────────────────────────────────────────────────
        // now = timestamp of the event that triggered processing (deterministic, from Kafka)
        ctx.put("now", Instant.ofEpochMilli(event.getTimestamp()).toString());
        // processingTime = wall-clock time when the Decision Service executes (may differ from now)
        ctx.put("processingTime", Instant.ofEpochMilli(System.currentTimeMillis()).toString());

        // ── Spread mode: merge payload fields into root (before entity) ───────
        // When currentEventField is blank the entire payload is flattened into the root context.
        // Entity data is written after the spread so the persisted entity value takes
        // precedence when both the payload and the entity share the same field name.
        if (spreadMode) {
            JsonNode payload = event.getPayload();
            if (payload != null && payload.isObject()) {
                payload.fields().forEachRemaining(e -> ctx.set(e.getKey(), e.getValue()));
                LOG.debug("Spread mode: merged {} payload field(s) into root context",
                    payload.size());
            }
        }

        // ── Entity (nullable correlated object) ──────────────────────────────
        // Written after spread so persisted entity state takes precedence over any
        // same-named field that was just spread in from the current event payload.
        // Skipped entirely when entityField is blank (injectEntity=false): those operations
        // rely solely on the spread payload and must not have entity state injected.
        JsonNode entityNode = state.getEntityData();
        if (injectEntity) {
            if (entityNode != null) {
                ctx.set(entityAlias, entityNode);
            } else if (!spreadMode) {
                // In non-spread mode always emit the entity alias (null is valid).
                ctx.putNull(entityAlias);
            }
            // In spread mode a null entity means the spread already populated the alias
            // from the payload (e.g. a minimal customerContext stub).  We do not overwrite
            // with null in that case.
        }

        // ── Current event (non-spread mode only) ─────────────────────────────
        if (!spreadMode) {
            ctx.set(currentEventAlias, event.getPayload() != null
                ? event.getPayload() : mapper.createObjectNode());
        }

        // ── History (flat, timestamp-sorted list) ────────────────────────────
        // The current event is NOT included; it is always accessed via currentEvent.
        List<AgentState.FlatHistoryEntry> flatHistory = state.getFlatHistory();

        // ── Per-event-type history fields ───────────────────────
        // For each configured history mapping, build a filtered array containing only
        // history entries whose eventType matches, and expose it under the domain field name.
        if (historyMappings != null) {
            for (HistoryFieldMapping mapping : historyMappings) {
                String targetEventType = mapping.getEventType();
                String targetField     = mapping.getField();
                if (targetEventType == null || targetField == null || targetField.isBlank()) {
                    continue;
                }
                ArrayNode filtered = mapper.createArrayNode();
                for (AgentState.FlatHistoryEntry entry : flatHistory) {
                    if (targetEventType.equals(entry.eventType())) {
                        filtered.add(entry.payload());
                    }
                }
                ctx.set(targetField, filtered);
            }
        }
        return ctx;
    }

    private static String historyAsString(List<FlatHistoryEntry> history) {
        if (history == null || history.isEmpty()) {
            return "[]";
        }
        return history.toString();
    }

    /**
     * Package-private overload without operation config — for backward compatibility in tests
     * that call {@code buildInner(state, event, mapper)} directly.
     */
    static ObjectNode buildInner(AgentState state, EventEnvelope event, ObjectMapper mapper) {
        return buildInner(state, event, mapper, (OperationConfig) null);
    }

    private static String safeString(String s) {
        return s != null ? s : "";
    }
}

