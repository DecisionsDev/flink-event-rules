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
        String entityAlias       = opConfig != null && opConfig.getEntityField() != null
            ? opConfig.getEntityField() : "entity";
        String currentEventAlias = opConfig != null && opConfig.getCurrentEventField() != null
            ? opConfig.getCurrentEventField() : "currentEvent";
        List<HistoryFieldMapping> historyMappings = opConfig != null ? opConfig.getHistory() : null;
        ObjectNode ctx = mapper.createObjectNode();

        // ── Temporal fields ───────────────────────────────────────────────────
        // now = timestamp of the event that triggered processing (deterministic, from Kafka)
        ctx.put("now", Instant.ofEpochMilli(event.getTimestamp()).toString());
        // processingTime = wall-clock time when the Decision Service executes (may differ from now)
        ctx.put("processingTime", Instant.ofEpochMilli(System.currentTimeMillis()).toString());

        // ── Entity (nullable correlated object) ──────────────────────────────
        JsonNode entityNode = state.getEntityData();
        if (entityNode != null) {
            ctx.set(entityAlias, entityNode);
        } else {
            ctx.putNull(entityAlias);
        }

        // ── Current event (event that triggered processing) ──────────────────
        ctx.set(currentEventAlias, event.getPayload() != null
            ? event.getPayload() : mapper.createObjectNode());

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

