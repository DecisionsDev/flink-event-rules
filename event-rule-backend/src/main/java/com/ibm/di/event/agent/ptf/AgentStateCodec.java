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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ibm.di.event.agent.AgentDescriptor;
import com.ibm.di.event.agent.AgentState;
import com.ibm.di.event.agent.EventSubscription;
import com.ibm.di.event.agent.HorizonConfig;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Converts an {@link AgentState} to and from the JSON string held by {@link AgentPtfState}.
 *
 * <p>Layout:
 * <pre>
 * { "entity": {...},
 *   "meta":   {"entityId", "entityType", "createdAtMs", "lastUpdateMs", "eventCount"},
 *   "hist":   {"&lt;eventType&gt;": [ {"t": &lt;epochMs&gt;, "p": {...}} ]} }
 * </pre>
 * The horizon of each history is not stored: it is re-derived from the subscriptions of the
 * descriptor, so a change of horizon in the descriptor applies to existing entities.
 */
public final class AgentStateCodec {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private AgentStateCodec() {}

    /** Serializes {@code state}; {@code null} yields {@code null}. */
    public static String encode(AgentState state) {
        if (state == null) return null;
        ObjectNode root = MAPPER.createObjectNode();
        if (state.getEntityData() != null) root.set("entity", state.getEntityData());

        AgentState.StateMetadata m = state.getMetadata();
        ObjectNode meta = root.putObject("meta");
        meta.put("entityId", m.getEntityId());
        meta.put("entityType", m.getEntityType());
        meta.put("createdAtMs", m.getCreatedAtMs());
        meta.put("lastUpdateMs", m.getLastUpdateMs());
        meta.put("eventCount", m.getEventCount());

        ObjectNode hist = root.putObject("hist");
        for (Entry<String, AgentState.EventHistory> e : state.getEventHistories().entrySet()) {
            ArrayNode events = hist.putArray(e.getKey());
            for (AgentState.HistoricalEvent he : e.getValue().getEvents()) {
                ObjectNode item = events.addObject();
                item.put("t", he.getTimestamp());
                if (he.getPayload() != null) item.set("p", he.getPayload());
            }
        }
        return root.toString();
    }

    /** Rebuilds the state; {@code null} or blank yields {@code null}. */
    public static AgentState decode(String json, AgentDescriptor descriptor) {
        if (json == null || json.isBlank()) return null;
        try {
            JsonNode root = MAPPER.readTree(json);
            AgentState state = new AgentState();
            if (root.has("entity")) state.setEntityData(root.get("entity"));

            JsonNode meta = root.get("meta");
            AgentState.StateMetadata m = state.getMetadata();
            m.setEntityId(meta.path("entityId").asText(null));
            m.setEntityType(meta.path("entityType").asText(null));
            m.setCreatedAtMs(meta.path("createdAtMs").asLong());
            m.setLastUpdateMs(meta.path("lastUpdateMs").asLong());
            m.setEventCount(meta.path("eventCount").asLong());

            Map<String, HorizonConfig> horizons = horizons(descriptor);
            Map<String, AgentState.EventHistory> histories = new HashMap<>();
            root.path("hist").fields().forEachRemaining(entry -> {
                AgentState.EventHistory h = new AgentState.EventHistory();
                h.setEventType(entry.getKey());
                h.setHorizon(horizons.getOrDefault(entry.getKey(), HorizonConfig.count(0)));
                ArrayDeque<AgentState.HistoricalEvent> events = new ArrayDeque<>();
                for (JsonNode item : entry.getValue()) {
                    events.add(new AgentState.HistoricalEvent(item.path("t").asLong(), item.get("p")));
                }
                h.setEvents(events);
                histories.put(entry.getKey(), h);
            });
            state.setEventHistories(histories);
            return state;
        } catch (IOException e) {
            throw new IllegalStateException("Corrupted agent state: " + e.getMessage(), e);
        }
    }

    private static Map<String, HorizonConfig> horizons(AgentDescriptor descriptor) {
        Map<String, HorizonConfig> result = new HashMap<>();
        if (descriptor.getSubscriptions() == null) return result;
        for (EventSubscription sub : descriptor.getSubscriptions()) {
            if (sub.getEventType() != null && sub.getHorizon() != null) {
                result.putIfAbsent(sub.getEventType(), sub.getHorizon());
            }
        }
        return result;
    }
}
