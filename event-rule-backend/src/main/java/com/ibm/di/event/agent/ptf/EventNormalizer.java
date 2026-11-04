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
import com.ibm.di.event.agent.JsonPathUtils;

import java.time.Instant;
import java.time.format.DateTimeParseException;

/**
 * Normalizes a raw Kafka value into the fields of an event row, with the same rules as
 * {@code AgentBasedDataStreamJob.EnvelopeDeserializer}:
 * <ul>
 *   <li>a value shaped like an agent envelope ({@code agentEventType} and {@code payload}) is
 *       unwrapped to its {@code payload};</li>
 *   <li>the timestamp is {@code payload.timestamp}, a number (epoch ms) or an ISO-8601 string;</li>
 *   <li>{@code payload.__eventType} overrides the subscription event type;</li>
 *   <li>the entity id is extracted with the JSONPath extractor, {@code "unknown"} when absent.</li>
 * </ul>
 * An unparseable value yields the event type {@code deserialization-error} and the entity id
 * {@code __error__}, so the stream is never interrupted.
 *
 * <p>It is a pure function of its arguments: when the payload has no usable timestamp, the field
 * {@code ts} is {@code null} and the SQL falls back to the Kafka record timestamp.
 */
public final class EventNormalizer {

    /** Field selectors accepted by {@link #field}. */
    public static final String FIELD_PAYLOAD    = "payload";
    public static final String FIELD_EVENT_TYPE = "event_type";
    public static final String FIELD_ENTITY_ID  = "entity_id";
    public static final String FIELD_TS         = "ts";

    static final String DESERIALIZATION_ERROR = "deserialization-error";
    static final String ERROR_ENTITY_ID       = "__error__";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private EventNormalizer() {}

    /**
     * Returns one normalized field of {@code raw}.
     *
     * @param raw           the Kafka value, or an already unwrapped payload
     * @param field         one of the {@code FIELD_*} constants
     * @param defaultType   event type of the subscription, used when the payload does not override it
     * @param extractor     JSONPath of the entity id; null or blank yields {@code "unknown"}
     * @return the field as a string, or {@code null} for {@code ts} when the payload has none
     * @throws IllegalArgumentException for an unknown {@code field}
     */
    public static String field(String raw, String field, String defaultType, String extractor) {
        JsonNode payload = parse(raw);
        switch (field) {
            case FIELD_PAYLOAD:
                return payload != null ? payload.toString() : raw;
            case FIELD_EVENT_TYPE:
                return payload != null ? eventType(payload, defaultType) : DESERIALIZATION_ERROR;
            case FIELD_ENTITY_ID:
                return payload != null ? JsonPathUtils.extractEntityId(payload, extractor) : ERROR_ENTITY_ID;
            case FIELD_TS:
                return payload != null ? timestamp(payload) : null;
            default:
                throw new IllegalArgumentException("Unknown event field: " + field);
        }
    }

    private static JsonNode parse(String raw) {
        if (raw == null) return null;
        try {
            JsonNode node = MAPPER.readTree(raw);
            if (node == null || node.isMissingNode()) return null;
            if (node.has("agentEventType") && node.has("payload")) return node.get("payload");
            return node;
        } catch (Exception e) {
            return null;
        }
    }

    private static String eventType(JsonNode payload, String defaultType) {
        if (payload.has("__eventType")) {
            String override = payload.get("__eventType").asText(null);
            if (override != null && !override.isBlank()) return override;
        }
        return defaultType;
    }

    private static String timestamp(JsonNode payload) {
        JsonNode ts = payload.get("timestamp");
        if (ts == null) return null;
        if (ts.isNumber()) return ts.asLong() > 0 ? Long.toString(ts.asLong()) : null;
        if (ts.isTextual()) {
            try {
                return Long.toString(Instant.parse(ts.asText()).toEpochMilli());
            } catch (DateTimeParseException e) {
                return null;
            }
        }
        return null;
    }
}
