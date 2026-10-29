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

import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Flink-serializable carrier for events flowing through the Agent pipeline.
 *
 * <p>All Kafka events are wrapped in an {@code EventEnvelope} before entering the
 * {@link StatefulAgentProcessor}.  This allows the processor to operate on typed,
 * uniformly structured events regardless of their original Kafka topic or schema.
 *
 * <p><strong>Serialization note:</strong> {@link #payload} is a Jackson
 * {@link JsonNode}, which is serializable by Jackson but <em>not</em> by Java's
 * built-in serialization.  Flink serializes this class via the Kryo fallback path
 * (or a custom serializer if registered); {@link #payload} is marked transient
 * to avoid issues with Java serialization in checkpoints — the JSON string
 * ({@link #payloadJson}) is the durable form.
 *
 * <h3>Lifecycle</h3>
 * <ol>
 *   <li>Raw bytes arrive from Kafka and are parsed into a {@link JsonNode}.</li>
 *   <li>The {@link EventEnvelope} is constructed via
 *       {@link #from(String, String, long, JsonNode)} — payload is stored as
 *       both the live {@code JsonNode} and a serialized JSON string.</li>
 *   <li>After a checkpoint restore the live {@code JsonNode} is reconstructed
 *       lazily from {@link #payloadJson} by calling {@link #getPayload()}.</li>
 * </ol>
 */
public class EventEnvelope implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    /** Shared mapper for lazy payload reconstruction — avoids creating a new instance per call. */
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Logical event type (e.g. {@code "transaction"}, {@code "account-update"}). */
    private String eventType;

    /** Source Kafka topic name, used for subscription matching. */
    private String sourceTopic;

    /** Event timestamp in epoch milliseconds. */
    private long timestamp;

    /**
     * Durable JSON representation of the payload — stored as a string so Java
     * serialization (checkpoint) can handle it without a custom Flink serializer.
     */
    private String payloadJson;

    /**
     * Live {@link JsonNode} view of the payload.  Reconstructed lazily from
     * {@link #payloadJson} after deserialization — never persisted directly.
     */
    private transient JsonNode payload;

    /** Entity ID extracted from the payload via the agent's {@code entityKeyExtractor}. */
    private String entityId;

    /** Arbitrary metadata (source info, processing flags, correlation IDs). */
    private Map<String, String> metadata;

    // ── Constructors ──────────────────────────────────────────────────────────

    public EventEnvelope() {}

    /**
     * Creates an envelope with an already-extracted entity ID.
     *
     * @param eventType   logical event-type label
     * @param sourceTopic Kafka topic the event arrived on
     * @param timestamp   event timestamp (epoch ms)
     * @param payload     parsed JSON payload — stored as both live node and string
     */
    public static EventEnvelope from(String eventType,
                                     String sourceTopic,
                                     long   timestamp,
                                     JsonNode payload) {
        return from(eventType, sourceTopic, timestamp, payload, null);
    }

    /**
     * Creates an envelope with an already-extracted entity ID.
     *
     * @param eventType   logical event-type label
     * @param sourceTopic Kafka topic the event arrived on
     * @param timestamp   event timestamp (epoch ms)
     * @param payload     parsed JSON payload — stored as both live node and string
     * @param entityId    pre-extracted entity ID (or null)
     */
    public static EventEnvelope from(String eventType,
                                     String sourceTopic,
                                     long   timestamp,
                                     JsonNode payload,
                                     String entityId) {
        EventEnvelope env = new EventEnvelope();
        env.eventType   = eventType;
        env.sourceTopic = sourceTopic;
        env.timestamp   = timestamp;
        env.entityId    = entityId;
        env.setPayload(payload);
        return env;
    }

    // ── Payload accessors ─────────────────────────────────────────────────────

    /**
     * Returns the live {@link JsonNode} payload.
     * If the live field is null (post-checkpoint-restore), it is lazily reconstructed
     * from {@link #payloadJson}.
     */
    public JsonNode getPayload() {
        if (payload == null && payloadJson != null) {
            try {
                payload = MAPPER.readTree(payloadJson);
            } catch (Exception e) {
                throw new IllegalStateException(
                    "EventEnvelope: failed to reconstruct payload from JSON string", e);
            }
        }
        return payload;
    }

    /**
     * Sets the payload, also serializing it to the durable {@link #payloadJson} string.
     */
    public void setPayload(JsonNode node) {
        this.payload = node;
        this.payloadJson = node != null ? node.toString() : null;
    }

    // ── Getters / Setters ─────────────────────────────────────────────────────

    public String getEventType()              { return eventType; }
    public void   setEventType(String et)     { this.eventType = et; }

    public String getSourceTopic()            { return sourceTopic; }
    public void   setSourceTopic(String st)   { this.sourceTopic = st; }

    public long   getTimestamp()              { return timestamp; }
    public void   setTimestamp(long ts)       { this.timestamp = ts; }

    public String getEntityId()               { return entityId; }
    public void   setEntityId(String id)      { this.entityId = id; }

    public Map<String, String> getMetadata()                     { return metadata; }
    public void                setMetadata(Map<String, String> m) { this.metadata = m; }

    /** Adds a single metadata entry, initialising the map if needed. */
    public void addMetadata(String key, String value) {
        if (metadata == null) metadata = new HashMap<>();
        metadata.put(key, value);
    }

    @Override
    public String toString() {
        return "EventEnvelope{eventType='" + eventType + "', sourceTopic='" + sourceTopic
            + "', entityId='" + entityId + "', timestamp=" + timestamp + " | " + Instant.ofEpochMilli(timestamp).toString() + " }";
    }
}

