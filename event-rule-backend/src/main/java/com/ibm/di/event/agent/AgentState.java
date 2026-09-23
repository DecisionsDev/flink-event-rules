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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Per-entity Flink {@link org.apache.flink.api.common.state.ValueState} for the
 * {@link StatefulAgentProcessor}.
 *
 * <h3>Structure</h3>
 * <ul>
 *   <li>{@link #entityData} — mutable entity profile; updated by rule results via the
 *       special {@code __entityState} field in the Decision Intelligence output.</li>
 *   <li>{@link #eventHistories} — one {@link EventHistory} per subscription
 *       {@code eventType}; horizon eviction runs on every {@link #addEvent} call.</li>
 *   <li>{@link #metadata} — bookkeeping (entity ID, type, timestamps, event count).</li>
 * </ul>
 *
 * <h3>Serialization</h3>
 * <p>All fields are plain Java types or {@link Serializable}.  Jackson {@link JsonNode}
 * instances are <em>never stored directly</em> — they are serialized to their JSON string
 * form and held in {@code String} fields, then reconstructed lazily on first access.
 * This pattern is used uniformly for both {@link #entityDataJson} (top-level profile)
 * and {@link HistoricalEvent#payloadJson} (per-event payloads).
 *
 * <h3>Horizon eviction</h3>
 * <p>{@link EventHistory#addEvent} applies the configured horizon immediately:
 * <ul>
 *   <li><b>count</b> — evict oldest events until {@code size ≤ countLimit}.</li>
 *   <li><b>time</b> — evict events whose age exceeds {@code timeLimitMs}.</li>
 *   <li><b>hybrid</b> — apply both bounds; the more restrictive one wins.</li>
 * </ul>
 * Time-based eviction at this point uses the <em>current event's timestamp</em>
 * as the reference, which is deterministic and checkpoint-safe.  The
 * {@link StatefulAgentProcessor#onTimer} path runs an additional sweep keyed by
 * Flink processing time to clean up entities that stop receiving events.
 */
public class AgentState implements Serializable {

    @Serial
    private static final long serialVersionUID = 2L; // bumped: entityData field layout changed

    /** Shared ObjectMapper for JSON (de)serialization — safe to share across instances. */
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Mutable entity profile updated by rules through {@code __entityState}.
     * Stored as JSON string so Flink checkpoint serialization works correctly.
     */
    private String entityDataJson;

    /**
     * Lazily-reconstructed live JsonNode for {@link #entityDataJson}.
     * Marked {@code transient} so it is never included in Java serialization;
     * it is rebuilt from {@link #entityDataJson} on first access after restore.
     */
    private transient JsonNode entityData;

    /** Event history per logical subscription event-type. */
    private Map<String, EventHistory> eventHistories = new HashMap<>();

    /** Bookkeeping metadata. */
    private StateMetadata metadata = new StateMetadata();

    // ── Factory ───────────────────────────────────────────────────────────────

    /** Creates a fresh state for a new entity. */
    public static AgentState init(String entityId, String entityType, long nowMs) {
        AgentState s = new AgentState();
        s.metadata.entityId      = entityId;
        s.metadata.entityType    = entityType;
        s.metadata.createdAtMs   = nowMs;
        s.metadata.lastUpdateMs  = nowMs;
        return s;
    }

    // ── Flat history view ─────────────────────────────────────────────────────

    /**
     * A single event from the flat, merged history view.
     *
     * <p>Surfaces {@code eventType} and {@code eventTime} at the top level so that
     * rule authors can write temporal conditions directly against the event timestamp without
     * navigating into individual payload objects.  The {@code payload} field carries
     * the original event payload unchanged.
     *
     * <p>This is a Java {@code record} — fields are immutable and accessors use the
     * record naming convention ({@code eventType()}, {@code eventTime()},
     * {@code payload()}).  The type implements {@link java.io.Serializable} for
     * compatibility, but it is <em>never stored in Flink {@code ValueState}</em>: it
     * is a transient projection returned by {@link #getFlatHistory()} and exists only
     * for the duration of a single {@code buildRuleContext} call.  No Kryo registration
     * is required.
     */
    public record FlatHistoryEntry(String eventType, long eventTime, JsonNode payload)
            implements Serializable {}

    /**
     * Returns a merged, timestamp-sorted (ascending) view of ALL retained events across
     * every history bucket.
     *
     * <p>Each entry carries its {@code eventType} and {@code eventTime} at the top level,
     * making temporal queries trivial inside Decision Intelligence rules:
     * <pre>
     * // "more than 5 transactions in the last 5 minutes"
     * when
     *   number of items in history
     *     where eventType is "transaction"
     *       and eventTime &gt;= (processingTime - 300_000)
     *   &gt; 5
     * then ...
     * </pre>
     *
     * <p>This is an additive projection — it does not modify or replace the underlying
     * per-type {@link EventHistory} buckets.
     *
     * @return mutable list sorted by {@code timestampMs} ascending; never null
     */
    public List<FlatHistoryEntry> getFlatHistory() {
        List<FlatHistoryEntry> all = new ArrayList<>();
        for (Map.Entry<String, EventHistory> entry : eventHistories.entrySet()) {
            String       type    = entry.getKey();
            EventHistory history = entry.getValue();
            for (HistoricalEvent he : history.getEvents()) {
                all.add(new FlatHistoryEntry(type, he.getTimestamp(), he.getPayload()));
            }
        }
        all.sort(Comparator.comparingLong(FlatHistoryEntry::eventTime));
        return all;
    }

    // ── Event history management ──────────────────────────────────────────────

    /**
     * Appends {@code event} to the history for {@code eventType}, creating the
     * history entry on first use, then applies horizon eviction.
     *
     * @param eventType  logical event-type label (subscription key)
     * @param event      the new event to record
     * @param horizon    the horizon configuration for this subscription
     * @param nowMs      reference time for time-based eviction (event timestamp)
     */
    public void addEvent(String eventType, HistoricalEvent event,
                         HorizonConfig horizon, long nowMs) {
        EventHistory history = eventHistories.computeIfAbsent(
            eventType, k -> new EventHistory(eventType, horizon));
        history.addEvent(event, nowMs);
        metadata.lastUpdateMs = nowMs;
        metadata.eventCount++;
    }

    /**
     * Removes events older than {@code cutoffMs} from all time-sensitive histories.
     * Called from {@link StatefulAgentProcessor#onTimer} for idle-entity cleanup.
     *
     * @param cutoffMs  evict events whose timestamp is &lt; this value
     */
    public void evictExpiredEvents(long cutoffMs) {
        for (EventHistory h : eventHistories.values()) {
            h.applyEviction(cutoffMs);
        }
    }

    /**
     * Returns the history for {@code eventType} as an immutable list of JSON payloads.
     * Used to build the {@code eventHistories} map in the rule execution context.
     */
    public List<JsonNode> getHistoryPayloads(String eventType) {
        EventHistory h = eventHistories.get(eventType);
        if (h == null) return Collections.emptyList();
        List<JsonNode> result = new ArrayList<>(h.events.size());
        for (HistoricalEvent e : h.events) {
            JsonNode node = e.getPayload();
            if (node != null) result.add(node);
        }
        return result;
    }

    // ── Getters / Setters ─────────────────────────────────────────────────────

    /**
     * Returns the entity profile as a live {@link JsonNode}, reconstructing it from
     * the stored JSON string if necessary (e.g. after a Flink checkpoint restore).
     */
    public JsonNode getEntityData() {
        if (entityData == null && entityDataJson != null) {
            try {
                entityData = MAPPER.readTree(entityDataJson);
            } catch (Exception e) {
                throw new IllegalStateException("AgentState: failed to reconstruct entityData", e);
            }
        }
        return entityData;
    }

    /**
     * Sets the entity profile, keeping both the live {@link JsonNode} and the
     * serializable JSON string in sync.
     */
    public void setEntityData(JsonNode nd) {
        this.entityData     = nd;
        this.entityDataJson = nd != null ? nd.toString() : null;
    }

    /** For framework deserialization only — sets the durable JSON string directly. */
    public String getEntityDataJson()            { return entityDataJson; }
    public void   setEntityDataJson(String json) {
        this.entityDataJson = json;
        this.entityData     = null; // invalidate cached live node
    }

    public Map<String, EventHistory> getEventHistories()                      { return eventHistories; }
    public void                      setEventHistories(Map<String, EventHistory> h) { this.eventHistories = h; }

    public StateMetadata getMetadata()               { return metadata; }
    public void          setMetadata(StateMetadata m) { this.metadata = m; }

    // ─────────────────────────────────────────────────────────────────────────
    // Inner classes
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Bounded deque of {@link HistoricalEvent}s for one subscription event-type.
     * Horizon eviction is applied inside {@link #addEvent}.
     */
    public static class EventHistory implements Serializable {

        @Serial
        private static final long serialVersionUID = 1L;

        private String       eventType;
        private Deque<HistoricalEvent> events  = new ArrayDeque<>();
        private HorizonConfig          horizon;

        public EventHistory() {}

        EventHistory(String eventType, HorizonConfig horizon) {
            this.eventType = eventType;
            this.horizon   = horizon != null ? horizon : HorizonConfig.count(0);
        }

        /**
         * Appends {@code event} and immediately applies horizon eviction.
         *
         * @param event the new event (must not be null)
         * @param nowMs reference time for time-based eviction
         */
        void addEvent(HistoricalEvent event, long nowMs) {
            if (horizon == null || "count".equals(horizon.getType()) && horizon.effectiveCountLimit() <= 0) {
                // No history — discard event immediately
                return;
            }
            events.addLast(event);
            applyEviction(nowMs);
        }

        /**
         * Removes all events whose timestamp is strictly older than {@code cutoffMs}.
         *
         * <p>Uses a full scan rather than a break-on-first-unexpired approach because
         * Flink's out-of-order watermark tolerance means events may arrive — and be
         * appended — slightly out of timestamp order.  A full-sweep eviction is
         * always correct regardless of insertion order.
         */
        void evictBefore(long cutoffMs) {
            events.removeIf(e -> e.getTimestamp() < cutoffMs);
        }

        public void applyEviction(long nowMs) {
            if (horizon == null) return;
            String type = horizon.getType();

            // Time-based eviction: remove front elements older than timeLimitMs
            long timeLimitMs = horizon.timeLimitMs();
            if (timeLimitMs > 0 && ("time".equals(type) || "hybrid".equals(type))) {
                long cutoff = nowMs - timeLimitMs;
                evictBefore(cutoff);
            }

            // Count-based eviction: remove front elements until size ≤ limit
            int limit = horizon.effectiveCountLimit();
            if (limit > 0 && ("count".equals(type) || "hybrid".equals(type))) {
                while (events.size() > limit) events.removeFirst();
            }
        }

        /** Returns an unmodifiable view of the event deque for inspection. */
        public Deque<HistoricalEvent> getEvents() { return events; }
        public void setEvents(Deque<HistoricalEvent> e) { this.events = e; }

        public String       getEventType()             { return eventType; }
        public void         setEventType(String et)    { this.eventType = et; }

        public HorizonConfig getHorizon()               { return horizon; }
        public void          setHorizon(HorizonConfig h) { this.horizon = h; }

        /** Current number of retained events. */
        public int size() { return events.size(); }
    }

    // ─────────────────────────────────────────────────────────────────────────

    /**
     * A single event in the history, carrying its timestamp and JSON payload.
     *
     * <p>The payload is stored as a JSON string ({@link #payloadJson}) so that
     * Flink checkpoint serialization works without a custom serializer.  The live
     * {@link JsonNode} is reconstructed lazily on first access.
     */
    public static class HistoricalEvent implements Serializable {

        @Serial
        private static final long serialVersionUID = 1L;

        private long   timestamp;
        private String payloadJson;
        private transient JsonNode payload;

        public HistoricalEvent() {}

        public HistoricalEvent(long timestamp, JsonNode payload) {
            this.timestamp   = timestamp;
            setPayload(payload);
        }

        public long getTimestamp()      { return timestamp; }
        public void setTimestamp(long t) { this.timestamp = t; }

        public JsonNode getPayload() {
            if (payload == null && payloadJson != null) {
                try {
                    payload = MAPPER.readTree(payloadJson);
                } catch (Exception e) {
                    throw new IllegalStateException(
                        "HistoricalEvent: failed to reconstruct payload", e);
                }
            }
            return payload;
        }

        public void setPayload(JsonNode node) {
            this.payload     = node;
            this.payloadJson = node != null ? node.toString() : null;
        }

        /** For deserialization only — sets the durable JSON string directly. */
        public String getPayloadJson()              { return payloadJson; }
        public void   setPayloadJson(String json)   {
            this.payloadJson = json;
            this.payload     = null; // invalidate cached live node
        }
    }

    // ─────────────────────────────────────────────────────────────────────────

    /** Bookkeeping metadata about an entity's processing state. */
    public static class StateMetadata implements Serializable {

        @Serial
        private static final long serialVersionUID = 1L;

        private String entityId;
        private String entityType;
        private long   createdAtMs;
        private long   lastUpdateMs;
        private long   eventCount;

        public StateMetadata() {}

        public String getEntityId()             { return entityId; }
        public void   setEntityId(String id)    { this.entityId = id; }

        public String getEntityType()            { return entityType; }
        public void   setEntityType(String et)   { this.entityType = et; }

        public long getCreatedAtMs()             { return createdAtMs; }
        public void setCreatedAtMs(long t)       { this.createdAtMs = t; }

        public long getLastUpdateMs()            { return lastUpdateMs; }
        public void setLastUpdateMs(long t)      { this.lastUpdateMs = t; }

        public long getEventCount()              { return eventCount; }
        public void setEventCount(long c)        { this.eventCount = c; }

        @Override
        public String toString() {
            return "StateMetadata{entityId='" + entityId + "', entityType='" + entityType
                + "', eventCount=" + eventCount + ", lastUpdateMs=" + lastUpdateMs + "}";
        }
    }
}

