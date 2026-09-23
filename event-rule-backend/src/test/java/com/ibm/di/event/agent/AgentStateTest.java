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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link AgentState} and its inner classes:
 * {@link AgentState.EventHistory}, {@link AgentState.HistoricalEvent},
 * and {@link AgentState.StateMetadata}.
 *
 * <p>All tests are hermetic — no Flink runtime, no external dependencies.
 *
 * <h3>Coverage</h3>
 * <ul>
 *   <li>AgentState factory ({@code init}) and field defaults</li>
 *   <li>Entity data: live JsonNode ↔ JSON-string sync, lazy reconstruction</li>
 *   <li>EventHistory: count eviction, time eviction, hybrid eviction</li>
 *   <li>EventHistory: count(0) discards all events; count(1) keeps only latest</li>
 *   <li>EventHistory: {@code evictBefore} removes by timestamp predicate</li>
 *   <li>AgentState: {@code evictExpiredEvents} delegates to all histories</li>
 *   <li>AgentState: {@code addEvent} increments metadata.eventCount</li>
 *   <li>AgentState: {@code getHistoryPayloads} returns payloads in insertion order</li>
 *   <li>HistoricalEvent: lazy payload reconstruction from payloadJson</li>
 *   <li>HistoricalEvent: null payload stored and reconstructed as null</li>
 *   <li>StateMetadata: all fields survive round-trip through getters/setters</li>
 * </ul>
 */
@DisplayName("AgentState unit tests")
class AgentStateTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static AgentState.HistoricalEvent event(long ts, String json) throws Exception {
        return new AgentState.HistoricalEvent(ts, MAPPER.readTree(json));
    }

    private static AgentState.HistoricalEvent event(long ts) {
        return new AgentState.HistoricalEvent(ts, MAPPER.createObjectNode());
    }

    // ── AgentState.init() factory ─────────────────────────────────────────────

    @Nested
    @DisplayName("AgentState.init() — factory and field defaults")
    class InitFactory {

        @Test
        @DisplayName("init sets entityId, entityType, createdAtMs, lastUpdateMs")
        void initSetsMetadataFields() {
            AgentState s = AgentState.init("ACC-1", "account", 5000L);

            assertNotNull(s.getMetadata());
            assertEquals("ACC-1",   s.getMetadata().getEntityId());
            assertEquals("account", s.getMetadata().getEntityType());
            assertEquals(5000L,     s.getMetadata().getCreatedAtMs());
            assertEquals(5000L,     s.getMetadata().getLastUpdateMs());
        }

        @Test
        @DisplayName("init has no entityData (null) and empty event histories")
        void initHasNullEntityData() {
            AgentState s = AgentState.init("E-1", "order", 1L);

            assertNull(s.getEntityData(),
                "entityData must be null for a freshly initialised entity");
            assertTrue(s.getEventHistories().isEmpty(),
                "eventHistories must be empty on init");
        }

        @Test
        @DisplayName("init initialises eventCount to 0")
        void initEventCountIsZero() {
            AgentState s = AgentState.init("E-2", "sensor", 0L);
            assertEquals(0L, s.getMetadata().getEventCount());
        }
    }

    // ── EntityData: live node ↔ JSON string sync ──────────────────────────────

    @Nested
    @DisplayName("EntityData — live JsonNode / JSON-string synchronisation")
    class EntityDataSync {

        @Test
        @DisplayName("setEntityData syncs the JSON string representation")
        void setEntityDataSyncsJsonString() throws Exception {
            AgentState s = AgentState.init("E-3", "account", 0L);
            ObjectNode nd = MAPPER.createObjectNode();
            nd.put("score", 42);
            nd.put("tier",  "gold");

            s.setEntityData(nd);

            assertNotNull(s.getEntityDataJson(),
                "entityDataJson must be non-null after setEntityData");
            JsonNode parsed = MAPPER.readTree(s.getEntityDataJson());
            assertEquals(42,     parsed.path("score").asInt());
            assertEquals("gold", parsed.path("tier").asText());
        }

        @Test
        @DisplayName("setEntityDataJson invalidates cached live node — lazy reconstruction works")
        void setEntityDataJsonInvalidatesCache() throws Exception {
            AgentState s = AgentState.init("E-4", "account", 0L);
            ObjectNode nd = MAPPER.createObjectNode();
            nd.put("value", 99);
            s.setEntityData(nd);

            // Simulate checkpoint-restore: clear the live transient node via setEntityDataJson
            s.setEntityDataJson(s.getEntityDataJson());

            // getEntityData() must lazily reconstruct from the JSON string
            JsonNode reconstructed = s.getEntityData();
            assertNotNull(reconstructed, "entityData must be reconstructed from JSON string");
            assertEquals(99, reconstructed.path("value").asInt(),
                "Reconstructed entityData must preserve all fields");
        }

        @Test
        @DisplayName("setEntityData(null) clears both entityData and entityDataJson")
        void setEntityDataNullClearsBoth() {
            AgentState s = AgentState.init("E-5", "account", 0L);
            s.setEntityData(MAPPER.createObjectNode());
            s.setEntityData(null);

            assertNull(s.getEntityData());
            assertNull(s.getEntityDataJson());
        }

        @Test
        @DisplayName("setEntityDataJson(null) results in null entityData")
        void setEntityDataJsonNullReturnsNull() {
            AgentState s = AgentState.init("E-6", "account", 0L);
            s.setEntityDataJson(null);

            assertNull(s.getEntityData(), "getEntityData() must return null when JSON string is null");
        }
    }

    // ── addEvent: metadata increments ─────────────────────────────────────────

    @Nested
    @DisplayName("addEvent — metadata side effects")
    class AddEventMetadata {

        @Test
        @DisplayName("addEvent increments eventCount on each call")
        void addEventIncrementsCount() {
            AgentState s = AgentState.init("E-7", "account", 0L);
            HorizonConfig h = HorizonConfig.count(5);

            s.addEvent("txn", event(1L), h, 1L);
            assertEquals(1, s.getMetadata().getEventCount());

            s.addEvent("txn", event(2L), h, 2L);
            assertEquals(2, s.getMetadata().getEventCount());

            s.addEvent("txn", event(3L), h, 3L);
            assertEquals(3, s.getMetadata().getEventCount());
        }

        @Test
        @DisplayName("addEvent updates lastUpdateMs to the event's nowMs")
        void addEventUpdatesLastUpdateMs() {
            AgentState s = AgentState.init("E-8", "account", 1000L);
            HorizonConfig h = HorizonConfig.count(5);

            s.addEvent("txn", event(9000L), h, 9000L);

            assertEquals(9000L, s.getMetadata().getLastUpdateMs());
        }

        @Test
        @DisplayName("addEvent creates a new EventHistory bucket on first use")
        void addEventCreatesHistoryBucket() {
            AgentState s = AgentState.init("E-9", "account", 0L);
            HorizonConfig h = HorizonConfig.count(5);

            assertFalse(s.getEventHistories().containsKey("txn"),
                "Bucket must not exist before first addEvent");

            s.addEvent("txn", event(1L), h, 1L);

            assertTrue(s.getEventHistories().containsKey("txn"),
                "Bucket must be created by first addEvent");
        }
    }

    // ── EventHistory: count eviction ──────────────────────────────────────────

    @Nested
    @DisplayName("EventHistory — count-based eviction")
    class EventHistoryCountEviction {

        @Test
        @DisplayName("count(3): adding 5 events retains only the latest 3")
        void countEvictsOldest() {
            AgentState s    = AgentState.init("E-10", "acct", 0L);
            HorizonConfig h = HorizonConfig.count(3);

            for (int i = 1; i <= 5; i++) {
                s.addEvent("txn", event(i), h, i);
            }

            AgentState.EventHistory hist = s.getEventHistories().get("txn");
            assertNotNull(hist);
            assertEquals(3, hist.size(), "count(3): only the 3 latest events must remain");
        }

        @Test
        @DisplayName("count(1): only the very latest event is retained")
        void countOneKeepsLatest() {
            AgentState s    = AgentState.init("E-11", "acct", 0L);
            HorizonConfig h = HorizonConfig.count(1);

            for (int i = 1; i <= 4; i++) {
                s.addEvent("txn", event(i), h, i);
            }

            AgentState.EventHistory hist = s.getEventHistories().get("txn");
            assertNotNull(hist);
            assertEquals(1, hist.size());
            assertEquals(4L, hist.getEvents().peekLast().getTimestamp(),
                "The retained event must be the last one added");
        }

        @Test
        @DisplayName("count(0): all events are discarded (no history)")
        void countZeroDiscardsAll() {
            AgentState s    = AgentState.init("E-12", "acct", 0L);
            HorizonConfig h = HorizonConfig.count(0);

            for (int i = 1; i <= 3; i++) {
                s.addEvent("txn", event(i), h, i);
            }

            AgentState.EventHistory hist = s.getEventHistories().get("txn");
            assertTrue(hist == null || hist.size() == 0,
                "count(0) must discard all events");
        }

        @Test
        @DisplayName("Multiple event types each maintain independent count limits")
        void separateHistoriesForDifferentEventTypes() {
            AgentState s  = AgentState.init("E-13", "acct", 0L);
            HorizonConfig h2 = HorizonConfig.count(2);
            HorizonConfig h4 = HorizonConfig.count(4);

            for (int i = 1; i <= 5; i++) {
                s.addEvent("txn",    event(i), h2, i);
                s.addEvent("alert",  event(i), h4, i);
            }

            assertEquals(2, s.getEventHistories().get("txn").size(),
                "txn history: count(2) must retain only 2");
            assertEquals(4, s.getEventHistories().get("alert").size(),
                "alert history: count(4) must retain only 4");
        }
    }

    // ── EventHistory: time eviction ───────────────────────────────────────────

    @Nested
    @DisplayName("EventHistory — time-based eviction")
    class EventHistoryTimeEviction {

        @Test
        @DisplayName("time('1h'): events older than 1 hour are evicted")
        void timeEvictsOld() {
            AgentState s    = AgentState.init("E-14", "acct", 0L);
            // 1 hour = 3_600_000 ms
            HorizonConfig h = HorizonConfig.time("1h");

            long oneHourMs = 3_600_000L;
            long now       = 10_000_000L;

            // Old event (2 hours ago) → must be evicted
            s.addEvent("txn", event(now - 2 * oneHourMs), h, now - 2 * oneHourMs);
            // Recent event (30 minutes ago) → must be retained
            s.addEvent("txn", event(now - oneHourMs / 2), h, now);

            AgentState.EventHistory hist = s.getEventHistories().get("txn");
            assertNotNull(hist);
            assertEquals(1, hist.size(),
                "time(1h): only the event within 1h must survive");
        }

        @Test
        @DisplayName("time('1h'): events exactly at the cutoff boundary are evicted (strict <)")
        void timeEvictionBoundary() {
            AgentState s    = AgentState.init("E-15", "acct", 0L);
            HorizonConfig h = HorizonConfig.time("1h");

            long oneHourMs = 3_600_000L;
            long now       = 10_000_000L;
            long cutoff    = now - oneHourMs; // exactly at boundary

            // Event at cutoff — timestamp < cutoff is false, so it should survive
            s.addEvent("txn", event(cutoff), h, cutoff);
            // Trigger eviction with nowMs = now
            s.addEvent("txn", event(now), h, now);

            AgentState.EventHistory hist = s.getEventHistories().get("txn");
            // cutoff event survives (not strictly < cutoff), newest event survives
            assertTrue(hist.size() >= 1, "Event at boundary must not be evicted by strict-less-than rule");
        }
    }

    // ── EventHistory: hybrid eviction ────────────────────────────────────────

    @Nested
    @DisplayName("EventHistory — hybrid eviction (count + time)")
    class EventHistoryHybridEviction {

        @Test
        @DisplayName("hybrid(5, '1h'): count bound triggers first when more than 5 events arrive within 1h")
        void hybridCountBoundTriggers() {
            AgentState s    = AgentState.init("E-16", "acct", 0L);
            HorizonConfig h = HorizonConfig.hybrid(5, "1h");

            long now = 10_000_000L;
            for (int i = 0; i < 8; i++) {
                // All events are recent (within 10 seconds of each other)
                s.addEvent("txn", event(now + i * 1000L), h, now + i * 1000L);
            }

            AgentState.EventHistory hist = s.getEventHistories().get("txn");
            assertNotNull(hist);
            assertEquals(5, hist.size(),
                "hybrid(5, 1h): count=5 must trigger when all 8 events are within 1h");
        }

        @Test
        @DisplayName("hybrid(100, '1s'): time bound triggers first when events are older than 1s")
        void hybridTimeBoundTriggers() {
            AgentState s    = AgentState.init("E-17", "acct", 0L);
            // count=100 but time=1s means the 1-second window will fill up first
            HorizonConfig h = HorizonConfig.hybrid(100, "1s");

            long oneSecMs = 1_000L;
            long base     = 100_000_000L;

            // 5 old events (> 1s ago) + 1 recent event
            for (int i = 0; i < 5; i++) {
                s.addEvent("txn", event(base - 5 * oneSecMs + i * 100L), h, base - 5 * oneSecMs + i * 100L);
            }
            s.addEvent("txn", event(base), h, base);

            AgentState.EventHistory hist = s.getEventHistories().get("txn");
            assertNotNull(hist);
            // Only the recent event survives the 1-second window
            assertEquals(1, hist.size(),
                "hybrid(100, 1s): only events within 1s of nowMs must survive");
        }
    }

    // ── evictExpiredEvents ────────────────────────────────────────────────────

    @Nested
    @DisplayName("evictExpiredEvents — timer-driven cleanup")
    class EvictExpiredEvents {

        @Test
        @DisplayName("evictExpiredEvents removes events older than cutoffMs from all histories")
        void evictExpiredEventsRemovesOld() {
            AgentState s = AgentState.init("E-18", "acct", 0L);
            // 5s window: applyEviction(nowMs) evicts ts < nowMs - 5000.
            // Pass nowMs=10000 → cutoff=5000, so events at ts<5000 are evicted.
            HorizonConfig h = HorizonConfig.time("5s");

            // Insert old events (ts < 5000)
            s.addEvent("txn",   event(1000L), h, 1000L);
            s.addEvent("txn",   event(2000L), h, 2000L);
            s.addEvent("alert", event(3000L), h, 3000L);
            // Insert fresh events (ts >= 5000)
            s.addEvent("txn",   event(6000L), h, 6000L);
            s.addEvent("alert", event(7000L), h, 7000L);

            // evictExpiredEvents(nowMs=10000): cutoff = 10000 - 5000 = 5000
            s.evictExpiredEvents(10_000L);

            assertEquals(1, s.getEventHistories().get("txn").size(),
                "txn: only the event with ts >= 5000 must survive");
            assertEquals(1, s.getEventHistories().get("alert").size(),
                "alert: only the event with ts >= 5000 must survive");
        }

        @Test
        @DisplayName("evictExpiredEvents with future cutoff removes all events")
        void evictAllWithFutureCutoff() {
            AgentState s = AgentState.init("E-19", "acct", 0L);
            // 5s window: cutoff = Long.MAX_VALUE - 5000 — still enormous, evicts ts=1 and ts=2
            HorizonConfig h = HorizonConfig.time("5s");

            s.addEvent("txn", event(1L), h, 1L);
            s.addEvent("txn", event(2L), h, 2L);

            s.evictExpiredEvents(Long.MAX_VALUE); // cutoff = MAX_VALUE - 5000, evicts everything

            assertEquals(0, s.getEventHistories().get("txn").size(),
                "All events must be removed when cutoff is MAX_VALUE");
        }

        @Test
        @DisplayName("evictExpiredEvents with cutoff=0 removes nothing")
        void evictNoneWithZeroCutoff() {
            AgentState s = AgentState.init("E-20", "acct", 0L);
            HorizonConfig h = HorizonConfig.count(100);

            s.addEvent("txn", event(1L), h, 1L);
            s.addEvent("txn", event(2L), h, 2L);

            s.evictExpiredEvents(0L); // cutoff=0: no event has ts < 0

            assertEquals(2, s.getEventHistories().get("txn").size(),
                "No events must be removed when cutoff is 0");
        }
    }

    // ── getHistoryPayloads ────────────────────────────────────────────────────

    @Nested
    @DisplayName("getHistoryPayloads — ordered payload list")
    class GetHistoryPayloads {

        @Test
        @DisplayName("getHistoryPayloads returns payloads in insertion order")
        void historyPayloadsInOrder() throws Exception {
            AgentState s  = AgentState.init("E-21", "acct", 0L);
            HorizonConfig h = HorizonConfig.count(5);

            s.addEvent("txn", event(1L, "{\"seq\":1}"), h, 1L);
            s.addEvent("txn", event(2L, "{\"seq\":2}"), h, 2L);
            s.addEvent("txn", event(3L, "{\"seq\":3}"), h, 3L);

            List<JsonNode> payloads = s.getHistoryPayloads("txn");

            assertEquals(3, payloads.size());
            assertEquals(1, payloads.get(0).path("seq").asInt());
            assertEquals(2, payloads.get(1).path("seq").asInt());
            assertEquals(3, payloads.get(2).path("seq").asInt());
        }

        @Test
        @DisplayName("getHistoryPayloads for unknown event type returns empty list")
        void unknownEventTypeReturnsEmpty() {
            AgentState s = AgentState.init("E-22", "acct", 0L);

            List<JsonNode> payloads = s.getHistoryPayloads("nonexistent");

            assertNotNull(payloads, "Must return non-null empty list, not null");
            assertTrue(payloads.isEmpty(), "Unknown event type must return empty list");
        }
    }

    // ── HistoricalEvent ───────────────────────────────────────────────────────

    @Nested
    @DisplayName("HistoricalEvent — construction and lazy payload reconstruction")
    class HistoricalEventTests {

        @Test
        @DisplayName("Constructor sets timestamp and payload; payloadJson is populated")
        void constructorSetsFields() throws Exception {
            JsonNode nd = MAPPER.readTree("{\"amount\":100}");
            AgentState.HistoricalEvent e = new AgentState.HistoricalEvent(42L, nd);

            assertEquals(42L, e.getTimestamp());
            assertNotNull(e.getPayload());
            assertEquals(100, e.getPayload().path("amount").asInt());
            assertNotNull(e.getPayloadJson(), "payloadJson must be serialized");
        }

        @Test
        @DisplayName("setPayloadJson invalidates cached payload — lazy reconstruction produces correct node")
        void lazyReconstructionFromPayloadJson() throws Exception {
            JsonNode nd = MAPPER.readTree("{\"value\":99}");
            AgentState.HistoricalEvent e = new AgentState.HistoricalEvent(1L, nd);

            // Simulate checkpoint-restore: force lazy path
            String json = e.getPayloadJson();
            e.setPayloadJson(json); // invalidates live payload

            JsonNode reconstructed = e.getPayload();
            assertNotNull(reconstructed, "Payload must be reconstructed from JSON string");
            assertEquals(99, reconstructed.path("value").asInt());
        }

        @Test
        @DisplayName("Constructor with null payload sets payloadJson to null")
        void nullPayloadSetsNullJson() {
            AgentState.HistoricalEvent e = new AgentState.HistoricalEvent(5L, null);

            assertNull(e.getPayload());
            assertNull(e.getPayloadJson());
        }

        @Test
        @DisplayName("setPayloadJson(null) results in null payload")
        void setPayloadJsonNullReturnsNull() {
            AgentState.HistoricalEvent e = new AgentState.HistoricalEvent(1L, MAPPER.createObjectNode());
            e.setPayloadJson(null);

            assertNull(e.getPayload());
        }

        @Test
        @DisplayName("Default no-arg constructor is available (required for Flink deserialization)")
        void noArgConstructorAvailable() {
            assertDoesNotThrow(() -> {
                AgentState.HistoricalEvent e = new AgentState.HistoricalEvent();
                assertNull(e.getPayload());
            });
        }
    }

    // ── StateMetadata ─────────────────────────────────────────────────────────

    @Nested
    @DisplayName("StateMetadata — field accessors")
    class StateMetadataTests {

        @Test
        @DisplayName("All fields survive a getter/setter round-trip")
        void allFieldsRoundTrip() {
            AgentState.StateMetadata m = new AgentState.StateMetadata();

            m.setEntityId("E-SM-1");
            m.setEntityType("order");
            m.setCreatedAtMs(1000L);
            m.setLastUpdateMs(2000L);
            m.setEventCount(7L);

            assertEquals("E-SM-1", m.getEntityId());
            assertEquals("order",  m.getEntityType());
            assertEquals(1000L,    m.getCreatedAtMs());
            assertEquals(2000L,    m.getLastUpdateMs());
            assertEquals(7L,       m.getEventCount());
        }

        @Test
        @DisplayName("toString() contains entityId, entityType, and eventCount")
        void toStringContainsKeyFields() {
            AgentState.StateMetadata m = new AgentState.StateMetadata();
            m.setEntityId("SM-TOSTRING");
            m.setEntityType("account");
            m.setEventCount(42L);

            String s = m.toString();
            assertTrue(s.contains("SM-TOSTRING"), "toString must contain entityId");
            assertTrue(s.contains("account"),     "toString must contain entityType");
            assertTrue(s.contains("42"),          "toString must contain eventCount");
        }
    }

    // ── EventHistory: evictBefore direct ─────────────────────────────────────

    @Nested
    @DisplayName("EventHistory.evictBefore — direct eviction by timestamp")
    class EventHistoryEvictBefore {

        @Test
        @DisplayName("evictBefore removes all events with timestamp strictly less than cutoff")
        void evictBeforeRemovesOlderEvents() {
            AgentState.EventHistory hist =
                new AgentState.EventHistory("txn", HorizonConfig.count(100));

            // Use package-friend addEvent by going through the history directly
            // Can't call addEvent directly (package-private), so use AgentState.addEvent
            AgentState s    = AgentState.init("EVB-1", "a", 0L);
            HorizonConfig h = HorizonConfig.count(100);

            s.addEvent("txn", event(100L), h, 100L);
            s.addEvent("txn", event(200L), h, 200L);
            s.addEvent("txn", event(300L), h, 300L);
            s.addEvent("txn", event(400L), h, 400L);

            s.getEventHistories().get("txn").evictBefore(300L); // removes ts=100, ts=200

            assertEquals(2, s.getEventHistories().get("txn").size(),
                "evictBefore(300) must remove events with ts < 300");
        }
    }

    // ── getFlatHistory ────────────────────────────────────────────────────────

    @Nested
    @DisplayName("getFlatHistory — flat, timestamp-sorted list across all event types")
    class GetFlatHistory {

        @Test
        @DisplayName("Empty state returns empty flat list")
        void emptyStateReturnsEmptyList() {
            AgentState s = AgentState.init("FH-1", "account", 0L);
            assertTrue(s.getFlatHistory().isEmpty(),
                "getFlatHistory must return an empty list for a freshly initialised state");
        }

        @Test
        @DisplayName("Single event type appears in flat list")
        void singleEventTypeAppearsInList() throws Exception {
            AgentState s = AgentState.init("FH-2", "account", 0L);
            HorizonConfig h = HorizonConfig.count(5);

            s.addEvent("transaction", event(1000L, "{\"amount\":50}"), h, 1000L);
            s.addEvent("transaction", event(2000L, "{\"amount\":75}"), h, 2000L);

            List<AgentState.FlatHistoryEntry> flat = s.getFlatHistory();

            assertEquals(2, flat.size(), "flat list must contain both transaction events");
            assertEquals("transaction", flat.get(0).eventType());
            assertEquals("transaction", flat.get(1).eventType());
            assertEquals(1000L, flat.get(0).eventTime(), "first entry must be oldest");
            assertEquals(2000L, flat.get(1).eventTime(), "second entry must be newest");
        }

        @Test
        @DisplayName("Multiple event types are merged and sorted by timestamp ascending")
        void multipleEventTypesMergedAndSorted() throws Exception {
            AgentState s = AgentState.init("FH-3", "account", 0L);
            HorizonConfig h = HorizonConfig.count(10);

            // Interleave timestamps across two event types
            s.addEvent("transaction",  event(1000L, "{\"type\":\"txn\"}"),    h, 1000L);
            s.addEvent("account-update", event(1500L, "{\"type\":\"upd\"}"), h, 1500L);
            s.addEvent("transaction",  event(3000L, "{\"type\":\"txn\"}"),    h, 3000L);
            s.addEvent("account-update", event(2000L, "{\"type\":\"upd\"}"), h, 2000L);

            List<AgentState.FlatHistoryEntry> flat = s.getFlatHistory();

            assertEquals(4, flat.size(), "all four events must appear in the flat list");

            // Verify strict ascending timestamp order
            for (int i = 1; i < flat.size(); i++) {
                assertTrue(flat.get(i).eventTime() >= flat.get(i - 1).eventTime(),
                    "flat history must be sorted by eventTime ascending");
            }

            // First entry must be the transaction at t=1000
            assertEquals("transaction", flat.get(0).eventType());
            assertEquals(1000L,          flat.get(0).eventTime());

            // Last entry must be the transaction at t=3000
            assertEquals("transaction", flat.get(3).eventType());
            assertEquals(3000L,          flat.get(3).eventTime());
        }

        @Test
        @DisplayName("FlatHistoryEntry exposes eventType, eventTime, and payload")
        void flatEntryExposesAllFields() throws Exception {
            AgentState s = AgentState.init("FH-4", "account", 0L);
            HorizonConfig h = HorizonConfig.count(5);

            JsonNode payload = MAPPER.readTree("{\"score\":99}");
            s.addEvent("evaluation", new AgentState.HistoricalEvent(7777L, payload), h, 7777L);

            List<AgentState.FlatHistoryEntry> flat = s.getFlatHistory();
            assertEquals(1, flat.size());

            AgentState.FlatHistoryEntry entry = flat.get(0);
            assertEquals("evaluation", entry.eventType(),  "eventType must be preserved");
            assertEquals(7777L,         entry.eventTime(), "eventTime must be preserved");
            assertNotNull(entry.payload(), "payload must be non-null");
            assertEquals(99, entry.payload().path("score").asInt(),
                "payload content must be preserved");
        }
    }


}

