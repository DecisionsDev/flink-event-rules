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
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.streaming.api.TimerService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("ScheduleManager unit tests")
class ScheduleManagerTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String T1 = "2026-10-18T23:29:16Z";
    private static final String T2 = "2026-10-19T08:00:00Z";
    private static final long   T1_MS = Instant.parse(T1).toEpochMilli();
    private static final long   T2_MS = Instant.parse(T2).toEpochMilli();

    // ── In-memory test doubles ────────────────────────────────────────────────

    static class MemMapState<K, V> implements MapState<K, V> {
        final Map<K, V> map = new HashMap<>();
        @Override public V get(K k)                         { return map.get(k); }
        @Override public void put(K k, V v)                 { map.put(k, v); }
        @Override public void putAll(Map<K, V> m)          { map.putAll(m); }
        @Override public void remove(K k)                   { map.remove(k); }
        @Override public boolean contains(K k)              { return map.containsKey(k); }
        @Override public Iterable<Map.Entry<K, V>> entries(){ return map.entrySet(); }
        @Override public Iterable<K> keys()                 { return map.keySet(); }
        @Override public Iterable<V> values()               { return map.values(); }
        @Override public Iterator<Map.Entry<K, V>> iterator(){ return map.entrySet().iterator(); }
        @Override public boolean isEmpty()                  { return map.isEmpty(); }
        @Override public void clear()                       { map.clear(); }
    }

    /** Records the event-time timers currently registered. */
    static class FakeTimers implements TimerService {
        final TreeSet<Long> eventTimers = new TreeSet<>();
        final List<Long> deleted = new ArrayList<>();
        @Override public long currentProcessingTime()        { return 0; }
        @Override public long currentWatermark()             { return 0; }
        @Override public void registerProcessingTimeTimer(long t) { }
        @Override public void registerEventTimeTimer(long t) { eventTimers.add(t); }
        @Override public void deleteProcessingTimeTimer(long t) { }
        @Override public void deleteEventTimeTimer(long t)   { eventTimers.remove(t); deleted.add(t); }
    }

    private MemMapState<String, ScheduleRequest> byId;
    private MemMapState<Long, List<String>>      idsByTime;
    private FakeTimers                           timers;
    private ScheduleManager                      mgr;

    @BeforeEach
    void setUp() {
        byId      = new MemMapState<>();
        idsByTime = new MemMapState<>();
        timers    = new FakeTimers();
        mgr       = new ScheduleManager(byId, idsByTime, "test");
    }

    private static ScheduleRequest start(String id, String tp) {
        return new ScheduleRequest(id, "desc-" + id, tp, ScheduleRequest.Status.START);
    }

    private static ScheduleRequest cancel(String id) {
        return new ScheduleRequest(id, null, null, ScheduleRequest.Status.CANCEL);
    }

    private static ScheduleRequest cancelAll() {
        return new ScheduleRequest(null, null, null, ScheduleRequest.Status.CANCELALL);
    }

    private static JsonNode json(String s) throws Exception { return MAPPER.readTree(s); }

    // ── Validation ────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("validate / parseRequests")
    class Validation {

        @Test
        void validStart() throws Exception {
            ScheduleRequest r = ScheduleManager.validate(json(
                "{\"id\":\"a\",\"description\":\"d\",\"timepoint\":\"" + T1 + "\",\"status\":\"START\"}"));
            assertEquals("a", r.getId());
            assertEquals("d", r.getDescription());
            assertEquals(T1_MS, r.timepointMs());
            assertEquals(ScheduleRequest.Status.START, r.getStatus());
        }

        @Test
        void statusIsCaseInsensitive() throws Exception {
            assertEquals(ScheduleRequest.Status.CANCEL, ScheduleManager.validate(
                json("{\"id\":\"a\",\"status\":\"cancel\"}")).getStatus());
        }

        @Test
        void cancelAllNeedsNeitherIdNorTimepoint() throws Exception {
            assertEquals(ScheduleRequest.Status.CANCELALL, ScheduleManager.validate(
                json("{\"status\":\"CANCELALL\"}")).getStatus());
        }

        @Test
        void missingDescriptionDefaultsToEmpty() throws Exception {
            assertEquals("", ScheduleManager.validate(json(
                "{\"id\":\"a\",\"timepoint\":\"" + T1 + "\",\"status\":\"START\"}")).getDescription());
        }

        @Test
        void rejectsInvalidPayloads() throws Exception {
            String[] bad = {
                "[]",
                "{\"id\":\"a\"}",                                              // no status
                "{\"id\":\"a\",\"status\":\"PAUSE\"}",                         // unknown status
                "{\"status\":\"START\",\"timepoint\":\"" + T1 + "\"}",         // START without id
                "{\"status\":\"CANCEL\"}",                                     // CANCEL without id
                "{\"id\":\"a\",\"status\":\"START\"}",                         // START without timepoint
                "{\"id\":\"a\",\"status\":\"START\",\"timepoint\":\"tomorrow\"}",
                "{\"id\":\"a\",\"status\":\"START\",\"timepoint\":\"2026-10-18T23:29:16\"}", // no zone
                "{\"id\":5,\"status\":\"CANCEL\"}",                            // id not a string
                "{\"id\":\"a\",\"description\":3,\"status\":\"CANCEL\"}"       // description not a string
            };
            for (String b : bad) {
                assertThrows(IllegalArgumentException.class,
                    () -> ScheduleManager.validate(json(b)), b);
            }
        }

        @Test
        void parseSkipsInvalidAndKeepsValid() throws Exception {
            List<ScheduleRequest> rs = ScheduleManager.parseRequests(json(
                "[{\"id\":\"a\",\"timepoint\":\"" + T1 + "\",\"status\":\"START\"},"
                + "{\"id\":\"b\",\"status\":\"START\"},"
                + "{\"id\":\"c\",\"status\":\"CANCEL\"}]"), "test");
            assertEquals(List.of("a", "c"), rs.stream().map(ScheduleRequest::getId).toList());
        }

        @Test
        void parseToleratesAbsentNullOrNonArray() throws Exception {
            assertTrue(ScheduleManager.parseRequests(null, "t").isEmpty());
            assertTrue(ScheduleManager.parseRequests(json("null"), "t").isEmpty());
            assertTrue(ScheduleManager.parseRequests(json("{}"), "t").isEmpty());
        }
    }

    // ── START ─────────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("START")
    class Start {

        @Test
        void registersTimerAndState() throws Exception {
            mgr.apply(List.of(start("a", T1)), timers);
            assertEquals(java.util.Set.of(T1_MS), timers.eventTimers);
            assertNotNull(byId.get("a"));
            assertEquals(List.of("a"), idsByTime.get(T1_MS));
        }

        @Test
        void sameIdReplacesAndMovesTimer() throws Exception {
            mgr.apply(List.of(start("a", T1)), timers);
            mgr.apply(List.of(start("a", T2)), timers);
            assertEquals(java.util.Set.of(T2_MS), timers.eventTimers, "old timer deleted, new registered");
            assertNull(idsByTime.get(T1_MS));
            assertEquals(List.of("a"), idsByTime.get(T2_MS));
            assertEquals(T2, byId.get("a").getTimepoint());
        }

        @Test
        void sameIdSameTimepointKeepsSingleEntry() throws Exception {
            mgr.apply(List.of(start("a", T1)), timers);
            mgr.apply(List.of(start("a", T1)), timers);
            assertEquals(List.of("a"), idsByTime.get(T1_MS));
            assertTrue(timers.eventTimers.contains(T1_MS), "timer must still exist");
        }

        @Test
        void twoRequestsSameTimepointShareOneTimer() throws Exception {
            mgr.apply(List.of(start("a", T1), start("b", T1)), timers);
            assertEquals(java.util.Set.of(T1_MS), timers.eventTimers);
            assertEquals(List.of("a", "b"), idsByTime.get(T1_MS));
        }
    }

    // ── CANCEL / CANCELALL ────────────────────────────────────────────────────

    @Nested
    @DisplayName("CANCEL and CANCELALL")
    class Cancel {

        @Test
        void cancelRemovesLastRequestAndDeletesTimer() throws Exception {
            mgr.apply(List.of(start("a", T1)), timers);
            mgr.apply(List.of(cancel("a")), timers);
            assertTrue(timers.eventTimers.isEmpty());
            assertTrue(byId.isEmpty());
            assertTrue(idsByTime.isEmpty());
        }

        @Test
        void cancelKeepsTimerWhileAnotherRequestRemainsAtSameTimepoint() throws Exception {
            mgr.apply(List.of(start("a", T1), start("b", T1)), timers);
            mgr.apply(List.of(cancel("a")), timers);
            assertTrue(timers.eventTimers.contains(T1_MS), "timer still needed by b");
            assertTrue(timers.deleted.isEmpty());
            assertEquals(List.of("b"), idsByTime.get(T1_MS));

            mgr.apply(List.of(cancel("b")), timers);
            assertFalse(timers.eventTimers.contains(T1_MS), "last request gone → timer deleted");
        }

        @Test
        void cancelUnknownIdIsNoOp() throws Exception {
            mgr.apply(List.of(start("a", T1)), timers);
            mgr.apply(List.of(cancel("zzz")), timers);
            assertEquals(java.util.Set.of(T1_MS), timers.eventTimers);
            assertNotNull(byId.get("a"));
        }

        @Test
        void cancelAllClearsEverything() throws Exception {
            mgr.apply(List.of(start("a", T1), start("b", T1), start("c", T2)), timers);
            mgr.apply(List.of(cancelAll()), timers);
            assertTrue(timers.eventTimers.isEmpty());
            assertTrue(byId.isEmpty());
            assertTrue(idsByTime.isEmpty());
        }

        @Test
        void requestsAreAppliedInOrder() throws Exception {
            mgr.apply(List.of(start("a", T1), cancelAll(), start("b", T2)), timers);
            assertEquals(java.util.Set.of(T2_MS), timers.eventTimers);
            assertNull(byId.get("a"));
            assertNotNull(byId.get("b"));
        }
    }

    // ── fire ──────────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("fire")
    class Fire {

        @Test
        void buildsOneScheduleEventPerRequestAndClearsState() throws Exception {
            mgr.apply(List.of(start("a", T1), start("b", T1), start("c", T2)), timers);

            List<EventEnvelope> events = mgr.fire(T1_MS, "entity-1");

            assertEquals(2, events.size());
            EventEnvelope e = events.get(0);
            assertEquals("Schedule", e.getEventType());
            assertEquals("entity-1", e.getEntityId());
            assertNull(e.getSourceTopic(), "Schedule events have no topic");
            assertEquals(T1_MS, e.getTimestamp());
            assertEquals(T1, e.getPayload().get("timestamp").asText());
            assertEquals("a", e.getPayload().get("id").asText());
            assertEquals("desc-a", e.getPayload().get("description").asText());
            assertEquals("b", events.get(1).getPayload().get("id").asText());

            // fired requests are removed; the other timepoint is untouched
            assertNull(byId.get("a"));
            assertNull(byId.get("b"));
            assertNull(idsByTime.get(T1_MS));
            assertNotNull(byId.get("c"));
            assertEquals(List.of("c"), idsByTime.get(T2_MS));
            assertTrue(timers.eventTimers.contains(T2_MS));
        }

        @Test
        void firingUnknownTimestampYieldsNothing() throws Exception {
            assertTrue(mgr.fire(T1_MS, "e").isEmpty());
        }

        @Test
        void requestCancelledBeforeFiringIsNotFired() throws Exception {
            mgr.apply(List.of(start("a", T1), start("b", T1)), timers);
            mgr.apply(List.of(cancel("a")), timers);
            List<EventEnvelope> events = mgr.fire(T1_MS, "e");
            assertEquals(1, events.size());
            assertEquals("b", events.get(0).getPayload().get("id").asText());
        }

        @Test
        void startFromWithinFireCycleWorks() throws Exception {
            mgr.apply(List.of(start("a", T1)), timers);
            mgr.fire(T1_MS, "e");
            // the Schedule event's rule re-schedules the same id (recurring schedule)
            mgr.apply(List.of(start("a", T2)), timers);
            assertEquals(List.of("a"), idsByTime.get(T2_MS));
            assertNotNull(byId.get("a"));
        }
    }
}
