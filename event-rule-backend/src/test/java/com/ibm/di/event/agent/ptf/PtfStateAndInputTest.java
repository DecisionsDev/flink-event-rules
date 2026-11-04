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
import com.ibm.di.event.agent.AgentDescriptor;
import com.ibm.di.event.agent.AgentState;
import com.ibm.di.event.agent.EventEnvelope;
import com.ibm.di.event.agent.HorizonConfig;
import com.ibm.di.event.agent.ScheduleRequest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.ibm.di.event.agent.ptf.PtfTestSupport.*;
import static org.junit.jupiter.api.Assertions.*;

@DisplayName("PTF state codec, schedule book, event normalizer, envelope JSON and descriptors")
class PtfStateAndInputTest {

    @Nested
    @DisplayName("AgentStateCodec")
    class Codec {

        @Test
        @DisplayName("null and blank map to null")
        void nulls() {
            assertNull(AgentStateCodec.encode(null));
            assertNull(AgentStateCodec.decode(null, descriptor(null)));
            assertNull(AgentStateCodec.decode(" ", descriptor(null)));
        }

        @Test
        @DisplayName("a state survives a round trip and takes its horizon from the descriptor")
        void roundTrip() {
            AgentDescriptor d = descriptor(null);
            AgentState state = AgentState.init("E1", "entity", 100L);
            state.setEntityData(json("{\"a\":1}"));
            state.addEvent("domain-event", new AgentState.HistoricalEvent(150L, json("{\"x\":1}")),
                HorizonConfig.count(10), 150L);

            AgentState back = AgentStateCodec.decode(AgentStateCodec.encode(state), d);

            assertEquals(json("{\"a\":1}"), back.getEntityData());
            assertEquals("E1", back.getMetadata().getEntityId());
            assertEquals("entity", back.getMetadata().getEntityType());
            assertEquals(state.getMetadata().getCreatedAtMs(), back.getMetadata().getCreatedAtMs());
            assertEquals(state.getMetadata().getEventCount(), back.getMetadata().getEventCount());
            AgentState.EventHistory h = back.getEventHistories().get("domain-event");
            assertEquals(1, h.size());
            assertEquals(150L, h.getEvents().getFirst().getTimestamp());
            assertEquals(json("{\"x\":1}"), h.getEvents().getFirst().getPayload());
            assertEquals(HorizonConfig.count(10).getType(), h.getHorizon().getType());
        }

        @Test
        @DisplayName("a history of an unknown event type gets a zero-count horizon")
        void unknownHistoryHorizon() {
            AgentState state = AgentState.init("E1", "entity", 1L);
            state.addEvent("legacy", new AgentState.HistoricalEvent(2L, null),
                HorizonConfig.count(5), 2L);

            AgentState back = AgentStateCodec.decode(AgentStateCodec.encode(state), descriptor(null));

            assertEquals(0, back.getEventHistories().get("legacy").getHorizon().effectiveCountLimit());
        }

        @Test
        @DisplayName("corrupted JSON is reported")
        void corrupted() {
            assertThrows(RuntimeException.class,
                () -> AgentStateCodec.decode("{not json", descriptor(null)));
        }
    }

    @Nested
    @DisplayName("ScheduleBook")
    class Book {

        private ScheduleRequest start(String id, String at) {
            return new ScheduleRequest(id, "d-" + id, at, ScheduleRequest.Status.START);
        }

        @Test
        @DisplayName("START adds, same id replaces, CANCEL removes, unknown CANCEL is ignored")
        void startCancel() {
            ScheduleBook b = new ScheduleBook(new SchedulePtfState());
            assertTrue(b.isEmpty());
            b.apply(List.of(start("a", "2026-01-01T00:00:00Z"), start("b", "2026-01-02T00:00:00Z")));
            assertEquals(2, b.timepoints().size());

            b.apply(List.of(start("a", "2026-01-03T00:00:00Z")));
            assertEquals(2, b.timepoints().size());
            assertEquals(java.time.Instant.parse("2026-01-03T00:00:00Z").toEpochMilli(),
                b.timepoints().get(1));

            b.apply(List.of(new ScheduleRequest("b", null, null, ScheduleRequest.Status.CANCEL),
                new ScheduleRequest("zzz", null, null, ScheduleRequest.Status.CANCEL)));
            assertEquals(1, b.timepoints().size());
        }

        @Test
        @DisplayName("CANCELALL empties the book")
        void cancelAll() {
            ScheduleBook b = new ScheduleBook(new SchedulePtfState());
            b.apply(List.of(start("a", "2026-01-01T00:00:00Z")));
            b.apply(List.of(new ScheduleRequest(null, null, null, ScheduleRequest.Status.CANCELALL)));
            assertTrue(b.isEmpty());
        }

        @Test
        @DisplayName("timepoints are distinct and ascending; due ones and fire() follow the timestamp")
        void dueAndFire() {
            ScheduleBook b = new ScheduleBook(new SchedulePtfState());
            b.apply(List.of(start("a", "2026-01-02T00:00:00Z"), start("b", "2026-01-01T00:00:00Z"),
                start("c", "2026-01-01T00:00:00Z")));
            long t1 = java.time.Instant.parse("2026-01-01T00:00:00Z").toEpochMilli();
            long t2 = java.time.Instant.parse("2026-01-02T00:00:00Z").toEpochMilli();

            assertEquals(List.of(t1, t2), b.timepoints());
            assertEquals(List.of(t1), b.dueTimepoints(t1 + 1));
            assertEquals(2, b.fire(t1).size());
            assertEquals(List.of(t2), b.timepoints());
            assertTrue(b.fire(t1).isEmpty());
        }

        @Test
        @DisplayName("the entity id is remembered, null is ignored")
        void entityId() {
            ScheduleBook b = new ScheduleBook(new SchedulePtfState());
            b.setEntityId("E1");
            b.setEntityId(null);
            assertEquals("E1", b.getEntityId());
        }

        @Test
        @DisplayName("corrupted schedule state is reported")
        void corrupted() {
            SchedulePtfState s = new SchedulePtfState();
            s.requests.put("a", "{broken");
            assertThrows(IllegalStateException.class, () -> new ScheduleBook(s).timepoints());
        }
    }

    @Nested
    @DisplayName("EventNormalizer")
    class Normalizer {

        private String f(String raw, String field) {
            return EventNormalizer.field(raw, field, "T", "$.id");
        }

        @Test
        @DisplayName("a plain payload yields its entity id, the default type and its numeric timestamp")
        void plain() {
            String raw = "{\"id\":\"E1\",\"timestamp\":1234}";
            assertEquals("E1", f(raw, EventNormalizer.FIELD_ENTITY_ID));
            assertEquals("T", f(raw, EventNormalizer.FIELD_EVENT_TYPE));
            assertEquals("1234", f(raw, EventNormalizer.FIELD_TS));
            assertEquals(json(raw), json(f(raw, EventNormalizer.FIELD_PAYLOAD)));
        }

        @Test
        @DisplayName("an envelope is unwrapped to its payload")
        void envelope() {
            String raw = "{\"agentEventType\":\"X\",\"payload\":{\"id\":\"E2\"}}";
            assertEquals("E2", f(raw, EventNormalizer.FIELD_ENTITY_ID));
            assertEquals("{\"id\":\"E2\"}", f(raw, EventNormalizer.FIELD_PAYLOAD));
        }

        @Test
        @DisplayName("an ISO timestamp becomes epoch milliseconds, a bad or missing one becomes null")
        void timestamps() {
            assertEquals("1000", f("{\"id\":\"E\",\"timestamp\":\"1970-01-01T00:00:01Z\"}",
                EventNormalizer.FIELD_TS));
            assertNull(f("{\"id\":\"E\",\"timestamp\":\"yesterday\"}", EventNormalizer.FIELD_TS));
            assertNull(f("{\"id\":\"E\",\"timestamp\":-5}", EventNormalizer.FIELD_TS));
            assertNull(f("{\"id\":\"E\",\"timestamp\":true}", EventNormalizer.FIELD_TS));
            assertNull(f("{\"id\":\"E\"}", EventNormalizer.FIELD_TS));
        }

        @Test
        @DisplayName("__eventType overrides the default type unless blank")
        void typeOverride() {
            assertEquals("Reset", f("{\"id\":\"E\",\"__eventType\":\"Reset\"}",
                EventNormalizer.FIELD_EVENT_TYPE));
            assertEquals("T", f("{\"id\":\"E\",\"__eventType\":\" \"}",
                EventNormalizer.FIELD_EVENT_TYPE));
        }

        @Test
        @DisplayName("unparseable input yields the error markers and keeps the raw payload")
        void unparseable() {
            assertEquals("deserialization-error", f("{oops", EventNormalizer.FIELD_EVENT_TYPE));
            assertEquals("__error__", f("{oops", EventNormalizer.FIELD_ENTITY_ID));
            assertNull(f("{oops", EventNormalizer.FIELD_TS));
            assertEquals("{oops", f("{oops", EventNormalizer.FIELD_PAYLOAD));
            assertEquals("deserialization-error", f(null, EventNormalizer.FIELD_EVENT_TYPE));
        }

        @Test
        @DisplayName("an unknown field is rejected")
        void unknownField() {
            assertThrows(IllegalArgumentException.class, () -> f("{}", "nope"));
        }

        @Test
        @DisplayName("the SQL function delegates to the normalizer")
        void scalarFunction() {
            assertEquals("E1", new EventEnvelopeFunction().eval("{\"id\":\"E1\"}", "entity_id", "T", "$.id"));
        }
    }

    @Test
    @DisplayName("EnvelopeJson writes the envelope fields and optional metadata")
    void envelopeJson() {
        EventEnvelope e = EventEnvelope.from("alert", "topic", 42L, json("{\"a\":1}"));
        e.setEntityId("E1");
        JsonNode out = json(EnvelopeJson.write(e));
        assertEquals("alert", out.get("agentEventType").asText());
        assertEquals("E1", out.get("entityId").asText());
        assertEquals("topic", out.get("sourceTopic").asText());
        assertEquals(42L, out.get("timestamp").asLong());
        assertEquals(1, out.get("payload").get("a").asInt());
        assertFalse(out.has("metadata"));

        EventEnvelope bare = new EventEnvelope();
        bare.setEventType("x");
        JsonNode bareOut = json(EnvelopeJson.write(bare));
        assertEquals("", bareOut.get("entityId").asText());
        assertFalse(bareOut.has("payload"));
    }

    @Test
    @DisplayName("AgentDescriptors round-trips a descriptor and rejects invalid JSON")
    void descriptors() {
        AgentDescriptor d = descriptor(operation());
        d.setStateConfig(stateConfig("1h", "changes"));

        AgentDescriptor back = AgentDescriptors.fromJson(AgentDescriptors.toJson(d));

        assertEquals("test-agent", back.getAgentId());
        assertEquals(2, back.getSubscriptions().size());
        assertEquals("changes", back.getStateConfig().getChangeLogTopic());
        assertEquals("Op", back.getRuleset().findOperation("Op").getOperation());
        assertThrows(IllegalArgumentException.class, () -> AgentDescriptors.fromJson("{"));
    }

    @Test
    @DisplayName("PTF state holders start empty")
    void stateHolders() {
        assertNull(new AgentPtfState().data);
        assertTrue(new SchedulePtfState().requests.isEmpty());
        assertNull(new SchedulePtfState().entityId);
    }
}
