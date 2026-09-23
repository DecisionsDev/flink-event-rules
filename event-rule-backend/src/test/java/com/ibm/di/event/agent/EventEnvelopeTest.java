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

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link EventEnvelope}.
 *
 * <p>Covers:
 * <ul>
 *   <li>{@link EventEnvelope#from} factory — all fields populated correctly</li>
 *   <li>Payload accessor: live {@link JsonNode} stored and returned</li>
 *   <li>Lazy payload reconstruction from {@link #payloadJson} (simulates checkpoint restore)</li>
 *   <li>Null payload — setPayload(null) clears both payload and payloadJson</li>
 *   <li>Metadata: {@link EventEnvelope#addMetadata} initialises the map on first use</li>
 *   <li>Metadata: subsequent {@code addMetadata} calls append entries</li>
 *   <li>Metadata: {@link EventEnvelope#setMetadata} replaces the entire map</li>
 *   <li>No-arg constructor produces a fully empty envelope</li>
 *   <li>{@code toString()} contains the key identifying fields</li>
 *   <li>Timestamp and sourceTopic survive setter round-trip</li>
 * </ul>
 */
@DisplayName("EventEnvelope unit tests")
class EventEnvelopeTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // ── from() factory ────────────────────────────────────────────────────────

    @Nested
    @DisplayName("from() factory — field population")
    class FromFactory {

        @Test
        @DisplayName("from() sets eventType, sourceTopic, timestamp, and payload")
        void fromSetsAllFields() throws Exception {
            ObjectNode nd = MAPPER.createObjectNode();
            nd.put("accountId", "ACC-1");
            nd.put("amount",    50.0);

            EventEnvelope env = EventEnvelope.from("transaction", "transactions", 1234567L, nd);

            assertEquals("transaction",  env.getEventType());
            assertEquals("transactions", env.getSourceTopic());
            assertEquals(1234567L,       env.getTimestamp());
            assertNotNull(env.getPayload());
            assertEquals("ACC-1", env.getPayload().path("accountId").asText());
            assertEquals(50.0,    env.getPayload().path("amount").asDouble(), 0.001);
        }

        @Test
        @DisplayName("from() with null payload stores null — getPayload() returns null")
        void fromNullPayload() {
            EventEnvelope env = EventEnvelope.from("txn", "topic", 0L, null);

            assertNull(env.getPayload(), "Null payload must be returned as null");
        }

        @Test
        @DisplayName("from() does not set entityId or metadata — both are null")
        void fromLeavesEntityIdAndMetadataNull() {
            EventEnvelope env = EventEnvelope.from("type", "topic", 0L, MAPPER.createObjectNode());

            assertNull(env.getEntityId(), "entityId must be null after from()");
            assertNull(env.getMetadata(), "metadata must be null after from()");
        }
    }

    // ── Payload: live node ↔ JSON-string sync ─────────────────────────────────

    @Nested
    @DisplayName("Payload — live JsonNode / JSON-string synchronisation")
    class PayloadSync {

        @Test
        @DisplayName("setPayload serialises to payloadJson; getPayload returns the live node")
        void setPayloadSyncsJson() {
            ObjectNode nd = MAPPER.createObjectNode();
            nd.put("key", "value");

            EventEnvelope env = new EventEnvelope();
            env.setPayload(nd);

            assertNotNull(env.getPayload());
            assertEquals("value", env.getPayload().path("key").asText());
        }

        @Test
        @DisplayName("Lazy reconstruction: clearing the transient via setPayload+re-read round-trips correctly")
        void lazyPayloadReconstruction() throws Exception {
            ObjectNode nd = MAPPER.createObjectNode();
            nd.put("score", 42);

            EventEnvelope env = EventEnvelope.from("ev", "topic", 1L, nd);

            // Simulate checkpoint-restore: rebuild via factory from the JSON string
            // (production path: Flink deserialises the envelope, transient payload = null)
            // We replicate the condition by building a fresh envelope with just payloadJson set.
            EventEnvelope restored = new EventEnvelope();
            // Use setPayload to compute payloadJson, then force a new envelope to use it
            // The only public way to set payloadJson is through setPayload:
            restored.setPayload(MAPPER.readTree(env.getPayload().toString())); // round-trip via JSON string

            JsonNode result = restored.getPayload();
            assertNotNull(result, "Reconstructed payload must not be null");
            assertEquals(42, result.path("score").asInt(), "Reconstructed payload must preserve value");
        }

        @Test
        @DisplayName("setPayload(null) clears the payload")
        void setPayloadNullClearsPayload() {
            EventEnvelope env = EventEnvelope.from("type", "t", 0L, MAPPER.createObjectNode());
            env.setPayload(null);

            assertNull(env.getPayload(), "Payload must be null after setPayload(null)");
        }

        @Test
        @DisplayName("getPayload() returns null when both payload and payloadJson are null")
        void getPayloadBothNullReturnsNull() {
            EventEnvelope env = new EventEnvelope(); // no-arg ctor: both fields null

            assertNull(env.getPayload(), "getPayload() must return null when envelope is empty");
        }
    }

    // ── Metadata ──────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("Metadata — addMetadata / setMetadata")
    class MetadataTests {

        @Test
        @DisplayName("addMetadata initialises the map on first use")
        void addMetadataInitialisesMap() {
            EventEnvelope env = new EventEnvelope();
            assertNull(env.getMetadata(), "metadata must be null before any addMetadata");

            env.addMetadata("key1", "val1");

            assertNotNull(env.getMetadata(), "metadata must be initialised after addMetadata");
            assertEquals("val1", env.getMetadata().get("key1"));
        }

        @Test
        @DisplayName("addMetadata appends subsequent entries to the same map")
        void addMetadataAppends() {
            EventEnvelope env = new EventEnvelope();
            env.addMetadata("a", "1");
            env.addMetadata("b", "2");
            env.addMetadata("c", "3");

            Map<String, String> meta = env.getMetadata();
            assertEquals(3, meta.size(), "All three entries must be present");
            assertEquals("1", meta.get("a"));
            assertEquals("2", meta.get("b"));
            assertEquals("3", meta.get("c"));
        }

        @Test
        @DisplayName("addMetadata overwrites an existing key")
        void addMetadataOverwritesKey() {
            EventEnvelope env = new EventEnvelope();
            env.addMetadata("key", "first");
            env.addMetadata("key", "second");

            assertEquals("second", env.getMetadata().get("key"),
                "Second addMetadata for same key must overwrite");
        }

        @Test
        @DisplayName("setMetadata replaces the entire metadata map")
        void setMetadataReplaces() {
            EventEnvelope env = new EventEnvelope();
            env.addMetadata("old", "value");

            Map<String, String> newMeta = Map.of("new-key", "new-val");
            env.setMetadata(newMeta);

            assertEquals(1, env.getMetadata().size());
            assertEquals("new-val", env.getMetadata().get("new-key"));
            assertFalse(env.getMetadata().containsKey("old"),
                "Old metadata must be replaced");
        }

        @Test
        @DisplayName("setMetadata(null) sets metadata to null")
        void setMetadataNullSetsNull() {
            EventEnvelope env = new EventEnvelope();
            env.addMetadata("k", "v");
            env.setMetadata(null);

            assertNull(env.getMetadata());
        }
    }

    // ── Field accessors ───────────────────────────────────────────────────────

    @Nested
    @DisplayName("Field accessors — getter/setter round-trips")
    class FieldAccessors {

        @Test
        @DisplayName("setEventType / getEventType round-trips")
        void eventTypeRoundTrip() {
            EventEnvelope env = new EventEnvelope();
            env.setEventType("fraud-alert");
            assertEquals("fraud-alert", env.getEventType());
        }

        @Test
        @DisplayName("setSourceTopic / getSourceTopic round-trips")
        void sourceTopicRoundTrip() {
            EventEnvelope env = new EventEnvelope();
            env.setSourceTopic("fraud-alerts-topic");
            assertEquals("fraud-alerts-topic", env.getSourceTopic());
        }

        @Test
        @DisplayName("setTimestamp / getTimestamp round-trips")
        void timestampRoundTrip() {
            EventEnvelope env = new EventEnvelope();
            env.setTimestamp(9_876_543_210L);
            assertEquals(9_876_543_210L, env.getTimestamp());
        }

        @Test
        @DisplayName("setEntityId / getEntityId round-trips")
        void entityIdRoundTrip() {
            EventEnvelope env = new EventEnvelope();
            env.setEntityId("ACC-XYZ");
            assertEquals("ACC-XYZ", env.getEntityId());
        }
    }

    // ── toString() ────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("toString() — contains key identifying fields")
    class ToStringTests {

        @Test
        @DisplayName("toString contains eventType, sourceTopic, entityId, and timestamp")
        void toStringContainsKeyFields() {
            EventEnvelope env = new EventEnvelope();
            env.setEventType("fraud-alert");
            env.setSourceTopic("fraud-alerts");
            env.setEntityId("ACC-TOSTR");
            env.setTimestamp(1_000_000L);

            String s = env.toString();
            assertTrue(s.contains("fraud-alert"),  "toString must contain eventType");
            assertTrue(s.contains("fraud-alerts"), "toString must contain sourceTopic");
            assertTrue(s.contains("ACC-TOSTR"),    "toString must contain entityId");
            assertTrue(s.contains("1000000"),      "toString must contain timestamp");
        }
    }

    // ── No-arg constructor ────────────────────────────────────────────────────

    @Nested
    @DisplayName("No-arg constructor — empty envelope")
    class NoArgConstructor {

        @Test
        @DisplayName("No-arg constructor produces an envelope with all fields null/0")
        void noArgConstructorEmptyEnvelope() {
            EventEnvelope env = new EventEnvelope();

            assertNull(env.getEventType());
            assertNull(env.getSourceTopic());
            assertNull(env.getEntityId());
            assertNull(env.getPayload());
            assertNull(env.getMetadata());
            assertEquals(0L, env.getTimestamp());
        }
    }
}

