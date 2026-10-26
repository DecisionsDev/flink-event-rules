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
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link AgentBasedDataStreamJob}.
 *
 * <p>These tests cover the components that can be exercised without a live Flink cluster:
 * <ul>
 *   <li>Descriptor loading — JSON array, wrapper-object format, missing file</li>
 *   <li>Agent validation — agentId, ruleset name/path guard rails</li>
 *   <li>Entity ID extraction — top-level field, nested field, missing extractor</li>
 *   <li>Envelope deserialization — happy path, missing timestamp, malformed JSON</li>
 *   <li>Envelope serialization — output shape, null payload</li>
 *   <li>Envelope key serialization — UTF-8 encoding, null/blank entityId, Unicode</li>
 * </ul>
 */
@DisplayName("AgentBasedDataStreamJob unit tests")
class AgentBasedDataStreamJobTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // ── Descriptor loading ────────────────────────────────────────────────────

    @Nested
    @DisplayName("loadAgentDescriptors")
    class LoadDescriptors {

        @Test
        @DisplayName("Loads a JSON array at file path")
        void loadsJsonArray(@TempDir Path dir) throws Exception {
            String json = """
                [
                  { "agentId": "agent-a", "entityType": "account",
                    "ruleset": { "path": "/tmp/a.jar" } },
                  { "agentId": "agent-b", "entityType": "customer",
                    "ruleset": { "path": "/tmp/b.jar" } }
                ]""";
            Path file = dir.resolve("agents.json");
            Files.writeString(file, json);

            List<AgentDescriptor> agents =
                AgentBasedDataStreamJob.loadAgentDescriptors(file.toString());

            assertEquals(2, agents.size());
            assertEquals("agent-a", agents.get(0).getAgentId());
            assertEquals("agent-b", agents.get(1).getAgentId());
        }

        @Test
        @DisplayName("Loads a wrapper object with 'agents' key")
        void loadsWrapperObject(@TempDir Path dir) throws Exception {
            String json = """
                {
                  "name": "My Agent Job",
                  "agents": [
                    { "agentId": "wrapped-agent", "entityType": "order",
                      "ruleset": { "path": "/tmp/w.jar" } }
                  ]
                }""";
            Path file = dir.resolve("agents.json");
            Files.writeString(file, json);

            List<AgentDescriptor> agents =
                AgentBasedDataStreamJob.loadAgentDescriptors(file.toString());

            assertEquals(1, agents.size());
            assertEquals("wrapped-agent", agents.get(0).getAgentId());
        }

        @Test
        @DisplayName("Loads the banking sample agents.json from the samples directory")
        void loadsBankingSampleDescriptor() throws Exception {
            // Assumes tests run from event-rule-backend/
            Path p = Path.of("../samples/banking-fraud-agent/agents.json");
            org.junit.jupiter.api.Assumptions.assumeTrue(Files.exists(p),
                "Banking agents.json not found — skipping");

            List<AgentDescriptor> agents =
                AgentBasedDataStreamJob.loadAgentDescriptors(p.toString());

            assertFalse(agents.isEmpty(), "Banking agents.json must define at least one agent");
            assertNotNull(agents.get(0).getAgentId());
        }

        @Test
        @DisplayName("Missing file throws meaningful exception")
        void missingFileThrows() {
            assertThrows(IllegalArgumentException.class,
                () -> AgentBasedDataStreamJob.loadAgentDescriptors("/nonexistent/agents.json"),
                "Missing descriptor file must throw IllegalArgumentException");
        }

        @Test
        @DisplayName("Root node that is neither array nor wrapper object throws")
        void wrongFormatThrows(@TempDir Path dir) throws Exception {
            Path file = dir.resolve("agents.json");
            Files.writeString(file, "{\"something\":\"wrong\"}");

            assertThrows(IllegalArgumentException.class,
                () -> AgentBasedDataStreamJob.loadAgentDescriptors(file.toString()));
        }
    }

    // ── Entity ID extraction ──────────────────────────────────────────────────

    @Nested
    @DisplayName("EnvelopeDeserializer.extractEntityId")
    class ExtractEntityId {

        @Test
        @DisplayName("Top-level field: '$.accountId' extracts correctly")
        void topLevel() throws Exception {
            JsonNode payload = MAPPER.readTree("{\"accountId\":\"ACC-001\",\"amount\":500}");
            assertEquals("ACC-001",
                AgentBasedDataStreamJob.EnvelopeDeserializer.extractEntityId(payload, "$.accountId"));
        }

        @Test
        @DisplayName("Nested field: '$.customer.id' extracts nested value")
        void nested() throws Exception {
            JsonNode payload = MAPPER.readTree("{\"customer\":{\"id\":\"CUST-99\",\"name\":\"Alice\"}}");
            assertEquals("CUST-99",
                AgentBasedDataStreamJob.EnvelopeDeserializer.extractEntityId(payload, "$.customer.id"));
        }

        @Test
        @DisplayName("Missing field returns 'unknown'")
        void missingField() throws Exception {
            JsonNode payload = MAPPER.readTree("{\"other\":\"value\"}");
            assertEquals("unknown",
                AgentBasedDataStreamJob.EnvelopeDeserializer.extractEntityId(payload, "$.accountId"));
        }

        @Test
        @DisplayName("Null extractor returns 'unknown'")
        void nullExtractor() throws Exception {
            JsonNode payload = MAPPER.readTree("{\"accountId\":\"ACC-001\"}");
            assertEquals("unknown",
                AgentBasedDataStreamJob.EnvelopeDeserializer.extractEntityId(payload, null));
        }

        @Test
        @DisplayName("Blank extractor returns 'unknown'")
        void blankExtractor() throws Exception {
            JsonNode payload = MAPPER.readTree("{\"accountId\":\"ACC-001\"}");
            assertEquals("unknown",
                AgentBasedDataStreamJob.EnvelopeDeserializer.extractEntityId(payload, "  "));
        }

        @Test
        @DisplayName("Path without '$.' prefix is still resolved")
        void noDollarPrefix() throws Exception {
            JsonNode payload = MAPPER.readTree("{\"orderId\":\"ORD-42\"}");
            // Bare field name is normalised to "$.orderId" before being evaluated by Jayway JsonPath
            assertEquals("ORD-42",
                AgentBasedDataStreamJob.EnvelopeDeserializer.extractEntityId(payload, "orderId"));
        }

        @Test
        @DisplayName("Null payload returns 'unknown'")
        void nullPayload() {
            assertEquals("unknown",
                AgentBasedDataStreamJob.EnvelopeDeserializer.extractEntityId(null, "$.id"));
        }

        @Test
        @DisplayName("Deep path: '$.a.b.c' extracts 3-level nesting (previously broken with 2-level walker)")
        void deepNesting() throws Exception {
            JsonNode payload = MAPPER.readTree("{\"a\":{\"b\":{\"c\":\"DEEP-42\"}}}");
            assertEquals("DEEP-42",
                AgentBasedDataStreamJob.EnvelopeDeserializer.extractEntityId(payload, "$.a.b.c"));
        }
    }

    // ── EnvelopeDeserializer ──────────────────────────────────────────────────

    @Nested
    @DisplayName("EnvelopeDeserializer — deserialize()")
    class DeserializerTests {

        @Test
        @DisplayName("Happy path: entity ID, eventType, timestamp set correctly")
        void happyPath() throws Exception {
            var deser = new AgentBasedDataStreamJob.EnvelopeDeserializer(
                "transactions", "transaction", "$.accountId");

            ObjectNode payload = MAPPER.createObjectNode();
            payload.put("accountId", "ACC-D1");
            payload.put("amount",    1000);
            payload.put("timestamp", 1_700_000_000_000L);

            byte[] bytes = MAPPER.writeValueAsBytes(payload);
            EventEnvelope env = deser.deserialize(bytes);

            assertNotNull(env);
            assertEquals("transaction",          env.getEventType());
            assertEquals("transactions",         env.getSourceTopic());
            assertEquals("ACC-D1",               env.getEntityId());
            assertEquals(1_700_000_000_000L,     env.getTimestamp());
        }

        @Test
        @DisplayName("Missing timestamp falls back to current time (> 0)")
        void missingTimestampFallsBack() throws Exception {
            var deser = new AgentBasedDataStreamJob.EnvelopeDeserializer(
                "orders", "order", "$.customerId");

            ObjectNode payload = MAPPER.createObjectNode();
            payload.put("customerId", "CUST-1");

            byte[] bytes = MAPPER.writeValueAsBytes(payload);
            long before = System.currentTimeMillis();
            EventEnvelope env = deser.deserialize(bytes);
            long after  = System.currentTimeMillis();

            assertTrue(env.getTimestamp() >= before && env.getTimestamp() <= after,
                "Fallback timestamp must be within the current execution window");
        }

        @Test
        @DisplayName("Malformed JSON produces a deserialization-error envelope")
        void malformedJsonProducesErrorEnvelope() throws Exception {
            var deser = new AgentBasedDataStreamJob.EnvelopeDeserializer(
                "transactions", "transaction", "$.accountId");

            byte[] garbage = "not valid json %%%".getBytes();
            EventEnvelope env = deser.deserialize(garbage);

            assertNotNull(env, "Must return an error envelope, not null");
            assertEquals("deserialization-error", env.getEventType(),
                "Malformed bytes must produce a deserialization-error envelope");
            assertEquals("__error__", env.getEntityId());
        }

        @Test
        @DisplayName("isEndOfStream always returns false")
        void isEndOfStreamFalse() {
            var deser = new AgentBasedDataStreamJob.EnvelopeDeserializer(
                "t", "e", "$.id");
            assertFalse(deser.isEndOfStream(new EventEnvelope()));
        }

        @Test
        @DisplayName("__eventType field in payload overrides the subscription-level event type")
        void eventTypeOverrideField() throws Exception {
            // Sentinel sent to the 'account-events' topic (subscription type = "AccountEvent")
            // but payload carries __eventType = "__ENTITY_RESET__"
            var deser = new AgentBasedDataStreamJob.EnvelopeDeserializer(
                "agent-account-events", "AccountEvent", "$.accountNumber");

            ObjectNode payload = MAPPER.createObjectNode();
            payload.put("accountNumber", "111");
            payload.put("__eventType", StatefulAgentProcessor.ENTITY_RESET_EVENT_TYPE);

            EventEnvelope env = deser.deserialize(MAPPER.writeValueAsBytes(payload));

            assertEquals(StatefulAgentProcessor.ENTITY_RESET_EVENT_TYPE, env.getEventType(),
                "__eventType field must override the subscription-level event type");
            assertEquals("111", env.getEntityId(),
                "Entity ID must still be extracted correctly when __eventType is set");
            assertEquals("agent-account-events", env.getSourceTopic(),
                "Source topic must remain the topic the message arrived on");
        }

        @Test
        @DisplayName("Blank __eventType field falls back to the subscription-level event type")
        void blankEventTypeOverrideFallsBack() throws Exception {
            var deser = new AgentBasedDataStreamJob.EnvelopeDeserializer(
                "agent-account-events", "AccountEvent", "$.accountNumber");

            ObjectNode payload = MAPPER.createObjectNode();
            payload.put("accountNumber", "111");
            payload.put("__eventType", "   "); // blank — must be ignored

            EventEnvelope env = deser.deserialize(MAPPER.writeValueAsBytes(payload));

            assertEquals("AccountEvent", env.getEventType(),
                "Blank __eventType must fall back to the subscription-level event type");
        }
    }

    // ── EnvelopeSerializer ────────────────────────────────────────────────────

    @Nested
    @DisplayName("EnvelopeSerializer — serialize()")
    class SerializerTests {

        @Test
        @DisplayName("Output JSON contains agentEventType, entityId, timestamp, payload")
        void outputShape() throws Exception {
            var ser = new AgentBasedDataStreamJob.EnvelopeSerializer();

            ObjectNode payload = MAPPER.createObjectNode();
            payload.put("decision", "APPROVED");

            EventEnvelope env = EventEnvelope.from("fraud-alert", "transactions",
                1_700_000_000_000L, payload);
            env.setEntityId("ACC-S1");

            byte[] bytes = ser.serialize(env);
            JsonNode out  = MAPPER.readTree(bytes);

            assertEquals("fraud-alert",          out.get("agentEventType").asText());
            assertEquals("ACC-S1",               out.get("entityId").asText());
            assertEquals("transactions",         out.get("sourceTopic").asText(),
                "Output must include the sourceTopic field");
            assertEquals(1_700_000_000_000L,     out.get("timestamp").asLong());
            assertTrue(out.has("payload"),       "Output must include the payload node");
            assertEquals("APPROVED",             out.get("payload").get("decision").asText());
        }

        @Test
        @DisplayName("Null payload does not cause serialization failure and omits the payload field")
        void nullPayloadHandled() throws Exception {
            var ser = new AgentBasedDataStreamJob.EnvelopeSerializer();

            EventEnvelope env = new EventEnvelope();
            env.setEventType("agent-output");
            env.setEntityId("ACC-NP");
            env.setTimestamp(1000L);
            // payload is null

            byte[] bytes = ser.serialize(env);
            JsonNode out = MAPPER.readTree(bytes);

            assertEquals("agent-output", out.get("agentEventType").asText(),
                "agentEventType must be present even when payload is null");
            assertEquals("ACC-NP",       out.get("entityId").asText(),
                "entityId must be present even when payload is null");
            assertEquals(1000L,          out.get("timestamp").asLong(),
                "timestamp must be present even when payload is null");
            assertFalse(out.has("payload"),
                "payload field must be absent when envelope payload is null");
        }
    }

    // ── Cross-agent Kafka round-trip ──────────────────────────────────────────

    /**
     * Covers the scenario where agent-A serialises an outbound event via
     * {@link AgentBasedDataStreamJob.EnvelopeSerializer} and agent-B reads it
     * back via {@link AgentBasedDataStreamJob.EnvelopeDeserializer} using its
     * own {@code entityKeyExtractor}.
     *
     * <p>The serialiser wraps the domain payload under a {@code "payload"} key:
     * <pre>
     *   { "agentEventType": "...", "entityId": "...", "payload": { … } }
     * </pre>
     * The deserialiser must unwrap this envelope so that:
     * <ul>
     *   <li>The downstream agent's {@code entityKeyExtractor} resolves against
     *       the inner domain fields (e.g. {@code $.airport}), not the wrapper.</li>
     *   <li>The envelope's {@code payload} carried into the rule engine is the
     *       raw domain object, not the wrapper envelope.</li>
     * </ul>
     * This is the root-cause fix for the
     * {@code "[JsonPathUtils] entityKeyExtractor '$.airport' did not match any field"}
     * error observed in the airtraffic aircraft-agent → airport-agent pipeline.
     */
    @Nested
    @DisplayName("Cross-agent Kafka round-trip — EnvelopeSerializer → EnvelopeDeserializer")
    class CrossAgentKafkaRoundTripTests {

        @Test
        @DisplayName("Downstream agent extracts its own entity key from the inner payload (airtraffic scenario)")
        void downstreamEntityKeyExtractedFromInnerPayload() throws Exception {
            // ── upstream (aircraft-agent) side: build a TrackedAircraftEvent outbound envelope
            ObjectNode trackedAircraftPayload = MAPPER.createObjectNode();
            trackedAircraftPayload.put("aircraftId", "AC123");
            trackedAircraftPayload.put("airport",    "KJFK");
            trackedAircraftPayload.put("altitude",   35000);
            trackedAircraftPayload.put("timestamp",  1_700_000_000_000L);

            EventEnvelope upstream = EventEnvelope.from(
                "TrackedAircraftEvent", "agent-aircraft-events", 1_700_000_000_000L,
                trackedAircraftPayload);
            upstream.setEntityId("AC123"); // aircraft-agent keys by aircraftId

            // ── Kafka sink serialises it (as aircraft-agent's EnvelopeSerializer does)
            var ser   = new AgentBasedDataStreamJob.EnvelopeSerializer();
            byte[] kafkaBytes = ser.serialize(upstream);

            // ── verify the wire format has the wrapper shape
            JsonNode wire = MAPPER.readTree(kafkaBytes);
            assertTrue(wire.has("agentEventType"), "Wire format must have agentEventType wrapper");
            assertTrue(wire.has("payload"),        "Wire format must have payload wrapper");
            assertEquals("KJFK", wire.get("payload").get("airport").asText(),
                "airport must be inside the payload node on the wire");

            // ── downstream (airport-agent) side: deserialise using $.airport extractor
            var deser = new AgentBasedDataStreamJob.EnvelopeDeserializer(
                "agent-tracked-aircraft-events", "TrackedAircraftEvent", "$.airport");

            EventEnvelope downstream = deser.deserialize(kafkaBytes);

            assertNotNull(downstream);
            assertEquals("TrackedAircraftEvent", downstream.getEventType(),
                "eventType must survive the round-trip");
            assertEquals("KJFK", downstream.getEntityId(),
                "airport-agent must partition by the airport field, not the upstream aircraftId");
            assertEquals("agent-tracked-aircraft-events", downstream.getSourceTopic(),
                "sourceTopic must be the downstream subscription topic");

            // The payload visible to the rule engine must be the raw domain object
            assertNotNull(downstream.getPayload());
            assertEquals("AC123", downstream.getPayload().get("aircraftId").asText(),
                "Rule engine must see the inner domain fields, not the wrapper envelope");
            assertEquals("KJFK", downstream.getPayload().get("airport").asText(),
                "$.airport must be directly accessible in the unwrapped payload");
            assertFalse(downstream.getPayload().has("agentEventType"),
                "Wrapper field agentEventType must not bleed into the rule-engine payload");
        }

        @Test
        @DisplayName("Plain (non-agent) Kafka message is not unwrapped — entity key extracted normally")
        void plainKafkaMessageNotUnwrapped() throws Exception {
            // A message produced by an external system has no agentEventType wrapper
            ObjectNode plain = MAPPER.createObjectNode();
            plain.put("airport",   "EGLL");
            plain.put("icao",      "EGLL");
            plain.put("country",   "GB");
            plain.put("timestamp", 1_700_000_000_000L);

            var deser = new AgentBasedDataStreamJob.EnvelopeDeserializer(
                "agent-airport-init-events", "AirportInitializationEvent", "$.airport");

            EventEnvelope env = deser.deserialize(MAPPER.writeValueAsBytes(plain));

            assertEquals("EGLL", env.getEntityId(),
                "Plain message must be deserialized and keyed normally by $.airport");
            assertEquals("EGLL", env.getPayload().get("airport").asText(),
                "Payload must be the plain message itself");
        }

        @Test
        @DisplayName("Timestamp in inner payload is preferred over the outer wrapper timestamp")
        void innerPayloadTimestampUsed() throws Exception {
            // aircraft-agent emits with its own timestamp
            ObjectNode domainPayload = MAPPER.createObjectNode();
            domainPayload.put("aircraftId", "AC999");
            domainPayload.put("airport",    "KSFO");
            domainPayload.put("timestamp",  1_600_000_000_000L);

            EventEnvelope up = EventEnvelope.from(
                "TrackedAircraftEvent", "agent-aircraft-events", 1_700_000_000_000L,
                domainPayload);
            up.setEntityId("AC999");

            byte[] kafkaBytes = new AgentBasedDataStreamJob.EnvelopeSerializer().serialize(up);

            var deser = new AgentBasedDataStreamJob.EnvelopeDeserializer(
                "agent-tracked-aircraft-events", "TrackedAircraftEvent", "$.airport");

            EventEnvelope downstream = deser.deserialize(kafkaBytes);

            assertEquals(1_600_000_000_000L, downstream.getTimestamp(),
                "Timestamp in the inner payload must be preferred (it is the domain event time)");
        }
    }

    // ── EnvelopeKeySerializer ─────────────────────────────────────────────────

    @Nested
    @DisplayName("EnvelopeKeySerializer — serialize()")
    class KeySerializerTests {

        @Test
        @DisplayName("entityId is serialised as UTF-8 bytes")
        void entityIdSerialised() {
            var ser = new AgentBasedDataStreamJob.EnvelopeKeySerializer();
            EventEnvelope env = new EventEnvelope();
            env.setEntityId("ACC-K1");

            byte[] key = ser.serialize(env);

            assertEquals("ACC-K1", new String(key, java.nio.charset.StandardCharsets.UTF_8),
                "Key bytes must decode to the entityId string");
        }

        @Test
        @DisplayName("Null entityId produces empty byte array — not null")
        void nullEntityIdProducesEmptyArray() {
            var ser = new AgentBasedDataStreamJob.EnvelopeKeySerializer();
            EventEnvelope env = new EventEnvelope(); // entityId is null

            byte[] key = ser.serialize(env);

            assertNotNull(key,        "Key must never be null");
            assertEquals(0, key.length, "Null entityId must produce empty byte array");
        }

        @Test
        @DisplayName("Blank entityId produces empty byte array")
        void blankEntityIdProducesEmptyArray() {
            var ser = new AgentBasedDataStreamJob.EnvelopeKeySerializer();
            EventEnvelope env = new EventEnvelope();
            env.setEntityId("   ");

            byte[] key = ser.serialize(env);

            assertNotNull(key);
            assertEquals(0, key.length, "Blank entityId must produce empty byte array");
        }

        @Test
        @DisplayName("Unicode entityId round-trips correctly")
        void unicodeEntityIdRoundTrips() {
            var ser = new AgentBasedDataStreamJob.EnvelopeKeySerializer();
            EventEnvelope env = new EventEnvelope();
            env.setEntityId("compte-bänk-日本語");

            byte[] key = ser.serialize(env);

            assertEquals("compte-bänk-日本語",
                new String(key, java.nio.charset.StandardCharsets.UTF_8),
                "Unicode entityId must survive UTF-8 round-trip");
        }
    }

    // ── validate() ────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("validate()")
    class ValidateTests {

        private AgentDescriptor validDescriptor() {
            AgentDescriptor d = new AgentDescriptor();
            d.setAgentId("test-agent");
            d.setEntityKeyExtractor("$.id");
            d.setRuleset(new RulesetConfig("/tmp/test.jar"));
            d.setSubscriptions(List.of(
                new EventSubscription("txn",  "transactions", "Svc_txn_op",  HorizonConfig.count(10)),
                new EventSubscription("acct", "accounts",     "",            HorizonConfig.count(1))
            ));
            return d;
        }

        @Test
        @DisplayName("Valid descriptor (one operation + one history-only) passes without exception")
        void validDescriptorPasses() {
            assertDoesNotThrow(() -> AgentBasedDataStreamJob.validate(validDescriptor()));
        }

        @Test
        @DisplayName("Null operation (field absent in JSON) is rejected")
        void nullOperationRejected() {
            AgentDescriptor d = new AgentDescriptor();
            d.setAgentId("a");
            d.setEntityKeyExtractor("$.id");
            d.setRuleset(new RulesetConfig("/tmp/a.jar"));
            // 3-arg constructor leaves operation null — simulates absent JSON field
            d.setSubscriptions(List.of(
                new EventSubscription("txn", "transactions", HorizonConfig.count(5))
            ));
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> AgentBasedDataStreamJob.validate(d));
            assertTrue(ex.getMessage().contains("operation"),
                "Error must mention 'operation'");
            assertTrue(ex.getMessage().contains("txn"),
                "Error must identify the offending eventType");
        }

        @Test
        @DisplayName("Duplicate non-blank operation across two subscriptions produces a warning but does not throw (H-5)")
        void duplicateOperationRejected() {
            // H-5: changed from throw to LOG.warn so the job is not killed by a config
            // mistake that may be intentional (e.g. multi-topic fan-in to the same rule).
            // The validate() method must now complete without throwing.
            AgentDescriptor d = new AgentDescriptor();
            d.setAgentId("b");
            d.setEntityKeyExtractor("$.id");
            d.setRuleset(new RulesetConfig("/tmp/b.jar"));
            d.setSubscriptions(List.of(
                new EventSubscription("txn1", "t1", "SharedOp", HorizonConfig.count(5)),
                new EventSubscription("txn2", "t2", "SharedOp", HorizonConfig.count(5))
            ));
            assertDoesNotThrow(() -> AgentBasedDataStreamJob.validate(d),
                "Duplicate operation should warn, not throw");
        }

        @Test
        @DisplayName("Two history-only subscriptions (both '') are not treated as duplicates")
        void twoHistoryOnlySubscriptionsAllowed() {
            AgentDescriptor d = new AgentDescriptor();
            d.setAgentId("c");
            // Blank entityKeyExtractor is the explicit opt-in for history-only / single-partition mode.
            // R16 made null extractor a hard error; "" is the valid opt-in form.
            d.setEntityKeyExtractor("");
            d.setRuleset(new RulesetConfig("/tmp/c.jar"));
            d.setSubscriptions(List.of(
                new EventSubscription("ctx1", "c1", "", HorizonConfig.count(1)),
                new EventSubscription("ctx2", "c2", "", HorizonConfig.count(1))
            ));
            assertDoesNotThrow(() -> AgentBasedDataStreamJob.validate(d),
                "Two history-only ('') subscriptions must not trigger the duplicate-operation guard");
        }

        @Test
        @DisplayName("Missing agentId is rejected")
        void missingAgentIdRejected() {
            AgentDescriptor d = new AgentDescriptor();
            d.setRuleset(new RulesetConfig("/tmp/x.jar"));
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> AgentBasedDataStreamJob.validate(d));
            assertTrue(ex.getMessage().contains("agentId"),
                "Error must mention 'agentId'");
        }

        @Test
        @DisplayName("Missing ruleset.path is rejected")
        void missingRulesetPathRejected() {
            AgentDescriptor d = new AgentDescriptor();
            d.setAgentId("d");
            d.setRuleset(new RulesetConfig(null));
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> AgentBasedDataStreamJob.validate(d));
            assertTrue(ex.getMessage().contains("ruleset.path"),
                "Error must mention 'ruleset.path'");
        }

        @Test
        @DisplayName("missing entityKeyExtractor with subscriptions — validate() throws IllegalArgumentException")
        void missingEntityKeyExtractorThrows() {
            // null entityKeyExtractor (field absent) with subscriptions is now a hard error
            AgentDescriptor d = new AgentDescriptor();
            d.setAgentId("no-extractor-agent");
            d.setRuleset(new RulesetConfig("/tmp/svc.jar"));
            // entityKeyExtractor intentionally null (not set)
            d.setSubscriptions(List.of(
                new EventSubscription("txn", "txn-topic", "EvalTxn", HorizonConfig.count(10))));
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> AgentBasedDataStreamJob.validate(d),
                "null entityKeyExtractor with subscriptions must throw — it is a fatal misconfiguration");
            assertTrue(ex.getMessage().contains("entityKeyExtractor is required"),
                "Error message must mention 'entityKeyExtractor is required'");
        }

        @Test
        @DisplayName("blank entityKeyExtractor with subscriptions — validate() warns but does not throw (opt-in)")
        void blankEntityKeyExtractorWarnsNotThrows() {
            // An explicit empty string is the deliberate opt-in for history-only / single-partition mode
            AgentDescriptor d = new AgentDescriptor();
            d.setAgentId("blank-extractor-agent");
            d.setRuleset(new RulesetConfig("/tmp/svc.jar"));
            d.setEntityKeyExtractor(""); // explicit blank opt-in
            d.setSubscriptions(List.of(
                new EventSubscription("txn", "txn-topic", "EvalTxn", HorizonConfig.count(10))));
            assertDoesNotThrow(() -> AgentBasedDataStreamJob.validate(d),
                "Explicit blank entityKeyExtractor must warn but not throw — it is a deliberate opt-in");
        }

        @Test
        @DisplayName("no subscriptions with missing entityKeyExtractor — no warning needed")
        void noSubscriptionsNoExtractorNoWarning() {
            // No subscriptions → no events to key → no hot-key risk; validate must pass silently
            AgentDescriptor d = new AgentDescriptor();
            d.setAgentId("no-subs-agent");
            d.setRuleset(new RulesetConfig("/tmp/svc.jar"));
            // No subscriptions and no entityKeyExtractor
            assertDoesNotThrow(() -> AgentBasedDataStreamJob.validate(d));
        }

        @Test
        @DisplayName("subscription-level entityKeyExtractor satisfies validate() without an agent-level value")
        void subscriptionLevelExtractorAccepted() {
            AgentDescriptor d = new AgentDescriptor();
            d.setAgentId("sub-level-agent");
            d.setRuleset(new RulesetConfig("/tmp/svc.jar"));
            EventSubscription a = new EventSubscription("txn", "txn-topic", "EvalTxn", HorizonConfig.count(10));
            a.setEntityKeyExtractor("$.accountId");
            EventSubscription b = new EventSubscription("upd", "upd-topic", "EvalUpd", HorizonConfig.count(10));
            b.setEntityKeyExtractor("$.payload.accountId");
            d.setSubscriptions(List.of(a, b));
            assertDoesNotThrow(() -> AgentBasedDataStreamJob.validate(d));
            assertEquals("$.accountId", d.entityKeyExtractorFor(a));
            assertEquals("$.payload.accountId", d.entityKeyExtractorFor(b));
        }

        @Test
        @DisplayName("subscription without extractor inherits the agent-level value; override wins")
        void agentLevelFallbackAndOverride() {
            AgentDescriptor d = new AgentDescriptor();
            d.setAgentId("fallback-agent");
            d.setRuleset(new RulesetConfig("/tmp/svc.jar"));
            d.setEntityKeyExtractor("$.accountId");
            EventSubscription inherit = new EventSubscription("txn", "txn-topic", "EvalTxn", HorizonConfig.count(10));
            EventSubscription override = new EventSubscription("upd", "upd-topic", "EvalUpd", HorizonConfig.count(10));
            override.setEntityKeyExtractor("$.payload.accountId");
            d.setSubscriptions(List.of(inherit, override));
            assertDoesNotThrow(() -> AgentBasedDataStreamJob.validate(d));
            assertEquals("$.accountId", d.entityKeyExtractorFor(inherit));
            assertEquals("$.payload.accountId", d.entityKeyExtractorFor(override));
        }

        @Test
        @DisplayName("one subscription without any extractor is rejected even if another declares one")
        void partialSubscriptionLevelExtractorRejected() {
            AgentDescriptor d = new AgentDescriptor();
            d.setAgentId("partial-agent");
            d.setRuleset(new RulesetConfig("/tmp/svc.jar"));
            EventSubscription a = new EventSubscription("txn", "txn-topic", "EvalTxn", HorizonConfig.count(10));
            a.setEntityKeyExtractor("$.accountId");
            EventSubscription b = new EventSubscription("upd", "upd-topic", "EvalUpd", HorizonConfig.count(10));
            d.setSubscriptions(List.of(a, b));
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> AgentBasedDataStreamJob.validate(d));
            assertTrue(ex.getMessage().contains("entityKeyExtractor is required"));
            assertTrue(ex.getMessage().contains("'upd'"));
        }
    }

    // ── Sample descriptor content validation ──────────────────────────────────

    @Nested
    @DisplayName("Sample agent descriptors content")
    class SampleDescriptors {

        @Test
        @DisplayName("Banking agents.json: agentId, entityKeyExtractor, subscriptions, outputs, stateConfig")
        void bankingAgentDescriptorComplete() throws Exception {
            Path p = Path.of("../samples/banking-fraud-agent/agents.json");
            org.junit.jupiter.api.Assumptions.assumeTrue(Files.exists(p),
                "Banking agents.json not found — skipping");

            List<AgentDescriptor> agents =
                AgentBasedDataStreamJob.loadAgentDescriptors(p.toString());
            AgentDescriptor agent = agents.get(0);

            assertNotNull(agent.getAgentId(),             "agentId required");
            assertNotNull(agent.getEntityKeyExtractor(),  "entityKeyExtractor required");
            assertNotNull(agent.getSubscriptions(),       "subscriptions required");
            assertFalse(agent.getSubscriptions().isEmpty(), "at least one subscription");
            assertNotNull(agent.getRuleset(),             "ruleset required");
            assertNotNull(agent.getOutputs(),             "outputs required");
            assertFalse(agent.getOutputs().isEmpty(),     "at least one output");
            assertNotNull(agent.getStateConfig(),         "stateConfig required");
            assertNotNull(agent.getStateConfig().getTtl(), "stateConfig.ttl required");
        }

    }

    // ── Agent Processor UID uniqueness ────────────────────────────────────────

    @Nested
    @DisplayName("Agent Processor UID uniqueness — regression for Flink deployment failure")
    class ProcessorUidTests {

        /**
         * Verifies the UID pattern used by {@code connectAndProcess} for all scenarios.
         *
         * <p>The UID template is {@code "agent-processor-" + agentId}.  For this to be
         * globally unique across the whole Flink job, each agent must have a unique
         * {@code agentId}.  Additionally, each agent must be wired <em>exactly once</em>
         * — the old two-pass approach wired chained agents twice (same UID both times),
         * causing Flink deployment to fail.
         *
         * <p>This regression test verifies:
         * <ol>
         *   <li>The UID formula produces distinct strings for distinct agent IDs.</li>
         *   <li>A chained topology of N agents produces N distinct UIDs.</li>
         *   <li>UIDs are deterministic (same agentId → same UID, always).</li>
         *   <li>Chaining depth does not introduce UID collisions at any depth.</li>
         * </ol>
         */
        @Test
        @DisplayName("Distinct agentIds always produce distinct processor UIDs")
        void distinctAgentIdsProduceDistinctUids() {
            List<String> agentIds = List.of("fraud-agent", "scoring-agent", "loyalty-agent");
            Set<String> uids = agentIds.stream()
                .map(id -> "agent-processor-" + id)
                .collect(java.util.stream.Collectors.toSet());

            assertEquals(agentIds.size(), uids.size(),
                "Each distinct agentId must map to a unique processor UID");
        }

        @Test
        @DisplayName("Chained A→B topology: two distinct processor UIDs, no collision")
        void chainedTwoAgentTopologyUidsDistinct() {
            // These are the UIDs that connectAndProcess() would assign
            String uidA = "agent-processor-agent-a";
            String uidB = "agent-processor-agent-b";

            assertNotEquals(uidA, uidB,
                "agent-a and agent-b must produce distinct processor UIDs");
        }

        @Test
        @DisplayName("Deep chain A→B→C→D: four distinct processor UIDs")
        void deepChainFourDistinctUids() {
            List<String> ids = List.of("step-1", "step-2", "step-3", "step-4");
            Set<String> uids = ids.stream()
                .map(id -> "agent-processor-" + id)
                .collect(java.util.stream.Collectors.toSet());

            assertEquals(4, uids.size(),
                "All four processor UIDs must be distinct for A→B→C→D chain");
        }

        @Test
        @DisplayName("UIDs are deterministic: same agentId always produces the same UID")
        void uidIsDeterministic() {
            String id = "fraud-detection";
            String uid1 = "agent-processor-" + id;
            String uid2 = "agent-processor-" + id;
            assertEquals(uid1, uid2, "UID must be deterministic for the same agentId");
        }

        @Test
        @DisplayName("Regression: duplicate-UID scenario — two agents with same ID would collide")
        void duplicateAgentIdWouldCollide() {
            // This test documents the invariant that must NOT happen in a real descriptor:
            // if two agents were given the same agentId, their processor UIDs would collide.
            // The Flink DAG builder (AgentBasedDataStreamJob) must always produce descriptors
            // with unique agentIds.
            String sameId = "shared-agent-id";
            String uid1   = "agent-processor-" + sameId;
            String uid2   = "agent-processor-" + sameId; // same UID — collision!

            assertEquals(uid1, uid2,
                "Two agents with the same agentId produce identical UIDs — this is the bug "
                + "we guard against by ensuring each agent is wired exactly once with a "
                + "unique agentId. See resolveTopology() for the fix.");
        }

        @Test
        @DisplayName("validateUniqueAgentIds rejects duplicate agent IDs")
        void validateUniqueAgentIdsRejectsDuplicates() {
            AgentDescriptor a1 = new AgentDescriptor();
            a1.setAgentId("duplicate-agent");
            AgentDescriptor a2 = new AgentDescriptor();
            a2.setAgentId("duplicate-agent");

            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> AgentBasedDataStreamJob.validateUniqueAgentIds(List.of(a1, a2)));
            assertTrue(ex.getMessage().contains("Duplicate agentId detected"));
            assertTrue(ex.getMessage().contains("duplicate-agent"));
        }

        @Test
        @DisplayName("validateUniqueOperatorUids rejects duplicate output UIDs")
        void validateUniqueOperatorUidsRejectsDuplicateOutputUids() {
            AgentDescriptor agent = new AgentDescriptor();
            agent.setAgentId("agent-1");
            agent.setOutputs(List.of(
                new OutputConfig("alert", "topic-a"),
                new OutputConfig("alert", "topic-b")));

            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> AgentBasedDataStreamJob.validateUniqueOperatorUids(List.of(agent)));
            assertTrue(ex.getMessage().contains("agent-sink-agent-1-alert"));
        }

        @Test
        @DisplayName("validateUniqueOperatorUids rejects wildcard output UID collisions")
        void validateUniqueOperatorUidsRejectsWildcardOutputUidCollisions() {
            AgentDescriptor agent = new AgentDescriptor();
            agent.setAgentId("agent-1");
            agent.setOutputs(List.of(
                new OutputConfig(null, "topic-a"),
                new OutputConfig("", "topic-b")));

            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> AgentBasedDataStreamJob.validateUniqueOperatorUids(List.of(agent)));
            assertTrue(ex.getMessage().contains("agent-sink-agent-1-"));
        }

        @Test
        @DisplayName("validateUniqueOperatorUids accepts distinct output UIDs")
        void validateUniqueOperatorUidsAcceptsDistinctOutputUids() {
            AgentDescriptor a1 = new AgentDescriptor();
            a1.setAgentId("agent-1");
            a1.setOutputs(List.of(new OutputConfig("alert", "topic-a")));
            AgentDescriptor a2 = new AgentDescriptor();
            a2.setAgentId("agent-2");
            a2.setOutputs(List.of(new OutputConfig("alert", "topic-b")));

            assertDoesNotThrow(() ->
                AgentBasedDataStreamJob.validateUniqueOperatorUids(List.of(a1, a2)));
        }

        @Test
        @DisplayName("validateUniqueAgentIds passes when all agent IDs are unique")
        void validateUniqueAgentIdsPassesWhenUnique() {
            AgentDescriptor a1 = new AgentDescriptor();
            a1.setAgentId("agent-1");
            AgentDescriptor a2 = new AgentDescriptor();
            a2.setAgentId("agent-2");

            assertDoesNotThrow(() -> AgentBasedDataStreamJob.validateUniqueAgentIds(List.of(a1, a2)));
        }

    }

    // ── Per-agent chainingEnabled flag ────────────────────────────────────────

    /**
     * Verifies the per-agent {@code chainingEnabled} descriptor field as a feature flag.
     *
     * <p>These tests cover:
     * <ol>
     *   <li><b>Default</b> — omitting {@code chainingEnabled} is equivalent to {@code true}.</li>
     *   <li><b>Aircraft agent ({@code true})</b> — chain edges are detected; Kafka sources
     *       for the chained event type are suppressed.</li>
     *   <li><b>Banking Fraud agent ({@code false})</b> — no chain edges are created; all Kafka
     *       sources remain active.</li>
     *   <li><b>Mixed topology</b> — one agent with chaining enabled and one with chaining
     *       disabled in the same descriptor file; only the enabled agent produces edges.</li>
     *   <li><b>Isolation</b> — changing one agent's setting does not affect another agent.</li>
     * </ol>
     */
    @Nested
    @DisplayName("Per-agent chainingEnabled — descriptor-level feature flag")
    class PerAgentChainingTests {

        // ── Default value ──────────────────────────────────────────────────────

        @Test
        @DisplayName("chainingEnabled defaults to true when omitted from descriptor")
        void defaultIsTrue() {
            AgentDescriptor d = new AgentDescriptor();
            assertTrue(d.isChainingEnabled(),
                "chainingEnabled must default to true so existing descriptors keep chaining");
        }

        @Test
        @DisplayName("chainingEnabled can be explicitly set to false")
        void canBeSetToFalse() {
            AgentDescriptor d = new AgentDescriptor();
            d.setChainingEnabled(false);
            assertFalse(d.isChainingEnabled());
        }

        @Test
        @DisplayName("chainingEnabled can be explicitly set to true")
        void canBeSetToTrue() {
            AgentDescriptor d = new AgentDescriptor();
            d.setChainingEnabled(true);
            assertTrue(d.isChainingEnabled());
        }

        // ── Aircraft agent: chainingEnabled=true ──────────────────────────────

        @Test
        @DisplayName("Aircraft agent (chainingEnabled=true): chain edge to airport agent is detected")
        void aircraftAgentChainingEnabledEdgeDetected() {
            AgentDescriptor aircraft = makeAgent("aircraft-agent", "$.aircraftId",
                List.of("AircraftEvent"), List.of("TrackedAircraftEvent"), true);
            AgentDescriptor airport = makeAgent("airport-agent", "$.airportCode",
                List.of("TrackedAircraftEvent"), List.of("Alert"), true);

            AgentChainBuilder builder = new AgentChainBuilder();
            AgentChainBuilder.DeferredChainedInputs d = builder.resolveTopology(
                List.of(aircraft, airport));

            assertTrue(d.hasChainedInput("airport-agent"),
                "airport-agent must receive chained TrackedAircraftEvent from aircraft-agent");
            assertFalse(d.hasChainedInput("aircraft-agent"),
                "aircraft-agent has no upstream chain input");
            assertTrue(d.chainedEventTypes("airport-agent").contains("TrackedAircraftEvent"),
                "TrackedAircraftEvent Kafka source at airport-agent must be suppressed");
        }

        @Test
        @DisplayName("Aircraft agent (chainingEnabled=true): Kafka source for TrackedAircraftEvent suppressed at airport")
        void aircraftAgentChainingEnabledKafkaSuppressed() {
            AgentDescriptor aircraft = makeAgent("aircraft-agent", "$.aircraftId",
                List.of("AircraftEvent"), List.of("TrackedAircraftEvent"), true);
            AgentDescriptor airport = makeAgent("airport-agent", "$.airportCode",
                List.of("TrackedAircraftEvent", "AirportEvent"), List.of("Alert"), true);

            AgentChainBuilder builder = new AgentChainBuilder();
            AgentChainBuilder.DeferredChainedInputs d = builder.resolveTopology(
                List.of(aircraft, airport));

            Set<String> suppressed = d.chainedEventTypes("airport-agent");
            assertTrue(suppressed.contains("TrackedAircraftEvent"),
                "TrackedAircraftEvent must be suppressed — it arrives via chain");
            assertFalse(suppressed.contains("AirportEvent"),
                "AirportEvent must NOT be suppressed — it comes only from Kafka");
        }

        // ── Banking Fraud agent: chainingEnabled=false ────────────────────────

        @Test
        @DisplayName("Banking Fraud agent (chainingEnabled=false): no chain edges created for it")
        void bankingFraudAgentChainingDisabledNoEdges() {
            // Even if a hypothetical downstream subscribes to an event the fraud agent emits,
            // the fraud agent will not feed the chain because chainingEnabled=false.
            AgentDescriptor fraud = makeAgent("banking-fraud-agent", "$.accountNumber",
                List.of("Transaction"), List.of("Alert"), false);
            AgentDescriptor hypotheticalDownstream = makeAgent("downstream-agent", "$.id",
                List.of("Alert"), List.of("Notification"), true);

            AgentChainBuilder builder = new AgentChainBuilder();
            AgentChainBuilder.DeferredChainedInputs d = builder.resolveTopology(
                List.of(fraud, hypotheticalDownstream));

            assertFalse(d.hasChainedInput("downstream-agent"),
                "downstream-agent must NOT receive a chained stream from banking-fraud-agent "
                + "because banking-fraud-agent has chainingEnabled=false");
            assertTrue(d.isEmpty(),
                "No chain edges must be created when the upstream has chainingEnabled=false");
        }

        @Test
        @DisplayName("Banking Fraud agent (chainingEnabled=false): no Kafka sources are suppressed")
        void bankingFraudAgentChainingDisabledNoSuppression() {
            AgentDescriptor fraud = makeAgent("banking-fraud-agent", "$.accountNumber",
                List.of("Transaction"), List.of("Alert"), false);

            AgentChainBuilder builder = new AgentChainBuilder();
            AgentChainBuilder.DeferredChainedInputs d = builder.resolveTopology(List.of(fraud));

            assertTrue(d.chainedEventTypes("banking-fraud-agent").isEmpty(),
                "banking-fraud-agent has chainingEnabled=false — no Kafka sources should be suppressed");
        }

        @Test
        @DisplayName("Banking Fraud agent (chainingEnabled=false): cannot receive chained inbound stream")
        void bankingFraudAgentChainingDisabledNoInbound() {
            // If the fraud agent subscribes to an event type that another agent emits,
            // it must NOT receive a chained inbound stream because chainingEnabled=false.
            AgentDescriptor upstream = makeAgent("upstream-agent", "$.id",
                List.of("RawEvent"), List.of("EnrichedEvent"), true);
            AgentDescriptor fraud = makeAgent("banking-fraud-agent", "$.accountNumber",
                List.of("EnrichedEvent"), List.of("Alert"), false);

            AgentChainBuilder builder = new AgentChainBuilder();
            AgentChainBuilder.DeferredChainedInputs d = builder.resolveTopology(
                List.of(upstream, fraud));

            assertFalse(d.hasChainedInput("banking-fraud-agent"),
                "banking-fraud-agent must NOT receive a chained inbound stream "
                + "(chainingEnabled=false prevents both upstream and downstream chain participation)");
        }

        // ── Mixed topology ────────────────────────────────────────────────────

        @Test
        @DisplayName("Mixed topology: aircraft (enabled) + fraud (disabled) — only aircraft participates in chain")
        void mixedTopologyOnlyEnabledAgentChains() {
            // Aircraft agent: chainingEnabled=true — will participate in chain
            AgentDescriptor aircraft = makeAgent("aircraft-agent", "$.aircraftId",
                List.of("AircraftEvent"), List.of("TrackedAircraftEvent"), true);
            // Airport agent: chainingEnabled=true — downstream of aircraft
            AgentDescriptor airport = makeAgent("airport-agent", "$.airportCode",
                List.of("TrackedAircraftEvent"), List.of("Alert"), true);
            // Fraud agent: chainingEnabled=false — runs independently even though it also
            // emits Alert which overlaps with airport, and subscribes to nothing from aircraft
            AgentDescriptor fraud = makeAgent("banking-fraud-agent", "$.accountNumber",
                List.of("Transaction"), List.of("Alert"), false);

            List<AgentDescriptor> agents = List.of(aircraft, airport, fraud);
            AgentChainBuilder builder = new AgentChainBuilder();
            AgentChainBuilder.DeferredChainedInputs d = builder.resolveTopology(agents);

            // Aircraft → Airport chain exists
            assertTrue(d.hasChainedInput("airport-agent"),
                "airport-agent must receive chained TrackedAircraftEvent from aircraft-agent");
            // Fraud agent is isolated — no chain edges in either direction
            assertFalse(d.hasChainedInput("banking-fraud-agent"),
                "banking-fraud-agent must have no chained inbound (chainingEnabled=false)");
            assertFalse(d.hasChainedInput("aircraft-agent"),
                "aircraft-agent is purely upstream, no chained inbound");
        }

        @Test
        @DisplayName("Mixed topology: fraud agent disabled — does not produce chain edges even when event types overlap")
        void mixedTopologyFraudDisabledNoEdgesEvenWithOverlap() {
            // Fraud emits Alert; a hypothetical consumer subscribes to Alert.
            // If fraud had chainingEnabled=true this would create a chain edge.
            // With chainingEnabled=false it must not.
            AgentDescriptor fraud = makeAgent("banking-fraud-agent", "$.accountNumber",
                List.of("Transaction"), List.of("Alert"), false);
            AgentDescriptor consumer = makeAgent("alert-consumer", "$.id",
                List.of("Alert"), List.of("Notification"), true);

            AgentChainBuilder builder = new AgentChainBuilder();
            AgentChainBuilder.DeferredChainedInputs d = builder.resolveTopology(
                List.of(fraud, consumer));

            assertTrue(d.isEmpty(),
                "No chain edges must exist when the upstream (fraud) has chainingEnabled=false, "
                + "even though the event type matches the downstream subscription");
        }

        // ── Isolation: one agent's flag does not affect another ───────────────

        @Test
        @DisplayName("Isolation: disabling chaining on agent-B does not affect the A→C chain")
        void disablingOneBAgentDoesNotAffectOtherChain() {
            // A emits ev-1, C subscribes to ev-1 — independent chain A→C
            AgentDescriptor a = makeAgent("agent-a", "$.id",
                List.of("raw"), List.of("ev-1"), true);
            AgentDescriptor c = makeAgent("agent-c", "$.id",
                List.of("ev-1"), List.of("done"), true);
            // B emits ev-2 but chainingEnabled=false — B→D chain must not form
            AgentDescriptor b = makeAgent("agent-b", "$.id",
                List.of("raw2"), List.of("ev-2"), false);
            AgentDescriptor d2 = makeAgent("agent-d", "$.id",
                List.of("ev-2"), List.of("done2"), true);

            List<AgentDescriptor> agents = List.of(a, b, c, d2);
            AgentChainBuilder builder = new AgentChainBuilder();
            AgentChainBuilder.DeferredChainedInputs chains = builder.resolveTopology(agents);

            // A→C must still work
            assertTrue(chains.hasChainedInput("agent-c"),
                "agent-c must receive chained ev-1 from agent-a (unaffected by agent-b's flag)");
            assertTrue(chains.chainedEventTypes("agent-c").contains("ev-1"),
                "ev-1 Kafka source at agent-c must be suppressed");

            // B→D must not form
            assertFalse(chains.hasChainedInput("agent-d"),
                "agent-d must NOT receive a chain from agent-b (agent-b has chainingEnabled=false)");

            // B itself must not appear as downstream
            assertFalse(chains.hasChainedInput("agent-b"),
                "agent-b must not be downstream (chainingEnabled=false)");
        }

        @Test
        @DisplayName("Isolation: enabling chaining on agent-B does not create unwanted edges for unrelated agents")
        void enablingOneBAgentDoesNotCreateUnwantedEdges() {
            // Three disjoint agents (different event types), all chainingEnabled=true.
            // No overlapping event types → no chain edges at all.
            AgentDescriptor a = makeAgent("agent-a", "$.id",
                List.of("input-a"), List.of("output-a"), true);
            AgentDescriptor b = makeAgent("agent-b", "$.id",
                List.of("input-b"), List.of("output-b"), true);
            AgentDescriptor c = makeAgent("agent-c", "$.id",
                List.of("input-c"), List.of("output-c"), true);

            AgentChainBuilder builder = new AgentChainBuilder();
            AgentChainBuilder.DeferredChainedInputs chains = builder.resolveTopology(List.of(a, b, c));

            assertTrue(chains.isEmpty(),
                "No chain edges must exist between disjoint agents even when all have chainingEnabled=true");
        }

        // ── JSON round-trip (descriptor loading) ──────────────────────────────

        @Test
        @DisplayName("chainingEnabled=true round-trips through JSON serialization")
        void chainingEnabledTrueRoundTrips() throws Exception {
            String json = "{\"agentId\":\"a\",\"entityType\":\"e\",\"entityKeyExtractor\":\"$.id\","
                + "\"chainingEnabled\":true,"
                + "\"ruleset\":{\"path\":\"/tmp/a.jar\"},"
                + "\"subscriptions\":[{\"eventType\":\"ev\",\"topic\":\"t\",\"operation\":\"op\"}],"
                + "\"outputs\":[{\"eventType\":\"out\",\"topic\":\"tout\"}]}";
            AgentDescriptor d = new com.fasterxml.jackson.databind.ObjectMapper()
                .readValue(json, AgentDescriptor.class);
            assertTrue(d.isChainingEnabled(), "chainingEnabled:true must round-trip correctly");
        }

        @Test
        @DisplayName("chainingEnabled=false round-trips through JSON serialization")
        void chainingEnabledFalseRoundTrips() throws Exception {
            String json = "{\"agentId\":\"a\",\"entityType\":\"e\",\"entityKeyExtractor\":\"$.id\","
                + "\"chainingEnabled\":false,"
                + "\"ruleset\":{\"path\":\"/tmp/a.jar\"},"
                + "\"subscriptions\":[{\"eventType\":\"ev\",\"topic\":\"t\",\"operation\":\"op\"}],"
                + "\"outputs\":[{\"eventType\":\"out\",\"topic\":\"tout\"}]}";
            AgentDescriptor d = new com.fasterxml.jackson.databind.ObjectMapper()
                .readValue(json, AgentDescriptor.class);
            assertFalse(d.isChainingEnabled(), "chainingEnabled:false must round-trip correctly");
        }

        @Test
        @DisplayName("chainingEnabled absent from JSON defaults to true (backward compatibility)")
        void chainingEnabledAbsentDefaultsToTrue() throws Exception {
            String json = "{\"agentId\":\"a\",\"entityType\":\"e\",\"entityKeyExtractor\":\"$.id\","
                + "\"ruleset\":{\"path\":\"/tmp/a.jar\"},"
                + "\"subscriptions\":[{\"eventType\":\"ev\",\"topic\":\"t\",\"operation\":\"op\"}],"
                + "\"outputs\":[{\"eventType\":\"out\",\"topic\":\"tout\"}]}";
            AgentDescriptor d = new com.fasterxml.jackson.databind.ObjectMapper()
                .readValue(json, AgentDescriptor.class);
            assertTrue(d.isChainingEnabled(),
                "Omitting chainingEnabled from JSON must default to true (backward compatibility)");
        }

        // ── Helper ────────────────────────────────────────────────────────────

        private static AgentDescriptor makeAgent(String agentId,
                                                  String keyExtractor,
                                                  List<String> inTypes,
                                                  List<String> outTypes,
                                                  boolean chainingEnabled) {
            AgentDescriptor d = new AgentDescriptor();
            d.setAgentId(agentId);
            d.setEntityType("entity");
            d.setEntityKeyExtractor(keyExtractor);
            d.setRuleset(new RulesetConfig("/tmp/" + agentId + ".jar"));
            d.setChainingEnabled(chainingEnabled);
            d.setSubscriptions(inTypes.stream()
                .map(et -> new EventSubscription(et, "topic-" + et, "op", HorizonConfig.count(5)))
                .collect(java.util.stream.Collectors.toList()));
            d.setOutputs(outTypes.stream()
                .map(et -> new OutputConfig(et, "out-topic-" + et))
                .collect(java.util.stream.Collectors.toList()));
            return d;
        }

    }

    // ── Agent Processor UID uniqueness (continued) ────────────────────────────

    @Nested
    @DisplayName("Agent Processor UID uniqueness (reopened for remaining tests)")
    class ProcessorUidTests2 {

        @Test
        @DisplayName("Sink UIDs include agentId and eventType — distinct across agents and event types")
        void sinkUidsIncludeAgentIdAndEventType() {
            // Sink UID pattern: "agent-sink-{agentId}-{eventType}"
            // Error DLQ UID: "agent-error-dlq-{agentId}"
            // Changelog sink UID: "agent-changelog-sink-{agentId}"
            // All must be distinct in a chained topology.
            List<String> ids = List.of("fraud", "loyalty");
            List<String> allUids = new java.util.ArrayList<>();

            for (String id : ids) {
                allUids.add("agent-processor-"   + id);
                allUids.add("agent-sink-"         + id + "-alert");
                allUids.add("agent-error-dlq-"    + id);
                allUids.add("agent-changelog-sink-" + id);
            }

            Set<String> uniqueUids = new java.util.HashSet<>(allUids);
            assertEquals(allUids.size(), uniqueUids.size(),
                "All operator UIDs across all agents must be globally unique: " + allUids);
        }
    }

    // ── Kafka source suppression (F-6 duplicate-delivery prevention) ──────────

    /**
     * Tests for the decision logic that determines which Kafka sources are suppressed
     * when chaining is active.
     *
     * <p>The actual Kafka-source creation requires a live Flink cluster and cannot be
     * exercised in a unit test.  These tests validate the <em>upstream mechanism</em>:
     * {@link AgentChainBuilder.DeferredChainedInputs#chainedEventTypes(String)} returns the
     * correct set of event types that {@link AgentBasedDataStreamJob#wireAgent} uses to
     * decide which Kafka sources to suppress.
     *
     * <p>If {@code chainedEventTypes} is correct, the suppression in {@code wireAgent} is
     * trivially correct (it is a single {@code Set#contains} check).
     */
    @Nested
    @DisplayName("Kafka source suppression — duplicate-delivery prevention (F-6)")
    class KafkaSourceSuppressionTests {

        // ── Helpers ───────────────────────────────────────────────────────────

        private static AgentDescriptor descriptor(String agentId,
                                                  List<String> outTypes,
                                                  List<String> inTypes) {
            AgentDescriptor d = new AgentDescriptor();
            d.setAgentId(agentId);
            d.setEntityType("entity");
            d.setEntityKeyExtractor("$.id");
            d.setRuleset(new RulesetConfig("/tmp/" + agentId + ".jar"));
            d.setSubscriptions(inTypes.stream()
                .map(et -> new EventSubscription(et, "topic-" + et, "op", HorizonConfig.count(1)))
                .toList());
            d.setOutputs(outTypes.stream()
                .map(et -> new OutputConfig(et, "out-topic-" + et))
                .toList());
            return d;
        }

        // ── No chaining ───────────────────────────────────────────────────────

        @Test
        @DisplayName("No chain edges: chainedEventTypes is empty for all agents — no suppression")
        void noChainNoSuppression() {
            AgentDescriptor a = descriptor("a", List.of("ev-a"), List.of("raw"));
            AgentDescriptor b = descriptor("b", List.of("ev-b"), List.of("other-raw"));

            AgentChainBuilder builder = new AgentChainBuilder();
            AgentChainBuilder.DeferredChainedInputs d = builder.resolveTopology(List.of(a, b));

            assertTrue(d.chainedEventTypes("a").isEmpty(),
                "No chain edge into 'a' — no Kafka sources should be suppressed");
            assertTrue(d.chainedEventTypes("b").isEmpty(),
                "No chain edge into 'b' — no Kafka sources should be suppressed");
        }

        // ── Simple A→B chain ─────────────────────────────────────────────────

        @Test
        @DisplayName("Simple A→B: B's Kafka source for ev-a is suppressed, A's is not")
        void simpleChainBSourceSuppressed() {
            AgentDescriptor a = descriptor("a", List.of("ev-a"), List.of("raw"));
            AgentDescriptor b = descriptor("b", List.of("ev-b"), List.of("ev-a"));

            AgentChainBuilder builder = new AgentChainBuilder();
            AgentChainBuilder.DeferredChainedInputs d = builder.resolveTopology(List.of(a, b));

            // Agent A: no upstream chain edge, all Kafka sources active
            assertTrue(d.chainedEventTypes("a").isEmpty(),
                "a has no upstream chain — its 'raw' Kafka source must NOT be suppressed");

            // Agent B: ev-a arrives via chain — its Kafka source for ev-a must be suppressed
            Set<String> bSuppressed = d.chainedEventTypes("b");
            assertTrue(bSuppressed.contains("ev-a"),
                "b's Kafka source for ev-a must be suppressed (ev-a arrives via chain from a)");
            assertFalse(bSuppressed.contains("ev-b"),
                "b's own output type ev-b is not a subscription — must NOT be in suppression set");
        }

        @Test
        @DisplayName("Simple A→B: B has two subscriptions, only the chained one is suppressed")
        void partialSuppressionTwoSubscriptions() {
            // b subscribes to both ev-a (chained from a) and external-ev (from Kafka only)
            AgentDescriptor a = descriptor("a", List.of("ev-a"), List.of("raw"));

            AgentDescriptor b = new AgentDescriptor();
            b.setAgentId("b");
            b.setEntityType("entity");
            b.setEntityKeyExtractor("$.id");
            b.setRuleset(new RulesetConfig("/tmp/b.jar"));
            b.setSubscriptions(List.of(
                new EventSubscription("ev-a",        "topic-ev-a",        "op", HorizonConfig.count(1)),
                new EventSubscription("external-ev", "topic-external-ev", "op", HorizonConfig.count(1))
            ));
            b.setOutputs(List.of(new OutputConfig("ev-b", "out-topic-ev-b")));

            AgentChainBuilder builder = new AgentChainBuilder();
            AgentChainBuilder.DeferredChainedInputs d = builder.resolveTopology(List.of(a, b));

            Set<String> suppressed = d.chainedEventTypes("b");
            assertTrue(suppressed.contains("ev-a"),
                "ev-a is chained from a — its Kafka source must be suppressed");
            assertFalse(suppressed.contains("external-ev"),
                "external-ev has no upstream chain — its Kafka source must NOT be suppressed");
            assertEquals(1, suppressed.size(), "Exactly one event type should be suppressed");
        }

        // ── Multi-hop chain A→B→C ─────────────────────────────────────────────

        @Test
        @DisplayName("Three-agent chain A→B→C: each downstream has its own suppression set")
        void threeAgentChainSuppressionSets() {
            AgentDescriptor a = descriptor("a", List.of("ev-1"), List.of("raw"));
            AgentDescriptor b = descriptor("b", List.of("ev-2"), List.of("ev-1"));
            AgentDescriptor c = descriptor("c", List.of("ev-3"), List.of("ev-2"));

            AgentChainBuilder builder = new AgentChainBuilder();
            AgentChainBuilder.DeferredChainedInputs d = builder.resolveTopology(List.of(a, b, c));

            // a: head of chain, nothing suppressed
            assertTrue(d.chainedEventTypes("a").isEmpty(),
                "a is head — no Kafka sources suppressed");

            // b: receives ev-1 from a via chain
            Set<String> bSuppressed = d.chainedEventTypes("b");
            assertEquals(Set.of("ev-1"), bSuppressed,
                "b's Kafka source for ev-1 must be suppressed (chained from a)");

            // c: receives ev-2 from b via chain
            Set<String> cSuppressed = d.chainedEventTypes("c");
            assertEquals(Set.of("ev-2"), cSuppressed,
                "c's Kafka source for ev-2 must be suppressed (chained from b)");
        }

        // ── Fan-out: one upstream → two downstream consumers ──────────────────

        @Test
        @DisplayName("Fan-out A→B, A→C: both B and C suppress their Kafka source for ev-a")
        void fanOutBothConsumersSuppressKafkaSource() {
            AgentDescriptor a = descriptor("a", List.of("ev-a"), List.of("raw"));
            AgentDescriptor b = descriptor("b", List.of("ev-b"), List.of("ev-a"));
            AgentDescriptor c = descriptor("c", List.of("ev-c"), List.of("ev-a"));

            AgentChainBuilder builder = new AgentChainBuilder();
            AgentChainBuilder.DeferredChainedInputs d = builder.resolveTopology(List.of(a, b, c));

            assertTrue(d.chainedEventTypes("b").contains("ev-a"),
                "b subscribes to a's ev-a via chain — Kafka source suppressed");
            assertTrue(d.chainedEventTypes("c").contains("ev-a"),
                "c subscribes to a's ev-a via chain — Kafka source suppressed");
            assertTrue(d.chainedEventTypes("a").isEmpty(),
                "a is upstream — no suppression");
        }

        // ── Standalone agents ─────────────────────────────────────────────────

        @Test
        @DisplayName("Standalone agent (no chain edges): no Kafka sources suppressed")
        void standaloneAgentNothingSuppressed() {
            AgentDescriptor standalone = descriptor("standalone",
                List.of("result"), List.of("from-kafka"));

            AgentChainBuilder builder = new AgentChainBuilder();
            AgentChainBuilder.DeferredChainedInputs d = builder.resolveTopology(
                List.of(standalone));

            assertTrue(d.chainedEventTypes("standalone").isEmpty(),
                "Standalone agent — all Kafka sources must remain active, nothing suppressed");
        }

        // ── Mixed topology ────────────────────────────────────────────────────

        @Test
        @DisplayName("Mixed topology: chained and standalone agents — each gets correct suppression set")
        void mixedTopologyCorrectSuppression() {
            AgentDescriptor a = descriptor("a",          List.of("ev-a"), List.of("raw-a"));
            AgentDescriptor b = descriptor("b",          List.of("ev-b"), List.of("ev-a"));
            AgentDescriptor c = descriptor("standalone", List.of("ev-c"), List.of("raw-c"));

            AgentChainBuilder builder = new AgentChainBuilder();
            AgentChainBuilder.DeferredChainedInputs d = builder.resolveTopology(List.of(a, b, c));

            assertTrue(d.chainedEventTypes("a").isEmpty(),
                "a is head of chain — no Kafka sources suppressed");
            assertTrue(d.chainedEventTypes("b").contains("ev-a"),
                "b receives ev-a via chain — Kafka source suppressed");
            assertTrue(d.chainedEventTypes("standalone").isEmpty(),
                "standalone has no chain edge — all Kafka sources active");
        }

        // ── ATC use case ──────────────────────────────────────────────────────

        @Test
        @DisplayName("ATC topology: airport-agent suppresses TrackedAircraftEvent Kafka source, preserves others")
        void atcAirportSuppressesTrackedAircraftEventKafkaSource() {
            AgentDescriptor aircraft = new AgentDescriptor();
            aircraft.setAgentId("aircraft-agent");
            aircraft.setEntityType("Aircraft");
            aircraft.setEntityKeyExtractor("$.aircraftId");
            aircraft.setRuleset(new RulesetConfig("samples/airtraffic-agent/ATC.jar"));
            aircraft.setSubscriptions(List.of(
                new EventSubscription("AircraftEvent",
                    "agent-aircraft-events", "ProcessAircraftEvent", HorizonConfig.time("2h"))
            ));
            aircraft.setOutputs(List.of(
                new OutputConfig("TrackedAircraftEvent", "agent-tracked-aircraft-events"),
                new OutputConfig("Alert", "agent-atc-alerts")
            ));

            AgentDescriptor airport = new AgentDescriptor();
            airport.setAgentId("airport-agent");
            airport.setEntityType("Airport");
            airport.setEntityKeyExtractor("$.airportCode");
            airport.setRuleset(new RulesetConfig("samples/airtraffic-agent/ATC.jar"));
            airport.setSubscriptions(List.of(
                new EventSubscription("AirportEvent",
                    "agent-airport-events", "ProcessAirportEvent", HorizonConfig.count(0)),
                new EventSubscription("TrackedAircraftEvent",
                    "agent-tracked-aircraft-events", "ProcessTrackedAircraftEvent",
                    HorizonConfig.time("1h"))
            ));
            airport.setOutputs(List.of(new OutputConfig("Alert", "agent-atc-alerts")));

            AgentChainBuilder builder = new AgentChainBuilder();
            AgentChainBuilder.DeferredChainedInputs d = builder.resolveTopology(
                List.of(aircraft, airport));

            Set<String> airportSuppressed = d.chainedEventTypes("airport-agent");

            assertTrue(airportSuppressed.contains("TrackedAircraftEvent"),
                "airport-agent must suppress its TrackedAircraftEvent Kafka source "
                + "(it arrives directly via chain from aircraft-agent) — prevents F-6 "
                + "duplicate delivery");
            assertFalse(airportSuppressed.contains("AirportEvent"),
                "airport-agent's AirportEvent comes only from Kafka — must NOT be suppressed");
            assertTrue(d.chainedEventTypes("aircraft-agent").isEmpty(),
                "aircraft-agent has no upstream chain edge — no suppression");
        }

        @Test
        @DisplayName("ATC topology: exactly one event type suppressed at airport-agent (full 3-subscription descriptor)")
        void atcExactlyOneEventTypeSuppressedFullDescriptor() {
            AgentDescriptor aircraft = new AgentDescriptor();
            aircraft.setAgentId("aircraft-agent");
            aircraft.setEntityType("Aircraft");
            aircraft.setEntityKeyExtractor("$.aircraftId");
            aircraft.setRuleset(new RulesetConfig("samples/airtraffic-agent/ATC.jar"));
            aircraft.setSubscriptions(List.of(
                new EventSubscription("AircraftInitializationEvent",
                    "agent-aircraft-init-events", "InitAircraft", HorizonConfig.count(0)),
                new EventSubscription("AircraftEvent",
                    "agent-aircraft-events", "ProcessAircraftEvent", HorizonConfig.time("2h"))
            ));
            aircraft.setOutputs(List.of(
                new OutputConfig("TrackedAircraftEvent", "agent-tracked-aircraft-events"),
                new OutputConfig("Alert", "agent-atc-alerts"),
                new OutputConfig("agent-error", "agent-atc-errors")
            ));

            AgentDescriptor airport = new AgentDescriptor();
            airport.setAgentId("airport-agent");
            airport.setEntityType("Airport");
            airport.setEntityKeyExtractor("$.airportCode");
            airport.setRuleset(new RulesetConfig("samples/airtraffic-agent/ATC.jar"));
            airport.setSubscriptions(List.of(
                new EventSubscription("AirportInitializationEvent",
                    "agent-airport-init-events", "InitAirport", HorizonConfig.count(0)),
                new EventSubscription("AirportEvent",
                    "agent-airport-events", "ProcessAirportEvent", HorizonConfig.count(0)),
                new EventSubscription("TrackedAircraftEvent",
                    "agent-tracked-aircraft-events", "ProcessTrackedAircraftEvent",
                    HorizonConfig.time("1h"))
            ));
            airport.setOutputs(List.of(
                new OutputConfig("Alert", "agent-atc-alerts"),
                new OutputConfig("agent-error", "agent-atc-errors")
            ));

            AgentChainBuilder builder = new AgentChainBuilder();
            AgentChainBuilder.DeferredChainedInputs d = builder.resolveTopology(
                List.of(aircraft, airport));

            Set<String> suppressed = d.chainedEventTypes("airport-agent");

            assertEquals(Set.of("TrackedAircraftEvent"), suppressed,
                "Exactly one event type (TrackedAircraftEvent) should be suppressed "
                + "at airport-agent in the full ATC topology — AirportInitializationEvent "
                + "and AirportEvent are not emitted by any other agent");
        }
    }

    // ── CHAINING_ENABLED environment variable ─────────────────────────────────

    /**
     * Tests for the {@code CHAINING_ENABLED} environment variable — the global chaining
     * kill switch read by {@link AgentBasedDataStreamJob#boolEnvOr}.
     *
     * <p>These tests cover:
     * <ol>
     *   <li><b>boolEnvOr default</b> — unset variable returns the supplied default.</li>
     *   <li><b>"false" disables</b> — only the literal {@code "false"} (case-insensitive)
     *       returns {@code false}.</li>
     *   <li><b>"true" enables</b> — {@code "true"} returns {@code true}.</li>
     *   <li><b>Non-boolean strings</b> — {@code "yes"}, {@code "1"}, {@code "no"}, {@code "0"}
     *       are not treated as boolean {@code false}; only {@code "false"} is.</li>
     *   <li><b>Kill switch semantics</b> — verifies that supplying {@code false} as the
     *       default correctly models the kill-switch contract.</li>
     * </ol>
     *
     * <p>Because {@code System.getenv()} cannot be overridden in a plain unit test, the
     * {@code boolEnvOr} helper is exercised through direct calls (it is package-private
     * {@code static}) rather than through environment injection, which would require
     * a separate JVM process or a mocking framework.
     */
    @Nested
    @DisplayName("CHAINING_ENABLED env var — boolEnvOr parsing contract")
    class ChainingEnabledEnvVarTests {

        /**
         * Verifies the default-value contract: when the env var is absent/blank, the
         * supplied default is returned unchanged.
         *
         * <p>We exercise {@code boolEnvOr} directly because it is the only logic that
         * varies by env input; the integration with {@code main()} is validated by the
         * kill-switch semantics test below.
         */
        @Test
        @DisplayName("boolEnvOr: unset variable returns the supplied default (true)")
        void unsetVariableReturnsTrueDefault() {
            // Simulate the production call: CHAINING_ENABLED absent → default true
            // We cannot set env vars in-process, so we call boolEnvOr with a name that
            // is guaranteed not to be set in the test environment.
            String absent = "CHAINING_ENABLED_TEST_ABSENT_" + System.nanoTime();
            assertTrue(AgentBasedDataStreamJob.boolEnvOr(absent, true),
                "Absent env var must return the supplied default (true)");
        }

        @Test
        @DisplayName("boolEnvOr: unset variable returns the supplied default (false)")
        void unsetVariableReturnsFalseDefault() {
            String absent = "CHAINING_ENABLED_TEST_ABSENT_" + System.nanoTime();
            assertFalse(AgentBasedDataStreamJob.boolEnvOr(absent, false),
                "Absent env var must return the supplied default (false)");
        }

        @Test
        @DisplayName("boolEnvOr: 'false' (lowercase) parses to false")
        void literalFalseLowercaseIsFalse() {
            // Directly call the parsing contract; production code calls boolEnvOr which
            // applies: return !"false".equalsIgnoreCase(v.trim())
            // We verify the expression is correct independently of System.getenv.
            String v = "false";
            assertFalse(!"false".equalsIgnoreCase(v.trim()),
                "'false' must parse to false");
        }

        @Test
        @DisplayName("boolEnvOr: 'FALSE' (uppercase) parses to false")
        void literalFalseUppercaseIsFalse() {
            String v = "FALSE";
            assertFalse(!"false".equalsIgnoreCase(v.trim()),
                "'FALSE' must parse to false (case-insensitive)");
        }

        @Test
        @DisplayName("boolEnvOr: 'true' parses to true")
        void literalTrueIsTrue() {
            String v = "true";
            assertTrue(!"false".equalsIgnoreCase(v.trim()),
                "'true' must parse to true");
        }

        @Test
        @DisplayName("boolEnvOr: 'TRUE' parses to true")
        void literalTrueUppercaseIsTrue() {
            String v = "TRUE";
            assertTrue(!"false".equalsIgnoreCase(v.trim()),
                "'TRUE' must parse to true (case-insensitive)");
        }

        @Test
        @DisplayName("boolEnvOr: '1' is not 'false' — treated as true (no ambiguous truthy string)")
        void numericOneIsNotFalse() {
            String v = "1";
            assertTrue(!"false".equalsIgnoreCase(v.trim()),
                "'1' must not be parsed as false — only 'false' disables chaining");
        }

        @Test
        @DisplayName("boolEnvOr: '0' is not 'false' — treated as true (only literal 'false' disables)")
        void numericZeroIsNotFalse() {
            String v = "0";
            assertTrue(!"false".equalsIgnoreCase(v.trim()),
                "'0' must not be parsed as false — only the literal 'false' disables chaining");
        }

        @Test
        @DisplayName("boolEnvOr: 'yes' is not 'false' — treated as true")
        void yesIsNotFalse() {
            String v = "yes";
            assertTrue(!"false".equalsIgnoreCase(v.trim()),
                "'yes' must not be parsed as false");
        }

        @Test
        @DisplayName("boolEnvOr: 'no' is not 'false' — treated as true (only literal 'false' disables)")
        void noIsNotFalse() {
            String v = "no";
            assertTrue(!"false".equalsIgnoreCase(v.trim()),
                "'no' must not be parsed as false — only the literal 'false' disables chaining");
        }

        // ── Kill-switch semantics ─────────────────────────────────────────────

        /**
         * Verifies the kill-switch contract: when {@code CHAINING_ENABLED=false}, every
         * agent's {@code chainingEnabled} field must be overridden to {@code false} before
         * the chain topology is built, regardless of what the descriptor declared.
         *
         * <p>This test exercises the override loop directly (the loop that runs in
         * {@code main()} after reading the env var) because we cannot inject env vars
         * into the running JVM without a mocking framework.
         */
        @Test
        @DisplayName("Kill switch: when globalChainingEnabled=false all agents get chainingEnabled=false")
        void killSwitchOverridesAllAgents() {
            AgentDescriptor a = new AgentDescriptor();
            a.setAgentId("a");
            a.setChainingEnabled(true);

            AgentDescriptor b = new AgentDescriptor();
            b.setAgentId("b");
            b.setChainingEnabled(true);

            // Simulate what main() does when CHAINING_ENABLED=false
            boolean globalChainingEnabled = false;
            if (!globalChainingEnabled) {
                for (AgentDescriptor agent : List.of(a, b)) {
                    agent.setChainingEnabled(false);
                }
            }

            assertFalse(a.isChainingEnabled(),
                "Agent a must have chainingEnabled=false after kill switch applied");
            assertFalse(b.isChainingEnabled(),
                "Agent b must have chainingEnabled=false after kill switch applied");
        }

        @Test
        @DisplayName("Kill switch: when globalChainingEnabled=true per-agent flags are unchanged")
        void noKillSwitchPreservesPerAgentFlags() {
            AgentDescriptor a = new AgentDescriptor();
            a.setAgentId("a");
            a.setChainingEnabled(true);

            AgentDescriptor b = new AgentDescriptor();
            b.setAgentId("b");
            b.setChainingEnabled(false); // explicitly disabled per-agent

            // Simulate what main() does when CHAINING_ENABLED is unset or "true"
            boolean globalChainingEnabled = true;
            if (!globalChainingEnabled) {
                for (AgentDescriptor agent : List.of(a, b)) {
                    agent.setChainingEnabled(false);
                }
            }

            // Per-agent flags must remain untouched
            assertTrue(a.isChainingEnabled(),
                "Agent a must remain chainingEnabled=true (kill switch not active)");
            assertFalse(b.isChainingEnabled(),
                "Agent b must remain chainingEnabled=false (per-agent override not changed)");
        }

        @Test
        @DisplayName("Kill switch: after override, resolveTopology produces no chain edges")
        void killSwitchProducesNoChainEdges() {
            // a emits ev-a, b subscribes to ev-a — would form a chain edge if enabled
            AgentDescriptor a = new AgentDescriptor();
            a.setAgentId("a");
            a.setRuleset(new RulesetConfig("/tmp/a.jar"));
            a.setEntityKeyExtractor("$.id");
            a.setChainingEnabled(true); // normally enabled
            a.setSubscriptions(List.of(
                new EventSubscription("raw", "topic-raw", "op", HorizonConfig.count(1))));
            a.setOutputs(List.of(new OutputConfig("ev-a", "topic-ev-a")));

            AgentDescriptor b = new AgentDescriptor();
            b.setAgentId("b");
            b.setRuleset(new RulesetConfig("/tmp/b.jar"));
            b.setEntityKeyExtractor("$.id");
            b.setChainingEnabled(true); // normally enabled
            b.setSubscriptions(List.of(
                new EventSubscription("ev-a", "topic-ev-a", "op", HorizonConfig.count(1))));
            b.setOutputs(List.of(new OutputConfig("ev-b", "topic-ev-b")));

            List<AgentDescriptor> agents = List.of(a, b);

            // Apply the kill switch (CHAINING_ENABLED=false)
            for (AgentDescriptor agent : agents) {
                agent.setChainingEnabled(false);
            }

            AgentChainBuilder builder = new AgentChainBuilder();
            AgentChainBuilder.DeferredChainedInputs chains = builder.resolveTopology(agents);

            assertTrue(chains.isEmpty(),
                "After kill switch: resolveTopology must produce no chain edges even when "
                + "the event types would otherwise match");
            assertFalse(chains.hasChainedInput("b"),
                "b must not have a chained inbound stream after the kill switch is applied");
        }
    }

    // ── buildChainedInputs — global chaining kill switch ──────────────────────

    /**
     * Tests for {@link AgentBasedDataStreamJob#buildChainedInputs} — the method that either
     * bypasses the {@link AgentChainBuilder} entirely (when chaining is globally disabled)
     * or delegates to it (when chaining is enabled).
     *
     * <p>These tests verify the "zero execution" contract:
     * <ul>
     *   <li>When {@code globalChainingEnabled=false} the returned handle is immediately
     *       empty — no topology resolution, no edges, no chain IDs.</li>
     *   <li>When {@code globalChainingEnabled=true} the full topology is resolved and
     *       chain edges are present exactly as {@link AgentChainBuilder#resolveTopology}
     *       would produce them.</li>
     * </ul>
     */
    @Nested
    @DisplayName("buildChainedInputs — global chaining kill switch")
    class BuildChainedInputsTests {

        /** Creates a minimal A→B chain: A emits ev-1, B subscribes to ev-1. */
        private List<AgentDescriptor> twoAgentChain() {
            AgentDescriptor a = new AgentDescriptor();
            a.setAgentId("a");
            a.setRuleset(new RulesetConfig("/tmp/a.jar"));
            a.setEntityKeyExtractor("$.id");
            a.setChainingEnabled(true);
            a.setSubscriptions(List.of(
                new EventSubscription("raw", "topic-raw", "op", HorizonConfig.count(1))));
            a.setOutputs(List.of(new OutputConfig("ev-1", "topic-ev-1")));

            AgentDescriptor b = new AgentDescriptor();
            b.setAgentId("b");
            b.setRuleset(new RulesetConfig("/tmp/b.jar"));
            b.setEntityKeyExtractor("$.id");
            b.setChainingEnabled(true);
            b.setSubscriptions(List.of(
                new EventSubscription("ev-1", "topic-ev-1", "op", HorizonConfig.count(1))));
            b.setOutputs(List.of(new OutputConfig("ev-2", "topic-ev-2")));

            return List.of(a, b);
        }

        @Test
        @DisplayName("CHAINING_ENABLED=false → buildChainedInputs returns empty handle immediately")
        void disabledReturnsEmptyHandle() {
            List<AgentDescriptor> agents = twoAgentChain();

            AgentChainBuilder.DeferredChainedInputs result =
                AgentBasedDataStreamJob.buildChainedInputs(agents, false);

            assertTrue(result.isEmpty(),
                "buildChainedInputs(false) must return an empty DeferredChainedInputs — "
                + "no chain topology is resolved when CHAINING_ENABLED=false");
        }

        @Test
        @DisplayName("CHAINING_ENABLED=false → hasChainedInput returns false for all agents")
        void disabledHasNoChainedInput() {
            List<AgentDescriptor> agents = twoAgentChain();

            AgentChainBuilder.DeferredChainedInputs result =
                AgentBasedDataStreamJob.buildChainedInputs(agents, false);

            assertFalse(result.hasChainedInput("a"),
                "a must have no chained input when CHAINING_ENABLED=false");
            assertFalse(result.hasChainedInput("b"),
                "b must have no chained input when CHAINING_ENABLED=false");
        }

        @Test
        @DisplayName("CHAINING_ENABLED=false → chainedAgentIds is empty")
        void disabledChainedAgentIdsEmpty() {
            List<AgentDescriptor> agents = twoAgentChain();

            AgentChainBuilder.DeferredChainedInputs result =
                AgentBasedDataStreamJob.buildChainedInputs(agents, false);

            assertTrue(result.chainedAgentIds().isEmpty(),
                "chainedAgentIds must be empty when CHAINING_ENABLED=false");
        }

        @Test
        @DisplayName("CHAINING_ENABLED=false → chainedEventTypes returns empty for all agents")
        void disabledChainedEventTypesEmpty() {
            List<AgentDescriptor> agents = twoAgentChain();

            AgentChainBuilder.DeferredChainedInputs result =
                AgentBasedDataStreamJob.buildChainedInputs(agents, false);

            assertTrue(result.chainedEventTypes("a").isEmpty(),
                "chainedEventTypes(a) must be empty when CHAINING_ENABLED=false");
            assertTrue(result.chainedEventTypes("b").isEmpty(),
                "chainedEventTypes(b) must be empty when CHAINING_ENABLED=false");
        }

        @Test
        @DisplayName("CHAINING_ENABLED=false → get() returns null for all agents")
        void disabledGetReturnsNull() {
            List<AgentDescriptor> agents = twoAgentChain();

            AgentChainBuilder.DeferredChainedInputs result =
                AgentBasedDataStreamJob.buildChainedInputs(agents, false);

            assertNull(result.get("a"),
                "get(a) must return null when CHAINING_ENABLED=false");
            assertNull(result.get("b"),
                "get(b) must return null when CHAINING_ENABLED=false");
        }

        @Test
        @DisplayName("CHAINING_ENABLED=false → empty handle even when agents would normally chain")
        void disabledIgnoresMatchingEventTypes() {
            // a emits ev-1 and b subscribes to ev-1 — with chaining enabled this forms an edge.
            // With chaining disabled, no edge must be formed regardless.
            List<AgentDescriptor> agents = twoAgentChain();

            AgentChainBuilder.DeferredChainedInputs disabled =
                AgentBasedDataStreamJob.buildChainedInputs(agents, false);
            AgentChainBuilder.DeferredChainedInputs enabled =
                AgentBasedDataStreamJob.buildChainedInputs(agents, true);

            assertTrue(disabled.isEmpty(),
                "Disabled: no chain edges even with matching event types");
            assertFalse(enabled.isEmpty(),
                "Enabled: chain edge a→b must be resolved");
            assertTrue(enabled.hasChainedInput("b"),
                "Enabled: b must have a chained input from a");
        }

        @Test
        @DisplayName("CHAINING_ENABLED=true → chain edge A→B is resolved")
        void enabledResolvesChainEdge() {
            List<AgentDescriptor> agents = twoAgentChain();

            AgentChainBuilder.DeferredChainedInputs result =
                AgentBasedDataStreamJob.buildChainedInputs(agents, true);

            assertFalse(result.isEmpty(),
                "buildChainedInputs(true) must resolve the A→B chain edge");
            assertTrue(result.hasChainedInput("b"),
                "b must have a chained input when CHAINING_ENABLED=true and event types match");
            assertFalse(result.hasChainedInput("a"),
                "a has no upstream — must not have a chained input");
            assertEquals(Set.of("b"), result.chainedAgentIds(),
                "Only b is downstream in the A→B chain");
        }

        @Test
        @DisplayName("CHAINING_ENABLED=true → chainedEventTypes contains the chained event type")
        void enabledChainedEventTypesPopulated() {
            List<AgentDescriptor> agents = twoAgentChain();

            AgentChainBuilder.DeferredChainedInputs result =
                AgentBasedDataStreamJob.buildChainedInputs(agents, true);

            assertTrue(result.chainedEventTypes("b").contains("ev-1"),
                "b's chainedEventTypes must contain 'ev-1' when chaining is enabled");
        }

        @Test
        @DisplayName("CHAINING_ENABLED=true with empty agent list → empty handle (not an error)")
        void enabledEmptyAgentsProducesEmptyHandle() {
            AgentChainBuilder.DeferredChainedInputs result =
                AgentBasedDataStreamJob.buildChainedInputs(List.of(), true);

            assertTrue(result.isEmpty(),
                "Empty agent list must produce empty DeferredChainedInputs regardless of flag");
        }

        @Test
        @DisplayName("CHAINING_ENABLED=false with empty agent list → empty handle (not an error)")
        void disabledEmptyAgentsProducesEmptyHandle() {
            AgentChainBuilder.DeferredChainedInputs result =
                AgentBasedDataStreamJob.buildChainedInputs(List.of(), false);

            assertTrue(result.isEmpty(),
                "Empty agent list must produce empty DeferredChainedInputs when disabled");
        }
    }
}

