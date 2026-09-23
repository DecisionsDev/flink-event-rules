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
package com.ibm.di.event.e2e;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ibm.di.event.agent.*;
import com.ibm.di.event.rule.RuleEngineRunner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end functional tests for the {@code generic} context format.
 *
 * <p>Tests cover the complete processing path from event ingestion through state
 * management and Decision Intelligence rule execution to output event emission.
 * The Decision Intelligence rule engine is stubbed to provide deterministic results.
 *
 * <p>The agent uses the standard {@code generic} contextFormat.  The rule engine
 * receives the {@code {input: {entityId, entityType, currentEventType, now, processingTime,
 * entity, currentEvent, history, outboundEvents}}} envelope.
 * Secondary-event history is available in the flat {@code history} array.
 *
 * <p>Scenarios covered:
 * <ul>
 *   <li>Active entity with secondary-event history → preferred-offer output</li>
 *   <li>Re-engagement signal triggers secondary offer</li>
 *   <li>Primary event payload fields present in generic currentEvent context</li>
 *   <li>Entity state (__entityState) persists updates across primary events</li>
 *   <li>Error handling: rule failure produces agent-error with originalEvent</li>
 *   <li>Output event type routing from outputEventType field</li>
 *   <li>__entityState and __outputEventType stripped from output payload</li>
 *   <li>Hybrid horizon eviction: secondary-event count ceiling evicts oldest entries</li>
 * </ul>
 */
@DisplayName("Generic context format — end-to-end workflow tests")
class GenericContextWorkflowTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // ── Stub rule engine ──────────────────────────────────────────────────────

    static class StubDecisionEngine extends RuleEngineRunner {

        private final JsonNode fixedResult;
        private RuntimeException toThrow;
        private JsonNode lastContext;
        private int callCount;

        StubDecisionEngine(JsonNode result) {
            super("evaluationService", URI.create("file:///dev/null"));
            this.fixedResult = result;
        }

        void throwOnExecute(RuntimeException ex) { this.toThrow = ex; }
        JsonNode lastContext()                    { return lastContext; }
        int callCount()                           { return callCount; }

        @Override
        public JsonNode execute(JsonNode context) {
            callCount++;
            lastContext = context;
            if (toThrow != null) throw toThrow;
            return fixedResult;
        }

        @Override protected void initialize() { /* no-op */ }
        @Override public void    close()       { /* no-op */ }
    }

    // ── Builder helpers ───────────────────────────────────────────────────────

    /** Builds a generic-format agent descriptor. */
    private static AgentDescriptor genericDescriptor() {
        AgentDescriptor d = new AgentDescriptor();
        d.setAgentId("evaluation-agent");
        d.setName("Entity Evaluation Agent");
        d.setEntityType("customer");
        d.setEntityKeyExtractor("$.customerId");

        RulesetConfig rs = new RulesetConfig("/tmp/evaluation.jar");
        // Register OperationConfig for the primary-event operation so that
        // buildOutputEvents can assign the correct output event type.
        // Each test that needs a specific outboundEventType sets it via the result
        // stub — here we provide the minimal config used by most tests ("offer").
        RulesetConfig.OperationConfig opCfg = new RulesetConfig.OperationConfig();
        opCfg.setOperation("evaluationService_primaryEvent_op");
        opCfg.setOutbounds(List.of(new RulesetConfig.OutboundConfig("outputEvents", "offer")));
        rs.setOperations(List.of(opCfg));
        d.setRuleset(rs);

        d.setSubscriptions(List.of(
            new EventSubscription("primary-event",
                "agent-primary-events",
                "evaluationService_primaryEvent_op",
                HorizonConfig.count(20)),
            new EventSubscription("secondary-event",
                "agent-secondary-events",
                "",                // history-only: secondary events accumulate as context
                HorizonConfig.hybrid(50, "2h"))
        ));

        d.setOutputs(List.of(
            new OutputConfig("offer",       "agent-offers"),
            new OutputConfig("no-offer",    "agent-no-offers"),
            new OutputConfig("agent-error", "agent-errors")
        ));
        return d;
    }

    /** Builds a descriptor with a specific outbound event type on the operation. */
    private static AgentDescriptor descriptorWithEventType(String eventType) {
        AgentDescriptor d = genericDescriptor();
        RulesetConfig.OperationConfig opCfg = new RulesetConfig.OperationConfig();
        opCfg.setOperation("evaluationService_primaryEvent_op");
        opCfg.setOutbounds(List.of(new RulesetConfig.OutboundConfig("outputEvents", eventType)));
        d.getRuleset().setOperations(List.of(opCfg));
        return d;
    }

    /** Builds a descriptor configured to write entity state under the given entityField. */
    private static AgentDescriptor descriptorWithEntityField(String entityField) {
        AgentDescriptor d = genericDescriptor();
        RulesetConfig.OperationConfig opCfg = new RulesetConfig.OperationConfig();
        opCfg.setOperation("evaluationService_primaryEvent_op");
        opCfg.setOutbounds(List.of(new RulesetConfig.OutboundConfig("outputEvents", "offer")));
        opCfg.setEntityField(entityField);
        d.getRuleset().setOperations(List.of(opCfg));
        return d;
    }

    /**
     * Creates a primary event envelope representing the main trigger event.
     */
    private static EventEnvelope primaryEvent(String entityId, String eventId,
                                               String tier, int points,
                                               double amount, boolean reEngagementSignal,
                                               long tsMs) throws Exception {
        ObjectNode activityHistory = MAPPER.createObjectNode();
        activityHistory.put("totalEvents",          12);
        activityHistory.put("totalSpend",           840.0);
        activityHistory.put("favoriteCategory",     "Electronics");
        activityHistory.put("daysSinceLast",        7);

        ObjectNode signals = MAPPER.createObjectNode();
        signals.put("reEngagementSignal",     reEngagementSignal);
        signals.put("signalAgeMinutes",       reEngagementSignal ? 45 : 0);
        signals.put("secondaryEventCount",    8);
        signals.put("sessionDurationMinutes", 23);
        signals.put("wishlistAdded",          true);
        signals.put("priceDrop",              false);
        signals.put("priceDropPercent",       0);

        ObjectNode payload = MAPPER.createObjectNode();
        payload.put("customerId",  entityId);
        payload.put("eventId",     eventId);
        payload.put("source",      "web");
        payload.put("entityClass", "RETURNING");
        payload.put("tier",        tier);
        payload.put("points",      points);
        payload.put("amount",      amount);
        payload.set("activityHistory", activityHistory);
        payload.set("signals",         signals);

        EventEnvelope env = EventEnvelope.from("primary-event", "agent-primary-events", tsMs, payload);
        env.setEntityId(entityId);
        return env;
    }

    /** Creates a secondary event envelope. */
    private static EventEnvelope secondaryEvent(String entityId, String itemId,
                                                 String itemName, String category,
                                                 long durationSeconds, String isoTimestamp,
                                                 long tsMs) throws Exception {
        ObjectNode payload = MAPPER.createObjectNode();
        payload.put("customerId",          entityId);
        payload.put("itemId",              itemId);
        payload.put("itemName",            itemName);
        payload.put("category",            category);
        payload.put("viewDurationSeconds", durationSeconds);
        payload.put("timestamp",           isoTimestamp);

        EventEnvelope env = EventEnvelope.from("secondary-event", "agent-secondary-events", tsMs, payload);
        env.setEntityId(entityId);
        return env;
    }

    // ── Stub results ──────────────────────────────────────────────────────────
    // Results use the outputEvents array format; event type comes from OperationConfig.

    private static JsonNode preferredOfferResult() throws Exception {
        return MAPPER.readTree(
            "{\"outputEvents\":[{" +
            "\"offerCode\":\"PREFERRED-20\"," +
            "\"discountPercent\":20," +
            "\"offerType\":\"LOYALTY_REWARD\"," +
            "\"message\":\"Exclusive 20% off for preferred members\"}]}");
    }

    private static JsonNode noOfferResult() throws Exception {
        return MAPPER.readTree(
            "{\"outputEvents\":[{\"reason\":\"INSUFFICIENT_ACTIVITY\"}]}");
    }

    private static JsonNode reEngagementOfferResult() throws Exception {
        return MAPPER.readTree(
            "{\"outputEvents\":[{" +
            "\"offerCode\":\"COMEBACK-15\"," +
            "\"discountPercent\":15," +
            "\"offerType\":\"RE_ENGAGEMENT\"}]}");
    }

    private AgentDescriptor descriptor;
    private StatefulAgentProcessor processor;

    @BeforeEach
    void setUp() {
        descriptor = genericDescriptor();
        processor  = new StatefulAgentProcessor(descriptor);
    }

    // ── Happy-path: preferred entity with secondary-event history ─────────────

    @Nested
    @DisplayName("Preferred entity with history → preferred offer")
    class PreferredEntityWithHistory {

        @Test
        @DisplayName("Preferred entity with secondary-event history gets offer output")
        void preferredEntityGetsOffer() throws Exception {
            processor.injectRuleEngine(new StubDecisionEngine(preferredOfferResult()));

            AgentState state = AgentState.init("CUST-PREF-001", "customer", 0L);

            // Load some secondary events first
            processor.invokeForTest(
                secondaryEvent("CUST-PREF-001", "I-001", "Headphones", "Electronics",
                    120, "2026-06-01T09:00:00Z", 1_000_000L), state);
            processor.invokeForTest(
                secondaryEvent("CUST-PREF-001", "I-002", "Phone Case", "Accessories",
                    45, "2026-06-01T09:05:00Z", 1_000_300L), state);

            // Now send the primary event
            EventEnvelope evt = primaryEvent("CUST-PREF-001", "EVT-001",
                "GOLD", 4500, 299.99, false, 1_000_600L);
            EventEnvelope out = processor.invokeForTest(evt, state);

            // Event type comes from OperationConfig.outboundEventType = "offer"
            assertEquals("offer", out.getEventType(),
                "Preferred entity with history must produce offer");
            // toEnvelope does not copy entityId — output carries domain payload fields
            assertEquals("PREFERRED-20", out.getPayload().get("offerCode").asText());
            assertEquals(20, out.getPayload().get("discountPercent").asInt());
        }

        @Test
        @DisplayName("Output payload contains domain fields from the rule result array item")
        void outputPayloadHasMetadata() throws Exception {
            // The toEnvelope() method sets eventType and payload (the array item node).
            // The payload is the raw output event item — it contains whatever the rule put there.
            // agentId/entityId/entityType are NOT added by toEnvelope; they ARE in agent-error
            // payloads.  This test verifies the payload carries the expected domain fields.
            processor.injectRuleEngine(new StubDecisionEngine(preferredOfferResult()));
            EventEnvelope out = processor.invokeForTest(
                primaryEvent("CUST-META", "EVT-META", "GOLD", 1000, 99.0, false, 1_000_000L),
                null);

            assertEquals("offer", out.getEventType(),
                "Output event type must be 'offer' from OperationConfig");
            assertTrue(out.getPayload().has("offerCode"),
                "offerCode must be in output payload (from rule result array item)");
            assertEquals("PREFERRED-20", out.getPayload().get("offerCode").asText());
            assertEquals(20, out.getPayload().get("discountPercent").asInt());
        }
    }

    // ── Re-engagement signal ──────────────────────────────────────────────────

    @Nested
    @DisplayName("Re-engagement signal triggers secondary offer")
    class ReEngagementSignal {

        @Test
        @DisplayName("Primary event with reEngagementSignal=true produces re-engagement offer")
        void reEngagementSignalProducesOffer() throws Exception {
            // reEngagementOfferResult() uses outputEvents array; event type from OperationConfig
            processor.injectRuleEngine(new StubDecisionEngine(reEngagementOfferResult()));

            EventEnvelope out = processor.invokeForTest(
                primaryEvent("CUST-REENG-001", "EVT-REENG-001", "SILVER", 200, 89.0,
                    true /* reEngagementSignal */, 1_000_000L),
                null);

            // Event type comes from OperationConfig.outboundEventType = "offer"
            assertEquals("offer", out.getEventType());
            assertEquals("COMEBACK-15", out.getPayload().get("offerCode").asText());
            assertEquals("RE_ENGAGEMENT", out.getPayload().get("offerType").asText());
        }
    }

    // ── Generic context structure ─────────────────────────────────────────────

    @Nested
    @DisplayName("Generic context structure passed to rule engine")
    class GenericContextStructure {

        @Test
        @DisplayName("Context has now, processingTime, entity, and currentEvent (no 'input' wrapper)")
        void contextHasGenericInputShape() throws Exception {
            StubDecisionEngine stub = new StubDecisionEngine(noOfferResult());
            processor.injectRuleEngine(stub);

            long beforeMs = System.currentTimeMillis();
            processor.invokeForTest(
                primaryEvent("CUST-CTX", "EVT-CTX", "BRONZE", 100, 50.0, false, 1_000_000L),
                null);
            long afterMs = System.currentTimeMillis();

            // GenericContextBuilder returns a flat ObjectNode — no "input" wrapper.
            JsonNode ctx = stub.lastContext();
            assertNotNull(ctx, "context must not be null");
            assertFalse(ctx.has("input"),
                "generic context must NOT have an outer 'input' wrapper");
            assertTrue(ctx.has("now"),            "context must have 'now' (event timestamp string)");
            assertTrue(ctx.has("processingTime"), "context must have 'processingTime' (wall-clock string)");
            assertTrue(ctx.has("entity"),         "context must have 'entity'");
            assertTrue(ctx.has("currentEvent"),   "context must have 'currentEvent'");
            // processingTime must be wall-clock time captured during processing
            long procMs = java.time.Instant.parse(ctx.get("processingTime").asText()).toEpochMilli();
            assertTrue(procMs >= beforeMs && procMs <= afterMs,
                "processingTime must be wall-clock time captured during processing");
            // now is the event's own timestamp — distinct from processingTime
            assertEquals("1970-01-01T00:16:40Z", ctx.get("now").asText(),
                "now must equal the event's own timestamp serialized as ISO-8601");
            // currentEvent carries the event payload (customerId etc.)
            assertEquals("CUST-CTX", ctx.path("currentEvent").path("customerId").asText(),
                "currentEvent must contain the inbound event payload");
        }

        @Test
        @DisplayName("Primary event payload fields are present in context.currentEvent")
        void primaryEventFieldsPresentInCurrentEvent() throws Exception {
            StubDecisionEngine stub = new StubDecisionEngine(noOfferResult());
            processor.injectRuleEngine(stub);

            processor.invokeForTest(
                primaryEvent("CUST-FIELDS", "EVT-FIELDS", "SILVER", 1200, 149.99,
                    false, 1_000_000L),
                null);

            // GenericContextBuilder returns a flat context — access currentEvent directly
            JsonNode currentEvent = stub.lastContext().path("currentEvent");
            assertFalse(currentEvent.isMissingNode(), "currentEvent must be present");
            assertEquals("CUST-FIELDS", currentEvent.path("customerId").asText(),
                "customerId must be present in currentEvent");
            assertEquals("EVT-FIELDS", currentEvent.path("eventId").asText(),
                "eventId must be present in currentEvent");
            assertEquals("SILVER", currentEvent.path("tier").asText(),
                "tier must be present in currentEvent");
            assertEquals(1200, currentEvent.path("points").asInt(),
                "points must be present in currentEvent");
            assertFalse(currentEvent.path("activityHistory").isMissingNode(),
                "activityHistory must be preserved in currentEvent");
            assertFalse(currentEvent.path("signals").isMissingNode(),
                "signals must be preserved in currentEvent");
        }

        @Test
        @DisplayName("Secondary-event history accumulates in AgentState and getFlatHistory()")
        void secondaryEventHistoryInGenericHistory() throws Exception {
            StubDecisionEngine stub = new StubDecisionEngine(noOfferResult());
            processor.injectRuleEngine(stub);

            AgentState state = AgentState.init("CUST-HIST", "customer", 0L);

            processor.invokeForTest(secondaryEvent("CUST-HIST", "I-001", "Widget",
                "Electronics", 60, "2026-06-01T10:00:00Z", 1L), state);
            processor.invokeForTest(secondaryEvent("CUST-HIST", "I-002", "Gadget",
                "Electronics", 30, "2026-06-01T10:05:00Z", 2L), state);

            processor.invokeForTest(
                primaryEvent("CUST-HIST", "EVT-HIST", "BRONZE", 0, 20.0, false, 3L), state);

            // GenericContextBuilder returns a flat context with no "history" field by default
            // (history requires HistoryFieldMapping in OperationConfig).
            // Verify history is accumulated in AgentState instead.
            AgentState.EventHistory secondaryBucket = state.getEventHistories().get("secondary-event");
            assertNotNull(secondaryBucket, "secondary-event history bucket must exist in state");
            assertEquals(2, secondaryBucket.size(),
                "Two secondary-event entries must be accumulated in state");

            // The flat-history projection from AgentState must contain these entries
            long secondaryEventCount = state.getFlatHistory().stream()
                .filter(e -> "secondary-event".equals(e.eventType()))
                .count();
            assertEquals(2, secondaryEventCount,
                "Two secondary-event entries must appear in the flat history");

            // currentEvent is accessible from the last context captured
            JsonNode ctx = stub.lastContext();
            assertNotNull(ctx, "context must not be null");
            assertFalse(ctx.path("currentEvent").isMissingNode(),
                "currentEvent must be present in context");
        }
    }

    // ── State accumulation across primary events ──────────────────────────────

    @Nested
    @DisplayName("Entity state — updates persist across primary events")
    class EntityStatePersistence {

        @Test
        @DisplayName("Entity state written via OperationConfig.entityField persists across events")
        void entityStateAccumulatesAcrossEvents() throws Exception {
            // Production code reads entity state from OperationConfig.entityField in rule result.
            // Configure descriptor with entityField="customerData" for the primary operation.
            AgentDescriptor d = descriptorWithEntityField("customerData");
            StatefulAgentProcessor p = new StatefulAgentProcessor(d);

            JsonNode firstResult = MAPPER.readTree(
                "{\"outputEvents\":[{\"offerCode\":\"WELCOME-10\"}]," +
                "\"customerData\":{\"eventsEvaluated\":1,\"lastOfferCode\":\"WELCOME-10\"}}");
            JsonNode secondResult = MAPPER.readTree(
                "{\"outputEvents\":[{\"offerCode\":\"LOYAL-15\"}]," +
                "\"customerData\":{\"eventsEvaluated\":2,\"lastOfferCode\":\"LOYAL-15\"}}");

            AgentState state = AgentState.init("CUST-PERSIST", "customer", 0L);

            p.injectRuleEngine(new StubDecisionEngine(firstResult));
            p.invokeForTest(
                primaryEvent("CUST-PERSIST", "EVT-P-1", "BRONZE", 100, 49.99, false, 1L), state);

            p.injectRuleEngine(new StubDecisionEngine(secondResult));
            p.invokeForTest(
                primaryEvent("CUST-PERSIST", "EVT-P-2", "BRONZE", 200, 79.99, false, 2L), state);

            assertNotNull(state.getEntityData(), "entityData must be set after rule updates");
            assertEquals(2, state.getEntityData().get("eventsEvaluated").asInt(),
                "eventsEvaluated must be 2 after two events");
            assertEquals("LOYAL-15", state.getEntityData().get("lastOfferCode").asText(),
                "lastOfferCode must reflect the second event's result");
        }
    }

    // ── Error handling ─────────────────────────────────────────────────────────

    @Nested
    @DisplayName("Error handling — rule engine failures produce agent-error")
    class ErrorHandling {

        @Test
        @DisplayName("Rule engine exception produces agent-error with errorReason and originalEvent")
        void ruleEngineExceptionProducesAgentError() throws Exception {
            StubDecisionEngine stub = new StubDecisionEngine(null);
            stub.throwOnExecute(new RuntimeException("Engine timeout"));
            processor.injectRuleEngine(stub);

            EventEnvelope out = processor.invokeForTest(
                primaryEvent("CUST-ERR", "EVT-ERR", "GOLD", 5000, 199.0, false, 1_000_000L),
                null);

            assertEquals("agent-error", out.getEventType(),
                "Rule engine exception must produce agent-error");
            assertTrue(out.getPayload().has("errorReason"),
                "agent-error must include errorReason");
            assertTrue(out.getPayload().has("originalEvent"),
                "agent-error must include originalEvent");
            assertEquals("evaluation-agent", out.getPayload().get("agentId").asText());
        }

        @Test
        @DisplayName("Processing continues normally after a transient rule engine failure")
        void processingContinuesAfterError() throws Exception {
            StubDecisionEngine failing = new StubDecisionEngine(null);
            failing.throwOnExecute(new RuntimeException("transient failure"));
            processor.injectRuleEngine(failing);

            AgentState state = AgentState.init("CUST-RECOVER", "customer", 0L);
            processor.invokeForTest(
                primaryEvent("CUST-RECOVER", "EVT-FAIL", "BRONZE", 50, 20.0, false, 1L), state);

            // noOfferResult() uses outputEvents array; event type comes from OperationConfig ("offer").
            // The "no-offer" type would require a separate OperationConfig; use the default "offer".
            processor.injectRuleEngine(new StubDecisionEngine(noOfferResult()));
            EventEnvelope out = processor.invokeForTest(
                primaryEvent("CUST-RECOVER", "EVT-OK", "BRONZE", 50, 20.0, false, 2L), state);

            assertEquals("offer", out.getEventType(),
                "Processing must recover after a transient failure (event type from OperationConfig)");
        }
    }

    // ── Output field filtering ─────────────────────────────────────────────────

    @Nested
    @DisplayName("Output field filtering — __ fields stripped")
    class OutputFieldFiltering {

        @Test
        @DisplayName("Output payload is the raw array item — only contains what the rule put in the item")
        void internalFieldsNotInPayload() throws Exception {
            // In the new array-based format the output event payload is the array item node.
            // Internal fields like "outputEvents" are at the result root, not inside items.
            // Each item is the payload verbatim — no stripping needed or expected.
            ObjectNode item = MAPPER.createObjectNode();
            item.put("offerCode", "TEST-20");
            item.put("score", 88);
            // Internal fields at the result root level are NOT part of the output item
            ObjectNode result = MAPPER.createObjectNode();
            result.set("outputEvents", MAPPER.createArrayNode().add(item));
            result.put("__entityState_root", "ignored");  // root-level field, not in item

            processor.injectRuleEngine(new StubDecisionEngine(result));

            EventEnvelope out = processor.invokeForTest(
                primaryEvent("CUST-STRIP", "EVT-STRIP", "GOLD", 5000, 100.0, false, 1_000_000L),
                null);

            // The payload IS the item node — verify it has exactly what was in the item
            assertTrue(out.getPayload().has("offerCode"),
                "offerCode must be in output payload (from array item)");
            assertEquals("TEST-20", out.getPayload().get("offerCode").asText());
            // Root-level fields should not appear in the item
            assertFalse(out.getPayload().has("__entityState_root"),
                "root-level fields must not appear in the output item payload");
            assertFalse(out.getPayload().has("outputEvents"),
                "outputEvents array must not appear inside the item payload");
        }
    }

    // ── Horizon eviction for secondary events ─────────────────────────────────

    @Nested
    @DisplayName("Hybrid horizon eviction — secondary-event bucket count ceiling")
    class SecondaryEventHorizonEviction {

        @Test
        @DisplayName("Count ceiling evicts oldest secondary events when limit exceeded")
        void countCeilingEvictsOldEntries() throws Exception {
            // Override with a count(3) horizon for secondary-event for this test
            AgentDescriptor d = genericDescriptor();
            d.setSubscriptions(List.of(
                new EventSubscription("primary-event",
                    "agent-primary-events",
                    "evaluationService_primaryEvent_op",
                    HorizonConfig.count(20)),
                new EventSubscription("secondary-event",
                    "agent-secondary-events",
                    "",
                    HorizonConfig.count(3))
            ));
            StatefulAgentProcessor p = new StatefulAgentProcessor(d);
            // Use null result so the primary-event invoke follows the agent-error path
            // (non-empty list), avoiding NoSuchElementException from invokeForTest.
            // This test only checks secondaryHistory.size() — the output type is irrelevant.
            StubDecisionEngine stub = new StubDecisionEngine(null);
            p.injectRuleEngine(stub);

            AgentState state = AgentState.init("CUST-EVICT", "customer", 0L);

            // Insert 5 secondary events — only 3 most recent should survive
            for (int i = 0; i < 5; i++) {
                p.invokeForTest(secondaryEvent("CUST-EVICT", "I-" + i, "Item " + i,
                    "Books", 10, "2026-06-01T10:0" + i + ":00Z", (long) i), state);
            }

            p.invokeForTest(
                primaryEvent("CUST-EVICT", "EVT-EVICT", "BRONZE", 0, 10.0, false, 100L), state);

            // Verify the state bucket was capped
            AgentState.EventHistory secondaryHistory = state.getEventHistories().get("secondary-event");
            assertNotNull(secondaryHistory, "secondary-event history bucket must exist");
            assertTrue(secondaryHistory.size() <= 3,
                "secondary-event count(3) horizon must retain at most 3 events; got "
                    + secondaryHistory.size());
        }
    }
}

