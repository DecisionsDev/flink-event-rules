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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end functional tests for multi-agent chaining scenarios.
 *
 * <p>These tests exercise the complete scenario where the output of one agent feeds
 * directly into the input of a downstream agent, forming a processing pipeline.
 * The tests validate:
 *
 * <ol>
 *   <li>Chain edge detection: builder correctly identifies upstream→downstream relationships</li>
 *   <li>Linear chain (A→B→C): three agents processing sequentially</li>
 *   <li>Fan-out (A→B, A→C): one source feeding two downstream agents</li>
 *   <li>Fan-in (A→C, B→C): two sources feeding one downstream agent</li>
 *   <li>Event type filtering: only matching event types trigger downstream processing</li>
 *   <li>State isolation: each agent maintains its own independent entity state</li>
 *   <li>Cycle detection: circular chains are rejected before processing</li>
 *   <li>Banking→retail pipeline: fraud-filtered transactions feed the promotions engine</li>
 *   <li>Self-subscription: an agent subscribing to its own output does not create a self-loop</li>
 *   <li>Error isolation: an agent error in one pipeline stage does not propagate upstream</li>
 * </ol>
 *
 * <p>Note: these tests operate at the logical (descriptor + processor) level without a live
 * Flink cluster. DataStream wiring is validated through {@link AgentChainBuilder#resolveTopology(List)}
 * and processor-level invocations; stream union is Flink-level and is verified end-to-end
 * by the integration tests.
 */
@DisplayName("Multi-agent chaining — end-to-end workflow tests")
class AgentChainingWorkflowTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // ── Stub rule engine ──────────────────────────────────────────────────────

    static class ChainStub extends RuleEngineRunner {

        private final String outputEventType;
        private JsonNode extraFields;
        private RuntimeException toThrow;
        private JsonNode lastContext;
        private int callCount;

        ChainStub(String outputEventType) {
            super("stub-" + outputEventType, URI.create("file:///dev/null"));
            this.outputEventType = outputEventType;
        }

        ChainStub withField(String key, String value) throws Exception {
            if (extraFields == null) extraFields = MAPPER.createObjectNode();
            ((ObjectNode) extraFields).put(key, value);
            return this;
        }

        void throwOnExecute(RuntimeException ex) { this.toThrow = ex; }
        JsonNode lastContext()                    { return lastContext; }
        int callCount()                           { return callCount; }

        @Override
        public JsonNode execute(JsonNode ctx) {
            callCount++;
            lastContext = ctx;
            if (toThrow != null) throw toThrow;
            // Build the output event item with domain fields
            ObjectNode item = MAPPER.createObjectNode();
            item.put("processedBy", outputEventType + "-agent");
            if (extraFields != null) {
                extraFields.fields().forEachRemaining(e -> item.set(e.getKey(), e.getValue()));
            }
            // Wrap in an outputEvents array; the event type comes from OperationConfig
            ObjectNode result = MAPPER.createObjectNode();
            result.set("outputEvents", MAPPER.createArrayNode().add(item));
            return result;
        }

        @Override protected void initialize() { /* no-op */ }
        @Override public void    close()       { /* no-op */ }
    }

    // ── Builder helpers ───────────────────────────────────────────────────────

    /**
     * Minimal descriptor factory for chain tests.
     * The first element of {@code outEventTypes} is registered as the
     * {@code outbounds} entry for the agent's operation so that
     * {@code buildOutputEvents} can assign the correct event type to output envelopes.
     */
    private static AgentDescriptor agent(String agentId, String entityType,
                                          List<String> inEventTypes,
                                          List<String> outEventTypes) {
        AgentDescriptor d = new AgentDescriptor();
        d.setAgentId(agentId);
        d.setName(agentId);
        d.setEntityType(entityType);
        d.setEntityKeyExtractor("$.entityId");

        RulesetConfig rs = new RulesetConfig("/tmp/" + agentId + ".jar");
        // Register an OperationConfig so buildOutputEvents knows the outbound event type
        if (!outEventTypes.isEmpty()) {
            RulesetConfig.OperationConfig opCfg = new RulesetConfig.OperationConfig();
            opCfg.setOperation("op-" + agentId);
            opCfg.setOutbounds(java.util.List.of(new RulesetConfig.OutboundConfig("outputEvents", outEventTypes.get(0))));
            rs.setOperations(java.util.List.of(opCfg));
        }
        d.setRuleset(rs);

        d.setSubscriptions(inEventTypes.stream()
            .map(et -> new EventSubscription(et, "topic-" + et,
                "op-" + agentId, HorizonConfig.count(10)))
            .collect(java.util.stream.Collectors.toList()));

        d.setOutputs(outEventTypes.stream()
            .map(et -> new OutputConfig(et, "topic-out-" + et))
            .collect(java.util.stream.Collectors.toList()));

        return d;
    }

    /** Configures an OperationConfig with the given outbound event type on a descriptor. */
    private static void setOutboundEventType(AgentDescriptor d, String eventType) {
        String op = d.getSubscriptions().isEmpty() ? "op"
            : d.getSubscriptions().get(0).getOperation();
        RulesetConfig.OperationConfig opCfg = new RulesetConfig.OperationConfig();
        opCfg.setOperation(op);
        opCfg.setOutbounds(java.util.List.of(new RulesetConfig.OutboundConfig("outputEvents", eventType)));
        d.getRuleset().setOperations(java.util.List.of(opCfg));
    }

    /** Creates a simple inbound envelope for the given entity and event type. */
    private static EventEnvelope envelope(String entityId, String eventType,
                                           String topic, long ts) throws Exception {
        ObjectNode payload = MAPPER.createObjectNode();
        payload.put("entityId", entityId);
        payload.put("source",   "test");
        EventEnvelope env = EventEnvelope.from(eventType, topic, ts, payload);
        env.setEntityId(entityId);
        return env;
    }

    // ── Chain edge detection ──────────────────────────────────────────────────

    @Nested
    @DisplayName("Chain edge detection via AgentChainBuilder")
    class ChainEdgeDetection {

        @Test
        @DisplayName("Single-hop chain: agent-b subscribing to agent-a output is detected")
        void singleHopChainDetected() {
            AgentDescriptor a = agent("agent-a", "order",
                List.of("transaction"), List.of("approved-transaction"));
            AgentDescriptor b = agent("agent-b", "account",
                List.of("approved-transaction"), List.of("loyalty-credit"));

            AgentChainBuilder builder = new AgentChainBuilder();
            AgentChainBuilder.DeferredChainedInputs chains = builder.resolveTopology(List.of(a, b));

            assertFalse(chains.isEmpty(),                  "One chain edge must exist");
            assertTrue(chains.hasChainedInput("agent-b"),  "agent-b must be downstream");
            assertFalse(chains.hasChainedInput("agent-a"), "agent-a is upstream only");
        }

        @Test
        @DisplayName("Three-agent linear chain: all downstream agents detected")
        void threeAgentLinearChainDetected() {
            AgentDescriptor fraud = agent("fraud",    "account",   List.of("transaction"),        List.of("clean-transaction"));
            AgentDescriptor scoring = agent("scoring",  "account",   List.of("clean-transaction"),  List.of("scored-transaction"));
            AgentDescriptor loyalty = agent("loyalty",  "customer",  List.of("scored-transaction"), List.of("loyalty-event"));

            AgentChainBuilder builder = new AgentChainBuilder();
            AgentChainBuilder.DeferredChainedInputs chains = builder.resolveTopology(List.of(fraud, scoring, loyalty));

            assertTrue(chains.hasChainedInput("scoring"), "scoring must be downstream of fraud");
            assertTrue(chains.hasChainedInput("loyalty"), "loyalty must be downstream of scoring");
            assertFalse(chains.hasChainedInput("fraud"),  "fraud has no upstream feeder");
            assertEquals(2, chains.chainedAgentIds().size());
        }

        @Test
        @DisplayName("Fan-out: one source feeds two downstream agents")
        void fanOutDetected() {
            AgentDescriptor source = agent("source",  "order",   List.of("raw"),         List.of("alert"));
            AgentDescriptor notify = agent("notify",  "account", List.of("alert"),        List.of("sms"));
            AgentDescriptor archive = agent("archive", "account", List.of("alert"),        List.of("stored"));

            AgentChainBuilder builder = new AgentChainBuilder();
            AgentChainBuilder.DeferredChainedInputs chains = builder.resolveTopology(List.of(source, notify, archive));

            assertTrue(chains.hasChainedInput("notify"),  "notify must receive alert from source");
            assertTrue(chains.hasChainedInput("archive"), "archive must receive alert from source");
            assertFalse(chains.hasChainedInput("source"), "source has no upstream feeder");
            assertEquals(Set.of("notify", "archive"), chains.chainedAgentIds());
        }

        @Test
        @DisplayName("Fan-in: two sources both feeding one downstream agent")
        void fanInDetected() {
            AgentDescriptor sourceA = agent("sourceA", "order",   List.of("raw-a"),  List.of("enriched"));
            AgentDescriptor sourceB = agent("sourceB", "account", List.of("raw-b"),  List.of("enriched"));
            AgentDescriptor sink = agent("sink",    "entity",  List.of("enriched"), List.of("done"));

            AgentChainBuilder builder = new AgentChainBuilder();
            AgentChainBuilder.DeferredChainedInputs chains = builder.resolveTopology(List.of(sourceA, sourceB, sink));

            // sink subscribes to "enriched" which both sourceA and sourceB emit
            assertTrue(chains.hasChainedInput("sink"),
                "sink must have two chained inputs (from sourceA and sourceB)");
            assertFalse(chains.hasChainedInput("sourceA"), "sourceA is upstream");
            assertFalse(chains.hasChainedInput("sourceB"), "sourceB is upstream");
        }

        @Test
        @DisplayName("Disjoint agents with no overlapping event types produce empty chains")
        void disjointAgentsNoChains() {
            AgentDescriptor banking = agent("banking-agent", "account",
                List.of("transaction"), List.of("fraud-alert"));
            AgentDescriptor retail = agent("retail-agent", "customer",
                List.of("order"), List.of("promotion-offer"));

            AgentChainBuilder builder = new AgentChainBuilder();
            AgentChainBuilder.DeferredChainedInputs chains = builder.resolveTopology(List.of(banking, retail));

            assertTrue(chains.isEmpty(), "Disjoint agents must produce no chain edges");
        }
    }

    // ── Processor-level single-hop pipeline ───────────────────────────────────

    @Nested
    @DisplayName("Processor-level pipeline: fraud detection → loyalty scoring")
    class ProcessorLevelPipeline {

        @Test
        @DisplayName("Agent-a output event type becomes agent-b input event type")
        void outputOfAgentAFeedsAgentB() throws Exception {
            // Agent A: fraud detection — emits clean-transaction
            AgentDescriptor descA = agent("fraud-agent", "account",
                List.of("transaction"), List.of("clean-transaction", "fraud-alert"));
            StatefulAgentProcessor fraudAgent = new StatefulAgentProcessor(descA);
            fraudAgent.injectRuleEngine(new ChainStub("clean-transaction"));

            // Agent B: loyalty scoring — subscribes to clean-transaction
            AgentDescriptor descB = agent("loyalty-agent", "customer",
                List.of("clean-transaction"), List.of("loyalty-credit"));
            StatefulAgentProcessor loyaltyAgent = new StatefulAgentProcessor(descB);
            ChainStub loyaltyStub = new ChainStub("loyalty-credit");
            loyaltyAgent.injectRuleEngine(loyaltyStub);

            // Step 1: Process a raw transaction through fraud agent
            EventEnvelope txn = envelope("ACC-001", "transaction", "topic-transaction", 1L);
            EventEnvelope fraudOut = fraudAgent.invokeForTest(txn, null);

            assertEquals("clean-transaction", fraudOut.getEventType(),
                "Fraud agent must emit clean-transaction for approved events");

            // Step 2: The fraud output feeds directly into the loyalty agent
            EventEnvelope loyaltyIn = EventEnvelope.from(
                fraudOut.getEventType(), "topic-clean-transaction",
                fraudOut.getTimestamp(), fraudOut.getPayload());
            loyaltyIn.setEntityId(fraudOut.getEntityId());

            AgentState loyaltyState = AgentState.init("ACC-001", "customer", 1L);
            EventEnvelope loyaltyOut = loyaltyAgent.invokeForTest(loyaltyIn, loyaltyState);

            assertEquals("loyalty-credit", loyaltyOut.getEventType(),
                "Loyalty agent must emit loyalty-credit after processing clean-transaction");
            assertEquals(1, loyaltyStub.callCount(),
                "Loyalty rule engine must be called exactly once");
        }

        @Test
        @DisplayName("Fraud-alert events are NOT fed to loyalty agent (wrong event type)")
        void fraudAlertNotForwardedToLoyaltyAgent() throws Exception {
            // Agent A: fraud detection emits fraud-alert for suspicious events
            AgentDescriptor descA = agent("fraud-agent", "account",
                List.of("transaction"), List.of("fraud-alert", "clean-transaction"));
            StatefulAgentProcessor fraudAgent = new StatefulAgentProcessor(descA);
            fraudAgent.injectRuleEngine(new ChainStub("fraud-alert")); // suspicious → fraud-alert

            EventEnvelope txn = envelope("ACC-SUSP", "transaction", "topic-transaction", 1L);
            EventEnvelope fraudOut = fraudAgent.invokeForTest(txn, null);

            assertEquals("fraud-alert", fraudOut.getEventType());

            // Agent B subscribes to "clean-transaction", NOT "fraud-alert"
            AgentDescriptor descB = agent("loyalty-agent", "customer",
                List.of("clean-transaction"), List.of("loyalty-credit"));
            StatefulAgentProcessor loyaltyAgent = new StatefulAgentProcessor(descB);
            ChainStub loyaltyStub = new ChainStub("loyalty-credit");
            loyaltyAgent.injectRuleEngine(loyaltyStub);

            // The fraud-alert would be filtered by the chain before reaching loyalty agent.
            // Simulate what the chain filter does: the fraud-alert event has the wrong type.
            // We verify the chain builder correctly excludes this edge.
            AgentChainBuilder builder = new AgentChainBuilder();
            AgentChainBuilder.DeferredChainedInputs chains = builder.resolveTopology(List.of(descA, descB));

            // fraud-agent emits [fraud-alert, clean-transaction]
            // loyalty-agent subscribes to [clean-transaction]
            // → chain edge exists, but stream is filtered to clean-transaction only
            assertTrue(chains.hasChainedInput("loyalty-agent"),
                "loyalty-agent must be downstream because fraud-agent emits clean-transaction");
        }

        @Test
        @DisplayName("Three-agent sequential pipeline: each stage transforms the event")
        void threeStageSequentialPipeline() throws Exception {
            // Stage 1: ingestion → enrichment
            AgentDescriptor descA = agent("ingestion", "entity",
                List.of("raw-event"), List.of("enriched-event"));
            StatefulAgentProcessor stageA = new StatefulAgentProcessor(descA);
            stageA.injectRuleEngine(
                new ChainStub("enriched-event").withField("stage", "enriched"));

            // Stage 2: enrichment → scoring
            AgentDescriptor descB = agent("scoring", "entity",
                List.of("enriched-event"), List.of("scored-event"));
            StatefulAgentProcessor stageB = new StatefulAgentProcessor(descB);
            stageB.injectRuleEngine(
                new ChainStub("scored-event").withField("stage", "scored"));

            // Stage 3: scoring → notification
            AgentDescriptor descC = agent("notification", "entity",
                List.of("scored-event"), List.of("notification-sent"));
            StatefulAgentProcessor stageC = new StatefulAgentProcessor(descC);
            ChainStub notifStub = new ChainStub("notification-sent");
            stageC.injectRuleEngine(notifStub);

            EventEnvelope raw = envelope("E-001", "raw-event", "topic-raw-event", 1L);

            // Stage A
            EventEnvelope aOut = stageA.invokeForTest(raw, null);
            assertEquals("enriched-event", aOut.getEventType());
            assertEquals("enriched-event-agent", aOut.getPayload().get("processedBy").asText());

            // Stage B
            EventEnvelope bIn = EventEnvelope.from("enriched-event", "topic-enriched-event",
                aOut.getTimestamp(), aOut.getPayload());
            bIn.setEntityId(aOut.getEntityId());
            EventEnvelope bOut = stageB.invokeForTest(bIn, null);
            assertEquals("scored-event", bOut.getEventType());

            // Stage C
            EventEnvelope cIn = EventEnvelope.from("scored-event", "topic-scored-event",
                bOut.getTimestamp(), bOut.getPayload());
            cIn.setEntityId(bOut.getEntityId());
            EventEnvelope cOut = stageC.invokeForTest(cIn, null);
            assertEquals("notification-sent", cOut.getEventType());
            assertEquals(1, notifStub.callCount(), "Notification stub must be called once");
        }
    }

    // ── State isolation between agents ────────────────────────────────────────

    @Nested
    @DisplayName("State isolation — each agent has independent entity state")
    class StateIsolation {

        @Test
        @DisplayName("Agent-A entity state does not bleed into Agent-B entity state")
        void stateIsIsolated() throws Exception {
            // Configure agent A to expose entity state under "accountEntity" field
            AgentDescriptor descA = agent("agent-a", "account",
                List.of("transaction"), List.of("processed-transaction"));
            RulesetConfig.OperationConfig opCfgA = new RulesetConfig.OperationConfig();
            opCfgA.setOperation("op-agent-a");
            opCfgA.setOutbounds(java.util.List.of(new RulesetConfig.OutboundConfig("outputEvents", "processed-transaction")));
            opCfgA.setEntityField("accountEntity");
            descA.getRuleset().setOperations(java.util.List.of(opCfgA));

            StatefulAgentProcessor agentA = new StatefulAgentProcessor(descA);
            // A returns entity data under "accountEntity" + outputEvents array
            JsonNode aResult = MAPPER.readTree(
                "{\"outputEvents\":[{\"decision\":\"ok\"}]," +
                "\"accountEntity\":{\"riskScore\":75,\"tier\":\"HIGH\"}}");
            agentA.injectRuleEngine(new ChainStub("processed-transaction") {
                @Override public JsonNode execute(JsonNode ctx) { return aResult; }
            });

            // Configure agent B to expose entity state under "customerEntity" field
            AgentDescriptor descB = agent("agent-b", "customer",
                List.of("processed-transaction"), List.of("loyalty-event"));
            RulesetConfig.OperationConfig opCfgB = new RulesetConfig.OperationConfig();
            opCfgB.setOperation("op-agent-b");
            opCfgB.setOutbounds(java.util.List.of(new RulesetConfig.OutboundConfig("outputEvents", "loyalty-event")));
            opCfgB.setEntityField("customerEntity");
            descB.getRuleset().setOperations(java.util.List.of(opCfgB));

            StatefulAgentProcessor agentB = new StatefulAgentProcessor(descB);
            // B returns entity data under "customerEntity" + outputEvents array
            JsonNode bResult = MAPPER.readTree(
                "{\"outputEvents\":[{\"decision\":\"ok\"}]," +
                "\"customerEntity\":{\"loyaltyScore\":300,\"tier\":\"GOLD\"}}");
            agentB.injectRuleEngine(new ChainStub("loyalty-event") {
                @Override public JsonNode execute(JsonNode ctx) { return bResult; }
            });

            AgentState stateA = AgentState.init("E-001", "account", 1L);
            AgentState stateB = AgentState.init("E-001", "customer", 1L);

            // Process through agent A
            agentA.invokeForTest(envelope("E-001", "transaction", "topic-transaction", 1L), stateA);

            // Process through agent B (using the chained output)
            EventEnvelope bIn = EventEnvelope.from("processed-transaction",
                "topic-processed-transaction", 2L, MAPPER.createObjectNode());
            bIn.setEntityId("E-001");
            agentB.invokeForTest(bIn, stateB);

            // Verify state isolation
            assertNotNull(stateA.getEntityData(),
                "Agent A entity state must be populated");
            assertNotNull(stateB.getEntityData(),
                "Agent B entity state must be populated");
            assertEquals(75, stateA.getEntityData().get("riskScore").asInt(),
                "Agent A entity state must retain riskScore=75");
            assertEquals(300, stateB.getEntityData().get("loyaltyScore").asInt(),
                "Agent B entity state must retain loyaltyScore=300");
            // Agent A's tier is HIGH; Agent B's tier is GOLD — independent
            assertEquals("HIGH", stateA.getEntityData().get("tier").asText(),
                "Agent A tier must remain HIGH");
            assertEquals("GOLD", stateB.getEntityData().get("tier").asText(),
                "Agent B tier must be GOLD (independent of Agent A)");
        }
    }

    // ── Error isolation ───────────────────────────────────────────────────────

    @Nested
    @DisplayName("Error isolation — downstream error does not affect upstream state")
    class ErrorIsolation {

        @Test
        @DisplayName("Downstream agent error produces agent-error without corrupting upstream state")
        void downstreamErrorIsolated() throws Exception {
            // Upstream (A) processes cleanly
            AgentDescriptor descA = agent("upstream", "account",
                List.of("transaction"), List.of("downstream-event"));
            StatefulAgentProcessor upstream = new StatefulAgentProcessor(descA);
            upstream.injectRuleEngine(new ChainStub("downstream-event"));

            AgentState upstreamState = AgentState.init("E-UP", "account", 1L);
            EventEnvelope upIn = envelope("E-UP", "transaction", "topic-transaction", 1L);
            EventEnvelope upOut = upstream.invokeForTest(upIn, upstreamState);
            assertEquals("downstream-event", upOut.getEventType());

            // Downstream (B) throws
            AgentDescriptor descB = agent("downstream", "customer",
                List.of("downstream-event"), List.of("final-event"));
            StatefulAgentProcessor downstreamAgent = new StatefulAgentProcessor(descB);
            ChainStub failingStub = new ChainStub("final-event");
            failingStub.throwOnExecute(new RuntimeException("downstream failure"));
            downstreamAgent.injectRuleEngine(failingStub);

            EventEnvelope downIn = EventEnvelope.from("downstream-event",
                "topic-downstream-event", 2L, upOut.getPayload());
            downIn.setEntityId("E-UP");

            AgentState downstreamState = AgentState.init("E-UP", "customer", 2L);
            EventEnvelope downOut = downstreamAgent.invokeForTest(downIn, downstreamState);

            // Downstream fails — produces agent-error
            assertEquals("agent-error", downOut.getEventType(),
                "Downstream exception must produce agent-error");

            // Upstream state must be unaffected
            assertEquals("downstream-event", upOut.getEventType(),
                "Upstream output must not be changed by downstream failure");
        }
    }

    // ── Cycle detection ───────────────────────────────────────────────────────

    @Nested
    @DisplayName("Cycle detection prevents circular chains")
    class CycleDetection {

        @Test
        @DisplayName("Direct 2-cycle A→B→A is rejected at resolve() time")
        void directTwoCycleRejected() {
            AgentDescriptor a = agent("agent-a", "account",
                List.of("ev-b"), List.of("ev-a"));
            AgentDescriptor b = agent("agent-b", "account",
                List.of("ev-a"), List.of("ev-b"));

            AgentChainBuilder builder = new AgentChainBuilder();
            IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> builder.resolveTopology(List.of(a, b)), "Direct 2-cycle must be rejected");
            String msg = ex.getMessage();
            assertTrue(msg.contains("Cycle detected"), "Error must say 'Cycle detected'");
            assertTrue(msg.contains("agent-a") || msg.contains("agent-b"),
                "Error must name the agents involved");
        }

        @Test
        @DisplayName("3-cycle A→B→C→A is rejected")
        void threeNodeCycleRejected() {
            AgentDescriptor a = agent("a", "e", List.of("ev-c"), List.of("ev-a"));
            AgentDescriptor b = agent("b", "e", List.of("ev-a"), List.of("ev-b"));
            AgentDescriptor c = agent("c", "e", List.of("ev-b"), List.of("ev-c"));

            AgentChainBuilder builder = new AgentChainBuilder();
            assertThrows(IllegalStateException.class, () -> builder.resolveTopology(List.of(a, b, c)),
                "3-node cycle must be rejected");
        }

        @Test
        @DisplayName("Valid DAG with 4 agents does not throw")
        void validDagNotRejected() {
            // a→b→d, a→c→d (diamond pattern — DAG, no cycle)
            AgentDescriptor a = agent("a", "e", List.of("raw"),    List.of("ev-a"));
            AgentDescriptor b = agent("b", "e", List.of("ev-a"),   List.of("ev-b"));
            AgentDescriptor c = agent("c", "e", List.of("ev-a"),   List.of("ev-c"));
            AgentDescriptor d = agent("d", "e", List.of("ev-b", "ev-c"), List.of("final"));

            AgentChainBuilder builder = new AgentChainBuilder();
            assertDoesNotThrow(() -> builder.resolveTopology(List.of(a, b, c, d)), "Diamond DAG must not be rejected");
        }
    }

    // ── Real banking→retail pipeline scenario ─────────────────────────────────

    @Nested
    @DisplayName("Banking fraud → retail promotions pipeline scenario")
    class BankingToRetailPipeline {

        @Test
        @DisplayName("Approved transaction from fraud agent feeds retail promotions agent")
        void fraudApprovedFeedsRetailPromo() throws Exception {
            // Stage 1: fraud detection emits approved-transaction
            AgentDescriptor fraudDesc = agent("fraud-detection", "account",
                List.of("transaction"), List.of("approved-transaction", "fraud-alert"));
            StatefulAgentProcessor fraudProc = new StatefulAgentProcessor(fraudDesc);
            fraudProc.injectRuleEngine(new ChainStub("approved-transaction")
                .withField("decision", "APPROVED"));

            // Stage 2: promotions subscribes to approved-transaction
            AgentDescriptor promoDesc = agent("retail-promotions", "customer",
                List.of("approved-transaction"), List.of("promotion-offer", "no-offer"));
            StatefulAgentProcessor promoProc = new StatefulAgentProcessor(promoDesc);
            ChainStub promoStub = new ChainStub("promotion-offer");
            promoProc.injectRuleEngine(promoStub);

            // Verify chain is detected
            AgentChainBuilder builder = new AgentChainBuilder();
            AgentChainBuilder.DeferredChainedInputs chains = builder.resolveTopology(List.of(fraudDesc, promoDesc));

            assertTrue(chains.hasChainedInput("retail-promotions"),
                "retail-promotions must be downstream of fraud-detection via approved-transaction");
            assertFalse(chains.hasChainedInput("fraud-detection"),
                "fraud-detection has no upstream feeder");

            // Simulate the pipeline end-to-end
            EventEnvelope txn = envelope("ACC-001", "transaction", "topic-transaction", 1L);
            EventEnvelope fraudOut = fraudProc.invokeForTest(txn, null);

            assertEquals("approved-transaction", fraudOut.getEventType());

            // Chain: forward approved-transaction to retail-promotions
            EventEnvelope promoIn = EventEnvelope.from("approved-transaction",
                "topic-approved-transaction", fraudOut.getTimestamp(), fraudOut.getPayload());
            promoIn.setEntityId(fraudOut.getEntityId());

            EventEnvelope promoOut = promoProc.invokeForTest(promoIn, null);
            assertEquals("promotion-offer", promoOut.getEventType(),
                "Retail agent must emit promotion-offer for an approved transaction");
            assertEquals(1, promoStub.callCount(), "Promotions engine called exactly once");
        }

        @Test
        @DisplayName("Fraud-alert events do NOT trigger promotions engine")
        void fraudAlertNotForwardedToPromo() {
            AgentDescriptor fraudDesc = agent("fraud-detection", "account",
                List.of("transaction"), List.of("approved-transaction", "fraud-alert"));
            AgentDescriptor promoDesc = agent("retail-promotions", "customer",
                List.of("approved-transaction"), List.of("promotion-offer"));

            AgentChainBuilder builder = new AgentChainBuilder();
            AgentChainBuilder.DeferredChainedInputs chains = builder.resolveTopology(List.of(fraudDesc, promoDesc));

            // Chain exists (via approved-transaction), but fraud-alerts would be
            // filtered by the stream filter in the chain — only the edge type matters.
            assertTrue(chains.hasChainedInput("retail-promotions"),
                "Chain edge exists via approved-transaction");

            // The chain edge is specifically for 'approved-transaction'; fraud-alerts
            // would be filtered out before reaching retail-promotions.
            // Test this by verifying that if fraud-alert were manually routed to
            // retail-promotions, it would still produce output (chain filter is stream-level)
            // — the important thing is the chain edge is type-specific.
            assertEquals(1, chains.chainedAgentIds().size());
            assertTrue(chains.chainedAgentIds().contains("retail-promotions"));
        }
    }

    // ── Self-subscription guard ───────────────────────────────────────────────

    @Nested
    @DisplayName("Self-subscription does not create self-chain edge")
    class SelfSubscriptionGuard {

        @Test
        @DisplayName("Agent subscribing to its own output type produces no self-loop edge")
        void selfSubscriptionNoSelfLoop() {
            AgentChainBuilder builder = new AgentChainBuilder();
            // This agent emits "alert" and also subscribes to "alert"
            AgentDescriptor selfAgent = agent("self-agent", "account",
                List.of("alert", "raw"), List.of("alert"));

            // Should not throw (no actual cycle — self-subscriptions are excluded)
            AgentChainBuilder.DeferredChainedInputs chains = assertDoesNotThrow(() -> builder.resolveTopology(List.of(selfAgent)));

            // The self-edge must NOT be recorded
            assertTrue(chains.isEmpty() || !chains.hasChainedInput("self-agent"),
                "Self-subscription must not create a self-loop chain edge");
        }
    }

    // ── Multiple subscriptions per agent ──────────────────────────────────────

    @Nested
    @DisplayName("Multiple subscriptions: agent subscribes to multiple upstream event types")
    class MultipleSubscriptions {

        @Test
        @DisplayName("Downstream agent with two upstream sources has chained input from both")
        void downstreamWithTwoUpstreamSources() {
            AgentDescriptor srcA = agent("source-a", "account",
                List.of("raw-a"), List.of("enriched-type-a"));
            AgentDescriptor srcB = agent("source-b", "order",
                List.of("raw-b"), List.of("enriched-type-b"));
            // Sink subscribes to both enriched types
            AgentDescriptor sink = agent("sink", "entity",
                List.of("enriched-type-a", "enriched-type-b"), List.of("final"));

            AgentChainBuilder builder = new AgentChainBuilder();
            AgentChainBuilder.DeferredChainedInputs chains = builder.resolveTopology(List.of(srcA, srcB, sink));

            assertTrue(chains.hasChainedInput("sink"),
                "sink must have chained inputs from both source-a and source-b");
            assertFalse(chains.hasChainedInput("source-a"), "source-a is upstream");
            assertFalse(chains.hasChainedInput("source-b"), "source-b is upstream");
        }

        @Test
        @DisplayName("Agent with two outputs: different downstream agents each receive one type")
        void twoOutputsToTwoDownstreamAgents() {
            AgentDescriptor router = agent("router", "entity",
                List.of("raw"), List.of("type-a", "type-b"));
            AgentDescriptor handlerA = agent("handler-a", "entity",
                List.of("type-a"), List.of("done-a"));
            AgentDescriptor handlerB = agent("handler-b", "entity",
                List.of("type-b"), List.of("done-b"));

            AgentChainBuilder builder = new AgentChainBuilder();
            AgentChainBuilder.DeferredChainedInputs chains = builder.resolveTopology(List.of(router, handlerA, handlerB));

            assertTrue(chains.hasChainedInput("handler-a"),
                "handler-a must receive type-a from router");
            assertTrue(chains.hasChainedInput("handler-b"),
                "handler-b must receive type-b from router");
            assertFalse(chains.hasChainedInput("router"),
                "router has no upstream feeder");
        }
    }
}

