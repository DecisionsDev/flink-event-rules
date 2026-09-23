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
package com.ibm.di.event;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ibm.di.event.agent.AgentBasedDataStreamJob;
import com.ibm.di.event.agent.AgentDescriptor;
import com.ibm.di.event.agent.EventEnvelope;
import com.ibm.di.event.agent.StatefulAgentProcessor;
import com.ibm.di.event.rule.RuleEngineRunner;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.net.URI;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Parallelism correctness tests")
class ParallelismCorrectnessTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // ── Agent job parallelism ─────────────────────────────────────────────────

    @Nested
    @DisplayName("AgentBasedDataStreamJob parallelism resolution")
    class AgentJobParallelism {

        @Test
        @DisplayName("loadFileDescriptor: top-level parallelism=4 in wrapper object is read")
        void topLevelParallelismParsed() throws Exception {
            String json = """
                {
                  "parallelism": 4,
                  "agents": [
                    {
                      "agentId": "test-agent",
                      "entityType": "account",
                      "entityKeyExtractor": "$.accountId",
                      "ruleset": { "name": "TestModel-op", "path": "/tmp/test.jar" }
                    }
                  ]
                }""";
            AgentBasedDataStreamJob.AgentFileDescriptor fd =
                invokeLoadFileDescriptor(json);
            assertEquals(4, fd.parallelism(),
                "top-level parallelism field must be propagated from descriptor JSON");
            assertEquals(1, fd.agents().size());
        }

        @Test
        @DisplayName("loadFileDescriptor: missing top-level parallelism returns 0 (env-var fallback)")
        void missingTopLevelParallelismReturnsZero() throws Exception {
            String json = """
                {
                  "agents": [
                    {
                      "agentId": "test-agent",
                      "entityType": "account",
                      "entityKeyExtractor": "$.accountId",
                      "ruleset": { "name": "TestModel-op", "path": "/tmp/test.jar" }
                    }
                  ]
                }""";
            AgentBasedDataStreamJob.AgentFileDescriptor fd =
                invokeLoadFileDescriptor(json);
            assertEquals(0, fd.parallelism(),
                "absent top-level parallelism must return 0 so env-var/default fallback kicks in");
        }

        @Test
        @DisplayName("loadFileDescriptor: plain JSON array has parallelism=0 (no wrapper)")
        void plainArrayHasZeroParallelism() throws Exception {
            String json = """
                [
                  {
                    "agentId": "test-agent",
                    "entityType": "account",
                    "entityKeyExtractor": "$.accountId",
                    "ruleset": { "name": "TestModel-op", "path": "/tmp/test.jar" }
                  }
                ]""";
            AgentBasedDataStreamJob.AgentFileDescriptor fd =
                invokeLoadFileDescriptor(json);
            assertEquals(0, fd.parallelism(),
                "plain array format has no top-level parallelism field; must return 0");
            assertEquals(1, fd.agents().size());
        }

        @Test
        @DisplayName("loadFileDescriptor: string value for parallelism is ignored (not an int node)")
        void stringParallelismIgnored() throws Exception {
            String json = """
                {
                  "parallelism": "4",
                  "agents": [
                    {
                      "agentId": "test-agent",
                      "entityType": "account",
                      "entityKeyExtractor": "$.accountId",
                      "ruleset": { "name": "TestModel-op", "path": "/tmp/test.jar" }
                    }
                  ]
                }""";
            AgentBasedDataStreamJob.AgentFileDescriptor fd =
                invokeLoadFileDescriptor(json);
            assertEquals(0, fd.parallelism(),
                "non-integer parallelism node must be rejected (isInt() guard) → returns 0");
        }

        @Test
        @DisplayName("loadFileDescriptor: per-agent parallelism=4 is still read on each AgentDescriptor")
        void perAgentParallelismPreserved() throws Exception {
            String json = """
                {
                  "parallelism": 2,
                  "agents": [
                    {
                      "agentId": "fraud-agent",
                      "entityType": "account",
                      "entityKeyExtractor": "$.accountId",
                      "parallelism": 4,
                      "ruleset": { "name": "FraudModel-op", "path": "/tmp/fraud.jar" }
                    }
                  ]
                }""";
            AgentBasedDataStreamJob.AgentFileDescriptor fd =
                invokeLoadFileDescriptor(json);
            assertEquals(2, fd.parallelism(), "job-level parallelism must be 2");
            assertEquals(4, fd.agents().get(0).getParallelism(),
                "per-agent parallelism=4 must survive parsing");
        }
    }

    // ── StatefulAgentProcessor correctness at parallelism > 1 ─────────────────

    @Nested
    @DisplayName("StatefulAgentProcessor correctness at parallelism > 1")
    class AgentProcessorParallelismSafety {

        /**
         * Simulates N independent subtask instances of {@link StatefulAgentProcessor}
         * (one per Flink subtask at the configured parallelism).  Each instance must
         * produce identical, deterministic output from the same input — verifying no
         * shared mutable state leaks between subtasks.
         *
         * Output type is controlled by {@link com.ibm.di.event.agent.RulesetConfig.OperationConfig}
         * on the descriptor; the rule result must contain a non-empty {@code outputEvents} array.
         */
        @Test
        @DisplayName("N independent subtask instances produce identical, correct output eventType")
        void independentSubtaskInstancesProduceIdenticalOutput() throws Exception {
            int simulatedParallelism = 4;

            AgentDescriptor descriptor = buildTestAgentDescriptor("fraud-alert");
            // Result: non-empty outputEvents array; type comes from OperationConfig
            ObjectNode adsResult = new ObjectMapper().createObjectNode();
            adsResult.set("outputEvents", new ObjectMapper().createArrayNode()
                .add(new ObjectMapper().createObjectNode()
                    .put("decision", "BLOCKED").put("riskScore", 91.0)));

            EventEnvelope first = null;
            for (int subtask = 0; subtask < simulatedParallelism; subtask++) {
                StatefulAgentProcessor processor = new StatefulAgentProcessor(descriptor);
                processor.injectObjectMapper(new ObjectMapper());
                processor.injectRuleEngine(new FakeRunner(adsResult));

                EventEnvelope event = new EventEnvelope();
                event.setEntityId("ACC-P" + subtask);
                event.setEventType("transaction");
                event.setSourceTopic("transactions");
                event.setTimestamp(1_700_000_000_000L + subtask);
                event.setPayload(new ObjectMapper().readTree(
                    "{\"transactionId\":\"TXN-P" + subtask + "\",\"amount\":999.0}"));

                EventEnvelope output = processor.invokeForTest(event, null);

                assertNotNull(output, "subtask " + subtask + " must produce a non-null output");
                assertEquals("fraud-alert", output.getEventType(),
                    "subtask " + subtask + ": eventType must be 'fraud-alert' from OperationConfig");
                assertNotNull(output.getPayload(), "subtask " + subtask + ": payload must not be null");

                if (first == null) {
                    first = output;
                } else {
                    assertEquals(first.getEventType(), output.getEventType(),
                        "subtask " + subtask + " must emit the same eventType as subtask 0");
                }
            }
        }

        @Test
        @DisplayName("No cross-subtask state contamination: each instance starts with a fresh entity state")
        void noSharedMutableState() throws Exception {
            // Use separate descriptors so each processor has its own OperationConfig
            AgentDescriptor descriptorA = buildTestAgentDescriptor("fraud-alert");
            AgentDescriptor descriptorB = buildTestAgentDescriptor("approved-transaction");

            ObjectNode resultA = new ObjectMapper().createObjectNode();
            resultA.set("outputEvents", new ObjectMapper().createArrayNode()
                .add(new ObjectMapper().createObjectNode().put("riskScore", 90)));

            ObjectNode resultB = new ObjectMapper().createObjectNode();
            resultB.set("outputEvents", new ObjectMapper().createArrayNode()
                .add(new ObjectMapper().createObjectNode().put("riskScore", 5)));

            StatefulAgentProcessor processorA = new StatefulAgentProcessor(descriptorA);
            processorA.injectObjectMapper(new ObjectMapper());
            processorA.injectRuleEngine(new FakeRunner(resultA));
            EventEnvelope eventA = makeEnvelope("ACC-A", "transaction", "transactions");
            EventEnvelope outputA = processorA.invokeForTest(eventA, null);

            StatefulAgentProcessor processorB = new StatefulAgentProcessor(descriptorB);
            processorB.injectObjectMapper(new ObjectMapper());
            processorB.injectRuleEngine(new FakeRunner(resultB));
            EventEnvelope eventB = makeEnvelope("ACC-B", "transaction", "transactions");
            EventEnvelope outputB = processorB.invokeForTest(eventB, null);

            assertEquals("fraud-alert", outputA.getEventType(),
                "subtask A output type must be 'fraud-alert'");
            assertEquals("approved-transaction", outputB.getEventType(),
                "subtask B output type must be 'approved-transaction' — no state bleed from A");
        }

        // ── Helpers ───────────────────────────────────────────────────────────

        private AgentDescriptor buildTestAgentDescriptor(String outboundEventType) {
            AgentDescriptor d = new AgentDescriptor();
            d.setAgentId("test-fraud-agent");
            d.setEntityType("account");
            d.setEntityKeyExtractor("$.accountId");
            d.setParallelism(4);
            com.ibm.di.event.agent.RulesetConfig rs =
                new com.ibm.di.event.agent.RulesetConfig("/tmp/fake-fraud.jar");
            // OperationConfig declares the output type; result must use "outputEvents" array
            com.ibm.di.event.agent.RulesetConfig.OperationConfig opCfg =
                new com.ibm.di.event.agent.RulesetConfig.OperationConfig();
            opCfg.setOperation("fraud-op");
            opCfg.setOutbounds(java.util.List.of(
                new com.ibm.di.event.agent.RulesetConfig.OutboundConfig("outputEvents", outboundEventType)));
            rs.setOperations(java.util.List.of(opCfg));
            d.setRuleset(rs);
            d.setSubscriptions(java.util.List.of(
                new com.ibm.di.event.agent.EventSubscription(
                    "transaction",
                    "transactions",
                    "fraud-op",
                    com.ibm.di.event.agent.HorizonConfig.count(10))
            ));
            return d;
        }

        private EventEnvelope makeEnvelope(String entityId, String eventType, String topic) {
            EventEnvelope e = new EventEnvelope();
            e.setEntityId(entityId);
            e.setEventType(eventType);
            e.setSourceTopic(topic);
            e.setTimestamp(System.currentTimeMillis());
            e.setPayload(new ObjectMapper().createObjectNode());
            return e;
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /**
     * Invokes the package-private {@code AgentBasedDataStreamJob.loadFileDescriptor} via
     * a temporary file so the method's classpath-then-filesystem resolution is exercised.
     */
    private static AgentBasedDataStreamJob.AgentFileDescriptor invokeLoadFileDescriptor(
            String json) throws Exception {
        java.nio.file.Path tmp = java.nio.file.Files.createTempFile("agents-test-", ".json");
        try {
            java.nio.file.Files.writeString(tmp, json);
            Method m = AgentBasedDataStreamJob.class.getDeclaredMethod(
                "loadFileDescriptor", String.class);
            m.setAccessible(true);
            return (AgentBasedDataStreamJob.AgentFileDescriptor) m.invoke(
                null, tmp.toAbsolutePath().toString());
        } finally {
            java.nio.file.Files.deleteIfExists(tmp);
        }
    }

    /** Minimal stub runner that returns a fixed result without touching ODM. */
    static class FakeRunner extends RuleEngineRunner {
        private final JsonNode result;
        FakeRunner(JsonNode result) {
            super("stub-op", URI.create("file:///dev/null"));
            this.result = result;
        }
        @Override public JsonNode execute(JsonNode event) { return result; }
        @Override protected void initialize() { /* no-op */ }
        @Override public void close() { /* no-op */ }
    }
}

