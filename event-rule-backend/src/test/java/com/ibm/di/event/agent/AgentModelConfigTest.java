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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Functional and regression tests for Agent domain model objects:
 * {@link EventSubscription}, {@link OutputConfig}, {@link RulesetConfig}, and {@link StateConfig}.
 */
@DisplayName("Agent Model Configuration Objects Tests")
class AgentModelConfigTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Nested
    @DisplayName("EventSubscription Tests")
    class EventSubscriptionTests {

        @Test
        @DisplayName("Constructors, getters, setters, and defaults")
        void subscriptionConstructorsAndAccessors() {
            EventSubscription sub1 = new EventSubscription();
            sub1.setEventType("Transaction");
            sub1.setTopic("agent-transactions");
            sub1.setOperation("EvaluateTransaction");
            sub1.setHorizon(HorizonConfig.count(5));

            assertEquals("Transaction", sub1.getEventType());
            assertEquals("agent-transactions", sub1.getTopic());
            assertEquals("EvaluateTransaction", sub1.getOperation());
            assertTrue(sub1.hasOperation());
            assertEquals(5, sub1.getHorizon().effectiveCountLimit());

            EventSubscription sub2 = new EventSubscription("Alert", "agent-alerts", HorizonConfig.time("1h"));
            assertEquals("Alert", sub2.getEventType());
            assertEquals("agent-alerts", sub2.getTopic());
            assertNull(sub2.getOperation());
            assertFalse(sub2.hasOperation());
            assertEquals("1h", sub2.getHorizon().getTimeLimit());

            EventSubscription sub3 = new EventSubscription("Event", "topic", "Op", null);
            assertNotNull(sub3.getHorizon());
            assertEquals(0, sub3.getHorizon().effectiveCountLimit());

            assertTrue(sub1.toString().contains("Transaction"));
        }

        @Test
        @DisplayName("JSON round-trip serialization")
        void jsonRoundTrip() throws Exception {
            EventSubscription sub = new EventSubscription("OrderEvent", "orders", "ProcessOrder", HorizonConfig.count(10));
            String json = mapper.writeValueAsString(sub);
            EventSubscription restored = mapper.readValue(json, EventSubscription.class);

            assertEquals(sub.getEventType(), restored.getEventType());
            assertEquals(sub.getTopic(), restored.getTopic());
            assertEquals(sub.getOperation(), restored.getOperation());
            assertEquals(sub.getHorizon().effectiveCountLimit(), restored.getHorizon().effectiveCountLimit());
        }
    }

    @Nested
    @DisplayName("OutputConfig Tests")
    class OutputConfigTests {

        @Test
        @DisplayName("Constructors, getters, setters, and toString")
        void outputConfigAccessors() {
            OutputConfig out = new OutputConfig("FraudAlert", "agent-fraud-alerts");
            out.setDescription("Emitted when fraud score > 50");

            assertEquals("FraudAlert", out.getEventType());
            assertEquals("agent-fraud-alerts", out.getTopic());
            assertEquals("Emitted when fraud score > 50", out.getDescription());
            assertTrue(out.toString().contains("FraudAlert"));
        }

        @Test
        @DisplayName("JSON serialization round-trip")
        void jsonRoundTrip() throws Exception {
            OutputConfig out = new OutputConfig("ATCAlert", "agent-atc-alerts");
            out.setDescription("Air traffic control warning");
            String json = mapper.writeValueAsString(out);
            OutputConfig restored = mapper.readValue(json, OutputConfig.class);

            assertEquals(out.getEventType(), restored.getEventType());
            assertEquals(out.getTopic(), restored.getTopic());
            assertEquals(out.getDescription(), restored.getDescription());
        }
    }

    @Nested
    @DisplayName("RulesetConfig & Nested Configs Tests")
    class RulesetConfigTests {

        @Test
        @DisplayName("RulesetConfig operations lookup and broadcast reload check")
        void rulesetConfigOperationsAndBroadcast() {
            RulesetConfig config = new RulesetConfig("/opt/rules/service.jar");
            assertFalse(config.hasBroadcastReload());

            config.setArchiveControlTopic("agent-archive-control");
            assertTrue(config.hasBroadcastReload());

            RulesetConfig.OperationConfig op1 = new RulesetConfig.OperationConfig();
            op1.setOperation("EvaluateTxn");
            op1.setEntityField("theAccount");
            op1.setCurrentEventField("theTransaction");

            RulesetConfig.HistoryFieldMapping h1 = new RulesetConfig.HistoryFieldMapping("Txn", "pastTxns");
            assertEquals("Txn", h1.getEventType());
            assertEquals("pastTxns", h1.getField());
            assertTrue(h1.toString().contains("pastTxns"));

            op1.setHistory(List.of(h1));

            RulesetConfig.OutboundConfig out1 = new RulesetConfig.OutboundConfig("theAlerts", "Alert");
            assertEquals("theAlerts", out1.getField());
            assertEquals("Alert", out1.getEventType());
            assertTrue(out1.toString().contains("theAlerts"));

            op1.setOutbounds(List.of(out1));

            config.setOperations(List.of(op1));

            assertNotNull(config.findOperation("EvaluateTxn"));
            assertEquals("theAccount", config.findOperation("EvaluateTxn").getEntityField());
            assertEquals("theTransaction", config.findOperation("EvaluateTxn").getCurrentEventField());
            assertEquals(1, config.findOperation("EvaluateTxn").getHistory().size());
            assertEquals(1, config.findOperation("EvaluateTxn").getOutbounds().size());

            assertNull(config.findOperation("NonExistentOp"));
            assertNull(config.findOperation(null));

            assertTrue(config.toString().contains("/opt/rules/service.jar"));
            assertTrue(op1.toString().contains("EvaluateTxn"));
        }

        @Test
        @DisplayName("JSON round-trip for full RulesetConfig hierarchy")
        void jsonHierarchyRoundTrip() throws Exception {
            RulesetConfig config = new RulesetConfig("samples/rules.jar");
            config.setArchiveControlTopic("control-topic");

            RulesetConfig.OperationConfig op = new RulesetConfig.OperationConfig();
            op.setOperation("Process");
            op.setEntityField("entity");
            op.setCurrentEventField("event");
            op.setHistory(List.of(new RulesetConfig.HistoryFieldMapping("E1", "h1")));
            op.setOutbounds(List.of(new RulesetConfig.OutboundConfig("o1", "Out1")));

            config.setOperations(List.of(op));

            String json = mapper.writeValueAsString(config);
            RulesetConfig restored = mapper.readValue(json, RulesetConfig.class);

            assertEquals(config.getPath(), restored.getPath());
            assertEquals(config.getArchiveControlTopic(), restored.getArchiveControlTopic());
            assertEquals(1, restored.getOperations().size());
            assertEquals("Process", restored.getOperations().get(0).getOperation());
            assertEquals("h1", restored.getOperations().get(0).getHistory().get(0).getField());
            assertEquals("Out1", restored.getOperations().get(0).getOutbounds().get(0).getEventType());
        }
    }

    @Nested
    @DisplayName("StateConfig Tests")
    class StateConfigTests {

        @Test
        @DisplayName("TTL parsing, changeLogTopic check, and toString")
        void stateConfigMethods() {
            StateConfig sc = new StateConfig();
            assertNull(sc.ttlDuration());
            assertFalse(sc.hasChangeLogTopic());

            sc.setTtl("30m");
            assertEquals(Duration.ofMinutes(30), sc.ttlDuration());

            sc.setTtl("  ");
            assertNull(sc.ttlDuration());

            sc.setTtl("1d");
            assertEquals(Duration.ofDays(1), sc.ttlDuration());

            sc.setBackend("rocksdb");
            assertEquals("rocksdb", sc.getBackend());

            sc.setCheckpointInterval("10s");
            assertEquals("10s", sc.getCheckpointInterval());

            sc.setChangeLogTopic("agent-changelog");
            assertTrue(sc.hasChangeLogTopic());
            assertEquals("agent-changelog", sc.getChangeLogTopic());

            assertTrue(sc.toString().contains("rocksdb"));
            assertTrue(sc.toString().contains("agent-changelog"));
        }

        @Test
        @DisplayName("JSON round-trip for StateConfig")
        void jsonRoundTrip() throws Exception {
            StateConfig sc = new StateConfig();
            sc.setTtl("24h");
            sc.setBackend("heap");
            sc.setCheckpointInterval("5s");
            sc.setChangeLogTopic("entity-changelog");

            String json = mapper.writeValueAsString(sc);
            StateConfig restored = mapper.readValue(json, StateConfig.class);

            assertEquals(sc.getTtl(), restored.getTtl());
            assertEquals(sc.getBackend(), restored.getBackend());
            assertEquals(sc.getCheckpointInterval(), restored.getCheckpointInterval());
            assertEquals(sc.getChangeLogTopic(), restored.getChangeLogTopic());
            assertEquals(Duration.ofHours(24), restored.ttlDuration());
        }
    }
}
