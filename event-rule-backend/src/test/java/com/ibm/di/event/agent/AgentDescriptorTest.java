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

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the Agent descriptor model classes:
 * {@link AgentDescriptor}, {@link EventSubscription}, {@link HorizonConfig},
 * {@link OutputConfig}, {@link RulesetConfig}, and {@link StateConfig}.
 *
 * <p>Covers JSON round-trip parsing, field defaults, derived helpers, and
 * the in-memory object model used by {@link StatefulAgentProcessor}.
 */
@DisplayName("Agent descriptor model tests")
class AgentDescriptorTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // ── AgentDescriptor JSON parsing ──────────────────────────────────────────

    @Nested
    @DisplayName("AgentDescriptor — JSON parsing")
    class AgentDescriptorParsing {

        @Test
        @DisplayName("Full descriptor parses all top-level fields")
        void fullDescriptorParsed() throws Exception {
            String json = """
                {
                  "agentId": "order-scoring-agent",
                  "name": "Order Scoring Agent",
                  "entityType": "order",
                  "entityKeyExtractor": "$.orderId",
                  "parallelism": 4,
                  "subscriptions": [],
                  "outputs": [],
                  "ruleset": { "path": "/tmp/scoring.jar" },
                  "stateConfig": { "ttl": "30d", "backend": "rocksdb" }
                }""";

            AgentDescriptor d = MAPPER.readValue(json, AgentDescriptor.class);

            assertEquals("order-scoring-agent", d.getAgentId());
            assertEquals("Order Scoring Agent",  d.getName());
            assertEquals("order",                d.getEntityType());
            assertEquals("$.orderId",            d.getEntityKeyExtractor());
            assertEquals(4,                         d.getParallelism());
            assertNotNull(d.getRuleset());
            assertNotNull(d.getStateConfig());
        }

        @Test
        @DisplayName("Missing optional fields default cleanly (null / 0)")
        void missingOptionalFieldsAreNull() throws Exception {
            String json = """
                {
                  "agentId": "minimal-agent",
                  "entityType": "order",
                  "ruleset": { "path": "/tmp/op.jar" }
                }""";

            AgentDescriptor d = MAPPER.readValue(json, AgentDescriptor.class);

            assertEquals("minimal-agent", d.getAgentId());
            assertNull(d.getName(),               "name must default to null");
            assertNull(d.getEntityKeyExtractor(), "entityKeyExtractor must default to null");
            assertEquals(0, d.getParallelism(),   "parallelism must default to 0 (unset)");
            assertNull(d.getSubscriptions(),      "subscriptions must default to null");
            assertNull(d.getOutputs(),            "outputs must default to null");
            assertNull(d.getStateConfig(),        "stateConfig must default to null");
        }

        @Test
        @DisplayName("displayName() returns name when set, agentId otherwise")
        void displayNameFallback() throws Exception {
            AgentDescriptor withName = MAPPER.readValue(
                "{\"agentId\":\"a\",\"name\":\"My Agent\",\"ruleset\":{\"path\":\"/tmp/a.jar\"}}",
                AgentDescriptor.class);
            assertEquals("My Agent", withName.displayName());

            AgentDescriptor withoutName = MAPPER.readValue(
                "{\"agentId\":\"b\",\"ruleset\":{\"path\":\"/tmp/b.jar\"}}",
                AgentDescriptor.class);
            assertEquals("b", withoutName.displayName());
        }

        @Test
        @DisplayName("toString() includes agentId and entityType")
        void toStringContainsKeyFields() {
            AgentDescriptor d = new AgentDescriptor();
            d.setAgentId("test-id");
            d.setEntityType("customer");
            String s = d.toString();
            assertTrue(s.contains("test-id"),   "toString must contain agentId");
            assertTrue(s.contains("customer"),  "toString must contain entityType");
        }
    }

    // ── EventSubscription ─────────────────────────────────────────────────────

    @Nested
    @DisplayName("EventSubscription — JSON parsing and defaults")
    class EventSubscriptionTests {

        @Test
        @DisplayName("Full subscription parses eventType, topic, operation, horizon")
        void fullSubscriptionParsed() throws Exception {
            String json = """
                {
                  "eventType": "transaction",
                  "topic": "transactions",
                  "operation": "fraudDetectionService_transaction_op",
                  "horizon": { "type": "count", "value": 50 }
                }""";

            EventSubscription sub = MAPPER.readValue(json, EventSubscription.class);

            assertEquals("transaction",                  sub.getEventType());
            assertEquals("transactions",                 sub.getTopic());
            assertEquals("fraudDetectionService_transaction_op", sub.getOperation());
            assertTrue(sub.hasOperation());
            assertNotNull(sub.getHorizon());
            assertEquals("count", sub.getHorizon().getType());
            assertEquals(50,      sub.getHorizon().getValue());
        }

        @Test
        @DisplayName("Subscription without operation has null operation and hasOperation() returns false")
        void subscriptionWithoutOperationHasNullOperation() throws Exception {
            String json = "{\"eventType\":\"order\",\"topic\":\"orders\"," +
                          "\"horizon\":{\"type\":\"count\",\"value\":5}}";
            EventSubscription sub = MAPPER.readValue(json, EventSubscription.class);
            assertNull(sub.getOperation(), "operation must be null when absent from JSON");
            assertFalse(sub.hasOperation(), "hasOperation() must return false when operation is null");
        }

        @Test
        @DisplayName("Missing horizon defaults to count(0) — no history")
        void missingHorizonDefaultsToCountZero() throws Exception {
            String json = "{\"eventType\":\"order\",\"topic\":\"orders\",\"operation\":\"svc_op\"}";
            EventSubscription sub = MAPPER.readValue(json, EventSubscription.class);

            assertNotNull(sub.getHorizon(), "default horizon must not be null");
            assertEquals("count", sub.getHorizon().getType());
            assertEquals(0,       sub.getHorizon().getValue());
        }

        @Test
        @DisplayName("setHorizon(null) resets to count(0)")
        void setNullHorizonResetsToDefault() {
            EventSubscription sub = new EventSubscription(
                "txn", "t", "txn-op", HorizonConfig.count(10));
            sub.setHorizon(null);
            assertNotNull(sub.getHorizon(), "horizon must never be null after setHorizon(null)");
            assertEquals("count", sub.getHorizon().getType());
            assertEquals(0, sub.getHorizon().getValue());
        }
    }

    // ── HorizonConfig ─────────────────────────────────────────────────────────

    @Nested
    @DisplayName("HorizonConfig — JSON parsing")
    class HorizonConfigTests {

        @Test
        @DisplayName("Count horizon parses type and value")
        void countHorizonParsed() throws Exception {
            String json = "{\"type\":\"count\",\"value\":100}";
            HorizonConfig h = MAPPER.readValue(json, HorizonConfig.class);
            assertEquals("count", h.getType());
            assertEquals(100,     h.getValue());
            assertEquals(100,     h.effectiveCountLimit());
        }

        @Test
        @DisplayName("Time horizon parses type and timeLimit")
        void timeHorizonParsed() throws Exception {
            String json = "{\"type\":\"time\",\"timeLimit\":\"24h\"}";
            HorizonConfig h = MAPPER.readValue(json, HorizonConfig.class);
            assertEquals("time", h.getType());
            assertEquals("24h",  h.getTimeLimit());
            assertEquals(86_400_000L, h.timeLimitMs());
        }

        @Test
        @DisplayName("Hybrid horizon parses countLimit and timeLimit")
        void hybridHorizonParsed() throws Exception {
            String json = "{\"type\":\"hybrid\",\"countLimit\":50,\"timeLimit\":\"7d\"}";
            HorizonConfig h = MAPPER.readValue(json, HorizonConfig.class);
            assertEquals("hybrid",         h.getType());
            assertEquals(50,               h.getCountLimit());
            assertEquals("7d",             h.getTimeLimit());
            assertEquals(50,               h.effectiveCountLimit());
            assertEquals(604_800_000L,     h.timeLimitMs());  // 7 * 86400 * 1000
        }

        @Test
        @DisplayName("effectiveCountLimit: countLimit takes precedence over value for hybrid")
        void effectiveCountLimitPreference() {
            HorizonConfig h = new HorizonConfig();
            h.setType("hybrid");
            h.setValue(5);
            h.setCountLimit(20);
            assertEquals(20, h.effectiveCountLimit(),
                "countLimit must win over value when both are set");
        }

        @Test
        @DisplayName("effectiveCountLimit: falls back to value when countLimit=0")
        void effectiveCountLimitFallback() {
            HorizonConfig h = new HorizonConfig();
            h.setType("count");
            h.setValue(10);
            h.setCountLimit(0);
            assertEquals(10, h.effectiveCountLimit());
        }

        @Test
        @DisplayName("description field is preserved in round-trip")
        void descriptionPreserved() throws Exception {
            String json = "{\"type\":\"count\",\"value\":5,\"description\":\"Keep last 5\"}";
            HorizonConfig h = MAPPER.readValue(json, HorizonConfig.class);
            assertEquals("Keep last 5", h.getDescription());
        }
    }

    // ── OutputConfig ──────────────────────────────────────────────────────────

    @Nested
    @DisplayName("OutputConfig — JSON parsing")
    class OutputConfigTests {

        @Test
        @DisplayName("Full output config parses eventType, topic, description")
        void fullOutputConfigParsed() throws Exception {
            String json = """
                {
                  "eventType": "fraud-alert",
                  "topic": "fraud-alerts",
                  "description": "High-risk transactions"
                }""";

            OutputConfig o = MAPPER.readValue(json, OutputConfig.class);

            assertEquals("fraud-alert",          o.getEventType());
            assertEquals("fraud-alerts",         o.getTopic());
            assertEquals("High-risk transactions", o.getDescription());
        }

        @Test
        @DisplayName("Two-arg constructor sets eventType and topic")
        void twoArgConstructor() {
            OutputConfig o = new OutputConfig("approved", "approved-txns");
            assertEquals("approved",      o.getEventType());
            assertEquals("approved-txns", o.getTopic());
            assertNull(o.getDescription());
        }

        @Test
        @DisplayName("toString contains eventType and topic")
        void toStringShape() {
            OutputConfig o = new OutputConfig("fraud-alert", "fraud-alerts");
            String s = o.toString();
            assertTrue(s.contains("fraud-alert"),  "toString must contain eventType");
            assertTrue(s.contains("fraud-alerts"), "toString must contain topic");
        }
    }

    // ── RulesetConfig ─────────────────────────────────────────────────────────

    @Nested
    @DisplayName("RulesetConfig — JSON parsing")
    class RulesetConfigTests {

        @Test
        @DisplayName("Parses path field")
        void parsesPath() throws Exception {
            String json = "{\"path\":\"/opt/rulesets/fraud.jar\"}";
            RulesetConfig r = MAPPER.readValue(json, RulesetConfig.class);
            assertEquals("/opt/rulesets/fraud.jar", r.getPath());
        }

    }

    // ── StateConfig ───────────────────────────────────────────────────────────

    @Nested
    @DisplayName("StateConfig — JSON parsing and ttlDuration")
    class StateConfigTests {

        @Test
        @DisplayName("Parses ttl, backend, checkpointInterval")
        void fullStateConfigParsed() throws Exception {
            String json = """
                {
                  "ttl": "30d",
                  "backend": "rocksdb",
                  "checkpointInterval": "1m"
                }""";

            StateConfig sc = MAPPER.readValue(json, StateConfig.class);
            assertEquals("30d",     sc.getTtl());
            assertEquals("rocksdb", sc.getBackend());
            assertEquals("1m",      sc.getCheckpointInterval());
        }

        @Test
        @DisplayName("ttlDuration: '30d' → 30 days")
        void ttlDurationThirtyDays() throws Exception {
            StateConfig sc = MAPPER.readValue("{\"ttl\":\"30d\"}", StateConfig.class);
            assertEquals(java.time.Duration.ofDays(30), sc.ttlDuration());
        }

        @Test
        @DisplayName("ttlDuration: '24h' → 24 hours")
        void ttlDurationTwentyFourHours() throws Exception {
            StateConfig sc = MAPPER.readValue("{\"ttl\":\"24h\"}", StateConfig.class);
            assertEquals(java.time.Duration.ofHours(24), sc.ttlDuration());
        }

        @Test
        @DisplayName("ttlDuration: null ttl → null Duration")
        void ttlDurationNullTtl() {
            StateConfig sc = new StateConfig();
            assertNull(sc.ttlDuration(), "Null TTL must return null Duration");
        }

        @Test
        @DisplayName("ttlDuration: blank ttl → null Duration")
        void ttlDurationBlankTtl() {
            StateConfig sc = new StateConfig();
            sc.setTtl("   ");
            assertNull(sc.ttlDuration(), "Blank TTL must return null Duration");
        }
    }

    // ── Full descriptor round-trip ────────────────────────────────────────────

    @Nested
    @DisplayName("Full agent descriptor round-trip (banking sample)")
    class BankingSampleRoundTrip {

        @Test
        @DisplayName("Banking agent descriptor parsed from inline JSON matches expected structure")
        void bankingDescriptorFromJson() throws Exception {
            // Note: account-created uses operation="" intentionally — this is a schema/round-trip
            // test, not a functional test. The real banking agent (BankingAgentIntegrationTest)
            // uses "fraudDetectionService_createdaccounts" because the rule engine must be invoked
            // to seed entityData from __entityState. Do NOT "correct" "" to match that value here.
            String json = """
                {
                  "agentId": "fraud-detection-agent",
                  "name": "Banking Fraud Detection Agent",
                  "entityType": "account",
                  "entityKeyExtractor": "$.accountId",
                  "subscriptions": [
                    {
                      "eventType": "transaction",
                      "topic": "transactions",
                      "operation": "fraudDetectionService_transaction_op",
                      "horizon": { "type": "hybrid", "countLimit": 100, "timeLimit": "24h" }
                    },
                    {
                      "eventType": "account-created",
                      "topic": "account-created-events",
                      "operation": "",
                      "horizon": { "type": "count", "value": 1 }
                    }
                  ],
                  "ruleset": {
                    "path": "fraud.jar"
                  },
                  "outputs": [
                    { "eventType": "fraud-alert",         "topic": "fraud-alerts" },
                    { "eventType": "approved-transaction", "topic": "approved-transactions" }
                  ],
                  "stateConfig": { "ttl": "30d", "backend": "rocksdb" },
                  "parallelism": 4
                }""";

            AgentDescriptor d = MAPPER.readValue(json, AgentDescriptor.class);

            assertEquals("fraud-detection-agent",             d.getAgentId());
            assertEquals("account",                           d.getEntityType());
            assertEquals("$.accountId",                       d.getEntityKeyExtractor());
            assertEquals(4,                                   d.getParallelism());
            assertEquals(2,                                   d.getSubscriptions().size());
            assertEquals("transaction",                       d.getSubscriptions().get(0).getEventType());
            assertEquals("fraudDetectionService_transaction_op", d.getSubscriptions().get(0).getOperation());
            assertEquals("hybrid",                            d.getSubscriptions().get(0).getHorizon().getType());
            assertEquals(100,                                 d.getSubscriptions().get(0).getHorizon().getCountLimit());
            assertEquals("24h",                               d.getSubscriptions().get(0).getHorizon().getTimeLimit());
            assertEquals("account-created",                   d.getSubscriptions().get(1).getEventType());
            assertEquals("",                                   d.getSubscriptions().get(1).getOperation());
            assertEquals(1,                                   d.getSubscriptions().get(1).getHorizon().getValue());
            assertEquals("fraud.jar",                         d.getRuleset().getPath());
            assertEquals(2,                                   d.getOutputs().size());
            assertEquals("fraud-alert",                       d.getOutputs().get(0).getEventType());
            assertEquals("fraud-alerts",                      d.getOutputs().get(0).getTopic());
            assertEquals("30d",                               d.getStateConfig().getTtl());
            assertEquals("rocksdb",                           d.getStateConfig().getBackend());
        }
    }
}

