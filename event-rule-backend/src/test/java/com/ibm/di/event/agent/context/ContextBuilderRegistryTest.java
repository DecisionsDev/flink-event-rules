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
package com.ibm.di.event.agent.context;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ibm.di.event.agent.AgentDescriptor;
import com.ibm.di.event.agent.AgentState;
import com.ibm.di.event.agent.EventEnvelope;
import com.ibm.di.event.agent.EventSubscription;
import com.ibm.di.event.agent.HorizonConfig;
import com.ibm.di.event.agent.RulesetConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit and regression tests for {@link ContextBuilderRegistry} and {@link GenericContextBuilder}.
 */
@DisplayName("ContextBuilder and Registry Tests")
class ContextBuilderRegistryTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Nested
    @DisplayName("Registry Resolution")
    class RegistryResolution {

        @Test
        @DisplayName("Returns generic builder for null or blank format")
        void returnsGenericForNullOrBlank() {
            ContextBuilder b1 = ContextBuilderRegistry.get(null);
            ContextBuilder b2 = ContextBuilderRegistry.get("");
            ContextBuilder b3 = ContextBuilderRegistry.get("   ");

            assertNotNull(b1);
            assertInstanceOf(GenericContextBuilder.class, b1);
            assertSame(b1, b2);
            assertSame(b1, b3);
        }

        @Test
        @DisplayName("Returns generic builder for unknown format")
        void returnsGenericForUnknown() {
            ContextBuilder b = ContextBuilderRegistry.get("non-existent-format");
            assertNotNull(b);
            assertInstanceOf(GenericContextBuilder.class, b);
        }

        @Test
        @DisplayName("Allows registering custom context builder")
        void registerCustomBuilder() {
            ContextBuilder custom = (descriptor, state, event, mapper) -> {
                ObjectNode node = mapper.createObjectNode();
                node.put("customKey", "customValue");
                return node;
            };
            ContextBuilderRegistry.register("custom-test-format", custom);

            ContextBuilder retrieved = ContextBuilderRegistry.get("custom-test-format");
            assertSame(custom, retrieved);

            EventEnvelope env = EventEnvelope.from("test", "test-topic", 1000L, mapper.createObjectNode());
            JsonNode result = retrieved.build(null, new AgentState(), env, mapper);
            assertEquals("customValue", result.get("customKey").asText());
        }

        @Test
        @DisplayName("Register validation checks")
        void registerValidation() {
            assertThrows(IllegalArgumentException.class, () -> ContextBuilderRegistry.register(null, (d, s, e, m) -> null));
            assertThrows(IllegalArgumentException.class, () -> ContextBuilderRegistry.register("   ", (d, s, e, m) -> null));
            assertThrows(IllegalArgumentException.class, () -> ContextBuilderRegistry.register("valid", null));
        }
    }

    @Nested
    @DisplayName("GenericContextBuilder Behavior")
    class GenericContextBuilderTests {

        private final GenericContextBuilder builder = new GenericContextBuilder();

        @Test
        @DisplayName("Builds complete context structure with currentEvent, entity, and history mapping")
        void buildsFullContext() throws Exception {
            JsonNode payload = mapper.readTree("{\"amount\":100,\"currency\":\"USD\"}");
            EventEnvelope current = EventEnvelope.from("Transaction", "agent-transactions", 1700000000000L, payload);

            AgentState state = new AgentState();
            state.setEntityData(mapper.readTree("{\"status\":\"ACTIVE\",\"balance\":5000}"));

            JsonNode h1Payload = mapper.readTree("{\"action\":\"CREATED\"}");
            state.addEvent("AccountEvent", new AgentState.HistoricalEvent(1699999000000L, h1Payload), HorizonConfig.count(5), 1700000000000L);

            AgentDescriptor descriptor = new AgentDescriptor();
            descriptor.setAgentId("banking-fraud-agent");
            descriptor.setSubscriptions(List.of(
                new EventSubscription("Transaction", "agent-transactions", "EvaluateTransaction", HorizonConfig.count(5))
            ));
            RulesetConfig ruleset = new RulesetConfig("rules.jar");
            RulesetConfig.OperationConfig op = new RulesetConfig.OperationConfig();
            op.setOperation("EvaluateTransaction");
            op.setEntityField("theAccount");
            op.setCurrentEventField("theTransaction");
            op.setHistory(List.of(new RulesetConfig.HistoryFieldMapping("AccountEvent", "pastAccountEvents")));
            ruleset.setOperations(List.of(op));
            descriptor.setRuleset(ruleset);

            JsonNode context = builder.build(descriptor, state, current, mapper);

            assertNotNull(context);
            assertTrue(context.has("theTransaction"));
            assertTrue(context.has("theAccount"));
            assertTrue(context.has("pastAccountEvents"));
            assertTrue(context.has("now"));
            assertTrue(context.has("processingTime"));

            assertEquals(100, context.get("theTransaction").get("amount").asInt());
            assertEquals("ACTIVE", context.get("theAccount").get("status").asText());
            assertEquals(1, context.get("pastAccountEvents").size());
        }

        @Test
        @DisplayName("Handles null descriptor and empty state gracefully")
        void handlesNullAndEmptyInputs() throws Exception {
            JsonNode payload = mapper.readTree("{\"foo\":\"bar\"}");
            EventEnvelope current = EventEnvelope.from("TestEvent", "test-topic", 1700000000000L, payload);

            JsonNode context = builder.build(null, new AgentState(), current, mapper);

            assertNotNull(context);
            assertTrue(context.has("currentEvent"));
            assertTrue(context.has("entity"));
            assertTrue(context.get("entity").isNull());
            assertEquals("bar", context.get("currentEvent").get("foo").asText());
        }

        @Test
        @DisplayName("Spread mode: empty currentEventField spreads payload fields into root context")
        void spreadModeEmptyCurrentEventFieldMergesPayloadToRoot() throws Exception {
            // Payload has multiple context fragments as top-level fields (the retail ShoppingEvent pattern)
            JsonNode payload = mapper.readTree(
                "{\"customerContext\":{\"customerIdentifier\":\"CUST-001\",\"loyaltyStatus\":\"Gold\"}," +
                "\"productContext\":{\"productCategory\":\"Electronics\",\"productPrice\":649.99}," +
                "\"shoppingContext\":{\"eventType\":\"PRODUCT_VIEW\"}}");

            EventEnvelope current = EventEnvelope.from("shoppingContext", "agent-shopping-events",
                1700000000000L, payload);

            AgentDescriptor descriptor = new AgentDescriptor();
            descriptor.setAgentId("retail-agent");
            descriptor.setSubscriptions(List.of(
                new EventSubscription("shoppingContext", "agent-shopping-events",
                    "GeneratedDecisionModel", HorizonConfig.count(10))
            ));
            RulesetConfig ruleset = new RulesetConfig("retail.jar");
            RulesetConfig.OperationConfig op = new RulesetConfig.OperationConfig();
            op.setOperation("GeneratedDecisionModel");
            op.setEntityField("customerContext");
            op.setCurrentEventField(""); // spread mode: empty string
            ruleset.setOperations(List.of(op));
            descriptor.setRuleset(ruleset);

            JsonNode context = builder.build(descriptor, new AgentState(), current, mapper);

            assertNotNull(context);
            // Payload fields spread to root — no "currentEvent" wrapper
            assertTrue(context.path("currentEvent").isMissingNode(),
                "spread mode must not produce a 'currentEvent' key");
            assertTrue(context.has("customerContext"),
                "customerContext must be present at root (spread from payload)");
            assertTrue(context.has("productContext"),
                "productContext must be present at root (spread from payload)");
            assertTrue(context.has("shoppingContext"),
                "shoppingContext must be present at root (spread from payload)");
            assertEquals("CUST-001",
                context.get("customerContext").get("customerIdentifier").asText(),
                "customerContext payload content must be preserved");
            assertEquals("Electronics",
                context.get("productContext").get("productCategory").asText(),
                "productContext payload content must be preserved");
            assertEquals("PRODUCT_VIEW",
                context.get("shoppingContext").get("eventType").asText(),
                "shoppingContext payload content must be preserved");
            // Temporal fields are still present
            assertTrue(context.has("now"), "now must still be present in spread mode");
            assertTrue(context.has("processingTime"),
                "processingTime must still be present in spread mode");
        }

        @Test
        @DisplayName("Spread mode: persisted entity takes precedence over same-named field from payload spread")
        void spreadModeEntityTakesPrecedenceOverPayloadField() throws Exception {
            // Payload has a minimal customerContext (just the identifier)
            JsonNode payload = mapper.readTree(
                "{\"customerContext\":{\"customerIdentifier\":\"CUST-001\"}," +
                "\"shoppingContext\":{\"eventType\":\"PRODUCT_VIEW\"}}");

            EventEnvelope current = EventEnvelope.from("shoppingContext", "agent-shopping-events",
                1700000000000L, payload);

            // Entity state has the full customer profile (persisted from a prior customerContext event)
            AgentState state = new AgentState();
            state.setEntityData(mapper.readTree(
                "{\"customerIdentifier\":\"CUST-001\",\"loyaltyStatus\":\"Gold\"," +
                "\"purchaseHistory\":[\"headphones\",\"laptop\"]}"));

            AgentDescriptor descriptor = new AgentDescriptor();
            descriptor.setAgentId("retail-agent");
            descriptor.setSubscriptions(List.of(
                new EventSubscription("shoppingContext", "agent-shopping-events",
                    "GeneratedDecisionModel", HorizonConfig.count(10))
            ));
            RulesetConfig ruleset = new RulesetConfig("retail.jar");
            RulesetConfig.OperationConfig op = new RulesetConfig.OperationConfig();
            op.setOperation("GeneratedDecisionModel");
            op.setEntityField("customerContext"); // entity written under "customerContext" key
            op.setCurrentEventField(""); // spread mode
            ruleset.setOperations(List.of(op));
            descriptor.setRuleset(ruleset);

            JsonNode context = builder.build(descriptor, state, current, mapper);

            // The entity (full profile) must overwrite the minimal customerContext from the spread
            JsonNode customerCtx = context.get("customerContext");
            assertNotNull(customerCtx, "customerContext must be present");
            assertEquals("Gold", customerCtx.get("loyaltyStatus").asText(),
                "persisted entity loyaltyStatus must take precedence over payload spread");
            assertTrue(customerCtx.has("purchaseHistory"),
                "persisted entity purchaseHistory must be present (entity takes precedence)");
        }

        @Test
        @DisplayName("Spread mode: null entity in state leaves payload spread customerContext intact")
        void spreadModeNullEntityDoesNotOverwritePayloadField() throws Exception {
            // Payload has a full customerContext in the shopping event
            JsonNode payload = mapper.readTree(
                "{\"customerContext\":{\"customerIdentifier\":\"CUST-NEW\"," +
                "\"loyaltyStatus\":\"Gold\"}," +
                "\"shoppingContext\":{\"eventType\":\"PRODUCT_VIEW\"}}");

            EventEnvelope current = EventEnvelope.from("shoppingContext", "agent-shopping-events",
                1700000000000L, payload);

            // No prior entity state — new customer
            AgentState state = new AgentState(); // entityData is null

            AgentDescriptor descriptor = new AgentDescriptor();
            descriptor.setAgentId("retail-agent");
            descriptor.setSubscriptions(List.of(
                new EventSubscription("shoppingContext", "agent-shopping-events",
                    "GeneratedDecisionModel", HorizonConfig.count(10))
            ));
            RulesetConfig ruleset = new RulesetConfig("retail.jar");
            RulesetConfig.OperationConfig op = new RulesetConfig.OperationConfig();
            op.setOperation("GeneratedDecisionModel");
            op.setEntityField("customerContext");
            op.setCurrentEventField(""); // spread mode
            ruleset.setOperations(List.of(op));
            descriptor.setRuleset(ruleset);

            JsonNode context = builder.build(descriptor, state, current, mapper);

            // With null entity and spread mode, customerContext comes from payload spread (not null)
            JsonNode customerCtx = context.get("customerContext");
            assertNotNull(customerCtx, "customerContext from payload spread must be present");
            assertFalse(customerCtx.isNull(),
                "customerContext must not be null — spread populated it from the payload");
            assertEquals("Gold", customerCtx.get("loyaltyStatus").asText(),
                "customerContext loyalty from payload must be intact when entity is null");
        }
    }
}
