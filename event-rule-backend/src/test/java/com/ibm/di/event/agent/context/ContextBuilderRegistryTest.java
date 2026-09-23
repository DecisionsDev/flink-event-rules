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
    }
}
