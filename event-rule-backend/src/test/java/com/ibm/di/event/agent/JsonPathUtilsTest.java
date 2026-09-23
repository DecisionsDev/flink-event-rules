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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link JsonPathUtils}.
 *
 * <p>These tests verify the single canonical entity-ID extraction implementation.
 * Both {@link AgentBasedDataStreamJob.EnvelopeDeserializer} and
 * {@link AgentChainBuilder.DeferredChainedInputs} delegate to this utility, so a
 * failure here would surface in both the Kafka and chain paths simultaneously.
 */
@DisplayName("JsonPathUtils — extractEntityId")
class JsonPathUtilsTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // ── Happy path ────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("Happy path — field present")
    class HappyPath {

        @Test
        @DisplayName("Top-level field with '$.' prefix")
        void topLevelWithPrefix() throws Exception {
            JsonNode payload = MAPPER.readTree("{\"accountId\":\"ACC-001\",\"amount\":500}");
            assertEquals("ACC-001", JsonPathUtils.extractEntityId(payload, "$.accountId"));
        }

        @Test
        @DisplayName("Top-level field without '$.' prefix is auto-normalised")
        void topLevelNoDollar() throws Exception {
            JsonNode payload = MAPPER.readTree("{\"orderId\":\"ORD-42\"}");
            assertEquals("ORD-42", JsonPathUtils.extractEntityId(payload, "orderId"));
        }

        @Test
        @DisplayName("Two-level nesting: '$.customer.id'")
        void twoLevelNesting() throws Exception {
            JsonNode payload = MAPPER.readTree("{\"customer\":{\"id\":\"CUST-99\"}}");
            assertEquals("CUST-99", JsonPathUtils.extractEntityId(payload, "$.customer.id"));
        }

        @Test
        @DisplayName("Three-level nesting: '$.a.b.c'")
        void threeLevelNesting() throws Exception {
            JsonNode payload = MAPPER.readTree("{\"a\":{\"b\":{\"c\":\"DEEP-42\"}}}");
            assertEquals("DEEP-42", JsonPathUtils.extractEntityId(payload, "$.a.b.c"));
        }

        @Test
        @DisplayName("Cross-domain extractor: '$.airportCode' (ATC chain use case)")
        void airportCodeExtractor() throws Exception {
            JsonNode payload = MAPPER.readTree(
                "{\"aircraftId\":\"XZF-435\",\"airportCode\":\"CDG\",\"status\":\"LANDING\"}");
            assertEquals("CDG", JsonPathUtils.extractEntityId(payload, "$.airportCode"));
        }

        @Test
        @DisplayName("Numeric value is coerced to string")
        void numericValue() throws Exception {
            JsonNode payload = MAPPER.readTree("{\"deviceId\":12345}");
            assertEquals("12345", JsonPathUtils.extractEntityId(payload, "$.deviceId"));
        }
    }

    // ── Returns "unknown" ─────────────────────────────────────────────────────

    @Nested
    @DisplayName("Returns 'unknown' on guard conditions")
    class ReturnsUnknown {

        @Test
        @DisplayName("Null extractor → 'unknown'")
        void nullExtractor() throws Exception {
            JsonNode payload = MAPPER.readTree("{\"accountId\":\"ACC-001\"}");
            assertEquals("unknown", JsonPathUtils.extractEntityId(payload, null));
        }

        @Test
        @DisplayName("Blank extractor → 'unknown'")
        void blankExtractor() throws Exception {
            JsonNode payload = MAPPER.readTree("{\"accountId\":\"ACC-001\"}");
            assertEquals("unknown", JsonPathUtils.extractEntityId(payload, "   "));
        }

        @Test
        @DisplayName("Null payload → 'unknown'")
        void nullPayload() {
            assertEquals("unknown", JsonPathUtils.extractEntityId(null, "$.accountId"));
        }

        @Test
        @DisplayName("Field absent from payload → 'unknown'")
        void fieldAbsent() throws Exception {
            JsonNode payload = MAPPER.readTree("{\"other\":\"value\"}");
            assertEquals("unknown", JsonPathUtils.extractEntityId(payload, "$.accountId"));
        }

        @Test
        @DisplayName("Null JSON value at extractor path → 'unknown'")
        void nullJsonValue() throws Exception {
            JsonNode payload = MAPPER.readTree("{\"accountId\":null}");
            // JsonPath.read returns null for JSON null; extractEntityId maps null → "unknown"
            assertEquals("unknown", JsonPathUtils.extractEntityId(payload, "$.accountId"));
        }

        @Test
        @DisplayName("Empty extractor string → 'unknown'")
        void emptyExtractor() throws Exception {
            JsonNode payload = MAPPER.readTree("{\"accountId\":\"ACC-001\"}");
            assertEquals("unknown", JsonPathUtils.extractEntityId(payload, ""));
        }
    }

    // ── Consistency with EnvelopeDeserializer ─────────────────────────────────

    @Nested
    @DisplayName("Consistency — JsonPathUtils equals EnvelopeDeserializer.extractEntityId")
    class ConsistencyWithDeserializer {

        /**
         * The Kafka path (EnvelopeDeserializer) and the chain path (DeferredChainedInputs)
         * both delegate to JsonPathUtils.  This test is the regression guard for Weakness 5:
         * both callsites must produce identical entity IDs for the same payload+extractor.
         */
        @Test
        @DisplayName("Top-level field: both paths produce identical entityId")
        void topLevelFieldConsistent() throws Exception {
            JsonNode payload = MAPPER.readTree("{\"accountId\":\"ACC-123\"}");
            String fromUtils       = JsonPathUtils.extractEntityId(payload, "$.accountId");
            String fromDeserializer = AgentBasedDataStreamJob.EnvelopeDeserializer
                .extractEntityId(payload, "$.accountId");
            assertEquals(fromUtils, fromDeserializer,
                "JsonPathUtils and EnvelopeDeserializer must produce the same entityId");
        }

        @Test
        @DisplayName("Missing field: both paths return 'unknown'")
        void missingFieldConsistent() throws Exception {
            JsonNode payload = MAPPER.readTree("{\"other\":\"x\"}");
            String fromUtils       = JsonPathUtils.extractEntityId(payload, "$.accountId");
            String fromDeserializer = AgentBasedDataStreamJob.EnvelopeDeserializer
                .extractEntityId(payload, "$.accountId");
            assertEquals(fromUtils, fromDeserializer,
                "Both paths must return 'unknown' for a missing field");
        }

        @Test
        @DisplayName("Cross-domain ATC extractor: both paths produce identical entityId")
        void crossDomainExtractorConsistent() throws Exception {
            JsonNode payload = MAPPER.readTree(
                "{\"aircraftId\":\"XZF-435\",\"airportCode\":\"CDG\"}");
            String fromUtils       = JsonPathUtils.extractEntityId(payload, "$.airportCode");
            String fromDeserializer = AgentBasedDataStreamJob.EnvelopeDeserializer
                .extractEntityId(payload, "$.airportCode");
            assertEquals("CDG", fromUtils);
            assertEquals(fromUtils, fromDeserializer,
                "Both extraction paths must produce 'CDG' for the ATC cross-domain case");
        }
    }
}
