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
package com.ibm.di.event.sql;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.di.event.agent.EventEnvelope;
import com.ibm.di.event.agent.SqlEnrichmentConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlEnrichmentRunnerTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    @DisplayName("Enriches FLIGHTOPEN with CSV reference data and passes through FLIGHTDELAYED")
    void testFlightEventSqlEnrichment() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String schemaPath = "samples/airline-agent/schemas/flight-event.schema.json";
        String schemaJson = SqlEnrichmentRunner.readFile(schemaPath);
        JsonNode rootSchema = MAPPER.readTree(schemaJson);
        Schema flinkSchema = JsonSchemaToFlinkConverter.convertToTableSchema(rootSchema);

        SqlEnrichmentConfig sqlConfig = new SqlEnrichmentConfig(
            "samples/airline-agent/enrichment/enrich_flight_events.sql",
            "raw_flight_events",
            "enriched_flight_events"
        );

        String flightOpenEvent = """
            {
              "timestamp": "2026-10-01T20:29:16Z",
              "flightNumber": "EY082",
              "type": "FLIGHTOPEN",
              "flight": {
                "flightNumber": "EY082"
              }
            }
            """;

        String flightDelayedEvent = """
            {
              "timestamp": "2026-10-15T20:30:00Z",
              "flightNumber": "EY082",
              "type": "FLIGHTDELAYED",
              "flight": {
                "flightNumber": "EY082",
                "expectedDate": "2026-10-15T22:30:00Z"
              }
            }
            """;

        String entityResetEvent = """
            {
              "__eventType": "__ENTITY_RESET__",
              "flightNumber": "AF021"
            }
            """;

        DataStream<String> rawStream = env.fromData(flightOpenEvent, flightDelayedEvent, entityResetEvent);

        DataStream<EventEnvelope> enrichedStream = SqlEnrichmentRunner.enrichStream(
            env,
            rawStream,
            sqlConfig,
            schemaJson,
            flinkSchema,
            "FlightEvent",
            "agent-flight-events",
            0L,
            60000L,
            "$.flightNumber"
        );

        List<EventEnvelope> results = new ArrayList<>();
        try (var iterator = enrichedStream.executeAndCollect()) {
            while (iterator.hasNext()) {
                EventEnvelope ev = iterator.next();
                results.add(ev);
            }
        }

        // Flink streaming LEFT JOINs emit speculative rows as each CSV table loads;
        // we may receive multiple FLIGHTOPEN versions. Pick the fully-enriched one
        // (the last one with aircraftId populated).
        EventEnvelope env1 = results.stream()
            .filter(e -> "FLIGHTOPEN".equals(e.getPayload().path("type").asText()))
            .filter(e -> !e.getPayload().path("flight").path("aircraftId").isNull()
                      && !e.getPayload().path("flight").path("aircraftId").isMissingNode())
            .reduce((first, second) -> second) // take last
            .orElseThrow(() -> new AssertionError("Fully enriched FLIGHTOPEN not found in: " + results));

        assertEquals("FlightEvent", env1.getEventType());
        assertEquals("EY082", env1.getEntityId());
        JsonNode payload1 = env1.getPayload();
        assertNotNull(payload1);
        assertEquals("FLIGHTOPEN", payload1.path("type").asText());
        JsonNode flightNode1 = payload1.path("flight");
        // Fields from the event itself
        assertEquals("EY082", flightNode1.path("flightNumber").asText());
        // Enriched fields from flights.csv
        assertEquals("AF-343-TY", flightNode1.path("aircraftId").asText());
        assertEquals("A380", flightNode1.path("aircraftType").asText());
        assertEquals("2026-10-15T20:29:16Z", flightNode1.path("date").asText());
        assertEquals("CDG", flightNode1.path("origin").asText());
        assertEquals("LHR", flightNode1.path("destination").asText());
        assertEquals("PT1H", flightNode1.path("duration").asText());

        // Event 2: FLIGHTDELAYED -> Pass-through
        EventEnvelope env2 = results.stream()
            .filter(e -> "FLIGHTDELAYED".equals(e.getPayload().path("type").asText()))
            .findFirst()
            .orElseThrow(() -> new AssertionError("FLIGHTDELAYED not found"));

        assertEquals("FlightEvent", env2.getEventType());
        assertEquals("EY082", env2.getEntityId());
        JsonNode payload2 = env2.getPayload();
        assertNotNull(payload2);
        assertEquals("FLIGHTDELAYED", payload2.path("type").asText());
        JsonNode flightNode2 = payload2.path("flight");
        assertEquals("2026-10-15T22:30:00Z", flightNode2.path("expectedDate").asText());
        // Bypassed CSV lookup -> enriched fields are not populated
        assertTrue(flightNode2.path("aircraftId").isMissingNode());

        // Event 3: __ENTITY_RESET__ -> Pass-through with overridden eventType
        EventEnvelope env3 = results.stream()
            .filter(e -> "AF021".equals(e.getEntityId()))
            .findFirst()
            .orElseThrow(() -> new AssertionError("__ENTITY_RESET__ event not found"));

        assertEquals("__ENTITY_RESET__", env3.getEventType());
        assertEquals("AF021", env3.getEntityId());

        // FLIGHTOPEN: passengers must be a JSON array, not an escaped string
        JsonNode passengersNode = flightNode1.path("passengers");
        assertTrue(passengersNode.isArray(),
            "passengers should be a JSON array but was: " + passengersNode.getNodeType() + " -> " + passengersNode);
        assertTrue(passengersNode.size() > 0, "passengers array should not be empty");
        JsonNode firstPassenger = passengersNode.get(0);
        assertTrue(firstPassenger.isObject(), "each passenger entry should be a JSON object");
        assertNotNull(firstPassenger.get("passengerId"), "passenger should have passengerId field");
        assertNotNull(firstPassenger.get("name"),        "passenger should have name field");
    }

    // -------------------------------------------------------------------------
    // Unit tests for unescapeJsonStrings
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("unescapeJsonStrings: leaves plain string values untouched")
    void unescapeJsonStrings_plainStringUnchanged() throws Exception {
        JsonNode input = MAPPER.readTree("{\"name\":\"John\",\"age\":\"30\"}");
        JsonNode result = SqlEnrichmentRunner.unescapeJsonStrings(input);
        assertEquals("John", result.path("name").asText());
        assertEquals("30",   result.path("age").asText());
    }

    @Test
    @DisplayName("unescapeJsonStrings: re-parses an escaped JSON array string into an array node")
    void unescapeJsonStrings_escapedArrayBecomesArray() throws Exception {
        String escapedArray = "[{\"id\":\"P1\",\"name\":\"Alice\"},{\"id\":\"P2\",\"name\":\"Bob\"}]";
        JsonNode input = MAPPER.readTree("{\"passengers\":\"" + escapedArray.replace("\"", "\\\"") + "\"}");
        // Before: passengers is a string node
        assertTrue(input.path("passengers").isTextual());

        JsonNode result = SqlEnrichmentRunner.unescapeJsonStrings(input);
        JsonNode passengers = result.path("passengers");
        assertTrue(passengers.isArray(), "passengers should be an array after unescaping");
        assertEquals(2, passengers.size());
        assertEquals("P1",    passengers.get(0).path("id").asText());
        assertEquals("Alice", passengers.get(0).path("name").asText());
    }

    @Test
    @DisplayName("unescapeJsonStrings: re-parses an escaped JSON object string into an object node")
    void unescapeJsonStrings_escapedObjectBecomesObject() throws Exception {
        String escapedObj = "{\"economy\":10,\"business\":5}";
        JsonNode input = MAPPER.readTree("{\"catering\":\"" + escapedObj.replace("\"", "\\\"") + "\"}");
        assertTrue(input.path("catering").isTextual());

        JsonNode result = SqlEnrichmentRunner.unescapeJsonStrings(input);
        JsonNode catering = result.path("catering");
        assertTrue(catering.isObject(), "catering should be an object after unescaping");
        assertEquals(10, catering.path("economy").asInt());
    }

    @Test
    @DisplayName("unescapeJsonStrings: recurses into nested objects")
    void unescapeJsonStrings_recursesIntoNested() throws Exception {
        // Build an object node that already has a nested object with an escaped array inside it,
        // simulating multiple levels of JSON_OBJECT escaping from Flink.
        com.fasterxml.jackson.databind.node.ObjectNode passengersHolder =
            MAPPER.createObjectNode();
        passengersHolder.put("passengers", "[{\"id\":\"P1\"}]"); // escaped array string

        com.fasterxml.jackson.databind.node.ObjectNode root = MAPPER.createObjectNode();
        root.set("flight", passengersHolder);

        JsonNode result = SqlEnrichmentRunner.unescapeJsonStrings(root);
        JsonNode flight = result.path("flight");
        assertTrue(flight.isObject());
        assertTrue(flight.path("passengers").isArray());
        assertEquals("P1", flight.path("passengers").get(0).path("id").asText());
    }

    // -------------------------------------------------------------------------
    // Outbound enrichment: PassengerNotification
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("Outbound: enriches PassengerNotification with contact details from passengers.csv")
    void testPassengerNotificationOutboundEnrichment() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String schemaPath = "samples/airline-agent/schemas/passenger-notification.schema.json";
        String schemaJson = SqlEnrichmentRunner.readFile(schemaPath);
        JsonNode rootSchema = MAPPER.readTree(schemaJson);
        Schema flinkSchema = JsonSchemaToFlinkConverter.convertToTableSchema(rootSchema);

        SqlEnrichmentConfig sqlConfig = new SqlEnrichmentConfig(
            "samples/airline-agent/enrichment/enrich_passenger_notifications.sql",
            "raw_passenger_notifications",
            "enriched_passenger_notifications"
        );

        // A notification produced by the rules engine for passenger JS on flight EY082
        String notifJs = """
            {
              "timestamp": "2026-10-13T20:29:16Z",
              "flightNumber": "EY082",
              "passengerNumber": "JS",
              "message": "Check-in for Flight EY082 is open. Please check-in online"
            }
            """;

        // A notification for passenger LW — different contact preference
        String notifLw = """
            {
              "timestamp": "2026-10-13T20:29:16Z",
              "flightNumber": "EY082",
              "passengerNumber": "LW",
              "message": "Check-in for Flight EY082 is open. Please check-in online"
            }
            """;

        DataStream<String> rawStream = env.fromData(notifJs, notifLw);

        DataStream<EventEnvelope> enrichedStream = SqlEnrichmentRunner.enrichStream(
            env,
            rawStream,
            sqlConfig,
            schemaJson,
            flinkSchema,
            "PassengerNotification",
            "agent-passenger-notifications",
            0L,
            60000L,
            "$.passengerNumber"
        );

        List<EventEnvelope> results = new ArrayList<>();
        try (var iterator = enrichedStream.executeAndCollect()) {
            while (iterator.hasNext()) {
                results.add(iterator.next());
            }
        }

        // Streaming LEFT JOINs may emit speculative rows; pick the last (fully enriched) version
        // for each passenger by taking the last result that has a non-null name.
        EventEnvelope envJs = results.stream()
            .filter(e -> "JS".equals(e.getPayload().path("passengerNumber").asText()))
            .filter(e -> !e.getPayload().path("name").isMissingNode()
                      && !e.getPayload().path("name").isNull())
            .reduce((first, second) -> second)
            .orElseThrow(() -> new AssertionError("Enriched notification for JS not found in: " + results));

        EventEnvelope envLw = results.stream()
            .filter(e -> "LW".equals(e.getPayload().path("passengerNumber").asText()))
            .filter(e -> !e.getPayload().path("name").isMissingNode()
                      && !e.getPayload().path("name").isNull())
            .reduce((first, second) -> second)
            .orElseThrow(() -> new AssertionError("Enriched notification for LW not found in: " + results));

        // ── JS assertions ─────────────────────────────────────────────────────
        assertEquals("PassengerNotification", envJs.getEventType());
        assertEquals("JS", envJs.getEntityId());
        JsonNode jsPayload = envJs.getPayload();
        assertEquals("EY082",                                     jsPayload.path("flightNumber").asText());
        assertEquals("JS",                                        jsPayload.path("passengerNumber").asText());
        assertEquals("Check-in for Flight EY082 is open. Please check-in online",
                                                                  jsPayload.path("message").asText());
        // Enriched contact fields
        assertEquals("John Smith",                                jsPayload.path("name").asText());
        assertEquals("Gold",                                      jsPayload.path("loyaltyStatus").asText());
        assertEquals("js@dummy.com",                              jsPayload.path("email").asText());
        assertEquals("+1-567-987-4567",                           jsPayload.path("phone").asText());
        assertEquals("SMS",                                       jsPayload.path("contactPreference").asText());

        // ── LW assertions ─────────────────────────────────────────────────────
        assertEquals("PassengerNotification", envLw.getEventType());
        assertEquals("LW", envLw.getEntityId());
        JsonNode lwPayload = envLw.getPayload();
        assertEquals("Lisa Williams",                             lwPayload.path("name").asText());
        assertEquals("Silver",                                    lwPayload.path("loyaltyStatus").asText());
        assertEquals("lw@dummy.com",                              lwPayload.path("email").asText());
        assertEquals("+1-567-987-4568",                           lwPayload.path("phone").asText());
        assertEquals("EMAIL",                                     lwPayload.path("contactPreference").asText());
    }
}
