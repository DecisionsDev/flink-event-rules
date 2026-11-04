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
package com.ibm.di.event.agent.enrichment;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Enricher")
class EnricherTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String DIR = "samples/airline-agent/enrichment/";

    private static final String REFERENCES = """
        "references": [
          {"name":"flights","type":"csv","file":"%sflights.csv"},
          {"name":"passengers","type":"csv","file":"%spassengers.csv"},
          {"name":"catering","type":"csv","file":"%scatering.csv"},
          {"name":"roster","type":"csv","file":"%sflight_passengers.csv"}
        ]""".formatted(DIR, DIR, DIR, DIR);

    private static final String FLIGHT = """
        {"reference":"flights","key":"flightNumber","on":"$.flightNumber","when":"$[?(@.type == 'FLIGHTOPEN')]",
         "into":"$.flight","fields":["aircraftId","aircraftType","origin","destination","duration"]}""";
    private static final String CATERING = """
        {"reference":"catering","key":"flightNumber","on":"$.flightNumber","when":"$[?(@.type == 'FLIGHTOPEN')]",
         "into":"$.flight.cateringInformation","fields":["economyTotalMealCount","businessTotalMealCount"]}""";
    private static final String ROSTER = """
        {"reference":"roster","key":"flightNumber","on":"$.flightNumber","when":"$[?(@.type == 'FLIGHTOPEN')]",
         "into":"$.flight.passengers","cardinality":"many",
         "fields":["passengerId",{"checkInStatus":"check-inStatus"},"travelClass"]}""";
    private static final String PASSENGERS = """
        {"reference":"passengers","key":"passengerId","on":"$.passengerId",
         "into":"$.flight.passengers[*]","fields":["name","loyaltyStatus"]}""";

    private static Enricher enricher(String root, String... steps) {
        return new Enricher(spec(root, steps));
    }

    private static EnrichmentSpec spec(String root, String... steps) {
        return EnrichmentSpec.fromJson("{\"root\":\"" + root + "\"," + REFERENCES
            + ",\"steps\":[" + String.join(",", steps) + "]}");
    }

    private static JsonNode json(String s) throws Exception {
        return MAPPER.readTree(s);
    }

    private static String open(String flightNumber) {
        return "{\"type\":\"FLIGHTOPEN\",\"flightNumber\":\"" + flightNumber + "\"}";
    }

    // ── End to end on the sample ──────────────────────────────────────────────

    @Test
    @DisplayName("FLIGHTOPEN: flight, catering, roster and passengers, whatever the declared order")
    void flightOpenEndToEnd() throws Exception {
        for (Enricher e : new Enricher[] {
                enricher("", FLIGHT, CATERING, ROSTER, PASSENGERS),
                enricher("", PASSENGERS, ROSTER, CATERING, FLIGHT)}) {
            JsonNode out = json(e.enrich(open("AF021")));

            assertEquals("FLIGHTOPEN", out.get("type").asText());
            JsonNode flight = out.get("flight");
            assertEquals("CDG", flight.get("origin").asText());
            assertTrue(flight.get("cateringInformation").has("economyTotalMealCount"));
            assertTrue(flight.get("cateringInformation").get("economyTotalMealCount").isIntegralNumber());

            JsonNode passengers = flight.get("passengers");
            assertEquals(10, passengers.size());
            for (JsonNode p : passengers) {
                assertTrue(p.has("passengerId"));
                assertTrue(p.has("check-inStatus"), "renamed");
                assertFalse(p.has("checkInStatus"));
                assertTrue(p.has("name"), "enriched by the passengers reference: " + p);
                assertTrue(p.has("loyaltyStatus"));
            }
            assertEquals("John Smith", passengers.get(0).get("name").asText());
        }
    }

    @Test
    @DisplayName("a when that does not match leaves the document untouched")
    void whenDoesNotMatch() {
        Enricher e = enricher("", FLIGHT, CATERING, ROSTER, PASSENGERS);
        String in = "{\"type\":\"FLIGHTCLOSED\",\"flightNumber\":\"AF021\"}";
        assertSame(in, e.enrich(in));
    }

    @Test
    @DisplayName("no row for the key: the event is left alone and the later steps still run")
    void missingRowContinues() throws Exception {
        String ghostPassenger = """
            {"reference":"passengers","key":"passengerId","on":"$.passengerId",
             "into":"$","fields":["name"]}""";
        Enricher e = enricher("", FLIGHT, ghostPassenger);
        JsonNode out = json(e.enrich("{\"type\":\"FLIGHTOPEN\",\"flightNumber\":\"NOPE\",\"passengerId\":\"JS\"}"));
        assertFalse(out.has("flight"), "unknown flight");
        assertEquals("John Smith", out.get("name").asText(), "the next step ran");
    }

    @Test
    @DisplayName("a passenger absent from the reference stays un-enriched, the others are enriched")
    void unknownElementLeftAlone() throws Exception {
        Enricher e = enricher("", PASSENGERS);
        JsonNode out = json(e.enrich("""
            {"flight":{"passengers":[{"passengerId":"JS"},{"passengerId":"ZZZ"},{"noId":1},"text"]}}"""));
        JsonNode ps = out.get("flight").get("passengers");
        assertEquals("John Smith", ps.get(0).get("name").asText());
        assertFalse(ps.get(1).has("name"));
        assertFalse(ps.get(2).has("name"));
        assertEquals("text", ps.get(3).asText());
    }

    @Test
    @DisplayName("[*] on an absent or non-array target does nothing")
    void eachOnNonArray() {
        Enricher e = enricher("", PASSENGERS);
        String absent = "{\"type\":\"X\"}";
        assertSame(absent, e.enrich(absent));
        String notArray = "{\"flight\":{\"passengers\":{\"passengerId\":\"JS\"}}}";
        assertSame(notArray, e.enrich(notArray));
    }

    @Test
    @DisplayName("on is relative to each array element")
    void onIsRelativeToElement() throws Exception {
        Enricher e = enricher("", """
            {"reference":"passengers","key":"passengerId","on":"$.who.id",
             "into":"$.people[*]","fields":["name"]}""");
        JsonNode out = json(e.enrich("{\"people\":[{\"who\":{\"id\":\"LW\"}}]}"));
        assertEquals("Lisa Williams", out.get("people").get(0).get("name").asText());
    }

    // ── Outbound ──────────────────────────────────────────────────────────────

    @Test
    @DisplayName("outbound: contact details are merged into the payload of the envelope")
    void outboundMergesIntoPayload() throws Exception {
        Enricher e = enricher("payload", """
            {"reference":"passengers","key":"passengerId","on":"$.passengerNumber",
             "into":"$","fields":["name","loyaltyStatus","email","contactPreference"]}""");
        JsonNode out = json(e.enrich(
            "{\"agentEventType\":\"PassengerNotification\",\"payload\":{\"passengerNumber\":\"JS\",\"text\":\"hi\"}}"));
        JsonNode payload = out.get("payload");
        assertEquals("hi", payload.get("text").asText());
        assertEquals("John Smith", payload.get("name").asText());
        assertEquals("js@dummy.com", payload.get("email").asText());
        assertFalse(out.has("name"), "the envelope itself is not touched");
    }

    @Test
    @DisplayName("outbound: unknown passenger or no payload leaves the envelope alone")
    void outboundUnknownOrNoPayload() {
        Enricher e = enricher("payload", """
            {"reference":"passengers","key":"passengerId","on":"$.passengerNumber",
             "into":"$","fields":["name"]}""");
        String unknown = "{\"payload\":{\"passengerNumber\":\"ZZZ\"}}";
        assertSame(unknown, e.enrich(unknown));
        String none = "{\"agentEventType\":\"X\"}";
        assertSame(none, e.enrich(none));
        String notObject = "{\"payload\":\"text\"}";
        assertSame(notObject, e.enrich(notObject));
    }

    // ── Robustness ────────────────────────────────────────────────────────────

    @Test
    @DisplayName("invalid input is returned as it is")
    void invalidInput() {
        Enricher e = enricher("", FLIGHT);
        assertNull(e.enrich(null));
        assertEquals("", e.enrich(""));
        assertEquals("not json", e.enrich("not json"));
        assertEquals("[1,2]", e.enrich("[1,2]"));
    }

    @Test
    @DisplayName("key columns stay strings, other columns are typed")
    void typing() throws Exception {
        Enricher e = enricher("", """
            {"reference":"catering","key":"flightNumber","on":"$.flightNumber",
             "into":"$","fields":["flightNumber","economyTotalMealCount"]}""");
        JsonNode out = json(e.enrich("{\"flightNumber\":\"AF021\"}"));
        assertTrue(out.get("flightNumber").isTextual());
        assertTrue(out.get("economyTotalMealCount").isIntegralNumber());
    }

    @Test
    @DisplayName("a when must use @.name: the @name form is rejected")
    void whenRequiresDotSyntax() {
        String shorthand = FLIGHT.replace("@.type", "@type");
        assertThrows(IllegalArgumentException.class, () -> enricher("", shorthand));
    }

    // ── Configuration errors ──────────────────────────────────────────────────

    @Test
    @DisplayName("configuration errors are thrown when the enricher is built")
    void configurationErrors() {
        assertThrows(IllegalArgumentException.class, () -> enricher("", """
            {"reference":"nope","key":"k","on":"$.a","into":"$","fields":["x"]}"""));
        assertThrows(IllegalArgumentException.class, () -> enricher("", """
            {"reference":"passengers","key":"nope","on":"$.a","into":"$","fields":["name"]}"""));
        assertThrows(IllegalArgumentException.class, () -> enricher("", """
            {"reference":"passengers","key":"passengerId","on":"$.a","into":"$","fields":["nope"]}"""));
        assertThrows(IllegalArgumentException.class, () -> enricher("", """
            {"reference":"passengers","key":"passengerId","on":"$.a","into":"$","fields":[]}"""));
        assertThrows(IllegalArgumentException.class, () -> enricher("", """
            {"reference":"passengers","key":"passengerId","on":"$.a","into":"$.a[0]","fields":["name"]}"""));
        assertThrows(IllegalArgumentException.class, () -> enricher("", """
            {"reference":"passengers","key":"passengerId","on":"$.a","into":"$.a[*]",
             "cardinality":"many","fields":["name"]}"""));
        assertThrows(IllegalArgumentException.class, () -> enricher("", """
            {"reference":"passengers","key":"passengerId","on":"$.a","into":"$.a",
             "cardinality":"several","fields":["name"]}"""));
    }

    @Test
    @DisplayName("the spec survives a JSON round trip")
    void specRoundTrip() {
        EnrichmentSpec spec = spec("payload", ROSTER, PASSENGERS);
        EnrichmentSpec back = EnrichmentSpec.fromJson(spec.toJson());
        assertEquals("payload", back.root);
        assertEquals(2, back.steps.size());
        assertEquals(4, back.references.size());
        assertEquals("check-inStatus", back.steps.get(0).getFields().get(1).getTarget());
        assertEquals("many", back.steps.get(0).getCardinality());
    }
}
