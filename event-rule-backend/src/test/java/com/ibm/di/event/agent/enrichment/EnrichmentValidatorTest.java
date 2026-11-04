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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.di.event.agent.AgentDescriptor;
import com.ibm.di.event.agent.AgentDescriptorValidator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("EnrichmentValidator")
class EnrichmentValidatorTest {

    private static final String PASSENGERS = "samples/airline-agent/enrichment/passengers.csv";

    private static AgentDescriptor agent(String references, String subscriptionSteps, String outputSteps) throws Exception {
        String json = """
            { "agentId": "a1", "entityType": "E", "entityKeyExtractor": "$.id",
              "references": %s,
              "subscriptions": [ { "eventType": "Ev", "topic": "ev-topic", "operation": "OnEv",
                                   "enrichment": %s } ],
              "outputs": [ { "eventType": "Out", "topic": "out-topic", "enrichment": %s } ],
              "ruleset": { "path": "x.jar", "operations": [ { "operation": "OnEv" } ] } }
            """.formatted(references, subscriptionSteps, outputSteps);
        return new ObjectMapper().readValue(json, AgentDescriptor.class);
    }

    private static String refs() {
        return "[{\"name\":\"p\",\"type\":\"csv\",\"file\":\"" + PASSENGERS + "\"}]";
    }

    private static String step(String into, String on) {
        return "{\"reference\":\"p\",\"key\":\"passengerId\",\"on\":\"" + on + "\",\"into\":\"" + into
            + "\",\"fields\":[\"name\"]}";
    }

    private static void assertInvalid(AgentDescriptor d, String message) {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
            () -> AgentDescriptorValidator.validate(d));
        assertTrue(e.getMessage().contains(message), e.getMessage());
    }

    @Test
    @DisplayName("a valid descriptor passes, structurally and with the data loaded")
    void valid() throws Exception {
        AgentDescriptor d = agent(refs(), "[" + step("$.p", "$.pid") + "]", "[" + step("$", "$.pid") + "]");
        assertDoesNotThrow(() -> AgentDescriptorValidator.validate(d));
        assertDoesNotThrow(() -> EnrichmentValidator.validate(d, true));
    }

    @Test
    @DisplayName("the sample descriptor passes with its data loaded")
    void sample() throws Exception {
        AgentDescriptor d = com.ibm.di.event.agent.AgentDescriptorLoader
            .loadFileDescriptor("../samples/airline-agent/agents.json").agents().get(0);
        assertDoesNotThrow(() -> EnrichmentValidator.validate(d, true));
    }

    @Test
    @DisplayName("an unknown reference is rejected")
    void unknownReference() throws Exception {
        AgentDescriptor d = agent("[]", "[" + step("$.p", "$.pid") + "]", "null");
        assertInvalid(d, "not declared");
    }

    @Test
    @DisplayName("a duplicate reference name is rejected")
    void duplicateReference() throws Exception {
        String twice = "[{\"name\":\"p\",\"file\":\"" + PASSENGERS + "\"},{\"name\":\"p\",\"file\":\"" + PASSENGERS + "\"}]";
        assertInvalid(agent(twice, "null", "null"), "duplicate reference name");
    }

    @Test
    @DisplayName("a reference needs a file and the csv type")
    void referenceShape() throws Exception {
        assertInvalid(agent("[{\"name\":\"p\"}]", "null", "null"), "needs a 'file'");
        assertInvalid(agent("[{\"name\":\"p\",\"type\":\"xml\",\"file\":\"a\"}]", "null", "null"), "unsupported type");
    }

    @Test
    @DisplayName("a bad into path is rejected")
    void badInto() throws Exception {
        assertInvalid(agent(refs(), "[" + step("$.a[0]", "$.pid") + "]", "null"), "into");
    }

    @Test
    @DisplayName("a bad on path is rejected")
    void badOn() throws Exception {
        assertInvalid(agent(refs(), "[" + step("$.a", "$..[") + "]", "null"), "step");
    }

    @Test
    @DisplayName("steps that depend on each other in a cycle are rejected")
    void cycle() throws Exception {
        AgentDescriptor d = agent(refs(), "[" + step("$.a", "$.b.id") + "," + step("$.b", "$.a.id") + "]", "null");
        assertInvalid(d, "cycle");
    }

    @Test
    @DisplayName("a when written @type instead of @.type is rejected")
    void badWhen() throws Exception {
        String s = "{\"reference\":\"p\",\"key\":\"passengerId\",\"on\":\"$.x\",\"into\":\"$\","
            + "\"when\":\"$[?(@type == 'X')]\",\"fields\":[\"name\"]}";
        assertInvalid(agent(refs(), "[" + s + "]", "null"), "when");
    }

    @Test
    @DisplayName("a step without fields is rejected")
    void noFields() throws Exception {
        String s = "{\"reference\":\"p\",\"key\":\"passengerId\",\"on\":\"$.x\",\"into\":\"$\",\"fields\":[]}";
        assertInvalid(agent(refs(), "[" + s + "]", "null"), "fields");
    }

    @Test
    @DisplayName("an unknown key or field column is detected when the data is loaded")
    void unknownColumn() throws Exception {
        String badKey = "{\"reference\":\"p\",\"key\":\"nope\",\"on\":\"$.x\",\"into\":\"$\",\"fields\":[\"name\"]}";
        AgentDescriptor d = agent(refs(), "[" + badKey + "]", "null");
        assertDoesNotThrow(() -> EnrichmentValidator.validate(d, false));
        assertThrows(IllegalArgumentException.class, () -> EnrichmentValidator.validate(d, true));

        String badField = "{\"reference\":\"p\",\"key\":\"passengerId\",\"on\":\"$.x\",\"into\":\"$\",\"fields\":[\"nope\"]}";
        AgentDescriptor d2 = agent(refs(), "null", "[" + badField + "]");
        assertThrows(IllegalArgumentException.class, () -> EnrichmentValidator.validate(d2, true));
    }
}
