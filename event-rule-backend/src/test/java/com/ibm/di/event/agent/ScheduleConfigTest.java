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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Schedule descriptor configuration")
class ScheduleConfigTest {

    private static final String AGENTS_JSON = """
        { "agents": [ {
            "agentId": "a1", "entityType": "E", "entityKeyExtractor": "$.id",
            "subscriptions": [
              { "eventType": "Ev", "topic": "ev-topic", "operation": "OnEv" },
              { "eventType": "Schedule", "operation": "OnSchedule" }
            ],
            "ruleset": { "path": "x.jar", "operations": [
              { "operation": "OnEv", "scheduleRequestsField": "scheduleRequests" },
              { "operation": "OnSchedule" }
            ] }
        } ] }
        """;

    @Test
    @DisplayName("agents.json: scheduleRequestsField and Schedule subscription deserialize")
    void deserializes() throws Exception {
        AgentDescriptor d = new ObjectMapper().readTree(AGENTS_JSON).get("agents").get(0)
            .traverse(new ObjectMapper()).readValueAs(AgentDescriptor.class);
        assertEquals("scheduleRequests",
            d.getRuleset().findOperation("OnEv").scheduleRequestsField());
        assertNull(d.getRuleset().findOperation("OnSchedule").scheduleRequestsField());
        assertTrue(d.getSubscriptions().get(1).isScheduleSubscription());
        assertFalse(d.getSubscriptions().get(0).isScheduleSubscription());
        assertNull(d.getSubscriptions().get(1).getTopic());
    }

    @Test
    @DisplayName("validate accepts a topic-less Schedule subscription")
    void validateAccepts() throws Exception {
        AgentDescriptor d = new ObjectMapper().readTree(AGENTS_JSON).get("agents").get(0)
            .traverse(new ObjectMapper()).readValueAs(AgentDescriptor.class);
        assertDoesNotThrow(() -> AgentBasedDataStreamJob.validate(d));
    }

    @Test
    @DisplayName("validate rejects a Schedule subscription that declares a topic")
    void validateRejectsTopic() {
        AgentDescriptor d = new AgentDescriptor();
        d.setAgentId("a1");
        d.setEntityKeyExtractor("$.id");
        d.setRuleset(new RulesetConfig("x.jar"));
        d.setSubscriptions(List.of(
            new EventSubscription("Schedule", "some-topic", "OnSchedule", HorizonConfig.count(0))));
        assertThrows(IllegalArgumentException.class, () -> AgentBasedDataStreamJob.validate(d));
    }

    @Test
    @DisplayName("agents.json: idleness is read per topic; others use the default")
    void idlenessPerTopic(@TempDir Path dir) throws Exception {
        Files.writeString(dir.resolve("agents.json"), """
            { "topics": [
                { "name": "t-fast", "idleness": "30s" },
                { "name": "t-slow", "idleness": "2h" },
                { "name": "t-default" }
              ],
              "agents": [] }
            """);
        Map<String, Long> idleness =
            AgentBasedDataStreamJob.loadTopicIdleness(dir.resolve("agents.json").toString());
        assertEquals(30_000L, idleness.get("t-fast"));
        assertEquals(7_200_000L, idleness.get("t-slow"));
        assertFalse(idleness.containsKey("t-default"));
        assertEquals(60_000L, AgentBasedDataStreamJob.DEFAULT_IDLENESS_MS, "default is 1 minute");
    }

    @Test
    @DisplayName("agents.json without a topics section → empty map")
    void noTopicsSection(@TempDir Path dir) throws Exception {
        Files.writeString(dir.resolve("agents.json"), AGENTS_JSON);
        assertTrue(AgentBasedDataStreamJob
            .loadTopicIdleness(dir.resolve("agents.json").toString()).isEmpty());
    }

    @Test
    @DisplayName("invalid idleness is rejected")
    void invalidIdleness(@TempDir Path dir) throws Exception {
        Files.writeString(dir.resolve("agents.json"),
            "{ \"topics\": [ { \"name\": \"t\", \"idleness\": \"soon\" } ], \"agents\": [] }");
        assertThrows(IllegalArgumentException.class, () -> AgentBasedDataStreamJob
            .loadTopicIdleness(dir.resolve("agents.json").toString()));
    }
}
