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
package com.ibm.di.event.agent.datastream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.di.event.agent.AgentDescriptor;
import com.ibm.di.event.agent.AgentDescriptorLoader;
import com.ibm.di.event.agent.EnrichmentConfig;
import com.ibm.di.event.agent.EventEnvelope;
import com.ibm.di.event.agent.FieldMapping;
import com.ibm.di.event.agent.ReferenceConfig;
import com.ibm.di.event.agent.enrichment.EnrichmentSpec;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Reference-data enrichment in the DataStream engine")
class EnrichEnvelopeFunctionTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String DIR = "samples/airline-agent/enrichment/";

    private static EnrichmentSpec spec(String reference, String file, String key, String on, String into,
                                       String... fields) {
        EnrichmentConfig c = new EnrichmentConfig();
        c.setReference(reference);
        c.setKey(key);
        c.setOn(on);
        c.setInto(into);
        c.setFields(java.util.Arrays.stream(fields).map(f -> new FieldMapping(f, f)).toList());
        return EnrichmentSpec.of("", List.of(c), List.of(new ReferenceConfig(reference, "csv", file)));
    }

    private static EventEnvelope envelope(String payload) throws Exception {
        EventEnvelope e = EventEnvelope.from("PassengerEvent", "topic", 42L, MAPPER.readTree(payload), "JS");
        e.addMetadata("k", "v");
        return e;
    }

    private static EnrichEnvelopeFunction opened(EnrichmentSpec spec) {
        EnrichEnvelopeFunction f = new EnrichEnvelopeFunction(spec);
        f.open((org.apache.flink.api.common.functions.OpenContext) null);
        return f;
    }

    @Test
    @DisplayName("an enriched event is a new envelope with the same identity and an enriched payload")
    void enrichesIntoNewEnvelope() throws Exception {
        EnrichEnvelopeFunction f = opened(spec("passengers", DIR + "passengers.csv", "passengerId",
            "$.passengerId", "$", "name", "loyaltyStatus"));
        EventEnvelope in = envelope("{\"passengerId\":\"JS\"}");

        EventEnvelope out = f.map(in);

        assertNotSame(in, out);
        assertEquals("John Smith", out.getPayload().get("name").asText());
        assertEquals("JS", out.getPayload().get("passengerId").asText());
        assertEquals("PassengerEvent", out.getEventType());
        assertEquals("topic", out.getSourceTopic());
        assertEquals(42L, out.getTimestamp());
        assertEquals("JS", out.getEntityId());
        assertEquals(Map.of("k", "v"), out.getMetadata());
        assertFalse(in.getPayload().has("name"), "the incoming envelope is not modified");
    }

    @Test
    @DisplayName("no row, or no payload: the same envelope is returned")
    void unchanged() throws Exception {
        EnrichEnvelopeFunction f = opened(spec("passengers", DIR + "passengers.csv", "passengerId",
            "$.passengerId", "$", "name"));
        EventEnvelope unknown = envelope("{\"passengerId\":\"ZZZ\"}");
        assertSame(unknown, f.map(unknown));
        EventEnvelope none = new EventEnvelope();
        assertSame(none, f.map(none));
    }

    @Test
    @DisplayName("the operator runs in a local DataStream job")
    void runsInLocalJob() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnrichmentSpec spec = spec("passengers", DIR + "passengers.csv", "passengerId",
            "$.passengerId", "$", "name");
        DataStream<EventEnvelope> in = env.fromData(
            envelope("{\"passengerId\":\"LW\"}"), envelope("{\"passengerId\":\"ZZZ\"}"));

        List<String> names = new ArrayList<>();
        try (CloseableIterator<EventEnvelope> it = in.map(new EnrichEnvelopeFunction(spec)).executeAndCollect()) {
            it.forEachRemaining(e -> {
                JsonNode n = e.getPayload().get("name");
                names.add(n == null ? "-" : n.asText());
            });
        }
        assertEquals(List.of("Lisa Williams", "-"), names);
    }

    @Test
    @DisplayName("wireAgent adds the enrichment operators of the airline sample")
    void wireAgentAddsOperators() throws Exception {
        AgentDescriptor airline = AgentDescriptorLoader
            .loadFileDescriptor("../samples/airline-agent/agents.json").agents().get(0);
        // The sample also declares sqlEnrichment, which needs Kafka schemas: test the new mechanism alone.
        airline.getSubscriptions().forEach(s -> s.setSqlEnrichment(null));
        airline.getOutputs().forEach(o -> o.setSqlEnrichment(null));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        AgentBasedDataStreamJob.wireAgent(env, airline, "localhost:9092", 5000L, null, null, Map.of());

        StreamGraph graph = env.getStreamGraph();
        List<String> names = graph.getStreamNodes().stream().map(StreamNode::getOperatorName).toList();
        assertTrue(names.stream().anyMatch(n -> n.startsWith("Enrich[airline-agent/FlightEvent]")), names.toString());
        assertTrue(names.stream().anyMatch(n -> n.startsWith("Enrich[airline-agent/PassengerNotification]")),
            names.toString());
    }

    @Test
    @DisplayName("with both declared, sqlEnrichment wins and the enrichment of that subscription/output is not wired")
    void sqlEnrichmentTakesPrecedence() throws Exception {
        String path = "../samples/airline-agent/agents-with-sql-enrichment.json";
        AgentDescriptor airline = AgentDescriptorLoader.loadFileDescriptor(path).agents().get(0);
        assertTrue(airline.getSubscriptions().get(0).hasSqlEnrichment());
        assertTrue(airline.getSubscriptions().get(0).hasEnrichment(), "the sample declares both");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        AgentBasedDataStreamJob.wireAgent(env, airline, "localhost:9092", 5000L, null, null, Map.of(),
            AgentDescriptorLoader.loadTopicSchemas(path));

        List<String> names = env.getStreamGraph().getStreamNodes().stream()
            .map(StreamNode::getOperatorName).toList();
        assertTrue(names.stream().noneMatch(n -> n.startsWith("Enrich[airline-agent/FlightEvent]")), names.toString());
        assertTrue(names.stream().noneMatch(n -> n.startsWith("Enrich[airline-agent/PassengerNotification]")),
            names.toString());
    }
}
