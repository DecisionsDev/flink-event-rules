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
package com.ibm.di.event.agent.ptf;

import com.fasterxml.jackson.databind.JsonNode;
import com.ibm.di.event.agent.AgentDescriptor;
import com.ibm.di.event.agent.ptf.PtfTestSupport.StubEngine;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static com.ibm.di.event.agent.ptf.PtfTestSupport.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Runs the function in a real local Flink {@code TableEnvironment}: Flink's own validation of the
 * function signature (state hints, argument traits, output type, {@code on_time}) and of the SQL
 * shape that {@link AgentSqlPipelineBuilder} generates for the call.
 */
@DisplayName("Process Table Function in a local Flink job")
class PtfLocalRunTest {

    private static final StubEngine ENGINE = new StubEngine(in ->
        json("{\"ent\":{\"n\":1},\"outputEvents\":[{\"level\":\"high\"}],\"scheduleRequests\":["
            + "{\"id\":\"s1\",\"status\":\"START\",\"timepoint\":\"" + SCHEDULE_TIMEPOINT
            + "\",\"description\":\"wake\"}]}"));

    private static List<Row> run(AgentDescriptor descriptor, List<Row> events) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStream<Row> stream = env.fromData(events);
        tEnv.createTemporaryView("agent_in", tEnv.fromDataStream(stream, Schema.newBuilder()
            .column("f0", DataTypes.STRING())
            .column("f1", DataTypes.STRING())
            .column("f2", DataTypes.STRING())
            .column("f3", DataTypes.STRING())
            .column("f4", DataTypes.BIGINT())
            .columnByExpression("event_ts", "TO_TIMESTAMP_LTZ(f4, 3)")
            .watermark("event_ts", "event_ts")
            .build()).as("entity_id", "event_type", "source_topic", "payload", "ts", "event_ts"));

        tEnv.createTemporarySystemFunction("agent_fn",
            new StatefulAgentProcessTableFunction(descriptor, operation -> ENGINE));

        List<Row> out = new ArrayList<>();
        try (CloseableIterator<Row> it = tEnv.executeSql("SELECT * FROM agent_fn("
            + "events => TABLE agent_in PARTITION BY entity_id, "
            + "on_time => DESCRIPTOR(event_ts), uid => 'agent-processor-test')").collect()) {
            it.forEachRemaining(out::add);
        }
        return out;
    }

    private static Row event(String entity, long ts) {
        return Row.of(entity, "domain-event", "domain-events", "{\"id\":\"" + entity + "\"}", ts);
    }

    @Test
    @DisplayName("events produce rows, and the schedule timer fires when the watermark passes it")
    void eventsAndScheduleTimer() throws Exception {
        ENGINE.inputs.clear();

        List<Row> rows = run(descriptor(operation()), List.of(event("E1", 1000), event("E2", 2000)));

        // Output columns: the partition key, the two declared columns, and the appended rowtime.
        assertEquals(4, rows.get(0).getArity(), rows.get(0).toString());
        assertEquals(4, rows.size(), "2 events + 2 Schedule events fired by the timers");
        // The Schedule events go through the Schedule operation, which has no outbound config.
        assertEquals(2, rows.stream().filter(r -> "alert".equals(r.getField(1))).count());
        List<Row> fired = rows.stream().filter(r -> "object".equals(r.getField(1))).toList();
        assertEquals(2, fired.size());
        assertEquals(Instant.ofEpochMilli(SCHEDULE_MS), fired.get(0).getField(3),
            "the output row time is the timer time");
        JsonNode first = json(rows.get(0).<String>getFieldAs(2));
        assertEquals("high", first.get("payload").get("level").asText());
        assertTrue(rows.stream().anyMatch(r -> "E1".equals(r.getField(0))));
        assertTrue(rows.stream().anyMatch(r -> "E2".equals(r.getField(0))));
    }

    @Test
    @DisplayName("EVENT_ENRICH runs as a SQL scalar function and enriches a JSON value")
    void eventEnrichRuns() throws Exception {
        org.apache.flink.table.api.TableEnvironment tEnv =
            org.apache.flink.table.api.TableEnvironment.create(
                org.apache.flink.table.api.EnvironmentSettings.inStreamingMode());
        tEnv.createTemporarySystemFunction(EventEnrichFunction.NAME, EventEnrichFunction.class);

        String spec = "{\"root\":\"\",\"references\":[{\"name\":\"p\",\"type\":\"csv\","
            + "\"file\":\"samples/airline-agent/enrichment/passengers.csv\"}],\"steps\":[{\"reference\":\"p\","
            + "\"key\":\"passengerId\",\"on\":\"$.passengerId\",\"into\":\"$\",\"fields\":[\"name\"]}]}";
        String sql = "SELECT EVENT_ENRICH('{\"passengerId\":\"JS\"}', " + AgentSqlPipelineBuilder.lit(spec) + ") AS v";

        try (CloseableIterator<Row> it = tEnv.executeSql(sql).collect()) {
            JsonNode out = json(it.next().<String>getFieldAs(0));
            assertEquals("JS", out.get("passengerId").asText());
            assertEquals("John Smith", out.get("name").asText());
            assertFalse(it.hasNext());
        }
    }

    @Test
    @DisplayName("the generated SQL of every sample is accepted by the Flink planner")
    void generatedSqlPlans() throws Exception {
        for (String sample : List.of("banking-fraud-agent", "airtraffic-agent", "retail-agent", "airline-agent")) {
            List<AgentDescriptor> agents = com.ibm.di.event.agent.AgentDescriptorLoader.loadFileDescriptor("../samples/" + sample + "/agents.json").agents();
            AgentSqlPipelineBuilder.Pipeline pipeline =
                new AgentSqlPipelineBuilder(new AgentSqlPipelineBuilder.Options()).build(agents);

            org.apache.flink.table.api.TableEnvironment tEnv =
                org.apache.flink.table.api.TableEnvironment.create(
                    org.apache.flink.table.api.EnvironmentSettings.inStreamingMode());
            String plan = AgentBasedPTFJob.register(tEnv, pipeline).explain();

            assertTrue(plan.contains("ProcessTableFunction") || plan.contains("PTF")
                || plan.contains("event_rule_"), sample + " plan:\n" + plan);
        }
    }
}
