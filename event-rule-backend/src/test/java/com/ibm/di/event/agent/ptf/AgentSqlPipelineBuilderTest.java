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

import com.ibm.di.event.agent.AgentDescriptorLoader;
import com.ibm.di.event.agent.AgentDescriptor;
import com.ibm.di.event.agent.EventSubscription;
import com.ibm.di.event.agent.HorizonConfig;
import com.ibm.di.event.agent.OutputConfig;
import com.ibm.di.event.agent.StateConfig;
import com.ibm.di.event.agent.ptf.AgentSqlPipelineBuilder.Dialect;
import com.ibm.di.event.agent.ptf.AgentSqlPipelineBuilder.Options;
import com.ibm.di.event.agent.ptf.AgentSqlPipelineBuilder.Pipeline;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static com.ibm.di.event.agent.ptf.PtfTestSupport.*;
import static org.junit.jupiter.api.Assertions.*;

@DisplayName("AgentSqlPipelineBuilder and AgentJobLauncher")
class AgentSqlPipelineBuilderTest {

    private static Pipeline build(Dialect dialect, boolean chaining, Map<String, String> env,
                                  AgentDescriptor... agents) {
        Options o = new Options();
        o.dialect = dialect;
        o.chainingEnabled = chaining;
        o.env = env;
        return new AgentSqlPipelineBuilder(o).build(List.of(agents));
    }

    private static boolean anySetup(Pipeline p, String fragment) {
        return p.setup.stream().anyMatch(s -> s.contains(fragment));
    }

    private static boolean anyInsert(Pipeline p, String fragment) {
        return p.inserts.stream().anyMatch(s -> s.contains(fragment));
    }

    private static AgentDescriptor basic() {
        AgentDescriptor d = descriptor(operation());
        d.setStateConfig(stateConfig("1h", "test-changes"));
        return d;
    }

    @Test
    @DisplayName("open source: Kafka source with watermark, function per agent, sinks, DLQ and changelog")
    void openSourceSingleAgent() {
        Pipeline p = build(Dialect.OPEN_SOURCE, true, Map.of(), basic());

        assertTrue(anySetup(p, "CREATE TEMPORARY TABLE `test_agent_src_domain_events`"));
        assertTrue(anySetup(p, "'topic' = 'domain-events'"));
        assertTrue(anySetup(p, "WATERMARK FOR `event_ts` AS `event_ts` - INTERVAL '5.000' SECOND"));
        assertTrue(anySetup(p, "CREATE TEMPORARY VIEW `test_agent_in`"));
        assertTrue(anySetup(p, "CREATE TEMPORARY VIEW `test_agent_out`"));
        assertTrue(anySetup(p, "FROM `event_rule_test_agent`("));
        assertTrue(anySetup(p, "events => TABLE `test_agent_in` PARTITION BY `entity_id`"));
        assertTrue(anySetup(p, "on_time => DESCRIPTOR(`event_ts`)"));
        assertTrue(anySetup(p, "uid => 'agent-processor-test-agent'"));
        assertFalse(anySetup(p, "agent_config"));
        assertEquals(Map.of("event_rule_test_agent", p.functions.get("event_rule_test_agent")),
            p.functions);

        assertEquals(3, p.inserts.size(), "output, DLQ, changelog");
        assertTrue(anySetup(p, "CREATE TEMPORARY TABLE `test_agent_sink_alerts`"));
        assertTrue(anyInsert(p, "INSERT INTO `test_agent_sink_alerts`"));
        assertTrue(anyInsert(p, "`out_type` = 'alert'"));
        assertTrue(anyInsert(p, "`out_type` = 'agent-error'"));
        assertTrue(anyInsert(p, "`out_type` = 'entity-changelog'"));
        assertTrue(anySetup(p, "'topic' = 'test-agent-errors'"));
        assertTrue(anySetup(p, "'topic' = 'test-changes'"));
    }

    @Test
    @DisplayName("the Schedule subscription has no source")
    void scheduleSubscriptionHasNoSource() {
        Pipeline p = build(Dialect.OPEN_SOURCE, true, Map.of(), basic());
        assertFalse(anySetup(p, "test_agent_src_schedule"));
    }

    @Test
    @DisplayName("the error topic comes from the environment, agent-specific first")
    void errorTopicFromEnvironment() {
        Pipeline general = build(Dialect.OPEN_SOURCE, true,
            Map.of("AGENT_ERROR_TOPIC", "all-errors"), basic());
        assertTrue(anySetup(general, "'topic' = 'all-errors'"));

        Pipeline specific = build(Dialect.OPEN_SOURCE, true,
            Map.of("AGENT_ERROR_TOPIC", "all-errors", "AGENT_ERROR_TOPIC_TEST_AGENT", "mine"), basic());
        assertTrue(anySetup(specific, "'topic' = 'mine'"));
    }

    @Test
    @DisplayName("a declared agent-error output replaces the default DLQ")
    void declaredErrorOutput() {
        AgentDescriptor d = basic();
        d.setOutputs(List.of(new OutputConfig("alert", "alerts"), new OutputConfig("agent-error", "errs")));
        Pipeline p = build(Dialect.OPEN_SOURCE, true, Map.of(), d);
        assertEquals(3, p.inserts.size(), "alert, error, changelog");
        assertFalse(anySetup(p, "test-agent-errors"));
    }

    @Test
    @DisplayName("a wildcard output takes everything but the changelog")
    void wildcardOutput() {
        AgentDescriptor d = basic();
        d.setOutputs(List.of(new OutputConfig(null, "everything")));
        Pipeline p = build(Dialect.OPEN_SOURCE, true, Map.of(), d);
        assertTrue(anyInsert(p, "`out_type` <> 'entity-changelog'"));
    }

    @Test
    @DisplayName("without outputs the events go to a print sink")
    void noOutputs() {
        AgentDescriptor d = basic();
        d.setOutputs(List.of());
        Pipeline p = build(Dialect.OPEN_SOURCE, true, Map.of(), d);
        assertTrue(anySetup(p, "'connector' = 'print'"));
        assertEquals(1, p.inserts.size());

        Pipeline confluent = build(Dialect.CONFLUENT, true, Map.of(), d);
        assertTrue(confluent.inserts.isEmpty());
    }

    @Test
    @DisplayName("a subscription without a topic is skipped, and an agent without a source is dropped")
    void noTopic() {
        AgentDescriptor d = basic();
        d.setSubscriptions(List.of(new EventSubscription("domain-event", null, "Op", HorizonConfig.count(1))));
        Pipeline p = build(Dialect.OPEN_SOURCE, true, Map.of(), d);
        assertTrue(p.setup.isEmpty());
        assertTrue(p.inserts.isEmpty());
    }

    @Test
    @DisplayName("confluent: topic tables, one function with agent_config, key/val sinks, no DDL")
    void confluentDialect() {
        Pipeline p = build(Dialect.CONFLUENT, true, Map.of(), basic());

        assertTrue(anySetup(p, "CREATE FUNCTION `event_rule_agent` AS "
            + "'com.ibm.di.event.agent.ptf.StatefulAgentProcessTableFunction' USING JAR"));
        assertTrue(anySetup(p, "CREATE FUNCTION `EVENT_FIELD` AS"));
        assertTrue(anySetup(p, "FROM `domain-events`"));
        assertTrue(anySetup(p, "`$rowtime` AS `event_ts`"));
        assertTrue(anySetup(p, "CAST(`val` AS STRING)"));
        assertTrue(anySetup(p, "agent_config => '"));
        assertFalse(anySetup(p, "'connector'"));
        assertFalse(anySetup(p, "WATERMARK"));
        assertTrue(p.functions.isEmpty());
        assertTrue(anyInsert(p, "INSERT INTO `alerts` (`key`, `val`)"));
        assertTrue(anyInsert(p, "CAST(`envelope` AS BYTES)"));
    }

    @Test
    @DisplayName("agent_config quotes are escaped")
    void agentConfigIsEscaped() {
        AgentDescriptor d = basic();
        d.setName("O'Brien agent");
        Pipeline p = build(Dialect.CONFLUENT, true, Map.of(), d);
        assertTrue(anySetup(p, "O''Brien agent"));
    }

    @Test
    @DisplayName("the script has the setup statements, then one statement set with the inserts")
    void script() {
        Pipeline p = build(Dialect.OPEN_SOURCE, true, Map.of(), basic());
        String script = p.toScript();
        assertTrue(script.indexOf("CREATE TEMPORARY TABLE") < script.indexOf("EXECUTE STATEMENT SET"));
        assertTrue(script.trim().endsWith("END;"));
        assertFalse(new Pipeline().toScript().contains("EXECUTE STATEMENT SET"));
    }

    @Test
    @DisplayName("the air traffic chain feeds the airport agent from the aircraft output view")
    void airTrafficChain() throws Exception {
        List<AgentDescriptor> agents = AgentDescriptorLoader.loadFileDescriptor("../samples/airtraffic-agent/agents.json").agents();
        Options o = new Options();
        o.chainingEnabled = true;

        Pipeline chained = new AgentSqlPipelineBuilder(o).build(agents);

        assertTrue(anySetup(chained, "FROM `aircraft_agent_out`"));
        assertTrue(anySetup(chained, "WHERE `out_type` = 'TrackedAircraftEvent'"));
        assertEquals(2, chained.functions.size());
        assertFalse(anySetup(chained, "airport_agent_src_agent_tracked_aircraft_events"),
            "the chained event type must not also be read from Kafka");

        o.chainingEnabled = false;
        Pipeline unchained = new AgentSqlPipelineBuilder(o).build(agents);
        assertFalse(anySetup(unchained, "FROM `aircraft_agent_out`"));
        assertTrue(anySetup(unchained, "airport_agent_src_agent_tracked_aircraft_events"));
    }

    @Test
    @DisplayName("every sample descriptor builds for both dialects")
    void samplesBuild() throws Exception {
        for (String sample : List.of("banking-fraud-agent", "airline-agent", "airtraffic-agent", "retail-agent")) {
            List<AgentDescriptor> agents = AgentDescriptorLoader.loadFileDescriptor("../samples/" + sample + "/agents.json").agents();
            for (Dialect dialect : Dialect.values()) {
                Options o = new Options();
                o.dialect = dialect;
                Pipeline p = new AgentSqlPipelineBuilder(o).build(agents);
                assertFalse(p.inserts.isEmpty(), sample + " " + dialect);
            }
        }
    }

    @Test
    @DisplayName("sqlEnrichment is ignored with a warning, not fatal")
    void sqlEnrichmentIgnored() {
        AgentDescriptor d = basic();
        EventSubscription sub = d.getSubscriptions().get(0);
        sub.setSqlEnrichment(new com.ibm.di.event.agent.SqlEnrichmentConfig("enrich.sql", "in", "out"));
        assertTrue(sub.hasSqlEnrichment());
        assertDoesNotThrow(() -> build(Dialect.OPEN_SOURCE, true, Map.of(), d));
    }

    @Test
    @DisplayName("reference-data enrichment: EVENT_ENRICH wraps the subscription source and the output sink")
    void referenceEnrichment() throws Exception {
        List<AgentDescriptor> agents = AgentDescriptorLoader.loadFileDescriptor("../samples/airline-agent/agents.json").agents();

        Pipeline open = new AgentSqlPipelineBuilder(new Options()).build(agents);
        assertTrue(anySetup(open, "EVENT_ENRICH("), "subscription enrichment");
        assertTrue(anyInsert(open, "EVENT_ENRICH("), "output enrichment");

        Options c = new Options();
        c.dialect = Dialect.CONFLUENT;
        Pipeline confluent = new AgentSqlPipelineBuilder(c).build(agents);
        assertTrue(anySetup(confluent, "CREATE FUNCTION `EVENT_ENRICH` AS "
            + "'com.ibm.di.event.agent.ptf.EventEnrichFunction' USING JAR"));
        assertTrue(anySetup(confluent, "EVENT_ENRICH("));
        assertTrue(anyInsert(confluent, "EVENT_ENRICH("));
    }

    @Test
    @DisplayName("no enrichment: no EVENT_ENRICH in the pipeline")
    void noEnrichmentNoFunction() {
        Pipeline open = build(Dialect.OPEN_SOURCE, true, Map.of(), basic());
        assertFalse(anySetup(open, "EVENT_ENRICH"));
        assertFalse(anyInsert(open, "EVENT_ENRICH"));
        Pipeline confluent = build(Dialect.CONFLUENT, true, Map.of(), basic());
        assertFalse(anySetup(confluent, "EVENT_ENRICH"));
    }

    @Test
    @DisplayName("statements mirror setup and inserts, each tagged with its agent and kind")
    void statementsMirrorScript() throws Exception {
        List<AgentDescriptor> agents = AgentDescriptorLoader.loadFileDescriptor("../samples/airtraffic-agent/agents.json").agents();
        for (Dialect dialect : Dialect.values()) {
            Options o = new Options();
            o.dialect = dialect;
            Pipeline p = new AgentSqlPipelineBuilder(o).build(agents);

            List<String> setup = p.statements.stream().filter(s -> !s.kind().equals("insert"))
                .map(AgentSqlPipelineBuilder.Statement::sql).toList();
            List<String> inserts = p.statements.stream().filter(s -> s.kind().equals("insert"))
                .map(AgentSqlPipelineBuilder.Statement::sql).toList();
            assertEquals(p.setup, setup, dialect + ": setup");
            assertEquals(p.inserts, inserts, dialect + ": inserts");
            assertTrue(p.statements.stream().noneMatch(s -> s.kind().equals("other")), dialect + ": every kind known");
        }
    }

    @Test
    @DisplayName("statements are attributed to their agent; shared Confluent functions have none")
    void statementsAreAttributed() throws Exception {
        List<AgentDescriptor> agents = AgentDescriptorLoader.loadFileDescriptor("../samples/airtraffic-agent/agents.json").agents();

        Pipeline open = new AgentSqlPipelineBuilder(new Options()).build(agents);
        for (String agent : List.of("aircraft-agent", "airport-agent")) {
            String name = AgentSqlPipelineBuilder.sanitize(agent);
            assertTrue(open.statements.stream().anyMatch(s -> agent.equals(s.agentId())
                && s.kind().equals("view") && s.sql().startsWith("CREATE TEMPORARY VIEW `" + name + "_in`")), agent + " input view");
            assertTrue(open.statements.stream().anyMatch(s -> agent.equals(s.agentId()) && s.kind().equals("insert")), agent + " insert");
        }
        assertTrue(open.statements.stream().allMatch(s -> s.agentId() != null), "open source has no shared statements");

        Options c = new Options();
        c.dialect = Dialect.CONFLUENT;
        Pipeline confluent = new AgentSqlPipelineBuilder(c).build(agents);
        List<AgentSqlPipelineBuilder.Statement> shared = confluent.statements.stream().filter(s -> s.agentId() == null).toList();
        assertFalse(shared.isEmpty());
        assertTrue(shared.stream().allMatch(s -> s.kind().equals("function")),
            "the leading comment must not hide the CREATE FUNCTION kind");
    }

    @Test
    @DisplayName("toJson carries the script and the statements, with quotes and newlines escaped")
    void toJson() throws Exception {
        AgentDescriptor d = basic();
        d.setName("O'Brien \"agent\"");
        Pipeline p = build(Dialect.CONFLUENT, true, Map.of(), d);

        com.fasterxml.jackson.databind.JsonNode json = new com.fasterxml.jackson.databind.ObjectMapper().readTree(p.toJson());
        assertEquals(p.toScript(), json.get("script").asText());
        assertEquals(p.statements.size(), json.get("statements").size());
        com.fasterxml.jackson.databind.JsonNode first = json.get("statements").get(0);
        assertEquals("function", first.get("kind").asText());
        assertTrue(first.get("agentId").isNull());
        assertEquals(p.statements.get(0).sql(), first.get("sql").asText());
        assertTrue(json.get("script").asText().contains("O''Brien"), "string content survives the round trip");
    }

    @Test
    @DisplayName("an empty pipeline renders as empty JSON arrays")
    void emptyJson() throws Exception {
        com.fasterxml.jackson.databind.JsonNode json = new com.fasterxml.jackson.databind.ObjectMapper().readTree(new Pipeline().toJson());
        assertEquals("", json.get("script").asText());
        assertEquals(0, json.get("statements").size());
    }

    @Test
    @DisplayName("helpers quote identifiers and literals and format seconds")
    void helpers() {
        assertEquals("a_b_c", AgentSqlPipelineBuilder.sanitize("a-b.c"));
        assertEquals("`a``b`", AgentSqlPipelineBuilder.id("a`b"));
        assertEquals("'it''s'", AgentSqlPipelineBuilder.lit("it's"));
        assertEquals("5.000", AgentSqlPipelineBuilder.seconds(5000));
        assertEquals("0.050", AgentSqlPipelineBuilder.seconds(50));
    }

    @Test
    @DisplayName("the launcher selects datastream by default and accepts ptf")
    void launcher() {
        assertEquals(AgentJobLauncher.Runtime.DATASTREAM, AgentJobLauncher.resolve(null));
        assertEquals(AgentJobLauncher.Runtime.DATASTREAM, AgentJobLauncher.resolve(" "));
        assertEquals(AgentJobLauncher.Runtime.DATASTREAM, AgentJobLauncher.resolve("DataStream"));
        assertEquals(AgentJobLauncher.Runtime.PTF, AgentJobLauncher.resolve(" PTF "));
        assertEquals(AgentJobLauncher.Runtime.PTF, AgentJobLauncher.resolve("sql"));
        assertThrows(IllegalArgumentException.class, () -> AgentJobLauncher.resolve("spark"));
    }

    @Test
    @DisplayName("StateConfig helper used by the fixtures keeps its ttl")
    void fixtureSanity() {
        StateConfig sc = stateConfig("2h", null);
        assertEquals("2h", sc.getTtl());
        assertFalse(sc.hasChangeLogTopic());
    }
}
