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

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ibm.di.event.agent.AgentChainBuilder;
import com.ibm.di.event.agent.AgentDescriptor;
import com.ibm.di.event.agent.EventSubscription;
import com.ibm.di.event.agent.OutputConfig;
import com.ibm.di.event.agent.enrichment.EnrichmentSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Generates the Flink SQL topology of a set of agents for the Process Table Function engine.
 *
 * <p>For each agent it produces, in this order:
 * <ol>
 *   <li>one <b>source</b> per topic subscription (a Kafka table, or the topic table of Confluent
 *       Cloud) normalized to the columns {@code entity_id, event_type, source_topic, payload,
 *       event_ts};</li>
 *   <li>one <b>chained input</b> per chain edge, selecting from the output view of the upstream
 *       agent exactly like {@code AgentChainBuilder.DeferredChainedInputs} does;</li>
 *   <li>the <b>input view</b> {@code <agent>_in}: the {@code UNION ALL} of all of them;</li>
 *   <li>the <b>output view</b> {@code <agent>_out}: the call of the Process Table Function,
 *       partitioned by {@code entity_id};</li>
 *   <li>one <b>sink</b> per configured output topic, the error topic and the changelog topic, each
 *       an {@code INSERT} filtered on {@code out_type}.</li>
 * </ol>
 * The result is pure text, so it can be unit-tested, printed (to be pasted in the Confluent Cloud
 * SQL workspace), or executed by {@link AgentBasedPTFJob}.
 *
 * <h3>Dialects</h3>
 * {@link Dialect#OPEN_SOURCE}: tables use the Kafka connector with a payload-derived event time
 * and watermark; one Process Table Function instance per agent holds its descriptor.
 * {@link Dialect#CONFLUENT}: topics are the tables Confluent Cloud exposes (raw format, columns
 * {@code key} and {@code val}), the event time is the Kafka record timestamp ({@code $rowtime}),
 * and a single function is called with the descriptor as {@code agent_config} argument.
 */
public final class AgentSqlPipelineBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(AgentSqlPipelineBuilder.class);

    /** Target SQL engine. */
    public enum Dialect { OPEN_SOURCE, CONFLUENT }

    /** Name of the agent function in the {@link Dialect#CONFLUENT} dialect. */
    public static final String CONFLUENT_FUNCTION = "event_rule_agent";

    /** Build options. */
    public static final class Options {
        public Dialect dialect = Dialect.OPEN_SOURCE;
        /** Kafka bootstrap servers ({@link Dialect#OPEN_SOURCE} only). */
        public String bootstrapServers = "localhost:9092";
        /** Maximum out-of-orderness of the event-time watermark ({@link Dialect#OPEN_SOURCE} only). */
        public long outOfOrderMs = 5_000L;
        /** Whether chain edges between agents are honoured. */
        public boolean chainingEnabled = true;
        /** Environment used to resolve {@code AGENT_ERROR_TOPIC*}. */
        public Map<String, String> env = Map.of();
    }

    /**
     * One generated statement and where it comes from.
     *
     * @param kind    {@code function}, {@code table}, {@code view} or {@code insert}
     * @param agentId the agent it was generated for, {@code null} for statements shared by all
     *                agents (the Confluent Cloud function registrations)
     * @param sql     the statement, without the trailing semicolon
     */
    public record Statement(String kind, String agentId, String sql) {}

    /** The generated statements and the functions the executor must register. */
    public static final class Pipeline {
        /** CREATE statements, in dependency order. */
        public final List<String> setup = new ArrayList<>();
        /** INSERT statements, to run as one statement set. */
        public final List<String> inserts = new ArrayList<>();
        /**
         * The same statements as {@link #setup} and {@link #inserts}, with their agent, in the
         * order they were generated (agent by agent). Not used to execute anything.
         */
        public final List<Statement> statements = new ArrayList<>();
        /** Open-source dialect: function name to the descriptor it serves. */
        public final Map<String, AgentDescriptor> functions = new LinkedHashMap<>();

        void addSetup(String agentId, String sql) {
            setup.add(sql);
            statements.add(new Statement(kindOf(sql), agentId, sql));
        }

        void addInsert(String agentId, String sql) {
            inserts.add(sql);
            statements.add(new Statement("insert", agentId, sql));
        }

        /** Kind of a CREATE statement, ignoring leading {@code --} comment lines. */
        private static String kindOf(String sql) {
            String s = sql.stripLeading();
            while (s.startsWith("--")) {
                int nl = s.indexOf('\n');
                s = nl < 0 ? "" : s.substring(nl + 1).stripLeading();
            }
            String upper = s.toUpperCase();
            if (upper.startsWith("CREATE FUNCTION")) return "function";
            if (upper.startsWith("CREATE TEMPORARY TABLE")) return "table";
            if (upper.startsWith("CREATE TEMPORARY VIEW")) return "view";
            return "other";
        }

        /**
         * The pipeline as JSON: {@code {"script": "...", "statements": [{"kind", "agentId",
         * "sql"}, ...]}}. {@code script} is {@link #toScript()}; {@code statements} lets a
         * client group the SQL by agent without parsing it.
         */
        public String toJson() {
            ObjectNode root = JsonNodeFactory.instance.objectNode();
            root.put("script", toScript());
            ArrayNode array = root.putArray("statements");
            for (Statement s : statements) {
                ObjectNode n = array.addObject();
                n.put("kind", s.kind());
                n.put("agentId", s.agentId());
                n.put("sql", s.sql());
            }
            return root.toString();
        }

        /** The whole pipeline as a SQL script. */
        public String toScript() {
            StringBuilder sb = new StringBuilder();
            for (String s : setup) sb.append(s).append(";\n\n");
            if (!inserts.isEmpty()) {
                sb.append("EXECUTE STATEMENT SET\nBEGIN\n");
                for (String s : inserts) sb.append(s).append(";\n");
                sb.append("END;\n");
            }
            return sb.toString();
        }
    }

    private final Options options;

    public AgentSqlPipelineBuilder(Options options) {
        this.options = options;
    }

    // ── Build ─────────────────────────────────────────────────────────────────

    /**
     * Builds the pipeline of {@code agents}, which must be in topological order when chaining.
     *
     * @throws IllegalStateException    when the chain graph has a cycle
     * @throws IllegalArgumentException when an upstream agent is declared after its downstream
     */
    public Pipeline build(List<AgentDescriptor> agents) {
        Pipeline p = new Pipeline();
        AgentChainBuilder.DeferredChainedInputs chain = options.chainingEnabled
            ? new AgentChainBuilder().resolveTopology(agents)
            : new AgentChainBuilder().resolveTopology(List.of());

        boolean enrichment = agents.stream().anyMatch(AgentSqlPipelineBuilder::usesEnrichment);

        if (options.dialect == Dialect.CONFLUENT) {
            p.addSetup(null, "-- Register once; the artifact id comes from the Confluent Cloud artifact upload\n"
                + "CREATE FUNCTION " + id(CONFLUENT_FUNCTION) + " AS '"
                + StatefulAgentProcessTableFunction.class.getName()
                + "' USING JAR 'confluent-artifact://<artifact-id>/<version-id>'");
            p.addSetup(null, "CREATE FUNCTION " + id(EventEnvelopeFunction.NAME) + " AS '"
                + EventEnvelopeFunction.class.getName()
                + "' USING JAR 'confluent-artifact://<artifact-id>/<version-id>'");
            if (enrichment) {
                p.addSetup(null, "CREATE FUNCTION " + id(EventEnrichFunction.NAME) + " AS '"
                    + EventEnrichFunction.class.getName()
                    + "' USING JAR 'confluent-artifact://<artifact-id>/<version-id>'");
            }
        }

        for (AgentDescriptor agent : agents) {
            buildAgent(p, agent, chain);
        }
        return p;
    }

    private void buildAgent(Pipeline p, AgentDescriptor agent,
                            AgentChainBuilder.DeferredChainedInputs chain) {
        String agentId = agent.getAgentId();
        String name = sanitize(agentId);
        List<String> inputs = new ArrayList<>();
        Set<String> chained = chain.chainedEventTypes(agentId);

        // 1. Kafka sources
        if (agent.getSubscriptions() != null) {
            for (EventSubscription sub : agent.getSubscriptions()) {
                if (sub.isScheduleSubscription()) continue;
                if (sub.getTopic() == null || sub.getTopic().isBlank()) {
                    LOG.warn("[{}] Subscription {} has no topic — skipping", agentId, sub.getEventType());
                    continue;
                }
                if (chained.contains(sub.getEventType())) {
                    LOG.info("[{}] Suppressing source topic={} eventType={} — covered by chain",
                        agentId, sub.getTopic(), sub.getEventType());
                    if (sub.hasEnrichment()) {
                        LOG.warn("[{}] enrichment on subscription {} is ignored: the event type is "
                            + "received through an agent chain", agentId, sub.getEventType());
                    }
                    continue;
                }
                if (sub.hasSqlEnrichment()) {
                    LOG.warn("[{}] sqlEnrichment on subscription {} is not supported by the Process "
                        + "Table Function engine and is ignored (use 'enrichment')",
                        agentId, sub.getEventType());
                }
                String query = source(p, agent, sub, name + "_src_" + sanitize(sub.getTopic()));
                if (sub.hasEnrichment()) {
                    query = enrichedInput(query, EnrichmentSpec.of("", sub.getEnrichment(), agent.getReferences()));
                }
                inputs.add(query);
            }
        }

        // 2. chained inputs
        for (AgentChainBuilder.EdgeSpec edge : chain.edgesFor(agentId)) {
            inputs.add(chainedInput(edge));
        }

        if (inputs.isEmpty()) {
            LOG.warn("[{}] No subscription sources created — agent skipped", agentId);
            return;
        }

        // 3. input view
        p.addSetup(agentId, createView(name + "_in", String.join("\nUNION ALL\n", inputs)));

        // 4. function call
        String fn = functionName(p, agent, name);
        p.addSetup(agentId, createView(name + "_out", "SELECT * FROM " + id(fn) + "(\n"
            + "    events => TABLE " + id(name + "_in") + " PARTITION BY " + id("entity_id") + ",\n"
            + "    on_time => DESCRIPTOR(" + id("event_ts") + "),\n"
            + (options.dialect == Dialect.CONFLUENT
                ? "    agent_config => " + lit(AgentDescriptors.toJson(agent)) + ",\n" : "")
            + "    uid => " + lit("agent-processor-" + agentId) + ")"));

        // 5. sinks
        buildSinks(p, agent, name);
    }

    /** Whether the agent has a reference-data enrichment on a subscription or an output. */
    private static boolean usesEnrichment(AgentDescriptor agent) {
        return (agent.getSubscriptions() != null
                && agent.getSubscriptions().stream().anyMatch(EventSubscription::hasEnrichment))
            || (agent.getOutputs() != null
                && agent.getOutputs().stream().anyMatch(OutputConfig::hasEnrichment));
    }

    /**
     * Wraps a source query so that the payload column goes through {@code EVENT_ENRICH}. The
     * other columns, in particular the time attribute {@code event_ts}, pass through unchanged.
     */
    private static String enrichedInput(String query, EnrichmentSpec spec) {
        return "SELECT\n"
            + "  " + id("entity_id") + ", " + id("event_type") + ", " + id("source_topic") + ",\n"
            + "  " + EventEnrichFunction.NAME + "(" + id("payload") + ", " + lit(spec.toJson())
            + ") AS " + id("payload") + ",\n"
            + "  " + id("event_ts") + "\n"
            + "FROM (\n" + query + "\n)";
    }

    private String functionName(Pipeline p, AgentDescriptor agent, String name) {
        if (options.dialect == Dialect.CONFLUENT) return CONFLUENT_FUNCTION;
        String fn = "event_rule_" + name;
        p.functions.put(fn, agent);
        return fn;
    }

    // ── Sources ───────────────────────────────────────────────────────────────

    private String source(Pipeline p, AgentDescriptor agent, EventSubscription sub, String table) {
        String extractor = agent.entityKeyExtractorFor(sub);
        String type = sub.getEventType();
        if (options.dialect == Dialect.CONFLUENT) {
            String raw = "CAST(" + id("val") + " AS STRING)";
            return "SELECT\n"
                + "  " + eventField(raw, "entity_id", type, extractor) + " AS " + id("entity_id") + ",\n"
                + "  " + eventField(raw, "event_type", type, extractor) + " AS " + id("event_type") + ",\n"
                + "  CAST(" + lit(sub.getTopic()) + " AS STRING) AS " + id("source_topic") + ",\n"
                + "  " + eventField(raw, "payload", type, extractor) + " AS " + id("payload") + ",\n"
                + "  " + id("$rowtime") + " AS " + id("event_ts") + "\n"
                + "FROM " + id(sub.getTopic());
        }

        String raw = id("raw");
        p.addSetup(agent.getAgentId(), "CREATE TEMPORARY TABLE " + id(table) + " (\n"
            + "  " + id("raw") + " STRING,\n"
            + "  " + id("rec_ts") + " TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',\n"
            + "  " + id("event_ts") + " AS COALESCE(TO_TIMESTAMP_LTZ(CAST("
            + eventField(raw, "ts", type, extractor) + " AS BIGINT), 3), " + id("rec_ts") + "),\n"
            + "  WATERMARK FOR " + id("event_ts") + " AS " + id("event_ts")
            + " - INTERVAL '" + seconds(options.outOfOrderMs) + "' SECOND\n"
            + ") WITH (\n"
            + "  'connector' = 'kafka',\n"
            + "  'topic' = " + lit(sub.getTopic()) + ",\n"
            + "  'properties.bootstrap.servers' = " + lit(options.bootstrapServers) + ",\n"
            + "  'properties.group.id' = " + lit("agent-" + agent.getAgentId()) + ",\n"
            + "  'properties.auto.offset.reset' = 'latest',\n"
            + "  'scan.startup.mode' = 'group-offsets',\n"
            + "  'format' = 'raw'\n"
            + ")");
        return "SELECT\n"
            + "  " + eventField(raw, "entity_id", type, extractor) + " AS " + id("entity_id") + ",\n"
            + "  " + eventField(raw, "event_type", type, extractor) + " AS " + id("event_type") + ",\n"
            + "  CAST(" + lit(sub.getTopic()) + " AS STRING) AS " + id("source_topic") + ",\n"
            + "  " + eventField(raw, "payload", type, extractor) + " AS " + id("payload") + ",\n"
            + "  " + id("event_ts") + "\n"
            + "FROM " + id(table);
    }

    private String chainedInput(AgentChainBuilder.EdgeSpec edge) {
        String env = id("envelope");
        String type = edge.eventType();
        String extractor = edge.downstreamEntityKeyExtractor();
        return "SELECT\n"
            + "  " + eventField(env, "entity_id", type, extractor) + " AS " + id("entity_id") + ",\n"
            + "  " + eventField(env, "event_type", type, extractor) + " AS " + id("event_type") + ",\n"
            + "  CAST(" + (edge.downstreamSubscriptionTopic() == null
                ? "NULL" : lit(edge.downstreamSubscriptionTopic())) + " AS STRING) AS " + id("source_topic") + ",\n"
            + "  " + eventField(env, "payload", type, extractor) + " AS " + id("payload") + ",\n"
            + "  " + id("rowtime") + " AS " + id("event_ts") + "\n"
            + "FROM " + id(sanitize(edge.upstreamAgentId()) + "_out") + "\n"
            + "WHERE " + id("out_type") + " = " + lit(type);
    }

    private static String eventField(String raw, String field, String type, String extractor) {
        return EventEnvelopeFunction.NAME + "(" + raw + ", " + lit(field) + ", " + lit(type) + ", "
            + lit(extractor == null ? "" : extractor) + ")";
    }

    // ── Sinks ─────────────────────────────────────────────────────────────────

    private void buildSinks(Pipeline p, AgentDescriptor agent, String name) {
        String agentId = agent.getAgentId();
        String out = name + "_out";

        if (agent.getOutputs() == null || agent.getOutputs().isEmpty()) {
            LOG.warn("[{}] No output configs declared — processed events discarded", agentId);
            if (options.dialect == Dialect.OPEN_SOURCE) {
                p.addSetup(agentId, "CREATE TEMPORARY TABLE " + id(name + "_print") + " (" + id("v")
                    + " STRING) WITH ('connector' = 'print', 'print-identifier' = "
                    + lit("agent-output-" + agentId) + ")");
                p.addInsert(agentId, "INSERT INTO " + id(name + "_print") + " SELECT " + id("envelope")
                    + " FROM " + id(out));
            }
            return;
        }

        boolean errorDeclared = false;
        for (OutputConfig output : agent.getOutputs()) {
            if (output.getTopic() == null || output.getTopic().isBlank()) continue;
            String type = output.getEventType();
            boolean wildcard = type == null || type.isBlank();
            if (!wildcard && AgentEventProcessor.ERROR_EVENT_TYPE.equals(type)) errorDeclared = true;
            String filter = wildcard
                ? id("out_type") + " <> " + lit(AgentEventProcessor.CHANGELOG_EVENT_TYPE)
                : id("out_type") + " = " + lit(type);
            if (output.hasEnrichment() && wildcard) {
                LOG.warn("[{}] enrichment on the output to topic {} is ignored: it has no eventType",
                    agentId, output.getTopic());
            }
            if (output.hasSqlEnrichment()) {
                LOG.warn("[{}] sqlEnrichment on output {} is not supported by the Process Table "
                    + "Function engine and is ignored (use 'enrichment')", agentId, type);
            }
            EnrichmentSpec spec = output.hasEnrichment() && !wildcard
                ? EnrichmentSpec.of("payload", output.getEnrichment(), agent.getReferences()) : null;
            sink(p, name + "_sink_" + sanitize(output.getTopic()), output.getTopic(), agentId, out, filter, spec);
        }

        if (!errorDeclared) {
            String key = "AGENT_ERROR_TOPIC_" + agentId.toUpperCase().replace('-', '_').replace('.', '_');
            String topic = envOr(key, envOr("AGENT_ERROR_TOPIC", agentId + "-errors"));
            sink(p, name + "_sink_dlq", topic, agentId, out,
                id("out_type") + " = " + lit(AgentEventProcessor.ERROR_EVENT_TYPE), null);
        }

        if (agent.getStateConfig() != null && agent.getStateConfig().hasChangeLogTopic()) {
            sink(p, name + "_sink_changelog", agent.getStateConfig().getChangeLogTopic(), agentId, out,
                id("out_type") + " = " + lit(AgentEventProcessor.CHANGELOG_EVENT_TYPE), null);
        }
    }

    /**
     * @param enrichment applied to the {@code payload} of the envelope before it is written, or
     *                   {@code null}
     */
    private void sink(Pipeline p, String table, String topic, String agentId, String outView,
                      String filter, EnrichmentSpec enrichment) {
        String envelope = enrichment == null ? id("envelope")
            : EventEnrichFunction.NAME + "(" + id("envelope") + ", " + lit(enrichment.toJson()) + ")";
        if (options.dialect == Dialect.CONFLUENT) {
            p.addInsert(agentId, "INSERT INTO " + id(topic) + " (" + id("key") + ", " + id("val") + ")\n"
                + "SELECT CAST(" + id("entity_id") + " AS BYTES), CAST(" + envelope + " AS BYTES)\n"
                + "FROM " + id(outView) + "\nWHERE " + filter);
            return;
        }
        String ddl = "CREATE TEMPORARY TABLE " + id(table) + " (\n"
            + "  " + id("k") + " STRING,\n"
            + "  " + id("v") + " STRING\n"
            + ") WITH (\n"
            + "  'connector' = 'kafka',\n"
            + "  'topic' = " + lit(topic) + ",\n"
            + "  'properties.bootstrap.servers' = " + lit(options.bootstrapServers) + ",\n"
            + "  'key.format' = 'raw',\n"
            + "  'key.fields' = 'k',\n"
            + "  'value.format' = 'raw',\n"
            + "  'value.fields-include' = 'EXCEPT_KEY',\n"
            + "  'sink.delivery-guarantee' = 'at-least-once',\n"
            + "  'sink.transactional-id-prefix' = " + lit(table) + "\n"
            + ")";
        if (!p.setup.contains(ddl)) {
            p.addSetup(agentId, ddl);
        }
        p.addInsert(agentId, "INSERT INTO " + id(table) + "\n"
            + "SELECT " + id("entity_id") + ", " + envelope + "\n"
            + "FROM " + id(outView) + "\nWHERE " + filter);
        LOG.info("[{}] Wired Kafka sink → topic={}", agentId, topic);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private String envOr(String name, String def) {
        String v = options.env.get(name);
        return v != null && !v.isBlank() ? v : def;
    }

    private static String createView(String name, String query) {
        return "CREATE TEMPORARY VIEW " + id(name) + " AS\n" + query;
    }

    /** Lower-case identifier safe for table, view and function names. */
    static String sanitize(String agentId) {
        return agentId.replaceAll("[^A-Za-z0-9]", "_");
    }

    /** Backtick-quoted identifier. */
    static String id(String name) {
        return "`" + name.replace("`", "``") + "`";
    }

    /** Single-quoted SQL string literal. */
    static String lit(String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    /** Milliseconds as a decimal number of seconds with three fractional digits. */
    static String seconds(long ms) {
        return (ms / 1000) + "." + String.format("%03d", ms % 1000);
    }
}
