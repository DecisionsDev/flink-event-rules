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
import com.ibm.di.event.agent.AgentDescriptor;
import com.ibm.di.event.agent.EventEnvelope;
import com.ibm.di.event.agent.EventSubscription;
import com.ibm.di.event.agent.JsonPathUtils;
import com.ibm.di.event.agent.OutputConfig;
import com.ibm.di.event.agent.SqlEnrichmentConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.Schema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;

/**
 * Handles execution of Flink Table / SQL enrichment nodes embedded between
 * Kafka sources and the {@link com.ibm.di.event.agent.StatefulAgentProcessor}.
 */
public class SqlEnrichmentRunner {

    private static final Logger LOG = LoggerFactory.getLogger(SqlEnrichmentRunner.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Wraps an outbound {@link DataStream}{@code <EventEnvelope>} produced by a
     * {@link com.ibm.di.event.agent.StatefulAgentProcessor} with a Flink SQL enrichment layer.
     *
     * <p>Each envelope's payload is serialised back to a JSON string and fed into
     * {@link #enrichStream} using the schema declared on the {@link OutputConfig}.
     * The output contract is identical to the inbound enrichment path: the SQL must
     * produce a view with three columns — {@code timestamp}, {@code entity_id}, and
     * {@code payload_json}.
     *
     * @param env              Flink execution environment
     * @param outboundStream   stream of envelopes emitted by the agent for this output type
     * @param output           output configuration carrying the SQL enrichment config and schema path
     * @param outOfOrderMs     bounded out-of-orderness watermark tolerance (milliseconds)
     * @param idlenessMs       source idleness timeout (milliseconds)
     * @param entityKeyExtractor JSONPath expression used to extract the entity ID as fallback
     * @return enriched stream of {@link EventEnvelope}s ready for the Kafka sink
     * @throws IOException     if the schema file or SQL file cannot be read
     * @throws IllegalArgumentException if a required schema path is missing or blank
     */
    public static DataStream<EventEnvelope> buildSqlEnrichedOutboundStream(
            StreamExecutionEnvironment env,
            DataStream<EventEnvelope> outboundStream,
            OutputConfig output,
            long outOfOrderMs,
            long idlenessMs,
            String entityKeyExtractor) throws IOException {

        SqlEnrichmentConfig sqlConfig = output.getSqlEnrichment();
        String eventType = output.getEventType();
        String schemaPath = output.getSchema();

        if (schemaPath == null || schemaPath.isBlank()) {
            throw new IllegalArgumentException("Output for eventType='" + eventType
                + "' defines sqlEnrichment but no 'schema' path is specified on the output config.");
        }

        String schemaJson = readFile(schemaPath);
        JsonNode rootSchema = MAPPER.readTree(schemaJson);
        Schema flinkSchema = JsonSchemaToFlinkConverter.convertToTableSchema(rootSchema);

        // Map each envelope payload to a raw JSON string so enrichStream can process it
        DataStream<String> rawJsonStream = outboundStream.map(env2 -> {
            JsonNode payload = env2.getPayload();
            return payload != null ? payload.toString() : "{}";
        });

        return enrichStream(env, rawJsonStream, sqlConfig, schemaJson, flinkSchema,
                eventType, output.getTopic(), outOfOrderMs, idlenessMs, entityKeyExtractor);
    }

    public static DataStream<EventEnvelope> buildSqlEnrichedStream(
            StreamExecutionEnvironment env,
            EventSubscription sub,
            String topicSchemaPath,
            String consumerGroup,
            String bootstrapServers,
            long outOfOrderMs,
            long idlenessMs,
            AgentDescriptor agent,
            String entityKeyExtractor) throws IOException {

        SqlEnrichmentConfig sqlConfig = sub.getSqlEnrichment();
        String topic = sub.getTopic();
        String eventType = sub.getEventType();

        if (topicSchemaPath == null || topicSchemaPath.isBlank()) {
            throw new IllegalArgumentException("Subscription for eventType='" + eventType
                + "' on topic='" + topic + "' defines sqlEnrichment, but no 'schema' file is specified on the "
                + "subscription or for topic '" + topic + "' in the top-level 'topics' section of agents.json");
        }

        String schemaJson = readFile(topicSchemaPath);
        JsonNode rootSchema = MAPPER.readTree(schemaJson);
        Schema flinkSchema = JsonSchemaToFlinkConverter.convertToTableSchema(rootSchema);

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
            .setBootstrapServers(bootstrapServers)
            .setTopics(topic)
            .setGroupId(consumerGroup)
            .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        DataStream<String> rawStream = env.fromSource(
            kafkaSource,
            WatermarkStrategy.noWatermarks(),
            "KafkaRaw[" + agent.getAgentId() + "/" + topic + "]"
        );

        return enrichStream(env, rawStream, sqlConfig, schemaJson, flinkSchema, eventType, topic,
            outOfOrderMs, idlenessMs, entityKeyExtractor);
    }

    public static DataStream<EventEnvelope> enrichStream(
            StreamExecutionEnvironment env,
            DataStream<String> rawJsonStream,
            SqlEnrichmentConfig sqlConfig,
            String schemaJson,
            Schema flinkSchema,
            String eventType,
            String topic,
            long outOfOrderMs,
            long idlenessMs,
            String entityKeyExtractor) throws IOException {

        OutputTag<EventEnvelope> sentinelTag = new OutputTag<EventEnvelope>("sentinel-stream") {};

        SingleOutputStreamOperator<String> filteredRawStream = rawJsonStream
            .process(new SentinelFilterFunction(sentinelTag, topic, entityKeyExtractor));

        DataStream<EventEnvelope> sentinelStream = filteredRawStream.getSideOutput(sentinelTag);

        JsonNode rootSchema = MAPPER.readTree(schemaJson);
        org.apache.flink.api.java.typeutils.RowTypeInfo rowTypeInfo =
            JsonSchemaToFlinkConverter.convertToRowTypeInfo(rootSchema);

        WatermarkStrategy<Row> rowWatermarks = WatermarkStrategy
            .<Row>forBoundedOutOfOrderness(Duration.ofMillis(outOfOrderMs))
            .withTimestampAssigner((row, recordTs) -> {
                try {
                    Object tsObj = row.getField("timestamp");
                    if (tsObj != null) {
                        if (tsObj instanceof Number) return ((Number) tsObj).longValue();
                        return Instant.parse(tsObj.toString()).toEpochMilli();
                    }
                } catch (Exception ignored) {}
                return recordTs > 0 ? recordTs : System.currentTimeMillis();
            })
            .withIdleness(Duration.ofMillis(idlenessMs));

        DataStream<Row> rowStream = filteredRawStream
            .map(new JsonToRowMapper(schemaJson))
            .returns(rowTypeInfo)
            .assignTimestampsAndWatermarks(rowWatermarks);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.createTemporaryView(sqlConfig.getInputViewName(), rowStream, flinkSchema);

        String sqlScript = readFile(sqlConfig.getSqlFile());
        List<String> statements = splitSqlStatements(sqlScript);
        for (String stmt : statements) {
            String resolvedStmt = resolveSqlPaths(stmt);
            LOG.info("[SqlEnrichment] Executing SQL statement: {}", resolvedStmt.replaceAll("\\s+", " "));
            tableEnv.executeSql(resolvedStmt);
        }

        Table outputTable = tableEnv.from(sqlConfig.getOutputViewName());
        DataStream<Row> changelogStream = tableEnv.toChangelogStream(outputTable);

        DataStream<Row> deduplicatedRowStream = changelogStream
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<Row>forBoundedOutOfOrderness(Duration.ofMillis(outOfOrderMs))
                    .withTimestampAssigner((row, recordTs) -> {
                        try {
                            Object tsObj = row.getField("timestamp");
                            if (tsObj != null) {
                                if (tsObj instanceof Number) return ((Number) tsObj).longValue();
                                return Instant.parse(tsObj.toString()).toEpochMilli();
                            }
                        } catch (Exception ignored) {}
                        return recordTs > 0 ? recordTs : System.currentTimeMillis();
                    })
                    .withIdleness(Duration.ofMillis(idlenessMs)))
            .keyBy(row -> {
                try {
                    Object id = row.getField("entity_id");
                    if (id != null) return id.toString();
                } catch (Exception ignored) {}
                return row.getArity() > 1 && row.getField(1) != null ? row.getField(1).toString() : "default";
            })
            .process(new ChangelogDeduplicationFunction());

        DataStream<EventEnvelope> envelopeStream = deduplicatedRowStream.map(row -> {
            String timestampVal = null;
            String entityIdVal = null;
            String payloadJsonVal = null;

            try {
                Object tsObj = row.getField("timestamp");
                if (tsObj != null) timestampVal = tsObj.toString();
            } catch (Exception ignored) {
                if (row.getArity() > 0 && row.getField(0) != null) timestampVal = row.getField(0).toString();
            }

            try {
                Object entityObj = row.getField("entity_id");
                if (entityObj != null) entityIdVal = entityObj.toString();
            } catch (Exception ignored) {
                if (row.getArity() > 1 && row.getField(1) != null) entityIdVal = row.getField(1).toString();
            }

            try {
                Object payloadObj = row.getField("payload_json");
                if (payloadObj != null) payloadJsonVal = payloadObj.toString();
            } catch (Exception ignored) {
                if (row.getArity() > 2 && row.getField(2) != null) payloadJsonVal = row.getField(2).toString();
            }

            long ts = 0L;
            if (timestampVal != null && !timestampVal.isBlank()) {
                try {
                    ts = Instant.parse(timestampVal).toEpochMilli();
                } catch (DateTimeParseException ex) {
                    try {
                        ts = Long.parseLong(timestampVal);
                    } catch (NumberFormatException ignored) {}
                }
            }

            JsonNode payloadNode = null;
            if (payloadJsonVal != null && !payloadJsonVal.isBlank()) {
                try {
                    payloadNode = unescapeJsonStrings(MAPPER.readTree(payloadJsonVal));
                } catch (Exception ignored) {}
            }

            if ((entityIdVal == null || entityIdVal.isBlank() || "unknown".equalsIgnoreCase(entityIdVal))
                    && payloadNode != null && entityKeyExtractor != null) {
                entityIdVal = JsonPathUtils.extractEntityId(payloadNode, entityKeyExtractor);
            }

            String effectiveEventType = eventType;
            if (payloadNode != null && payloadNode.has("__eventType")) {
                String override = payloadNode.get("__eventType").asText(null);
                if (override != null && !override.isBlank()) {
                    effectiveEventType = override;
                }
            }

            return EventEnvelope.from(effectiveEventType, topic, ts, payloadNode, entityIdVal);
        });

        DataStream<EventEnvelope> unionedStream = envelopeStream.union(sentinelStream);

        WatermarkStrategy<EventEnvelope> watermarks = WatermarkStrategy
            .<EventEnvelope>forBoundedOutOfOrderness(Duration.ofMillis(outOfOrderMs))
            .withTimestampAssigner((envelope, recordTs) ->
                envelope.getTimestamp() > 0 ? envelope.getTimestamp() : recordTs)
            .withIdleness(Duration.ofMillis(idlenessMs));

        return unionedStream.assignTimestampsAndWatermarks(watermarks);
    }

    private static long extractTimestamp(JsonNode payload) {
        long ts = 0L;
        if (payload != null && payload.has("timestamp")) {
            JsonNode tsNode = payload.get("timestamp");
            if (tsNode.isNumber()) ts = tsNode.asLong();
            if (tsNode.isTextual()) {
                try {
                    ts = Instant.parse(tsNode.asText()).toEpochMilli();
                } catch (DateTimeParseException ignored) {}
            }
        }
        return ts > 0L ? ts : System.currentTimeMillis();
    }

    static class SentinelFilterFunction extends ProcessFunction<String, String> {
        private static final long serialVersionUID = 1L;
        private final OutputTag<EventEnvelope> sentinelTag;
        private final String topic;
        private final String entityKeyExtractor;

        public SentinelFilterFunction(OutputTag<EventEnvelope> sentinelTag, String topic, String entityKeyExtractor) {
            this.sentinelTag = sentinelTag;
            this.topic = topic;
            this.entityKeyExtractor = entityKeyExtractor;
        }

        @Override
        public void processElement(String jsonString, Context ctx, Collector<String> out) throws Exception {
            try {
                JsonNode node = MAPPER.readTree(jsonString);
                if (node != null && node.has("__eventType")) {
                    String override = node.get("__eventType").asText(null);
                    if (override != null && !override.isBlank()) {
                        long ts = extractTimestamp(node);
                        String entityId = JsonPathUtils.extractEntityId(node, entityKeyExtractor);
                        EventEnvelope sentinelEnv = EventEnvelope.from(override, topic, ts, node, entityId);
                        ctx.output(sentinelTag, sentinelEnv);
                        return;
                    }
                }
            } catch (Exception ignored) {}
            out.collect(jsonString);
        }
    }

    /**
     * Deduplicates speculative INSERT/UPDATE_AFTER pairs produced by Flink streaming joins.
     */
    public static class ChangelogDeduplicationFunction
            extends org.apache.flink.streaming.api.functions.KeyedProcessFunction<String, Row, Row> {

        private transient org.apache.flink.api.common.state.MapState<String, Row> pendingInserts;

        @Override
        public void open(org.apache.flink.api.common.functions.OpenContext openContext) throws Exception {
            super.open(openContext);
            var stateDesc = new org.apache.flink.api.common.state.MapStateDescriptor<>(
                "pending-inserts", String.class, Row.class);
            pendingInserts = getRuntimeContext().getMapState(stateDesc);
        }

        private String eventKey(Row row) {
            Object ts = null;
            Object id = null;
            try {
                ts = row.getField("timestamp");
            } catch (Exception ignored) {}
            if (ts == null && row.getArity() > 0) ts = row.getField(0);

            try {
                id = row.getField("entity_id");
            } catch (Exception ignored) {}
            if (id == null && row.getArity() > 1) id = row.getField(1);

            return (ts != null ? ts.toString() : "") + "||" + (id != null ? id.toString() : "");
        }

        @Override
        public void processElement(Row row, Context ctx, org.apache.flink.util.Collector<Row> out) throws Exception {
            String key = eventKey(row);
            if (row.getKind() == org.apache.flink.types.RowKind.UPDATE_AFTER) {
                pendingInserts.remove(key);
                out.collect(row);
            } else if (row.getKind() == org.apache.flink.types.RowKind.UPDATE_BEFORE) {
                pendingInserts.remove(key);
            } else if (row.getKind() == org.apache.flink.types.RowKind.INSERT) {
                pendingInserts.put(key, row);
                long currentWatermark = ctx.timerService().currentWatermark();
                long ts = ctx.timestamp();
                long timerTs = Math.max(ts, currentWatermark) + 1;
                ctx.timerService().registerEventTimeTimer(timerTs);
                ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 20);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, org.apache.flink.util.Collector<Row> out) throws Exception {
            List<String> keysToRemove = new ArrayList<>();
            for (var entry : pendingInserts.entries()) {
                out.collect(entry.getValue());
                keysToRemove.add(entry.getKey());
            }
            for (String k : keysToRemove) {
                pendingInserts.remove(k);
            }
        }
    }

    /**
     * Flink 2.2 JSON_OBJECT escapes any STRING value as a JSON string literal, even when
     * that string contains a valid JSON array or object (e.g. from JSON_ARRAYAGG).
     * This method walks the parsed JsonNode tree and re-parses any string-valued field
     * whose content starts with '[' or '{' back into a proper JsonNode, so that the
     * downstream deserializer receives a real array/object rather than an escaped string.
     */
    static JsonNode unescapeJsonStrings(JsonNode node) {
        if (node == null) return null;
        if (node.isObject()) {
            com.fasterxml.jackson.databind.node.ObjectNode obj =
                (com.fasterxml.jackson.databind.node.ObjectNode) node;
            obj.fields().forEachRemaining(entry -> {
                JsonNode child = entry.getValue();
                if (child.isTextual()) {
                    String text = child.textValue().trim();
                    if ((text.startsWith("[") || text.startsWith("{")) ) {
                        try {
                            obj.set(entry.getKey(), MAPPER.readTree(text));
                        } catch (Exception ignored) {}
                    }
                } else {
                    obj.set(entry.getKey(), unescapeJsonStrings(child));
                }
            });
        } else if (node.isArray()) {
            com.fasterxml.jackson.databind.node.ArrayNode arr =
                (com.fasterxml.jackson.databind.node.ArrayNode) node;
            for (int i = 0; i < arr.size(); i++) {
                arr.set(i, unescapeJsonStrings(arr.get(i)));
            }
        }
        return node;
    }

    public static List<String> splitSqlStatements(String sqlScript) {
        List<String> list = new ArrayList<>();
        String[] lines = sqlScript.split("\\r?\\n");
        StringBuilder current = new StringBuilder();
        for (String line : lines) {
            String trimmed = line.trim();
            if (trimmed.startsWith("--")) {
                continue;
            }
            if (trimmed.contains("--")) {
                int commentIdx = line.indexOf("--");
                line = line.substring(0, commentIdx);
            }
            current.append(line).append("\n");
            if (current.toString().contains(";")) {
                int semi = current.indexOf(";");
                String stmt = current.substring(0, semi).trim();
                if (!stmt.isEmpty()) {
                    list.add(stmt);
                }
                current.delete(0, semi + 1);
            }
        }
        String rem = current.toString().trim();
        if (!rem.isEmpty()) {
            list.add(rem);
        }
        return list;
    }

    public static String readFile(String path) throws IOException {
        File file = resolveFile(path);
        if (file != null && file.exists()) {
            try (InputStream in = new FileInputStream(file)) {
                return new String(in.readAllBytes(), StandardCharsets.UTF_8);
            }
        }
        try (InputStream in = SqlEnrichmentRunner.class.getClassLoader().getResourceAsStream(path)) {
            if (in != null) {
                return new String(in.readAllBytes(), StandardCharsets.UTF_8);
            }
        }
        throw new IllegalArgumentException("Resource or file not found: " + path
            + " (working dir: " + System.getProperty("user.dir") + ")");
    }

    public static File resolveFile(String path) {
        File f = new File(path);
        if (f.exists()) return f;
        f = new File(System.getProperty("user.dir"), path);
        if (f.exists()) return f;
        f = new File("..", path);
        if (f.exists()) return f;
        File userDir = new File(System.getProperty("user.dir"));
        if (userDir.getParentFile() != null) {
            f = new File(userDir.getParentFile(), path);
            if (f.exists()) return f;
        }
        return null;
    }

    private static String resolveSqlPaths(String stmt) {
        // Resolve relative file paths in WITH ('path' = '...') to absolute paths
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("'path'\\s*=\\s*'([^']+)'");
        java.util.regex.Matcher matcher = pattern.matcher(stmt);
        if (matcher.find()) {
            String pathVal = matcher.group(1);
            if (!pathVal.startsWith("s3:") && !pathVal.startsWith("hdfs:") && !pathVal.startsWith("http")) {
                File resolved = resolveFile(pathVal);
                if (resolved != null && resolved.exists()) {
                    return matcher.replaceFirst("'path' = '" + resolved.getAbsolutePath() + "'");
                }
            }
        }
        return stmt;
    }
}
