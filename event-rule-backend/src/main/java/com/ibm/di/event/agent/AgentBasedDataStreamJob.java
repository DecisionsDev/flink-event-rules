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
import com.ibm.di.event.rule.broadcast.ArchiveControlCmd;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serial;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;

/**
 * Flink DataStream job that builds a processing DAG from an {@link AgentDescriptor} JSON file.
 *
 * <p>The job reads descriptors from {@code AGENT_DESCRIPTOR_PATH} (a JSON array or a wrapper
 * object with an {@code "agents"} key).  For each agent it builds one Kafka source per
 * subscription, a {@code keyBy(entityId)} partition, the {@link StatefulAgentProcessor}, and
 * one Kafka sink per declared output.  A second pass wires inter-agent chains via
 * {@link AgentChainBuilder}.
 *
 * <p>Agent chaining is controlled at two levels:
 * <ol>
 *   <li><b>Global</b> — the {@code CHAINING_ENABLED} environment variable acts as a kill switch.
 *       Set {@code CHAINING_ENABLED=false} to disable chaining for every agent regardless of
 *       what the individual descriptors declare.  Set {@code CHAINING_ENABLED=true} (or leave
 *       the variable unset) to respect each agent's own {@code "chainingEnabled"} field.</li>
 *   <li><b>Per-agent</b> — the {@code "chainingEnabled"} field in each agent descriptor
 *       (default: {@code true}).  Set {@code "chainingEnabled": false} on an agent to make it
 *       read exclusively from Kafka and never feed its output to another agent in-process.
 *       This per-agent flag is only consulted when the global variable allows chaining.</li>
 * </ol>
 *
 * <p>For the full architecture, descriptor schema, and configuration reference see
 * {@code README.md} §4 (Architecture Overview) and {@code docs/AGENT_PATTERN_DESIGN.md}.
 *
 * <p>Key environment variables: {@code KAFKA_BOOTSTRAP_SERVERS}, {@code AGENT_DESCRIPTOR_PATH},
 * {@code FLINK_PARALLELISM}, {@code CHECKPOINT_INTERVAL_MS}, {@code WATERMARK_OUT_OF_ORDER_MS},
 * {@code CHAINING_ENABLED}.
 */
public class AgentBasedDataStreamJob {

    private static final Logger LOG = LoggerFactory.getLogger(AgentBasedDataStreamJob.class);

    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String DEFAULT_DESCRIPTOR_PATH   = "samples/agent/agents.json";
    private static final String DEFAULT_JOB_NAME          = "Agent-Based Event Processor";

    // ── Entry point ───────────────────────────────────────────────────────────

    public static void main(String[] args) throws Exception {
        LOG.info("{}", "=".repeat(80));
        LOG.info("Agent-Based Event Processing Job — Apache Flink");
        LOG.info("{}", "=".repeat(80));

        // ── Configuration ─────────────────────────────────────────────────────
        String bootstrapServers = envOr("KAFKA_BOOTSTRAP_SERVERS",
            args.length > 0 ? args[0] : DEFAULT_BOOTSTRAP_SERVERS);
        String descriptorPath   = envOr("AGENT_DESCRIPTOR_PATH",
            args.length > 1 ? args[1] : DEFAULT_DESCRIPTOR_PATH);
        // args[2] overrides FLINK_JOB_NAME so the BFF can assign a unique name per
        // wizard-created agent via programArgsList in the Flink REST submit call.
        // flinkConfiguration['pipeline.name'] only sets the graph name, not the
        // job name returned by GET /jobs/overview, so programArgsList is the only
        // reliable injection mechanism from the REST API.
        String jobName          = envOr("FLINK_JOB_NAME",
            args.length > 2 ? args[2] : DEFAULT_JOB_NAME);
        long   checkpointMs     = longEnvOr("CHECKPOINT_INTERVAL_MS", 60_000L);
        long   outOfOrderMs     = longEnvOr("WATERMARK_OUT_OF_ORDER_MS", 5_000L);
        // CHAINING_ENABLED=false disables in-process agent chaining for every agent.
        // When unset or set to "true", per-agent chainingEnabled fields are respected.
        // Parsed strictly as a boolean: only the literal "false" disables chaining.
        boolean globalChainingEnabled = boolEnvOr("CHAINING_ENABLED", true);

        LOG.info("  Bootstrap servers  : {}", bootstrapServers);
        LOG.info("  Descriptor path    : {}", descriptorPath);
        LOG.info("  Checkpoint interval: {} ms", checkpointMs);
        LOG.info("  Watermark OOO bound: {} ms", outOfOrderMs);
        LOG.info("  Chaining enabled   : {}", globalChainingEnabled);

        // ── Load agent descriptors ────────────────────────────────────────────
        // Resolution order: descriptor top-level "parallelism" → FLINK_PARALLELISM env var → 1
        AgentFileDescriptor fileDescriptor = loadFileDescriptor(descriptorPath);
        List<AgentDescriptor> agents = fileDescriptor.agents();
        if (agents.isEmpty()) {
            throw new IllegalStateException(
                "No agent descriptors found in: " + descriptorPath
                    + " — check that the file exists and contains a non-empty \"agents\" array.");
        }
        validateUniqueAgentIds(agents);
        validateUniqueOperatorUids(agents);

        // Job-level parallelism: descriptor field takes priority over env var
        int jobParallelism = fileDescriptor.parallelism();
        if (jobParallelism <= 0) jobParallelism = intEnvOr("FLINK_PARALLELISM", 0);
        if (jobParallelism <= 0) jobParallelism = 1;

        LOG.info("  Job parallelism    : {}", jobParallelism);

        // Apply the global chaining kill switch: if CHAINING_ENABLED=false, override every
        // agent's per-agent field so the chain builder sees no eligible agents.
        if (!globalChainingEnabled) {
            LOG.warn("CHAINING_ENABLED=false — disabling in-process chaining for all agents");
            for (AgentDescriptor a : agents) {
                a.setChainingEnabled(false);
            }
        }

        LOG.info("Loaded {} agent descriptor(s):", agents.size());
        for (AgentDescriptor a : agents) {
            LOG.info("  • {} entityType={} parallelism={} chainingEnabled={}",
                a.getAgentId(), a.getEntityType(),
                a.getParallelism() > 0 ? a.getParallelism() : "job-default",
                a.isChainingEnabled());
        }

        // ── Flink environment ─────────────────────────────────────────────────
        final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(jobParallelism);
        env.enableCheckpointing(checkpointMs);
        env.getCheckpointConfig().setCheckpointTimeout(180_000);

        // ── Wire each agent into its own DAG sub-graph ────────────────────────
        // When CHAINING_ENABLED=false the chain builder is bypassed entirely — no
        // AgentChainBuilder is created and resolveTopology is never called.
        // When CHAINING_ENABLED=true (or unset) the full topology is resolved and
        // each agent's output is registered so downstream agents can receive it.
        AgentChainBuilder.DeferredChainedInputs chainedInputs =
            buildChainedInputs(agents, globalChainingEnabled);

        for (AgentDescriptor agent : agents) {
            String agentId = agent.getAgentId();
            DataStream<EventEnvelope> chainedIn = chainedInputs.get(agentId);
            java.util.Set<String> chainedTypes = chainedInputs.chainedEventTypes(agentId);
            if (chainedIn != null) {
                LOG.info("[chain] Wiring agent={} with chained inbound stream (types={})",
                    agentId, chainedTypes);
            }
            DataStream<EventEnvelope> processed =
                wireAgent(env, agent, bootstrapServers, outOfOrderMs, chainedIn, chainedTypes);
            // Register this agent's output so downstream agents wired later can receive it.
            // When chaining is globally disabled chainedInputs is the empty no-op sentinel
            // and registerOutput is a no-op too, so there is no overhead.
            if (processed != null) {
                chainedInputs.registerOutput(agentId, processed);
            }
        }

        // ── Execute ───────────────────────────────────────────────────────────
        LOG.info("Starting Flink job: {}", jobName);
        env.execute(jobName);
    }

    // ── Agent DAG construction ────────────────────────────────────────────────

    /**
     * Wires one agent's full processing sub-graph and returns the processed stream so that
     * downstream agents in a chain can subscribe to it via {@link AgentChainBuilder}.
     *
     * <p>When {@code chainedInbound} is non-null it is union-ed with the agent's Kafka sources
     * before the {@code keyBy} step, enabling agents to consume output events from other agents
     * without an intermediate Kafka hop.
     *
     * <p>When {@code chainedEventTypes} is non-empty, Kafka sources for subscriptions whose
     * event type is already covered by an incoming chain edge are suppressed.  This prevents
     * duplicate delivery: without suppression each event would be processed twice — once
     * immediately via the in-memory chain and again after the Kafka round-trip (F-6).
     *
     * <p>When {@link RulesetConfig#hasBroadcastReload()} is true the agent's event stream is
     * connected to an {@link ArchiveControlCmd} broadcast stream sourced from
     * {@link RulesetConfig#getArchiveControlTopic()} before the {@code keyBy} step, enabling
     * hot-swap of the decision archive via {@link StatefulAgentProcessor#processBroadcastElement}.
     *
     * @param chainedInbound    optional upstream stream to union with Kafka sources; {@code null}
     *                          for standalone agents
     * @param chainedEventTypes set of event types already delivered via chain; Kafka sources for
     *                          those types are suppressed to prevent duplicate processing
     * @return the {@link DataStream} of processed {@link EventEnvelope}s emitted by this agent's
     *         {@link StatefulAgentProcessor}, or {@code null} when the agent was skipped
     */
    static DataStream<EventEnvelope> wireAgent(StreamExecutionEnvironment env,
                                               AgentDescriptor agent,
                                               String bootstrapServers,
                                               long outOfOrderMs,
                                               DataStream<EventEnvelope> chainedInbound,
                                               java.util.Set<String> chainedEventTypes) {

        validate(agent);

        final String agentId     = agent.getAgentId();
        final String consumerGrp = "agent-" + agentId;
        final int    agentParallelism = agent.getParallelism(); // 0 = job default

        LOG.info("[{}] Wiring agent subscriptions={}", agentId,
            agent.getSubscriptions() != null ? agent.getSubscriptions().size() : 0);

        // ── 1. Build one Kafka source per subscription, union into one stream ──
        // Subscriptions whose event type is covered by an incoming chain edge are
        // skipped — those events arrive directly via the chain stream, so wiring the
        // Kafka source for the same topic would cause duplicate processing (F-6).
        List<DataStream<EventEnvelope>> subscriptionStreams = new ArrayList<>();

        if (agent.getSubscriptions() != null) {
            for (EventSubscription sub : agent.getSubscriptions()) {
                if (sub.getTopic() == null || sub.getTopic().isBlank()) {
                    LOG.warn("[{}] Subscription {} has no topic — skipping",
                        agentId, sub.getEventType());
                    continue;
                }
                // Suppress Kafka source when this event type is already delivered via chain
                if (chainedEventTypes != null && chainedEventTypes.contains(sub.getEventType())) {
                    LOG.info("[{}] Suppressing Kafka source topic={} eventType={} "
                        + "— covered by chain (duplicate-delivery prevention)",
                        agentId, sub.getTopic(), sub.getEventType());
                    continue;
                }
                DataStream<EventEnvelope> topicStream =
                    buildTopicStream(env, sub.getTopic(), sub.getEventType(),
                        consumerGrp, bootstrapServers, outOfOrderMs, agent);
                subscriptionStreams.add(topicStream);
                LOG.info("[{}] Wired Kafka source: topic={} eventType={}",
                    agentId, sub.getTopic(), sub.getEventType());
            }
        }

        if (subscriptionStreams.isEmpty() && chainedInbound == null) {
            LOG.warn("[{}] No subscription sources created — agent skipped", agentId);
            return null;
        }

        // ── 2. Union all subscription streams (+ optional chained inbound) ────
        DataStream<EventEnvelope> unioned;
        if (subscriptionStreams.isEmpty()) {
            // Chained-only: no Kafka sources, only upstream agent output
            unioned = chainedInbound;
            LOG.info("[{}] No Kafka sources — using only chained inbound stream", agentId);
        } else {
            unioned = subscriptionStreams.getFirst();
            for (int i = 1; i < subscriptionStreams.size(); i++) {
                unioned = unioned.union(subscriptionStreams.get(i));
            }
            if (chainedInbound != null) {
                unioned = unioned.union(chainedInbound);
                LOG.info("[{}] Unioned chained inbound stream with {} Kafka source(s)",
                    agentId, subscriptionStreams.size());
            }
        }

        // ── 3. Key by entity ID ───────────────────────────────────────────────
        // ── 4. StatefulAgentProcessor — with or without broadcast reload ──────
        StatefulAgentProcessor processor = new StatefulAgentProcessor(agent);

        BroadcastStream<ArchiveControlCmd> controlBroadcast;
        if (agent.getRuleset().hasBroadcastReload()) {
            String controlTopic = agent.getRuleset().getArchiveControlTopic();
            controlBroadcast = buildArchiveControlStream(env, controlTopic,
                    consumerGrp + "-archive-ctrl", bootstrapServers)
                .broadcast(StatefulAgentProcessor.ARCHIVE_STATE_DESC);
            LOG.info("[{}] Wired broadcast-reload archive-control source: topic={}",
                agentId, controlTopic);
        } else {
            // No archive-control topic — use empty broadcast so the operator is uniform
            controlBroadcast = env
                .fromData(java.util.List.<ArchiveControlCmd>of(),
                        TypeInformation.of(ArchiveControlCmd.class))
                .broadcast(StatefulAgentProcessor.ARCHIVE_STATE_DESC);
        }

        SingleOutputStreamOperator<EventEnvelope> processed =
            (SingleOutputStreamOperator<EventEnvelope>)
                connectAndProcess(unioned, controlBroadcast, processor, agentId, agentParallelism);

        // ── 5. Output sinks ───────────────────────────────────────────────────
        if (agent.getOutputs() == null || agent.getOutputs().isEmpty()) {
            LOG.warn("[{}] No output configs declared — processed events discarded", agentId);
            processed.print("agent-output-" + agentId).name("Print[" + agentId + "]");
            return processed;
        }

        // Track whether agent-error is already covered by a declared output config
        boolean agentErrorSinkDeclared = agent.getOutputs().stream()
            .anyMatch(o -> "agent-error".equals(o.getEventType())
                && o.getTopic() != null && !o.getTopic().isBlank());

        for (OutputConfig output : agent.getOutputs()) {
            if (output.getTopic() == null || output.getTopic().isBlank()) continue;

            final String outEventType = output.getEventType();
            final String outTopic     = output.getTopic();

            // Filter to this output's event type, then serialize and sink
            DataStream<EventEnvelope> filtered = processed.filter(env2 -> {
                if (outEventType == null || outEventType.isBlank()) return true; // wildcard
                return outEventType.equals(env2.getEventType());
            }).name("Filter[" + agentId + "/" + outEventType + "]");

            // Unique transactional ID prefix per sink instance — required by flink-connector-kafka
            // 4.0.0-2.0 BackchannelFactory which rejects duplicate (including null) prefixes.
            String txPrefix = agentId + "-" + outEventType;
            KafkaSink<EventEnvelope> sink = buildKafkaSink(bootstrapServers, outTopic, txPrefix);

            filtered.sinkTo(sink)
                .uid("agent-sink-" + agentId + "-" + outEventType)
                .name("Sink[" + agentId + "/" + outEventType + " → " + outTopic + "]");

            LOG.info("[{}] Wired Kafka sink: eventType={} → topic={}", agentId, outEventType, outTopic);
        }

        // ── 5b. Agent-error DLQ sink ──────────────────────────────────────────
        // Always wire a dedicated DLQ sink for agent-error envelopes so errors are
        // never silently dropped, even when no explicit agent-error output is declared.
        // Topic resolution order (most- to least-specific):
        //   1. AGENT_ERROR_TOPIC_<AGENT_ID_UPPER> env var  (per-agent override)
        //   2. AGENT_ERROR_TOPIC env var                   (global override)
        //   3. <agentId>-errors                            (per-agent default)
        if (!agentErrorSinkDeclared) {
            String agentEnvKey = "AGENT_ERROR_TOPIC_"
                + agentId.toUpperCase().replace('-', '_').replace('.', '_');
            String dlqTopic = envOr(agentEnvKey,
                               envOr("AGENT_ERROR_TOPIC",
                                agentId + "-errors"));
            DataStream<EventEnvelope> errorStream = processed
                .filter(e -> "agent-error".equals(e.getEventType()))
                .name("Filter[" + agentId + "/agent-error-dlq]");
            errorStream.sinkTo(buildKafkaSink(bootstrapServers, dlqTopic, agentId + "-agent-error-dlq"))
                .uid("agent-error-dlq-" + agentId)
                .name("DLQ[" + agentId + "/agent-error → " + dlqTopic + "]");
            LOG.info("[{}] Wired agent-error DLQ sink → topic={}", agentId, dlqTopic);
        }

        // ── 5c. Entity-state changelog sink ───────────────────────────────────
        if (agent.getStateConfig() != null && agent.getStateConfig().hasChangeLogTopic()) {
            String changeLogTopic = agent.getStateConfig().getChangeLogTopic();
            DataStream<EventEnvelope> changeLogStream = processed
                .getSideOutput(processor.getChangelogTag());
            changeLogStream
                .sinkTo(buildKafkaSink(bootstrapServers, changeLogTopic,
                    agentId + "-entity-changelog"))
                .uid("agent-changelog-sink-" + agentId)
                .name("ChangeLog[" + agentId + " → " + changeLogTopic + "]");
            LOG.info("[{}] Wired entity-state changelog sink → topic={}", agentId, changeLogTopic);
        }

        return processed;
    }

    /**
     * Convenience overload for callers that do not need chained-event-type suppression
     * (e.g. tests, non-chaining code paths).  Delegates to the full 6-arg form with
     * an empty {@code chainedEventTypes} set.
     */
    static DataStream<EventEnvelope> wireAgent(StreamExecutionEnvironment env,
                                               AgentDescriptor agent,
                                               String bootstrapServers,
                                               long outOfOrderMs,
                                               DataStream<EventEnvelope> chainedInbound) {
        return wireAgent(env, agent, bootstrapServers, outOfOrderMs, chainedInbound, null);
    }

    // ── Kafka source builders ─────────────────────────────────────────────────

    /**
     * Keys the event stream by entity ID, connects the broadcast-control stream,
     * applies the {@link StatefulAgentProcessor}, and sets operator UID/name and parallelism.
     */
    private static DataStream<EventEnvelope> connectAndProcess(
            DataStream<EventEnvelope> events,
            BroadcastStream<ArchiveControlCmd> controlBroadcast,
            StatefulAgentProcessor processor,
            String agentId,
            int agentParallelism) {
        var op = events
            .keyBy(EventEnvelope::getEntityId)
            .connect(controlBroadcast)
            .process(processor)
            .uid("agent-processor-" + agentId)
            .name("Agent[" + agentId + "]");
        if (agentParallelism > 0) op = op.setParallelism(agentParallelism);
        return op;
    }

    private static DataStream<ArchiveControlCmd> buildArchiveControlStream(
            StreamExecutionEnvironment env,
            String topic,
            String consumerGroup,
            String bootstrapServers) {

        KafkaSource<ArchiveControlCmd> source = KafkaSource.<ArchiveControlCmd>builder()
            .setBootstrapServers(bootstrapServers)
            .setTopics(topic)
            .setGroupId(consumerGroup)
            .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
            .setValueOnlyDeserializer(new ArchiveControlCmdDeserializer())
            .build();

        return env.fromSource(source,
                WatermarkStrategy.noWatermarks(),
                "ArchiveControl[" + topic + "]")
            .setParallelism(1); // broadcast source must be parallelism 1
    }

    /**
     * Builds an event-time Kafka source stream for {@code topic}, wrapping each raw
     * {@link JsonNode} into an {@link EventEnvelope} with entity ID extracted.
     */
    private static DataStream<EventEnvelope> buildTopicStream(
            StreamExecutionEnvironment env,
            String topic,
            String eventType,
            String consumerGroup,
            String bootstrapServers,
            long outOfOrderMs,
            AgentDescriptor agent) {

        final String entityKeyExtractor = agent.getEntityKeyExtractor();

        KafkaSource<EventEnvelope> source = KafkaSource.<EventEnvelope>builder()
            .setBootstrapServers(bootstrapServers)
            .setTopics(topic)
            .setGroupId(consumerGroup)
            .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
            .setValueOnlyDeserializer(new EnvelopeDeserializer(topic, eventType, entityKeyExtractor))
            .build();

        // Event-time watermark: bounded out-of-orderness using the envelope timestamp.
        // Falls back to ingestion time when the event has no timestamp field.
        WatermarkStrategy<EventEnvelope> watermarks = WatermarkStrategy
            .<EventEnvelope>forBoundedOutOfOrderness(Duration.ofMillis(outOfOrderMs))
            .withTimestampAssigner((envelope, recordTs) ->
                envelope.getTimestamp() > 0 ? envelope.getTimestamp() : recordTs)
            .withIdleness(Duration.ofSeconds(30));

        return env.fromSource(source, watermarks,
            "Kafka[" + agent.getAgentId() + "/" + topic + "]");
    }

    // ── Kafka sink builder ────────────────────────────────────────────────────

    /**
     * Builds a Kafka sink for {@code topic} with a unique {@code transactionalIdPrefix}.
     *
     * <p>The flink-connector-kafka 4.0.0-2.0 {@code BackchannelFactory} uses a
     * static JVM-level registry keyed by {@code transactionalIdPrefix}.  When multiple
     * {@code KafkaSink} operators share the same operator chain (which happens when all
     * Filter→Sink chains are fused into the same slot as the {@link StatefulAgentProcessor}),
     * each sink must have a unique, non-null prefix or the factory throws
     * {@code IllegalStateException: Writable backchannel already exists}.
     *
     * <p>{@link DeliveryGuarantee#AT_LEAST_ONCE} is used with an explicit prefix so Flink
     * can safely track each producer's commit state independently.
     * @param transactionalIdPrefix unique per-sink identifier, e.g. {@code "agentId-eventType"}
     */
    private static KafkaSink<EventEnvelope> buildKafkaSink(String bootstrapServers,
                                                            String topic,
                                                            String transactionalIdPrefix) {
        return KafkaSink.<EventEnvelope>builder()
            .setBootstrapServers(bootstrapServers)
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic(topic)
                .setKeySerializationSchema(new EnvelopeKeySerializer())
                .setValueSerializationSchema(new EnvelopeSerializer())
                .build())
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .setTransactionalIdPrefix(transactionalIdPrefix)
            .build();
    }

    // ── Descriptor loading ────────────────────────────────────────────────────

    /**
     * Container for the agents descriptor file contents.
     *
     * @param parallelism job-level parallelism declared in the file ({@code 0} = not set)
     * @param agents      list of agent descriptors
     */
    public record AgentFileDescriptor(int parallelism, List<AgentDescriptor> agents) {}

    /**
     * Loads the agent descriptor file and returns an {@link AgentFileDescriptor}.
     * Accepts:
     * <ul>
     *   <li>A JSON array: {@code [ { … }, { … } ]}</li>
     *   <li>A wrapper object with an optional top-level {@code "parallelism"} field and an
     *       {@code "agents"} array: {@code { "parallelism": 4, "agents": [ … ] }}</li>
     * </ul>
     */
    static AgentFileDescriptor loadFileDescriptor(String descriptorPath) throws IOException {
        String json = readFile(descriptorPath);
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(json);

        int jobParallelism = 0;
        JsonNode arrayNode;
        if (root.isArray()) {
            arrayNode = root;
        } else if (root.has("agents") && root.get("agents").isArray()) {
            arrayNode = root.get("agents");
            // Read optional top-level parallelism field
            if (root.has("parallelism") && root.get("parallelism").isInt()) {
                jobParallelism = root.get("parallelism").asInt(0);
            }
        } else {
            throw new IllegalArgumentException(
                "Agent descriptor file must be a JSON array or an object with an 'agents' array: "
                    + descriptorPath);
        }

        List<AgentDescriptor> agents = new ArrayList<>();
        for (JsonNode node : arrayNode) {
            AgentDescriptor agent = mapper.treeToValue(node, AgentDescriptor.class);
            agents.add(agent);
        }
        return new AgentFileDescriptor(jobParallelism, agents);
    }

    /**
     * Convenience method that returns only the agent list.
     * Kept for callers that do not need the job-level parallelism.
     */
    static List<AgentDescriptor> loadAgentDescriptors(String descriptorPath) throws IOException {
        return loadFileDescriptor(descriptorPath).agents();
    }

    private static String readFile(String path) throws IOException {
        // Try classpath first
        try (InputStream in = AgentBasedDataStreamJob.class.getClassLoader()
                .getResourceAsStream(path)) {
            if (in != null) {
                return new String(in.readAllBytes(), StandardCharsets.UTF_8);
            }
        }
        // Then filesystem
        File f = new File(path);
        if (!f.exists()) f = new File(System.getProperty("user.dir"), path);
        if (!f.exists()) {
            throw new IllegalArgumentException("Agent descriptor not found: " + path
                + " (working dir: " + System.getProperty("user.dir") + ")");
        }
        try (InputStream in = new FileInputStream(f)) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    // ── Chain topology builder ────────────────────────────────────────────────

    /**
     * Resolves the agent chain topology, or returns an empty no-op sentinel when
     * chaining is globally disabled.
     *
     * <p>When {@code globalChainingEnabled} is {@code false} this method returns immediately
     * with an empty {@link AgentChainBuilder.DeferredChainedInputs} — the
     * {@link AgentChainBuilder} is never instantiated and {@code resolveTopology} is never
     * called.  This guarantees <em>zero agent chaining execution</em> when the
     * {@code CHAINING_ENABLED=false} kill switch is active.
     *
     * <p>When {@code globalChainingEnabled} is {@code true} the full topology resolution
     * runs as normal: a new {@link AgentChainBuilder} is created, cycle detection and
     * wiring-order validation execute, and the resulting
     * {@link AgentChainBuilder.DeferredChainedInputs} is returned.
     *
     * @param agents               list of agent descriptors (already kill-switch-applied)
     * @param globalChainingEnabled {@code false} to skip chaining entirely; {@code true} to resolve
     * @return a {@link AgentChainBuilder.DeferredChainedInputs} — empty when disabled, full when enabled
     */
    static AgentChainBuilder.DeferredChainedInputs buildChainedInputs(
            List<AgentDescriptor> agents, boolean globalChainingEnabled) {
        if (!globalChainingEnabled) {
            LOG.info("[AgentBasedDataStreamJob] CHAINING_ENABLED=false — "
                + "AgentChainBuilder bypassed entirely, no chain topology resolved");
            return new AgentChainBuilder().resolveTopology(java.util.List.of());
        }
        return new AgentChainBuilder().resolveTopology(agents);
    }

    // ── Validation ────────────────────────────────────────────────────────────

    /**
     * Validates every explicit UID generated by the agent topology before Flink graph generation.
     *
     * <p>This mirrors the UID scope enforced by Flink: all logical operators in one job must have
     * distinct UIDs. Output event types are part of sink identity, so duplicate or blank output
     * event types are rejected when they would generate the same sink UID.
     */
    static void validateUniqueOperatorUids(List<AgentDescriptor> agents) {
        if (agents == null || agents.isEmpty()) return;

        java.util.Set<String> seenUids = new java.util.HashSet<>();
        for (AgentDescriptor agent : agents) {
            if (agent == null) continue;
            String agentId = agent.getAgentId();
            if (agentId == null || agentId.isBlank()) continue;

            registerUid(seenUids, "agent-processor-" + agentId, agentId);

            boolean agentErrorSinkDeclared = false;
            if (agent.getOutputs() != null) {
                for (OutputConfig output : agent.getOutputs()) {
                    if (output == null || output.getTopic() == null || output.getTopic().isBlank()) continue;
                    String eventType = output.getEventType();
                    // Normalize null/blank event types to "" so that both map to the same UID
                    // (both act as wildcards — "route everything"), making the duplicate check work.
                    String normalizedType = (eventType == null || eventType.isBlank()) ? "" : eventType;
                    registerUid(seenUids, "agent-sink-" + agentId + "-" + normalizedType, agentId);
                    if ("agent-error".equals(eventType)) agentErrorSinkDeclared = true;
                }
            }

            if (!agentErrorSinkDeclared) {
                registerUid(seenUids, "agent-error-dlq-" + agentId, agentId);
            }

            if (agent.getStateConfig() != null && agent.getStateConfig().hasChangeLogTopic()) {
                registerUid(seenUids, "agent-changelog-sink-" + agentId, agentId);
            }
        }
    }

    private static void registerUid(java.util.Set<String> seenUids, String uid, String agentId) {
        if (!seenUids.add(uid)) {
            throw new IllegalArgumentException(
                "Duplicate Flink operator UID generated for agentId='" + agentId + "': '" + uid + "'. "
                + "Each logical operator in a Flink job must have a unique UID.");
        }
    }

    /**
     * Validates that all agent descriptors in a topology have unique non-blank {@code agentId}s.
     *
     * <p>Duplicate agent IDs within a single Flink job cause identical operator UIDs
     * (e.g. {@code "agent-processor-<agentId>"}), which will be rejected by Flink's
     * {@code StreamingJobGraphGenerator} at submission/deployment time.
     *
     * @param agents the list of agent descriptors to validate
     * @throws IllegalArgumentException if duplicate or blank agent IDs are detected
     */
    static void validateUniqueAgentIds(List<AgentDescriptor> agents) {
        if (agents == null || agents.isEmpty()) return;
        java.util.Set<String> seenIds = new java.util.HashSet<>();
        for (AgentDescriptor agent : agents) {
            if (agent == null) continue;
            String id = agent.getAgentId();
            if (id == null || id.isBlank()) {
                throw new IllegalArgumentException("AgentDescriptor.agentId must not be blank");
            }
            if (!seenIds.add(id)) {
                throw new IllegalArgumentException(
                    "Duplicate agentId detected in descriptor list: '" + id
                    + "'. Each agent in a Flink job must have a unique agentId to guarantee unique operator UIDs.");
            }
        }
    }

    /**
     * Validates an {@link AgentDescriptor} before wiring it into the Flink DAG.
     *
     * <p>Checks enforced:
     * <ol>
     *   <li>{@code agentId} must be non-blank.</li>
     *   <li>{@code ruleset} and {@code ruleset.path} must be non-blank.</li>
     *   <li>Every subscription must declare {@code operation} explicitly: {@code ""} means
     *       history-only (event stored but rule engine not invoked); absent ({@code null})
     *       is rejected to prevent silent misconfiguration.</li>
     *   <li>No two subscriptions may share the same non-blank operation name.</li>
     *   <li>No two configured output sinks may generate the same operator UID.</li>
     *   <li>A WARN is emitted when {@code entityKeyExtractor} is absent and the agent has
     *       subscriptions: all events will key to {@code "unknown"}, which concentrates
     *       processing on a single Flink partition (hot-key risk — ).</li>
     * </ol>
     */
    static void validate(AgentDescriptor agent) {
        if (agent.getAgentId() == null || agent.getAgentId().isBlank())
            throw new IllegalArgumentException("AgentDescriptor.agentId must not be blank");
        if (agent.getRuleset() == null)
            throw new IllegalArgumentException(
                "AgentDescriptor.ruleset must not be null for agentId=" + agent.getAgentId());
        if (agent.getRuleset().getPath() == null || agent.getRuleset().getPath().isBlank())
            throw new IllegalArgumentException(
                "AgentDescriptor.ruleset.path must not be blank for agentId=" + agent.getAgentId());

        // ── enforce entityKeyExtractor when subscriptions are present ────────
        // A missing extractor means every event keys to "unknown", routing all processing
        // to a single Flink keyed partition (hot-key).  Raise an error at descriptor-load
        // time so misconfiguration is caught before the Flink job starts rather than
        // silently degrading throughput under load.
        // To opt out explicitly for history-only agents with no keying need, set
        // "entityKeyExtractor": "" in the descriptor.
        if (agent.getEntityKeyExtractor() == null
                && agent.getSubscriptions() != null && !agent.getSubscriptions().isEmpty()) {
            throw new IllegalArgumentException(
                "agentId=" + agent.getAgentId()
                + ": entityKeyExtractor is required when an agent has subscriptions. "
                + "Set it to a JSONPath expression (e.g. \"$.accountId\") to partition "
                + "events by entity key. Use \"entityKeyExtractor\": \"\" to opt into "
                + "history-only mode where all events key to 'unknown'.");
        }
        if (agent.getEntityKeyExtractor() != null
                && agent.getEntityKeyExtractor().isBlank()
                && agent.getSubscriptions() != null && !agent.getSubscriptions().isEmpty()) {
            LOG.warn("[{}] entityKeyExtractor is blank — all events will key to 'unknown' "
                + "(history-only / single-partition mode). Intentional opt-in only.",
                agent.getAgentId());
        }

        if (agent.getOutputs() != null) {
            java.util.Set<String> seenOutputUids = new java.util.HashSet<>();
            for (OutputConfig output : agent.getOutputs()) {
                if (output == null || output.getTopic() == null || output.getTopic().isBlank()) continue;
                String rawType = output.getEventType();
                String normalizedType = (rawType == null || rawType.isBlank()) ? "" : rawType;
                String uid = "agent-sink-" + agent.getAgentId() + "-" + normalizedType;
                if (!seenOutputUids.add(uid)) {
                    throw new IllegalArgumentException(
                        "agentId=" + agent.getAgentId()
                        + ": duplicate output operator UID '" + uid
                        + "'. Output event types must uniquely identify configured sinks.");
                }
            }
        }

        if (agent.getSubscriptions() != null) {
            java.util.Set<String> seenOperations = new java.util.HashSet<>();
            for (EventSubscription sub : agent.getSubscriptions()) {
                // Rule: operation must be explicitly present (null = field absent in JSON)
                if (sub.getOperation() == null) {
                    throw new IllegalArgumentException(
                        "agentId=" + agent.getAgentId()
                        + ": subscription for eventType='" + sub.getEventType()
                        + "' is missing the required 'operation' field. "
                        + "Use \"operation\": \"\" to subscribe for context only (history-only mode), "
                        + "or provide the IBM Decision Intelligence operation name to invoke.");
                }
                // Rule: each non-blank operation name must be unique within this agent
                if (sub.hasOperation()) {
                    if (!seenOperations.add(sub.getOperation())) {
                        throw new IllegalArgumentException(
                            "agentId=" + agent.getAgentId()
                            + ": duplicate operation name '" + sub.getOperation()
                            + "' found in subscriptions. Each operation may appear at most once "
                            + "per agent because one operation processes exactly one event type.");
                    }
                }
            }
        }
    }

    // ── Environment helpers ───────────────────────────────────────────────────

    private static String envOr(String name, String def) {
        String v = System.getenv(name);
        return (v != null && !v.isBlank()) ? v : def;
    }

    private static int intEnvOr(String name, int def) {
        String v = System.getenv(name);
        if (v == null || v.isBlank()) return def;
        try { return Integer.parseInt(v.trim()); }
        catch (NumberFormatException e) { return def; }
    }

    private static long longEnvOr(String name, long def) {
        String v = System.getenv(name);
        if (v == null || v.isBlank()) return def;
        try { return Long.parseLong(v.trim()); }
        catch (NumberFormatException e) { return def; }
    }

    /**
     * Reads a boolean environment variable.  Only the literal string {@code "false"}
     * (case-insensitive) returns {@code false}; everything else (including an unset or blank
     * value) returns the provided default.  This avoids the common pitfall where a non-empty
     * string like {@code "0"} or {@code "no"} would be treated as {@code true} by a naïve
     * {@code Boolean.parseBoolean} call.
     */
    static boolean boolEnvOr(String name, boolean def) {
        String v = System.getenv(name);
        if (v == null || v.isBlank()) return def;
        return !"false".equalsIgnoreCase(v.trim());
    }

    // ── Serialization schemas ─────────────────────────────────────────────────

    /**
     * Deserializes a Kafka message into an {@link ArchiveControlCmd} using Jackson.
     * Returns a {@link ArchiveControlCmd.CommandType#NOOP} command on parse failure
     * so the control stream never interrupts event processing.
     */
    static final class ArchiveControlCmdDeserializer
            implements DeserializationSchema<ArchiveControlCmd> {

        @java.io.Serial
        private static final long serialVersionUID = 1L;

        private transient ObjectMapper mapper;

        @Override
        public ArchiveControlCmd deserialize(byte[] message) {
            if (mapper == null) mapper = new ObjectMapper();
            try {
                return mapper.readValue(message, ArchiveControlCmd.class);
            } catch (Exception e) {
                LOG.error("[ArchiveControlCmdDeserializer] Failed to parse control command: {}",
                    e.getMessage());
                ArchiveControlCmd noop = new ArchiveControlCmd();
                noop.setCommandType(ArchiveControlCmd.CommandType.NOOP);
                return noop;
            }
        }

        @Override public boolean isEndOfStream(ArchiveControlCmd next) { return false; }

        @Override
        public TypeInformation<ArchiveControlCmd> getProducedType() {
            return TypeInformation.of(ArchiveControlCmd.class);
        }
    }


    /**
     * Deserializes a raw Kafka message into an {@link EventEnvelope}, extracting the
     * entity ID using the configured JSONPath expression.
     */
    static final class EnvelopeDeserializer
            implements DeserializationSchema<EventEnvelope> {

        @Serial
        private static final long serialVersionUID = 1L;

        private final String topic;
        private final String eventType;
        private final String entityKeyExtractor;

        private transient ObjectMapper mapper;

        EnvelopeDeserializer(String topic, String eventType, String entityKeyExtractor) {
            this.topic              = topic;
            this.eventType          = eventType;
            this.entityKeyExtractor = entityKeyExtractor;
        }

        @Override
        public EventEnvelope deserialize(byte[] message) {
            if (mapper == null) mapper = new ObjectMapper();
            try {
                JsonNode raw = mapper.readTree(message);

                // ── Agent-envelope unwrapping ─────────────────────────────────
                // When an upstream agent emits an event via EnvelopeSerializer the
                // Kafka record has the shape:
                //   { "agentEventType": "...", "entityId": "...", "payload": { … } }
                // The downstream agent's entityKeyExtractor targets fields inside the
                // *inner* payload (e.g. "$.airport"), not the wrapper object.
                // Detect this shape and unwrap so that entity extraction and rule
                // execution always operate on the raw domain object, exactly as if
                // the event had been produced directly by an external source.
                JsonNode payload;
                if (raw.has("agentEventType") && raw.has("payload")) {
                    payload = raw.get("payload");
                } else {
                    payload = raw;
                }

                // Extract timestamp from payload if present
                long ts = 0L;
                if (payload.has("timestamp")) {
                    JsonNode tsNode = payload.get("timestamp");
                    if (tsNode.isNumber()) ts = tsNode.asLong();
                    if (tsNode.isTextual()) {
                        Instant t = null;
                        try {
                            t = Instant.parse(tsNode.asText());
                        } catch(DateTimeParseException e) {}
                        if (t != null) {
                            ts = t.toEpochMilli();
                        }
                    }
                }
                if (ts == 0L) ts = System.currentTimeMillis();

                // Allow the payload to override the subscription-level event type via a
                // reserved "__eventType" field.  This lets control sentinels (e.g.
                // "__ENTITY_RESET__") travel through any existing subscribed topic and still
                // be identified correctly by processElement without a separate control topic.
                String effectiveEventType = eventType;
                if (payload.has("__eventType")) {
                    String override = payload.get("__eventType").asText(null);
                    if (override != null && !override.isBlank()) {
                        effectiveEventType = override;
                    }
                }

                EventEnvelope env = EventEnvelope.from(effectiveEventType, topic, ts, payload);

                // Entity ID extraction via simplified JSONPath ($.field or $.a.b)
                env.setEntityId(extractEntityId(payload, entityKeyExtractor));
                return env;

            } catch (Exception e) {
                LOG.error("[EnvelopeDeserializer] Failed to deserialize message on topic {}: {}",
                    topic, e.getMessage());
                // Return an error envelope rather than null so the stream is not interrupted
                EventEnvelope err = new EventEnvelope();
                err.setEventType("deserialization-error");
                err.setSourceTopic(topic);
                err.setTimestamp(System.currentTimeMillis());
                err.setEntityId("__error__");
                return err;
            }
        }

        @Override
        public boolean isEndOfStream(EventEnvelope next) { return false; }

        @Override
        public TypeInformation<EventEnvelope> getProducedType() {
            return TypeInformation.of(EventEnvelope.class);
        }

        /**
         * Extracts the entity ID from a payload using a full JSONPath expression (Jayway JsonPath).
         * Supports arbitrary nesting depth, e.g. {@code "$.a.b.c.d"}.
         * Returns {@code "unknown"} when the field is absent, the extractor is null/blank, or
         * the path does not match.
         *
         * <p>Delegates to {@link JsonPathUtils#extractEntityId} — the single canonical
         * implementation shared with {@link AgentChainBuilder.DeferredChainedInputs} to ensure
         * that entity IDs produced at Kafka deserialization time and at chain normalisation time
         * are always identical for the same payload and extractor expression (Weakness 5 fix).
         */
        static String extractEntityId(JsonNode payload, String extractor) {
            return JsonPathUtils.extractEntityId(payload, extractor);
        }
    }

    static final class EnvelopeKeySerializer implements SerializationSchema<EventEnvelope> {

        @Serial
        private static final long serialVersionUID = 1L;

        @Override
        public byte[] serialize(EventEnvelope element) {
            String entityId = element.getEntityId();
            if (entityId == null || entityId.isBlank()) return new byte[0];
            return entityId.getBytes(StandardCharsets.UTF_8);
        }
    }

    /**
     * Serializes an {@link EventEnvelope} to JSON bytes for the Kafka sink.
     */
    static final class EnvelopeSerializer implements SerializationSchema<EventEnvelope> {

        @Serial
        private static final long serialVersionUID = 1L;

        private transient ObjectMapper mapper;

        @Override
        public byte[] serialize(EventEnvelope element) {
            if (mapper == null) mapper = new ObjectMapper();
            try {
                // Build a compact output envelope: metadata + payload
                com.fasterxml.jackson.databind.node.ObjectNode out = mapper.createObjectNode();
                out.put("agentEventType", element.getEventType());
                out.put("entityId",       element.getEntityId() != null ? element.getEntityId() : "");
                out.put("sourceTopic",    element.getSourceTopic() != null ? element.getSourceTopic() : "");
                out.put("timestamp",      element.getTimestamp());
                if (element.getPayload() != null) {
                    out.set("payload", element.getPayload());
                }
                if (element.getMetadata() != null && !element.getMetadata().isEmpty()) {
                    out.set("metadata", mapper.valueToTree(element.getMetadata()));
                }
                return mapper.writeValueAsBytes(out);
            } catch (Exception e) {
                throw new RuntimeException("Failed to serialize EventEnvelope", e);
            }
        }
    }
}

