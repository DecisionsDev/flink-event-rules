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
package com.ibm.di.event.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.di.event.rule.broadcast.ArchiveControlCmd;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Publishes {@link ArchiveControlCmd} messages to the Kafka archive-control topic
 * for any agent pipeline.
 *
 * <p>This is the HTTP API's write path: every accepted deployment request is converted
 * into an {@link ArchiveControlCmd} and published here.  The Flink broadcast-state
 * mechanism then delivers the command to all running subtasks.
 *
 * <p>The send is <em>synchronous</em> (blocks on {@code Future.get()}) so the HTTP
 * handler can return {@code 202} only when Kafka has acknowledged the record, or
 * {@code 500} when publish fails.
 *
 * <h3>Topic resolution</h3>
 * <p>The archive-control topic for a pipeline is resolved by convention from an
 * environment variable:
 * <pre>
 *   ARCHIVE_CONTROL_TOPIC_&lt;PIPELINE_UPPER&gt;
 * </pre>
 * where {@code PIPELINE_UPPER} is the pipeline name upper-cased with hyphens replaced
 * by underscores.  For example, pipeline {@code "my-agent"} reads env var
 * {@code ARCHIVE_CONTROL_TOPIC_MY_AGENT}.
 *
 * <p>If the env var is absent the topic name defaults to
 * {@code &lt;pipeline&gt;-archive-control} (e.g. {@code my-agent-archive-control}).
 *
 * <p>Thread safety: {@link KafkaProducer} is thread-safe; this class may be shared
 * across concurrent HTTP handler invocations.
 */
public class KafkaCommandPublisher implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaCommandPublisher.class);

    private final KafkaProducer<String, String> producer;
    private final ObjectMapper objectMapper;

    // ── Construction ──────────────────────────────────────────────────────────

    /**
     * Creates a publisher that connects to {@code bootstrapServers}.
     *
     * @param bootstrapServers comma-separated list of Kafka broker addresses
     */
    public KafkaCommandPublisher(String bootstrapServers) {
        this(buildProducer(bootstrapServers), new ObjectMapper());
    }

    /** Package-private constructor for testing with an injected producer. */
    KafkaCommandPublisher(KafkaProducer<String, String> producer,
                          ObjectMapper objectMapper) {
        this.producer     = producer;
        this.objectMapper = objectMapper;
        LOG.info("[KafkaCommandPublisher] initialized");
    }

    // ── Publish ───────────────────────────────────────────────────────────────

    /**
     * Publishes {@code cmd} to the archive-control topic for {@code pipeline}.
     *
     * <p>The topic is resolved via {@link #topicFor(String)}.
     *
     * @param cmd      the command to publish (must be fully populated)
     * @param pipeline the agent pipeline id (e.g. the agent folder name)
     * @throws IllegalArgumentException if {@code pipeline} is null or blank
     * @throws Exception if serialisation or Kafka send fails
     */
    public void publish(ArchiveControlCmd cmd, String pipeline) throws Exception {
        String topic = topicFor(pipeline);
        String json  = objectMapper.writeValueAsString(cmd);
        String key   = cmd.getVersion(); // partition by version for ordering

        LOG.info("[KafkaCommandPublisher] publishing cmd={} topic={}", cmd, topic);
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, json);
        producer.send(record).get(); // synchronous — throws on broker error
        LOG.info("[KafkaCommandPublisher] ✓ published version={} to topic={}", cmd.getVersion(), topic);
    }

    /**
     * Returns the Kafka archive-control topic for {@code pipeline}.
     *
     * <p>Resolution order:
     * <ol>
     *   <li>Env var {@code ARCHIVE_CONTROL_TOPIC_<PIPELINE_UPPER>}
     *       (hyphens in the pipeline name become underscores in the env-var name).</li>
     *   <li>Default: {@code <pipeline>-archive-control}.</li>
     * </ol>
     *
     * @param pipeline the agent pipeline id (must not be null or blank)
     * @throws IllegalArgumentException if {@code pipeline} is null or blank
     */
    public String topicFor(String pipeline) {
        if (pipeline == null || pipeline.isBlank()) {
            throw new IllegalArgumentException("pipeline must not be null or blank");
        }
        String envKey = "ARCHIVE_CONTROL_TOPIC_" + pipeline.toUpperCase().replace('-', '_');
        return resolveEnv(envKey, pipeline.toLowerCase() + "-archive-control");
    }

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    @Override
    public void close() {
        if (producer == null) return;
        try {
            producer.flush();
            producer.close();
            LOG.info("[KafkaCommandPublisher] closed");
        } catch (Exception e) {
            LOG.warn("[KafkaCommandPublisher] error during close: {}", e.getMessage());
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static KafkaProducer<String, String> buildProducer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30_000);
        return new KafkaProducer<>(props);
    }

    private static String resolveEnv(String key, String defaultValue) {
        String val = System.getenv(key);
        return (val != null && !val.isBlank()) ? val : defaultValue;
    }
}

