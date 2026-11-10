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
package com.ibm.di.event.e2e;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.*;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * End-to-end integration test for the Retail Promotion Agent.
 *
 * <h2>What this test does</h2>
 * <ol>
 *   <li>Deploys the Retail Promotion Agent sample by calling
 *       {@code POST /api/agents} on the running BFF, then
 *       {@code POST /api/agents/retail-agent/start} to submit the Flink job.</li>
 *   <li>Waits until the Flink job is running (polls {@code GET /api/flink/status}).</li>
 *   <li>Injects the events of the {@code large-purchase-reward} scenario of
 *       {@code samples/retail-agent/demo-scenarios.json} (onboarding + purchases) directly to Kafka,
 *       for a customer id that is unique to this run.</li>
 *   <li>Waits for the agent to publish a {@code Promotion} on the {@code agent-promotions} topic.</li>
 *   <li>Validates the {@code Promotion} (10% discount, SPORT category, reward message).</li>
 *   <li>Cleans up the deployed agent via {@code POST /api/agents/:id/stop}
 *       then {@code DELETE /api/agents/:id}.</li>
 * </ol>
 *
 * <h2>Running the test</h2>
 * <pre>
 *   mvn verify -pl event-rule-backend --settings settings.xml -Pe2e
 * </pre>
 *
 * <p>This test is only activated by the {@code -Pe2e} Maven profile when live infrastructure is available.
 */
@DisplayName("Retail Promotion Agent — end-to-end test")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class RetailAgentE2ETest {

    // ── Configuration (system-property / env overrides) ───────────────────────

    private static final String BFF_URL = resolveLocalhost(
        prop("BFF_URL", "http://localhost:8080"));
    private static final String KAFKA_BOOTSTRAP = prop("KAFKA_BOOTSTRAP", "localhost:9092");
    private static final long AGENT_STARTUP_TIMEOUT_MS =
        Long.parseLong(prop("E2E_AGENT_STARTUP_TIMEOUT_MS", "120000"));
    private static final long OUTPUT_WAIT_TIMEOUT_MS =
        Long.parseLong(prop("E2E_OUTPUT_WAIT_TIMEOUT_MS", "60000"));

    // ── Kafka topic names (must match agents.json) ───────────

    private static final String TOPIC_PROMOTIONS = "agent-promotions";
    private static final String TOPIC_CUSTOMER_CHANGELOG = "agent-customer-entity-changelog";
    private static final long BARRIER_TIMEOUT_MS = 20_000;
    private static final long BARRIER_GRACE_MS = 10_000;

    // ── Agent IDs ─────────────────────────────────────────────────────────────

    private static final String USE_CASE_ID = "retail-agent";
    /** Flink job name: {@code flinkJobName} in samples/retail-agent/agents.json. */
    private static final String AGENT_NAME  = "Retail Promotion Agent";

    /** Scenario of samples/retail-agent/demo-scenarios.json used by this test. */
    private static final String SCENARIO_ID = "large-purchase-reward";
    /** Customer id hard-coded in that scenario; replaced by a unique id for each run. */
    private static final String SCENARIO_CUSTOMER_ID = "WM";

    // ── Shared state ──────────────────────────────────────────────────────────

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(10))
        .version(HttpClient.Version.HTTP_1_1)
        .build();

    private static boolean agentStarted = false;

    // ── Pre-flight ────────────────────────────────────────────────────────────

    @BeforeAll
    static void requireStack() {
        assumeTrue(bffIsReachable(),
            "BFF is not reachable at " + BFF_URL
            + " — start the Docker Compose stack before running E2E tests: docker compose up -d");
    }

    // ── Step 1: deploy the agent ──────────────────────────────────────────────

    @Test
    @Order(1)
    @DisplayName("Step 1: POST /api/agents — deploy Retail Promotion Agent")
    void step1_deployAgent() throws Exception {
        String body = retailAgentBody();
        HttpResponse<String> res = post("/api/agents", body);

        assertTrue(res.statusCode() == 201 || res.statusCode() == 409,
            "Expected 201 or 409 from POST /api/agents, got " + res.statusCode()
            + ": " + res.body());
        if (res.statusCode() == 201) {
            JsonNode resp = MAPPER.readTree(res.body());
            assertTrue(resp.path("saved").asBoolean(),
                "saved must be true: " + res.body());
        }
    }

    // ── Step 2: start the Flink job ───────────────────────────────────────────

    @Test
    @Order(2)
    @DisplayName("Step 2: POST /api/agents/:id/start — submit Flink job")
    void step2_startAgent() throws Exception {
        HttpResponse<String> res = post("/api/agents/" + USE_CASE_ID + "/start", "{}");
        assumeTrue(res.statusCode() == 200 || res.statusCode() == 202,
            "Skipping E2E: /start returned " + res.statusCode()
            + " — Flink may not be available: " + res.body());
        agentStarted = true;
    }

    // ── Step 3: wait for the Flink job to be running ─────────────────────────

    @Test
    @Order(3)
    @DisplayName("Step 3: Wait until the Retail Promotion Agent job is RUNNING in Flink")
    void step3_waitForJobRunning() {
        assumeTrue(agentStarted, "Agent was not started — skipping");
        boolean running = waitForFlinkJob(AGENT_NAME, AGENT_STARTUP_TIMEOUT_MS);
        assertTrue(running,
            "Flink job '" + AGENT_NAME + "' did not become RUNNING within "
            + (AGENT_STARTUP_TIMEOUT_MS / 1000) + "s. Check Flink UI at " + BFF_URL + "/api/flink/status");
    }

    // ── Step 4: inject the scenario events ────────────────────────────────────

    @Test
    @Order(4)
    @DisplayName("Step 4: Inject the large-purchase-reward scenario events to Kafka")
    void step4_injectInputEvents() throws Exception {
        assumeTrue(agentStarted, "Agent was not started — skipping");

        String customerId = "E2E-" + System.currentTimeMillis();
        JsonNode events = loadScenarioEvents(customerId);
        assertTrue(events.isArray() && events.size() > 0,
            "Scenario '" + SCENARIO_ID + "' has no events");

        try (KafkaProducer<String, String> producer = newProducer()) {
            boolean first = true;
            for (JsonNode event : events) {
                producer.send(new ProducerRecord<>(
                    event.path("topic").asText(), customerId,
                    MAPPER.writeValueAsString(event.path("payload")))).get();
                producer.flush();
                if (first) {
                    // The first event (customer onboarding) is on another topic than the purchases:
                    // wait until the job has really processed it before the dependent events are
                    // published (a fixed pause is not enough on a slow host).
                    // The changelog record can be delayed by the Kafka sink on a slow host, so the wait is
                    // bounded; on timeout a grace pause (the source is reading within seconds of RUNNING)
                    // keeps the order, and step 5 still requires the real Promotion.
                    if (!awaitEntityProcessed(TOPIC_CUSTOMER_CHANGELOG, customerId, BARRIER_TIMEOUT_MS)) {
                        System.err.println("[e2e] No changelog record for " + customerId + " within "
                            + (BARRIER_TIMEOUT_MS / 1000) + "s: continuing after a grace pause");
                        Thread.sleep(BARRIER_GRACE_MS);
                    }
                    first = false;
                } else {
                    Thread.sleep(300);
                }
            }
        }

        System.setProperty("e2e.retail.customerId", customerId);
    }

    // ── Step 5: verify the Promotion output ───────────────────────────────────

    @Test
    @Order(5)
    @DisplayName("Step 5: Verify a Promotion is produced on agent-promotions")
    void step5_verifyPromotionOutput() {
        assumeTrue(agentStarted, "Agent was not started — skipping");
        String customerId = System.getProperty("e2e.retail.customerId");
        assumeTrue(customerId != null, "No customer ID set by step4 — skipping");

        JsonNode promotion = pollForPromotion(customerId, OUTPUT_WAIT_TIMEOUT_MS);
        assertNotNull(promotion,
            "No Promotion found on topic '" + TOPIC_PROMOTIONS + "' for customerId '"
            + customerId + "' within " + (OUTPUT_WAIT_TIMEOUT_MS / 1000) + "s.");

        // The agent publishes an envelope {agentEventType, entityId, sourceTopic, timestamp, payload:{...}}.
        assertEquals("Promotion", promotion.path("agentEventType").asText(), "promotion.agentEventType");
        assertEquals(customerId, promotion.path("entityId").asText(), "promotion.entityId");
        JsonNode body = promotion.path("payload");
        assertEquals(customerId, body.path("customerId").asText(), "promotion.customerId");
        assertEquals(10, body.path("discount").asDouble(), 0.01, "Discount should be 10%");
        assertEquals("SPORT", body.path("productCategory").asText(), "promotion.productCategory");
        assertTrue(body.path("message").asText().contains("Thanks for your recent purchase"),
            "Unexpected promotion message: " + body.path("message").asText());
    }

    // ── Step 6: clean up ──────────────────────────────────────────────────────

    @Test
    @Order(6)
    @DisplayName("Step 6: Stop and delete the deployed Retail Agent")
    void step6_cleanup() throws Exception {
        post("/api/agents/" + USE_CASE_ID + "/stop", "{}");

        // Kafka may still be re-electing partition leaders right after the job stopped, which makes
        // the topic purge/delete answer 500 "not the leader": retry such transient failures.
        HttpResponse<String> deleteRes = delete("/api/agents/" + USE_CASE_ID);
        for (int attempt = 1; attempt < 6 && deleteRes.statusCode() == 500; attempt++) {
            Thread.sleep(5_000);
            deleteRes = delete("/api/agents/" + USE_CASE_ID);
        }
        assertTrue(deleteRes.statusCode() == 200 || deleteRes.statusCode() == 404,
            "Unexpected status from DELETE: " + deleteRes.statusCode() + " " + deleteRes.body());
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static String retailAgentBody() throws Exception {
        java.nio.file.Path agentsJson =
            java.nio.file.Path.of("../samples/retail-agent/agents.json");
        String descriptor = java.nio.file.Files.exists(agentsJson)
            ? java.nio.file.Files.readString(agentsJson)
            : "{\"agents\":[]}";

        ObjectNode body = MAPPER.createObjectNode();
        body.put("useCaseId", USE_CASE_ID);
        body.set("descriptor", MAPPER.readTree(descriptor));
        return MAPPER.writeValueAsString(body);
    }

    /** Events of {@link #SCENARIO_ID}, with the scenario customer id replaced by {@code customerId}. */
    private static JsonNode loadScenarioEvents(String customerId) throws Exception {
        java.nio.file.Path scenarios =
            java.nio.file.Path.of("../samples/retail-agent/demo-scenarios.json");
        String json = java.nio.file.Files.readString(scenarios)
            .replace("\"" + SCENARIO_CUSTOMER_ID + "\"", "\"" + customerId + "\"");
        for (JsonNode scenario : MAPPER.readTree(json)) {
            if (SCENARIO_ID.equals(scenario.path("id").asText())) return scenario.path("events");
        }
        throw new IllegalStateException("Scenario not found: " + SCENARIO_ID);
    }

    private static boolean waitForFlinkJob(String jobName, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            try {
                HttpResponse<String> res = get("/api/flink/status");
                if (res.statusCode() == 200) {
                    JsonNode body = MAPPER.readTree(res.body());
                    JsonNode jobs = body.path("jobs");
                    if (jobs.isArray()) {
                        for (JsonNode j : jobs) {
                            if (jobName.equals(j.asText())) return true;
                        }
                    }
                }
            } catch (Exception ignored) { }
            try { Thread.sleep(3000); } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return false;
    }

    private static JsonNode pollForPromotion(String customerId, long timeoutMs) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "e2e-retail-verifier-" + System.nanoTime());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

        long deadline = System.currentTimeMillis() + timeoutMs;
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(TOPIC_PROMOTIONS));
            while (System.currentTimeMillis() < deadline) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) break;
                var records = consumer.poll(Duration.ofMillis(Math.min(remaining, 3000)));
                for (ConsumerRecord<String, String> record : records) {
                    if (customerId.equals(record.key()) || (record.value() != null && record.value().contains(customerId))) {
                        if (record.value() != null) {
                            return MAPPER.readTree(record.value());
                        }
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("[E2E] Error polling " + TOPIC_PROMOTIONS + ": " + e.getMessage());
        }
        return null;
    }

    /**
     * Blocks until the job has <em>processed</em> the first event of an entity: the agent writes a
     * record for the entity to its changelog topic once that event has updated the entity state.
     *
     * <p>"Job RUNNING" in Flink only means the tasks are deployed, not that the Kafka sources are
     * already reading. The input topics are read in parallel, so events published right after the
     * job starts can be consumed in any order: a dependent event (a transaction, a purchase) can
     * be processed before the event that creates the entity, and then produces no output. Waiting
     * for this record makes the publication order deterministic regardless of how fast the job
     * starts (the E2E runs on a slower host failed on exactly this).
     */
    private static boolean awaitEntityProcessed(String changelogTopic, String entityId, long timeoutMs) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "e2e-barrier-" + System.nanoTime());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        long deadline = System.currentTimeMillis() + timeoutMs;
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(changelogTopic));
            while (System.currentTimeMillis() < deadline) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) break;
                for (ConsumerRecord<String, String> record
                        : consumer.poll(Duration.ofMillis(Math.min(remaining, 1000)))) {
                    if (entityId.equals(record.key())
                            || (record.value() != null && record.value().contains(entityId))) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private static HttpResponse<String> get(String path) throws Exception {
        return HTTP.send(
            HttpRequest.newBuilder(URI.create(BFF_URL + path))
                .GET().timeout(Duration.ofSeconds(15)).build(),
            HttpResponse.BodyHandlers.ofString());
    }

    private static HttpResponse<String> post(String path, String jsonBody) throws Exception {
        return HTTP.send(
            HttpRequest.newBuilder(URI.create(BFF_URL + path))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                .timeout(Duration.ofSeconds(30)).build(),
            HttpResponse.BodyHandlers.ofString());
    }

    private static HttpResponse<String> delete(String path) throws Exception {
        return HTTP.send(
            HttpRequest.newBuilder(URI.create(BFF_URL + path))
                .DELETE().timeout(Duration.ofSeconds(60)).build(),
            HttpResponse.BodyHandlers.ofString());
    }

    private static boolean bffIsReachable() {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            try {
                HttpResponse<String> res = get("/health");
                if (res.statusCode() == 200) return true;
            } catch (Exception ignored) { }
            try { Thread.sleep(500); } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return false;
    }

    private static KafkaProducer<String, String> newProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        return new KafkaProducer<>(props);
    }

    private static String resolveLocalhost(String url) {
        if (url == null) return url;
        return url.replaceAll("(?<=[:/]{2})localhost(?=[:/]|$)", "127.0.0.1");
    }

    private static String prop(String name, String defaultVal) {
        String v = System.getProperty(name);
        if (v != null && !v.isBlank()) return v;
        v = System.getenv(name);
        return (v != null && !v.isBlank()) ? v : defaultVal;
    }
}
