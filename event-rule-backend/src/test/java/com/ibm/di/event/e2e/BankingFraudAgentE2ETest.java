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
 * End-to-end test for the Banking Fraud Detection Agent.
 *
 * <h2>What this test does</h2>
 * <ol>
 *   <li>Deploys the Banking Fraud Agent sample by calling
 *       {@code POST /api/agents} on the running BFF, then
 *       {@code POST /api/agents/banking-fraud-agent/start} to submit the Flink job.</li>
 *   <li>Waits until the Flink job is running (polls {@code GET /api/flink/status}).</li>
 *   <li>Injects representative input events directly to Kafka (Account + Transaction).</li>
 *   <li>Waits for the agent to process the events and produce output on the
 *       {@code agent-fraud-alerts} topic.</li>
 *   <li>Asserts that at least one {@code Alert} event was produced.</li>
 *   <li>Cleans up the deployed agent via {@code POST /api/agents/:id/stop}
 *       then {@code DELETE /api/agents/:id}.</li>
 * </ol>
 *
 * <h2>Prerequisites</h2>
 * <pre>
 *   # Build the fat JAR first (if not already done):
 *   mvn clean package -pl event-rule-backend --settings settings.xml -DskipTests
 *
 *   # Start the full Docker Compose stack:
 *   docker compose up -d
 * </pre>
 *
 * <h2>Running the test</h2>
 * <pre>
 *   mvn verify -pl event-rule-backend --settings settings.xml -Pe2e
 * </pre>
 *
 * <p>Override connection parameters via system properties or environment variables:
 * <pre>
 *   BFF_URL=http://127.0.0.1:8080           (default: http://127.0.0.1:8080)
 *   KAFKA_BOOTSTRAP=localhost:9092          (default: localhost:9092)
 *   E2E_AGENT_STARTUP_TIMEOUT_MS=120000     (default: 120 000 — 2 minutes)
 *   E2E_OUTPUT_WAIT_TIMEOUT_MS=60000        (default: 60 000 — 1 minute)
 * </pre>
 *
 * <p>This test is <b>never executed during {@code mvn clean install}</b>.
 * It is only activated by the {@code -Pe2e} Maven profile.
 */
@DisplayName("Banking Fraud Agent — end-to-end test")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class BankingFraudAgentE2ETest {

    // ── Configuration (system-property / env overrides) ───────────────────────

    private static final String BFF_URL = resolveLocalhost(
        prop("BFF_URL", "http://localhost:8080"));
    private static final String KAFKA_BOOTSTRAP = prop("KAFKA_BOOTSTRAP", "localhost:9092");
    private static final long   AGENT_STARTUP_TIMEOUT_MS =
        Long.parseLong(prop("E2E_AGENT_STARTUP_TIMEOUT_MS", "120000"));
    private static final long   OUTPUT_WAIT_TIMEOUT_MS =
        Long.parseLong(prop("E2E_OUTPUT_WAIT_TIMEOUT_MS", "60000"));

    // ── Kafka topic names (must match agents.json / descriptor.json) ───────────

    private static final String TOPIC_ACCOUNT_EVENTS = "agent-account-events";
    private static final String TOPIC_TRANSACTIONS   = "agent-transactions";
    private static final String TOPIC_FRAUD_ALERTS   = "agent-fraud-alerts";

    // ── Agent IDs ─────────────────────────────────────────────────────────────

    /** Fixed use-case id — matches the sample directory name. */
    private static final String USE_CASE_ID = "banking-fraud-agent";

    // ── Shared state ──────────────────────────────────────────────────────────

    private static final ObjectMapper MAPPER    = new ObjectMapper();
    private static final HttpClient   HTTP      = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(10))
        .version(HttpClient.Version.HTTP_1_1)
        .build();

    /** Set to true once the agent is successfully started; governs cleanup. */
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
    @DisplayName("Step 1: POST /api/agents — deploy Banking Fraud Agent")
    void step1_deployAgent() throws Exception {
        String body = bankingFraudAgentBody();
        HttpResponse<String> res = post("/api/agents", body);

        // 201 = created; 409 = already exists (prior run didn't clean up) — both are OK
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
        assumeTrue(res.statusCode() == 200,
            "Skipping E2E: /start returned " + res.statusCode()
            + " — Flink may not be available: " + res.body());
        agentStarted = true;
    }

    // ── Step 3: wait for the Flink job to be running ─────────────────────────

    @Test
    @Order(3)
    @DisplayName("Step 3: Wait until the Banking Fraud Detection Agent job is RUNNING in Flink")
    void step3_waitForJobRunning() {
        assumeTrue(agentStarted, "Agent was not started — skipping");
        boolean running = waitForFlinkJob("Banking Fraud Detection Agent", AGENT_STARTUP_TIMEOUT_MS);
        assertTrue(running,
            "Flink job 'Banking Fraud Detection Agent' did not become RUNNING within "
            + (AGENT_STARTUP_TIMEOUT_MS / 1000) + "s. Check Flink UI at " + BFF_URL + "/api/flink/status");
    }

    // ── Step 4: inject input events ───────────────────────────────────────────

    @Test
    @Order(4)
    @DisplayName("Step 4: Inject Account creation + Transaction events to Kafka")
    void step4_injectInputEvents() throws Exception {
        assumeTrue(agentStarted, "Agent was not started — skipping");

        String accountNumber = "E2E-" + System.currentTimeMillis();

        // Send an AccountEvent first so the agent initialises entity state
        ObjectNode accountEvent = MAPPER.createObjectNode();
        accountEvent.put("accountNumber",   accountNumber);
        accountEvent.put("accountType",     "CHECKING");
        accountEvent.put("balance",         10000);
        accountEvent.put("currency",        "DOLLAR");
        accountEvent.put("description",     "E2E test account creation");
        accountEvent.put("overdraftLimit",  15000);
        accountEvent.put("owner",           "E2E Test User");
        accountEvent.put("ownerCountry",    "USA");
        accountEvent.put("status",          "ACTIVE");
        accountEvent.put("timestamp",       Instant.now().toString());
        accountEvent.put("type",            "OPENING");

        // Send a large transaction that the fraud rules should flag
        ObjectNode transaction = MAPPER.createObjectNode();
        transaction.put("accountNumber",    accountNumber);
        transaction.put("amount",           50000);       // large amount — likely triggers alert
        transaction.put("channel",          "ONLINE");
        transaction.put("country",          "FOREIGN");   // foreign country — higher risk
        transaction.put("currency",         "DOLLAR");
        transaction.put("merchant",         "Unknown Merchant");
        transaction.put("merchantCategory", "OTHER");
        transaction.put("timestamp",        Instant.now().toString());
        transaction.put("transactionId",    "E2E-TXN-" + System.nanoTime());
        transaction.put("type",             "DEBIT");

        try (KafkaProducer<String, String> producer = newProducer()) {
            // Publish account event
            producer.send(new ProducerRecord<>(
                TOPIC_ACCOUNT_EVENTS, accountNumber,
                MAPPER.writeValueAsString(accountEvent))).get();

            // Small pause so the agent processes the AccountEvent before the transaction
            Thread.sleep(500);

            // Publish transaction event
            producer.send(new ProducerRecord<>(
                TOPIC_TRANSACTIONS, accountNumber,
                MAPPER.writeValueAsString(transaction))).get();

            producer.flush();
        }

        // Store the account number so step 5 can correlate
        System.setProperty("e2e.accountNumber", accountNumber);
    }

    // ── Step 5: verify output ─────────────────────────────────────────────────

    @Test
    @Order(5)
    @DisplayName("Step 5: Verify Alert event produced on agent-fraud-alerts topic")
    void step5_verifyOutput() {
        assumeTrue(agentStarted, "Agent was not started — skipping");
        String accountNumber = System.getProperty("e2e.accountNumber");
        assumeTrue(accountNumber != null, "No account number set by step4 — skipping");

        boolean alertFound = pollForAlert(accountNumber, OUTPUT_WAIT_TIMEOUT_MS);
        assertTrue(alertFound,
            "No Alert event found on topic '" + TOPIC_FRAUD_ALERTS + "' for accountNumber '"
            + accountNumber + "' within " + (OUTPUT_WAIT_TIMEOUT_MS / 1000) + "s. "
            + "Check that the Flink job processed the transaction and the fraud rules fired.");
    }

    // ── Step 6: clean up ──────────────────────────────────────────────────────

    @Test
    @Order(6)
    @DisplayName("Step 6: Stop and delete the deployed Banking Fraud Agent")
    void step6_cleanup() throws Exception {
        if (agentStarted) {
            HttpResponse<String> stopRes =
                post("/api/agents/" + USE_CASE_ID + "/stop", "{}");
            assertTrue(stopRes.statusCode() == 200 || stopRes.statusCode() == 404,
                "Unexpected status from /stop: " + stopRes.statusCode() + " " + stopRes.body());
        }

        HttpResponse<String> deleteRes = delete("/api/agents/" + USE_CASE_ID);
        assertTrue(deleteRes.statusCode() == 200 || deleteRes.statusCode() == 404,
            "Unexpected status from DELETE: " + deleteRes.statusCode() + " " + deleteRes.body());
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /**
     * Returns the agents.json payload for the Banking Fraud Agent,
     * pointing to the sample JAR and descriptor that are bundled in the project.
     */
    private static String bankingFraudAgentBody() throws Exception {
        // Read the agents.json from the sample directory
        java.nio.file.Path agentsJson =
            java.nio.file.Path.of("../samples/banking-fraud-agent/agents.json");
        String descriptor = java.nio.file.Files.exists(agentsJson)
            ? java.nio.file.Files.readString(agentsJson)
            : "{\"agents\":[]}";

        ObjectNode body = MAPPER.createObjectNode();
        body.put("useCaseId", USE_CASE_ID);
        body.set("descriptor", MAPPER.readTree(descriptor));
        return MAPPER.writeValueAsString(body);
    }

    /**
     * Polls {@code GET /api/flink/status} until the named job appears in the
     * running jobs list, or until {@code timeoutMs} elapses.
     */
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
            } catch (Exception ignored) { /* not yet ready */ }
            try { Thread.sleep(3000); } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return false;
    }

    /**
     * Polls the {@code agent-fraud-alerts} Kafka topic until an Alert message
     * whose key or payload contains {@code accountNumber} is found, or until
     * {@code timeoutMs} elapses.
     */
    private static boolean pollForAlert(String accountNumber, long timeoutMs) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
            "e2e-verifier-" + System.nanoTime());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

        long deadline = System.currentTimeMillis() + timeoutMs;
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(TOPIC_FRAUD_ALERTS));
            while (System.currentTimeMillis() < deadline) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) break;
                var records = consumer.poll(Duration.ofMillis(Math.min(remaining, 3000)));
                for (ConsumerRecord<String, String> record : records) {
                    // Match by key (entity id = accountNumber) or payload content
                    if (accountNumber.equals(record.key())) return true;
                    if (record.value() != null && record.value().contains(accountNumber)) return true;
                }
            }
        } catch (Exception e) {
            System.err.println("[E2E] Error polling " + TOPIC_FRAUD_ALERTS + ": " + e.getMessage());
        }
        return false;
    }

    // ── HTTP helpers ──────────────────────────────────────────────────────────

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
            } catch (Exception ignored) { /* not yet */ }
            try { Thread.sleep(500); } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return false;
    }

    private static KafkaProducer<String, String> newProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,  KAFKA_BOOTSTRAP);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        return new KafkaProducer<>(props);
    }

    /** Replaces {@code localhost} with {@code 127.0.0.1} to avoid IPv6 issues on macOS. */
    private static String resolveLocalhost(String url) {
        if (url == null) return url;
        return url.replaceAll("(?<=[:/]{2})localhost(?=[:/]|$)", "127.0.0.1");
    }

    /** Reads a string from system property, then env variable, then returns {@code defaultVal}. */
    private static String prop(String name, String defaultVal) {
        String v = System.getProperty(name);
        if (v != null && !v.isBlank()) return v;
        v = System.getenv(name);
        return (v != null && !v.isBlank()) ? v : defaultVal;
    }
}

