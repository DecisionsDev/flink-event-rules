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
 * End-to-end integration test for the Retail Next Best Offer Agent.
 *
 * <h2>What this test does</h2>
 * <ol>
 *   <li>Deploys the Retail Next Best Offer Agent sample by calling
 *       {@code POST /api/agents} on the running BFF, then
 *       {@code POST /api/agents/retail-next-best-offer-agent/start} to submit the Flink job.</li>
 *   <li>Waits until the Flink job is running (polls {@code GET /api/flink/status}).</li>
 *   <li>Injects multi-event context sequence (CustomerEvent -> ProductEvent -> InventoryEvent ->
 *       PromotionEvent -> ShoppingEvent) directly to Kafka topics.</li>
 *   <li>Waits for the agent to process the events and evaluate the decision pipeline to produce
 *       a {@code NextBestOffer} event on the {@code agent-next-best-offers} topic.</li>
 *   <li>Validates the presence and field assertions of the {@code NextBestOffer} event.</li>
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
@DisplayName("Retail Next Best Offer Agent — end-to-end test")
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

    private static final String TOPIC_CUSTOMER_EVENTS  = "agent-customer-events";
    private static final String TOPIC_PRODUCT_EVENTS   = "agent-product-events";
    private static final String TOPIC_INVENTORY_EVENTS = "agent-inventory-events";
    private static final String TOPIC_PROMOTION_EVENTS = "agent-promotion-events";
    private static final String TOPIC_SHOPPING_EVENTS  = "agent-shopping-events";
    private static final String TOPIC_NEXT_BEST_OFFERS = "agent-next-best-offers";

    // ── Agent IDs ─────────────────────────────────────────────────────────────

    private static final String USE_CASE_ID = "retail-agent";
    private static final String AGENT_NAME  = "Retail Next Best Offer Agent";

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
    @DisplayName("Step 1: POST /api/agents — deploy Retail Next Best Offer Agent")
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
    @DisplayName("Step 3: Wait until the Retail Next Best Offer Agent job is RUNNING in Flink")
    void step3_waitForJobRunning() {
        assumeTrue(agentStarted, "Agent was not started — skipping");
        boolean running = waitForFlinkJob(AGENT_NAME, AGENT_STARTUP_TIMEOUT_MS);
        assertTrue(running,
            "Flink job '" + AGENT_NAME + "' did not become RUNNING within "
            + (AGENT_STARTUP_TIMEOUT_MS / 1000) + "s. Check Flink UI at " + BFF_URL + "/api/flink/status");
    }

    // ── Step 4: inject context events and shopping trigger ────────────────────

    @Test
    @Order(4)
    @DisplayName("Step 4: Inject multi-context events and trigger shopping event to Kafka")
    void step4_injectInputEvents() throws Exception {
        assumeTrue(agentStarted, "Agent was not started — skipping");

        String customerId = "CUST-E2E-" + System.currentTimeMillis();

        // 1. Customer Context
        ObjectNode customerEvent = MAPPER.createObjectNode();
        ObjectNode customerContext = customerEvent.putObject("customerContext");
        customerContext.put("customerIdentifier", customerId);
        customerContext.put("customerProfile", "premium");
        ObjectNode loyalty = customerContext.putObject("loyaltyInformation");
        loyalty.put("loyaltyPoints", 1500);
        loyalty.put("loyaltyStatus", "Gold");
        var history = customerContext.putArray("purchaseHistory");
        history.add("headphones").add("laptop").add("tablet").add("speaker").add("keyboard").add("monitor");

        // 2. Product Context
        ObjectNode productEvent = MAPPER.createObjectNode();
        productEvent.putObject("customerContext").put("customerIdentifier", customerId);
        ObjectNode productContext = productEvent.putObject("productContext");
        ObjectNode productDetails = productContext.putObject("productDetails");
        productDetails.put("productId", "HEADPHONES-PRO-X1");
        productDetails.put("productCategory", "Electronics");
        productDetails.put("productPrice", 649.99);
        var compProducts = productContext.putArray("complementaryProducts");
        ObjectNode compItem = compProducts.addObject();
        compItem.put("productId", "CARRYING-CASE-X1");
        compItem.put("productCategory", "Accessories");
        compItem.put("productPrice", 49.99);

        // 3. Inventory Context
        ObjectNode inventoryEvent = MAPPER.createObjectNode();
        inventoryEvent.putObject("customerContext").put("customerIdentifier", customerId);
        ObjectNode inventoryContext = inventoryEvent.putObject("inventoryContext");
        inventoryContext.put("productAvailability", true);
        inventoryContext.put("storeInventory", 12);
        inventoryContext.put("inventoryConstraints", "None");

        // 4. Promotion Context
        ObjectNode promotionEvent = MAPPER.createObjectNode();
        promotionEvent.putObject("customerContext").put("customerIdentifier", customerId);
        ObjectNode promotionContext = promotionEvent.putObject("promotionContext");
        var availPromos = promotionContext.putArray("availablePromotions");
        ObjectNode promo = availPromos.addObject();
        promo.put("product", "Carrying Case X1");
        promo.put("discountPercentage", 15.0);
        promo.put("promotionType", "ACCESSORY_DISCOUNT");
        promotionContext.put("discountRules", "15% off carrying case with premium Electronics purchase");
        promotionContext.put("promotionEligibility", "Gold loyalty members");
        promotionContext.put("promotionValidity", "2026-12-31");

        // 5. Shopping Event (Trigger)
        ObjectNode shoppingEvent = MAPPER.createObjectNode();
        shoppingEvent.putObject("customerContext").put("customerIdentifier", customerId);
        ObjectNode shoppingContext = shoppingEvent.putObject("shoppingContext");
        shoppingContext.put("eventType", "PRODUCT_VIEW");
        ObjectNode shoppingProduct = shoppingContext.putObject("productInformation");
        shoppingProduct.put("productId", "HEADPHONES-PRO-X1");
        shoppingProduct.put("productCategory", "Electronics");
        shoppingProduct.put("productPrice", 649.99);
        shoppingContext.put("searchQuery", "wireless noise cancelling headphones");
        ObjectNode store = shoppingContext.putObject("storeContext");
        store.put("storeId", "STORE-NYC-001");
        store.put("storeLocation", "New York, NY");

        try (KafkaProducer<String, String> producer = newProducer()) {
            producer.send(new ProducerRecord<>(TOPIC_CUSTOMER_EVENTS, customerId, MAPPER.writeValueAsString(customerEvent))).get();
            Thread.sleep(300);

            producer.send(new ProducerRecord<>(TOPIC_PRODUCT_EVENTS, customerId, MAPPER.writeValueAsString(productEvent))).get();
            Thread.sleep(300);

            producer.send(new ProducerRecord<>(TOPIC_INVENTORY_EVENTS, customerId, MAPPER.writeValueAsString(inventoryEvent))).get();
            Thread.sleep(300);

            producer.send(new ProducerRecord<>(TOPIC_PROMOTION_EVENTS, customerId, MAPPER.writeValueAsString(promotionEvent))).get();
            Thread.sleep(300);

            producer.send(new ProducerRecord<>(TOPIC_SHOPPING_EVENTS, customerId, MAPPER.writeValueAsString(shoppingEvent))).get();
            producer.flush();
        }

        System.setProperty("e2e.retail.customerId", customerId);
    }

    // ── Step 5: verify NextBestOffer output ────────────────────────────────────

    @Test
    @Order(5)
    @DisplayName("Step 5: Verify NextBestOffer event produced on agent-next-best-offers topic")
    void step5_verifyNextBestOfferOutput() {
        assumeTrue(agentStarted, "Agent was not started — skipping");
        String customerId = System.getProperty("e2e.retail.customerId");
        assumeTrue(customerId != null, "No customer ID set by step4 — skipping");

        JsonNode offerRecord = pollForNextBestOffer(customerId, OUTPUT_WAIT_TIMEOUT_MS);
        assertNotNull(offerRecord,
            "No NextBestOffer event found on topic '" + TOPIC_NEXT_BEST_OFFERS + "' for customerId '"
            + customerId + "' within " + (OUTPUT_WAIT_TIMEOUT_MS / 1000) + "s.");

        // Validate payload structure and decision outcome
        assertEquals("present offer", offerRecord.path("decision").asText(), "Decision should be 'present offer'");
        assertEquals(15.0, offerRecord.path("discount").asDouble(), 0.01, "Discount should be 15%");
        assertEquals("carrying case", offerRecord.path("recommendedProduct").asText(), "Recommended product should be 'carrying case'");
        assertEquals("15% off carrying case", offerRecord.path("selectedPromotion").asText(), "Selected promotion should match");
    }

    // ── Step 6: clean up ──────────────────────────────────────────────────────

    @Test
    @Order(6)
    @DisplayName("Step 6: Stop and delete the deployed Retail Agent")
    void step6_cleanup() throws Exception {
        post("/api/agents/" + USE_CASE_ID + "/stop", "{}");

        HttpResponse<String> deleteRes = delete("/api/agents/" + USE_CASE_ID);
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

    private static JsonNode pollForNextBestOffer(String customerId, long timeoutMs) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "e2e-retail-verifier-" + System.nanoTime());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

        long deadline = System.currentTimeMillis() + timeoutMs;
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(TOPIC_NEXT_BEST_OFFERS));
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
            System.err.println("[E2E] Error polling " + TOPIC_NEXT_BEST_OFFERS + ": " + e.getMessage());
        }
        return null;
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
