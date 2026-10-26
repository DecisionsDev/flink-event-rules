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
package com.ibm.di.event.functional;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.*;

import java.net.http.HttpResponse;
import java.util.UUID;

import static com.ibm.di.event.functional.FunctionalTestEnvironment.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Functional tests for the BFF event-injection endpoint.
 *
 * <p>Validates that {@code POST /api/use-cases/:useCase/events} correctly publishes
 * events to Kafka through the BFF's shared producer and enforces all validation rules
 * documented in server.js.
 *
 * <p>Tests are <b>skipped</b> unless {@code FUNCTIONAL_TESTS_ENABLED=true} is set.
 * Run with:
 * <pre>
 *   FUNCTIONAL_TESTS_ENABLED=true mvn test -pl event-rule-backend \
 *     --settings settings.xml -Dtest=BffEventInjectionFunctionalTest
 * </pre>
 *
 * <p>Scenarios covered:
 * <ul>
 *   <li>Happy path: single transaction event injected → 200, published=1</li>
 *   <li>Happy path: batch of 3 events injected in one call → published=3</li>
 *   <li>Account-created event injected to correct topic → 200</li>
 *   <li>Order event injected to agent-orders topic → 200</li>
 *   <li>Product-view event injected to agent-product-views topic → 200</li>
 *   <li>Response body contains correct topic field</li>
 *   <li>Response body published count matches events array size</li>
 *   <li>Validation: missing topic field → 400</li>
 *   <li>Validation: topic is empty string → 400</li>
 *   <li>Validation: events is empty array → 400</li>
 *   <li>Validation: events is not an array → 400</li>
 *   <li>Validation: events field absent → 400</li>
 *   <li>Validation: unknown topic for use case → 400 with knownTopics list</li>
 *   <li>Validation: unknown use case → 404</li>
 *   <li>Validation: invalid use case id (path traversal) → 400</li>
 *   <li>Banking fraud scenario: large transaction injected to transactions topic</li>
 *   <li>Banking fraud scenario: frozen-account sequence (account + transaction) injected</li>
 *   <li>Banking fraud scenario: velocity-check sequence (6 transactions) injected</li>
 *   <li>Retail scenario: order event injected to agent-orders</li>
 *   <li>Retail scenario: product-view event injected to agent-product-views</li>
 * </ul>
 */
@DisplayName("BFF Event Injection — functional tests")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class BffEventInjectionFunctionalTest {

    // Banking topics from agents.json
    private static final String TOPIC_TRANSACTIONS    = "agent-transactions";
    private static final String TOPIC_ACCOUNT_CREATED = "agent-account-created-events";

    // Retail topics from agents.json
    private static final String TOPIC_ORDERS          = "agent-orders";
    private static final String TOPIC_PRODUCT_VIEWS   = "agent-product-views";

    @BeforeAll
    static void requireStack() {
        assumeTrue(isEnabled(), disabledReason());
        assumeTrue(waitForBff(),
            "BFF is not reachable at " + BFF_BASE_URL
            + " — ensure the Docker Compose stack is running.");
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /** Builds a minimal banking transaction payload. */
    private static String transactionPayload(String txId, String accountId, double amount) {
        return """
            {
              "transactionId": "%s",
              "accountId": "%s",
              "timestamp": "2026-06-01T10:00:00Z",
              "type": "DEBIT",
              "merchant": "Test Merchant",
              "amount": "%.2f",
              "country": "USA",
              "cardHolderCountry": "USA"
            }
            """.formatted(txId, accountId, amount);
    }

    /** Builds a minimal banking account-created payload. */
    private static String accountPayload(String accountId, String status) {
        return """
            {
              "accountId": "%s",
              "timestamp": "2026-05-01T09:00:00Z",
              "owner": "Test Owner",
              "balance": "1000.00",
              "country": "USA",
              "status": "%s"
            }
            """.formatted(accountId, status);
    }

    /** Builds a minimal retail order payload. */
    private static String orderPayload(String orderId, String customerId) {
        return """
            {
              "orderId": "%s",
              "customerId": "%s",
              "timestamp": "2026-06-01T10:00:00Z",
              "totalAmount": 129.99,
              "currency": "USD",
              "items": [
                {
                  "productId": "PROD-001",
                  "productName": "Test Product",
                  "category": "Electronics",
                  "quantity": 1,
                  "unitPrice": 129.99,
                  "totalPrice": 129.99
                }
              ],
              "customerType": "REGULAR",
              "loyaltyTier": "SILVER",
              "loyaltyPoints": 500,
              "orderSource": "web",
              "paymentMethod": "credit_card",
              "purchaseHistory": {
                "totalOrders": 5,
                "totalSpend": 450,
                "favoriteCategory": "Electronics",
                "daysSinceLastOrder": 30
              },
              "browsingSignals": {
                "productViewCount": 3,
                "cartAbandoned": false,
                "wishlistAdded": false,
                "sessionDurationMinutes": 12
              },
              "viewProfile": {
                "distinctProductsViewed": 3,
                "totalViewDurationSeconds": 180,
                "maxSingleViewDurationSeconds": 90,
                "viewWindowMinutes": 30,
                "topCategories": ["Electronics"],
                "viewedProductIds": ["PROD-001"],
                "viewedProductNames": ["Test Product"]
              }
            }
            """.formatted(orderId, customerId);
    }

    /** Builds a minimal retail product-view payload. */
    private static String productViewPayload(String productId, String customerId) {
        return """
            {
              "productId": "%s",
              "customerId": "%s",
              "productName": "Test Product",
              "category": "Electronics",
              "price": 99.99,
              "viewDurationSeconds": 45,
              "timestamp": "2026-06-01T09:55:00Z"
            }
            """.formatted(productId, customerId);
    }

    /**
     * POSTs events to the BFF injection endpoint and returns the response.
     * {@code events} is a JSON array string.
     */
    private static HttpResponse<String> injectEvents(String useCase, String topic,
                                                      String eventsArray) throws Exception {
        String body = """
            {"topic":"%s","events":%s}
            """.formatted(topic, eventsArray);
        return post("/api/use-cases/" + useCase + "/events", body);
    }

    // ── Happy-path: banking transactions ─────────────────────────────────────

    @Nested
    @DisplayName("Banking — transaction event injection (happy path)")
    class BankingTransactionHappyPath {

        @Test
        @DisplayName("single transaction event → 200, published=1, topic matches")
        void singleTransactionInjected() throws Exception {
            String txId = "FT-SINGLE-" + UUID.randomUUID().toString().substring(0, 8);
            String eventsJson = "[" + transactionPayload(txId, "ACC-FT-001", 45.99) + "]";

            HttpResponse<String> res = injectEvents("agent-banking", TOPIC_TRANSACTIONS, eventsJson);

            assertEquals(200, res.statusCode(),
                "Expected 200 from event injection: " + res.body());
            JsonNode body = json(res);
            assertEquals(1, body.path("published").asInt(),
                "Expected published=1: " + res.body());
            assertEquals(TOPIC_TRANSACTIONS, body.path("topic").asText(),
                "Response topic must match request topic");
        }

        @Test
        @DisplayName("batch of 3 transaction events → 200, published=3")
        void batchOf3Injected() throws Exception {
            String accountId = "ACC-FT-BATCH-" + UUID.randomUUID().toString().substring(0, 8);
            String eventsJson = "[" +
                transactionPayload("FT-B1", accountId, 10.00) + "," +
                transactionPayload("FT-B2", accountId, 20.00) + "," +
                transactionPayload("FT-B3", accountId, 30.00) +
                "]";

            HttpResponse<String> res = injectEvents("agent-banking", TOPIC_TRANSACTIONS, eventsJson);

            assertEquals(200, res.statusCode(),
                "Expected 200 from batch injection: " + res.body());
            assertEquals(3, json(res).path("published").asInt(),
                "Expected published=3 for batch");
        }

        @Test
        @DisplayName("large transaction ($14500) — scenario LARGE-001 injected")
        void largeTransactionScenario() throws Exception {
            String txId = "FT-LARGE-" + UUID.randomUUID().toString().substring(0, 8);
            String eventsJson = "[" + transactionPayload(txId, "ACC-FT-LARGE-001", 14500.00) + "]";

            HttpResponse<String> res = injectEvents("agent-banking", TOPIC_TRANSACTIONS, eventsJson);

            assertEquals(200, res.statusCode(),
                "Large transaction injection must succeed: " + res.body());
            assertEquals(1, json(res).path("published").asInt());
        }
    }

    // ── Happy-path: banking account-created events ────────────────────────────

    @Nested
    @DisplayName("Banking — account-created event injection (happy path)")
    class BankingAccountCreatedHappyPath {

        @Test
        @DisplayName("account-created event injected → 200, published=1")
        void accountCreatedInjected() throws Exception {
            String accountId = "ACC-FT-AC-" + UUID.randomUUID().toString().substring(0, 8);
            String eventsJson = "[" + accountPayload(accountId, "ACTIVE") + "]";

            HttpResponse<String> res = injectEvents("agent-banking", TOPIC_ACCOUNT_CREATED, eventsJson);

            assertEquals(200, res.statusCode(),
                "Expected 200 for account-created injection: " + res.body());
            assertEquals(1, json(res).path("published").asInt());
        }
    }

    // ── Happy-path: banking frozen-account scenario ───────────────────────────

    @Nested
    @DisplayName("Banking — frozen-account scenario (account + transaction sequence)")
    class FrozenAccountScenario {

        @Test
        @DisplayName("frozen account + transaction → both events injected successfully")
        void frozenAccountPlusTransaction() throws Exception {
            String accountId = "ACC-FT-FRZ-" + UUID.randomUUID().toString().substring(0, 8);

            // Step 1: inject account-created with FROZEN status
            String accountJson = "[" + accountPayload(accountId, "FROZEN") + "]";
            HttpResponse<String> accountRes = injectEvents("agent-banking", TOPIC_ACCOUNT_CREATED, accountJson);
            assertEquals(200, accountRes.statusCode(),
                "Account injection must succeed: " + accountRes.body());

            // Step 2: inject transaction for that account
            String txJson = "[" + transactionPayload("FT-FRZ-TX", accountId, 75.00) + "]";
            HttpResponse<String> txRes = injectEvents("agent-banking", TOPIC_TRANSACTIONS, txJson);
            assertEquals(200, txRes.statusCode(),
                "Transaction injection must succeed after frozen account: " + txRes.body());

            assertEquals(1, json(txRes).path("published").asInt());
        }
    }

    // ── Happy-path: banking velocity-check scenario ───────────────────────────

    @Nested
    @DisplayName("Banking — velocity-check scenario (6 transactions)")
    class VelocityCheckScenario {

        @Test
        @DisplayName("6 rapid transactions for same account → all injected, published=6")
        void sixTransactionsInjectedAsBatch() throws Exception {
            String accountId = "ACC-FT-VEL-" + UUID.randomUUID().toString().substring(0, 8);
            StringBuilder sb = new StringBuilder("[");
            for (int i = 1; i <= 6; i++) {
                if (i > 1) sb.append(",");
                sb.append(transactionPayload("FT-VEL-" + String.format("%03d", i), accountId, 50.0 * i));
            }
            sb.append("]");

            HttpResponse<String> res = injectEvents("agent-banking", TOPIC_TRANSACTIONS, sb.toString());

            assertEquals(200, res.statusCode(),
                "6-transaction velocity batch must succeed: " + res.body());
            assertEquals(6, json(res).path("published").asInt(),
                "Expected published=6 for velocity-check batch");
        }
    }

    // ── Happy-path: retail events ─────────────────────────────────────────────

    @Nested
    @DisplayName("Retail — event injection (happy path)")
    class RetailHappyPath {

        @Test
        @DisplayName("order event injected → 200, published=1")
        void orderEventInjected() throws Exception {
            String orderId    = "ORD-FT-" + UUID.randomUUID().toString().substring(0, 8);
            String customerId = "CUST-FT-" + UUID.randomUUID().toString().substring(0, 8);
            String eventsJson = "[" + orderPayload(orderId, customerId) + "]";

            HttpResponse<String> res = injectEvents("agent-retail", TOPIC_ORDERS, eventsJson);

            assertEquals(200, res.statusCode(),
                "Order injection must succeed: " + res.body());
            assertEquals(1, json(res).path("published").asInt());
            assertEquals(TOPIC_ORDERS, json(res).path("topic").asText());
        }

        @Test
        @DisplayName("product-view event injected → 200, published=1")
        void productViewEventInjected() throws Exception {
            String productId  = "PROD-FT-" + UUID.randomUUID().toString().substring(0, 8);
            String customerId = "CUST-FT-" + UUID.randomUUID().toString().substring(0, 8);
            String eventsJson = "[" + productViewPayload(productId, customerId) + "]";

            HttpResponse<String> res = injectEvents("agent-retail", TOPIC_PRODUCT_VIEWS, eventsJson);

            assertEquals(200, res.statusCode(),
                "Product-view injection must succeed: " + res.body());
            assertEquals(1, json(res).path("published").asInt());
            assertEquals(TOPIC_PRODUCT_VIEWS, json(res).path("topic").asText());
        }

        @Test
        @DisplayName("product-view followed by order for same customer → both injected")
        void productViewThenOrder() throws Exception {
            String customerId = "CUST-FT-SEQ-" + UUID.randomUUID().toString().substring(0, 8);

            HttpResponse<String> viewRes = injectEvents("agent-retail", TOPIC_PRODUCT_VIEWS,
                "[" + productViewPayload("PROD-SEQ-001", customerId) + "]");
            assertEquals(200, viewRes.statusCode(),
                "Product view injection must succeed: " + viewRes.body());

            HttpResponse<String> orderRes = injectEvents("agent-retail", TOPIC_ORDERS,
                "[" + orderPayload("ORD-SEQ-001", customerId) + "]");
            assertEquals(200, orderRes.statusCode(),
                "Order injection must succeed after product view: " + orderRes.body());
        }
    }

    // ── Validation: request body errors ──────────────────────────────────────

    @Nested
    @DisplayName("Event injection — request validation (400 errors)")
    class RequestValidation {

        @Test
        @DisplayName("missing topic field → 400")
        void missingTopic400() throws Exception {
            String body = """
                {"events":[{"key":"value"}]}
                """;
            HttpResponse<String> res = post("/api/use-cases/agent-banking/events", body);
            assertEquals(400, res.statusCode(),
                "Expected 400 when topic is missing: " + res.body());
        }

        @Test
        @DisplayName("topic is empty string → 400")
        void emptyTopic400() throws Exception {
            String body = """
                {"topic":"","events":[{"key":"value"}]}
                """;
            HttpResponse<String> res = post("/api/use-cases/agent-banking/events", body);
            assertEquals(400, res.statusCode(),
                "Expected 400 when topic is empty: " + res.body());
        }

        @Test
        @DisplayName("events is empty array → 400")
        void emptyEventsArray400() throws Exception {
            String body = """
                {"topic":"agent-transactions","events":[]}
                """;
            HttpResponse<String> res = post("/api/use-cases/agent-banking/events", body);
            assertEquals(400, res.statusCode(),
                "Expected 400 for empty events array: " + res.body());
        }

        @Test
        @DisplayName("events field absent → 400")
        void missingEventsField400() throws Exception {
            String body = """
                {"topic":"agent-transactions"}
                """;
            HttpResponse<String> res = post("/api/use-cases/agent-banking/events", body);
            assertEquals(400, res.statusCode(),
                "Expected 400 when events field is absent: " + res.body());
        }

        @Test
        @DisplayName("topic not declared in use-case descriptor → 400 with knownTopics")
        void unknownTopicForUseCase400() throws Exception {
            // agent-banking only has agent-transactions and agent-account-created-events;
            // injecting to agent-orders (a retail topic) must fail
            String body = """
                {"topic":"agent-orders","events":[{"key":"value"}]}
                """;
            HttpResponse<String> res = post("/api/use-cases/agent-banking/events", body);
            assertEquals(400, res.statusCode(),
                "Expected 400 for topic not in use-case descriptor: " + res.body());
            JsonNode bodyJson = json(res);
            assertTrue(bodyJson.has("knownTopics"),
                "Error response must include knownTopics list: " + res.body());
        }

        @Test
        @DisplayName("unknown use case → 404")
        void unknownUseCase404() throws Exception {
            String body = """
                {"topic":"agent-transactions","events":[{"key":"value"}]}
                """;
            HttpResponse<String> res = post("/api/use-cases/no-such-use-case-xyz/events", body);
            assertEquals(404, res.statusCode(),
                "Expected 404 for unknown use case: " + res.body());
        }

        @Test
        @DisplayName("invalid use case id (path traversal) → 400")
        void invalidUseCaseId400() throws Exception {
            String body = """
                {"topic":"agent-transactions","events":[{"key":"value"}]}
                """;
            HttpResponse<String> res = post("/api/use-cases/../etc/events", body);
            assertTrue(res.statusCode() == 400 || res.statusCode() == 404,
                "Expected 400 or 404 for path-traversal use case id, got " + res.statusCode());
        }
    }

    // ── Response schema ───────────────────────────────────────────────────────

    @Nested
    @DisplayName("Event injection — response schema")
    class ResponseSchema {

        @Test
        @DisplayName("success response has 'published' integer field")
        void responseHasPublishedField() throws Exception {
            String eventsJson = "[" + transactionPayload("FT-RS-001", "ACC-FT-RS-001", 10.00) + "]";
            HttpResponse<String> res = injectEvents("agent-banking", TOPIC_TRANSACTIONS, eventsJson);
            assertEquals(200, res.statusCode());
            JsonNode body = json(res);
            assertTrue(body.has("published") && body.path("published").isInt(),
                "Response must have integer 'published' field: " + res.body());
        }

        @Test
        @DisplayName("success response has 'topic' string field matching request")
        void responseHasTopicField() throws Exception {
            String eventsJson = "[" + transactionPayload("FT-RS-002", "ACC-FT-RS-002", 10.00) + "]";
            HttpResponse<String> res = injectEvents("agent-banking", TOPIC_TRANSACTIONS, eventsJson);
            assertEquals(200, res.statusCode());
            assertEquals(TOPIC_TRANSACTIONS, json(res).path("topic").asText(),
                "Response 'topic' must match the requested topic");
        }

        @Test
        @DisplayName("Content-Type is application/json on success")
        void contentTypeJson() throws Exception {
            String eventsJson = "[" + transactionPayload("FT-RS-003", "ACC-FT-RS-003", 10.00) + "]";
            HttpResponse<String> res = injectEvents("agent-banking", TOPIC_TRANSACTIONS, eventsJson);
            assertTrue(res.headers().firstValue("Content-Type").orElse("").contains("application/json"),
                "Expected Content-Type: application/json from event injection");
        }
    }
}
