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

import java.net.URI;
import java.net.http.HttpResponse;
import java.net.http.WebSocket;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.ibm.di.event.functional.FunctionalTestEnvironment.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Functional tests for the BFF WebSocket endpoint.
 *
 * <p>Tests validate the WebSocket handshake, the {@code selectUseCase} message
 * protocol, and the initial data delivery format.  Event-streaming tests
 * (waiting for Kafka messages to propagate) are marked with a generous timeout
 * but do not require a full Flink pipeline — they inject events via the BFF
 * HTTP API and verify that the same BFF broadcasts them over WebSocket, which
 * exercises the complete path:
 * <pre>
 *   HTTP POST /events → Kafka producer → BFF Kafka consumer → WebSocket broadcast
 * </pre>
 *
 * <p>Tests are <b>skipped</b> unless {@code FUNCTIONAL_TESTS_ENABLED=true} is set.
 * Run with:
 * <pre>
 *   FUNCTIONAL_TESTS_ENABLED=true mvn test -pl event-rule-backend \
 *     --settings settings.xml -Dtest=BffWebSocketFunctionalTest
 * </pre>
 *
 * <p>Scenarios covered:
 * <ul>
 *   <li>WebSocket connection opens successfully</li>
 *   <li>selectUseCase for agent-banking → 'initial' message received</li>
 *   <li>'initial' message has 'data' and 'descriptor' fields</li>
 *   <li>'initial' descriptor matches GET /api/use-cases/:id/descriptor</li>
 *   <li>selectUseCase for agent-retail → 'initial' message received</li>
 *   <li>Sending invalid JSON → 'error' message received</li>
 *   <li>Sending valid JSON without 'type' field → 'error' message received</li>
 *   <li>Selecting unknown use case → 'error' message with descriptive message</li>
 *   <li>Event broadcast: injecting event via POST triggers 'event' WebSocket message</li>
 *   <li>'event' message has topicType and data fields</li>
 * </ul>
 */
@DisplayName("BFF WebSocket — functional tests")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class BffWebSocketFunctionalTest {

    /** WebSocket URL derived from the BFF_BASE_URL by replacing http with ws. */
    private static final String WS_URL =
        BFF_BASE_URL.replace("http://", "ws://").replace("https://", "wss://");

    private static final int WS_TIMEOUT_SECONDS = 10;

    @BeforeAll
    static void requireStack() {
        assumeTrue(isEnabled(), disabledReason());
        assumeTrue(waitForBff(),
            "BFF is not reachable at " + BFF_BASE_URL
            + " — ensure the Docker Compose stack is running.");
    }

    // ── WebSocket helper ──────────────────────────────────────────────────────

    /**
     * Lightweight synchronous WebSocket test client that buffers received text
     * frames and exposes a {@link #nextMessage(long, TimeUnit)} method.
     */
    static class TestWsClient implements WebSocket.Listener {

        private final BlockingQueue<String> messages = new LinkedBlockingQueue<>();
        private final CompletableFuture<Void> openFuture = new CompletableFuture<>();
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private WebSocket ws;
        private final StringBuilder fragBuf = new StringBuilder();

        @Override
        public void onOpen(WebSocket webSocket) {
            this.ws = webSocket;
            openFuture.complete(null);
            webSocket.request(Long.MAX_VALUE);
        }

        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            fragBuf.append(data);
            if (last) {
                messages.add(fragBuf.toString());
                fragBuf.setLength(0);
            }
            webSocket.request(1);
            return null;
        }

        @Override
        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
            closed.set(true);
            return null;
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
            closed.set(true);
        }

        /** Sends a text message; throws if send fails. */
        void send(String text) {
            ws.sendText(text, true);
        }

        /** Waits for and returns the next message, parsing it as JSON. */
        JsonNode nextMessage(long timeout, TimeUnit unit) throws Exception {
            String raw = messages.poll(timeout, unit);
            assertNotNull(raw,
                "Expected a WebSocket message within " + timeout + " " + unit
                + " but none arrived");
            return MAPPER.readTree(raw);
        }

        /** Waits for the next message whose {@code type} field matches {@code expectedType}. */
        JsonNode nextMessageOfType(String expectedType, long timeoutSeconds) throws Exception {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
            while (System.nanoTime() < deadline) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) break;
                String raw = messages.poll(remaining, TimeUnit.NANOSECONDS);
                if (raw == null) break;
                JsonNode node = MAPPER.readTree(raw);
                if (expectedType.equals(node.path("type").asText())) return node;
                // Put back non-matching messages for later — re-queue them
                // (BlockingQueue doesn't support requeue; skip and retry)
            }
            fail("Expected WebSocket message of type '" + expectedType
                + "' but none arrived within " + timeoutSeconds + "s");
            return null; // unreachable
        }

        void close() {
            try {
                if (ws != null && !closed.get()) ws.sendClose(WebSocket.NORMAL_CLOSURE, "test done");
            } catch (Exception ignored) { /* best-effort */ }
        }

        boolean waitForOpen(long timeoutSeconds) {
            try {
                openFuture.get(timeoutSeconds, TimeUnit.SECONDS);
                return true;
            } catch (Exception e) {
                return false;
            }
        }
    }

    /**
     * Opens a new WebSocket connection to the BFF and waits for the open event.
     * Callers must call {@link TestWsClient#close()} in a finally block.
     */
    private static TestWsClient connect() throws Exception {
        TestWsClient listener = new TestWsClient();
        HTTP.newWebSocketBuilder()
            .buildAsync(URI.create(WS_URL), listener)
            .get(WS_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertTrue(listener.waitForOpen(WS_TIMEOUT_SECONDS),
            "WebSocket did not open within " + WS_TIMEOUT_SECONDS + "s at " + WS_URL);
        return listener;
    }

    // ── Connection tests ──────────────────────────────────────────────────────

    @Nested
    @DisplayName("WebSocket connection")
    class Connection {

        @Test
        @DisplayName("connection opens successfully")
        void connectionOpens() throws Exception {
            TestWsClient ws = connect();
            try {
                assertFalse(ws.closed.get(), "WebSocket must be open after connect()");
            } finally {
                ws.close();
            }
        }
    }

    // ── selectUseCase protocol ─────────────────────────────────────────────────

    @Nested
    @DisplayName("selectUseCase protocol")
    class SelectUseCase {

        @Test
        @DisplayName("agent-banking → 'initial' message received")
        void bankingSelectUseCaseInitial() throws Exception {
            TestWsClient ws = connect();
            try {
                ws.send("{\"type\":\"selectUseCase\",\"useCase\":\"agent-banking\"}");
                // nextMessageOfType skips any stale 'event' frames from prior runs
                JsonNode msg = ws.nextMessageOfType("initial", WS_TIMEOUT_SECONDS);
                assertEquals("initial", msg.path("type").asText(),
                    "Expected 'initial' message type after selectUseCase: " + msg);
            } finally {
                ws.close();
            }
        }

        @Test
        @DisplayName("'initial' message contains data object and descriptor object")
        void initialMessageHasDataAndDescriptor() throws Exception {
            TestWsClient ws = connect();
            try {
                ws.send("{\"type\":\"selectUseCase\",\"useCase\":\"agent-banking\"}");
                JsonNode msg = ws.nextMessageOfType("initial", WS_TIMEOUT_SECONDS);

                assertTrue(msg.has("data") && msg.path("data").isObject(),
                    "Expected 'data' object in initial message: " + msg);
                assertTrue(msg.has("descriptor") && msg.path("descriptor").isObject(),
                    "Expected 'descriptor' object in initial message: " + msg);
            } finally {
                ws.close();
            }
        }

        @Test
        @DisplayName("'initial' descriptor name matches REST /descriptor endpoint")
        void initialDescriptorMatchesRest() throws Exception {
            TestWsClient ws = connect();
            try {
                ws.send("{\"type\":\"selectUseCase\",\"useCase\":\"agent-banking\"}");
                JsonNode wsMsg = ws.nextMessageOfType("initial", WS_TIMEOUT_SECONDS);

                JsonNode restDescriptor = json(get("/api/use-cases/agent-banking/descriptor"));
                assertEquals(
                    restDescriptor.path("name").asText(),
                    wsMsg.path("descriptor").path("name").asText(),
                    "Descriptor name from WebSocket must match REST descriptor");
            } finally {
                ws.close();
            }
        }

        @Test
        @DisplayName("agent-retail → 'initial' message received")
        void retailSelectUseCaseInitial() throws Exception {
            TestWsClient ws = connect();
            try {
                ws.send("{\"type\":\"selectUseCase\",\"useCase\":\"agent-retail\"}");
                JsonNode msg = ws.nextMessageOfType("initial", WS_TIMEOUT_SECONDS);
                assertEquals("initial", msg.path("type").asText(),
                    "Expected 'initial' for agent-retail: " + msg);
            } finally {
                ws.close();
            }
        }

        @Test
        @DisplayName("unknown use case → 'error' message with descriptive text")
        void unknownUseCaseError() throws Exception {
            TestWsClient ws = connect();
            try {
                ws.send("{\"type\":\"selectUseCase\",\"useCase\":\"no-such-use-case-xyz\"}");
                JsonNode msg = ws.nextMessage(WS_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                assertEquals("error", msg.path("type").asText(),
                    "Expected 'error' for unknown use case: " + msg);
                assertFalse(msg.path("message").asText().isBlank(),
                    "Error message must not be blank");
            } finally {
                ws.close();
            }
        }
    }

    // ── Error handling protocol ───────────────────────────────────────────────

    @Nested
    @DisplayName("WebSocket error handling")
    class ErrorHandling {

        @Test
        @DisplayName("sending invalid JSON → 'error' message received")
        void invalidJsonError() throws Exception {
            TestWsClient ws = connect();
            try {
                ws.send("{not valid json");
                JsonNode msg = ws.nextMessage(WS_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                assertEquals("error", msg.path("type").asText(),
                    "Expected 'error' for invalid JSON: " + msg);
            } finally {
                ws.close();
            }
        }

        @Test
        @DisplayName("valid JSON without 'type' field → 'error' message")
        void missingTypeFieldError() throws Exception {
            TestWsClient ws = connect();
            try {
                ws.send("{\"foo\":\"bar\"}");
                JsonNode msg = ws.nextMessage(WS_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                assertEquals("error", msg.path("type").asText(),
                    "Expected 'error' for message without type: " + msg);
            } finally {
                ws.close();
            }
        }
    }

    // ── Event broadcast ───────────────────────────────────────────────────────

    @Nested
    @DisplayName("Event broadcast via WebSocket")
    class EventBroadcast {

        /**
         * Verifies the complete event pipeline from HTTP injection through
         * Kafka to WebSocket broadcast.
         *
         * <p>The BFF's shared Kafka producer publishes the event; the BFF's
         * Kafka consumer (started when the first WebSocket client selects the
         * use case) receives it and broadcasts it to all subscribed WebSocket
         * clients as a {@code {"type":"event"}} message.
         */
        @Test
        @DisplayName("injected transaction event is broadcast as 'event' message")
        void injectedEventBroadcast() throws Exception {
            TestWsClient ws = connect();
            try {
                // 1. Subscribe to agent-banking use case
                ws.send("{\"type\":\"selectUseCase\",\"useCase\":\"agent-banking\"}");
                // Use nextMessageOfType to skip any stale 'event' frames already
                // buffered from prior test runs before the 'initial' snapshot arrives.
                JsonNode initMsg = ws.nextMessageOfType("initial", WS_TIMEOUT_SECONDS);

                // 2. Inject a uniquely identifiable transaction via REST API
                String uniqueTxId = "WS-BCAST-" + UUID.randomUUID().toString().substring(0, 8);
                String eventsJson = String.format("""
                    [{"transactionId":"%s","accountId":"ACC-WS-001",
                      "timestamp":"2026-06-01T10:00:00Z","type":"DEBIT",
                      "merchant":"WS Test Merchant","amount":"55.00",
                      "country":"USA","cardHolderCountry":"USA"}]
                    """, uniqueTxId);
                String body = "{\"topic\":\"agent-transactions\",\"events\":" + eventsJson + "}";
                HttpResponse<String> injectRes = post("/api/use-cases/agent-banking/events", body);
                assertEquals(200, injectRes.statusCode(),
                    "Event injection must succeed: " + injectRes.body());

                // 3. Wait for the broadcasted 'event' message (up to 15s for Kafka round-trip)
                JsonNode eventMsg = ws.nextMessageOfType("event", 15);

                // 4. Validate the broadcast message structure
                assertEquals("event", eventMsg.path("type").asText());
                assertFalse(eventMsg.path("topicType").asText().isBlank(),
                    "Broadcast message must have 'topicType' field: " + eventMsg);
                assertTrue(eventMsg.has("data") && eventMsg.path("data").isObject(),
                    "Broadcast message must have 'data' object: " + eventMsg);

            } finally {
                ws.close();
            }
        }

        @Test
        @DisplayName("'event' message topicType matches descriptor topic type")
        void eventMessageTopicTypeValid() throws Exception {
            TestWsClient ws = connect();
            try {
                ws.send("{\"type\":\"selectUseCase\",\"useCase\":\"agent-banking\"}");
                ws.nextMessageOfType("initial", WS_TIMEOUT_SECONDS); // consume initial, skip stale events

                // Inject one event
                String body = """
                    {"topic":"agent-transactions","events":[
                      {"transactionId":"WS-TT-001","accountId":"ACC-TT-001",
                       "timestamp":"2026-06-01T10:00:00Z","type":"DEBIT",
                       "merchant":"TT Merchant","amount":"10.00",
                       "country":"USA","cardHolderCountry":"USA"}
                    ]}
                    """;
                HttpResponse<String> injectRes = post("/api/use-cases/agent-banking/events", body);
                assumeTrue(200 == injectRes.statusCode(),
                    "Event injection must succeed to test broadcast, got: " + injectRes.statusCode());

                JsonNode eventMsg = ws.nextMessageOfType("event", 15);

                // topicType for agent-transactions is "transaction" per agents.json
                assertEquals("transaction", eventMsg.path("topicType").asText(),
                    "topicType for agent-transactions events must be 'transaction': " + eventMsg);

            } finally {
                ws.close();
            }
        }
    }
}
