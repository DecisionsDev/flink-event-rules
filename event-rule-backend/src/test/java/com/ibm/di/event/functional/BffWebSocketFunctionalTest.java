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
 * Functional tests for the BFF WebSocket endpoint ({@code ws://<bff>/ws}).
 *
 * <p>Protocol (see the WebSocket section of {@code event-rule-frontend/bff/server.js}):
 * <ul>
 *   <li>client → server {@code {"type":"subscribe","useCase":"<id>"}}</li>
 *   <li>server → client {@code {"type":"events","useCase":"<id>","data":{"<topic>":[events…]}}}:
 *       the current snapshot right after the subscription, then again each time a Kafka
 *       message is stored for that use case</li>
 *   <li>client → server {@code {"type":"ping"}}, server → client {@code {"type":"pong"}}</li>
 *   <li>malformed frames, unknown message types and unknown use cases get <b>no</b> reply
 *       (the frame is ignored); the connection stays usable</li>
 * </ul>
 *
 * <p>Event-streaming tests inject events through the BFF HTTP API and verify that the same
 * BFF pushes them over WebSocket, which exercises the complete path:
 * <pre>
 *   HTTP POST /events → Kafka producer → BFF Kafka consumer → WebSocket push
 * </pre>
 * They do not need a running Flink job.
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
 *   <li>WebSocket connection opens successfully on /ws</li>
 *   <li>ping → pong</li>
 *   <li>subscribe to banking-fraud-agent → 'events' snapshot with the use case's topics</li>
 *   <li>the snapshot topics match the REST descriptor (inbound and outbound)</li>
 *   <li>subscribe to retail-agent → 'events' snapshot</li>
 *   <li>invalid JSON / message without 'type' / unknown type / unknown use case → ignored,
 *       connection still answers a ping</li>
 *   <li>Push: injecting an event via POST triggers an 'events' message containing it</li>
 * </ul>
 */
@DisplayName("BFF WebSocket — functional tests")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class BffWebSocketFunctionalTest {

    /** WebSocket URL derived from the BFF_BASE_URL (http→ws, https→wss); the BFF serves it at /ws. */
    private static final String WS_URL =
        BFF_BASE_URL.replace("http://", "ws://").replace("https://", "wss://") + "/ws";

    private static final int WS_TIMEOUT_SECONDS = 10;
    private static final int SNAPSHOT_ATTEMPT_SECONDS = 10;
    private static final int SNAPSHOT_TOTAL_SECONDS = 90;
    private static final int PUSH_REINJECT_SECONDS = 8;
    private static final int PUSH_TOTAL_SECONDS = 60;

    @BeforeAll
    static void requireStack() throws Exception {
        assumeTrue(isEnabled(), disabledReason());
        assumeTrue(waitForBff(),
            "BFF is not reachable at " + BFF_BASE_URL
            + " — ensure the Docker Compose stack is running.");
        ensureUseCaseTopics("banking-fraud-agent", "retail-agent");
    }

    /**
     * Makes sure every topic of the given use cases exists and has a leader before the tests
     * subscribe. The E2E tests delete the topics of the agents they deploy; the BFF's live
     * consumer then fails with "This server does not host this topic-partition" for as long as
     * Kafka has not re-created them, which on a slow host outlasts the snapshot retry window.
     */
    private static void ensureUseCaseTopics(String... useCases) throws Exception {
        java.util.Set<String> names = new java.util.TreeSet<>();
        for (String useCase : useCases) {
            HttpResponse<String> res = get("/api/use-cases/" + useCase + "/descriptor");
            if (res.statusCode() != 200) continue;
            JsonNode descriptor = json(res);
            for (String group : new String[] {"topics", "outboundTopics"}) {
                for (JsonNode t : descriptor.path(group)) {
                    if (!t.path("name").asText().isBlank()) names.add(t.path("name").asText());
                }
            }
        }
        if (names.isEmpty()) return;

        java.util.Properties props = new java.util.Properties();
        props.put("bootstrap.servers", System.getProperty("KAFKA_BOOTSTRAP",
            System.getenv("KAFKA_BOOTSTRAP") != null ? System.getenv("KAFKA_BOOTSTRAP") : "localhost:9092"));
        props.put("request.timeout.ms", "10000");
        try (org.apache.kafka.clients.admin.AdminClient admin = org.apache.kafka.clients.admin.AdminClient.create(props)) {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            while (System.nanoTime() < deadline) {
                try {
                    java.util.Set<String> existing = admin.listTopics().names().get(10, TimeUnit.SECONDS);
                    java.util.List<org.apache.kafka.clients.admin.NewTopic> missing = new java.util.ArrayList<>();
                    for (String n : names) if (!existing.contains(n)) missing.add(new org.apache.kafka.clients.admin.NewTopic(n, 1, (short) 1));
                    if (!missing.isEmpty()) {
                        try { admin.createTopics(missing).all().get(10, TimeUnit.SECONDS); }
                        catch (ExecutionException e) { /* already exists / being deleted: re-checked below */ }
                    }
                    boolean allLed = true;
                    for (org.apache.kafka.clients.admin.TopicDescription d
                            : admin.describeTopics(names).allTopicNames().get(10, TimeUnit.SECONDS).values()) {
                        for (org.apache.kafka.common.TopicPartitionInfo p : d.partitions()) {
                            if (p.leader() == null) allLed = false;
                        }
                    }
                    if (allLed) return;
                } catch (ExecutionException | TimeoutException e) {
                    // topic not visible yet (deletion in progress): retry
                }
                Thread.sleep(1000);
            }
        }
        fail("Kafka topics " + names + " did not become available within 60s");
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

        /**
         * Sends a text message and waits until it has been written; throws if the send fails.
         * java.net.http.WebSocket forbids a new sendText while the previous one is outstanding
         * (the second future would fail silently), so consecutive sends must be serialised.
         */
        void send(String text) {
            ws.sendText(text, true).join();
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

        /** Like {@link #nextMessageOfType} but returns {@code null} instead of failing on timeout. */
        JsonNode pollMessageOfType(String expectedType, long timeoutSeconds) throws Exception {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
            while (true) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) return null;
                String raw = messages.poll(remaining, TimeUnit.NANOSECONDS);
                if (raw == null) return null;
                JsonNode node = MAPPER.readTree(raw);
                if (expectedType.equals(node.path("type").asText())) return node;
            }
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

    /**
     * Subscribes to a use case and returns its first 'events' snapshot.
     *
     * <p>The BFF starts the use case's Kafka consumer when the first client subscribes, and sends
     * the snapshot only once that start succeeds. Right after other suites deleted the use-case
     * topics (the E2E tests do), the first start can fail ("does not host this topic-partition")
     * and no snapshot is ever sent for that subscription; subscribing again retries it. So the
     * subscribe frame is re-sent until a snapshot for the use case arrives, up to
     * {@link #SNAPSHOT_TOTAL_SECONDS}.
     */
    private static JsonNode subscribeForSnapshot(TestWsClient ws, String useCase) throws Exception {
        String frame = "{\"type\":\"subscribe\",\"useCase\":\"" + useCase + "\"}";
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(SNAPSHOT_TOTAL_SECONDS);
        int attempts = 0;
        while (System.nanoTime() < deadline) {
            attempts++;
            ws.send(frame);
            JsonNode msg = ws.pollMessageOfType("events", SNAPSHOT_ATTEMPT_SECONDS);
            if (msg != null && useCase.equals(msg.path("useCase").asText())) return msg;
        }
        fail("No 'events' snapshot for '" + useCase + "' after " + attempts + " subscribe attempt(s) in "
            + SNAPSHOT_TOTAL_SECONDS + "s");
        return null; // unreachable
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

    // ── Keep-alive ────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("WebSocket keep-alive")
    class KeepAlive {

        @Test
        @DisplayName("ping → pong")
        void pingPong() throws Exception {
            TestWsClient ws = connect();
            try {
                ws.send("{\"type\":\"ping\"}");
                JsonNode msg = ws.nextMessageOfType("pong", WS_TIMEOUT_SECONDS);
                assertEquals("pong", msg.path("type").asText());
            } finally {
                ws.close();
            }
        }
    }

    // ── subscribe protocol ────────────────────────────────────────────────────

    @Nested
    @DisplayName("subscribe protocol")
    class Subscribe {

        @Test
        @DisplayName("banking-fraud-agent → 'events' snapshot received")
        void bankingSubscribeSnapshot() throws Exception {
            TestWsClient ws = connect();
            try {
                JsonNode msg = subscribeForSnapshot(ws, "banking-fraud-agent");
                assertEquals("banking-fraud-agent", msg.path("useCase").asText(),
                    "Snapshot must name the subscribed use case: " + msg);
            } finally {
                ws.close();
            }
        }

        @Test
        @DisplayName("snapshot 'data' is an object with one array per use-case topic")
        void snapshotHasArrayPerTopic() throws Exception {
            TestWsClient ws = connect();
            try {
                JsonNode msg = subscribeForSnapshot(ws, "banking-fraud-agent");

                JsonNode data = msg.path("data");
                assertTrue(data.isObject() && data.size() > 0,
                    "Expected a non-empty 'data' object in the snapshot: " + msg);
                data.fields().forEachRemaining(e -> assertTrue(e.getValue().isArray(),
                    "Topic '" + e.getKey() + "' must map to an array of events"));
            } finally {
                ws.close();
            }
        }

        @Test
        @DisplayName("snapshot topics include every inbound topic of the REST descriptor")
        void snapshotTopicsMatchRestDescriptor() throws Exception {
            TestWsClient ws = connect();
            try {
                JsonNode wsMsg = subscribeForSnapshot(ws, "banking-fraud-agent");

                JsonNode restDescriptor = json(get("/api/use-cases/banking-fraud-agent/descriptor"));
                JsonNode topics = restDescriptor.path("topics");
                assertTrue(topics.isArray() && topics.size() > 0, "REST descriptor must list topics");
                for (JsonNode t : topics) {
                    String name = t.path("name").asText();
                    assertTrue(wsMsg.path("data").has(name),
                        "Snapshot must contain topic '" + name + "' from the REST descriptor: " + wsMsg);
                }
            } finally {
                ws.close();
            }
        }

        @Test
        @DisplayName("retail-agent → 'events' snapshot received")
        void retailSubscribeSnapshot() throws Exception {
            TestWsClient ws = connect();
            try {
                JsonNode msg = subscribeForSnapshot(ws, "retail-agent");
                assertEquals("retail-agent", msg.path("useCase").asText(),
                    "Expected a snapshot for retail-agent: " + msg);
            } finally {
                ws.close();
            }
        }
    }

    // ── Malformed input ───────────────────────────────────────────────────────

    /**
     * The BFF replies to nothing it does not understand: the frame is dropped silently and the
     * connection keeps working. Each test sends the bad frame, then a ping, and expects the
     * very next message to be the pong (so no error / data message was produced in between).
     */
    @Nested
    @DisplayName("WebSocket malformed input is ignored")
    class MalformedInput {

        private void assertIgnored(String frame) throws Exception {
            TestWsClient ws = connect();
            try {
                ws.send(frame);
                ws.send("{\"type\":\"ping\"}");
                JsonNode msg = ws.nextMessage(WS_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                assertEquals("pong", msg.path("type").asText(),
                    "Frame '" + frame + "' must be ignored; first reply should be the pong: " + msg);
            } finally {
                ws.close();
            }
        }

        @Test
        @DisplayName("invalid JSON → ignored, connection still answers ping")
        void invalidJsonIgnored() throws Exception {
            assertIgnored("{not valid json");
        }

        @Test
        @DisplayName("valid JSON without 'type' → ignored")
        void missingTypeIgnored() throws Exception {
            assertIgnored("{\"foo\":\"bar\"}");
        }

        @Test
        @DisplayName("unknown message type (legacy 'selectUseCase') → ignored")
        void unknownTypeIgnored() throws Exception {
            assertIgnored("{\"type\":\"selectUseCase\",\"useCase\":\"banking-fraud-agent\"}");
        }

        @Test
        @DisplayName("subscribe to an unknown use case → no snapshot, connection still usable")
        void unknownUseCaseNoSnapshot() throws Exception {
            assertIgnored("{\"type\":\"subscribe\",\"useCase\":\"no-such-use-case-xyz\"}");
        }
    }

    // ── Event push ────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("Event push via WebSocket")
    class EventPush {

        /**
         * Verifies the complete event pipeline from HTTP injection through Kafka to the
         * WebSocket push. The BFF's shared producer publishes the event; its Kafka consumer
         * (started by the subscription) stores it and pushes the updated snapshot to every
         * subscribed client as an {@code {"type":"events"}} message.
         */
        @Test
        @DisplayName("injected transaction event is pushed in an 'events' message")
        void injectedEventPushed() throws Exception {
            TestWsClient ws = connect();
            try {
                // 1. Subscribe and consume the initial snapshot
                subscribeForSnapshot(ws, "banking-fraud-agent");

                // 2. Inject a uniquely identifiable transaction via the REST API. The snapshot is
                //    served from the history backfill, i.e. BEFORE the BFF's live Kafka consumer
                //    (which reads from the latest offset) has joined its group: an event injected
                //    in that window is never pushed. So a new transaction is injected every
                //    PUSH_REINJECT_SECONDS until a push containing one of them arrives.
                java.util.List<String> txIds = new java.util.ArrayList<>();
                JsonNode pushed = null;
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(PUSH_TOTAL_SECONDS);
                while (pushed == null && System.nanoTime() < deadline) {
                    String uniqueTxId = "WS-PUSH-" + UUID.randomUUID().toString().substring(0, 8);
                    txIds.add(uniqueTxId);
                    String body = String.format("""
                        {"events":[{"topic":"agent-transactions","payload":
                          {"transactionId":"%s","accountNumber":"ACC-WS-001",
                           "timestamp":"2026-06-01T10:00:00Z","type":"DEBIT",
                           "merchant":"WS Test Merchant","amount":55,
                           "country":"USA","currency":"DOLLAR"}}]}
                        """, uniqueTxId);
                    HttpResponse<String> injectRes = post("/api/use-cases/banking-fraud-agent/events", body);
                    assertEquals(200, injectRes.statusCode(),
                        "Event injection must succeed: " + injectRes.body());

                    // 3. Wait for a push that contains any transaction injected so far
                    long attemptEnd = System.nanoTime() + TimeUnit.SECONDS.toNanos(PUSH_REINJECT_SECONDS);
                    while (pushed == null) {
                        long remaining = attemptEnd - System.nanoTime();
                        if (remaining <= 0) break;
                        JsonNode msg = ws.pollMessageOfType("events",
                            Math.max(1, TimeUnit.NANOSECONDS.toSeconds(remaining)));
                        if (msg == null) break;
                        String txs = msg.path("data").path("agent-transactions").toString();
                        if (txIds.stream().anyMatch(txs::contains)) pushed = msg;
                    }
                }
                assertNotNull(pushed,
                    "No 'events' push containing any of the injected transactions " + txIds
                    + " within " + PUSH_TOTAL_SECONDS + "s");
                assertEquals("banking-fraud-agent", pushed.path("useCase").asText());
            } finally {
                ws.close();
            }
        }
    }
}
