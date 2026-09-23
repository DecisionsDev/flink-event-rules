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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.di.event.rule.broadcast.ArchiveControlCmd;
import com.ibm.di.event.rule.broadcast.DeploymentAuditLog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for {@link DeploymentApiServer}.
 *
 * <p>Starts a real {@link com.sun.net.httpserver.HttpServer} on an ephemeral port.
 * Uses a stub {@link KafkaCommandPublisher} (no live Kafka required).
 *
 * <p>Covered:
 * <ul>
 *   <li>POST /deployments         → 202 on valid LOAD request</li>
 *   <li>POST /deployments/rollback → 202 on valid ROLLBACK request</li>
 *   <li>POST /deployments/prefetch → 202 on valid PREFETCH request</li>
 *   <li>GET  /deployments/audit    → 200 with entries from the audit log</li>
 *   <li>GET  /health               → 200 with status UP</li>
 *   <li>400 on each field validation failure (version, url, sha256, pipeline)</li>
 *   <li>400 on malformed JSON body</li>
 *   <li>405 on wrong HTTP method</li>
 *   <li>404 on unknown path</li>
 *   <li>415 on missing Content-Type</li>
 *   <li>500 when the publisher throws</li>
 *   <li>202 response body fields: accepted, pipeline, version, commandType, requestId</li>
 *   <li>Server assigns a requestId when caller omits it</li>
 * </ul>
 */
@DisplayName("DeploymentApiServer Integration Tests")
class DeploymentApiServerTest {

    // ── Test infrastructure ───────────────────────────────────────────────────

    private static final String VALID_VERSION = "2025-07-v1";
    private static final String VALID_URL     = "s3://bucket/v1.jar";
    private static final String VALID_SHA256  = "b".repeat(64);
    private static final ObjectMapper MAPPER  = new ObjectMapper();

    /** Generic pipeline names used across all tests — no domain dependency. */
    private static final String DEFAULT_TEST_PIPELINE  = "test-agent";
    private static final String SECONDARY_TEST_PIPELINE = "secondary-agent";
    private static final Set<String> TEST_PIPELINES =
        Set.of(DEFAULT_TEST_PIPELINE, SECONDARY_TEST_PIPELINE);

    /** Stub publisher: records published commands; optionally throws. */
    static class StubPublisher extends KafkaCommandPublisher {

        final List<ArchiveControlCmd> published = new ArrayList<>();
        final List<String>            pipelines = new ArrayList<>();
        boolean shouldThrow = false;
        String  thrownMessage = "simulated Kafka failure";

        StubPublisher() {
            super("localhost:9092");
        }

        @Override
        public void publish(ArchiveControlCmd cmd, String pipeline) throws Exception {
            if (shouldThrow) throw new Exception(thrownMessage);
            published.add(cmd);
            pipelines.add(pipeline);
        }

        @Override
        public void close() { /* no-op */ }
    }

    private StubPublisher      stub;
    private DeploymentAuditLog auditLog;
    private DeploymentApiServer server;
    private int                 port;
    private HttpClient          client;

    @BeforeEach
    void startServer() throws Exception {
        stub     = new StubPublisher();
        auditLog = new DeploymentAuditLog();

        // Use a fixed port in the ephemeral range to avoid conflicts.
        // Increment by test run PID hash to reduce flakiness when run in parallel.
        port = 18080 + (int)(ProcessHandle.current().pid() % 1000);

        server = new DeploymentApiServer(
            port, stub, auditLog, MAPPER, TEST_PIPELINES, DEFAULT_TEST_PIPELINE);
        server.start();
        client = HttpClient.newHttpClient();
    }

    @AfterEach
    void stopServer() {
        server.stop();
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private String url(String path) {
        return "http://localhost:" + port + path;
    }

    private HttpResponse<String> post(String path, String body) throws Exception {
        return client.send(
            HttpRequest.newBuilder(URI.create(url(path)))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build(),
            HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> postNoContentType(String path, String body) throws Exception {
        return client.send(
            HttpRequest.newBuilder(URI.create(url(path)))
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build(),
            HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> get(String path) throws Exception {
        return client.send(
            HttpRequest.newBuilder(URI.create(url(path))).GET().build(),
            HttpResponse.BodyHandlers.ofString());
    }

    private String validBody() {
        // pipeline is omitted — server applies defaultPipeline (DEFAULT_TEST_PIPELINE)
        return """
            {"version":"%s","archiveUrl":"%s","sha256":"%s"}
            """.formatted(VALID_VERSION, VALID_URL, VALID_SHA256);
    }

    private JsonNode json(String body) throws Exception {
        return MAPPER.readTree(body);
    }

    // ── 202 Accepted ──────────────────────────────────────────────────────────

    @Nested
    @DisplayName("202 Accepted — successful commands")
    class Accepted {

        @Test
        @DisplayName("POST /deployments → 202 LOAD, command published")
        void postDeployments202() throws Exception {
            HttpResponse<String> res = post("/deployments", validBody());
            assertEquals(202, res.statusCode());
            JsonNode body = json(res.body());
            assertTrue(body.path("accepted").asBoolean());
            assertEquals("LOAD",                  body.path("commandType").asText());
            assertEquals(DEFAULT_TEST_PIPELINE,    body.path("pipeline").asText());
            assertEquals(VALID_VERSION,            body.path("version").asText());
            assertEquals(1, stub.published.size());
            assertEquals(ArchiveControlCmd.CommandType.LOAD, stub.published.get(0).getCommandType());
        }

        @Test
        @DisplayName("POST /deployments/rollback → 202 ROLLBACK")
        void postRollback202() throws Exception {
            HttpResponse<String> res = post("/deployments/rollback", validBody());
            assertEquals(202, res.statusCode());
            assertEquals("ROLLBACK", json(res.body()).path("commandType").asText());
            assertEquals(ArchiveControlCmd.CommandType.ROLLBACK, stub.published.get(0).getCommandType());
        }

        @Test
        @DisplayName("POST /deployments/prefetch → 202 PREFETCH")
        void postPrefetch202() throws Exception {
            HttpResponse<String> res = post("/deployments/prefetch", validBody());
            assertEquals(202, res.statusCode());
            assertEquals("PREFETCH", json(res.body()).path("commandType").asText());
            assertEquals(ArchiveControlCmd.CommandType.PREFETCH, stub.published.get(0).getCommandType());
        }

        @Test
        @DisplayName("pipeline field is passed to publisher")
        void pipelinePassedToPublisher() throws Exception {
            String body = """
                {"version":"%s","archiveUrl":"%s","sha256":"%s","pipeline":"%s"}
                """.formatted(VALID_VERSION, VALID_URL, VALID_SHA256, SECONDARY_TEST_PIPELINE);
            post("/deployments", body);
            assertEquals(SECONDARY_TEST_PIPELINE, stub.pipelines.get(0));
        }

        @Test
        @DisplayName("requestedBy is forwarded to the ArchiveControlCmd")
        void requestedByForwarded() throws Exception {
            String body = """
                {"version":"%s","archiveUrl":"%s","sha256":"%s","requestedBy":"alice"}
                """.formatted(VALID_VERSION, VALID_URL, VALID_SHA256);
            post("/deployments", body);
            assertEquals("alice", stub.published.get(0).getRequestedBy());
        }

        @Test
        @DisplayName("Server assigns a requestId when caller omits it")
        void serverAssignsRequestId() throws Exception {
            HttpResponse<String> res = post("/deployments", validBody());
            String requestId = json(res.body()).path("requestId").asText();
            assertNotNull(requestId);
            assertFalse(requestId.isBlank(), "requestId must be non-blank when auto-generated");
        }

        @Test
        @DisplayName("Caller-supplied requestId is preserved in 202 body")
        void callerRequestIdPreserved() throws Exception {
            String body = """
                {"version":"%s","archiveUrl":"%s","sha256":"%s","requestId":"my-req-99"}
                """.formatted(VALID_VERSION, VALID_URL, VALID_SHA256);
            HttpResponse<String> res = post("/deployments", body);
            assertEquals("my-req-99", json(res.body()).path("requestId").asText());
        }

        @Test
        @DisplayName("Response Content-Type is application/json")
        void contentTypeIsJson() throws Exception {
            HttpResponse<String> res = post("/deployments", validBody());
            assertTrue(res.headers().firstValue("Content-Type").orElse("")
                .contains("application/json"));
        }
    }

    // ── 400 Bad Request ───────────────────────────────────────────────────────

    @Nested
    @DisplayName("400 Bad Request — validation failures")
    class BadRequest {

        @Test
        @DisplayName("Missing version → 400 MISSING_VERSION")
        void missingVersion() throws Exception {
            String body = """
                {"archiveUrl":"%s","sha256":"%s"}
                """.formatted(VALID_URL, VALID_SHA256);
            assertError(400, "MISSING_VERSION", post("/deployments", body));
        }

        @Test
        @DisplayName("Missing archiveUrl → 400 MISSING_ARCHIVE_URL")
        void missingArchiveUrl() throws Exception {
            String body = """
                {"version":"%s","sha256":"%s"}
                """.formatted(VALID_VERSION, VALID_SHA256);
            assertError(400, "MISSING_ARCHIVE_URL", post("/deployments", body));
        }

        @Test
        @DisplayName("Missing sha256 → 400 MISSING_SHA256")
        void missingSha256() throws Exception {
            String body = """
                {"version":"%s","archiveUrl":"%s"}
                """.formatted(VALID_VERSION, VALID_URL);
            assertError(400, "MISSING_SHA256", post("/deployments", body));
        }

        @Test
        @DisplayName("Invalid sha256 (wrong length) → 400 INVALID_SHA256")
        void invalidSha256() throws Exception {
            String body = """
                {"version":"%s","archiveUrl":"%s","sha256":"tooshort"}
                """.formatted(VALID_VERSION, VALID_URL);
            assertError(400, "INVALID_SHA256", post("/deployments", body));
        }

        @Test
        @DisplayName("Unknown pipeline → 400 INVALID_PIPELINE")
        void unknownPipeline() throws Exception {
            String body = """
                {"version":"%s","archiveUrl":"%s","sha256":"%s","pipeline":"unknown"}
                """.formatted(VALID_VERSION, VALID_URL, VALID_SHA256);
            assertError(400, "INVALID_PIPELINE", post("/deployments", body));
        }

        @Test
        @DisplayName("Malformed JSON → 400 MALFORMED_JSON")
        void malformedJson() throws Exception {
            assertError(400, "MALFORMED_JSON", post("/deployments", "{not valid json}"));
        }

        @Test
        @DisplayName("No published commands on 400")
        void noPublishOnValidationError() throws Exception {
            String body = """
                {"archiveUrl":"%s","sha256":"%s"}
                """.formatted(VALID_URL, VALID_SHA256);
            post("/deployments", body);
            assertTrue(stub.published.isEmpty(), "No commands must be published on validation error");
        }
    }

    // ── 415 Unsupported Media Type ────────────────────────────────────────────

    @Test
    @DisplayName("POST without Content-Type: application/json → 415")
    void missingContentType() throws Exception {
        HttpResponse<String> res = postNoContentType("/deployments", validBody());
        assertError(415, "UNSUPPORTED_MEDIA_TYPE", res);
    }

    // ── 405 Method Not Allowed ────────────────────────────────────────────────

    @Test
    @DisplayName("GET /deployments → 405")
    void getDeployments405() throws Exception {
        assertError(405, "METHOD_NOT_ALLOWED", get("/deployments"));
    }

    @Test
    @DisplayName("GET /deployments/rollback → 405")
    void getRollback405() throws Exception {
        assertError(405, "METHOD_NOT_ALLOWED", get("/deployments/rollback"));
    }

    // ── 404 Not Found ─────────────────────────────────────────────────────────

    @Test
    @DisplayName("Unknown path → 404")
    void unknownPath404() throws Exception {
        HttpResponse<String> res = get("/unknown-path");
        assertEquals(404, res.statusCode());
    }

    // ── 500 Internal Server Error ─────────────────────────────────────────────

    @Test
    @DisplayName("Publisher exception → 500 INTERNAL_ERROR")
    void publisherException500() throws Exception {
        stub.shouldThrow    = true;
        stub.thrownMessage  = "broker unavailable";
        HttpResponse<String> res = post("/deployments", validBody());
        assertError(500, "INTERNAL_ERROR", res);
        assertTrue(json(res.body()).path("message").asText().contains("broker unavailable"));
    }

    // ── GET /deployments/audit ────────────────────────────────────────────────

    @Nested
    @DisplayName("GET /deployments/audit")
    class AuditEndpoint {

        @Test
        @DisplayName("Empty log → 200 with total=0 and empty entries array")
        void emptyAuditLog() throws Exception {
            HttpResponse<String> res = get("/deployments/audit");
            assertEquals(200, res.statusCode());
            JsonNode body = json(res.body());
            assertEquals(0, body.path("total").asInt());
            assertTrue(body.path("entries").isArray());
            assertEquals(0, body.path("entries").size());
        }

        @Test
        @DisplayName("Log with entries → 200 with correct total and entry fields")
        void auditLogWithEntries() throws Exception {
            // Seed the audit log with one entry
            ArchiveControlCmd cmd = new ArchiveControlCmd("v2", "s3://b/v2.jar", VALID_SHA256, "req-1", "ci");
            auditLog.record(cmd, "v1", 0);

            HttpResponse<String> res = get("/deployments/audit");
            assertEquals(200, res.statusCode());
            JsonNode body  = json(res.body());
            assertEquals(1, body.path("total").asInt());
            JsonNode entry = body.path("entries").get(0);
            assertEquals("ci",   entry.path("who").asText());
            assertEquals("v1",   entry.path("fromVersion").asText());
            assertEquals("v2",   entry.path("toVersion").asText());
            assertEquals("LOAD", entry.path("commandType").asText());
            assertEquals("req-1",entry.path("requestId").asText());
            assertFalse(entry.path("when").asText().isBlank());
        }

        @Test
        @DisplayName("POST /deployments/audit → 405")
        void postAudit405() throws Exception {
            HttpResponse<String> res = post("/deployments/audit",
                "{\"ignored\":true}");
            assertEquals(405, res.statusCode());
        }
    }

    // ── GET /health ───────────────────────────────────────────────────────────

    @Nested
    @DisplayName("GET /health")
    class HealthEndpoint {

        @Test
        @DisplayName("/health → 200 with status UP")
        void healthUp() throws Exception {
            HttpResponse<String> res = get("/health");
            assertEquals(200, res.statusCode());
            assertEquals("UP", json(res.body()).path("status").asText());
        }

        @Test
        @DisplayName("/health response has a timestamp field")
        void healthTimestamp() throws Exception {
            HttpResponse<String> res = get("/health");
            assertFalse(json(res.body()).path("timestamp").asText().isBlank());
        }

        @Test
        @DisplayName("POST /health → 405")
        void postHealth405() throws Exception {
            HttpResponse<String> res = post("/health", "{}");
            assertEquals(405, res.statusCode());
        }
    }

    // ── Assertion helpers ─────────────────────────────────────────────────────

    private static void assertError(int expectedStatus, String expectedCode,
                                    HttpResponse<String> res) throws Exception {
        assertEquals(expectedStatus, res.statusCode());
        JsonNode body = MAPPER.readTree(res.body());
        assertEquals(expectedStatus, body.path("status").asInt());
        assertEquals(expectedCode,   body.path("code").asText(),
            "Expected error code " + expectedCode + " but got: " + res.body());
        assertFalse(body.path("message").asText().isBlank(),
            "Error response must contain a non-blank message");
    }
}

