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
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.InetAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Locale;

/**
 * Shared environment utilities for functional tests that run against a live
 * Docker Compose stack.
 *
 * <h2>Opt-in activation</h2>
 * Functional tests are skipped by default so that {@code mvn test} continues to
 * be fast and require no external infrastructure.  To enable them, set:
 * <pre>
 *   export FUNCTIONAL_TESTS_ENABLED=true
 *   # or
 *   mvn test -DFUNCTIONAL_TESTS_ENABLED=true
 * </pre>
 *
 * <h2>Pre-requisites</h2>
 * The Docker Compose infrastructure stack must be running before these tests
 * are executed.  Run the base stack (no use-case profiles required):
 * <pre>
 *   docker compose up -d zookeeper kafka bff
 *   # or the full stack:
 *   ./run.sh up
 * </pre>
 *
 * <h2>Endpoints exercised</h2>
 * <ul>
 *   <li>BFF REST API — http://localhost:8080 (resolved to 127.0.0.1 to avoid IPv6)</li>
 *   <li>Flink REST API — http://localhost:8081 (optional — tested only when available)</li>
 * </ul>
 *
 * <h2>IPv4 resolution</h2>
 * On macOS with Docker Desktop the JVM may resolve {@code localhost} to {@code ::1}
 * (IPv6) while Docker only binds ports on {@code 127.0.0.1} (IPv4).  To prevent the
 * 30-second probe from timing out silently, any URL whose host is literally
 * {@code "localhost"} is rewritten to {@code "http://127.0.0.1:…"} before the HTTP
 * client is built.  Override with {@code -DBFF_URL=http://127.0.0.1:8080} if you
 * need a different address.
 */
public final class FunctionalTestEnvironment {

    /** System property / env var that enables the functional test suite. */
    private static final String ENABLED_PROP = "FUNCTIONAL_TESTS_ENABLED";

    /**
     * BFF base URL used by all HTTP helpers.
     *
     * <p>Resolution order: {@code -DBFF_URL} system property →
     * {@code BFF_URL} environment variable → {@code http://127.0.0.1:8080}.
     *
     * <p>When the resolved URL contains the literal hostname {@code localhost} it is
     * rewritten to {@code 127.0.0.1} so that the JVM's HTTP client connects over IPv4
     * rather than resolving to {@code ::1} (IPv6).  Docker Desktop on macOS only
     * binds ports on the IPv4 loopback address.
     */
    public static final String BFF_BASE_URL = resolveLocalhost(
        System.getProperty("BFF_URL", System.getenv("BFF_URL") != null
            ? System.getenv("BFF_URL") : "http://localhost:8080"));

    /**
     * Flink REST API base URL (optional — tests that use it check availability first).
     * Receives the same {@code localhost} → {@code 127.0.0.1} rewrite as {@link #BFF_BASE_URL}.
     */
    public static final String FLINK_BASE_URL = resolveLocalhost(
        System.getProperty("FLINK_URL", System.getenv("FLINK_URL") != null
            ? System.getenv("FLINK_URL") : "http://localhost:8081"));

    /** Maximum time to wait for BFF to become reachable (used by readiness probe). */
    private static final Duration READINESS_TIMEOUT = Duration.ofSeconds(30);

    public static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Shared HTTP client for all functional tests.
     *
     * <p>Built with {@link HttpClient.Version#HTTP_1_1} to avoid the h2c upgrade
     * handshake that some embedded HTTP servers do not support, and to ensure the
     * client reuses existing TCP connections rather than opening a new one per request.
     */
    public static final HttpClient HTTP = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(10))
        .version(HttpClient.Version.HTTP_1_1)
        .build();

    private FunctionalTestEnvironment() {}

    // ── URL normalisation ─────────────────────────────────────────────────────

    /**
     * Replaces the literal hostname {@code localhost} with {@code 127.0.0.1} in the
     * supplied URL string, leaving all other parts unchanged.
     *
     * <p>This is necessary on macOS + IBM Semeru (OpenJ9) where
     * {@link InetAddress#getAllByName(String) InetAddress.getAllByName("localhost")}
     * returns the IPv6 loopback {@code ::1} before the IPv4 {@code 127.0.0.1}, but
     * Docker Desktop only exposes container ports on the IPv4 loopback interface.
     *
     * @param url a base URL such as {@code http://localhost:8080}; must not be null
     * @return the same URL with {@code localhost} replaced by {@code 127.0.0.1},
     *         or the original string if it does not contain {@code localhost}
     */
    static String resolveLocalhost(String url) {
        if (url == null) return url;
        // Replace exactly the host token "localhost" (with optional port) only.
        // This regex matches the literal string "localhost" preceded by "://" and
        // followed by "/" or ":" or end-of-string — it does not alter hostnames
        // that merely contain "localhost" as a substring (e.g. "my-localhost-vm").
        return url.replaceAll("(?<=[:/]{2})localhost(?=[:/]|$)", "127.0.0.1");
    }

    // ── Activation guard ──────────────────────────────────────────────────────

    /**
     * Returns {@code true} when the functional test suite has been explicitly
     * enabled via system property or environment variable.
     */
    public static boolean isEnabled() {
        String prop = System.getProperty(ENABLED_PROP);
        if (prop != null) return "true".equalsIgnoreCase(prop.trim());
        String env = System.getenv(ENABLED_PROP);
        return env != null && "true".equalsIgnoreCase(env.trim());
    }

    /**
     * Returns a human-readable reason string suitable for use in
     * {@code @DisabledIf} / assumption failures.
     */
    public static String disabledReason() {
        return "Functional tests are disabled. Set FUNCTIONAL_TESTS_ENABLED=true and ensure "
            + "the Docker Compose stack is running (docker compose up -d zookeeper kafka bff).";
    }

    // ── HTTP helpers ──────────────────────────────────────────────────────────

    /** Issues a GET and returns the raw response. */
    public static HttpResponse<String> get(String path) throws Exception {
        return HTTP.send(
            HttpRequest.newBuilder(URI.create(BFF_BASE_URL + path))
                .GET()
                .timeout(Duration.ofSeconds(15))
                .build(),
            HttpResponse.BodyHandlers.ofString());
    }

    /** Issues a POST with {@code application/json} body and returns the raw response. */
    public static HttpResponse<String> post(String path, String jsonBody) throws Exception {
        return HTTP.send(
            HttpRequest.newBuilder(URI.create(BFF_BASE_URL + path))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                .timeout(Duration.ofSeconds(20))
                .build(),
            HttpResponse.BodyHandlers.ofString());
    }

    /** Issues a PUT with {@code application/json} body and returns the raw response. */
    public static HttpResponse<String> put(String path, String jsonBody) throws Exception {
        return HTTP.send(
            HttpRequest.newBuilder(URI.create(BFF_BASE_URL + path))
                .header("Content-Type", "application/json")
                .PUT(HttpRequest.BodyPublishers.ofString(jsonBody))
                .timeout(Duration.ofSeconds(15))
                .build(),
            HttpResponse.BodyHandlers.ofString());
    }

    /**
     * Issues a DELETE and returns the raw response.
     *
     * <p>The timeout is set to 60 seconds because {@code DELETE /api/agents/:id}
     * triggers a Kafka admin operation (topic deletion) that can take ~25–30 s on
     * a local Docker Compose stack.
     */
    public static HttpResponse<String> delete(String path) throws Exception {
        return HTTP.send(
            HttpRequest.newBuilder(URI.create(BFF_BASE_URL + path))
                .DELETE()
                .timeout(Duration.ofSeconds(60))
                .build(),
            HttpResponse.BodyHandlers.ofString());
    }

    /** Parses the response body as a {@link JsonNode}. */
    public static JsonNode json(HttpResponse<String> response) throws Exception {
        return MAPPER.readTree(response.body());
    }

    /** Parses a JSON string as a {@link JsonNode}. */
    public static JsonNode json(String raw) throws Exception {
        return MAPPER.readTree(raw);
    }

    // ── Readiness probe ───────────────────────────────────────────────────────

    /**
     * Polls {@code GET /health} until the BFF responds with HTTP 200, or until
     * {@link #READINESS_TIMEOUT} is exceeded.
     *
     * @return {@code true} when BFF is reachable, {@code false} on timeout.
     */
    public static boolean waitForBff() {
        long deadline = System.currentTimeMillis() + READINESS_TIMEOUT.toMillis();
        while (System.currentTimeMillis() < deadline) {
            try {
                HttpResponse<String> res = get("/health");
                if (res.statusCode() == 200) return true;
            } catch (Exception ignored) { /* not yet ready */ }
            try { Thread.sleep(500); } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return false;
    }

    /**
     * Polls {@code GET /api/flink/status} until Flink reports at least one running
     * job named {@code expectedJobName}, or until {@code timeoutMs} elapses.
     */
    public static boolean waitForFlinkJob(String expectedJobName, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            try {
                HttpResponse<String> res = get("/api/flink/status");
                if (res.statusCode() == 200) {
                    JsonNode body = json(res);
                    JsonNode jobs = body.path("jobs");
                    if (jobs.isArray()) {
                        for (JsonNode j : jobs) {
                            if (expectedJobName.equals(j.asText())) return true;
                        }
                    }
                }
            } catch (Exception ignored) { /* not yet ready */ }
            try { Thread.sleep(2000); } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return false;
    }

    /**
     * Polls the Flink REST API directly until the job with {@code jobId} reaches a
     * terminal state ({@code CANCELED}, {@code FINISHED}, {@code FAILED},
     * {@code SUSPENDED}), or until {@code timeoutMs} elapses.
     *
     * <p>A 404 response from Flink is treated as terminal (already gone).
     *
     * @param flinkBaseUrl the Flink REST base URL (e.g. {@code http://127.0.0.1:8081})
     * @param jobId        the Flink job id to poll
     * @param timeoutMs    maximum wait time in milliseconds
     * @return {@code true} when the job is terminal, {@code false} on timeout
     */
    public static boolean waitForFlinkJobTerminal(String flinkBaseUrl, String jobId, long timeoutMs) {
        final java.util.Set<String> TERMINAL = java.util.Set.of("CANCELED", "FINISHED", "FAILED", "SUSPENDED");
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            try {
                HttpResponse<String> res = HTTP.send(
                    HttpRequest.newBuilder(URI.create(flinkBaseUrl + "/jobs/" + jobId))
                        .GET()
                        .timeout(Duration.ofSeconds(5))
                        .build(),
                    HttpResponse.BodyHandlers.ofString());
                if (res.statusCode() == 404) return true; // already gone
                if (res.statusCode() == 200) {
                    JsonNode body = json(res);
                    if (TERMINAL.contains(body.path("state").asText())) return true;
                }
            } catch (Exception ignored) { /* transient — keep polling */ }
            try { Thread.sleep(1000); } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return false;
    }

    /** Returns a lowercase identifier safe for use as a Kafka topic / agent id. */
    public static String randomId(String prefix) {
        return (prefix + "-" + Long.toHexString(System.nanoTime()))
            .toLowerCase(Locale.ROOT)
            .replaceAll("[^a-z0-9-]", "-");
    }
}
