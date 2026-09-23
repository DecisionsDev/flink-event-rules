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
import com.ibm.di.event.rule.broadcast.DeploymentAuditLog;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * Lightweight RESTful HTTP API server for deploying Decision Intelligence archives.
 *
 * <p>Uses the JDK's built-in {@link HttpServer} (zero additional runtime dependencies).
 * Publishes validated {@link ArchiveControlCmd} messages to Kafka via
 * {@link KafkaCommandPublisher}; the Flink broadcast-state mechanism then delivers
 * each command to all running subtasks.
 *
 * <h3>Routes</h3>
 * <pre>
 *   POST /deployments           → LOAD a new archive version            → 202
 *   POST /deployments/rollback  → ROLLBACK to a previous version        → 202
 *   POST /deployments/prefetch  → Pre-warm the download cache           → 202
 *   GET  /deployments/audit     → List all audit log entries            → 200
 *   GET  /health                → Liveness / readiness probe            → 200
 * </pre>
 *
 * <h3>Lifecycle</h3>
 * <p>Call {@link #start()} once after construction; call {@link #stop()} on shutdown.
 * The server runs on a fixed-thread daemon pool and does not block the calling thread.
 *
 * <h3>Configuration</h3>
 * <p>Port is read from the {@code DEPLOY_API_PORT} environment variable; defaults to
 * {@code 8080}.  Bootstrap servers from {@code KAFKA_BOOTSTRAP_SERVERS}; defaults to
 * {@code localhost:9092}.  Allowed pipelines are read from {@code ALLOWED_PIPELINES}
 * (comma-separated {@code agentId} values); the first entry becomes the default pipeline
 * used when the caller omits the {@code pipeline} field.  See {@link #fromEnvironment}.
 */
public class DeploymentApiServer {

    private static final Logger LOG = LoggerFactory.getLogger(DeploymentApiServer.class);

    static final int DEFAULT_PORT = 8080;

    private final int                  port;
    private final KafkaCommandPublisher publisher;
    private final DeploymentAuditLog    auditLog;
    private final ObjectMapper          mapper;
    private final java.util.Set<String> allowedPipelines;
    private final String                defaultPipeline;
    private       HttpServer            server;

    // ── Construction ──────────────────────────────────────────────────────────

    /**
     * Creates a server that publishes commands using the supplied {@code publisher}
     * and serves audit entries from {@code auditLog}.
     *
     * @param port             TCP port to bind
     * @param publisher        Kafka command publisher (caller owns lifecycle)
     * @param auditLog         deployment audit log to expose on {@code GET /deployments/audit};
     *                         may be an empty log when running standalone
     * @param allowedPipelines set of permitted pipeline identifiers (each is the
     *                         {@code agentId} from an {@code agents.json}); must not be empty.
     *                         See {@code docs/AGENT_PATTERN_DESIGN.md} §3 for the field reference.
     * @param defaultPipeline  pipeline used when the caller omits the {@code pipeline} field;
     *                         must be a member of {@code allowedPipelines}
     */
    public DeploymentApiServer(int port,
                               KafkaCommandPublisher publisher,
                               DeploymentAuditLog auditLog,
                               java.util.Set<String> allowedPipelines,
                               String defaultPipeline) {
        this(port, publisher, auditLog, new ObjectMapper(), allowedPipelines, defaultPipeline);
    }

    /** Package-private constructor for testing with an injected ObjectMapper. */
    DeploymentApiServer(int port,
                        KafkaCommandPublisher publisher,
                        DeploymentAuditLog auditLog,
                        ObjectMapper mapper,
                        java.util.Set<String> allowedPipelines,
                        String defaultPipeline) {
        this.port             = port;
        this.publisher        = publisher;
        this.auditLog         = auditLog;
        this.mapper           = mapper;
        this.allowedPipelines = java.util.Set.copyOf(allowedPipelines);
        this.defaultPipeline  = defaultPipeline;
    }

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    /**
     * Binds to the configured port and starts the HTTP server on a daemon thread pool.
     *
     * @throws IOException if the port is unavailable
     */
    public void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), /*backlog*/ 16);
        server.setExecutor(Executors.newFixedThreadPool(4, r -> {
            Thread t = new Thread(r, "deploy-api-worker");
            t.setDaemon(true);
            return t;
        }));

        server.createContext("/health",               new HealthHandler());
        server.createContext("/deployments/rollback", new CommandHandler(ArchiveControlCmd.CommandType.ROLLBACK));
        server.createContext("/deployments/prefetch", new CommandHandler(ArchiveControlCmd.CommandType.PREFETCH));
        server.createContext("/deployments/audit",    new AuditHandler());
        // Register /deployments last — HttpServer matches longest prefix first,
        // so /deployments/rollback etc. are matched before the shorter /deployments.
        server.createContext("/deployments",          new CommandHandler(ArchiveControlCmd.CommandType.LOAD));

        server.start();
        LOG.info("[DeploymentApiServer] started on port {}", port);
    }

    /**
     * Stops the server, allowing up to 1 second for in-flight requests to complete.
     */
    public void stop() {
        if (server != null) {
            server.stop(1);
            LOG.info("[DeploymentApiServer] stopped");
        }
    }

    // ── Handlers ──────────────────────────────────────────────────────────────

    /**
     * Handles POST commands: LOAD, ROLLBACK, PREFETCH.
     * Validates the request body and publishes the {@link ArchiveControlCmd} to Kafka.
     */
    private final class CommandHandler implements HttpHandler {

        private final ArchiveControlCmd.CommandType commandType;

        CommandHandler(ArchiveControlCmd.CommandType commandType) {
            this.commandType = commandType;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String requestId = null;
            try {
                // ── Method guard ──────────────────────────────────────────────
                if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                    respond(exchange, 405,
                        ApiErrorResponse.methodNotAllowed(
                            "Use POST " + exchange.getHttpContext().getPath(), null));
                    return;
                }

                // ── Content-Type guard ────────────────────────────────────────
                String contentType = exchange.getRequestHeaders().getFirst("Content-Type");
                if (contentType == null || !contentType.toLowerCase().contains("application/json")) {
                    respond(exchange, 415, ApiErrorResponse.unsupportedMediaType(null));
                    return;
                }

                // ── Parse body ────────────────────────────────────────────────
                DeployArchiveRequest req;
                try (InputStream body = exchange.getRequestBody()) {
                    req = mapper.readValue(body, DeployArchiveRequest.class);
                } catch (Exception e) {
                    respond(exchange, 400,
                        ApiErrorResponse.badRequest("MALFORMED_JSON",
                            "Request body is not valid JSON: " + e.getMessage(), null));
                    return;
                }
                requestId = req.getRequestId();

                // ── Apply pipeline default ────────────────────────────────────
                // When the caller omits pipeline, fall back to the configured default.
                if (req.getPipeline() == null || req.getPipeline().isBlank()) {
                    req.setPipeline(defaultPipeline);
                }

                // ── Validate ──────────────────────────────────────────────────
                try {
                    req.validate();
                } catch (DeployArchiveRequest.ValidationException ve) {
                    respond(exchange, 400,
                        ApiErrorResponse.badRequest(ve.code, ve.getMessage(), requestId));
                    return;
                }

                // ── Allowlist pipeline ────────────────────────────────────────
                String effectivePipeline = req.effectivePipeline();
                if (!allowedPipelines.contains(effectivePipeline)) {
                    respond(exchange, 400,
                        ApiErrorResponse.badRequest("INVALID_PIPELINE",
                            "Unknown pipeline '" + effectivePipeline
                                + "'. Valid values: " + allowedPipelines, requestId));
                    return;
                }

                // ── Build command ─────────────────────────────────────────────
                ArchiveControlCmd cmd = req.toArchiveControlCmd(commandType);

                // Assign a server-side requestId when the caller omitted one
                if (cmd.getRequestId() == null || cmd.getRequestId().isBlank()) {
                    cmd.setRequestId(UUID.randomUUID().toString());
                    requestId = cmd.getRequestId();
                }

                // ── Publish ───────────────────────────────────────────────────
                try {
                    publisher.publish(cmd, effectivePipeline);
                } catch (Exception e) {
                    LOG.error("[DeploymentApiServer] Kafka publish failed: {}", e.getMessage(), e);
                    respond(exchange, 500,
                        ApiErrorResponse.internal(
                            "Failed to publish command to Kafka: " + e.getMessage(), requestId));
                    return;
                }

                // ── 202 Accepted ──────────────────────────────────────────────
                Map<String, Object> body = new LinkedHashMap<>();
                body.put("accepted",    true);
                body.put("requestId",   cmd.getRequestId());
                body.put("pipeline",    effectivePipeline);
                body.put("version",     cmd.getVersion());
                body.put("commandType", cmd.getCommandType().name());
                body.put("message",     "Command published to archive-control topic");
                respond(exchange, 202, body);

            } catch (Exception unexpected) {
                LOG.error("[DeploymentApiServer] unexpected error: {}", unexpected.getMessage(), unexpected);
                respond(exchange, 500, ApiErrorResponse.internal(
                    "Unexpected error: " + unexpected.getMessage(), requestId));
            }
        }
    }

    /**
     * Handles {@code GET /deployments/audit}: returns the in-process audit log as JSON.
     */
    private final class AuditHandler implements HttpHandler {

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                respond(exchange, 405,
                    ApiErrorResponse.methodNotAllowed("Use GET /deployments/audit", null));
                return;
            }
            try {
                List<Map<String, Object>> entries = auditLog.getEntries().stream()
                    .map(e -> {
                        Map<String, Object> m = new LinkedHashMap<>();
                        m.put("who",          e.who);
                        m.put("when",         e.when.toString());
                        m.put("fromVersion",  e.fromVersion);
                        m.put("toVersion",    e.toVersion);
                        m.put("commandType",  e.commandType != null ? e.commandType.name() : null);
                        m.put("requestId",    e.requestId);
                        m.put("subtaskIndex", e.subtaskIndex);
                        return m;
                    })
                    .collect(Collectors.toList());

                Map<String, Object> body = new LinkedHashMap<>();
                body.put("total",   entries.size());
                body.put("entries", entries);
                respond(exchange, 200, body);
            } catch (Exception e) {
                LOG.error("[DeploymentApiServer] audit read error: {}", e.getMessage(), e);
                respond(exchange, 500, ApiErrorResponse.internal(e.getMessage(), null));
            }
        }
    }

    /**
     * Handles {@code GET /health}: returns a minimal liveness response.
     */
    private final class HealthHandler implements HttpHandler {

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                respond(exchange, 405,
                    ApiErrorResponse.methodNotAllowed("Use GET /health", null));
                return;
            }
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("status",    "UP");
            body.put("timestamp", Instant.now().toString());
            respond(exchange, 200, body);
        }
    }

    // ── Response helper ───────────────────────────────────────────────────────

    /**
     * Serialises {@code body} to JSON and writes it as the HTTP response.
     */
    void respond(HttpExchange exchange, int status, Object body) throws IOException {
        byte[] bytes = mapper.writeValueAsBytes(body);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    // ── Static factory ────────────────────────────────────────────────────────

    /**
     * Creates a {@link DeploymentApiServer} from environment variables:
     * <ul>
     *   <li>{@code DEPLOY_API_PORT} — TCP port (default {@code 8080})</li>
     *   <li>{@code KAFKA_BOOTSTRAP_SERVERS} — broker addresses (default {@code localhost:9092})</li>
     *   <li>{@code ALLOWED_PIPELINES} — comma-separated list of permitted pipeline identifiers,
     *       each matching an {@code agentId} in {@code agents.json}
     *       (e.g. {@code "fraud-detection-agent,order-scoring-agent"}).
     *       The <em>first</em> entry is used as the default when a caller omits the
     *       {@code pipeline} field.  Required — must contain at least one entry.</li>
     * </ul>
     *
     * @param auditLog optional audit log to expose; pass an empty {@link DeploymentAuditLog}
     *                 when none is available
     * @throws IllegalStateException if {@code ALLOWED_PIPELINES} is absent or empty
     */
    public static DeploymentApiServer fromEnvironment(DeploymentAuditLog auditLog) {
        int port = DEFAULT_PORT;
        String portEnv = System.getenv("DEPLOY_API_PORT");
        if (portEnv != null && !portEnv.isBlank()) {
            try { port = Integer.parseInt(portEnv.trim()); }
            catch (NumberFormatException e) {
                LOG.warn("[DeploymentApiServer] Invalid DEPLOY_API_PORT '{}', using {}", portEnv, DEFAULT_PORT);
            }
        }
        String bootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        if (bootstrapServers == null || bootstrapServers.isBlank()) {
            bootstrapServers = "localhost:9092";
        }

        String pipelinesEnv = System.getenv("ALLOWED_PIPELINES");
        if (pipelinesEnv == null || pipelinesEnv.isBlank()) {
            throw new IllegalStateException(
                "Environment variable ALLOWED_PIPELINES must be set to a comma-separated list "
                + "of permitted pipeline identifiers (e.g. 'fraud-detection-agent,order-scoring-agent'). "
                + "Each entry must match an agentId in agents.json.");
        }
        List<String> pipelineList = java.util.Arrays.stream(pipelinesEnv.split(","))
            .map(String::trim)
            .filter(s -> !s.isBlank())
            .collect(Collectors.toList());
        if (pipelineList.isEmpty()) {
            throw new IllegalStateException(
                "ALLOWED_PIPELINES is set but contains no valid entries after parsing.");
        }
        java.util.Set<String> allowedPipelines = new java.util.LinkedHashSet<>(pipelineList);
        String defaultPipeline = pipelineList.get(0);

        return new DeploymentApiServer(port,
            new KafkaCommandPublisher(bootstrapServers),
            auditLog != null ? auditLog : new DeploymentAuditLog(),
            allowedPipelines,
            defaultPipeline);
    }
}

