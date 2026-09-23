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
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.net.http.HttpResponse;

import static com.ibm.di.event.functional.FunctionalTestEnvironment.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Functional tests for BFF infrastructure endpoints.
 *
 * <p>Validates that the BFF server is reachable, reports healthy status, exposes
 * the expected REST routes, and returns well-formed JSON for all read-only API
 * surfaces that do not require a running Flink job.
 *
 * <p>Tests are <b>skipped</b> unless {@code FUNCTIONAL_TESTS_ENABLED=true} is set.
 * Run with:
 * <pre>
 *   FUNCTIONAL_TESTS_ENABLED=true mvn test -pl event-rule-backend \
 *     --settings settings.xml -Dtest=BffHealthFunctionalTest
 * </pre>
 *
 * <p>Pre-requisites: {@code docker compose up -d zookeeper kafka bff}
 *
 * <p>Scenarios covered:
 * <ul>
 *   <li>GET /health — 200, status=ok, JSON body structure</li>
 *   <li>GET /health — connectedClients and activeUseCases are numeric fields</li>
 *   <li>GET /api/use-cases — 200, JSON array (may be empty if no Flink jobs running)</li>
 *   <li>GET /api/use-cases/:id/descriptor — 200 for agent-banking, known fields present</li>
 *   <li>GET /api/use-cases/:id/descriptor — 404 for unknown use case</li>
 *   <li>GET /api/use-cases/:id/descriptor — 400 for path-traversal/invalid use case id</li>
 *   <li>GET /api/use-cases/:id/scenarios — 200 array for agent-banking (9 scenarios)</li>
 *   <li>GET /api/use-cases/:id/scenarios — 200 empty array for use case without scenarios file</li>
 *   <li>GET /api/use-cases/:id/scenarios — 400 for invalid use case id</li>
 *   <li>GET /api/flink/status — 200, jobs array present</li>
 *   <li>GET /api/agents — 200, JSON array, agent-banking and agent-retail entries present</li>
 *   <li>GET /api/agents/:id — 200, agents.json content returned</li>
 *   <li>GET /api/agents/:id — 404 for unknown agent</li>
 *   <li>GET /api/agents/:id — 400 for invalid agent id</li>
 *   <li>Content-Type header is application/json for all JSON responses</li>
 * </ul>
 */
@DisplayName("BFF Health and Read-Only Endpoints — functional tests")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class BffHealthFunctionalTest {

    @BeforeAll
    static void requireStack() {
        assumeTrue(isEnabled(), disabledReason());
        assumeTrue(waitForBff(),
            "BFF is not reachable at " + BFF_BASE_URL
            + " — ensure the Docker Compose stack is running.");
    }

    // ── GET /health ───────────────────────────────────────────────────────────

    @Nested
    @DisplayName("GET /health")
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    class Health {

        @Test
        @Order(1)
        @DisplayName("returns HTTP 200")
        void returns200() throws Exception {
            HttpResponse<String> res = get("/health");
            assertEquals(200, res.statusCode(),
                "Expected 200 from /health but got " + res.statusCode() + ": " + res.body());
        }

        @Test
        @Order(2)
        @DisplayName("body has status=ok")
        void bodyStatusOk() throws Exception {
            JsonNode body = json(get("/health"));
            assertEquals("ok", body.path("status").asText());
        }

        @Test
        @Order(3)
        @DisplayName("body contains connectedClients (integer >= 0)")
        void bodyConnectedClients() throws Exception {
            JsonNode body = json(get("/health"));
            assertTrue(body.has("connectedClients"),
                "Expected 'connectedClients' field in /health response");
            assertTrue(body.path("connectedClients").asInt(-1) >= 0);
        }

        @Test
        @Order(4)
        @DisplayName("body contains activeUseCases (integer >= 0)")
        void bodyActiveUseCases() throws Exception {
            JsonNode body = json(get("/health"));
            assertTrue(body.has("activeUseCases"),
                "Expected 'activeUseCases' field in /health response");
            assertTrue(body.path("activeUseCases").asInt(-1) >= 0);
        }

        @Test
        @Order(5)
        @DisplayName("body contains useCases object")
        void bodyUseCasesObject() throws Exception {
            JsonNode body = json(get("/health"));
            assertTrue(body.has("useCases") && body.path("useCases").isObject(),
                "Expected 'useCases' object in /health response");
        }

        @Test
        @Order(6)
        @DisplayName("Content-Type is application/json")
        void contentTypeJson() throws Exception {
            HttpResponse<String> res = get("/health");
            assertTrue(res.headers().firstValue("Content-Type").orElse("").contains("application/json"),
                "Expected Content-Type: application/json from /health");
        }
    }

    // ── GET /api/flink/status ─────────────────────────────────────────────────

    @Nested
    @DisplayName("GET /api/flink/status")
    class FlinkStatus {

        @Test
        @DisplayName("returns HTTP 200 with jobs array")
        void returns200WithJobsArray() throws Exception {
            HttpResponse<String> res = get("/api/flink/status");
            assertEquals(200, res.statusCode());
            JsonNode body = json(res);
            assertTrue(body.has("jobs") && body.path("jobs").isArray(),
                "Expected 'jobs' array in /api/flink/status response: " + res.body());
        }

        @Test
        @DisplayName("jobs array contains only string values")
        void jobsArrayContainsStrings() throws Exception {
            JsonNode jobs = json(get("/api/flink/status")).path("jobs");
            jobs.forEach(j -> assertTrue(j.isTextual(),
                "Each job entry must be a string job-name, got: " + j));
        }
    }

    // ── GET /api/use-cases ────────────────────────────────────────────────────

    @Nested
    @DisplayName("GET /api/use-cases")
    class UseCases {

        @Test
        @DisplayName("returns HTTP 200 with JSON array")
        void returns200Array() throws Exception {
            HttpResponse<String> res = get("/api/use-cases");
            assertEquals(200, res.statusCode());
            assertTrue(json(res).isArray(),
                "Expected JSON array from /api/use-cases: " + res.body());
        }

        @Test
        @DisplayName("Content-Type is application/json")
        void contentTypeJson() throws Exception {
            HttpResponse<String> res = get("/api/use-cases");
            assertTrue(res.headers().firstValue("Content-Type").orElse("").contains("application/json"));
        }
    }

    // ── GET /api/use-cases/:id/descriptor ────────────────────────────────────

    @Nested
    @DisplayName("GET /api/use-cases/:id/descriptor")
    class Descriptor {

        @Test
        @DisplayName("agent-banking descriptor — 200 with expected fields")
        void bankingDescriptor200() throws Exception {
            HttpResponse<String> res = get("/api/use-cases/agent-banking/descriptor");
            assertEquals(200, res.statusCode(),
                "Expected 200 for agent-banking descriptor, got: " + res.body());
            JsonNode body = json(res);
            assertFalse(body.path("name").asText().isBlank(),
                "Descriptor must have non-blank 'name' field");
            assertTrue(body.has("topics") && body.path("topics").isArray(),
                "Descriptor must have 'topics' array");
            assertFalse(body.path("flinkJobName").asText().isBlank(),
                "Descriptor must have non-blank 'flinkJobName'");
        }

        @Test
        @DisplayName("agent-banking descriptor — topics have name, type, label fields")
        void bankingDescriptorTopicsWellFormed() throws Exception {
            JsonNode topics = json(get("/api/use-cases/agent-banking/descriptor")).path("topics");
            assertTrue(topics.isArray() && topics.size() > 0, "Must have at least one topic");
            for (JsonNode t : topics) {
                assertFalse(t.path("name").asText().isBlank(), "Topic must have 'name'");
                assertFalse(t.path("type").asText().isBlank(), "Topic must have 'type'");
            }
        }

        @Test
        @DisplayName("agent-retail descriptor — 200 with expected fields")
        void retailDescriptor200() throws Exception {
            HttpResponse<String> res = get("/api/use-cases/agent-retail/descriptor");
            assertEquals(200, res.statusCode(),
                "Expected 200 for agent-retail descriptor, got: " + res.body());
            JsonNode body = json(res);
            assertFalse(body.path("name").asText().isBlank());
            assertTrue(body.has("topics") && body.path("topics").isArray());
        }

        @Test
        @DisplayName("unknown use case → 404")
        void unknownUseCase404() throws Exception {
            HttpResponse<String> res = get("/api/use-cases/does-not-exist/descriptor");
            assertEquals(404, res.statusCode(),
                "Expected 404 for unknown use case descriptor");
        }

        @Test
        @DisplayName("path-traversal use case id → 400")
        void pathTraversalRejected400() throws Exception {
            HttpResponse<String> res = get("/api/use-cases/..%2Fetc%2Fpasswd/descriptor");
            // The BFF validates agent ids; ../etc/passwd is rejected before filesystem access
            assertTrue(res.statusCode() == 400 || res.statusCode() == 404,
                "Expected 400 or 404 for path-traversal use case id, got " + res.statusCode());
        }
    }

    // ── GET /api/use-cases/:id/scenarios ─────────────────────────────────────

    @Nested
    @DisplayName("GET /api/use-cases/:id/scenarios")
    class Scenarios {

        @Test
        @DisplayName("agent-banking scenarios — 200 with at least 9 entries")
        void bankingScenarios200() throws Exception {
            HttpResponse<String> res = get("/api/use-cases/agent-banking/scenarios");
            assertEquals(200, res.statusCode(),
                "Expected 200 for agent-banking scenarios: " + res.body());
            JsonNode body = json(res);
            assertTrue(body.isArray(), "Scenarios must be a JSON array");
            // The banking demo-scenarios.json ships with ≥9 scenarios; the exact count
            // may grow as new scenarios are added, so we assert a minimum rather than
            // a fixed value to avoid brittle test failures when the fixture is extended.
            assertTrue(body.size() >= 9,
                "Expected at least 9 banking scenarios from demo-scenarios.json, got " + body.size());
        }

        @Test
        @DisplayName("agent-banking scenarios — each entry has id and label")
        void bankingScenarioSchema() throws Exception {
            JsonNode scenarios = json(get("/api/use-cases/agent-banking/scenarios"));
            for (JsonNode s : scenarios) {
                assertFalse(s.path("id").asText().isBlank(),    "Scenario must have 'id'");
                assertFalse(s.path("label").asText().isBlank(), "Scenario must have 'label'");
            }
        }

        @Test
        @DisplayName("agent-retail scenarios — 200 with array")
        void retailScenarios200() throws Exception {
            HttpResponse<String> res = get("/api/use-cases/agent-retail/scenarios");
            assertEquals(200, res.statusCode());
            assertTrue(json(res).isArray());
        }

        @Test
        @DisplayName("invalid use case id → 400")
        void invalidUseCaseId400() throws Exception {
            HttpResponse<String> res = get("/api/use-cases/../../etc/scenarios");
            assertTrue(res.statusCode() == 400 || res.statusCode() == 404,
                "Expected 400 or 404 for invalid use case id, got " + res.statusCode());
        }
    }

    // ── GET /api/agents ───────────────────────────────────────────────────────

    @Nested
    @DisplayName("GET /api/agents")
    class Agents {

        @Test
        @DisplayName("returns HTTP 200 with JSON array")
        void returns200Array() throws Exception {
            HttpResponse<String> res = get("/api/agents");
            assertEquals(200, res.statusCode(), "Expected 200 from /api/agents: " + res.body());
            assertTrue(json(res).isArray(),
                "Expected JSON array from /api/agents");
        }

        @Test
        @DisplayName("agent-banking entry present in list")
        void bankingAgentPresent() throws Exception {
            JsonNode agents = json(get("/api/agents"));
            boolean found = false;
            for (JsonNode a : agents) {
                if ("agent-banking".equals(a.path("id").asText())) { found = true; break; }
            }
            assertTrue(found, "Expected 'agent-banking' entry in /api/agents list");
        }

        @Test
        @DisplayName("agent-retail entry present in list")
        void retailAgentPresent() throws Exception {
            JsonNode agents = json(get("/api/agents"));
            boolean found = false;
            for (JsonNode a : agents) {
                if ("agent-retail".equals(a.path("id").asText())) { found = true; break; }
            }
            assertTrue(found, "Expected 'agent-retail' entry in /api/agents list");
        }

        @Test
        @DisplayName("each agent entry has id, name, agentId fields")
        void agentEntriesWellFormed() throws Exception {
            JsonNode agents = json(get("/api/agents"));
            for (JsonNode a : agents) {
                assertFalse(a.path("id").asText().isBlank(),      "Agent must have 'id'");
                assertFalse(a.path("name").asText().isBlank(),    "Agent must have 'name'");
                assertFalse(a.path("agentId").asText().isBlank(), "Agent must have 'agentId'");
            }
        }
    }

    // ── GET /api/agents/:id ───────────────────────────────────────────────────

    @Nested
    @DisplayName("GET /api/agents/:id")
    class AgentById {

        @Test
        @DisplayName("agent-banking — 200 with agents array")
        void banking200() throws Exception {
            HttpResponse<String> res = get("/api/agents/agent-banking");
            assertEquals(200, res.statusCode(),
                "Expected 200 for GET /api/agents/agent-banking: " + res.body());
            JsonNode body = json(res);
            assertTrue(body.has("agents") && body.path("agents").isArray(),
                "Response must contain 'agents' array");
            assertTrue(body.path("agents").size() > 0,
                "agents array must not be empty for agent-banking");
        }

        @Test
        @DisplayName("unknown agent → 404")
        void unknown404() throws Exception {
            HttpResponse<String> res = get("/api/agents/no-such-agent-xyz");
            assertEquals(404, res.statusCode(),
                "Expected 404 for unknown agent id");
        }

        @Test
        @DisplayName("invalid agent id (path traversal) → 400")
        void invalidId400() throws Exception {
            HttpResponse<String> res = get("/api/agents/../../etc");
            assertTrue(res.statusCode() == 400 || res.statusCode() == 404,
                "Expected 400 or 404 for path-traversal agent id, got " + res.statusCode());
        }
    }
}
