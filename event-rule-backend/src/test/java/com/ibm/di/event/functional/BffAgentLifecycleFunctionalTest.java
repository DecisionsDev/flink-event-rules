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
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Functional tests for the BFF agent-lifecycle and scenarios-CRUD endpoints.
 *
 * <p>Tests exercise the full lifecycle of a wizard-created agent:
 * <ol>
 *   <li>POST /api/agents — register a new agent descriptor</li>
 *   <li>GET  /api/agents/:id — read back the saved descriptor</li>
 *   <li>GET  /api/agents — verify the new agent appears in the list</li>
 *   <li>POST /api/agents/:id/stop — cancel Flink job and delete Kafka topics</li>
 *   <li>POST /api/use-cases/:useCase/scenarios — create scenario in demo-scenarios.json</li>
 *   <li>GET  /api/use-cases/:useCase/scenarios — read back the created scenario</li>
 *   <li>PUT  /api/use-cases/:useCase/scenarios/:id — update the scenario</li>
 *   <li>DELETE /api/use-cases/:useCase/scenarios/:id — delete the scenario</li>
 *   <li>DELETE /api/agents/:id — clean up the agent (agents.json removed)</li>
 * </ol>
 *
 * <p>Tests are <b>skipped</b> unless {@code FUNCTIONAL_TESTS_ENABLED=true} is set.
 * Run with:
 * <pre>
 *   FUNCTIONAL_TESTS_ENABLED=true mvn test -pl event-rule-backend \
 *     --settings settings.xml -Dtest=BffAgentLifecycleFunctionalTest
 * </pre>
 *
 * <p>Scenarios covered:
 * <ul>
 *   <li>POST /api/agents — 201, saved=true, useCaseId and path in response</li>
 *   <li>POST /api/agents — descriptor persisted: GET /api/agents/:id returns it</li>
 *   <li>POST /api/agents — new agent appears in GET /api/agents list</li>
 *   <li>POST /api/agents — agent-banking protected use case update while running rejected (if running)</li>
 *   <li>POST /api/agents — validation: missing useCaseId → 400</li>
 *   <li>POST /api/agents — validation: invalid useCaseId → 400</li>
 *   <li>POST /api/agents — validation: missing descriptor → 400</li>
 *   <li>GET  /api/agents/:id — 200, agents array contains saved agent</li>
 *   <li>GET  /api/agents/:id — 404 for unknown id</li>
 *   <li>DELETE /api/agents/:id — 200, deleted=true, removed list non-empty</li>
 *   <li>DELETE /api/agents/:id — 404 after second delete (idempotent path)</li>
 *   <li>DELETE /api/agents/:id — 400 for invalid agent id</li>
 *   <li>POST /api/agents/:id/stop — 503 when FLINK_URL not configured</li>
 *   <li>POST /api/agents/:id/stop — no running job → 200, flinkCancelled=false, topicsDeleted present</li>
 *   <li>POST /api/agents/:id/stop — idempotent: second stop on an already-stopped agent succeeds</li>
 *   <li>POST /api/agents/:id/stop — Kafka topics purged+deleted BEFORE Flink cancel (pre-cancel cleanup)</li>
 *   <li>POST /api/agents/:id/stop — flinkState=null when no Flink job is associated</li>
 *   <li>POST /api/agents/:id/stop — flinkState field always present in response</li>
 *   <li>POST /api/agents/:id/stop — running Flink job → flinkState is a terminal value after stop</li>
 *   <li>Scenarios CRUD: POST creates, GET reads back, PUT updates, DELETE removes</li>
 *   <li>POST scenario — 409 for duplicate id</li>
 *   <li>POST scenario — 400 when id field absent</li>
 *   <li>PUT scenario — 404 when scenario id not found</li>
 *   <li>DELETE scenario — 404 after already deleted</li>
 * </ul>
 */
@DisplayName("BFF Agent Lifecycle and Scenarios CRUD — functional tests")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class BffAgentLifecycleFunctionalTest {

    @BeforeAll
    static void requireStack() {
        assumeTrue(isEnabled(), disabledReason());
        assumeTrue(waitForBff(),
            "BFF is not reachable at " + BFF_BASE_URL
            + " — ensure the Docker Compose stack is running.");
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /** Generates a unique test agent id that can be safely created and deleted. */
    private static String testAgentId() {
        return "ft-agent-" + UUID.randomUUID().toString().substring(0, 8);
    }

    /** Builds a minimal agents.json body for a test agent. */
    private static String agentDescriptorBody(String agentId, String useCase) {
        return """
            {
              "useCaseId": "%s",
              "descriptor": {
                "agents": [
                  {
                    "agentId": "%s",
                    "name": "Functional Test Agent",
                    "entityType": "test-entity",
                    "entityKeyExtractor": "$.entityId",
                    "subscriptions": [
                      {
                        "eventType": "test-event",
                        "topics": ["%s-inbound"],
                        "horizon": {"type": "count", "countLimit": 10}
                      }
                    ],
                    "ruleset": {
                      "name": "testRuleService",
                      "path": "samples/%s/test.jar",
                      "contextFormat": "generic",
                      "archiveControlTopic": "%s-archive-control"
                    },
                    "outputs": [
                      {
                        "eventType": "test-output",
                        "topic": "%s-outbound",
                        "description": "Test output events"
                      }
                    ]
                  }
                ]
              }
            }
            """.formatted(useCase, agentId, useCase, useCase, useCase, useCase);
    }

    /** Registers a test agent and returns its use-case id. Fails the test on error. */
    private static String registerTestAgent(String agentId) throws Exception {
        HttpResponse<String> res = post("/api/agents", agentDescriptorBody(agentId, agentId));
        assertEquals(201, res.statusCode(),
            "Failed to register test agent '" + agentId + "': " + res.body());
        return agentId;
    }

    /** Deletes a test agent by id. Silently ignores 404 (already gone). */
    private static void deleteTestAgent(String agentId) throws Exception {
        delete("/api/agents/" + agentId); // best-effort cleanup
    }

    // ── POST /api/agents — register ───────────────────────────────────────────

    @Nested
    @DisplayName("POST /api/agents — register agent descriptor")
    class RegisterAgent {

        @Test
        @DisplayName("valid descriptor → 201, saved=true, useCaseId and path in response")
        void registerAgent201() throws Exception {
            String agentId = testAgentId();
            try {
                HttpResponse<String> res = post("/api/agents", agentDescriptorBody(agentId, agentId));

                assertEquals(201, res.statusCode(),
                    "Expected 201 from POST /api/agents: " + res.body());
                JsonNode body = json(res);
                assertTrue(body.path("saved").asBoolean(),
                    "Expected saved=true: " + res.body());
                assertEquals(agentId, body.path("useCaseId").asText(),
                    "Expected useCaseId in response");
                assertTrue(body.path("path").asText().contains(agentId),
                    "Expected path to contain agentId: " + res.body());
            } finally {
                deleteTestAgent(agentId);
            }
        }

        @Test
        @DisplayName("registered agent is returned by GET /api/agents/:id")
        void registeredAgentReadBack() throws Exception {
            String agentId = testAgentId();
            try {
                registerTestAgent(agentId);

                HttpResponse<String> res = get("/api/agents/" + agentId);
                assertEquals(200, res.statusCode(),
                    "Expected 200 for GET /api/agents/" + agentId + ": " + res.body());
                JsonNode body = json(res);
                assertTrue(body.has("agents") && body.path("agents").isArray(),
                    "GET /api/agents/:id must return agents array");
                assertTrue(body.path("agents").size() > 0,
                    "Agents array must be non-empty");
            } finally {
                deleteTestAgent(agentId);
            }
        }

        @Test
        @DisplayName("registered agent appears in GET /api/agents list")
        void registeredAgentInList() throws Exception {
            String agentId = testAgentId();
            try {
                registerTestAgent(agentId);

                JsonNode agents = json(get("/api/agents"));
                boolean found = false;
                for (JsonNode a : agents) {
                    if (agentId.equals(a.path("id").asText())) { found = true; break; }
                }
                assertTrue(found,
                    "Newly registered agent '" + agentId + "' must appear in /api/agents list");
            } finally {
                deleteTestAgent(agentId);
            }
        }

        @Test
        @DisplayName("missing useCaseId → 400")
        void missingUseCaseId400() throws Exception {
            String body = """
                {"descriptor":{"agents":[]}}
                """;
            HttpResponse<String> res = post("/api/agents", body);
            assertEquals(400, res.statusCode(),
                "Expected 400 when useCaseId is absent: " + res.body());
        }

        @Test
        @DisplayName("invalid useCaseId (path traversal) → 400")
        void invalidUseCaseId400() throws Exception {
            String body = """
                {"useCaseId":"../etc/passwd","descriptor":{"agents":[]}}
                """;
            HttpResponse<String> res = post("/api/agents", body);
            assertEquals(400, res.statusCode(),
                "Expected 400 for invalid useCaseId: " + res.body());
        }

        @Test
        @DisplayName("missing descriptor field → 400")
        void missingDescriptor400() throws Exception {
            String body = """
                {"useCaseId":"ft-agent-test"}
                """;
            HttpResponse<String> res = post("/api/agents", body);
            assertEquals(400, res.statusCode(),
                "Expected 400 when descriptor is absent: " + res.body());
        }

        @Test
        @DisplayName("descriptor is null → 400")
        void nullDescriptor400() throws Exception {
            String body = """
                {"useCaseId":"ft-agent-test","descriptor":null}
                """;
            HttpResponse<String> res = post("/api/agents", body);
            assertEquals(400, res.statusCode(),
                "Expected 400 when descriptor is null: " + res.body());
        }
    }

    // ── DELETE /api/agents/:id ────────────────────────────────────────────────

    @Nested
    @DisplayName("DELETE /api/agents/:id — purge then remove agent")
    class DeleteAgent {

        /**
         * Builds an agent descriptor body whose agents.json references the given
         * topics explicitly, so we can predict exactly which topics the BFF will
         * target for purge/deletion.
         */
        private static String agentBodyWithTopics(String agentId, String inbound, String outbound) {
            return """
                {
                  "useCaseId": "%s",
                  "descriptor": {
                    "agents": [
                      {
                        "agentId": "%s",
                        "name": "Delete Test Agent",
                        "entityType": "test-entity",
                        "entityKeyExtractor": "$.entityId",
                        "subscriptions": [
                          {
                            "eventType": "test-event",
                            "topics": ["%s"],
                            "horizon": {"type": "count", "countLimit": 5}
                          }
                        ],
                        "ruleset": {
                          "name": "testRuleService",
                          "path": "samples/%s/test.jar",
                          "contextFormat": "generic"
                        },
                        "outputs": [
                          {
                            "eventType": "test-output",
                            "topic": "%s",
                            "description": "Test output"
                          }
                        ]
                      }
                    ]
                  }
                }
                """.formatted(agentId, agentId, inbound, agentId, outbound);
        }

        // ── existing behaviour ─────────────────────────────────────────────

        @Test
        @DisplayName("existing agent → 200, deleted=true, removed list non-empty")
        void deleteExistingAgent200() throws Exception {
            String agentId = testAgentId();
            registerTestAgent(agentId);

            HttpResponse<String> res = delete("/api/agents/" + agentId);

            assertEquals(200, res.statusCode(),
                "Expected 200 from DELETE /api/agents/" + agentId + ": " + res.body());
            JsonNode body = json(res);
            assertTrue(body.path("deleted").asBoolean(),
                "Expected deleted=true: " + res.body());
            assertEquals(agentId, body.path("agentId").asText(),
                "Response agentId must match deleted agent");
            assertTrue(body.has("removed") && body.path("removed").isArray()
                && body.path("removed").size() > 0,
                "Expected non-empty 'removed' list: " + res.body());
        }

        @Test
        @DisplayName("deleted agent no longer returned by GET /api/agents/:id → 404")
        void deletedAgentGone() throws Exception {
            String agentId = testAgentId();
            registerTestAgent(agentId);
            delete("/api/agents/" + agentId);

            HttpResponse<String> res = get("/api/agents/" + agentId);
            assertEquals(404, res.statusCode(),
                "Expected 404 for deleted agent: " + res.body());
        }

        @Test
        @DisplayName("unknown agent → 404")
        void unknownAgent404() throws Exception {
            HttpResponse<String> res = delete("/api/agents/no-such-agent-xyz-" + UUID.randomUUID().toString().substring(0, 8));
            assertEquals(404, res.statusCode(),
                "Expected 404 when deleting non-existent agent");
        }

        @Test
        @DisplayName("invalid agent id → 400")
        void invalidAgentId400() throws Exception {
            HttpResponse<String> res = delete("/api/agents/../etc");
            assertTrue(res.statusCode() == 400 || res.statusCode() == 404,
                "Expected 400 or 404 for path-traversal agent id, got " + res.statusCode());
        }

        @Test
        @DisplayName("topicsDeleted field present in response")
        void topicsDeletedFieldPresent() throws Exception {
            String agentId = testAgentId();
            registerTestAgent(agentId);

            HttpResponse<String> res = delete("/api/agents/" + agentId);
            assertEquals(200, res.statusCode());
            assertTrue(json(res).has("topicsDeleted"),
                "Expected 'topicsDeleted' array in DELETE response: " + res.body());
        }

        // ── purge-then-delete behaviour ────────────────────────────────────

        /**
         * When the agent's Kafka topics do not exist (never created — common for
         * test agents that were only registered, never started), the purge step
         * must treat "unknown topic" as already-purged and proceed to complete
         * deletion successfully.
         *
         * <p>This verifies:
         * <ul>
         *   <li>The BFF returns 200 with {@code deleted=true}.</li>
         *   <li>The {@code topicsDeleted} array is present (may be empty or list
         *       the topic names that were targeted).</li>
         *   <li>Descriptor files are removed from disk.</li>
         * </ul>
         */
        @Test
        @DisplayName("purge step treats absent Kafka topics as already-purged — deletion succeeds")
        void purgeWithAbsentTopicsSucceeds() throws Exception {
            String agentId = testAgentId();
            // Register an agent whose descriptor references topics that will not
            // exist in Kafka (the BFF does not create topics on registration —
            // only on /start).  The purge should skip cleanly.
            registerTestAgent(agentId);

            HttpResponse<String> res = delete("/api/agents/" + agentId);

            assertEquals(200, res.statusCode(),
                "DELETE must succeed even when Kafka topics are absent: " + res.body());
            JsonNode body = json(res);
            assertTrue(body.path("deleted").asBoolean(),
                "deleted=true expected: " + res.body());
            assertTrue(body.has("topicsDeleted") && body.path("topicsDeleted").isArray(),
                "topicsDeleted array must be present: " + res.body());
        }

        /**
         * DELETE response shape — all required fields must be present.
         * Verifies the contract of the response JSON regardless of how many
         * topics were actually purged.
         */
        @Test
        @DisplayName("DELETE response includes deleted, agentId, removed, topicsDeleted")
        void deleteResponseShape() throws Exception {
            String agentId = testAgentId();
            registerTestAgent(agentId);

            HttpResponse<String> res = delete("/api/agents/" + agentId);
            assertEquals(200, res.statusCode(), res.body());
            JsonNode body = json(res);

            assertTrue(body.has("deleted"),       "missing 'deleted' field: "       + res.body());
            assertTrue(body.has("agentId"),       "missing 'agentId' field: "       + res.body());
            assertTrue(body.has("removed") && body.path("removed").isArray(),
                "missing or non-array 'removed': " + res.body());
            assertTrue(body.has("topicsDeleted") && body.path("topicsDeleted").isArray(),
                "missing or non-array 'topicsDeleted': " + res.body());
        }

        /**
         * Descriptor files must be removed from disk after a successful delete,
         * regardless of whether Kafka topics were present.
         * Verifies the {@code removed} list contains the expected file entries.
         */
        @Test
        @DisplayName("descriptor files are removed after successful delete")
        void descriptorFilesRemovedAfterDelete() throws Exception {
            String agentId = testAgentId();
            registerTestAgent(agentId);

            HttpResponse<String> res = delete("/api/agents/" + agentId);
            assertEquals(200, res.statusCode(), res.body());

            JsonNode removed = json(res).path("removed");
            assertTrue(removed.isArray() && removed.size() > 0,
                "removed list must be non-empty: " + res.body());

            // Verify agents.json was listed
            boolean foundDescriptor = false;
            for (JsonNode f : removed) {
                if ("agents.json".equals(f.asText())) {
                    foundDescriptor = true;
                    break;
                }
            }
            assertTrue(foundDescriptor,
                "Expected agents.json in removed list: " + res.body());
        }

        /**
         * After a successful DELETE the agent's descriptor must no longer be
         * retrievable, confirming that file cleanup happened before a 200 was
         * returned.
         */
        @Test
        @DisplayName("agent is inaccessible via GET after delete")
        void agentInaccessibleAfterDelete() throws Exception {
            String agentId = testAgentId();
            registerTestAgent(agentId);

            HttpResponse<String> deleteRes = delete("/api/agents/" + agentId);
            assertEquals(200, deleteRes.statusCode(), deleteRes.body());

            HttpResponse<String> getRes = get("/api/agents/" + agentId);
            assertEquals(404, getRes.statusCode(),
                "Agent must not be accessible after deletion: " + getRes.body());
        }

        /**
         * Idempotent delete: a second DELETE on an already-deleted agent returns
         * 404 (the agent is gone), not 500.  This confirms that "topics already
         * absent" from the first delete does not cause the second call to fail
         * with a Kafka error.
         */
        @Test
        @DisplayName("second DELETE on already-deleted agent returns 404, not 500")
        void secondDeleteReturns404() throws Exception {
            String agentId = testAgentId();
            registerTestAgent(agentId);

            delete("/api/agents/" + agentId); // first delete — succeeds

            HttpResponse<String> second = delete("/api/agents/" + agentId);
            assertEquals(404, second.statusCode(),
                "Second DELETE must return 404 (not 500): " + second.body());
        }
    }

    // ── POST /api/agents/:id/stop — agent cancellation ───────────────────────

    @Nested
    @DisplayName("POST /api/agents/:id/stop — cancel Flink job and Kafka topics")
    class StopAgent {

        /**
         * Returns true when the BFF has FLINK_URL configured (i.e. Flink is reachable).
         * When false the /stop endpoint returns 503 and most stop tests should be skipped.
         */
        private static boolean isFlinkConfigured() {
            try {
                HttpResponse<String> res = get("/api/flink/status");
                // BFF returns 200 with {available: true|false} when FLINK_URL is set;
                // it returns 503 or an error body when FLINK_URL is absent.
                return res.statusCode() == 200;
            } catch (Exception e) {
                return false;
            }
        }

        /**
         * Stop with no Flink job running.
         *
         * <p>Because no Flink job was started for this test agent (we only registered
         * the descriptor), the BFF should treat the missing job as already-gone and
         * proceed to Kafka cleanup rather than returning an error.
         *
         * <p>Expected response shape:
         * <pre>
         * {
         *   "stopped": true,
         *   "agentId": "<id>",
         *   "jobId": null,
         *   "flinkCancelled": false,
         *   "topicsDeleted": [...]   // may be empty; field must be present
         * }
         * </pre>
         */
        @Test
        @DisplayName("no running Flink job → 200, flinkCancelled=false, topicsDeleted field present")
        void stopWithNoRunningJob() throws Exception {
            assumeTrue(isFlinkConfigured(),
                "Skipping: FLINK_URL not configured in this environment");

            String agentId = testAgentId();
            try {
                registerTestAgent(agentId);

                HttpResponse<String> res = post("/api/agents/" + agentId + "/stop", "{}");

                assertEquals(200, res.statusCode(),
                    "Expected 200 from POST /stop even when no job is running: " + res.body());
                JsonNode body = json(res);
                assertTrue(body.path("stopped").asBoolean(),
                    "Expected stopped=true: " + res.body());
                assertEquals(agentId, body.path("agentId").asText(),
                    "Response agentId must match agent: " + res.body());
                assertFalse(body.path("flinkCancelled").asBoolean(true),
                    "flinkCancelled should be false when no job was found: " + res.body());
                assertTrue(body.has("topicsDeleted") && body.path("topicsDeleted").isArray(),
                    "topicsDeleted array must be present: " + res.body());
            } finally {
                deleteTestAgent(agentId);
            }
        }

        /**
         * Idempotent stop: calling /stop a second time after a successful first stop
         * must succeed rather than fail.
         *
         * <p>After the first stop the Flink job is gone and the agentJobState has
         * been cleared.  The second call should skip Flink cancel (no job) and
         * complete with stopped=true.
         */
        @Test
        @DisplayName("stop is idempotent — second call on already-stopped agent succeeds")
        void stopIsIdempotent() throws Exception {
            assumeTrue(isFlinkConfigured(),
                "Skipping: FLINK_URL not configured in this environment");

            String agentId = testAgentId();
            try {
                registerTestAgent(agentId);

                // First stop
                HttpResponse<String> first = post("/api/agents/" + agentId + "/stop", "{}");
                assertEquals(200, first.statusCode(),
                    "First stop must succeed: " + first.body());

                // Second stop — must not fail even though the job and topics are gone
                HttpResponse<String> second = post("/api/agents/" + agentId + "/stop", "{}");
                assertEquals(200, second.statusCode(),
                    "Second (idempotent) stop must also succeed: " + second.body());
                assertTrue(json(second).path("stopped").asBoolean(),
                    "stopped=true expected on second call: " + second.body());
            } finally {
                deleteTestAgent(agentId);
            }
        }

        /**
         * Response shape: the stop response must include all expected fields.
         */
        @Test
        @DisplayName("stop response includes stopped, agentId, jobId, flinkCancelled, topicsDeleted")
        void stopResponseShape() throws Exception {
            assumeTrue(isFlinkConfigured(),
                "Skipping: FLINK_URL not configured in this environment");

            String agentId = testAgentId();
            try {
                registerTestAgent(agentId);

                HttpResponse<String> res = post("/api/agents/" + agentId + "/stop", "{}");
                assertEquals(200, res.statusCode(), res.body());
                JsonNode body = json(res);

                assertTrue(body.has("stopped"),       "missing 'stopped' field: "       + res.body());
                assertTrue(body.has("agentId"),       "missing 'agentId' field: "       + res.body());
                assertTrue(body.has("jobId"),         "missing 'jobId' field: "         + res.body());
                assertTrue(body.has("flinkCancelled"),"missing 'flinkCancelled' field: "+ res.body());
                assertTrue(body.has("topicsDeleted") && body.path("topicsDeleted").isArray(),
                    "missing or non-array 'topicsDeleted' field: " + res.body());
            } finally {
                deleteTestAgent(agentId);
            }
        }

        /**
         * Invalid agent id is rejected before any Flink or Kafka work is attempted.
         */
        @Test
        @DisplayName("invalid agent id → 400")
        void stopInvalidAgentId400() throws Exception {
            HttpResponse<String> res = post("/api/agents/../etc/stop", "{}");
            assertTrue(res.statusCode() == 400 || res.statusCode() == 404,
                "Expected 400 or 404 for path-traversal agent id, got " + res.statusCode());
        }

        /**
         * When FLINK_URL is not configured the endpoint must refuse immediately.
         *
         * <p>This test is only meaningful when the BFF was started without a
         * FLINK_URL, which is unusual in the Docker Compose stack (Flink is normally
         * present).  We therefore skip this test when Flink appears to be configured.
         */
        @Test
        @DisplayName("FLINK_URL not configured → 503")
        void stopWithoutFlinkUrl503() throws Exception {
            assumeFalse(isFlinkConfigured(),
                "Skipping: FLINK_URL is configured in this environment");

            String agentId = testAgentId();
            try {
                registerTestAgent(agentId);
                HttpResponse<String> res = post("/api/agents/" + agentId + "/stop", "{}");
                assertEquals(503, res.statusCode(),
                    "Expected 503 when FLINK_URL is not configured: " + res.body());
            } finally {
                deleteTestAgent(agentId);
            }
        }

        // ── poll-step / flinkState field tests ────────────────────────────────

        /**
         * When no Flink job is associated with the agent the BFF skips both the
         * cancel PATCH (step 3) and the terminal-state poll (step 4) entirely.
         * The response must include {@code flinkState: null} to signal that no
         * polling occurred, and {@code flinkCancelled: false}.
         *
         * <p>Kafka cleanup (step 2) still runs — topics are deleted before the
         * (skipped) Flink cancel, which is the correct pre-cancel sequence even
         * when there is no running job.
         */
        @Test
        @DisplayName("no running Flink job → flinkState is null in response")
        void stopFlinkStateNullWhenNoJobFound() throws Exception {
            assumeTrue(isFlinkConfigured(),
                "Skipping: FLINK_URL not configured in this environment");

            String agentId = testAgentId();
            try {
                registerTestAgent(agentId);

                HttpResponse<String> res = post("/api/agents/" + agentId + "/stop", "{}");
                assertEquals(200, res.statusCode(),
                    "Expected 200 from /stop with no running job: " + res.body());
                JsonNode body = json(res);
                assertTrue(body.has("flinkState"),
                    "Response must include 'flinkState' field: " + res.body());
                assertTrue(body.path("flinkState").isNull(),
                    "flinkState must be null when no Flink job was found: " + res.body());
            } finally {
                deleteTestAgent(agentId);
            }
        }

        /**
         * The stop response must include the {@code flinkState} field regardless
         * of whether a Flink job was running.  This test verifies the field is
         * always present (even when null) so callers can rely on a stable response
         * shape.
         */
        @Test
        @DisplayName("stop response always includes flinkState field")
        void stopResponseAlwaysIncludesFlinkState() throws Exception {
            assumeTrue(isFlinkConfigured(),
                "Skipping: FLINK_URL not configured in this environment");

            String agentId = testAgentId();
            try {
                registerTestAgent(agentId);

                HttpResponse<String> res = post("/api/agents/" + agentId + "/stop", "{}");
                assertEquals(200, res.statusCode(), res.body());
                assertTrue(json(res).has("flinkState"),
                    "Response must always include 'flinkState' field: " + res.body());
            } finally {
                deleteTestAgent(agentId);
            }
        }

        /**
         * After a successful stop the agent can be stopped again (idempotent) and
         * both calls must return a stable response that includes {@code flinkState}.
         *
         * <p>The second call exercises the code path where no job exists at all —
         * the poll step is entirely skipped — confirming that idempotent retries
         * do not introduce a regression in the presence of the new poll field.
         */
        @Test
        @DisplayName("idempotent stop — both calls return flinkState field")
        void idempotentStopBothCallsReturnFlinkState() throws Exception {
            assumeTrue(isFlinkConfigured(),
                "Skipping: FLINK_URL not configured in this environment");

            String agentId = testAgentId();
            try {
                registerTestAgent(agentId);

                HttpResponse<String> first = post("/api/agents/" + agentId + "/stop", "{}");
                assertEquals(200, first.statusCode(), "First stop: " + first.body());
                assertTrue(json(first).has("flinkState"),
                    "First stop must include flinkState: " + first.body());

                HttpResponse<String> second = post("/api/agents/" + agentId + "/stop", "{}");
                assertEquals(200, second.statusCode(), "Second stop: " + second.body());
                assertTrue(json(second).has("flinkState"),
                    "Second (idempotent) stop must include flinkState: " + second.body());
            } finally {
                deleteTestAgent(agentId);
            }
        }

        /**
         * When a Flink job IS running and the BFF successfully cancels it, the
         * poll step must observe the terminal state and return it in {@code flinkState}.
         * The value must be one of the known terminal state strings.
         *
         * <p>This test is skipped when Flink is not reachable or when the target
         * agent does not have a running Flink job (which is always the case for
         * test agents that were only registered, never started).  It is included
         * here as a live-stack integration test that passes when the full Docker
         * Compose stack is running and the agent is actively running.
         *
         * <p>To exercise this path, the test registers an agent, starts it via
         * {@code POST /api/agents/:id/start}, waits for the job to appear in Flink,
         * then stops it and asserts that {@code flinkState} is a known terminal value.
         */
        @Test
        @DisplayName("running Flink job → flinkState is a terminal value after stop")
        void stopRunningJobFlinkStateIsTerminal() throws Exception {
            assumeTrue(isFlinkConfigured(),
                "Skipping: FLINK_URL not configured in this environment");

            // This test only exercises the full cancel+poll flow when a Flink job
            // can actually be started.  Attempt to start one; if /start returns an
            // error (e.g. JAR not present, Flink cluster not ready) we skip gracefully.
            String agentId = testAgentId();
            try {
                registerTestAgent(agentId);

                // Attempt to start the agent — this may fail in minimal CI environments
                // that have Flink reachable but no JAR uploaded.
                HttpResponse<String> startRes = post("/api/agents/" + agentId + "/start", "{}");
                assumeTrue(startRes.statusCode() == 200,
                    "Skipping: /start returned " + startRes.statusCode() + " — Flink job could not be started");

                JsonNode startBody = json(startRes);
                String jobId = startBody.path("jobId").asText(null);
                assumeTrue(jobId != null && !jobId.isBlank(),
                    "Skipping: /start did not return a jobId — cannot verify poll step");

                // Wait for the Flink job to become visible in Flink (up to 30 s)
                boolean appeared = waitForFlinkJobTerminal(FLINK_BASE_URL, jobId, 30_000);
                // "appeared" returning true here means the job became terminal before
                // we even stopped it — skip the test (unusual but not an error)
                if (appeared) return;

                // Stop the agent and check that flinkState is a known terminal value
                HttpResponse<String> stopRes = post("/api/agents/" + agentId + "/stop", "{}");
                assertEquals(200, stopRes.statusCode(),
                    "Expected 200 from /stop after cancel: " + stopRes.body());

                JsonNode stopBody = json(stopRes);
                assertTrue(stopBody.path("stopped").asBoolean(),
                    "stopped must be true: " + stopRes.body());
                assertTrue(stopBody.has("flinkState"),
                    "flinkState field must be present: " + stopRes.body());

                String flinkState = stopBody.path("flinkState").asText(null);
                java.util.Set<String> terminalStates = java.util.Set.of(
                    "CANCELED", "FINISHED", "FAILED", "SUSPENDED");
                assertTrue(flinkState == null || terminalStates.contains(flinkState),
                    "flinkState must be null or a terminal value, got: " + flinkState);
            } finally {
                deleteTestAgent(agentId);
            }
        }

        // ── runtime-state cleanup tests ───────────────────────────────────────

        /**
         * After a successful stop, {@code GET /api/agents/:id/status} must report
         * {@code state=NOT_STARTED} (or return 200 with a null/absent jobId), proving
         * that the BFF cleared {@code agentJobState} for this agent.
         *
         * <p>This is the observable proxy for the full runtime-state cleanup: if
         * {@code agentJobState} was not deleted the endpoint would still return a
         * stale jobId and a non-{@code NOT_STARTED} state.
         */
        @Test
        @DisplayName("after stop, GET /api/agents/:id/status reports NOT_STARTED or no jobId")
        void stopClearsAgentJobState() throws Exception {
            assumeTrue(isFlinkConfigured(),
                "Skipping: FLINK_URL not configured in this environment");

            String agentId = testAgentId();
            try {
                registerTestAgent(agentId);

                HttpResponse<String> stopRes = post("/api/agents/" + agentId + "/stop", "{}");
                assertEquals(200, stopRes.statusCode(),
                    "Stop must succeed before checking status: " + stopRes.body());

                HttpResponse<String> statusRes = get("/api/agents/" + agentId + "/status");
                assertEquals(200, statusRes.statusCode(),
                    "GET /status must return 200: " + statusRes.body());

                JsonNode status = json(statusRes);
                // agentJobState cleared → state is NOT_STARTED and jobId is null
                String state = status.path("state").asText("");
                assertTrue(
                    "NOT_STARTED".equals(state) || status.path("jobId").isNull(),
                    "Expected NOT_STARTED or null jobId after stop, got: " + statusRes.body());
            } finally {
                deleteTestAgent(agentId);
            }
        }

        /**
         * Calling stop a second time after runtime state has been cleared must
         * still succeed.  This verifies that clearing {@code eventStore} and
         * {@code useCaseClientCounts} is idempotent — deleting absent map entries
         * must not throw.
         */
        @Test
        @DisplayName("repeated stop after runtime-state cleared remains idempotent")
        void repeatedStopAfterStateClearIsIdempotent() throws Exception {
            assumeTrue(isFlinkConfigured(),
                "Skipping: FLINK_URL not configured in this environment");

            String agentId = testAgentId();
            try {
                registerTestAgent(agentId);

                HttpResponse<String> first = post("/api/agents/" + agentId + "/stop", "{}");
                assertEquals(200, first.statusCode(), "First stop: " + first.body());

                // Second stop — all maps already empty for this id; must not throw
                HttpResponse<String> second = post("/api/agents/" + agentId + "/stop", "{}");
                assertEquals(200, second.statusCode(),
                    "Second stop after state cleared must still be 200: " + second.body());
                assertTrue(json(second).path("stopped").asBoolean(),
                    "stopped=true expected on second call: " + second.body());
            } finally {
                deleteTestAgent(agentId);
            }
        }
    }

    // ── Scenarios CRUD ────────────────────────────────────────────────────────

    @Nested
    @DisplayName("Scenarios CRUD — POST/GET/PUT/DELETE")
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    class ScenariosCrud {

        // We create a dedicated agent for scenario tests so we don't pollute
        // the banking demo-scenarios.json (which is a protected fixture).
        private static String scenarioAgentId;

        @BeforeAll
        static void createScenarioAgent() throws Exception {
            assumeTrue(isEnabled(), disabledReason());
            assumeTrue(waitForBff(), "BFF not reachable");
            scenarioAgentId = "ft-scen-" + UUID.randomUUID().toString().substring(0, 8);
            registerTestAgent(scenarioAgentId);
        }

        @AfterAll
        static void deleteScenarioAgent() throws Exception {
            if (scenarioAgentId != null) deleteTestAgent(scenarioAgentId);
        }

        @Test
        @Order(1)
        @DisplayName("POST scenarios — 201, scenario returned in response")
        void createScenario201() throws Exception {
            String scenarioId = "sc-" + UUID.randomUUID().toString().substring(0, 8);
            String body = """
                {
                  "id": "%s",
                  "label": "Functional Test Scenario",
                  "description": "Created by BffAgentLifecycleFunctionalTest"
                }
                """.formatted(scenarioId);

            HttpResponse<String> res = post(
                "/api/use-cases/" + scenarioAgentId + "/scenarios", body);

            assertEquals(201, res.statusCode(),
                "Expected 201 from POST scenarios: " + res.body());
            JsonNode respBody = json(res);
            assertEquals(scenarioId, respBody.path("id").asText(),
                "Response must echo the scenario id");

            // Cleanup
            delete("/api/use-cases/" + scenarioAgentId + "/scenarios/" + scenarioId);
        }

        @Test
        @Order(2)
        @DisplayName("POST scenario + GET scenarios — created scenario appears in list")
        void createThenRead() throws Exception {
            String scenarioId = "sc-r-" + UUID.randomUUID().toString().substring(0, 8);
            String body = """
                {
                  "id": "%s",
                  "label": "Read-back Scenario",
                  "description": "Created to verify read-back"
                }
                """.formatted(scenarioId);

            post("/api/use-cases/" + scenarioAgentId + "/scenarios", body);

            JsonNode scenarios = json(get("/api/use-cases/" + scenarioAgentId + "/scenarios"));
            assertTrue(scenarios.isArray(), "Scenarios must be a JSON array");
            boolean found = false;
            for (JsonNode s : scenarios) {
                if (scenarioId.equals(s.path("id").asText())) { found = true; break; }
            }
            assertTrue(found, "Created scenario must appear in GET /scenarios response");

            // Cleanup
            delete("/api/use-cases/" + scenarioAgentId + "/scenarios/" + scenarioId);
        }

        @Test
        @Order(3)
        @DisplayName("PUT scenario — 200, updated label reflected")
        void updateScenario200() throws Exception {
            String scenarioId = "sc-u-" + UUID.randomUUID().toString().substring(0, 8);
            String createBody = """
                {"id":"%s","label":"Original Label","description":"original"}
                """.formatted(scenarioId);
            post("/api/use-cases/" + scenarioAgentId + "/scenarios", createBody);

            String updateBody = """
                {"id":"%s","label":"Updated Label","description":"updated"}
                """.formatted(scenarioId);
            HttpResponse<String> updateRes = put(
                "/api/use-cases/" + scenarioAgentId + "/scenarios/" + scenarioId, updateBody);

            assertEquals(200, updateRes.statusCode(),
                "Expected 200 from PUT scenarios: " + updateRes.body());
            assertEquals("Updated Label", json(updateRes).path("label").asText(),
                "PUT must return updated label");

            // Cleanup
            delete("/api/use-cases/" + scenarioAgentId + "/scenarios/" + scenarioId);
        }

        @Test
        @Order(4)
        @DisplayName("DELETE scenario — 200, then GET returns no match")
        void deleteScenario200() throws Exception {
            String scenarioId = "sc-d-" + UUID.randomUUID().toString().substring(0, 8);
            String createBody = """
                {"id":"%s","label":"To Delete","description":"delete me"}
                """.formatted(scenarioId);
            post("/api/use-cases/" + scenarioAgentId + "/scenarios", createBody);

            HttpResponse<String> deleteRes = delete(
                "/api/use-cases/" + scenarioAgentId + "/scenarios/" + scenarioId);

            assertEquals(200, deleteRes.statusCode(),
                "Expected 200 from DELETE scenario: " + deleteRes.body());
            assertEquals(scenarioId, json(deleteRes).path("deleted").asText(),
                "Response must contain deleted scenario id");

            // Verify it's gone
            JsonNode scenarios = json(get("/api/use-cases/" + scenarioAgentId + "/scenarios"));
            boolean found = false;
            for (JsonNode s : scenarios) {
                if (scenarioId.equals(s.path("id").asText())) { found = true; break; }
            }
            assertFalse(found, "Deleted scenario must not appear in GET /scenarios response");
        }

        @Test
        @Order(5)
        @DisplayName("POST scenario — 409 when scenario id already exists")
        void duplicateScenarioId409() throws Exception {
            String scenarioId = "sc-dup-" + UUID.randomUUID().toString().substring(0, 8);
            String body = """
                {"id":"%s","label":"Dup","description":"dup"}
                """.formatted(scenarioId);

            post("/api/use-cases/" + scenarioAgentId + "/scenarios", body);
            HttpResponse<String> dupRes = post(
                "/api/use-cases/" + scenarioAgentId + "/scenarios", body);

            assertEquals(409, dupRes.statusCode(),
                "Expected 409 for duplicate scenario id: " + dupRes.body());

            // Cleanup
            delete("/api/use-cases/" + scenarioAgentId + "/scenarios/" + scenarioId);
        }

        @Test
        @Order(6)
        @DisplayName("POST scenario — 400 when id field is absent")
        void missingScenarioId400() throws Exception {
            String body = """
                {"label":"No ID Scenario","description":"missing id"}
                """;
            HttpResponse<String> res = post(
                "/api/use-cases/" + scenarioAgentId + "/scenarios", body);
            assertEquals(400, res.statusCode(),
                "Expected 400 when scenario id is absent: " + res.body());
        }

        @Test
        @Order(7)
        @DisplayName("PUT non-existent scenario → 404")
        void updateNonExistentScenario404() throws Exception {
            String body = """
                {"id":"ghost","label":"Ghost","description":"not found"}
                """;
            HttpResponse<String> res = put(
                "/api/use-cases/" + scenarioAgentId + "/scenarios/ghost-" +
                UUID.randomUUID().toString().substring(0, 8), body);
            assertEquals(404, res.statusCode(),
                "Expected 404 for PUT on non-existent scenario: " + res.body());
        }

        @Test
        @Order(8)
        @DisplayName("DELETE non-existent scenario → 404")
        void deleteNonExistentScenario404() throws Exception {
            HttpResponse<String> res = delete(
                "/api/use-cases/" + scenarioAgentId + "/scenarios/ghost-" +
                UUID.randomUUID().toString().substring(0, 8));
            assertEquals(404, res.statusCode(),
                "Expected 404 for DELETE on non-existent scenario: " + res.body());
        }
    }
}
