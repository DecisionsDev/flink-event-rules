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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link DeployArchiveRequest}.
 *
 * <p>Covered:
 * <ul>
 *   <li>Happy path: all required fields present → no exception</li>
 *   <li>version: null, blank, exceeds 128 chars</li>
 *   <li>archiveUrl: null, blank, invalid URI</li>
 *   <li>sha256: null, blank, too short, too long, uppercase, non-hex chars</li>
 *   <li>pipeline: null/absent → MISSING_PIPELINE, blank → MISSING_PIPELINE, invalid chars → INVALID_PIPELINE, any valid identifier accepted</li>
 *   <li>{@code toArchiveControlCmd()} maps all fields correctly for each CommandType</li>
 *   <li>Jackson deserialisation round-trip with optional fields absent</li>
 *   <li>{@code effectivePipeline()} returns lower-cased pipeline value</li>
 * </ul>
 */
@DisplayName("DeployArchiveRequest Unit Tests")
class DeployArchiveRequestTest {

    private static final String VALID_VERSION = "2025-07-scoring-v3";
    private static final String VALID_URL     = "s3://my-bucket/archives/v3.jar";
    private static final String VALID_SHA256  = "a".repeat(64);   // 64 'a' chars — valid hex

    // ── Happy path ────────────────────────────────────────────────────────────

    @Test
    @DisplayName("Valid request does not throw")
    void validRequestDoesNotThrow() {
        assertDoesNotThrow(() -> request(VALID_VERSION, VALID_URL, VALID_SHA256).validate());
    }

    // ── version ───────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("Field: version")
    class VersionValidation {

        @Test
        @DisplayName("null version → MISSING_VERSION")
        void nullVersion() {
            assertCode("MISSING_VERSION", request(null, VALID_URL, VALID_SHA256));
        }

        @Test
        @DisplayName("blank version → MISSING_VERSION")
        void blankVersion() {
            assertCode("MISSING_VERSION", request("  ", VALID_URL, VALID_SHA256));
        }

        @Test
        @DisplayName("version exceeds 128 chars → VERSION_TOO_LONG")
        void versionTooLong() {
            assertCode("VERSION_TOO_LONG", request("x".repeat(129), VALID_URL, VALID_SHA256));
        }

        @Test
        @DisplayName("version of exactly 128 chars is accepted")
        void versionMaxLength() {
            assertDoesNotThrow(() -> request("x".repeat(128), VALID_URL, VALID_SHA256).validate());
        }
    }

    // ── archiveUrl ────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("Field: archiveUrl")
    class ArchiveUrlValidation {

        @Test
        @DisplayName("null archiveUrl → MISSING_ARCHIVE_URL")
        void nullUrl() {
            assertCode("MISSING_ARCHIVE_URL", request(VALID_VERSION, null, VALID_SHA256));
        }

        @Test
        @DisplayName("blank archiveUrl → MISSING_ARCHIVE_URL")
        void blankUrl() {
            assertCode("MISSING_ARCHIVE_URL", request(VALID_VERSION, "  ", VALID_SHA256));
        }

        @Test
        @DisplayName("URI with spaces → INVALID_ARCHIVE_URL")
        void invalidUri() {
            assertCode("INVALID_ARCHIVE_URL", request(VALID_VERSION, "not a uri", VALID_SHA256));
        }

        @ParameterizedTest(name = "valid scheme {0}")
        @ValueSource(strings = {
            "s3://bucket/archive.jar",
            "cos://bucket/archive.jar",
            "https://host/archive.jar",
            "file:///tmp/archive.jar"
        })
        @DisplayName("Accepted URI schemes")
        void acceptedSchemes(String url) {
            assertDoesNotThrow(() -> request(VALID_VERSION, url, VALID_SHA256).validate());
        }
    }

    // ── sha256 ────────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("Field: sha256")
    class Sha256Validation {

        @Test
        @DisplayName("null sha256 → MISSING_SHA256")
        void nullSha() {
            assertCode("MISSING_SHA256", request(VALID_VERSION, VALID_URL, null));
        }

        @Test
        @DisplayName("blank sha256 → MISSING_SHA256")
        void blankSha() {
            assertCode("MISSING_SHA256", request(VALID_VERSION, VALID_URL, "  "));
        }

        @Test
        @DisplayName("63 hex chars (too short) → INVALID_SHA256")
        void tooShort() {
            assertCode("INVALID_SHA256", request(VALID_VERSION, VALID_URL, "a".repeat(63)));
        }

        @Test
        @DisplayName("65 hex chars (too long) → INVALID_SHA256")
        void tooLong() {
            assertCode("INVALID_SHA256", request(VALID_VERSION, VALID_URL, "a".repeat(65)));
        }

        @Test
        @DisplayName("uppercase hex → INVALID_SHA256")
        void uppercaseHex() {
            assertCode("INVALID_SHA256", request(VALID_VERSION, VALID_URL, "A".repeat(64)));
        }

        @Test
        @DisplayName("non-hex character 'g' → INVALID_SHA256")
        void nonHexChar() {
            assertCode("INVALID_SHA256", request(VALID_VERSION, VALID_URL, "g".repeat(64)));
        }

        @Test
        @DisplayName("exactly 64 lowercase hex chars → accepted")
        void exactlyValid() {
            // 64 hex chars: 8 groups of 8
            String sha = "1a2b3c4d" + "5e6f0123" + "456789ab" + "cdef0123"
                       + "456789ab" + "cdef0123" + "456789ab" + "cdef0123";
            assertEquals(64, sha.length());
            assertDoesNotThrow(() -> request(VALID_VERSION, VALID_URL, sha).validate());
        }
    }

    // ── pipeline ──────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("Field: pipeline")
    class PipelineValidation {

        @Test
        @DisplayName("null pipeline → no error (pipeline is optional in validate())")
        void nullPipeline() {
            DeployArchiveRequest req = request(VALID_VERSION, VALID_URL, VALID_SHA256);
            req.setPipeline(null);
            assertDoesNotThrow(req::validate);
        }

        @Test
        @DisplayName("blank pipeline → MISSING_PIPELINE")
        void blankPipeline() {
            DeployArchiveRequest req = request(VALID_VERSION, VALID_URL, VALID_SHA256);
            req.setPipeline("  ");
            assertCode("MISSING_PIPELINE", req);
        }

        @Test
        @DisplayName("any valid identifier is accepted — no fixed allowlist")
        void arbitraryValidIdentifierAccepted() {
            for (String id : new String[]{"my-agent", "agent_v2", "order-scoring-agent", "scoring123"}) {
                DeployArchiveRequest req = request(VALID_VERSION, VALID_URL, VALID_SHA256);
                req.setPipeline(id);
                assertDoesNotThrow(req::validate,
                    "Pipeline '" + id + "' must be accepted");
            }
        }

        @Test
        @DisplayName("pipeline with invalid characters (spaces) → INVALID_PIPELINE")
        void pipelineWithSpaces() {
            DeployArchiveRequest req = request(VALID_VERSION, VALID_URL, VALID_SHA256);
            req.setPipeline("bad pipeline");
            assertCode("INVALID_PIPELINE", req);
        }

        @Test
        @DisplayName("pipeline exceeding 128 chars → INVALID_PIPELINE")
        void pipelineTooLong() {
            DeployArchiveRequest req = request(VALID_VERSION, VALID_URL, VALID_SHA256);
            req.setPipeline("a".repeat(129));
            assertCode("INVALID_PIPELINE", req);
        }
    }

    // ── effectivePipeline() ───────────────────────────────────────────────────

    @Test
    @DisplayName("effectivePipeline() returns lower-cased value")
    void effectivePipelineLowerCased() {
        DeployArchiveRequest req = request(VALID_VERSION, VALID_URL, VALID_SHA256);
        req.setPipeline("MY-AGENT");
        // effectivePipeline lower-cases; validate would reject uppercase — tested separately
        assertEquals("my-agent", req.effectivePipeline());
    }

    @Test
    @DisplayName("effectivePipeline() returns empty string when field is null")
    void effectivePipelineNullReturnsEmpty() {
        DeployArchiveRequest req = request(VALID_VERSION, VALID_URL, VALID_SHA256);
        req.setPipeline(null);
        assertEquals("", req.effectivePipeline());
    }

    // ── toArchiveControlCmd() ─────────────────────────────────────────────────

    @Nested
    @DisplayName("toArchiveControlCmd()")
    class ToArchiveControlCmd {

        @Test
        @DisplayName("LOAD command maps all fields")
        void loadMapsAllFields() {
            DeployArchiveRequest req = request(VALID_VERSION, VALID_URL, VALID_SHA256);
            req.setRequestId("req-1");
            req.setRequestedBy("ci-pipeline");

            ArchiveControlCmd cmd = req.toArchiveControlCmd(ArchiveControlCmd.CommandType.LOAD);

            assertEquals(VALID_VERSION,                       cmd.getVersion());
            assertEquals(VALID_URL,                           cmd.getArchiveUrl());
            assertEquals(VALID_SHA256,                        cmd.getSha256());
            assertEquals(ArchiveControlCmd.CommandType.LOAD,  cmd.getCommandType());
            assertEquals("req-1",                             cmd.getRequestId());
            assertEquals("ci-pipeline",                       cmd.getRequestedBy());
        }

        @Test
        @DisplayName("ROLLBACK sets CommandType correctly")
        void rollbackSetsCommandType() {
            ArchiveControlCmd cmd = request(VALID_VERSION, VALID_URL, VALID_SHA256)
                .toArchiveControlCmd(ArchiveControlCmd.CommandType.ROLLBACK);
            assertEquals(ArchiveControlCmd.CommandType.ROLLBACK, cmd.getCommandType());
        }

        @Test
        @DisplayName("PREFETCH sets CommandType correctly")
        void prefetchSetsCommandType() {
            ArchiveControlCmd cmd = request(VALID_VERSION, VALID_URL, VALID_SHA256)
                .toArchiveControlCmd(ArchiveControlCmd.CommandType.PREFETCH);
            assertEquals(ArchiveControlCmd.CommandType.PREFETCH, cmd.getCommandType());
        }

        @Test
        @DisplayName("Optional fields absent → requestId and requestedBy are null")
        void optionalFieldsAbsent() {
            ArchiveControlCmd cmd = request(VALID_VERSION, VALID_URL, VALID_SHA256)
                .toArchiveControlCmd(ArchiveControlCmd.CommandType.LOAD);
            assertNull(cmd.getRequestId());
            assertNull(cmd.getRequestedBy());
        }
    }

    // ── JSON deserialisation round-trip ───────────────────────────────────────

    @Nested
    @DisplayName("Jackson deserialisation")
    class JacksonDeserialisation {

        private static final ObjectMapper MAPPER = new ObjectMapper();

        @Test
        @DisplayName("Full JSON round-trip preserves all fields")
        void fullJsonRoundTrip() throws Exception {
            String json = """
                {
                  "version":     "2025-07-v3",
                  "archiveUrl":  "s3://b/v3.jar",
                  "sha256":      "%s",
                  "pipeline":    "my-scoring-agent",
                  "requestedBy": "ci-bot",
                  "requestId":   "req-xyz"
                }
                """.formatted(VALID_SHA256);

            DeployArchiveRequest req = MAPPER.readValue(json, DeployArchiveRequest.class);
            assertEquals("2025-07-v3",        req.getVersion());
            assertEquals("s3://b/v3.jar",     req.getArchiveUrl());
            assertEquals(VALID_SHA256,         req.getSha256());
            assertEquals("my-scoring-agent",  req.getPipeline());
            assertEquals("ci-bot",            req.getRequestedBy());
            assertEquals("req-xyz",           req.getRequestId());
        }

        @Test
        @DisplayName("Minimal JSON (only required fields) deserialises and validate() passes — pipeline is optional")
        void minimalJsonWithoutPipelinePassesValidation() throws Exception {
            String json = """
                {"version":"v1","archiveUrl":"file:///tmp/a.jar","sha256":"%s"}
                """.formatted(VALID_SHA256);
            DeployArchiveRequest req = MAPPER.readValue(json, DeployArchiveRequest.class);
            assertDoesNotThrow(req::validate);
        }

        @Test
        @DisplayName("Minimal JSON with pipeline validates cleanly")
        void minimalJsonWithPipeline() throws Exception {
            String json = """
                {"version":"v1","archiveUrl":"file:///tmp/a.jar","sha256":"%s","pipeline":"my-agent"}
                """.formatted(VALID_SHA256);
            DeployArchiveRequest req = MAPPER.readValue(json, DeployArchiveRequest.class);
            assertDoesNotThrow(req::validate);
        }

        @Test
        @DisplayName("Unknown fields are ignored")
        void unknownFieldsIgnored() throws Exception {
            String json = """
                {"version":"v1","archiveUrl":"file:///tmp/a.jar","sha256":"%s","unknownField":"x"}
                """.formatted(VALID_SHA256);
            assertDoesNotThrow(() -> MAPPER.readValue(json, DeployArchiveRequest.class));
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static DeployArchiveRequest request(String version, String url, String sha256) {
        DeployArchiveRequest req = new DeployArchiveRequest();
        req.setVersion(version);
        req.setArchiveUrl(url);
        req.setSha256(sha256);
        return req;
    }

    /** Assert that {@code req.validate()} throws a {@link DeployArchiveRequest.ValidationException}
     *  with the given {@code expectedCode}. */
    private static void assertCode(String expectedCode, DeployArchiveRequest req) {
        DeployArchiveRequest.ValidationException ex =
            assertThrows(DeployArchiveRequest.ValidationException.class, req::validate,
                "Expected ValidationException with code=" + expectedCode);
        assertEquals(expectedCode, ex.code,
            "ValidationException code mismatch");
    }
}

