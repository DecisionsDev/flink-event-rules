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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.ibm.di.event.rule.broadcast.ArchiveControlCmd;

import java.net.URI;
import java.util.regex.Pattern;

/**
 * JSON request body for archive deployment endpoints.
 *
 * <p>Call {@link #validate()} before converting to an {@link ArchiveControlCmd}.
 * Any constraint violation throws a {@link ValidationException} with a
 * machine-readable {@link ValidationException#code} and a human-readable message.
 *
 * <p>Field contract:
 * <ul>
 *   <li>{@link #version}     — required; unique version label (e.g. {@code "2025-07-v3"}), ≤ 128 chars</li>
 *   <li>{@link #archiveUrl}  — required; valid URI pointing to the archive JAR in object storage</li>
 *   <li>{@link #sha256}      — required; exactly 64 lowercase hex characters</li>
 *   <li>{@link #pipeline}    — required; the agent pipeline id (e.g. the agent folder name,
 *       {@code "my-agent-id"}).  No fixed allowlist — any non-blank value is accepted.
 *       {@link KafkaCommandPublisher} resolves the Kafka topic by reading the env var
 *       {@code ARCHIVE_CONTROL_TOPIC_<PIPELINE_UPPER>} (underscores replace hyphens in the name).
 *       Example: pipeline {@code "my-agent"} → env {@code ARCHIVE_CONTROL_TOPIC_MY_AGENT}.</li>
 *   <li>{@link #requestedBy} — optional; principal identity recorded in the audit log</li>
 *   <li>{@link #requestId}   — optional; caller-supplied correlation ID</li>
 * </ul>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DeployArchiveRequest {

    private static final int     MAX_VERSION_LENGTH = 128;
    private static final int     SHA256_HEX_LENGTH  = 64;
    private static final Pattern SHA256_PATTERN     = Pattern.compile("[0-9a-f]{64}");
    /** Valid pipeline id: 1–128 alphanumeric/hyphen/underscore characters. */
    private static final Pattern PIPELINE_PATTERN   = Pattern.compile("[a-zA-Z0-9_-]{1,128}");

    // ── Fields ────────────────────────────────────────────────────────────────

    private String version;
    private String archiveUrl;
    private String sha256;
    private String pipeline;
    private String requestedBy;
    private String requestId;

    // ── No-arg constructor (Jackson) ──────────────────────────────────────────

    public DeployArchiveRequest() {}

    // ── Validation ────────────────────────────────────────────────────────────

    /**
     * Validates all fields against the contract described in the class Javadoc.
     *
     * @throws ValidationException on the first constraint violation found
     */
    public void validate() {
        if (version == null || version.isBlank()) {
            throw new ValidationException("MISSING_VERSION",
                "Field 'version' is required and must not be blank");
        }
        if (version.length() > MAX_VERSION_LENGTH) {
            throw new ValidationException("VERSION_TOO_LONG",
                "Field 'version' must not exceed " + MAX_VERSION_LENGTH + " characters");
        }
        if (archiveUrl == null || archiveUrl.isBlank()) {
            throw new ValidationException("MISSING_ARCHIVE_URL",
                "Field 'archiveUrl' is required and must not be blank");
        }
        try {
            URI.create(archiveUrl);
        } catch (IllegalArgumentException e) {
            throw new ValidationException("INVALID_ARCHIVE_URL",
                "Field 'archiveUrl' is not a valid URI: " + e.getMessage());
        }
        if (sha256 == null || sha256.isBlank()) {
            throw new ValidationException("MISSING_SHA256",
                "Field 'sha256' is required and must not be blank");
        }
        if (sha256.length() != SHA256_HEX_LENGTH || !SHA256_PATTERN.matcher(sha256).matches()) {
            throw new ValidationException("INVALID_SHA256",
                "Field 'sha256' must be exactly 64 lowercase hexadecimal characters");
        }
        // pipeline is optional at the HTTP layer (the server applies a default before calling
        // validate()), but when present it must be non-blank and match the identifier pattern.
        if (pipeline != null) {
            if (pipeline.isBlank()) {
                throw new ValidationException("MISSING_PIPELINE",
                    "Field 'pipeline' must not be blank when provided");
            }
            if (!PIPELINE_PATTERN.matcher(pipeline).matches()) {
                throw new ValidationException("INVALID_PIPELINE",
                    "Field 'pipeline' must contain only alphanumeric characters, hyphens, or underscores (got '"
                    + pipeline + "')");
            }
        }
    }

    /**
     * Converts this validated request into an {@link ArchiveControlCmd} with the given
     * {@code commandType}.  Call {@link #validate()} first.
     */
    public ArchiveControlCmd toArchiveControlCmd(ArchiveControlCmd.CommandType commandType) {
        ArchiveControlCmd cmd = new ArchiveControlCmd();
        cmd.setVersion(version);
        cmd.setArchiveUrl(archiveUrl);
        cmd.setSha256(sha256);
        cmd.setCommandType(commandType);
        cmd.setRequestId(requestId);
        cmd.setRequestedBy(requestedBy);
        return cmd;
    }

    /**
     * Returns the effective (lower-cased, never null) pipeline name.
     */
    public String effectivePipeline() {
        return (pipeline == null || pipeline.isBlank()) ? "" : pipeline.toLowerCase();
    }

    // ── Getters / Setters ─────────────────────────────────────────────────────

    public String getVersion()             { return version; }
    public void   setVersion(String v)     { this.version = v; }

    public String getArchiveUrl()          { return archiveUrl; }
    public void   setArchiveUrl(String u)  { this.archiveUrl = u; }

    public String getSha256()              { return sha256; }
    public void   setSha256(String s)      { this.sha256 = s; }

    public String getPipeline()            { return pipeline; }
    public void   setPipeline(String p)    { this.pipeline = p; }

    public String getRequestedBy()         { return requestedBy; }
    public void   setRequestedBy(String b) { this.requestedBy = b; }

    public String getRequestId()           { return requestId; }
    public void   setRequestId(String r)   { this.requestId = r; }

    // ── Nested exception ──────────────────────────────────────────────────────

    /**
     * Thrown by {@link #validate()} when a field constraint is violated.
     */
    public static final class ValidationException extends RuntimeException {

        /** Machine-readable error code — e.g. {@code "MISSING_VERSION"}. */
        public final String code;

        public ValidationException(String code, String message) {
            super(message);
            this.code = code;
        }
    }
}

