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
package com.ibm.di.event.rule.broadcast;

import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;

import java.io.Serializable;

/**
 * Control message published to the {@code archive-control} Kafka topic to trigger
 * a live reload of the Decision Intelligence archive in all running Flink subtasks.
 *
 * <p>Producers (CI/CD pipeline, admin CLI) publish this after uploading a new archive
 * JAR to object storage.  The message is broadcast to every parallel instance of the
 * rule processor via Flink's broadcast-state mechanism, which guarantees ordered,
 * exactly-once delivery to all subtasks before the next checkpoint barrier.
 *
 * <p>Field contract:
 * <ul>
 *   <li>{@link #version}     — unique, human-readable version label (e.g. {@code "2024-11-01-v3"})</li>
 *   <li>{@link #archiveUrl}  — fully-qualified URL to the JAR in object storage
 *                              ({@code s3://…}, {@code cos://…}, or {@code file:///…} for local dev)</li>
 *   <li>{@link #sha256}      — lowercase hex SHA-256 digest of the JAR bytes; verified before load</li>
 *   <li>{@link #commandType} — what action subtasks should take (default: {@link CommandType#LOAD})</li>
 *   <li>{@link #requestId}   — opaque correlation ID for audit logs and DLQ messages</li>
 *   <li>{@link #requestedBy} — identity of the principal that issued this command
 *                              (e.g. CI/CD pipeline service account, operator user-name);
 *                              optional but recommended for audit purposes</li>
 * </ul>
 */
public class ArchiveControlCmd implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Action requested by this control message. */
    public enum CommandType {
        /**
         * Download {@link #archiveUrl}, verify SHA-256, initialize a new
         * {@link com.ibm.di.event.rule.RuleEngineRunner}, smoke-test it, then
         * atomically swap it in as the active engine.  Also used for
         * {@code ROLLBACK} — the processor treats them identically since the
         * prior archive URL is supplied explicitly in the command.
         */
        LOAD,

        /**
         * Pre-fetch the archive from {@link #archiveUrl} without activating it.
         * Useful for warming the download cache before a subsequent {@link #LOAD}
         * to minimise the blocking window on the Flink task thread.
         */
        PREFETCH,

        /**
         * Roll back to a previous archive.
         *
         * <p>Two modes:
         * <ul>
         *   <li><strong>Implicit</strong> — omit both {@link #archiveUrl} and {@link #sha256}.
         *       The processor automatically resolves the target from the
         *       {@link ArchiveEntry#getPreviousEntry() last-known-good} stored in broadcast
         *       state.  Only {@link #version} (optional label override) and {@link #requestId}
         *       need to be set.</li>
         *   <li><strong>Explicit</strong> — supply {@link #archiveUrl} and {@link #sha256}
         *       to target a specific version (e.g. when rolling back more than one level).</li>
         * </ul>
         * If implicit resolution is attempted but no previous entry exists in broadcast state,
         * the command is rejected to the DLQ.
         */
        ROLLBACK,

        /** No-op; used for health-check pings on the control topic. */
        NOOP,

        /**
         * Clears all keyed entity state (entity data + event history) for every entity
         * managed by this agent processor.  Intended for test-scenario isolation: send
         * this command between scenario executions to guarantee each scenario starts from
         * a clean state without restarting the Flink job.
         *
         * <p>{@link #archiveUrl}, {@link #sha256}, and {@link #version} are not required
         * for this command type.  Only {@link #requestId} is recommended for audit purposes.
         *
         * <p><strong>Production caution</strong>: this irrecoverably discards live entity
         * state.  It should only be sent in test or staging environments unless a full
         * state purge is explicitly intended.
         */
        RESET_STATE
    }

    // ── Fields ────────────────────────────────────────────────────────────────

    /** Human-readable archive version label. Must be non-null for LOAD and ROLLBACK. */
    private String version;

    /** Object-storage URL of the archive JAR.  Must be accessible from all TaskManagers. */
    private String archiveUrl;

    /** Lowercase hex SHA-256 of the archive JAR bytes.  Required for LOAD; optional for NOOP. */
    private String sha256;

    /**
     * Action to perform.  Defaults to {@link CommandType#LOAD}.
     *
     * <p>{@code @JsonSetter(nulls = Nulls.SKIP)} ensures that if the JSON field is
     * explicitly {@code null}, Jackson keeps the field-initializer default ({@link CommandType#LOAD})
     * rather than setting the field to {@code null}, which would cause a NullPointerException
     * in the command-dispatch switch.
     */
    @JsonSetter(nulls = Nulls.SKIP)
    private CommandType commandType = CommandType.LOAD;

    /** Opaque correlation ID for audit trails and DLQ entries. */
    private String requestId;

    /**
     * Identity of the principal that issued this command — e.g. a CI/CD pipeline
     * service-account name, an operator's username, or a deploy-tool identifier.
     * Optional; when absent the audit log records {@code "unknown"}.
     */
    private String requestedBy;

    // ── Constructors ──────────────────────────────────────────────────────────

    public ArchiveControlCmd() {}

    public ArchiveControlCmd(String version, String archiveUrl, String sha256, String requestId) {
        this.version     = version;
        this.archiveUrl  = archiveUrl;
        this.sha256      = sha256;
        this.requestId   = requestId;
        this.commandType = CommandType.LOAD;
    }

    public ArchiveControlCmd(String version, String archiveUrl, String sha256,
                             String requestId, String requestedBy) {
        this(version, archiveUrl, sha256, requestId);
        this.requestedBy = requestedBy;
    }

    // ── Getters / Setters ─────────────────────────────────────────────────────

    public String      getVersion()     { return version; }
    public void        setVersion(String v)      { this.version = v; }

    public String      getArchiveUrl()  { return archiveUrl; }
    public void        setArchiveUrl(String u)   { this.archiveUrl = u; }

    public String      getSha256()      { return sha256; }
    public void        setSha256(String s)       { this.sha256 = s; }

    public CommandType getCommandType() { return commandType; }
    public void        setCommandType(CommandType t) { this.commandType = t; }

    public String      getRequestId()   { return requestId; }
    public void        setRequestId(String r)    { this.requestId = r; }

    public String      getRequestedBy() { return requestedBy; }
    public void        setRequestedBy(String b)  { this.requestedBy = b; }

    @Override
    public String toString() {
        return "ArchiveControlCmd{version='" + version + "', commandType=" + commandType
            + ", requestId='" + requestId + "', requestedBy='" + requestedBy
            + "', url='" + archiveUrl + "'}";
    }
}

