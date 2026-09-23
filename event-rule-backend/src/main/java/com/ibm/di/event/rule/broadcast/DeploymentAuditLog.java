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

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Per-subtask in-memory audit log for Decision Intelligence archive deployments.
 *
 * <p>Each {@link Entry} records one successful {@code LOAD} or {@code ROLLBACK} deployment
 * with the four audit dimensions:
 * <ul>
 *   <li><strong>who</strong>   — {@link ArchiveControlCmd#getRequestedBy()} or {@code "unknown"}</li>
 *   <li><strong>when</strong>  — wall-clock timestamp captured at the moment of activation</li>
 *   <li><strong>from</strong>  — the version label that was active before this deployment</li>
 *   <li><strong>to</strong>    — the version label that became active after this deployment</li>
 * </ul>
 *
 * <p>The log lives in operator memory (not in Flink state).  It accumulates entries for the
 * lifetime of the task and is cleared on restart / restore.  Operators expose it via
 * {@link com.ibm.di.event.agent.StatefulAgentProcessor#getDeploymentAuditLog()} for testing and
 * operational introspection.
 *
 * <p>Only successful activations are recorded — failed or DLQ'd commands are not logged here
 * (they are already captured by the DLQ side output and SLF4J error logs).
 *
 * <p>Thread safety: not externally synchronized; safe under Flink's single-threaded
 * task-thread model.  Instances are held inside {@link com.ibm.di.event.agent.StatefulAgentProcessor}
 * subtasks and are never shared across threads.
 */
public final class DeploymentAuditLog {

    private final List<Entry> entries = new ArrayList<>();

    // ── Append ────────────────────────────────────────────────────────────────

    /**
     * Records a successful deployment.
     *
     * @param cmd          the control command that triggered this deployment
     * @param fromVersion  version label that was active before this deployment ({@code null} = cold start)
     * @param subtaskIndex Flink subtask index (0-based) of the subtask that activated the archive
     */
    public void record(ArchiveControlCmd cmd, String fromVersion, int subtaskIndex) {
        String who  = (cmd.getRequestedBy() != null && !cmd.getRequestedBy().isBlank())
                        ? cmd.getRequestedBy() : "unknown";
        entries.add(new Entry(
            who,
            Instant.now(),
            fromVersion,
            cmd.getVersion(),
            cmd.getCommandType(),
            cmd.getRequestId(),
            subtaskIndex
        ));
    }

    // ── Read ──────────────────────────────────────────────────────────────────

    /** Returns an unmodifiable view of all recorded audit entries (oldest first). */
    public List<Entry> getEntries() {
        return Collections.unmodifiableList(entries);
    }

    /** Returns the number of recorded entries. */
    public int size() { return entries.size(); }

    // ── Entry ─────────────────────────────────────────────────────────────────

    /**
     * Immutable record of a single successful archive deployment.
     */
    public static final class Entry {

        /** Principal that issued the command ({@code "unknown"} when not provided). */
        public final String who;

        /** Wall-clock time when the archive was activated on this subtask. */
        public final Instant when;

        /** Version that was active before this deployment; {@code null} on a cold start. */
        public final String fromVersion;

        /** Version that became active after this deployment. */
        public final String toVersion;

        /** Command type ({@code LOAD} or {@code ROLLBACK}). */
        public final ArchiveControlCmd.CommandType commandType;

        /** Correlation ID from the original command; may be {@code null}. */
        public final String requestId;

        /** 0-based Flink subtask index of the task that processed this command. */
        public final int subtaskIndex;

        Entry(String who, Instant when, String fromVersion, String toVersion,
              ArchiveControlCmd.CommandType commandType, String requestId, int subtaskIndex) {
            this.who         = who;
            this.when        = when;
            this.fromVersion = fromVersion;
            this.toVersion   = toVersion;
            this.commandType = commandType;
            this.requestId   = requestId;
            this.subtaskIndex = subtaskIndex;
        }

        @Override
        public String toString() {
            return "DeploymentAuditLog.Entry{who='" + who + "', when=" + when
                + ", from='" + fromVersion + "', to='" + toVersion
                + "', type=" + commandType + ", requestId='" + requestId
                + "', subtask=" + subtaskIndex + "}";
        }
    }
}

