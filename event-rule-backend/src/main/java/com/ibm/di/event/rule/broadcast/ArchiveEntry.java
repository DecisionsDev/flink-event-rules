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

import java.io.Serial;
import java.io.Serializable;

/**
 * Serializable value stored in Flink broadcast state representing the currently active
 * Decision Intelligence archive.
 *
 * <p><strong>Serialization contract:</strong> all five persisted fields
 * ({@code version}, {@code archiveUrl}, {@code sha256}, {@code loadedAtMs},
 * {@code previousEntry}) are included in Flink checkpoints.
 * The live {@link com.ibm.di.event.rule.RuleEngineRunner} is held exclusively in
 * the processor's per-subtask {@code runnerCache} map and is never stored here —
 * avoids any risk of Flink's POJO serializer touching a non-serializable
 * {@link java.net.URLClassLoader}.  After a job restore the runner is
 * re-materialised from {@link #archiveUrl} by the processor on the first event
 * processed.
 *
 * <p>{@link #previousEntry} records the archive that was active <em>before</em>
 * this one was loaded.  It is used by {@link com.ibm.di.event.agent.StatefulAgentProcessor}
 * to satisfy a {@code ROLLBACK} command that does not supply an explicit URL and
 * SHA-256 — the processor can re-activate the previous version without any
 * external bookkeeping by the caller.
 *
 * <p>Instances are effectively immutable after creation; the broadcast processor creates
 * a new {@code ArchiveEntry} for each successful reload.
 */
public class ArchiveEntry implements Serializable {

    @Serial
    private static final long serialVersionUID = 2L;   // bumped: new previousEntry field

    // ── Persisted fields (included in checkpoint) ─────────────────────────────

    /** Human-readable version label (e.g. {@code "2024-11-01-v3"}). */
    private String version;

    /** Object-storage URL used to (re-)download the JAR after a checkpoint restore. */
    private String archiveUrl;

    /** Lowercase hex SHA-256 of the JAR bytes; re-verified on restore. */
    private String sha256;

    /** Epoch-millisecond timestamp when this entry was activated. */
    private long loadedAtMs;

    /**
     * The archive that was active immediately before this one.
     * {@code null} on the first ever load (cold start / bootstrap).
     *
     * <p>Only one level of history is kept: rolling back twice in a row
     * restores the archive that was active before the <em>first</em> rollback.
     * Storing a full history chain would grow unboundedly in checkpoint state.
     */
    private ArchiveEntry previousEntry;

    // ── Constructors ──────────────────────────────────────────────────────────

    /** No-arg constructor required by Flink's POJO serializer. */
    public ArchiveEntry() {}

    public ArchiveEntry(String version, String archiveUrl, String sha256, long loadedAtMs) {
        this.version    = version;
        this.archiveUrl = archiveUrl;
        this.sha256     = sha256;
        this.loadedAtMs = loadedAtMs;
    }

    public ArchiveEntry(String version, String archiveUrl, String sha256,
                        long loadedAtMs, ArchiveEntry previousEntry) {
        this(version, archiveUrl, sha256, loadedAtMs);
        this.previousEntry = previousEntry;
    }

    // ── Getters / Setters ─────────────────────────────────────────────────────

    public String getVersion()    { return version; }
    public void   setVersion(String v)   { this.version = v; }

    public String getArchiveUrl() { return archiveUrl; }
    public void   setArchiveUrl(String u) { this.archiveUrl = u; }

    public String getSha256()     { return sha256; }
    public void   setSha256(String s)    { this.sha256 = s; }

    public long   getLoadedAtMs() { return loadedAtMs; }
    public void   setLoadedAtMs(long t)  { this.loadedAtMs = t; }

    public ArchiveEntry getPreviousEntry()              { return previousEntry; }
    public void         setPreviousEntry(ArchiveEntry p) { this.previousEntry = p; }

    /** Returns {@code true} when a last-known-good archive is available to roll back to. */
    public boolean hasPreviousEntry() {
        return previousEntry != null
            && previousEntry.getArchiveUrl() != null && !previousEntry.getArchiveUrl().isBlank()
            && previousEntry.getSha256()     != null && !previousEntry.getSha256().isBlank();
    }

    @Override
    public String toString() {
        return "ArchiveEntry{version='" + version + "', loadedAtMs=" + loadedAtMs
            + ", url='" + archiveUrl + "'"
            + (previousEntry != null ? ", previous='" + previousEntry.getVersion() + "'" : "")
            + "}";
    }
}

