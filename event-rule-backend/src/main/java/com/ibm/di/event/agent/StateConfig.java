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
package com.ibm.di.event.agent;

import java.io.Serial;
import java.io.Serializable;
import java.time.Duration;

/**
 * Flink state management configuration for a {@link StatefulAgentProcessor}.
 *
 * <p>All fields are optional; safe defaults are applied when absent:
 * <ul>
 *   <li>{@link #ttl} — no TTL (state lives forever); recommended to set in production</li>
 *   <li>{@link #backend} — Flink's job-default state backend is used</li>
 *   <li>{@link #checkpointInterval} — Flink's job-default checkpoint interval is used</li>
 *   <li>{@link #changeLogTopic} — no changelog is published when absent</li>
 * </ul>
 */
public class StateConfig implements Serializable {

    @Serial
    private static final long serialVersionUID = 2L;

    /**
     * State time-to-live. Supports the same format as {@link HorizonConfig#getTimeLimit()}:
     * {@code "30d"}, {@code "24h"}, {@code "60m"}, {@code "90s"}.
     * When {@code null} or blank, no TTL is configured.
     */
    private String ttl;

    /**
     * Preferred state backend hint: {@code "rocksdb"} or {@code "heap"}.
     * The actual backend is always determined by the Flink job configuration;
     * this field is informational only at the descriptor level.
     */
    private String backend;

    /**
     * Preferred checkpoint interval hint (same format as TTL).
     * Informational only — the job's global checkpoint interval takes precedence.
     */
    private String checkpointInterval;

    /**
     * Optional Kafka topic name for entity-state change events.
     *
     * <p>When set, the {@link StatefulAgentProcessor} emits a side-output record to this
     * topic every time the entity data is updated by the rule engine (i.e. the rule
     * returned a non-null entity that differs from the entity sent as input).
     *
     * <p>Each record is a JSON object with the following structure:
     * <pre>
     * {
     *   "entityId":   "...",
     *   "entityType": "...",
     *   "timestamp":  1234567890,
     *   "entity":     { ... }
     * }
     * </pre>
     *
     * <p>The Kafka record key is the entity ID (UTF-8), matching the convention used for
     * all other agent output topics so consumers can partition by entity.
     *
     * <p>When {@code null} or blank, no changelog is published.
     */
    private String changeLogTopic;

    // ── Constructors ──────────────────────────────────────────────────────────

    public StateConfig() {}

    // ── Derived helpers ───────────────────────────────────────────────────────

    /**
     * Returns the configured TTL as a {@link Duration}, or {@code null} when no TTL is set.
     * Used by {@link StatefulAgentProcessor#open} to configure Flink {@code StateTtlConfig}.
     */
    public Duration ttlDuration() {
        if (ttl == null || ttl.isBlank()) return null;
        // Re-use HorizonConfig's parser (same unit syntax)
        long ms = HorizonConfig.time(ttl).timeLimitMs();
        return ms > 0 ? Duration.ofMillis(ms) : null;
    }

    // ── Getters / Setters ─────────────────────────────────────────────────────

    public String getTtl()                 { return ttl; }
    public void   setTtl(String t)         { this.ttl = t; }

    public String getBackend()             { return backend; }
    public void   setBackend(String b)     { this.backend = b; }

    public String getCheckpointInterval()              { return checkpointInterval; }
    public void   setCheckpointInterval(String ci)     { this.checkpointInterval = ci; }

    public String getChangeLogTopic()                  { return changeLogTopic; }
    public void   setChangeLogTopic(String t)          { this.changeLogTopic = t; }

    /** Returns {@code true} when a changelog topic is configured. */
    public boolean hasChangeLogTopic() {
        return changeLogTopic != null && !changeLogTopic.isBlank();
    }

    @Override
    public String toString() {
        return "StateConfig{ttl='" + ttl + "', backend='" + backend
            + "', changeLogTopic='" + changeLogTopic + "'}";
    }
}

