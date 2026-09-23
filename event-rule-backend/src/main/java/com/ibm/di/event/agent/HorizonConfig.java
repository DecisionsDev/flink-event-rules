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

/**
 * Configures how many historical events an {@link AgentState.EventHistory} retains
 * per entity and event-type subscription.
 *
 * <h3>Horizon types</h3>
 * <dl>
 *   <dt>{@code "count"}</dt>
 *   <dd>Keep the last {@link #value} events.  Set {@code value=0} to disable history
 *       (current-event only, no state accumulation).</dd>
 *   <dt>{@code "time"}</dt>
 *   <dd>Keep all events whose age is ≤ {@link #timeLimit}.  Supported units:
 *       {@code s} (seconds), {@code m} (minutes), {@code h} (hours), {@code d} (days).
 *       Example: {@code "24h"}.</dd>
 *   <dt>{@code "hybrid"}</dt>
 *   <dd>Keep the last {@link #countLimit} events <em>and</em> only those within
 *       {@link #timeLimit}.  Whichever bound is reached first triggers eviction.
 *       Recommended for production to prevent unbounded state growth.</dd>
 * </dl>
 */
public class HorizonConfig implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    /** Horizon type: {@code "count"}, {@code "time"}, or {@code "hybrid"}. */
    private String type = "count";

    /** Maximum event count.  Used by {@code "count"} and as the count ceiling for {@code "hybrid"}. */
    private int value = 0;

    /** Count ceiling for {@code "hybrid"} horizon. Alias for clarity in JSON. */
    private int countLimit = 0;

    /**
     * Age ceiling.  Supports {@code "24h"}, {@code "7d"}, {@code "30m"}, {@code "90s"}.
     * Used by {@code "time"} and {@code "hybrid"} horizons.
     */
    private String timeLimit;

    /** Human-readable description for observability / logging. */
    private String description;

    // ── Constructors ──────────────────────────────────────────────────────────

    public HorizonConfig() {}

    /** Convenience factory for a count-based horizon. */
    public static HorizonConfig count(int limit) {
        HorizonConfig h = new HorizonConfig();
        h.type  = "count";
        h.value = limit;
        return h;
    }

    /** Convenience factory for a time-based horizon. */
    public static HorizonConfig time(String timeLimit) {
        HorizonConfig h = new HorizonConfig();
        h.type      = "time";
        h.timeLimit = timeLimit;
        return h;
    }

    /** Convenience factory for a hybrid horizon. */
    public static HorizonConfig hybrid(int countLimit, String timeLimit) {
        HorizonConfig h = new HorizonConfig();
        h.type       = "hybrid";
        h.countLimit = countLimit;
        h.value      = countLimit; // keep both in sync
        h.timeLimit  = timeLimit;
        return h;
    }

    // ── Derived helpers ───────────────────────────────────────────────────────

    /**
     * Returns the effective count ceiling: {@link #countLimit} when set (hybrid),
     * otherwise {@link #value}.  A value of {@code 0} means unlimited count (time-only).
     */
    public int effectiveCountLimit() {
        return countLimit > 0 ? countLimit : value;
    }

    /**
     * Parses {@link #timeLimit} into milliseconds.  Returns {@code 0} when
     * {@code timeLimit} is null or blank (i.e. no time ceiling).
     *
     * @throws IllegalArgumentException if the string is non-blank but unparseable
     */
    public long timeLimitMs() {
        if (timeLimit == null || timeLimit.isBlank()) return 0L;
        String s = timeLimit.trim().toLowerCase();
        try {
            if (s.endsWith("d"))  return Long.parseLong(s.substring(0, s.length() - 1)) * 86_400_000L;
            if (s.endsWith("h"))  return Long.parseLong(s.substring(0, s.length() - 1)) *  3_600_000L;
            if (s.endsWith("m"))  return Long.parseLong(s.substring(0, s.length() - 1)) *     60_000L;
            if (s.endsWith("s"))  return Long.parseLong(s.substring(0, s.length() - 1)) *      1_000L;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                "Unparseable timeLimit '" + timeLimit + "'. Expected format: 30d / 24h / 60m / 90s", e);
        }
        throw new IllegalArgumentException(
            "Unparseable timeLimit '" + timeLimit + "'. Missing unit suffix (d/h/m/s).");
    }

    // ── Getters / Setters ─────────────────────────────────────────────────────

    public String getType()        { return type; }
    public void   setType(String t) { this.type = t; }

    public int  getValue()         { return value; }
    public void setValue(int v)    { this.value = v; }

    public int  getCountLimit()          { return countLimit; }
    public void setCountLimit(int limit) { this.countLimit = limit; }

    public String getTimeLimit()           { return timeLimit; }
    public void   setTimeLimit(String tl)  { this.timeLimit = tl; }

    public String getDescription()           { return description; }
    public void   setDescription(String d)   { this.description = d; }

    @Override
    public String toString() {
        return "HorizonConfig{type='" + type + "', value=" + value
            + ", countLimit=" + countLimit + ", timeLimit='" + timeLimit + "'}";
    }
}

