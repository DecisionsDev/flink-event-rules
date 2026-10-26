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
import java.time.Instant;

/**
 * A request, emitted by a rule, to start or cancel an event-time schedule for the current entity.
 *
 * <p>Instances with status {@link Status#START} are persisted in Flink keyed state until the
 * corresponding event-time timer fires or the request is cancelled.  The class follows the
 * Flink POJO rules (public no-arg constructor, private fields with getters/setters) so that
 * Flink can serialize it efficiently.
 *
 * <p>See {@code docs/AGENT_PATTERN_DESIGN.md} §7.4 for the full contract.
 */
public class ScheduleRequest implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    /** What the request asks the agent to do. */
    public enum Status { START, CANCEL, CANCELALL }

    /** Identifier of the schedule, unique per entity. */
    private String id;

    /** Free-text description, copied into the {@code Schedule} event when the timer fires. */
    private String description;

    /** Time at which the schedule fires, ISO-8601 UTC, e.g. {@code 2026-10-18T23:29:16Z}. */
    private String timepoint;

    private Status status;

    public ScheduleRequest() {}

    public ScheduleRequest(String id, String description, String timepoint, Status status) {
        this.id          = id;
        this.description = description;
        this.timepoint   = timepoint;
        this.status      = status;
    }

    /** The timepoint as epoch milliseconds (the Flink event-time timer timestamp). */
    public long timepointMs() {
        return Instant.parse(timepoint).toEpochMilli();
    }

    public String getId()                    { return id; }
    public void   setId(String id)           { this.id = id; }

    public String getDescription()           { return description; }
    public void   setDescription(String d)   { this.description = d; }

    public String getTimepoint()             { return timepoint; }
    public void   setTimepoint(String t)     { this.timepoint = t; }

    public Status getStatus()                { return status; }
    public void   setStatus(Status s)        { this.status = s; }

    @Override
    public String toString() {
        return "ScheduleRequest{id='" + id + "', timepoint='" + timepoint
            + "', status=" + status + "}";
    }
}
