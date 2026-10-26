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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.streaming.api.TimerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * Keeps the {@link ScheduleRequest}s of the current entity in keyed state and keeps the Flink
 * event-time timers in sync with them.
 *
 * <p>Two {@link MapState}s are used, both scoped to the current key (the entity):
 * <ul>
 *   <li>{@code byId}: schedule id to request, used to replace or cancel a schedule by id;</li>
 *   <li>{@code idsByTime}: timepoint (epoch ms) to the ids registered at that timepoint, used to
 *       know when the last request of a timepoint is gone and its timer can be deleted, and to
 *       find the requests to fire in {@code onTimer}.</li>
 * </ul>
 *
 * <p>Several requests may share a timepoint; the Flink timer for a timepoint is deleted only when
 * no request is left for it.
 *
 * <p>This class has no dependency on the Flink runtime beyond the {@link MapState} and
 * {@link TimerService} interfaces, so it can be unit-tested with in-memory implementations.
 */
public class ScheduleManager {

    private static final Logger LOG = LoggerFactory.getLogger(ScheduleManager.class);

    /** Event type of the events created when a schedule fires. Reserved. */
    public static final String SCHEDULE_EVENT_TYPE = "Schedule";

    private final MapState<String, ScheduleRequest> byId;
    private final MapState<Long, List<String>>      idsByTime;
    private final String                            tag;

    public ScheduleManager(MapState<String, ScheduleRequest> byId,
                           MapState<Long, List<String>> idsByTime,
                           String tag) {
        this.byId      = byId;
        this.idsByTime = idsByTime;
        this.tag       = tag;
    }

    // ── Parsing / validation ──────────────────────────────────────────────────

    /**
     * Parses and validates the list of schedule requests returned by the rule engine.
     * Invalid entries are logged at WARN level and skipped; they never fail event processing.
     *
     * @param node the result field holding the list; {@code null}, missing or JSON null yields
     *             an empty list
     */
    public static List<ScheduleRequest> parseRequests(JsonNode node, String tag) {
        if (node == null || node.isNull() || node.isMissingNode()) return Collections.emptyList();
        if (!node.isArray()) {
            LOG.warn("[{}] Schedule request field is not an array — ignoring", tag);
            return Collections.emptyList();
        }
        List<ScheduleRequest> requests = new ArrayList<>();
        for (JsonNode item : node) {
            try {
                requests.add(validate(item));
            } catch (IllegalArgumentException e) {
                LOG.warn("[{}] Invalid ScheduleRequest ignored ({}): {}", tag, e.getMessage(), item);
            }
        }
        return requests;
    }

    /**
     * Validates one schedule request payload.
     *
     * <ul>
     *   <li>{@code status} is required: {@code START}, {@code CANCEL} or {@code CANCELALL}
     *       (case-insensitive);</li>
     *   <li>{@code id} is required for {@code START} and {@code CANCEL};</li>
     *   <li>{@code timepoint} is required for {@code START}, ISO-8601 UTC such as
     *       {@code 2026-10-18T23:29:16Z};</li>
     *   <li>{@code description}, when present, must be a string.</li>
     * </ul>
     *
     * @throws IllegalArgumentException describing the first problem found
     */
    public static ScheduleRequest validate(JsonNode n) {
        if (n == null || !n.isObject()) {
            throw new IllegalArgumentException("not a JSON object");
        }
        JsonNode statusNode = n.get("status");
        if (statusNode == null || !statusNode.isTextual()) {
            throw new IllegalArgumentException("status is missing or not a string");
        }
        ScheduleRequest.Status status;
        try {
            status = ScheduleRequest.Status.valueOf(
                statusNode.asText().trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                "unknown status '" + statusNode.asText() + "' (expected START, CANCEL or CANCELALL)");
        }

        JsonNode idNode = n.get("id");
        String id = null;
        if (idNode != null && !idNode.isNull()) {
            if (!idNode.isTextual()) throw new IllegalArgumentException("id is not a string");
            id = idNode.asText();
        }
        if (status != ScheduleRequest.Status.CANCELALL && (id == null || id.isBlank())) {
            throw new IllegalArgumentException("id is required for status " + status);
        }

        JsonNode descNode = n.get("description");
        String description = "";
        if (descNode != null && !descNode.isNull()) {
            if (!descNode.isTextual()) throw new IllegalArgumentException("description is not a string");
            description = descNode.asText();
        }

        String timepoint = null;
        if (status == ScheduleRequest.Status.START) {
            JsonNode tpNode = n.get("timepoint");
            if (tpNode == null || !tpNode.isTextual()) {
                throw new IllegalArgumentException("timepoint is missing or not a string");
            }
            timepoint = tpNode.asText().trim();
            try {
                Instant.parse(timepoint);
            } catch (DateTimeParseException e) {
                throw new IllegalArgumentException(
                    "timepoint '" + timepoint + "' is not an ISO-8601 UTC date-time "
                    + "such as 2026-10-18T23:29:16Z");
            }
        }
        return new ScheduleRequest(id, description, timepoint, status);
    }

    // ── Applying requests ─────────────────────────────────────────────────────

    /** Applies the requests in order. */
    public void apply(List<ScheduleRequest> requests, TimerService timerService) throws Exception {
        for (ScheduleRequest r : requests) {
            switch (r.getStatus()) {
                case START     -> start(r, timerService);
                case CANCEL    -> {
                    if (!cancel(r.getId(), timerService)) {
                        LOG.debug("[{}] CANCEL ignored: no schedule with id={}", tag, r.getId());
                    }
                }
                case CANCELALL -> cancelAll(timerService);
            }
        }
    }

    /** Registers the schedule; an existing schedule with the same id is cancelled first. */
    private void start(ScheduleRequest r, TimerService timerService) throws Exception {
        cancel(r.getId(), timerService);
        long ms = r.timepointMs();
        byId.put(r.getId(), r);
        List<String> ids = idsByTime.get(ms);
        List<String> updated = ids == null ? new ArrayList<>() : new ArrayList<>(ids);
        updated.add(r.getId());
        idsByTime.put(ms, updated);
        timerService.registerEventTimeTimer(ms);
        LOG.debug("[{}] Schedule started id={} timepoint={}", tag, r.getId(), r.getTimepoint());
    }

    /**
     * Removes the schedule with the given id. The Flink timer is deleted only when no other
     * request is registered at the same timepoint.
     *
     * @return {@code true} if a schedule with this id existed
     */
    private boolean cancel(String id, TimerService timerService) throws Exception {
        ScheduleRequest existing = byId.get(id);
        if (existing == null) return false;
        byId.remove(id);
        long ms = existing.timepointMs();
        List<String> ids = idsByTime.get(ms);
        if (ids != null) {
            List<String> remaining = new ArrayList<>(ids);
            remaining.remove(id);
            if (remaining.isEmpty()) {
                idsByTime.remove(ms);
                timerService.deleteEventTimeTimer(ms);
            } else {
                idsByTime.put(ms, remaining);
            }
        }
        LOG.debug("[{}] Schedule cancelled id={}", tag, id);
        return true;
    }

    /** Cancels every schedule of the current entity. */
    public void cancelAll(TimerService timerService) throws Exception {
        List<Long> timepoints = new ArrayList<>();
        for (Long ms : idsByTime.keys()) timepoints.add(ms);
        for (Long ms : timepoints) timerService.deleteEventTimeTimer(ms);
        idsByTime.clear();
        byId.clear();
        LOG.debug("[{}] All schedules cancelled ({} timepoint(s))", tag, timepoints.size());
    }

    // ── Firing ────────────────────────────────────────────────────────────────

    /**
     * Removes the requests registered at {@code timestamp} from state and returns one
     * {@code Schedule} event per request. Called from {@code onTimer}; Flink has already
     * discarded the fired timer so it is not deleted here.
     *
     * <p>The state is updated <em>before</em> the events are processed by the caller, so a rule
     * that reacts to a {@code Schedule} event by starting a new schedule sees consistent state.
     *
     * @param timestamp the timer timestamp passed to {@code onTimer}
     * @param entityId  the entity key of the current timer
     */
    public List<EventEnvelope> fire(long timestamp, String entityId) throws Exception {
        List<String> ids = idsByTime.get(timestamp);
        if (ids == null) return Collections.emptyList();
        idsByTime.remove(timestamp);

        List<EventEnvelope> events = new ArrayList<>();
        for (String id : ids) {
            ScheduleRequest r = byId.get(id);
            if (r == null) continue;
            byId.remove(id);
            events.add(buildEvent(r, timestamp, entityId));
        }
        return events;
    }

    /**
     * Builds the {@code Schedule} event: {@code {"timestamp": "...", "id": "...",
     * "description": "..."}}. It has no source topic.
     */
    static EventEnvelope buildEvent(ScheduleRequest r, long timestamp, String entityId) {
        ObjectNode payload = JsonNodeFactory.instance.objectNode();
        payload.put("timestamp",   Instant.ofEpochMilli(timestamp).toString());
        payload.put("id",          r.getId());
        payload.put("description", r.getDescription() != null ? r.getDescription() : "");

        EventEnvelope env = EventEnvelope.from(SCHEDULE_EVENT_TYPE, null, timestamp, payload);
        env.setEntityId(entityId);
        return env;
    }

    /**
     * Return the list of planned scheduled requests still registered a timestamp and prior timepoints.
     * Return an ordered list, from the oldest timestamp to the newest
     */
    public List<Long> getAllScheduledTimestamps(long timestamp, String currentKey) throws Exception {
        List<Long> result = new ArrayList<>();
        for (Long ms : idsByTime.keys()) {
            if (ms <= timestamp) result.add(ms);
        }
        Collections.sort(result);
        return result;
    }
}
