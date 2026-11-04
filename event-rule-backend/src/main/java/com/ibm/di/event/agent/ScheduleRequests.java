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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * Engine-independent handling of {@link ScheduleRequest}s: parsing and validation of the
 * requests returned by the rule engine, and construction of the {@code Schedule} event created
 * when a schedule fires. Shared by the DataStream and PTF implementations.
 */
public final class ScheduleRequests {

    private static final Logger LOG = LoggerFactory.getLogger(ScheduleRequests.class);

    /** Event type of the events created when a schedule fires. Reserved. */
    public static final String SCHEDULE_EVENT_TYPE = "Schedule";

    private ScheduleRequests() {}

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

    /**
     * Builds the {@code Schedule} event: {@code {"timestamp": "...", "id": "...",
     * "description": "..."}}. It has no source topic.
     */
    public static EventEnvelope buildEvent(ScheduleRequest r, long timestamp, String entityId) {
        ObjectNode payload = JsonNodeFactory.instance.objectNode();
        payload.put("timestamp",   Instant.ofEpochMilli(timestamp).toString());
        payload.put("id",          r.getId());
        payload.put("description", r.getDescription() != null ? r.getDescription() : "");

        EventEnvelope env = EventEnvelope.from(SCHEDULE_EVENT_TYPE, null, timestamp, payload);
        env.setEntityId(entityId);
        return env;
    }
}
