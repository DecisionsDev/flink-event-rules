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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serial;
import java.io.Serializable;
import java.util.List;

/**
 * References the IBM Decision Intelligence archive JAR that the agent applies to each event.
 *
 * <p>When {@link #archiveControlTopic} is set, the agent subscribes to that Kafka topic for
 * {@link com.ibm.di.event.rule.broadcast.ArchiveControlCmd} messages, enabling hot-swap of the
 * decision archive without restarting the Flink job.  When {@code null} or blank, the static JAR
 * at {@link #path} is used for the lifetime of the job.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class RulesetConfig implements Serializable {

    @Serial
    private static final long serialVersionUID = 9L;

    /**
     * Maps one event-type bucket in the history to a domain-specific field name.
     *
     * <p>Example:
     * <pre>
     * { "eventType": "Transaction", "field": "thePastTransactions" }
     * </pre>
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class HistoryFieldMapping implements Serializable {

        @Serial
        private static final long serialVersionUID = 1L;

        /** The event type whose history entries should be placed under {@link #field}. */
        private String eventType;

        /** Domain-specific field name for this event type's history list. */
        private String field;

        public HistoryFieldMapping() {}

        public HistoryFieldMapping(String eventType, String field) {
            this.eventType = eventType;
            this.field     = field;
        }

        public String getEventType()              { return eventType; }
        public void   setEventType(String et)     { this.eventType = et; }

        public String getField()                  { return field; }
        public void   setField(String f)          { this.field = f; }

        @Override
        public String toString() {
            return "HistoryFieldMapping{eventType='" + eventType + "', field='" + field + "'}";
        }
    }

    /**
     * Maps a single outbound-events array field in the rule result to an event type
     * that will be assigned to every element emitted from that array.
     *
     * <p>Example:
     * <pre>
     * { "field": "theFraudAlerts", "eventType": "Alert" }
     * </pre>
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class OutboundConfig implements Serializable {

        @Serial
        private static final long serialVersionUID = 1L;

        /**
         * JSON name of the data-model attribute in the Decision Service result that carries
         * the outbound events array. Must match the JSON name exactly (not the display name).
         * Defaults to {@code "outputEvents"} when absent or blank.
         */
        private String field;

        /**
         * Event type label assigned to every event emitted from this outbound-events array.
         */
        private String eventType;

        public OutboundConfig() {}

        public OutboundConfig(String field, String eventType) {
            this.field     = field;
            this.eventType = eventType;
        }

        public String getField()                  { return field; }
        public void   setField(String f)          { this.field = f; }

        public String getEventType()              { return eventType; }
        public void   setEventType(String t)      { this.eventType = t; }

        @Override
        public String toString() {
            return "OutboundConfig{field='" + field + "', eventType='" + eventType + "'}";
        }
    }

    /**
     * Per-operation field-name mappings that tell the generic context builder which
     * domain-specific names the Decision Intelligence archive uses for the standard
     * {@code entity}, {@code currentEvent}, {@code history}, and {@code outboundEvents}
     * input fields.
     *
     * <p>When an {@link EventSubscription}'s {@link EventSubscription#getOperation() operation}
     * matches an entry here, the generic context builder exposes the entity, current event,
     * per-type history lists, and outbound-events lists under the configured field names
     * instead of (or in addition to) the canonical generic names.
     *
     * <p>For the full field contract, JSON schema, and examples see
     * {@code docs/AGENT_PATTERN_DESIGN.md}.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class OperationConfig implements Serializable {

        @Serial
        private static final long serialVersionUID = 4L;

        /** The IBM Decision Intelligence operation name this mapping applies to. */
        private String operation;

        /**
         * Domain-specific field name for the entity object inside the rule input context.
         * When non-null the entity is exposed under this key instead of (or alongside)
         * the canonical {@code "entity"} key.
         * A field with this name may also be present in the rule output context, indicating
         * that the entity has been modified by the rules (unless overridden by {@link #outboundEntityField}).
         */
        private String entityField;

        /**
         * Optional domain-specific field name for the entity object inside the rule result.
         * When set, {@code StatefulAgentProcessor.updateEntityState()} reads the modified entity
         * state from this field in the rule execution result instead of {@link #entityField}.
         * If unset or blank, {@link #entityField} is used.
         */
        private String outboundEntityField;

        /**
         * Domain-specific field name for the current event inside the rule input context.
         * When non-null the current event is exposed under this key instead of (or alongside)
         * the canonical {@code "currentEvent"} key.
         */
        private String currentEventField;

        /**
         * Per-event-type history field mappings.
         *
         * <p>Each entry maps one event-type bucket to a domain-specific field name.
         * The generic context builder will add a separate array field for every entry,
         * containing only the history events whose {@code eventType} matches.
         *
         * <p>The canonical flat {@code "history"} array (all event types merged) is always
         * present regardless of this configuration.
         */
        private List<HistoryFieldMapping> history;

        /**
         * Outbound event descriptors: each entry maps a result-field name to the event type
         * that will be assigned to every element emitted from that array.
         *
         * <p>Replaces the former single {@code outboundEventsField} / {@code outboundEventType}
         * pair, enabling an operation to emit several distinct event types from different
         * result-object fields in a single rule invocation.
         *
         * <p>{@code null} or empty means the operation emits no outbound events.
         */
        private List<OutboundConfig> outbounds;

        /**
         * Optional. JSON name of the data-model attribute in the Decision Service result that
         * holds the list of {@link ScheduleRequest}s the operation emits. Must match the JSON
         * name exactly. {@code null} or blank means the operation never schedules anything.
         */
        private String scheduleRequestsField;

        public OperationConfig() {}

        public String getOperation()                      { return operation; }
        public void   setOperation(String op)             { this.operation = op; }

        public String getEntityField()                    { return entityField; }
        public void   setEntityField(String f)            { this.entityField = f; }

        public String getOutboundEntityField()              { return outboundEntityField; }
        public void   setOutboundEntityField(String f)      { this.outboundEntityField = f; }

        public String getCurrentEventField()              { return currentEventField; }
        public void   setCurrentEventField(String f)      { this.currentEventField = f; }

        public List<HistoryFieldMapping> getHistory()                    { return history; }
        public void                      setHistory(List<HistoryFieldMapping> h) { this.history = h; }

        public List<OutboundConfig> getOutbounds()                     { return outbounds; }
        public void                 setOutbounds(List<OutboundConfig> o) { this.outbounds = o; }

        public String getScheduleRequestsField()              { return scheduleRequestsField; }
        public void   setScheduleRequestsField(String f)      { this.scheduleRequestsField = f; }

        /** Returns the schedule-requests result field name, or {@code null} when not configured. */
        public String scheduleRequestsField() {
            return scheduleRequestsField != null && !scheduleRequestsField.isBlank()
                ? scheduleRequestsField : null;
        }

        @Override
        public String toString() {
            return "OperationConfig{operation='" + operation
                + "', entityField='" + entityField
                + "', outboundEntityField='" + outboundEntityField
                + "', currentEventField='" + currentEventField
                + "', history=" + (history != null ? history.size() : 0) + " mappings"
                + "', outbounds=" + (outbounds != null ? outbounds.size() : 0) + " entries}";
        }
    }

    /** Path to the decision archive JAR. */
    private String path;

    /**
     * Per-operation field-name mappings.  Keyed by operation name inside the processor
     * via {@link #findOperation(String)}.
     */
    private List<OperationConfig> operations;

    /**
     * Kafka topic for {@link com.ibm.di.event.rule.broadcast.ArchiveControlCmd} messages.
     * When set, enables live archive hot-swap via broadcast state.
     * Leave {@code null} or blank to disable (static JAR only).
     */
    private String archiveControlTopic;

    // ── Constructors ──────────────────────────────────────────────────────────

    /** No-arg constructor for Jackson deserialization. */
    public RulesetConfig() {}

    public RulesetConfig(String path) {
        this.path = path;
    }

    // ── Getters / Setters ─────────────────────────────────────────────────────

    public String getPath()                      { return path; }
    public void   setPath(String p)              { this.path = p; }

    public List<OperationConfig> getOperations()                    { return operations; }
    public void                  setOperations(List<OperationConfig> ops) { this.operations = ops; }

    /**
     * Returns the {@link OperationConfig} for the given operation name, or {@code null}
     * if none is configured.
     */
    public OperationConfig findOperation(String operationName) {
        if (operations == null || operationName == null) return null;
        return operations.stream()
            .filter(op -> operationName.equals(op.getOperation()))
            .findFirst()
            .orElse(null);
    }

    public String getArchiveControlTopic()            { return archiveControlTopic; }
    public void   setArchiveControlTopic(String topic) { this.archiveControlTopic = topic; }

    /** Returns {@code true} when broadcast-state archive reload is configured. */
    public boolean hasBroadcastReload() {
        return archiveControlTopic != null && !archiveControlTopic.isBlank();
    }

    @Override
    public String toString() {
        return "RulesetConfig{path='" + path
            + "', operations=" + (operations != null ? operations.size() : 0)
            + "', archiveControlTopic='" + archiveControlTopic + "'}";
    }
}

