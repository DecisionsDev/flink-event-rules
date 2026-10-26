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
 * Declares that an agent wants to receive events of a given logical type from a
 * Kafka topic and keep a bounded history of those events per entity.
 *
 * <p>The {@code eventType} is the logical name (e.g. {@code "transaction"},
 * {@code "account-update"}) used as the key in the agent's event-history map.
 * The {@code topic} names the single Kafka topic whose events belong to this type.
 */
public class EventSubscription implements Serializable {

    @Serial
    private static final long serialVersionUID = 4L;

    /** Logical event-type label — used as the history-map key. */
    private String eventType;

    /** Kafka topic name whose events belong to this subscription. */
    private String topic;

    /**
     * The IBM Decision Intelligence operation name to invoke when an event of this type arrives.
     *
     * <p>This is the operation name passed to {@link com.ibm.di.event.rule.RuleEngineRunner}
     * (e.g. {@code "EvaluateTransaction"}).  Each subscription independently names the
     * operation that handles its event type; each operation name must be unique within the agent.
     *
     * <p>When {@code null} or blank, the event is stored in history and horizon eviction
     * is applied, but the rule engine is <strong>not</strong> invoked and no output event
     * is emitted.  This supports the "subscribe for context, skip processing" pattern.
     */
    private String operation;

    /**
     * Optional JSONPath expression overriding the agent-level {@code entityKeyExtractor}
     * for events of this subscription.  {@code null} means inherit the agent-level value;
     * {@code ""} is the explicit single-partition opt-in.  All subscriptions of an agent
     * must resolve to the same entity ID for the same real-world entity, because events
     * are keyed after the subscription streams are unioned.
     */
    private String entityKeyExtractor;

    /** Controls how many historical events are retained per entity for this type. */
    private HorizonConfig horizon = HorizonConfig.count(0); // default: no history

    // ── Constructors ──────────────────────────────────────────────────────────

    public EventSubscription() {}

    /**
     * Creates a subscription with an explicit operation.
     *
     * <p>Pass {@code ""} (empty string) as {@code operation} to configure a
     * <em>history-only</em> subscription: the event is stored in history and horizon
     * eviction is applied, but the rule engine is <strong>not</strong> invoked and no
     * output event is emitted.
     *
     * @param eventType  logical event-type label (history-map key)
     * @param topic      Kafka topic name whose events belong to this subscription
     * @param operation  IBM Decision Intelligence operation name; {@code ""} for history-only
     * @param horizon    how many historical events to retain per entity
     */
    public EventSubscription(String eventType, String topic,
                             String operation, HorizonConfig horizon) {
        this.eventType = eventType;
        this.topic     = topic;
        this.operation = operation;
        this.horizon   = horizon != null ? horizon : HorizonConfig.count(0);
    }

    /**
     * Creates a history-only subscription (no operation).
     *
     * @param eventType  logical event-type label (history-map key)
     * @param topic      Kafka topic name whose events belong to this subscription
     * @param horizon    how many historical events to retain per entity
     */
    public EventSubscription(String eventType, String topic, HorizonConfig horizon) {
        this.eventType = eventType;
        this.topic     = topic;
        this.horizon   = horizon != null ? horizon : HorizonConfig.count(0);
    }

    // ── Getters / Setters ─────────────────────────────────────────────────────

    public String getEventType()              { return eventType; }
    public void   setEventType(String et)     { this.eventType = et; }

    public String getTopic()                  { return topic; }
    public void   setTopic(String t)          { this.topic = t; }

    public String getOperation()              { return operation; }
    public void   setOperation(String op)     { this.operation = op; }

    public String getEntityKeyExtractor()           { return entityKeyExtractor; }
    public void   setEntityKeyExtractor(String eke) { this.entityKeyExtractor = eke; }

    /** Returns {@code true} when this subscription has a non-blank operation name. */
    public boolean hasOperation() {
        return operation != null && !operation.isBlank();
    }

    /**
     * Returns {@code true} when this subscription declares the {@code Schedule} event type,
     * i.e. the events the agent synthesises when a schedule fires (see {@link ScheduleManager}).
     * Such a subscription has no Kafka topic and no horizon: no source is wired for it.
     */
    @com.fasterxml.jackson.annotation.JsonIgnore
    public boolean isScheduleSubscription() {
        return ScheduleManager.SCHEDULE_EVENT_TYPE.equals(eventType);
    }

    public HorizonConfig getHorizon()             { return horizon; }
    public void          setHorizon(HorizonConfig h) {
        this.horizon = h != null ? h : HorizonConfig.count(0);
    }

    @Override
    public String toString() {
        return "EventSubscription{eventType='" + eventType + "', topic='" + topic
            + "', operation='" + operation + "', horizon=" + horizon + "}";
    }
}

