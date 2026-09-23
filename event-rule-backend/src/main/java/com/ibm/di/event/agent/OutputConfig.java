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
 * Maps an agent output event type to the Kafka topic it should be written to.
 *
 * <p>The {@link com.ibm.di.event.agent.StatefulAgentProcessor} emits
 * {@link com.ibm.di.event.agent.EventEnvelope} objects tagged with a logical
 * {@code eventType}.  The downstream routing step filters by this type and
 * writes matching envelopes to the configured {@link #topic}.
 */
public class OutputConfig implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    /** Logical output event type (e.g. {@code "fraud-alert"}, {@code "approved-transaction"}). */
    private String eventType;

    /** Kafka topic name where events of this type should be written. */
    private String topic;

    /** Optional human-readable description used in logging and monitoring. */
    private String description;

    // ── Constructors ──────────────────────────────────────────────────────────

    public OutputConfig() {}

    public OutputConfig(String eventType, String topic) {
        this.eventType = eventType;
        this.topic     = topic;
    }

    // ── Getters / Setters ─────────────────────────────────────────────────────

    public String getEventType()              { return eventType; }
    public void   setEventType(String et)     { this.eventType = et; }

    public String getTopic()                  { return topic; }
    public void   setTopic(String t)          { this.topic = t; }

    public String getDescription()            { return description; }
    public void   setDescription(String d)    { this.description = d; }

    @Override
    public String toString() {
        return "OutputConfig{eventType='" + eventType + "', topic='" + topic + "'}";
    }
}

