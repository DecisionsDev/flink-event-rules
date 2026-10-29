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

/**
 * Maps an agent output event type to the Kafka topic it should be written to.
 *
 * <p>The {@link com.ibm.di.event.agent.StatefulAgentProcessor} emits
 * {@link com.ibm.di.event.agent.EventEnvelope} objects tagged with a logical
 * {@code eventType}.  The downstream routing step filters by this type and
 * writes matching envelopes to the configured {@link #topic}.
 *
 * <p>When {@link #sqlEnrichment} is set, emitted envelopes are passed through a
 * Flink SQL transform before reaching the Kafka sink.  The SQL file receives the
 * outbound stream as a temporary view named {@link SqlEnrichmentConfig#getInputViewName()}
 * and reads the enriched result from {@link SqlEnrichmentConfig#getOutputViewName()}.
 * The output view must expose the three conventional columns expected by
 * {@link com.ibm.di.event.sql.SqlEnrichmentRunner}: {@code timestamp}, {@code entity_id},
 * and {@code payload_json}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class OutputConfig implements Serializable {

    @Serial
    private static final long serialVersionUID = 2L;

    /** Logical output event type (e.g. {@code "fraud-alert"}, {@code "approved-transaction"}). */
    private String eventType;

    /** Kafka topic name where events of this type should be written. */
    private String topic;

    /** Optional human-readable description used in logging and monitoring. */
    private String description;

    /**
     * Path to a JSON Schema file describing the payload shape of outbound events of this type.
     * Required when {@link #sqlEnrichment} is set: the schema is used to map each
     * {@link com.ibm.di.event.agent.EventEnvelope} payload into a typed Flink {@code Row}
     * so that SQL can reference individual fields by name.
     */
    private String schema;

    /**
     * Optional SQL enrichment applied to outbound events of this type before they
     * reach the Kafka sink.  {@code null} means no enrichment (default behaviour).
     */
    private SqlEnrichmentConfig sqlEnrichment;

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

    public String getSchema()              { return schema; }
    public void   setSchema(String s)      { this.schema = s; }

    public SqlEnrichmentConfig getSqlEnrichment()                      { return sqlEnrichment; }
    public void                setSqlEnrichment(SqlEnrichmentConfig se) { this.sqlEnrichment = se; }

    /** Returns {@code true} when this output has an active SQL enrichment configuration. */
    public boolean hasSqlEnrichment() {
        return sqlEnrichment != null && sqlEnrichment.hasSqlFile();
    }

    @Override
    public String toString() {
        return "OutputConfig{eventType='" + eventType + "', topic='" + topic
            + "', sqlEnrichment=" + (hasSqlEnrichment() ? sqlEnrichment.getSqlFile() : "none") + "'}";
    }
}

