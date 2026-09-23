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
 * Top-level descriptor for a stateful Agent.
 *
 * <p>An Agent subscribes to one or more event types, keeps a bounded history of
 * events per entity, applies a ruleset (IBM Decision Intelligence archive), and emits
 * typed output events to designated Kafka topics.
 *
 * <p>For the full descriptor schema, field reference, and examples see
 * {@code docs/AGENT_PATTERN_DESIGN.md} and the sample descriptors under {@code samples/}.
 *
 * <p>The {@link #entityKeyExtractor} is a JSONPath expression (Jayway JsonPath)
 * (e.g. {@code "$.accountId"}, {@code "$.customer.id"}, or any deeper path like
 * {@code "$.a.b.c"}).  Resolved by {@link StatefulAgentProcessor}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class AgentDescriptor implements Serializable {

    @Serial
    private static final long serialVersionUID = 2L;

    /** Unique, stable identifier for this agent (used as Flink operator UID). */
    private String agentId;

    /** Human-readable name (used as Flink operator name and in logs). */
    private String name;

    /**
     * Business entity managed by this agent (e.g. {@code "account"}, {@code "customer"}).
     * Informational — the actual partitioning key is extracted by {@link #entityKeyExtractor}.
     */
    private String entityType;

    /**
     * JSONPath expression (Jayway JsonPath) to extract the entity ID from incoming events.
     * Supports arbitrary nesting depth, e.g. {@code "$.fieldName"} or {@code "$.a.b.c"}.
     */
    private String entityKeyExtractor;

    /** Event subscriptions defining which topics feed this agent and their history horizons. */
    private List<EventSubscription> subscriptions;

    /** Decision Intelligence ruleset applied to every incoming event. */
    private RulesetConfig ruleset;

    /** Output event types and their destination Kafka topics. */
    private List<OutputConfig> outputs;

    /** State configuration: TTL, backend preference. */
    private StateConfig stateConfig;

    /**
     * Flink parallelism for this agent's {@link StatefulAgentProcessor}.
     * {@code 0} means inherit the job-level default.
     */
    private int parallelism;

    /**
     * Whether this agent participates in direct in-process agent-to-agent chaining.
     *
     * <p>When {@code true} (the default), the agent may act as an upstream source
     * (its output stream is offered to downstream agents that subscribe to the same
     * event type) and as a downstream consumer (it may receive a chained inbound
     * stream from an upstream agent instead of reading exclusively from Kafka).
     *
     * <p>When {@code false}, the agent is wired independently: it always reads from
     * Kafka and its output stream is never forwarded directly to another agent.
     * No chain edges are created for this agent regardless of what other agents declare.
     *
     * <p>Omitting this field from a descriptor is equivalent to {@code true}.
     */
    private boolean chainingEnabled = true;

    // ── Constructors ──────────────────────────────────────────────────────────

    public AgentDescriptor() {}

    // ── Getters / Setters ─────────────────────────────────────────────────────

    public String getAgentId()                           { return agentId; }
    public void   setAgentId(String id)                  { this.agentId = id; }

    public String getName()                              { return name; }
    public void   setName(String n)                      { this.name = n; }

    public String getEntityType()                        { return entityType; }
    public void   setEntityType(String et)               { this.entityType = et; }

    public String getEntityKeyExtractor()                { return entityKeyExtractor; }
    public void   setEntityKeyExtractor(String eke)      { this.entityKeyExtractor = eke; }

    public List<EventSubscription> getSubscriptions()                        { return subscriptions; }
    public void                    setSubscriptions(List<EventSubscription> s) { this.subscriptions = s; }

    public RulesetConfig getRuleset()                       { return ruleset; }
    public void          setRuleset(RulesetConfig r)        { this.ruleset = r; }

    public List<OutputConfig> getOutputs()                     { return outputs; }
    public void               setOutputs(List<OutputConfig> o) { this.outputs = o; }

    public StateConfig getStateConfig()                   { return stateConfig; }
    public void        setStateConfig(StateConfig sc)     { this.stateConfig = sc; }

    public int  getParallelism()                          { return parallelism; }
    public void setParallelism(int p)                     { this.parallelism = p; }

    public boolean isChainingEnabled()                    { return chainingEnabled; }
    public void    setChainingEnabled(boolean ce)         { this.chainingEnabled = ce; }

    /** Display name for Flink operators; falls back to {@link #agentId}. */
    public String displayName() {
        return (name != null && !name.isBlank()) ? name
            : (agentId != null ? agentId : "unnamed-agent");
    }

    @Override
    public String toString() {
        return "AgentDescriptor{agentId='" + agentId + "', entityType='" + entityType
            + "', subscriptions=" + (subscriptions != null ? subscriptions.size() : 0) + "}";
    }
}

