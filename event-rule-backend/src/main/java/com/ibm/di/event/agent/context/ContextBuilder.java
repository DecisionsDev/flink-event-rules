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
package com.ibm.di.event.agent.context;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.di.event.agent.AgentDescriptor;
import com.ibm.di.event.agent.AgentState;
import com.ibm.di.event.agent.EventEnvelope;

/**
 * Strategy interface for building the JSON input context passed to the
 * IBM Decision Intelligence rule engine.
 *
 * <p>Each implementation corresponds to one {@code contextFormat} value in
 * {@link com.ibm.di.event.agent.RulesetConfig}.  Implementations are stateless,
 * thread-safe, and registered in {@link ContextBuilderRegistry}.
 *
 * <p>Adding a new format only requires:
 * <ol>
 *   <li>Implementing this interface.</li>
 *   <li>Registering the implementation in {@link ContextBuilderRegistry#register}.</li>
 * </ol>
 *
 * No changes to {@link com.ibm.di.event.agent.StatefulAgentProcessor} are needed.
 */
@FunctionalInterface
public interface ContextBuilder {

    /**
     * Builds the Decision Intelligence input context for a single rule evaluation.
     *
     * @param descriptor the agent descriptor (provides ruleset config and subscriptions)
     * @param state      the current per-entity state (history + entity data)
     * @param event      the inbound event being evaluated
     * @param mapper     shared {@link ObjectMapper} for JSON construction
     * @return           the JSON node to pass as input to {@link com.ibm.di.event.rule.RuleEngineRunner#execute}
     */
    JsonNode build(AgentDescriptor descriptor, AgentState state, EventEnvelope event, ObjectMapper mapper);

}

