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
package com.ibm.di.event.agent.ptf;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ibm.di.event.agent.EventEnvelope;

/**
 * Serializes an {@link EventEnvelope} to the JSON written to Kafka by the DataStream engine
 * ({@code AgentBasedDataStreamJob.EnvelopeSerializer}):
 * {@code {agentEventType, entityId, sourceTopic, timestamp, payload, metadata?}}.
 *
 * <p>It is duplicated here, not shared, so that the Process Table Function artifact does not load
 * the DataStream job class and the Kafka connector it imports.
 */
public final class EnvelopeJson {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private EnvelopeJson() {}

    public static String write(EventEnvelope element) {
        ObjectNode out = MAPPER.createObjectNode();
        out.put("agentEventType", element.getEventType());
        out.put("entityId",       element.getEntityId() != null ? element.getEntityId() : "");
        out.put("sourceTopic",    element.getSourceTopic() != null ? element.getSourceTopic() : "");
        out.put("timestamp",      element.getTimestamp());
        if (element.getPayload() != null) {
            out.set("payload", element.getPayload());
        }
        if (element.getMetadata() != null && !element.getMetadata().isEmpty()) {
            out.set("metadata", MAPPER.valueToTree(element.getMetadata()));
        }
        return out.toString();
    }
}
