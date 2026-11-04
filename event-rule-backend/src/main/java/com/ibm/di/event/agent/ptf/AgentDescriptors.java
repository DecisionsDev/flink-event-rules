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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.di.event.agent.AgentDescriptor;

/**
 * JSON (de)serialization of an {@link AgentDescriptor}, used to hand a descriptor to a function
 * that Confluent Cloud instantiates itself (public no-argument constructor).
 *
 * <p>The mapper ignores unknown properties: serializing a descriptor also writes derived
 * properties (for example {@code scheduleSubscription}) that have no setter.
 */
public final class AgentDescriptors {

    private static final ObjectMapper MAPPER = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private AgentDescriptors() {}

    public static String toJson(AgentDescriptor descriptor) {
        try {
            return MAPPER.writeValueAsString(descriptor);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot serialize descriptor "
                + descriptor.getAgentId() + ": " + e.getMessage(), e);
        }
    }

    public static AgentDescriptor fromJson(String json) {
        try {
            return MAPPER.readValue(json, AgentDescriptor.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid agent descriptor JSON: " + e.getMessage(), e);
        }
    }
}
