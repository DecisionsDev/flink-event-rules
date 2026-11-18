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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("AgentDescriptorValidator unit tests")
class AgentDescriptorValidatorTest {

    private AgentDescriptor validAgent(String agentId) {
        AgentDescriptor d = new AgentDescriptor();
        d.setAgentId(agentId);
        d.setEntityType("Entity");
        d.setEntityKeyExtractor("$.id");
        RulesetConfig rs = new RulesetConfig("/tmp/" + agentId + ".jar");
        rs.setArchiveControlTopic("agent-" + agentId + "-archive-control");
        d.setRuleset(rs);
        d.setSubscriptions(List.of(
            new EventSubscription("Ev1", "topic-ev1", "OnEv1", HorizonConfig.count(10))
        ));
        d.setOutputs(List.of(
            new OutputConfig("Out1", "topic-out1")
        ));
        return d;
    }

    @Test
    @DisplayName("Valid agent descriptor passes validation")
    void validDescriptorPasses() {
        AgentDescriptor agent = validAgent("fraud-agent");
        assertDoesNotThrow(() -> AgentDescriptorValidator.validate(agent));
    }

    @Test
    @DisplayName("Missing or invalid agentId is rejected")
    void invalidAgentIdRejected() {
        AgentDescriptor d1 = validAgent("a");
        d1.setAgentId(null);
        IllegalArgumentException e1 = assertThrows(IllegalArgumentException.class, () -> AgentDescriptorValidator.validate(d1));
        assertTrue(e1.getMessage().contains("agentId must not be blank"));

        AgentDescriptor d2 = validAgent("a");
        d2.setAgentId("invalid id with spaces");
        IllegalArgumentException e2 = assertThrows(IllegalArgumentException.class, () -> AgentDescriptorValidator.validate(d2));
        assertTrue(e2.getMessage().contains("is invalid"));
    }

    @Test
    @DisplayName("Missing entityType is rejected")
    void missingEntityTypeRejected() {
        AgentDescriptor d = validAgent("a");
        d.setEntityType(null);
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> AgentDescriptorValidator.validate(d));
        assertTrue(ex.getMessage().contains("entityType is required"));
    }

    @Test
    @DisplayName("Missing ruleset or ruleset.path is rejected")
    void missingRulesetRejected() {
        AgentDescriptor d1 = validAgent("a");
        d1.setRuleset(null);
        IllegalArgumentException e1 = assertThrows(IllegalArgumentException.class, () -> AgentDescriptorValidator.validate(d1));
        assertTrue(e1.getMessage().contains("ruleset must not be null"));

        AgentDescriptor d2 = validAgent("a");
        d2.getRuleset().setPath("");
        IllegalArgumentException e2 = assertThrows(IllegalArgumentException.class, () -> AgentDescriptorValidator.validate(d2));
        assertTrue(e2.getMessage().contains("ruleset.path must not be blank"));
    }

    @Test
    @DisplayName("Missing or invalid ruleset.archiveControlTopic is rejected")
    void invalidArchiveControlTopicRejected() {
        AgentDescriptor d1 = validAgent("a");
        d1.getRuleset().setArchiveControlTopic(null);
        IllegalArgumentException e1 = assertThrows(IllegalArgumentException.class, () -> AgentDescriptorValidator.validate(d1));
        assertTrue(e1.getMessage().contains("ruleset.archiveControlTopic is required"));

        AgentDescriptor d2 = validAgent("a");
        d2.getRuleset().setArchiveControlTopic("   ");
        IllegalArgumentException e2 = assertThrows(IllegalArgumentException.class, () -> AgentDescriptorValidator.validate(d2));
        assertTrue(e2.getMessage().contains("ruleset.archiveControlTopic is required"));

        AgentDescriptor d3 = validAgent("a");
        d3.getRuleset().setArchiveControlTopic("topic with spaces");
        IllegalArgumentException e3 = assertThrows(IllegalArgumentException.class, () -> AgentDescriptorValidator.validate(d3));
        assertTrue(e3.getMessage().contains("contains invalid characters"));
    }

    @Test
    @DisplayName("Empty subscriptions list is rejected")
    void emptySubscriptionsRejected() {
        AgentDescriptor d = validAgent("a");
        d.setSubscriptions(List.of());
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> AgentDescriptorValidator.validate(d));
        assertTrue(ex.getMessage().contains("at least one event subscription is required"));
    }

    @Test
    @DisplayName("Subscription without topic is rejected (when not Schedule)")
    void subscriptionWithoutTopicRejected() {
        AgentDescriptor d = validAgent("a");
        d.setSubscriptions(List.of(
            new EventSubscription("Ev1", "", "OnEv1", HorizonConfig.count(5))
        ));
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> AgentDescriptorValidator.validate(d));
        assertTrue(ex.getMessage().contains("must declare a non-blank 'topic'"));
    }

    @Test
    @DisplayName("Output with blank topic or eventType is rejected")
    void invalidOutputConfigRejected() {
        AgentDescriptor d1 = validAgent("a");
        d1.setOutputs(List.of(new OutputConfig("Out1", "")));
        IllegalArgumentException e1 = assertThrows(IllegalArgumentException.class, () -> AgentDescriptorValidator.validate(d1));
        assertTrue(e1.getMessage().contains("output destination topic must not be blank"));

        AgentDescriptor d2 = validAgent("a");
        d2.setOutputs(List.of(new OutputConfig("", "out-topic")));
        IllegalArgumentException e2 = assertThrows(IllegalArgumentException.class, () -> AgentDescriptorValidator.validate(d2));
        assertTrue(e2.getMessage().contains("output eventType must not be blank"));
    }

    @Test
    @DisplayName("Invalid stateConfig.ttl is rejected")
    void invalidStateTtlRejected() {
        AgentDescriptor d = validAgent("a");
        StateConfig sc = new StateConfig();
        sc.setTtl("invalid-ttl");
        d.setStateConfig(sc);
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> AgentDescriptorValidator.validate(d));
        assertTrue(ex.getMessage().contains("invalid stateConfig.ttl"));
    }

    @Test
    @DisplayName("validateUniqueAgentIds rejects duplicate or blank ids")
    void validateUniqueAgentIdsTests() {
        AgentDescriptor a1 = validAgent("agent-1");
        AgentDescriptor a2 = validAgent("agent-1");
        assertThrows(IllegalArgumentException.class, () -> AgentDescriptorValidator.validateUniqueAgentIds(List.of(a1, a2)));

        AgentDescriptor a3 = validAgent("");
        assertThrows(IllegalArgumentException.class, () -> AgentDescriptorValidator.validateUniqueAgentIds(List.of(a3)));
    }

    @Test
    @DisplayName("validateUniqueOperatorUids detects duplicate sink UIDs across agents")
    void validateUniqueOperatorUidsTests() {
        AgentDescriptor a1 = validAgent("a");
        AgentDescriptor a2 = validAgent("b");
        assertDoesNotThrow(() -> AgentDescriptorValidator.validateUniqueOperatorUids(List.of(a1, a2)));

        // Duplicate sink event type on same agent produces duplicate UID
        a1.setOutputs(List.of(
            new OutputConfig("Out1", "topic-1"),
            new OutputConfig("Out1", "topic-2")
        ));
        assertThrows(IllegalArgumentException.class, () -> AgentDescriptorValidator.validate(a1));
    }
}
