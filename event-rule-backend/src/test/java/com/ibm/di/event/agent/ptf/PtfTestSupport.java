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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.di.event.agent.AgentDescriptor;
import com.ibm.di.event.agent.EventSubscription;
import com.ibm.di.event.agent.HorizonConfig;
import com.ibm.di.event.agent.OutputConfig;
import com.ibm.di.event.agent.RulesetConfig;
import com.ibm.di.event.agent.ScheduleRequests;
import com.ibm.di.event.agent.StateConfig;
import com.ibm.di.event.rule.RuleEngineRunner;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/** Shared fixtures of the Process Table Function tests. */
final class PtfTestSupport {

    static final ObjectMapper MAPPER = new ObjectMapper();

    /** 2026-10-18T23:29:16Z */
    static final String SCHEDULE_TIMEPOINT = "2026-10-18T23:29:16Z";
    static final long SCHEDULE_MS = java.time.Instant.parse(SCHEDULE_TIMEPOINT).toEpochMilli();

    private PtfTestSupport() {}

    /** A rule engine that answers with a function of its input and records the inputs. */
    static final class StubEngine extends RuleEngineRunner {
        final List<JsonNode> inputs = new ArrayList<>();
        private final Function<JsonNode, JsonNode> answer;

        StubEngine(Function<JsonNode, JsonNode> answer) {
            super("stub-op", URI.create("file:///dev/null"));
            this.answer = answer;
        }

        @Override public JsonNode execute(JsonNode event) {
            inputs.add(event);
            return answer.apply(event);
        }

        @Override protected void initialize() { /* no-op */ }
        @Override public void close() { /* no-op */ }
    }

    static AgentEventProcessor.RunnerProvider providerOf(StubEngine engine) {
        return operation -> engine;
    }

    /**
     * Agent with a count-horizon subscription "domain-event" on "domain-events" (operation "Op"),
     * a "Schedule" subscription (operation "SchedOp") and one output.
     */
    static AgentDescriptor descriptor(RulesetConfig.OperationConfig operation) {
        AgentDescriptor d = new AgentDescriptor();
        d.setAgentId("test-agent");
        d.setName("Test Agent");
        d.setEntityType("entity");
        d.setEntityKeyExtractor("$.id");

        RulesetConfig rs = new RulesetConfig("/tmp/test.jar");
        if (operation != null) rs.setOperations(List.of(operation));
        d.setRuleset(rs);

        EventSubscription schedule = new EventSubscription();
        schedule.setEventType(ScheduleRequests.SCHEDULE_EVENT_TYPE);
        schedule.setOperation("SchedOp");
        d.setSubscriptions(List.of(
            new EventSubscription("domain-event", "domain-events", "Op", HorizonConfig.count(10)),
            schedule));

        d.setOutputs(List.of(new OutputConfig("alert", "alerts")));
        return d;
    }

    /** Operation "Op": entity in result field "ent", alerts in "outputEvents", schedules in "scheduleRequests". */
    static RulesetConfig.OperationConfig operation() {
        RulesetConfig.OperationConfig op = new RulesetConfig.OperationConfig();
        op.setOperation("Op");
        op.setEntityField("ent");
        op.setScheduleRequestsField("scheduleRequests");
        op.setOutbounds(List.of(new RulesetConfig.OutboundConfig("outputEvents", "alert")));
        return op;
    }

    static StateConfig stateConfig(String ttl, String changelogTopic) {
        StateConfig sc = new StateConfig();
        sc.setTtl(ttl);
        sc.setChangeLogTopic(changelogTopic);
        return sc;
    }

    static JsonNode json(String s) {
        try {
            return MAPPER.readTree(s);
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }
}
