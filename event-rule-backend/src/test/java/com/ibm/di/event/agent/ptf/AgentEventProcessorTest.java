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
import com.ibm.di.event.agent.AgentDescriptor;
import com.ibm.di.event.agent.EventEnvelope;
import com.ibm.di.event.agent.HorizonConfig;
import com.ibm.di.event.agent.ptf.PtfTestSupport.StubEngine;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.ibm.di.event.agent.ptf.PtfTestSupport.*;
import static org.junit.jupiter.api.Assertions.*;

@DisplayName("AgentEventProcessor")
class AgentEventProcessorTest {

    private static final String RESULT =
        "{\"ent\":{\"count\":1},\"outputEvents\":[{\"level\":\"high\"}]}";

    private static EventEnvelope event(String type, String topic, long ts, String payload) {
        EventEnvelope e = EventEnvelope.from(type, topic, ts, json(payload));
        e.setEntityId("E1");
        return e;
    }

    private static AgentEventProcessor.Session session() {
        return new AgentEventProcessor.Session(null, new ScheduleBook(new SchedulePtfState()));
    }

    private static AgentEventProcessor processor(AgentDescriptor d, StubEngine engine) {
        return new AgentEventProcessor(d, providerOf(engine));
    }

    @Test
    @DisplayName("an event runs the rule, updates the entity and emits the outbound events")
    void processEmitsOutputsAndUpdatesEntity() throws Exception {
        StubEngine engine = new StubEngine(in -> json(RESULT));
        AgentEventProcessor p = processor(descriptor(operation()), engine);
        AgentEventProcessor.Session s = session();

        p.process(s, event("domain-event", "domain-events", 1000, "{\"id\":\"E1\"}"));

        assertEquals(1, engine.inputs.size());
        assertEquals(json("{\"count\":1}"), s.state.getEntityData());
        assertEquals(1, s.outputs.size());
        assertEquals("alert", s.outputs.get(0).getEventType());
        assertEquals("E1", s.book.getEntityId());
        assertEquals(1, s.state.getEventHistories().get("domain-event").size());
    }

    @Test
    @DisplayName("an event no subscription handles only updates the history of nothing and emits nothing")
    void unknownEventIsIgnored() throws Exception {
        StubEngine engine = new StubEngine(in -> json(RESULT));
        AgentEventProcessor p = processor(descriptor(operation()), engine);
        AgentEventProcessor.Session s = session();

        p.process(s, event("other", "other-topic", 1000, "{}"));

        assertTrue(engine.inputs.isEmpty());
        assertTrue(s.outputs.isEmpty());
        assertNotNull(s.state);
    }

    @Test
    @DisplayName("a null event is skipped")
    void nullEventSkipped() throws Exception {
        AgentEventProcessor p = processor(descriptor(operation()), new StubEngine(in -> null));
        AgentEventProcessor.Session s = session();
        p.process(s, null);
        assertNull(s.state);
    }

    @Test
    @DisplayName("a rule result of null yields an agent-error output")
    void ruleFailureYieldsError() throws Exception {
        StubEngine engine = new StubEngine(in -> null);
        AgentEventProcessor p = processor(descriptor(operation()), engine);
        AgentEventProcessor.Session s = session();

        p.process(s, event("domain-event", "domain-events", 1000, "{\"id\":\"E1\"}"));

        assertEquals(1, s.outputs.size());
        assertEquals(AgentEventProcessor.ERROR_EVENT_TYPE, s.outputs.get(0).getEventType());
        assertEquals("test-agent", s.outputs.get(0).getPayload().get("agentId").asText());
    }

    @Test
    @DisplayName("a rule that throws is reported as an agent-error and does not escape")
    void ruleExceptionYieldsError() throws Exception {
        StubEngine engine = new StubEngine(in -> { throw new IllegalStateException("boom"); });
        AgentEventProcessor p = processor(descriptor(operation()), engine);
        AgentEventProcessor.Session s = session();

        p.process(s, event("domain-event", "domain-events", 1000, "{\"id\":\"E1\"}"));

        assertEquals(AgentEventProcessor.ERROR_EVENT_TYPE, s.outputs.get(0).getEventType());
    }

    @Test
    @DisplayName("a changed entity produces a changelog envelope, an unchanged one does not")
    void changelogOnlyOnChange() throws Exception {
        AgentDescriptor d = descriptor(operation());
        d.setStateConfig(stateConfig(null, "changes"));
        StubEngine engine = new StubEngine(in -> json("{\"ent\":{\"count\":1}}"));
        AgentEventProcessor p = processor(d, engine);
        AgentEventProcessor.Session s = session();

        p.process(s, event("domain-event", "domain-events", 1000, "{\"id\":\"E1\"}"));
        assertEquals(1, s.outputs.size());
        assertEquals(AgentEventProcessor.CHANGELOG_EVENT_TYPE, s.outputs.get(0).getEventType());
        assertEquals("E1", s.outputs.get(0).getPayload().get("entityId").asText());

        s.outputs.clear();
        p.process(s, event("domain-event", "domain-events", 2000, "{\"id\":\"E1\"}"));
        assertTrue(s.outputs.isEmpty(), "same entity again: no changelog");
    }

    @Test
    @DisplayName("the entity falls back to the payload field when the result has none")
    void entityPayloadFallback() throws Exception {
        StubEngine engine = new StubEngine(in -> json("{\"other\":1}"));
        AgentEventProcessor p = processor(descriptor(operation()), engine);
        AgentEventProcessor.Session s = session();

        p.process(s, event("domain-event", "domain-events", 1000, "{\"id\":\"E1\",\"ent\":{\"a\":1}}"));

        assertEquals(json("{\"a\":1}"), s.state.getEntityData());
    }

    @Test
    @DisplayName("the reset sentinel clears the state and the schedules")
    void entityReset() throws Exception {
        StubEngine engine = new StubEngine(in -> json(RESULT));
        AgentEventProcessor p = processor(descriptor(operation()), engine);
        AgentEventProcessor.Session s = session();
        p.process(s, event("domain-event", "domain-events", 1000, "{\"id\":\"E1\"}"));
        s.book.apply(List.of(new com.ibm.di.event.agent.ScheduleRequest("a", "d",
            SCHEDULE_TIMEPOINT, com.ibm.di.event.agent.ScheduleRequest.Status.START)));

        AgentEventProcessor.Session reset =
            new AgentEventProcessor.Session(s.state, s.book);
        p.process(reset, event(AgentEventProcessor.ENTITY_RESET_EVENT_TYPE, null, 3000, "{}"));

        assertTrue(reset.reset);
        assertNull(reset.state);
        assertTrue(reset.book.isEmpty());
        assertEquals(java.util.Set.of(SCHEDULE_MS), reset.timersToClear());
    }

    @Test
    @DisplayName("the reset sentinel is recognised through the __eventType payload override")
    void resetThroughPayloadOverride() {
        assertTrue(AgentEventProcessor.isEntityResetEvent(
            event("domain-event", "t", 1, "{\"__eventType\":\"__ENTITY_RESET__\"}")));
        assertFalse(AgentEventProcessor.isEntityResetEvent(
            event("domain-event", "t", 1, "{\"__eventType\":\"x\"}")));
    }

    @Test
    @DisplayName("schedule requests of the rule result are stored and ask for a timer")
    void scheduleRequestsRegisterTimers() throws Exception {
        String result = "{\"ent\":{\"a\":1},\"scheduleRequests\":[{\"id\":\"s1\",\"status\":\"START\","
            + "\"timepoint\":\"" + SCHEDULE_TIMEPOINT + "\",\"description\":\"wake\"}]}";
        StubEngine engine = new StubEngine(in -> json(result));
        AgentEventProcessor p = processor(descriptor(operation()), engine);
        AgentEventProcessor.Session s = session();

        p.process(s, event("domain-event", "domain-events", 1000, "{\"id\":\"E1\"}"));

        assertEquals(List.of(SCHEDULE_MS), s.book.timepoints().stream().toList());
        assertEquals(java.util.Set.of(SCHEDULE_MS), s.timersToRegister());
        assertTrue(s.timersToClear().isEmpty());
    }

    @Test
    @DisplayName("the timer fires the schedule as a Schedule event routed to its operation")
    void timerFiresSchedule() throws Exception {
        StubEngine engine = new StubEngine(in -> json("{\"outputEvents\":[{\"x\":1}]}"));
        List<String> operations = new java.util.ArrayList<>();
        AgentEventProcessor p = new AgentEventProcessor(descriptor(operation()), op -> {
            operations.add(op);
            return engine;
        });
        SchedulePtfState ss = new SchedulePtfState();
        ScheduleBook book = new ScheduleBook(ss);
        book.setEntityId("E1");
        book.apply(List.of(new com.ibm.di.event.agent.ScheduleRequest("s1", "wake",
            SCHEDULE_TIMEPOINT, com.ibm.di.event.agent.ScheduleRequest.Status.START)));
        AgentEventProcessor.Session s = new AgentEventProcessor.Session(null, book);

        p.onTimer(s, SCHEDULE_MS);

        assertEquals(1, engine.inputs.size());
        assertTrue(book.isEmpty());
        assertEquals(java.util.Set.of(SCHEDULE_MS), s.timersToClear());
        assertEquals(1, s.outputs.size());
        assertEquals(List.of("SchedOp"), operations, "routed to the Schedule subscription's operation");
    }

    @Test
    @DisplayName("an event past a pending timepoint fires the due schedule first, without recursion")
    void dueScheduleFiresBeforeEvent() throws Exception {
        StubEngine engine = new StubEngine(in -> json("{\"ent\":{\"a\":1}}"));
        AgentEventProcessor p = processor(descriptor(operation()), engine);
        ScheduleBook book = new ScheduleBook(new SchedulePtfState());
        book.apply(List.of(new com.ibm.di.event.agent.ScheduleRequest("s1", "wake",
            SCHEDULE_TIMEPOINT, com.ibm.di.event.agent.ScheduleRequest.Status.START)));
        AgentEventProcessor.Session s = new AgentEventProcessor.Session(null, book);

        p.process(s, event("domain-event", "domain-events", SCHEDULE_MS + 10, "{\"id\":\"E1\"}"));

        assertEquals(2, engine.inputs.size(), "the Schedule event and the domain event");
        assertTrue(book.isEmpty());
    }

    @Test
    @DisplayName("onTimer evicts time-expired events")
    void timerEvictsExpiredEvents() throws Exception {
        AgentDescriptor d = descriptor(operation());
        d.getSubscriptions().get(0).setHorizon(HorizonConfig.time("1mn"));
        StubEngine engine = new StubEngine(in -> json("{\"ent\":{\"a\":1}}"));
        AgentEventProcessor p = processor(d, engine);
        AgentEventProcessor.Session s = session();
        p.process(s, event("domain-event", "domain-events", 1000, "{\"id\":\"E1\"}"));
        assertEquals(1, s.state.getEventHistories().get("domain-event").size());

        AgentEventProcessor.Session later = new AgentEventProcessor.Session(s.state, s.book);
        p.onTimer(later, 1000 + 120_000);

        assertEquals(0, later.state.getEventHistories().get("domain-event").size());
    }

    @Test
    @DisplayName("the cleanup timer is the newest event plus the shortest time horizon plus 1 ms")
    void cleanupTimer() throws Exception {
        AgentDescriptor d = descriptor(operation());
        d.getSubscriptions().get(0).setHorizon(HorizonConfig.time("1mn"));
        AgentEventProcessor p = processor(d, new StubEngine(in -> json("{}")));
        AgentEventProcessor.Session s = session();
        assertNull(p.cleanupTimerAt(s), "no event seen yet");

        p.process(s, event("domain-event", "domain-events", 5000, "{\"id\":\"E1\"}"));

        assertEquals(5000 + 60_000 + 1, p.cleanupTimerAt(s));
        assertEquals(5000, s.lastEventTs());
    }

    @Test
    @DisplayName("no time horizon means no cleanup timer")
    void noCleanupTimerWithoutTimeHorizon() throws Exception {
        AgentEventProcessor p = processor(descriptor(operation()), new StubEngine(in -> json("{}")));
        AgentEventProcessor.Session s = session();
        p.process(s, event("domain-event", "domain-events", 5000, "{\"id\":\"E1\"}"));
        assertNull(p.cleanupTimerAt(s));
    }

    @Test
    @DisplayName("the default outbound is outputEvents of type object")
    void defaultOutbound() throws Exception {
        StubEngine engine = new StubEngine(in -> json("{\"outputEvents\":[{\"a\":1},{\"a\":2}]}"));
        AgentEventProcessor p = processor(descriptor(null), engine);
        AgentEventProcessor.Session s = session();

        p.process(s, event("domain-event", "domain-events", 1000, "{\"id\":\"E1\"}"));

        assertEquals(2, s.outputs.size());
        assertEquals("object", s.outputs.get(0).getEventType());
        JsonNode first = s.outputs.get(0).getPayload();
        assertEquals(1, first.get("a").asInt());
    }
}
