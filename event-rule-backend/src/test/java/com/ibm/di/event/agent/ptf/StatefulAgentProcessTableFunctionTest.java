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
import com.ibm.di.event.agent.HorizonConfig;
import com.ibm.di.event.agent.ptf.PtfTestSupport.StubEngine;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.functions.TableSemantics;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.ibm.di.event.agent.ptf.PtfTestSupport.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Drives {@link StatefulAgentProcessTableFunction} with stub Flink contexts: the module has no
 * Process Table Function test harness, so the function is called the way the runtime calls it.
 */
@DisplayName("StatefulAgentProcessTableFunction")
class StatefulAgentProcessTableFunctionTest {

    /** Records timers; also serves as the {@code OnTimerContext} when {@link #current} is set. */
    static final class StubContext implements ProcessTableFunction.OnTimerContext {
        final Map<String, Instant> timers = new LinkedHashMap<>();
        Instant time = Instant.ofEpochMilli(1000);
        String current;
        boolean allTimersCleared;

        private final ProcessTableFunction.TimeContext<Instant> timeContext =
            new ProcessTableFunction.TimeContext<>() {
                @Override public Instant time() { return time; }
                @Override public Instant currentWatermark() { return time; }
                @Override public void registerOnTime(String name, Instant t) { timers.put(name, t); }
                @Override public void registerOnTime(Instant t) { timers.put("", t); }
                @Override public void clearTimer(String name) { timers.remove(name); }
                @Override public void clearTimer(Instant t) { timers.values().remove(t); }
                @Override public void clearAllTimers() { timers.clear(); }
            };

        @SuppressWarnings("unchecked")
        @Override public <T> ProcessTableFunction.TimeContext<T> timeContext(Class<T> type) {
            return (ProcessTableFunction.TimeContext<T>) timeContext;
        }
        @Override public String currentTimer() { return current; }
        @Override public TableSemantics tableSemanticsFor(String name) { return null; }
        @Override public void clearState(String name) { }
        @Override public void clearAllState() { }
        @Override public void clearAllTimers() { allTimersCleared = true; timers.clear(); }
        @Override public void clearAll() { clearAllTimers(); }
        @Override public ChangelogMode getChangelogMode() { return null; }
    }

    private final List<Row> rows = new ArrayList<>();
    private final AgentPtfState agent = new AgentPtfState();
    private final SchedulePtfState schedules = new SchedulePtfState();
    private final StubContext ctx = new StubContext();

    private StatefulAgentProcessTableFunction function;

    @BeforeEach
    void setUp() {
        function = create(descriptor(operation()),
            new StubEngine(in -> json("{\"ent\":{\"n\":1},\"outputEvents\":[{\"level\":\"high\"}]}")));
    }

    private StatefulAgentProcessTableFunction create(AgentDescriptor d, StubEngine engine) {
        StatefulAgentProcessTableFunction f =
            new StatefulAgentProcessTableFunction(d, providerOf(engine));
        f.setCollector(new Collector<>() {
            @Override public void collect(Row r) { rows.add(r); }
            @Override public void close() { }
        });
        return f;
    }

    private static Row input(String type, String topic, String payload) {
        Row r = Row.withNames();
        r.setField("entity_id", "E1");
        r.setField("event_type", type);
        r.setField("source_topic", topic);
        r.setField("payload", payload);
        return r;
    }

    @Test
    @DisplayName("an event emits its output rows and stores the entity state")
    void eventEmitsRowsAndState() throws Exception {
        function.eval(ctx, agent, schedules, input("domain-event", "domain-events", "{\"id\":\"E1\"}"), null);

        assertEquals(1, rows.size());
        assertEquals("alert", rows.get(0).<String>getFieldAs(0));
        JsonNode envelope = json(rows.get(0).<String>getFieldAs(1));
        assertEquals("high", envelope.get("payload").get("level").asText());
        assertEquals("E1", envelope.get("entityId").asText());
        assertEquals(1000L, envelope.get("timestamp").asLong());

        assertNotNull(agent.data);
        assertEquals(json("{\"n\":1}"), json(agent.data).get("entity"));
        assertEquals("E1", schedules.entityId);
    }

    @Test
    @DisplayName("the state is read back on the next event")
    void stateIsCarriedOver() throws Exception {
        function.eval(ctx, agent, schedules, input("domain-event", "domain-events", "{\"id\":\"E1\"}"), null);
        ctx.time = Instant.ofEpochMilli(2000);
        function.eval(ctx, agent, schedules, input("domain-event", "domain-events", "{\"id\":\"E1\"}"), null);

        assertEquals(2, json(agent.data).get("hist").get("domain-event").size());
        assertEquals(2, rows.size());
    }

    @Test
    @DisplayName("a null payload is processed as an empty object")
    void nullPayload() throws Exception {
        function.eval(ctx, agent, schedules, input("domain-event", "domain-events", null), null);
        assertEquals(1, rows.size());
    }

    @Test
    @DisplayName("a time horizon registers the cleanup timer and a ttl registers the ttl timer")
    void cleanupAndTtlTimers() throws Exception {
        AgentDescriptor d = descriptor(operation());
        d.getSubscriptions().get(0).setHorizon(HorizonConfig.time("1mn"));
        d.setStateConfig(stateConfig("1h", null));
        function = create(d, new StubEngine(in -> json("{\"ent\":{\"n\":1}}")));

        function.eval(ctx, agent, schedules, input("domain-event", "domain-events", "{\"id\":\"E1\"}"), null);

        assertEquals(Instant.ofEpochMilli(1000 + 60_000 + 1), ctx.timers.get("cleanup"));
        assertEquals(Instant.ofEpochMilli(1000 + 3_600_000), ctx.timers.get("ttl"));
    }

    @Test
    @DisplayName("a schedule request registers a named timer, and the timer fires it")
    void scheduleTimer() throws Exception {
        String result = "{\"ent\":{\"a\":1},\"scheduleRequests\":[{\"id\":\"s1\",\"status\":\"START\","
            + "\"timepoint\":\"" + SCHEDULE_TIMEPOINT + "\",\"description\":\"wake\"}]}";
        function = create(descriptor(operation()), new StubEngine(in -> json(result)));

        function.eval(ctx, agent, schedules, input("domain-event", "domain-events", "{\"id\":\"E1\"}"), null);
        String timer = "sched-" + SCHEDULE_MS;
        assertEquals(Instant.ofEpochMilli(SCHEDULE_MS), ctx.timers.get(timer));

        function = create(descriptor(operation()),
            new StubEngine(in -> json("{\"outputEvents\":[{\"fired\":true}]}")));
        rows.clear();
        ctx.time = Instant.ofEpochMilli(SCHEDULE_MS);
        ctx.current = timer;
        ctx.timers.remove(timer); // Flink removes the timer that fires
        function.onTimer(ctx, agent, schedules);

        assertEquals(1, rows.size());
        assertTrue(schedules.requests.isEmpty());
        assertFalse(ctx.timers.containsKey(timer), "the fired timer is not registered again");
    }

    @Test
    @DisplayName("the ttl timer drops the entity state but keeps the schedules")
    void ttlTimer() throws Exception {
        function.eval(ctx, agent, schedules, input("domain-event", "domain-events", "{\"id\":\"E1\"}"), null);
        schedules.requests.put("s1", "{}");
        ctx.current = "ttl";

        function.onTimer(ctx, agent, schedules);

        assertNull(agent.data);
        assertEquals(1, schedules.requests.size());
    }

    @Test
    @DisplayName("the reset sentinel clears the state and every timer")
    void reset() throws Exception {
        function.eval(ctx, agent, schedules, input("domain-event", "domain-events", "{\"id\":\"E1\"}"), null);
        rows.clear();

        function.eval(ctx, agent, schedules,
            input("__ENTITY_RESET__", "domain-events", "{}"), null);

        assertNull(agent.data);
        assertTrue(ctx.allTimersCleared);
        assertTrue(rows.isEmpty());
    }

    @Test
    @DisplayName("without any descriptor the function refuses to run")
    void noDescriptor() {
        StatefulAgentProcessTableFunction f = new StatefulAgentProcessTableFunction();
        Row row = input("domain-event", "domain-events", "{}");
        assertThrows(IllegalStateException.class, () -> f.eval(ctx, agent, schedules, row, " "));
    }

    @Test
    @DisplayName("the descriptor can come from the agent_config argument")
    void descriptorFromArgument() throws Exception {
        AgentDescriptor d = descriptor(operation());
        // The ruleset archive does not exist, so the call must fail while loading it: this proves
        // the descriptor was parsed and used.
        StatefulAgentProcessTableFunction f = new StatefulAgentProcessTableFunction();
        Row row = input("domain-event", "domain-events", "{}");
        assertThrows(RuntimeException.class, () ->
            f.eval(ctx, agent, schedules, row, AgentDescriptors.toJson(d)));
    }
}
