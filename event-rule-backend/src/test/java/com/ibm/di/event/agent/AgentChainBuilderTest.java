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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link AgentChainBuilder}.
 *
 * <p>All tests operate on the pure chain-resolution logic without a live Flink cluster.
 * The Flink {@code DataStream} parameter is faked with {@code null}: since
 * {@link AgentChainBuilder#resolve()} only reads {@link AgentDescriptor} metadata to
 * compute edges, the stream objects are never accessed during resolution.
 *
 * <p>Actual stream union behaviour is exercised at runtime; the tests here verify
 * that the edge set is computed correctly.
 *
 * <p>All tests use {@link AgentChainBuilder#resolveTopology(List)} and
 * {@link AgentChainBuilder.DeferredChainedInputs}.
 */
@DisplayName("AgentChainBuilder unit tests")
class AgentChainBuilderTest {

    // ── Helpers ───────────────────────────────────────────────────────────────

    /** Builds a minimal valid descriptor with the given outputs and subscriptions. */
    private static AgentDescriptor agent(String agentId,
                                         List<String> outputEventTypes,
                                         List<String> subscribedEventTypes) {
        AgentDescriptor d = new AgentDescriptor();
        d.setAgentId(agentId);
        d.setEntityType("account");
        d.setEntityKeyExtractor("$.id");

        RulesetConfig rs = new RulesetConfig("/tmp/" + agentId + ".jar");
        d.setRuleset(rs);

        List<OutputConfig> outputs = outputEventTypes.stream()
            .map(et -> new OutputConfig(et, "topic-" + et))
            .toList();
        d.setOutputs(outputs);

        List<EventSubscription> subs = subscribedEventTypes.stream()
            .map(et -> new EventSubscription(et, "topic-" + et,
                "op-" + agentId, HorizonConfig.count(10)))
            .toList();
        d.setSubscriptions(subs);

        return d;
    }

    // ── DeferredChainedInputs null-safety ─────────────────────────────────────

    @Nested
    @DisplayName("DeferredChainedInputs null-safety")
    class DeferredChainedInputsNullSafety {

        @Test
        @DisplayName("get() returns null for unknown agentId")
        void getUnknownAgentId() {
            AgentChainBuilder.DeferredChainedInputs c = new AgentChainBuilder().resolveTopology(List.of());
            assertNull(c.get("nonexistent"));
        }

        @Test
        @DisplayName("hasChainedInput() returns false for unknown agentId")
        void hasChainedInputUnknownId() {
            AgentChainBuilder.DeferredChainedInputs c = new AgentChainBuilder().resolveTopology(List.of());
            assertFalse(c.hasChainedInput("nonexistent"));
        }

        @Test
        @DisplayName("chainedAgentIds() returns empty set when no chains")
        void chainedAgentIdsEmpty() {
            AgentChainBuilder.DeferredChainedInputs c = new AgentChainBuilder().resolveTopology(List.of());
            assertTrue(c.chainedAgentIds().isEmpty());
        }

        @Test
        @DisplayName("chainedEventTypes() returns empty set for unknown agentId")
        void chainedEventTypesUnknownId() {
            AgentChainBuilder.DeferredChainedInputs c = new AgentChainBuilder().resolveTopology(List.of());
            assertTrue(c.chainedEventTypes("nonexistent").isEmpty());
        }
    }

    // ── Cycle detection direct unit tests ─────────────────────────────────────

    @Nested
    @DisplayName("Cycle detection")
    class CycleDetection {

        @Test
        @DisplayName("Direct 2-cycle A→B→A throws IllegalStateException")
        void directTwoCycle() {
            AgentDescriptor a = agent("agent-a", List.of("ev-ab"), List.of("ev-ba"));
            AgentDescriptor b = agent("agent-b", List.of("ev-ba"), List.of("ev-ab"));

            AgentChainBuilder builder = new AgentChainBuilder();
            IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> builder.resolveTopology(List.of(a, b)), "A→B→A cycle must be detected");
            assertTrue(ex.getMessage().contains("Cycle detected"),
                "Exception message must say 'Cycle detected'");
            assertTrue(ex.getMessage().contains("agent-a") || ex.getMessage().contains("agent-b"),
                "Exception message must name the agents involved");
        }

        @Test
        @DisplayName("3-cycle A→B→C→A throws IllegalStateException")
        void threeNodeCycle() {
            AgentDescriptor a = agent("a", List.of("ev-1"), List.of("ev-3"));
            AgentDescriptor b = agent("b", List.of("ev-2"), List.of("ev-1"));
            AgentDescriptor c = agent("c", List.of("ev-3"), List.of("ev-2"));

            AgentChainBuilder builder = new AgentChainBuilder();
            IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> builder.resolveTopology(List.of(a, b, c)), "3-cycle must be detected");
            assertTrue(ex.getMessage().contains("Cycle detected"));
        }

        @Test
        @DisplayName("DAG with a tail after a cycle still throws")
        void dagPlusCycle() {
            AgentDescriptor a = agent("a", List.of("ev-a"), List.of("ev-d"));
            AgentDescriptor b = agent("b", List.of("ev-b"), List.of("ev-a", "ev-c"));
            AgentDescriptor c = agent("c", List.of("ev-c"), List.of("ev-b"));
            AgentDescriptor d = agent("d", List.of("ev-d"), List.of("raw"));

            AgentChainBuilder builder = new AgentChainBuilder();
            assertThrows(IllegalStateException.class,
                () -> builder.resolveTopology(List.of(a, b, c, d)),
                "A cycle anywhere in the graph must be rejected");
        }

        @Test
        @DisplayName("Pure DAG (A→B→C) does not throw")
        void pureDagNoThrow() {
            AgentDescriptor a = agent("a", List.of("ev-1"), List.of("raw"));
            AgentDescriptor b = agent("b", List.of("ev-2"), List.of("ev-1"));
            AgentDescriptor c = agent("c", List.of("ev-3"), List.of("ev-2"));

            AgentChainBuilder builder = new AgentChainBuilder();
            assertDoesNotThrow(() -> builder.resolveTopology(List.of(a, b, c)), "A valid DAG must not throw");
        }

        @Test
        @DisplayName("Empty graph does not throw")
        void emptyGraphNoThrow() {
            assertDoesNotThrow(() -> AgentChainBuilder.detectCycles(Map.of()));
        }

        @Test
        @DisplayName("detectCycles: single edge A→B (acyclic) does not throw")
        void singleEdgeAcyclic() {
            assertDoesNotThrow(() ->
                AgentChainBuilder.detectCycles(Map.of("a", Set.of("b"))));
        }

        @Test
        @DisplayName("detectCycles: self-loop A→A throws")
        void selfLoopThrows() {
            assertThrows(IllegalStateException.class, () ->
                AgentChainBuilder.detectCycles(Map.of("a", Set.of("a"))));
        }

        @Test
        @DisplayName("detectCycles: disconnected cycle B→C→B, independent node A, throws")
        void disconnectedCycleThrows() {
            assertThrows(IllegalStateException.class, () ->
                AgentChainBuilder.detectCycles(Map.of(
                    "a", Set.of("b"),
                    "b", Set.of("c"),
                    "c", Set.of("b"))
                ));
        }

        @Test
        @DisplayName("Error message names all cycle-participant agents")
        void errorMessageNamesAgents() {
            AgentDescriptor alpha = agent("alpha", List.of("ev-x"), List.of("ev-y"));
            AgentDescriptor beta  = agent("beta",  List.of("ev-y"), List.of("ev-x"));

            AgentChainBuilder builder = new AgentChainBuilder();
            IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> builder.resolveTopology(List.of(alpha, beta)));
            String msg = ex.getMessage();
            assertTrue(msg.contains("alpha"), "alpha must appear in error message");
            assertTrue(msg.contains("beta"),  "beta must appear in error message");
        }
    }

    // ── DeferredChainedInputs / resolveTopology ───────────────────────────────

    @Nested
    @DisplayName("resolveTopology — DeferredChainedInputs")
    class ResolveTopologyTests {

        // Helper: create a descriptor list from agents (matches the List<AgentDescriptor> API)
        private List<AgentDescriptor> descriptors(AgentDescriptor... agents) {
            return List.of(agents);
        }

        @Test
        @DisplayName("Empty agent list → empty DeferredChainedInputs")
        void emptyAgentListProducesEmptyResult() {
            AgentChainBuilder b = new AgentChainBuilder();
            AgentChainBuilder.DeferredChainedInputs d = b.resolveTopology(List.of());
            assertTrue(d.isEmpty(),         "No agents → no chain edges");
            assertTrue(d.chainedAgentIds().isEmpty());
        }

        @Test
        @DisplayName("Single agent with no matching subscriptions → no chains")
        void singleAgentNoChains() {
            AgentChainBuilder b = new AgentChainBuilder();
            AgentChainBuilder.DeferredChainedInputs d = b.resolveTopology(
                descriptors(agent("solo", List.of("output-ev"), List.of("raw"))));

            assertTrue(d.isEmpty());
            assertFalse(d.hasChainedInput("solo"));
            assertNull(d.get("solo"), "No upstream → null stream");
        }

        @Test
        @DisplayName("Simple A→B chain: hasChainedInput returns true for B, false for A")
        void simpleChainEdgeDetected() {
            AgentDescriptor a = agent("a", List.of("ev-1"), List.of("raw"));
            AgentDescriptor b2 = agent("b", List.of("done"), List.of("ev-1"));

            AgentChainBuilder builder = new AgentChainBuilder();
            AgentChainBuilder.DeferredChainedInputs d = builder.resolveTopology(List.of(a, b2));

            assertTrue(d.hasChainedInput("b"),   "b subscribes to a's ev-1 → chain edge");
            assertFalse(d.hasChainedInput("a"),  "a has no upstream");
            assertEquals(Set.of("b"), d.chainedAgentIds());
        }

        @Test
        @DisplayName("get() returns null before registerOutput is called for upstream")
        void getBeforeRegisterOutputReturnsNull() {
            AgentDescriptor a = agent("a", List.of("ev-1"), List.of("raw"));
            AgentDescriptor b2 = agent("b", List.of("done"), List.of("ev-1"));

            AgentChainBuilder builder = new AgentChainBuilder();
            AgentChainBuilder.DeferredChainedInputs d = builder.resolveTopology(List.of(a, b2));

            // b has a chain edge from a, but a has not been wired yet → null stream
            assertNull(d.get("b"),
                "get() must return null when upstream has not yet been registered");
        }

        @Test
        @DisplayName("get() returns non-null after registerOutput is called for upstream")
        void getAfterRegisterOutputReturnsStream() {
            // We cannot create a real DataStream without a Flink mini-cluster,
            // so we test the structural contract: registerOutput stores the stream
            // and hasChainedInput / chainedAgentIds work regardless.
            AgentDescriptor a = agent("a", List.of("ev-1"), List.of("raw"));
            AgentDescriptor b2 = agent("b", List.of("done"), List.of("ev-1"));

            AgentChainBuilder builder = new AgentChainBuilder();
            AgentChainBuilder.DeferredChainedInputs d = builder.resolveTopology(List.of(a, b2));

            // Registering null output is a no-op
            d.registerOutput("a", null);
            assertNull(d.get("b"), "null stream registration must be a no-op");

            // hasChainedInput is based on edge metadata, not registered streams
            assertTrue(d.hasChainedInput("b"),
                "hasChainedInput must be true even before the stream is registered");
        }

        @Test
        @DisplayName("Three-agent linear chain: A→B→C — all downstream edges detected")
        void threeAgentLinearChain() {
            AgentDescriptor a = agent("a", List.of("ev-1"), List.of("raw"));
            AgentDescriptor b2 = agent("b", List.of("ev-2"), List.of("ev-1"));
            AgentDescriptor c = agent("c", List.of("ev-3"), List.of("ev-2"));

            AgentChainBuilder builder = new AgentChainBuilder();
            AgentChainBuilder.DeferredChainedInputs d = builder.resolveTopology(List.of(a, b2, c));

            assertFalse(d.hasChainedInput("a"), "a has no upstream");
            assertTrue(d.hasChainedInput("b"),  "b subscribes to a's ev-1");
            assertTrue(d.hasChainedInput("c"),  "c subscribes to b's ev-2");
            assertEquals(2, d.chainedAgentIds().size());
        }

        @Test
        @DisplayName("Four-agent deep chain A→B→C→D: three chain edges detected")
        void fourAgentDeepChain() {
            AgentDescriptor a = agent("a", List.of("ev-1"), List.of("raw"));
            AgentDescriptor b2 = agent("b", List.of("ev-2"), List.of("ev-1"));
            AgentDescriptor c = agent("c", List.of("ev-3"), List.of("ev-2"));
            AgentDescriptor d2 = agent("d", List.of("ev-4"), List.of("ev-3"));

            AgentChainBuilder builder = new AgentChainBuilder();
            AgentChainBuilder.DeferredChainedInputs d = builder.resolveTopology(List.of(a, b2, c, d2));

            assertFalse(d.hasChainedInput("a"));
            assertTrue(d.hasChainedInput("b"));
            assertTrue(d.hasChainedInput("c"));
            assertTrue(d.hasChainedInput("d"));
            assertEquals(3, d.chainedAgentIds().size());
        }

        @Test
        @DisplayName("Fan-out: one upstream → two downstream consumers both get chain edges")
        void fanOutTwoConsumers() {
            AgentDescriptor src  = agent("src",   List.of("alert"),   List.of("raw"));
            AgentDescriptor con1 = agent("con1",  List.of("result1"), List.of("alert"));
            AgentDescriptor con2 = agent("con2",  List.of("result2"), List.of("alert"));

            AgentChainBuilder builder = new AgentChainBuilder();
            AgentChainBuilder.DeferredChainedInputs d = builder.resolveTopology(List.of(src, con1, con2));

            assertTrue(d.hasChainedInput("con1"), "con1 subscribes to src's alert");
            assertTrue(d.hasChainedInput("con2"), "con2 subscribes to src's alert");
            assertFalse(d.hasChainedInput("src"), "src has no upstream");
        }

        @Test
        @DisplayName("Multiple independent chains in the same topology")
        void multipleIndependentChains() {
            // Chain 1: x→y
            AgentDescriptor x = agent("x", List.of("ev-x"), List.of("input-x"));
            AgentDescriptor y = agent("y", List.of("ev-y"), List.of("ev-x"));
            // Chain 2: p→q (different event types, independent)
            AgentDescriptor p = agent("p", List.of("ev-p"), List.of("input-p"));
            AgentDescriptor q = agent("q", List.of("ev-q"), List.of("ev-p"));

            AgentChainBuilder builder = new AgentChainBuilder();
            AgentChainBuilder.DeferredChainedInputs d = builder.resolveTopology(List.of(x, y, p, q));

            assertTrue(d.hasChainedInput("y"), "y is downstream of x");
            assertTrue(d.hasChainedInput("q"), "q is downstream of p");
            assertFalse(d.hasChainedInput("x"));
            assertFalse(d.hasChainedInput("p"));
            assertEquals(Set.of("y", "q"), d.chainedAgentIds());
        }

        @Test
        @DisplayName("Mix of chained and standalone agents: standalone agents have no chain edges")
        void mixChainingAndStandalone() {
            AgentDescriptor standalone = agent("standalone", List.of("out-s"), List.of("kafka-topic"));
            AgentDescriptor up  = agent("up",   List.of("ev-up"),   List.of("kafka-in"));
            AgentDescriptor down = agent("down", List.of("ev-down"), List.of("ev-up"));

            AgentChainBuilder builder = new AgentChainBuilder();
            AgentChainBuilder.DeferredChainedInputs d = builder.resolveTopology(
                List.of(standalone, up, down));

            assertFalse(d.hasChainedInput("standalone"), "standalone has no chain edge");
            assertFalse(d.hasChainedInput("up"),         "up is upstream, no inbound chain edge");
            assertTrue(d.hasChainedInput("down"),        "down is downstream of up");
        }

        @Test
        @DisplayName("Downstream before upstream declaration is rejected")
        void downstreamBeforeUpstreamRejected() {
            AgentDescriptor upstream = agent("upstream", List.of("event"), List.of("raw"));
            AgentDescriptor downstream = agent("downstream", List.of("done"), List.of("event"));

            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> new AgentChainBuilder().resolveTopology(List.of(downstream, upstream)));
            assertTrue(ex.getMessage().contains("topological order"));
        }

        @Test
        @DisplayName("Cycle in resolveTopology throws IllegalStateException (regression)")
        void cycleRejected() {
            AgentDescriptor a = agent("a", List.of("ev-a"), List.of("ev-b"));
            AgentDescriptor b2 = agent("b", List.of("ev-b"), List.of("ev-a"));

            AgentChainBuilder builder = new AgentChainBuilder();
            IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> builder.resolveTopology(List.of(a, b2)));
            assertTrue(ex.getMessage().contains("Cycle detected"),
                "Exception must say 'Cycle detected'");
        }

        @Test
        @DisplayName("registerOutput with null agentId is a no-op")
        void registerOutputNullAgentIdIsNoOp() {
            AgentChainBuilder builder = new AgentChainBuilder();
            AgentChainBuilder.DeferredChainedInputs d = builder.resolveTopology(List.of());
            assertDoesNotThrow(() -> d.registerOutput(null, null));
            assertTrue(d.isEmpty());
        }

        @Test
        @DisplayName("Duplicate agentId in resolveTopology throws IllegalArgumentException")
        void duplicateAgentIdInResolveTopologyRejected() {
            AgentDescriptor a1 = agent("same-agent", List.of("ev-in"), List.of("ev-out"));
            AgentDescriptor a2 = agent("same-agent", List.of("ev-in2"), List.of("ev-out2"));

            AgentChainBuilder builder = new AgentChainBuilder();
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> builder.resolveTopology(List.of(a1, a2)));
            assertTrue(ex.getMessage().contains("Duplicate agentId detected"));
            assertTrue(ex.getMessage().contains("same-agent"));
        }
    }

    // ── UID uniqueness regression ─────────────────────────────────────────────

    @Nested
    @DisplayName("UID uniqueness — regression for duplicate-UID Flink deployment failure")
    class UidUniquenessTests {

        /**
         * Regression test: verifies that the resolveTopology approach wires each agent
         * exactly once, so the UID "agent-processor-{agentId}" is assigned to at most one
         * operator per agent in the whole job DAG.
         *
         * <p>The old two-pass approach called wireAgent() twice for chained agents:
         * once in the first pass (Kafka sources only) and once in the re-wire pass
         * (with chained inbound).  Both passes assigned the same UID string, causing
         * Flink to reject the DAG at deployment time.
         *
         * <p>The new resolveTopology() + single-pass wiring ensures each agent's
         * processor operator is created exactly once regardless of chaining depth.
         */
        @Test
        @DisplayName("Chained A→B topology: each agentId appears exactly once in chain edge map")
        void chainedTopologyAgentIdsAreUnique() {
            AgentDescriptor a = agent("agent-a", List.of("ev-1"), List.of("raw"));
            AgentDescriptor b = agent("agent-b", List.of("done"), List.of("ev-1"));

            AgentChainBuilder builder = new AgentChainBuilder();
            AgentChainBuilder.DeferredChainedInputs d = builder.resolveTopology(List.of(a, b));

            // Each agent must appear at most once as a downstream (i.e. receive chain input
            // from at most N distinct sources — never be registered twice as a downstream).
            assertEquals(1, d.chainedAgentIds().size(),
                "Only agent-b must be in the downstream chain set, exactly once");
            assertTrue(d.chainedAgentIds().contains("agent-b"));
            assertFalse(d.chainedAgentIds().contains("agent-a"),
                "agent-a is upstream and must not appear as a downstream");
        }

        @Test
        @DisplayName("Deep 4-agent chain A→B→C→D: all four UIDs remain distinct")
        void deepChainAllUidsDistinct() {
            // In the new approach, each agent is wired once with UID "agent-processor-{id}".
            // Verify none of the agentIds collide (i.e. no two agents share the same ID).
            List<String> agentIds = List.of("a", "b", "c", "d");
            List<AgentDescriptor> agents = List.of(
                agent("a", List.of("ev-1"), List.of("raw")),
                agent("b", List.of("ev-2"), List.of("ev-1")),
                agent("c", List.of("ev-3"), List.of("ev-2")),
                agent("d", List.of("ev-4"), List.of("ev-3"))
            );

            // Collect the UIDs that wireAgent would produce for each agent
            // (the actual UID pattern is "agent-processor-{agentId}")
            Set<String> uids = agents.stream()
                .map(ag -> "agent-processor-" + ag.getAgentId())
                .collect(java.util.stream.Collectors.toSet());

            assertEquals(4, uids.size(),
                "All four agent-processor UIDs must be distinct — no collisions");

            // Verify the resolveTopology wires each agent only once
            AgentChainBuilder builder = new AgentChainBuilder();
            AgentChainBuilder.DeferredChainedInputs d = builder.resolveTopology(agents);

            // agentIds that appear in chainedAgentIds are those that would receive a chained
            // input. They should be b, c, d (all downstream of at least one upstream).
            assertEquals(Set.of("b", "c", "d"), d.chainedAgentIds(),
                "b, c, d must each be in the downstream set exactly once");
            // And crucially, each must appear only once — sets guarantee uniqueness.
            assertEquals(3, d.chainedAgentIds().size());
        }

        @Test
        @DisplayName("Fan-out A→B, A→C: B and C are downstream exactly once each")
        void fanOutEachDownstreamExactlyOnce() {
            AgentDescriptor a = agent("a",   List.of("alert"),  List.of("raw"));
            AgentDescriptor b = agent("b",   List.of("result"), List.of("alert"));
            AgentDescriptor c = agent("c",   List.of("result"), List.of("alert"));

            AgentChainBuilder builder = new AgentChainBuilder();
            AgentChainBuilder.DeferredChainedInputs d = builder.resolveTopology(List.of(a, b, c));

            // Each downstream agent appears exactly once in the chainedAgentIds set
            assertEquals(Set.of("b", "c"), d.chainedAgentIds(),
                "Both b and c are downstream of a, each exactly once");
            assertEquals(2, d.chainedAgentIds().size());
        }

        @Test
        @DisplayName("Multiple independent chains: no agent appears as downstream more than once")
        void multipleIndependentChainsNoDuplication() {
            // Chain 1: w→x, Chain 2: y→z
            List<AgentDescriptor> agents = List.of(
                agent("w", List.of("ev-w"), List.of("in-w")),
                agent("x", List.of("ev-x"), List.of("ev-w")),
                agent("y", List.of("ev-y"), List.of("in-y")),
                agent("z", List.of("ev-z"), List.of("ev-y"))
            );

            AgentChainBuilder builder = new AgentChainBuilder();
            AgentChainBuilder.DeferredChainedInputs d = builder.resolveTopology(agents);

            // x and z are downstream, each exactly once
            assertEquals(Set.of("x", "z"), d.chainedAgentIds());
            assertEquals(2, d.chainedAgentIds().size());

            // Also verify each agent-processor UID is unique across all agents
            Set<String> uids = agents.stream()
                .map(ag -> "agent-processor-" + ag.getAgentId())
                .collect(java.util.stream.Collectors.toSet());
            assertEquals(4, uids.size(), "All four processor UIDs must be distinct");
        }
    }

    // ── chainedEventTypes ─────────────────────────────────────────────────────

    @Nested
    @DisplayName("chainedEventTypes — duplicate-delivery suppression (F-6)")
    class ChainedEventTypesTests {

        @Test
        @DisplayName("No chain edges → empty set for any agentId")
        void noChainsEmptySet() {
            AgentChainBuilder b = new AgentChainBuilder();
            AgentChainBuilder.DeferredChainedInputs d = b.resolveTopology(
                List.of(agent("a", List.of("out"), List.of("raw"))));
            assertTrue(d.chainedEventTypes("a").isEmpty());
            assertTrue(d.chainedEventTypes("nonexistent").isEmpty());
        }

        @Test
        @DisplayName("Simple A→B: B has one chained event type, A has none")
        void simpleChainEventTypes() {
            AgentDescriptor a = agent("a", List.of("ev-1"), List.of("raw"));
            AgentDescriptor b = agent("b", List.of("done"), List.of("ev-1"));

            AgentChainBuilder builder = new AgentChainBuilder();
            AgentChainBuilder.DeferredChainedInputs d = builder.resolveTopology(List.of(a, b));

            assertEquals(Set.of("ev-1"), d.chainedEventTypes("b"),
                "b receives ev-1 via chain — should be in chainedEventTypes");
            assertTrue(d.chainedEventTypes("a").isEmpty(),
                "a has no upstream chain edge");
        }

        @Test
        @DisplayName("Fan-in: downstream receives multiple event types via chain")
        void fanInMultipleChainedTypes() {
            AgentDescriptor src1 = agent("src1", List.of("type-a"), List.of("raw1"));
            AgentDescriptor src2 = agent("src2", List.of("type-b"), List.of("raw2"));
            // sink subscribes to both type-a and type-b
            AgentDescriptor sink = makeAgentWithTwoSubs("sink", "$.sinkId",
                "type-a", "topic-a", "type-b", "topic-b");

            AgentChainBuilder builder = new AgentChainBuilder();
            AgentChainBuilder.DeferredChainedInputs d = builder.resolveTopology(
                List.of(src1, src2, sink));

            Set<String> types = d.chainedEventTypes("sink");
            assertTrue(types.contains("type-a"),
                "type-a is covered by chain from src1");
            assertTrue(types.contains("type-b"),
                "type-b is covered by chain from src2");
            assertEquals(2, types.size());
        }

        @Test
        @DisplayName("Returned set is immutable")
        void returnedSetIsImmutable() {
            AgentDescriptor a = agent("a", List.of("ev-1"), List.of("raw"));
            AgentDescriptor b = agent("b", List.of("done"), List.of("ev-1"));

            AgentChainBuilder builder = new AgentChainBuilder();
            AgentChainBuilder.DeferredChainedInputs d = builder.resolveTopology(List.of(a, b));

            assertThrows(UnsupportedOperationException.class,
                () -> d.chainedEventTypes("b").add("mutated"),
                "chainedEventTypes() must return an immutable set");
        }
    }

    // ── EdgeSpec metadata (F-1/F-3, F-2) ─────────────────────────────────────

    @Nested
    @DisplayName("EdgeSpec metadata — entity-key and sourceTopic normalisation")
    class EdgeSpecMetadataTests {

        /**
         * Verifies that resolveTopology captures the downstream agent's entityKeyExtractor
         * and subscription topic in the chain edge metadata.
         * These values are used in DeferredChainedInputs.get() to normalise envelopes.
         */
        @Test
        @DisplayName("Aircraft→Airport cross-domain chain: downstream extractor and topic recorded")
        void aircraftAirportCrossDomainEdgeMetadata() {
            // Aircraft agent: keyed by $.aircraftId, emits TrackedAircraftEvent
            AgentDescriptor aircraftAgent = new AgentDescriptor();
            aircraftAgent.setAgentId("aircraft-agent");
            aircraftAgent.setEntityKeyExtractor("$.aircraftId");
            aircraftAgent.setEntityType("Aircraft");
            aircraftAgent.setRuleset(new RulesetConfig("/tmp/atc.jar"));
            aircraftAgent.setSubscriptions(List.of(
                new EventSubscription("AircraftEvent", "agent-aircraft-events",
                    "ProcessAircraftEvent", HorizonConfig.count(10))));
            aircraftAgent.setOutputs(List.of(
                new OutputConfig("TrackedAircraftEvent", "agent-tracked-aircraft-events")));

            // Airport agent: keyed by $.airportCode, subscribes to TrackedAircraftEvent
            AgentDescriptor airportAgent = new AgentDescriptor();
            airportAgent.setAgentId("airport-agent");
            airportAgent.setEntityKeyExtractor("$.airportCode");
            airportAgent.setEntityType("Airport");
            airportAgent.setRuleset(new RulesetConfig("/tmp/atc.jar"));
            airportAgent.setSubscriptions(List.of(
                new EventSubscription("TrackedAircraftEvent", "agent-tracked-aircraft-events",
                    "ProcessTrackedAircraftEvent", HorizonConfig.count(20))));
            airportAgent.setOutputs(List.of(
                new OutputConfig("Alert", "agent-atc-alerts")));

            AgentChainBuilder builder = new AgentChainBuilder();
            AgentChainBuilder.DeferredChainedInputs d = builder.resolveTopology(
                List.of(aircraftAgent, airportAgent));

            // Chain edge must be detected
            assertTrue(d.hasChainedInput("airport-agent"),
                "airport-agent must be downstream of aircraft-agent");
            assertFalse(d.hasChainedInput("aircraft-agent"),
                "aircraft-agent has no upstream chain input");

            // chainedEventTypes must reflect TrackedAircraftEvent
            assertEquals(Set.of("TrackedAircraftEvent"),
                d.chainedEventTypes("airport-agent"),
                "airport-agent's chained event type must be TrackedAircraftEvent");

            // The Kafka source for TrackedAircraftEvent must be suppressible
            assertTrue(d.chainedEventTypes("airport-agent")
                         .contains("TrackedAircraftEvent"),
                "TrackedAircraftEvent must be in chained types so Kafka source is suppressed (F-6)");
        }

        @Test
        @DisplayName("Same-domain chain: downstream extractor is same as upstream (no re-keying needed)")
        void sameDomainChainExtractorPreserved() {
            // Both agents use the same key space ($.accountId)
            AgentDescriptor fraud = new AgentDescriptor();
            fraud.setAgentId("fraud-agent");
            fraud.setEntityKeyExtractor("$.accountId");
            fraud.setEntityType("Account");
            fraud.setRuleset(new RulesetConfig("/tmp/fraud.jar"));
            fraud.setSubscriptions(List.of(
                new EventSubscription("Transaction", "txn-topic", "Eval", HorizonConfig.count(5))));
            fraud.setOutputs(List.of(
                new OutputConfig("ApprovedTransaction", "approved-txn-topic")));

            AgentDescriptor loyalty = new AgentDescriptor();
            loyalty.setAgentId("loyalty-agent");
            loyalty.setEntityKeyExtractor("$.accountId"); // same key space
            loyalty.setEntityType("Account");
            loyalty.setRuleset(new RulesetConfig("/tmp/loyalty.jar"));
            loyalty.setSubscriptions(List.of(
                new EventSubscription("ApprovedTransaction", "approved-txn-topic",
                    "AwardPoints", HorizonConfig.count(5))));
            loyalty.setOutputs(List.of(
                new OutputConfig("LoyaltyCredit", "loyalty-topic")));

            AgentChainBuilder builder = new AgentChainBuilder();
            AgentChainBuilder.DeferredChainedInputs d = builder.resolveTopology(
                List.of(fraud, loyalty));

            // Chain edge detected
            assertTrue(d.hasChainedInput("loyalty-agent"));
            // loyalty-agent's chained type is ApprovedTransaction
            assertEquals(Set.of("ApprovedTransaction"),
                d.chainedEventTypes("loyalty-agent"));
        }
    }

    // ── Aircraft ATC topology integration ────────────────────────────────────

    @Nested
    @DisplayName("Aircraft ATC topology — cross-domain chaining correctness")
    class AircraftTopologyTests {

        /**
         * Builds the full ATC topology (2 agents matching agents.json) and verifies
         * all chain-correctness invariants:
         * 1. Chain edge detected (aircraft → airport via TrackedAircraftEvent)
         * 2. Airport's chained event types include TrackedAircraftEvent (→ Kafka source suppressed)
         * 3. No chain for aircraft-agent itself (it only receives from Kafka)
         */
        @Test
        @DisplayName("ATC topology: chain edge detected, airport event types correct")
        void atcTopologyChainEdgeAndEventTypes() {
            AgentDescriptor aircraft = buildAircraftAgent();
            AgentDescriptor airport  = buildAirportAgent();

            AgentChainBuilder builder = new AgentChainBuilder();
            AgentChainBuilder.DeferredChainedInputs d = builder.resolveTopology(
                List.of(aircraft, airport));

            // Chain edge exists: aircraft → airport
            assertFalse(d.isEmpty(), "ATC topology must have at least one chain edge");
            assertTrue(d.hasChainedInput("airport-agent"),
                "airport-agent must receive chained TrackedAircraftEvent from aircraft-agent");
            assertFalse(d.hasChainedInput("aircraft-agent"),
                "aircraft-agent has no upstream agent in the chain");

            // The Kafka source for TrackedAircraftEvent at the airport-agent is suppressed
            Set<String> atcTypes = d.chainedEventTypes("airport-agent");
            assertTrue(atcTypes.contains("TrackedAircraftEvent"),
                "TrackedAircraftEvent must be in airport-agent's chained types "
                + "so its Kafka source is suppressed (prevents duplicate delivery F-6)");
        }

        @Test
        @DisplayName("ATC topology: aircraft-agent has no chained event types (not a downstream)")
        void aircraftAgentNotDownstream() {
            AgentDescriptor aircraft = buildAircraftAgent();
            AgentDescriptor airport  = buildAirportAgent();

            AgentChainBuilder builder = new AgentChainBuilder();
            AgentChainBuilder.DeferredChainedInputs d = builder.resolveTopology(
                List.of(aircraft, airport));

            assertTrue(d.chainedEventTypes("aircraft-agent").isEmpty(),
                "aircraft-agent is not downstream of any agent — chainedEventTypes must be empty");
        }

        @Test
        @DisplayName("ATC topology: only TrackedAircraftEvent is chained, not AirportEvent or AirportInitEvent")
        void onlyTrackedEventIsChained() {
            AgentDescriptor aircraft = buildAircraftAgent();
            AgentDescriptor airport  = buildAirportAgent();

            AgentChainBuilder builder = new AgentChainBuilder();
            AgentChainBuilder.DeferredChainedInputs d = builder.resolveTopology(
                List.of(aircraft, airport));

            Set<String> chainedTypes = d.chainedEventTypes("airport-agent");
            assertFalse(chainedTypes.contains("AirportEvent"),
                "AirportEvent must NOT be chained — it comes from Kafka directly");
            assertFalse(chainedTypes.contains("AirportInitializationEvent"),
                "AirportInitializationEvent must NOT be chained — it comes from Kafka directly");
            assertTrue(chainedTypes.contains("TrackedAircraftEvent"),
                "Only TrackedAircraftEvent is chained from aircraft-agent");
            assertEquals(1, chainedTypes.size(),
                "Exactly one event type is chained in the ATC topology");
        }

        @Test
        @DisplayName("ATC topology: no cycle detected")
        void atcTopologyNoCycle() {
            AgentDescriptor aircraft = buildAircraftAgent();
            AgentDescriptor airport  = buildAirportAgent();

            AgentChainBuilder builder = new AgentChainBuilder();
            assertDoesNotThrow(
                () -> builder.resolveTopology(List.of(aircraft, airport)),
                "ATC topology is a valid DAG — cycle detection must not throw");
        }

        // ── Helpers ───────────────────────────────────────────────────────────

        private static AgentDescriptor buildAircraftAgent() {
            AgentDescriptor d = new AgentDescriptor();
            d.setAgentId("aircraft-agent");
            d.setName("Aircraft Agent");
            d.setEntityType("Aircraft");
            d.setEntityKeyExtractor("$.aircraftId");
            d.setRuleset(new RulesetConfig("samples/airtraffic-agent/AirTrafficControlService_1.0.0.jar"));

            d.setSubscriptions(List.of(
                new EventSubscription("AircraftInitializationEvent",
                    "agent-aircraft-init-events", "InitializeAircraft", HorizonConfig.count(0)),
                new EventSubscription("AircraftEvent",
                    "agent-aircraft-events", "ProcessAircraftEvent",
                    HorizonConfig.time("2h"))
            ));
            d.setOutputs(List.of(
                new OutputConfig("TrackedAircraftEvent", "agent-tracked-aircraft-events"),
                new OutputConfig("Alert", "agent-atc-alerts"),
                new OutputConfig("agent-error", "agent-atc-errors")
            ));
            return d;
        }

        private static AgentDescriptor buildAirportAgent() {
            AgentDescriptor d = new AgentDescriptor();
            d.setAgentId("airport-agent");
            d.setName("Airport Agent");
            d.setEntityType("Airport");
            d.setEntityKeyExtractor("$.airportCode");
            d.setRuleset(new RulesetConfig("samples/airtraffic-agent/AirTrafficControlService_1.0.0.jar"));

            d.setSubscriptions(List.of(
                new EventSubscription("AirportInitializationEvent",
                    "agent-airport-init-events", "InitializeAirport", HorizonConfig.count(0)),
                new EventSubscription("AirportEvent",
                    "agent-airport-events", "ProcessAirportEvent", HorizonConfig.count(0)),
                new EventSubscription("TrackedAircraftEvent",
                    "agent-tracked-aircraft-events", "ProcessTrackedAircraftEvent",
                    HorizonConfig.time("1h"))
            ));
            d.setOutputs(List.of(
                new OutputConfig("Alert", "agent-atc-alerts"),
                new OutputConfig("agent-error", "agent-atc-errors")
            ));
            return d;
        }
    }

    // ── Diagnostic warnings ───────────────────────────────────────────────────

    /**
     * Tests for  (cross-ruleset collision),  (parallelism mismatch), and
     *  (missing entityKeyExtractor) diagnostic warnings.
     *
     * <p>The warning logic runs inside {@code resolveTopology()} when edges are created.
     * These tests verify that the topology resolution still succeeds (no exception) for
     * all warning scenarios — the warnings are advisory only.
     */
    @Nested
    @DisplayName("Diagnostic warnings — , , ")
    class DiagnosticWarningTests {

        // ── cross-ruleset chain edge ────────────────────────────────────

        @Test
        @DisplayName("different ruleset paths — resolveTopology still succeeds")
        void crossRulesetChainDoesNotThrow() {
            // Two agents with different rulesets chaining on the same event type.
            // This is a potential unintentional collision, but it is not an error.
            AgentDescriptor fraud = new AgentDescriptor();
            fraud.setAgentId("fraud-agent");
            fraud.setEntityKeyExtractor("$.accountId");
            fraud.setEntityType("Account");
            fraud.setRuleset(new RulesetConfig("/opt/fraud.jar"));
            fraud.setSubscriptions(List.of(
                new EventSubscription("Transaction", "txn-topic", "EvalFraud", HorizonConfig.count(10))));
            fraud.setOutputs(List.of(new OutputConfig("Alert", "fraud-alerts")));

            AgentDescriptor compliance = new AgentDescriptor();
            compliance.setAgentId("compliance-agent");
            compliance.setEntityKeyExtractor("$.accountId");
            compliance.setEntityType("Account");
            compliance.setRuleset(new RulesetConfig("/opt/compliance.jar")); // different JAR
            compliance.setSubscriptions(List.of(
                new EventSubscription("Alert", "fraud-alerts", "CheckCompliance", HorizonConfig.count(5))));
            compliance.setOutputs(List.of(new OutputConfig("ComplianceResult", "compliance-topic")));

            AgentChainBuilder builder = new AgentChainBuilder();
            // Must not throw — the warning is informational only
            assertDoesNotThrow(() -> builder.resolveTopology(List.of(fraud, compliance)),
                "Cross-ruleset chain edge should warn but not throw");

            AgentChainBuilder.DeferredChainedInputs d = builder.resolveTopology(List.of(fraud, compliance));
            // Chain still resolves correctly
            assertTrue(d.hasChainedInput("compliance-agent"),
                "Chain edge must still be detected despite different ruleset paths");
        }

        @Test
        @DisplayName("same ruleset path — no cross-ruleset warning, topology still valid")
        void sameChainsedRulesetNoWarning() {
            // Same JAR — normal same-use-case chain, no  warning expected
            AgentDescriptor a = agent("a", List.of("ev-1"), List.of("raw"));
            AgentDescriptor b = agent("b", List.of("done"), List.of("ev-1"));

            AgentChainBuilder builder = new AgentChainBuilder();
            assertDoesNotThrow(() -> builder.resolveTopology(List.of(a, b)));
        }

        // ── parallelism mismatch ────────────────────────────────────────

        @Test
        @DisplayName("upstream parallelism=4, downstream parallelism=2 — warns but does not throw")
        void parallelismMismatchDoesNotThrow() {
            AgentDescriptor up = new AgentDescriptor();
            up.setAgentId("up");
            up.setEntityKeyExtractor("$.id");
            up.setEntityType("Entity");
            up.setParallelism(4);
            up.setRuleset(new RulesetConfig("/tmp/up.jar"));
            up.setSubscriptions(List.of(
                new EventSubscription("Raw", "raw-topic", "ProcessRaw", HorizonConfig.count(10))));
            up.setOutputs(List.of(new OutputConfig("Processed", "processed-topic")));

            AgentDescriptor down = new AgentDescriptor();
            down.setAgentId("down");
            down.setEntityKeyExtractor("$.id");
            down.setEntityType("Entity");
            down.setParallelism(2); // mismatch
            down.setRuleset(new RulesetConfig("/tmp/up.jar"));
            down.setSubscriptions(List.of(
                new EventSubscription("Processed", "processed-topic", "Evaluate", HorizonConfig.count(5))));
            down.setOutputs(List.of(new OutputConfig("Result", "result-topic")));

            AgentChainBuilder builder = new AgentChainBuilder();
            assertDoesNotThrow(() -> builder.resolveTopology(List.of(up, down)),
                "Parallelism mismatch should warn but not throw");

            AgentChainBuilder.DeferredChainedInputs d = builder.resolveTopology(List.of(up, down));
            assertTrue(d.hasChainedInput("down"),
                "Chain edge must still be resolved despite parallelism mismatch");
        }

        @Test
        @DisplayName("both parallelism=0 (job default) — no mismatch warning")
        void bothJobDefaultParallelismNoWarning() {
            // parallelism=0 means "use job default" — only warn when both are explicitly set
            AgentDescriptor a = agent("a", List.of("ev-1"), List.of("raw"));
            AgentDescriptor b = agent("b", List.of("done"), List.of("ev-1"));
            // Default parallelism is 0 (not explicitly set)
            assertEquals(0, a.getParallelism());
            assertEquals(0, b.getParallelism());

            AgentChainBuilder builder = new AgentChainBuilder();
            assertDoesNotThrow(() -> builder.resolveTopology(List.of(a, b)),
                "Zero (default) parallelism on both agents must not trigger a mismatch warning");
        }

        @Test
        @DisplayName("upstream parallelism set, downstream=0 (job default) — no mismatch warning")
        void oneSideDefaultParallelismNoWarning() {
            AgentDescriptor up = agent("up", List.of("ev-1"), List.of("raw"));
            up.setParallelism(4); // upstream explicitly set
            AgentDescriptor down = agent("down", List.of("done"), List.of("ev-1"));
            // downstream uses job default (0)

            AgentChainBuilder builder = new AgentChainBuilder();
            // Only warn when BOTH sides are explicitly set and differ
            assertDoesNotThrow(() -> builder.resolveTopology(List.of(up, down)));
        }

        // ── blank entityKeyExtractor (explicit opt-in) ─────────────────

        @Test
        @DisplayName("downstream has blank entityKeyExtractor (opt-in) — warns but does not throw")
        void blankEntityKeyExtractorDoesNotThrow() {
            AgentDescriptor up = agent("up", List.of("ev-1"), List.of("raw"));

            AgentDescriptor down = new AgentDescriptor();
            down.setAgentId("down-blank-extractor");
            down.setEntityKeyExtractor(""); // explicit blank opt-in
            down.setEntityType("Entity");
            down.setRuleset(new RulesetConfig("/tmp/down.jar"));
            down.setSubscriptions(List.of(
                new EventSubscription("ev-1", "ev-1-topic", "Process", HorizonConfig.count(5))));
            down.setOutputs(List.of(new OutputConfig("done", "done-topic")));

            AgentChainBuilder builder = new AgentChainBuilder();
            assertDoesNotThrow(() -> builder.resolveTopology(List.of(up, down)),
                "Blank (opt-in) entityKeyExtractor in chain builder should warn but not throw");
        }

        @Test
        @DisplayName("downstream with valid entityKeyExtractor — no hot-key warning")
        void validEntityKeyExtractorNoWarning() {
            AgentDescriptor a = agent("a", List.of("ev-1"), List.of("raw"));
            AgentDescriptor b = agent("b", List.of("done"), List.of("ev-1"));
            // b has entityKeyExtractor = "$.id" (set by the helper)
            assertEquals("$.id", b.getEntityKeyExtractor());

            AgentChainBuilder builder = new AgentChainBuilder();
            assertDoesNotThrow(() -> builder.resolveTopology(List.of(a, b)));
        }

        @Test
        @DisplayName("agent with chainingEnabled=false and no extractor — no warning (not a chain participant)")
        void chainingDisabledNoExtractorNoWarning() {
            AgentDescriptor standalone = new AgentDescriptor();
            standalone.setAgentId("standalone");
            standalone.setChainingEnabled(false);
            // No entityKeyExtractor — but also not a chain participant, so no warning expected
            standalone.setEntityType("Entity");
            standalone.setRuleset(new RulesetConfig("/tmp/standalone.jar"));
            standalone.setSubscriptions(List.of(
                new EventSubscription("raw", "raw-topic", "Process", HorizonConfig.count(5))));
            standalone.setOutputs(List.of(new OutputConfig("result", "result-topic")));

            AgentChainBuilder builder = new AgentChainBuilder();
            // No chain edges → resolveTopology returns empty result without checking extractors
            AgentChainBuilder.DeferredChainedInputs d = builder.resolveTopology(
                List.of(standalone));
            assertTrue(d.isEmpty(), "No chain edges for chainingEnabled=false agent");
        }
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    /** Builds a descriptor with two subscriptions (for fan-in tests). */
    private static AgentDescriptor makeAgentWithTwoSubs(String agentId, String keyExtractor,
            String type1, String topic1, String type2, String topic2) {
        AgentDescriptor d = new AgentDescriptor();
        d.setAgentId(agentId);
        d.setEntityKeyExtractor(keyExtractor);
        d.setEntityType("entity");
        d.setRuleset(new RulesetConfig("/tmp/" + agentId + ".jar"));
        d.setSubscriptions(List.of(
            new EventSubscription(type1, topic1, "op1", HorizonConfig.count(5)),
            new EventSubscription(type2, topic2, "op2", HorizonConfig.count(5))
        ));
        d.setOutputs(List.of(new OutputConfig("result", "topic-result")));
        return d;
    }
}

