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

import org.apache.flink.streaming.api.datastream.DataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Resolves inter-agent chaining within a single Flink job: connects the output stream of an
 * upstream agent directly to the input of any downstream agent that subscribes to the same
 * event type, avoiding an unnecessary round-trip through Kafka.
 *
 * <p>Call {@link #resolveTopology(List)} to obtain a {@link DeferredChainedInputs} handle.
 * Wire agents one-by-one in declaration order; after each agent is wired call
 * {@link DeferredChainedInputs#registerOutput(String, DataStream)} to supply its output stream.
 * Subsequent calls to {@link DeferredChainedInputs#get(String)} then return the correctly-filtered,
 * normalised union stream for that downstream agent.
 *
 * <p>{@link #resolveTopology(List)} runs a Kahn's-algorithm topological sort (O(V+E)) over the
 * chain-edge graph and throws {@link IllegalStateException} on any cycle.  It also emits
 * diagnostic warnings for common configuration pitfalls (event-type collisions across different
 * use cases, parallelism mismatches).  Not thread-safe; use single-threaded during job
 * construction only.
 */
public class AgentChainBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(AgentChainBuilder.class);

    // ── Public API ────────────────────────────────────────────────────────────

    /**
     * Resolves all chain edges from descriptor metadata only and returns a
     * {@link DeferredChainedInputs} that agents are wired into exactly once.
     *
     * <p>This method builds a <em>deferred</em> result: the chain graph (upstream/downstream
     * edges and event-type filters) is computed entirely from {@link AgentDescriptor} metadata.
     * Actual {@link DataStream} objects are not needed yet.  The caller wires agents one-by-one
     * (in topological order), and after each agent is wired it calls
     * {@link DeferredChainedInputs#registerOutput(String, DataStream)} to supply its
     * output stream.  Subsequent calls to {@link DeferredChainedInputs#get(String)} then
     * return the correctly-filtered union stream built from all already-registered upstream outputs.
     *
     * <p><strong>Diagnostic warnings emitted by this method:</strong>
     * <ul>
     *   <li><b> / event-type collision</b> — a WARN is logged whenever a chain edge is
     *       created between two agents that use different ruleset {@code path} values.
     *       Different paths typically indicate different use cases; an implicit chain between
     *       them is likely unintentional and caused by a shared event-type name.</li>
     *   <li><b> / parallelism mismatch</b> — a WARN is logged when the upstream and
     *       downstream agents declare different non-zero parallelism values.  Different
     *       parallelism levels force a Flink network exchange on the chain stream, partially
     *       negating the latency benefit of in-process chaining.</li>
     *   <li><b> / missing entityKeyExtractor</b> — a WARN is logged when the downstream
     *       agent has no {@code entityKeyExtractor} configured.  All events normalised through
     *       the chain will key to {@code "unknown"}, concentrating processing on a single Flink
     *       partition (hot-key problem).</li>
     * </ul>
     *
     * @param orderedAgents list of descriptors in the desired wiring order (typically the
     *                      original declaration order, which is validated to be a valid
     *                      topological ordering by the cycle-detection step)
     * @return a {@link DeferredChainedInputs} ready to receive output streams as agents
     *         are wired
     * @throws IllegalStateException when a cycle is detected in the chain graph
     */
    public DeferredChainedInputs resolveTopology(List<AgentDescriptor> orderedAgents) {
        if (orderedAgents == null || orderedAgents.isEmpty()) {
            return new DeferredChainedInputs(Map.of());
        }

        // Validate uniqueness of agent IDs across the topology
        Set<String> seenAgentIds = new java.util.HashSet<>();
        for (AgentDescriptor agent : orderedAgents) {
            if (agent == null) continue;
            String id = agent.getAgentId();
            if (id != null && !id.isBlank()) {
                if (!seenAgentIds.add(id)) {
                    throw new IllegalArgumentException(
                        "Duplicate agentId detected in topology: '" + id
                        + "'. Each agent in the chain topology must have a unique agentId.");
                }
            }
        }

        // Build the edge map: downstream agentId → list of (upstream agentId, eventType) pairs
        // that represent chain edges — i.e. the upstream emits a type the downstream subscribes to.
        Map<String, List<EdgeSpec>> downstreamEdges = new HashMap<>();
        Map<String, Set<String>> edgesFrom = new HashMap<>(); // for cycle detection

        for (AgentDescriptor downstream : orderedAgents) {
            if (downstream.getSubscriptions() == null) continue;
            // If this agent has opted out of chaining it never accepts a chained inbound stream.
            if (!downstream.isChainingEnabled()) continue;

            // ── warn if downstream has a blank entityKeyExtractor (opt-in single-partition mode)
            // A null entityKeyExtractor is now rejected by validate() before the job starts;
            // the blank case is the explicit opt-in and still deserves a chain-level notice.
            if (downstream.getEntityKeyExtractor() != null
                    && downstream.getEntityKeyExtractor().isBlank()) {
                LOG.warn("[AgentChainBuilder] Agent '{}' has chainingEnabled=true and "
                    + "entityKeyExtractor=\"\" (blank opt-in). All events normalised through "
                    + "any chain edge pointing to this agent will key to 'unknown', "
                    + "concentrating processing on a single Flink partition (hot-key). "
                    + "Consider setting a non-blank entityKeyExtractor.",
                    downstream.getAgentId());
            }

            for (EventSubscription sub : downstream.getSubscriptions()) {
                String subscribedType = sub.getEventType();

                for (AgentDescriptor upstream : orderedAgents) {
                    if (upstream.getAgentId().equals(downstream.getAgentId())) continue; // self
                    if (upstream.getOutputs() == null) continue;
                    // If the upstream has opted out of chaining it never feeds the chain.
                    if (!upstream.isChainingEnabled()) continue;

                    boolean upstreamEmitsType = upstream.getOutputs().stream()
                        .anyMatch(o -> subscribedType.equals(o.getEventType()));

                    if (upstreamEmitsType) {
                        downstreamEdges
                            .computeIfAbsent(downstream.getAgentId(), k -> new ArrayList<>())
                            .add(new EdgeSpec(
                                upstream.getAgentId(),
                                subscribedType,
                                downstream.getEntityKeyExtractor(),  // for entity-key re-extraction
                                sub.getTopic()));                     // for sourceTopic correction

                        edgesFrom
                            .computeIfAbsent(upstream.getAgentId(),
                                k -> new java.util.LinkedHashSet<>())
                            .add(downstream.getAgentId());

                        LOG.info("[AgentChainBuilder] Chain edge: {} → {} (eventType={} "
                            + "downstreamTopic={} downstreamKeyExtractor={})",
                            upstream.getAgentId(), downstream.getAgentId(), subscribedType,
                            sub.getTopic(), downstream.getEntityKeyExtractor());

                        // ── warn on cross-ruleset chain edges ────────────
                        // Different ruleset paths typically signal different use cases.
                        // A chain between them is likely unintentional (event-type name collision).
                        String upPath   = upstream.getRuleset() != null
                            ? upstream.getRuleset().getPath() : null;
                        String downPath = downstream.getRuleset() != null
                            ? downstream.getRuleset().getPath() : null;
                        if (upPath != null && downPath != null && !upPath.equals(downPath)) {
                            LOG.warn("[AgentChainBuilder] Cross-ruleset chain edge detected: "
                                + "{} (ruleset={}) → {} (ruleset={}) on eventType='{}'. "
                                + "Agents from different rulesets chaining on the same event-type "
                                + "name may indicate an unintentional collision. "
                                + "Set chainingEnabled=false on one agent if this chain is unwanted.",
                                upstream.getAgentId(), upPath,
                                downstream.getAgentId(), downPath, subscribedType);
                        }

                        // ── warn on parallelism mismatch ─────────────────
                        // Different parallelism levels force a Flink network exchange on the
                        // chain stream, partially negating the latency advantage of chaining.
                        int upPar   = upstream.getParallelism();
                        int downPar = downstream.getParallelism();
                        if (upPar > 0 && downPar > 0 && upPar != downPar) {
                            LOG.warn("[AgentChainBuilder] Parallelism mismatch on chain edge "
                                + "{} (parallelism={}) → {} (parallelism={}): "
                                + "a Flink network shuffle will be inserted on the chain stream, "
                                + "reducing the latency benefit of in-process chaining. "
                                + "Consider aligning parallelism values or accepting the trade-off.",
                                upstream.getAgentId(), upPar,
                                downstream.getAgentId(), downPar);
                        }
                    }
                }
            }
        }

        detectCycles(edgesFrom);
        validateWiringOrder(orderedAgents, downstreamEdges);

        // ── Topology summary log ───────────────────────────────────────────────
        if (downstreamEdges.isEmpty()) {
            LOG.info("[AgentChainBuilder] No chain edges detected — all agents are independent");
        } else {
            int edgeCount = downstreamEdges.values().stream().mapToInt(List::size).sum();
            LOG.info("[AgentChainBuilder] Resolved {} chain edge(s) across {} downstream agent(s)",
                edgeCount, downstreamEdges.size());
            logTopologySummary(downstreamEdges, orderedAgents);
        }

        return new DeferredChainedInputs(downstreamEdges);
    }


    /**
     * Runs Kahn's topological sort on the chain-edge graph described by {@code edgesFrom}.
     *
     * <p>If the graph contains a cycle, all nodes that are part of a strongly-connected
     * component (i.e. never reach in-degree 0) are identified and reported in an
     * {@link IllegalStateException} with a human-readable message showing which agents
     * are involved.
     *
     * <p>Time complexity: O(V + E) where V = agents, E = chain edges.
     *
     * @param edgesFrom adjacency map: upstream agent ID → set of downstream agent IDs
     * @throws IllegalStateException when a cycle is detected
     */
    static void detectCycles(Map<String, Set<String>> edgesFrom) {
        if (edgesFrom.isEmpty()) return;

        // Collect every node that appears in the graph (sources AND sinks)
        Set<String> allNodes = new java.util.LinkedHashSet<>(edgesFrom.keySet());
        for (Set<String> targets : edgesFrom.values()) {
            allNodes.addAll(targets);
        }

        // Compute in-degree for each node
        Map<String, Integer> inDegree = new HashMap<>();
        for (String node : allNodes) inDegree.put(node, 0);
        for (Set<String> targets : edgesFrom.values()) {
            for (String t : targets) inDegree.merge(t, 1, Integer::sum);
        }

        // Kahn's BFS: start from all zero-in-degree nodes
        Queue<String> queue = new ArrayDeque<>();
        for (Map.Entry<String, Integer> e : inDegree.entrySet()) {
            if (e.getValue() == 0) queue.add(e.getKey());
        }

        int visited = 0;
        while (!queue.isEmpty()) {
            String node = queue.poll();
            visited++;
            Set<String> neighbours = edgesFrom.getOrDefault(node, Set.of());
            for (String neighbour : neighbours) {
                int newDegree = inDegree.merge(neighbour, -1, Integer::sum);
                if (newDegree == 0) queue.add(neighbour);
            }
        }

        if (visited < allNodes.size()) {
            // Nodes never reaching in-degree 0 are part of a cycle
            Set<String> cycleNodes = allNodes.stream()
                .filter(n -> inDegree.get(n) > 0)
                .collect(Collectors.toCollection(java.util.LinkedHashSet::new));

            throw new IllegalStateException(
                "Cycle detected in agent chain graph — agents involved: " + cycleNodes
                + ". Agent chains must form a directed acyclic graph (DAG). "
                + "Review the outputs/subscriptions of these agents and remove the circular dependency.");
        }
    }

    /**
     * Ensures the declaration order is topological so chained inputs are registered before
     * downstream agents are wired. This prevents a missing upstream stream from being silently
     * omitted by deferred resolution.
     */
    private static void validateWiringOrder(List<AgentDescriptor> orderedAgents,
                                            Map<String, List<EdgeSpec>> downstreamEdges) {
        Map<String, Integer> positions = new HashMap<>();
        for (int i = 0; i < orderedAgents.size(); i++) {
            AgentDescriptor agent = orderedAgents.get(i);
            if (agent != null) positions.put(agent.getAgentId(), i);
        }

        for (Map.Entry<String, List<EdgeSpec>> entry : downstreamEdges.entrySet()) {
            Integer downstreamPosition = positions.get(entry.getKey());
            for (EdgeSpec edge : entry.getValue()) {
                Integer upstreamPosition = positions.get(edge.upstreamAgentId());
                if (upstreamPosition != null && downstreamPosition != null
                        && upstreamPosition >= downstreamPosition) {
                    throw new IllegalArgumentException(
                        "Agent descriptors must be in topological order: upstream agent '"
                        + edge.upstreamAgentId() + "' must appear before downstream agent '"
                        + entry.getKey() + "'.");
                }
            }
        }
    }

    // ── Nested types ──────────────────────────────────────────────────────────

    /**
     * A chain edge specification: the upstream agent ID and the event type that
     * the downstream agent subscribes to from that upstream agent.
     *
     * <p>The two additional fields support correct envelope normalisation when the
     * chained stream crosses an entity-key boundary (e.g. aircraft-agent→airport-agent):
     * <ul>
     *   <li>{@code downstreamEntityKeyExtractor} — JSONPath expression used by the
     *       downstream agent to extract its entity ID from the event payload; applied
     *       in a {@code map()} step so that {@code keyBy(entityId)} in the downstream
     *       agent partitions correctly (fixes F-1/F-3).</li>
     *   <li>{@code downstreamSubscriptionTopic} — the Kafka topic name that the
     *       downstream agent's subscription declares for this event type; set as
     *       {@code sourceTopic} so that the downstream's operation resolver and history
     *       updater see the expected topic (fixes F-2).</li>
     * </ul>
     */
    record EdgeSpec(String upstreamAgentId,
                    String eventType,
                    String downstreamEntityKeyExtractor,
                    String downstreamSubscriptionTopic) {}

    /**
     * A deferred, incrementally-built view of chain inputs produced by
     * {@link #resolveTopology(List)}.
     *
     * <p>{@code DeferredChainedInputs} holds only the edge metadata at creation time.
     * As each agent is wired and its output stream becomes available, the caller registers it via
     * {@link #registerOutput(String, DataStream)}. Subsequent {@link #get(String)} calls
     * build the filtered union stream on-the-fly from already-registered outputs.
     *
     * <p>This approach guarantees that every agent is wired exactly once, which in turn
     * guarantees that the Flink operator UID {@code "agent-processor-<agentId>"} is
     * assigned to exactly one operator in the entire job DAG.
     */
    public static final class DeferredChainedInputs {

        /**
         * For each downstream agent: the list of (upstreamId, eventType, ...) specs that
         * constitute its chain edges.  Computed once at construction from descriptor
         * metadata, never mutated afterwards.
         */
        private final Map<String, List<EdgeSpec>> downstreamEdges;

        /**
         * Output streams of already-wired upstream agents, keyed by agent ID.
         * Populated incrementally via {@link #registerOutput}.
         */
        private final Map<String, DataStream<EventEnvelope>> wiredOutputs = new HashMap<>();

        DeferredChainedInputs(Map<String, List<EdgeSpec>> downstreamEdges) {
            this.downstreamEdges = downstreamEdges;
        }

        /**
         * Records the output stream of a newly-wired agent so it can be provided
         * as a chained input to any downstream agents registered later.
         *
         * @param agentId      the agent whose sub-graph was just wired
         * @param outputStream the processed {@link DataStream} returned by
         *                     {@code wireAgent()}
         */
        public void registerOutput(String agentId, DataStream<EventEnvelope> outputStream) {
            if (agentId != null && outputStream != null) {
                wiredOutputs.put(agentId, outputStream);
            }
        }

        /**
         * Returns {@code true} when at least one upstream agent is configured to feed
         * into {@code agentId} via a chain edge (regardless of whether the upstream
         * stream has been registered yet).
         */
        public boolean hasChainedInput(String agentId) {
            List<EdgeSpec> edges = downstreamEdges.get(agentId);
            return edges != null && !edges.isEmpty();
        }

        /**
         * Returns the set of event types that arrive via chain edges for {@code agentId}.
         *
         * <p>Used by {@link AgentBasedDataStreamJob#wireAgent} to suppress Kafka sources
         * for subscriptions that are already covered by an incoming chain, avoiding
         * duplicate event delivery (one copy via chain + one via Kafka round-trip).
         *
         * @return immutable set of eventType strings; empty when no chain edges exist
         */
        public java.util.Set<String> chainedEventTypes(String agentId) {
            List<EdgeSpec> edges = downstreamEdges.get(agentId);
            if (edges == null || edges.isEmpty()) return java.util.Set.of();
            java.util.Set<String> types = new java.util.LinkedHashSet<>();
            for (EdgeSpec edge : edges) types.add(edge.eventType());
            return java.util.Collections.unmodifiableSet(types);
        }

        /**
         * Returns the union of all chained upstream streams for {@code agentId},
         * filtered to the subscribing event type, normalised for the downstream agent,
         * or {@code null} when there are none.
         *
         * <p>Each chained stream is normalised before being delivered to the downstream
         * agent to ensure that the downstream's {@code keyBy(entityId)} partitions
         * correctly and its operation resolver finds the right subscription:
         * <ol>
         *   <li><b>Entity-key re-extraction (F-1/F-3)</b> — the envelope's
         *       {@code entityId} is re-extracted from the event payload using the
         *       downstream agent's own {@code entityKeyExtractor} expression via
         *       {@link JsonPathUtils#extractEntityId}.  This is essential when the
         *       upstream and downstream agents manage entities in different key spaces
         *       (e.g. aircraft-agent keys by {@code $.aircraftId} while airport-agent
         *       keys by {@code $.airportCode}).</li>
         *   <li><b>sourceTopic correction (F-2)</b> — the envelope's {@code sourceTopic}
         *       is set to the downstream agent's configured subscription topic for this
         *       event type.  This ensures the downstream's
         *       {@link StatefulAgentProcessor#resolveOperation} topic-match arm finds
         *       the correct subscription, and that output envelopes carry the expected
         *       provenance topic rather than the upstream agent's original source topic.</li>
         * </ol>
         *
         * <p>The topology validator requires callers to wire agents in topological order, so every
         * resolved upstream must be registered before this method is called.
         */
        public DataStream<EventEnvelope> get(String agentId) {
            List<EdgeSpec> edges = downstreamEdges.get(agentId);
            if (edges == null || edges.isEmpty()) return null;

            DataStream<EventEnvelope> union = null;
                for (EdgeSpec edge : edges) {
                    DataStream<EventEnvelope> upstreamOut = wiredOutputs.get(edge.upstreamAgentId());
                    if (upstreamOut == null) {
                        // Upstream not yet registered — skip this edge (caller wires in order;
                        // if the stream is genuinely missing the topology validator will have
                        // already reported the out-of-order declaration at resolveTopology time).
                        continue;
                    }

                final String eventType         = edge.eventType();
                final String keyExtractor      = edge.downstreamEntityKeyExtractor();
                final String subscriptionTopic = edge.downstreamSubscriptionTopic();

                DataStream<EventEnvelope> filtered = upstreamOut
                    .filter(e -> eventType.equals(e.getEventType()))
                    .name("Chain[" + edge.upstreamAgentId() + "→" + agentId
                        + "/" + eventType + "]");

                // ── Normalise envelope for the downstream agent ──────────────────
                // Re-extract the entity ID using the downstream agent's own extractor
                // via JsonPathUtils (single canonical implementation — Weakness 5 fix).
                // Correct the sourceTopic to the downstream's subscription topic so
                // that operation resolution and history update find the right entry.
                DataStream<EventEnvelope> normalised = filtered
                    .map(envelope -> {
                        EventEnvelope out = new EventEnvelope();
                        out.setEventType(envelope.getEventType());
                        out.setPayload(envelope.getPayload());
                        out.setTimestamp(envelope.getTimestamp());
                        out.setMetadata(envelope.getMetadata());
                        // Re-key via shared utility (ensures identical extraction to Kafka path)
                        out.setEntityId(JsonPathUtils.extractEntityId(
                            envelope.getPayload(), keyExtractor));
                        // Correct sourceTopic to the downstream's subscription topic
                        out.setSourceTopic(subscriptionTopic != null ? subscriptionTopic
                                                                      : envelope.getSourceTopic());
                        return out;
                    })
                    .name("ChainNorm[" + edge.upstreamAgentId() + "→" + agentId
                        + "/" + eventType + "]");

                union = (union == null) ? normalised : union.union(normalised);
            }
            return union;
        }

        /** The set of agent IDs that have at least one chain edge pointing to them. */
        public java.util.Set<String> chainedAgentIds() {
            return downstreamEdges.keySet();
        }

        /** Returns {@code true} when no chain edges were resolved. */
        public boolean isEmpty() {
            return downstreamEdges.isEmpty();
        }
    }

    // ── Utilities ─────────────────────────────────────────────────────────────

    /**
     * Logs a human-readable topology summary at INFO level.
     * Summarises each chain edge with upstream → downstream, event type, suppressed Kafka topics,
     * and key extractor, so operators can verify the topology without grepping raw logs.
     */
    private static void logTopologySummary(Map<String, List<EdgeSpec>> downstreamEdges,
                                           List<AgentDescriptor> agents) {
        // Build a quick lookup: agentId → descriptor (for parallelism display)
        Map<String, AgentDescriptor> byId = new HashMap<>();
        for (AgentDescriptor a : agents) byId.put(a.getAgentId(), a);

        StringBuilder sb = new StringBuilder();
        sb.append("[AgentChainBuilder] ── Chain Topology Summary ──────────────────────────\n");
        for (Map.Entry<String, List<EdgeSpec>> entry : downstreamEdges.entrySet()) {
            String downId = entry.getKey();
            for (EdgeSpec edge : entry.getValue()) {
                AgentDescriptor up   = byId.get(edge.upstreamAgentId());
                AgentDescriptor down = byId.get(downId);
                int upPar   = up   != null ? up.getParallelism()   : 0;
                int downPar = down != null ? down.getParallelism() : 0;
                sb.append(String.format(
                    "  %s (p=%s) ──[%s]──► %s (p=%s)%n"
                    + "      suppressed Kafka topic : %s%n"
                    + "      downstream key extractor: %s%n",
                    edge.upstreamAgentId(),
                    upPar > 0 ? upPar : "job-default",
                    edge.eventType(),
                    downId,
                    downPar > 0 ? downPar : "job-default",
                    edge.downstreamSubscriptionTopic() != null
                        ? edge.downstreamSubscriptionTopic() : "(none)",
                    edge.downstreamEntityKeyExtractor() != null
                        ? edge.downstreamEntityKeyExtractor() : "(none — hot-key risk)"));
            }
        }
        sb.append("[AgentChainBuilder] ─────────────────────────────────────────────────────");
        LOG.info("{}", sb);
    }

    private static String outputEventTypes(AgentDescriptor d) {
        if (d.getOutputs() == null) return "[]";
        return d.getOutputs().stream()
            .map(OutputConfig::getEventType)
            .collect(java.util.stream.Collectors.joining(", ", "[", "]"));
    }
}
