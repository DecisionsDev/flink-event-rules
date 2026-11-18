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

/*
 * Pure graph model + layout of the read-only Agent Diagram View.
 *
 *   agents.json  ->  buildGraph()  ->  { nodes, edges, stats }  (React Flow ready)
 *
 * Topics are shared nodes (keyed by topic name), so agent chaining
 * (agent A outputs topic X, agent B subscribes to X) naturally renders as
 *     topic -> agent A -> topic X -> agent B -> topic
 * Nodes are placed in columns by dependency depth, ordered per column with a
 * barycentre heuristic to keep edge crossings low.
 */

// ─── Geometry (shared with the node components, which size themselves from it) ──
export const GEO = {
  topicW: 244,
  topicH: 120,
  agentW: 348,
  headH: 68,   // agent card header
  chipsH: 40,  // agent chips row (entity, ops, state …)
  statH: 34,   // live stat strip
  opsHeadH: 32,
  opH: 48,
  opGap: 6,
  padB: 12,
  gapX: 240,   // horizontal gap between columns (room for edge labels)
  gapY: 40,    // vertical gap between nodes of a column
  inOutY: 34,  // default connection-port height on an agent card
};

// ─── Palette (blue accent from App.css, plus semantic hues) ─────────────────
export const PALETTE = {
  agent:    '#2563eb',
  entity:   '#7c3aed',
  source:   '#4f46e5',
  schedule: '#9333ea',
  link:     '#0891b2',
  sink:     '#16a34a',
  alert:    '#d97706',
  error:    '#dc2626',
};

export const TOPIC_KIND_META = {
  source:   { label: 'Event source', color: PALETTE.source },
  schedule: { label: 'Scheduler',    color: PALETTE.schedule },
  link:     { label: 'Chained topic', color: PALETTE.link },
  sink:     { label: 'Output',       color: PALETTE.sink },
  alert:    { label: 'Alert output', color: PALETTE.alert },
  error:    { label: 'Error output', color: PALETTE.error },
};

// ─── Small helpers ──────────────────────────────────────────────────────────

export function basename(p) {
  return p ? String(p).replace(/.*[/\\]/, '') : '';
}

/**
 * Compact, human-readable form of an agents.json horizon.
 * { type:'hybrid', countLimit:100, timeLimit:'30d' } -> "100 events / 30d"
 */
export function formatHorizon(horizon) {
  if (!horizon) return null;
  const { type, countLimit, value, timeLimit } = horizon;
  if (type === 'count')  return `${countLimit ?? value ?? 0} events`;
  if (type === 'time')   return timeLimit || null;
  if (type === 'hybrid') return `${countLimit ?? value ?? 0} events / ${timeLimit || '?'}`;
  return type || null;
}

/** 'error' | 'alert' | 'data' — drives colour and hide-toggles of outputs. */
export function eventKind(eventType) {
  const t = String(eventType || '');
  if (/error/i.test(t)) return 'error';
  if (/alert/i.test(t)) return 'alert';
  return 'data';
}

const pushUnique = (arr, v) => { if (v && !arr.includes(v)) arr.push(v); };

function topicKindOf(t) {
  if (t.schedule) return 'schedule';
  if (t.producers.length === 0) return 'source';
  if (t.consumers.length > 0) return 'link';
  const kinds = t.eventTypes.map(eventKind);
  if (kinds.includes('error')) return 'error';
  if (kinds.includes('alert')) return 'alert';
  return 'sink';
}

/** An operation is shown when it is subscribed to or carries real configuration. */
function opIsConfigured(o) {
  return !!(o.entityField || o.currentEventField || o.scheduleRequestsField
    || (o.history && o.history.length) || (o.outbounds && o.outbounds.length));
}

/** Vertical offset of the i-th operation row inside an agent card. */
function opRowTop(i) {
  return GEO.headH + GEO.chipsH + GEO.statH + GEO.opsHeadH + i * (GEO.opH + GEO.opGap);
}

export function agentHeight(opCount, expanded) {
  const base = GEO.headH + GEO.chipsH + GEO.statH;
  if (!expanded || opCount === 0) return base;
  return base + GEO.opsHeadH + opCount * (GEO.opH + GEO.opGap) + GEO.padB;
}

// ─── Graph construction ─────────────────────────────────────────────────────

/**
 * @param {object} agentsJson   parsed agents.json
 * @param {{expanded?: Set<string>, hidden?: Set<string>}} [opts]
 *        expanded — agentIds whose card shows decision operations
 *        hidden   — output kinds to leave out ('alert' | 'error'); only pure sinks are removed
 */
export function buildGraph(agentsJson, { expanded = new Set(), hidden = new Set() } = {}) {
  const agents = agentsJson?.agents ?? [];
  const topicMeta = new Map((agentsJson?.topics ?? []).map(t => [t.name, t]));

  const topics = new Map(); // node id -> topic data
  const getTopic = (id, name, schedule = false) => {
    if (!topics.has(id)) {
      const meta = topicMeta.get(name) || {};
      topics.set(id, {
        id, name, schedule,
        eventTypes: [], label: meta.label || '', description: '',
        producers: [], consumers: [],
        order: topics.size,
      });
    }
    return topics.get(id);
  };

  // Pass 1 — subscriptions (so pass 2 knows which output topics are consumed).
  const subLinks = [];
  for (const agent of agents) {
    (agent.subscriptions ?? []).forEach((sub, i) => {
      const schedule = !sub.topic;
      const tid = schedule ? `schedule:${agent.agentId}` : `topic:${sub.topic}`;
      const t = getTopic(tid, schedule ? 'Scheduler' : sub.topic, schedule);
      pushUnique(t.eventTypes, sub.eventType);
      t.consumers.push({
        agentId: agent.agentId, agentName: agent.name || agent.agentId,
        eventType: sub.eventType, operation: sub.operation || '',
        horizon: formatHorizon(sub.horizon),
      });
      subLinks.push({ tid, agent, sub, i });
    });
  }

  // Pass 2 — outputs.
  const outLinks = [];
  for (const agent of agents) {
    (agent.outputs ?? []).forEach((out, i) => {
      if (!out.topic) return;
      const tid = `topic:${out.topic}`;
      const consumed = (topics.get(tid)?.consumers.length ?? 0) > 0;
      if (!consumed && hidden.has(eventKind(out.eventType))) return;
      const t = getTopic(tid, out.topic);
      pushUnique(t.eventTypes, out.eventType);
      if (!t.label && out.label) t.label = out.label;
      if (!t.description && out.description) t.description = out.description;
      t.producers.push({ agentId: agent.agentId, agentName: agent.name || agent.agentId, eventType: out.eventType });
      outLinks.push({ tid, agent, out, i });
    });
  }

  const agentById = new Map(agents.map(a => [a.agentId, a]));
  for (const t of topics.values()) {
    t.kind = topicKindOf(t);
    const involved = [...t.producers, ...t.consumers].map(x => agentById.get(x.agentId));
    t.chained = t.kind === 'link' && involved.every(a => a && a.chainingEnabled !== false);
  }

  // Agent nodes (+ operation rows).
  const agentNodes = agents.map(agent => {
    const subs = agent.subscriptions ?? [];
    const rsOps = agent.ruleset?.operations ?? [];
    const subscribedOps = [];
    subs.forEach(s => pushUnique(subscribedOps, s.operation));
    const names = [...subscribedOps];
    for (const o of rsOps) if (opIsConfigured(o)) pushUnique(names, o.operation);
    const isExpanded = expanded.has(agent.agentId);

    const rows = names.map((name, i) => {
      const cfg = rsOps.find(o => o.operation === name) || {};
      const ins = [];
      subs.filter(s => s.operation === name).forEach(s => pushUnique(ins, s.eventType));
      const outs = [];
      (cfg.outbounds ?? []).forEach(b => pushUnique(outs, b.eventType));
      const top = opRowTop(i);
      return {
        name, ins, outs,
        schedule: !!cfg.scheduleRequestsField,
        top, handleY: top + GEO.opH / 2,
      };
    });

    return {
      id: `agent:${agent.agentId}`,
      type: 'agent',
      width: GEO.agentW,
      height: agentHeight(rows.length, isExpanded),
      data: {
        agentId: agent.agentId,
        name: agent.name || agent.agentId,
        entityType: agent.entityType || '',
        jar: basename(agent.ruleset?.path),
        ops: rows,
        opTotal: rsOps.length || rows.length,
        subCount: subs.length,
        outCount: outLinks.filter(l => l.agent === agent).length,
        stateBackend: agent.stateConfig?.backend || '',
        stateTtl: agent.stateConfig?.ttl || '',
        chaining: agent.chainingEnabled !== false,
        parallelism: agent.parallelism || 0,
        expanded: isExpanded,
        agent,
      },
    };
  });
  const agentNodeById = new Map(agentNodes.map(n => [n.data.agentId, n]));

  const topicNodes = [...topics.values()].map(t => ({
    id: t.id,
    type: 'topic',
    width: GEO.topicW,
    height: GEO.topicH,
    data: { ...t },
  }));
  const kindOfTopic = new Map(topicNodes.map(n => [n.id, n.data.kind]));

  // Edges.
  const edges = [];
  let gid = 0;
  for (const { tid, agent, sub, i } of subLinks) {
    const an = agentNodeById.get(agent.agentId);
    const row = an.data.ops.find(o => o.name === sub.operation);
    const useOpPort = an.data.expanded && row;
    const kind = kindOfTopic.get(tid);
    edges.push({
      id: `sub:${agent.agentId}:${i}`,
      source: tid,
      target: an.id,
      sourceHandle: 'out',
      targetHandle: useOpPort ? `in:${sub.operation}` : 'in',
      type: 'flow',
      data: {
        gid: gid++, direction: 'in', agentId: agent.agentId,
        eventType: sub.eventType, operation: sub.operation || '', ops: sub.operation ? [sub.operation] : [],
        horizon: formatHorizon(sub.horizon),
        category: 'data', chained: !!topics.get(tid).chained,
        srcColor: TOPIC_KIND_META[kind].color, tgtColor: PALETTE.agent,
      },
    });
  }
  for (const { tid, agent, out, i } of outLinks) {
    const an = agentNodeById.get(agent.agentId);
    const emitting = an.data.ops.filter(o => o.outs.includes(out.eventType));
    const useOpPort = an.data.expanded && emitting.length > 0;
    const kind = kindOfTopic.get(tid);
    edges.push({
      id: `out:${agent.agentId}:${i}`,
      source: an.id,
      target: tid,
      sourceHandle: useOpPort ? `out:${emitting[0].name}` : 'out',
      targetHandle: 'in',
      type: 'flow',
      data: {
        gid: gid++, direction: 'out', agentId: agent.agentId,
        eventType: out.eventType, operation: '', ops: emitting.map(o => o.name),
        horizon: null,
        category: eventKind(out.eventType), chained: !!topics.get(tid).chained,
        srcColor: PALETTE.agent, tgtColor: TOPIC_KIND_META[kind].color,
      },
    });
  }

  const nodes = [...topicNodes, ...agentNodes];
  layout(nodes, edges);

  return {
    nodes, edges,
    stats: {
      agents: agentNodes.length,
      topics: topicNodes.length,
      links: edges.length,
      chains: topicNodes.filter(n => n.data.chained).length,
    },
  };
}

// ─── Layout ─────────────────────────────────────────────────────────────────

function addTo(map, key, value) {
  if (!map.has(key)) map.set(key, new Set());
  map.get(key).add(value);
}

/** Assigns `position` to every node (mutates). Columns by dependency depth, barycentre ordering. */
function layout(nodes, edges) {
  if (nodes.length === 0) return;
  const byId = new Map(nodes.map(n => [n.id, n]));
  const agentsN = nodes.filter(n => n.type === 'agent');

  const producersOf = new Map(); // topic id -> agent node ids
  const consumersOf = new Map(); // topic id -> agent node ids
  const neighbours = new Map(nodes.map(n => [n.id, new Set()]));
  for (const e of edges) {
    neighbours.get(e.source).add(e.target);
    neighbours.get(e.target).add(e.source);
    if (byId.get(e.source).type === 'agent') addTo(producersOf, e.target, e.source);
    else addTo(consumersOf, e.source, e.target);
  }

  // Agent depth = longest chain of upstream agents (bounded passes guard against cycles).
  const layer = new Map(agentsN.map(n => [n.id, 0]));
  for (let pass = 0; pass < agentsN.length; pass++) {
    let changed = false;
    for (const [tid, prods] of producersOf) {
      for (const p of prods) for (const c of consumersOf.get(tid) ?? []) {
        if (p !== c && layer.get(c) < layer.get(p) + 1) { layer.set(c, layer.get(p) + 1); changed = true; }
      }
    }
    if (!changed) break;
  }

  const colOf = new Map();
  for (const n of agentsN) colOf.set(n.id, layer.get(n.id) * 2 + 1);
  for (const n of nodes) {
    if (n.type !== 'topic') continue;
    const prods = producersOf.get(n.id);
    const cons = consumersOf.get(n.id);
    if (prods?.size) colOf.set(n.id, Math.max(...[...prods].map(p => colOf.get(p))) + 1);
    else if (cons?.size) colOf.set(n.id, Math.min(...[...cons].map(c => colOf.get(c))) - 1);
    else colOf.set(n.id, 0);
  }

  const usedCols = [...new Set(colOf.values())].sort((a, b) => a - b);
  const columns = usedCols.map(c => nodes.filter(n => colOf.get(n.id) === c));

  // Barycentre ordering: alternate sweeps, stable on ties.
  const rank = new Map();
  const reRank = () => columns.forEach(col => col.forEach((n, i) => rank.set(n.id, col.length > 1 ? i / (col.length - 1) : 0.5)));
  reRank();
  for (let iter = 0; iter < 6; iter++) {
    const seq = iter % 2 === 0 ? columns : [...columns].reverse();
    for (const col of seq) {
      const key = new Map(col.map((n, i) => {
        const nb = [...neighbours.get(n.id)].filter(id => colOf.get(id) !== colOf.get(n.id));
        const bary = nb.length ? nb.reduce((s, id) => s + rank.get(id), 0) / nb.length : rank.get(n.id);
        return [n.id, { bary, i }];
      }));
      col.sort((a, b) => (key.get(a.id).bary - key.get(b.id).bary) || (key.get(a.id).i - key.get(b.id).i));
      col.forEach((n, i) => rank.set(n.id, col.length > 1 ? i / (col.length - 1) : 0.5));
    }
  }

  let x = 0;
  for (const col of columns) {
    const colW = Math.max(...col.map(n => n.width));
    const total = col.reduce((s, n) => s + n.height, 0) + (col.length - 1) * GEO.gapY;
    let y = -total / 2;
    for (const n of col) {
      n.position = { x: x + (colW - n.width) / 2, y };
      y += n.height + GEO.gapY;
    }
    x += colW + GEO.gapX;
  }
}

// ─── Lineage & search ───────────────────────────────────────────────────────

/** Every node/edge upstream and downstream of `nodeId`. */
export function lineage(graph, nodeId) {
  const outBy = new Map();
  const inBy = new Map();
  for (const e of graph.edges) {
    if (!outBy.has(e.source)) outBy.set(e.source, []);
    if (!inBy.has(e.target)) inBy.set(e.target, []);
    outBy.get(e.source).push(e);
    inBy.get(e.target).push(e);
  }
  const nodes = new Set([nodeId]);
  const edges = new Set();
  const walk = (start, adj, next) => {
    const seen = new Set([start]);
    const queue = [start];
    while (queue.length) {
      const cur = queue.shift();
      for (const e of adj.get(cur) ?? []) {
        edges.add(e.id);
        const nid = next(e);
        nodes.add(nid);
        if (!seen.has(nid)) { seen.add(nid); queue.push(nid); }
      }
    }
  };
  walk(nodeId, outBy, e => e.target);
  walk(nodeId, inBy, e => e.source);
  return { nodes, edges };
}

/** Lineage of a single edge's endpoints plus the edge itself. */
export function edgeFocus(edge) {
  return { nodes: new Set([edge.source, edge.target]), edges: new Set([edge.id]) };
}

/** Focus for one operation row of an agent: its edges and their endpoints. */
export function operationFocus(graph, agentId, opName) {
  const agentNodeId = `agent:${agentId}`;
  const nodes = new Set([agentNodeId]);
  const edges = new Set();
  for (const e of graph.edges) {
    if (e.data.agentId === agentId && e.data.ops.includes(opName)) {
      edges.add(e.id); nodes.add(e.source); nodes.add(e.target);
    }
  }
  return { nodes, edges };
}

function searchText(node) {
  const d = node.data;
  if (node.type === 'agent') {
    return [d.name, d.agentId, d.entityType, d.jar, ...d.ops.map(o => o.name)].join(' ');
  }
  return [d.name, d.label, d.description, ...d.eventTypes,
    ...d.producers.map(p => p.agentName), ...d.consumers.map(c => `${c.agentName} ${c.operation}`)].join(' ');
}

/** Ids of nodes matching a free-text query (every whitespace-separated term must match). */
export function matchNodes(graph, query) {
  const terms = String(query || '').toLowerCase().split(/\s+/).filter(Boolean);
  if (terms.length === 0) return null;
  const ids = new Set();
  for (const n of graph.nodes) {
    const hay = searchText(n).toLowerCase();
    if (terms.every(t => hay.includes(t))) ids.add(n.id);
  }
  return ids;
}
