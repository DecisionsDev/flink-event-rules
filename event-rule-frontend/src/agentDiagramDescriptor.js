/*
 * Conversion between an agents.json descriptor and the model edited by the Agent Diagram
 * Editor (React Flow nodes + edges).
 *
 *   descriptorToDiagram(descriptor)         -> { meta, nodes, edges }
 *   buildDiagramDescriptor(meta, nodes, edges) -> descriptor
 *
 * Mapping
 *   topics[]                          Topic node
 *   agents[]                          Agent node
 *   agent.entityType / changeLogTopic Entity node  --entity-->  Agent
 *   agent.subscriptions[]             Topic node  --op-in:<operation>-->  Agent operation
 *                                     (operation "" -> the agent-level "history only" handle;
 *                                      no topic      -> agent.data.topiclessSubscriptions)
 *   ruleset.operations[].outbounds[]  Agent operation  --op-out:<operation>-->  Outbound node
 *   agent.outputs[]                   Outbound node (also linked from the agent-level
 *                                     handle when no operation produces it)
 *
 * Every property the editor has no widget for is kept in an `extra` object and written back
 * unchanged, so opening and saving a descriptor never loses information.
 */

export const opInHandle = (op) => `op-in:${op}`;
export const opOutHandle = (op) => `op-out:${op}`;
export const TOPIC_OUT_HANDLE = 'topic-out';
export const HISTORY_IN_HANDLE = 'sub-history-in';
export const AGENT_OUT_HANDLE = 'agent-out';
export const ENTITY_IN_HANDLE = 'agent-entity-in';
export const ENTITY_OUT_HANDLE = 'entity-out';
export const OUTBOUND_IN_HANDLE = 'outbound-in';

// Properties the editor models explicitly, per descriptor object.
const TOP_KEYS = ['name', 'flinkJobName', 'description', 'consumerGroupId', 'parallelism', 'topics', 'agents'];
const TOPIC_KEYS = ['name', 'type', 'label', 'schema', 'idleness'];
const AGENT_KEYS = [
  'agentId', 'name', 'description', 'entityType', 'entityKeyExtractor', 'chainingEnabled', 'parallelism',
  'references', 'subscriptions', 'ruleset', 'outputs', 'stateConfig',
];
const SUB_KEYS = ['eventType', 'topic', 'operation', 'entityKeyExtractor', 'horizon', 'schema', 'sqlEnrichment', 'enrichment'];
const RULESET_KEYS = ['path', 'project', 'archiveControlTopic', 'operations'];
const OPERATION_KEYS = [
  'operation', 'entityField', 'currentEventField', 'outboundEntityField', 'scheduleRequestsField', 'history', 'outbounds',
];
const OP_OUTBOUND_KEYS = ['field', 'eventType'];
const OUTPUT_KEYS = [
  'eventType', 'topic', 'label', 'description', 'schema', 'sqlEnrichment', 'enrichment', 'filterKey', 'filterValue', 'filterLabel',
];
const STATE_KEYS = ['ttl', 'backend', 'checkpointInterval', 'changeLogTopic'];
const ENRICHMENT_KEYS = ['reference', 'key', 'on', 'when', 'into', 'cardinality', 'fields'];

// ─── Small helpers ──────────────────────────────────────────────────────────

const isObj = (v) => v !== null && typeof v === 'object' && !Array.isArray(v);
const text = (v) => (typeof v === 'string' ? v.trim() : '');
const list = (v) => (Array.isArray(v) ? v : []);

/** The properties of `obj` that are not in `known`, or undefined when there are none. */
function extraOf(obj, known) {
  const extra = {};
  for (const [k, v] of Object.entries(obj || {})) if (!known.includes(k)) extra[k] = v;
  return Object.keys(extra).length > 0 ? extra : undefined;
}

/** Adds the `extra` properties to `target` without overriding the modelled ones. */
function withExtra(target, extra) {
  for (const [k, v] of Object.entries(extra || {})) if (!(k in target)) target[k] = v;
  return target;
}

/** target[key] = trimmed value, unless blank. */
function put(target, key, value) {
  const t = text(value);
  if (t) target[key] = t;
}

/**
 * target[key] = trimmed value whenever a string is set, even blank: for entityKeyExtractor an
 * explicit "" is meaningful (it opts out of keying) and differs from an absent property.
 */
function putPresent(target, key, value) {
  if (typeof value === 'string') target[key] = value.trim();
}

// ─── Descriptor builders ────────────────────────────────────────────────────

function buildHorizon(h) {
  if (!isObj(h)) return undefined;
  const description = text(h.description);
  const tail = description ? { description } : {};
  const n = (v, dflt) => (v !== '' && v != null ? Number(v) : dflt);
  if (h.type === 'count') return { type: 'count', value: n(h.value, 100), ...tail };
  if (h.type === 'time') return { type: 'time', timeLimit: text(h.timeLimit), ...tail };
  if (h.type === 'hybrid') {
    return { type: 'hybrid', countLimit: n(h.countLimit, 100), timeLimit: text(h.timeLimit), ...tail };
  }
  return undefined;
}

/** { schema?, sqlEnrichment? } of a subscription or an output. */
function buildSqlFields(d) {
  const out = {};
  put(out, 'schema', d.schema);
  const se = isObj(d.sqlEnrichment) ? d.sqlEnrichment : {};
  if (text(se.sqlFile)) {
    const sql = { sqlFile: text(se.sqlFile) };
    put(sql, 'inputViewName', se.inputViewName);
    put(sql, 'outputViewName', se.outputViewName);
    out.sqlEnrichment = sql;
  }
  return out;
}

/** An enrichment field is "name" or { "column": "newName" }. */
function buildEnrichmentFields(fields) {
  return list(fields).flatMap((f) => {
    if (typeof f === 'string') return text(f) ? [text(f)] : [];
    if (!isObj(f)) return [];
    const [source, target] = Object.entries(f)[0] || [];
    const src = text(source);
    const tgt = text(target);
    if (!src) return [];
    return [tgt && tgt !== src ? { [src]: tgt } : src];
  });
}

function buildEnrichment(steps) {
  return list(steps)
    .filter(isObj)
    .filter((s) => text(s.reference) || text(s.key) || text(s.on) || text(s.into))
    .map((s) => {
      const out = {};
      for (const k of ['reference', 'key', 'on', 'when', 'into']) put(out, k, s[k]);
      out.fields = buildEnrichmentFields(s.fields);
      put(out, 'cardinality', s.cardinality);
      return withExtra(out, extraOf(s, ENRICHMENT_KEYS));
    });
}

/** Subscription entry from the edge / topic-less data `d`. */
function buildSubscription(d, { topic, operation, eventType }) {
  const sub = {};
  put(sub, 'eventType', eventType || d.eventType);
  put(sub, 'topic', topic);
  sub.operation = operation;
  putPresent(sub, 'entityKeyExtractor', d.entityKeyExtractor);
  const horizon = buildHorizon(d.horizon);
  if (horizon) sub.horizon = horizon;
  Object.assign(sub, buildSqlFields(d));
  const enrichment = buildEnrichment(d.enrichment);
  if (enrichment.length > 0) sub.enrichment = enrichment;
  return withExtra(sub, d.extra);
}

function buildOutput(node, fallbackEventType) {
  const d = node.data || {};
  const out = {};
  put(out, 'eventType', d.eventType || fallbackEventType);
  put(out, 'topic', d.topic);
  put(out, 'label', d.label);
  put(out, 'description', d.description);
  Object.assign(out, buildSqlFields(d));
  const enrichment = buildEnrichment(d.enrichment);
  if (enrichment.length > 0) out.enrichment = enrichment;
  put(out, 'filterKey', d.filterKey);
  put(out, 'filterValue', d.filterValue);
  put(out, 'filterLabel', d.filterLabel);
  if (!out.eventType && !out.topic) return null;
  // An output only implied by an operation outbound stays implicit until it gets a topic.
  if (d.implicit && !out.topic) return null;
  return withExtra(out, d.extra);
}

function buildOperation(op, cfg, outEdges, nodeById) {
  const obj = { operation: op };
  put(obj, 'entityField', cfg.entityField);
  put(obj, 'currentEventField', cfg.currentEventField);
  put(obj, 'scheduleRequestsField', cfg.scheduleRequestsField);
  put(obj, 'outboundEntityField', cfg.outboundEntityField);

  const history = list(cfg.history)
    .filter((h) => text(h?.eventType) || text(h?.field))
    .map((h) => ({ ...h, eventType: text(h.eventType), field: text(h.field) }));
  if (history.length > 0) obj.history = history;

  const outbounds = outEdges
    .map((e) => {
      const tgt = nodeById.get(e.target);
      return withExtra(
        { field: text(e.data?.field), eventType: text(tgt?.data?.eventType || e.data?.eventType) },
        e.data?.extra,
      );
    })
    .filter((o) => o.field || o.eventType);
  if (outbounds.length > 0) obj.outbounds = outbounds;

  return withExtra(obj, cfg.extra);
}

/**
 * Builds the agents.json descriptor from the application metadata, the diagram nodes and edges.
 */
export function buildDiagramDescriptor(agentsMeta = {}, nodes = [], edges = []) {
  const nodeById = new Map(nodes.map((n) => [n.id, n]));
  const descriptor = {};
  put(descriptor, 'name', agentsMeta.name);
  put(descriptor, 'flinkJobName', agentsMeta.flinkJobName);
  put(descriptor, 'description', agentsMeta.description);
  put(descriptor, 'consumerGroupId', agentsMeta.consumerGroupId);
  if (Number(agentsMeta.parallelism) > 0) descriptor.parallelism = Number(agentsMeta.parallelism);

  // Topics. A topic only implied by a subscription is not declared, unless other topics are:
  // a declared topics[] replaces the list the BFF derives from the subscriptions.
  const declaresTopics = nodes.some((n) => n.type === 'topic' && !n.data?.implicit);
  const topics = nodes
    .filter((n) => n.type === 'topic' && (declaresTopics || !n.data?.implicit))
    .map((n) => {
      const d = n.data || {};
      const topic = {};
      put(topic, 'name', d.name || d.label);
      put(topic, 'type', d.type);
      put(topic, 'label', d.label);
      put(topic, 'schema', d.schema);
      put(topic, 'idleness', d.idleness);
      return topic.name ? withExtra(topic, d.extra) : null;
    })
    .filter(Boolean);
  if (topics.length > 0) descriptor.topics = topics;

  // Agents
  descriptor.agents = nodes
    .filter((n) => n.type === 'agent')
    .map((agentNode) => {
      const a = agentNode.data || {};
      const incoming = edges.filter((e) => e.target === agentNode.id);
      const outgoing = edges.filter((e) => e.source === agentNode.id);

      const agent = {
        agentId: text(a.agentId),
        name: text(a.label || a.agentId),
      };
      put(agent, 'description', a.description);

      // Entity (entity -> agent)
      let changeLogTopic = '';
      for (const e of incoming) {
        const src = nodeById.get(e.source);
        if (src?.type !== 'entity') continue;
        put(agent, 'entityType', src.data?.entityType);
        if (text(src.data?.topic)) changeLogTopic = text(src.data.topic);
      }
      putPresent(agent, 'entityKeyExtractor', a.entityKeyExtractor);
      if (typeof a.chainingEnabled === 'boolean') agent.chainingEnabled = a.chainingEnabled;
      if (Number(a.parallelism) > 0) agent.parallelism = Number(a.parallelism);

      const references = list(a.references)
        .filter((r) => isObj(r) && text(r.name))
        .map((r) => {
          // pendingFile is a File waiting for upload; it never belongs in the descriptor
          const ref = { ...r, name: text(r.name), type: text(r.type) || 'csv', file: text(r.file) };
          delete ref.pendingFile;
          return ref;
        });
      if (references.length > 0) agent.references = references;

      // Subscriptions: topic -> operation edges, then the topic-less ones (e.g. Schedule)
      const subscriptions = [];
      for (const e of incoming) {
        const src = nodeById.get(e.source);
        if (src?.type !== 'topic') continue;
        const operation = e.targetHandle?.startsWith('op-in:')
          ? e.targetHandle.slice('op-in:'.length)
          : text(e.data?.operation);
        subscriptions.push(buildSubscription(e.data || {}, {
          topic: text(src.data?.name || src.data?.label),
          operation,
          // A topic only implied by a subscription takes the event type of that subscription.
          eventType: src.data?.implicit
            ? text(e.data?.eventType) || text(src.data?.type)
            : text(src.data?.type) || text(e.data?.eventType),
        }));
      }
      for (const s of list(a.topiclessSubscriptions)) {
        subscriptions.push(buildSubscription(s, { operation: text(s.operation) }));
      }
      if (subscriptions.length > 0) agent.subscriptions = subscriptions;

      // Ruleset
      const ruleset = {};
      put(ruleset, 'path', a.ruleJar);
      put(ruleset, 'project', a.ruleProject);
      put(ruleset, 'archiveControlTopic', a.archiveControlTopic);
      const opNames = Array.from(new Set([
        ...list(a.ruleOperations),
        ...Object.keys(a.operationConfigs || {}),
      ]));
      const operations = opNames.map((op) => buildOperation(
        op,
        a.operationConfigs?.[op] || {},
        outgoing.filter((e) => e.sourceHandle === opOutHandle(op)),
        nodeById,
      ));
      if (operations.length > 0) ruleset.operations = operations;
      withExtra(ruleset, a.rulesetExtra);
      if (Object.keys(ruleset).length > 0) agent.ruleset = ruleset;

      // Outputs: each Outbound node linked from this agent, once, whatever the number of links
      const linked = new Set(outgoing.map((e) => e.target));
      const outputs = nodes
        .filter((n) => n.type === 'outbound' && linked.has(n.id))
        .map((n) => buildOutput(n, outgoing.find((e) => e.target === n.id)?.data?.eventType))
        .filter(Boolean);

      if (a.errorOutput) {
        const eo = a.errorOutput;
        const err = { eventType: 'agent-error' };
        put(err, 'topic', eo.topic);
        put(err, 'label', eo.label);
        put(err, 'description', eo.description);
        const finalErr = withExtra(err, eo.extra);
        if (finalErr.topic || finalErr.label || finalErr.description || eo.extra) {
          outputs.push(finalErr);
        }
      }
      if (outputs.length > 0) agent.outputs = outputs;

      // State
      const state = {};
      const sc = isObj(a.stateConfig) ? a.stateConfig : {};
      for (const k of ['ttl', 'backend', 'checkpointInterval']) put(state, k, sc[k]);
      withExtra(state, extraOf(sc, STATE_KEYS));
      if (changeLogTopic) state.changeLogTopic = changeLogTopic;
      if (Object.keys(state).length > 0) agent.stateConfig = state;

      return withExtra(agent, a.extra);
    });

  return withExtra(descriptor, agentsMeta.extra);
}

// ─── Descriptor -> diagram ──────────────────────────────────────────────────

const subscriptionData = (s) => ({
  eventType: s.eventType || '',
  operation: s.operation ?? '',
  entityKeyExtractor: typeof s.entityKeyExtractor === 'string' ? s.entityKeyExtractor : undefined,
  horizon: isObj(s.horizon) ? { ...s.horizon } : undefined,
  schema: s.schema || '',
  sqlEnrichment: isObj(s.sqlEnrichment) ? { ...s.sqlEnrichment } : undefined,
  enrichment: list(s.enrichment),
  extra: extraOf(s, SUB_KEYS),
});

/**
 * Turns an agents.json descriptor into { meta, nodes, edges }. Edges carry no styling (the
 * editor decorates them); nodes are laid out with layoutNodes().
 */
export function descriptorToDiagram(descriptor) {
  const d = isObj(descriptor) ? descriptor : {};
  const meta = {
    name: d.name || '',
    description: d.description || '',
    flinkJobName: d.flinkJobName || '',
    consumerGroupId: d.consumerGroupId || '',
    parallelism: Number(d.parallelism) > 0 ? Number(d.parallelism) : '',
    extra: extraOf(d, TOP_KEYS),
  };

  const nodes = [];
  const edges = [];
  let edgeSeq = 0;
  const addEdge = (source, sourceHandle, target, targetHandle, data) => {
    edges.push({ id: `edge-${++edgeSeq}`, source, sourceHandle, target, targetHandle, ...(data && { data }) });
  };

  // Topic nodes are shared by name; those only implied by a subscription are "implicit"
  // (not written to topics[] until edited, so a plain open/save keeps the file unchanged).
  const topicByName = new Map();
  const addTopic = (name, data) => {
    const node = { id: `topic-${topicByName.size + 1}`, type: 'topic', position: { x: 0, y: 0 }, data: { name, ...data } };
    nodes.push(node);
    topicByName.set(name, node);
    return node;
  };
  for (const t of list(d.topics)) {
    if (!isObj(t) || !t.name || topicByName.has(t.name)) continue;
    addTopic(t.name, {
      type: t.type || '',
      label: t.label || '',
      schema: t.schema || '',
      idleness: t.idleness || '',
      extra: extraOf(t, TOPIC_KEYS),
    });
  }

  list(d.agents).filter(isObj).forEach((a, ai) => {
    const agentNodeId = `agent-${ai + 1}`;
    const rs = isObj(a.ruleset) ? a.ruleset : {};
    const sc = isObj(a.stateConfig) ? a.stateConfig : {};
    const operations = list(rs.operations).filter(isObj);

    const ruleOperations = operations.map((o) => o.operation).filter(Boolean);
    const operationConfigs = {};
    for (const o of operations) {
      if (!o.operation) continue;
      operationConfigs[o.operation] = {
        entityField: o.entityField || '',
        currentEventField: o.currentEventField || '',
        scheduleRequestsField: o.scheduleRequestsField || '',
        outboundEntityField: o.outboundEntityField || '',
        history: list(o.history).filter(isObj).map((h) => ({ ...h })),
        extra: extraOf(o, OPERATION_KEYS),
      };
    }

    const agentNode = {
      id: agentNodeId,
      type: 'agent',
      position: { x: 0, y: 0 },
      data: {
        label: a.name || a.agentId || '',
        agentId: a.agentId || '',
        description: a.description || '',
        entityKeyExtractor: typeof a.entityKeyExtractor === 'string' ? a.entityKeyExtractor : undefined,
        parallelism: Number(a.parallelism) > 0 ? Number(a.parallelism) : '',
        // undefined = not declared (the backend then defaults to true)
        chainingEnabled: typeof a.chainingEnabled === 'boolean' ? a.chainingEnabled : undefined,
        references: list(a.references).filter(isObj).map((r) => ({ ...r })),
        ruleJar: rs.path || '',
        ruleProject: rs.project || '',
        archiveControlTopic: rs.archiveControlTopic || '',
        ruleOperations,
        operationConfigs,
        topiclessSubscriptions: [],
        errorOutput: (() => {
          const eo = list(a.outputs).find((o) => isObj(o) && o.eventType === 'agent-error');
          return eo
            ? {
                eventType: 'agent-error',
                topic: eo.topic || '',
                label: eo.label || '',
                description: eo.description || '',
                extra: extraOf(eo, OUTPUT_KEYS),
              }
            : { eventType: 'agent-error', topic: '', label: '', description: '' };
        })(),
        stateConfig: Object.fromEntries(Object.entries(sc).filter(([k]) => k !== 'changeLogTopic')),
        rulesetExtra: extraOf(rs, RULESET_KEYS),
        extra: extraOf(a, AGENT_KEYS),
      },
    };
    nodes.push(agentNode);

    // Entity
    if (a.entityType || sc.changeLogTopic) {
      const entityId = `entity-${ai + 1}`;
      nodes.push({
        id: entityId,
        type: 'entity',
        position: { x: 0, y: 0 },
        data: { label: a.entityType || 'Entity', entityType: a.entityType || '', topic: sc.changeLogTopic || '' },
      });
      addEdge(entityId, ENTITY_OUT_HANDLE, agentNodeId, ENTITY_IN_HANDLE);
    }

    // Subscriptions
    for (const s of list(a.subscriptions).filter(isObj)) {
      const op = s.operation ?? '';
      if (op && !ruleOperations.includes(op)) {
        ruleOperations.push(op);
        operationConfigs[op] = { entityField: '', currentEventField: '', scheduleRequestsField: '', outboundEntityField: '', history: [] };
      }
      if (!s.topic) {
        agentNode.data.topiclessSubscriptions.push(subscriptionData(s));
        continue;
      }
      const topicNode = topicByName.get(s.topic)
        || addTopic(s.topic, { type: s.eventType || '', label: s.eventType || '', schema: '', idleness: '', implicit: true });
      addEdge(
        topicNode.id, TOPIC_OUT_HANDLE, agentNodeId,
        op ? opInHandle(op) : HISTORY_IN_HANDLE,
        subscriptionData(s),
      );
    }

    // Outputs
    const outboundByType = new Map();
    list(a.outputs).filter(isObj).forEach((o, oi) => {
      if (o.eventType === 'agent-error') return;
      const node = {
        id: `outbound-${ai + 1}-${oi + 1}`,
        type: 'outbound',
        position: { x: 0, y: 0 },
        data: {
          label: o.label || '',
          topic: o.topic || '',
          eventType: o.eventType || '',
          description: o.description || '',
          schema: o.schema || '',
          sqlEnrichment: isObj(o.sqlEnrichment) ? { ...o.sqlEnrichment } : undefined,
          enrichment: list(o.enrichment),
          filterKey: o.filterKey || '',
          filterValue: o.filterValue || '',
          filterLabel: o.filterLabel || '',
          extra: extraOf(o, OUTPUT_KEYS),
        },
      };
      nodes.push(node);
      if (o.eventType && !outboundByType.has(o.eventType)) outboundByType.set(o.eventType, node);
    });

    // Operation outbounds
    for (const o of operations) {
      if (!o.operation) continue;
      list(o.outbounds).filter(isObj).forEach((ob) => {
        let target = outboundByType.get(ob.eventType);
        if (!target) {
          target = {
            id: `outbound-${ai + 1}-x${nodes.length}`,
            type: 'outbound',
            position: { x: 0, y: 0 },
            data: { label: '', topic: '', eventType: ob.eventType || '', description: '', enrichment: [], implicit: true },
          };
          nodes.push(target);
          if (ob.eventType) outboundByType.set(ob.eventType, target);
        }
        addEdge(agentNodeId, opOutHandle(o.operation), target.id, OUTBOUND_IN_HANDLE, {
          field: ob.field || '',
          eventType: ob.eventType || '',
          extra: extraOf(ob, OP_OUTBOUND_KEYS),
        });
      });
    }
  });

  return { meta, nodes: layoutNodes(nodes, edges), edges };
}

// ─── Layout ─────────────────────────────────────────────────────────────────

const COLUMN_X = { topic: 0, agent: 440, entity: 440, outbound: 900 };
const agentHeight = (n) => 190 + 34 * (n.data?.ruleOperations?.length || 0);

// Approximate rendered geometry, used to estimate where a link meets an agent / node.
const OP_ROW_Y0 = 123;   // centre of the first operation row, from the agent top
const OP_ROW_STEP = 31;
const NODE_H = { topic: 95, outbound: 80 };
const COLUMN_GAP = 24;
const MAX_PERMUTED_AGENTS = 6;

function permutations(items) {
  if (items.length <= 1) return [items];
  return items.flatMap((item, i) => permutations([...items.slice(0, i), ...items.slice(i + 1)])
    .map((rest) => [item, ...rest]));
}

// Number of crossing pairs among straight links drawn between two columns.
// links: [{ rank (position in the movable column), y (anchor on the fixed side) }]
function countCrossings(links) {
  let n = 0;
  for (let i = 0; i < links.length; i++) {
    for (let j = i + 1; j < links.length; j++) {
      if ((links[i].rank - links[j].rank) * (links[i].y - links[j].y) < 0) n++;
    }
  }
  return n;
}

const mean = (ys) => (ys?.length ? ys.reduce((a, b) => a + b, 0) / ys.length : Infinity);

/**
 * Orders one movable column against a fixed one: barycenter sort, then adjacent swaps while
 * they reduce crossings. anchors: Map id -> [y of every link end on the fixed side].
 */
function orderColumn(ids, anchors) {
  const cost = (order) => countCrossings(
    order.flatMap((id, rank) => (anchors.get(id) || []).map((y) => ({ rank, y }))),
  );
  const order = ids
    .map((id, i) => ({ id, i }))
    .sort((a, b) => (mean(anchors.get(a.id)) - mean(anchors.get(b.id))) || a.i - b.i)
    .map((o) => o.id);
  let best = cost(order);
  for (let improved = true; improved && best > 0;) {
    improved = false;
    for (let i = 0; i + 1 < order.length; i++) {
      [order[i], order[i + 1]] = [order[i + 1], order[i]];
      const c = cost(order);
      if (c < best) { best = c; improved = true; } else [order[i], order[i + 1]] = [order[i + 1], order[i]];
    }
  }
  return { order, crossings: best };
}

/**
 * Column layout: topics | agents (each with its entity below) | outbounds.
 * Agent order, and the order of topics / outbounds, are chosen to minimise link crossings;
 * topics and outbounds are then centred on the agent rows they connect to.
 * Returns new node objects; `nodes` is not modified.
 */
export function layoutNodes(nodes, edges = []) {
  const byId = new Map(nodes.map((n) => [n.id, n]));
  const agents = nodes.filter((n) => n.type === 'agent');
  const topics = nodes.filter((n) => n.type === 'topic');
  const outbounds = nodes.filter((n) => n.type === 'outbound');
  const entityOf = (agentId) => edges
    .filter((e) => e.target === agentId)
    .map((e) => byId.get(e.source))
    .filter((n) => n?.type === 'entity');
  const inLinks = edges.filter((e) => byId.get(e.source)?.type === 'topic' && byId.get(e.target)?.type === 'agent');
  const outLinks = edges.filter((e) => byId.get(e.source)?.type === 'agent' && byId.get(e.target)?.type === 'outbound');

  // Vertical stack of agents (with their entities) in a given order.
  const stackAgents = (order) => {
    const agentY = new Map();
    const entityPos = new Map();
    let y = 0;
    for (const agent of order) {
      agentY.set(agent.id, y);
      let h = agentHeight(agent);
      entityOf(agent.id).forEach((en) => {
        if (entityPos.has(en.id)) return;
        entityPos.set(en.id, y + h + 60);
        h += 100;
      });
      y += h + 120;
    }
    return { agentY, entityPos, end: y };
  };

  // y of the link end on an agent, from its handle id ('op-in:Op', 'op-out:Op', history/other)
  const handleY = (agentNode, handle, agentY) => {
    const ops = agentNode.data?.ruleOperations || [];
    const idx = ops.indexOf(String(handle || '').replace(/^op-(in|out):/, ''));
    return agentY.get(agentNode.id) + OP_ROW_Y0 + OP_ROW_STEP * (idx >= 0 ? idx : ops.length);
  };

  const arrange = (agentOrder) => {
    const stack = stackAgents(agentOrder);
    const anchorsOf = (links, ownKey, otherKey, agentKey) => {
      const anchors = new Map();
      for (const e of links) {
        const ys = anchors.get(e[ownKey]) || [];
        ys.push(handleY(byId.get(e[agentKey]), e[otherKey], stack.agentY));
        anchors.set(e[ownKey], ys);
      }
      return anchors;
    };
    const topicAnchors = anchorsOf(inLinks, 'source', 'targetHandle', 'target');
    const outAnchors = anchorsOf(outLinks, 'target', 'sourceHandle', 'source');
    const t = orderColumn(topics.map((n) => n.id), topicAnchors);
    const o = orderColumn(outbounds.map((n) => n.id), outAnchors);
    return { stack, topicAnchors, outAnchors, t, o, crossings: t.crossings + o.crossings };
  };

  let best = arrange(agents);
  if (agents.length > 1 && agents.length <= MAX_PERMUTED_AGENTS) {
    for (const order of permutations(agents).slice(1)) {
      if (best.crossings === 0) break;
      const candidate = arrange(order);
      if (candidate.crossings < best.crossings) best = candidate;
    }
  }

  const pos = new Map();
  best.stack.agentY.forEach((y, id) => pos.set(id, { x: COLUMN_X.agent, y }));
  best.stack.entityPos.forEach((y, id) => pos.set(id, { x: COLUMN_X.entity, y }));
  let y = best.stack.end;
  for (const en of nodes.filter((n) => n.type === 'entity' && !pos.has(n.id))) {
    pos.set(en.id, { x: COLUMN_X.entity, y });
    y += 100;
  }

  // Centre each node on its links, pushing down where nodes would overlap.
  const placeColumn = (type, order, anchors) => {
    const h = NODE_H[type];
    let cursor = null;
    for (const id of order) {
      const c = mean(anchors.get(id));
      const wanted = Number.isFinite(c) ? c - h / 2 : cursor ?? 0;
      const top = cursor === null ? wanted : Math.max(wanted, cursor);
      pos.set(id, { x: COLUMN_X[type], y: top });
      cursor = top + h + COLUMN_GAP;
    }
  };
  placeColumn('topic', best.t.order, best.topicAnchors);
  placeColumn('outbound', best.o.order, best.outAnchors);

  const minY = Math.min(0, ...[...pos.values()].map((p) => p.y));
  return nodes.map((n) => (pos.has(n.id) ? { ...n, position: { x: pos.get(n.id).x, y: pos.get(n.id).y - minY } } : n));
}

// ─── Validation ─────────────────────────────────────────────────────────────

const isScheduleEvent = (eventType) => eventType === 'Schedule';

/**
 * Mirrors the structural checks of the backend (AgentDescriptorValidator and
 * EnrichmentValidator) that can run in the browser, so problems show before saving.
 * Returns a list of messages; empty when the descriptor looks valid.
 */
export function validateDescriptor(descriptor) {
  const problems = [];
  const agents = list(descriptor?.agents);
  if (agents.length === 0) problems.push('The application needs at least one agent.');

  const agentIds = new Set();
  for (const a of agents) {
    const who = `Agent '${a.agentId || a.name || '?'}'`;
    if (!a.agentId) problems.push(`${who}: Agent ID is required.`);
    else if (!/^[A-Za-z0-9_-]+$/.test(a.agentId)) problems.push(`${who}: Agent ID may only contain letters, digits, '-' and '_'.`);
    else if (agentIds.has(a.agentId)) problems.push(`${who}: duplicate Agent ID.`);
    agentIds.add(a.agentId);

    if (!a.ruleset?.path) problems.push(`${who}: the Rule JAR path is required.`);

    const refNames = new Set();
    for (const r of list(a.references)) {
      if (!r.name) problems.push(`${who}: every reference needs a name.`);
      else if (refNames.has(r.name)) problems.push(`${who}: duplicate reference '${r.name}'.`);
      refNames.add(r.name);
      if (r.type && r.type.toLowerCase() !== 'csv') problems.push(`${who}: reference '${r.name}' has unsupported type '${r.type}' (only csv).`);
      if (!r.file) problems.push(`${who}: reference '${r.name}' needs a file.`);
    }

    const checkSteps = (where, steps) => {
      for (const s of list(steps)) {
        for (const k of ['reference', 'key', 'on', 'into']) {
          if (!s[k]) problems.push(`${who}, ${where}: an enrichment needs '${k}'.`);
        }
        if (s.reference && !refNames.has(s.reference)) problems.push(`${who}, ${where}: unknown reference '${s.reference}'.`);
        if (list(s.fields).length === 0) problems.push(`${who}, ${where}: an enrichment lists no fields.`);
      }
    };

    const operations = new Set();
    for (const s of list(a.subscriptions)) {
      const where = `subscription '${s.eventType || '?'}'`;
      if (isScheduleEvent(s.eventType) && s.topic) problems.push(`${who}: the Schedule subscription must not have a topic.`);
      if (s.operation && operations.has(s.operation)) problems.push(`${who}: operation '${s.operation}' is subscribed twice.`);
      if (s.operation) operations.add(s.operation);
      if (!isScheduleEvent(s.eventType) && s.entityKeyExtractor == null && a.entityKeyExtractor == null) {
        problems.push(`${who}, ${where}: an entity key extractor is required (on the subscription or on the agent).`);
      }
      checkSteps(where, s.enrichment);
    }

    const sinks = new Set();
    for (const o of list(a.outputs)) {
      if (o.topic) {
        if (sinks.has(o.eventType || '')) problems.push(`${who}: two outputs share the event type '${o.eventType || ''}'.`);
        sinks.add(o.eventType || '');
      }
      if (list(o.enrichment).length > 0 && !o.eventType) problems.push(`${who}: an output with enrichment needs an event type.`);
      checkSteps(`output '${o.eventType || '?'}'`, o.enrichment);
    }
  }
  return problems;
}
