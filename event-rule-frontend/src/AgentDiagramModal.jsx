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
 * Agent Diagram View — read-only, interactive architecture map of an agents.json.
 *
 *   agentDiagramModel.js   pure graph model + layout (topics ⇄ agents, chaining, lineage, search)
 *   AgentDiagramCanvas.jsx React Flow nodes / edges / minimap
 *   useAgentLiveStats.js   live per-agent event counters from the BFF
 *   this file              modal shell (toolbar, search, legend) + inspector side panel
 */
import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { ReactFlowProvider, useReactFlow, useStore, useNodesInitialized } from '@xyflow/react';
import './AgentDiagramModal.css';
import { API_URL } from './config.js';
import AgentDiagramCanvas from './AgentDiagramCanvas';
import Icon from './AgentDiagramIcons';
import { useAgentLiveStats } from './useAgentLiveStats';
import {
  buildGraph, lineage, edgeFocus, operationFocus, matchNodes, formatHorizon, TOPIC_KIND_META, PALETTE,
} from './agentDiagramModel';

const ENTITY_POLL_MS = 3000;

// ─── Agent Detail view (operational) ─────────────────────────────────────────
/**
 * Shows what the agent has been doing:
 *   - Total inbound events processed per subscription, with horizon config
 *   - The most recent Rule Engine result (the payload extracted from the last outbound envelope)
 */
function AgentDetailView({ selected, agentDescriptor }) {
  // The rule engine result is the payload inside the outbound envelope.
  // lastPayload shape from BFF: { agentEventType, entityId, sourceTopic, timestamp, payload: {...} }
  const ruleResult = selected?.lastPayload?.payload ?? null;

  // Build a map of subscriptionEventType → horizonConfig from the agent descriptor
  const horizonByType = {};
  for (const sub of agentDescriptor?.subscriptions ?? []) {
    if (sub.eventType) horizonByType[sub.eventType] = sub.horizon ?? null;
  }

  const hasCounts = selected?.inboundCounts
    && Object.keys(selected.inboundCounts).length > 0;

  return (
    <div className="esp-detail">
      {/* Events processed section */}
      <div className="esp-section">
        <div className="esp-section-title">Events processed</div>
        {hasCounts ? (
          Object.entries(selected.inboundCounts).map(([type, count]) => {
            const horizon = horizonByType[type];
            const horizonStr = formatHorizon(horizon);
            return (
              <div key={type} className="esp-agent-row">
                <div className="esp-agent-row-top">
                  <span className="esp-agent-event-type">{type}</span>
                  <span className="esp-agent-count">{count}</span>
                </div>
                {horizonStr && (
                  <div className="esp-agent-horizon">
                    <span className="esp-horizon-icon">⧖</span> {horizonStr}
                  </div>
                )}
              </div>
            );
          })
        ) : (
          <div className="esp-no-data">No events processed yet.</div>
        )}
      </div>

      {/* Last rule engine result */}
      <div className="esp-section">
        <div className="esp-section-title">Last rule engine result</div>
        {ruleResult ? (
          <pre className="esp-json">
            {JSON.stringify(ruleResult, null, 2)}
          </pre>
        ) : (
          <div className="esp-no-data">No rule engine result recorded yet.</div>
        )}
      </div>
    </div>
  );
}

// ─── Entity Detail view ────────────────────────────────────────────────────────
/**
 * Shows the list of all entity instances for the selected agent, lets the user
 * pick one, and displays its live entityData snapshot from the changelog store.
 *
 * Data source: GET /api/agents/:useCaseId/entity-state?agentId=<agentId>
 *   → { agentId, entityType, entities: [{ entityId, entityData, timestamp }, …] }
 *
 * Polls every POLL_MS to stay current without requiring a page reload.
 */
function EntityDetailView({ useCaseId, agentId, entityType }) {
  const [entities,        setEntities]        = useState([]);
  const [selectedId,      setSelectedId]      = useState(null);
  const [loading,         setLoading]         = useState(true);
  const [error,           setError]           = useState(null);

  // Fetch (or re-fetch) the entity list for this agent
  const fetchEntities = useCallback(() => {
    if (!useCaseId || !agentId) return;
    fetch(`${API_URL}/api/agents/${useCaseId}/entity-state?agentId=${encodeURIComponent(agentId)}`)
      .then(r => {
        if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
        return r.json();
      })
      .then(json => {
        const list = json.entities || [];
        setEntities(list);
        setLoading(false);
        setError(null);
        // Auto-select the first entity when none chosen yet, or keep the
        // current selection if that entityId still exists in the new list.
        setSelectedId(prev => {
          if (prev && list.some(e => e.entityId === prev)) return prev;
          return list[0]?.entityId ?? null;
        });
      })
      .catch(err => {
        setError(err.message);
        setLoading(false);
      });
  }, [useCaseId, agentId]);

  // Initial fetch + reset selection whenever the clicked agent changes
  useEffect(() => {
    setLoading(true);
    setError(null);
    setEntities([]);
    setSelectedId(null);
    fetchEntities();
  }, [fetchEntities]);

  // Poll for updates
  useEffect(() => {
    const id = setInterval(fetchEntities, ENTITY_POLL_MS);
    return () => clearInterval(id);
  }, [fetchEntities]);

  const selectedEntity = entities.find(e => e.entityId === selectedId) ?? null;
  const entityData     = selectedEntity?.entityData ?? null;

  if (loading) return <div className="esp-loading">Loading…</div>;
  if (error)   return <div className="esp-error">Could not load: {error}</div>;

  if (entities.length === 0) {
    return (
      <div className="esp-detail">
        <div className="esp-entity-no-payload">
          <div className="esp-empty-icon">{ '{  }' }</div>
          <div className="esp-empty-text">No {entityType || 'entity'} instances observed yet.</div>
          <div className="esp-empty-hint">
            State will appear here after the rule engine processes the first event.
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="esp-detail">
      {/* ── Entity picker ── */}
      <div className="esp-section esp-entity-picker-section">
        <div className="esp-section-title">{entityType || 'Entity'} instances ({entities.length})</div>
        <div className="esp-entity-picker">
          {entities.map(e => (
            <button
              key={e.entityId}
              className={`esp-entity-chip${e.entityId === selectedId ? ' esp-entity-chip--active' : ''}`}
              onClick={() => setSelectedId(e.entityId)}
              title={e.entityId}
            >
              {e.entityId}
            </button>
          ))}
        </div>
      </div>

      {/* ── Entity data ── */}
      <div className="esp-section esp-section--fill">
        {selectedId && <div className="esp-section-title">{selectedId}</div>}
        {entityData ? (
          <>
            <div className="esp-changelog-badge">Live entity state</div>
            <pre className="esp-json esp-json--fill">
              {JSON.stringify(entityData, null, 2)}
            </pre>
          </>
        ) : (
          <div className="esp-entity-no-payload">
            <div className="esp-empty-icon">{ '{  }' }</div>
            <div className="esp-empty-text">No state snapshot yet.</div>
            <div className="esp-empty-hint">
              Changelog data will appear here after the entity is modified by the rule engine.
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

// ─── Selection Detail Panel ────────────────────────────────────────────────────
/**
 * Slide-in side panel that shows details for the last clicked diagram node.
 *
 * • When an entity node is clicked → renders EntityDetailView, which fetches all
 *   entity instances for that agent from the changelog store and lets the user pick one.
 * • When an agent box is clicked   → fetches entity-state and shows the
 *   operational summary via AgentDetailView.
 *
 * `selection` shape: { kind: 'entity'|'agent', agent, agentId, entityType? }
 */
function SelectionDetailPanel({ useCaseId, selection, onClose }) {
  const [data, setData]       = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError]     = useState(null);

  const { kind, agent: agentDescriptor, agentId, entityType } = selection;

  // Agent-click path: fetch (and re-fetch) the event-store snapshot for this agent.
  const fetchAgentState = useCallback(() => {
    if (kind !== 'agent') return;
    if (!useCaseId || !agentDescriptor?.agentId) return;
    fetch(`${API_URL}/api/agents/${useCaseId}/entity-state?agentId=${encodeURIComponent(agentDescriptor.agentId)}&summary=true`)
      .then(r => {
        if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
        return r.json();
      })
      .then(json => {
        setData(json);
        setLoading(false);
        setError(null);
      })
      .catch(err => {
        setError(err.message);
        setLoading(false);
      });
  }, [useCaseId, kind, agentDescriptor?.agentId]);

  // Initial fetch + reset whenever the selected agent changes
  useEffect(() => {
    if (kind !== 'agent') return;
    setLoading(true);
    setError(null);
    setData(null);
    fetchAgentState();
  }, [fetchAgentState, kind]);

  // Poll so the agent summary stays current as new events arrive
  useEffect(() => {
    if (kind !== 'agent') return;
    const id = setInterval(fetchAgentState, ENTITY_POLL_MS);
    return () => clearInterval(id);
  }, [fetchAgentState, kind]);

  // For an agent click, use the aggregated agent-level entry (inboundCounts + lastPayload).
  const agentSelected = (() => {
    if (!data) return null;
    return data.agentSummary ?? data.entities?.[0] ?? null;
  })();

  // Header badge label
  const badgeLabel = kind === 'agent'
    ? (agentDescriptor?.name || agentDescriptor?.agentId || 'Agent')
    : (entityType || 'Entity');

  // Header sub-label (agent id when showing an agent)
  const subLabel = kind === 'agent' && agentDescriptor?.agentId
    ? agentDescriptor.agentId
    : null;

  return (
    <div className="esp-panel" role="complementary" aria-label="Selection detail">
      {/* ── Header ── */}
      <div className="esp-header">
        <span className="esp-title">
          <span className={`esp-type-badge esp-type-badge--${kind}`}>{badgeLabel}</span>
          {subLabel && <span className="esp-sub-label">{subLabel}</span>}
        </span>
        <button className="esp-close" onClick={onClose} aria-label="Close panel">×</button>
      </div>

      {/* ── Entity kind: self-contained fetch + picker inside EntityDetailView ── */}
      {kind === 'entity' && (
        <EntityDetailView
          useCaseId={useCaseId}
          agentId={agentId}
          entityType={entityType}
        />
      )}

      {/* ── Agent kind: event-store snapshot ── */}
      {kind === 'agent' && loading && <div className="esp-loading">Loading…</div>}
      {kind === 'agent' && error   && <div className="esp-error">Could not load: {error}</div>}
      {kind === 'agent' && !loading && !error && !agentSelected && (
        <div className="esp-empty">
          <div className="esp-empty-icon">⬡</div>
          <div className="esp-empty-text">No data observed yet.</div>
          <div className="esp-empty-hint">
            Send events to this use case and data will appear here.
          </div>
        </div>
      )}
      {kind === 'agent' && !loading && !error && agentSelected && (
        <AgentDetailView selected={agentSelected} agentDescriptor={agentDescriptor} />
      )}
    </div>
  );
}


// ─── Topic / link inspectors (config-only: no backend needed) ─────────────────

function TopicPanel({ node, onClose, onPickAgent }) {
  const d = node.data;
  const meta = TOPIC_KIND_META[d.kind];
  return (
    <div className="esp-panel" role="complementary" aria-label="Selection detail">
      <div className="esp-header">
        <span className="esp-title">
          <span className="esp-type-badge adv-kind-badge" style={{ '--k': meta.color }}>{d.schedule ? 'Scheduler' : meta.label}</span>
          <span className="esp-sub-label" title={d.name}>{d.name}</span>
        </span>
        <button className="esp-close" onClick={onClose} aria-label="Close panel">×</button>
      </div>
      <div className="esp-detail adv-scroll">
        {(d.label || d.description) && (
          <div className="esp-section">
            <div className="esp-section-title">About</div>
            <div className="adv-prose">{d.label || d.description}</div>
            {d.label && d.description && d.description !== d.label && <div className="adv-prose adv-prose--muted">{d.description}</div>}
          </div>
        )}
        <div className="esp-section">
          <div className="esp-section-title">Event types</div>
          <div className="adv-chip-row">
            {d.eventTypes.length ? d.eventTypes.map(t => <span key={t} className="adv-chip adv-chip--type">{t}</span>) : <span className="esp-no-data">None declared.</span>}
          </div>
          {d.chained && (
            <div className="adv-note"><Icon name="bolt" size={13} /> Chained — events flow in-process between the agents; no Kafka round trip.</div>
          )}
        </div>
        <div className="esp-section">
          <div className="esp-section-title">Produced by ({d.producers.length})</div>
          {d.producers.length === 0 && <div className="esp-no-data">External publishers (Kafka producers, scenarios, scripts).</div>}
          {d.producers.map((p, i) => (
            <button key={`${p.agentId}-${i}`} type="button" className="adv-rel" onClick={() => onPickAgent(p.agentId)}>
              <Icon name="agent" size={14} /> <span className="adv-rel-main">{p.agentName}</span>
              <span className="adv-chip adv-chip--type">{p.eventType}</span>
            </button>
          ))}
        </div>
        <div className="esp-section">
          <div className="esp-section-title">Consumed by ({d.consumers.length})</div>
          {d.consumers.length === 0 && <div className="esp-no-data">Terminal output — nothing in this application consumes it.</div>}
          {d.consumers.map((c, i) => (
            <button key={`${c.agentId}-${i}`} type="button" className="adv-rel" onClick={() => onPickAgent(c.agentId)}>
              <Icon name="agent" size={14} />
              <span className="adv-rel-main">{c.agentName}
                {c.operation && <span className="adv-rel-sub">→ {c.operation}</span>}
              </span>
              {c.horizon && <span className="adv-chip" title="History retained"><Icon name="hourglass" size={11} /> {c.horizon}</span>}
            </button>
          ))}
        </div>
      </div>
    </div>
  );
}

function LinkPanel({ edge, graph, live, onClose, onPickNode }) {
  const d = edge.data;
  const src = graph.nodes.find(n => n.id === edge.source);
  const tgt = graph.nodes.find(n => n.id === edge.target);
  const nameOf = n => (n.type === 'agent' ? n.data.name : n.data.name);
  const count = d.direction === 'in' ? live?.[d.agentId]?.counts?.[d.eventType] : undefined;
  return (
    <div className="esp-panel" role="complementary" aria-label="Selection detail">
      <div className="esp-header">
        <span className="esp-title">
          <span className="esp-type-badge adv-kind-badge" style={{ '--k': d.direction === 'in' ? d.srcColor : d.tgtColor }}>
            {d.direction === 'in' ? 'Subscription' : 'Output'}
          </span>
          <span className="esp-sub-label" title={d.eventType}>{d.eventType}</span>
        </span>
        <button className="esp-close" onClick={onClose} aria-label="Close panel">×</button>
      </div>
      <div className="esp-detail adv-scroll">
        <div className="esp-section">
          <div className="esp-section-title">Route</div>
          <button type="button" className="adv-rel" onClick={() => onPickNode(src.id)}>
            <Icon name={src.type === 'agent' ? 'agent' : 'stream'} size={14} /> <span className="adv-rel-main">{nameOf(src)}</span>
          </button>
          <div className="adv-route-arrow" aria-hidden="true">↓</div>
          <button type="button" className="adv-rel" onClick={() => onPickNode(tgt.id)}>
            <Icon name={tgt.type === 'agent' ? 'agent' : 'stream'} size={14} /> <span className="adv-rel-main">{nameOf(tgt)}</span>
          </button>
        </div>
        <div className="esp-section">
          <div className="esp-section-title">Details</div>
          <dl className="adv-dl">
            <dt>Event type</dt><dd>{d.eventType}</dd>
            {d.operation && <><dt>Invokes operation</dt><dd>{d.operation}</dd></>}
            {d.direction === 'out' && d.ops.length > 0 && <><dt>Emitted by operation</dt><dd>{d.ops.join(', ')}</dd></>}
            {d.horizon && <><dt>History retained</dt><dd>{d.horizon}</dd></>}
            <dt>Transport</dt><dd>{d.chained ? 'In-process (agent chaining)' : 'Kafka'}</dd>
            {count !== undefined && <><dt>Processed (live)</dt><dd>{Number(count).toLocaleString()}</dd></>}
          </dl>
        </div>
      </div>
    </div>
  );
}

// ─── Modal shell ──────────────────────────────────────────────────────────────

const selectionKey = (s) => {
  if (!s) return null;
  if (s.kind === 'agent' || s.kind === 'entity') return `${s.kind}:${s.agent?.agentId}`;
  return `${s.kind}:${s.nodeId || s.edgeId}`;
};

function ToolButton({ icon, label, pressed, onClick, disabled, title, children }) {
  return (
    <button
      type="button"
      className={`adv-tool${pressed ? ' adv-tool--on' : ''}`}
      onClick={onClick}
      disabled={disabled}
      aria-pressed={pressed}
      aria-label={label}
      title={title || label}
    >
      <Icon name={icon} size={15} />
      {children && <span className="adv-tool-text">{children}</span>}
    </button>
  );
}

function LegendItem({ color, label, dashed }) {
  return (
    <span className="adv-legend-item">
      <span className={`adv-legend-swatch${dashed ? ' adv-legend-swatch--dashed' : ''}`} style={{ '--k': color }} />
      {label}
    </span>
  );
}

function DiagramShell({ agentsJson, useCaseId, onClose }) {
  const agents = useMemo(() => agentsJson?.agents ?? [], [agentsJson]);
  const { zoomIn, zoomOut, fitView } = useReactFlow();
  const zoomPct = useStore(s => Math.round(s.transform[2] * 100));

  const [expanded, setExpanded]     = useState(() => new Set());
  const [showAlerts, setShowAlerts] = useState(true);
  const [showErrors, setShowErrors] = useState(true);
  const [showLabels, setShowLabels] = useState(true);
  const [animate, setAnimate]       = useState(true);
  const [showMap, setShowMap]       = useState(true);
  const [query, setQuery]           = useState('');
  const [selection, setSelection]   = useState(null);
  const [hoverId, setHoverId]       = useState(null);
  const [hoverEdgeId, setHoverEdgeId] = useState(null);
  const [opFocus, setOpFocus]       = useState(null);
  const searchRef = useRef(null);

  const hidden = useMemo(() => {
    const h = new Set();
    if (!showAlerts) h.add('alert');
    if (!showErrors) h.add('error');
    return h;
  }, [showAlerts, showErrors]);

  const graph = useMemo(() => buildGraph(agentsJson, { expanded, hidden }), [agentsJson, expanded, hidden]);
  const live = useAgentLiveStats(useCaseId, useMemo(() => agents.map(a => a.agentId), [agents]));
  const matches = useMemo(() => matchNodes(graph, query), [graph, query]);

  // Frame the whole diagram once nodes are initialised and the pane has a real size.
  const nodesReady = useNodesInitialized();
  const paneReady = useStore(s => s.width > 0 && s.height > 0);
  const framed = useRef(false);
  useEffect(() => {
    if (!nodesReady || !paneReady || framed.current) return;
    framed.current = true;
    fitView({ padding: 0.14, maxZoom: 1 });
  }, [nodesReady, paneReady, fitView]);

  // Agents whose card can be expanded (have decision operations) — the single source of truth
  // shared by the card toggles and the toolbar "Operations" button.
  const expandableIds = useMemo(
    () => graph.nodes.filter(n => n.type === 'agent' && n.data.ops.length > 0).map(n => n.data.agentId),
    [graph],
  );
  const allExpanded = expandableIds.length > 0 && expandableIds.every(id => expanded.has(id));

  // Re-frame the diagram when whole layers are toggled (not on every card expand).
  const reframeKey = `${showAlerts}|${showErrors}|${allExpanded}`;
  const firstFrame = useRef(true);
  useEffect(() => {
    if (firstFrame.current) { firstFrame.current = false; return undefined; }
    const t = setTimeout(() => fitView({ padding: 0.14, maxZoom: 1, duration: 450 }), 60);
    return () => clearTimeout(t);
  }, [reframeKey, fitView]);

  // Search: glide to the matching nodes.
  useEffect(() => {
    if (!matches || matches.size === 0) return undefined;
    const t = setTimeout(() => fitView({ nodes: [...matches].map(id => ({ id })), padding: 0.5, maxZoom: 1, duration: 450 }), 180);
    return () => clearTimeout(t);
  }, [matches, fitView]);

  const selectedNodeId = !selection ? null
    : selection.kind === 'topic' ? selection.nodeId
    : (selection.kind === 'agent' || selection.kind === 'entity') ? `agent:${selection.agent.agentId}`
    : null;

  // What is lit up: op trace > hover > selected node > selected link.
  const focus = useMemo(() => {
    if (opFocus) return operationFocus(graph, opFocus.agentId, opFocus.name);
    const nodeId = hoverId || selectedNodeId;
    if (nodeId && graph.nodes.some(n => n.id === nodeId)) return lineage(graph, nodeId);
    if (selection?.kind === 'edge') {
      const e = graph.edges.find(x => x.id === selection.edgeId);
      if (e) return edgeFocus(e);
    }
    return null;
  }, [graph, opFocus, hoverId, selectedNodeId, selection]);

  const toggleSelection = useCallback((next) => {
    setSelection(p => (selectionKey(p) === selectionKey(next) ? null : next));
  }, []);

  const actions = useMemo(() => ({
    hoverNode: setHoverId,
    selectNode: (id) => {
      const n = graph.nodes.find(x => x.id === id);
      if (!n) return;
      setOpFocus(null);
      toggleSelection(n.type === 'agent'
        ? { kind: 'agent', agent: n.data.agent, agentId: n.data.agentId }
        : { kind: 'topic', nodeId: id });
    },
    selectEntity: (agentId) => {
      const n = graph.nodes.find(x => x.id === `agent:${agentId}`);
      if (!n) return;
      setOpFocus(null);
      toggleSelection({ kind: 'entity', agent: n.data.agent, agentId, entityType: n.data.entityType });
    },
    toggleExpand: (agentId) => {
      setOpFocus(null);
      setExpanded(prev => {
        const next = new Set(prev);
        if (next.has(agentId)) next.delete(agentId); else next.add(agentId);
        return next;
      });
    },
    focusOperation: (agentId, name) => {
      setOpFocus(p => (p && p.agentId === agentId && p.name === name ? null : { agentId, name }));
    },
  }), [graph, toggleSelection]);

  const context = useMemo(() => ({
    focus, matches, live, animate, showLabels,
    selectedId: selectedNodeId, selectedEdgeId: selection?.kind === 'edge' ? selection.edgeId : null,
    hoverEdgeId, opFocus, actions,
  }), [focus, matches, live, animate, showLabels, selectedNodeId, selection, hoverEdgeId, opFocus, actions]);

  const toggleAll = () => {
    setOpFocus(null);
    setExpanded(allExpanded ? new Set() : new Set(expandableIds));
  };

  const pickAgent = useCallback((agentId) => actions.selectNode(`agent:${agentId}`), [actions]);
  const pickNode = useCallback((id) => {
    const n = graph.nodes.find(x => x.id === id);
    if (n) { setSelection(n.type === 'agent' ? { kind: 'agent', agent: n.data.agent, agentId: n.data.agentId } : { kind: 'topic', nodeId: id }); }
  }, [graph]);

  // Keyboard: Esc peels back one layer; + − 0 zoom; f fit; / search.
  useEffect(() => {
    const handler = (e) => {
      const typing = /^(INPUT|TEXTAREA|SELECT)$/.test(e.target?.tagName || '') || e.target?.isContentEditable;
      if (e.key === 'Escape') {
        if (opFocus) { setOpFocus(null); return; }
        if (selection) { setSelection(null); return; }
        onClose();
        return;
      }
      if (typing || e.metaKey || e.ctrlKey || e.altKey) return;
      if (e.key === '=' || e.key === '+') { zoomIn({ duration: 200 }); return; }
      if (e.key === '-' || e.key === '_') { zoomOut({ duration: 200 }); return; }
      if (e.key === '0' || e.key === 'f' || e.key === 'F') { fitView({ padding: 0.14, maxZoom: 1, duration: 350 }); return; }
      if (e.key === '/') { e.preventDefault(); searchRef.current?.focus(); }
    };
    document.addEventListener('keydown', handler);
    return () => document.removeEventListener('keydown', handler);
  }, [onClose, zoomIn, zoomOut, fitView, selection, opFocus]);

  const clearAll = () => { setSelection(null); setOpFocus(null); };
  const selectedEdge = selection?.kind === 'edge' ? graph.edges.find(e => e.id === selection.edgeId) : null;
  const selectedTopic = selection?.kind === 'topic' ? graph.nodes.find(n => n.id === selection.nodeId) : null;
  const panelOpen = (selection?.kind === 'agent' || selection?.kind === 'entity') ? !!useCaseId : !!(selectedEdge || selectedTopic);
  const noMatch = matches && matches.size === 0;
  const stats = graph.stats;

  return (
    <div className="agent-diagram-overlay" onClick={onClose}>
      <div
        className="agent-diagram-modal adv-modal"
        onClick={(e) => e.stopPropagation()}
        role="dialog"
        aria-modal="true"
        aria-label="Agent Diagram View"
      >
        <div className="agent-diagram-header adv-header">
          <span className="adv-title-icon"><Icon name="map" size={20} /></span>
          <div className="adv-title-block">
            <span className="agent-diagram-title">Agent Diagram View</span>
            <span className="adv-subtitle">
              {stats.agents} agent{stats.agents === 1 ? '' : 's'} · {stats.topics} topic{stats.topics === 1 ? '' : 's'} · {stats.links} link{stats.links === 1 ? '' : 's'}
              {stats.chains > 0 && <> · <span className="adv-subtitle-chain"><Icon name="bolt" size={11} /> {stats.chains} chained</span></>}
              {useCaseId && Object.keys(live).length > 0 && <> · <span className="adv-subtitle-live"><span className="adv-pulse adv-pulse--on" /> live</span></>}
            </span>
          </div>

          <div className="adv-search">
            <Icon name="search" size={15} />
            <input
              ref={searchRef}
              type="search"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              onKeyDown={(e) => { if (e.key === 'Escape' && query) { e.stopPropagation(); setQuery(''); } }}
              placeholder="Find agent, topic, event type, operation…  ( / )"
              aria-label="Search the diagram"
              spellCheck={false}
            />
            {matches && <span className={`adv-search-count${noMatch ? ' adv-search-count--none' : ''}`} aria-live="polite">{matches.size} match{matches.size === 1 ? '' : 'es'}</span>}
          </div>

          <div className="adv-toolgroup" role="group" aria-label="View options">
            <ToolButton icon={allExpanded ? 'fold' : 'unfold'} label={allExpanded ? 'Collapse all agents' : 'Expand all agents'} onClick={toggleAll} pressed={allExpanded}>Operations</ToolButton>
            <ToolButton icon="bell" label="Show alert outputs" pressed={showAlerts} onClick={() => setShowAlerts(v => !v)}>Alerts</ToolButton>
            <ToolButton icon="warn" label="Show error outputs" pressed={showErrors} onClick={() => setShowErrors(v => !v)}>Errors</ToolButton>
            <ToolButton icon="list" label="Show event labels on links" pressed={showLabels} onClick={() => setShowLabels(v => !v)} />
            <ToolButton icon="pulse" label="Animate event flow" pressed={animate} onClick={() => setAnimate(v => !v)} />
            <ToolButton icon="map" label="Show minimap" pressed={showMap} onClick={() => setShowMap(v => !v)} />
          </div>

          <div className="agent-diagram-zoom" role="group" aria-label="Zoom">
            <button className="adz-btn" onClick={() => zoomOut({ duration: 200 })} aria-label="Zoom out" title="Zoom out (−)"><Icon name="minus" size={14} /></button>
            <button className="adz-pct" onClick={() => fitView({ padding: 0.14, maxZoom: 1, duration: 350 })} title="Fit to screen (0 / F)" aria-label={`Zoom ${zoomPct}% — click to fit`}>{zoomPct}%</button>
            <button className="adz-btn" onClick={() => zoomIn({ duration: 200 })} aria-label="Zoom in" title="Zoom in (+)"><Icon name="plus" size={14} /></button>
            <button className="adz-btn" onClick={() => fitView({ padding: 0.14, maxZoom: 1, duration: 350 })} aria-label="Fit to screen" title="Fit to screen (F)"><Icon name="fit" size={14} /></button>
          </div>

          <button className="agent-diagram-close" onClick={onClose} aria-label="Close diagram">×</button>
        </div>

        <div className="agent-diagram-body adv-body">
          {agents.length === 0 ? (
            <p className="agent-diagram-empty">No agent data available.</p>
          ) : (
            <div className="adv-canvas">
              <AgentDiagramCanvas
                graph={graph}
                context={context}
                showMiniMap={showMap}
                onEdgeHover={setHoverEdgeId}
                onEdgeClick={(edgeId) => { setOpFocus(null); toggleSelection({ kind: 'edge', edgeId }); }}
                onPaneClick={clearAll}
              />

              {noMatch && <div className="adv-nomatch" role="status">Nothing matches “{query}”.</div>}

              <div className="adv-legend" aria-label="Legend">
                <LegendItem color={PALETTE.source} label="Event source" />
                <LegendItem color={PALETTE.agent} label="Agent" />
                <LegendItem color={PALETTE.link} label="Chained topic" />
                <LegendItem color={PALETTE.sink} label="Output" />
                <LegendItem color={PALETTE.alert} label="Alert" />
                <LegendItem color={PALETTE.error} label="Error" />
                <span className="adv-legend-hint">Hover to trace lineage · click to inspect · Esc to step back</span>
              </div>
            </div>
          )}

          {panelOpen && selection.kind === 'topic' && (
            <TopicPanel node={selectedTopic} onClose={() => setSelection(null)} onPickAgent={pickAgent} />
          )}
          {panelOpen && selection.kind === 'edge' && (
            <LinkPanel edge={selectedEdge} graph={graph} live={live} onClose={() => setSelection(null)} onPickNode={pickNode} />
          )}
          {panelOpen && (selection.kind === 'agent' || selection.kind === 'entity') && (
            <SelectionDetailPanel
              useCaseId={useCaseId}
              selection={selection}
              onClose={() => setSelection(null)}
            />
          )}
        </div>
      </div>
    </div>
  );
}

export default function AgentDiagramModal(props) {
  return (
    <ReactFlowProvider>
      <DiagramShell {...props} />
    </ReactFlowProvider>
  );
}
