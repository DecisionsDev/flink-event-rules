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
 * React Flow canvas of the read-only Agent Diagram View: node cards, flow edges, minimap.
 * All transient UI state (focus, search, live stats …) arrives through DiagramContext so the
 * node/edge objects stay stable between hovers.
 */
import React, { createContext, useContext, useEffect, useMemo } from 'react';
import {
  ReactFlow, Background, BackgroundVariant, MiniMap, Handle, Position, BaseEdge, EdgeLabelRenderer,
  getBezierPath, useStore, useUpdateNodeInternals,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import { GEO, PALETTE, TOPIC_KIND_META } from './agentDiagramModel';
import Icon from './AgentDiagramIcons';

export const DiagramContext = createContext({
  focus: null, matches: null, live: {}, animate: true, showLabels: true,
  selectedId: null, selectedEdgeId: null, hoverEdgeId: null, opFocus: null,
  actions: {},
});

const KIND_ICON = { source: 'source', schedule: 'clock', link: 'link', sink: 'sink', alert: 'bell', error: 'warn' };

const isDim = (ctx, id) => !!((ctx.focus && !ctx.focus.nodes.has(id)) || (ctx.matches && !ctx.matches.has(id)));

/** Below this zoom cards switch to a "map" look: only names, larger. */
const LOD_ZOOM = 0.55;
const useLowDetail = () => useStore(s => s.transform[2] < LOD_ZOOM);

function Sparkline({ history }) {
  const deltas = [];
  for (let i = 1; i < history.length; i++) deltas.push(Math.max(0, history[i] - history[i - 1]));
  if (deltas.length < 2) return null;
  const W = 56, H = 16;
  const max = Math.max(1, ...deltas);
  const pts = deltas.map((d, i) => `${(i / (deltas.length - 1)) * W},${H - 1 - (d / max) * (H - 3)}`);
  return (
    <svg className="adv-spark" width={W} height={H} viewBox={`0 0 ${W} ${H}`} aria-hidden="true">
      <polyline points={pts.join(' ')} fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinejoin="round" strokeLinecap="round" />
    </svg>
  );
}

// ─── Topic node ─────────────────────────────────────────────────────────────

function TopicNode({ id, data }) {
  const ctx = useContext(DiagramContext);
  const low = useLowDetail();
  const meta = TOPIC_KIND_META[data.kind];
  const selected = ctx.selectedId === id;
  const types = data.eventTypes;
  const caption = data.label || data.description || '';
  const flow = data.kind === 'source' || data.kind === 'schedule'
    ? `${data.consumers.length} consumer${data.consumers.length === 1 ? '' : 's'}`
    : data.kind === 'link'
      ? `${data.producers.map(p => p.agentName).join(', ')} → ${data.consumers.map(c => c.agentName).join(', ')}`
      : `from ${data.producers.map(p => p.agentName).join(', ')}`;

  return (
    <div
      className={`adv-card adv-topic adv-topic--${data.kind}${selected ? ' adv-card--selected' : ''}${isDim(ctx, id) ? ' adv-card--dim' : ''}${ctx.matches?.has(id) ? ' adv-card--match' : ''}${low ? ' adv-lod' : ''}`}
      style={{ '--k': meta.color }}
      onClick={() => ctx.actions.selectNode?.(id)}
      onMouseEnter={() => ctx.actions.hoverNode?.(id)}
      onMouseLeave={() => ctx.actions.hoverNode?.(null)}
    >
      <Handle type="target" position={Position.Left} id="in" className="adv-handle" />
      <div className="adv-topic-top">
        <span className="adv-tile"><Icon name={KIND_ICON[data.kind]} size={16} /></span>
        <span className="adv-topic-kind">{data.schedule ? 'Scheduler' : meta.label}</span>
        {data.chained && (
          <span className="adv-badge adv-badge--bolt" title="Chained: forwarded in-process between agents, without a Kafka round trip">
            <Icon name="bolt" size={11} /> in-process
          </span>
        )}
      </div>
      <button type="button" className="adv-topic-name" title={data.name} aria-label={`Inspect ${data.schedule ? 'scheduler' : 'topic'} ${data.name}`}>
        {data.name}
      </button>
      {caption && <div className="adv-topic-caption" title={caption}>{caption}</div>}
      <div className="adv-topic-foot">
        {types.slice(0, 2).map(t => <span key={t} className="adv-chip adv-chip--type" title={t}><span className="adv-ellipsis">{t}</span></span>)}
        {types.length > 2 && <span className="adv-chip adv-chip--more">+{types.length - 2}</span>}
        <span className="adv-topic-flow" title={flow}>{flow}</span>
      </div>
      <Handle type="source" position={Position.Right} id="out" className="adv-handle" />
    </div>
  );
}

// ─── Agent node ─────────────────────────────────────────────────────────────

function AgentNode({ id, data }) {
  const ctx = useContext(DiagramContext);
  const low = useLowDetail();
  const updateNodeInternals = useUpdateNodeInternals();
  const opsKey = data.ops.map(o => o.name).join('\u0000');
  // Operation ports appear/disappear with the card's expanded state: re-measure them.
  useEffect(() => { updateNodeInternals(id); }, [id, data.expanded, opsKey, updateNodeInternals]);

  const live = ctx.live[data.agentId];
  const active = !!live && live.history.length > 1 && live.history[live.history.length - 1] > live.history[live.history.length - 2];
  const selected = ctx.selectedId === id;
  const hasOps = data.ops.length > 0;

  return (
    <div
      className={`adv-card adv-agent${selected ? ' adv-card--selected' : ''}${isDim(ctx, id) ? ' adv-card--dim' : ''}${ctx.matches?.has(id) ? ' adv-card--match' : ''}${low ? ' adv-lod' : ''}`}
      onClick={() => ctx.actions.selectNode?.(id)}
      onMouseEnter={() => ctx.actions.hoverNode?.(id)}
      onMouseLeave={() => ctx.actions.hoverNode?.(null)}
    >
      {/* Collapsed ports (header height). Hidden while operation ports are shown. */}
      <Handle type="target" position={Position.Left} id="in" className="adv-handle adv-handle--in" style={{ top: GEO.headH / 2 }} />
      <Handle type="source" position={Position.Right} id="out" className="adv-handle adv-handle--out" style={{ top: GEO.headH / 2 }} />

      <div className="adv-agent-head" style={{ height: GEO.headH }}>
        <span className="adv-tile adv-tile--agent"><Icon name="agent" size={20} /></span>
        <div className="adv-agent-titles">
          <button type="button" className="adv-agent-name" title={data.name} aria-label={`Inspect agent ${data.name}`}>{data.name}</button>
          <div className="adv-agent-id" title={data.agentId}>{data.agentId}</div>
        </div>
        {hasOps && (
          <button
            type="button"
            className={`adv-toggle${data.expanded ? ' adv-toggle--open' : ''}`}
            aria-expanded={data.expanded}
            aria-label={`${data.expanded ? 'Collapse' : 'Expand'} decision operations of ${data.name}`}
            title={data.expanded ? 'Hide decision operations' : 'Show decision operations'}
            onClick={(e) => { e.stopPropagation(); ctx.actions.toggleExpand?.(data.agentId); }}
          >
            <Icon name="chevron" size={16} />
          </button>
        )}
      </div>

      <div className="adv-agent-chips" style={{ height: GEO.chipsH }}>
        {data.entityType && (
          <button
            type="button"
            className="adv-chip adv-chip--entity"
            title={`Inspect ${data.entityType} entity state`}
            onClick={(e) => { e.stopPropagation(); ctx.actions.selectEntity?.(data.agentId); }}
          >
            <Icon name="entity" size={12} /> {data.entityType}
          </button>
        )}
        {data.opTotal > 0 && <span className="adv-chip" title={`${data.opTotal} decision operation(s)`}><Icon name="layers" size={12} /> {data.opTotal} ops</span>}
        {data.stateBackend && <span className="adv-chip" title={`State backend ${data.stateBackend}${data.stateTtl ? `, TTL ${data.stateTtl}` : ''}`}><Icon name="package" size={12} /> {data.stateBackend}{data.stateTtl ? ` · ${data.stateTtl}` : ''}</span>}
        {!data.chaining && <span className="adv-chip adv-chip--warn" title="chainingEnabled=false: always reads from Kafka">no chaining</span>}
        {data.parallelism > 1 && <span className="adv-chip" title="Flink parallelism">×{data.parallelism}</span>}
        {data.jar && <span className="adv-chip adv-chip--jar" title={data.jar}><span className="adv-ellipsis">{data.jar}</span></span>}
      </div>

      <div className="adv-agent-stat" style={{ height: GEO.statH }}>
        <span className="adv-stat-io" title="Subscriptions in / outputs out">
          <b>{data.subCount}</b> in <i>·</i> <b>{data.outCount}</b> out
        </span>
        <span className="adv-stat-live" title={live ? 'Events processed (live)' : 'No live data'}>
          {live ? (
            <>
              <span className={`adv-pulse${active ? ' adv-pulse--on' : ''}`} />
              <b>{live.total.toLocaleString()}</b> events
              <Sparkline history={live.history} />
            </>
          ) : <span className="adv-stat-none">no live data</span>}
        </span>
      </div>

      {data.expanded && hasOps && (
        <div className="adv-ops">
          <div className="adv-ops-head" style={{ height: GEO.opsHeadH }}>Decision operations</div>
          {data.ops.map(op => {
            const focused = ctx.opFocus?.agentId === data.agentId && ctx.opFocus?.name === op.name;
            return (
              <button
                type="button"
                key={op.name}
                data-op={op.name}
                className={`adv-op${focused ? ' adv-op--focus' : ''}`}
                style={{ height: GEO.opH, marginBottom: GEO.opGap }}
                aria-pressed={focused}
                title={`Trace ${op.name}`}
                onClick={(e) => { e.stopPropagation(); ctx.actions.focusOperation?.(data.agentId, op.name); }}
              >
                <span className="adv-op-name">{op.name}</span>
                <span className="adv-op-meta">
                  {op.ins.length > 0 && <span title="Consumes">{op.ins.join(', ')}</span>}
                  {op.ins.length > 0 && op.outs.length > 0 && <i> → </i>}
                  {op.outs.length > 0 && <span title="Emits">{op.outs.join(', ')}</span>}
                  {op.schedule && <span className="adv-op-sched" title="Can schedule future events"><Icon name="clock" size={11} /></span>}
                </span>
              </button>
            );
          })}
          {data.ops.map(op => (
            <React.Fragment key={`h-${op.name}`}>
              <Handle type="target" position={Position.Left} id={`in:${op.name}`} className="adv-handle adv-handle--in" style={{ top: op.handleY }} />
              <Handle type="source" position={Position.Right} id={`out:${op.name}`} className="adv-handle adv-handle--out" style={{ top: op.handleY }} />
            </React.Fragment>
          ))}
        </div>
      )}
    </div>
  );
}

// ─── Flow edge ──────────────────────────────────────────────────────────────

function FlowEdge({ id, source, target, sourceX, sourceY, targetX, targetY, sourcePosition, targetPosition, data }) {
  const ctx = useContext(DiagramContext);
  const [path, labelX, labelY] = getBezierPath({
    sourceX, sourceY, sourcePosition, targetX, targetY, targetPosition, curvature: 0.38,
  });

  const focusActive = !!ctx.focus;
  const inFocus = focusActive && ctx.focus.edges.has(id);
  const searchDim = !!ctx.matches && !(ctx.matches.has(source) || ctx.matches.has(target));
  const dim = (focusActive && !inFocus) || searchDim;
  const hot = ctx.hoverEdgeId === id || ctx.selectedEdgeId === id;

  const live = ctx.live[data.agentId];
  const count = data.direction === 'in' ? (live?.counts?.[data.eventType] ?? 0) : (live?.total ?? 0);
  const traffic = count > 0;
  const width = 2 + (data.direction === 'in' && traffic ? Math.min(2.5, Math.log10(count + 1)) : 0) + (hot ? 1 : 0);

  const gradId = `adv-g-${data.gid}`;
  const markId = `adv-m-${data.gid}`;
  const showLabel = ctx.showLabels || hot || inFocus;
  const animate = ctx.animate && !dim;

  const meta = [data.direction === 'in' && data.operation ? `→ ${data.operation}` : null, data.ops.length && data.direction === 'out' ? `from ${data.ops.join(', ')}` : null, data.horizon ? `history ${data.horizon}` : null, data.direction === 'in' && traffic ? `${count.toLocaleString()} processed` : null].filter(Boolean);

  return (
    <>
      <defs>
        <linearGradient id={gradId} gradientUnits="userSpaceOnUse" x1={sourceX} y1={sourceY} x2={targetX} y2={targetY}>
          <stop offset="0" stopColor={data.srcColor} />
          <stop offset="1" stopColor={data.tgtColor} />
        </linearGradient>
        <marker id={markId} viewBox="0 0 10 10" refX="8" refY="5" markerWidth="9" markerHeight="9" markerUnits="userSpaceOnUse" orient="auto-start-reverse">
          <path d="M1 1.5 L9 5 L1 8.5 z" fill={data.tgtColor} />
        </marker>
      </defs>
      <g className={`adv-edge adv-edge--${data.category}${dim ? ' adv-edge--dim' : ''}${hot || inFocus ? ' adv-edge--hot' : ''}`}>
        <BaseEdge
          id={id}
          path={path}
          interactionWidth={22}
          markerEnd={`url(#${markId})`}
          style={{ stroke: `url(#${gradId})`, strokeWidth: width }}
        />
        {animate && <path d={path} className="adv-edge-flow" fill="none" style={{ strokeWidth: Math.max(1.2, width - 1) }} />}
        {animate && traffic && (
          <circle r={3.2} className="adv-edge-packet" fill={data.tgtColor}>
            <animateMotion dur={`${data.direction === 'in' ? 2.6 : 3.2}s`} repeatCount="indefinite" path={path} />
          </circle>
        )}
      </g>
      {showLabel && !dim && (
        <EdgeLabelRenderer>
          <div
            className={`adv-edge-label adv-edge-label--${data.category}${hot || inFocus ? ' adv-edge-label--hot' : ''}`}
            style={{ transform: `translate(-50%, -50%) translate(${labelX}px, ${labelY}px)`, '--k': data.direction === 'in' ? data.srcColor : data.tgtColor }}
          >
            <span className="adv-edge-label-type">{data.eventType}</span>
            {(hot || inFocus) && meta.length > 0 && <span className="adv-edge-label-meta">{meta.join(' · ')}</span>}
          </div>
        </EdgeLabelRenderer>
      )}
    </>
  );
}

const NODE_TYPES = { topic: TopicNode, agent: AgentNode };
const EDGE_TYPES = { flow: FlowEdge };

const minimapColor = (n) => (n.type === 'agent' ? PALETTE.agent : (TOPIC_KIND_META[n.data?.kind]?.color || '#94a3b8'));

/** Strips model-only fields so React Flow receives plain node/edge objects. */
export function toFlow(graph) {
  return {
    nodes: graph.nodes.map(n => ({
      id: n.id, type: n.type, position: n.position, data: n.data, width: n.width, height: n.height,
      // Sizes are fixed by the model. The nodes are controlled without onNodesChange, so React Flow
      // can never write measurements back: provide them, or `fitView` waits forever.
      measured: { width: n.width, height: n.height },
      // With selectable/draggable off and no node-level handlers React Flow writes an inline
      // `pointer-events: none` on the node wrapper, which the card's own buttons (collapse toggle,
      // entity chip, operation rows) would inherit. Cards handle their own clicks, so opt back in.
      style: { pointerEvents: 'all' },
      draggable: false, selectable: false, focusable: false, connectable: false,
    })),
    edges: graph.edges.map(e => ({ ...e, selectable: false, focusable: false })),
  };
}

export default function AgentDiagramCanvas({
  graph, context, showMiniMap, onEdgeHover, onEdgeClick, onPaneClick,
}) {
  const flow = useMemo(() => toFlow(graph), [graph]);
  return (
    <DiagramContext.Provider value={context}>
      <ReactFlow
        nodes={flow.nodes}
        edges={flow.edges}
        nodeTypes={NODE_TYPES}
        edgeTypes={EDGE_TYPES}
        nodesDraggable={false}
        nodesConnectable={false}
        nodesFocusable={false}
        edgesFocusable={false}
        elementsSelectable={false}
        zoomOnDoubleClick={false}
        fitView
        fitViewOptions={{ padding: 0.14, maxZoom: 1 }}
        minZoom={0.1}
        maxZoom={2.5}
        onEdgeMouseEnter={(_, e) => onEdgeHover(e.id)}
        onEdgeMouseLeave={() => onEdgeHover(null)}
        onEdgeClick={(_, e) => onEdgeClick(e.id)}
        onPaneClick={onPaneClick}
        proOptions={{ hideAttribution: true }}
        aria-label="Agent architecture diagram"
      >
        <Background variant={BackgroundVariant.Dots} gap={22} size={1.2} color="#cbd5e1" />
        {showMiniMap && (
          <MiniMap
            className="adv-minimap"
            pannable
            zoomable
            nodeColor={minimapColor}
            nodeStrokeWidth={0}
            nodeBorderRadius={4}
            maskColor="rgba(15, 23, 42, 0.07)"
            ariaLabel="Diagram overview"
          />
        )}
      </ReactFlow>
    </DiagramContext.Provider>
  );
}
