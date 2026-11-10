/*
 * Agent Diagram Editor
 *
 * Full-screen graphical editor for agents.json files.
 * Uses @xyflow/react (React Flow) for the interactive canvas.
 *
 * Layout:
 *   [Header]
 *   [Toolbar  — inside ReactFlowProvider so it can call useReactFlow()]
 *   [Palette | Canvas | Splitter | Detail Panel]
 */
import React, { createContext, useCallback, useContext, useEffect, useId, useMemo, useRef, useState } from 'react';
import { API_URL } from './config.js';
import { exploreDecisionServiceProject, buildVocabOptions } from './decisionServiceProject';
import { useDiagramHistory, deepClone } from './useDiagramHistory';
import {
  buildDiagramDescriptor,
  descriptorToDiagram,
  layoutNodes,
  validateDescriptor,
  opInHandle,
  opOutHandle,
  HISTORY_IN_HANDLE,
} from './agentDiagramDescriptor';
import {
  ReactFlow,
  ReactFlowProvider,
  addEdge,
  useNodesState,
  useEdgesState,
  useReactFlow,
  useStore,
  useUpdateNodeInternals,
  useNodesInitialized,
  BaseEdge,
  EdgeLabelRenderer,
  getSmoothStepPath,
  Handle,
  Position,
  Background,
  BackgroundVariant,
  Controls,
  MarkerType,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import {
  Field,
  TextField,
  CommitInput,
  ReferencesEditor,
  EnrichmentEditor,
  SqlEnrichmentFields,
  ErrorsEditor,
  StateConfigEditor,
} from './AgentDiagramFields';
import './AgentDiagramEditor.css';

// ─── Node type metadata (used by palette + detail panel header) ─────────────
const NODE_TYPES_META = {
  topic:        { label: 'Topic',         icon: '↓', abbr: 'T' },
  agent:        { label: 'Agent',         icon: '⚙', abbr: 'A' },
  entity:       { label: 'Entity',        icon: '□', abbr: 'E' },
  outbound:     { label: 'Outbound',      icon: '↑', abbr: 'O' },
};

let _idCounter = 1;
function nextId(prefix) {
  return `${prefix}-${Date.now()}-${_idCounter++}`;
}

/** Agent ID derived from an Agent name: letters, digits, '-' and '_' only (e.g. "Order Agent" → "order-agent"). */
function slugifyAgentId(name) {
  return (name || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_-]+/g, '-')
    .replace(/-{2,}/g, '-')
    .replace(/^-+|-+$/g, '');
}

/** Default decision archive control topic of an agent: agent-<agentId>-archive-control. */
function archiveControlTopicFor(agentId) {
  return agentId ? `agent-${agentId}-archive-control` : '';
}

/** Slug of `name` that does not collide with the agentId of any existing agent node. */
function uniqueAgentId(name, nodes) {
  const taken = new Set((nodes || []).filter(n => n.type === 'agent').map(n => n.data?.agentId));
  const base = slugifyAgentId(name) || 'agent';
  let candidate = base;
  for (let i = 2; taken.has(candidate); i++) candidate = `${base}-${i}`;
  return candidate;
}

/** Initial data of a new Agent node. */
function newAgentData(agentId, label = 'New Agent') {
  return {
    label, agentId, description: '', ruleJar: '', ruleProject: '',
    archiveControlTopic: archiveControlTopicFor(agentId),
    ruleOperations: [], operationConfigs: {}, references: [], topiclessSubscriptions: [],
    errorOutput: { eventType: 'agent-error', topic: '', label: '', description: '' },
    stateConfig: {},
  };
}

// ─── Empty canvas hint + Decision Service project drop zone ────────────────
function EmptyCanvasHint({ onProjectFile }) {
  const inputRef = useRef(null);
  const [over, setOver] = useState(false);
  const isZip = (file) => file.name.toLowerCase().endsWith('.zip');

  const onDragOver = (e) => {
    const hasFile = [...(e.dataTransfer.items || [])].some(f => f.kind === 'file');
    if (!hasFile) return; // a palette drag: let the canvas handle it
    e.preventDefault();
    e.stopPropagation();
    setOver(true);
  };
  const onDrop = (e) => {
    const file = [...(e.dataTransfer.files || [])].find(isZip);
    if (!file) return;
    e.preventDefault();
    e.stopPropagation();
    setOver(false);
    onProjectFile(file, e);
  };

  return (
    <div className="ade-empty-hint">
      <div className="ade-empty-title">This diagram is empty</div>
      <p className="ade-empty-text">
        Drag an <strong>Agent</strong> from the palette onto the canvas to create a new agent.
      </p>
      <p className="ade-empty-text">
        Or drop a <strong>Decision Service project</strong> (.zip) below to create an agent already linked to it.
      </p>
      <input
        ref={inputRef}
        type="file"
        accept=".zip"
        aria-label="Decision Service project zip"
        style={{ display: 'none' }}
        onChange={e => {
          const file = e.target.files?.[0];
          if (file && isZip(file)) onProjectFile(file);
          e.target.value = '';
        }}
      />
      <div
        className={`ade-empty-dropzone${over ? ' ade-empty-dropzone--over' : ''}`}
        onDragOver={onDragOver}
        onDragLeave={() => setOver(false)}
        onDrop={onDrop}
        onClick={() => inputRef.current?.click()}
        role="button"
        tabIndex={0}
        onKeyDown={e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); inputRef.current?.click(); } }}
      >
        {over ? 'Drop to create the agent' : 'Drop a Decision Service project (.zip) here, or click to browse'}
      </div>
    </div>
  );
}

// ─── Custom Node Components ─────────────────────────────────────────────────

function TopicNode({ data, selected }) {
  return (
    <div className={`ade-node ade-node--topic${selected ? ' ade-node--selected' : ''}`}>
      <div className="ade-node-header">
        <span className="ade-node-type-badge">Topic</span>
      </div>
      <div className="ade-node-body">
        <div className="ade-node-name">{data.label || data.name || 'New Topic'}</div>
        {data.name && <div className="ade-node-id">{data.name}</div>}
        {data.type && <div className="ade-node-id">{data.type}</div>}
      </div>
      {/* Source — connects to Agent's sub-in handle */}
      <Handle
        type="source"
        position={Position.Right}
        id="topic-out"
        style={{ background: '#0f62fe' }}
      />
    </div>
  );
}

// Currently selected operation sub node ({ nodeId, name } | null), shared with AgentNode.
const SelectedOperationContext = React.createContext(null);

function AgentNode({ id, data, selected }) {
  const updateNodeInternals = useUpdateNodeInternals();
  const selectedOp = React.useContext(SelectedOperationContext);
  const opsKey = (data.ruleOperations || []).join('\u0000');
  // Operation handles are created/removed dynamically — tell React Flow to re-measure.
  useEffect(() => { updateNodeInternals(id); }, [id, opsKey, updateNodeInternals]);

  return (
    <div className={`ade-node ade-node--agent${selected ? ' ade-node--selected' : ''}`}>
      <div className="ade-node-header">
        <span className="ade-node-type-badge">Agent</span>
      </div>
      <div className="ade-node-body">
        <div className="ade-node-name">{data.label || 'New Agent'}</div>
        {data.agentId && <div className="ade-node-id">{data.agentId}</div>}

        {/* Rule JAR badge — shown when set */}
        {data.ruleJar && (
          <div className="ade-node-jar-row" title={data.ruleJar}>
            <svg width="11" height="11" viewBox="0 0 32 32" fill="currentColor" style={{ flexShrink: 0 }}>
              <path d="M28 8H20V4a2 2 0 00-2-2h-4a2 2 0 00-2 2v4H4a2 2 0 00-2 2v18a2 2 0 002 2h24a2 2 0 002-2V10a2 2 0 00-2-2zm-16-4h8v4h-8V4zm16 24H4V10h24v18z"/>
            </svg>
            <span className="ade-node-jar-name">{data.ruleJar.split('/').pop()}</span>
          </div>
        )}

        {/* One sub node per operation: subscriptions connect on the left, outbounds on the right */}
        {data.ruleOperations?.length > 0 && (
          <div className="ade-node-ops">
            <div className="ade-node-ds-title">Decision Operations ({data.ruleOperations.length})</div>
            {data.ruleOperations.map(op => (
              <div
                key={op}
                data-op={op}
                className={`ade-subnode${selectedOp?.nodeId === id && selectedOp?.name === op ? ' ade-subnode--selected' : ''}`}
                title={op}
              >
                <Handle
                  type="target"
                  position={Position.Left}
                  id={opInHandle(op)}
                  style={{ background: '#0f62fe' }}
                />
                <span className="ade-subnode-name">{op}</span>
                <Handle
                  type="source"
                  position={Position.Right}
                  id={opOutHandle(op)}
                  style={{ background: '#198038' }}
                />
              </div>
            ))}
          </div>
        )}

        {/* Links that belong to no operation: history-only subscriptions */}
        <div className="ade-node-ops">
          <div className="ade-subnode ade-subnode--aux" title="Subscribe a topic for history only (no rule invocation)">
            <Handle type="target" position={Position.Left} id={HISTORY_IN_HANDLE} style={{ background: '#0f62fe' }} />
            <span className="ade-subnode-name">context</span>
          </div>
        </div>

        {/* Subscription rows with per-row target handles */}
        {(data.subscriptionHandles || []).map((sh, i) => (
          <div key={sh.id} className="ade-node-row">
            <span className="ade-node-row-label">{sh.label}</span>
            <span className="ade-node-row-badge">sub</span>
            <Handle
              type="target"
              position={Position.Left}
              id={sh.id}
              style={{ top: 74 + i * 28, background: '#0f62fe' }}
            />
          </div>
        ))}

        {/* Outbound rows with per-row source handles */}
        {(data.outboundHandles || []).map((oh, i) => (
          <div key={oh.id} className="ade-node-row">
            <span className="ade-node-row-label">{oh.label}</span>
            <span className="ade-node-row-badge">out</span>
            <Handle
              type="source"
              position={Position.Right}
              id={oh.id}
              style={{
                top: 74 + (data.subscriptionHandles?.length || 0) * 28 + i * 28,
                background: '#198038',
              }}
            />
          </div>
        ))}
      </div>

      {/* Entity related-link comes from below — the only connection point on the Agent object */}
      <Handle
        type="target"
        position={Position.Bottom}
        id="agent-entity-in"
        style={{ background: '#6929c4' }}
      />
    </div>
  );
}

function EntityNode({ data, selected }) {
  return (
    <div className={`ade-node ade-node--entity${selected ? ' ade-node--selected' : ''}`}>
      <div className="ade-node-header">
        <span className="ade-node-type-badge">Entity</span>
      </div>
      <div className="ade-node-body">
        <div className="ade-node-name">{data.label || 'New Entity'}</div>
        {data.entityType && <div className="ade-node-id">{data.entityType}</div>}
      </div>
      {/* Source — connects upward to Agent's entity-in handle */}
      <Handle
        type="source"
        position={Position.Top}
        id="entity-out"
        style={{ background: '#6929c4' }}
      />
    </div>
  );
}

function OutboundNode({ data, selected }) {
  return (
    <div className={`ade-node ade-node--outbound${selected ? ' ade-node--selected' : ''}`}>
      <div className="ade-node-header">
        <span className="ade-node-type-badge">Outbound</span>
      </div>
      <div className="ade-node-body">
        <div className="ade-node-name">{data.label || 'New Outbound'}</div>
        {data.topic && <div className="ade-node-id">{data.topic}</div>}
      </div>
      {/* Target — connected from Agent */}
      <Handle
        type="target"
        position={Position.Left}
        id="outbound-in"
        style={{ background: '#9e1da8' }}
      />
    </div>
  );
}

const NODE_TYPES = {
  topic:        TopicNode,
  agent:        AgentNode,
  entity:       EntityNode,
  outbound:     OutboundNode,
};

// ─── Connection validation & edge style helpers ─────────────────────────────

/** Only allow the three authorised connection patterns. */
function isConnectionValid(connection, nodes) {
  const src = nodes.find(n => n.id === connection.source);
  const tgt = nodes.find(n => n.id === connection.target);
  if (!src || !tgt || src.id === tgt.id) return false;
  const s = src.type, t = tgt.type;
  // A Topic subscribes to an operation: it can only target an operation sub node
  // (op-in handle) inside the agent, never the agent itself.
  if (s === 'topic') {
    return t === 'agent' &&
      (!!connection.targetHandle?.startsWith('op-in:') || connection.targetHandle === HISTORY_IN_HANDLE);
  }
  // Operation sub node left handles accept Topics only
  if (s === 'entity') {
    return t === 'agent' && connection.targetHandle === 'agent-entity-in';
  }
  // Outputs come from an operation
  if (s === 'agent') {
    return t === 'outbound' && !!connection.sourceHandle?.startsWith('op-out:');
  }
  return false;
}

function edgeStyle(srcType) {
  if (srcType === 'topic')        return { stroke: '#0f62fe', strokeWidth: 2 };
  if (srcType === 'entity')       return { stroke: '#6929c4', strokeWidth: 2, strokeDasharray: '5,4' };
  if (srcType === 'agent')        return { stroke: '#198038', strokeWidth: 2 };
  return { stroke: '#8d8d8d', strokeWidth: 1.5 };
}

function edgeLabel(srcType) {
  if (srcType === 'topic')        return 'subscribes';
  if (srcType === 'entity')       return 'relates to';
  if (srcType === 'agent')        return 'generates';
  return '';
}

/** Adds the visual properties (type, label, arrow, colour) to a bare { source, target, ... } edge. */
function decorateEdge(edge, srcType) {
  const style = edgeStyle(srcType);
  return {
    ...edge,
    type: 'labeled',
    animated: false,
    label: edgeLabel(srcType),
    labelStyle: { fontSize: 10, fill: '#525252' },
    labelBgStyle: { fill: '#fff', fillOpacity: 0.8 },
    markerEnd: { type: MarkerType.ArrowClosed, color: style.stroke },
    style,
  };
}

/** Descriptor -> { meta, nodes, edges } ready for React Flow. */
function loadDiagram(descriptor) {
  const { meta, nodes, edges } = descriptorToDiagram(descriptor);
  const typeById = new Map(nodes.map(n => [n.id, n.type]));
  return { meta, nodes, edges: edges.map(e => decorateEdge(e, typeById.get(e.source))) };
}

const LABEL_CHAR_W = 5.5;
const LABEL_PAD = 8;
const LABEL_H = 12;
const LABEL_MIN_GAP = 6;
const LABEL_SIDE_GAP = 6;

/** Centre and side (left/right/top/bottom) of a handle in flow coordinates, or null while the node is not measured yet. */
function handleCenter(nodeLookup, nodeId, handleId, kind) {
  const node = nodeLookup.get(nodeId);
  const bounds = node?.internals?.handleBounds?.[kind];
  if (!bounds?.length) return null;
  const h = bounds.find(b => (handleId ?? null) === (b.id ?? null)) || (handleId ? null : bounds[0]);
  if (!h) return null;
  const abs = node.internals.positionAbsolute;
  return { x: abs.x + h.x + h.width / 2, y: abs.y + h.y + h.height / 2, position: h.position };
}

const isVertical = position => position === Position.Top || position === Position.Bottom;

/**
 * Same placement as LabeledEdge: above the first horizontal run, midway to the first bend;
 * or, when the link leaves its source vertically, beside the first vertical run.
 */
function labelBox(edge, nodeLookup) {
  if (!edge.label) return null;
  const s = handleCenter(nodeLookup, edge.source, edge.sourceHandle, 'source');
  const t = handleCenter(nodeLookup, edge.target, edge.targetHandle, 'target');
  if (!s || !t) return null;
  const w = String(edge.label).length * LABEL_CHAR_W + LABEL_PAD;
  if (isVertical(s.position)) {
    return { x: s.x + LABEL_SIDE_GAP + w / 2, y: (s.y + (s.y + t.y) / 2) / 2, w, runY: NaN };
  }
  const bendX = (s.x + t.x) / 2;
  return {
    x: (s.x + bendX) / 2,
    y: s.y - 3 - LABEL_H / 2,
    w,
    runY: s.y,
    runFrom: Math.min(s.x, bendX),
    runTo: Math.max(s.x, bendX),
  };
}

/** Two labels belong to the same segment when their first horizontal runs are collinear and overlap. */
const sameSegment = (a, b) => Math.abs(a.runY - b.runY) < 2 && a.runFrom < b.runTo && b.runFrom < a.runTo;

/**
 * True when the label of edge `id` would sit on the same segment as, overlap, or sit too
 * close to, the label of an earlier edge that is itself shown (first come, first shown).
 */
function isLabelSuppressed(id, edges, nodeLookup) {
  const shown = [];
  for (const e of edges) {
    const box = labelBox(e, nodeLookup);
    if (!box) continue;
    const clash = shown.some(o => sameSegment(o, box)
      || (Math.abs(o.x - box.x) < (o.w + box.w) / 2 + LABEL_MIN_GAP
        && Math.abs(o.y - box.y) < LABEL_H + LABEL_MIN_GAP));
    if (e.id === id) return clash;
    if (!clash) shown.push(box);
  }
  return false;
}

/**
 * Smoothstep edge whose label sits just above the first horizontal run, centred
 * between the source handle and the first bend, so it never straddles the
 * vertical segment of the routed line. When the link leaves its source vertically
 * (top/bottom handle) the label sits beside that vertical run instead. A label that
 * would overlap or crowd the label of another edge is not rendered.
 */
function LabeledEdge({
  id, sourceX, sourceY, targetX, targetY, sourcePosition, targetPosition,
  markerEnd, style, label, selected,
}) {
  const suppressed = useStore(s => isLabelSuppressed(id, s.edges, s.nodeLookup));
  const [path] = getSmoothStepPath({
    sourceX, sourceY, sourcePosition, targetX, targetY, targetPosition,
  });
  const vertical = isVertical(sourcePosition);
  const bendX = (sourceX + targetX) / 2;
  const labelTransform = vertical
    ? `translate(0, -50%) translate(${sourceX + LABEL_SIDE_GAP}px, ${(sourceY + (sourceY + targetY) / 2) / 2}px)`
    : `translate(-50%, -100%) translate(${(sourceX + bendX) / 2}px, ${sourceY - 3}px)`;
  return (
    <>
      <BaseEdge
        id={id}
        path={path}
        markerEnd={markerEnd}
        style={selected ? { ...style, strokeWidth: (style?.strokeWidth || 2) + 1.5, filter: 'drop-shadow(0 0 3px rgba(15,98,254,0.6))' } : style}
      />
      {label && !suppressed && (
        <EdgeLabelRenderer>
          <div
            className="ade-edge-label"
            style={{ transform: labelTransform }}
          >
            {label}
          </div>
        </EdgeLabelRenderer>
      )}
    </>
  );
}

const EDGE_TYPES = { labeled: LabeledEdge };


// ─── Palette items ──────────────────────────────────────────────────────────
const PALETTE_NODES = [
  { type: 'agent',        ...NODE_TYPES_META.agent },
  { type: 'entity',       ...NODE_TYPES_META.entity },
  { type: 'topic',        ...NODE_TYPES_META.topic },
  { type: 'outbound',     ...NODE_TYPES_META.outbound },
];

function PaletteItem({ type, label, hint, abbr }) {
  const onDragStart = (e) => {
    e.dataTransfer.effectAllowed = 'copy';
    e.dataTransfer.setData('application/ade-type', type);
  };
  return (
    <div
      className={`ade-palette-item ade-palette-item--${type}`}
      draggable
      onDragStart={onDragStart}
      title={hint || label}
    >
      <div className="ade-palette-item-icon">{abbr}</div>
      <div>
        <div className="ade-palette-item-label">{label}</div>
        {hint && <div className="ade-palette-item-hint">{hint}</div>}
      </div>
    </div>
  );
}

// ─── Application-level properties (shown when no node is selected) ──────────
function ApplicationPropertiesEditor({ meta, onUpdate }) {
  // Derive a URL-safe slug from a display name.
  // "Banking Fraud Detection Agent" → "banking-fraud-detection-agent"
  const toSlug = (name) =>
    name
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, '-')
      .replace(/^-+|-+$/g, '');

  // inferFromName — fills empty sibling fields; called on blur / Enter only.
  const inferFromName = (val) => {
    const slug = toSlug(val);
    const updates = { ...meta, name: val };
    if (!meta.flinkJobName)    updates.flinkJobName    = val;
    if (!meta.folderName)      updates.folderName      = slug;
    if (!meta.consumerGroupId) updates.consumerGroupId = slug ? `event-rule-bff-${slug}` : '';
    onUpdate(updates);
  };

  const set = (key, val) => onUpdate({ ...meta, [key]: val });

  const field = (key, label, opts = {}) => (
    <div className="ade-field-group" key={key}>
      <label className="ade-field-label">{label}</label>
      {opts.hint && (
        <div className="ade-field-hint">{opts.hint}</div>
      )}
      {opts.prefix ? (
        <div className="ade-field-prefix-wrap">
          <span className="ade-field-prefix">{opts.prefix}</span>
          <input
            className="ade-field-input ade-field-input--prefixed"
            type="text"
            value={meta[key] || ''}
            onChange={e => set(key, e.target.value)}
            placeholder={opts.placeholder || ''}
            spellCheck={false}
          />
        </div>
      ) : opts.multiline ? (
        <textarea
          className="ade-field-input ade-field-textarea"
          value={meta[key] || ''}
          onChange={e => set(key, e.target.value)}
          rows={3}
          placeholder={opts.placeholder || ''}
        />
      ) : (
        <input
          className="ade-field-input"
          type="text"
          value={meta[key] || ''}
          onChange={e => set(key, e.target.value)}
          placeholder={opts.placeholder || ''}
          spellCheck={false}
        />
      )}
    </div>
  );

  return (
    <>
      <div className="ade-field-section-title">Event Rule Application</div>
      <div className="ade-field-group">
        <label className="ade-field-label">Name</label>
        <input
          className="ade-field-input"
          type="text"
          value={meta.name || ''}
          onChange={e => set('name', e.target.value)}
          onBlur={e => inferFromName(e.target.value)}
          onKeyDown={e => { if (e.key === 'Enter') { e.preventDefault(); inferFromName(e.target.value); e.target.blur(); } }}
          placeholder="e.g. Banking Fraud Detection Agent"
          spellCheck={false}
        />
      </div>
      {field('folderName', 'Folder name', {
        prefix: 'samples/',
        placeholder: 'e.g. banking-fraud-agent',
        hint: 'The agents.json file will be saved under samples/<folder>/agents.json',
      })}
      {field('description',     'Description',     { multiline: true, placeholder: 'Describe the purpose of this application…' })}
      <div className="ade-field-section-title">Flink Job</div>
      {field('flinkJobName',    'Flink Job Name',  { placeholder: 'e.g. banking-fraud-job', hint: 'Displayed in the Flink dashboard. Defaults to Name if left blank.' })}
      {field('consumerGroupId', 'Consumer Group',  { placeholder: 'e.g. event-rule-bff-my-agent', hint: 'Kafka consumer group ID used by the BFF.' })}
      <div className="ade-field-group">
        <label className="ade-field-label">Job Parallelism</label>
        <div className="ade-field-hint">Flink job-level parallelism. Blank or 0 = FLINK_PARALLELISM environment setting.</div>
        <input
          className="ade-field-input"
          type="number"
          min="0"
          aria-label="Job parallelism"
          value={meta.parallelism ?? ''}
          onChange={e => set('parallelism', e.target.value === '' ? '' : Number(e.target.value))}
        />
      </div>

    </>
  );
}


// Sub-folder of samples/<application>/ holding the Decision Service project zip.
const PROJECT_DIR = 'di-project';

// ─── Archive file picker field (click-to-browse + drop zone) ────────────────
function ArchiveFileField({ label, hint, ext, accept, value, onChange, pathPlaceholder }) {
  const inputRef = useRef(null);
  const [over, setOver] = useState(false);

  const isValid = (file) => file.name.toLowerCase().endsWith(ext);

  const handleFile = (file) => {
    if (!file || !isValid(file)) return;
    onChange(file.name, file);
  };

  const onDragOver = (e) => {
    const hasFile = [...(e.dataTransfer.items || [])].some(f => f.kind === 'file');
    if (!hasFile) return;
    e.preventDefault();
    setOver(true);
  };

  const onDrop = (e) => {
    e.preventDefault();
    setOver(false);
    const file = [...(e.dataTransfer.files || [])].find(isValid);
    if (file) handleFile(file);
  };

  const basename = value ? value.split('/').pop() : '';

  return (
    <div className="ade-field-group">
      <label className="ade-field-label">{label}</label>
      {hint && <div className="ade-field-hint">{hint}</div>}
      {/* Hidden native file input — validation done on select */}
      <input
        ref={inputRef}
        type="file"
        accept={accept}
        style={{ display: 'none' }}
        onChange={e => { handleFile(e.target.files?.[0]); e.target.value = ''; }}
      />

      {/* Drop zone + pick button */}
      <div
        className={`ade-jar-dropzone${over ? ' ade-jar-dropzone--over' : ''}${value ? ' ade-jar-dropzone--set' : ''}`}
        onDragOver={onDragOver}
        onDragLeave={() => setOver(false)}
        onDrop={onDrop}
        onClick={() => inputRef.current?.click()}
        title={value || `Click or drop a ${ext} file`}
      >
        <svg width="14" height="14" viewBox="0 0 32 32" fill="currentColor" className="ade-jar-icon">
          <path d="M28 8H20V4a2 2 0 00-2-2h-4a2 2 0 00-2 2v4H4a2 2 0 00-2 2v18a2 2 0 002 2h24a2 2 0 002-2V10a2 2 0 00-2-2zm-16-4h8v4h-8V4zm16 24H4V10h24v18z"/>
        </svg>
        {value ? (
          <span className="ade-jar-filename" title={value}>{basename}</span>
        ) : (
          <span className="ade-jar-placeholder">
            {over ? 'Drop to set' : `Click to browse or drop ${ext}`}
          </span>
        )}
        {value && (
          <button
            className="ade-jar-clear"
            title={`Clear ${label} selection`}
            onClick={e => { e.stopPropagation(); onChange(''); }}
          >
            ✕
          </button>
        )}
      </div>

      {/* Editable path — also allows pasting a full path manually */}
      <input
        className="ade-field-input"
        type="text"
        value={value}
        onChange={e => onChange(e.target.value)}
        placeholder={pathPlaceholder}
        spellCheck={false}
        style={{ marginTop: 5, fontSize: '0.75rem', color: 'var(--text-muted)' }}
      />
    </div>
  );
}


// ─── Type / Field combo boxes ───────────────────────────────────────────────
// Options come from the Decision Service Project(s) of the agent(s) in scope
// (the agent being edited, or the agents a node is connected to):
//   - Type  → classes (.voc)
//   - Field → variables exposed by an operation (task model .dop / decision model .dmo)
//   - Field of an outbound link → attributes of the type of the operation's output
//     (OUT variable of a task model / top-level decision of a decision model)
// Free text stays allowed (datalist).
const VocabContext = createContext({ typeOptions: [], variablesByOperation: {}, outputFieldsByOperation: {} });

/**
 * kind: 'type' (list of classes), 'field' (variables of `operation`) or
 * 'output-field' (attributes of the output type of `operation`).
 * Without kind, renders a plain text input.
 */
function ComboInput({ kind, operation, ...inputProps }) {
  const { typeOptions, variablesByOperation, outputFieldsByOperation } = useContext(VocabContext);
  const listId = useId();
  if (!kind) return <input type="text" {...inputProps} />;
  const options = kind === 'type'
    ? typeOptions
    : kind === 'output-field'
      ? outputFieldsByOperation[operation] || []
      : (variablesByOperation[operation] || []).map(v => ({ value: v, label: v }));
  return (
    <>
      <input type="text" list={listId} autoComplete="off" spellCheck={false} {...inputProps} />
      <datalist id={listId}>
        {options.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
      </datalist>
    </>
  );
}

// ─── Operation editor (agents[*].ruleset.operations[*]) ─────────────────────
const EMPTY_OPERATION = {
  entityField: '',
  currentEventField: '',
  scheduleRequestsField: '',
  outboundEntityField: '',
  history: [],
};

/** Keep only the configs of operations that still exist. */
function pruneOperationConfigs(configs, operations) {
  const keep = new Set(operations);
  return Object.fromEntries(Object.entries(configs || {}).filter(([name]) => keep.has(name)));
}

function OperationEditor({ operation, config, onChange, onRename, onDelete }) {
  const cfg = { ...EMPTY_OPERATION, ...config };
  const set = (key, val) => onChange({ ...cfg, [key]: val });

  const textField = (key, label, placeholder, hint, kind = 'field') => (
    <div className="ade-field-group" key={key}>
      <label className="ade-field-label">{label}</label>
      {hint && <div className="ade-field-hint">{hint}</div>}
      <ComboInput
        kind={kind}
        operation={operation}
        className="ade-field-input"
        value={cfg[key] || ''}
        onChange={e => set(key, e.target.value)}
        placeholder={placeholder}
      />
    </div>
  );

  // Editable list of { a, b } pairs (history: eventType/field)
  const pairList = (key, title, keyA, labelA, keyB, labelB, hint) => (
    <>
      <div className="ade-field-section-title">{title}</div>
      {hint && <div className="ade-field-hint">{hint}</div>}
      {cfg[key].map((item, i) => (
        <div className="ade-pair-row" key={i}>
          <ComboInput
            kind="type"
            className="ade-field-input"
            value={item[keyA] || ''}
            placeholder={labelA}
            onChange={e => set(key, cfg[key].map((x, j) => j === i ? { ...x, [keyA]: e.target.value } : x))}
          />
          <ComboInput
            kind="field"
            operation={operation}
            className="ade-field-input"
            value={item[keyB] || ''}
            placeholder={labelB}
            onChange={e => set(key, cfg[key].map((x, j) => j === i ? { ...x, [keyB]: e.target.value } : x))}
          />
          <button
            className="ade-pair-remove"
            title="Remove"
            onClick={() => set(key, cfg[key].filter((_, j) => j !== i))}
          >
            ✕
          </button>
        </div>
      ))}
      <button
        className="ade-pair-add"
        onClick={() => set(key, [...cfg[key], { [keyA]: '', [keyB]: '' }])}
      >
        + Add
      </button>
    </>
  );

  return (
    <>
      <div className="ade-field-group">
        <label className="ade-field-label">Decision Operation</label>
        <div className="ade-field-hint">Name of the Decision Service operation used to process this event.</div>
        <CommitInput
          aria-label="Operation name"
          value={operation}
          onCommit={name => { if (name.trim()) onRename(name.trim()); }}
        />
        <button className="ade-pair-add" onClick={onDelete}>Delete operation</button>
      </div>
      <div className="ade-field-section-title">Input</div>
      {textField('entityField', 'Entity Field (required)', 'e.g. theAccount', 'Operation parameter that receives the entity.')}
      {textField('currentEventField', 'Event Field (required)', 'e.g. theTransaction', 'Operation parameter that receives the event being processed.')}
      {pairList('history', 'History Fields', 'eventType', 'Event type', 'field', 'Rule field', 'Operation parameter that receives historical events of this type (e.g. Transaction → thePastTransactions).')}
      <div className="ade-field-section-title">Output</div>
      {textField('outboundEntityField', 'Outbound Entity Field', 'e.g. theUpdatedAccount', 'Field of the decision service result that holds the updated entity. Use Entity Field if blank.', 'output-field')}
      {textField('scheduleRequestsField', 'Schedule Requests Field', 'e.g. theScheduleRequests', 'Field of the decision service result that holds the schedule requests posted by the rule engine.', 'output-field')}
    </>
  );
}
// ─── Outbound link editor (agents[*].ruleset.operations[*].outbounds[*]) ────
function OutboundLinkEditor({ operation, outboundLabel, eventType = '', data, onChange }) {
  const d = { field: '', ...data };
  const set = (key, val) => onChange({ ...d, [key]: val });
  const resolvedEventType = eventType || d.eventType || '';
  return (
    <>
      <div className="ade-field-group">
        <label className="ade-field-label">Link</label>
        <input
          className="ade-field-input"
          type="text"
          value={`${operation} → ${outboundLabel}`}
          readOnly
        />
      </div>
      <div className="ade-field-group">
        <label className="ade-field-label">Field</label>
        <div className="ade-field-hint">Output decision operation parameter that holds the generated events.</div>
        <ComboInput
          kind="output-field"
          operation={operation}
          className="ade-field-input"
          value={d.field}
          onChange={e => set('field', e.target.value)}
          placeholder="e.g. theFraudAlerts"
        />
      </div>
      <div className="ade-field-group">
        <label className="ade-field-label">Event Type</label>
        <div className="ade-field-hint">Type of events sent to this topic. Derived from the target Outbound event type.</div>
        <input
          className="ade-field-input"
          type="text"
          value={resolvedEventType}
          readOnly
        />
      </div>
    </>
  );
}



// ─── Subscription editor (agents[*].subscriptions[*]) ────────────────────────
const HORIZON_TYPES = ['count', 'time', 'hybrid'];
const EMPTY_SUBSCRIPTION = {
  eventType: '',
  operation: '',
  horizon: { type: 'count', value: 100, description: '' },
};

/**
 * Edits one subscription: the data of a Topic → operation link, or (with `topicless`) a
 * subscription that has no topic, such as Schedule, whose event type and operation are edited
 * here since no Topic node supplies them.
 */
function SubscriptionLinkEditor({
  topicName, agentLabel, eventType = '', data, onChange,
  topicless = false, operations = [], referenceNames = [],
}) {
  const d = { ...EMPTY_SUBSCRIPTION, ...data, horizon: { ...EMPTY_SUBSCRIPTION.horizon, ...data?.horizon } };
  const h = d.horizon;
  const set = (key, val) => onChange({ ...d, [key]: val });
  const setH = (key, val) => set('horizon', { ...h, [key]: val });
  const num = (v) => (v === '' ? '' : Number(v));
  const resolvedEventType = topicless ? d.eventType : (eventType || d.eventType || '');

  return (
    <>
      {!topicless && (
        <div className="ade-field-group">
          <label className="ade-field-label">Subscription</label>
          <input className="ade-field-input" type="text" value={`${topicName} → ${agentLabel}`} readOnly />
        </div>
      )}
      <div className="ade-field-group">
        <label className="ade-field-label">Event Type</label>
        <input
          className="ade-field-input"
          type="text"
          value={resolvedEventType}
          readOnly={!topicless}
          onChange={e => set('eventType', e.target.value)}
          placeholder="e.g. Schedule"
          spellCheck={false}
        />
        {!topicless && <div className="ade-field-hint">Derived from the subscribed Topic event type.</div>}
      </div>
      <div className="ade-field-group">
        <label className="ade-field-label">Decision Operation</label>
        {topicless ? (
          <select className="ade-field-input" value={d.operation || ''} onChange={e => set('operation', e.target.value)}>
            <option value="">(history only)</option>
            {operations.map(op => <option key={op} value={op}>{op}</option>)}
          </select>
        ) : (
          <input className="ade-field-input" type="text" value={d.operation || '(history only)'} readOnly />
        )}
      </div>
      <TextField
        label="Entity Key Extractor"
        hint="JSONPath of the entity key; overrides the agent-level extractor. Required here when the agent has none; blank inherits the agent's."
        value={d.entityKeyExtractor}
        placeholder="Inherit the agent extractor"
        onChange={v => set('entityKeyExtractor', v === '' ? undefined : v)}
      />

      <div className="ade-field-section-title">Horizon</div>
      <div className="ade-field-group">
        <label className="ade-field-label">Type</label>
        <select
          className="ade-field-input"
          value={h.type}
          onChange={e => setH('type', e.target.value)}
        >
          {HORIZON_TYPES.map(t => <option key={t} value={t}>{t}</option>)}
        </select>
      </div>
      {h.type === 'count' && (
        <div className="ade-field-group">
          <label className="ade-field-label">Value</label>
          <div className="ade-field-hint">Number of events kept (0 disables history).</div>
          <input
            className="ade-field-input"
            type="number"
            min="0"
            value={h.value ?? ''}
            onChange={e => setH('value', num(e.target.value))}
          />
        </div>
      )}
      {h.type === 'hybrid' && (
        <div className="ade-field-group">
          <label className="ade-field-label">Count Limit</label>
          <input
            className="ade-field-input"
            type="number"
            min="0"
            value={h.countLimit ?? ''}
            onChange={e => setH('countLimit', num(e.target.value))}
          />
        </div>
      )}
      {(h.type === 'time' || h.type === 'hybrid') && (
        <div className="ade-field-group">
          <label className="ade-field-label">Time Limit</label>
          <div className="ade-field-hint">Units: s, mn, h, d, m (months). e.g. 24h, 30d.</div>
          <input
            className="ade-field-input"
            type="text"
            value={h.timeLimit || ''}
            onChange={e => setH('timeLimit', e.target.value)}
            placeholder="e.g. 30d"
            spellCheck={false}
          />
        </div>
      )}
      <div className="ade-field-group">
        <label className="ade-field-label">Description</label>
        <textarea
          className="ade-field-input ade-field-textarea"
          rows={2}
          value={h.description || ''}
          onChange={e => setH('description', e.target.value)}
          placeholder="e.g. Keep the last 100 events"
        />
      </div>

      {!topicless && (
        <>
          <EnrichmentEditor
            steps={d.enrichment || []}
            referenceNames={referenceNames}
            onChange={steps => set('enrichment', steps)}
            hint="Applied to each incoming event before the agent processes it."
          />
          <SqlEnrichmentFields data={d} subject="incoming events" onChange={patch => onChange({ ...d, ...patch })} />
        </>
      )}
    </>
  );
}

// ─── Detail editor panel ────────────────────────────────────────────────────
function DetailEditor({ node, onUpdate, agentsMeta, onMetaUpdate, selectedOperation, onRenameOperation, referenceNames = [], linkedEntityType = '' }) {
  if (!node) {
    return (
      <ApplicationPropertiesEditor meta={agentsMeta} onUpdate={onMetaUpdate} />
    );
  }

  const { type, data } = node;

  const removeOperation = (op) => {
    const rest = { ...data.operationConfigs };
    delete rest[op];
    onUpdate(node.id, {
      ...data,
      ruleOperations: (data.ruleOperations || []).filter(o => o !== op),
      operationConfigs: rest,
    });
  };

  if (type === 'agent' && selectedOperation) {
    return (
      <OperationEditor
        operation={selectedOperation}
        config={data.operationConfigs?.[selectedOperation]}
        onChange={cfg => onUpdate(node.id, {
          ...data,
          operationConfigs: { ...data.operationConfigs, [selectedOperation]: cfg },
        })}
        onRename={name => onRenameOperation(node.id, selectedOperation, name)}
        onDelete={() => removeOperation(selectedOperation)}
      />
    );
  }

  // Editing a Topic that was only implied by a subscription declares it in topics[].
  const set = (key, val) => {
    const next = {
      ...data,
      [key]: val,
      ...(type === 'topic' && { implicit: false }),
      // The Agent ID follows the Name until it has been edited by hand.
      ...(type === 'agent' && key === 'label' && (data.agentId || '') === slugifyAgentId(data.label) && { agentId: slugifyAgentId(val) }),
    };
    // The archive control topic is derived from the Agent ID while it is empty or still the derived default.
    if (type === 'agent' && next.agentId !== data.agentId
        && (!data.archiveControlTopic || data.archiveControlTopic === archiveControlTopicFor(data.agentId))) {
      next.archiveControlTopic = archiveControlTopicFor(next.agentId);
    }
    onUpdate(node.id, next);
  };

  // Decision Service Project: on zip selection, explore it and store the
  // operations / classes found on the agent node.
  const onProjectChange = async (name, file) => {
    if (!file) {
      onUpdate(node.id, {
        ...data,
        ruleProject: name,
        ruleProjectFile: undefined, // a typed / cleared path supersedes any pending upload
        ...(name ? {} : { ruleClasses: [], ruleAttributes: {}, ruleOperationVariables: {}, ruleOperationOutputTypes: {}, ruleProjectError: '' }),
      });
      return;
    }
    try {
      const { operations: found, classes, attributes, operationVariables, operationOutputTypes } = await exploreDecisionServiceProject(file);
      // Operations already configured or linked in the diagram stay, even when the project lacks them.
      const operations = Array.from(new Set([...found, ...Object.keys(data.operationConfigs || {})]));
      onUpdate(node.id, {
        ...data,
        // The zip is uploaded to the di-project sub-folder when the diagram is saved.
        ruleProject: `${PROJECT_DIR}/${name}`,
        ruleProjectFile: file,
        ruleOperations: operations,
        operationConfigs: pruneOperationConfigs(data.operationConfigs, operations),
        ruleClasses: classes,
        ruleAttributes: attributes,
        ruleOperationVariables: operationVariables,
        ruleOperationOutputTypes: operationOutputTypes,
        ruleProjectError: '',
      });
    } catch (err) {
      onUpdate(node.id, {
        ...data,
        ruleProject: `${PROJECT_DIR}/${name}`,
        ruleProjectFile: file,
        ruleOperations: [],
        ruleClasses: [],
        ruleAttributes: {},
        ruleOperationVariables: {},
        ruleOperationOutputTypes: {},
        ruleProjectError: `Cannot read zip: ${err.message}`,
      });
    }
  };

  const field = (key, label, opts = {}) => (
    <div className="ade-field-group" key={key}>
      <label className="ade-field-label">{label}</label>
      {opts.hint && (
        <div className="ade-field-hint">{opts.hint}</div>
      )}
      {opts.multiline ? (
        <textarea
          className="ade-field-input ade-field-textarea"
          value={data[key] || ''}
          onChange={e => set(key, e.target.value)}
          rows={3}
          placeholder={opts.placeholder || ''}
        />
      ) : (
        <ComboInput
          kind={opts.combo}
          className="ade-field-input"
          value={data[key] || ''}
          onChange={e => set(key, e.target.value)}
          placeholder={opts.placeholder || ''}
        />
      )}
    </div>
  );

  return (
    <>
      {type === 'topic' && (
        <>
          {field('label', 'Name', {
            placeholder: 'e.g. Flight Events',
            hint: 'Human-readable display name for this topic.',
          })}
          {field('name', 'Kafka Topic Name', {
            placeholder: 'e.g. agent-flight-events',
            hint: 'Kafka topic name where events are published and consumed.',
          })}
          {field('type', 'Event Type', {
            placeholder: 'e.g. FlightEvent',
            combo: 'type',
            hint: 'Java or business class name of events carried on this topic.',
          })}
          <ArchiveFileField
            label="Event Schema (optional)"
            hint="JSON Schema file defining the structure and validation rules for event payloads."
            ext=".json"
            accept=".json,application/json"
            value={data.schema || ''}
            onChange={val => set('schema', val)}
            pathPlaceholder="e.g. samples/my-agent/schemas/flight-event.schema.json"
          />
          {field('idleness', 'Topic Idleness (optional)', {
            placeholder: 'e.g. 30s',
            hint: 'Flink watermark idleness timeout for advancing event time when a partition is idle.',
          })}
        </>
      )}
      {type === 'agent' && (
        <>
          {field('label',       'Name',        { placeholder: 'e.g. OrderAgent' })}
          {field('agentId',     'Agent ID',    { placeholder: 'e.g. order-agent' })}
          {field('description', 'Description', { multiline: true })}
          <div className="ade-field-group">
            <label className="ade-field-label">Entity Type</label>
            <div className="ade-field-hint">Inferred from the Entity linked to this agent.</div>
            <input
              className="ade-field-input"
              type="text"
              aria-label="Entity type"
              value={linkedEntityType}
              placeholder="(no entity linked)"
              readOnly
            />
          </div>
          <TextField
            label="Entity Key Extractor"
            hint="JSONPath of the entity key in each event, e.g. $.accountId. Subscriptions may override it."
            value={data.entityKeyExtractor}
            placeholder="e.g. $.accountId"
            onChange={v => set('entityKeyExtractor', v === '' ? undefined : v)}
          />
          <Field label="Chaining" hint="Whether the agent takes part in operator chaining (default: enabled).">
            <select
              className="ade-field-input"
              value={data.chainingEnabled === false ? 'false' : 'true'}
              onChange={e => set('chainingEnabled', e.target.value === 'false' ? false : undefined)}
            >
              <option value="true">enabled</option>
              <option value="false">disabled</option>
            </select>
          </Field>
          <Field label="Agent Parallelism" hint="Operator parallelism of this agent. Blank or 0 inherits the job setting.">
            <input
              className="ade-field-input"
              type="number"
              min="0"
              aria-label="Agent parallelism"
              value={data.parallelism ?? ''}
              onChange={e => set('parallelism', e.target.value === '' ? '' : Number(e.target.value))}
            />
          </Field>
          <div className="ade-field-section-title">Rule Engine</div>
          <ArchiveFileField
            label="Decision Service Archive (Jar)"
            ext=".jar"
            accept=".jar,application/java-archive"
            value={data.ruleJar || ''}
            onChange={(val, file) => {
              // A picked jar is stored under the application folder: samples/<folderName>/<jar>.
              const folder = agentsMeta?.folderName?.trim();
              set('ruleJar', file && folder ? `samples/${folder}/${val}` : val);
            }}
            pathPlaceholder="e.g. samples/my-agent/rules.jar"
          />
          <ArchiveFileField
            label="Decision Service Project"
            hint={`Decision Service project zip, stored in the ${PROJECT_DIR} sub-folder of the application folder when the diagram is saved.`}
            ext=".zip"
            accept=".zip,application/zip"
            value={data.ruleProject || ''}
            onChange={onProjectChange}
            pathPlaceholder={`e.g. samples/my-agent/${PROJECT_DIR}/decision-service.zip`}
          />
          {data.ruleProjectError && (
            <div className="ade-field-error">{data.ruleProjectError}</div>
          )}

          <TextField
            label="Decision Archive Control Topic (required)"
            hint="Kafka topic that controls decision archive updates. Derived from the Agent ID when empty."
            value={data.archiveControlTopic}
            placeholder="e.g. agent-archive-control"
            onChange={v => set('archiveControlTopic', v)}
          />

          <div className="ade-field-section-title">Decision Operations</div>
          <div className="ade-field-hint">
            Click an operation in the agent node to edit it; link Topics to its left handle and Outbounds to its right handle.
          </div>
          {(data.ruleOperations || []).map(op => (
            <div className="ade-op-chip" key={op}>
              {op}
              <button
                className="ade-op-chip-remove"
                title={`Remove operation ${op}`}
                aria-label={`Remove operation ${op}`}
                onClick={() => removeOperation(op)}
              >
                ✕
              </button>
            </div>
          ))}
          <CommitInput
            aria-label="New operation name"
            value=""
            placeholder="+ Add operation (name, then Enter)"
            onCommit={name => {
              const op = name.trim();
              if (!op || (data.ruleOperations || []).includes(op)) return;
              onUpdate(node.id, {
                ...data,
                ruleOperations: [...(data.ruleOperations || []), op],
                operationConfigs: { ...data.operationConfigs, [op]: { ...EMPTY_OPERATION } },
              });
            }}
          />

          <ReferencesEditor references={data.references || []} onChange={v => set('references', v)} />

          <div className="ade-field-section-title">Schedules</div>
          <div className="ade-field-hint">
            Schedule event processing. Schedule events are generated by schedule requests.
          </div>
          {(data.topiclessSubscriptions || []).map((sub, i) => (
            <div className="ade-card" key={i}>
              <div className="ade-card-header">
                <span>{sub.eventType || 'Subscription'}</span>
                <button
                  className="ade-pair-remove"
                  title="Remove subscription"
                  aria-label={`Remove subscription ${i + 1}`}
                  onClick={() => set('topiclessSubscriptions', data.topiclessSubscriptions.filter((_, j) => j !== i))}
                >
                  ✕
                </button>
              </div>
              <SubscriptionLinkEditor
                topicless
                data={sub}
                operations={data.ruleOperations || []}
                onChange={d => set('topiclessSubscriptions', data.topiclessSubscriptions.map((x, j) => (j === i ? d : x)))}
              />
            </div>
          ))}
          <button
            className="ade-pair-add"
            onClick={() => set('topiclessSubscriptions', [
              ...(data.topiclessSubscriptions || []),
              { eventType: 'Schedule', operation: '', horizon: { type: 'count', value: 0, description: '' } },
            ])}
          >
            + Add schedule
          </button>

          <ErrorsEditor value={data.errorOutput || {}} onChange={v => set('errorOutput', v)} />

          <StateConfigEditor value={data.stateConfig || {}} onChange={v => set('stateConfig', v)} />
        </>
      )}
      {type === 'entity' && (
        <>
          {field('label',       'Name',             { placeholder: 'e.g. Customer' })}
          {field('entityType',  'Entity Type',      { placeholder: 'e.g. customer', combo: 'type' })}
          {field('topic',       'Changelog Topic',  { placeholder: 'e.g. customer-changelog', hint: 'Kafka topic that receives the entity state changes (stateConfig.changeLogTopic).' })}
        </>
      )}
      {type === 'outbound' && (
        <>
          {field('label',     'Name',             { placeholder: 'e.g. DecisionOutput' })}
          {field('topic',     'Kafka Topic',      { placeholder: 'e.g. decisions-topic' })}
          {field('eventType', 'Output Event Type',{ placeholder: 'e.g. com.example.Decision', combo: 'type' })}
          {field('description','Description',     { multiline: true })}
          <EnrichmentEditor
            steps={data.enrichment || []}
            referenceNames={referenceNames}
            onChange={steps => set('enrichment', steps)}
            hint="Applied to the payload of each emitted event before it is written to the topic."
          />
          <SqlEnrichmentFields data={data} subject="emitted events" onChange={patch => onUpdate(node.id, { ...data, ...patch })} />
        </>
      )}
    </>
  );
}

export { buildDiagramDescriptor };

const DEFAULT_APPLICATION_NAME = 'My Application';
const DEFAULT_APPLICATION_FOLDER = 'my-application';

// ─── Inner editor — needs to be inside ReactFlowProvider ───────────────────
// Receives callbacks from the outer shell to update nodes/selection.
function InnerEditor({ onClose, initialFolderName = '', initialDescriptor = null }) {
  // An existing descriptor is converted once, when the editor opens.
  const initial = useMemo(
    () => (initialDescriptor ? loadDiagram(initialDescriptor) : null),
    [], // eslint-disable-line react-hooks/exhaustive-deps
  );
  const [nodes, setNodes, onNodesChangeOriginal] = useNodesState(initial?.nodes || []);
  const [edges, setEdges, onEdgesChangeOriginal] = useEdgesState(initial?.edges || []);
  const [selectedNode, setSelectedNode] = useState(null);
  const [selectedOp, setSelectedOp] = useState(null);
  const [selectedEdgeId, setSelectedEdgeId] = useState(null);
  const [detailWidth, setDetailWidth] = useState(420);
  const [saveState, setSaveState] = useState('idle'); // 'idle' | 'saving' | 'saved' | 'error'
  const [saveError, setSaveError] = useState(null);
  const saveResetTimer = useRef(null);
  useEffect(() => () => clearTimeout(saveResetTimer.current), []);
  const [showClearModal, setShowClearModal] = useState(false);

  // Top-level agents.json metadata (edited in the detail panel when no node is selected)
  // A brand-new application (no folder, no descriptor) starts with a default name and folder.
  const isNewApplication = !initialFolderName && !initialDescriptor;
  const [agentsMeta, setAgentsMeta] = useState({
    name: isNewApplication ? DEFAULT_APPLICATION_NAME : '',
    description: '',
    flinkJobName: '',
    consumerGroupId: '',
    parallelism: '',
    ...initial?.meta,
    folderName: initialFolderName || (isNewApplication ? DEFAULT_APPLICATION_FOLDER : ''),
  });

  const {
    canUndo,
    canRedo,
    undo,
    redo,
    pushSnapshot,
    updateDebouncedSnapshot,
    startDrag,
    stopDrag,
    setBaseline,
    presentRef,
  } = useDiagramHistory({
    nodes: initial?.nodes || [],
    edges: initial?.edges || [],
    agentsMeta: {
      name: isNewApplication ? DEFAULT_APPLICATION_NAME : '',
      description: '',
      flinkJobName: '',
      consumerGroupId: '',
      parallelism: '',
      ...initial?.meta,
      folderName: initialFolderName || (isNewApplication ? DEFAULT_APPLICATION_FOLDER : ''),
    },
  });

  // Apply a snapshot to state and keep detail selection in sync
  const applySnapshot = useCallback((snapshot) => {
    if (!snapshot) return;
    setNodes(deepClone(snapshot.nodes));
    setEdges(deepClone(snapshot.edges));
    setAgentsMeta(deepClone(snapshot.agentsMeta));

    setSelectedNode(prev => {
      if (!prev) return null;
      const found = snapshot.nodes.find(n => n.id === prev.id);
      return found ? deepClone(found) : null;
    });

    setSelectedOp(prev => {
      if (!prev) return null;
      const node = snapshot.nodes.find(n => n.id === prev.nodeId);
      if (!node || !node.data?.ruleOperations?.includes(prev.name)) return null;
      return prev;
    });

    setSelectedEdgeId(prev => {
      if (!prev) return null;
      return snapshot.edges.some(e => e.id === prev) ? prev : null;
    });
  }, [setNodes, setEdges, setAgentsMeta]);

  const handleUndo = useCallback(() => {
    const snapshot = undo();
    if (snapshot) {
      applySnapshot(snapshot);
    }
  }, [undo, applySnapshot]);

  const handleRedo = useCallback(() => {
    const snapshot = redo();
    if (snapshot) {
      applySnapshot(snapshot);
    }
  }, [redo, applySnapshot]);


  // A registered Decision Service project (ruleset.project) is loaded once, when the editor
  // opens, to populate the Type / Field combo boxes. Operations stay those of the descriptor.
  useEffect(() => {
    let cancelled = false;
    for (const n of initial?.nodes || []) {
      const path = n.type === 'agent' ? n.data.ruleProject : '';
      if (!path) continue;
      (async () => {
        try {
          const res = await fetch(`${API_URL}/api/project-file?path=${encodeURIComponent(path)}`);
          if (!res.ok) throw new Error(`HTTP ${res.status}`);
          const { classes, attributes, operationVariables, operationOutputTypes } =
            await exploreDecisionServiceProject(await res.arrayBuffer());
          if (cancelled) return;
          setNodes(nds => {
            const nextNodes = nds.map(x => (x.id === n.id
              ? {
                ...x,
                data: {
                  ...x.data,
                  ruleClasses: classes,
                  ruleAttributes: attributes,
                  ruleOperationVariables: operationVariables,
                  ruleOperationOutputTypes: operationOutputTypes,
                  ruleProjectError: '',
                },
              }
              : x));
            setBaseline({ nodes: nextNodes, edges, agentsMeta });
            return nextNodes;
          });
        } catch (err) {
          if (cancelled) return;
          setNodes(nds => {
            const nextNodes = nds.map(x => (x.id === n.id
              ? { ...x, data: { ...x.data, ruleProjectError: `Cannot load project ${path}: ${err.message}` } }
              : x));
            setBaseline({ nodes: nextNodes, edges, agentsMeta });
            return nextNodes;
          });
        }
      })();
    }
    return () => { cancelled = true; };
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  const bodyRef = useRef(null);
  const splitterDragging = useRef(false);
  const { screenToFlowPosition, zoomIn, zoomOut, fitView, getZoom, getNodes } = useReactFlow();

  const deleteSelectedElement = useCallback(() => {
    if (selectedNode) {
      const removedId = selectedNode.id;
      const currentNodes = presentRef.current.nodes;
      const currentEdges = presentRef.current.edges;
      const currentMeta = presentRef.current.agentsMeta;
      const nextNodes = currentNodes.filter(n => n.id !== removedId);
      const nextEdges = currentEdges.filter(e => e.source !== removedId && e.target !== removedId);
      pushSnapshot(nextNodes, nextEdges, currentMeta);
      setNodes(nextNodes);
      setEdges(nextEdges);
      setSelectedNode(null);
      setSelectedOp(null);
    } else if (selectedEdgeId) {
      const removedId = selectedEdgeId;
      const currentNodes = presentRef.current.nodes;
      const currentEdges = presentRef.current.edges;
      const currentMeta = presentRef.current.agentsMeta;
      const nextEdges = currentEdges.filter(e => e.id !== removedId);
      pushSnapshot(currentNodes, nextEdges, currentMeta);
      setEdges(nextEdges);
      setSelectedEdgeId(null);
    }
  }, [selectedNode, selectedEdgeId, presentRef, pushSnapshot, setNodes, setEdges]);

  // Global keyboard shortcuts: Ctrl/Cmd+Z (Undo), Ctrl/Cmd+Shift+Z or Ctrl/Cmd+Y (Redo), Delete/Backspace
  useEffect(() => {
    const handleKeyDown = (e) => {
      if (showClearModal) {
        if (e.key === 'Escape') {
          e.preventDefault();
          setShowClearModal(false);
        }
        return;
      }

      const target = e.target;
      const isInput = target && (
        target.tagName === 'INPUT' ||
        target.tagName === 'TEXTAREA' ||
        target.tagName === 'SELECT' ||
        target.isContentEditable
      );
      if (isInput) return;

      const isCtrlOrMeta = e.ctrlKey || e.metaKey;
      if (isCtrlOrMeta) {
        if (e.key === 'z' || e.key === 'Z') {
          if (e.shiftKey) {
            e.preventDefault();
            handleRedo();
          } else {
            e.preventDefault();
            handleUndo();
          }
        } else if (e.key === 'y' || e.key === 'Y') {
          e.preventDefault();
          handleRedo();
        }
      } else if (e.key === 'Delete' || e.key === 'Backspace') {
        if (selectedNode || selectedEdgeId) {
          e.preventDefault();
          deleteSelectedElement();
        }
      }
    };

    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [handleUndo, handleRedo, deleteSelectedElement, selectedNode, selectedEdgeId, showClearModal]);

  // Fit the diagram to the view once, when the editor opens and React Flow has measured every
  // node. The built-in `fitView` prop runs before the nodes are measured and leaves the viewport
  // at its default zoom.
  const nodesInitialized = useNodesInitialized();
  // An empty diagram has nothing to fit: nodes added later are placed by the user.
  const fittedOnOpen = useRef(!initial?.nodes?.length);
  useEffect(() => {
    if (!nodesInitialized || fittedOnOpen.current) return;
    fittedOnOpen.current = true;
    requestAnimationFrame(() => fitView({ padding: 0.2, minZoom: 0.1 }));
  }, [nodesInitialized, fitView]);

  // Zoom display polling
  const [zoomPct, setZoomPct] = useState(100);
  useEffect(() => {
    const id = setInterval(() => setZoomPct(Math.round((getZoom() || 1) * 100)), 250);
    return () => clearInterval(id);
  }, [getZoom]);

  // ── Keep selectedNode data fresh when nodes change (e.g. external update) ─
  useEffect(() => {
    if (!selectedNode) return;
    const fresh = nodes.find(n => n.id === selectedNode.id);
    if (!fresh) { setSelectedNode(null); return; }
    // Only update if data actually changed to avoid infinite loop
    if (JSON.stringify(fresh.data) !== JSON.stringify(selectedNode.data)) {
      setSelectedNode(fresh);
    }
  }, [nodes]); // eslint-disable-line react-hooks/exhaustive-deps

  // ── onConnect — validate and style edges ────────────────────────────────
  const onConnect = useCallback((params) => {
    const allNodes = getNodes();
    if (!isConnectionValid(params, allNodes)) return;
    const src = allNodes.find(n => n.id === params.source);
    const tgt = allNodes.find(n => n.id === params.target);
    // Operation → Outbound links carry the operation's outbound entry (field + eventType)
    const isOpOutbound = params.sourceHandle?.startsWith('op-out:');
    // Topic → Agent links carry the subscription (eventType, operation, horizon)
    const isSubscription = src?.type === 'topic';
    let data;
    if (isSubscription) {
      data = {
        eventType: src.data?.type || '',
        // '' = history only
        operation: params.targetHandle?.startsWith('op-in:') ? params.targetHandle.slice('op-in:'.length) : '',
        horizon: { type: 'count', value: 100, description: '' },
      };
    } else if (isOpOutbound) {
      data = { field: '', eventType: tgt?.data?.eventType || '' };
    }
    const newEdge = { ...decorateEdge(params, src?.type), data };
    const currentEdges = presentRef.current.edges;
    const currentNodes = presentRef.current.nodes;
    const currentMeta = presentRef.current.agentsMeta;
    const nextEdges = addEdge(newEdge, currentEdges);
    pushSnapshot(currentNodes, nextEdges, currentMeta);
    setEdges(nextEdges);
  }, [getNodes, pushSnapshot, setEdges, presentRef]);

  // ── Drop from palette ────────────────────────────────────────────────────
  const [isDragOver, setIsDragOver] = useState(false);

  const onDragOver = useCallback((e) => {
    e.preventDefault();
    e.dataTransfer.dropEffect = 'copy';
    setIsDragOver(true);
  }, []);

  const onDragLeave = useCallback(() => setIsDragOver(false), []);

  const onDrop = useCallback((e) => {
    e.preventDefault();
    setIsDragOver(false);
    const nodeType = e.dataTransfer.getData('application/ade-type');
    if (!nodeType) return;

    const position = screenToFlowPosition({ x: e.clientX, y: e.clientY });
    const id = nextId(nodeType);

    const defaultData = {
      topic:        { name: '', type: '', label: 'New Topic', schema: '', idleness: '' },
      agent:        newAgentData(uniqueAgentId('New Agent', presentRef.current.nodes)),
      entity:       { label: 'New Entity', entityType: '', topic: '' },
      outbound:     { label: 'New Outbound', topic: '', eventType: '' },
    }[nodeType] || { label: 'New Node' };

    const newNode = { id, type: nodeType, position, data: defaultData };
    const currentNodes = presentRef.current.nodes;
    const currentEdges = presentRef.current.edges;
    const currentMeta = presentRef.current.agentsMeta;
    const nextNodes = [...currentNodes, newNode];
    pushSnapshot(nextNodes, currentEdges, currentMeta);
    setNodes(nextNodes);
    setSelectedNode(newNode);
    setSelectedOp(null);
    setSelectedEdgeId(null);
  }, [screenToFlowPosition, pushSnapshot, setNodes, presentRef]);

  // ── Drop / pick a Decision Service project (zip) on the empty canvas ─────
  // Creates an Agent already linked to the project. The zip is uploaded when the diagram is saved.
  const canvasRef = useRef(null);
  const createAgentFromProject = useCallback(async (file, dropEvent) => {
    setIsDragOver(false);
    const rect = canvasRef.current?.getBoundingClientRect();
    const position = screenToFlowPosition(dropEvent
      ? { x: dropEvent.clientX, y: dropEvent.clientY }
      : { x: (rect?.left || 0) + (rect?.width || 0) / 2, y: (rect?.top || 0) + (rect?.height || 0) / 2 });
    const id = nextId('agent');
    const data = {
      ...newAgentData(uniqueAgentId(file.name.replace(/\.zip$/i, ''), presentRef.current.nodes), file.name.replace(/\.zip$/i, '')),
      ruleProject: `${PROJECT_DIR}/${file.name}`,
      ruleProjectFile: file,
    };
    try {
      const { operations, classes, attributes, operationVariables, operationOutputTypes } = await exploreDecisionServiceProject(file);
      Object.assign(data, {
        ruleOperations: operations,
        ruleClasses: classes,
        ruleAttributes: attributes,
        ruleOperationVariables: operationVariables,
        ruleOperationOutputTypes: operationOutputTypes,
        ruleProjectError: '',
      });
    } catch (err) {
      data.ruleProjectError = `Cannot read zip: ${err.message}`;
    }
    const newNode = { id, type: 'agent', position, data };
    const currentNodes = presentRef.current.nodes;
    const currentEdges = presentRef.current.edges;
    const currentMeta = presentRef.current.agentsMeta;
    const nextNodes = [...currentNodes, newNode];
    pushSnapshot(nextNodes, currentEdges, currentMeta);
    setNodes(nextNodes);
    setSelectedNode(newNode);
    setSelectedOp(null);
    setSelectedEdgeId(null);
  }, [screenToFlowPosition, pushSnapshot, setNodes, presentRef]);

  // ── Node / edge click ───────────────────────────────────────────────────
  // Clicking an operation sub node inside an Agent selects that operation.
  // Clicking an Operation → Outbound link selects that link.
  const onNodeClick = useCallback((e, node) => {
    setSelectedNode(node);
    setSelectedEdgeId(null);
    const opName = node.type === 'agent' ? e.target.closest?.('[data-op]')?.dataset.op : null;
    setSelectedOp(opName ? { nodeId: node.id, name: opName } : null);
  }, []);
  const onEdgeClick = useCallback((_e, edge) => {
    const isOutbound = edge.sourceHandle?.startsWith('op-out:');
    const isSubscription = nodes.find(n => n.id === edge.source)?.type === 'topic';
    if (!isOutbound && !isSubscription) return;
    setSelectedNode(null);
    setSelectedOp(null);
    setSelectedEdgeId(edge.id);
  }, [nodes]);
  const onPaneClick = useCallback(() => {
    setSelectedNode(null); setSelectedOp(null); setSelectedEdgeId(null);
  }, []);
  // Delete / Backspace removes the selected nodes / links (handled by React Flow);
  // drop the detail-panel selection that pointed at a deleted element.
  const onNodesDelete = useCallback((deleted) => {
    const ids = new Set(deleted.map(n => n.id));
    setSelectedNode(prev => (prev && ids.has(prev.id) ? null : prev));
    setSelectedOp(prev => (prev && ids.has(prev.nodeId) ? null : prev));
  }, []);
  const onEdgesDelete = useCallback((deleted) => {
    const ids = new Set(deleted.map(e => e.id));
    setSelectedEdgeId(prev => (prev && ids.has(prev) ? null : prev));
  }, []);

  // React Flow changes wrapper to record removals in history
  // Deleting a node automatically removes its incoming and outgoing links in the same snapshot.
  const onNodesChange = useCallback((changes) => {
    const hasRemoval = changes.some(c => c.type === 'remove');
    if (hasRemoval) {
      const removedIds = new Set(changes.filter(c => c.type === 'remove').map(c => c.id));
      const currentNodes = presentRef.current.nodes;
      const currentEdges = presentRef.current.edges;
      const currentMeta = presentRef.current.agentsMeta;
      const nextNodes = currentNodes.filter(n => !removedIds.has(n.id));
      const nextEdges = currentEdges.filter(e => !removedIds.has(e.source) && !removedIds.has(e.target));
      pushSnapshot(nextNodes, nextEdges, currentMeta);
    }
    onNodesChangeOriginal(changes);
  }, [pushSnapshot, onNodesChangeOriginal, presentRef]);

  const onEdgesChange = useCallback((changes) => {
    const hasRemoval = changes.some(c => c.type === 'remove');
    if (hasRemoval) {
      const removedIds = new Set(changes.filter(c => c.type === 'remove').map(c => c.id));
      const currentNodes = presentRef.current.nodes;
      const currentEdges = presentRef.current.edges;
      const currentMeta = presentRef.current.agentsMeta;
      const nextEdges = currentEdges.filter(e => !removedIds.has(e.id));
      pushSnapshot(currentNodes, nextEdges, currentMeta);
    }
    onEdgesChangeOriginal(changes);
  }, [pushSnapshot, onEdgesChangeOriginal, presentRef]);

  // Drag start/stop for node movement history tracking
  const onNodeDragStart = useCallback(() => {
    startDrag({ nodes, edges, agentsMeta });
  }, [nodes, edges, agentsMeta, startDrag]);

  const onNodeDragStop = useCallback(() => {
    stopDrag({ nodes, edges, agentsMeta });
  }, [nodes, edges, agentsMeta, stopDrag]);

  const selectedEdge = edges.find(e => e.id === selectedEdgeId) || null;
  const selectedEdgeSource = selectedEdge ? nodes.find(n => n.id === selectedEdge.source) : null;
  // Entity type of the Entity node linked to the selected agent (entity -> agent edge).
  const linkedEntityType = selectedNode?.type === 'agent'
    ? edges
        .filter(e => e.target === selectedNode.id)
        .map(e => nodes.find(n => n.id === e.source))
        .find(n => n?.type === 'entity')?.data?.entityType || ''
    : '';
  const selectedEdgeIsSubscription = selectedEdgeSource?.type === 'topic';
  const updateEdgeData = useCallback((edgeId, data) => {
    const nextEdges = edges.map(e => e.id === edgeId ? { ...e, data } : e);
    updateDebouncedSnapshot(nodes, nextEdges, agentsMeta);
    setEdges(nextEdges);
  }, [nodes, edges, agentsMeta, updateDebouncedSnapshot, setEdges]);
  const activeOperation =
    selectedNode && selectedOp?.nodeId === selectedNode.id &&
    selectedNode.data.ruleOperations?.includes(selectedOp.name)
      ? selectedOp.name
      : null;

  // Type / Field combo boxes are scoped to the agent(s) in context, each with its own
  // Decision Service Project: the selected agent, the agents a selected node is linked
  // to, or the agent of a selected link.
  const vocabOptions = useMemo(() => {
    const agentIds = new Set();
    if (selectedEdge) {
      // outbound link: source agent; subscription link: target agent (non-agents are filtered out below)
      agentIds.add(selectedEdge.source);
      agentIds.add(selectedEdge.target);
    } else if (selectedNode?.type === 'agent') {
      agentIds.add(selectedNode.id);
    } else if (selectedNode) {
      for (const e of edges) {
        if (e.source === selectedNode.id) agentIds.add(e.target);
        if (e.target === selectedNode.id) agentIds.add(e.source);
      }
    }
    return buildVocabOptions(
      nodes.filter(n => n.type === 'agent' && agentIds.has(n.id)).map(n => n.data),
    );
  }, [nodes, edges, selectedNode, selectedEdge]);

  // ── Detail editor update ────────────────────────────────────────────────
  const handleDetailUpdate = useCallback((nodeId, newData) => {
    const nextNodes = nodes.map(n => n.id === nodeId ? { ...n, data: newData } : n);
    // Drop edges attached to operation sub nodes that no longer exist
    const ops = new Set(newData.ruleOperations || []);
    const nextEdges = edges.filter(e => {
      if (e.target === nodeId && e.targetHandle?.startsWith('op-in:')) {
        return ops.has(e.targetHandle.slice('op-in:'.length));
      }
      if (e.source === nodeId && e.sourceHandle?.startsWith('op-out:')) {
        return ops.has(e.sourceHandle.slice('op-out:'.length));
      }
      return true;
    });

    updateDebouncedSnapshot(nextNodes, nextEdges, agentsMeta);
    setNodes(nextNodes);
    setEdges(nextEdges);
    setSelectedNode(prev => prev?.id === nodeId ? { ...prev, data: newData } : prev);
  }, [nodes, edges, agentsMeta, updateDebouncedSnapshot, setNodes, setEdges]);

  // Renaming an operation moves its configuration and its links to the new name.
  const handleRenameOperation = useCallback((nodeId, from, to) => {
    if (from === to) return;
    const clash = nodes.find(n => n.id === nodeId)?.data.ruleOperations?.includes(to);
    if (clash) return;

    const nextNodes = nodes.map(n => {
      if (n.id !== nodeId) return n;
      const { [from]: cfg, ...rest } = n.data.operationConfigs || {};
      const { [from]: vars, ...restVars } = n.data.ruleOperationVariables || {};
      const { [from]: outType, ...restTypes } = n.data.ruleOperationOutputTypes || {};
      return {
        ...n,
        data: {
          ...n.data,
          ruleOperations: n.data.ruleOperations.map(o => (o === from ? to : o)),
          operationConfigs: { ...rest, [to]: cfg || { ...EMPTY_OPERATION } },
          ...(vars && { ruleOperationVariables: { ...restVars, [to]: vars } }),
          ...(outType && { ruleOperationOutputTypes: { ...restTypes, [to]: outType } }),
        },
      };
    });

    const nextEdges = edges.map(e => {
      if (e.target === nodeId && e.targetHandle === opInHandle(from)) {
        return { ...e, targetHandle: opInHandle(to), data: { ...e.data, operation: to } };
      }
      if (e.source === nodeId && e.sourceHandle === opOutHandle(from)) {
        return { ...e, sourceHandle: opOutHandle(to) };
      }
      return e;
    });

    pushSnapshot(nextNodes, nextEdges, agentsMeta);
    setNodes(nextNodes);
    setEdges(nextEdges);
    setSelectedOp({ nodeId, name: to });
  }, [nodes, edges, agentsMeta, pushSnapshot, setNodes, setEdges]);

  // Top-level metadata update wrapper with debounced history
  const handleMetaUpdate = useCallback((newMetaOrUpdater) => {
    setAgentsMeta(prev => {
      const nextMeta = typeof newMetaOrUpdater === 'function' ? newMetaOrUpdater(prev) : newMetaOrUpdater;
      updateDebouncedSnapshot(nodes, edges, nextMeta);
      return nextMeta;
    });
  }, [nodes, edges, updateDebouncedSnapshot]);

  // Reference names offered by the enrichment editors: those of the agent(s) in context
  const referenceNames = useMemo(() => {
    const ids = new Set();
    if (selectedEdge) { ids.add(selectedEdge.source); ids.add(selectedEdge.target); }
    else if (selectedNode) {
      ids.add(selectedNode.id);
      for (const e of edges) {
        if (e.source === selectedNode.id) ids.add(e.target);
        if (e.target === selectedNode.id) ids.add(e.source);
      }
    }
    const names = [];
    for (const n of nodes) {
      if (n.type === 'agent' && ids.has(n.id)) {
        for (const r of n.data.references || []) if (r.name && !names.includes(r.name)) names.push(r.name);
      }
    }
    return names;
  }, [nodes, edges, selectedNode, selectedEdge]);

  const problems = useMemo(
    () => validateDescriptor(buildDiagramDescriptor(agentsMeta, nodes, edges)),
    [agentsMeta, nodes, edges],
  );
  const [showProblems, setShowProblems] = useState(false);

  // ── Splitter resize ──────────────────────────────────────────────────────
  const onSplitterMouseDown = useCallback((e) => {
    e.preventDefault();
    splitterDragging.current = true;
    document.body.style.cursor = 'col-resize';
    document.body.style.userSelect = 'none';

    const onMove = (mv) => {
      if (!splitterDragging.current || !bodyRef.current) return;
      const rect = bodyRef.current.getBoundingClientRect();
      setDetailWidth(Math.max(200, Math.min(rect.right - mv.clientX, rect.width - 220)));
    };
    const onUp = () => {
      splitterDragging.current = false;
      document.body.style.cursor = '';
      document.body.style.userSelect = '';
      document.removeEventListener('mousemove', onMove);
      document.removeEventListener('mouseup', onUp);
    };
    document.addEventListener('mousemove', onMove);
    document.addEventListener('mouseup', onUp);
  }, []);

  // ── Auto-layout: column-based by node type ───────────────────────────────
  const handleAutoLayout = useCallback(() => {
    const layouted = layoutNodes(nodes, edges);
    pushSnapshot(layouted, edges, agentsMeta);
    setNodes(layouted);
    setTimeout(() => fitView({ padding: 0.3, duration: 300 }), 50);
  }, [nodes, edges, agentsMeta, pushSnapshot, setNodes, fitView]);

  const handleClear = () => {
    setShowClearModal(true);
  };

  const handleConfirmClear = () => {
    pushSnapshot([], [], agentsMeta);
    setNodes([]);
    setEdges([]);
    setSelectedNode(null);
    setSelectedOp(null);
    setSelectedEdgeId(null);
    setShowClearModal(false);
  };

  const handleExport = () => {
    const blob = new Blob([JSON.stringify(buildDiagramDescriptor(agentsMeta, nodes, edges), null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'agents.json';
    a.click();
    URL.revokeObjectURL(url);
  };

  const handleSave = useCallback(async () => {
    const useCaseId = agentsMeta.folderName?.trim();
    if (!useCaseId) return;

    setSaveState('saving');
    setSaveError(null);

    try {
      // Upload each pending Decision Service project zip to <application>/di-project first;
      // the path returned by the BFF becomes the agent's ruleset.project.
      const uploaded = new Map(); // nodeId -> stored path
      for (const n of nodes) {
        const file = n.type === 'agent' ? n.data.ruleProjectFile : null;
        if (!file) continue;
        const fd = new FormData();
        fd.append('project', file);
        const up = await fetch(`${API_URL}/api/agents/${encodeURIComponent(useCaseId)}/project`, { method: 'POST', body: fd });
        if (!up.ok) {
          const body = await up.json().catch(() => ({}));
          throw new Error(`Decision Service project upload failed: ${body.error || `HTTP ${up.status}`}`);
        }
        uploaded.set(n.id, (await up.json()).path);
      }
      // Same for the reference CSV files picked from disk (stored in <application>/enrichment).
      const uploadedRefs = new Map(); // `${nodeId}:${index}` -> stored path
      for (const n of nodes) {
        if (n.type !== 'agent') continue;
        const refs = n.data.references || [];
        for (let i = 0; i < refs.length; i++) {
          if (!refs[i].pendingFile) continue;
          const fd = new FormData();
          fd.append('reference', refs[i].pendingFile);
          const up = await fetch(`${API_URL}/api/agents/${encodeURIComponent(useCaseId)}/reference`, { method: 'POST', body: fd });
          if (!up.ok) {
            const body = await up.json().catch(() => ({}));
            throw new Error(`Reference CSV upload failed: ${body.error || `HTTP ${up.status}`}`);
          }
          uploadedRefs.set(`${n.id}:${i}`, (await up.json()).path);
        }
      }
      const savedNodes = (uploaded.size === 0 && uploadedRefs.size === 0) ? nodes : nodes.map(n => {
        let data = n.data;
        if (uploaded.has(n.id)) {
          data = { ...data, ruleProject: uploaded.get(n.id), ruleProjectFile: undefined };
        }
        if (n.type === 'agent' && (data.references || []).some((_, i) => uploadedRefs.has(`${n.id}:${i}`))) {
          data = {
            ...data,
            references: data.references.map((r, i) => (
              uploadedRefs.has(`${n.id}:${i}`) ? { ...r, file: uploadedRefs.get(`${n.id}:${i}`), pendingFile: undefined } : r
            )),
          };
        }
        return data === n.data ? n : { ...n, data };
      });
      if (savedNodes !== nodes) {
        setNodes(savedNodes);
        setBaseline({ nodes: savedNodes, edges, agentsMeta });
      }

      const descriptor = buildDiagramDescriptor(agentsMeta, savedNodes, edges);

      const res = await fetch(`${API_URL}/api/agents`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          useCaseId,
          descriptor,
          mode: 'replace',
        }),
      });

      if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        throw new Error(body.error || body.detail || `HTTP ${res.status}`);
      }

      setSaveState('saved');
      clearTimeout(saveResetTimer.current);
      saveResetTimer.current = setTimeout(() => {
        setSaveState(s => (s === 'saved' ? 'idle' : s));
      }, 2500);
    } catch (err) {
      console.error('Failed to save agent descriptor:', err);
      setSaveState('error');
      setSaveError(err.message);
    }
  }, [agentsMeta, nodes, edges, setNodes, setBaseline]);

  return (
    <div className="ade-root">
      {/* ── Header — reuses di-header dark shell from UseCaseSelector.css ──── */}
      <header className="di-header">
        <div className="di-header-left">
          <div className="di-header-brand">
            <span className="di-header-brand-ibm">IBM</span>
            <span className="di-header-brand-title">Decision Intelligence - Event Rules (Tech Asset)</span>
          </div>
          <div className="di-header-brand">
            <span className="ade-header-sep">/</span>
            <span className="ade-header-page">Agent Diagram Editor</span>
          </div>
        </div>
        <div className="di-header-right">
          {problems.length > 0 && (
            <button
              className="ade-header-btn ade-header-btn--warn"
              onClick={() => setShowProblems(v => !v)}
              title="Descriptor problems the backend would reject"
            >
              ⚠ {problems.length} {problems.length === 1 ? 'problem' : 'problems'}
            </button>
          )}
          {saveError && (
            <span className="ade-header-error" title={saveError}>
              ⚠ {saveError}
            </span>
          )}
          <button
            className="ade-header-btn"
            onClick={handleExport}
            title="Download agents.json"
          >
            <svg width="14" height="14" viewBox="0 0 32 32" fill="currentColor">
              <path d="M26 24v4H6v-4H4v4a2 2 0 002 2h20a2 2 0 002-2v-4zm0-10l-1.41-1.41L17 20.17V2h-2v18.17l-7.59-7.58L6 14l10 10 10-10z"/>
            </svg>
            Export JSON
          </button>
          {agentsMeta.folderName?.trim() && (
            <button
              className={`ade-header-btn ade-header-btn--save${saveState === 'saved' ? ' ade-header-btn--saved' : ''}${saveState === 'error' ? ' ade-header-btn--error' : ''}`}
              onClick={handleSave}
              disabled={saveState === 'saving'}
              title="Save agents.json to backend"
            >
              {saveState === 'saving' ? (
                'Saving…'
              ) : saveState === 'saved' ? (
                '✓ Saved'
              ) : (
                <>
                  <svg width="14" height="14" viewBox="0 0 32 32" fill="currentColor">
                    <path d="M27.71 9.29l-5-5A1 1 0 0022 4H6a2 2 0 00-2 2v20a2 2 0 002 2h20a2 2 0 002-2V10a1 1 0 00-.29-.71zM12 6h8v4h-8zm8 20H12v-8h8zm2 0v-8a2 2 0 00-2-2H12a2 2 0 00-2 2v8H6V6h4v4a2 2 0 002 2h8a2 2 0 002-2V6.41l4 4V26z"/>
                  </svg>
                  Save
                </>
              )}
            </button>
          )}
          <button className="ade-header-btn ade-header-btn--primary" onClick={onClose}>
            ✕ Close Editor
          </button>
        </div>
      </header>

      {showProblems && problems.length > 0 && (
        <ul className="ade-problems" aria-label="Descriptor problems">
          {problems.map((p, i) => <li key={i}>{p}</li>)}
        </ul>
      )}

      {/* ── Toolbar — 48 px white bar, mirrors di-toolbar-bar ─────────────── */}
      <div className="ade-toolbar">
        <button className="ade-tool-btn" onClick={() => zoomOut()} title="Zoom out">
          <svg width="14" height="14" viewBox="0 0 32 32" fill="currentColor">
            <path d="M22.45 21A10.87 10.87 0 0025 14 11 11 0 1014 25a10.87 10.87 0 007-2.55L27 28.4 28.4 27zm-8.45 2a9 9 0 119-9 9 9 0 01-9 9zM9 13h10v2H9z"/>
          </svg>
        </button>
        <span className="ade-zoom-label">{zoomPct}%</span>
        <button className="ade-tool-btn" onClick={() => zoomIn()} title="Zoom in">
          <svg width="14" height="14" viewBox="0 0 32 32" fill="currentColor">
            <path d="M22.45 21A10.87 10.87 0 0025 14 11 11 0 1014 25a10.87 10.87 0 007-2.55L27 28.4 28.4 27zm-8.45 2a9 9 0 119-9 9 9 0 01-9 9zm8-10h-5V8h-2v5H9v2h5v5h2v-5h5z"/>
          </svg>
        </button>

        <div className="ade-toolbar-sep" />

        <button
          className="ade-tool-btn"
          onClick={() => fitView({ padding: 0.3, duration: 300 })}
          title="Fit view to content"
        >
          <svg width="14" height="14" viewBox="0 0 32 32" fill="currentColor">
            <path d="M28 4v6h-2V6h-4V4h6zM4 4h6v2H6v4H4V4zm24 18h-2v4h-4v2h6v-6zM4 22v6h6v-2H6v-4H4z"/>
          </svg>
          Fit view
        </button>

        <button className="ade-tool-btn" onClick={handleAutoLayout} title="Auto-arrange nodes into type columns">
          <svg width="14" height="14" viewBox="0 0 32 32" fill="currentColor">
            <path d="M4 6h6v20H4zm9 0h6v8h-6zm9 0h6v14h-6zM13 20h6v6h-6z"/>
          </svg>
          Auto layout
        </button>

        <div className="ade-toolbar-sep" />

        <button
          className="ade-tool-btn"
          onClick={handleUndo}
          disabled={!canUndo}
          title="Undo (Ctrl+Z / ⌘Z)"
          aria-label="Undo"
        >
          <svg width="14" height="14" viewBox="0 0 32 32" fill="currentColor">
            <path d="M14 26h10a6 6 0 000-12H8.83l4.58-4.59L12 8l-8 8 8 8 1.41-1.41L8.83 18H24a4 4 0 010 8H14z"/>
          </svg>
          Undo
        </button>

        <button
          className="ade-tool-btn"
          onClick={handleRedo}
          disabled={!canRedo}
          title="Redo (Ctrl+Y / ⌘⇧Z)"
          aria-label="Redo"
        >
          <svg width="14" height="14" viewBox="0 0 32 32" fill="currentColor">
            <path d="M18 26h-4a4 4 0 010-8h15.17l-4.58 4.59L20 24l8-8-8-8-1.41 1.41L23.17 14H14a6 6 0 000 12h4z"/>
          </svg>
          Redo
        </button>

        <div className="ade-toolbar-sep" />

        <button className="ade-tool-btn ade-tool-btn--danger" onClick={handleClear} title="Clear diagram">
          <svg width="14" height="14" viewBox="0 0 32 32" fill="currentColor">
            <path d="M12 12h2v12h-2zm6 0h2v12h-2zM4 6v2h2l2 18a2 2 0 002 2h12a2 2 0 002-2l2-18h2V6zm5.88 20L8.06 8h15.88l-1.82 18H9.88zM12 4h8v2h-8z"/>
          </svg>
          Clear
        </button>
      </div>

      {/* ── Body ──────────────────────────────────────────────────────────── */}
      <div className="ade-body" ref={bodyRef}>

        {/* ── Palette ───────────────────────────────────────────────────── */}
        <div className="ade-palette">
          <div className="ade-palette-title">Palette</div>
          <div className="ade-palette-section">
            <div className="ade-palette-section-label">Nodes</div>
            {PALETTE_NODES.map(item => (
              <PaletteItem key={item.type} {...item} />
            ))}
          </div>
        </div>

        {/* ── Canvas ────────────────────────────────────────────────────── */}
        <div
          ref={canvasRef}
          className={`ade-canvas-area${isDragOver ? ' ade-canvas-drop-active' : ''}`}
          onDragOver={onDragOver}
          onDragLeave={onDragLeave}
          onDrop={onDrop}
        >
          <SelectedOperationContext.Provider value={selectedOp}>
          <ReactFlow
            nodes={nodes}
            edges={edges}
            nodeTypes={NODE_TYPES}
            edgeTypes={EDGE_TYPES}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            onNodeDragStart={onNodeDragStart}
            onNodeDragStop={onNodeDragStop}
            onSelectionDragStart={onNodeDragStart}
            onSelectionDragStop={onNodeDragStop}
            onConnect={onConnect}
            onNodeClick={onNodeClick}
            onEdgeClick={onEdgeClick}
            onPaneClick={onPaneClick}
            isValidConnection={(conn) => isConnectionValid(conn, nodes)}
            minZoom={0.1}
            deleteKeyCode={null}
            onNodesDelete={onNodesDelete}
            onEdgesDelete={onEdgesDelete}
            multiSelectionKeyCode="Shift"
            connectionLineStyle={{ stroke: 'var(--accent, #2563eb)', strokeWidth: 2 }}
            defaultEdgeOptions={{ type: 'labeled' }}
            proOptions={{ hideAttribution: true }}
          >
            <Background variant={BackgroundVariant.Dots} gap={20} size={1} color="#e0e0e0" />
            <Controls showFitView={false} showZoom={false} showInteractive={false} />
          </ReactFlow>
          </SelectedOperationContext.Provider>
          {nodes.length === 0 && <EmptyCanvasHint onProjectFile={createAgentFromProject} />}
        </div>

        {/* ── Splitter ──────────────────────────────────────────────────── */}
        <div
          className="ade-splitter"
          onMouseDown={onSplitterMouseDown}
          title="Drag to resize detail panel"
        />

        {/* ── Detail Panel ──────────────────────────────────────────────── */}
        <div className="ade-detail" style={{ width: detailWidth }}>
          <div className="ade-detail-header">
            <span className="ade-detail-title">
              {selectedEdge
                ? (selectedEdgeIsSubscription ? 'Subscription' : 'Output')
                : activeOperation
                  ? 'Operation'
                  : selectedNode
                    ? (NODE_TYPES_META[selectedNode.type]?.label || 'Properties')
                    : 'Event Rule Application'}
            </span>
            {selectedNode ? (
              <>
                <span className={`ade-detail-type-badge ade-detail-type-badge--${selectedNode.type}`}>
                  {activeOperation ? 'operation' : selectedNode.type}
                </span>
                <button
                  className="ade-detail-delete"
                  onClick={deleteSelectedElement}
                  title={`Delete ${selectedNode.type} node`}
                  aria-label={`Delete ${selectedNode.type} node`}
                >
                  <svg width="12" height="12" viewBox="0 0 32 32" fill="currentColor">
                    <path d="M12 12h2v12h-2zm6 0h2v12h-2zM4 6v2h2l2 18a2 2 0 002 2h12a2 2 0 002-2l2-18h2V6zm5.88 20L8.06 8h15.88l-1.82 18H9.88zM12 4h8v2h-8z"/>
                  </svg>
                </button>
                <button
                  className="ade-detail-close"
                  onClick={() => { setSelectedNode(null); setSelectedOp(null); }}
                  title="Deselect node"
                >
                  ✕
                </button>
              </>
            ) : selectedEdge ? (
              <>
                <span className="ade-detail-type-badge ade-detail-type-badge--agent">link</span>
                <button
                  className="ade-detail-delete"
                  onClick={deleteSelectedElement}
                  title="Delete link"
                  aria-label="Delete link"
                >
                  <svg width="12" height="12" viewBox="0 0 32 32" fill="currentColor">
                    <path d="M12 12h2v12h-2zm6 0h2v12h-2zM4 6v2h2l2 18a2 2 0 002 2h12a2 2 0 002-2l2-18h2V6zm5.88 20L8.06 8h15.88l-1.82 18H9.88zM12 4h8v2h-8z"/>
                  </svg>
                </button>
                <button
                  className="ade-detail-close"
                  onClick={() => setSelectedEdgeId(null)}
                  title="Deselect link"
                >
                  ✕
                </button>
              </>
            ) : (
              <span className="ade-detail-type-badge" style={{ color: '#525252', background: '#f4f4f4', borderColor: '#e0e0e0' }}>
                agents.json
              </span>
            )}
          </div>
          <VocabContext.Provider value={vocabOptions}>
          <div className="ade-detail-body">
            {selectedEdge && !selectedNode ? (
              selectedEdgeIsSubscription ? (
                <SubscriptionLinkEditor
                  topicName={selectedEdgeSource?.data?.name || selectedEdgeSource?.data?.label || ''}
                  agentLabel={nodes.find(n => n.id === selectedEdge.target)?.data?.label || ''}
                  eventType={selectedEdgeSource?.data?.type || ''}
                  data={selectedEdge.data}
                  referenceNames={referenceNames}
                  onChange={d => updateEdgeData(selectedEdge.id, d)}
                />
              ) : (
                <OutboundLinkEditor
                  operation={selectedEdge.sourceHandle.slice('op-out:'.length)}
                  outboundLabel={nodes.find(n => n.id === selectedEdge.target)?.data?.label || ''}
                  eventType={nodes.find(n => n.id === selectedEdge.target)?.data?.eventType || ''}
                  data={selectedEdge.data}
                  onChange={d => updateEdgeData(selectedEdge.id, d)}
                />
              )
            ) : (
              <DetailEditor
                node={selectedNode}
                onUpdate={handleDetailUpdate}
                agentsMeta={agentsMeta}
                onMetaUpdate={handleMetaUpdate}
                selectedOperation={activeOperation}
                onRenameOperation={handleRenameOperation}
                referenceNames={referenceNames}
                linkedEntityType={linkedEntityType}
              />
            )}
          </div>
          </VocabContext.Provider>
        </div>

      </div>

      {/* ── Carbon Confirmation Modal for Clear ─────────────────────────────── */}
      {showClearModal && (
        <div
          className="ade-modal-overlay"
          onClick={(e) => {
            if (e.target === e.currentTarget) setShowClearModal(false);
          }}
        >
          <div
            className="ade-confirm-modal"
            role="dialog"
            aria-modal="true"
            aria-labelledby="ade-confirm-title"
            aria-describedby="ade-confirm-body"
          >
            <div className="ade-confirm-header">
              <span className="ade-confirm-label">Diagram Editor</span>
              <h3 id="ade-confirm-title" className="ade-confirm-title">Clear diagram</h3>
              <button
                type="button"
                className="ade-confirm-close"
                onClick={() => setShowClearModal(false)}
                aria-label="Close"
                title="Close"
              >
                <svg width="16" height="16" viewBox="0 0 32 32" fill="currentColor">
                  <path d="M24 9.4L22.6 8 16 14.6 9.4 8 8 9.4l6.6 6.6L8 22.6 9.4 24l6.6-6.6 6.6 6.6 1.4-1.4-6.6-6.6L24 9.4z" />
                </svg>
              </button>
            </div>

            <div className="ade-confirm-body" id="ade-confirm-body">
              <p className="ade-confirm-text">
                Are you sure you want to clear all nodes and edges from the diagram? This action can be undone using the Undo button.
              </p>
            </div>

            <div className="ade-confirm-footer">
              <button
                type="button"
                className="ade-confirm-btn ade-confirm-btn--secondary"
                onClick={() => setShowClearModal(false)}
              >
                Cancel
              </button>
              <button
                type="button"
                className="ade-confirm-btn ade-confirm-btn--danger"
                onClick={handleConfirmClear}
              >
                <span>Clear diagram</span>
                <svg width="16" height="16" viewBox="0 0 32 32" fill="currentColor">
                  <path d="M12 12h2v12h-2zm6 0h2v12h-2zM4 6v2h2l2 18a2 2 0 002 2h12a2 2 0 002-2l2-18h2V6zm5.88 20L8.06 8h15.88l-1.82 18H9.88zM12 4h8v2h-8z" />
                </svg>
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

// ─── Public export — wraps InnerEditor in the required ReactFlowProvider ────
export default function AgentDiagramEditor({ onClose, initialFolderName, initialDescriptor }) {
  return (
    <ReactFlowProvider>
      <InnerEditor onClose={onClose} initialFolderName={initialFolderName} initialDescriptor={initialDescriptor} />
    </ReactFlowProvider>
  );
}
