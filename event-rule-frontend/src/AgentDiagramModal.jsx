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
import React, { useEffect, useRef, useState, useCallback } from 'react';
import './AgentDiagramModal.css';
import { API_URL } from './config.js';

// ─── Layout constants ──────────────────────────────────────────────────────────
const AGENT_W      = 200;
const AGENT_H      = 62;  // fixed height — name + id only

const CYL_W        = 220;
const CYL_H        = 50;
const CYL_RY       = 10;  // half-height of the ellipse caps on horizontal cylinders

const VERT_CYL_W   = 80;
const VERT_CYL_H   = 70;
const VERT_CYL_RX  = 18; // half-width of the ellipse caps on vertical cylinders

const RE_BASE_H    = 54;  // rule-engine box base height (header + JAR line)
const RE_OP_H      = 13;  // extra height per operation

const COL_GAP      = 60;  // horizontal gap between columns
const ROW_GAP      = 20;  // vertical gap between items in a column
const AGENT_RE_GAP = 30;  // gap between entity↔agent and agent↔rule-engine boxes

// ─── Colour palette (mirrors App.css tokens) ──────────────────────────────────
const C = {
  entity:        { fill: '#dbeafe', stroke: '#1d4ed8' }, // deep blue
  agent:         { fill: '#eff6ff', stroke: '#2563eb' }, // accent blue
  ruleEngine:    { fill: '#e0f2fe', stroke: '#0369a1' }, // sky blue
  inbound:       { fill: '#eef2ff', stroke: '#4338ca' }, // indigo
  outbound:      { fill: '#f0f9ff', stroke: '#0284c7' }, // light sky
  text:          '#1f2328',
  textMuted:     '#57606a',
  textFaint:     '#9ca3af',
  connector:     '#93c5fd',
  connectorDown: '#60a5fa',
};

// ─── Helpers ───────────────────────────────────────────────────────────────────

/** Break a long string into lines of at most `maxChars` characters. */
function wrapText(text, maxChars) {
  if (!text) return [];
  const words = String(text).split(' ');
  const lines = [];
  let current = '';
  for (const w of words) {
    if (current.length + w.length + 1 > maxChars && current) {
      lines.push(current);
      current = w;
    } else {
      current = current ? `${current} ${w}` : w;
    }
  }
  if (current) lines.push(current);
  return lines;
}

/** Shorten a path to just its filename. */
function basename(p) {
  if (!p) return '';
  return String(p).replace(/.*[/\\]/, '');
}

// ─── SVG primitives ────────────────────────────────────────────────────────────

/** Horizontal cylinder (for inbound subscriptions and outbound topics). */
function HorizCylinder({ x, y, w, h, ry, fill, stroke, children }) {
  const rx = ry; // same rx for caps
  return (
    <g>
      {/* Body rectangle */}
      <rect x={x + rx} y={y} width={w - 2 * rx} height={h} fill={fill} stroke={stroke} strokeWidth={1.2} />
      {/* Left cap ellipse */}
      <ellipse cx={x + rx} cy={y + h / 2} rx={rx} ry={h / 2} fill={fill} stroke={stroke} strokeWidth={1.2} />
      {/* Right cap ellipse — drawn last so it overlaps the rect edge cleanly */}
      <ellipse cx={x + w - rx} cy={y + h / 2} rx={rx} ry={h / 2} fill={fill} stroke={stroke} strokeWidth={1.2} />
      {children}
    </g>
  );
}

/** Vertical cylinder (for the Entity). */
function VertCylinder({ x, y, w, h, rx, fill, stroke, children }) {
  const ry = rx * 0.4; // flatten the ellipse to look like a cylinder top
  return (
    <g>
      {/* Body rectangle */}
      <rect x={x} y={y + ry} width={w} height={h - 2 * ry} fill={fill} stroke={stroke} strokeWidth={1.2} />
      {/* Bottom cap */}
      <ellipse cx={x + w / 2} cy={y + h - ry} rx={w / 2} ry={ry} fill={fill} stroke={stroke} strokeWidth={1.2} />
      {/* Top cap — drawn after bottom so it appears on top */}
      <ellipse cx={x + w / 2} cy={y + ry} rx={w / 2} ry={ry} fill={fill} stroke={stroke} strokeWidth={1.2} />
      {children}
    </g>
  );
}

/** Single connector line. dashed=true for horizontal subscriptions/outbounds. */
function Connector({ x1, y1, x2, y2, color = C.connector, dashed = true }) {
  return (
    <line
      x1={x1} y1={y1} x2={x2} y2={y2}
      stroke={color}
      strokeWidth={1.4}
      strokeDasharray={dashed ? '4 3' : undefined}
      markerEnd={dashed ? 'url(#arrow)' : 'url(#arrow-blue)'}
    />
  );
}

// ─── Clickable wrapper for SVG groups ─────────────────────────────────────────
// Renders a transparent interactive rect on top of the real shape so we get
// consistent hover/click feedback without touching the shape itself.
function ClickableNode({ x, y, w, h, onClick, label }) {
  return (
    <rect
      x={x} y={y} width={w} height={h}
      fill="transparent"
      stroke="transparent"
      style={{ cursor: 'pointer' }}
      rx={6} ry={6}
      onClick={onClick}
      aria-label={label}
      role="button"
      tabIndex={0}
      onKeyDown={e => (e.key === 'Enter' || e.key === ' ') && onClick(e)}
    />
  );
}

// ─── Agent box (name + id only) ────────────────────────────────────────────────
function AgentBox({ x, y, w, h, agent, onClick }) {
  const { agentId, name } = agent;
  const pad = 10;
  return (
    <g>
      <rect x={x} y={y} width={w} height={h} rx={6} ry={6}
        fill={C.agent.fill} stroke={C.agent.stroke} strokeWidth={1.5} />

      {/* Header bar */}
      <rect x={x} y={y} width={w} height={22} rx={6} ry={6}
        fill={C.agent.stroke} />
      <rect x={x} y={y + 16} width={w} height={6} fill={C.agent.stroke} />

      <text x={x + w / 2} y={y + 14} textAnchor="middle"
        fill="#fff" fontSize={9} fontWeight="700" letterSpacing="0.04em">
        AGENT
      </text>

      {/* Name */}
      <text x={x + pad} y={y + 34} fill={C.text} fontSize={9} fontWeight="700">
        {name || agentId}
      </text>

      {/* ID */}
      <text x={x + pad} y={y + 48} fill={C.textFaint} fontSize={8}
        fontFamily="'SF Mono','Fira Code',Consolas,monospace">
        {agentId}
      </text>

      {/* Invisible click target over the whole box */}
      {onClick && (
        <ClickableNode
          x={x} y={y} w={w} h={h}
          label={`Inspect agent: ${name || agentId}`}
          onClick={onClick}
        />
      )}
    </g>
  );
}

// ─── Rule Engine box ───────────────────────────────────────────────────────────
function RuleEngineBox({ x, y, w, h, ruleset }) {
  const jar  = basename(ruleset?.path);
  const ops  = ruleset?.operations?.map(o => o.operation) ?? [];
  const pad  = 10;
  const lineH = RE_OP_H;

  return (
    <g>
      <rect x={x} y={y} width={w} height={h} rx={6} ry={6}
        fill={C.ruleEngine.fill} stroke={C.ruleEngine.stroke} strokeWidth={1.5} />

      {/* Header bar */}
      <rect x={x} y={y} width={w} height={20} rx={6} ry={6}
        fill={C.ruleEngine.stroke} />
      <rect x={x} y={y + 14} width={w} height={6} fill={C.ruleEngine.stroke} />

      <text x={x + w / 2} y={y + 13} textAnchor="middle"
        fill="#fff" fontSize={8} fontWeight="700" letterSpacing="0.05em">
        DI RULE ENGINE
      </text>

      {/* JAR */}
      {jar && (
        <text x={x + pad} y={y + 31} fill={C.textMuted} fontSize={7.5}>
          {jar}
        </text>
      )}

      {/* Operations label */}
      {ops.length > 0 && (
        <text x={x + pad} y={y + (jar ? 43 : 31)} fill={C.textMuted} fontSize={7}
          fontWeight="700" letterSpacing="0.05em">
          OPERATIONS
        </text>
      )}

      {/* Operations list */}
      {ops.map((op, i) => {
        const baseY = jar ? 43 : 31;
        return (
          <text key={op} x={x + pad} y={y + baseY + 10 + i * lineH}
            fill={C.text} fontSize={8}>
            {'• '}{op}
          </text>
        );
      })}
    </g>
  );
}

// ─── Main diagram layout function ─────────────────────────────────────────────
function buildLayout(agents) {
  // We render one row per agent, stacking vertically.
  const nodes   = [];  // { type, x, y, w, h, agent?, data? }
  const edges   = [];  // { x1, y1, x2, y2 }
  const MARGIN  = 20;

  let cursorY = MARGIN;

  // Column x positions (left edges)
  const xInbound  = MARGIN;
  const xAgent    = xInbound + CYL_W + COL_GAP;
  const xOutbound = xAgent + AGENT_W + COL_GAP;

  // Entity is horizontally centred on the agent box
  const entityX = xAgent + (AGENT_W - VERT_CYL_W) / 2;

  for (const agent of agents) {
    const subs    = agent.subscriptions ?? [];
    const outputs = agent.outputs ?? [];
    const ops     = agent.ruleset?.operations ?? [];
    const hasJar  = !!agent.ruleset?.path;

    // Rule engine box height: header + JAR line (optional) + ops
    const reH = RE_BASE_H + (hasJar ? 0 : -12) + ops.length * RE_OP_H;

    // Centre column: entity (above) + agent + rule engine (below)
    const centerColH = VERT_CYL_H + AGENT_RE_GAP + AGENT_H + AGENT_RE_GAP + reH;

    // Left column (inbound subscriptions)
    const leftH = subs.length > 0 ? subs.length * (CYL_H + ROW_GAP) - ROW_GAP : 0;
    // Right column (outbound outputs)
    const rightH = outputs.length > 0 ? outputs.length * (CYL_H + ROW_GAP) - ROW_GAP : 0;
    const rowH   = Math.max(centerColH, leftH, rightH);

    // Centre column anchored at the top of the row
    const centerStartY = cursorY + Math.max(0, (rowH - centerColH) / 2);
    const entityY = centerStartY;
    const agentY  = entityY + VERT_CYL_H + AGENT_RE_GAP;
    const reY     = agentY + AGENT_H + AGENT_RE_GAP;

    const agentCX = xAgent + AGENT_W / 2;
    const agentCY = agentY + AGENT_H / 2;

    // ── Entity (above agent, centre column) ──
    nodes.push({
      type: 'entity',
      x: entityX, y: entityY, w: VERT_CYL_W, h: VERT_CYL_H,
      data: { entityType: agent.entityType },
      agent,   // carried so the click handler can derive useCaseId / agentId
    });
    // Solid downward connector: entity bottom → agent top
    edges.push({ x1: agentCX, y1: entityY + VERT_CYL_H, x2: agentCX, y2: agentY, dashed: false });

    // ── Agent box ──
    nodes.push({ type: 'agent',      x: xAgent, y: agentY, w: AGENT_W, h: AGENT_H, agent });

    // ── Rule engine box (below agent) ──
    nodes.push({ type: 'ruleEngine', x: xAgent, y: reY,    w: AGENT_W, h: reH, data: { ruleset: agent.ruleset } });
    // Solid downward connector: agent bottom → rule engine top
    edges.push({ x1: agentCX, y1: agentY + AGENT_H, x2: agentCX, y2: reY, dashed: false });

    // ── Inbound subscriptions (left column, centred vertically) ──
    const subTotalH = subs.length > 0 ? subs.length * (CYL_H + ROW_GAP) - ROW_GAP : 0;
    const subStartY = cursorY + (rowH - subTotalH) / 2;
    subs.forEach((sub, i) => {
      const sy = subStartY + i * (CYL_H + ROW_GAP);
      nodes.push({ type: 'inbound', x: xInbound, y: sy, w: CYL_W, h: CYL_H, data: sub });
      edges.push({ x1: xInbound + CYL_W, y1: sy + CYL_H / 2, x2: xAgent, y2: agentCY, dashed: true });
    });

    // ── Outbound outputs (right column, centred vertically) ──
    const outTotalH = outputs.length > 0 ? outputs.length * (CYL_H + ROW_GAP) - ROW_GAP : 0;
    const outStartY = cursorY + (rowH - outTotalH) / 2;
    outputs.forEach((out, i) => {
      const oy = outStartY + i * (CYL_H + ROW_GAP);
      nodes.push({ type: 'outbound', x: xOutbound, y: oy, w: CYL_W, h: CYL_H, data: out });
      edges.push({
        x1: xAgent + AGENT_W, y1: agentCY,
        x2: xOutbound, y2: oy + CYL_H / 2,
        dashed: true,
      });
    });

    cursorY += rowH + ROW_GAP * 2;
  }

  const totalW = xOutbound + CYL_W + MARGIN;
  const totalH = cursorY + MARGIN;
  return { nodes, edges, totalW, totalH };
}

// ─── Diagram SVG ──────────────────────────────────────────────────────────────
function AgentDiagramSVG({ agents, svgRef, onNodeClick }) {
  const { nodes, edges, totalW, totalH } = buildLayout(agents);

  return (
    <svg
      ref={svgRef}
      viewBox={`0 0 ${totalW} ${totalH}`}
      width={totalW}
      height={totalH}
      xmlns="http://www.w3.org/2000/svg"
      style={{ display: 'block', maxWidth: '100%' }}
    >
      <defs>
        <marker id="arrow" markerWidth="7" markerHeight="7" refX="5" refY="3.5" orient="auto">
          <path d="M0,0 L0,7 L7,3.5 z" fill={C.connector} />
        </marker>
        <marker id="arrow-blue" markerWidth="7" markerHeight="7" refX="5" refY="3.5" orient="auto">
          <path d="M0,0 L0,7 L7,3.5 z" fill={C.connectorDown} />
        </marker>
      </defs>

      {/* Edges drawn first so they appear behind nodes */}
      {edges.map((e, i) => (
        <Connector key={i} x1={e.x1} y1={e.y1} x2={e.x2} y2={e.y2}
          dashed={e.dashed !== false}
          color={e.dashed === false ? C.connectorDown : C.connector}
        />
      ))}

      {/* Nodes */}
      {nodes.map((n, i) => {
        if (n.type === 'agent') {
          return (
            <AgentBox key={i} x={n.x} y={n.y} w={n.w} h={n.h} agent={n.agent}
              onClick={() => onNodeClick && onNodeClick({ kind: 'agent', agent: n.agent })}
            />
          );
        }

        if (n.type === 'ruleEngine') {
          return (
            <RuleEngineBox key={i} x={n.x} y={n.y} w={n.w} h={n.h} ruleset={n.data.ruleset} />
          );
        }

        if (n.type === 'entity') {
          const { entityType } = n.data;
          const agentForNode   = n.agent;
          return (
            <g key={i}>
              <VertCylinder x={n.x} y={n.y} w={n.w} h={n.h} rx={VERT_CYL_RX}
                fill={C.entity.fill} stroke={C.entity.stroke}>
                <text x={n.x + n.w / 2} y={n.y + n.h / 2 - 4} textAnchor="middle"
                  fill={C.entity.stroke} fontSize={8} fontWeight="700" letterSpacing="0.05em">
                  ENTITY
                </text>
                <text x={n.x + n.w / 2} y={n.y + n.h / 2 + 8} textAnchor="middle"
                  fill={C.text} fontSize={9} fontWeight="600">
                  {entityType || '—'}
                </text>
              </VertCylinder>
              {/* Hover ring — shown as a subtle highlight around the cylinder shape */}
              <ellipse
                cx={n.x + n.w / 2} cy={n.y + VERT_CYL_RX * 0.4}
                rx={n.w / 2 + 3} ry={VERT_CYL_RX * 0.4 + 3}
                fill="none" stroke={C.entity.stroke} strokeWidth={1.5}
                strokeDasharray="3 3" opacity={0.5}
                style={{ pointerEvents: 'none' }}
              />
              <ClickableNode
                x={n.x - 3} y={n.y - 3} w={n.w + 6} h={n.h + 6}
                label={`Inspect entity state: ${entityType}`}
                onClick={() => onNodeClick && onNodeClick({
                  kind: 'entity',
                  agent: agentForNode,
                  agentId: agentForNode?.agentId,
                  entityType,
                })}
              />
            </g>
          );
        }

        if (n.type === 'inbound') {
          const { topic, eventType, operation, horizon } = n.data;
          const horizonStr = horizon
            ? (horizon.type === 'count'
                ? `${horizon.countLimit ?? horizon.value} events`
                : horizon.type === 'time'
                  ? horizon.timeLimit
                  : horizon.type === 'hybrid'
                    ? `${horizon.countLimit}ev / ${horizon.timeLimit}`
                    : horizon.type)
            : null;
          return (
            <HorizCylinder key={i} x={n.x} y={n.y} w={n.w} h={n.h} ry={CYL_RY}
              fill={C.inbound.fill} stroke={C.inbound.stroke}>
              <text x={n.x + n.w / 2} y={n.y + 12} textAnchor="middle"
                fill={C.inbound.stroke} fontSize={7} fontWeight="700" letterSpacing="0.06em">
                EVENT SUBSCRIPTION
              </text>
              <text x={n.x + n.w / 2} y={n.y + 22} textAnchor="middle"
                fill={C.text} fontSize={8} fontWeight="600">
                {topic || 'no Kafka topic'}
              </text>
              <text x={n.x + n.w / 2} y={n.y + 33} textAnchor="middle"
                fill={C.textMuted} fontSize={7.5}>
                {eventType}{operation ? ` · ${operation}` : ''}
              </text>
              {horizonStr && (
                <text x={n.x + n.w / 2} y={n.y + 44} textAnchor="middle"
                  fill={C.textFaint} fontSize={7}>
                  ⧖ {horizonStr}
                </text>
              )}
            </HorizCylinder>
          );
        }

        if (n.type === 'outbound') {
          const { topic, eventType, description } = n.data;
          const descLines = description ? wrapText(description, 22) : [];
          return (
            <HorizCylinder key={i} x={n.x} y={n.y} w={n.w} h={n.h} ry={CYL_RY}
              fill={C.outbound.fill} stroke={C.outbound.stroke}>
              <text x={n.x + n.w / 2} y={n.y + 12} textAnchor="middle"
                fill={C.outbound.stroke} fontSize={7} fontWeight="700" letterSpacing="0.06em">
                OUTPUT
              </text>
              <text x={n.x + n.w / 2} y={n.y + 22} textAnchor="middle"
                fill={C.text} fontSize={8} fontWeight="600">
                {topic}
              </text>
              <text x={n.x + n.w / 2} y={n.y + 33} textAnchor="middle"
                fill={C.textMuted} fontSize={7.5}>
                {eventType}
              </text>
              {descLines[0] && (
                <text x={n.x + n.w / 2} y={n.y + 44} textAnchor="middle"
                  fill={C.textFaint} fontSize={7}>
                  {descLines[0]}
                </text>
              )}
            </HorizCylinder>
          );
        }

        return null;
      })}
    </svg>
  );
}

// ─── Helpers ───────────────────────────────────────────────────────────────────

/**
 * Formats a HorizonConfig object from agents.json into a compact human-readable string.
 * e.g. { type: 'hybrid', countLimit: 100, timeLimit: '30d' } → "100 events / 30d"
 */
function formatHorizon(horizon) {
  if (!horizon) return null;
  const { type, countLimit, value, timeLimit } = horizon;
  if (type === 'count')  return `${countLimit ?? value ?? 0} events`;
  if (type === 'time')   return timeLimit || null;
  if (type === 'hybrid') return `${countLimit ?? value ?? 0} events / ${timeLimit || '?'}`;
  return type || null;
}

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
const ENTITY_POLL_MS = 3000;

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

// ─── Modal shell ──────────────────────────────────────────────────────────────
const ZOOM_MIN  = 0.25;
const ZOOM_MAX  = 3;
const ZOOM_STEP = 0.2;

export default function AgentDiagramModal({ agentsJson, useCaseId, onClose }) {
  const agents  = agentsJson?.agents ?? [];
  const [zoom, setZoom]     = useState(null); // null = not yet calculated
  const bodyRef             = useRef(null);
  const svgRef              = useRef(null);
  // Selection detail panel: null when hidden, { kind, agent, entityType? } when open
  const [selection, setSelection] = useState(null);

  const clampZoom = (z) => Math.min(ZOOM_MAX, Math.max(ZOOM_MIN, z));

  // Compute the zoom that makes the SVG fit inside the body with some padding.
  const calcFitZoom = useCallback(() => {
    const body = bodyRef.current;
    const svg  = svgRef.current;
    if (!body || !svg) return 1;
    const PADDING = 48; // total horizontal + vertical breathing room
    const scaleW  = (body.clientWidth  - PADDING) / svg.getAttribute('width');
    const scaleH  = (body.clientHeight - PADDING) / svg.getAttribute('height');
    return clampZoom(parseFloat(Math.min(scaleW, scaleH).toFixed(3)));
  }, []);

  // Set fit-zoom once after first render (body has real dimensions by then).
  useEffect(() => {
    setZoom(calcFitZoom());
  }, [calcFitZoom]);

  const zoomIn    = useCallback(() => setZoom(z => clampZoom(parseFloat(((z ?? 1) + ZOOM_STEP).toFixed(2)))), []);
  const zoomOut   = useCallback(() => setZoom(z => clampZoom(parseFloat(((z ?? 1) - ZOOM_STEP).toFixed(2)))), []);
  // Reset returns to fit-zoom, not 1.
  const zoomReset = useCallback(() => setZoom(calcFitZoom()), [calcFitZoom]);

  // Close on Escape; zoom on +/- keys
  useEffect(() => {
    const handler = (e) => {
      // Escape: close selection panel first, then the whole modal on second press
      if (e.key === 'Escape') {
        if (selection) { setSelection(null); return; }
        onClose();
        return;
      }
      if (e.key === '=' || e.key === '+') { zoomIn();    return; }
      if (e.key === '-' || e.key === '_') { zoomOut();   return; }
      if (e.key === '0')                  { zoomReset(); return; }
    };
    document.addEventListener('keydown', handler);
    return () => document.removeEventListener('keydown', handler);
  }, [onClose, zoomIn, zoomOut, zoomReset, selection]);

  // Ctrl/⌘ + wheel zooms; plain wheel scrolls
  useEffect(() => {
    const el = bodyRef.current;
    if (!el) return;
    const handler = (e) => {
      if (!e.ctrlKey && !e.metaKey) return;
      e.preventDefault();
      const delta = e.deltaY > 0 ? -ZOOM_STEP : ZOOM_STEP;
      setZoom(z => clampZoom(parseFloat((z + delta).toFixed(2))));
    };
    el.addEventListener('wheel', handler, { passive: false });
    return () => el.removeEventListener('wheel', handler);
  }, []);

  const zoomPct = zoom !== null ? Math.round(zoom * 100) : '…';

  return (
    <div className="agent-diagram-overlay" onClick={onClose}>
      <div
        className="agent-diagram-modal"
        onClick={(e) => e.stopPropagation()}
        role="dialog"
        aria-modal="true"
        aria-label="Agent Diagram View"
      >
        <div className="agent-diagram-header">
          <span className="agent-diagram-title">Agent Diagram View</span>

          {/* Zoom controls */}
          <div className="agent-diagram-zoom">
            <button
              className="adz-btn"
              onClick={zoomOut}
              disabled={zoom <= ZOOM_MIN}
              aria-label="Zoom out"
              title="Zoom out (−)"
            >−</button>
            <button
              className="adz-pct"
              onClick={zoomReset}
              title="Reset zoom (0)"
              aria-label={`Zoom ${zoomPct}% — click to reset`}
            >{zoomPct}%</button>
            <button
              className="adz-btn"
              onClick={zoomIn}
              disabled={zoom >= ZOOM_MAX}
              aria-label="Zoom in"
              title="Zoom in (+)"
            >+</button>
          </div>

          <button
            className="agent-diagram-close"
            onClick={onClose}
            aria-label="Close diagram"
          >
            ×
          </button>
        </div>

        <div className="agent-diagram-body" ref={bodyRef}>
          {agents.length === 0 ? (
            <p className="agent-diagram-empty">No agent data available.</p>
          ) : (
            <div className={`agent-diagram-canvas${selection ? ' agent-diagram-canvas--with-panel' : ''}`}>
              <div
                className="agent-diagram-scroll"
                style={{ transform: zoom !== null ? `scale(${zoom})` : 'none' }}
              >
                <AgentDiagramSVG
                  agents={agents}
                  svgRef={svgRef}
                  onNodeClick={(sel) =>
                    setSelection(p =>
                      p && p.kind === sel.kind &&
                      p.agent?.agentId === sel.agent?.agentId &&
                      p.entityType === sel.entityType
                        ? null   // clicking the same node again closes the panel
                        : sel
                    )
                  }
                />
              </div>
            </div>
          )}

          {/* Selection detail side panel */}
          {selection && useCaseId && (
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
