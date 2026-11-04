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
import React, { useMemo, useState, useCallback, useEffect, useRef } from 'react';
import {
  buildSqlGraph, describeNode, describeWrite, describeEnrichment, describeBranch, SqlParseError,
  labelWidth, LABEL_MAX_CHARS, ZOOM_MIN, ZOOM_MAX, ZOOM_STEP, clampZoom, fitZoom, initialZoom,
} from './sqlGraph.js';
import './SqlGraphView.css';

/**
 * SqlGraphView
 *
 * Human-readable rendering of the statements of a PTF deployment as a data-flow diagram:
 * Kafka topics in, input view, Process Table Function, Kafka topics out — one lane per agent.
 * Everything shown is derived from the parsed SQL (see sqlGraph.js); statements the parser
 * does not fully understand are listed with the reason instead of being drawn, and every
 * node / edge offers a jump to the exact SQL it was derived from.
 *
 * Props
 *   statements        [{ kind, agentId, sql }]  as served by GET /api/agents/:id/sql
 *   onShowStatement   (statementIndex) => void  switch to the raw SQL at that statement
 *   onShowRaw         () => void                switch to the raw SQL (fallback link)
 */

const ELLIPSIS = '…';
function truncate(text, max) {
  const s = String(text ?? '');
  return s.length > max ? s.slice(0, max - 1) + ELLIPSIS : s;
}

/** Visual class and wording of a node. */
function nodeLook(node) {
  if (node.type === 'external') return { cls: 'external', head: 'Undefined', title: node.name, sub: 'not defined in this script' };
  if (node.type === 'function') return { cls: 'function', head: 'Function', title: node.name, sub: node.model.className };
  if (node.type === 'table') {
    const o = node.model.options;
    const topic = o.connector === 'kafka' ? o.topic : null;
    const head = node.role === 'sink' ? 'Kafka sink' : node.role === 'source' ? 'Kafka source' : 'Table';
    return { cls: node.role === 'sink' ? 'sink' : node.role === 'source' ? 'source' : 'table', head: topic ? head : 'Table', title: topic ?? node.name, sub: node.name };
  }
  const sel = node.model.query.selects[0];
  if (sel.from.type === 'call') return { cls: 'processor', head: 'Processor (PTF)', title: sel.from.name, sub: node.name };
  const n = node.model.query.selects.length;
  return { cls: 'view', head: n > 1 ? `Input view · ${n} inputs` : 'View', title: node.name, sub: n > 1 ? 'UNION ALL' : 'SELECT' };
}

/** What an edge means, in words. */
function describeEdge(edge, nodes) {
  if (edge.kind === 'writes') return describeWrite(edge);
  if (edge.kind === 'topic') {
    return {
      title: `Kafka topic ${edge.label}`,
      kindLabel: 'Topic hand-off',
      summary: `Rows written to the topic ‘${edge.label}’ by the sink are read from the same topic by the source.`,
      facts: [{ label: 'From', value: edge.from }, { label: 'To', value: edge.to }],
    };
  }
  const to = nodes.get(edge.to);
  const sel = to.model.query.selects[edge.branchIndex];
  if (sel.from.type === 'call') {
    return {
      title: `${edge.from} → ${edge.to}`, kindLabel: 'Function input',
      summary: `‘${edge.from}’ is passed to the function as its input table.`,
      facts: [{ label: 'From', value: edge.from }, { label: 'Into', value: edge.to }],
    };
  }
  const b = describeBranch(sel);
  const facts = [{ label: 'From', value: edge.from }, { label: 'Into', value: edge.to }];
  if (b.eventType) facts.push({ label: 'Event type', value: b.eventType });
  if (b.entityPath) facts.push({ label: 'Entity key', value: b.entityPath });
  if (b.sourceTopic) facts.push({ label: 'Tagged with topic', value: b.sourceTopic });
  if (b.where) facts.push({ label: 'Only rows where', value: b.where });
  const lines = b.enrich ? describeEnrichment(b.enrich.spec) : [];
  return {
    title: `${edge.from} → ${edge.to}`,
    kindLabel: b.chained ? 'Chained input' : 'Input',
    summary: b.chained
      ? `Reads the output of the upstream agent${b.where ? ` (${b.where})` : ''}.`
      : `Normalises ${b.eventType ? `‘${b.eventType}’ ` : ''}events from ‘${edge.from}’ into the shared input columns.`,
    facts,
    enrichment: lines,
  };
}

function Node({ node, selected, onSelect }) {
  const look = nodeLook(node);
  const activate = (e) => {
    if (e.type === 'keydown' && e.key !== 'Enter' && e.key !== ' ') return;
    e.preventDefault();
    onSelect({ type: 'node', id: node.id });
  };
  return (
    <g
      className={`sqg-node sqg-node--${look.cls}${selected ? ' sqg-node--selected' : ''}`}
      transform={`translate(${node.x},${node.y})`}
      role="button"
      tabIndex={0}
      aria-pressed={selected}
      aria-label={`${look.head}: ${look.title}`}
      onClick={activate}
      onKeyDown={activate}
    >
      <title>{`${look.head}\n${look.title}\n${look.sub}`}</title>
      <rect className="sqg-node-box" width={node.w} height={node.h} rx={8} />
      <rect className="sqg-node-bar" width={5} height={node.h} rx={3} />
      <text className="sqg-node-head" x={14} y={19}>{truncate(look.head, 30)}</text>
      <text className="sqg-node-title" x={14} y={38}>{truncate(look.title, 34)}</text>
      <text className="sqg-node-sub" x={14} y={54}>{truncate(look.sub, 40)}</text>
    </g>
  );
}

function Edge({ edge, selected, onSelect }) {
  const clickable = edge.kind !== 'reads' || edge.label;
  const activate = (e) => {
    if (e.type === 'keydown' && e.key !== 'Enter' && e.key !== ' ') return;
    e.preventDefault();
    onSelect({ type: 'edge', id: edge.id });
  };
  const text = truncate(edge.label, LABEL_MAX_CHARS);
  const w = labelWidth(edge.label);
  return (
    <g className={`sqg-edge sqg-edge--${edge.kind}${selected ? ' sqg-edge--selected' : ''}`}>
      <path className="sqg-edge-path" d={edge.path} markerEnd="url(#sqg-arrow)" />
      <path className="sqg-edge-hit" d={edge.path} onClick={activate} />
      {clickable && text && (
        <g
          className="sqg-edge-label"
          transform={`translate(${edge.labelX},${edge.labelY})`}
          role="button"
          tabIndex={0}
          aria-pressed={selected}
          aria-label={`Flow ${edge.from} to ${edge.to}: ${edge.label}`}
          onClick={activate}
          onKeyDown={activate}
        >
          <title>{edge.label}</title>
          <rect x={-w / 2} y={-9} width={w} height={18} rx={9} />
          <text textAnchor="middle" y={4}>{text}</text>
        </g>
      )}
    </g>
  );
}

function Details({ model, sql, statementIndex, onShowStatement }) {
  return (
    <aside className="sqg-details" aria-label="Details">
      <div className="sqg-details-header">
        <div className="sqg-details-meta">
          <span className="sqg-details-kind">{model.kindLabel}</span>
          <h3 className="sqg-details-title">{model.title}</h3>
        </div>
        {statementIndex !== null && statementIndex !== undefined && (
          <button
            type="button"
            className="agent-mgmt-btn agent-mgmt-btn-ghost sqg-goto"
            onClick={() => onShowStatement(statementIndex)}
          >
            Show in raw SQL
          </button>
        )}
      </div>

      <div className="sqg-details-grid">
        <div className="sqg-details-col sqg-details-col--summary">
          <div className="sqg-card">
            <h4 className="sqg-card-title">Overview &amp; Configuration</h4>
            <p className="sqg-details-summary">{model.summary}</p>
            {model.facts.length > 0 && (
              <dl className="sqg-facts">
                {model.facts.map(f => (
                  <React.Fragment key={f.label}>
                    <dt>{f.label}</dt>
                    <dd>{f.value}</dd>
                  </React.Fragment>
                ))}
              </dl>
            )}
          </div>

          {model.enrichment && model.enrichment.length > 0 && (
            <div className="sqg-enrich">
              <div className="sqg-enrich-title">Reference-data enrichment</div>
              <ul>{model.enrichment.map((l, i) => <li key={i}>{l}</li>)}</ul>
            </div>
          )}
          {model.enrichments && model.enrichments.map((e, i) => (
            <div className="sqg-enrich" key={i}>
              <div className="sqg-enrich-title">Reference-data enrichment</div>
              <ul>{e.map((l, k) => <li key={k}>{l}</li>)}</ul>
            </div>
          ))}
        </div>

        {sql && (
          <div className="sqg-details-col sqg-details-col--sql">
            <div className="sqg-card sqg-card--code">
              <div className="sqg-card-header">
                <h4 className="sqg-card-title">
                  SQL {model.kindLabel.toLowerCase().includes('insert') ? 'Statement' : 'Definition'}
                </h4>
              </div>
              <pre tabIndex={0} className="sqg-sql-code">{sql}</pre>
            </div>
          </div>
        )}
      </div>
    </aside>
  );
}

export default function SqlGraphView({ statements, onShowStatement, onShowRaw }) {
  const [selection, setSelection] = useState(null);
  // null = automatic (initialZoom); a number once the user has zoomed.
  const [manualZoom, setManualZoom] = useState(null);
  const [viewportWidth, setViewportWidth] = useState(0);
  const canvasRef = useRef(null);

  const result = useMemo(() => {
    if (!Array.isArray(statements) || statements.length === 0) return { error: 'There are no SQL statements to draw.' };
    try {
      return { graph: buildSqlGraph(statements) };
    } catch (err) {
      if (err instanceof SqlParseError) return { error: err.message };
      return { error: 'The SQL could not be analysed.' };
    }
  }, [statements]);

  const nodeById = useMemo(() => new Map((result.graph?.nodes ?? []).map(n => [n.id, n])), [result]);

  const details = useMemo(() => {
    if (!result.graph || !selection) return null;
    if (selection.type === 'node') {
      const node = nodeById.get(selection.id);
      if (!node) return null;
      const model = describeNode(node);
      if (node.type === 'view') {
        const enrichments = node.branches.filter(b => b.enrich).map(b => describeEnrichment(b.enrich.spec));
        if (enrichments.length) model.enrichments = enrichments;
      }
      return { model, sql: node.sql, statementIndex: node.statementIndex };
    }
    const edge = result.graph.edges.find(e => e.id === selection.id);
    if (!edge) return null;
    const model = describeEdge(edge, nodeById);
    if (edge.kind === 'writes' && edge.enrich) model.enrichment = describeEnrichment(edge.enrich.spec);
    return { model, sql: edge.kind === 'writes' ? edge.insert.text : null, statementIndex: edge.statementIndex };
  }, [result, selection, nodeById]);

  const graphWidth = result.graph?.width ?? 0;
  const zoom = manualZoom ?? initialZoom(viewportWidth, graphWidth);

  // A new set of statements starts again from the automatic scale and an empty selection.
  useEffect(() => { setManualZoom(null); setSelection(null); }, [statements]);

  // Track the visible width of the canvas so that "fit" and the initial scale follow the layout.
  useEffect(() => {
    const el = canvasRef.current;
    if (!el) return undefined;
    const measure = () => setViewportWidth(Math.max(0, el.clientWidth - 24)); // minus padding
    measure();
    if (typeof ResizeObserver === 'undefined') return undefined;
    const ro = new ResizeObserver(measure);
    ro.observe(el);
    return () => ro.disconnect();
  }, [result.graph]);

  const zoomBy = useCallback((factor) => setManualZoom(clampZoom(zoom * factor)), [zoom]);
  const zoomFit = useCallback(() => setManualZoom(fitZoom(viewportWidth, graphWidth)), [viewportWidth, graphWidth]);
  const zoomActual = useCallback(() => setManualZoom(1), []);

  const select = useCallback((sel) => {
    setSelection(prev => (prev && prev.type === sel.type && prev.id === sel.id ? null : sel));
  }, []);

  if (result.error) {
    return (
      <div className="sqg-fallback" role="alert">
        <p>The graphical view is not available: {result.error}</p>
        <button type="button" className="agent-mgmt-btn agent-mgmt-btn-primary" onClick={onShowRaw}>Show raw SQL</button>
      </div>
    );
  }

  const { graph } = result;

  return (
    <div className="sqg-root">
      {graph.unsupported.length > 0 && (
        <div className="sqg-unsupported" role="status">
          <strong>
            {graph.unsupported.length} of {graph.total} statement{graph.total === 1 ? '' : 's'} could not be drawn
          </strong>
          {' '}— the diagram shows the other {graph.represented}. The SQL below is shown exactly as executed.
          <ul>
            {graph.unsupported.map(u => (
              <li key={u.index}>
                <div>
                  Statement {u.index + 1}{u.agentId ? ` (${u.agentId})` : ''}: {u.reason}.{' '}
                  <button type="button" className="sqg-link" onClick={() => onShowStatement(u.index)}>Show in raw SQL</button>
                </div>
                <pre tabIndex={0}>{u.sql}</pre>
              </li>
            ))}
          </ul>
        </div>
      )}

      <div className="sqg-toolbar">
        <div className="sqg-legend" aria-hidden="true">
          <span className="sqg-chip sqg-chip--source">Kafka source</span>
          <span className="sqg-chip sqg-chip--view">View</span>
          <span className="sqg-chip sqg-chip--processor">Processor (PTF)</span>
          <span className="sqg-chip sqg-chip--sink">Kafka sink</span>
        </div>
        <div className="sqg-zoom" role="group" aria-label="Zoom">
          <button type="button" className="sqg-zoom-btn" onClick={() => zoomBy(1 / ZOOM_STEP)} disabled={zoom <= ZOOM_MIN} aria-label="Zoom out" title="Zoom out">−</button>
          <span className="sqg-zoom-value" data-testid="zoom-level">{Math.round(zoom * 100)}%</span>
          <button type="button" className="sqg-zoom-btn" onClick={() => zoomBy(ZOOM_STEP)} disabled={zoom >= ZOOM_MAX} aria-label="Zoom in" title="Zoom in">+</button>
          <button type="button" className="sqg-zoom-btn sqg-zoom-btn--text" onClick={zoomFit} title="Fit the diagram to the width of the window">Fit</button>
          <button type="button" className="sqg-zoom-btn sqg-zoom-btn--text" onClick={zoomActual} title="Show at actual size">100%</button>
        </div>
      </div>

      <div className="sqg-layout">
        <div className="sqg-canvas" ref={canvasRef} tabIndex={-1}>
          <svg
            className="sqg-svg"
            width={Math.round(graph.width * zoom)}
            height={Math.round(graph.height * zoom)}
            viewBox={`0 0 ${graph.width} ${graph.height}`}
            role="group"
            aria-label="Data flow of the deployed SQL"
          >
            <defs>
              <marker id="sqg-arrow" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto">
                <path d="M0,0 L0,8 L8,4 z" className="sqg-arrow" />
              </marker>
            </defs>
            {graph.bands.map(b => (
              <g key={b.agentId ?? '__shared__'} className="sqg-band">
                <rect x={b.x} y={b.y} width={b.w} height={b.h} rx={8} />
                <text x={b.x + 12} y={b.y + 16}>{b.agentId ?? 'Shared'}</text>
              </g>
            ))}
            {graph.edges.map(e => (
              <Edge key={e.id} edge={e} selected={selection?.type === 'edge' && selection.id === e.id} onSelect={select} />
            ))}
            {graph.nodes.map(n => (
              <Node key={n.id} node={n} selected={selection?.type === 'node' && selection.id === n.id} onSelect={select} />
            ))}
          </svg>
        </div>

        {details
          ? <Details {...details} onShowStatement={onShowStatement} />
          : (
            <aside className="sqg-details sqg-details--hint" aria-label="Details">
              <div className="sqg-hint-box">
                <span className="sqg-hint-icon" aria-hidden="true">💡</span>
                <div className="sqg-hint-body">
                  <p className="sqg-hint-text">
                    Select a box or a label on an arrow to see what it does in plain words, with the SQL behind it.
                  </p>
                  <p className="sqg-hint-count">{graph.represented} of {graph.total} statements drawn.</p>
                </div>
              </div>
            </aside>
          )}
      </div>
    </div>
  );
}
