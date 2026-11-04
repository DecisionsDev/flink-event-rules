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
import React, { useState, useEffect, useRef, useCallback, useMemo } from 'react';
import { API_URL } from './config.js';
import SqlGraphView from './SqlGraphView';
import './SqlViewModal.css';

/**
 * SqlViewModal
 *
 * Read-only view of the Flink SQL script of a use case deployed on the PTF runtime.
 * The script is the snapshot the BFF stored at deployment time
 * (GET /api/agents/:useCaseId/sql), so it is what the job was started with even if
 * agents.json has been edited since — in which case a "stale" notice is shown.
 *
 * Two presentations of the same statements, chosen with the Raw SQL / Graphical toggle:
 *   Raw SQL     — the statements as executed, highlighted, grouped by agent;
 *   Graphical   — a data-flow diagram derived from them (SqlGraphView), where every element
 *                 links back to its statement here.  Statements the diagram cannot represent
 *                 are listed with the reason, and the raw view is always one click away.
 *
 * Tokens are rendered as React nodes (never as HTML), so topic names, agent ids and JSON
 * literals inside the SQL cannot inject markup.
 */

const KEYWORDS = new Set([
  'CREATE', 'TEMPORARY', 'TABLE', 'VIEW', 'FUNCTION', 'AS', 'SELECT', 'FROM', 'WHERE', 'INSERT',
  'INTO', 'UNION', 'ALL', 'WITH', 'WATERMARK', 'FOR', 'METADATA', 'PARTITION', 'BY',
  'DESCRIPTOR', 'EXECUTE', 'STATEMENT', 'SET', 'BEGIN', 'END', 'USING', 'JAR', 'CAST', 'COALESCE',
  'INTERVAL', 'SECOND', 'NULL', 'AND', 'OR', 'NOT', 'STRING', 'BIGINT', 'BYTES', 'TIMESTAMP_LTZ',
]);

// Order matters: comment, string, quoted identifier, number, word.
const TOKEN_RE = /(--.*$)|('(?:[^']|'')*'?)|(`(?:[^`]|``)*`?)|(\b\d+(?:\.\d+)?\b)|([A-Za-z_$][A-Za-z0-9_$]*)/y;

/**
 * Splits one line of SQL into [{ type, text }] where type is one of
 * comment | string | ident | number | keyword | plain.  The concatenation of the texts
 * is always the original line.
 */
export function tokenizeSqlLine(line) {
  const tokens = [];
  let plain = '';
  let i = 0;
  const flush = () => { if (plain) { tokens.push({ type: 'plain', text: plain }); plain = ''; } };
  while (i < line.length) {
    TOKEN_RE.lastIndex = i;
    const m = TOKEN_RE.exec(line);
    if (!m) { plain += line[i]; i += 1; continue; }
    const text = m[0];
    let type;
    if (m[1] !== undefined)      type = 'comment';
    else if (m[2] !== undefined) type = 'string';
    else if (m[3] !== undefined) type = 'ident';
    else if (m[4] !== undefined) type = 'number';
    else type = KEYWORDS.has(text.toUpperCase()) ? 'keyword' : 'plain';
    if (type === 'plain') plain += text;
    else { flush(); tokens.push({ type, text }); }
    i += text.length;
  }
  flush();
  return tokens;
}

/**
 * Renders SQL text as numbered, highlighted lines.
 * `ranges` maps statements to their lines ({ index, from, to }, 0-based, inclusive), so the
 * lines of the statement `highlight` can be marked and scrolled to.
 */
function SqlCode({ sql, label, ranges = [], highlight = null }) {
  const lines = useMemo(() => sql.replace(/\n+$/, '').split('\n').map(tokenizeSqlLine), [sql]);
  const range = highlight === null ? null : ranges.find(r => r.index === highlight);
  const starts = useMemo(() => new Map(ranges.map(r => [r.from, r.index])), [ranges]);
  return (
    <pre className="sql-view-code" aria-label={label} tabIndex={0}>
      <code>
        {lines.map((tokens, i) => {
          const marked = range && i >= range.from && i <= range.to;
          return (
            <div
              className={`sql-view-line${marked ? ' sql-view-line--marked' : ''}`}
              key={i}
              data-statement={starts.get(i)}
              data-marked={marked && i === range.from ? 'true' : undefined}
            >
              <span className="sql-view-lineno" aria-hidden="true">{i + 1}</span>
              <span className="sql-view-text">
                {tokens.length === 0 ? '\u200b' : tokens.map((t, j) => (
                  t.type === 'plain'
                    ? <React.Fragment key={j}>{t.text}</React.Fragment>
                    : <span key={j} className={`sql-tok sql-tok--${t.type}`}>{t.text}</span>
                ))}
              </span>
            </div>
          );
        })}
      </code>
    </pre>
  );
}

/**
 * Groups the attributed statements by agent, in order of first appearance.  Statements with
 * no agent (shared registrations) go into a group with agentId null.
 */
export function groupStatements(statements) {
  const groups = new Map();
  statements.forEach((st, index) => {
    const key = st.agentId ?? null;
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push({ ...st, index });
  });
  return [...groups].map(([agentId, items]) => {
    // Statements are joined by one blank line; remember which lines each one occupies.
    let line = 0;
    const ranges = items.map((it) => {
      const count = it.sql.replace(/\n+$/, '').split('\n').length;
      const range = { index: it.index, from: line, to: line + count - 1 };
      line += count + 1;
      return range;
    });
    return { agentId, items, ranges, sql: items.map(i => i.sql).join('\n\n') };
  });
}

function copyText(text) {
  if (navigator.clipboard && window.isSecureContext) {
    return navigator.clipboard.writeText(text);
  }
  // Plain-http deployments are not a secure context: fall back to a hidden textarea.
  return new Promise((resolve, reject) => {
    const ta = document.createElement('textarea');
    ta.value = text;
    ta.setAttribute('readonly', '');
    ta.style.position = 'fixed';
    ta.style.opacity = '0';
    document.body.appendChild(ta);
    ta.select();
    try {
      document.execCommand('copy') ? resolve() : reject(new Error('copy command rejected'));
    } catch (err) {
      reject(err);
    } finally {
      document.body.removeChild(ta);
    }
  });
}

function downloadText(filename, text) {
  const url = URL.createObjectURL(new Blob([text], { type: 'text/plain;charset=utf-8' }));
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

export default function SqlViewModal({ useCaseId, onClose }) {
  const [state, setState]   = useState({ phase: 'loading', data: null, error: null });
  const [copied, setCopied] = useState(null); // null | 'ok' | 'failed'
  const [view, setView]     = useState('raw');  // 'raw' | 'graph'
  const [focus, setFocus]   = useState(null);   // statement index to highlight in the raw view
  const bodyRef             = useRef(null);
  const closeRef            = useRef(null);

  useEffect(() => {
    const controller = new AbortController();
    setState({ phase: 'loading', data: null, error: null });
    setFocus(null);
    (async () => {
      try {
        const r = await fetch(`${API_URL}/api/agents/${encodeURIComponent(useCaseId)}/sql`, { signal: controller.signal });
        const body = await r.json().catch(() => ({}));
        if (controller.signal.aborted) return;
        if (!r.ok) {
          setState({ phase: 'error', data: null, error: body.error || r.statusText || 'Request failed' });
          return;
        }
        setState({ phase: 'ready', data: body, error: null });
      } catch (err) {
        if (err.name === 'AbortError') return;
        setState({ phase: 'error', data: null, error: 'Could not reach the server.' });
      }
    })();
    return () => controller.abort();
  }, [useCaseId]);

  useEffect(() => { closeRef.current?.focus(); }, []);

  useEffect(() => {
    const handler = (e) => { if (e.key === 'Escape') onClose(); };
    document.addEventListener('keydown', handler);
    return () => document.removeEventListener('keydown', handler);
  }, [onClose]);

  const script = state.data?.script ?? '';
  const statements = state.data?.statements;
  const groups = useMemo(
    () => (Array.isArray(statements) && statements.length > 0 ? groupStatements(statements) : null),
    [statements],
  );

  const showRaw = useCallback(() => { setFocus(null); setView('raw'); }, []);
  const showStatement = useCallback((index) => { setFocus(index); setView('raw'); }, []);

  // Bring the statement chosen in the graphical view into sight.
  useEffect(() => {
    if (view !== 'raw' || focus === null) return;
    const el = bodyRef.current?.querySelector('[data-marked="true"]');
    if (el && typeof el.scrollIntoView === 'function') el.scrollIntoView({ block: 'center' });
  }, [view, focus, state.phase]);

  const handleCopy = useCallback(async () => {
    try { await copyText(script); setCopied('ok'); } catch { setCopied('failed'); }
    setTimeout(() => setCopied(null), 2000);
  }, [script]);

  const handleDownload = useCallback(() => {
    downloadText(`${useCaseId}-ptf.sql`, script);
  }, [useCaseId, script]);

  const { data } = state;

  return (
    <div className="sql-view-overlay" onClick={onClose}>
      <div
        className="sql-view-modal"
        onClick={(e) => e.stopPropagation()}
        role="dialog"
        aria-modal="true"
        aria-label="SQL View"
      >
        <div className="sql-view-header">
          <span className="sql-view-title">SQL View</span>
          <span className="sql-view-subtitle">{useCaseId}</span>
          <div className="sql-view-spacer" />
          <div className="sql-view-toggle" role="group" aria-label="SQL presentation">
            <button
              type="button"
              className={`sql-view-toggle-btn${view === 'raw' ? ' sql-view-toggle-btn--on' : ''}`}
              aria-pressed={view === 'raw'}
              onClick={showRaw}
            >
              Raw SQL
            </button>
            <button
              type="button"
              className={`sql-view-toggle-btn${view === 'graph' ? ' sql-view-toggle-btn--on' : ''}`}
              aria-pressed={view === 'graph'}
              onClick={() => setView('graph')}
              disabled={state.phase !== 'ready'}
            >
              Graphical
            </button>
          </div>
          <button className="agent-mgmt-btn agent-mgmt-btn-ghost" onClick={handleCopy} disabled={state.phase !== 'ready'}>
            {copied === 'ok' ? 'Copied' : copied === 'failed' ? 'Copy failed' : 'Copy'}
          </button>
          <button className="agent-mgmt-btn agent-mgmt-btn-ghost" onClick={handleDownload} disabled={state.phase !== 'ready'}>
            Download
          </button>
          <button className="agent-diagram-close" ref={closeRef} onClick={onClose} aria-label="Close SQL view">×</button>
        </div>

        {state.phase === 'ready' && (
          <div className="sql-view-notes">
            <span>
              Flink SQL equivalent of the deployed job
              {data.generatedAt ? `, generated ${new Date(data.generatedAt).toLocaleString()}` : ''}
              {data.jobId ? ` (job ${String(data.jobId).slice(0, 8)}…)` : ''}.
              {' '}Chaining {data.chainingEnabled === false ? 'disabled' : 'enabled'}.
            </span>
            <span>
              The functions <code>EVENT_FIELD</code>, <code>EVENT_ENRICH</code> and <code>event_rule_*</code> are
              registered through the Java API by the job, not by SQL statements.
            </span>
            {data.stale && (
              <span className="sql-view-stale" role="status">
                The agent descriptor has changed since this job was deployed. Redeploy to update the job.
              </span>
            )}
          </div>
        )}

        <div className="sql-view-body" ref={bodyRef}>
          {state.phase === 'loading' && <p className="sql-view-message">Loading SQL…</p>}
          {state.phase === 'error' && <p className="sql-view-message sql-view-message--error" role="alert">{state.error}</p>}
          {state.phase === 'ready' && view === 'graph' && (
            <SqlGraphView
              statements={Array.isArray(statements) ? statements : []}
              onShowStatement={showStatement}
              onShowRaw={showRaw}
            />
          )}
          {state.phase === 'ready' && view === 'raw' && (
            groups ? groups.map(g => (
              <section className="sql-view-group" key={g.agentId ?? '__shared__'}>
                <h3 className="sql-view-group-title">
                  {g.agentId ?? 'Shared'}
                  <span className="sql-view-group-count">
                    {g.items.length} statement{g.items.length === 1 ? '' : 's'}
                  </span>
                </h3>
                <SqlCode
                  sql={g.sql}
                  label={`SQL of ${g.agentId ?? 'shared statements'}`}
                  ranges={g.ranges}
                  highlight={focus}
                />
              </section>
            )) : <SqlCode sql={script} label="Generated SQL script" />
          )}
        </div>
      </div>
    </div>
  );
}
