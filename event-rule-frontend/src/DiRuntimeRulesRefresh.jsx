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
import React, { useEffect, useState } from 'react';
import { API_URL } from './config.js';

/**
 * "Refresh Rules" sub-section of the Agent Management panel: live refresh of the rules of a
 * deployed job.
 *
 * Same runtime / deployment space / decision selection as "Fetch from DI Runtime" of the
 * Agent Diagram Editor.  Pressing "Refresh Rules" asks the BFF to download the selected
 * decision archive into the folder of the use case and to publish a LOAD command to the
 * archive control topic of the job (see scripts/broadcast-reload.sh).
 *
 * `disabled` greys the whole sub-section out; it is set when the job does not support hot
 * reload (the PTF engine ignores archiveControlTopic) or when no job is deployed.
 */
export default function DiRuntimeRulesRefresh({ useCaseId, disabled = false, disabledReason = null }) {
  const [runtimes, setRuntimes]             = useState(null); // null = not loaded
  const [runtimesError, setRuntimesError]   = useState(null);
  const [runtimeId, setRuntimeId]           = useState('');
  const [space, setSpace]                   = useState('');
  const [decisions, setDecisions]           = useState([]);
  const [decisionsState, setDecisionsState] = useState('idle'); // idle|loading|ready|error
  const [decisionsError, setDecisionsError] = useState(null);
  const [decisionId, setDecisionId]         = useState('');
  const [refreshState, setRefreshState]     = useState('idle'); // idle|refreshing|done|error
  const [refreshMsg, setRefreshMsg]         = useState(null);
  const [expanded, setExpanded]             = useState(false);

  useEffect(() => {
    if (disabled || runtimes !== null) return;
    fetch(`${API_URL}/api/ads-runtimes`)
      .then(r => (r.ok ? r.json() : r.json().then(b => Promise.reject(new Error(b.error || r.statusText)))))
      .then(list => setRuntimes(Array.isArray(list) ? list : []))
      .catch(err => { setRuntimes([]); setRuntimesError(err.message); });
  }, [disabled, runtimes]);

  const resetSelection = () => { setDecisionId(''); setRefreshState('idle'); setRefreshMsg(null); };

  const loadDecisions = async (rtId, spaceValue) => {
    setDecisionsState('loading');
    setDecisionsError(null);
    setDecisions([]);
    resetSelection();
    try {
      const sp = (spaceValue || '').trim();
      const query = sp ? `?deploymentSpace=${encodeURIComponent(sp)}` : '';
      const r = await fetch(`${API_URL}/api/ads-runtimes/${encodeURIComponent(rtId)}/decisions${query}`);
      const body = await r.json();
      if (!r.ok) throw new Error(body.error || r.statusText);
      setDecisions(body);
      setDecisionsState('ready');
    } catch (err) {
      setDecisionsState('error');
      setDecisionsError(err.message);
    }
  };

  const onRuntimeChange = (rtId) => {
    setRuntimeId(rtId);
    resetSelection();
    setDecisions([]);
    setDecisionsState('idle');
    if (!rtId) { setSpace(''); return; }
    const seed = (runtimes || []).find(r => r.id === rtId)?.deploymentSpace || '';
    setSpace(seed);
    loadDecisions(rtId, seed);
  };

  const handleRefresh = async () => {
    if (!runtimeId || !decisionId) return;
    setRefreshState('refreshing');
    setRefreshMsg(null);
    try {
      const sp = space.trim();
      const spaceQuery = sp ? `&deploymentSpace=${encodeURIComponent(sp)}` : '';
      const r = await fetch(
        `${API_URL}/api/ads-runtimes/${encodeURIComponent(runtimeId)}/refresh-rules`
        + `?decisionId=${encodeURIComponent(decisionId)}&useCaseId=${encodeURIComponent(useCaseId)}${spaceQuery}`,
        { method: 'POST' },
      );
      const body = await r.json().catch(() => ({}));
      if (!r.ok) throw new Error(body.error || r.statusText);
      setRefreshState('done');
      setRefreshMsg(`Archive ${body.filename} saved; reload command sent to ${(body.topics || []).join(', ')}.`);
    } catch (err) {
      setRefreshState('error');
      setRefreshMsg(`Refresh failed: ${err.message}`);
    }
  };

  return (
    <section className="agent-mgmt-refresh" aria-label="Refresh Rules" data-testid="refresh-rules-section">
      <div className="agent-mgmt-refresh-title agent-mgmt-refresh-header">
        <button
          type="button"
          className="section-toggle-btn"
          aria-expanded={expanded}
          aria-controls="refresh-rules-body"
          aria-label={expanded ? 'Collapse Refresh Rules' : 'Expand Refresh Rules'}
          title={expanded ? 'Collapse' : 'Expand'}
          onClick={() => setExpanded(e => !e)}
        >
          {expanded ? '−' : '+'}
        </button>
        Refresh Rules
      </div>
      <div id="refresh-rules-body" hidden={!expanded} className="agent-mgmt-refresh-body">
      {disabled && disabledReason && <div className="ade-field-hint">{disabledReason}</div>}
      <fieldset className="agent-mgmt-refresh-fields" disabled={disabled}>
        {runtimesError && <div className="ade-field-error">Could not load runtimes: {runtimesError}</div>}
        {runtimes === null && !runtimesError && !disabled && <div className="ade-field-hint">Loading runtimes…</div>}
        {runtimes !== null && runtimes.length === 0 && !runtimesError && (
          <div className="ade-field-hint">
            No DI runtimes configured. Add entries to <code>DI_RUNTIMES</code> in your <code>.env</code> file.
          </div>
        )}
        <label className="ade-field-label" htmlFor="refresh-rules-runtime">DI Runtime</label>
        <select
          id="refresh-rules-runtime"
          className="ade-field-input"
          aria-label="Select DI runtime"
          value={runtimeId}
          onChange={e => onRuntimeChange(e.target.value)}
        >
          <option value="">— select a runtime —</option>
          {(runtimes || []).map(rt => <option key={rt.id} value={rt.id}>{rt.name}</option>)}
        </select>

        {runtimeId && (
          <>
            <label className="ade-field-label" htmlFor="refresh-rules-space">Deployment Space</label>
            <div className="ade-di-fetch-row">
              <input
                id="refresh-rules-space"
                className="ade-field-input"
                type="text"
                aria-label="Deployment space"
                value={space}
                placeholder="embedded"
                spellCheck={false}
                onChange={e => setSpace(e.target.value)}
              />
              <button
                type="button"
                className="ade-di-fetch-btn"
                disabled={!space.trim() || decisionsState === 'loading'}
                onClick={() => loadDecisions(runtimeId, space)}
              >
                {decisionsState === 'loading' ? 'Loading…' : 'Load Decisions'}
              </button>
            </div>
            <label className="ade-field-label" htmlFor="refresh-rules-decision">Decision service archive</label>
            {decisionsState === 'error' && <div className="ade-field-error">Could not load decisions: {decisionsError}</div>}
            {decisionsState === 'ready' && decisions.length === 0 && (
              <div className="ade-field-hint">No decisions deployed on this runtime.</div>
            )}
            {decisionsState === 'ready' && decisions.length > 0 && (
              <select
                id="refresh-rules-decision"
                className="ade-field-input"
                aria-label="Select decision"
                value={decisionId}
                onChange={e => { setDecisionId(e.target.value); setRefreshState('idle'); setRefreshMsg(null); }}
              >
                <option value="">— select a decision —</option>
                {decisions.map(d => <option key={d.id} value={d.id}>{d.name || d.id}</option>)}
              </select>
            )}
          </>
        )}

        <div className="ade-di-fetch-row">
          <button
            type="button"
            className="ade-di-fetch-btn ade-di-fetch-btn--primary"
            disabled={!runtimeId || !decisionId || refreshState === 'refreshing'}
            title="Save the selected archive in the use-case folder and send the reload command to the archive control topic"
            onClick={handleRefresh}
          >
            {refreshState === 'refreshing' ? 'Refreshing…' : 'Refresh Rules'}
          </button>
        </div>
        {refreshMsg && (
          <div className={refreshState === 'error' ? 'ade-field-error' : 'ade-field-hint'}>{refreshMsg}</div>
        )}
      </fieldset>
      </div>
    </section>
  );
}
