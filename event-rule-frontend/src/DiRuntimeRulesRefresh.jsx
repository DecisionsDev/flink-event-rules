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
import React, { useEffect, useMemo, useState } from 'react';
import { API_URL } from './config.js';
import ArchiveProjectBrowser, { statusTone } from './ArchiveProjectBrowser.jsx';

export { statusTone };

/**
 * Parts shown on an archive card.  The BFF parses the decision id (the only place that knows its
 * shape) and returns `service`, `version`, `build` and `group`; this only derives display strings.
 */
export function describeArchive(d) {
  const service = d.service || d.name || d.id;
  const version = d.version || '';
  const project = d.group && d.group !== service ? d.group : '';
  return { service, version, project, label: version ? `${service} ${version}` : service };
}

/** Decision Services the BFF resolved for the use case (X-Decision-Services header). */
function readBoundServices(response) {
  try {
    const raw = response.headers?.get?.('X-Decision-Services');
    const parsed = raw ? JSON.parse(decodeURIComponent(raw)) : null;
    return Array.isArray(parsed) ? parsed : null;
  } catch {
    return null;
  }
}

const normKey = (name) => String(name ?? '').toLowerCase().replace(/[^a-z0-9]/g, '');

/**
 * The listed archive that is the jar the job runs, or null. An archive saved from the runtime
 * carries the exact jar name; the jar a use case ships with (`<Service>_<version>.jar`) has no
 * build suffix, so it is matched by service + version, and only when that is unambiguous.
 */
export function findArchiveForJar(archives, jar) {
  if (!jar) return null;
  const exact = archives.filter(a => a.filename && a.filename === jar.filename);
  if (exact.length === 1) return exact[0].id;
  if (exact.length > 1) return null;
  if (!jar.service || !jar.version) return null;
  const loose = archives.filter(a => normKey(a.service) === normKey(jar.service) && a.version === jar.version);
  return loose.length === 1 ? loose[0].id : null;
}

export function SkeletonCards() {
  return (
    <div className="rr-grid" aria-hidden="true">
      {[0, 1, 2].map(i => <div key={i} className="rr-card rr-card--skeleton" />)}
    </div>
  );
}

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
  const [boundServices, setBoundServices]   = useState(null); // null = unknown
  const [decisionsState, setDecisionsState] = useState('idle'); // idle|loading|ready|error
  const [decisionsError, setDecisionsError] = useState(null);
  const [decisionId, setDecisionId]         = useState('');
  const [refreshState, setRefreshState]     = useState('idle'); // idle|refreshing|done|error
  const [refreshMsg, setRefreshMsg]         = useState(null);
  const [expanded, setExpanded]             = useState(false);
  // Explicit opt-in: also write the refreshed archive into agents.json (ruleset.path).
  const [updateAgentsJson, setUpdateAgentsJson] = useState(false);
  // READY TO APPLY details: user-controlled only; selecting/clearing an archive never changes it.
  const [detailsOpen, setDetailsOpen]       = useState(false);
  // What the job really runs, as recorded in agents.json once the job confirmed a load.
  // It changes only after a confirmed refresh/restore, never on a failure.
  const [rules, setRules]                   = useState(null); // {active, default, isDefault, inSync} | null

  useEffect(() => {
    if (disabled || runtimes !== null) return;
    fetch(`${API_URL}/api/ads-runtimes`)
      .then(r => (r.ok ? r.json() : r.json().then(b => Promise.reject(new Error(b.error || r.statusText)))))
      .then(list => setRuntimes(Array.isArray(list) ? list : []))
      .catch(err => { setRuntimes([]); setRuntimesError(err.message); });
  }, [disabled, runtimes]);

  const loadRules = async () => {
    try {
      const r = await fetch(`${API_URL}/api/ads-runtimes/rules-status?useCaseId=${encodeURIComponent(useCaseId ?? '')}`);
      const body = await r.json().catch(() => ({}));
      if (r.ok && body && typeof body === 'object') setRules(body);
    } catch {
      // keep what is shown: an unreadable status must not change the indicator
    }
  };

  useEffect(() => {
    if (disabled || !useCaseId) return;
    loadRules();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [disabled, useCaseId]);

  const resetSelection = () => { setDecisionId(''); setRefreshState('idle'); setRefreshMsg(null); };

  const loadDecisions = async (rtId, spaceValue) => {
    setDecisionsState('loading');
    setDecisionsError(null);
    setDecisions([]);
    setBoundServices(null);
    resetSelection();
    try {
      const sp = (spaceValue || '').trim();
      // useCaseId makes the BFF return only the versions of the Decision Service bound to the
      // use case; it is always sent so that an archive of another service is never listed.
      const query = `?useCaseId=${encodeURIComponent(useCaseId ?? '')}`
        + (sp ? `&deploymentSpace=${encodeURIComponent(sp)}` : '');
      const r = await fetch(`${API_URL}/api/ads-runtimes/${encodeURIComponent(rtId)}/decisions${query}`);
      const body = await r.json();
      if (!r.ok) throw new Error(body.error || r.statusText);
      setBoundServices(readBoundServices(r));
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

  const selectDecision = (id) => { setDecisionId(id); setRefreshState('idle'); setRefreshMsg(null); };

  // Both actions are confirmed by the job before the BFF answers 200; only then is the
  // indicator re-read. On any error the previous indicator stays.
  const runRulesAction = async (url, failure, success) => {
    setRefreshState('refreshing');
    setRefreshMsg(null);
    try {
      const r = await fetch(url, { method: 'POST' });
      const body = await r.json().catch(() => ({}));
      if (!r.ok) throw new Error(body.error || r.statusText);
      setRefreshState('done');
      setRefreshMsg(success(body));
      await loadRules();
    } catch (err) {
      setRefreshState('error');
      setRefreshMsg(`${failure}: ${err.message}`);
    }
  };

  const handleRefresh = () => {
    if (!runtimeId || !decisionId) return undefined;
    const sp = space.trim();
    const spaceQuery = sp ? `&deploymentSpace=${encodeURIComponent(sp)}` : '';
    return runRulesAction(
      `${API_URL}/api/ads-runtimes/${encodeURIComponent(runtimeId)}/refresh-rules`
        + `?decisionId=${encodeURIComponent(decisionId)}&useCaseId=${encodeURIComponent(useCaseId)}${spaceQuery}`
        + `&updateAgentsJson=${updateAgentsJson}`,
      'Refresh failed',
      body => `Archive ${body.filename} is now running — confirmed by the job (${(body.topics || []).join(', ')}).`,
    );
  };

  const handleRestoreDefault = () => runRulesAction(
    `${API_URL}/api/ads-runtimes/restore-default?useCaseId=${encodeURIComponent(useCaseId)}`,
    'Restore failed',
    body => `Default archive ${body.filename} restored — confirmed by the job.`,
  );

  const archives = useMemo(
    () => decisions.map(d => ({ ...d, ...describeArchive(d) })),
    [decisions],
  );
  const selected = archives.find(a => a.id === decisionId) || null;
  const activeJar  = rules?.active || null;
  const defaultJar = rules?.default || null;
  const activeId   = useMemo(() => findArchiveForJar(archives, activeJar), [archives, activeJar]);
  const defaultId  = useMemo(() => findArchiveForJar(archives, defaultJar), [archives, defaultJar]);
  const refreshing = refreshState === 'refreshing';

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
        {!expanded && selected && (
          <span className="rr-header-chip" title={selected.id}>{selected.label}</span>
        )}
      </div>
      <div id="refresh-rules-body" hidden={!expanded} className="agent-mgmt-refresh-body">
        {disabled && disabledReason && <div className="rr-note rr-note--warn">{disabledReason}</div>}
        <fieldset className="agent-mgmt-refresh-fields" disabled={disabled}>
          {runtimesError && <div className="rr-note rr-note--error" role="alert">Could not load runtimes: {runtimesError}</div>}
          {runtimes === null && !runtimesError && !disabled && <div className="rr-note">Loading runtimes…</div>}
          {runtimes !== null && runtimes.length === 0 && !runtimesError && (
            <div className="rr-note">
              No DI runtimes configured. Add entries to <code>DI_RUNTIMES</code> in your <code>.env</code> file.
            </div>
          )}

          {/* What the job runs now, and the way back to the default */}
          {activeJar && (
            <div className={`rr-running${rules.isDefault ? '' : ' rr-running--custom'}`} data-testid="rr-running">
              <span className="rr-running-dot" aria-hidden="true" />
              <div className="rr-running-text">
                <span className="rr-running-label">Running now</span>
                <span className="rr-running-name" title={activeJar.path}>{activeJar.filename}</span>
              </div>
              <span className={`rr-badge ${rules.isDefault ? 'rr-badge--ok' : 'rr-badge--accent'}`}>
                {rules.isDefault ? 'Default' : 'Custom'}
              </span>
              {!rules.isDefault && defaultJar && (
                <button
                  type="button"
                  className="rr-btn"
                  disabled={refreshing}
                  title={`Reload the default archive (${defaultJar.filename}) into the running job`}
                  onClick={handleRestoreDefault}
                >
                  Restore default
                </button>
              )}
              {rules.inSync === false && (
                <span className="rr-running-warn" role="alert">
                  The agents of this use case do not use the same archive.
                </span>
              )}
            </div>
          )}

          {/* Step 1 — where do the archives come from */}
          <div className="rr-step">
            <div className="rr-step-head"><span className="rr-step-num">1</span>Source</div>
            <div className="rr-source">
              <div className="rr-field">
                <label className="rr-label" htmlFor="refresh-rules-runtime">DI Runtime</label>
                <select
                  id="refresh-rules-runtime"
                  className="rr-input"
                  aria-label="Select DI runtime"
                  value={runtimeId}
                  onChange={e => onRuntimeChange(e.target.value)}
                >
                  <option value="">— select a runtime —</option>
                  {(runtimes || []).map(rt => <option key={rt.id} value={rt.id}>{rt.name}</option>)}
                </select>
              </div>
              {runtimeId && (
                <div className="rr-field">
                  <label className="rr-label" htmlFor="refresh-rules-space">Deployment Space</label>
                  <div className="rr-inline">
                    <input
                      id="refresh-rules-space"
                      className="rr-input"
                      type="text"
                      aria-label="Deployment space"
                      value={space}
                      placeholder="embedded"
                      spellCheck={false}
                      onChange={e => setSpace(e.target.value)}
                      onKeyDown={e => {
                        if (e.key === 'Enter' && space.trim() && decisionsState !== 'loading') {
                          e.preventDefault();
                          loadDecisions(runtimeId, space);
                        }
                      }}
                    />
                    <button
                      type="button"
                      className="rr-btn"
                      disabled={!space.trim() || decisionsState === 'loading'}
                      onClick={() => loadDecisions(runtimeId, space)}
                    >
                      {decisionsState === 'loading' ? 'Loading…' : 'Load Decisions'}
                    </button>
                  </div>
                </div>
              )}
            </div>
          </div>

          {/* Step 2 — choose the decision service archive */}
          <div className="rr-step">
            <div className="rr-step-head">
              <span className="rr-step-num">2</span>
              <span>Decision service archive</span>
              {decisionsState === 'ready' && archives.length > 0 && (
                <span className="rr-count">{archives.length} available</span>
              )}
            </div>

            {!runtimeId && (
              <div className="rr-empty">
                <strong>Select a runtime to browse archives</strong>
                <span>Deployed decision service archives will appear here.</span>
              </div>
            )}

            {runtimeId && decisionsState === 'loading' && (
              <div aria-busy="true">
                <SkeletonCards />
                <span className="rr-sr-only" role="status">Loading archives…</span>
              </div>
            )}

            {runtimeId && decisionsState === 'error' && (
              <div className="rr-note rr-note--error rr-note--row" role="alert">
                <span>Could not load decisions: {decisionsError}</span>
                <button type="button" className="rr-btn" onClick={() => loadDecisions(runtimeId, space)}>Retry</button>
              </div>
            )}

            {decisionsState === 'ready' && archives.length === 0 && boundServices && boundServices.length === 0 && (
              <div className="rr-empty">
                <strong>This use case has no decision service binding</strong>
                <span>
                  Set <code>ruleset.path</code> (<code>&lt;Service&gt;_&lt;version&gt;.jar</code>) or{' '}
                  <code>ruleset.project</code> in its agents.json so its archives can be listed.
                </span>
              </div>
            )}

            {decisionsState === 'ready' && archives.length === 0 && !(boundServices && boundServices.length === 0) && (
              <div className="rr-empty">
                <strong>No version of this use case&apos;s decision service on this runtime</strong>
                <span>
                  {boundServices && boundServices.length > 0
                    ? <>Looked for <strong>{boundServices.join(', ')}</strong> in this deployment space. </>
                    : null}
                  Only versions of the decision service bound to this use case are listed.
                  Check the deployment space, then load again.
                </span>
              </div>
            )}

            {decisionsState === 'ready' && archives.length > 0 && (
              <ArchiveProjectBrowser
                archives={archives}
                decisionId={decisionId}
                onSelect={selectDecision}
                radioName="refresh-rules-decision"
                ariaLabelFor={a => (a.id === activeId ? `${a.label} (running now)` : a.label)}
                isHighlighted={a => a.id === activeId}
                tagsFor={a => (
                  <>
                    {a.id === activeId && <span className="rr-card-active">Running</span>}
                    {a.id === defaultId && <span className="rr-card-default">Default</span>}
                  </>
                )}
              />
            )}
          </div>

          {/* Step 3 — review and apply */}
          <div className="rr-footer">
            <div className="rr-summary" aria-live="polite">
              {selected ? (
                <>
                  <button
                    type="button"
                    className="rr-summary-toggle"
                    aria-expanded={detailsOpen}
                    aria-controls="rr-summary-details"
                    onClick={() => setDetailsOpen(o => !o)}
                  >
                    <span className="rr-summary-caret" aria-hidden="true">{detailsOpen ? '▾' : '▸'}</span>
                    <span className="rr-summary-label">Ready to apply</span>
                    <span className="rr-summary-value">{selected.label}</span>
                  </button>
                  <span id="rr-summary-details" className="rr-summary-meta" hidden={!detailsOpen}>{selected.id}</span>
                </>
              ) : (
                <span className="rr-summary-label">Select an archive to continue</span>
              )}
            </div>
            <div className="rr-footer-actions">
              <label
                className="rr-checkbox"
                title="Checked: write the refreshed archive to agents.json and keep the archive file. Unchecked: the archive file is deleted once the job confirmed the new rules."
              >
                <input
                  type="checkbox"
                  checked={updateAgentsJson}
                  disabled={refreshing}
                  onChange={e => setUpdateAgentsJson(e.target.checked)}
                />
                Persist Rules
              </label>
              {selected && !refreshing && (
                <button type="button" className="rr-btn rr-btn--ghost" onClick={() => selectDecision('')}>
                  Clear
                </button>
              )}
              <button
                type="button"
                className="rr-btn rr-btn--primary"
                disabled={!runtimeId || !decisionId || refreshing}
                title="Save the selected archive in the use-case folder and send the reload command to the archive control topic"
                onClick={handleRefresh}
              >
                {refreshing ? 'Waiting for the job…' : 'Refresh Rules'}
              </button>
            </div>
          </div>

          {refreshMsg && (
            <div
              className={`rr-note ${refreshState === 'error' ? 'rr-note--error' : 'rr-note--success'}`}
              role={refreshState === 'error' ? 'alert' : 'status'}
            >
              <span>{refreshMsg}</span>
            </div>
          )}
        </fieldset>
      </div>
    </section>
  );
}
