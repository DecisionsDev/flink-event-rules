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
import React, { useEffect, useId, useMemo, useRef, useState } from 'react';
import { API_URL } from './config.js';
import { SkeletonCards, describeArchive, findArchiveForJar } from './DiRuntimeRulesRefresh.jsx';
import ArchiveProjectBrowser from './ArchiveProjectBrowser.jsx';

/** First bytes of a jar: it is a zip archive ("PK\x03\x04"). */
function readHead(blob) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(new Uint8Array(reader.result));
    reader.onerror = () => reject(reader.error);
    reader.readAsArrayBuffer(blob.slice(0, 4));
  });
}

/** Resolves an error message when the downloaded bytes cannot be a decision archive, else null. */
async function archiveProblem(blob, filename) {
  if (!blob.size) return 'the runtime returned an empty archive.';
  if (!/\.jar$/i.test(filename)) return `“${filename}” is not a .jar file.`;
  const head = await readHead(blob);
  const isZip = head[0] === 0x50 && head[1] === 0x4b;
  return isZip ? null : `“${filename}” is not a valid Java archive.`;
}

/**
 * "Fetch from DI Runtime" of the Agent Diagram Editor: pick a decision service archive deployed on
 * a DI runtime and download it into the browser.
 *
 * Same runtime / deployment space / archive-card flow as the "Refresh Rules" section of the Agent
 * Management panel (DiRuntimeRulesRefresh), without any job interaction: the archive is fetched with
 * a GET (nothing is written on the server) and handed to `onFetched` as a File; it is uploaded next
 * to agents.json when the diagram is saved, and simply dropped if the editor is closed without saving.
 *
 * `currentJar` is the jar currently set on the agent; the matching archive is tagged "Current".
 */
export default function DiRuntimeArchiveFetch({ onFetched, currentJar = '' }) {
  const uid = useId();
  const [open, setOpen]                     = useState(false);
  const [runtimes, setRuntimes]             = useState(null); // null = not loaded
  const [runtimesError, setRuntimesError]   = useState(null);
  const [runtimeId, setRuntimeId]           = useState('');
  const [space, setSpace]                   = useState('');
  const [decisions, setDecisions]           = useState([]);
  const [decisionsState, setDecisionsState] = useState('idle'); // idle|loading|ready|error
  const [decisionsError, setDecisionsError] = useState(null);
  const [decisionId, setDecisionId]         = useState('');
  const [fetchState, setFetchState]         = useState('idle'); // idle|fetching|fetched|error|cancelled
  const [fetchMsg, setFetchMsg]             = useState(null);
  const abortRef   = useRef(null); // in-flight archive download
  const requestRef = useRef(0);    // latest decision-list request (older answers are ignored)

  useEffect(() => {
    if (!open || runtimes !== null) return;
    fetch(`${API_URL}/api/ads-runtimes`)
      .then(r => (r.ok ? r.json() : r.json().then(b => Promise.reject(new Error(b.error || r.statusText)))))
      .then(list => setRuntimes(Array.isArray(list) ? list : []))
      .catch(err => { setRuntimes([]); setRuntimesError(err.message); });
  }, [open, runtimes]);

  useEffect(() => () => abortRef.current?.abort(), []);

  const resetSelection = () => { setDecisionId(''); setFetchState('idle'); setFetchMsg(null); };

  const loadDecisions = async (rtId, spaceValue) => {
    const request = ++requestRef.current;
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
      if (request !== requestRef.current) return;
      setDecisions(Array.isArray(body) ? body : []);
      setDecisionsState('ready');
    } catch (err) {
      if (request !== requestRef.current) return;
      setDecisionsState('error');
      setDecisionsError(err.message);
    }
  };

  const onRuntimeChange = (rtId) => {
    requestRef.current += 1;
    setRuntimeId(rtId);
    resetSelection();
    setDecisions([]);
    setDecisionsState('idle');
    if (!rtId) { setSpace(''); return; }
    const seed = (runtimes || []).find(r => r.id === rtId)?.deploymentSpace || '';
    setSpace(seed);
    loadDecisions(rtId, seed);
  };

  const selectDecision = (id) => { setDecisionId(id); setFetchState('idle'); setFetchMsg(null); };

  const retryRuntimes = () => { setRuntimesError(null); setRuntimes(null); };

  const handleFetch = async () => {
    if (!runtimeId || !decisionId) return;
    const controller = new AbortController();
    abortRef.current = controller;
    setFetchState('fetching');
    setFetchMsg(null);
    try {
      const sp = space.trim();
      const spaceQuery = sp ? `&deploymentSpace=${encodeURIComponent(sp)}` : '';
      const r = await fetch(
        `${API_URL}/api/ads-runtimes/${encodeURIComponent(runtimeId)}/archive?decisionId=${encodeURIComponent(decisionId)}${spaceQuery}`,
        { signal: controller.signal },
      );
      if (!r.ok) {
        const body = await r.json().catch(() => ({}));
        throw new Error(body.error || r.statusText);
      }
      const filename = r.headers.get('X-Archive-Filename') || 'archive.jar';
      const blob = await r.blob();
      const problem = await archiveProblem(blob, filename);
      if (problem) {
        setFetchState('error');
        setFetchMsg(`Invalid archive: ${problem} The agent was left unchanged.`);
        return;
      }
      onFetched(new File([blob], filename, { type: 'application/java-archive' }));
      setFetchState('fetched');
      setFetchMsg(`Archive fetched (${filename}) — it is stored when you save the diagram.`);
    } catch (err) {
      if (controller.signal.aborted) {
        setFetchState('cancelled');
        setFetchMsg('Fetch cancelled. The agent was left unchanged.');
      } else {
        setFetchState('error');
        setFetchMsg(`Fetch failed: ${err.message}`);
      }
    } finally {
      if (abortRef.current === controller) abortRef.current = null;
    }
  };

  /** Cancel an ongoing download, otherwise close the panel and drop the selection. */
  const handleCancel = () => {
    if (abortRef.current) { abortRef.current.abort(); return; }
    requestRef.current += 1;
    setOpen(false);
    setRuntimeId('');
    setSpace('');
    setDecisions([]);
    setDecisionsState('idle');
    resetSelection();
  };

  const archives = useMemo(
    () => decisions.map(d => ({ ...d, ...describeArchive(d) })),
    [decisions],
  );
  const selected = archives.find(a => a.id === decisionId) || null;
  const currentName = currentJar ? currentJar.split('/').pop() : '';
  const currentId = useMemo(
    () => (currentName ? findArchiveForJar(archives, { filename: currentName }) : null),
    [archives, currentName],
  );
  const fetching = fetchState === 'fetching';
  const noteTone = fetchState === 'error' ? 'error' : fetchState === 'fetched' ? 'success' : 'warn';

  return (
    <div className="ade-field-group">
      <div className="ade-di-fetch-header">
        <button
          type="button"
          className="rr-btn"
          aria-expanded={open}
          aria-controls={`${uid}-panel`}
          onClick={() => (open ? handleCancel() : setOpen(true))}
        >
          ⬇ Fetch from DI Runtime
        </button>
        {!open && fetchState === 'fetched' && fetchMsg && (
          <span className="rr-header-chip" title={fetchMsg}>Fetched</span>
        )}
      </div>
      {open && (
        <fieldset id={`${uid}-panel`} className="agent-mgmt-refresh-fields ade-di-fetch-panel">
          {runtimesError && (
            <div className="rr-note rr-note--error rr-note--row" role="alert">
              <span>Could not load runtimes: {runtimesError}</span>
              <button type="button" className="rr-btn" onClick={retryRuntimes}>Retry</button>
            </div>
          )}
          {runtimes === null && !runtimesError && <div className="rr-note" role="status">Loading runtimes…</div>}
          {runtimes !== null && runtimes.length === 0 && !runtimesError && (
            <div className="rr-note">
              No DI runtimes configured. Add entries to <code>DI_RUNTIMES</code> in your <code>.env</code> file.
            </div>
          )}

          {/* Step 1 — where do the archives come from */}
          {runtimes !== null && runtimes.length > 0 && (
            <div className="rr-step">
              <div className="rr-step-head"><span className="rr-step-num">1</span>Source</div>
              <div className="rr-source">
                <div className="rr-field">
                  <label className="rr-label" htmlFor={`${uid}-runtime`}>DI Runtime</label>
                  <select
                    id={`${uid}-runtime`}
                    className="rr-input"
                    aria-label="Select DI runtime"
                    value={runtimeId}
                    disabled={fetching}
                    onChange={e => onRuntimeChange(e.target.value)}
                  >
                    <option value="">— select a runtime —</option>
                    {runtimes.map(rt => <option key={rt.id} value={rt.id}>{rt.name}</option>)}
                  </select>
                </div>
                {runtimeId && (
                  <div className="rr-field">
                    <label className="rr-label" htmlFor={`${uid}-space`}>Deployment Space</label>
                    <div className="rr-inline">
                      <input
                        id={`${uid}-space`}
                        className="rr-input"
                        type="text"
                        aria-label="Deployment space"
                        value={space}
                        placeholder="embedded"
                        spellCheck={false}
                        disabled={fetching}
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
                        disabled={!space.trim() || decisionsState === 'loading' || fetching}
                        onClick={() => loadDecisions(runtimeId, space)}
                      >
                        {decisionsState === 'loading' ? 'Loading…' : 'Load Decisions'}
                      </button>
                    </div>
                  </div>
                )}
              </div>
            </div>
          )}

          {/* Step 2 — choose the decision service archive */}
          {runtimes !== null && runtimes.length > 0 && (
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

              {decisionsState === 'ready' && archives.length === 0 && (
                <div className="rr-empty">
                  <strong>No decision service archive on this runtime</strong>
                  <span>Check the deployment space, then load again.</span>
                </div>
              )}

              {decisionsState === 'ready' && archives.length > 0 && (
                <ArchiveProjectBrowser
                  archives={archives}
                  decisionId={decisionId}
                  onSelect={selectDecision}
                  disabled={fetching}
                  radioName={`${uid}-decision`}
                  ariaLabelFor={a => (a.id === currentId ? `${a.label} (current)` : a.label)}
                  isHighlighted={a => a.id === currentId}
                  tagsFor={a => a.id === currentId && <span className="rr-card-active">Current</span>}
                />
              )}
            </div>
          )}

          {/* Step 3 — review and fetch */}
          <div className="rr-footer">
            <div className="rr-summary" aria-live="polite">
              {selected ? (
                <>
                  <span className="rr-summary-label">Ready to fetch</span>
                  <span className="rr-summary-value" title={selected.id}>{selected.label}</span>
                </>
              ) : (
                <span className="rr-summary-label">Select an archive to continue</span>
              )}
            </div>
            <div className="rr-footer-actions">
              {selected && !fetching && (
                <button type="button" className="rr-btn rr-btn--ghost" onClick={() => selectDecision('')}>
                  Clear
                </button>
              )}
              <button type="button" className="rr-btn rr-btn--ghost" onClick={handleCancel}>
                Cancel
              </button>
              <button
                type="button"
                className="rr-btn rr-btn--primary"
                disabled={!runtimeId || !decisionId || fetching}
                title="Download the selected archive into the editor; it is stored when you save the diagram"
                onClick={handleFetch}
              >
                {fetching ? 'Fetching…' : '⬇ Fetch Archive'}
              </button>
            </div>
          </div>

          {fetching && <div className="rr-note" role="status">Downloading {selected?.label || 'archive'} from the runtime…</div>}
          {fetchMsg && (
            <div
              className={`rr-note rr-note--${noteTone}`}
              role={fetchState === 'error' ? 'alert' : 'status'}
            >
              <span>{fetchMsg}</span>
            </div>
          )}
        </fieldset>
      )}
    </div>
  );
}
