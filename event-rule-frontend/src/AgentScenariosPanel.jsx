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
import React, { useState, useEffect, useRef, useCallback, Fragment } from 'react';
import { API_URL } from './config.js';

/**
 * AgentScenariosPanel
 *
 * Renders a panel listing every saved agent descriptor discovered via
 * GET /api/agents.  Each entry shows a "Deploy" button that submits the agent's
 * Flink job via POST /api/agents/:id/start, then polls GET /api/agents/:id/status
 * until the job reaches a running state.
 *
 * When a job fails the "Deploy" button is replaced by "✎ Edit" which opens
 * the AgentWizard pre-filled with that descriptor.
 *
 * Auto-refresh:
 *  - Polls GET /api/flink/status every POLL_MS ms.
 *  - When the set of running jobs changes the descriptor list is re-fetched so
 *    newly created agents appear automatically.
 *
 * Run-state is kept at the panel level (keyed by agent id) so it is NOT reset
 * when the agent list is re-fetched by the background Flink poller.  This
 * prevents the button from flickering back to "Deploy" while a job is still
 * being submitted or is polling for a RUNNING state.
 */

// States that count as "the job is running / warming up"
const RUNNING_STATES = new Set(['RUNNING', 'RESTARTING', 'CREATED', 'INITIALIZING', 'RECONCILING']);

const POLL_MS        = 3000; // Flink status poll interval
const STATUS_POLL_MS = 3000; // per-agent status poll while job is starting

// ── Per-agent run state machine ───────────────────────────────────────────────
// probing | idle | starting | polling | running | error
//
// The hook is now instantiated in AgentScenariosPanel (one per agent id) so
// that its state outlives individual AgentScenarioCard renders.

function useAgentRunState(agentId) {
  const [phase, setPhase]   = useState('probing');  // probing | idle | starting | polling | running | error
  const [jobId, setJobId]   = useState(null);
  const [errMsg, setErrMsg] = useState(null);
  const pollRef             = useRef(null);
  const mountedRef          = useRef(true);

  useEffect(() => {
    mountedRef.current = true;
    return () => {
      mountedRef.current = false;
      clearInterval(pollRef.current);
    };
  }, []);

  // Called once on panel mount to check current job state.
  // Sets phase to 'running' for active states, 'error' for terminal failure states,
  // and falls back to 'idle' when the job hasn't started or Flink is unreachable.
  const probe = useCallback(async () => {
    try {
      const r = await fetch(`${API_URL}/api/agents/${agentId}/status`);
      if (!mountedRef.current) return;
      const status = r.ok ? await r.json() : null;
      if (!mountedRef.current) return;
      if (status?.state && RUNNING_STATES.has(status.state)) {
        setJobId(status.jobId);
        setPhase('running');
      } else if (status?.state === 'FAILED' || status?.state === 'CANCELED') {
        setJobId(status.jobId);
        setErrMsg(`Job ended with state: ${status.state}`);
        setPhase('error');
      } else {
        setPhase('idle');
      }
    } catch { setPhase('idle'); /* Flink unreachable */ }
  }, [agentId]);

  const startPoll = useCallback((initialJobId) => {
    clearInterval(pollRef.current);
    pollRef.current = setInterval(async () => {
      try {
        const r = await fetch(`${API_URL}/api/agents/${agentId}/status`);
        if (!r.ok || !mountedRef.current) return;
        const status = await r.json();
        if (!mountedRef.current) return;
        if (status.state && RUNNING_STATES.has(status.state)) {
          setJobId(status.jobId || initialJobId);
          setPhase('running');
          clearInterval(pollRef.current);
        } else if (status.state === 'FAILED' || status.state === 'CANCELED') {
          setPhase('error');
          setErrMsg(`Job ended with state: ${status.state}`);
          clearInterval(pollRef.current);
        }
      } catch { /* keep polling */ }
    }, STATUS_POLL_MS);
  }, [agentId]);

  const run = useCallback(async () => {
    setPhase('starting');
    setErrMsg(null);
    try {
      const r = await fetch(`${API_URL}/api/agents/${agentId}/start`, { method: 'POST' });
      const body = await r.json();
      if (!r.ok) throw new Error(body.error || r.statusText);
      if (!mountedRef.current) return;
      setJobId(body.jobId);
      setPhase('polling');
      startPoll(body.jobId);
    } catch (err) {
      if (!mountedRef.current) return;
      // Log the full error for developer diagnostics; do not surface it in the UI.
      console.error(`[AgentScenariosPanel] Deployment failed for agentId=${agentId}:`, err.message);
      setPhase('error');
      setErrMsg('Deployment failed.');
    }
  }, [agentId, startPoll]);

  return { phase, jobId, errMsg, probe, run };
}

// ── Shared SVG icons (module-level to avoid per-render recreation) ────────────

const IconDeploy = () => (
  <svg width="11" height="11" viewBox="0 0 11 11" fill="currentColor" aria-hidden="true" style={{flexShrink:0}}>
    <path d="M2 1.5l7 4-7 4V1.5z"/>
  </svg>
);
const IconRunning = () => (
  <svg width="9" height="9" viewBox="0 0 9 9" fill="currentColor" aria-hidden="true" style={{flexShrink:0}}>
    <circle cx="4.5" cy="4.5" r="4.5"/>
  </svg>
);
const IconEdit = () => (
  <svg width="12" height="12" viewBox="0 0 12 12" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true" style={{flexShrink:0}}>
    <path d="M8.5 1.5l2 2-6 6H2.5v-2l6-6z"/>
  </svg>
);
const IconTrash = () => (
  <svg width="12" height="12" viewBox="0 0 12 12" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true" style={{flexShrink:0}}>
    <path d="M1.5 3h9M4 3V2a.5.5 0 01.5-.5h3A.5.5 0 018 2v1m1.5 0l-.5 7.5a.5.5 0 01-.5.5h-5a.5.5 0 01-.5-.5L2.5 3"/>
  </svg>
);

// ── Single agent card ─────────────────────────────────────────────────────────

// AgentScenarioCard renders one use-case tile.
// For multi-agent use-cases the tile shows a sub-list of individual agents,
// each with its own Edit button that opens the wizard pre-filled for that agent.
// For single-agent use-cases the edit button edits the use-case as a whole.
function AgentScenarioCard({ agent, runState, onEditAgent, onEditSpecificAgent, onAddAgentToJob, onDeleteAgent, onDeleteSpecificAgent, onSelectUseCase }) {
  const { phase, jobId, errMsg, run } = runState;

  const isProbing  = phase === 'probing';
  const isRunning  = phase === 'running';
  const isBusy     = phase === 'starting' || phase === 'polling';
  const isFailed   = phase === 'error';

  // Edit/delete is available whenever the job is NOT actively running or starting.
  // Running jobs must be stopped before the descriptor can be changed (the BFF enforces this).
  const canEdit = !isProbing && !isBusy && !isRunning;

  // Local confirm state: null | { type: 'use-case' } | { type: 'agent', agentId }
  const [confirm, setConfirm] = useState(null);
  const [deleteError, setDeleteError] = useState(null);
  const [deleteInFlight, setDeleteInFlight] = useState(false);

  const handleConfirmDelete = async () => {
    setDeleteInFlight(true);
    setDeleteError(null);
    try {
      if (confirm.type === 'use-case') {
        await onDeleteAgent(agent.id);
      } else {
        await onDeleteSpecificAgent(agent.id, confirm.agentId);
      }
      setConfirm(null);
      setDeleteInFlight(false);
    } catch (err) {
      setDeleteError(err.message);
      setDeleteInFlight(false);
      setConfirm(null);
    }
  };

  const agentList  = agent.agents || [];

  const confirmMsg = confirm?.type === 'use-case'
    ? `Delete Event Rule Application "${agent.id}" and all its descriptor files? This cannot be undone.`
    : `Remove agent "${confirm?.agentId}" from the application "${agent.id}"? This cannot be undone.`;

  const cleanTitle = agent.name || agent.id;

  const description = agent.description || 'No description added yet';

  const lastUpdated = agent.lastModified
    ? new Date(agent.lastModified).toLocaleString()
    : 'Unknown';

  return (
    <Fragment>
      {/* Inline confirm dialog — rendered above card, scoped to this card */}
      {confirm && (
        <div className="agent-confirm-overlay">
          <div className="agent-confirm-dialog">
            <p className="agent-confirm-message">{confirmMsg}</p>
            <div className="agent-confirm-actions">
              <button
                className="agent-mgmt-btn agent-mgmt-btn-ghost"
                onClick={() => setConfirm(null)}
                disabled={deleteInFlight}
              >
                Cancel
              </button>
              <button
                className="agent-mgmt-btn agent-mgmt-btn-danger"
                onClick={handleConfirmDelete}
                disabled={deleteInFlight}
              >
                {deleteInFlight ? 'Deleting…' : 'Yes, delete'}
              </button>
            </div>
          </div>
        </div>
      )}

      <div className={`di-agent-card${isRunning ? ' di-agent-card--running' : ''}${isFailed ? ' di-agent-card--failed' : ''}`}>
        <div className="di-agent-card-header">
          <div className="di-agent-card-title-group">
            <h2 className="di-agent-card-title">{cleanTitle}</h2>
            {agentList.length > 0 && (
              <span className="di-agent-count-badge" title={`${agentList.length} agent${agentList.length !== 1 ? 's' : ''} in this job`}>
                {agentList.length} {agentList.length === 1 ? 'agent' : 'agents'}
              </span>
            )}
            {isRunning && jobId && (
              <span className="di-job-badge" title={`Job ID: ${jobId}`}>
                {jobId.slice(0, 8)}…
              </span>
            )}
          </div>
          <button
            className="di-agent-card-menu-btn"
            disabled
            title="Options (coming soon)"
            aria-label="Options (coming soon)"
          >
            <svg width="16" height="16" viewBox="0 0 32 32" fill="currentColor">
              <circle cx="16" cy="8" r="2" />
              <circle cx="16" cy="16" r="2" />
              <circle cx="16" cy="24" r="2" />
            </svg>
          </button>
        </div>

        <div className="di-agent-card-id">{agent.id}</div>
        <p className="di-agent-card-description">{description}</p>

        {errMsg && <div className="di-agent-error">{errMsg}</div>}
        {deleteError && <div className="di-agent-error">{deleteError}</div>}

        {/* Per-agent sub-list */}
        {agentList.length > 0 && (
          <div className="di-agent-sub-list">
            {agentList.map(a => (
              <div key={a.agentId} className="di-agent-sub-row">
                <span className="di-agent-sub-name">{a.name || a.agentId}</span>
                {(canEdit && (onEditSpecificAgent || onDeleteSpecificAgent)) && (
                  <div className="di-agent-sub-actions">
                    {canEdit && onEditSpecificAgent && (
                      <button
                        className="di-sub-btn"
                        onClick={() => onEditSpecificAgent(agent.id, a.agentId)}
                        title={`Edit agent '${a.agentId}'`}
                      >
                        <IconEdit /> Edit
                      </button>
                    )}
                    {canEdit && onDeleteSpecificAgent && (
                      <button
                        className="di-sub-btn di-sub-btn--danger"
                        onClick={() => { setDeleteError(null); setConfirm({ type: 'agent', agentId: a.agentId }); }}
                        title={`Delete agent '${a.agentId}'`}
                      >
                        <IconTrash />
                      </button>
                    )}
                  </div>
                )}
              </div>
            ))}
          </div>
        )}

        <div className="di-agent-card-footer">
          <div className="di-card-meta">
            <span className="di-card-meta-label">Last updated:</span>
            <span className="di-card-meta-value">{lastUpdated}</span>
          </div>

          <div className="di-card-divider" />

          <div className="di-agent-card-actions">
            <div className="di-agent-card-actions-left">
              {/* Deploy / Running button */}
              {!isFailed && (
                <button
                  className={`di-btn-deploy${isRunning ? ' di-btn-deploy--running' : ''}`}
                  onClick={run}
                  disabled={isProbing || isBusy || isRunning}
                  title={isRunning ? `Job running (${jobId || '…'})` : `Deploy ${cleanTitle}`}
                >
                  {isRunning ? <IconRunning /> : (!isBusy && <IconDeploy />)}
                  {phase === 'starting' ? 'Submitting…' : phase === 'polling' ? 'Starting…' : isRunning ? 'Running' : 'Deploy'}
                </button>
              )}

              {/* Card-level Edit/Delete for single/legacy agents */}
              {agentList.length === 0 && (canEdit || isFailed) && (
                <button
                  className="di-btn-secondary-action"
                  onClick={() => onEditAgent(agent.id)}
                  title={`Edit descriptor for ${cleanTitle}`}
                >
                  <IconEdit /> Edit
                </button>
              )}

              {agentList.length === 0 && canEdit && onDeleteAgent && (
                <button
                  className="di-btn-secondary-action di-btn-secondary-action--danger"
                  onClick={() => { setDeleteError(null); setConfirm({ type: 'use-case' }); }}
                  title={`Delete agent "${cleanTitle}"`}
                >
                  <IconTrash /> Delete
                </button>
              )}

              {onAddAgentToJob && (
                <button
                  className="di-btn-secondary-action"
                  onClick={() => onAddAgentToJob(agent.id)}
                  title={`Add another agent to '${agent.id}'`}
                >
                  + Add Agent
                </button>
              )}
            </div>

            <div className="di-agent-card-actions-right">
              {/* View Dashboard Button — active only when Flink job is running */}
              <button
                className="di-btn-view-dashboard"
                onClick={() => onSelectUseCase && onSelectUseCase(agent.id)}
                disabled={!isRunning}
                title={isRunning ? `Open dashboard for ${cleanTitle}` : 'Dashboard is available when job is running'}
              >
                View Dashboard
              </button>
            </div>
          </div>
        </div>
      </div>
    </Fragment>
  );
}

// ── Wrapper that owns the run-state for a single agent ────────────────────────

function AgentScenarioCardWithState({ agent, onEditAgent, onEditSpecificAgent, onAddAgentToJob, onDeleteAgent, onDeleteSpecificAgent, onSelectUseCase }) {
  const runState = useAgentRunState(agent.id);
  // Keep a stable ref to probe so the mount effect runs exactly once regardless
  // of any future identity changes to the callback (M-3).
  const probeRef = useRef(runState.probe);
  useEffect(() => { probeRef.current = runState.probe; }, [runState.probe]);

  // Probe initial state exactly once on mount
  useEffect(() => { probeRef.current(); }, []);

  return (
    <AgentScenarioCard
      agent={agent}
      runState={runState}
      onEditAgent={onEditAgent}
      onEditSpecificAgent={onEditSpecificAgent}
      onAddAgentToJob={onAddAgentToJob}
      onDeleteAgent={onDeleteAgent}
      onDeleteSpecificAgent={onDeleteSpecificAgent}
      onSelectUseCase={onSelectUseCase}
    />
  );
}

// ── Single Agent List Row (for List View mode) ───────────────────────────────

function AgentScenarioListRowWithState({ agent, onEditAgent, onSelectUseCase }) {
  const runState = useAgentRunState(agent.id);
  const { phase, jobId, run } = runState;
  // Stable ref pattern — same fix as AgentScenarioCardWithState (M-3).
  const probeRef = useRef(runState.probe);
  useEffect(() => { probeRef.current = runState.probe; }, [runState.probe]);

  // Probe initial state exactly once on mount
  useEffect(() => { probeRef.current(); }, []);

  const isProbing  = phase === 'probing';
  const isRunning  = phase === 'running';
  const isBusy     = phase === 'starting' || phase === 'polling';
  const isFailed   = phase === 'error';
  const canEdit    = !isProbing && !isBusy && !isRunning;

  const cleanTitle = agent.name || agent.id;

  const description = agent.description || 'No description added yet';

  const lastUpdated = agent.lastModified
    ? new Date(agent.lastModified).toLocaleString()
    : 'Unknown';

  return (
    <div className="di-list-item">
      <div className="di-list-item-left">
        <div className="di-list-item-title-group">
          <h3 className="di-list-item-title">{cleanTitle}</h3>
          <span className="di-agent-card-id">({agent.id})</span>
          {isRunning && jobId && (
            <span className="di-job-badge">{jobId.slice(0, 8)}…</span>
          )}
        </div>
        <div className="di-list-item-desc">{description}</div>
        <div className="di-list-item-meta">Last updated: {lastUpdated}</div>
      </div>
      <div className="di-list-item-right">
        <button
          className="di-btn-view-dashboard"
          onClick={() => onSelectUseCase && onSelectUseCase(agent.id)}
          disabled={!isRunning}
          title={isRunning ? `Open dashboard for ${cleanTitle}` : 'Dashboard is available when job is running'}
        >
          View Dashboard
        </button>
        {!isFailed && (
          <button
            className={`di-btn-deploy${isRunning ? ' di-btn-deploy--running' : ''}`}
            onClick={run}
            disabled={isProbing || isBusy || isRunning}
          >
            {isRunning ? 'Running' : 'Deploy'}
          </button>
        )}
        {canEdit && (
          <button
            className="di-btn-secondary-action"
            onClick={() => onEditAgent(agent.id)}
          >
            Edit
          </button>
        )}
      </div>
    </div>
  );
}

// ── Panel ─────────────────────────────────────────────────────────────────────

function AgentScenariosPanel({ onEditAgent, onEditSpecificAgent, onAddAgentToJob, onDeleteAgent, onDeleteSpecificAgent, onSelectUseCase, refreshToken, searchQuery, sortBy, viewMode }) {
  const [agents,  setAgents]  = useState([]);
  const [loading, setLoading] = useState(true);
  const [error,   setError]   = useState(null);

  // Track last Flink job signature to detect additions/removals without re-rendering on every poll tick
  const lastSigRef = useRef(null);

  const fetchAgents = useCallback(async () => {
    try {
      const r = await fetch(`${API_URL}/api/agents`);
      if (!r.ok) throw new Error('Failed to fetch agents');
      const data = await r.json();
      setAgents(data);
      setError(null);
    } catch (err) {
      setError('Could not load agent descriptors.');
      console.error('[AgentScenariosPanel]', err);
    } finally {
      setLoading(false);
    }
  }, []);

  // Re-fetch immediately whenever the parent bumps refreshToken (after a
  // wizard save, edit, or successful delete). The initial value (0) fires
  // the first fetch on mount, so this replaces the standalone fetchAgents()
  // call that was previously inside the Flink-poll effect.
  useEffect(() => {
    fetchAgents();
  }, [fetchAgents, refreshToken]);

  useEffect(() => {
    // Seed the signature with the current Flink state before the first poll
    // tick fires, so that an unchanged job set never triggers a spurious
    // fetchAgents() call (which was resetting button states ~3 s after mount).
    let cancelled = false;
    fetch(`${API_URL}/api/flink/status`)
      .then(r => r.ok ? r.json() : null)
      .then(data => {
        if (!cancelled && data?.jobs) {
          lastSigRef.current = JSON.stringify([...data.jobs].sort());
        }
      })
      .catch(() => {});

    // Poll Flink status; re-fetch agent list when the running job set changes
    const intervalId = setInterval(async () => {
      try {
        const r = await fetch(`${API_URL}/api/flink/status`);
        if (!r.ok) return;
        const { jobs } = await r.json();
        const sig = JSON.stringify([...jobs].sort());
        if (sig !== lastSigRef.current) {
          lastSigRef.current = sig;
          fetchAgents();
        }
      } catch { /* Flink not reachable — keep polling */ }
    }, POLL_MS);

    return () => { cancelled = true; clearInterval(intervalId); };
  }, [fetchAgents]);

  if (loading) return null; // Don't flash a spinner — UseCaseSelector has its own loading state
  if (error)   return null; // Silently suppress — backend may not have any agents yet

  const filteredAgents = agents
    .filter(a => {
      if (!searchQuery?.trim()) return true;
      const q = searchQuery.toLowerCase();
      const name = (a.name || '').toLowerCase();
      const id = (a.id || '').toLowerCase();
      const desc = (a.description || '').toLowerCase();
      return name.includes(q) || id.includes(q) || desc.includes(q);
    })
    .sort((a, b) => {
      if (sortBy === 'name_asc') return (a.name || a.id || '').localeCompare(b.name || b.id || '');
      if (sortBy === 'name_desc') return (b.name || b.id || '').localeCompare(a.name || a.id || '');
      return 0; // Default recently updated
    });

  if (filteredAgents.length === 0) {
    return (
      <div className="di-empty-state">
        <p>No decision automations found.</p>
      </div>
    );
  }

  if (viewMode === 'list') {
    return (
      <div className="di-cards-list">
        {filteredAgents.map(agent => (
          <AgentScenarioListRowWithState
            key={agent.id}
            agent={agent}
            onEditAgent={onEditAgent}
            onEditSpecificAgent={onEditSpecificAgent}
            onAddAgentToJob={onAddAgentToJob}
            onDeleteAgent={onDeleteAgent}
            onDeleteSpecificAgent={onDeleteSpecificAgent}
            onSelectUseCase={onSelectUseCase}
          />
        ))}
      </div>
    );
  }

  return (
    <div className="di-cards-grid">
      {filteredAgents.map(agent => (
        <AgentScenarioCardWithState
          key={agent.id}
          agent={agent}
          onEditAgent={onEditAgent}
          onEditSpecificAgent={onEditSpecificAgent}
          onAddAgentToJob={onAddAgentToJob}
          onDeleteAgent={onDeleteAgent}
          onDeleteSpecificAgent={onDeleteSpecificAgent}
          onSelectUseCase={onSelectUseCase}
        />
      ))}
    </div>
  );
}

export default AgentScenariosPanel;
