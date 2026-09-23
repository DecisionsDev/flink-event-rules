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
import React, { useState, useEffect, useRef, useCallback } from 'react';
import './App.css';
import './UseCaseSelector.css';
import UseCaseSelector from './UseCaseSelector';
import AgentWizard from './AgentWizard';
import SendEventsPanel from './SendEventsPanel';
import AgentDiagramModal from './AgentDiagramModal';
import { renderCard, OutboundEmptyState } from './eventCardRegistry';
import { useKafkaNotifications } from './useKafkaNotifications';
import { useEventStream } from './useEventStream';
import NotificationContainer from './NotificationContainer';

// Configuration constants
const MAX_EVENTS_DISPLAY = 50;
const POLL_INTERVAL_MS   = 2000;

// ─── Normalise agent EventEnvelope shape ─────────────────────────────────────
// The agent pipeline emits events wrapped in an EventEnvelope:
//   { agentEventType, entityId, sourceTopic, timestamp, payload: { decision, ... } }
// Flatten payload up to top level so downstream card detectors and renderers
// work without knowing about the envelope.  Top-level envelope fields are
// preserved under their original names so they remain accessible if needed.

function normaliseEvent(event) {
  if (!event || typeof event !== 'object') return event;
  if (!event.agentEventType || typeof event.payload !== 'object') return event;
  return {
    ...event.payload,      // decision, riskScore, triggeredRules, originalTransaction, etc.
    agentEventType: event.agentEventType,
    entityId:       event.entityId,
    sourceTopic:    event.sourceTopic,
    timestamp:      event.payload.timestamp || event.timestamp,
  };
}


// ─── Outbound topic filtering ─────────────────────────────────────────────────
// Topics can declare a `filterKey` and `filterValue` in the descriptor to
// suppress events whose filterKey field equals filterValue.  A `filterLabel`
// provides the toggle text shown to the user.
//
// Example in descriptor.json outboundTopics:
//   { "type": "promotion-offer", "filterKey": "decision", "filterValue": "NO_ACTION",
//     "filterLabel": "NO_ACTION" }

function applyOutboundFilter(events, ot, show) {
  if (!ot.filterKey || !ot.filterValue) return { visible: events, hidden: 0 };
  if (show) return { visible: events, hidden: 0 };
  const visible = events.filter(e => {
    const ev = normaliseEvent(e);
    return (ev[ot.filterKey] ?? 'NO_ACTION') !== ot.filterValue;
  });
  return { visible, hidden: events.length - visible.length };
}

// ─── Agent Management Panel ───────────────────────────────────────────────────
// Shown on the dashboard for agent-based use cases. Provides Edit, Stop Job,
// Delete and Cancel (back to selector) operations, with confirmation dialogs
// for destructive actions.

const API_URL_APP = import.meta.env.VITE_API_URL || 'http://localhost:8080';

function ConfirmDialog({ message, confirmLabel, confirmDanger, onConfirm, onCancel }) {
  return (
    <div className="agent-confirm-overlay">
      <div className="agent-confirm-dialog">
        <p className="agent-confirm-message">{message}</p>
        <div className="agent-confirm-actions">
          <button className="agent-mgmt-btn agent-mgmt-btn-ghost" onClick={onCancel}>
            No, keep it
          </button>
          <button
            className={`agent-mgmt-btn ${confirmDanger ? 'agent-mgmt-btn-danger' : 'agent-mgmt-btn-primary'}`}
            onClick={onConfirm}
          >
            {confirmLabel}
          </button>
        </div>
      </div>
    </div>
  );
}

// agentId   — human-readable name shown in confirm dialogs (agents[0].agentId or folder name)
// useCaseId — use-case folder name used as the :id key in all BFF API calls
function AgentManagementPanel({ agentId, useCaseId, onEdit, onNavigateBack, agentsJson }) {
  const [confirm, setConfirm] = useState(null);          // null | 'cancel' | 'delete'
  const [cancelState, setCancelState] = useState('idle'); // idle | stopping | error
  const [deleteState, setDeleteState] = useState('idle'); // idle | deleting | error
  const [actionError, setActionError] = useState(null);
  const [showDiagram, setShowDiagram] = useState(false);

  // Calls POST /api/agents/:useCaseId/stop then navigates back.
  // A 404 from the BFF means no job is registered as running — treat as success.
  const handleCancelConfirmed = async () => {
    setConfirm(null);
    setCancelState('stopping');
    setActionError(null);
    try {
      const r = await fetch(`${API_URL_APP}/api/agents/${useCaseId}/stop`, { method: 'POST' });
      if (!r.ok && r.status !== 404) {
        const body = await r.json().catch(() => ({}));
        throw new Error(body.error || r.statusText);
      }
      onNavigateBack();
    } catch (err) {
      setCancelState('error');
      setActionError(`Cancel failed: ${err.message}`);
    }
  };

  const handleDeleteConfirmed = async () => {
    setConfirm(null);
    setDeleteState('deleting');
    setActionError(null);
    try {
      const r = await fetch(`${API_URL_APP}/api/agents/${useCaseId}`, { method: 'DELETE' });
      if (!r.ok) {
        const body = await r.json().catch(() => ({}));
        throw new Error(body.error || r.statusText);
      }
      onNavigateBack();
    } catch (err) {
      setDeleteState('error');
      setActionError(`Delete failed: ${err.message}`);
    }
  };

  const isBusy       = cancelState === 'stopping' || deleteState === 'deleting';
  const hasUseCaseId = useCaseId && useCaseId.length > 0;

  return (
    <>
      {showDiagram && (
        <AgentDiagramModal
          agentsJson={agentsJson}
          useCaseId={useCaseId}
          onClose={() => setShowDiagram(false)}
        />
      )}
      {confirm === 'cancel' && (
        <ConfirmDialog
          message={`Cancel the running Flink job for agent "${agentId}" and return to the use case selection screen?`}
          confirmLabel="Yes, cancel job"
          confirmDanger={true}
          onConfirm={handleCancelConfirmed}
          onCancel={() => setConfirm(null)}
        />
      )}
      {confirm === 'delete' && (
        <ConfirmDialog
          message={`Permanently delete agent "${agentId}" and all its descriptor files? Any running Flink job will be cancelled first. This cannot be undone.`}
          confirmLabel="Yes, delete agent"
          confirmDanger={true}
          onConfirm={handleDeleteConfirmed}
          onCancel={() => setConfirm(null)}
        />
      )}

      <div className="agent-mgmt-panel">
        <div className="agent-mgmt-panel-title">Agent Management</div>
        <div className="agent-mgmt-actions">
          <button
            className="agent-mgmt-btn agent-mgmt-btn-primary"
            onClick={onEdit}
            disabled={!hasUseCaseId || isBusy}
            title="Edit the agent descriptor"
          >
            Edit Agent
          </button>
          <button
            className="agent-mgmt-btn agent-mgmt-btn-ghost"
            onClick={() => setShowDiagram(true)}
            disabled={!agentsJson}
            title="View agent architecture diagram"
          >
            Diagram View
          </button>
          <button
            className="agent-mgmt-btn agent-mgmt-btn-danger"
            onClick={() => setConfirm('delete')}
            disabled={!hasUseCaseId || isBusy}
            title="Delete agent and all descriptor files"
          >
            {deleteState === 'deleting' ? 'Deleting…' : 'Delete Agent'}
          </button>
          <button
            className="agent-mgmt-btn agent-mgmt-btn-ghost"
            onClick={() => setConfirm('cancel')}
            disabled={isBusy}
            title="Cancel the Flink job and return to use case selection"
          >
            {cancelState === 'stopping' ? 'Undeploying…' : 'Undeploy Agent'}
          </button>
        </div>
        {actionError && (
          <div className="agent-mgmt-error">{actionError}</div>
        )}
      </div>
    </>
  );
}

// ─── App ──────────────────────────────────────────────────────────────────────

// ── Notification configuration ────────────────────────────────────────────────
// Centralised so behaviour can be adjusted without touching component code.
const NOTIFICATION_CONFIG = {
  maxVisible:    5,
  autoDismissMs: 5000,
};

function App() {
  const [selectedUseCase, setSelectedUseCase] = useState(null);
  const [showWizard, setShowWizard] = useState(false);
  const [editDescriptor, setEditDescriptor] = useState(null);
  // useCaseId, targetAgentId, and wizardMode are passed to the wizard when opening in
  // edit mode (useCaseId + targetAgentId) or append mode (useCaseId + wizardMode="append").
  const [wizardUseCaseId, setWizardUseCaseId] = useState(null);
  const [wizardTargetAgentId, setWizardTargetAgentId] = useState(null);
  const [wizardMode, setWizardMode] = useState('replace');
  // refreshToken — bumped whenever the agent list should be re-fetched (after
  // create, edit, or delete). AgentScenariosPanel watches this value and calls
  // fetchAgents() when it changes.
  const [refreshToken, setRefreshToken] = useState(0);
  const [descriptor, setDescriptor] = useState(null);
  const [agentsJson, setAgentsJson] = useState(null);
  // Per-topic show-filtered toggle state, keyed by topic type.
  // Used when a descriptor outbound topic declares a filterKey/filterValue.
  const [showFiltered, setShowFiltered] = useState({});
  // Whether to display changelog topics (type: "entity-changelog") in the dashboard.
  // Off by default to reduce noise.
  const [showChangelogs, setShowChangelogs] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  // ── Live event stream (WebSocket push with HTTP-poll fallback) ───────────────
  const { eventsByType, setEventsByType, error: streamError } = useEventStream(selectedUseCase, API_URL_APP, {
    pollIntervalMs: POLL_INTERVAL_MS,
  });
  const [cleaningTopics, setCleaningTopics] = useState(false);
  const [cleanTopicsError, setCleanTopicsError] = useState(null);


  // ── Generic Kafka event notifications ───────────────────────────────────────
  // useKafkaNotifications compares successive poll snapshots to detect new events
  // and emits generic notification records — no domain-specific logic here.
  const { notifications, dismissNotification, autoDismissMs } = useKafkaNotifications(
    eventsByType,
    descriptor,
    {
      ...NOTIFICATION_CONFIG,
      showChangelogs,
    },
  );

  // ── Split-pane state (dashboard only) ───────────────────────────────────
  const SPLITTER_MIN_LEFT  = 220;
  const SPLITTER_MIN_RIGHT = 320;
  const [leftWidth, setLeftWidth] = useState(null); // null = use CSS default (45%)
  const isDragging = useRef(false);
  const splitContainerRef = useRef(null);

  const onSplitterMouseDown = useCallback((e) => {
    e.preventDefault();
    isDragging.current = true;
    document.body.style.cursor = 'col-resize';
    document.body.style.userSelect = 'none';

    const onMouseMove = (mv) => {
      if (!isDragging.current || !splitContainerRef.current) return;
      const rect = splitContainerRef.current.getBoundingClientRect();
      const newLeft = mv.clientX - rect.left;
      const maxLeft = rect.width - SPLITTER_MIN_RIGHT - 6;
      setLeftWidth(Math.max(SPLITTER_MIN_LEFT, Math.min(newLeft, maxLeft)));
    };

    const onMouseUp = () => {
      isDragging.current = false;
      document.body.style.cursor = '';
      document.body.style.userSelect = '';
      document.removeEventListener('mousemove', onMouseMove);
      document.removeEventListener('mouseup', onMouseUp);
    };

    document.addEventListener('mousemove', onMouseMove);
    document.addEventListener('mouseup', onMouseUp);
  }, []);

  // ── Descriptor + agents.json fetch ──────────────────────────────────────────
  // Event data comes from useEventStream above; only metadata is fetched here.
  useEffect(() => {
    if (!selectedUseCase) return;
    let cancelled = false;
    setLoading(true);
    setError(null);
    fetch(`${API_URL_APP}/api/use-cases/${selectedUseCase}/descriptor`)
      .then(r => { if (!r.ok) throw new Error(r.statusText); return r.json(); })
      .then(desc => { if (!cancelled) { setDescriptor(desc); setLoading(false); } })
      .catch(err => { if (!cancelled) { setError(`Failed to load descriptor: ${err.message}`); setLoading(false); } });
    // Fetch agents.json for the diagram (best-effort — no error shown on failure).
    fetch(`${API_URL_APP}/api/agents/${selectedUseCase}`)
      .then(r => { if (!r.ok) throw new Error(r.statusText); return r.json(); })
      .then(data => { if (!cancelled) setAgentsJson(data); })
      .catch(() => { /* diagram simply stays disabled */ });
    return () => { cancelled = true; };
  }, [selectedUseCase]);

  // Surface stream errors in the main error state
  useEffect(() => {
    if (streamError) setError(streamError);
  }, [streamError]);

  const handleUseCaseSelect = (useCaseId) => {
    setSelectedUseCase(useCaseId);
    setLoading(true);
  };

  const handleChangeUseCase = () => {
    setSelectedUseCase(null);
    setDescriptor(null);
    setAgentsJson(null);
    setShowFiltered({});
    setShowChangelogs(false);
  };

  // handleCleanTopics — purges all Kafka topic messages for the selected use case
  // and clears the in-memory event store on the server, then empties the UI panels.
  const handleCleanTopics = async () => {
    if (!selectedUseCase || cleaningTopics) return;
    setCleaningTopics(true);
    setCleanTopicsError(null);
    try {
      const r = await fetch(
        `${API_URL_APP}/api/use-cases/${selectedUseCase}/reset-topics`,
        { method: 'POST' }
      );
      if (!r.ok) {
        const body = await r.json().catch(() => ({}));
        throw new Error(body.error || r.statusText);
      }
      // Immediately clear the frontend event state to update UI synchronously
      if (setEventsByType) {
        setEventsByType({});
      }
    } catch (err) {
      setCleanTopicsError(`Clean failed: ${err.message}`);
    } finally {
      setCleaningTopics(false);
    }
  };



  // handleEditAgent — opens the wizard to edit the first (or only) agent in a use-case.
  // Called from the dashboard "Edit Agent" button and from failed-job cards.
  // Guard against SyntheticEvent being passed as the first argument.
  const handleEditAgent = async (useCaseId) => {
    const id = (typeof useCaseId === 'string' ? useCaseId : null) || selectedUseCase;
    try {
      const r = await fetch(`${API_URL_APP}/api/agents/${id}`);
      if (!r.ok) throw new Error(r.statusText);
      const agentsJson = await r.json();
      setEditDescriptor(agentsJson);
      // Determine the correct save mode:
      //   - single-agent use-case → replace (overwrite entire file)
      //   - multi-agent use-case  → patch   (update only agents[0], preserve siblings)
      const isMulti = (agentsJson.agents || []).length > 1;
      setWizardTargetAgentId(isMulti ? agentsJson.agents[0].agentId : null);
      setWizardMode(isMulti ? 'patch' : 'replace');
    } catch {
      setEditDescriptor(null);
      setWizardTargetAgentId(null);
      setWizardMode('replace');
    }
    setWizardUseCaseId(id);
    setShowWizard(true);
  };

  // handleEditSpecificAgent — opens the wizard to edit a named agent within a use-case.
  // Called from AgentScenariosPanel when the user clicks "✎ Edit" on a specific agent card.
  // useCaseId: folder key; targetAgentId: the agentId to edit within the file.
  const handleEditSpecificAgent = async (useCaseId, targetAgentId) => {
    try {
      const r = await fetch(`${API_URL_APP}/api/agents/${useCaseId}`);
      if (!r.ok) throw new Error(r.statusText);
      const agentsJson = await r.json();
      setEditDescriptor(agentsJson);
    } catch {
      setEditDescriptor(null);
    }
    setWizardUseCaseId(useCaseId);
    setWizardTargetAgentId(targetAgentId);
    // Always use patch mode when editing a specific named agent —
    // this ensures siblings are never disturbed regardless of agent count.
    setWizardMode('patch');
    setShowWizard(true);
  };

  // handleDeleteAgent — deletes the entire use-case (single-agent).
  // Calls DELETE /api/agents/:useCaseId which stops the job, purges topics,
  // removes all descriptor files, and tears down the folder.
  // On success: closes the wizard (if open) and bumps refreshToken so the panel re-fetches.
  // Throws on failure so the card can surface the error inline (wizard stays open).
  const handleDeleteAgent = async (useCaseId) => {
    const r = await fetch(`${API_URL_APP}/api/agents/${useCaseId}`, { method: 'DELETE' });
    if (!r.ok) {
      const body = await r.json().catch(() => ({}));
      throw new Error(body.error || r.statusText);
    }
    // Close the wizard if it was open for this use-case (or any use-case — a
    // deleted agent's wizard should never remain visible).
    if (showWizard) handleWizardClose();
    setRefreshToken(t => t + 1);
  };

  // handleDeleteSpecificAgent — removes one agent from a multi-agent use-case.
  // Calls DELETE /api/agents/:useCaseId/agents/:agentId which updates agents.json
  // in-place (or does a full teardown when the last agent is removed).
  // On success: closes the wizard (if open) and bumps refreshToken.
  // Throws on failure so the card can surface the error inline (wizard stays open).
  const handleDeleteSpecificAgent = async (useCaseId, agentId) => {
    const r = await fetch(`${API_URL_APP}/api/agents/${useCaseId}/agents/${agentId}`, { method: 'DELETE' });
    if (!r.ok) {
      const body = await r.json().catch(() => ({}));
      throw new Error(body.error || r.statusText);
    }
    if (showWizard) handleWizardClose();
    setRefreshToken(t => t + 1);
  };

  // Opens the wizard in append mode pre-filled with the given use-case id,
  // so the user adds a second agent to an existing Flink Job.
  const handleAddAgentToJob = (useCaseId) => {
    setEditDescriptor(null);          // blank form — new agent
    setWizardUseCaseId(useCaseId);
    setWizardTargetAgentId(null);
    setWizardMode('append');
    setShowWizard(true);
  };

  const handleWizardClose = () => {
    setShowWizard(false);
    setEditDescriptor(null);
    setWizardUseCaseId(null);
    setWizardTargetAgentId(null);
    setWizardMode('replace');
    // Bump the refresh token so AgentScenariosPanel re-fetches after every
    // wizard session (create, edit, or cancel — harmless to re-fetch on cancel).
    setRefreshToken(t => t + 1);
  };

  // ── Use Case Selector screen ─────────────────────────────────────────────

  if (showWizard) {
    return (
      <AgentWizard
        onClose={handleWizardClose}
        initialDescriptor={editDescriptor}
        initialUseCaseId={wizardUseCaseId}
        initialTargetAgentId={wizardTargetAgentId}
        initialMode={wizardMode}
      />
    );
  }

  if (!selectedUseCase) {
    return (
      <div className="App di-view">
        <main className="App-main App-main--fullwidth">
          <UseCaseSelector
            onSelectUseCase={handleUseCaseSelect}
            onCreateDescriptor={() => {
              setEditDescriptor(null);
              setWizardUseCaseId(null);
              setWizardTargetAgentId(null);
              setWizardMode('replace');
              setShowWizard(true);
            }}
            onEditAgent={handleEditAgent}
            onEditSpecificAgent={handleEditSpecificAgent}
            onAddAgentToJob={handleAddAgentToJob}
            onDeleteAgent={handleDeleteAgent}
            onDeleteSpecificAgent={handleDeleteSpecificAgent}
            refreshToken={refreshToken}
          />
        </main>
      </div>
    );
  }

  // ── Dashboard ─────────────────────────────────────────────────────────

  return (
    <div className="App">
      {/* Generic event notification overlay — domain-agnostic, always rendered */}
      <NotificationContainer
        notifications={notifications}
        onDismiss={dismissNotification}
        autoDismissMs={autoDismissMs}
      />

      <header className="di-header">
        <div className="di-header-left">
          <button className="di-header-brand di-header-brand--clickable" onClick={handleChangeUseCase}>
            <span className="di-header-brand-ibm">IBM</span>
            <span className="di-header-brand-title">Decision Intelligence - Event Rules</span>
          </button>
          {descriptor?.name && (
            <div className="di-header-brand">
              <span className="di-header-brand-sep">/</span>
              <span className="di-header-brand-usecase">{descriptor.name}</span>
            </div>
          )}
        </div>
        <div className="di-header-right">
          <button className="di-header-action" title="Back to applications" aria-label="Back to applications" onClick={handleChangeUseCase}>            
            <svg width="18" height="18" viewBox="0 0 32 32" fill="currentColor">
              <path d="M19 26l-10-10 10-10 1.414 1.414L11.828 16l8.586 8.586L19 26z"/>
            </svg>
          </button>
          <button className="di-header-action" title="Settings" aria-label="Settings">
            <svg width="18" height="18" viewBox="0 0 32 32" fill="currentColor">
              <path d="M29.41 14.16l-2.61-.43a11.13 11.13 0 00-.91-2.19l1.55-2.18a1 1 0 00-.14-1.3l-2.67-2.67a1 1 0 00-1.3-.14l-2.18 1.55a11.13 11.13 0 00-2.19-.91l-.43-2.61A1 1 0 0017.65 2h-3.3a1 1 0 00-.99.84l-.43 2.61a11.13 11.13 0 00-2.19.91L8.56 4.81a1 1 0 00-1.3.14L4.59 7.62a1 1 0 00-.14 1.3l1.55 2.18a11.13 11.13 0 00-.91 2.19l-2.61.43A1 1 0 001.5 14.72v3.3a1 1 0 00.84.99l2.61.43a11.13 11.13 0 00.91 2.19L4.31 23.81a1 1 0 00.14 1.3l2.67 2.67a1 1 0 001.3.14l2.18-1.55a11.13 11.13 0 002.19.91l.43 2.61a1 1 0 00.99.84h3.3a1 1 0 00.99-.84l.43-2.61a11.13 11.13 0 002.19-.91l2.18 1.55a1 1 0 001.3-.14l2.67-2.67a1 1 0 00.14-1.3l-1.55-2.18a11.13 11.13 0 00.91-2.19l2.61-.43a1 1 0 00.84-.99v-3.3a1 1 0 00-.84-.86zM16 21a5 5 0 115-5 5 5 0 01-5 5z"/>
            </svg>
          </button>
          <div className="di-user-avatar" title="John Doe">JD</div>
        </div>
      </header>

      <main className="App-main App-main--split">
        {loading && <div className="loading">Loading dashboard…</div>}
        {error && (
          <div className="error">
            {error}
          </div>
        )}

        {!loading && descriptor && (
          <div className="dashboard-split" ref={splitContainerRef}>
            {/* ── Left pane: Agent management + Send Events (scenarios) ── */}
            <div className="dashboard-left" style={leftWidth !== null ? { width: leftWidth, minWidth: SPLITTER_MIN_LEFT, flex: 'none' } : undefined}>
              <AgentManagementPanel
                agentId={selectedUseCase}
                useCaseId={selectedUseCase}
                onEdit={handleEditAgent}
                onNavigateBack={handleChangeUseCase}
                agentsJson={agentsJson}
              />
              <SendEventsPanel useCase={selectedUseCase} descriptor={descriptor} />
            </div>

            {/* ── Splitter ─────────────────────────────────────────────── */}
            <div
              className="dashboard-splitter"
              onMouseDown={onSplitterMouseDown}
              title="Drag to resize"
            />

            {/* ── Right pane: Topic panels ──────────────────────────────── */}
            <div className="dashboard-right">
              {/* Right-pane toolbar: changelog toggle + clean topics button */}
              <div className="dashboard-right-toolbar">
                {(descriptor.outboundTopics || []).some(ot => ot.type === 'entity-changelog' || ot.type === 'agent-error') && (
                  <label
                    className="toggle-label"
                    onClick={e => { e.preventDefault(); setShowChangelogs(v => !v); }}
                  >
                    <span className="toggle-switch">
                      <input
                        type="checkbox"
                        checked={showChangelogs}
                        onChange={() => {}}
                        tabIndex={-1}
                        aria-hidden="true"
                      />
                      <span className="toggle-slider" />
                    </span>
                    Show changelog & errors topics
                  </label>
                )}
                <div className="clean-topics-control">
                  <button
                    className="clean-topics-btn"
                    onClick={handleCleanTopics}
                    disabled={cleaningTopics}
                    title="Purge all Kafka topic messages for this use case and clear the event panels"
                  >
                    {cleaningTopics ? (
                      <>
                        <span className="clean-topics-spinner" aria-hidden="true" />
                        Cleaning…
                      </>
                    ) : (
                      <>
                        <svg width="13" height="13" viewBox="0 0 32 32" fill="currentColor" aria-hidden="true" style={{ flexShrink: 0 }}>
                          <path d="M24 4H8L2 12l14 16 14-16-6-8zm-2.5 2l3.5 4.78V18H7v-11.2L10.5 6h11zM7.34 18h17.32l-8.66 9.93L7.34 18z"/>
                        </svg>
                        Clean topics
                      </>
                    )}
                  </button>
                  {cleanTopicsError && (
                    <span className="clean-topics-error" role="alert">{cleanTopicsError}</span>
                  )}
                </div>
              </div>

              {/* ── Build topic role map: which topics are input-only, output-only, or both ── */}
              {(() => {
                const inboundNames  = new Set((descriptor.topics || []).map(t => t.name));
                const outboundNames = new Set((descriptor.outboundTopics || []).map(t => t.name));
                // role: 'input' | 'output' | 'both'
                const topicRole = (name) => {
                  const isIn  = inboundNames.has(name);
                  const isOut = outboundNames.has(name);
                  if (isIn && isOut) return 'both';
                  if (isOut)        return 'output';
                  return 'input';
                };

                // Topics that appear in both lists — rendered once (from the inbound list),
                // skipped when iterating outboundTopics.
                const sharedNames = new Set(
                  [...inboundNames].filter(n => outboundNames.has(n))
                );

                return (
                  <>
                    {/* Inbound topic sections (includes shared topics, rendered once) */}
                    {(descriptor.topics || []).map((topic) => {
                      const role   = topicRole(topic.name);
                      const events = eventsByType[topic.name] || [];
                      const displayEvents = events.slice(-MAX_EVENTS_DISPLAY).reverse();

                      // For shared topics also apply the outbound filter if one is declared
                      const ot = role === 'both'
                        ? (descriptor.outboundTopics || []).find(o => o.name === topic.name)
                        : null;
                      const show = showFiltered[topic.name] || false;
                      const { visible: visibleEvents, hidden: hiddenCount } =
                        ot ? applyOutboundFilter(events, ot, show) : { visible: events, hidden: 0 };
                      const hasFilter = !!(ot?.filterKey && ot?.filterValue);
                      const displayFiltered = visibleEvents.slice(-MAX_EVENTS_DISPLAY).reverse();

                      return (
                        <section key={topic.name} className={`data-section topic-role--${role}`}>
                          <h2>
                            <span className={`topic-role-badge topic-role-badge--${role}`}>
                              {role === 'input' ? 'Input' : role === 'output' ? 'Output' : 'Input & Output'}
                            </span>
                            {topic.label}
                            <span style={{ marginLeft: 'auto', fontSize: '0.72rem', fontWeight: 600, color: '#9ca3af', background: '#f8fafc', border: '1px solid #e5e7eb', borderRadius: '10px', padding: '2px 8px' }}>
                              {events.length} received
                            </span>
                          </h2>

                          {hasFilter && (
                            <div className="outbound-toolbar">
                              <span className="data-count">
                                {visibleEvents.length} shown
                                {hiddenCount > 0 && (
                                  <span className="filtered-count"> · {hiddenCount} {ot.filterLabel || ot.filterValue} hidden</span>
                                )}
                              </span>
                              <label
                                className="toggle-label"
                                onClick={e => { e.preventDefault(); setShowFiltered(prev => ({ ...prev, [topic.name]: !prev[topic.name] })); }}
                              >
                                <span className="toggle-switch">
                                  <input type="checkbox" checked={show} onChange={() => {}} tabIndex={-1} aria-hidden="true" />
                                  <span className="toggle-slider" />
                                </span>
                                Show {ot.filterLabel || ot.filterValue}
                              </label>
                            </div>
                          )}

                          <div className="data-list">
                            {(role === 'both' ? visibleEvents : events).length === 0 ? (
                              <p>No events received yet — waiting for Kafka messages…</p>
                            ) : (
                              displayFiltered.map((event, index) =>
                                renderCard(normaliseEvent(event), topic.type, index)
                              )
                            )}
                          </div>
                        </section>
                      );
                    })}

                    {/* Outbound / results sections — skip topics already shown above */}
                    {(descriptor.outboundTopics || [])
                      .filter(ot => !sharedNames.has(ot.name))
                      .filter(ot => showChangelogs || (ot.type !== 'entity-changelog' && ot.type !== 'agent-error'))
                      .map(ot => {
                        const allEvents = eventsByType[ot.name] || [];
                        const show = showFiltered[ot.name] || false;
                        const { visible: visibleEvents, hidden: hiddenCount } =
                          applyOutboundFilter(allEvents, ot, show);
                        const displayEvents = visibleEvents.slice(-MAX_EVENTS_DISPLAY).reverse();
                        const hasFilter     = !!(ot.filterKey && ot.filterValue);

                        return (
                          <section key={ot.name} className="data-section topic-role--output">
                            <h2>
                              <span className="topic-role-badge topic-role-badge--output">Output</span>
                              {ot.label}
                              <span style={{ marginLeft: 'auto', fontSize: '0.72rem', fontWeight: 600, color: '#9ca3af', background: '#f8fafc', border: '1px solid #e5e7eb', borderRadius: '10px', padding: '2px 8px' }}>
                                {allEvents.length} received
                              </span>
                            </h2>

                            {hasFilter && (
                              <div className="outbound-toolbar">
                                <span className="data-count">
                                  {visibleEvents.length} shown
                                  {hiddenCount > 0 && (
                                    <span className="filtered-count"> · {hiddenCount} {ot.filterLabel || ot.filterValue} hidden</span>
                                  )}
                                </span>
                                <label
                                  className="toggle-label"
                                  onClick={e => { e.preventDefault(); setShowFiltered(prev => ({ ...prev, [ot.name]: !prev[ot.name] })); }}
                                >
                                  <span className="toggle-switch">
                                    <input type="checkbox" checked={show} onChange={() => {}} tabIndex={-1} aria-hidden="true" />
                                    <span className="toggle-slider" />
                                  </span>
                                  Show {ot.filterLabel || ot.filterValue}
                                </label>
                              </div>
                            )}

                            <div className="data-list">
                              {visibleEvents.length === 0 ? (
                                <OutboundEmptyState topicType={ot.type} />
                              ) : (
                                displayEvents.map((event, index) =>
                                  renderCard(normaliseEvent(event), ot.type, index)
                                )
                              )}
                            </div>
                          </section>
                        );
                      })
                    }
                  </>
                );
              })()}
            </div>
          </div>
        )}
      </main>

      <footer className="App-footer">
        <p>Event Rule Application — Streaming from Kafka</p>
      </footer>
    </div>
  );
}

export default App;
