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
import SampleEventModal from './SampleEventModal';
import ScenarioTimeline from './ScenarioTimeline';
import { API_URL } from './config.js';

/**
 * SendEventsPanel — lets the demo operator inject pre-built scenarios into
 * the live Kafka pipeline with a single button click, or generate and send
 * a custom event from a live JSON editor.
 *
 * For each scenario it calls POST /api/use-cases/:useCase/events for every
 * distinct topic referenced in the scenario's event arrays.
 *
 * CRUD support:
 *   • Save Scenario  — PUT /api/use-cases/:useCase/scenarios/:id
 *   • Add New        — POST /api/use-cases/:useCase/scenarios  (opens inline form)
 *   • Delete         — DELETE /api/use-cases/:useCase/scenarios/:id (with confirm)
 *
 * Sample generation:
 *   The "Generate Sample Event" button is shown when the descriptor exposes at
 *   least one inbound topic and the BFF has a fat JAR available.  It is driven
 *   by the descriptor passed in as a prop — no `agent-` prefix check needed.
 */


/**
 * Build the serialisable payload object for a scenario so it can be shown
 * in the JSON editor.  The scenario may carry events under any key(s) — we
 * serialise the full scenario payload as-is, stripping only non-payload
 * metadata fields (id, label, description, topic) that are not sent to Kafka.
 */
function scenarioToEditorPayload(scenario) {
  const META_KEYS = new Set(['id', 'label', 'description']);
  const payload = {};
  for (const [k, v] of Object.entries(scenario)) {
    if (!META_KEYS.has(k)) payload[k] = v;
  }
  return Object.keys(payload).length === 0 ? { events: [] } : payload;
}

/**
 * Flatten a scenario payload into an ordered list of { topic, payload } items,
 * preserving the declaration order so Kafka receives them in sequence.
 *
 * Supports two scenario payload conventions:
 *
 * 1. Per-event topic routing — `events` is an array where each item carries its
 *    own `topic` (and an optional `payload` wrapper for the actual Kafka body).
 *    This is the format used by multi-topic scenarios (e.g. banking-fraud-agent):
 *      { events: [{ type, topic, payload: {...} }, ...] }
 *
 * 2. <EventType>Events keys — keys that end with "Events" are treated as event
 *    arrays routed to the topic whose logical type matches the key prefix.
 *    e.g. `accountEvents` → topicFor("account") → the Kafka topic name.
 *    This is the convention for domain-specific scenario files.
 *
 * 3. Bare `events` array with a top-level `topic` — generic single-topic list.
 */
function scenarioToOrderedEvents(parsed, topicFor) {
  const items = [];

  for (const [key, value] of Object.entries(parsed)) {
    if (!Array.isArray(value) || value.length === 0) continue;

    if (key === 'events') {
      const hasPerEventTopic = value.every(e => e && typeof e === 'object' && typeof e.topic === 'string');
      if (hasPerEventTopic) {
        // Per-event topic: preserve order, unwrap `payload` wrapper when present.
        for (const e of value) {
          items.push({ schemaType: e.type || null, topic: e.topic, payload: e.payload !== undefined ? e.payload : e });
        }
      } else {
        // Generic single-topic list.
        const topic = parsed.topic || topicFor('events');
        for (const e of value) {
          items.push({ schemaType: null, topic, payload: e });
        }
      }
    } else if (key.endsWith('Events')) {
      const eventType = key.slice(0, -6); // strip trailing "Events"
      const kebab     = eventType.replace(/([A-Z])/g, c => `-${c.toLowerCase()}`);
      const topic     = topicFor(eventType) || topicFor(kebab);
      if (topic) {
        for (const e of value) {
          items.push({ schemaType: eventType, topic, payload: e });
        }
      }
    }
  }

  return items;
}

/** Derive a URL-safe id slug from a label string */
function labelToId(label) {
  return label
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
}

// ── New Scenario inline form ──────────────────────────────────────────────────

function NewScenarioForm({ onSave, onCancel, existingIds }) {
  const [label, setLabel]       = useState('');
  const [desc,  setDesc]        = useState('');
  const [idErr, setIdErr]       = useState('');

  const derivedId = labelToId(label);

  const handleSave = () => {
    if (!label.trim()) { setIdErr('Name is required'); return; }
    if (!derivedId)    { setIdErr('Name must produce a valid id'); return; }
    if (existingIds.has(derivedId)) {
      setIdErr(`A scenario with id "${derivedId}" already exists`);
      return;
    }
    setIdErr('');
    onSave({ id: derivedId, label: label.trim(), description: desc.trim() || 'New scenario' });
  };

  return (
    <div className="scenario-new-form">
      <div className="scenario-new-form-title">New Scenario</div>
      <div className="scenario-new-form-fields">
        <div className="scenario-new-form-field">
          <label className="scenario-editor-label" htmlFor="new-scenario-name">
            Name <span className="scenario-new-id-preview">{derivedId ? `→ id: ${derivedId}` : ''}</span>
          </label>
          <input
            id="new-scenario-name"
            className={`scenario-new-input${idErr ? ' scenario-new-input-error' : ''}`}
            type="text"
            value={label}
            onChange={e => { setLabel(e.target.value); setIdErr(''); }}
            placeholder="e.g. High-risk late-night purchase"
            autoFocus
          />
          {idErr && <div className="scenario-editor-error-msg">{idErr}</div>}
        </div>
        <div className="scenario-new-form-field">
          <label className="scenario-editor-label" htmlFor="new-scenario-desc">Description (optional)</label>
          <input
            id="new-scenario-desc"
            className="scenario-new-input"
            type="text"
            value={desc}
            onChange={e => setDesc(e.target.value)}
            placeholder="Short description shown on the scenario card"
          />
        </div>
      </div>
      <div className="scenario-new-form-footer">
        <button className="scenario-new-cancel-btn" onClick={onCancel}>Cancel</button>
        <button className="scenario-new-save-btn" onClick={handleSave} disabled={!label.trim()}>
          Create Scenario
        </button>
      </div>
    </div>
  );
}

// ── Main component ────────────────────────────────────────────────────────────

/**
 * Props:
 *   useCase   {string}            — use-case folder id
 *   descriptor {object|null}      — descriptor from the WebSocket initial message;
 *                                   used to determine inbound topics and whether
 *                                   sample generation is available.
 */
function SendEventsPanel({ useCase, descriptor, onBeforeRun }) {
  const [scenarios, setScenarios]   = useState([]);
  const [sending, setSending]       = useState(null);  // scenarioId currently sending
  const [lastResult, setLastResult] = useState(null);  // { id, ok, message }
  const [isExpanded, setIsExpanded] = useState(true); // expanded by default
  const [showModal, setShowModal]   = useState(false); // controls SampleEventModal
  const [editorTab, setEditorTab]   = useState('timeline'); // 'timeline' | 'json'
  const [activeStep, setActiveStep] = useState(null);  // index of the event currently animating
  const stepTimersRef = useRef([]);  // refs to active setTimeout ids so they can be cleared

  // ── Inline editor state ──────────────────────────────────────────────────
  const [selectedId, setSelectedId]   = useState(null);   // which scenario is in the editor
  const [editorText, setEditorText]   = useState('');     // raw text in the textarea
  const [editorError, setEditorError] = useState('');     // JSON parse error message

  // ── CRUD state ───────────────────────────────────────────────────────────
  const [saveState,   setSaveState]   = useState('idle');   // idle | saving | saved | error
  const [deleteState, setDeleteState] = useState('idle');   // idle | deleting | error
  const [crudError,   setCrudError]   = useState('');
  const [confirmDelete, setConfirmDelete] = useState(false); // inline delete confirm
  const [showNewForm,   setShowNewForm]   = useState(false);

  const editorRef   = useRef(null);
  const splitPxRef = useRef(null);  // null → use default; set to px on first drag
  const [splitPx, setSplitPx] = useState(null); // top pane height in px (null = auto)

  const onSplitterMouseDown = useCallback((e) => {
    e.preventDefault();
    const container = e.currentTarget.parentElement;
    const onMove = (mv) => {
      const rect    = container.getBoundingClientRect();
      const raw     = mv.clientY - rect.top;
      const clamped = Math.min(Math.max(raw, 80), rect.height - 120);
      splitPxRef.current = clamped;
      setSplitPx(clamped);
    };
    const onUp = () => {
      document.removeEventListener('mousemove', onMove);
      document.removeEventListener('mouseup', onUp);
    };
    document.addEventListener('mousemove', onMove);
    document.addEventListener('mouseup', onUp);
  }, []);

  // descriptor.topics gives us the real Kafka topic names for this use-case,
  // keyed by eventType. Used to route scenario events to the correct topic.
  const [topicsByType, setTopicsByType] = useState({}); // { eventType → topicName }

  // Whether sample generation is available for this use case.
  // Driven by whether the descriptor declares inbound topics (any agent-based or
  // rule-engine-backed use case should support sample generation).
  const hasSampleGeneration = !!(descriptor?.topics?.length);

  // Rebuild topicsByType whenever descriptor changes (object reference or content).
  // This is intentionally separated from the scenarios fetch so that descriptor
  // reference churn (e.g. parent re-renders) does not trigger an extra network
  // request for scenarios (M-5).
  useEffect(() => {
    const topicMap = {};
    if (descriptor?.topics) {
      for (const t of descriptor.topics) {
        topicMap[t.type] = t.name;
      }
    }
    setTopicsByType(topicMap);
  }, [descriptor]);

  // Fetch scenarios only when the use-case changes, not on every descriptor update.
  useEffect(() => {
    if (!useCase) return;
    fetch(`${API_URL}/api/use-cases/${useCase}/scenarios`)
      .then(r => r.ok ? r.json() : [])
      .catch(() => [])
      .then(scenarioData => setScenarios(scenarioData));
  }, [useCase]);

  // Clean up any pending animation timers when the component unmounts.
  useEffect(() => () => {
    stepTimersRef.current.forEach(id => clearTimeout(id));
  }, []);

  /** Cancel any pending step-animation timers and clear the active step. */
  const clearStepAnimation = useCallback(() => {
    stepTimersRef.current.forEach(id => clearTimeout(id));
    stepTimersRef.current = [];
    setActiveStep(null);
  }, []);

  // Render nothing if neither scenarios nor the sample button are available
  if (scenarios.length === 0 && !hasSampleGeneration) return null;

  /**
   * Resolves the Kafka topic name for a given logical event type.
   * Falls back to the raw eventType string so the call always returns something usable.
   */
  const topicFor = (eventType) =>
    topicsByType[eventType]
    || topicsByType[eventType.replace(/-/g, '')]
    || topicsByType[eventType.replace(/-([a-z])/g, (_match, c) => c.toUpperCase())]
    || eventType;

  // ── Select a scenario card → populate the editor ─────────────────────────
  const handleSelectScenario = (scenario) => {
    const isSame = selectedId === scenario.id;
    // Cancel any in-progress animation when switching or closing a scenario.
    clearStepAnimation();
    if (isSame) {
      setSelectedId(null);
      setEditorText('');
      setEditorError('');
      return;
    }
    setSelectedId(scenario.id);
    setEditorText(JSON.stringify(scenarioToEditorPayload(scenario), null, 2));
    setEditorError('');
    setLastResult(null);
    setSaveState('idle');
    setCrudError('');
    setConfirmDelete(false);
    setEditorTab('timeline'); // always open on the Timeline tab
    // Auto-resize on next tick after render
    setTimeout(() => {
      const ta = editorRef.current;
      if (ta) {
        ta.style.height = 'auto';
        ta.style.height = Math.min(ta.scrollHeight, 380) + 'px';
      }
    }, 0);
  };

  // ── JSON editor text change ───────────────────────────────────────────────
  const handleEditorChange = (e) => {
    const val = e.target.value;
    setEditorText(val);
    if (lastResult && lastResult.id === selectedId) setLastResult(null);
    if (saveState !== 'idle') { setSaveState('idle'); setCrudError(''); }
    if (val.trim()) {
      try { JSON.parse(val); setEditorError(''); }
      catch (err) { setEditorError(`Invalid JSON: ${err.message}`); }
    } else {
      setEditorError('');
    }
    const ta = e.target;
    ta.style.height = 'auto';
    ta.style.height = Math.min(ta.scrollHeight, 380) + 'px';
  };

  // ── Reset editor to original payload ─────────────────────────────────────
  const handleResetEditor = () => {
    const scenario = scenarios.find(s => s.id === selectedId);
    if (!scenario) return;
    // Cancel any in-progress animation on explicit reset.
    clearStepAnimation();
    const text = JSON.stringify(scenarioToEditorPayload(scenario), null, 2);
    setEditorText(text);
    setEditorError('');
    setSaveState('idle');
    setCrudError('');
    setTimeout(() => {
      const ta = editorRef.current;
      if (ta) {
        ta.style.height = 'auto';
        ta.style.height = Math.min(ta.scrollHeight, 380) + 'px';
      }
    }, 0);
  };

  // ── Send using whatever is currently in the editor ────────────────────────
  const handleSend = async () => {
    const scenario = scenarios.find(s => s.id === selectedId);
    if (!scenario) return;

    let parsed;
    try {
      parsed = JSON.parse(editorText);
      setEditorError('');
    } catch (err) {
      setEditorError(`Invalid JSON: ${err.message}`);
      return;
    }

    // Clear any leftover animation from a previous run before starting.
    clearStepAnimation();

    // Notify the parent so it can clear the live timeline before new events arrive.
    onBeforeRun?.();

    setSending(scenario.id);
    setLastResult(null);

    try {
      // Reset both topic history and entity state before publishing new events so
      // that the live timeline and topic panels show only this scenario's events.
      //
      // reset-topics  — purges Kafka message history, clears the BFF ring buffer,
      //                 and broadcasts an empty snapshot to all WebSocket clients.
      // reset-entities — sends a sentinel event so Flink resets entity state before
      //                  the first real event of this scenario arrives.
      //
      // Both calls are fired in parallel; a failure in either is only warned (not
      // fatal) so a Kafka hiccup does not prevent the scenario from running.
      const [topicsResp, resetResp] = await Promise.all([
        fetch(`${API_URL}/api/use-cases/${useCase}/reset-topics`, { method: 'POST' }),
        fetch(
          `${API_URL}/api/use-cases/${useCase}/reset-entities`,
          {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ scenarioId: scenario.id }),
          }
        ),
      ]);
      if (!topicsResp.ok) {
        console.warn(`[reset-topics] Reset failed for scenario '${scenario.id}': HTTP ${topicsResp.status}`);
      }
      if (!resetResp.ok) {
        console.warn(`[reset-entities] Reset failed for scenario '${scenario.id}': HTTP ${resetResp.status}`);
      }

      const events = scenarioToOrderedEvents(parsed, topicFor);

      if (events.length === 0) {
        throw new Error('No events found in the payload — please add at least one event.');
      }

      const DELAY_MS = 2000; // must match the delayMs sent to the backend

      // Schedule animated step progression to mirror the backend's delivery timing.
      // Each event i is sent by the backend at approximately i * DELAY_MS after the
      // batch is submitted.  We light up step i just before the backend sends it,
      // then clear the animation once the final step's delay window has passed.
      const timers = [];
      for (let i = 0; i < events.length; i++) {
        timers.push(
          setTimeout(() => setActiveStep(i), i * DELAY_MS)
        );
      }
      // After the last event has been "processed", extinguish the animation.
      timers.push(
        setTimeout(() => setActiveStep(null), events.length * DELAY_MS)
      );
      stepTimersRef.current = timers;

      const resp = await fetch(
        `${API_URL}/api/use-cases/${useCase}/events`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ events, delayMs: DELAY_MS }),
        }
      );
      const body = await resp.json();
      if (!resp.ok) throw new Error(body.error || resp.statusText);
      const totalPublished = body.published || 0;

      setLastResult({
        id: scenario.id,
        ok: true,
        message: `Sent ${totalPublished} event${totalPublished !== 1 ? 's' : ''} — watch the results panel`,
      });
    } catch (err) {
      // On error stop animating immediately.
      clearStepAnimation();
      setLastResult({ id: scenario.id, ok: false, message: `Error: ${err.message}` });
    } finally {
      setSending(null);
    }
  };

  // ── Save scenario ─────────────────────────────────────────────────────────
  const handleSaveScenario = async () => {
    const scenario = scenarios.find(s => s.id === selectedId);
    if (!scenario) return;

    let parsed;
    try {
      parsed = JSON.parse(editorText);
    } catch (err) {
      setEditorError(`Invalid JSON: ${err.message}`);
      return;
    }

    setSaveState('saving');
    setCrudError('');
    try {
      // Merge the edited payload keys back into the full scenario object
      const updated = { ...scenario, ...parsed };
      const resp = await fetch(
        `${API_URL}/api/use-cases/${useCase}/scenarios/${selectedId}`,
        {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(updated),
        }
      );
      const body = await resp.json();
      if (!resp.ok) throw new Error(body.error || resp.statusText);
      // Update local state with the saved version
      setScenarios(prev => prev.map(s => s.id === selectedId ? body : s));
      setSaveState('saved');
      // Auto-revert save badge after 2 s
      setTimeout(() => setSaveState('idle'), 2000);
    } catch (err) {
      setSaveState('error');
      setCrudError(`Save failed: ${err.message}`);
    }
  };

  // ── Delete scenario (with confirm) ────────────────────────────────────────
  const handleDeleteConfirmed = async () => {
    setConfirmDelete(false);
    setDeleteState('deleting');
    setCrudError('');
    try {
      const resp = await fetch(
        `${API_URL}/api/use-cases/${useCase}/scenarios/${selectedId}`,
        { method: 'DELETE' }
      );
      const body = await resp.json();
      if (!resp.ok) throw new Error(body.error || resp.statusText);
      setScenarios(prev => prev.filter(s => s.id !== selectedId));
      setSelectedId(null);
      setEditorText('');
      setEditorError('');
      setDeleteState('idle');
    } catch (err) {
      setDeleteState('error');
      setCrudError(`Delete failed: ${err.message}`);
    }
  };

  // ── Create new scenario ───────────────────────────────────────────────────
  const handleCreateScenario = async ({ id, label, description }) => {
    const newScenario = { id, label, description };
    try {
      const resp = await fetch(
        `${API_URL}/api/use-cases/${useCase}/scenarios`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(newScenario),
        }
      );
      const body = await resp.json();
      if (!resp.ok) throw new Error(body.error || resp.statusText);
      setScenarios(prev => [...prev, body]);
      setShowNewForm(false);
      // Immediately select the new scenario so the user can start editing its payload
      setSelectedId(body.id);
      setEditorText(JSON.stringify(scenarioToEditorPayload(body), null, 2));
      setEditorError('');
      setSaveState('idle');
      setCrudError('');
      setLastResult(null);
    } catch (err) {
      setCrudError(`Create failed: ${err.message}`);
    }
  };

  const selectedScenario = scenarios.find(s => s.id === selectedId) || null;
  const isSending        = sending === selectedId;
  const canSend          = !isSending && !editorError && editorText.trim() !== '';
  const canSave          = !isSending && deleteState !== 'deleting' && saveState !== 'saving' && !editorError && editorText.trim() !== '';
  const isBusyCrud       = saveState === 'saving' || deleteState === 'deleting';
  const result           = lastResult && lastResult.id === selectedId ? lastResult : null;
  const existingIds      = new Set(scenarios.map(s => s.id));

  return (
    <>
      <section className="data-section send-events-section">
        <h2>
          <button
            className="section-toggle-btn"
            onClick={() => setIsExpanded(v => !v)}
            aria-expanded={isExpanded}
            title={isExpanded ? 'Collapse' : 'Expand'}
          >
            {isExpanded ? '−' : '+'}
          </button>
          Demo Scenarios
          {!isExpanded && scenarios.length > 0 && (
            <span style={{ marginLeft: '8px', fontSize: '0.72rem', fontWeight: 600, color: '#9ca3af', background: '#f8fafc', border: '1px solid #e5e7eb', borderRadius: '10px', padding: '2px 8px' }}>
              {scenarios.length} scenario{scenarios.length !== 1 ? 's' : ''}
            </span>
          )}
          {/* Generate Sample Event button — shown when the descriptor has inbound topics */}
          {hasSampleGeneration && (
            <button
              className="generate-sample-btn"
              onClick={() => setShowModal(true)}
              title="Generate a sample event pre-populated from the agent's decision archive schema"
            >
              Generate Sample Event
            </button>
          )}
        </h2>

        {isExpanded && (
          <>
            <div className="send-events-toolbar">
              <p style={{ color: '#57606a', fontSize: '0.82rem', margin: 0, lineHeight: 1.5 }}>
                Select a scenario to preview and optionally edit its payload, then click <strong>Run Scenario</strong>.
              </p>
              <button
                className="scenario-add-btn"
                onClick={() => { setShowNewForm(v => !v); setCrudError(''); }}
                title="Create a new scenario"
              >
                + New Scenario
              </button>
            </div>

            {/* New scenario inline form */}
            {showNewForm && (
              <NewScenarioForm
                onSave={handleCreateScenario}
                onCancel={() => { setShowNewForm(false); setCrudError(''); }}
                existingIds={existingIds}
              />
            )}

            {/* Top-level CRUD error (e.g. create failed) */}
            {crudError && !selectedScenario && (
              <div className="scenario-crud-error">{crudError}</div>
            )}

            {scenarios.length > 0 ? (
              <>
                {/* ── Split container: scenario grid top, editor bottom ── */}
                <div
                  className={`scenario-split${selectedScenario ? ' scenario-split--active' : ''}`}
                  style={{ marginTop: showNewForm ? '14px' : '1rem' }}
                >
                  {/* Top pane — scenario grid */}
                  <div
                    className="scenario-split-top"
                    style={selectedScenario ? { height: splitPx !== null ? `${splitPx}px` : '390px' } : {}}
                  >
                    <div className="scenario-grid">
                      {scenarios.map((s) => {
                        const isSelected = selectedId === s.id;
                        return (
                          <div
                            key={s.id}
                            className={`scenario-card${isSelected ? ' scenario-card-selected' : ''}`}
                            onClick={() => handleSelectScenario(s)}
                            role="button"
                            tabIndex={0}
                            onKeyDown={(e) => { if (e.key === 'Enter' || e.key === ' ') handleSelectScenario(s); }}
                            aria-pressed={isSelected}
                            title={isSelected ? 'Click to collapse editor' : 'Click to select and preview payload'}
                            style={{ cursor: 'pointer' }}
                          >
                            <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                              <span style={{ fontSize: '0.9rem', flexShrink: 0 }}>{s.icon || '▶'}</span>
                              <div className="scenario-label">{s.label}</div>
                              {isSelected && (
                                <span className="scenario-selected-badge">selected</span>
                              )}
                            </div>
                            <div className="scenario-desc">{s.description}</div>
                          </div>
                        );
                      })}
                    </div>
                  </div>

                  {/* Draggable splitter — only shown when an editor is open */}
                  {selectedScenario && (
                    <div className="scenario-splitter" onMouseDown={onSplitterMouseDown} />
                  )}

                  {/* Bottom pane — inline editor panel */}
                  {selectedScenario && (
                  <div className="scenario-split-bottom">
                  <div className="scenario-editor-panel scenario-editor-panel--split">
                    <div className="scenario-editor-header">
                      <div className="scenario-editor-title">
                        <span style={{ fontSize: '0.9rem' }}>{selectedScenario.icon || '▶'}</span>
                        <strong>{selectedScenario.label}</strong>
                        <span className="scenario-editor-hint">— edit the payload below or run as-is</span>
                      </div>
                      <div className="scenario-editor-header-actions">
                        <button
                          className="scenario-editor-reset-btn"
                          onClick={handleResetEditor}
                          disabled={isBusyCrud || isSending}
                          title="Reset to original payload"
                        >
                          ↺ Reset
                        </button>
                        {/* Delete button with inline confirm */}
                        {!confirmDelete ? (
                          <button
                            className="scenario-delete-btn"
                            onClick={() => setConfirmDelete(true)}
                            disabled={isBusyCrud || isSending}
                            title="Delete this scenario"
                          >
                            {deleteState === 'deleting' ? 'Deleting…' : 'Delete'}
                          </button>
                        ) : (
                          <span className="scenario-delete-confirm">
                            <span className="scenario-delete-confirm-msg">Delete permanently?</span>
                            <button
                              className="scenario-delete-confirm-yes"
                              onClick={handleDeleteConfirmed}
                            >
                              Yes, delete
                            </button>
                            <button
                              className="scenario-delete-confirm-no"
                              onClick={() => setConfirmDelete(false)}
                            >
                              Cancel
                            </button>
                          </span>
                        )}
                      </div>
                    </div>

                    {/* ── Tab bar ── */}
                    <div className="scenario-editor-tabs">
                      <button
                        className={`scenario-editor-tab${editorTab === 'timeline' ? ' scenario-editor-tab--active' : ''}`}
                        onClick={() => setEditorTab('timeline')}
                      >
                        Timeline
                      </button>
                      <button
                        className={`scenario-editor-tab${editorTab === 'json' ? ' scenario-editor-tab--active' : ''}`}
                        onClick={() => setEditorTab('json')}
                      >
                        JSON
                      </button>
                    </div>

                    {/* ── Timeline tab ── */}
                    {editorTab === 'timeline' && (() => {
                      let timelineEvents = [];
                      try {
                        const parsed = JSON.parse(editorText);
                        timelineEvents = scenarioToOrderedEvents(parsed, topicFor).map(({ schemaType, topic, payload }) => {
                          return { schemaType, topic, payload };
                        });
                      } catch { /* leave empty on parse error */ }
                      return (
                        <div className="scenario-editor-timeline-tab">
                          <ScenarioTimeline events={timelineEvents} activeStep={activeStep} />
                        </div>
                      );
                    })()}

                    {/* ── JSON tab ── */}
                    {editorTab === 'json' && (
                      <div className="scenario-editor-field">
                        <textarea
                          id="scenario-json-editor"
                          ref={editorRef}
                          className={`scenario-editor-textarea${editorError ? ' scenario-editor-textarea-error' : ''}`}
                          value={editorText}
                          onChange={handleEditorChange}
                          spellCheck={false}
                          disabled={isSending || isBusyCrud}
                          aria-describedby={editorError ? 'scenario-editor-error' : undefined}
                        />
                        {editorError && (
                          <div id="scenario-editor-error" className="scenario-editor-error-msg">
                            {editorError}
                          </div>
                        )}
                      </div>
                    )}

                    {/* CRUD feedback (save / delete errors) */}
                    {crudError && (
                      <div className="scenario-crud-error" style={{ margin: '0 14px' }}>
                        {crudError}
                      </div>
                    )}

                    <div className="scenario-editor-footer">
                      {result && (
                        <div
                          className="scenario-result"
                          style={{ color: result.ok ? '#15803d' : '#b91c1c', flex: 1 }}
                        >
                          {result.ok ? '✓ ' : '✗ '}{result.message}
                        </div>
                      )}
                      {/* Save scenario button */}
                      <button
                        className={`scenario-btn scenario-save-btn${saveState === 'saved' ? ' scenario-save-btn--saved' : ''}`}
                        disabled={!canSave}
                        onClick={handleSaveScenario}
                        title="Persist changes to this scenario"
                      >
                        {saveState === 'saving' ? 'Saving…'
                          : saveState === 'saved'  ? '✓ Saved'
                          : 'Save Scenario'}
                      </button>
                      {/* Send / run button */}
                      <button
                        className="scenario-btn scenario-run-btn"
                        disabled={!canSend}
                        onClick={handleSend}
                      >
                        {isSending ? 'Sending…' : 'Run Scenario'}
                      </button>
                    </div>
                  </div>
                  </div>
                  )}
                </div>

                {!selectedScenario && (
                  <p className="scenario-select-hint">
                    ↑ Select a scenario above to preview its payload and run it.
                  </p>
                )}
              </>
            ) : (
              hasSampleGeneration && !showNewForm && (
                <p style={{ color: '#57606a', fontSize: '0.82rem', fontStyle: 'italic' }}>
                  No pre-built scenarios for this use case. Use <strong>Generate Sample Event</strong> above to compose and send a custom event, or create a new scenario with <strong>+ New Scenario</strong>.
                </p>
              )
            )}
          </>
        )}
      </section>

      {/* Sample event modal — rendered outside the section so z-index stacking is clean */}
      {showModal && (
        <SampleEventModal
          agentId={useCase}
          useCase={useCase}
          onClose={() => setShowModal(false)}
        />
      )}
    </>
  );
}

export default SendEventsPanel;
