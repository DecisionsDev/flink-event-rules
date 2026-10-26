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
import React, { useState, useCallback, useEffect, useRef } from 'react';
import './AgentWizard.css';
import { API_URL } from './config.js';
import { newKey } from './newKey.js';

// ── Defaults ──────────────────────────────────────────────────────────────────

const DEFAULT_OPERATION = {
  operation: '',
  entityField: '',
  currentEventField: '',
  history: [],
  outbounds: [],
  scheduleRequestsField: '',
};

const DEFAULT_AGENT = {
  parallelism: 0,          // job-level
  useCaseId: '',           // BFF folder key / Flink Job identity; defaults to agentId for single-agent cases
  // Use-case properties stored at the top level of agents.json. Blank = not written,
  // the BFF then derives the value (name from the agent, flinkJobName = <useCaseId>-job, ...).
  useCaseName: '',
  flinkJobName: '',
  useCaseDescription: '',
  consumerGroupId: '',
  agentId: '',
  name: '',
  entityType: '',
  entityKeyExtractor: '$.id',
  agentParallelism: 0,     // agent-level
  chainingEnabled: true,   // per-agent chaining flag (default true = participate in chain)
  subscriptions: [
    {
      _key: newKey(),
      eventType: '',
      topic: '',             // single Kafka topic name (EventSubscription.topic is a String)
      operation: '',         // IBM DI operation name; "" = history-only (no rule processing)
      entityKeyExtractor: '', // blank = inherit the agent-level entityKeyExtractor
      horizonType: 'hybrid', // matches the "Add Subscription" default
      horizonValue: 10,
      horizonCountLimit: 100,
      horizonTimeLimit: '24h',
      horizonDescription: '',
    },
  ],
  ruleset: {
    path: '',
    outputField: '',
    archiveControlTopic: '',
    operations: [],
  },
  outputs: [
    { _key: newKey(), eventType: '', topic: '', description: '' },
  ],
  stateConfig: {
    ttl: '30d',
    backend: 'rocksdb',
  },
};

const STEPS = [
  'Job & Identity',
  'Ruleset',
  'Subscriptions',
  'Outputs',
  'State & Review',
  'Publish & Deploy',
];

// Job states that indicate the Flink job is actively running
const RUNNING_STATES = new Set(['RUNNING', 'RESTARTING', 'CREATED', 'INITIALIZING']);
const STATUS_POLL_MS = 3000;

// ── Helper: resolve the use-case folder key ───────────────────────────────────

// Lowercase, hyphen-separated slug that satisfies the BFF's id pattern
// (^[a-zA-Z0-9_-]{1,64}$). Returns '' when nothing usable remains.
export function slugify(text) {
  return (text || '')
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '')   // strip accents
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 64)
    .replace(/-+$/, '');
}

// The folder under samples/ that holds the use case, and the key of every /api/agents call.
//   1. form.useCaseId — set when editing or appending to an existing use case (never changes,
//      so renaming the use case does not move its folder);
//   2. otherwise a slug of the Use-Case Name;
//   3. otherwise the Agent ID.
export function resolveUseCaseId(form) {
  return (form.useCaseId || '').trim()
    || slugify(form.useCaseName)
    || (form.agentId || '').trim();
}

// ── Helper: build the agents.json object from wizard state ────────────────────

// Returns { descriptor, useCaseId } where descriptor is the agents.json object
// and useCaseId is the BFF folder key (may differ from agentId in multi-agent jobs).
export function buildDescriptor(form) {
  const agent = {
    agentId: form.agentId.trim(),
    // Always emit name — derive a human-readable title from agentId when blank.
    name: form.name.trim() || form.agentId.trim().replace(/[-_]+/g, ' ').replace(/\b\w/g, c => c.toUpperCase()),
    ...(form.entityType.trim()  && { entityType:  form.entityType.trim()  }),
    ...(form.entityKeyExtractor.trim() && { entityKeyExtractor: form.entityKeyExtractor.trim() }),
    subscriptions: form.subscriptions
      .filter(s => s.eventType.trim())
      .map(s => {
        const horizon = buildHorizon(s);
        return {
          eventType: s.eventType.trim(),
          // EventSubscription.topic is an optional single String in the Java model: sources
          // such as Schedule subscriptions have no Kafka topic, so omit it when blank.
          ...(s.topic?.trim() && { topic: s.topic.trim() }),
          operation: s.operation ?? '',  // required; "" = history-only
          // Optional per-subscription override of the agent-level entityKeyExtractor.
          ...(s.entityKeyExtractor?.trim() && { entityKeyExtractor: s.entityKeyExtractor.trim() }),
          horizon,
        };
      }),
    ruleset: buildRuleset(form.ruleset),
    outputs: form.outputs
      .filter(o => o.eventType.trim() && o.topic.trim())
      .map(o => ({
        eventType: o.eventType.trim(),
        topic: o.topic.trim(),
        ...(o.description.trim() && { description: o.description.trim() }),
      })),
    stateConfig: {
      ...(form.stateConfig.ttl?.trim()  && { ttl:     form.stateConfig.ttl.trim()  }),
      ...(form.stateConfig.backend      && { backend: form.stateConfig.backend     }),
    },
    ...(form.agentParallelism > 0 && { parallelism: form.agentParallelism }),
    // Only emit chainingEnabled when false — true is the default and omitting it
    // keeps generated descriptors clean for the common case.
    ...(form.chainingEnabled === false && { chainingEnabled: false }),
  };

  const wrapper = {};
  // Use-case properties come first in the file; blank values are omitted.
  if ((form.useCaseName || '').trim())        wrapper.name            = form.useCaseName.trim();
  if ((form.flinkJobName || '').trim())       wrapper.flinkJobName    = form.flinkJobName.trim();
  if ((form.useCaseDescription || '').trim()) wrapper.description     = form.useCaseDescription.trim();
  if ((form.consumerGroupId || '').trim())    wrapper.consumerGroupId = form.consumerGroupId.trim();
  wrapper.agents = [agent];
  if (form.parallelism > 0) wrapper.parallelism = form.parallelism;
  return { descriptor: wrapper, useCaseId: resolveUseCaseId(form) };
}

function buildHorizon(s) {
  if (s.horizonType === 'count') {
    return { type: 'count', value: Number(s.horizonValue),
      ...(s.horizonDescription && { description: s.horizonDescription }) };
  }
  if (s.horizonType === 'time') {
    return { type: 'time', timeLimit: s.horizonTimeLimit.trim(),
      ...(s.horizonDescription && { description: s.horizonDescription }) };
  }
  // hybrid
  return {
    type: 'hybrid',
    countLimit: Number(s.horizonCountLimit),
    timeLimit: s.horizonTimeLimit.trim(),
    ...(s.horizonDescription && { description: s.horizonDescription }),
  };
}

function buildRuleset(rs) {
  // Explicitly strip _jarFile (a browser File object) — it must never reach JSON.stringify.
  // eslint-disable-next-line no-unused-vars
  const { _jarFile, ...r } = rs;
  const obj = {
    path: r.path.trim(),
    ...(r.archiveControlTopic?.trim() && { archiveControlTopic: r.archiveControlTopic.trim() }),
    ...(r.outputField?.trim() && { outputField: r.outputField.trim() }),
  };
  // Emit operations only when at least one has a name
  const ops = (r.operations || []).filter(op => op.operation.trim());
  if (ops.length > 0) {
    obj.operations = ops.map(op => ({
      operation: op.operation.trim(),
      entityField: op.entityField.trim(),
      currentEventField: op.currentEventField.trim(),
      history: (op.history || [])
        .filter(h => h.eventType.trim())
        .map(h => ({ eventType: h.eventType.trim(), field: h.field.trim() })),
      outbounds: (op.outbounds || [])
        .filter(o => o.field?.trim() || o.eventType?.trim())
        .map(o => ({ field: o.field?.trim() || '', eventType: o.eventType?.trim() || '' })),
      ...(op.scheduleRequestsField?.trim() && { scheduleRequestsField: op.scheduleRequestsField.trim() }),
    }));
  }
  return obj;
}

// ── Validation per step ────────────────────────────────────────────────────────

// ── Step 6: Publish & Deploy ──────────────────────────────────────────────────

// props:
//   useCaseId    — BFF folder key; all job-lifecycle API calls use this
//   agentId      — individual agent id (used for per-agent delete; may equal useCaseId)
//   mode         — "replace" | "append"  (default: "replace")
//   jarFile      — File object for JAR upload, or null
//   descriptor   — pre-built agents.json descriptor object; passed on mount and kept
//                  in a ref so the manual "Save Descriptor" retry button can re-invoke
//                  handleSave with the most-recent descriptor without needing a parent callback
//   onMountSave  — callback(saveFn) called on mount so parent can trigger the initial auto-save
//   onJobRunning — callback(useCaseId) fired once when job reaches RUNNING
function StepPublish({ useCaseId, agentId, mode = 'replace', jarFile, descriptor: descriptorProp, onMountSave, onJobRunning }) {
  const [jarState,       setJarState]       = useState(jarFile ? 'idle' : 'not-needed');
  const [jarMsg,         setJarMsg]         = useState(null);
  const [saveState,      setSaveState]      = useState('idle');
  const [startState,     setStartState]     = useState('idle');
  const [stopState,      setStopState]      = useState('idle');
  const [deleteState,    setDeleteState]    = useState('idle');
  const [jobStatus,      setJobStatus]      = useState(null);
  const [saveMsg,        setSaveMsg]        = useState(null);
  const [actionMsg,      setActionMsg]      = useState(null);
  const [restartRequired, setRestartRequired] = useState(false);
  const pollRef            = useRef(null);
  const jobRunningFiredRef = useRef(false);
  // Keep the latest descriptor in a ref so the manual "Save Descriptor" retry button
  // can call handleSave(descriptorRef.current) without the parent needing to re-trigger.
  const descriptorRef      = useRef(descriptorProp);
  useEffect(() => { descriptorRef.current = descriptorProp; }, [descriptorProp]);

  // Capture onMountSave in a ref so the mount effect sees a stable reference
  // even if the parent passes a new function identity on a re-render (M-6).
  const onMountSaveRef = useRef(onMountSave);
  useEffect(() => { onMountSaveRef.current = onMountSave; }, [onMountSave]);

  // Auto-trigger save on mount via parent callback — runs exactly once.
  useEffect(() => {
    if (onMountSaveRef.current) onMountSaveRef.current(handleSave);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Poll job status when a job is active.
  useEffect(() => {
    const poll = async () => {
      try {
        const r = await fetch(`${API_URL}/api/agents/${useCaseId}/status`);
        if (!r.ok) return;
        const status = await r.json();
        setJobStatus(status);
        if (
          !jobRunningFiredRef.current &&
          onJobRunning &&
          status.state &&
          RUNNING_STATES.has(status.state)
        ) {
          jobRunningFiredRef.current = true;
          onJobRunning(useCaseId);
        }
      } catch { /* Flink unreachable — leave last status */ }
    };
    if (startState === 'started' || (jobStatus?.state && RUNNING_STATES.has(jobStatus.state))) {
      poll();
      pollRef.current = setInterval(poll, STATUS_POLL_MS);
    }
    return () => clearInterval(pollRef.current);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [startState, useCaseId]);

  const handleJarUpload = async () => {
    if (!jarFile) return;
    setJarState('uploading'); setJarMsg(null);
    try {
      const fd = new FormData();
      fd.append('jar', jarFile);
      // JAR is always uploaded to the use-case folder (useCaseId), not agentId
      const r = await fetch(`${API_URL}/api/agents/${useCaseId}/jar`, { method: 'POST', body: fd });
      const body = await r.json();
      if (!r.ok) throw new Error(body.error || r.statusText);
      setJarState('uploaded');
      setJarMsg(`JAR saved — ${body.path}`);
    } catch (err) {
      setJarState('error');
      setJarMsg(`JAR upload failed: ${err.message}`);
    }
  };

  const handleSave = async (descriptor) => {
    setSaveState('saving'); setSaveMsg(null); setActionMsg(null); setRestartRequired(false);
    try {
      let r;
      if (mode === 'patch') {
        // Edit a specific agent within an existing multi-agent use-case.
        // PATCH replaces only the matched agent; sibling agents are untouched.
        r = await fetch(`${API_URL}/api/agents/${useCaseId}/agents/${agentId}`, {
          method:  'PATCH',
          headers: { 'Content-Type': 'application/json' },
          body:    JSON.stringify({
            descriptor: descriptor.agents[0],
            // Blank values remove the property from agents.json (the BFF then derives it).
            useCaseProperties: {
              name:            descriptor.name            ?? '',
              flinkJobName:    descriptor.flinkJobName    ?? '',
              description:     descriptor.description     ?? '',
              consumerGroupId: descriptor.consumerGroupId ?? '',
            },
          }),
        });
      } else {
        r = await fetch(`${API_URL}/api/agents`, {
          method:  'POST',
          headers: { 'Content-Type': 'application/json' },
          body:    JSON.stringify({ useCaseId, descriptor, mode }),
        });
      }
      const body = await r.json();
      if (!r.ok) {
        // Friendly message for duplicate-agentId conflict in append mode
        if (body.error === 'DUPLICATE_AGENT_ID')
          throw new Error(`Agent ID '${body.agentId}' already exists in event rule application '${useCaseId}'. Choose a different Agent ID.`);
        throw new Error(body.error || r.statusText);
      }
      setSaveState('saved');
      setSaveMsg(`Saved to samples/${useCaseId}/agents.json`);
      if (body.restartRequired) setRestartRequired(true);
    } catch (err) {
      setSaveState('error');
      setSaveMsg(`Save failed: ${err.message}`);
    }
  };

  const handleStart = async () => {
    setStartState('starting'); setActionMsg(null);
    try {
      const r = await fetch(`${API_URL}/api/agents/${useCaseId}/start`, { method: 'POST' });
      const body = await r.json();
      if (!r.ok) throw new Error(body.error || r.statusText);
      setStartState('started');
      setJobStatus({ state: 'CREATED', jobId: body.jobId });
      setActionMsg(`Job submitted — ID: ${body.jobId}`);
    } catch (err) {
      setStartState('error');
      setActionMsg(`Start failed: ${err.message}`);
    }
  };

  const handleStop = async () => {
    setStopState('stopping'); setActionMsg(null);
    try {
      const r = await fetch(`${API_URL}/api/agents/${useCaseId}/stop`, { method: 'POST' });
      const body = await r.json();
      if (!r.ok) throw new Error(body.error || r.statusText);
      setStopState('stopped');
      setStartState('idle');
      setJobStatus(prev => ({ ...prev, state: 'CANCELED' }));
      setActionMsg('Job cancelled.');
    } catch (err) {
      setStopState('error');
      setActionMsg(`Stop failed: ${err.message}`);
    }
  };

  const handleDelete = async () => {
    setDeleteState('deleting'); setActionMsg(null);
    try {
      const r = await fetch(`${API_URL}/api/agents/${useCaseId}`, { method: 'DELETE' });
      const body = await r.json();
      if (!r.ok) throw new Error(body.error || r.statusText);
      clearInterval(pollRef.current);
      setDeleteState('deleted');
      setStartState('idle');
      setJobStatus(null);
      setActionMsg(`Event rule application '${useCaseId}' and all descriptor files deleted.`);
    } catch (err) {
      setDeleteState('error');
      setActionMsg(`Delete failed: ${err.message}`);
    }
  };

  const jobState  = jobStatus?.state || 'NOT_STARTED';
  const isRunning = RUNNING_STATES.has(jobState);
  const isSaved   = saveState === 'saved';
  const isDeleted = deleteState === 'deleted';
  const jarReady  = jarState === 'not-needed' || jarState === 'uploaded';

  return (
    <div className="wz-step-body">
      {/* Restart-required warning — shown when agent was appended to a running job */}
      {restartRequired && (
        <div className="wz-deploy-feedback wz-deploy-feedback-warn">
          ⚠ Changes saved. The Flink Job must be restarted for the new agent to take effect.
          Stop the job below, then start it again. Any in-flight state for existing agents will
          be lost unless a savepoint was taken beforehand.
        </div>
      )}

      {/* 0 — Upload JAR (shown only when a JAR file was selected in Step 3) */}
      {jarFile && (
        <div className="wz-deploy-section">
          <div className="wz-section-label">0 — Upload Ruleset JAR</div>
          <p className="wz-hint wz-hint-block">
            Saves <code>{jarFile.name}</code> to <code>samples/{useCaseId}/</code> on the BFF server.
          </p>
          <div className="wz-deploy-row">
            <button
              className={`wz-primary-btn${jarState === 'uploading' ? ' wz-btn-busy' : ''}`}
              onClick={handleJarUpload}
              disabled={jarState === 'uploading' || jarState === 'uploaded' || isDeleted}
            >
              {jarState === 'uploading' ? 'Uploading…' : jarState === 'uploaded' ? '✓ Uploaded' : 'Upload JAR'}
            </button>
            {jarMsg && (
              <span className={`wz-deploy-msg ${jarState === 'error' ? 'wz-deploy-msg-err' : 'wz-deploy-msg-ok'}`}>
                {jarMsg}
              </span>
            )}
          </div>
        </div>
      )}

      {/* 1 — Save */}
      <div className={`wz-deploy-section${!jarReady ? ' wz-deploy-section-disabled' : ''}`}>
        <div className="wz-section-label">1 — Save Descriptor</div>
        <p className="wz-hint wz-hint-block">
          {mode === 'patch'
            ? <>Updates agent <code>{agentId}</code> inside <code>samples/{useCaseId}/agents.json</code>.
               Sibling agents are not modified.</>
            : <>Writes <code>samples/{useCaseId}/agents.json</code> to the BFF server,
               and ensures all referenced Kafka topics exist.
               {mode === 'append' && <> Agent will be <strong>appended</strong> to the existing event rule application.</>}</>
          }
        </p>
        <div className="wz-deploy-row">
          <button
            className={`wz-primary-btn${saveState === 'saving' ? ' wz-btn-busy' : ''}`}
            onClick={() => handleSave(descriptorRef.current)}
            disabled={saveState === 'saving' || saveState === 'saved' || isDeleted || !jarReady}
          >
            {saveState === 'saving' ? 'Saving…' : saveState === 'saved' ? '✓ Saved' : 'Save Descriptor'}
          </button>
          {saveMsg && (
            <span className={`wz-deploy-msg ${saveState === 'error' ? 'wz-deploy-msg-err' : 'wz-deploy-msg-ok'}`}>
              {saveMsg}
            </span>
          )}
        </div>
      </div>

      {/* 2 — Start */}
      <div className={`wz-deploy-section${!isSaved ? ' wz-deploy-section-disabled' : ''}`}>
        <div className="wz-section-label">2 — Start Flink Job</div>
        <p className="wz-hint wz-hint-block">
          Uploads the JAR to Flink (if needed) and submits
          <code>AgentBasedDataStreamJob</code> with the saved descriptor.
        </p>
        <div className="wz-deploy-row">
          <button
            className={`wz-primary-btn${startState === 'starting' ? ' wz-btn-busy' : ''}`}
            disabled={!isSaved || isRunning || startState === 'starting' || isDeleted}
            onClick={handleStart}
          >
            {startState === 'starting' ? 'Starting…' : '▶ Start Job'}
          </button>
          {isRunning && (
            <button
              className={`wz-danger-btn${stopState === 'stopping' ? ' wz-btn-busy' : ''}`}
              disabled={stopState === 'stopping'}
              onClick={handleStop}
            >
              {stopState === 'stopping' ? 'Stopping…' : '■ Stop Job'}
            </button>
          )}
          {jobStatus && (
            <span className={`wz-job-badge wz-job-badge-${jobState.toLowerCase()}`}>
              {jobState}
              {jobStatus.jobId && <span className="wz-job-id"> — {jobStatus.jobId.slice(0, 8)}…</span>}
            </span>
          )}
        </div>
      </div>

      {/* Action feedback */}
      {actionMsg && (
        <div className={`wz-deploy-feedback ${(startState === 'error' || stopState === 'error' || deleteState === 'error') ? 'wz-deploy-feedback-err' : 'wz-deploy-feedback-ok'}`}>
          {actionMsg}
        </div>
      )}

      {/* 3 — Delete */}
      {isSaved && (
        <div className="wz-deploy-section wz-deploy-danger-zone">
          <div className="wz-section-label">Delete event rule application</div>
          <p className="wz-hint wz-hint-block">
            Cancels any running job and removes all descriptor files for this event rule application.
            This cannot be undone.
          </p>
          <button
            className={`wz-danger-btn${deleteState === 'deleting' ? ' wz-btn-busy' : ''}`}
            disabled={deleteState === 'deleting' || isDeleted}
            onClick={handleDelete}
          >
            {deleteState === 'deleting' ? 'Deleting…' : isDeleted ? '✓ Deleted' : 'Delete Event Rule Application'}
          </button>
        </div>
      )}
    </div>
  );
}

export function validateStep(step, form) {
  if (step === 0) {
    if (!form.agentId.trim()) return 'Agent ID is required.';
    // The use-case folder key is derived (existing id, else slug of the use-case name, else
    // agentId). Only the agentId fallback can be invalid; the BFF requires this pattern.
    if (!/^[a-zA-Z0-9_-]{1,64}$/.test(resolveUseCaseId(form)))
      return 'Agent ID may only contain letters, numbers, hyphens, or underscores (max 64 chars) — it names the event rule application folder when no Event Rule Application Name is given.';
    // Warn when entityKeyExtractor uses a complex expression that may not work
    // with the built-in string extractor (L-6).  Simple paths like "$.id" or
    // "$.account.id" are fine; predicates and array indices are not.
    const eke = (form.entityKeyExtractor || '').trim();
    if (eke && /[[\]?@(),:*]/.test(eke))
      return 'Warning: the entity key extractor looks complex. Only simple dot-notation JSONPath expressions (e.g. $.id, $.account.id) are supported. Verify before continuing.';
  }
  // Step 1 is now Ruleset (was step 2)
  if (step === 1) {
    if (!form.ruleset.path.trim()) return 'Ruleset JAR path is required.';
  }
  // Step 2 is now Subscriptions (was step 1)
  if (step === 2) {
    const TIME_LIMIT_RE = /^\d+(?:mn|[smhd])$/;
    for (const s of form.subscriptions) {
      if (!s.eventType.trim()) continue;
      if (s.operation === null || s.operation === undefined)
        return `Subscription "${s.eventType}" is missing an operation. Use "" to subscribe for context only.`;
      const subEke = (s.entityKeyExtractor || '').trim();
      if (subEke && /[[\]?@(),:*]/.test(subEke))
        return `Subscription "${s.eventType}": the entity key extractor looks complex. Only simple dot-notation JSONPath expressions (e.g. $.id, $.account.id) are supported.`;
      if ((s.horizonType === 'time' || s.horizonType === 'hybrid') && s.horizonTimeLimit) {
        if (!TIME_LIMIT_RE.test(s.horizonTimeLimit.trim()))
          return `Subscription "${s.eventType}": time limit must be a number followed by s, mn, h, d, or m (e.g. 24h, 30mn, 7m).`;
      }
    }
  }
  if (step === 3) {
    // Re-evaluate against the current form state — the user may have returned to
    // Step 2 and cleared all operation fields, which removes the output requirement.
    const hasActiveOutput = form.subscriptions.some(
      s => s.eventType.trim() && s.operation !== null && s.operation !== undefined && s.operation !== ''
    );
    if (hasActiveOutput) {
      const valid = form.outputs.filter(o => o.eventType.trim() && o.topic.trim());
      if (valid.length === 0) return 'At least one output with event type and topic is required.';
    }
  }
  return null;
}

// ── Sub-components ─────────────────────────────────────────────────────────────

function Field({ label, hint, children }) {
  return (
    <div className="wz-field">
      <label className="wz-label">{label}</label>
      {hint && <span className="wz-hint">{hint}</span>}
      {children}
    </div>
  );
}

function TextInput({ value, onChange, placeholder, mono, readOnly }) {
  return (
    <input
      className={`wz-input${mono ? ' wz-mono' : ''}${readOnly ? ' wz-input-readonly' : ''}`}
      type="text"
      value={value}
      onChange={readOnly ? undefined : e => onChange(e.target.value)}
      readOnly={readOnly}
      placeholder={placeholder}
    />
  );
}

function NumberInput({ value, onChange, min = 0 }) {
  return (
    <input
      className="wz-input wz-input-sm"
      type="number"
      min={min}
      value={value}
      onChange={e => onChange(Number(e.target.value))}
    />
  );
}

function Select({ value, onChange, options }) {
  return (
    <select className="wz-select" value={value} onChange={e => onChange(e.target.value)}>
      {options.map(o => (
        <option key={o.value} value={o.value}>{o.label}</option>
      ))}
    </select>
  );
}

// ── Step 1: Job & Identity ────────────────────────────────────────────────────

function StepIdentity({ form, set, saveMode }) {
  // Append mode adds an agent to an existing use case: its properties are not edited here.
  const useCaseLocked = saveMode === 'append';
  const effectiveId = resolveUseCaseId(form) || '<use-case-id>';
  const existingUseCase = (form.useCaseId || '').trim();
  return (
    <div className="wz-step-body">
      {useCaseLocked && (
        <p className="wz-note">
          This new agent will be appended to the existing event rule application <code>{existingUseCase}</code>.
        </p>
      )}
      {!useCaseLocked && (
        <>
          <Field
            label="Event Rule Application Name"
            hint={existingUseCase
              ? `Display name of the event rule application in the dashboard (agents.json “name”). Leave blank to derive it from the agent. The event rule application stays in samples/${existingUseCase}/.`
              : `Display name of the event rule application in the dashboard (agents.json “name”). Leave blank to derive it from the agent. The event rule application is stored in samples/${effectiveId}/ (a slug of this name, else the Agent ID).`}
          >
            <TextInput value={form.useCaseName} onChange={v => set('useCaseName', v)} placeholder="Fraud Detection" />
          </Field>
          <Field
            label="Flink Job Name"
            hint="Name under which the Flink job is submitted and looked up (agents.json “flinkJobName”). Leave blank to use <event-rule-application-id>-job."
          >
            <TextInput value={form.flinkJobName} onChange={v => set('flinkJobName', v)} placeholder={`${effectiveId}-job`} />
          </Field>
          <Field
            label="Event Rule Application Description"
            hint="Short description shown on the event rule application tile (agents.json “description”). Optional."
          >
            <TextInput value={form.useCaseDescription} onChange={v => set('useCaseDescription', v)} placeholder="Detects fraudulent transactions in real time" />
          </Field>
          <Field
            label="Consumer Group ID"
            hint="Kafka consumer group used by the dashboard (agents.json “consumerGroupId”). Leave blank to use event-rule-bff-<event-rule-application-id>."
          >
            <TextInput value={form.consumerGroupId} onChange={v => set('consumerGroupId', v)} placeholder={`event-rule-bff-${effectiveId}`} mono />
          </Field>
          <div className="wz-section-divider" data-testid="wz-agent-section-divider">
            <span className="wz-section-label">Agent</span>
            <span className="wz-hint">Properties of this individual agent within the event rule application.</span>
          </div>
        </>
      )}
      <Field
        label="Agent ID *"
        hint="Unique, stable identifier for this agent used as the Flink operator UID. Must be unique within the Flink Job. Use lowercase with hyphens (e.g. fraud-detection-agent)."
      >
        <TextInput value={form.agentId} onChange={v => set('agentId', v)} placeholder="fraud-detection-agent" mono />
      </Field>
      <Field label="Display Name" hint="Human-readable name shown in the Flink UI and dashboard. Defaults to Agent ID if left blank.">
        <TextInput value={form.name} onChange={v => set('name', v)} placeholder="Fraud Detection Agent" />
      </Field>
      <Field
        label="Entity Type"
        hint="The business entity this agent tracks state for (e.g. account, customer, device). Each unique entity key gets its own isolated state."
      >
        <TextInput value={form.entityType} onChange={v => set('entityType', v)} placeholder="account" />
      </Field>
      <Field
        label="Entity Key Extractor"
        hint="JSONPath expression that extracts the entity's unique key from each incoming event. All events with the same key share state. Used by every subscription unless it sets its own extractor in the Subscriptions step."
      >
        <TextInput value={form.entityKeyExtractor} onChange={v => set('entityKeyExtractor', v)} placeholder="$.accountId" mono />
      </Field>
      <div className="wz-row">
        <Field label="Job Parallelism" hint="Flink job-level parallelism. 0 = use the FLINK_PARALLELISM environment variable.">
          <NumberInput value={form.parallelism} onChange={v => set('parallelism', v)} />
        </Field>
        <Field label="Agent Parallelism" hint="Per-agent operator parallelism. 0 = inherit the job-level setting above.">
          <NumberInput value={form.agentParallelism} onChange={v => set('agentParallelism', v)} />
        </Field>
      </div>
      <Field
        label="Agent Chaining"
        hint="When enabled, this agent may send its output directly to downstream agents in-process, avoiding a Kafka round-trip. Disable for agents that should always read from and write to Kafka only."
      >
        <Select
          value={form.chainingEnabled ? 'true' : 'false'}
          onChange={v => set('chainingEnabled', v === 'true')}
          options={[
            { value: 'true',  label: 'Enabled — participate in direct agent-to-agent chaining' },
            { value: 'false', label: 'Disabled — always read from / write to Kafka only' },
          ]}
        />
      </Field>
    </div>
  );
}

// ── Step 2: Subscriptions ─────────────────────────────────────────────────────

function SubscriptionRow({ sub, onChange, onRemove, index, total }) {
  const update = (key, val) => onChange({ ...sub, [key]: val });
  return (
    <div className="wz-card">
      <div className="wz-card-header">
        <span className="wz-card-title">Subscription {index + 1}</span>
        {total > 1 && (
          <button className="wz-remove-btn" onClick={onRemove} title="Remove">✕</button>
        )}
      </div>
      <Field label="Event Type" hint="Logical name for this event stream — used as the key in the agent's history map.">
        <TextInput value={sub.eventType} onChange={v => update('eventType', v)} placeholder="transaction" mono />
      </Field>
      <Field label="Kafka Topic" hint="Optional. The Kafka topic name to consume this event type from. Leave blank for subscriptions with no Kafka source, such as Schedule.">
        <TextInput value={sub.topic} onChange={v => update('topic', v)} placeholder="agent-transactions" mono />
      </Field>
      <Field
        label="Operation"
        hint="Rule engine operation to invoke when this event arrives (e.g. MyService_myEvent_op). Leave blank to store the event in history without invoking the rule engine."
      >
        <TextInput value={sub.operation ?? ''} onChange={v => update('operation', v)} placeholder="MyService_eventType_op" mono />
      </Field>
      <Field
        label="Entity Key Extractor"
        hint="Optional. JSONPath that extracts the entity key from events of this subscription. Leave blank to use the agent-level extractor. Every subscription must resolve to the same entity key for the same entity."
      >
        <TextInput value={sub.entityKeyExtractor ?? ''} onChange={v => update('entityKeyExtractor', v)} placeholder="Inherit agent-level extractor" mono />
      </Field>
      <Field label="History Window" hint="How much history to retain per entity for this event type.">
        <Select value={sub.horizonType} onChange={v => update('horizonType', v)} options={[
          { value: 'count',  label: 'count — keep last N events' },
          { value: 'time',   label: 'time — keep events within a time window' },
          { value: 'hybrid', label: 'hybrid — whichever limit is reached first (recommended)' },
        ]} />
      </Field>
      {sub.horizonType === 'count' && (
        <Field label="Count Limit" hint="Maximum number of events to retain per entity. Older events are evicted.">
          <NumberInput value={sub.horizonValue} onChange={v => update('horizonValue', v)} />
        </Field>
      )}
      {sub.horizonType === 'time' && (
        <Field label="Time Limit" hint="Retain events from this far back. Examples: 24h, 7d, 1m, 30mn">
          <TextInput value={sub.horizonTimeLimit} onChange={v => update('horizonTimeLimit', v)} placeholder="24h" mono />
        </Field>
      )}
      {sub.horizonType === 'hybrid' && (
        <div className="wz-row">
          <Field label="Count Limit" hint="Maximum events per entity.">
            <NumberInput value={sub.horizonCountLimit} onChange={v => update('horizonCountLimit', v)} />
          </Field>
          <Field label="Time Limit" hint="e.g. 24h, 7d">
            <TextInput value={sub.horizonTimeLimit} onChange={v => update('horizonTimeLimit', v)} placeholder="24h" mono />
          </Field>
        </div>
      )}
      <Field label="Horizon Description" hint="Optional note for logs and monitoring (e.g. Keep last 100 events or 24 hours of history).">
        <TextInput value={sub.horizonDescription} onChange={v => update('horizonDescription', v)} placeholder="Keep last 100 transactions or 24 hours of history" />
      </Field>
    </div>
  );
}

function StepSubscriptions({ form, setSubscriptions }) {
  const add = () => setSubscriptions([...form.subscriptions, {
    _key: newKey(),
    eventType: '', topic: '', operation: '', entityKeyExtractor: '', horizonType: 'hybrid',
    horizonValue: 10, horizonCountLimit: 100, horizonTimeLimit: '24h', horizonDescription: '',
  }]);
  const remove = i => setSubscriptions(form.subscriptions.filter((_, idx) => idx !== i));
  const change = (i, val) => setSubscriptions(form.subscriptions.map((s, idx) => idx === i ? val : s));

  return (
    <div className="wz-step-body">
      <p className="wz-note">
        Each subscription connects an event type to an optional Kafka topic and defines
        how much history to retain per entity. Add one subscription per event type your agent needs.
      </p>
      {form.subscriptions.map((sub, i) => (
        <SubscriptionRow key={sub._key ?? i} sub={sub} index={i} total={form.subscriptions.length}
          onChange={v => change(i, v)} onRemove={() => remove(i)} />
      ))}
      <button className="wz-add-btn" onClick={add}>+ Add Subscription</button>
    </div>
  );
}

// ── Step 3: Ruleset ───────────────────────────────────────────────────────────

// ── Operation history row (inside an OperationRow) ───────────────────────────

function HistoryRow({ h, onChange, onRemove, total }) {
  const update = (k, v) => onChange({ ...h, [k]: v });
  return (
    <div className="wz-history-row">
      <TextInput value={h.eventType} onChange={v => update('eventType', v)} placeholder="EventType" mono />
      <TextInput value={h.field} onChange={v => update('field', v)} placeholder="fieldName" mono />
      {total > 1 && (
        <button className="wz-remove-btn wz-remove-btn-sm" onClick={onRemove} title="Remove history entry">✕</button>
      )}
    </div>
  );
}

// ── Single operation card ─────────────────────────────────────────────────────

// ── Single outbound config row ────────────────────────────────────────────────

function OutboundRow({ outbound, onChange, onRemove, total }) {
  const update = (k, v) => onChange({ ...outbound, [k]: v });
  return (
    <div className="wz-sub-row">
      <div className="wz-row wz-row-inline">
        <Field label="Field" hint="JSON field name in the rule result that holds this outbound events array.">
          <TextInput value={outbound.field ?? ''} onChange={v => update('field', v)} placeholder="theFraudAlerts" mono />
        </Field>
        <Field label="Event Type" hint="Event type assigned to each element emitted from this array.">
          <TextInput value={outbound.eventType ?? ''} onChange={v => update('eventType', v)} placeholder="Alert" mono />
        </Field>
        {total > 1 && (
          <button className="wz-remove-btn wz-remove-btn-inline" onClick={onRemove} title="Remove">✕</button>
        )}
      </div>
    </div>
  );
}

function OperationRow({ op, onChange, onRemove, index, total }) {
  const update = (k, v) => onChange({ ...op, [k]: v });
  const addHistory     = () => update('history',  [...(op.history  || []), { eventType: '', field: '' }]);
  const removeHistory  = i  => update('history',  (op.history  || []).filter((_, idx) => idx !== i));
  const changeHistory  = (i, val) => update('history',  (op.history  || []).map((h, idx) => idx === i ? val : h));

  const addOutbound    = () => update('outbounds', [...(op.outbounds || []), { field: '', eventType: '' }]);
  const removeOutbound = i  => update('outbounds', (op.outbounds || []).filter((_, idx) => idx !== i));
  const changeOutbound = (i, val) => update('outbounds', (op.outbounds || []).map((o, idx) => idx === i ? val : o));

  return (
    <div className="wz-card">
      <div className="wz-card-header">
        <span className="wz-card-title">Operation {index + 1}</span>
        {total > 1 && (
          <button className="wz-remove-btn" onClick={onRemove} title="Remove">✕</button>
        )}
      </div>
      <Field label="Operation Name" hint="Java method name in the rule service (e.g. EvaluateTransaction).">
        <TextInput value={op.operation} onChange={v => update('operation', v)} placeholder="EvaluateTransaction" mono />
      </Field>
      <div className="wz-row">
        <Field label="Entity Field" hint="Field name the rule engine uses to access the entity object.">
          <TextInput value={op.entityField} onChange={v => update('entityField', v)} placeholder="theAccount" mono />
        </Field>
        <Field label="Current Event Field" hint="Field name for the triggering event inside the rule context.">
          <TextInput value={op.currentEventField} onChange={v => update('currentEventField', v)} placeholder="theTransaction" mono />
        </Field>
      </div>
      <Field
        label="History Bindings"
        hint="Map each event type in the entity's history to a named field. Add one row per event type."
      >
        {(op.history || []).length === 0 && (
          <p className="wz-muted-note">No history bindings yet.</p>
        )}
        {(op.history || []).map((h, i) => (
          <HistoryRow
            key={h._key ?? i}
            h={h}
            index={i}
            total={(op.history || []).length}
            onChange={v => changeHistory(i, v)}
            onRemove={() => removeHistory(i)}
          />
        ))}
        <button className="wz-add-btn wz-add-btn-sm" onClick={addHistory}>+ Add History Binding</button>
      </Field>
      <Field
        label="Outbound Events"
        hint="Each entry maps a result field (array of events) to the event type assigned to each element. Add one row per outbound array."
      >
        {(op.outbounds || []).length === 0 && (
          <p className="wz-muted-note">No outbound events configured.</p>
        )}
        {(op.outbounds || []).map((o, i) => (
          <OutboundRow
            key={o._key ?? i}
            outbound={o}
            index={i}
            total={(op.outbounds || []).length}
            onChange={v => changeOutbound(i, v)}
            onRemove={() => removeOutbound(i)}
          />
        ))}
        <button className="wz-add-btn wz-add-btn-sm" onClick={addOutbound}>+ Add Outbound</button>
      </Field>
      <Field
        label="Schedule Requests Field (optional)"
        hint="Name of the result field holding the list of schedule requests this operation emits. Leave blank if the operation never schedules anything."
      >
        <TextInput value={op.scheduleRequestsField || ''} onChange={v => update('scheduleRequestsField', v)} placeholder="scheduleRequests" mono />
      </Field>
    </div>
  );
}

function StepRuleset({ form, setRuleset }) {
  const rs  = form.ruleset;
  const set = (k, v) => setRuleset({ ...rs, [k]: v });

  // ── Archive source: 'upload' or 'ads' ────────────────────────────────────────
  const [archiveSource, setArchiveSource] = useState('upload');

  // ── DI fetch state ───────────────────────────────────────────────────────────
  const [runtimes,         setRuntimes]         = useState(null);    // null=not loaded, []|[...]=loaded
  const [runtimesError,    setRuntimesError]    = useState(null);
  const [selectedRuntime,  setSelectedRuntime]  = useState('');
  const [deploymentSpace,  setDeploymentSpace]  = useState('');     // UI-overridable; seeded from env default
  const [decisions,        setDecisions]        = useState(null);    // null=not loaded
  const [decisionsState,   setDecisionsState]   = useState('idle');  // idle|loading|ready|error
  const [decisionsError,   setDecisionsError]   = useState(null);
  const [selectedDecision, setSelectedDecision] = useState('');
  const [fetchState,       setFetchState]       = useState('idle');  // idle|fetching|fetched|error
  const [fetchMsg,         setFetchMsg]         = useState(null);

  // Load runtime list once when the user switches to DI mode
  useEffect(() => {
    if (archiveSource !== 'ads' || runtimes !== null) return;
    setRuntimesError(null);
    fetch(`${API_URL}/api/ads-runtimes`)
      .then(r => r.ok ? r.json() : r.json().then(b => Promise.reject(new Error(b.error || r.statusText))))
      .then(data => setRuntimes(data))
      .catch(err => { setRuntimes([]); setRuntimesError(err.message); });
  }, [archiveSource, runtimes]);

  // Fetches decisions for the given runtime using an explicit space value.
  const fetchDecisions = async (rtId, space) => {
    setDecisionsState('loading');
    setDecisionsError(null);
    try {
      const spaceParam = (space || '').trim();
      const query      = spaceParam ? `?deploymentSpace=${encodeURIComponent(spaceParam)}` : '';
      const r = await fetch(`${API_URL}/api/ads-runtimes/${encodeURIComponent(rtId)}/decisions${query}`);
      const body = await r.json();
      if (!r.ok) throw new Error(body.error || r.statusText);
      setDecisions(body);
      setDecisionsState('ready');
    } catch (err) {
      setDecisionsState('error');
      setDecisionsError(err.message);
      setDecisions([]);
    }
  };

  // Called when the user selects a different runtime: resets state and seeds the
  // deploymentSpace field from the env default for that runtime.
  const loadDecisions = async (rtId) => {
    setSelectedRuntime(rtId);
    setDecisions(null);
    setSelectedDecision('');
    setFetchState('idle');
    setFetchMsg(null);
    if (!rtId) {
      setDeploymentSpace('');
      return;
    }
    // Seed the deploymentSpace field from the selected runtime's env default
    const rt = (runtimes || []).find(r => r.id === rtId);
    const seedSpace = rt?.deploymentSpace || '';
    setDeploymentSpace(seedSpace);
    await fetchDecisions(rtId, seedSpace);
  };

  // Called when the user edits the deploymentSpace field and clicks "Load Decisions".
  const reloadDecisions = () => {
    setDecisions(null);
    setSelectedDecision('');
    setFetchState('idle');
    setFetchMsg(null);
    fetchDecisions(selectedRuntime, deploymentSpace);
  };

  const handleFetchArchive = async () => {
    if (!selectedRuntime || !selectedDecision) return;
    const agentId = form.agentId.trim();
    if (!agentId) { setFetchState('error'); setFetchMsg('Set the Agent ID first (step 1) before fetching.'); return; }
    setFetchState('fetching');
    setFetchMsg(null);
    try {
      const spaceParam = deploymentSpace.trim();
      const spaceQuery = spaceParam ? `&deploymentSpace=${encodeURIComponent(spaceParam)}` : '';
      const r = await fetch(
        `${API_URL}/api/ads-runtimes/${encodeURIComponent(selectedRuntime)}/archive?decisionId=${encodeURIComponent(selectedDecision)}&agentId=${encodeURIComponent(agentId)}${spaceQuery}`,
        { method: 'POST' },
      );
      const body = await r.json();
      if (!r.ok) throw new Error(body.error || r.statusText);
      // Archive is now saved server-side; update the ruleset path.
      // _jarFile stays null — the Publish step already handles the "not-needed" case.
      setRuleset({ ...rs, _jarFile: null, path: body.path });
      setFetchState('fetched');
      setFetchMsg(`Archive saved — ${body.path}`);
    } catch (err) {
      setFetchState('error');
      setFetchMsg(`Fetch failed: ${err.message}`);
    }
  };

  const addOperation    = () => set('operations', [...(rs.operations || []), { ...DEFAULT_OPERATION, history: [] }]);
  const removeOperation = i  => set('operations', (rs.operations || []).filter((_, idx) => idx !== i));
  const changeOperation = (i, val) => set('operations', (rs.operations || []).map((op, idx) => idx === i ? val : op));

  return (
    <div className="wz-step-body">

      {/* ── Archive source selector ──────────────────────────────────────── */}
      <div className="wz-source-selector" role="group" aria-label="Archive source">
        <button
          className={`wz-source-btn${archiveSource === 'upload' ? ' wz-source-btn-active' : ''}`}
          onClick={() => setArchiveSource('upload')}
          type="button"
        >
          ↑ Upload Decision Archive
        </button>
        <button
          className={`wz-source-btn${archiveSource === 'ads' ? ' wz-source-btn-active' : ''}`}
          onClick={() => setArchiveSource('ads')}
          type="button"
        >
          ⬇ Fetch from DI Runtime
        </button>
      </div>

      {/* ── Option 1: Upload ─────────────────────────────────────────────── */}
      {archiveSource === 'upload' && (
        <Field
          label="Ruleset JAR *"
          hint="Select the compiled Decision Intelligence rule archive (.jar). The path is set automatically. The file is uploaded to the BFF server in the Publish & Deploy step."
        >
          <div className="wz-file-row">
            <label className="wz-file-btn">
              Choose JAR…
              <input
                type="file"
                accept=".jar"
                style={{ display: 'none' }}
                onChange={e => {
                  const file = e.target.files[0] || null;
                  const agentId = form.agentId.trim();
                  setRuleset({
                    ...rs,
                    _jarFile: file,
                    path: file ? `samples/${agentId || '<agentId>'}/${file.name}` : rs.path,
                  });
                }}
              />
            </label>
            {rs._jarFile
              ? <span className="wz-file-name">{rs._jarFile.name}</span>
              : rs.path
                ? <span className="wz-file-name wz-mono">{rs.path}</span>
                : <span className="wz-file-name wz-muted">No file selected</span>
            }
          </div>
        </Field>
      )}

      {/* ── Option 2: Fetch from DI Runtime ────────────────────────────── */}
      {archiveSource === 'ads' && (
        <div className="wz-ads-panel">
          {/* Runtime select */}
          <Field label="DI Runtime" hint="Select the IBM Decision Intelligence runtime instance to browse.">
            {runtimesError && (
              <p className="wz-ads-error">Could not load runtimes: {runtimesError}</p>
            )}
            {runtimes === null && !runtimesError && (
              <p className="wz-ads-loading">Loading runtimes…</p>
            )}
            {runtimes !== null && runtimes.length === 0 && !runtimesError && (
              <p className="wz-ads-empty">
                No DI runtimes configured. Add entries to <code>DI_RUNTIMES</code> in your <code>.env</code> file.
              </p>
            )}
            {runtimes !== null && runtimes.length > 0 && (
              <select
                className="wz-select"
                value={selectedRuntime}
                onChange={e => loadDecisions(e.target.value)}
                aria-label="Select DI runtime"
              >
                <option value="">— select a runtime —</option>
                {runtimes.map(rt => (
                  <option key={rt.id} value={rt.id}>{rt.name}</option>
                ))}
              </select>
            )}
          </Field>

          {/* Deployment Space override */}
          {selectedRuntime && (
            <Field
              label="Deployment Space"
              hint="Deployment space used to browse and fetch decisions. Pre-populated from the .env default; edit to override, then click Load Decisions."
            >
              <div className="wz-file-row">
                <TextInput
                  value={deploymentSpace}
                  onChange={v => setDeploymentSpace(v)}
                  placeholder="embedded"
                  mono
                />
                <button
                  className="wz-primary-btn"
                  type="button"
                  onClick={reloadDecisions}
                  disabled={!deploymentSpace.trim() || decisionsState === 'loading'}
                  aria-label="Load decisions for deployment space"
                >
                  {decisionsState === 'loading' ? 'Loading…' : 'Load Decisions'}
                </button>
              </div>
            </Field>
          )}

          {/* Decision list */}
          {selectedRuntime && (
            <Field label="Decision" hint="Select the deployed decision whose archive you want to fetch.">
              {decisionsState === 'loading' && (
                <p className="wz-ads-loading">Loading decisions…</p>
              )}
              {decisionsState === 'error' && (
                <p className="wz-ads-error">Could not load decisions: {decisionsError}</p>
              )}
              {decisionsState === 'ready' && decisions.length === 0 && (
                <p className="wz-ads-empty">No decisions deployed on this runtime.</p>
              )}
              {decisionsState === 'ready' && decisions.length > 0 && (
                <select
                  className="wz-select"
                  value={selectedDecision}
                  onChange={e => { setSelectedDecision(e.target.value); setFetchState('idle'); setFetchMsg(null); }}
                  aria-label="Select decision"
                >
                  <option value="">— select a decision —</option>
                  {decisions.map(d => (
                    <option key={d.id} value={d.id}>
                      {d.name || d.id}
                    </option>
                  ))}
                </select>
              )}
            </Field>
          )}

          {/* Fetch button */}
          {selectedDecision && (
            <div className="wz-deploy-row">
              <button
                className={`wz-primary-btn${fetchState === 'fetching' ? ' wz-btn-busy' : ''}`}
                disabled={fetchState === 'fetching' || fetchState === 'fetched'}
                onClick={handleFetchArchive}
                type="button"
              >
                {fetchState === 'fetching' ? 'Fetching…' : fetchState === 'fetched' ? '✓ Fetched' : '⬇ Fetch Archive'}
              </button>
              {fetchMsg && (
                <span className={`wz-deploy-msg ${fetchState === 'error' ? 'wz-deploy-msg-err' : 'wz-deploy-msg-ok'}`}>
                  {fetchMsg}
                </span>
              )}
            </div>
          )}

          {/* Current path (once fetched) */}
          {rs.path && (
            <p className="wz-muted-note">Archive path: <code>{rs.path}</code></p>
          )}
        </div>
      )}

      <Field
        label="Archive Control Topic"
        hint="Kafka topic used to reload the ruleset JAR at runtime without restarting the job (LOAD / PREFETCH / ROLLBACK commands). Leave blank to disable."
      >
        <TextInput value={rs.archiveControlTopic} onChange={v => set('archiveControlTopic', v)} placeholder="agent-archive-control" mono />
      </Field>
      <div className="wz-section-divider">
        <span className="wz-section-label">Operations</span>
        <span className="wz-hint">
          Define how each rule-engine operation maps the entity and event fields. Optional — leave empty for simple single-operation rulesets.
        </span>
      </div>
      {(rs.operations || []).map((op, i) => (
        <OperationRow
          key={i}
          op={op}
          index={i}
          total={(rs.operations || []).length}
          onChange={v => changeOperation(i, v)}
          onRemove={() => removeOperation(i)}
        />
      ))}
      <button className="wz-add-btn" onClick={addOperation}>+ Add Operation</button>
    </div>
  );
}

// ── Step 4: Outputs ───────────────────────────────────────────────────────────

function OutputRow({ out, onChange, onRemove, index, total }) {
  const update = (k, v) => onChange({ ...out, [k]: v });
  return (
    <div className="wz-card">
      <div className="wz-card-header">
        <span className="wz-card-title">Output {index + 1}</span>
        {total > 1 && (
          <button className="wz-remove-btn" onClick={onRemove} title="Remove">✕</button>
        )}
      </div>
      <div className="wz-row">
        <Field label="Event Type" hint="Logical name for events written to this output (e.g. decision-result, agent-error).">
          <TextInput value={out.eventType} onChange={v => update('eventType', v)} placeholder="decision-result" mono />
        </Field>
        <Field label="Kafka Topic" hint="Topic where this event type will be published.">
          <TextInput value={out.topic} onChange={v => update('topic', v)} placeholder="agent-decision-results" mono />
        </Field>
      </div>
      <Field label="Description" hint="Optional label shown in Flink operator names and logs.">
        <TextInput value={out.description} onChange={v => update('description', v)} placeholder="Rule engine decisions" />
      </Field>
    </div>
  );
}

function StepOutputs({ form, setOutputs }) {
  const add = () => setOutputs([...form.outputs, { _key: newKey(), eventType: '', topic: '', description: '' }]);
  const remove = i => setOutputs(form.outputs.filter((_, idx) => idx !== i));
  const change = (i, val) => setOutputs(form.outputs.map((o, idx) => idx === i ? val : o));

  return (
    <div className="wz-step-body">
      <p className="wz-note">
        Add one output per event type your rule engine produces. Include an <code>agent-error</code>{' '}
        output to capture processing failures — if omitted, the job falls back to the{' '}
        <code>AGENT_ERROR_TOPIC</code> environment variable.
      </p>
      {form.outputs.map((out, i) => (
        <OutputRow key={out._key ?? i} out={out} index={i} total={form.outputs.length}
          onChange={v => change(i, v)} onRemove={() => remove(i)} />
      ))}
      <button className="wz-add-btn" onClick={add}>+ Add Output</button>
    </div>
  );
}

// ── Step 5: State & Review ────────────────────────────────────────────────────

function StepReview({ form, setStateConfig }) {
  const sc = form.stateConfig;
  const set = (k, v) => setStateConfig({ ...sc, [k]: v });
  const { descriptor } = buildDescriptor(form);
  const json = JSON.stringify(descriptor, null, 2);

  const download = useCallback(() => {
    const blob = new Blob([json], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'agents.json';
    a.click();
    URL.revokeObjectURL(url);
  }, [json]);

  const copy = useCallback(() => {
    navigator.clipboard.writeText(json).catch(() => {});
  }, [json]);

  return (
    <div className="wz-step-body">
      <div className="wz-row">
        <Field
          label="State TTL"
          hint="How long per-entity state is retained after the last event. Examples: 1m, 30d, 24h, 60mn. Use a value that covers your longest expected gap between events."
        >
          <TextInput value={sc.ttl} onChange={v => set('ttl', v)} placeholder="30d" mono />
        </Field>
        <Field
          label="State Backend"
          hint="Storage backend for entity state. RocksDB persists to disk (suitable for large state); heap keeps state in memory (faster, but limited by JVM heap)."
        >
          <Select value={sc.backend} onChange={v => set('backend', v)} options={[
            { value: 'rocksdb', label: 'rocksdb — disk-backed (recommended for production)' },
            { value: 'heap',    label: 'heap — in-memory (for small state or dev/test)' },
          ]} />
        </Field>
      </div>

      <div className="wz-preview-header">
        <span className="wz-section-label">Generated agents.json</span>
        <div className="wz-preview-actions">
          <button className="wz-ghost-btn" onClick={copy}>Copy</button>
          <button className="wz-primary-btn" onClick={download}>⬇ Download agents.json</button>
        </div>
      </div>
      <pre className="wz-json-preview">{json}</pre>
      <p className="wz-note">
        Review the descriptor above. Click <strong>Next</strong> to save and deploy, or
        download to keep a local copy.
      </p>
    </div>
  );
}

// ── Merge a saved descriptor back into wizard form state ─────────────────────
// Converts the agents.json shape back to the flat wizard form shape so the
// wizard can be pre-populated when editing an existing agent.
//
// targetAgentId: when provided, load the agent matching that id from the array;
//   falls back to agents[0] when not found (single-agent / backward-compat case).
// useCaseId: the BFF folder key for this descriptor, pre-filled into form.useCaseId.

export function descriptorToForm(descriptor, targetAgentId, useCaseId) {
  if (!descriptor) return useCaseId ? { ...DEFAULT_AGENT, useCaseId } : DEFAULT_AGENT;
  const allAgents = descriptor.agents || [];
  if (!allAgents.length) return useCaseId ? { ...DEFAULT_AGENT, useCaseId } : DEFAULT_AGENT;

  // Find the agent to edit: prefer the one matching targetAgentId, else fall back to [0].
  const agent = (targetAgentId && allAgents.find(a => a.agentId === targetAgentId))
    || allAgents[0];

  const rs = agent.ruleset || {};
  const sc = agent.stateConfig || {};

  const subscriptions = (agent.subscriptions || []).map(s => {
    const h = s.horizon || {};
    return {
      _key: newKey(),
      eventType: s.eventType || '',
      // The Java model uses a single-string topic field.  Accept both spellings for
      // resilience when loading hand-authored descriptors that pre-date this fix.
      topic: s.topic || (Array.isArray(s.topics) ? s.topics[0] : s.topics) || '',
      // operation is required in the model; null means the field was absent in JSON (legacy) —
      // treat it as "" (history-only) in the wizard so the form is always in a valid state.
      operation: s.operation ?? '',
      entityKeyExtractor: s.entityKeyExtractor || '',
      horizonType: h.type || 'count',
      horizonValue: h.value ?? 10,
      horizonCountLimit: h.countLimit ?? 100,
      horizonTimeLimit: h.timeLimit || '24h',
      horizonDescription: h.description || '',
    };
  });

  const operations = (rs.operations || []).map(op => ({
    operation: op.operation || '',
    entityField: op.entityField || '',
    currentEventField: op.currentEventField || '',
    history: (op.history || []).map(h => ({
      eventType: h.eventType || '',
      field: h.field || '',
    })),
    outbounds: (op.outbounds || []).map(o => ({
      field: o.field || '',
      eventType: o.eventType || '',
    })),
    scheduleRequestsField: op.scheduleRequestsField || '',
  }));

  return {
    parallelism: descriptor.parallelism ?? 0,
    useCaseId:   useCaseId || '',    // pre-filled by caller when editing an existing use-case
    useCaseName:        descriptor.name            || '',
    flinkJobName:       descriptor.flinkJobName    || '',
    useCaseDescription: descriptor.description     || '',
    consumerGroupId:    descriptor.consumerGroupId || '',
    agentId: agent.agentId || '',
    name: agent.name || '',
    entityType: agent.entityType || '',
    entityKeyExtractor: agent.entityKeyExtractor || '$.id',
    agentParallelism: agent.parallelism ?? 0,
    // chainingEnabled is absent from most descriptors (defaults to true in the backend).
    // Treat null/undefined as true so the wizard form stays consistent with the Java default.
    chainingEnabled: agent.chainingEnabled !== false,
    subscriptions: subscriptions.length ? subscriptions : DEFAULT_AGENT.subscriptions,
    ruleset: {
      path: rs.path || '',
      outputField: rs.outputField || '',
      archiveControlTopic: rs.archiveControlTopic || '',
      operations,
    },
    outputs: (agent.outputs || []).map(o => ({
      _key: newKey(),
      eventType: o.eventType || '',
      topic: o.topic || '',
      description: o.description || '',
    })),
    stateConfig: {
      ttl: sc.ttl || '30d',
      backend: sc.backend || 'rocksdb',
    },
  };
}

// ── Wizard root ────────────────────────────────────────────────────────────────

// props:
//   onClose          — close the wizard
//   onJobRunning     — callback(useCaseId) when job reaches RUNNING
//   initialDescriptor — agents.json object to pre-fill (edit mode)
//   initialUseCaseId  — BFF folder key of the existing use case (edit/append/patch mode);
//                       for a new use case it is derived from the Use-Case Name or Agent ID
//   initialTargetAgentId — agentId to edit within a multi-agent descriptor (patch mode)
//   initialMode       — "replace" | "append" | "patch" (default: "replace")
//     replace: create/overwrite entire use-case (single-agent create flow)
//     append:  add a new agent to an existing use-case
//     patch:   edit one existing agent inside a use-case without touching siblings
function AgentWizard({ onClose, onJobRunning, initialDescriptor, initialUseCaseId, initialTargetAgentId, initialMode }) {
  // isEditMode = true whenever the wizard is opened with an existing descriptor to edit
  // (both single-agent replace-edit and multi-agent patch flows).
  const isEditMode = !!initialDescriptor || initialMode === 'patch';
  const [step, setStep] = useState(0);
  const [form, setForm] = useState(() =>
    descriptorToForm(initialDescriptor, initialTargetAgentId, initialUseCaseId)
  );
  const [error, setError] = useState(null);

  // The mode is fixed for the lifetime of this wizard session.
  const saveMode = initialMode || 'replace';

  const set = (key, val) => setForm(f => ({ ...f, [key]: val }));
  const setSubscriptions = val => set('subscriptions', val);
  const setRuleset       = val => set('ruleset', val);
  const setOutputs       = val => set('outputs', val);
  const setStateConfig   = val => set('stateConfig', val);

  const next = () => {
    const err = validateStep(step, form);
    if (err) { setError(err); return; }
    setError(null);
    setStep(s => Math.min(s + 1, STEPS.length - 1));
  };

  const back = () => { setError(null); setStep(s => Math.max(s - 1, 0)); };

  // When the user arrives on Step 5 (Publish & Deploy), auto-trigger the initial save.
  // buildDescriptor returns { descriptor, useCaseId }; we pass only descriptor to saveFn.
  const handleEnterPublish = useCallback((saveFn) => {
    const { descriptor } = buildDescriptor(form);
    saveFn(descriptor);
  }, [form]);

  // Effective use-case id for display and job-lifecycle calls
  const effectiveUseCaseId = resolveUseCaseId(form);

  return (
    <div className="wz-overlay">
      <div className="wz-modal">
        {/* Header */}
        <div className="wz-modal-header">
          <div>
            <div className="wz-modal-title">
              {initialMode === 'patch' ? 'Edit Agent' : isEditMode ? 'Edit Agent Descriptor' : 'New Agent Descriptor'}
            </div>
            <div className="wz-modal-subtitle">Step {step + 1} of {STEPS.length} — {STEPS[step]}</div>
          </div>
          <button className="wz-close-btn" onClick={onClose} title="Close">✕</button>
        </div>

        {/* Step indicators */}
        <div className="wz-steps">
          {STEPS.map((label, i) => (
            <div
              key={i}
              className={`wz-step-indicator${i === step ? ' active' : ''}${i < step ? ' done' : ''}`}
            >
              <div className="wz-step-dot">{i < step ? '✓' : i + 1}</div>
              <div className="wz-step-label">{label}</div>
            </div>
          ))}
        </div>

        {/* Body */}
        <div className="wz-modal-body">
          {step === 0 && <StepIdentity      form={form} set={set} saveMode={saveMode} />}
          {step === 1 && <StepRuleset       form={form} setRuleset={setRuleset} />}
          {step === 2 && <StepSubscriptions form={form} setSubscriptions={setSubscriptions} />}
          {step === 3 && <StepOutputs       form={form} setOutputs={setOutputs} />}
          {step === 4 && <StepReview        form={form} setStateConfig={setStateConfig} />}
          {step === 5 && (
            <StepPublish
              useCaseId={effectiveUseCaseId}
              agentId={form.agentId.trim()}
              mode={saveMode}
              jarFile={form.ruleset._jarFile || null}
              descriptor={buildDescriptor(form).descriptor}
              onMountSave={handleEnterPublish}
              onJobRunning={onJobRunning}
            />
          )}
        </div>

        {/* Error */}
        {error && <div className="wz-error">{error}</div>}

        {/* Footer */}
        <div className="wz-modal-footer">
          <button className="wz-ghost-btn" onClick={step === 0 ? onClose : back}>
            {step === 0 ? 'Cancel' : '← Back'}
          </button>
          {step < STEPS.length - 1 && (
            <button className="wz-primary-btn" onClick={next}>Next →</button>
          )}
          {step === STEPS.length - 1 && (
            <button className="wz-ghost-btn" onClick={onClose}>Close</button>
          )}
        </div>
      </div>
    </div>
  );
}

export default AgentWizard;

