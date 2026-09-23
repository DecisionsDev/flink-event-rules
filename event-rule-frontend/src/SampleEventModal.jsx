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
import React, { useState, useEffect, useCallback, useRef } from 'react';

/**
 * SampleEventModal
 *
 * Opens as an overlay when the user clicks "Generate Sample Event".
 * Flow:
 *   1. Fetches GET /api/agents/:agentId/sample-input → pre-populated JSON template.
 *   2. Renders an editable <textarea> pre-filled with that JSON.
 *   3. The user picks the target Kafka topic from a <select>.
 *   4. "Send Event" validates the JSON, then calls POST /api/use-cases/:useCase/events.
 *
 * Props:
 *   agentId   {string}   — the agent folder id (e.g. "agent-banking")
 *   useCase   {string}   — the use-case folder id (same as agentId for agent use cases)
 *   onClose   {function} — called when the modal is dismissed
 */

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8080';

function SampleEventModal({ agentId, useCase, onClose }) {
  // ── State ──────────────────────────────────────────────────────────────────
  const [loadState, setLoadState]     = useState('loading'); // loading | ready | error
  const [loadError, setLoadError]     = useState('');
  const [topics, setTopics]           = useState([]);        // [{name, type, label}]
  const [selectedTopic, setSelectedTopic] = useState('');
  const [samplesByTopic, setSamplesByTopic] = useState({}); // topicName → sample object
  const [jsonText, setJsonText]       = useState('');
  const [jsonError, setJsonError]     = useState('');
  const [sendState, setSendState]     = useState('idle');    // idle | sending | success | error
  const [sendMessage, setSendMessage] = useState('');

  const textareaRef = useRef(null);

  // ── Fetch sample on mount ──────────────────────────────────────────────────
  useEffect(() => {
    const fetchId = agentId || useCase;
    fetch(`${API_URL}/api/agents/${fetchId}/sample-input`)
      .then(r => r.ok ? r.json() : r.json().then(b => Promise.reject(new Error(b.error || r.statusText))))
      .then(data => {
        setTopics(data.topics || []);
        setSamplesByTopic(data.samplesByTopic || {});
        setSelectedTopic(data.topic || '');
        setJsonText(JSON.stringify(data.sample, null, 2));
        setLoadState('ready');
      })
      .catch(err => {
        setLoadError(err.message || 'Failed to generate sample');
        setLoadState('error');
      });
  }, [agentId, useCase]);

  // ── Auto-resize textarea ───────────────────────────────────────────────────
  useEffect(() => {
    const ta = textareaRef.current;
    if (!ta) return;
    ta.style.height = 'auto';
    ta.style.height = Math.min(ta.scrollHeight, 420) + 'px';
  }, [jsonText]);

  // ── Close on Escape ────────────────────────────────────────────────────────
  useEffect(() => {
    const handler = (e) => { if (e.key === 'Escape') onClose(); };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [onClose]);

  // ── JSON validation ────────────────────────────────────────────────────────
  const validateJson = useCallback((text) => {
    if (!text.trim()) { setJsonError('Payload cannot be empty.'); return null; }
    try {
      const parsed = JSON.parse(text);
      setJsonError('');
      return parsed;
    } catch (e) {
      setJsonError(`Invalid JSON: ${e.message}`);
      return null;
    }
  }, []);

  const handleTextChange = (e) => {
    const val = e.target.value;
    setJsonText(val);
    // Clear send result on any edit
    if (sendState === 'success' || sendState === 'error') {
      setSendState('idle');
      setSendMessage('');
    }
    // Only show JSON error after the user has started typing something
    if (val.trim()) validateJson(val);
    else setJsonError('');
  };

  // ── Send event ─────────────────────────────────────────────────────────────
  const handleSend = async () => {
    const parsed = validateJson(jsonText);
    if (!parsed) return;
    if (!selectedTopic) { setJsonError('Please select a topic.'); return; }

    setSendState('sending');
    setSendMessage('');

    try {
      const resp = await fetch(`${API_URL}/api/use-cases/${useCase}/events`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ topic: selectedTopic, events: [parsed] }),
      });
      const body = await resp.json();
      if (!resp.ok) throw new Error(body.error || resp.statusText);
      setSendState('success');
      setSendMessage(`Event published to "${selectedTopic}" — watch the results panel.`);
    } catch (err) {
      setSendState('error');
      setSendMessage(`Send failed: ${err.message}`);
    }
  };

  // ── Topic selector change — swap editor content to the new topic's sample ──
  const handleTopicChange = (newTopic) => {
    setSelectedTopic(newTopic);
    setSendState('idle');
    setSendMessage('');
    // If we have a pre-built sample for this topic, replace the editor content.
    // If not (e.g. custom wizard agent with only one topic), keep the current text.
    const topicSample = samplesByTopic[newTopic];
    if (topicSample !== undefined) {
      setJsonText(JSON.stringify(topicSample, null, 2));
      setJsonError('');
    }
  };

  // ── Regenerate sample ──────────────────────────────────────────────────────
  const handleRegenerate = () => {
    setLoadState('loading');
    setLoadError('');
    setSendState('idle');
    setSendMessage('');
    setJsonError('');
    const fetchId = agentId || useCase;
    fetch(`${API_URL}/api/agents/${fetchId}/sample-input`)
      .then(r => r.ok ? r.json() : r.json().then(b => Promise.reject(new Error(b.error || r.statusText))))
      .then(data => {
        setSamplesByTopic(data.samplesByTopic || {});
        setTopics(data.topics || []);
        // Keep the currently selected topic; refresh its sample with new random values.
        setSelectedTopic(prev => {
          const topic = prev || data.topic || '';
          const newSamples = data.samplesByTopic || {};
          const freshSample = newSamples[topic] ?? data.sample;
          setJsonText(JSON.stringify(freshSample, null, 2));
          return topic;
        });
        setLoadState('ready');
      })
      .catch(err => {
        setLoadError(err.message || 'Failed to generate sample');
        setLoadState('error');
      });
  };

  const isSending  = sendState === 'sending';
  const isDisabled = isSending || loadState !== 'ready' || !!jsonError || !jsonText.trim();

  // ── Render ─────────────────────────────────────────────────────────────────
  return (
    <div className="sample-modal-overlay" onClick={(e) => { if (e.target === e.currentTarget) onClose(); }}>
      <div className="sample-modal" role="dialog" aria-modal="true" aria-label="Generate Sample Event">

        {/* Header */}
        <div className="sample-modal-header">
          <div className="sample-modal-title">Generate Sample Event</div>
          <button className="sample-modal-close" onClick={onClose} aria-label="Close">×</button>
        </div>

        {/* Body */}
        <div className="sample-modal-body">

          {loadState === 'loading' && (
            <div className="sample-modal-loading">
              <span className="sample-modal-spinner" aria-hidden="true" />
              Generating sample payload…
            </div>
          )}

          {loadState === 'error' && (
            <div className="sample-modal-banner sample-modal-banner-error">
              {loadError}
              <button className="sample-modal-retry-btn" onClick={handleRegenerate}>Retry</button>
            </div>
          )}

          {loadState === 'ready' && (
            <>
              {/* Topic selector */}
              {topics.length > 1 && (
                <div className="sample-modal-field">
                  <label className="sample-modal-label" htmlFor="sample-topic-select">
                    Target Topic
                  </label>
                  <select
                    id="sample-topic-select"
                    className="sample-modal-select"
                    value={selectedTopic}
                    onChange={e => handleTopicChange(e.target.value)}
                    disabled={isSending}
                  >
                    {topics.map(t => (
                      <option key={t.name} value={t.name}>
                        {t.label || t.type} ({t.name})
                      </option>
                    ))}
                  </select>
                </div>
              )}

              {/* JSON editor */}
              <div className="sample-modal-field">
                <div className="sample-modal-label-row">
                  <label className="sample-modal-label" htmlFor="sample-json-editor">
                    Event Payload
                  </label>
                  <button
                    className="sample-modal-regen-btn"
                    onClick={handleRegenerate}
                    disabled={isSending}
                    title="Re-generate with fresh random values"
                  >
                    ↺ Regenerate
                  </button>
                </div>
                <textarea
                  id="sample-json-editor"
                  ref={textareaRef}
                  className={`sample-modal-editor${jsonError ? ' sample-modal-editor-error' : ''}`}
                  value={jsonText}
                  onChange={handleTextChange}
                  spellCheck={false}
                  disabled={isSending}
                  aria-describedby={jsonError ? 'sample-json-error' : undefined}
                />
                {jsonError && (
                  <div id="sample-json-error" className="sample-modal-field-error">{jsonError}</div>
                )}
              </div>

              {/* Send result banner */}
              {(sendState === 'success' || sendState === 'error') && (
                <div className={`sample-modal-banner ${sendState === 'success' ? 'sample-modal-banner-ok' : 'sample-modal-banner-error'}`}>
                  {sendState === 'success' ? '✓ ' : '✗ '}{sendMessage}
                </div>
              )}
            </>
          )}
        </div>

        {/* Footer */}
        <div className="sample-modal-footer">
          <button
            className="sample-modal-btn sample-modal-btn-ghost"
            onClick={onClose}
            disabled={isSending}
          >
            Close
          </button>
          <button
            className="sample-modal-btn sample-modal-btn-primary"
            onClick={handleSend}
            disabled={isDisabled}
          >
            {isSending ? 'Sending…' : 'Send Event'}
          </button>
        </div>

      </div>
    </div>
  );
}

export default SampleEventModal;

