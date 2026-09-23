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
import React, { useState } from 'react';

/**
 * ProcessingErrorCard — generic presentation component for pipeline processing errors.
 *
 * Accepts a normalized `error` object:
 *   {
 *     source:     string?   — which processor/agent emitted the error
 *     entityId:   string?   — entity being processed when the error occurred
 *     origin:     string?   — source topic or stage label
 *     reason:     string?   — human-readable error reason
 *     original:   object?   — original event payload (collapsible)
 *     timestamp:  number|string?
 *   }
 *
 * The component is domain-agnostic.
 */
export default function ProcessingErrorCard({ error }) {
  const [expanded, setExpanded] = useState(false);
  if (!error) return null;

  const hasOriginal = error.original && typeof error.original === 'object'
    && Object.keys(error.original).length > 0;

  return (
    <div className="data-item processing-error-card">
      {/* Header */}
      <div className="processing-error-header">
        <div className="processing-error-badge">Error</div>
        <div className="processing-error-title">
          {error.source || 'processor'}
          {error.entityId && error.entityId !== 'unknown' && (
            <span className="processing-error-entity">{error.entityId}</span>
          )}
        </div>
        {error.origin && (
          <span className="processing-error-origin">{error.origin}</span>
        )}
      </div>

      {/* Error reason */}
      {error.reason && (
        <div className="processing-error-reason">{error.reason}</div>
      )}

      {/* Original event — collapsible */}
      {hasOriginal && (
        <div className="processing-error-original">
          <button
            className="processing-error-toggle"
            onClick={() => setExpanded(v => !v)}
            aria-expanded={expanded}
          >
            {expanded ? '▾' : '▸'} Original Event
          </button>
          {expanded && (
            <pre className="processing-error-json">
              {JSON.stringify(error.original, null, 2)}
            </pre>
          )}
        </div>
      )}

      {/* Timestamp */}
      {error.timestamp && (
        <div className="processing-error-timestamp">
          {new Date(
            typeof error.timestamp === 'number' ? error.timestamp : Date.parse(error.timestamp)
          ).toLocaleString()}
        </div>
      )}
    </div>
  );
}
