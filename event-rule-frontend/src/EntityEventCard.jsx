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
import React from 'react';

/**
 * EntityEventCard — generic presentation component for entity/domain events.
 *
 * Accepts a normalized `entity` object produced by a domain adapter:
 *
 *   {
 *     id:           string          — primary identifier (e.g. account id, order id)
 *     label:        string?         — prefix label before the id (e.g. "Account", "Order")
 *     name:         string?         — secondary name beside the id
 *     status:       string?         — status badge text
 *     statusStyle:  { bg, color, border }?   — status badge colours
 *     accentColor:  string?         — left border colour
 *     badges:       { label, style }[]?      — additional badges in the header
 *     fields:       { key, value }[]         — key/value pairs to display
 *     chips:        { label, cls }[]?        — small signal/tag chips at the bottom
 *     extras:       ReactNode?      — domain-specific content (e.g. item lists)
 *     timestamp:    number|string?
 *   }
 *
 * The component is domain-agnostic.
 */

function StatusBadge({ label, style }) {
  if (!label) return null;
  const s = style || { bg: '#f3f4f6', color: '#374151', border: '#d1d5db' };
  return (
    <span style={{
      display: 'inline-block',
      padding: '2px 10px',
      borderRadius: '12px',
      fontSize: '0.78rem',
      fontWeight: 700,
      background: s.bg,
      color: s.color,
      border: `1px solid ${s.border}`,
    }}>
      {label}
    </span>
  );
}

export default function EntityEventCard({ entity }) {
  if (!entity) return null;

  return (
    <div className="data-item entity-event-card" style={entity.accentColor ? { borderLeftColor: entity.accentColor } : undefined}>
      {/* Header */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: '6px', marginBottom: '8px' }}>
        <div style={{ fontWeight: 600, fontSize: '0.9rem', color: '#1f2328' }}>
          {entity.label && (
            <span style={{ color: '#57606a', fontWeight: 400, marginRight: '4px' }}>{entity.label}:</span>
          )}
          {entity.id}
          {entity.name && (
            <span style={{ marginLeft: '8px', fontWeight: 400, color: '#57606a', fontSize: '0.85rem' }}>
              {entity.name}
            </span>
          )}
        </div>
        <div style={{ display: 'flex', gap: '6px', flexWrap: 'wrap', alignItems: 'center' }}>
          {(entity.badges || []).map((b, i) => (
            <StatusBadge key={i} label={b.label} style={b.style} />
          ))}
          <StatusBadge label={entity.status} style={entity.statusStyle} />
        </div>
      </div>

      {/* Key-value fields */}
      {entity.fields && entity.fields.length > 0 && (
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: '14px', fontSize: '0.82rem', color: '#57606a', marginBottom: entity.extras || entity.chips ? '10px' : '0' }}>
          {entity.fields.map((f, i) => (
            <span key={i}>
              <strong style={{ color: '#1f2328' }}>{f.key}: </strong>
              {f.value}
            </span>
          ))}
        </div>
      )}

      {/* Domain-specific extras (item lists, browsing signals, etc.) */}
      {entity.extras}

      {/* Chip row */}
      {entity.chips && entity.chips.length > 0 && (
        <div style={{ display: 'flex', gap: '6px', flexWrap: 'wrap', marginTop: '6px' }}>
          {entity.chips.map((chip, i) => (
            <span key={i} className={`entity-chip${chip.cls ? ` ${chip.cls}` : ''}`}>
              {chip.label}
            </span>
          ))}
        </div>
      )}

      {/* Timestamp */}
      {entity.timestamp && (
        <div style={{ fontSize: '0.7rem', color: '#9ca3af', marginTop: '6px', textAlign: 'right' }}>
          {new Date(typeof entity.timestamp === 'number' ? entity.timestamp : Date.parse(entity.timestamp)).toLocaleString()}
        </div>
      )}
    </div>
  );
}
