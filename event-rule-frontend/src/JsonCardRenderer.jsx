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
 * JsonCardRenderer — universal recursive JSON → Card UI renderer.
 */
function toLabel(key) {
  if (!key || typeof key !== 'string') return String(key ?? '');

  // Replace separators (_, -, .) with spaces
  let s = key.replace(/[_.-]+/g, ' ');

  // Insert space before sequences: lowercase→uppercase, uppercase→uppercase+lowercase
  // e.g. "camelCase" → "camel Case", "APIKey" → "API Key"
  s = s
    .replace(/([a-z])([A-Z])/g, '$1 $2')
    .replace(/([A-Z]+)([A-Z][a-z])/g, '$1 $2');

  // Title-case each word
  return s
    .split(/\s+/)
    .filter(Boolean)
    .map(w => w.charAt(0).toUpperCase() + w.slice(1))
    .join(' ');
}

// ── Type helpers ──────────────────────────────────────────────────────────────

function isPrimitive(v) {
  return v === null || v === undefined || typeof v === 'string'
    || typeof v === 'number' || typeof v === 'boolean';
}

function isAllPrimitives(arr) {
  return arr.every(isPrimitive);
}

// ── Primitive value formatter ─────────────────────────────────────────────────

function formatPrimitive(v) {
  if (v === null || v === undefined) return <span style={styles.nullValue}>Not provided</span>;
  if (typeof v === 'boolean') return <span style={styles.boolValue}>{v ? 'Yes' : 'No'}</span>;
  if (typeof v === 'number') return <span style={styles.numValue}>{v}</span>;

  const s = String(v);

  // Detect ISO timestamps and format them nicely
  if (/^\d{4}-\d{2}-\d{2}T[\d:.Z+-]+$/.test(s)) {
    try {
      return <span style={styles.strValue}>{new Date(s).toLocaleString()}</span>;
    } catch { /* fall through */ }
  }

  // Detect epoch-ms timestamps (13-digit numbers stored as strings)
  if (/^\d{13}$/.test(s)) {
    const ms = Number(s);
    if (ms > 1_000_000_000_000) {
      return <span style={styles.strValue}>{new Date(ms).toLocaleString()}</span>;
    }
  }

  return <span style={styles.strValue}>{s}</span>;
}

// ── Sub-components ────────────────────────────────────────────────────────────

/** Single label/value row */
function LabelValue({ label, value }) {
  return (
    <div style={styles.labelValueRow}>
      <span style={styles.label}>{label}</span>
      <span style={styles.value}>{formatPrimitive(value)}</span>
    </div>
  );
}

/** Chip list for arrays of primitives */
function ChipList({ items }) {
  if (!items || items.length === 0) {
    return <span style={styles.emptyText}>No items</span>;
  }
  return (
    <div style={styles.chipList}>
      {items.map((item, i) => (
        <span key={i} style={styles.chip}>
          {item === null || item === undefined ? 'null' : String(item)}
        </span>
      ))}
    </div>
  );
}

/** Collapsible nested card for object values */
function NestedCard({ label, children, defaultOpen }) {
  const [open, setOpen] = useState(defaultOpen);

  return (
    <div style={styles.nestedCard}>
      <button
        style={styles.nestedCardToggle}
        onClick={() => setOpen(o => !o)}
        aria-expanded={open}
        type="button"
      >
        <span style={styles.nestedCardTitle}>{label}</span>
        <span style={{ ...styles.chevron, transform: open ? 'rotate(90deg)' : 'rotate(0deg)' }} aria-hidden="true">
          ▶
        </span>
      </button>
      {open && (
        <div style={styles.nestedCardBody}>
          {children}
        </div>
      )}
    </div>
  );
}

/** Card for one item within an array-of-objects */
function ArrayItemCard({ index, children }) {
  const [open, setOpen] = useState(true);
  return (
    <div style={styles.arrayItemCard}>
      <button
        style={styles.arrayItemToggle}
        onClick={() => setOpen(o => !o)}
        aria-expanded={open}
        type="button"
      >
        <span style={styles.arrayItemIndex}>#{index + 1}</span>
        <span style={{ ...styles.chevron, transform: open ? 'rotate(90deg)' : 'rotate(0deg)' }} aria-hidden="true">
          ▶
        </span>
      </button>
      {open && (
        <div style={styles.arrayItemBody}>
          {children}
        </div>
      )}
    </div>
  );
}

// ── Core recursive renderer ───────────────────────────────────────────────────

/**
 * Recursively renders any JSON value as UI.
 *
 * @param {*}      value      - the value to render
 * @param {string} key        - the property name (used for labelling)
 * @param {number} depth      - current nesting depth (0 = top level)
 * @param {number} reactKey   - stable React key for list rendering
 */
function renderNode(value, key, depth = 0, reactKey = 0) {
  const label = toLabel(key);

  // ── null / undefined ────────────────────────────────────────────────────────
  if (value === null || value === undefined) {
    return (
      <LabelValue key={reactKey} label={label} value={null} />
    );
  }

  // ── primitive ───────────────────────────────────────────────────────────────
  if (isPrimitive(value)) {
    return (
      <LabelValue key={reactKey} label={label} value={value} />
    );
  }

  // ── array ───────────────────────────────────────────────────────────────────
  if (Array.isArray(value)) {
    if (value.length === 0) {
      return (
        <div key={reactKey} style={styles.labelValueRow}>
          <span style={styles.label}>{label}</span>
          <span style={styles.emptyText}>No items</span>
        </div>
      );
    }

    if (isAllPrimitives(value)) {
      return (
        <div key={reactKey} style={styles.labelValueRow}>
          <span style={styles.label}>{label}</span>
          <ChipList items={value} />
        </div>
      );
    }

    // Mixed or object array — render each item as its own card
    return (
      <div key={reactKey} style={styles.arraySection}>
        <div style={styles.arraySectionLabel}>{label}</div>
        <div style={styles.arrayList}>
          {value.map((item, i) => {
            if (isPrimitive(item)) {
              return (
                <div key={i} style={styles.arrayMixedItem}>
                  {formatPrimitive(item)}
                </div>
              );
            }
            // object or nested array item
            return (
              <ArrayItemCard key={i} index={i}>
                {renderObjectFields(item, depth + 1)}
              </ArrayItemCard>
            );
          })}
        </div>
      </div>
    );
  }

  // ── object ──────────────────────────────────────────────────────────────────
  if (typeof value === 'object') {
    const entries = Object.entries(value);
    if (entries.length === 0) {
      return (
        <div key={reactKey} style={styles.labelValueRow}>
          <span style={styles.label}>{label}</span>
          <span style={styles.emptyText}>No details available</span>
        </div>
      );
    }

    // Auto-expand at depth 0 and 1; collapse deeper nodes
    const defaultOpen = depth <= 1;

    return (
      <NestedCard key={reactKey} label={label} defaultOpen={defaultOpen}>
        {renderObjectFields(value, depth + 1)}
      </NestedCard>
    );
  }

  // ── Fallback (should not be reached for valid JSON) ─────────────────────────
  return (
    <LabelValue key={reactKey} label={label} value={String(value)} />
  );
}

/** Renders all own-property key/value pairs of a plain object. */
function renderObjectFields(obj, depth) {
  if (!obj || typeof obj !== 'object' || Array.isArray(obj)) {
    return renderNode(obj, 'value', depth, 0);
  }
  const entries = Object.entries(obj);
  if (entries.length === 0) {
    return <span style={styles.emptyText}>No details available</span>;
  }
  return entries.map(([k, v], i) => renderNode(v, k, depth, i));
}

// ── Public entry-point component ─────────────────────────────────────────────

/**
 * JsonCardRenderer — renders any JSON object as a polished card.
 *
 * Usage:
 *   <JsonCardRenderer data={anyJsonObject} />
 *
 * Props:
 *   data  {object|array|*}  The JSON to render. Must be a parsed JS value.
 */
function JsonCardRenderer({ data }) {
  if (data === null || data === undefined) {
    return (
      <div style={styles.root}>
        <span style={styles.emptyText}>Not provided</span>
      </div>
    );
  }

  return (
    <div style={styles.root}>
      {renderObjectFields(data, 0)}
    </div>
  );
}

// ── Inline styles ─────────────────────────────────────────────────────────────
// Inline styles keep this component self-contained with zero CSS dependencies.

const styles = {
  root: {
    display: 'flex',
    flexDirection: 'column',
    gap: '2px',
  },
  labelValueRow: {
    display: 'flex',
    alignItems: 'baseline',
    gap: '8px',
    padding: '4px 0',
    borderBottom: '1px solid #f0f2f5',
    flexWrap: 'wrap',
    minHeight: '26px',
  },
  label: {
    fontSize: '0.72rem',
    fontWeight: 700,
    color: '#57606a',
    textTransform: 'uppercase',
    letterSpacing: '0.05em',
    whiteSpace: 'nowrap',
    minWidth: '90px',
    flexShrink: 0,
  },
  value: {
    fontSize: '0.83rem',
    color: '#1f2328',
    flex: 1,
    wordBreak: 'break-word',
  },
  strValue:  { color: '#1f2328' },
  numValue:  { color: '#2563eb', fontVariantNumeric: 'tabular-nums' },
  boolValue: { color: '#7c3aed', fontWeight: 600 },
  nullValue: { color: '#9ca3af', fontStyle: 'italic' },
  emptyText: { color: '#9ca3af', fontStyle: 'italic', fontSize: '0.8rem' },

  chipList: {
    display: 'flex',
    flexWrap: 'wrap',
    gap: '4px',
    flex: 1,
  },
  chip: {
    background: '#f3f4f6',
    border: '1px solid #e5e7eb',
    borderRadius: '10px',
    padding: '1px 8px',
    fontSize: '0.75rem',
    color: '#374151',
    whiteSpace: 'nowrap',
  },

  nestedCard: {
    border: '1px solid #e5e7eb',
    borderRadius: '6px',
    overflow: 'hidden',
    marginTop: '4px',
    marginBottom: '4px',
    background: '#ffffff',
  },
  nestedCardToggle: {
    width: '100%',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'space-between',
    padding: '6px 10px',
    background: '#f8fafc',
    border: 'none',
    cursor: 'pointer',
    textAlign: 'left',
    borderBottom: '1px solid #e5e7eb',
  },
  nestedCardTitle: {
    fontSize: '0.72rem',
    fontWeight: 700,
    color: '#57606a',
    textTransform: 'uppercase',
    letterSpacing: '0.05em',
  },
  nestedCardBody: {
    padding: '8px 10px',
    display: 'flex',
    flexDirection: 'column',
    gap: '2px',
  },

  arraySection: {
    marginTop: '4px',
    marginBottom: '4px',
  },
  arraySectionLabel: {
    fontSize: '0.72rem',
    fontWeight: 700,
    color: '#57606a',
    textTransform: 'uppercase',
    letterSpacing: '0.05em',
    marginBottom: '4px',
    paddingBottom: '3px',
    borderBottom: '1px solid #e5e7eb',
  },
  arrayList: {
    display: 'flex',
    flexDirection: 'column',
    gap: '4px',
  },
  arrayItemCard: {
    border: '1px solid #e5e7eb',
    borderRadius: '6px',
    overflow: 'hidden',
    background: '#ffffff',
  },
  arrayItemToggle: {
    width: '100%',
    display: 'flex',
    alignItems: 'center',
    gap: '8px',
    padding: '5px 10px',
    background: '#f8fafc',
    border: 'none',
    cursor: 'pointer',
    textAlign: 'left',
    borderBottom: '1px solid #e5e7eb',
  },
  arrayItemIndex: {
    fontSize: '0.72rem',
    fontWeight: 700,
    color: '#9ca3af',
    flex: 1,
  },
  arrayItemBody: {
    padding: '8px 10px',
    display: 'flex',
    flexDirection: 'column',
    gap: '2px',
  },
  arrayMixedItem: {
    padding: '4px 8px',
    background: '#f8fafc',
    border: '1px solid #e5e7eb',
    borderRadius: '6px',
    fontSize: '0.83rem',
    color: '#1f2328',
  },

  chevron: {
    fontSize: '0.6rem',
    color: '#9ca3af',
    transition: 'transform 0.15s ease',
    display: 'inline-block',
    flexShrink: 0,
  },
};

export default JsonCardRenderer;
