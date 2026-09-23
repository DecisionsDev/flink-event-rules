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
 * DecisionResultCard — generic presentation component for rule-engine results.
 *
 * Accepts a normalized `result` object produced by a domain adapter:
 *
 *   {
 *     id:           string          — primary identifier shown in header
 *     subtitle:     string?         — secondary identifier (e.g. account, customer)
 *     meta:         string[]?       — small metadata pills below the header
 *     decision:     string          — badge label
 *     decisionStyle:{ bg, color, border }  — badge colour
 *     accentColor:  string          — left-border colour of the card
 *     score:        { label, value: 0–100, color }?  — optional score bar
 *     rules:        { id, name, severity, message }[]?
 *     alerts:       { id?, title, type?, severity, message? }[]?
 *     actions:      { id?, label, priority?, description? }[]?
 *     extras:       ReactNode?      — domain-specific sections rendered after actions
 *     emptyState:   ReactNode?      — shown when visibleEvents.length === 0
 *     timestamp:    number|string?
 *   }
 *
 * The component is domain-agnostic — it knows nothing about banking or retail.
 * All domain knowledge lives in the adapter that produces the `result` prop.
 */

const SEVERITY_COLORS = {
  CRITICAL: '#b91c1c',
  HIGH:     '#ea580c',
  MEDIUM:   '#d97706',
  LOW:      '#16a34a',
};

function severityColor(s) {
  return SEVERITY_COLORS[s] || '#57606a';
}

// ── Sub-components ─────────────────────────────────────────────────────────────

function DecisionBadge({ label, style }) {
  return (
    <span style={{
      display: 'inline-block',
      padding: '3px 10px',
      borderRadius: '12px',
      fontSize: '0.75rem',
      fontWeight: 700,
      background: style.bg,
      color: style.color,
      border: `1px solid ${style.border}`,
      letterSpacing: '0.03em',
      whiteSpace: 'nowrap',
    }}>
      {label}
    </span>
  );
}

function ScoreBar({ label, value, color }) {
  const pct = Math.min(100, Math.max(0, value || 0));
  return (
    <div style={{ marginBottom: '8px' }}>
      {label && (
        <span style={{ fontSize: '0.7rem', color: '#57606a', fontWeight: 700, letterSpacing: '0.06em', textTransform: 'uppercase' }}>
          {label}
        </span>
      )}
      <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginTop: '3px' }}>
        <div style={{ flex: 1, background: '#e5e7eb', borderRadius: '4px', height: '6px' }}>
          <div style={{ width: `${pct}%`, background: color, borderRadius: '4px', height: '6px', transition: 'width 0.4s ease' }} />
        </div>
        <span style={{ fontSize: '0.75rem', fontWeight: 600, color, minWidth: '34px', textAlign: 'right' }}>
          {pct.toFixed(0)}
        </span>
      </div>
    </div>
  );
}

function SectionLabel({ children }) {
  return (
    <div style={{ fontSize: '0.7rem', color: '#57606a', fontWeight: 700, letterSpacing: '0.06em', textTransform: 'uppercase', marginBottom: '4px' }}>
      {children}
    </div>
  );
}

function RulesList({ rules }) {
  if (!rules || rules.length === 0) return null;
  return (
    <div style={{ marginBottom: '8px' }}>
      <SectionLabel>Rules</SectionLabel>
      {rules.map((rule, i) => (
        <div key={rule.id || i} style={{ display: 'flex', alignItems: 'baseline', gap: '6px', marginBottom: '2px', fontSize: '0.8rem' }}>
          <span style={{ fontWeight: 700, color: severityColor(rule.severity), minWidth: '70px', fontSize: '0.72rem' }}>
            {rule.id}
          </span>
          <span style={{ color: '#1f2328' }}>{rule.name}</span>
          {rule.message && <span style={{ color: '#57606a', fontSize: '0.75rem' }}>— {rule.message}</span>}
        </div>
      ))}
    </div>
  );
}

function AlertsList({ alerts }) {
  if (!alerts || alerts.length === 0) return null;
  return (
    <div style={{ marginBottom: '4px' }}>
      <SectionLabel>Alerts</SectionLabel>
      {alerts.map((alert, i) => (
        <div key={alert.id || i} style={{
          background: '#fafafa', border: '1px solid #e5e7eb',
          borderLeft: `3px solid ${severityColor(alert.severity)}`,
          borderRadius: '4px', padding: '4px 8px', marginBottom: '4px', fontSize: '0.8rem',
        }}>
          <strong style={{ color: severityColor(alert.severity) }}>
            {alert.title}
          </strong>
          {alert.type && alert.title && (
            <span style={{ marginLeft: '6px', fontSize: '0.72rem', color: '#9ca3af', fontWeight: 400 }}>
              {alert.type}
            </span>
          )}
          {alert.message && (
            <div style={{ color: '#57606a', fontSize: '0.75rem', marginTop: '2px' }}>{alert.message}</div>
          )}
        </div>
      ))}
    </div>
  );
}

function ActionsList({ actions }) {
  if (!actions || actions.length === 0) return null;
  return (
    <div>
      <SectionLabel>Actions</SectionLabel>
      {actions.map((action, i) => (
        <div key={action.id || i} style={{ fontSize: '0.8rem', color: '#57606a', marginBottom: '2px' }}>
          <span style={{ fontWeight: 600, color: '#2563eb' }}>{action.label}</span>
          {action.priority && (
            <span style={{ fontSize: '0.72rem', marginLeft: '6px', color: '#9ca3af' }}>[{action.priority}]</span>
          )}
          {action.description && <span style={{ marginLeft: '4px' }}>— {action.description}</span>}
        </div>
      ))}
    </div>
  );
}

/** Collapsible rules section — used by PromotionOfferCard adapter */
export function CollapsibleRules({ rules }) {
  const [open, setOpen] = useState(false);
  if (!rules || rules.length === 0) return null;
  return (
    <section style={{ marginTop: '6px' }}>
      <button
        style={{ background: 'none', border: 'none', cursor: 'pointer', padding: '2px 0', display: 'flex', alignItems: 'center', gap: '6px' }}
        onClick={() => setOpen(o => !o)}
        aria-expanded={open}
      >
        <span style={{ fontSize: '0.7rem', color: '#57606a', fontWeight: 700, letterSpacing: '0.06em', textTransform: 'uppercase' }}>
          Rules <span style={{ fontWeight: 400, color: '#9ca3af' }}>({rules.length})</span>
        </span>
        <span style={{ fontSize: '0.65rem', color: '#9ca3af' }}>{open ? '▲' : '▼'}</span>
      </button>
      {open && (
        <div style={{ marginTop: '4px' }}>
          {rules.map((rule, i) => (
            <div key={rule.id || i} style={{ display: 'flex', gap: '6px', fontSize: '0.8rem', marginBottom: '2px' }}>
              <span style={{ color: severityColor(rule.severity), fontWeight: 700, fontSize: '0.72rem', minWidth: '60px' }}>{rule.id}</span>
              <span style={{ color: '#1f2328' }}>{rule.name}</span>
              {rule.message && <span style={{ color: '#57606a', fontSize: '0.75rem' }}>— {rule.message}</span>}
            </div>
          ))}
        </div>
      )}
    </section>
  );
}

// ── Public entry-point ─────────────────────────────────────────────────────────

export default function DecisionResultCard({ result }) {
  if (!result) return null;

  return (
    <div className="data-item decision-result-card" style={{ borderLeftColor: result.accentColor }}>
      {/* Header row */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: '6px', marginBottom: '8px' }}>
        <div style={{ fontWeight: 700, fontSize: '0.88rem' }}>
          {result.id || 'Result'}
          {result.subtitle && (
            <span style={{ marginLeft: '8px', fontSize: '0.78rem', fontWeight: 400, color: '#57606a' }}>
              {result.subtitle}
            </span>
          )}
        </div>
        {result.decision && (
          <DecisionBadge label={result.decision} style={result.decisionStyle || { bg: '#f3f4f6', color: '#374151', border: '#d1d5db' }} />
        )}
      </div>

      {/* Meta row (entity id, agent id, etc.) */}
      {result.meta && result.meta.length > 0 && (
        <div style={{ fontSize: '0.78rem', color: '#57606a', marginBottom: '8px', display: 'flex', gap: '10px', flexWrap: 'wrap' }}>
          {result.meta.map((item, i) => (
            <span key={i} dangerouslySetInnerHTML={undefined}>
              {item}
            </span>
          ))}
        </div>
      )}

      {/* Score bar */}
      {result.score && (
        <ScoreBar label={result.score.label} value={result.score.value} color={result.score.color} />
      )}

      {/* Domain-specific extras rendered before rules (e.g. offer pills) */}
      {result.extrasTop}

      {/* Rules / alerts / actions */}
      <RulesList  rules={result.rules}   />
      <AlertsList alerts={result.alerts} />
      <ActionsList actions={result.actions} />

      {/* Domain-specific extras rendered after actions */}
      {result.extras}

      {/* Timestamp */}
      {result.timestamp && (
        <div style={{ fontSize: '0.72rem', color: '#9ca3af', marginTop: '6px', textAlign: 'right' }}>
          {new Date(
            typeof result.timestamp === 'number' ? result.timestamp : Date.parse(result.timestamp)
          ).toLocaleString()}
        </div>
      )}
    </div>
  );
}
