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
import DecisionResultCard from './DecisionResultCard';
import EntityEventCard from './EntityEventCard';
import ProcessingErrorCard from './ProcessingErrorCard';
import JsonCardRenderer from './JsonCardRenderer';
import { EventCardErrorBoundary } from './EventCardErrorBoundary.jsx';

/**
 * eventCardRegistry — maps events to the best available presentation card.
 *
 * Rendering strategy (first match wins):
 *   1. agent-error topic type → ProcessingErrorCard (cross-domain)
 *   2. Event carries rule-engine output fields (decision, rules, alerts, actions,
 *      score-like fields) → DecisionResultCard via generic adapter
 *   3. Event carries entity-state fields (status badge, key/value fields)
 *      → EntityEventCard via generic adapter
 *   4. Universal fallback → JsonCardRenderer (renders any JSON shape)
 *
 * No domain-specific knowledge (banking, retail, …) lives in this file.
 * Adding a new agent requires no changes here.
 */

// ── Shared utilities ──────────────────────────────────────────────────────────

/**
 * Converts an UPPER_SNAKE_CASE or camelCase enum/type name to readable Title Case.
 * e.g. "LARGE_TRANSACTION_DETECTED" → "Large Transaction Detected"
 *      "cartAbandonment"            → "Cart Abandonment"
 */
export function toLabel(raw) {
  if (!raw) return '';
  return String(raw)
    .replace(/_/g, ' ')
    .replace(/([a-z])([A-Z])/g, '$1 $2')
    .trim()
    .replace(/\s+/g, ' ')
    .replace(/\b\w/g, c => c.toUpperCase())
    .trim();
}

/** Normalise a score value to 0–100.  Values ≤1 are treated as fractions. */
function toPercent(v) {
  if (v === undefined || v === null) return 0;
  return v > 1 ? Math.min(100, Math.max(0, v)) : Math.min(100, Math.max(0, v * 100));
}

/** Pick a traffic-light colour for a 0–100 score. */
function scoreColor(pct) {
  return pct >= 70 ? '#ef4444' : pct >= 50 ? '#f59e0b' : '#22c55e';
}

// ── Shared normalisation helpers ──────────────────────────────────────────────

// Accept both the primary field name and the pluralised form emitted by the IBM DI
// Java model's @JsonProperty annotations (e.g. "triggeredRules" and "triggeredRuleses"
// are both accepted).  Note: the "es" suffix is a deliberate Java serialisation quirk,
// not standard English pluralisation — it is intentional here.
function coerce(event, key) {
  return event[key] ?? event[`${key}s`] ?? event[`${key}es`] ?? [];
}

function normaliseRules(raw) {
  if (!Array.isArray(raw)) return [];
  return raw.map(r => ({
    id:       r.ruleId || r.id || '',
    name:     r.ruleName || r.name || '',
    severity: r.severity,
    message:  r.message,
  }));
}

function normaliseAlerts(raw) {
  if (!Array.isArray(raw)) return [];
  return raw.map((a, i) => ({
    id:       a.alertId || a.id || i,
    title:    a.title || toLabel(a.alertType || a.type || ''),
    type:     a.title ? toLabel(a.alertType || a.type || '') : null,
    severity: a.severity,
    message:  a.message,
  }));
}

function normaliseActions(raw) {
  if (!Array.isArray(raw)) return [];
  return raw.map((a, i) => ({
    id:          a.actionId || i,
    label:       toLabel(a.actionType || a.type || ''),
    priority:    a.priority,
    description: a.description,
  }));
}

// ── Processing-error adapter (cross-domain) ────────────────────────────────────

function adaptProcessingError(event) {
  return {
    source:    event.agentId || event.processorId,
    entityId:  event.entityId !== 'unknown' ? event.entityId : undefined,
    origin:    event.sourceTopic,
    reason:    event.errorReason || event.reason,
    original:  event.originalEvent,
    timestamp: event.timestamp,
  };
}

// ── Generic decision-result adapter ───────────────────────────────────────────
// Works for any event that carries a `decision` field and/or rule-engine arrays.
// Badge colours follow semantic buckets rather than domain-specific enum lists.

const SEMANTIC_BADGE = {
  bg: '#f8fafc', color: '#64748b', border: '#cbd5e1',   // neutral default
};

// Match normalised decision strings: uppercase letters and underscores only.
// Underscores are NOT stripped before testing so multi-word decisions like
// OFFER_SENT remain matchable; each regex uses _? to tolerate both the
// original form and a compacted form without separators.
// Input to these regexes is already uppercased by decisionBadge(), so the /i
// flag is redundant and has been removed (L-4).
const POSITIVE_WORDS  = /^(APPROVED?|ACCEPT|ALLOW|PASS|SUCCESS|OK|YES|SENT|OFFER_?SENT)$/;
const NEGATIVE_WORDS  = /^(BLOCK|BLOCKED|DENIED?|DECLINED?|REJECTED?|SUSPEND|FRAUDULENT|FLAGGED?)$/;
const WARNING_WORDS   = /^(PENDING|REVIEW|VERIFY|VERIFICATION|HOLD|INVESTIGATE)$/;

function decisionBadge(decision) {
  // Normalise to uppercase; replace non-alpha characters with underscores
  // (preserving underscores from the original value for multi-word matching).
  const d = String(decision || '').toUpperCase().replace(/[^A-Z_]/g, '_');
  if (POSITIVE_WORDS.test(d))  return { bg: '#f0fdf4', color: '#15803d', border: '#86efac' };
  if (NEGATIVE_WORDS.test(d))  return { bg: '#fef2f2', color: '#b91c1c', border: '#fca5a5' };
  if (WARNING_WORDS.test(d))   return { bg: '#fffbeb', color: '#b45309', border: '#fbbf24' };
  return SEMANTIC_BADGE;
}

function adaptDecisionResult(event) {
  const decision = event.decision;
  const badge    = decisionBadge(decision);

  // Pick any numeric score field generically
  const scoreField = ['riskScore', 'score', 'intentScore', 'confidenceScore', 'probability']
    .find(k => event[k] !== undefined);
  const scoreValue = scoreField ? toPercent(event[scoreField]) : undefined;

  // Primary identifier — accept common id field names
  const id = event.transactionId || event.alertId || event.id
    || event.entityId || event.orderId || event.requestId || 'Result';

  // Secondary identifier
  const subtitle = event.accountId || event.customerId || event.entityId !== id ? event.entityId : undefined;

  const meta = [];
  if (event.entityId && event.entityId !== id && event.entityId !== 'unknown') {
    meta.push(<span key="entity">Entity: <strong style={{ color: '#1f2328' }}>{event.entityId}</strong></span>);
  }
  if (event.agentId) {
    meta.push(<span key="agent">Agent: <strong style={{ color: '#1f2328' }}>{event.agentId}</strong></span>);
  }

  return {
    id,
    subtitle,
    meta:          meta.length ? meta : undefined,
    decision:      decision != null ? toLabel(String(decision)) : undefined,
    decisionStyle: badge,
    accentColor:   badge.border,
    score:         scoreField
      ? { label: toLabel(scoreField), value: scoreValue, color: scoreColor(scoreValue) }
      : undefined,
    rules:         normaliseRules(coerce(event, 'triggeredRules')),
    alerts:        normaliseAlerts(coerce(event, 'alerts')),
    actions:       normaliseActions(coerce(event, 'actions')),
    timestamp:     event.timestamp || event.processedAt,
  };
}

/** True when the event carries decision/rule-engine output fields */
function isDecisionResultLike(event) {
  return event && (
    event.decision !== undefined ||
    Array.isArray(event.triggeredRules) || Array.isArray(event.triggeredRuleses) ||
    Array.isArray(event.alerts)         || Array.isArray(event.alertses) ||
    Array.isArray(event.actions)        || Array.isArray(event.actionses)
  );
}

// ── Generic entity-event adapter ──────────────────────────────────────────────
// Works for entity state events that carry a status + key/value payload.

function adaptEntityEvent(event) {
  // Collect scalar fields as key/value pairs, skipping metadata and large objects
  const SKIP = new Set(['agentEventType', 'sourceTopic', 'timestamp', 'agentId', 'status']);
  const fields = Object.entries(event)
    .filter(([k, v]) => !SKIP.has(k) && (typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean'))
    .slice(0, 8)
    .map(([k, v]) => ({ key: toLabel(k), value: String(v) }));

  const id = event.id || event.entityId || event.accountId || event.customerId
    || event.orderId || event.requestId || 'Entity';

  return {
    id,
    status:      event.status,
    statusStyle: undefined, // no domain-specific colour map — falls back to neutral in EntityEventCard
    accentColor: '#64748b',
    fields,
    timestamp:   event.timestamp,
  };
}

/** True when the event looks like entity state (has a status field + id-like field) */
function isEntityEventLike(event) {
  return event && event.status !== undefined && !isDecisionResultLike(event);
}

// ── Registry ─────────────────────────────────────────────────────────────────

/**
 * TOPIC_RENDERERS — exact topic-type overrides.
 * Only `agent-error` needs a dedicated renderer since it has a fixed cross-domain shape.
 */
const TOPIC_RENDERERS = {
  'agent-error': (event) => (
    <ProcessingErrorCard error={adaptProcessingError(event)} />
  ),
};

/**
 * CONTENT_RENDERERS — ordered list of { matches, render } entries.
 * First match wins.
 */
const CONTENT_RENDERERS = [
  {
    matches: isDecisionResultLike,
    render:  (event) => <DecisionResultCard result={adaptDecisionResult(event)} />,
  },
  {
    matches: isEntityEventLike,
    render:  (event) => <EntityEventCard entity={adaptEntityEvent(event)} />,
  },
];

// ── Public API ────────────────────────────────────────────────────────────────

/**
 * Renders the best available card for a given event.
 *
 * @param {object} event      - already-normalised event payload
 * @param {string} topicType  - logical topic type from the descriptor
 * @param {number} index      - position in the list (for fallback key generation)
 * @returns {ReactNode}
 */
export function renderCard(event, topicType, index) {
  // Prefer the stable BFF-assigned sequence number (_seq) when available (M-2).
  // Fall back to the position-based index for events that pre-date the _seq
  // addition (e.g. historical events fetched before the BFF was updated).
  // Content-derived keys (transactionId, accountId, etc.) are intentionally NOT
  // used here — multiple events from the same entity share those values, causing
  // React to treat distinct list entries as the same node.
  const key = event?._seq != null ? `${topicType}-seq-${event._seq}` : `${topicType}-${index}`;

  let inner;

  // 1. Exact topic-type match
  const byTopic = TOPIC_RENDERERS[topicType];
  if (byTopic) {
    inner = byTopic(event);
  } else {
    // 2. Content-based match
    let matched = false;
    for (const entry of CONTENT_RENDERERS) {
      if (entry.matches(event)) {
        inner = entry.render(event);
        matched = true;
        break;
      }
    }

    // 3. Universal fallback
    if (!matched) {
      inner = (
        <div className="data-item">
          <JsonCardRenderer data={event} />
        </div>
      );
    }
  }

  return (
    <EventCardErrorBoundary key={key} eventType={topicType} rawEvent={event}>
      {inner}
    </EventCardErrorBoundary>
  );
}

/**
 * Generic empty-state node for outbound topics.
 * @param {string} [topicType] - optional topic type used to tailor the message
 */
export function OutboundEmptyState({ topicType }) {
  if (topicType === 'agent-error') {
    return <p>No processing errors — the rule engine is running cleanly.</p>;
  }
  return <p>No events yet — waiting for rule engine output…</p>;
}
