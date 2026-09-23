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
/**
 * useKafkaNotifications
 *
 * Detects new Kafka events by comparing successive poll snapshots and surfaces
 * them as generic notification objects.  Completely domain-agnostic: it treats
 * every arriving event as a { id, type, timestamp, message, topicType } record
 * regardless of the business domain that produced it.
 *
 * Usage:
 *   const { notifications, dismissNotification } = useKafkaNotifications(eventsByType, descriptor);
 *
 * @param {object} eventsByType   - map of topicType → event[] from the poll loop
 * @param {object|null} descriptor - use-case descriptor (provides topic labels)
 * @param {object} [options]
 * @param {number} [options.maxVisible=5]    - max notifications on screen at once
 * @param {number} [options.autoDismissMs=5000] - ms before a notification auto-dismisses
 * @param {boolean} [options.showChangelogs=false] - whether to emit notifications for entity-changelog topics
 */

import { useState, useEffect, useRef, useCallback } from 'react';

/** Build a human-readable one-line summary from any event object. */
function summariseEvent(rawEvent) {
  if (!rawEvent || typeof rawEvent !== 'object') return 'New event received';

  // Unwrap EventEnvelope payload if present (e.g. { agentEventType, entityId, payload: { ... } })
  const event = (rawEvent.agentEventType && rawEvent.payload && typeof rawEvent.payload === 'object')
    ? { ...rawEvent.payload, ...rawEvent }
    : rawEvent;

  // Prefer explicit message-like fields
  const message =
    event.message    ??
    event.description ??
    event.reason     ??
    event.summary    ??
    event.title      ??
    null;
  if (message && typeof message === 'string') return message;

  // Fall back to the decision field if present
  if (event.decision != null) return String(event.decision);

  // Try any "type" field
  if (event.eventType || event.type) return String(event.eventType ?? event.type);

  return 'New event received';
}

/**
 * Convert a raw Kafka event payload into a generic notification record.
 * The record shape is stable regardless of the originating domain.
 */
function toNotification(event, topicType, topicLabel) {
  return {
    id:         `${topicType}-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
    type:       topicType,
    label:      topicLabel || topicType,
    timestamp:  event?.timestamp ?? new Date().toISOString(),
    message:    summariseEvent(event),
    // Preserve the raw event so the caller can render additional detail if needed
    raw:        event,
  };
}

export function useKafkaNotifications(
  eventsByType,
  descriptor,
  {
    maxVisible     = 5,
    autoDismissMs  = 5000,
    showChangelogs = false,
  } = {}
) {
  const [notifications, setNotifications] = useState([]);

  // Track the event-count per topic seen in the *previous* poll cycle.
  // Using a ref avoids adding it to the useEffect dependency array.
  const prevCountsRef = useRef({});

  // Build topicName → { label, type } lookup from the descriptor
  const labelMapRef = useRef({});
  const topicTypeMapRef = useRef({});
  useEffect(() => {
    if (!descriptor) {
      // Use case was deselected — clear label/type maps so stale topic labels
      // from the previous use case do not bleed into the next one.
      labelMapRef.current     = {};
      topicTypeMapRef.current = {};
      return;
    }
    const labelMap = {};
    const typeMap = {};
    for (const t of descriptor.topics ?? []) {
      labelMap[t.name] = t.label ?? t.type ?? t.name;
      if (t.type) typeMap[t.name] = t.type;
    }
    for (const t of descriptor.outboundTopics ?? []) {
      labelMap[t.name] = t.label ?? t.type ?? t.name;
      if (t.type) typeMap[t.name] = t.type;
    }
    labelMapRef.current = labelMap;
    topicTypeMapRef.current = typeMap;
  }, [descriptor]);

  useEffect(() => {
    if (!eventsByType || typeof eventsByType !== 'object') return;

    const newNotifs = [];

    for (const [topicType, events] of Object.entries(eventsByType)) {
      if (!Array.isArray(events)) continue;
      const prevCount = prevCountsRef.current[topicType] ?? 0;
      const currCount = events.length;

      // When showChangelogs is false, suppress notifications for entity-changelog topics
      const resolvedType = topicTypeMapRef.current[topicType] ?? topicType;
      const isChangelog = resolvedType === 'entity-changelog' || topicType === 'entity-changelog';

      if (currCount > prevCount) {
        // Surface every newly arrived event (handles bursts)
        if (showChangelogs || !isChangelog) {
          for (let i = prevCount; i < currCount; i++) {
            newNotifs.push(toNotification(events[i], topicType, labelMapRef.current[topicType]));
          }
        }
      }
      prevCountsRef.current[topicType] = currCount;
    }

    if (newNotifs.length === 0) return;

    setNotifications(prev => {
      const combined = [...prev, ...newNotifs];
      // Trim to maxVisible: keep the most recent
      return combined.slice(-maxVisible);
    });
  }, [eventsByType, maxVisible, showChangelogs]);

  const dismissNotification = useCallback((id) => {
    setNotifications(prev => prev.filter(n => n.id !== id));
  }, []);

  return { notifications, dismissNotification, autoDismissMs };
}
