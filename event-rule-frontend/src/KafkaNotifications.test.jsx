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
 * Tests for the generic Kafka event notification system.
 *
 * Covers:
 *  1. useKafkaNotifications — event detection, deduplication, burst handling,
 *     max-visible capping, dismissal, label resolution.
 *  2. NotificationToast     — renders notification fields, dismiss callback,
 *     entry/exit class transitions.
 *  3. NotificationContainer — renders nothing when empty, renders N toasts
 *     when the list is populated.
 *
 * The tests are domain-agnostic: no "fraud", "agent-error" or other business
 * names appear.  Any event shape passes through the generic contract.
 */

import React from 'react';
import { render, screen, act, fireEvent } from '@testing-library/react';
import { renderHook } from '@testing-library/react';
import { useKafkaNotifications } from './useKafkaNotifications';
import NotificationToast from './NotificationToast';
import NotificationContainer from './NotificationContainer';

// ── Helpers ───────────────────────────────────────────────────────────────────

function makeDescriptor(types = ['typeA', 'typeB']) {
  return {
    topics: types.slice(0, 2).map(t => ({ name: t, type: t, label: `Label ${t}` })),
    outboundTopics: [],
  };
}

function makeEvent(overrides = {}) {
  return {
    id:        'evt-1',
    timestamp: '2024-01-01T10:00:00.000Z',
    message:   'Something happened',
    ...overrides,
  };
}

// ── useKafkaNotifications ─────────────────────────────────────────────────────

describe('useKafkaNotifications', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  test('returns empty notifications on first render with no events', () => {
    const { result } = renderHook(() =>
      useKafkaNotifications({}, makeDescriptor())
    );
    expect(result.current.notifications).toHaveLength(0);
  });

  test('detects a new event on the next render and emits one notification', () => {
    let eventsByType = {};
    const { result, rerender } = renderHook(
      ({ evts }) => useKafkaNotifications(evts, makeDescriptor()),
      { initialProps: { evts: eventsByType } }
    );
    expect(result.current.notifications).toHaveLength(0);

    // Simulate one event arriving for typeA
    rerender({ evts: { typeA: [makeEvent()] } });
    expect(result.current.notifications).toHaveLength(1);
  });

  test('emits one notification per newly arrived event (burst of 3)', () => {
    const { result, rerender } = renderHook(
      ({ evts }) => useKafkaNotifications(evts, makeDescriptor()),
      { initialProps: { evts: {} } }
    );

    const events = [makeEvent({ id: '1' }), makeEvent({ id: '2' }), makeEvent({ id: '3' })];
    rerender({ evts: { typeA: events } });

    expect(result.current.notifications).toHaveLength(3);
  });

  test('does not re-emit notifications for already-seen events on subsequent polls', () => {
    const events = [makeEvent()];
    const { result, rerender } = renderHook(
      ({ evts }) => useKafkaNotifications(evts, makeDescriptor()),
      { initialProps: { evts: { typeA: events } } }
    );

    // First render surfaces 1
    expect(result.current.notifications).toHaveLength(1);

    // Identical snapshot — no new events
    rerender({ evts: { typeA: events } });
    expect(result.current.notifications).toHaveLength(1);
  });

  test('caps visible notifications at maxVisible', () => {
    const events = Array.from({ length: 10 }, (_, i) => makeEvent({ id: String(i) }));
    const { result, rerender } = renderHook(
      ({ evts }) => useKafkaNotifications(evts, makeDescriptor(), { maxVisible: 3 }),
      { initialProps: { evts: {} } }
    );

    rerender({ evts: { typeA: events } });
    expect(result.current.notifications).toHaveLength(3);
  });

  test('notification includes the resolved topic label', () => {
    const descriptor = makeDescriptor(['typeA']);
    const { result, rerender } = renderHook(
      ({ evts }) => useKafkaNotifications(evts, descriptor),
      { initialProps: { evts: {} } }
    );

    rerender({ evts: { typeA: [makeEvent()] } });
    expect(result.current.notifications[0].label).toBe('Label typeA');
  });

  test('notification type matches the Kafka topic type', () => {
    const { result, rerender } = renderHook(
      ({ evts }) => useKafkaNotifications(evts, makeDescriptor()),
      { initialProps: { evts: {} } }
    );

    rerender({ evts: { typeA: [makeEvent()] } });
    expect(result.current.notifications[0].type).toBe('typeA');
  });

  test('notification message prefers event.message field', () => {
    const { result, rerender } = renderHook(
      ({ evts }) => useKafkaNotifications(evts, makeDescriptor()),
      { initialProps: { evts: {} } }
    );

    rerender({ evts: { typeA: [makeEvent({ message: 'Custom message text' })] } });
    expect(result.current.notifications[0].message).toBe('Custom message text');
  });

  test('notification message falls back to decision field when message is absent', () => {
    const { result, rerender } = renderHook(
      ({ evts }) => useKafkaNotifications(evts, makeDescriptor()),
      { initialProps: { evts: {} } }
    );

    rerender({ evts: { typeA: [{ decision: 'APPROVED' }] } });
    expect(result.current.notifications[0].message).toBe('APPROVED');
  });

  test('notification message falls back to generic text when no known field present', () => {
    const { result, rerender } = renderHook(
      ({ evts }) => useKafkaNotifications(evts, makeDescriptor()),
      { initialProps: { evts: {} } }
    );

    rerender({ evts: { typeA: [{ unknownField: 42 }] } });
    expect(result.current.notifications[0].message).toBe('New event received');
  });

  test('unwraps EventEnvelope payload to find message or decision', () => {
    const { result, rerender } = renderHook(
      ({ evts }) => useKafkaNotifications(evts, makeDescriptor()),
      { initialProps: { evts: {} } }
    );

    rerender({
      evts: {
        typeA: [
          {
            agentEventType: 'Alert',
            entityId: 'AC123',
            sourceTopic: 'agent-atc-alerts',
            timestamp: 1700000000000,
            payload: { message: 'Touch-and-go detected at NCE', severity: 'HIGH' },
          },
        ],
      },
    });
    expect(result.current.notifications).toHaveLength(1);
    expect(result.current.notifications[0].message).toBe('Touch-and-go detected at NCE');
  });

  test('dismissNotification removes the notification with the given id', () => {
    const { result, rerender } = renderHook(
      ({ evts }) => useKafkaNotifications(evts, makeDescriptor()),
      { initialProps: { evts: {} } }
    );

    rerender({ evts: { typeA: [makeEvent()] } });
    const id = result.current.notifications[0].id;

    act(() => {
      result.current.dismissNotification(id);
    });

    expect(result.current.notifications).toHaveLength(0);
  });

  test('handles multiple topics simultaneously', () => {
    const { result, rerender } = renderHook(
      ({ evts }) => useKafkaNotifications(evts, makeDescriptor(['typeA', 'typeB']), { maxVisible: 10 }),
      { initialProps: { evts: {} } }
    );

    rerender({ evts: { typeA: [makeEvent({ id: 'a1' })], typeB: [makeEvent({ id: 'b1' })] } });
    expect(result.current.notifications).toHaveLength(2);
  });

  test('handles null/undefined eventsByType gracefully', () => {
    const { result, rerender } = renderHook(
      ({ evts }) => useKafkaNotifications(evts, makeDescriptor()),
      { initialProps: { evts: null } }
    );

    expect(result.current.notifications).toHaveLength(0);
    rerender({ evts: undefined });
    expect(result.current.notifications).toHaveLength(0);
  });

  test('exposes autoDismissMs from options', () => {
    const { result } = renderHook(() =>
      useKafkaNotifications({}, makeDescriptor(), { autoDismissMs: 3000 })
    );
    expect(result.current.autoDismissMs).toBe(3000);
  });

  test('each notification has a unique id', () => {
    const events = [makeEvent({ id: '1' }), makeEvent({ id: '2' })];
    const { result, rerender } = renderHook(
      ({ evts }) => useKafkaNotifications(evts, makeDescriptor()),
      { initialProps: { evts: {} } }
    );

    rerender({ evts: { typeA: events } });
    const ids = result.current.notifications.map(n => n.id);
    const unique = new Set(ids);
    expect(unique.size).toBe(ids.length);
  });

  test('suppresses entity-changelog notifications by default when showChangelogs is false', () => {
    const descriptor = {
      topics: [{ name: 'inbound-events', type: 'inbound-events', label: 'Inbound' }],
      outboundTopics: [
        { name: 'agent-alerts', type: 'Alert', label: 'Alerts' },
        { name: 'agent-entity-changelog', type: 'entity-changelog', label: 'Entity Changelog' },
      ],
    };

    const { result, rerender } = renderHook(
      ({ evts, showChangelogs }) => useKafkaNotifications(evts, descriptor, { showChangelogs }),
      { initialProps: { evts: {}, showChangelogs: false } }
    );

    // Arrive an alert event and a changelog event
    rerender({
      evts: {
        'agent-alerts': [makeEvent({ id: 'alert-1', message: 'Fraud detected' })],
        'agent-entity-changelog': [makeEvent({ id: 'cl-1', message: 'Entity changed' })],
      },
      showChangelogs: false,
    });

    // Only the alert should generate a notification
    expect(result.current.notifications).toHaveLength(1);
    expect(result.current.notifications[0].type).toBe('agent-alerts');
    expect(result.current.notifications[0].message).toBe('Fraud detected');
  });

  test('emits entity-changelog notifications when showChangelogs is true', () => {
    const descriptor = {
      topics: [{ name: 'inbound-events', type: 'inbound-events', label: 'Inbound' }],
      outboundTopics: [
        { name: 'agent-alerts', type: 'Alert', label: 'Alerts' },
        { name: 'agent-entity-changelog', type: 'entity-changelog', label: 'Entity Changelog' },
      ],
    };

    const { result, rerender } = renderHook(
      ({ evts, showChangelogs }) => useKafkaNotifications(evts, descriptor, { showChangelogs }),
      { initialProps: { evts: {}, showChangelogs: true } }
    );

    rerender({
      evts: {
        'agent-alerts': [makeEvent({ id: 'alert-1', message: 'Fraud detected' })],
        'agent-entity-changelog': [makeEvent({ id: 'cl-1', message: 'Entity changed' })],
      },
      showChangelogs: true,
    });

    expect(result.current.notifications).toHaveLength(2);
    const types = result.current.notifications.map(n => n.type);
    expect(types).toContain('agent-alerts');
    expect(types).toContain('agent-entity-changelog');
  });
});

// ── NotificationToast ─────────────────────────────────────────────────────────

describe('NotificationToast', () => {
  beforeEach(() => { vi.useFakeTimers(); });
  afterEach(() => { vi.useRealTimers(); });

  const baseNotification = {
    id:        'notif-1',
    type:      'typeA',
    label:     'Type A',
    timestamp: '2024-01-01T12:30:00.000Z',
    message:   'Something happened on typeA',
  };

  test('renders the notification label', () => {
    render(
      <NotificationToast
        notification={baseNotification}
        onDismiss={() => {}}
        autoDismissMs={60000}
      />
    );
    expect(screen.getByText('Type A')).toBeTruthy();
  });

  test('renders the notification message', () => {
    render(
      <NotificationToast
        notification={baseNotification}
        onDismiss={() => {}}
        autoDismissMs={60000}
      />
    );
    expect(screen.getByText('Something happened on typeA')).toBeTruthy();
  });

  test('calls onDismiss with the notification id when the close button is clicked', () => {
    const onDismiss = vi.fn();
    render(
      <NotificationToast
        notification={baseNotification}
        onDismiss={onDismiss}
        autoDismissMs={60000}
      />
    );

    fireEvent.click(screen.getByLabelText('Dismiss notification'));
    // onDismiss is called after the exit animation timeout (280ms) — advance timers
    act(() => { vi.advanceTimersByTime(300); });
    expect(onDismiss).toHaveBeenCalledWith('notif-1');
  });

  test('renders a progress bar element', () => {
    const { container } = render(
      <NotificationToast
        notification={baseNotification}
        onDismiss={() => {}}
        autoDismissMs={5000}
      />
    );
    expect(container.querySelector('.kafka-notification__progress')).not.toBeNull();
  });
});

// ── NotificationContainer ─────────────────────────────────────────────────────

describe('NotificationContainer', () => {
  beforeEach(() => { vi.useFakeTimers(); });
  afterEach(() => { vi.useRealTimers(); });

  test('renders nothing when the list is empty', () => {
    const { container } = render(
      <NotificationContainer notifications={[]} onDismiss={() => {}} autoDismissMs={5000} />
    );
    expect(container.firstChild).toBeNull();
  });

  test('renders nothing when notifications is null', () => {
    const { container } = render(
      <NotificationContainer notifications={null} onDismiss={() => {}} autoDismissMs={5000} />
    );
    expect(container.firstChild).toBeNull();
  });

  test('renders one toast per notification', () => {
    const notifications = [
      { id: 'n1', type: 'typeA', label: 'Type A', timestamp: '', message: 'msg A' },
      { id: 'n2', type: 'typeB', label: 'Type B', timestamp: '', message: 'msg B' },
    ];

    render(
      <NotificationContainer
        notifications={notifications}
        onDismiss={() => {}}
        autoDismissMs={60000}
      />
    );

    expect(screen.getByText('msg A')).toBeTruthy();
    expect(screen.getByText('msg B')).toBeTruthy();
  });

  test('passes onDismiss to each toast', () => {
    const onDismiss = vi.fn();
    const notifications = [
      { id: 'n1', type: 'typeA', label: 'Type A', timestamp: '', message: 'hello' },
    ];

    render(
      <NotificationContainer
        notifications={notifications}
        onDismiss={onDismiss}
        autoDismissMs={60000}
      />
    );

    fireEvent.click(screen.getByLabelText('Dismiss notification'));
    act(() => { vi.advanceTimersByTime(300); });
    expect(onDismiss).toHaveBeenCalledWith('n1');
  });
});
