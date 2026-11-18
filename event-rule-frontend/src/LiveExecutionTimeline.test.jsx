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
 * LiveExecutionTimeline — system-topic visibility ("Show system topics" toggle).
 *
 * The descriptor mirrors the Air Traffic Control sample, whose two agents get
 * different change-log types from the BFF: 'entity-changelog' for the first and
 * 'entity-changelog-<agentId>' for the second.
 */
import React from 'react';
import { render } from '@testing-library/react';
import { describe, test, expect, beforeAll } from 'vitest';
import LiveExecutionTimeline from './LiveExecutionTimeline';

beforeAll(() => {
  if (!globalThis.ResizeObserver) {
    globalThis.ResizeObserver = class {
      observe() {}
      unobserve() {}
      disconnect() {}
    };
  }
});

const DESCRIPTOR = {
  topics: [{ name: 'agent-aircraft-events', type: 'AircraftEvent', label: 'AircraftEvent' }],
  outboundTopics: [
    { name: 'agent-atc-alerts',                  type: 'Alert',                          label: 'Alerts' },
    { name: 'agent-atc-errors',                  type: 'agent-error',                    label: 'Agent Errors' },
    { name: 'agent-aircraft-entity-changelog',   type: 'entity-changelog',               label: 'Entity Changelog (Aircraft)' },
    { name: 'agent-airport-entity-changelog',    type: 'entity-changelog-airport-agent', label: 'Entity Changelog (Airport)' },
  ],
};

const envelope = (agentEventType, ts) => ({
  agentEventType, entityId: 'e1', timestamp: ts, payload: { type: agentEventType },
});

const EVENTS = {
  'agent-aircraft-events':            [{ type: 'AircraftEvent', timestamp: 1000 }],
  'agent-atc-alerts':                 [envelope('Alert', 2000)],
  'agent-atc-errors':                 [envelope('agent-error', 3000)],
  'agent-aircraft-entity-changelog':  [envelope('entity-changelog', 4000)],
  'agent-airport-entity-changelog':   [envelope('entity-changelog', 5000)],
};

const countText = (container) =>
  container.querySelector('.live-execution-timeline-count').textContent;
const legendText = (container) =>
  [...container.querySelectorAll('.timeline-legend-label')].map(n => n.textContent);

describe('LiveExecutionTimeline — system topics', () => {
  test('hides both change-log topics and agent-error by default', () => {
    const { container } = render(<LiveExecutionTimeline eventsByType={EVENTS} descriptor={DESCRIPTOR} />);
    expect(countText(container)).toBe('2 events');
    const legend = legendText(container);
    expect(legend.some(l => l.includes('Entity Changelog'))).toBe(false);
    expect(legend).not.toContain('Agent Errors');
  });

  test('hides system topics when showSystemTopics is false', () => {
    const { container } = render(
      <LiveExecutionTimeline eventsByType={EVENTS} descriptor={DESCRIPTOR} showSystemTopics={false} />
    );
    expect(countText(container)).toBe('2 events');
  });

  test('shows both change-log topics and agent-error when showSystemTopics is true', () => {
    const { container } = render(
      <LiveExecutionTimeline eventsByType={EVENTS} descriptor={DESCRIPTOR} showSystemTopics />
    );
    expect(countText(container)).toBe('5 events');
    const legend = legendText(container);
    expect(legend).toContain('Entity Changelog (Aircraft)');
    expect(legend).toContain('Entity Changelog (Airport)');
    expect(legend).toContain('Agent Errors');
  });
});
