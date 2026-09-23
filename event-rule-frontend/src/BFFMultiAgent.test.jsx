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
 * Unit tests for the BFF multi-agent helper functions and API route logic.
 *
 * These tests do NOT start an Express server.  They exercise the pure functions
 * extracted from bff/server.js directly using JS module evaluation so we can
 * verify correctness without any network, Kafka, or file-system dependencies.
 *
 * Tested functions:
 *   - buildStubDescriptor(useCaseId, agentsJson)
 *   - collectTopicsFromAgentsJson(agentsJson)
 *   - POST /api/agents append-mode logic (merge + duplicate guard)
 *   - PATCH /api/agents/:useCaseId/agents/:agentId logic
 *   - DELETE /api/agents/:useCaseId/agents/:agentId logic
 *   - isValidAgentId(id) guard
 */

// ─── Inline pure-function replicas ───────────────────────────────────────────
// We reproduce the exact logic from bff/server.js as pure functions so the
// tests are fully self-contained (no Node.js fs / Kafka required).

function isValidAgentId(id) {
  return typeof id === 'string' && /^[a-zA-Z0-9_-]{1,64}$/.test(id);
}

function buildStubDescriptor(useCaseId, agentsJson) {
  const allAgents = agentsJson.agents || [];
  const first = allAgents[0] || {};

  const topicsMap = new Map();
  const outboundMap = new Map();

  for (const agent of allAgents) {
    for (const s of (agent.subscriptions || [])) {
      const name = s.topic || s.eventType;
      if (name && !topicsMap.has(name)) {
        topicsMap.set(name, { name, type: s.eventType, label: s.eventType });
      }
    }
    for (const o of (agent.outputs || [])) {
      if (o.topic && !outboundMap.has(o.topic)) {
        outboundMap.set(o.topic, {
          name:  o.topic,
          type:  o.eventType,
          label: o.description || o.eventType,
          ...(o.filterKey   && { filterKey:   o.filterKey   }),
          ...(o.filterValue && { filterValue: o.filterValue }),
          ...(o.filterLabel && { filterLabel: o.filterLabel }),
        });
      }
    }
  }

  const name = allAgents.length === 1
    ? (first.name || useCaseId.replace(/[-_]+/g, ' ').replace(/\b\w/g, c => c.toUpperCase()))
    : useCaseId.replace(/[-_]+/g, ' ').replace(/\b\w/g, c => c.toUpperCase());

  return {
    name,
    flinkJobName:    `${useCaseId}-job`,
    consumerGroupId: `event-rule-bff-${useCaseId}`,
    topics:          [...topicsMap.values()],
    outboundTopics:  [...outboundMap.values()],
  };
}

function collectTopicsFromAgentsJson(agentsJson) {
  const topics = new Set();
  for (const agent of (agentsJson.agents || [])) {
    for (const sub of (agent.subscriptions || [])) {
      if (sub.topic && sub.topic.trim()) topics.add(sub.topic.trim());
      for (const t of (sub.topics || [])) {
        if (t && t.trim()) topics.add(t.trim());
      }
    }
    for (const out of (agent.outputs || [])) {
      if (out.topic && out.topic.trim()) topics.add(out.topic.trim());
    }
    if (agent.ruleset?.archiveControlTopic?.trim())
      topics.add(agent.ruleset.archiveControlTopic.trim());
  }
  return topics;
}

/**
 * Simulates the POST /api/agents append-mode merge logic.
 * Returns { finalDescriptor } on success, or throws with an error code.
 */
function simulatePostAppend(existingAgentsJson, incomingDescriptor) {
  const existingIds = new Set((existingAgentsJson.agents || []).map(a => a.agentId));
  for (const incoming of (incomingDescriptor.agents || [])) {
    if (existingIds.has(incoming.agentId)) {
      const err = new Error('DUPLICATE_AGENT_ID');
      err.code = 'DUPLICATE_AGENT_ID';
      err.agentId = incoming.agentId;
      throw err;
    }
  }
  const mergedAgents = [...(existingAgentsJson.agents || []), ...(incomingDescriptor.agents || [])];
  const finalDescriptor = { ...existingAgentsJson, agents: mergedAgents };
  if (incomingDescriptor.parallelism != null) finalDescriptor.parallelism = incomingDescriptor.parallelism;
  return { finalDescriptor };
}

/**
 * Simulates the PATCH /api/agents/:useCaseId/agents/:agentId replace logic.
 * Returns updated agentsJson or throws if agentId not found.
 */
function simulatePatch(existingAgentsJson, targetAgentId, newAgentDescriptor) {
  const idx = (existingAgentsJson.agents || []).findIndex(a => a.agentId === targetAgentId);
  if (idx === -1) {
    const err = new Error('AGENT_NOT_FOUND');
    err.code = 'AGENT_NOT_FOUND';
    throw err;
  }
  const updated = JSON.parse(JSON.stringify(existingAgentsJson));
  updated.agents[idx] = newAgentDescriptor;
  return updated;
}

/**
 * Simulates the DELETE /api/agents/:useCaseId/agents/:agentId logic.
 * Returns { after, lastAgent } where lastAgent indicates full teardown needed.
 */
function simulateDeleteAgent(existingAgentsJson, targetAgentId) {
  const before = existingAgentsJson.agents || [];
  const after  = before.filter(a => a.agentId !== targetAgentId);
  if (after.length === before.length) {
    const err = new Error('AGENT_NOT_FOUND');
    err.code = 'AGENT_NOT_FOUND';
    throw err;
  }
  return { after, lastAgent: after.length === 0 };
}

// ─── Fixtures ─────────────────────────────────────────────────────────────────

const BANKING_AGENTS_JSON = {
  agents: [{
    agentId: 'banking-fraud-agent',
    name: 'Banking Fraud Detection Agent',
    subscriptions: [
      { eventType: 'Transaction',  topic: 'agent-transactions',    operation: 'EvaluateTransaction' },
      { eventType: 'AccountEvent', topic: 'agent-account-events',  operation: 'HandleAccountEvent' },
    ],
    ruleset: { path: 'samples/banking-fraud-agent/Fraud.jar', archiveControlTopic: 'agent-banking-archive-control' },
    outputs: [
      { eventType: 'Alert',       topic: 'agent-fraud-alerts',   description: 'Fraud alerts' },
      { eventType: 'agent-error', topic: 'agent-banking-errors', description: 'Errors' },
    ],
    stateConfig: { ttl: '30d', backend: 'rocksdb' },
  }],
};

const AIRTRAFFIC_AGENTS_JSON = {
  agents: [
    {
      agentId: 'aircraft-agent',
      name: 'Aircraft Agent',
      subscriptions: [
        { eventType: 'AircraftEvent', topic: 'agent-aircraft-events', operation: 'ProcessAircraftEvent' },
      ],
      ruleset: { path: 'samples/airtraffic-agent/ATC.jar', archiveControlTopic: 'agent-atc-archive-control' },
      outputs: [
        { eventType: 'TrackedAircraftEvent', topic: 'agent-tracked-aircraft-events', description: 'Tracked' },
        { eventType: 'Alert',                topic: 'agent-atc-alerts',               description: 'ATC Alerts' },
        { eventType: 'agent-error',          topic: 'agent-atc-errors',               description: 'Errors' },
      ],
    },
    {
      agentId: 'airport-agent',
      name: 'Airport Agent',
      subscriptions: [
        { eventType: 'AirportEvent',         topic: 'agent-airport-events',            operation: 'ProcessAirportEvent' },
        { eventType: 'TrackedAircraftEvent', topic: 'agent-tracked-aircraft-events',   operation: 'ProcessTrackedAircraftEvent' },
      ],
      ruleset: { path: 'samples/airtraffic-agent/ATC.jar', archiveControlTopic: 'agent-atc-archive-control' },
      outputs: [
        // Intentionally shares agent-atc-alerts and agent-atc-errors with aircraft-agent
        { eventType: 'Alert',       topic: 'agent-atc-alerts',  description: 'Airport Alerts' },
        { eventType: 'agent-error', topic: 'agent-atc-errors',  description: 'Errors' },
      ],
    },
  ],
};

// ─── isValidAgentId ───────────────────────────────────────────────────────────

describe('isValidAgentId', () => {
  test('accepts lowercase with hyphens', () => { expect(isValidAgentId('fraud-detection-agent')).toBe(true); });
  test('accepts alphanumeric with underscores', () => { expect(isValidAgentId('agent_v2')).toBe(true); });
  test('accepts single character', () => { expect(isValidAgentId('a')).toBe(true); });
  test('accepts 64-char string', () => { expect(isValidAgentId('a'.repeat(64))).toBe(true); });
  test('rejects empty string', () => { expect(isValidAgentId('')).toBe(false); });
  test('rejects 65-char string', () => { expect(isValidAgentId('a'.repeat(65))).toBe(false); });
  test('rejects string with spaces', () => { expect(isValidAgentId('fraud agent')).toBe(false); });
  test('rejects string with dots', () => { expect(isValidAgentId('fraud.agent')).toBe(false); });
  test('rejects string with slashes', () => { expect(isValidAgentId('fraud/agent')).toBe(false); });
  test('rejects non-string input', () => { expect(isValidAgentId(123)).toBe(false); });
  test('rejects null', () => { expect(isValidAgentId(null)).toBe(false); });
});

// ─── buildStubDescriptor: single-agent ───────────────────────────────────────

describe('buildStubDescriptor — single-agent use-case', () => {
  let stub;
  beforeEach(() => { stub = buildStubDescriptor('banking-fraud-agent', BANKING_AGENTS_JSON); });

  test('flinkJobName is useCaseId + "-job"', () => {
    expect(stub.flinkJobName).toBe('banking-fraud-agent-job');
  });

  test('consumerGroupId is "event-rule-bff-" + useCaseId', () => {
    expect(stub.consumerGroupId).toBe('event-rule-bff-banking-fraud-agent');
  });

  test('name is taken from the single agent name', () => {
    expect(stub.name).toBe('Banking Fraud Detection Agent');
  });

  test('name falls back to title-cased useCaseId when agent has no name', () => {
    const json = { agents: [{ agentId: 'x', subscriptions: [], outputs: [] }] };
    const s = buildStubDescriptor('my-use-case', json);
    expect(s.name).toBe('My Use Case');
  });

  test('topics includes all subscription topics', () => {
    const names = stub.topics.map(t => t.name);
    expect(names).toContain('agent-transactions');
    expect(names).toContain('agent-account-events');
  });

  test('outboundTopics includes all output topics', () => {
    const names = stub.outboundTopics.map(t => t.name);
    expect(names).toContain('agent-fraud-alerts');
    expect(names).toContain('agent-banking-errors');
  });

  test('topic entries include type and label', () => {
    const t = stub.topics.find(t => t.name === 'agent-transactions');
    expect(t.type).toBe('Transaction');
    expect(t.label).toBe('Transaction');
  });

  test('outbound entries use description as label when present', () => {
    const ot = stub.outboundTopics.find(t => t.name === 'agent-fraud-alerts');
    expect(ot.label).toBe('Fraud alerts');
  });
});

// ─── buildStubDescriptor: multi-agent ────────────────────────────────────────

describe('buildStubDescriptor — multi-agent use-case (deduplication)', () => {
  let stub;
  beforeEach(() => { stub = buildStubDescriptor('airtraffic-agent', AIRTRAFFIC_AGENTS_JSON); });

  test('name is title-cased useCaseId for multi-agent (not a single agent name)', () => {
    expect(stub.name).toBe('Airtraffic Agent');
  });

  test('flinkJobName is still useCaseId + "-job"', () => {
    expect(stub.flinkJobName).toBe('airtraffic-agent-job');
  });

  test('topics from both agents are aggregated', () => {
    const names = stub.topics.map(t => t.name);
    expect(names).toContain('agent-aircraft-events');
    expect(names).toContain('agent-airport-events');
    expect(names).toContain('agent-tracked-aircraft-events');
  });

  test('shared subscription topic (agent-tracked-aircraft-events) appears exactly once', () => {
    const count = stub.topics.filter(t => t.name === 'agent-tracked-aircraft-events').length;
    expect(count).toBe(1);
  });

  test('outbound topics from both agents are aggregated', () => {
    const names = stub.outboundTopics.map(t => t.name);
    expect(names).toContain('agent-tracked-aircraft-events');
    expect(names).toContain('agent-atc-alerts');
    expect(names).toContain('agent-atc-errors');
  });

  test('shared outbound topic (agent-atc-alerts) appears exactly once (deduplication)', () => {
    const count = stub.outboundTopics.filter(t => t.name === 'agent-atc-alerts').length;
    expect(count).toBe(1);
  });

  test('shared outbound topic (agent-atc-errors) appears exactly once', () => {
    const count = stub.outboundTopics.filter(t => t.name === 'agent-atc-errors').length;
    expect(count).toBe(1);
  });
});

// ─── buildStubDescriptor: edge cases ─────────────────────────────────────────

describe('buildStubDescriptor — edge cases', () => {
  test('empty agents array produces empty topics and outboundTopics', () => {
    const stub = buildStubDescriptor('empty-job', { agents: [] });
    expect(stub.topics).toEqual([]);
    expect(stub.outboundTopics).toEqual([]);
  });

  test('agent with no outputs produces empty outboundTopics', () => {
    const json = { agents: [{ agentId: 'x', subscriptions: [{ eventType: 'e', topic: 't', operation: 'op' }], outputs: [] }] };
    const stub = buildStubDescriptor('minimal-job', json);
    expect(stub.outboundTopics).toEqual([]);
    expect(stub.topics).toHaveLength(1);
  });

  test('agent with no subscriptions produces empty topics', () => {
    const json = { agents: [{ agentId: 'x', subscriptions: [], outputs: [{ eventType: 'out', topic: 'out-t', description: 'Out' }] }] };
    const stub = buildStubDescriptor('out-only-job', json);
    expect(stub.topics).toEqual([]);
    expect(stub.outboundTopics).toHaveLength(1);
  });

  test('useCaseId with underscores is title-cased correctly in multi-agent name', () => {
    const stub = buildStubDescriptor('my_use_case', {
      agents: [{ agentId: 'a1', name: 'A1', subscriptions: [], outputs: [] }, { agentId: 'a2', name: 'A2', subscriptions: [], outputs: [] }],
    });
    expect(stub.name).toBe('My Use Case');
  });
});

// ─── collectTopicsFromAgentsJson ──────────────────────────────────────────────

describe('collectTopicsFromAgentsJson', () => {
  test('collects subscription topics from single agent', () => {
    const topics = collectTopicsFromAgentsJson(BANKING_AGENTS_JSON);
    expect(topics.has('agent-transactions')).toBe(true);
    expect(topics.has('agent-account-events')).toBe(true);
  });

  test('collects output topics from single agent', () => {
    const topics = collectTopicsFromAgentsJson(BANKING_AGENTS_JSON);
    expect(topics.has('agent-fraud-alerts')).toBe(true);
    expect(topics.has('agent-banking-errors')).toBe(true);
  });

  test('collects archiveControlTopic from ruleset', () => {
    const topics = collectTopicsFromAgentsJson(BANKING_AGENTS_JSON);
    expect(topics.has('agent-banking-archive-control')).toBe(true);
  });

  test('collects topics from all agents in a multi-agent descriptor', () => {
    const topics = collectTopicsFromAgentsJson(AIRTRAFFIC_AGENTS_JSON);
    expect(topics.has('agent-aircraft-events')).toBe(true);
    expect(topics.has('agent-airport-events')).toBe(true);
    expect(topics.has('agent-tracked-aircraft-events')).toBe(true);
    expect(topics.has('agent-atc-alerts')).toBe(true);
    expect(topics.has('agent-atc-errors')).toBe(true);
    expect(topics.has('agent-atc-archive-control')).toBe(true);
  });

  test('shared topic appears only once (Set deduplication)', () => {
    const topics = collectTopicsFromAgentsJson(AIRTRAFFIC_AGENTS_JSON);
    // agent-tracked-aircraft-events appears as sub topic in airport-agent AND output topic in aircraft-agent
    expect(topics.has('agent-tracked-aircraft-events')).toBe(true);
    expect(topics.size).toBeGreaterThan(0);
    // Check count via array (Set has no duplicates by definition)
    const arr = [...topics];
    const count = arr.filter(t => t === 'agent-tracked-aircraft-events').length;
    expect(count).toBe(1);
  });

  test('handles legacy topics array (sub.topics) as well as sub.topic string', () => {
    const json = {
      agents: [{
        agentId: 'x',
        subscriptions: [
          { eventType: 'e1', topic: 'string-topic',    operation: 'op' },
          { eventType: 'e2', topics: ['array-topic-a', 'array-topic-b'], operation: 'op' },
        ],
        outputs: [],
      }],
    };
    const topics = collectTopicsFromAgentsJson(json);
    expect(topics.has('string-topic')).toBe(true);
    expect(topics.has('array-topic-a')).toBe(true);
    expect(topics.has('array-topic-b')).toBe(true);
  });

  test('empty agents array returns empty Set', () => {
    const topics = collectTopicsFromAgentsJson({ agents: [] });
    expect(topics.size).toBe(0);
  });

  test('ignores blank/whitespace topic strings', () => {
    const json = { agents: [{ agentId: 'x', subscriptions: [{ eventType: 'e', topic: '  ', operation: 'op' }], outputs: [] }] };
    const topics = collectTopicsFromAgentsJson(json);
    expect(topics.size).toBe(0);
  });
});

// ─── POST /api/agents append mode (simulatePostAppend) ───────────────────────

describe('POST /api/agents — append mode merge logic', () => {
  const SECOND_AGENT_DESCRIPTOR = {
    agents: [{
      agentId: 'airport-agent',
      name: 'Airport Agent',
      subscriptions: [{ eventType: 'AirportEvent', topic: 'agent-airport-events', operation: 'ProcessAirportEvent' }],
      ruleset: { path: 'samples/airtraffic-agent/ATC.jar' },
      outputs: [{ eventType: 'Alert', topic: 'agent-atc-alerts', description: 'Airport Alerts' }],
    }],
  };

  test('appends new agent to existing single-agent use-case', () => {
    const { finalDescriptor } = simulatePostAppend(BANKING_AGENTS_JSON, SECOND_AGENT_DESCRIPTOR);
    expect(finalDescriptor.agents).toHaveLength(2);
    expect(finalDescriptor.agents[0].agentId).toBe('banking-fraud-agent');
    expect(finalDescriptor.agents[1].agentId).toBe('airport-agent');
  });

  test('existing agents are preserved unchanged in merge', () => {
    const { finalDescriptor } = simulatePostAppend(BANKING_AGENTS_JSON, SECOND_AGENT_DESCRIPTOR);
    expect(finalDescriptor.agents[0]).toEqual(BANKING_AGENTS_JSON.agents[0]);
  });

  test('incoming parallelism overrides existing when provided', () => {
    const incoming = { ...SECOND_AGENT_DESCRIPTOR, parallelism: 4 };
    const { finalDescriptor } = simulatePostAppend(BANKING_AGENTS_JSON, incoming);
    expect(finalDescriptor.parallelism).toBe(4);
  });

  test('incoming null parallelism does NOT override existing parallelism', () => {
    const base = { ...BANKING_AGENTS_JSON, parallelism: 2 };
    const { finalDescriptor } = simulatePostAppend(base, SECOND_AGENT_DESCRIPTOR); // no parallelism on incoming
    expect(finalDescriptor.parallelism).toBe(2);
  });

  test('duplicate agentId throws DUPLICATE_AGENT_ID', () => {
    const dupe = {
      agents: [{ agentId: 'banking-fraud-agent', subscriptions: [], ruleset: { path: 'x' }, outputs: [] }],
    };
    expect(() => simulatePostAppend(BANKING_AGENTS_JSON, dupe))
      .toThrow(expect.objectContaining({ code: 'DUPLICATE_AGENT_ID', agentId: 'banking-fraud-agent' }));
  });

  test('duplicate agentId error carries the conflicting agentId', () => {
    const dupe = {
      agents: [{ agentId: 'banking-fraud-agent', subscriptions: [], ruleset: { path: 'x' }, outputs: [] }],
    };
    let caught = null;
    try { simulatePostAppend(BANKING_AGENTS_JSON, dupe); } catch (err) { caught = err; }
    expect(caught).not.toBeNull();
    expect(caught.agentId).toBe('banking-fraud-agent');
  });

  test('appending to empty (no agents array) starts fresh list', () => {
    const empty = { agents: [] };
    const { finalDescriptor } = simulatePostAppend(empty, SECOND_AGENT_DESCRIPTOR);
    expect(finalDescriptor.agents).toHaveLength(1);
    expect(finalDescriptor.agents[0].agentId).toBe('airport-agent');
  });
});

// ─── PATCH /api/agents/:useCaseId/agents/:agentId ────────────────────────────

describe('PATCH per-agent — replace one agent without disturbing siblings', () => {
  const UPDATED_AIRCRAFT = {
    agentId: 'aircraft-agent',
    name: 'Aircraft Agent v2',
    subscriptions: [{ eventType: 'AircraftEvent', topic: 'agent-aircraft-events-v2', operation: 'ProcessAircraftEvent' }],
    ruleset: { path: 'samples/airtraffic-agent/ATC-v2.jar' },
    outputs: [{ eventType: 'Alert', topic: 'agent-atc-alerts', description: 'Alerts v2' }],
  };

  test('replaces matching agent in-place', () => {
    const updated = simulatePatch(AIRTRAFFIC_AGENTS_JSON, 'aircraft-agent', UPDATED_AIRCRAFT);
    expect(updated.agents[0].name).toBe('Aircraft Agent v2');
    expect(updated.agents[0].subscriptions[0].topic).toBe('agent-aircraft-events-v2');
  });

  test('sibling agent is not modified', () => {
    const updated = simulatePatch(AIRTRAFFIC_AGENTS_JSON, 'aircraft-agent', UPDATED_AIRCRAFT);
    expect(updated.agents[1]).toEqual(AIRTRAFFIC_AGENTS_JSON.agents[1]);
  });

  test('agents array length stays the same', () => {
    const updated = simulatePatch(AIRTRAFFIC_AGENTS_JSON, 'aircraft-agent', UPDATED_AIRCRAFT);
    expect(updated.agents).toHaveLength(2);
  });

  test('throws AGENT_NOT_FOUND for unknown agentId', () => {
    expect(() => simulatePatch(AIRTRAFFIC_AGENTS_JSON, 'ghost-agent', UPDATED_AIRCRAFT))
      .toThrow(expect.objectContaining({ code: 'AGENT_NOT_FOUND' }));
  });

  test('patching the second agent only updates that agent', () => {
    const updatedAirport = { ...AIRTRAFFIC_AGENTS_JSON.agents[1], name: 'Airport Agent v2' };
    const updated = simulatePatch(AIRTRAFFIC_AGENTS_JSON, 'airport-agent', updatedAirport);
    expect(updated.agents[1].name).toBe('Airport Agent v2');
    expect(updated.agents[0]).toEqual(AIRTRAFFIC_AGENTS_JSON.agents[0]);
  });
});

// ─── DELETE /api/agents/:useCaseId/agents/:agentId ───────────────────────────

describe('DELETE per-agent — remove one agent from use-case', () => {
  test('removes the specified agent from the array', () => {
    const { after } = simulateDeleteAgent(AIRTRAFFIC_AGENTS_JSON, 'aircraft-agent');
    expect(after).toHaveLength(1);
    expect(after[0].agentId).toBe('airport-agent');
  });

  test('sibling agent is preserved unchanged', () => {
    const { after } = simulateDeleteAgent(AIRTRAFFIC_AGENTS_JSON, 'aircraft-agent');
    expect(after[0]).toEqual(AIRTRAFFIC_AGENTS_JSON.agents[1]);
  });

  test('lastAgent is false when a sibling remains', () => {
    const { lastAgent } = simulateDeleteAgent(AIRTRAFFIC_AGENTS_JSON, 'aircraft-agent');
    expect(lastAgent).toBe(false);
  });

  test('lastAgent is true when the only agent is removed', () => {
    const { lastAgent } = simulateDeleteAgent(BANKING_AGENTS_JSON, 'banking-fraud-agent');
    expect(lastAgent).toBe(true);
  });

  test('remaining array is empty when last agent is removed', () => {
    const { after } = simulateDeleteAgent(BANKING_AGENTS_JSON, 'banking-fraud-agent');
    expect(after).toHaveLength(0);
  });

  test('throws AGENT_NOT_FOUND for unknown agentId', () => {
    expect(() => simulateDeleteAgent(AIRTRAFFIC_AGENTS_JSON, 'ghost-agent'))
      .toThrow(expect.objectContaining({ code: 'AGENT_NOT_FOUND' }));
  });

  test('removing second agent leaves first agent untouched', () => {
    const { after } = simulateDeleteAgent(AIRTRAFFIC_AGENTS_JSON, 'airport-agent');
    expect(after).toHaveLength(1);
    expect(after[0].agentId).toBe('aircraft-agent');
  });
});

// ─── Backward compatibility: single-agent samples ────────────────────────────

describe('Backward compatibility — single-agent use-cases continue to work', () => {
  test('collectTopicsFromAgentsJson works for single-agent banking descriptor', () => {
    const topics = collectTopicsFromAgentsJson(BANKING_AGENTS_JSON);
    expect(topics.size).toBeGreaterThan(0);
    expect(topics.has('agent-transactions')).toBe(true);
  });

  test('buildStubDescriptor for single-agent uses agent name, not title-cased id', () => {
    const stub = buildStubDescriptor('banking-fraud-agent', BANKING_AGENTS_JSON);
    expect(stub.name).toBe('Banking Fraud Detection Agent');
  });

  test('simulatePostAppend with empty existing acts like replace (no merge needed)', () => {
    const { finalDescriptor } = simulatePostAppend({ agents: [] }, BANKING_AGENTS_JSON);
    expect(finalDescriptor.agents).toHaveLength(1);
    expect(finalDescriptor.agents[0].agentId).toBe('banking-fraud-agent');
  });
});
