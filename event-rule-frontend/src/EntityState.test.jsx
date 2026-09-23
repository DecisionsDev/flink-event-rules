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
 * Tests for the entity-state BFF logic.
 *
 * Covers:
 *   1. extractEntityIdFromEvent — mirrors the Flink entityKeyExtractor JSONPath logic
 *   2. Entity-state snapshot construction:
 *        - inbound entity discovery uses entityKeyExtractor (not hard-coded field names)
 *        - outbound entity discovery uses top-level entityId (populated by toEnvelope fix)
 *        - multi-agent use cases assign per-agent entityType correctly
 *        - entities without an entityId are silently skipped
 *        - lastPayload is the outbound envelope with the newest timestamp
 *        - entities are sorted newest-first
 *
 * All logic is reproduced inline (pure functions) so no server/Kafka/fs is required.
 */

// ─── extractEntityIdFromEvent (replica from bff/server.js) ───────────────────

/**
 * Replica of the extractEntityIdFromEvent function added to bff/server.js.
 * Must stay in sync with the production implementation.
 */
function extractEntityIdFromEvent(event, extractor) {
  if (!extractor || !event || typeof event !== 'object') return null;
  const path = extractor.replace(/^\$\.?/, '');
  if (!path) return null;
  const parts = path.split('.');
  let cursor = event;
  for (const part of parts) {
    if (cursor === null || cursor === undefined || typeof cursor !== 'object') return null;
    cursor = cursor[part];
  }
  if (cursor === null || cursor === undefined) return null;
  const str = String(cursor);
  return (str && str !== 'unknown') ? str : null;
}

// ─── Entity-state snapshot builder (replica of the BFF endpoint core logic) ──

/**
 * Replica of the entity-map building logic from GET /api/agents/:id/entity-state.
 * Returns { entityType, entities }.
 */
function buildEntitySnapshot(agentsJson, descriptorOutboundTopics, useCaseStore) {
  const agents = agentsJson.agents || [];

  // outbound topicName → entityType of the owning agent
  const outboundNameToEntityType = new Map();
  const outboundTopicNames = new Set(
    (descriptorOutboundTopics || []).filter(ot => ot.name).map(ot => ot.name)
  );
  for (const agent of agents) {
    for (const out of agent.outputs || []) {
      const match = (descriptorOutboundTopics || []).find(
        ot => ot.name && out.topic && ot.name === out.topic
      );
      if (match) {
        outboundNameToEntityType.set(match.name, agent.entityType || null);
      }
    }
  }

  // inbound topicName → agent descriptor
  const inboundNameToAgent = new Map();
  for (const agent of agents) {
    for (const sub of agent.subscriptions || []) {
      if (sub.topic) inboundNameToAgent.set(sub.topic, agent);
    }
  }

  // changelog topic names
  const changelogTopicNames = new Set();
  const changelogNameToAgent = new Map();
  for (const agent of agents) {
    const clf = agent.stateConfig?.changeLogTopic;
    if (clf && clf.trim()) {
      changelogTopicNames.add(clf.trim());
      changelogNameToAgent.set(clf.trim(), agent);
    }
  }

  const entityMap = new Map();
  const latestInboundByEntity = new Map();

  const getOrCreate = (eid, entityType) => {
    if (!entityMap.has(eid)) {
      entityMap.set(eid, {
        entityId:      eid,
        entityType:    entityType || null,
        lastSeen:      null,
        eventCount:    0,
        lastPayload:   null,
        entityData:    null,
        inboundCounts: {},
      });
    }
    return entityMap.get(eid);
  };

  if (useCaseStore) {
    // Inbound
    for (const [topicName, agentDesc] of inboundNameToAgent) {
      const events = useCaseStore.get(topicName) || [];
      const extractor = agentDesc.entityKeyExtractor || null;
      for (const ev of events) {
        const eid = extractEntityIdFromEvent(ev, extractor);
        if (!eid || eid === 'unknown') continue;
        const entry = getOrCreate(eid, agentDesc.entityType);
        entry.inboundCounts[topicName] = (entry.inboundCounts[topicName] || 0) + 1;
        entry.eventCount++;
        const ts = ev.timestamp
          ? (typeof ev.timestamp === 'number'
              ? new Date(ev.timestamp).toISOString()
              : String(ev.timestamp))
          : null;
        if (ts && (!entry.lastSeen || ts > entry.lastSeen)) entry.lastSeen = ts;

        const prev = latestInboundByEntity.get(eid);
        if (!prev || (ts && (!prev.ts || ts >= prev.ts))) {
          latestInboundByEntity.set(eid, { ts, eventType: topicName, event: ev });
        }
      }
    }

    // Outbound
    for (const outboundTopicName of outboundTopicNames) {
      const events = useCaseStore.get(outboundTopicName) || [];
      const ownerEntityType = outboundNameToEntityType.get(outboundTopicName) || null;
      for (const ev of events) {
        const eid = ev.entityId || null;
        if (!eid) continue;
        const entry = getOrCreate(eid, ownerEntityType);
        const tsMs = (typeof ev.timestamp === 'number' && ev.timestamp > 0) ? ev.timestamp : null;
        const ts   = tsMs ? new Date(tsMs).toISOString() : null;
        const isNewer = ts && (!entry.lastSeen || ts >= entry.lastSeen);
        if (isNewer) {
          entry.lastSeen    = ts;
          entry.lastPayload = ev;
        } else if (!entry.lastPayload) {
          entry.lastPayload = ev;
        }
      }
    }

    // Changelog topics
    for (const changelogTopicName of changelogTopicNames) {
      const agentDesc = changelogNameToAgent.get(changelogTopicName);
      const events = useCaseStore.get(changelogTopicName) || [];
      let _changelogTsMap = new Map(); // eid → latest changelog ts string
      for (const ev of events) {
        const inner = ev.payload || ev;
        const eid = inner.entityId || ev.entityId || null;
        if (!eid) continue;
        const entry = getOrCreate(eid, inner.entityType || agentDesc?.entityType || null);
        const tsMs = typeof inner.timestamp === 'number' && inner.timestamp > 0
          ? inner.timestamp : null;
        const ts = tsMs ? new Date(tsMs).toISOString() : null;
        if (inner.entity && (entry.entityData === null || (ts && ts >= (_changelogTsMap.get(eid) || '')))) {
          entry.entityData = inner.entity;
          _changelogTsMap.set(eid, ts || '');
          if (ts && (!entry.lastSeen || ts > entry.lastSeen)) entry.lastSeen = ts;
        }
      }
    }

    // Inbound-only fallback: synthesize lastPayload for entities with no outbound events
    for (const [eid, latest] of latestInboundByEntity) {
      const entry = entityMap.get(eid);
      if (!entry || entry.lastPayload !== null) continue;
      entry.lastPayload = {
        agentEventType: latest.eventType,
        entityId:       eid,
        sourceTopic:    latest.event.topic || null,
        timestamp:      typeof latest.event.timestamp === 'number'
                          ? latest.event.timestamp
                          : (latest.ts ? new Date(latest.ts).getTime() : null),
        payload:        latest.event,
      };
    }
  }

  const entities = [...entityMap.values()].sort((a, b) => {
    if (!a.lastSeen && !b.lastSeen) return 0;
    if (!a.lastSeen) return 1;
    if (!b.lastSeen) return -1;
    return b.lastSeen.localeCompare(a.lastSeen);
  });

  const topLevelEntityType = agents.length === 1 ? (agents[0]?.entityType || null) : null;
  return { entityType: topLevelEntityType, entities };
}

// ─── Fixtures ─────────────────────────────────────────────────────────────────

const BANKING_AGENTS_JSON = {
  agents: [{
    agentId:            'banking-fraud-agent',
    entityType:         'Account',
    entityKeyExtractor: '$.accountNumber',
    subscriptions: [
      { eventType: 'Transaction',   topic: 'agent-transactions',   operation: 'EvaluateTransaction' },
      { eventType: 'AccountEvent',  topic: 'agent-account-events', operation: 'HandleAccountEvent'  },
    ],
    outputs: [
      { eventType: 'Alert',        topic: 'agent-fraud-alerts'    },
      { eventType: 'agent-error',  topic: 'agent-banking-errors'  },
    ],
  }],
};

const BANKING_OUTBOUND_TOPICS = [
  { name: 'agent-fraud-alerts',   type: 'Alert'       },
  { name: 'agent-banking-errors', type: 'agent-error' },
];

// ─── Tests: extractEntityIdFromEvent ─────────────────────────────────────────

describe('extractEntityIdFromEvent', () => {
  test('extracts top-level field with "$.field" path', () => {
    expect(extractEntityIdFromEvent({ accountNumber: 'ACC-001' }, '$.accountNumber'))
      .toBe('ACC-001');
  });

  test('extracts top-level field with bare "fieldName" path (no leading $)', () => {
    expect(extractEntityIdFromEvent({ accountNumber: 'ACC-002' }, 'accountNumber'))
      .toBe('ACC-002');
  });

  test('extracts nested field with "$.a.b" path', () => {
    expect(extractEntityIdFromEvent({ customer: { id: 'C-99' } }, '$.customer.id'))
      .toBe('C-99');
  });

  test('extracts deeply nested field', () => {
    const ev = { a: { b: { c: 'DEEP-1' } } };
    expect(extractEntityIdFromEvent(ev, '$.a.b.c')).toBe('DEEP-1');
  });

  test('returns null when field is missing', () => {
    expect(extractEntityIdFromEvent({ other: 'x' }, '$.accountNumber')).toBeNull();
  });

  test('returns null for null event', () => {
    expect(extractEntityIdFromEvent(null, '$.accountNumber')).toBeNull();
  });

  test('returns null for blank extractor', () => {
    expect(extractEntityIdFromEvent({ accountNumber: 'ACC' }, '')).toBeNull();
    expect(extractEntityIdFromEvent({ accountNumber: 'ACC' }, null)).toBeNull();
  });

  test('returns null when extracted value is the literal string "unknown"', () => {
    expect(extractEntityIdFromEvent({ aircraftId: 'unknown' }, '$.aircraftId')).toBeNull();
  });

  test('handles numeric entity IDs (converts to string)', () => {
    expect(extractEntityIdFromEvent({ id: 42 }, '$.id')).toBe('42');
  });

  test('does NOT fall back to hard-coded field names (accountId, customerId, etc.)', () => {
    // If extractor is $.accountNumber, the presence of "customerId" must not help
    const ev = { customerId: 'CUST-1', accountNumber: 'ACC-003' };
    expect(extractEntityIdFromEvent(ev, '$.accountNumber')).toBe('ACC-003');
    // Ensure no leakage to other fields when the extractor path is wrong
    expect(extractEntityIdFromEvent(ev, '$.wrongField')).toBeNull();
  });
});

// ─── Tests: buildEntitySnapshot ───────────────────────────────────────────────

describe('buildEntitySnapshot — inbound entity discovery', () => {
  test('finds entities using entityKeyExtractor, not hard-coded field names', () => {
    const store = new Map([
      ['agent-transactions', [
        { accountNumber: 'ACC-001', amount: 100, timestamp: 1000 },
        { accountNumber: 'ACC-002', amount: 200, timestamp: 2000 },
      ]],
    ]);
    const { entities } = buildEntitySnapshot(BANKING_AGENTS_JSON, BANKING_OUTBOUND_TOPICS, store);
    const ids = entities.map(e => e.entityId);
    expect(ids).toContain('ACC-001');
    expect(ids).toContain('ACC-002');
  });

  test('ignores inbound events where entityKeyExtractor path is missing', () => {
    const store = new Map([
      ['agent-transactions', [
        { wrongField: 'X', amount: 100, timestamp: 1000 },
      ]],
    ]);
    const { entities } = buildEntitySnapshot(BANKING_AGENTS_JSON, BANKING_OUTBOUND_TOPICS, store);
    expect(entities).toHaveLength(0);
  });

  test('counts inbound events per topicName', () => {
    const store = new Map([
      ['agent-transactions', [
        { accountNumber: 'ACC-001', timestamp: 1000 },
        { accountNumber: 'ACC-001', timestamp: 2000 },
        { accountNumber: 'ACC-001', timestamp: 3000 },
      ]],
    ]);
    const { entities } = buildEntitySnapshot(BANKING_AGENTS_JSON, BANKING_OUTBOUND_TOPICS, store);
    expect(entities[0].inboundCounts['agent-transactions']).toBe(3);
    expect(entities[0].eventCount).toBe(3);
  });

  test('assigns entityType from the owning agent', () => {
    const store = new Map([
      ['agent-transactions', [{ accountNumber: 'ACC-001', timestamp: 1000 }]],
    ]);
    const { entities } = buildEntitySnapshot(BANKING_AGENTS_JSON, BANKING_OUTBOUND_TOPICS, store);
    expect(entities[0].entityType).toBe('Account');
  });
});

describe('buildEntitySnapshot — outbound entity discovery (toEnvelope fix)', () => {
  test('finds entities from outbound envelopes via top-level entityId', () => {
    // This requires the toEnvelope() fix — outbound events now carry entityId
    const store = new Map([
      ['agent-fraud-alerts', [
        { agentEventType: 'Alert', entityId: 'ACC-001', sourceTopic: 'agent-transactions', timestamp: 5000, payload: { decision: 'FRAUD' } },
      ]],
    ]);
    const { entities } = buildEntitySnapshot(BANKING_AGENTS_JSON, BANKING_OUTBOUND_TOPICS, store);
    expect(entities).toHaveLength(1);
    expect(entities[0].entityId).toBe('ACC-001');
  });

  test('outbound events with empty entityId are ignored', () => {
    // This represents the pre-fix state: entityId was not propagated → empty string
    const store = new Map([
      ['agent-fraud-alerts', [
        { agentEventType: 'Alert', entityId: '', timestamp: 5000 },
      ]],
    ]);
    const { entities } = buildEntitySnapshot(BANKING_AGENTS_JSON, BANKING_OUTBOUND_TOPICS, store);
    expect(entities).toHaveLength(0);
  });

  test('outbound events with timestamp=0 do not advance lastSeen (regression guard)', () => {
    // Pre-fix outbound events had timestamp=0 because toEnvelope() did not propagate the
    // inbound timestamp.  A timestamp-0 event must NOT advance entity.lastSeen — it would
    // appear as "1970-01-01T00:00:00.000Z" and push the entity to the end of the sorted list.
    // The event may still be stored as lastPayload (fallback path), but lastSeen must not
    // be set to a 1970 epoch string.
    const store = new Map([
      ['agent-transactions', [
        { accountNumber: 'ACC-001', timestamp: 3000 },
      ]],
      ['agent-fraud-alerts', [
        // entityId present (fixed) but timestamp is 0 (hypothetical old event)
        { agentEventType: 'Alert', entityId: 'ACC-001', timestamp: 0 },
      ]],
    ]);
    const { entities } = buildEntitySnapshot(BANKING_AGENTS_JSON, BANKING_OUTBOUND_TOPICS, store);
    const e = entities.find(e => e.entityId === 'ACC-001');
    expect(e).toBeDefined();
    // lastSeen must come from the inbound Transaction (ts=3000), NOT from the zero-timestamp outbound
    expect(e.lastSeen).toBe(new Date(3000).toISOString());
    // lastSeen must NOT be the 1970 epoch
    expect(e.lastSeen).not.toBe(new Date(0).toISOString());
  });

  test('lastPayload is the outbound envelope with the highest timestamp', () => {
    const store = new Map([
      ['agent-fraud-alerts', [
        { agentEventType: 'Alert', entityId: 'ACC-001', timestamp: 1000, payload: { score: 10 } },
        { agentEventType: 'Alert', entityId: 'ACC-001', timestamp: 9000, payload: { score: 99 } },
        { agentEventType: 'Alert', entityId: 'ACC-001', timestamp: 5000, payload: { score: 50 } },
      ]],
    ]);
    const { entities } = buildEntitySnapshot(BANKING_AGENTS_JSON, BANKING_OUTBOUND_TOPICS, store);
    expect(entities[0].lastPayload.payload.score).toBe(99);
  });
});

describe('buildEntitySnapshot — multi-agent use case', () => {
  const AIR_AGENTS_JSON = {
    agents: [
      {
        agentId:            'aircraft-agent',
        entityType:         'Aircraft',
        entityKeyExtractor: '$.aircraftId',
        subscriptions: [{ eventType: 'AircraftEvent', topic: 'agent-aircraft-events', operation: 'Process' }],
        outputs: [{ eventType: 'TrackedAircraftEvent', topic: 'agent-tracked-aircraft-events' }],
      },
      {
        agentId:            'airport-agent',
        entityType:         'Airport',
        entityKeyExtractor: '$.airportCode',
        subscriptions: [{ eventType: 'AirportEvent', topic: 'agent-airport-events', operation: 'Process' }],
        outputs: [{ eventType: 'Alert', topic: 'agent-atc-alerts' }],
      },
    ],
  };
  const AIR_OUTBOUND_TOPICS = [
    { name: 'agent-tracked-aircraft-events', type: 'TrackedAircraftEvent' },
    { name: 'agent-atc-alerts',              type: 'Alert'                },
  ];

  test('topLevelEntityType is null for multi-agent use cases', () => {
    const { entityType } = buildEntitySnapshot(AIR_AGENTS_JSON, AIR_OUTBOUND_TOPICS, new Map());
    expect(entityType).toBeNull();
  });

  test('aircraft entity has entityType="Aircraft"', () => {
    const store = new Map([
      ['agent-aircraft-events', [{ aircraftId: 'BA123', timestamp: 1000 }]],
    ]);
    const { entities } = buildEntitySnapshot(AIR_AGENTS_JSON, AIR_OUTBOUND_TOPICS, store);
    const aircraft = entities.find(e => e.entityId === 'BA123');
    expect(aircraft).toBeDefined();
    expect(aircraft.entityType).toBe('Aircraft');
  });

  test('airport entity has entityType="Airport"', () => {
    const store = new Map([
      ['agent-airport-events', [{ airportCode: 'LHR', timestamp: 2000 }]],
    ]);
    const { entities } = buildEntitySnapshot(AIR_AGENTS_JSON, AIR_OUTBOUND_TOPICS, store);
    const airport = entities.find(e => e.entityId === 'LHR');
    expect(airport).toBeDefined();
    expect(airport.entityType).toBe('Airport');
  });
});

describe('buildEntitySnapshot — inbound-only fallback (no outbound events)', () => {
  // This is the exact scenario: "Account creation from event" sends one AccountEvent.
  // HandleAccountEvent has no outbounds in agents.json — it only updates Flink keyed state.
  // The entity must still be visible with its full inbound event as the Entity tab payload.

  test('entity 110: AccountEvent with no outbound produces a lastPayload from the inbound event', () => {
    const store = new Map([
      ['agent-account-events', [{
        accountNumber: '110',
        accountType:   'CHECKING',
        balance:       10000,
        currency:      'DOLLAR',
        owner:         'John Smith',
        status:        'ACTIVE',
        timestamp:     '2026-08-23T20:29:16Z',
        type:          'OPENING',
      }]],
    ]);
    const { entities } = buildEntitySnapshot(BANKING_AGENTS_JSON, BANKING_OUTBOUND_TOPICS, store);
    expect(entities).toHaveLength(1);
    const e = entities[0];
    expect(e.entityId).toBe('110');
    expect(e.lastPayload).not.toBeNull();
    expect(e.lastPayload.payload.accountNumber).toBe('110');
    expect(e.lastPayload.payload.accountType).toBe('CHECKING');
    expect(e.lastPayload.payload.owner).toBe('John Smith');
  });

  test('inbound-only entity payload carries the correct agentEventType', () => {
    const store = new Map([
      ['agent-account-events', [{ accountNumber: '110', timestamp: 1000 }]],
    ]);
    const { entities } = buildEntitySnapshot(BANKING_AGENTS_JSON, BANKING_OUTBOUND_TOPICS, store);
    expect(entities[0].lastPayload.agentEventType).toBe('agent-account-events');
    expect(entities[0].lastPayload.entityId).toBe('110');
  });

  test('inbound-only fallback does NOT override an outbound payload when one exists', () => {
    // Entity receives both an inbound AccountEvent AND an outbound Alert.
    // The outbound envelope (with richer rule-engine result) must win.
    const store = new Map([
      ['agent-account-events', [{ accountNumber: '111', timestamp: 1000 }]],
      ['agent-fraud-alerts', [{
        agentEventType: 'Alert', entityId: '111', timestamp: 5000,
        payload: { decision: 'FRAUD', score: 0.95 },
      }]],
    ]);
    const { entities } = buildEntitySnapshot(BANKING_AGENTS_JSON, BANKING_OUTBOUND_TOPICS, store);
    const e = entities.find(en => en.entityId === '111');
    expect(e).toBeDefined();
    // lastPayload must be the outbound Alert envelope, not the raw inbound event
    expect(e.lastPayload.agentEventType).toBe('Alert');
    expect(e.lastPayload.payload.decision).toBe('FRAUD');
  });

  test('most recent inbound event wins when multiple inbound events exist for same entity', () => {
    const store = new Map([
      ['agent-account-events', [
        { accountNumber: '110', status: 'ACTIVE',   timestamp: 1000 },
        { accountNumber: '110', status: 'SUSPENDED', timestamp: 5000 },
        { accountNumber: '110', status: 'CLOSED',   timestamp: 3000 },
      ]],
    ]);
    const { entities } = buildEntitySnapshot(BANKING_AGENTS_JSON, BANKING_OUTBOUND_TOPICS, store);
    const e = entities[0];
    // timestamp=5000 is the newest — its status must win
    expect(e.lastPayload.payload.status).toBe('SUSPENDED');
  });
});

describe('buildEntitySnapshot — sort order', () => {
  test('entities are sorted newest-first by lastSeen', () => {
    const store = new Map([
      ['agent-transactions', [
        { accountNumber: 'OLD', timestamp: 1000 },
        { accountNumber: 'NEW', timestamp: 9000 },
        { accountNumber: 'MID', timestamp: 5000 },
      ]],
    ]);
    const { entities } = buildEntitySnapshot(BANKING_AGENTS_JSON, BANKING_OUTBOUND_TOPICS, store);
    expect(entities[0].entityId).toBe('NEW');
    expect(entities[1].entityId).toBe('MID');
    expect(entities[2].entityId).toBe('OLD');
  });

  test('entities with no lastSeen go to the end', () => {
    const store = new Map([
      ['agent-fraud-alerts', [
        { agentEventType: 'Alert', entityId: 'HAS-TS', timestamp: 5000 },
        { agentEventType: 'Alert', entityId: 'NO-TS',  timestamp: 0 },
      ]],
    ]);
    const { entities } = buildEntitySnapshot(BANKING_AGENTS_JSON, BANKING_OUTBOUND_TOPICS, store);
    const hasTs = entities.find(e => e.entityId === 'HAS-TS');
    const noTs  = entities.find(e => e.entityId === 'NO-TS');
    if (hasTs && noTs) {
      expect(entities.indexOf(hasTs)).toBeLessThan(entities.indexOf(noTs));
    }
  });
});

// ─── Tests: changelog topic → entityData ─────────────────────────────────────

describe('buildEntitySnapshot — changelog topic populates entityData', () => {
  // Fixtures: banking agent with changeLogTopic declared
  const BANKING_WITH_CHANGELOG = {
    agents: [{
      agentId:            'banking-fraud-agent',
      entityType:         'Account',
      entityKeyExtractor: '$.accountNumber',
      subscriptions: [
        { eventType: 'Transaction',  topic: 'agent-transactions',   operation: 'EvaluateTransaction' },
        { eventType: 'AccountEvent', topic: 'agent-account-events', operation: 'HandleAccountEvent'  },
      ],
      outputs: [
        { eventType: 'Alert',       topic: 'agent-fraud-alerts'   },
        { eventType: 'agent-error', topic: 'agent-banking-errors' },
      ],
      stateConfig: { changeLogTopic: 'agent-account-entity-changelog' },
    }],
  };

  const OUTBOUND_WITH_CHANGELOG = [
    { name: 'agent-fraud-alerts',             type: 'Alert'             },
    { name: 'agent-banking-errors',           type: 'agent-error'       },
    { name: 'agent-account-entity-changelog', type: 'entity-changelog'  },
  ];

  // Helper that builds a changelog envelope as Flink's EnvelopeSerializer emits it:
  // { agentEventType, entityId, sourceTopic, timestamp, payload: { entityId, entityType, timestamp, entity } }
  const makeChangelogEnvelope = (entityId, entityObj, tsMs) => ({
    agentEventType: 'entity-changelog',
    entityId,
    sourceTopic:    'agent-transactions',
    timestamp:      tsMs,
    payload: {
      entityId,
      entityType: 'Account',
      timestamp:  tsMs,
      entity:     entityObj,
    },
  });

  test('entityData is populated from the changelog topic', () => {
    const accountSnapshot = { accountNumber: 'ACC-001', balance: 5000, riskScore: 0.1 };
    const store = new Map([
      ['agent-account-entity-changelog', [
        makeChangelogEnvelope('ACC-001', accountSnapshot, 5000),
      ]],
    ]);
    const { entities } = buildEntitySnapshot(BANKING_WITH_CHANGELOG, OUTBOUND_WITH_CHANGELOG, store);
    expect(entities).toHaveLength(1);
    expect(entities[0].entityId).toBe('ACC-001');
    expect(entities[0].entityData).toEqual(accountSnapshot);
  });

  test('entityData is null when no changelog events exist', () => {
    const store = new Map([
      ['agent-transactions', [{ accountNumber: 'ACC-001', timestamp: 1000 }]],
    ]);
    const { entities } = buildEntitySnapshot(BANKING_WITH_CHANGELOG, OUTBOUND_WITH_CHANGELOG, store);
    expect(entities[0].entityData).toBeNull();
  });

  test('newest changelog snapshot wins when multiple events exist for the same entity', () => {
    const store = new Map([
      ['agent-account-entity-changelog', [
        makeChangelogEnvelope('ACC-001', { balance: 1000, riskScore: 0.1 }, 1000),
        makeChangelogEnvelope('ACC-001', { balance: 9000, riskScore: 0.9 }, 9000),
        makeChangelogEnvelope('ACC-001', { balance: 5000, riskScore: 0.5 }, 5000),
      ]],
    ]);
    const { entities } = buildEntitySnapshot(BANKING_WITH_CHANGELOG, OUTBOUND_WITH_CHANGELOG, store);
    expect(entities[0].entityData.balance).toBe(9000);
    expect(entities[0].entityData.riskScore).toBe(0.9);
  });

  test('changelog snapshot advances lastSeen', () => {
    const store = new Map([
      ['agent-transactions', [{ accountNumber: 'ACC-001', timestamp: 1000 }]],
      ['agent-account-entity-changelog', [
        makeChangelogEnvelope('ACC-001', { balance: 5000 }, 8000),
      ]],
    ]);
    const { entities } = buildEntitySnapshot(BANKING_WITH_CHANGELOG, OUTBOUND_WITH_CHANGELOG, store);
    expect(entities[0].lastSeen).toBe(new Date(8000).toISOString());
  });

  test('entityData is set even when no inbound or outbound events exist', () => {
    // Entity only visible through changelog — no domain events seen yet
    const store = new Map([
      ['agent-account-entity-changelog', [
        makeChangelogEnvelope('ACC-NEW', { accountNumber: 'ACC-NEW', balance: 0 }, 3000),
      ]],
    ]);
    const { entities } = buildEntitySnapshot(BANKING_WITH_CHANGELOG, OUTBOUND_WITH_CHANGELOG, store);
    expect(entities).toHaveLength(1);
    expect(entities[0].entityId).toBe('ACC-NEW');
    expect(entities[0].entityData).not.toBeNull();
  });

  test('changelog events with missing entity field do not crash', () => {
    const store = new Map([
      ['agent-account-entity-changelog', [
        { agentEventType: 'entity-changelog', entityId: 'ACC-001', timestamp: 5000,
          payload: { entityId: 'ACC-001', entityType: 'Account', timestamp: 5000 /* no entity key */ } },
      ]],
    ]);
    // Should not throw; entityData remains null
    expect(() => {
      buildEntitySnapshot(BANKING_WITH_CHANGELOG, OUTBOUND_WITH_CHANGELOG, store);
    }).not.toThrow();
    const { entities } = buildEntitySnapshot(BANKING_WITH_CHANGELOG, OUTBOUND_WITH_CHANGELOG, store);
    const e = entities.find(en => en.entityId === 'ACC-001');
    if (e) expect(e.entityData).toBeNull();
  });

  test('entityData co-exists with lastPayload from outbound Alert', () => {
    const store = new Map([
      ['agent-fraud-alerts', [{
        agentEventType: 'Alert', entityId: 'ACC-001', timestamp: 7000,
        payload: { fraudScore: 0.95 },
      }]],
      ['agent-account-entity-changelog', [
        makeChangelogEnvelope('ACC-001', { balance: 3000 }, 6000),
      ]],
    ]);
    const { entities } = buildEntitySnapshot(BANKING_WITH_CHANGELOG, OUTBOUND_WITH_CHANGELOG, store);
    const e = entities.find(en => en.entityId === 'ACC-001');
    expect(e.lastPayload.payload.fraudScore).toBe(0.95); // outbound Alert
    expect(e.entityData.balance).toBe(3000);             // changelog snapshot
  });
});

// ─── upsertEntityState (replica from bff/server.js) ──────────────────────────

/**
 * Replica of upsertEntityState from bff/server.js — must stay in sync.
 * Manages entityStateStore (Map<useCaseId, Map<agentId, Map<entityId, snapshot>>).
 */
function makeEntityStateStore() {
  return new Map();
}

function getEntityAgentMap(entityStateStore, useCaseId, agentId) {
  if (!entityStateStore.has(useCaseId)) entityStateStore.set(useCaseId, new Map());
  const byAgent = entityStateStore.get(useCaseId);
  if (!byAgent.has(agentId)) byAgent.set(agentId, new Map());
  return byAgent.get(agentId);
}

function upsertEntityState(entityStateStore, useCaseId, agentId, entityType, envelope) {
  const inner    = envelope.payload || envelope;
  const entityId = inner.entityId || envelope.entityId;
  const entity   = inner.entity;
  if (!entityId || !entity) return;

  const tsMs = typeof inner.timestamp === 'number' && inner.timestamp > 0
    ? inner.timestamp
    : (typeof envelope.timestamp === 'number' && envelope.timestamp > 0 ? envelope.timestamp : null);

  const agentMap = getEntityAgentMap(entityStateStore, useCaseId, agentId);
  const existing = agentMap.get(entityId);

  // Replace when the incoming snapshot is newer or equal-timestamp (last-write-wins).
  if (!existing || !existing.timestamp || (tsMs !== null && tsMs >= existing.timestamp)) {
    agentMap.set(entityId, {
      entityId,
      entityType: inner.entityType || entityType || null,
      agentId,
      entityData: entity,
      timestamp:  tsMs,
    });
  }
}

/**
 * Simulates the __ENTITY_RESET__ handling added to the live consumer in bff/server.js.
 * When a reset sentinel arrives, the corresponding entityStateStore entry is cleared.
 */
function applyEntityReset(entityStateStore, useCaseId, changelogTopicAgentMap, resetSentinel) {
  if (resetSentinel.__eventType !== '__ENTITY_RESET__') return;
  const keyField = Object.keys(resetSentinel).find(k => k !== '__eventType');
  if (!keyField) return;
  const resetEntityId = String(resetSentinel[keyField]);
  for (const [, agentDesc] of changelogTopicAgentMap) {
    const agentMap = entityStateStore.get(useCaseId)?.get(agentDesc.agentId);
    if (agentMap && agentMap.has(resetEntityId)) {
      agentMap.delete(resetEntityId);
    }
  }
}

// ─── Tests: upsertEntityState ─────────────────────────────────────────────────

describe('upsertEntityState — basic insertion', () => {
  const makeEnvelope = (entityId, entityObj, tsMs) => ({
    agentEventType: 'entity-changelog',
    entityId,
    timestamp: tsMs,
    payload: { entityId, entityType: 'Account', timestamp: tsMs, entity: entityObj },
  });

  test('inserts a new entity snapshot', () => {
    const store = makeEntityStateStore();
    upsertEntityState(store, 'uc1', 'agent1', 'Account',
      makeEnvelope('ACC-001', { balance: 1000 }, 5000));
    const snap = store.get('uc1')?.get('agent1')?.get('ACC-001');
    expect(snap).toBeDefined();
    expect(snap.entityData.balance).toBe(1000);
    expect(snap.timestamp).toBe(5000);
  });

  test('updates an existing snapshot when new timestamp is strictly newer', () => {
    const store = makeEntityStateStore();
    upsertEntityState(store, 'uc1', 'agent1', 'Account',
      makeEnvelope('ACC-001', { balance: 1000 }, 5000));
    upsertEntityState(store, 'uc1', 'agent1', 'Account',
      makeEnvelope('ACC-001', { balance: 9000 }, 6000));
    expect(store.get('uc1').get('agent1').get('ACC-001').entityData.balance).toBe(9000);
  });

  test('updates an existing snapshot when new timestamp is equal (>=, last-write-wins)', () => {
    const store = makeEntityStateStore();
    upsertEntityState(store, 'uc1', 'agent1', 'Account',
      makeEnvelope('ACC-001', { balance: 1000 }, 5000));
    // Same timestamp — must overwrite so scenario replay works
    upsertEntityState(store, 'uc1', 'agent1', 'Account',
      makeEnvelope('ACC-001', { balance: 9999 }, 5000));
    expect(store.get('uc1').get('agent1').get('ACC-001').entityData.balance).toBe(9999);
  });

  test('does NOT update when new timestamp is strictly older', () => {
    const store = makeEntityStateStore();
    upsertEntityState(store, 'uc1', 'agent1', 'Account',
      makeEnvelope('ACC-001', { balance: 9000 }, 6000));
    upsertEntityState(store, 'uc1', 'agent1', 'Account',
      makeEnvelope('ACC-001', { balance: 1000 }, 5000)); // older
    expect(store.get('uc1').get('agent1').get('ACC-001').entityData.balance).toBe(9000);
  });

  test('silently skips envelope with no entity field', () => {
    const store = makeEntityStateStore();
    upsertEntityState(store, 'uc1', 'agent1', 'Account', {
      entityId: 'ACC-001', timestamp: 5000,
      payload: { entityId: 'ACC-001', entityType: 'Account', timestamp: 5000 /* no entity */ },
    });
    expect(store.get('uc1')?.get('agent1')?.get('ACC-001')).toBeUndefined();
  });

  test('silently skips envelope with no entityId', () => {
    const store = makeEntityStateStore();
    upsertEntityState(store, 'uc1', 'agent1', 'Account', {
      timestamp: 5000,
      payload: { entityType: 'Account', timestamp: 5000, entity: { balance: 1 } },
    });
    // Should be empty — no entityId to key by
    const byAgent = store.get('uc1')?.get('agent1');
    expect(!byAgent || byAgent.size === 0).toBe(true);
  });

  test('stores snapshots independently per entity id', () => {
    const store = makeEntityStateStore();
    upsertEntityState(store, 'uc1', 'agent1', 'Account',
      makeEnvelope('ACC-001', { balance: 100 }, 1000));
    upsertEntityState(store, 'uc1', 'agent1', 'Account',
      makeEnvelope('ACC-002', { balance: 200 }, 2000));
    expect(store.get('uc1').get('agent1').size).toBe(2);
    expect(store.get('uc1').get('agent1').get('ACC-001').entityData.balance).toBe(100);
    expect(store.get('uc1').get('agent1').get('ACC-002').entityData.balance).toBe(200);
  });
});

// ─── Tests: __ENTITY_RESET__ clears entityStateStore ─────────────────────────

describe('applyEntityReset — entityStateStore cleared on scenario reset', () => {
  const makeEnvelope = (entityId, entityObj, tsMs) => ({
    agentEventType: 'entity-changelog',
    entityId,
    timestamp: tsMs,
    payload: { entityId, entityType: 'Account', timestamp: tsMs, entity: entityObj },
  });

  const CHANGELOG_AGENT_MAP = new Map([
    ['agent-account-entity-changelog', { agentId: 'banking-fraud-agent', entityType: 'Account' }],
  ]);

  test('clears the entity snapshot when a reset sentinel is received', () => {
    const store = makeEntityStateStore();
    upsertEntityState(store, 'uc1', 'banking-fraud-agent', 'Account',
      makeEnvelope('ACC-001', { balance: 5000 }, 5000));
    expect(store.get('uc1').get('banking-fraud-agent').has('ACC-001')).toBe(true);

    applyEntityReset(store, 'uc1', CHANGELOG_AGENT_MAP, { __eventType: '__ENTITY_RESET__', accountNumber: 'ACC-001' });
    expect(store.get('uc1').get('banking-fraud-agent').has('ACC-001')).toBe(false);
  });

  test('after reset, same-timestamp changelog event repopulates the entity', () => {
    const store = makeEntityStateStore();
    // Round 1: entity created at ts=5000
    upsertEntityState(store, 'uc1', 'banking-fraud-agent', 'Account',
      makeEnvelope('ACC-001', { balance: 5000 }, 5000));
    // Reset clears the snapshot
    applyEntityReset(store, 'uc1', CHANGELOG_AGENT_MAP, { __eventType: '__ENTITY_RESET__', accountNumber: 'ACC-001' });
    // Round 2: same scenario replayed — same timestamp, new entity data
    upsertEntityState(store, 'uc1', 'banking-fraud-agent', 'Account',
      makeEnvelope('ACC-001', { balance: 9999 }, 5000));
    const snap = store.get('uc1').get('banking-fraud-agent').get('ACC-001');
    expect(snap).toBeDefined();
    expect(snap.entityData.balance).toBe(9999);
  });

  test('does not clear unrelated entity snapshots', () => {
    const store = makeEntityStateStore();
    upsertEntityState(store, 'uc1', 'banking-fraud-agent', 'Account',
      makeEnvelope('ACC-001', { balance: 1000 }, 1000));
    upsertEntityState(store, 'uc1', 'banking-fraud-agent', 'Account',
      makeEnvelope('ACC-002', { balance: 2000 }, 2000));

    applyEntityReset(store, 'uc1', CHANGELOG_AGENT_MAP, { __eventType: '__ENTITY_RESET__', accountNumber: 'ACC-001' });
    // ACC-001 cleared; ACC-002 untouched
    expect(store.get('uc1').get('banking-fraud-agent').has('ACC-001')).toBe(false);
    expect(store.get('uc1').get('banking-fraud-agent').has('ACC-002')).toBe(true);
  });

  test('is a no-op when the entity has no existing snapshot', () => {
    const store = makeEntityStateStore();
    // No prior snapshot — reset should not throw
    expect(() => {
      applyEntityReset(store, 'uc1', CHANGELOG_AGENT_MAP, { __eventType: '__ENTITY_RESET__', accountNumber: 'ACC-999' });
    }).not.toThrow();
  });

  test('is a no-op for non-reset sentinel values', () => {
    const store = makeEntityStateStore();
    upsertEntityState(store, 'uc1', 'banking-fraud-agent', 'Account',
      makeEnvelope('ACC-001', { balance: 5000 }, 5000));
    // Not a reset sentinel
    applyEntityReset(store, 'uc1', CHANGELOG_AGENT_MAP, { accountNumber: 'ACC-001' });
    expect(store.get('uc1').get('banking-fraud-agent').has('ACC-001')).toBe(true);
  });
});

// ─── Tests: buildEntitySnapshot — same-timestamp scenario replay ──────────────

describe('buildEntitySnapshot — same-timestamp scenario replay (>= fix)', () => {
  const BANKING_WITH_CHANGELOG = {
    agents: [{
      agentId:            'banking-fraud-agent',
      entityType:         'Account',
      entityKeyExtractor: '$.accountNumber',
      subscriptions: [
        { eventType: 'Transaction',  topic: 'agent-transactions',   operation: 'EvaluateTransaction' },
        { eventType: 'AccountEvent', topic: 'agent-account-events', operation: 'HandleAccountEvent'  },
      ],
      outputs: [
        { eventType: 'Alert',       topic: 'agent-fraud-alerts'   },
        { eventType: 'agent-error', topic: 'agent-banking-errors' },
      ],
      stateConfig: { changeLogTopic: 'agent-account-entity-changelog' },
    }],
  };

  const OUTBOUND_WITH_CHANGELOG = [
    { name: 'agent-fraud-alerts',             type: 'Alert'            },
    { name: 'agent-banking-errors',           type: 'agent-error'      },
    { name: 'agent-account-entity-changelog', type: 'entity-changelog' },
  ];

  const makeEnvelope = (entityId, entityObj, tsMs) => ({
    agentEventType: 'entity-changelog',
    entityId,
    sourceTopic: 'agent-transactions',
    timestamp: tsMs,
    payload: { entityId, entityType: 'Account', timestamp: tsMs, entity: entityObj },
  });

  test('second same-timestamp changelog overwrites first (last-write-wins)', () => {
    // Simulates: scenario run 1 emits changelog at ts=5000, ring buffer stores it.
    // Scenario run 2 replays with same ts=5000. Ring buffer now has both events.
    // Slow-path scan must return the LAST event (post-reset entity data).
    const store = new Map([
      ['agent-account-entity-changelog', [
        makeEnvelope('ACC-001', { balance: 1000, status: 'pre-reset'  }, 5000),
        makeEnvelope('ACC-001', { balance: 9999, status: 'post-reset' }, 5000),
      ]],
    ]);
    const { entities } = buildEntitySnapshot(BANKING_WITH_CHANGELOG, OUTBOUND_WITH_CHANGELOG, store);
    const e = entities.find(en => en.entityId === 'ACC-001');
    expect(e).toBeDefined();
    // With >= fix the second event at ts=5000 must win
    expect(e.entityData.balance).toBe(9999);
    expect(e.entityData.status).toBe('post-reset');
  });

  test('strictly newer timestamp still wins over older snapshot', () => {
    const store = new Map([
      ['agent-account-entity-changelog', [
        makeEnvelope('ACC-001', { balance: 1000 }, 5000),
        makeEnvelope('ACC-001', { balance: 9999 }, 6000),
        makeEnvelope('ACC-001', { balance: 5000 }, 3000), // older — must lose
      ]],
    ]);
    const { entities } = buildEntitySnapshot(BANKING_WITH_CHANGELOG, OUTBOUND_WITH_CHANGELOG, store);
    const e = entities.find(en => en.entityId === 'ACC-001');
    expect(e.entityData.balance).toBe(9999);
  });

  test('multiple rapid same-timestamp events — last stored event wins', () => {
    const store = new Map([
      ['agent-account-entity-changelog', [
        makeEnvelope('ACC-001', { step: 1 }, 5000),
        makeEnvelope('ACC-001', { step: 2 }, 5000),
        makeEnvelope('ACC-001', { step: 3 }, 5000),
        makeEnvelope('ACC-001', { step: 4 }, 5000),
      ]],
    ]);
    const { entities } = buildEntitySnapshot(BANKING_WITH_CHANGELOG, OUTBOUND_WITH_CHANGELOG, store);
    const e = entities.find(en => en.entityId === 'ACC-001');
    expect(e.entityData.step).toBe(4);
  });
});

// ─── Tests: lastPayload / Last Rule Engine Result — same-timestamp replay ──────

describe('buildEntitySnapshot — lastPayload updates correctly after reset+replay (>= fix)', () => {
  // Fixtures: banking agent (has outbound fraud-alerts topic)
  const BANKING_AGENTS_JSON_OB = {
    agents: [{
      agentId:            'banking-fraud-agent',
      entityType:         'Account',
      entityKeyExtractor: '$.accountNumber',
      subscriptions: [
        { eventType: 'Transaction',  topic: 'agent-transactions',   operation: 'EvaluateTransaction' },
        { eventType: 'AccountEvent', topic: 'agent-account-events', operation: 'HandleAccountEvent'  },
      ],
      outputs: [
        { eventType: 'Alert',       topic: 'agent-fraud-alerts'   },
        { eventType: 'agent-error', topic: 'agent-banking-errors' },
      ],
    }],
  };
  const OUTBOUND = [
    { name: 'agent-fraud-alerts',   type: 'Alert'       },
    { name: 'agent-banking-errors', type: 'agent-error' },
  ];

  const makeAlert = (entityId, payload, tsMs) => ({
    agentEventType: 'Alert',
    entityId,
    sourceTopic:    'agent-transactions',
    timestamp:      tsMs,
    payload,
  });

  test('second same-timestamp outbound event replaces lastPayload (last-write-wins)', () => {
    // Simulates ring buffer after reset+replay: old alert then new alert at same timestamp.
    const store = new Map([
      ['agent-fraud-alerts', [
        makeAlert('ACC-001', { decision: 'FRAUD', run: 'pre-reset'  }, 5000),
        makeAlert('ACC-001', { decision: 'FRAUD', run: 'post-reset' }, 5000),
      ]],
    ]);
    const { entities } = buildEntitySnapshot(BANKING_AGENTS_JSON_OB, OUTBOUND, store);
    const e = entities.find(en => en.entityId === 'ACC-001');
    expect(e).toBeDefined();
    // With >= fix the second (post-reset) alert must win
    expect(e.lastPayload.payload.run).toBe('post-reset');
  });

  test('strictly newer outbound event still wins over older one', () => {
    const store = new Map([
      ['agent-fraud-alerts', [
        makeAlert('ACC-001', { score: 10 }, 3000),
        makeAlert('ACC-001', { score: 99 }, 6000),
        makeAlert('ACC-001', { score: 50 }, 1000), // oldest — must not win
      ]],
    ]);
    const { entities } = buildEntitySnapshot(BANKING_AGENTS_JSON_OB, OUTBOUND, store);
    const e = entities.find(en => en.entityId === 'ACC-001');
    expect(e.lastPayload.payload.score).toBe(99);
  });

  test('multiple rapid same-timestamp outbound events — last stored wins', () => {
    const store = new Map([
      ['agent-fraud-alerts', [
        makeAlert('ACC-001', { step: 1 }, 5000),
        makeAlert('ACC-001', { step: 2 }, 5000),
        makeAlert('ACC-001', { step: 3 }, 5000),
        makeAlert('ACC-001', { step: 4 }, 5000),
      ]],
    ]);
    const { entities } = buildEntitySnapshot(BANKING_AGENTS_JSON_OB, OUTBOUND, store);
    const e = entities.find(en => en.entityId === 'ACC-001');
    expect(e.lastPayload.payload.step).toBe(4);
  });

  test('agentSummary.lastPayload reflects the latest outbound event after same-timestamp replay', () => {
    // This is the exact path for "Last Rule Engine Result" in AgentDetailView.
    // The summary is built with summary=true — we verify lastPayload here directly.
    const BANKING_WITH_CHANGELOG = {
      agents: [{
        agentId:            'banking-fraud-agent',
        entityType:         'Account',
        entityKeyExtractor: '$.accountNumber',
        subscriptions: [
          { eventType: 'Transaction',  topic: 'agent-transactions',   operation: 'EvaluateTransaction' },
          { eventType: 'AccountEvent', topic: 'agent-account-events', operation: 'HandleAccountEvent'  },
        ],
        outputs: [
          { eventType: 'Alert',       topic: 'agent-fraud-alerts'   },
          { eventType: 'agent-error', topic: 'agent-banking-errors' },
        ],
        stateConfig: { changeLogTopic: 'agent-account-entity-changelog' },
      }],
    };
    const OUTBOUND_WITH_CHANGELOG = [
      { name: 'agent-fraud-alerts',             type: 'Alert'            },
      { name: 'agent-banking-errors',           type: 'agent-error'      },
      { name: 'agent-account-entity-changelog', type: 'entity-changelog' },
    ];

    const store = new Map([
      ['agent-fraud-alerts', [
        makeAlert('ACC-001', { decision: 'FRAUD', run: 'old' }, 5000),
        makeAlert('ACC-001', { decision: 'NO_ACTION', run: 'new' }, 5000),
      ]],
    ]);
    const { entities } = buildEntitySnapshot(BANKING_WITH_CHANGELOG, OUTBOUND_WITH_CHANGELOG, store);

    // Simulate the agentSummary reduce (same logic as BFF)
    const agentSummary = entities.reduce((summary, entry) => {
      for (const [eventType, count] of Object.entries(entry.inboundCounts || {})) {
        summary.inboundCounts[eventType] = (summary.inboundCounts[eventType] || 0) + count;
      }
      if (entry.lastPayload && (!summary.lastPayload ||
          (entry.lastSeen && entry.lastSeen >= (summary.lastSeen || '')))) {
        summary.lastPayload = entry.lastPayload;
        summary.lastSeen = entry.lastSeen;
      }
      return summary;
    }, { inboundCounts: {}, lastPayload: null, lastSeen: null });

    // The "Last Rule Engine Result" is agentSummary.lastPayload.payload
    expect(agentSummary.lastPayload).not.toBeNull();
    expect(agentSummary.lastPayload.payload.run).toBe('new');
    expect(agentSummary.lastPayload.payload.decision).toBe('NO_ACTION');
  });

  test('lastPayload reflects the latest outbound event across different entities', () => {
    // Two entities both updated at ts=5000 after a reset replay
    const store = new Map([
      ['agent-fraud-alerts', [
        makeAlert('ACC-001', { score: 10, run: 'old' }, 5000),
        makeAlert('ACC-002', { score: 20, run: 'old' }, 5000),
        makeAlert('ACC-001', { score: 99, run: 'new' }, 5000),
        makeAlert('ACC-002', { score: 88, run: 'new' }, 5000),
      ]],
    ]);
    const { entities } = buildEntitySnapshot(BANKING_AGENTS_JSON_OB, OUTBOUND, store);
    const e1 = entities.find(en => en.entityId === 'ACC-001');
    const e2 = entities.find(en => en.entityId === 'ACC-002');
    expect(e1.lastPayload.payload.run).toBe('new');
    expect(e2.lastPayload.payload.run).toBe('new');
  });
});

// ─── Tests: inbound-only fallback lastPayload — same-timestamp replay ──────────

describe('buildEntitySnapshot — inbound-only fallback lastPayload same-timestamp replay', () => {
  // HandleAccountEvent has no outbounds — uses inbound fallback for lastPayload.
  const ACCOUNT_ONLY_AGENTS = {
    agents: [{
      agentId:            'banking-fraud-agent',
      entityType:         'Account',
      entityKeyExtractor: '$.accountNumber',
      subscriptions: [
        { eventType: 'AccountEvent', topic: 'agent-account-events', operation: 'HandleAccountEvent' },
      ],
      outputs: [], // no outbounds
    }],
  };

  test('second same-timestamp inbound event replaces fallback lastPayload', () => {
    const store = new Map([
      ['agent-account-events', [
        { accountNumber: 'ACC-001', balance: 1000, run: 'pre-reset',  timestamp: 5000 },
        { accountNumber: 'ACC-001', balance: 9999, run: 'post-reset', timestamp: 5000 },
      ]],
    ]);
    const { entities } = buildEntitySnapshot(ACCOUNT_ONLY_AGENTS, [], store);
    const e = entities.find(en => en.entityId === 'ACC-001');
    expect(e).toBeDefined();
    // The inbound fallback lastPayload.payload must be the last inbound event
    expect(e.lastPayload.payload.run).toBe('post-reset');
    expect(e.lastPayload.payload.balance).toBe(9999);
  });
});
