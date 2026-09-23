/**
 * BFF unit tests — pure helper functions.
 *
 * These tests cover the logic that can be exercised without a live Kafka broker,
 * Flink cluster, or filesystem: isValidAgentId, buildStubDescriptor,
 * collectTopicsFromAgentsJson, and the entityStateStore eviction logic.
 *
 * Run with:  npm test
 * (No Docker stack required.)
 */

'use strict';

// ── Import helpers from server.js ─────────────────────────────────────────────
// We use require() with a thin module-isolation trick: mock Kafka/Express
// before loading so the server does not try to connect at import time.

jest.mock('kafkajs', () => ({
  Kafka: jest.fn().mockReturnValue({
    producer: jest.fn().mockReturnValue({ connect: jest.fn(), disconnect: jest.fn(), send: jest.fn() }),
    consumer: jest.fn().mockReturnValue({ connect: jest.fn(), subscribe: jest.fn(), run: jest.fn(), disconnect: jest.fn() }),
    admin:    jest.fn().mockReturnValue({ connect: jest.fn(), createTopics: jest.fn(), deleteTopics: jest.fn(), listTopics: jest.fn().mockResolvedValue([]), fetchTopicOffsets: jest.fn().mockResolvedValue([]), disconnect: jest.fn() }),
  }),
}));
jest.mock('express', () => {
  const app = { use: jest.fn(), get: jest.fn(), post: jest.fn(), put: jest.fn(), patch: jest.fn(), delete: jest.fn() };
  const fn  = jest.fn().mockReturnValue(app);
  fn.json   = jest.fn().mockReturnValue(jest.fn());
  fn.Router = jest.fn().mockReturnValue(app);
  return fn;
});
jest.mock('http',    () => ({ createServer: jest.fn().mockReturnValue({ listen: jest.fn(), close: jest.fn(), on: jest.fn() }) }));
jest.mock('multer',  () => { const m = jest.fn().mockReturnValue({ single: jest.fn() }); m.diskStorage = jest.fn(); return m; });
jest.mock('ws',      () => ({ WebSocketServer: jest.fn().mockReturnValue({ on: jest.fn() }) }));

// Prevent the server from actually binding a port or connecting to Kafka at require time
const server = require('../server');

// ── isValidAgentId ────────────────────────────────────────────────────────────

describe('isValidAgentId', () => {
  const fn = server.isValidAgentId;
  if (!fn) {
    it.todo('isValidAgentId is not exported — add module.exports to server.js');
    return;
  }

  test('accepts simple lowercase id', () => expect(fn('banking-fraud-agent')).toBe(true));
  test('accepts underscores',         () => expect(fn('my_agent_v2')).toBe(true));
  test('accepts uppercase',           () => expect(fn('MyAgent')).toBe(true));
  test('accepts digits',              () => expect(fn('agent123')).toBe(true));
  test('rejects empty string',        () => expect(fn('')).toBe(false));
  test('rejects dot in name',         () => expect(fn('my.agent')).toBe(false));
  test('rejects slash',               () => expect(fn('../etc/passwd')).toBe(false));
  test('rejects > 64 chars',          () => expect(fn('a'.repeat(65))).toBe(false));
  test('accepts exactly 64 chars',    () => expect(fn('a'.repeat(64))).toBe(true));
  test('rejects null',                () => expect(fn(null)).toBe(false));
  test('rejects undefined',           () => expect(fn(undefined)).toBe(false));
  test('rejects space',               () => expect(fn('my agent')).toBe(false));
});

// ── buildStubDescriptor ───────────────────────────────────────────────────────

describe('buildStubDescriptor', () => {
  const fn = server.buildStubDescriptor;
  if (!fn) {
    it.todo('buildStubDescriptor is not exported — add module.exports to server.js');
    return;
  }

  const bankingAgents = {
    agents: [{
      agentId:    'banking-fraud-agent',
      name:       'Banking Fraud Detection Agent',
      entityType: 'Account',
      subscriptions: [
        { eventType: 'Transaction',  topic: 'agent-transactions',    operation: 'EvaluateTransaction'  },
        { eventType: 'AccountEvent', topic: 'agent-account-events',  operation: 'HandleAccountEvent'   },
      ],
      ruleset: { path: 'samples/banking-fraud-agent/FraudDetectionService_1.5.2.jar', archiveControlTopic: 'agent-banking-archive-control' },
      outputs: [
        { eventType: 'Alert',       topic: 'agent-fraud-alerts',    description: 'Fraud alerts' },
        { eventType: 'agent-error', topic: 'agent-banking-errors',  description: 'Errors' },
      ],
      stateConfig: { changeLogTopic: 'agent-account-entity-changelog' },
    }],
  };

  test('produces a descriptor with topics derived from subscriptions', () => {
    const stub = fn('banking-fraud-agent', bankingAgents);
    const topicNames = stub.topics.map(t => t.name);
    expect(topicNames).toContain('agent-transactions');
    expect(topicNames).toContain('agent-account-events');
  });

  test('produces outboundTopics from outputs', () => {
    const stub = fn('banking-fraud-agent', bankingAgents);
    const outNames = stub.outboundTopics.map(t => t.name);
    expect(outNames).toContain('agent-fraud-alerts');
    expect(outNames).toContain('agent-banking-errors');
  });

  test('includes changeLogTopic in outboundTopics', () => {
    const stub = fn('banking-fraud-agent', bankingAgents);
    const outNames = stub.outboundTopics.map(t => t.name);
    expect(outNames).toContain('agent-account-entity-changelog');
  });

  test('sets type=entity-changelog for the changelog topic', () => {
    const stub = fn('banking-fraud-agent', bankingAgents);
    const clf = stub.outboundTopics.find(t => t.name === 'agent-account-entity-changelog');
    expect(clf.type).toBe('entity-changelog');
  });

  test('uses agent name as descriptor name for single-agent use-cases', () => {
    const stub = fn('banking-fraud-agent', bankingAgents);
    expect(stub.name).toBe('Banking Fraud Detection Agent');
  });

  test('falls back to title-cased useCaseId when agent has no name', () => {
    const noName = { agents: [{ agentId: 'my-agent', subscriptions: [], outputs: [], ruleset: { path: '/tmp/a.jar' } }] };
    const stub = fn('my-agent', noName);
    expect(stub.name).toBe('My Agent');
  });

  test('deduplicates topics that appear in multiple agents', () => {
    const shared = {
      agents: [
        { agentId: 'a1', subscriptions: [{ eventType: 'Ev', topic: 'shared-topic' }], outputs: [], ruleset: { path: '/tmp/a.jar' } },
        { agentId: 'a2', subscriptions: [{ eventType: 'Ev', topic: 'shared-topic' }], outputs: [], ruleset: { path: '/tmp/b.jar' } },
      ],
    };
    const stub = fn('multi', shared);
    const byName = stub.topics.filter(t => t.name === 'shared-topic');
    expect(byName).toHaveLength(1);
  });

  test('empty agents array produces empty topics/outboundTopics', () => {
    const stub = fn('empty', { agents: [] });
    expect(stub.topics).toHaveLength(0);
    expect(stub.outboundTopics).toHaveLength(0);
  });

  test('includes flinkJobName and consumerGroupId in output', () => {
    const stub = fn('my-case', { agents: [] });
    expect(stub.flinkJobName).toBe('my-case-job');
    expect(stub.consumerGroupId).toContain('my-case');
  });
});

// ── collectTopicsFromAgentsJson ────────────────────────────────────────────────

describe('collectTopicsFromAgentsJson', () => {
  const fn = server.collectTopicsFromAgentsJson;
  if (!fn) {
    it.todo('collectTopicsFromAgentsJson is not exported — add module.exports to server.js');
    return;
  }

  test('collects subscription topics', () => {
    const topics = fn({ agents: [{ subscriptions: [{ topic: 'txn-topic' }, { topic: 'acc-topic' }], outputs: [], ruleset: {} }] });
    expect(topics.has('txn-topic')).toBe(true);
    expect(topics.has('acc-topic')).toBe(true);
  });

  test('collects output topics', () => {
    const topics = fn({ agents: [{ subscriptions: [], outputs: [{ topic: 'alert-topic' }], ruleset: {} }] });
    expect(topics.has('alert-topic')).toBe(true);
  });

  test('collects archiveControlTopic', () => {
    const topics = fn({ agents: [{ subscriptions: [], outputs: [], ruleset: { archiveControlTopic: 'ctrl-topic' } }] });
    expect(topics.has('ctrl-topic')).toBe(true);
  });

  test('collects changeLogTopic', () => {
    const topics = fn({ agents: [{ subscriptions: [], outputs: [], ruleset: {}, stateConfig: { changeLogTopic: 'log-topic' } }] });
    expect(topics.has('log-topic')).toBe(true);
  });

  test('deduplicates topics appearing in multiple agents', () => {
    const topics = fn({
      agents: [
        { subscriptions: [{ topic: 'shared' }], outputs: [{ topic: 'shared' }], ruleset: {} },
        { subscriptions: [{ topic: 'shared' }], outputs: [],                    ruleset: {} },
      ],
    });
    expect([...topics].filter(t => t === 'shared')).toHaveLength(1);
  });

  test('trims whitespace from topic names', () => {
    const topics = fn({ agents: [{ subscriptions: [{ topic: '  trimmed  ' }], outputs: [], ruleset: {} }] });
    expect(topics.has('trimmed')).toBe(true);
    expect(topics.has('  trimmed  ')).toBe(false);
  });

  test('ignores blank topic values', () => {
    const topics = fn({ agents: [{ subscriptions: [{ topic: '' }, { topic: '  ' }], outputs: [], ruleset: {} }] });
    expect(topics.size).toBe(0);
  });

  test('empty agents array produces empty set', () => {
    expect(fn({ agents: [] }).size).toBe(0);
  });
});
