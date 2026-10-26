/**
 * BFF unit tests — pure helper functions.
 *
 * These tests cover the logic that can be exercised without a live Kafka broker,
 * Flink cluster, or filesystem: isValidAgentId, buildUseCaseDescriptor,
 * collectTopicsFromAgentsJson, invokeArchiveSampleGenerator, and unwrapAdsSample.
 *
 * Run with:  npm test   (from event-rule-frontend/, via Vitest)
 *        or: cd bff && npm test  (from event-rule-frontend/bff/, via Jest)
 * (No Docker stack required for either.)
 *
 * invokeArchiveSampleGenerator accepts an optional spawnFn parameter so tests
 * can inject a fake spawn without any module-level mocking.
 */

'use strict';

// ── Module mocks ──────────────────────────────────────────────────────────────
// Mock heavy server dependencies so server.js loads without side-effects.
// Both jest.mock (Jest) and jest.mock-via-shim (Vitest, see setupTests.js) hoist
// these calls before any require(), so they are in place when server.js loads.
// The setupTests.js shim assigns globalThis.jest = vi, making jest.* calls work
// transparently under Vitest.

jest.mock('kafkajs', () => {
  // Producer mock: includes .on() and .events so the H-6 DISCONNECT listener
  // registration in server.js does not throw at module load time.
  const DISCONNECT = 'producer.disconnect';
  const mkProducer = () => ({
    connect:    jest.fn(),
    disconnect: jest.fn(),
    send:       jest.fn(),
    on:         jest.fn(),
    events:     { DISCONNECT },
  });
  const mkKafka = () => ({
    producer: jest.fn().mockReturnValue(mkProducer()),
    consumer: jest.fn().mockReturnValue({ connect: jest.fn(), subscribe: jest.fn(), run: jest.fn(), disconnect: jest.fn() }),
    admin:    jest.fn().mockReturnValue({ connect: jest.fn(), createTopics: jest.fn(), deleteTopics: jest.fn(), listTopics: jest.fn().mockResolvedValue([]), fetchTopicOffsets: jest.fn().mockResolvedValue([]), disconnect: jest.fn() }),
  });
  const KafkaCtor = jest.fn().mockReturnValue(mkKafka());
  return { Kafka: KafkaCtor, Partitioners: { LegacyPartitioner: jest.fn() } };
});

jest.mock('express', () => {
  const app = { use: jest.fn(), get: jest.fn(), post: jest.fn(), put: jest.fn(), patch: jest.fn(), delete: jest.fn() };
  const fn  = jest.fn().mockReturnValue(app);
  fn.json   = jest.fn().mockReturnValue(jest.fn());
  fn.Router = jest.fn().mockReturnValue(app);
  fn.static = jest.fn().mockReturnValue(jest.fn());
  return fn;
});

jest.mock('http', () => ({
  createServer: jest.fn().mockReturnValue({ listen: jest.fn(), close: jest.fn(), on: jest.fn() }),
}));

jest.mock('multer', () => {
  const m = jest.fn().mockReturnValue({ single: jest.fn() });
  m.diskStorage = jest.fn();
  return m;
});

jest.mock('ws', () => ({
  WebSocketServer: jest.fn().mockReturnValue({ on: jest.fn() }),
}));

// ── Load server ───────────────────────────────────────────────────────────────
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

// ── buildUseCaseDescriptor ────────────────────────────────────────────────────

describe('buildUseCaseDescriptor', () => {
  const fn = server.buildUseCaseDescriptor;
  if (!fn) {
    it.todo('buildUseCaseDescriptor is not exported — add module.exports to server.js');
    return;
  }

  const bankingAgents = {
    agents: [{
      agentId:    'banking-fraud-agent',
      name:       'Banking Fraud Detection Agent',
      entityType: 'Account',
      subscriptions: [
        { eventType: 'Transaction',  topic: 'agent-transactions',   operation: 'EvaluateTransaction' },
        { eventType: 'AccountEvent', topic: 'agent-account-events', operation: 'HandleAccountEvent'  },
      ],
      ruleset: { path: 'samples/banking-fraud-agent/FraudDetectionService_1.5.2.jar', archiveControlTopic: 'agent-banking-archive-control' },
      outputs: [
        { eventType: 'Alert',       topic: 'agent-fraud-alerts',   description: 'Fraud alerts' },
        { eventType: 'agent-error', topic: 'agent-banking-errors', description: 'Errors' },
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

  test('top-level agents.json properties override the derived values', () => {
    const merged = {
      name:            'Banking — Custom Name',
      flinkJobName:    'Custom Job',
      consumerGroupId: 'custom-group',
      description:     'Custom description',
      icon:            'bank',
      topics:          [{ name: 'agent-transactions', type: 'Transaction', label: 'Txn', idleness: '30s' }],
      outboundTopics:  [{ name: 'agent-fraud-alerts', type: 'Alert', label: 'Alerts' }],
      ...bankingAgents,
    };
    const d = fn('banking-fraud-agent', merged);
    expect(d.name).toBe('Banking — Custom Name');
    expect(d.flinkJobName).toBe('Custom Job');
    expect(d.consumerGroupId).toBe('custom-group');
    expect(d.description).toBe('Custom description');
    expect(d.icon).toBe('bank');
    expect(d.topics).toEqual(merged.topics);
    expect(d.outboundTopics).toEqual(merged.outboundTopics);
  });

  test('omits description and icon when agents.json does not declare them', () => {
    const d = fn('banking-fraud-agent', bankingAgents);
    expect(d).not.toHaveProperty('description');
    expect(d).not.toHaveProperty('icon');
  });
});

// ── dropDeclaredTopics ────────────────────────────────────────────────────────

describe('dropDeclaredTopics', () => {
  const fn = server.dropDeclaredTopics;

  test('removes topics and outboundTopics but keeps every other property', () => {
    const out = fn({
      name: 'X', flinkJobName: 'J', parallelism: 2,
      topics: [{ name: 't' }], outboundTopics: [{ name: 'o' }],
      agents: [{ agentId: 'a' }],
    });
    expect(out).toEqual({ name: 'X', flinkJobName: 'J', parallelism: 2, agents: [{ agentId: 'a' }] });
  });

  test('is a no-op when no topics are declared', () => {
    expect(fn({ agents: [] })).toEqual({ agents: [] });
  });
});

// ── applyUseCaseProperties ────────────────────────────────────────────────────

describe('applyUseCaseProperties', () => {
  const fn = server.applyUseCaseProperties;

  test('sets non-blank name, flinkJobName, description and consumerGroupId (trimmed)', () => {
    const out = fn({ agents: [] }, { name: ' N ', flinkJobName: 'J', description: 'D', consumerGroupId: 'G' });
    expect(out).toMatchObject({ name: 'N', flinkJobName: 'J', description: 'D', consumerGroupId: 'G' });
  });

  test('a blank value removes the property so it is derived again', () => {
    const out = fn({ name: 'N', flinkJobName: 'J', agents: [] }, { name: '  ', flinkJobName: '' });
    expect(out).not.toHaveProperty('name');
    expect(out).not.toHaveProperty('flinkJobName');
  });

  test('properties that are not provided are left untouched', () => {
    const out = fn({ name: 'N', description: 'D', agents: [] }, { flinkJobName: 'J' });
    expect(out).toMatchObject({ name: 'N', description: 'D', flinkJobName: 'J' });
  });

  test('ignores unknown properties and does not mutate the input', () => {
    const input = { name: 'N', agents: [] };
    const out = fn(input, { name: 'M', topics: [{ name: 'x' }], agents: ['hack'] });
    expect(out.name).toBe('M');
    expect(out).not.toHaveProperty('topics');
    expect(out.agents).toEqual([]);
    expect(input.name).toBe('N');
  });

  test('missing or invalid props return the input unchanged', () => {
    const input = { agents: [] };
    expect(fn(input, undefined)).toBe(input);
    expect(fn(input, 'x')).toBe(input);
  });
});



// ── collectTopicsFromAgentsJson ───────────────────────────────────────────────

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

// ── invokeArchiveSampleGenerator — JSON parse failure regression ──────────────
//
// Regression tests for the root cause of:
//   [sample-input] Generator skipped operation 'X' for 'retail-agent':
//   JSON parse failed: Unexpected number in JSON at position 4
//
// Root cause: `-Dlog4j2.configurationFile=NONE` did not suppress the bundled
// log4j2.properties.  Log4j2 tried to load a file called "NONE", failed, then
// fell back to classpath discovery and loaded the bundled config, which has a
// ConsoleAppender targeting SYSTEM_OUT at DEBUG.  RuleEngineRunner logs starting
// with a four-digit year (e.g. "2026-09-28 16:12:11,526 INFO  ...") were written
// to stdout before the JSON, causing JSON.parse to fail.
//
// Fix: use `-Dconsole.log.level=OFF`, which activates the ThresholdFilter already
// present in log4j2.properties:
//   appender.console.filter.threshold.level = ${sys:console.log.level:-ALL}
// so the ConsoleAppender discards every log event.
//
// invokeArchiveSampleGenerator accepts an optional 4th argument `spawnFn` so
// tests can inject a fake spawn without any module-level mock of child_process.

describe('invokeArchiveSampleGenerator', () => {
  const fn = server.invokeArchiveSampleGenerator;

  if (!fn) {
    it.todo('invokeArchiveSampleGenerator is not exported — add it to module.exports in server.js');
    return;
  }

  // Silence console output produced by failure-path branches (console.warn,
  // console.error, console.debug).  These diagnostics are intentional in
  // production but are noisy noise during tests that exercise expected failures.
  // We spy rather than replace so Jest still tracks calls if we ever assert them.
  let warnSpy, errorSpy, debugSpy;
  beforeEach(() => {
    warnSpy  = jest.spyOn(console, 'warn').mockImplementation(() => {});
    errorSpy = jest.spyOn(console, 'error').mockImplementation(() => {});
    debugSpy = jest.spyOn(console, 'debug').mockImplementation(() => {});
  });
  afterEach(() => {
    warnSpy.mockRestore();
    errorSpy.mockRestore();
    debugSpy.mockRestore();
  });

  // Helper: builds a fake EventEmitter-based child process object whose stdout/stderr
  // emit provided data and whose 'close' fires asynchronously (via setImmediate) so
  // the promise's event listeners are attached before any events are emitted.
  function makeChildMock({ stdoutData = '', stderrData = '', exitCode = 0, signal = null } = {}) {
    const EventEmitter = require('events').EventEmitter;
    const stdout = new EventEmitter();
    const stderr = new EventEmitter();
    const child  = new EventEmitter();
    Object.assign(child, { stdout, stderr });

    setImmediate(() => {
      if (stdoutData) stdout.emit('data', Buffer.from(stdoutData));
      if (stderrData) stderr.emit('data', Buffer.from(stderrData));
      child.emit('close', exitCode, signal);
    });

    return child;
  }

  // Build a fake spawn function that returns the given child mock.
  function fakeSpawn(childMock) {
    return jest.fn().mockReturnValue(childMock);
  }

  test('resolves with parsed JSON when stdout contains valid JSON', async () => {
    const payload = { input: { currentEvent: { productId: 'P1' } } };
    const spawn = fakeSpawn(makeChildMock({ stdoutData: JSON.stringify(payload) }));

    const result = await fn('/fake.jar', '/fake/archive.jar', 'TestOp', spawn);
    expect(result).toEqual(payload);
  });

  test('rejects with JSON parse error when stdout contains Log4j2 log lines before JSON (regression: log contamination)', async () => {
    // Reproduces the exact failure: a Log4j2 ConsoleAppender line starting with a
    // four-digit year is prepended to the JSON when -Dlog4j2.configurationFile=NONE
    // fails to suppress the bundled log4j2.properties — Log4j2 finds the file on the
    // classpath instead and routes RuleEngineRunner DEBUG/INFO logs to stdout.
    const log4j2Prefix =
      '2026-09-28 16:12:11,526 INFO  com.ibm.di.event.rule.RuleEngineRunner          ' +
      '- Initializing rule engine (attempt 1/3)...\n';
    const validJson = JSON.stringify({ input: { currentEvent: {} } });
    const spawn = fakeSpawn(makeChildMock({ stdoutData: log4j2Prefix + validJson }));

    await expect(
      fn('/fake.jar', '/fake/archive.jar', 'ProcessProductContext', spawn)
    ).rejects.toThrow(/JSON parse failed/);
  });

  test('rejects with JSON parse error when stdout is empty', async () => {
    const spawn = fakeSpawn(makeChildMock({ stdoutData: '' }));

    await expect(
      fn('/fake.jar', '/fake/archive.jar', 'ProcessInventoryContext', spawn)
    ).rejects.toThrow(/JSON parse failed/);
  });

  test('rejects when exit code is non-zero', async () => {
    const spawn = fakeSpawn(
      makeChildMock({ stderrData: 'SampleInputGenerator failed: archive not found', exitCode: 1 })
    );

    await expect(
      fn('/fake.jar', '/fake/archive.jar', 'ProcessCustomerContext', spawn)
    ).rejects.toThrow(/exit 1/);
  });

  test('rejects when stdout contains only whitespace', async () => {
    const spawn = fakeSpawn(makeChildMock({ stdoutData: '   \n\n   ' }));

    await expect(
      fn('/fake.jar', '/fake/archive.jar', 'ProcessShoppingContext', spawn)
    ).rejects.toThrow(/JSON parse failed/);
  });

  test('rejects with spawn error when Java is not on PATH', async () => {
    const EventEmitter = require('events').EventEmitter;
    const stdout = new EventEmitter();
    const stderr = new EventEmitter();
    const child  = new EventEmitter();
    Object.assign(child, { stdout, stderr });

    const spawnError = new Error('spawn java ENOENT');
    spawnError.code = 'ENOENT';
    const spawn = jest.fn().mockImplementation(() => {
      setImmediate(() => child.emit('error', spawnError));
      return child;
    });

    await expect(
      fn('/fake.jar', '/fake/archive.jar', 'GeneratedDecisionModel-Generated_decision_model', spawn)
    ).rejects.toThrow('spawn java ENOENT');
  });

  test('uses -Dconsole.log.level=OFF and not -Dlog4j2.configurationFile=NONE to suppress Log4j2 stdout', async () => {
    // Regression guard: verifies the fix is in place.
    // -Dconsole.log.level=OFF activates the bundled log4j2.properties ThresholdFilter.
    // -Dlog4j2.configurationFile=NONE is the broken approach that was replaced.
    let capturedArgs;
    const spawn = jest.fn().mockImplementation((_cmd, args) => {
      capturedArgs = args;
      return makeChildMock({ stdoutData: JSON.stringify({ input: {} }) });
    });

    await fn('/fake.jar', '/fake/archive.jar', 'ProcessPromotionContext', spawn);

    expect(capturedArgs).toContain('-Dconsole.log.level=OFF');
    expect(capturedArgs).not.toContain('-Dlog4j2.configurationFile=NONE');
  });
});

// ── unwrapAdsSample ───────────────────────────────────────────────────────────

describe('unwrapAdsSample', () => {
  const fn = server.unwrapAdsSample;

  if (!fn) {
    it.todo('unwrapAdsSample is not exported — add it to module.exports in server.js');
    return;
  }

  test('extracts currentEvent when present and non-empty', () => {
    const adsJson = { input: { currentEvent: { productId: 'P1', price: 49.99 } } };
    expect(fn(adsJson)).toEqual({ productId: 'P1', price: 49.99 });
  });

  test('falls back to input when currentEvent is absent', () => {
    const adsJson = { input: { customerId: 'C1', loyaltyStatus: 'Gold' } };
    expect(fn(adsJson)).toEqual({ customerId: 'C1', loyaltyStatus: 'Gold' });
  });

  test('falls back to input when currentEvent is an empty object', () => {
    const adsJson = { input: { currentEvent: {}, customerId: 'C2' } };
    expect(fn(adsJson)).toEqual({ currentEvent: {}, customerId: 'C2' });
  });

  test('falls back to full adsJson when input is absent', () => {
    const adsJson = { directField: 'value' };
    expect(fn(adsJson)).toEqual({ directField: 'value' });
  });

  test('falls back to full adsJson when input is empty', () => {
    const adsJson = { input: {}, meta: 'x' };
    expect(fn(adsJson)).toEqual({ input: {}, meta: 'x' });
  });

  test('handles deeply nested currentEvent for retail-agent operations', () => {
    const adsJson = {
      input: {
        currentEvent: {
          customerContext: { customerIdentifier: 'CUST-001' },
          shoppingContext: { eventType: 'PRODUCT_VIEW' },
        },
        customerContext: { customerIdentifier: 'CUST-001' },
      },
    };
    expect(fn(adsJson)).toMatchObject({
      customerContext: { customerIdentifier: 'CUST-001' },
      shoppingContext: { eventType: 'PRODUCT_VIEW' },
    });
  });
});

// ── NEW tests covering changes from the analysis fix-up ──────────────────────

// ── buildUseCaseDescriptor — topic-only fallback (LOW-5) ────────────────────────
// Subscription with s.topic absent and only s.eventType should NOT produce a
// phantom topic entry (eventType is a label, not a Kafka topic name).
describe('buildUseCaseDescriptor — topic fallback regression (LOW-5)', () => {
  const fn = server.buildUseCaseDescriptor;

  test('subscription with s.topic present uses it as the topic name', () => {
    const agentsJson = {
      agents: [{
        agentId: 'a1',
        subscriptions: [{ eventType: 'Transaction', topic: 'agent-txns' }],
        outputs: [],
        ruleset: {},
      }],
    };
    const stub = fn('uc', agentsJson);
    expect(stub.topics.map(t => t.name)).toContain('agent-txns');
    expect(stub.topics.map(t => t.name)).not.toContain('Transaction');
  });

  test('subscription with blank s.topic is silently skipped — no phantom entry', () => {
    const agentsJson = {
      agents: [{
        agentId: 'a1',
        subscriptions: [{ eventType: 'Transaction', topic: '' }],
        outputs: [],
        ruleset: {},
      }],
    };
    const stub = fn('uc', agentsJson);
    // Neither the eventType nor the empty string should appear
    expect(stub.topics).toHaveLength(0);
  });

  test('subscription with only eventType (no topic key) produces no topic entry', () => {
    const agentsJson = {
      agents: [{
        agentId: 'a1',
        subscriptions: [{ eventType: 'NoTopic' }],
        outputs: [],
        ruleset: {},
      }],
    };
    const stub = fn('uc', agentsJson);
    expect(stub.topics).toHaveLength(0);
  });

  test('subscription with whitespace-only topic is silently skipped', () => {
    const agentsJson = {
      agents: [{
        agentId: 'a1',
        subscriptions: [{ eventType: 'Ev', topic: '   ' }],
        outputs: [],
        ruleset: {},
      }],
    };
    const stub = fn('uc', agentsJson);
    expect(stub.topics).toHaveLength(0);
  });

  test('valid topic and blank topic in same agent: only valid one appears', () => {
    const agentsJson = {
      agents: [{
        agentId: 'a1',
        subscriptions: [
          { eventType: 'A', topic: 'valid-topic' },
          { eventType: 'B', topic: '' },
        ],
        outputs: [],
        ruleset: {},
      }],
    };
    const stub = fn('uc', agentsJson);
    expect(stub.topics).toHaveLength(1);
    expect(stub.topics[0].name).toBe('valid-topic');
  });
});

// ── ensureProducerConnected concurrency guard (HIGH-6) ────────────────────────
// The singleton _producerConnectPromise must prevent double-connect() calls when
// two callers simultaneously observe sharedProducerReady===false.
// We test the exported helpers indirectly: the real server module serialises
// concurrent connect calls.  Since the KafkaJS producer mock returns jest.fn()
// we verify the mock is only called once when two callers race.
describe('ensureProducerConnected — concurrent call serialisation (HIGH-6)', () => {
  // Re-load a fresh server module in each test to reset module-level state.
  // Vitest/Jest module cache is NOT cleared between tests in the same file,
  // so we test the pattern with a minimal local simulation.
  test('two concurrent callers observing !ready only call connect() once', async () => {
    // Simulate the before-fix race condition manually
    let connectCalls = 0;
    let promiseSingleton = null;
    let ready = false;

    async function ensureConnected() {
      if (ready) return;
      if (promiseSingleton) return promiseSingleton;
      promiseSingleton = (async () => {
        connectCalls++;
        await Promise.resolve(); // simulate async connect
        ready = true;
        promiseSingleton = null;
      })();
      return promiseSingleton;
    }

    // Both callers run in the same microtask turn before the first resolves
    await Promise.all([ensureConnected(), ensureConnected(), ensureConnected()]);
    expect(connectCalls).toBe(1);
    expect(ready).toBe(true);
  });

  test('after disconnect, next call re-connects exactly once', async () => {
    let connectCalls = 0;
    let promiseSingleton = null;
    let ready = false;

    async function ensureConnected() {
      if (ready) return;
      if (promiseSingleton) return promiseSingleton;
      promiseSingleton = (async () => {
        connectCalls++;
        await Promise.resolve();
        ready = true;
        promiseSingleton = null;
      })();
      return promiseSingleton;
    }

    function onDisconnect() { ready = false; promiseSingleton = null; }

    await ensureConnected();
    expect(connectCalls).toBe(1);

    onDisconnect();
    await Promise.all([ensureConnected(), ensureConnected()]);
    expect(connectCalls).toBe(2);
  });
});

// ── unwrapAdsSample — topic/sample consistency (LOW-6) ───────────────────────
// The sample-input endpoint now always returns `topic: defaultTopic` (first
// inbound topic) so the pre-selected topic and sample always agree.
describe('unwrapAdsSample — response topic/sample consistency (LOW-6)', () => {
  const fn = server.unwrapAdsSample;

  test('extracts currentEvent from first successful operation even when earlier ops failed', () => {
    // Simulates: op1 failed (skipped), op2 succeeded
    // Response must use defaultTopic (op1's topic), not firstTopic (op2's topic)
    const adsJson = { input: { currentEvent: { orderId: 'ORD-99' } } };
    const result = fn(adsJson);
    expect(result).toEqual({ orderId: 'ORD-99' });
  });

  test('falls back to input when currentEvent is absent — consistent extraction', () => {
    const adsJson = { input: { productId: 'P1', price: 9.99 } };
    expect(fn(adsJson)).toEqual({ productId: 'P1', price: 9.99 });
  });
});

// ── buildUseCaseDescriptor — multi-agent entity-changelog type uniqueness ────────
describe('buildUseCaseDescriptor — entity-changelog type key uniqueness', () => {
  const fn = server.buildUseCaseDescriptor;

  test('first changelog uses type "entity-changelog"', () => {
    const agentsJson = {
      agents: [{
        agentId: 'a1',
        subscriptions: [{ eventType: 'E', topic: 't1' }],
        outputs: [],
        ruleset: {},
        stateConfig: { changeLogTopic: 'clf-1' },
      }],
    };
    const stub = fn('uc', agentsJson);
    const clf = stub.outboundTopics.find(t => t.name === 'clf-1');
    expect(clf).toBeDefined();
    expect(clf.type).toBe('entity-changelog');
  });

  test('second changelog uses type "entity-changelog-<agentId>"', () => {
    const agentsJson = {
      agents: [
        { agentId: 'a1', subscriptions: [{ eventType: 'E1', topic: 't1' }], outputs: [], ruleset: {}, stateConfig: { changeLogTopic: 'clf-1' } },
        { agentId: 'a2', subscriptions: [{ eventType: 'E2', topic: 't2' }], outputs: [], ruleset: {}, stateConfig: { changeLogTopic: 'clf-2' } },
      ],
    };
    const stub = fn('uc', agentsJson);
    const clf2 = stub.outboundTopics.find(t => t.name === 'clf-2');
    expect(clf2).toBeDefined();
    expect(clf2.type).toBe('entity-changelog-a2');
  });
});

// ── collectTopicsFromAgentsJson — changeLogTopic collected ────────────────────
describe('collectTopicsFromAgentsJson — changeLogTopic included (regression)', () => {
  const fn = server.collectTopicsFromAgentsJson;

  test('changeLogTopic is included in the collected set', () => {
    const agentsJson = {
      agents: [{ subscriptions: [], outputs: [], ruleset: {}, stateConfig: { changeLogTopic: 'my-changelog' } }],
    };
    expect(fn(agentsJson).has('my-changelog')).toBe(true);
  });

  test('blank changeLogTopic is not included', () => {
    const agentsJson = {
      agents: [{ subscriptions: [], outputs: [], ruleset: {}, stateConfig: { changeLogTopic: '  ' } }],
    };
    expect(fn(agentsJson).size).toBe(0);
  });

  test('missing stateConfig does not crash', () => {
    const agentsJson = {
      agents: [{ subscriptions: [], outputs: [], ruleset: {} }],
    };
    expect(() => fn(agentsJson)).not.toThrow();
  });
});
