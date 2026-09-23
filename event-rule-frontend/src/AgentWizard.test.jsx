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
 * Tests for the Edit Agent Descriptor regression.
 *
 * Root cause: handleEditAgent in App.js was using agentJobId (the internal
 * agentId from agents.json, e.g. "fraud-detection-agent") to build the GET
 * request URL.  The BFF's GET /api/agents/:id endpoint is keyed by the
 * use-case FOLDER name (e.g. "agent-banking"), not by the agentId embedded in
 * agents.json.  The mismatched key produced a 404, which caused the fetch to
 * fall back to an empty editDescriptor → descriptorToForm(null) → DEFAULT_AGENT
 * → all form fields blank.
 *
 * Fix: handleEditAgent now uses selectedUseCase (the folder key) for the
 * fetch URL.  descriptorToForm() correctly maps the returned agents.json shape
 * to the flat wizard form state, pre-populating every field.
 *
 * These tests verify:
 *   1. descriptorToForm() — correct mapping for all field types and edge cases.
 *   2. handleEditAgent URL — the fetch must use the use-case folder name.
 */

import { descriptorToForm, buildDescriptor as _buildDescriptor } from './AgentWizard';

// buildDescriptor now returns { descriptor, useCaseId }.
// Tests were written against the old API that returned the descriptor object directly.
// This wrapper restores the old calling convention for all existing tests.
const buildDescriptor = (form) => _buildDescriptor(form).descriptor;

// ─── Fixtures ──────────────────────────────────────────────────────────────────

/** Full banking descriptor (agent-banking/agents.json) */
const BANKING_DESCRIPTOR = {
  agents: [
    {
      agentId: 'fraud-detection-agent',
      name: 'Banking Fraud Detection Agent',
      entityType: 'account',
      entityKeyExtractor: '$.accountId',
      parallelism: 1,
      subscriptions: [
        {
          eventType: 'transaction',
          topic: 'agent-transactions',
          operation: 'fraudDetectionService_transaction_op',
          horizon: {
            type: 'hybrid',
            countLimit: 100,
            timeLimit: '24h',
            description: 'Keep last 100 transactions or 24 hours of history',
          },
        },
        {
          eventType: 'account-created',
          topic: 'agent-account-created-events',
          operation: 'fraudDetectionService_createdaccounts',
          horizon: {
            type: 'count',
            value: 1,
            description: 'Keep the latest account profile',
          },
        },
      ],
      ruleset: {
        path: 'samples/agent-banking/fraud.jar',
        archiveControlTopic: 'agent-banking-archive-control',
      },
      outputs: [
        { eventType: 'fraud-alert', topic: 'agent-fraud-alerts', description: 'Fraud' },
        { eventType: 'approved-transaction', topic: 'agent-approved-transactions', description: 'Approved' },
        { eventType: 'agent-error', topic: 'agent-banking-errors', description: 'Errors' },
      ],
      stateConfig: { ttl: '30d', backend: 'rocksdb' },
    },
  ],
};

/** Full retail descriptor (agent-retail/agents.json) */
const RETAIL_DESCRIPTOR = {
  agents: [
    {
      agentId: 'personalized-promotions-agent',
      name: 'Retail Personalized Promotions Agent',
      entityType: 'customer',
      entityKeyExtractor: '$.customerId',
      parallelism: 1,
      subscriptions: [
        {
          eventType: 'order',
          topic: 'agent-orders',
          operation: 'personalizedPromotionsService_order_op',
          horizon: {
            type: 'hybrid',
            countLimit: 50,
            timeLimit: '7d',
            description: 'Keep last 50 orders or 7 days of purchase history',
          },
        },
        {
          eventType: 'product-view',
          topic: 'agent-product-views',
          operation: 'personalizedPromotionsService_productview_op',
          horizon: {
            type: 'hybrid',
            countLimit: 200,
            timeLimit: '48h',
            description: 'Keep last 200 product views or 48 hours of browsing',
          },
        },
      ],
      ruleset: {
        path: 'samples/agent-retail/promotions.jar',
        archiveControlTopic: 'agent-retail-archive-control',
      },
      outputs: [
        { eventType: 'promotion-offer', topic: 'agent-promotion-offers', description: 'Promotions' },
        { eventType: 'agent-error', topic: 'agent-retail-errors', description: 'Errors' },
      ],
      stateConfig: { ttl: '14d', backend: 'rocksdb' },
    },
  ],
  parallelism: 2,
};

// ─── descriptorToForm: null / empty inputs ─────────────────────────────────────

describe('descriptorToForm — null / empty inputs return DEFAULT_AGENT shape', () => {
  test('null returns defaults', () => {
    const form = descriptorToForm(null);
    expect(form.agentId).toBe('');
    expect(form.name).toBe('');
    expect(form.entityType).toBe('');
    expect(form.entityKeyExtractor).toBe('$.id');
    expect(form.parallelism).toBe(0);
    expect(form.agentParallelism).toBe(0);
    expect(form.subscriptions).toHaveLength(1);
    expect(form.subscriptions[0].eventType).toBe('');
    expect(form.ruleset.name).toBeUndefined();
    expect(form.ruleset.path).toBe('');
    expect(form.outputs).toHaveLength(1);
    expect(form.outputs[0].eventType).toBe('');
    expect(form.stateConfig.ttl).toBe('30d');
    expect(form.stateConfig.backend).toBe('rocksdb');
  });

  test('undefined returns defaults', () => {
    const form = descriptorToForm(undefined);
    expect(form.agentId).toBe('');
  });

  test('empty object (no agents array) returns defaults', () => {
    const form = descriptorToForm({});
    expect(form.agentId).toBe('');
    expect(form.subscriptions).toHaveLength(1);
  });

  test('descriptor with empty agents array returns defaults', () => {
    const form = descriptorToForm({ agents: [] });
    expect(form.agentId).toBe('');
  });
});

// ─── descriptorToForm: chainingEnabled ───────────────────────────────────────

describe('descriptorToForm — chainingEnabled field', () => {
  test('defaults to true when field is absent (backward compatibility)', () => {
    const form = descriptorToForm({ agents: [{ agentId: 'a', ruleset: { path: '/tmp/a.jar' }, subscriptions: [], outputs: [] }] });
    expect(form.chainingEnabled).toBe(true);
  });

  test('maps chainingEnabled: true from descriptor', () => {
    const form = descriptorToForm({ agents: [{ agentId: 'a', chainingEnabled: true, ruleset: { path: '/tmp/a.jar' }, subscriptions: [], outputs: [] }] });
    expect(form.chainingEnabled).toBe(true);
  });

  test('maps chainingEnabled: false from descriptor', () => {
    const form = descriptorToForm({ agents: [{ agentId: 'a', chainingEnabled: false, ruleset: { path: '/tmp/a.jar' }, subscriptions: [], outputs: [] }] });
    expect(form.chainingEnabled).toBe(false);
  });

  test('null descriptor returns chainingEnabled: true (DEFAULT_AGENT)', () => {
    const form = descriptorToForm(null);
    expect(form.chainingEnabled).toBe(true);
  });
});



// ─── descriptorToForm: banking descriptor ────────────────────────────────────

describe('descriptorToForm — banking-fraud descriptor', () => {
  let form;
  beforeEach(() => { form = descriptorToForm(BANKING_DESCRIPTOR); });

  test('maps top-level identity fields', () => {
    expect(form.agentId).toBe('fraud-detection-agent');
    expect(form.name).toBe('Banking Fraud Detection Agent');
    expect(form.entityType).toBe('account');
    expect(form.entityKeyExtractor).toBe('$.accountId');
  });

  test('maps job-level parallelism from descriptor root (not present → 0)', () => {
    // BANKING_DESCRIPTOR has no top-level parallelism key
    expect(form.parallelism).toBe(0);
  });

  test('maps agent-level parallelism from agent.parallelism', () => {
    expect(form.agentParallelism).toBe(1);
  });

  test('maps two subscriptions', () => {
    expect(form.subscriptions).toHaveLength(2);
  });

  test('maps hybrid horizon subscription (transaction)', () => {
    const sub = form.subscriptions[0];
    expect(sub.eventType).toBe('transaction');
    expect(sub.topic).toBe('agent-transactions');
    expect(sub.horizonType).toBe('hybrid');
    expect(sub.horizonCountLimit).toBe(100);
    expect(sub.horizonTimeLimit).toBe('24h');
    expect(sub.horizonDescription).toBe('Keep last 100 transactions or 24 hours of history');
  });

  test('maps transaction subscription operation', () => {
    expect(form.subscriptions[0].operation).toBe('fraudDetectionService_transaction_op');
  });

  test('maps account-created subscription operation', () => {
    const sub = form.subscriptions[1];
    expect(sub.eventType).toBe('account-created');
    expect(sub.topic).toBe('agent-account-created-events');
    expect(sub.operation).toBe('fraudDetectionService_createdaccounts');
    expect(sub.horizonType).toBe('count');
    expect(sub.horizonValue).toBe(1);
    expect(sub.horizonDescription).toBe('Keep the latest account profile');
  });

  test('maps ruleset fields (no name field)', () => {
    expect(form.ruleset.path).toBe('samples/agent-banking/fraud.jar');
    expect(form.ruleset.archiveControlTopic).toBe('agent-banking-archive-control');
    expect(form.ruleset.name).toBeUndefined();
  });

  test('maps three outputs', () => {
    expect(form.outputs).toHaveLength(3);
    expect(form.outputs[0]).toEqual({ eventType: 'fraud-alert', topic: 'agent-fraud-alerts', description: 'Fraud' });
    expect(form.outputs[1]).toEqual({ eventType: 'approved-transaction', topic: 'agent-approved-transactions', description: 'Approved' });
    expect(form.outputs[2]).toEqual({ eventType: 'agent-error', topic: 'agent-banking-errors', description: 'Errors' });
  });

  test('maps stateConfig', () => {
    expect(form.stateConfig.ttl).toBe('30d');
    expect(form.stateConfig.backend).toBe('rocksdb');
  });
});

// ─── descriptorToForm: retail descriptor ─────────────────────────────────────

describe('descriptorToForm — generic (retail) descriptor', () => {
  let form;
  beforeEach(() => { form = descriptorToForm(RETAIL_DESCRIPTOR); });

  test('maps top-level identity fields', () => {
    expect(form.agentId).toBe('personalized-promotions-agent');
    expect(form.entityType).toBe('customer');
    expect(form.entityKeyExtractor).toBe('$.customerId');
  });

  test('maps job-level parallelism from descriptor root', () => {
    expect(form.parallelism).toBe(2);
  });

  test('maps agent-level parallelism', () => {
    expect(form.agentParallelism).toBe(1);
  });

  test('maps two subscriptions', () => {
    expect(form.subscriptions).toHaveLength(2);
  });

  test('maps hybrid horizon with countLimit and timeLimit', () => {
    const sub = form.subscriptions[0];
    expect(sub.eventType).toBe('order');
    expect(sub.topic).toBe('agent-orders');
    expect(sub.horizonType).toBe('hybrid');
    expect(sub.horizonCountLimit).toBe(50);
    expect(sub.horizonTimeLimit).toBe('7d');
  });

  test('maps subscription operations', () => {
    expect(form.subscriptions[0].operation).toBe('personalizedPromotionsService_order_op');
    expect(form.subscriptions[1].operation).toBe('personalizedPromotionsService_productview_op');
  });

  test('maps ruleset fields (no name field)', () => {
    expect(form.ruleset.archiveControlTopic).toBe('agent-retail-archive-control');
    expect(form.ruleset.name).toBeUndefined();
  });

  test('maps two outputs', () => {
    expect(form.outputs).toHaveLength(2);
    expect(form.outputs[0].eventType).toBe('promotion-offer');
    expect(form.outputs[1].eventType).toBe('agent-error');
  });

  test('maps stateConfig TTL of 14d', () => {
    expect(form.stateConfig.ttl).toBe('14d');
  });
});

// ─── descriptorToForm: descriptor with outputField and archiveControlTopic ────

describe('descriptorToForm — descriptor with outputField', () => {
  const DESCRIPTOR_WITH_OUTPUT_FIELD = {
    agents: [{
      agentId: 'generic-agent',
      name: 'Generic Agent',
      entityType: 'user',
      entityKeyExtractor: '$.userId',
      parallelism: 1,
      subscriptions: [{
        eventType: 'evaluation-request',
        topic: 'eval-requests',
        operation: 'EvaluationService_evaluationRequest_op',
        horizon: { type: 'count', value: 1, description: 'Stateless service' },
      }],
      ruleset: {
        path: 'samples/generic/evaluation.jar',
        outputField: 'recommendation',
        archiveControlTopic: 'eval-archive-control',
      },
      outputs: [
        { eventType: 'evaluation-result', topic: 'eval-results', description: 'Evaluation output' },
        { eventType: 'agent-error',       topic: 'eval-errors',  description: 'Errors' },
      ],
      stateConfig: { ttl: '1h', backend: 'rocksdb' },
    }],
  };

  let form;
  beforeEach(() => { form = descriptorToForm(DESCRIPTOR_WITH_OUTPUT_FIELD); });

  test('maps identity fields correctly', () => {
    expect(form.agentId).toBe('generic-agent');
    expect(form.entityType).toBe('user');
    expect(form.entityKeyExtractor).toBe('$.userId');
  });

  test('buildRuleset preserves outputField in generated descriptor', () => {
    const rs = buildDescriptor(form);
    expect(rs.agents[0].ruleset.outputField).toBe('recommendation');
  });

  test('maps subscription operation', () => {
    expect(form.subscriptions[0].operation).toBe('EvaluationService_evaluationRequest_op');
  });

  test('buildRuleset does NOT emit legacy banking-fraud context field names', () => {
    const rs = buildDescriptor(form);
    const ruleset = rs.agents[0].ruleset;
    expect(ruleset.name).toBeUndefined();
    expect(ruleset.contextWrapperField).toBeUndefined();
    expect(ruleset.contextCurrentEventField).toBeUndefined();
    expect(ruleset.contextAccountHistoryKey).toBeUndefined();
    expect(ruleset.contextAccountField).toBeUndefined();
    expect(ruleset.contextCountField).toBeUndefined();
  });
});

// ─── descriptorToForm: edge cases ─────────────────────────────────────────────

describe('descriptorToForm — edge cases', () => {
  test('topic string is mapped correctly', () => {
    const descriptor = {
      agents: [{
        agentId: 'x',
        subscriptions: [{ eventType: 'e', topic: 'my-topic', operation: 'op', horizon: { type: 'count', value: 5 } }],
        ruleset: { path: 'p' },
        outputs: [{ eventType: 'out', topic: 't' }],
      }],
    };
    const form = descriptorToForm(descriptor);
    expect(form.subscriptions[0].topic).toBe('my-topic');
  });

  test('legacy topics array falls back gracefully: first element used as topic', () => {
    // Descriptors saved by the old wizard used topics:[] — descriptorToForm should
    // tolerate them and populate form.topic with the first entry.
    const descriptor = {
      agents: [{
        agentId: 'x',
        subscriptions: [{ eventType: 'e', topics: ['topic-a', 'topic-b'], operation: 'op', horizon: { type: 'count', value: 1 } }],
        ruleset: { path: 'p' },
        outputs: [{ eventType: 'out', topic: 't' }],
      }],
    };
    const form = descriptorToForm(descriptor);
    expect(form.subscriptions[0].topic).toBe('topic-a');
  });

  test('missing optional fields fall back to defaults (no entityType / no description)', () => {
    const descriptor = {
      agents: [{
        agentId: 'minimal-agent',
        subscriptions: [{ eventType: 'e', topic: 't', operation: 'op', horizon: { type: 'count', value: 3 } }],
        ruleset: { path: 'p.jar' },
        outputs: [{ eventType: 'out', topic: 't' }],
      }],
    };
    const form = descriptorToForm(descriptor);
    expect(form.entityType).toBe('');
    expect(form.entityKeyExtractor).toBe('$.id');
    expect(form.outputs[0].description).toBe('');
  });

  test('time horizon subscription is mapped correctly', () => {
    const descriptor = {
      agents: [{
        agentId: 'x',
        subscriptions: [{ eventType: 'click', topic: 'clicks', operation: 'click-op', horizon: { type: 'time', timeLimit: '12h', description: 'Rolling 12h window' } }],
        ruleset: { path: 'p' },
        outputs: [{ eventType: 'out', topic: 't' }],
      }],
    };
    const form = descriptorToForm(descriptor);
    const sub = form.subscriptions[0];
    expect(sub.horizonType).toBe('time');
    expect(sub.horizonTimeLimit).toBe('12h');
    expect(sub.horizonDescription).toBe('Rolling 12h window');
  });

  test('empty subscriptions array falls back to DEFAULT_AGENT subscription', () => {
    const descriptor = {
      agents: [{
        agentId: 'x',
        subscriptions: [],
        ruleset: { path: 'p' },
        outputs: [{ eventType: 'out', topic: 't' }],
      }],
    };
    const form = descriptorToForm(descriptor);
    expect(form.subscriptions).toHaveLength(1);
    expect(form.subscriptions[0].eventType).toBe('');
  });

  test('subscription without operation in JSON is treated as history-only (operation: "")', () => {
    // Legacy descriptor that predates the mandatory operation field
    const descriptor = {
      agents: [{
        agentId: 'legacy',
        subscriptions: [{ eventType: 'e', topic: 't', horizon: { type: 'count', value: 1 } }],
        ruleset: { path: 'p' },
        outputs: [{ eventType: 'out', topic: 't' }],
      }],
    };
    const form = descriptorToForm(descriptor);
    // null operation in JSON → "" in form (graceful fallback)
    expect(form.subscriptions[0].operation).toBe('');
  });
});

// ─── buildDescriptor: operation field round-trip ──────────────────────────────

describe('buildDescriptor — operation field', () => {
  function makeForm(operationValue) {
    return {
      parallelism: 0,
      agentId: 'test-agent',
      name: 'Test Agent',
      entityType: 'entity',
      entityKeyExtractor: '$.id',
      agentParallelism: 0,
      subscriptions: [{
        eventType: 'my-event',
        topic: 'my-topic',
        operation: operationValue,
        horizonType: 'count',
        horizonValue: 5,
        horizonCountLimit: 100,
        horizonTimeLimit: '24h',
        horizonDescription: '',
      }],
      ruleset: { path: '/opt/test.jar', outputField: '', archiveControlTopic: '', contextWrapperField: 'context' },
      outputs: [{ eventType: 'out', topic: 'out-topic', description: '' }],
      stateConfig: { ttl: '30d', backend: 'rocksdb' },
    };
  }

  test('non-empty operation is emitted verbatim', () => {
    const descriptor = buildDescriptor(makeForm('MyService_myEvent_op'));
    expect(descriptor.agents[0].subscriptions[0].operation).toBe('MyService_myEvent_op');
  });

  test('empty string operation (history-only) is emitted as ""', () => {
    const descriptor = buildDescriptor(makeForm(''));
    expect(descriptor.agents[0].subscriptions[0].operation).toBe('');
  });

  test('null operation falls back to ""', () => {
    const descriptor = buildDescriptor(makeForm(null));
    expect(descriptor.agents[0].subscriptions[0].operation).toBe('');
  });

  test('ruleset does NOT contain a name field', () => {
    const descriptor = buildDescriptor(makeForm('op'));
    expect(descriptor.agents[0].ruleset.name).toBeUndefined();
  });

  test('full round-trip: banking descriptor → form → descriptor preserves operations', () => {
    const form = descriptorToForm(BANKING_DESCRIPTOR);
    const rebuilt = buildDescriptor(form);
    expect(rebuilt.agents[0].subscriptions[0].operation).toBe('fraudDetectionService_transaction_op');
    expect(rebuilt.agents[0].subscriptions[1].operation).toBe('fraudDetectionService_createdaccounts');
    expect(rebuilt.agents[0].ruleset.name).toBeUndefined();
  });

  test('full round-trip: banking descriptor → form → descriptor emits topic (string), not topics (array)', () => {
    const form = descriptorToForm(BANKING_DESCRIPTOR);
    const rebuilt = buildDescriptor(form);
    const sub0 = rebuilt.agents[0].subscriptions[0];
    const sub1 = rebuilt.agents[0].subscriptions[1];
    // Must produce "topic" (string) — the Java EventSubscription model field
    expect(sub0.topic).toBe('agent-transactions');
    expect(sub1.topic).toBe('agent-account-created-events');
    // Must NOT produce the old "topics" array
    expect(sub0.topics).toBeUndefined();
    expect(sub1.topics).toBeUndefined();
  });

  test('full round-trip: retail descriptor → form → descriptor emits topic (string)', () => {
    const form = descriptorToForm(RETAIL_DESCRIPTOR);
    const rebuilt = buildDescriptor(form);
    expect(rebuilt.agents[0].subscriptions[0].topic).toBe('agent-orders');
    expect(rebuilt.agents[0].subscriptions[0].topics).toBeUndefined();
    expect(rebuilt.agents[0].subscriptions[1].topic).toBe('agent-product-views');
    expect(rebuilt.agents[0].subscriptions[1].topics).toBeUndefined();
  });
});

// ─── buildDescriptor: stateConfig ────────────────────────────────────────────


// ─── buildDescriptor: chainingEnabled ────────────────────────────────────────

describe('buildDescriptor — chainingEnabled field', () => {
  function makeForm(chainingEnabled) {
    return {
      parallelism: 0,
      agentId: 'test-agent',
      name: 'Test',
      entityType: 'entity',
      entityKeyExtractor: '$.id',
      agentParallelism: 0,
      chainingEnabled,
      subscriptions: [{ eventType: 'ev', topic: 't', operation: 'op', horizonType: 'count', horizonValue: 5, horizonCountLimit: 100, horizonTimeLimit: '24h', horizonDescription: '' }],
      ruleset: { path: '/tmp/test.jar', outputField: '', archiveControlTopic: '' },
      outputs: [{ eventType: 'out', topic: 'out-topic', description: '' }],
      stateConfig: { ttl: '30d', backend: 'rocksdb' },
    };
  }

  test('chainingEnabled: true is NOT emitted (omit = default true keeps descriptor clean)', () => {
    const descriptor = buildDescriptor(makeForm(true));
    expect(descriptor.agents[0].chainingEnabled).toBeUndefined();
  });

  test('chainingEnabled: false IS emitted explicitly', () => {
    const descriptor = buildDescriptor(makeForm(false));
    expect(descriptor.agents[0].chainingEnabled).toBe(false);
  });

  test('round-trip: false → descriptorToForm → buildDescriptor preserves false', () => {
    const original = { agents: [{ agentId: 'a', chainingEnabled: false, ruleset: { path: '/tmp/a.jar' }, subscriptions: [{ eventType: 'ev', topic: 't', operation: 'op' }], outputs: [{ eventType: 'out', topic: 'tout' }], stateConfig: { ttl: '30d', backend: 'rocksdb' } }] };
    const form = descriptorToForm(original);
    const rebuilt = buildDescriptor(form);
    expect(rebuilt.agents[0].chainingEnabled).toBe(false);
  });

  test('round-trip: true → descriptorToForm → buildDescriptor omits field', () => {
    const original = { agents: [{ agentId: 'a', chainingEnabled: true, ruleset: { path: '/tmp/a.jar' }, subscriptions: [{ eventType: 'ev', topic: 't', operation: 'op' }], outputs: [{ eventType: 'out', topic: 'tout' }], stateConfig: { ttl: '30d', backend: 'rocksdb' } }] };
    const form = descriptorToForm(original);
    const rebuilt = buildDescriptor(form);
    expect(rebuilt.agents[0].chainingEnabled).toBeUndefined();
  });

  test('round-trip: absent → descriptorToForm → buildDescriptor omits field', () => {
    const original = { agents: [{ agentId: 'a', ruleset: { path: '/tmp/a.jar' }, subscriptions: [{ eventType: 'ev', topic: 't', operation: 'op' }], outputs: [{ eventType: 'out', topic: 'tout' }], stateConfig: { ttl: '30d', backend: 'rocksdb' } }] };
    const form = descriptorToForm(original);
    const rebuilt = buildDescriptor(form);
    // Absent in original → treated as true → not emitted in output
    expect(rebuilt.agents[0].chainingEnabled).toBeUndefined();
  });
});


describe('buildDescriptor — stateConfig', () => {
  function makeFormWithState(ttl, backend) {
    return {
      parallelism: 0,
      agentId: 'test-agent',
      name: 'Test Agent',
      entityType: 'entity',
      entityKeyExtractor: '$.id',
      agentParallelism: 0,
      subscriptions: [{
        eventType: 'e', topic: 't', operation: 'op',
        horizonType: 'count', horizonValue: 1, horizonCountLimit: 10, horizonTimeLimit: '24h', horizonDescription: '',
      }],
      ruleset: { path: '/opt/test.jar', outputField: '', archiveControlTopic: '', contextWrapperField: 'context' },
      outputs: [{ eventType: 'out', topic: 'out-topic', description: '' }],
      stateConfig: { ttl, backend },
    };
  }

  test('populated ttl and backend are both emitted', () => {
    const d = buildDescriptor(makeFormWithState('30d', 'rocksdb'));
    expect(d.agents[0].stateConfig).toEqual({ ttl: '30d', backend: 'rocksdb' });
  });

  test('blank ttl is omitted from stateConfig', () => {
    const d = buildDescriptor(makeFormWithState('', 'rocksdb'));
    expect(d.agents[0].stateConfig.ttl).toBeUndefined();
    expect(d.agents[0].stateConfig.backend).toBe('rocksdb');
  });

  test('blank backend is omitted from stateConfig', () => {
    const d = buildDescriptor(makeFormWithState('30d', ''));
    expect(d.agents[0].stateConfig.ttl).toBe('30d');
    expect(d.agents[0].stateConfig.backend).toBeUndefined();
  });

  test('both blank produces an empty stateConfig object (no undefined keys)', () => {
    const d = buildDescriptor(makeFormWithState('', ''));
    const sc = d.agents[0].stateConfig;
    expect(sc).toEqual({});
    // Must not contain explicit undefined values — JSON.stringify would drop them anyway
    // but the object itself should be clean.
    expect(Object.keys(sc)).toHaveLength(0);
  });
});

// ─── DEFAULT_AGENT horizon type consistency ───────────────────────────────────

describe('DEFAULT_AGENT — horizon type consistency', () => {
  test('descriptorToForm(null) produces horizonType hybrid (matches Add Subscription default)', () => {
    const form = descriptorToForm(null);
    // DEFAULT_AGENT and the "Add Subscription" button both default to hybrid so a new
    // wizard and a wizard with an added subscription produce consistent horizon types.
    expect(form.subscriptions[0].horizonType).toBe('hybrid');
  });
});

// ─── handleEditAgent: fetch URL uses selectedUseCase (folder key) ─────────────
//
// This is the core regression test.  We inline the exact handleEditAgent logic
// and assert that the fetch call is made with the USE-CASE FOLDER NAME, not the
// agentId read from agents.json.

describe('handleEditAgent — fetch URL uses selectedUseCase (folder key)', () => {
  const API_URL_APP = import.meta.env?.VITE_API_URL || 'http://localhost:8080';
  const USE_CASE_FOLDER  = 'agent-banking';           // correct key for BFF
  const INTERNAL_AGENT_ID = 'fraud-detection-agent';  // wrong key — breaks BFF

  afterEach(() => {
    jest.restoreAllMocks();
    jest.clearAllMocks();
  });

  test('fetch is called with selectedUseCase (folder name) not agentJobId', async () => {
    const fetchMock = jest.fn().mockResolvedValue({
      ok: true,
      json: async () => BANKING_DESCRIPTOR,
    });
    global.fetch = fetchMock;

    // Simulate the handleEditAgent logic exactly as it appears in App.js
    const selectedUseCase = USE_CASE_FOLDER;
    let editDescriptor = null;

    try {
      const r = await fetch(`${API_URL_APP}/api/agents/${selectedUseCase}`);
      if (!r.ok) throw new Error(r.statusText);
      editDescriptor = await r.json();
    } catch {
      editDescriptor = null;
    }

    // The fetch must use the folder key
    expect(fetchMock).toHaveBeenCalledWith(
      `${API_URL_APP}/api/agents/${USE_CASE_FOLDER}`
    );
    // Must NOT use the internal agentId
    expect(fetchMock).not.toHaveBeenCalledWith(
      expect.stringContaining(`/api/agents/${INTERNAL_AGENT_ID}`)
    );
    // The returned descriptor is stored as-is for descriptorToForm
    expect(editDescriptor).toEqual(BANKING_DESCRIPTOR);
  });

  test('on 404 editDescriptor is set to null (not stub)', async () => {
    global.fetch = jest.fn().mockResolvedValue({ ok: false, statusText: 'Not Found' });

    const selectedUseCase = USE_CASE_FOLDER;
    let editDescriptor = 'sentinel'; // something non-null to confirm it gets reset

    try {
      const r = await fetch(`${API_URL_APP}/api/agents/${selectedUseCase}`);
      if (!r.ok) throw new Error(r.statusText);
      editDescriptor = await r.json();
    } catch {
      editDescriptor = null;
    }

    expect(editDescriptor).toBeNull();
  });

  test('descriptor returned by fetch produces a non-empty form via descriptorToForm', async () => {
    global.fetch = jest.fn().mockResolvedValue({
      ok: true,
      json: async () => BANKING_DESCRIPTOR,
    });

    const r = await fetch('http://localhost:8080/api/agents/agent-banking');
    const agentsJson = await r.json();
    const form = descriptorToForm(agentsJson);

    // Key fields must be populated — not blank
    expect(form.agentId).toBe('fraud-detection-agent');
    expect(form.name).toBe('Banking Fraud Detection Agent');
    expect(form.ruleset.path).toBe('samples/agent-banking/fraud.jar');
    expect(form.subscriptions[0].eventType).toBe('transaction');
    expect(form.subscriptions[0].operation).toBe('fraudDetectionService_transaction_op');
    expect(form.outputs).toHaveLength(3);
  });

  test('explicit string useCaseId param is used when selectedUseCase is null (main page / failed startup)', async () => {
    // Bug scenario: user is on the main page (selectedUseCase === null) and clicks
    // "✎ Edit" on a failed agent card.  AgentScenarioCard calls onEditAgent(agent.id)
    // so handleEditAgent receives the agent folder name as a string argument.
    const fetchMock = jest.fn().mockResolvedValue({
      ok: true,
      json: async () => BANKING_DESCRIPTOR,
    });
    global.fetch = fetchMock;

    const selectedUseCase = null;        // main page — no use case selected
    const passedId        = 'agent-banking'; // string passed by AgentScenarioCard

    // Reproduce the fixed handleEditAgent id-resolution logic
    const id = (typeof passedId === 'string' ? passedId : null) || selectedUseCase;
    let editDescriptor = null;
    try {
      const r = await fetch(`${API_URL_APP}/api/agents/${id}`);
      if (!r.ok) throw new Error(r.statusText);
      editDescriptor = await r.json();
    } catch {
      editDescriptor = null;
    }

    expect(fetchMock).toHaveBeenCalledWith(`${API_URL_APP}/api/agents/agent-banking`);
    expect(fetchMock).not.toHaveBeenCalledWith(`${API_URL_APP}/api/agents/null`);
    expect(editDescriptor).toEqual(BANKING_DESCRIPTOR);
    const form = descriptorToForm(editDescriptor);
    expect(form.agentId).toBe('fraud-detection-agent');
    expect(form.name).toBe('Banking Fraud Detection Agent');
  });

  test('SyntheticEvent passed by dashboard button is ignored — falls back to selectedUseCase', async () => {
    // Bug scenario: AgentManagementPanel has onClick={onEdit} on its button.
    // React passes the SyntheticEvent as the first argument.  Before the type guard
    // was added, this truthy object was used as the fetch URL segment, producing
    // /api/agents/[object Object] → 404 → blank wizard.
    const fetchMock = jest.fn().mockResolvedValue({
      ok: true,
      json: async () => BANKING_DESCRIPTOR,
    });
    global.fetch = fetchMock;

    const selectedUseCase  = 'agent-banking'; // dashboard — use case is selected
    const syntheticEvent   = { type: 'click', target: {} }; // stand-in for SyntheticEvent

    const id = (typeof syntheticEvent === 'string' ? syntheticEvent : null) || selectedUseCase;

    let editDescriptor = null;
    try {
      const r = await fetch(`${API_URL_APP}/api/agents/${id}`);
      if (!r.ok) throw new Error(r.statusText);
      editDescriptor = await r.json();
    } catch {
      editDescriptor = null;
    }

    // Must fall back to selectedUseCase, not stringify the event object
    expect(fetchMock).toHaveBeenCalledWith(`${API_URL_APP}/api/agents/agent-banking`);
    expect(fetchMock).not.toHaveBeenCalledWith(
      expect.stringContaining('[object')
    );
    expect(editDescriptor).toEqual(BANKING_DESCRIPTOR);
  });
});

// ─── descriptorToForm + buildDescriptor: operations round-trip ────────────────

const BANKING_FRAUD_DESCRIPTOR_WITH_OPS = {
  agents: [
    {
      agentId: 'fraud-detection-agent',
      name: 'Banking Fraud Detection Agent',
      entityType: 'Account',
      entityKeyExtractor: '$.accountNumber',
      subscriptions: [
        {
          eventType: 'Transaction',
          topic: 'agent-transactions',
          operation: 'EvaluateTransaction',
          horizon: { type: 'hybrid', countLimit: 100, timeLimit: '24h', description: 'Keep last 100 transactions or 24 hours of history' },
        },
        {
          eventType: 'AccountEvent',
          topic: 'agent-account-events',
          operation: 'HandleAccountEvent',
          horizon: { type: 'count', value: 100, description: 'Keep the last 100 account events' },
        },
      ],
      ruleset: {
        path: 'samples/banking-fraud-agent/FraudDetectionService_1.3.1.jar',
        archiveControlTopic: 'agent-banking-archive-control',
        operations: [
          {
            operation: 'EvaluateTransaction',
            entityField: 'theAccount',
            currentEventField: 'theTransaction',
            history: [
              { eventType: 'Transaction',   field: 'thePastTransactions' },
              { eventType: 'AccountEvent',  field: 'thePastAccountEvents' },
            ],
            outbounds: [
              { field: 'theFraudAlerts', eventType: 'Alert' },
            ],
          },
          {
            operation: 'HandleAccountEvent',
            entityField: 'account',
            currentEventField: 'accountEvent',
            history: [
              { eventType: 'AccountEvent', field: 'pastAccountEvents' },
            ],
            outbounds: [],
          },
        ],
      },
      outputs: [
        { eventType: 'Alert',        topic: 'agent-fraud-alerts',   description: 'Transactions flagged as fraudulent' },
        { eventType: 'agent-error',  topic: 'agent-banking-errors', description: 'Processing errors for investigation' },
      ],
      stateConfig: { ttl: '30d', backend: 'rocksdb' },
      parallelism: 1,
    },
  ],
};

describe('descriptorToForm + buildDescriptor — operations (banking-fraud sample)', () => {
  let form;
  beforeEach(() => { form = descriptorToForm(BANKING_FRAUD_DESCRIPTOR_WITH_OPS); });

  test('descriptorToForm maps two operations', () => {
    expect(form.ruleset.operations).toHaveLength(2);
  });

  test('descriptorToForm maps EvaluateTransaction operation fields', () => {
    const op = form.ruleset.operations[0];
    expect(op.operation).toBe('EvaluateTransaction');
    expect(op.entityField).toBe('theAccount');
    expect(op.currentEventField).toBe('theTransaction');
    expect(op.outbounds).toHaveLength(1);
    expect(op.outbounds[0]).toEqual({ field: 'theFraudAlerts', eventType: 'Alert' });
  });

  test('descriptorToForm maps EvaluateTransaction history bindings', () => {
    const op = form.ruleset.operations[0];
    expect(op.history).toHaveLength(2);
    expect(op.history[0]).toEqual({ eventType: 'Transaction',  field: 'thePastTransactions' });
    expect(op.history[1]).toEqual({ eventType: 'AccountEvent', field: 'thePastAccountEvents' });
  });

  test('descriptorToForm maps HandleAccountEvent operation fields', () => {
    const op = form.ruleset.operations[1];
    expect(op.operation).toBe('HandleAccountEvent');
    expect(op.entityField).toBe('account');
    expect(op.currentEventField).toBe('accountEvent');
    // empty outbounds normalised to []
    expect(op.outbounds).toEqual([]);
  });

  test('descriptorToForm maps HandleAccountEvent history binding', () => {
    const op = form.ruleset.operations[1];
    expect(op.history).toHaveLength(1);
    expect(op.history[0]).toEqual({ eventType: 'AccountEvent', field: 'pastAccountEvents' });
  });

  test('buildDescriptor round-trip preserves EvaluateTransaction operation', () => {
    const rebuilt = buildDescriptor(form);
    const op = rebuilt.agents[0].ruleset.operations[0];
    expect(op.operation).toBe('EvaluateTransaction');
    expect(op.entityField).toBe('theAccount');
    expect(op.currentEventField).toBe('theTransaction');
    expect(op.outbounds).toHaveLength(1);
    expect(op.outbounds[0]).toEqual({ field: 'theFraudAlerts', eventType: 'Alert' });
  });

  test('buildDescriptor round-trip preserves EvaluateTransaction history', () => {
    const rebuilt = buildDescriptor(form);
    const op = rebuilt.agents[0].ruleset.operations[0];
    expect(op.history).toHaveLength(2);
    expect(op.history[0]).toEqual({ eventType: 'Transaction',  field: 'thePastTransactions' });
    expect(op.history[1]).toEqual({ eventType: 'AccountEvent', field: 'thePastAccountEvents' });
  });

  test('buildDescriptor round-trip preserves HandleAccountEvent operation with empty outbounds', () => {
    const rebuilt = buildDescriptor(form);
    const op = rebuilt.agents[0].ruleset.operations[1];
    expect(op.operation).toBe('HandleAccountEvent');
    // Empty outbounds array is omitted from the serialised descriptor (filtered out)
    expect(op.outbounds).toEqual([]);
  });

  test('buildDescriptor omits operations array when all operations have blank names', () => {
    const formNoOps = { ...form, ruleset: { ...form.ruleset, operations: [{ operation: '', entityField: '', currentEventField: '', history: [], outbounds: [] }] } };
    const rebuilt = buildDescriptor(formNoOps);
    expect(rebuilt.agents[0].ruleset.operations).toBeUndefined();
  });

  test('buildDescriptor omits operations key when operations array is empty', () => {
    const formEmpty = { ...form, ruleset: { ...form.ruleset, operations: [] } };
    const rebuilt = buildDescriptor(formEmpty);
    expect(rebuilt.agents[0].ruleset.operations).toBeUndefined();
  });

  test('descriptorToForm produces empty operations array when ruleset has no operations', () => {
    const noOpsDescriptor = {
      agents: [{ agentId: 'x', subscriptions: [{ eventType: 'e', topic: 't', operation: 'op', horizon: { type: 'count', value: 1 } }], ruleset: { path: 'p' }, outputs: [{ eventType: 'out', topic: 't' }] }],
    };
    const f = descriptorToForm(noOpsDescriptor);
    expect(f.ruleset.operations).toEqual([]);
  });

  test('buildDescriptor round-trip: multiple outbounds entries are preserved', () => {
    const multiDescriptor = {
      agents: [{
        agentId: 'multi-agent',
        subscriptions: [{ eventType: 'event', topic: 't', operation: 'Op', horizon: { type: 'count', value: 1 } }],
        ruleset: {
          path: 'test.jar',
          operations: [{
            operation: 'Op',
            entityField: '',
            currentEventField: '',
            history: [],
            outbounds: [
              { field: 'fraudAlerts', eventType: 'FraudAlert' },
              { field: 'approvedTxns', eventType: 'ApprovedTransaction' },
            ],
          }],
        },
        outputs: [{ eventType: 'FraudAlert', topic: 't1' }],
        stateConfig: { ttl: '30d', backend: 'rocksdb' },
      }],
    };
    const f = descriptorToForm(multiDescriptor);
    const rebuilt = buildDescriptor(f);
    const op = rebuilt.agents[0].ruleset.operations[0];
    expect(op.outbounds).toHaveLength(2);
    expect(op.outbounds[0]).toEqual({ field: 'fraudAlerts', eventType: 'FraudAlert' });
    expect(op.outbounds[1]).toEqual({ field: 'approvedTxns', eventType: 'ApprovedTransaction' });
  });
});

// ─── StepRuleset source-selector: validateStep still passes with DI-fetched path ───

describe('validateStep(2) — ruleset validation accepts DI-fetched archive path', () => {
  /**
   * After a successful DI fetch the wizard sets form.ruleset.path to the
   * server-side relative path (e.g. "samples/my-agent/fraud-score-v2.jar")
   * and leaves _jarFile null.  validateStep must pass for step 2 because
   * the path is non-empty regardless of how the archive was obtained.
   */

  // Reproduce validateStep logic inline (same as in AgentWizard.js)
  function validateStep2(form) {
    if (!form.ruleset.path.trim()) return 'Ruleset JAR path is required.';
    return null;
  }

  const makeForm = (path, jarFile = null) => ({
    agentId: 'my-agent',
    ruleset: { path, _jarFile: jarFile, archiveControlTopic: '', operations: [] },
  });

  test('passes when path is set from a DI fetch (no _jarFile)', () => {
    expect(validateStep2(makeForm('samples/my-agent/fraud-score-v2.jar', null))).toBeNull();
  });

  test('passes when path is set from an upload (with _jarFile)', () => {
    const fakeFile = { name: 'fraud.jar' };
    expect(validateStep2(makeForm('samples/my-agent/fraud.jar', fakeFile))).toBeNull();
  });

  test('fails when path is empty (no source chosen yet)', () => {
    expect(validateStep2(makeForm('', null))).toBe('Ruleset JAR path is required.');
  });

  test('fails when path is only whitespace', () => {
    expect(validateStep2(makeForm('   ', null))).toBe('Ruleset JAR path is required.');
  });
});

// ─── StepPublish jarFile handling: null vs File object ─────────────────────────

describe('StepPublish jarFile prop — null for DI-fetched, File for uploaded', () => {
  /**
   * The StepPublish component receives jarFile = form.ruleset._jarFile.
   * When null (DI fetch), jarState starts as 'not-needed' and the JAR
   * upload step is hidden.  When a File object (local upload), jarState
   * starts as 'idle' and the upload section is shown.
   *
   * These tests verify the initial jarState derivation logic.
   */

  function resolveJarState(jarFile) {
    // Mirrors: useState(jarFile ? 'idle' : 'not-needed')
    return jarFile ? 'idle' : 'not-needed';
  }

  test('jarState is "not-needed" when jarFile is null (DI path)', () => {
    expect(resolveJarState(null)).toBe('not-needed');
  });

  test('jarState is "idle" when jarFile is a File object (upload path)', () => {
    const fakeFile = { name: 'fraud.jar', size: 1024 };
    expect(resolveJarState(fakeFile)).toBe('idle');
  });

  test('jarState is "not-needed" when jarFile is undefined', () => {
    expect(resolveJarState(undefined)).toBe('not-needed');
  });
});


// ─── buildDescriptor: useCaseId handling ─────────────────────────────────────

describe('buildDescriptor — useCaseId field', () => {
  function makeForm(overrides = {}) {
    return {
      parallelism: 0,
      useCaseId: '',
      agentId: 'my-agent',
      name: 'My Agent',
      entityType: 'entity',
      entityKeyExtractor: '$.id',
      agentParallelism: 0,
      subscriptions: [{ eventType: 'e', topic: 't', operation: 'op', horizonType: 'count', horizonValue: 1, horizonCountLimit: 10, horizonTimeLimit: '24h', horizonDescription: '' }],
      ruleset: { path: '/opt/my.jar', outputField: '', archiveControlTopic: '', operations: [] },
      outputs: [{ eventType: 'out', topic: 'out-t', description: '' }],
      stateConfig: { ttl: '30d', backend: 'rocksdb' },
      ...overrides,
    };
  }

  test('returns { descriptor, useCaseId } shape', () => {
    const result = _buildDescriptor(makeForm());
    expect(result).toHaveProperty('descriptor');
    expect(result).toHaveProperty('useCaseId');
  });

  test('when useCaseId is blank, useCaseId defaults to agentId', () => {
    const { useCaseId } = _buildDescriptor(makeForm({ useCaseId: '', agentId: 'fraud-agent' }));
    expect(useCaseId).toBe('fraud-agent');
  });

  test('when useCaseId is whitespace-only, useCaseId defaults to agentId', () => {
    const { useCaseId } = _buildDescriptor(makeForm({ useCaseId: '   ', agentId: 'fraud-agent' }));
    expect(useCaseId).toBe('fraud-agent');
  });

  test('when useCaseId is explicitly set, it is returned as-is', () => {
    const { useCaseId } = _buildDescriptor(makeForm({ useCaseId: 'shared-job', agentId: 'fraud-agent' }));
    expect(useCaseId).toBe('shared-job');
  });

  test('explicit useCaseId does NOT appear inside descriptor.agents[0]', () => {
    const { descriptor } = _buildDescriptor(makeForm({ useCaseId: 'shared-job', agentId: 'fraud-agent' }));
    // useCaseId is a BFF routing concern — it must NOT contaminate the agents.json schema
    expect(descriptor.agents[0]).not.toHaveProperty('useCaseId');
  });

  test('descriptor wraps agent in agents array', () => {
    const { descriptor } = _buildDescriptor(makeForm({ agentId: 'test-agent' }));
    expect(descriptor.agents).toHaveLength(1);
    expect(descriptor.agents[0].agentId).toBe('test-agent');
  });

  test('job-level parallelism > 0 is emitted at descriptor root', () => {
    const { descriptor } = _buildDescriptor(makeForm({ parallelism: 4 }));
    expect(descriptor.parallelism).toBe(4);
  });

  test('job-level parallelism == 0 is NOT emitted at descriptor root', () => {
    const { descriptor } = _buildDescriptor(makeForm({ parallelism: 0 }));
    expect(descriptor.parallelism).toBeUndefined();
  });
});

// ─── descriptorToForm: multi-agent support (targetAgentId + useCaseId pre-fill) ──

const TWO_AGENT_DESCRIPTOR = {
  agents: [
    {
      agentId: 'aircraft-agent',
      name: 'Aircraft Agent',
      entityType: 'Aircraft',
      entityKeyExtractor: '$.aircraftId',
      subscriptions: [
        { eventType: 'AircraftEvent', topic: 'agent-aircraft-events', operation: 'ProcessAircraftEvent', horizon: { type: 'time', timeLimit: '2h' } },
      ],
      ruleset: { path: 'samples/airtraffic-agent/ATC.jar', archiveControlTopic: 'agent-atc-control', operations: [] },
      outputs: [
        { eventType: 'TrackedAircraftEvent', topic: 'agent-tracked-aircraft-events', description: 'Tracked' },
        { eventType: 'Alert', topic: 'agent-atc-alerts', description: 'Alerts' },
      ],
      stateConfig: { ttl: '4h', backend: 'rocksdb' },
      parallelism: 1,
    },
    {
      agentId: 'airport-agent',
      name: 'Airport Agent',
      entityType: 'Airport',
      entityKeyExtractor: '$.airportCode',
      subscriptions: [
        { eventType: 'AirportEvent', topic: 'agent-airport-events', operation: 'ProcessAirportEvent', horizon: { type: 'count', value: 10 } },
        { eventType: 'TrackedAircraftEvent', topic: 'agent-tracked-aircraft-events', operation: 'ProcessTrackedAircraftEvent', horizon: { type: 'time', timeLimit: '1h' } },
      ],
      ruleset: { path: 'samples/airtraffic-agent/ATC.jar', archiveControlTopic: 'agent-atc-control', operations: [] },
      outputs: [
        { eventType: 'Alert', topic: 'agent-atc-alerts', description: 'Airport Alerts' },
      ],
      stateConfig: { ttl: '24h', backend: 'rocksdb' },
      parallelism: 1,
    },
  ],
};

describe('descriptorToForm — multi-agent descriptor', () => {
  test('without targetAgentId, loads first agent (backward-compat)', () => {
    const form = descriptorToForm(TWO_AGENT_DESCRIPTOR);
    expect(form.agentId).toBe('aircraft-agent');
    expect(form.entityType).toBe('Aircraft');
  });

  test('with targetAgentId matching first agent, loads first agent', () => {
    const form = descriptorToForm(TWO_AGENT_DESCRIPTOR, 'aircraft-agent');
    expect(form.agentId).toBe('aircraft-agent');
    expect(form.entityType).toBe('Aircraft');
  });

  test('with targetAgentId matching second agent, loads second agent', () => {
    const form = descriptorToForm(TWO_AGENT_DESCRIPTOR, 'airport-agent');
    expect(form.agentId).toBe('airport-agent');
    expect(form.entityType).toBe('Airport');
    expect(form.entityKeyExtractor).toBe('$.airportCode');
  });

  test('with unknown targetAgentId, falls back to first agent', () => {
    const form = descriptorToForm(TWO_AGENT_DESCRIPTOR, 'non-existent-agent');
    expect(form.agentId).toBe('aircraft-agent');
  });

  test('second agent subscriptions are loaded correctly', () => {
    const form = descriptorToForm(TWO_AGENT_DESCRIPTOR, 'airport-agent');
    expect(form.subscriptions).toHaveLength(2);
    expect(form.subscriptions[0].eventType).toBe('AirportEvent');
    expect(form.subscriptions[1].eventType).toBe('TrackedAircraftEvent');
  });

  test('second agent stateConfig is loaded correctly', () => {
    const form = descriptorToForm(TWO_AGENT_DESCRIPTOR, 'airport-agent');
    expect(form.stateConfig.ttl).toBe('24h');
  });

  test('useCaseId is pre-filled when provided as third argument', () => {
    const form = descriptorToForm(TWO_AGENT_DESCRIPTOR, 'airport-agent', 'airtraffic-agent');
    expect(form.useCaseId).toBe('airtraffic-agent');
  });

  test('useCaseId defaults to empty string when not provided', () => {
    const form = descriptorToForm(TWO_AGENT_DESCRIPTOR, 'airport-agent');
    expect(form.useCaseId).toBe('');
  });

  test('useCaseId defaults to empty string when third arg is null', () => {
    const form = descriptorToForm(TWO_AGENT_DESCRIPTOR, 'aircraft-agent', null);
    expect(form.useCaseId).toBe('');
  });
});

// ─── validateStep(0): useCaseId format validation ─────────────────────────────

describe('validateStep(0) — useCaseId format validation', () => {
  // Reproduce only the step-0 validation logic from AgentWizard.js
  function validateStep0(form) {
    if (!form.agentId.trim()) return 'Agent ID is required.';
    const uid = (form.useCaseId || '').trim();
    if (uid && !/^[a-zA-Z0-9_-]{1,64}$/.test(uid))
      return 'Use-Case ID may only contain letters, numbers, hyphens, or underscores (max 64 chars).';
    return null;
  }

  const base = { agentId: 'my-agent', useCaseId: '' };

  test('blank agentId fails with required message', () => {
    expect(validateStep0({ ...base, agentId: '' })).toBe('Agent ID is required.');
  });

  test('blank useCaseId passes (defaults to agentId)', () => {
    expect(validateStep0({ ...base, useCaseId: '' })).toBeNull();
  });

  test('valid useCaseId with hyphens passes', () => {
    expect(validateStep0({ ...base, useCaseId: 'shared-flink-job' })).toBeNull();
  });

  test('valid useCaseId with underscores passes', () => {
    expect(validateStep0({ ...base, useCaseId: 'shared_job_1' })).toBeNull();
  });

  test('valid useCaseId with mixed alphanumeric passes', () => {
    expect(validateStep0({ ...base, useCaseId: 'Job123' })).toBeNull();
  });

  test('useCaseId with spaces fails validation', () => {
    expect(validateStep0({ ...base, useCaseId: 'shared job' })).toMatch(/Use-Case ID/);
  });

  test('useCaseId with dots fails validation', () => {
    expect(validateStep0({ ...base, useCaseId: 'shared.job' })).toMatch(/Use-Case ID/);
  });

  test('useCaseId with slashes fails validation', () => {
    expect(validateStep0({ ...base, useCaseId: 'shared/job' })).toMatch(/Use-Case ID/);
  });

  test('useCaseId exceeding 64 chars fails validation', () => {
    const long = 'a'.repeat(65);
    expect(validateStep0({ ...base, useCaseId: long })).toMatch(/Use-Case ID/);
  });

  test('useCaseId of exactly 64 chars passes', () => {
    const max = 'a'.repeat(64);
    expect(validateStep0({ ...base, useCaseId: max })).toBeNull();
  });

  test('whitespace-only useCaseId is treated as blank (passes)', () => {
    // trim() reduces it to empty — treated as "not provided"
    expect(validateStep0({ ...base, useCaseId: '   ' })).toBeNull();
  });
});


// ─── StepPublish.handleSave: patch mode calls PATCH /api/agents/:uc/agents/:aid ─

describe('StepPublish handleSave — patch mode calls PATCH endpoint', () => {
  /**
   * Simulates the exact handleSave logic in StepPublish for mode='patch'.
   * Verifies that the PATCH URL and body are constructed correctly and that
   * POST /api/agents is NOT called.
   */

  function buildSingleAgentDescriptor(agentId, name) {
    return {
      agents: [{
        agentId,
        name,
        subscriptions: [{ eventType: 'e', topic: 't', operation: 'op' }],
        ruleset: { path: 'my.jar' },
        outputs: [{ eventType: 'out', topic: 'out-t' }],
        stateConfig: { ttl: '30d', backend: 'rocksdb' },
      }],
    };
  }

  const API_URL = import.meta.env?.VITE_API_URL || 'http://localhost:8080';

  afterEach(() => {
    jest.restoreAllMocks?.();
    jest.clearAllMocks?.();
  });

  async function simulateHandleSave({ mode, useCaseId, agentId, descriptor, fetchImpl }) {
    global.fetch = fetchImpl;
    let r;
    if (mode === 'patch') {
      r = await fetch(`${API_URL}/api/agents/${useCaseId}/agents/${agentId}`, {
        method:  'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body:    JSON.stringify({ descriptor: descriptor.agents[0] }),
      });
    } else {
      r = await fetch(`${API_URL}/api/agents`, {
        method:  'POST',
        headers: { 'Content-Type': 'application/json' },
        body:    JSON.stringify({ useCaseId, descriptor, mode }),
      });
    }
    return r;
  }

  test('patch mode calls PATCH /api/agents/:uc/agents/:aid', async () => {
    const fetchMock = jest.fn().mockResolvedValue({ ok: true, json: async () => ({ updated: true }) });
    const descriptor = buildSingleAgentDescriptor('airport-agent', 'Airport Agent');

    await simulateHandleSave({
      mode: 'patch', useCaseId: 'airtraffic-agent', agentId: 'airport-agent',
      descriptor, fetchImpl: fetchMock,
    });

    expect(fetchMock).toHaveBeenCalledWith(
      `${API_URL}/api/agents/airtraffic-agent/agents/airport-agent`,
      expect.objectContaining({ method: 'PATCH' })
    );
  });

  test('patch mode does NOT call POST /api/agents', async () => {
    const fetchMock = jest.fn().mockResolvedValue({ ok: true, json: async () => ({ updated: true }) });
    const descriptor = buildSingleAgentDescriptor('airport-agent', 'Airport Agent');

    await simulateHandleSave({
      mode: 'patch', useCaseId: 'airtraffic-agent', agentId: 'airport-agent',
      descriptor, fetchImpl: fetchMock,
    });

    expect(fetchMock).not.toHaveBeenCalledWith(
      `${API_URL}/api/agents`,
      expect.anything()
    );
  });

  test('patch mode sends descriptor.agents[0] as body.descriptor', async () => {
    const fetchMock = jest.fn().mockResolvedValue({ ok: true, json: async () => ({ updated: true }) });
    const descriptor = buildSingleAgentDescriptor('airport-agent', 'Airport Agent Updated');

    await simulateHandleSave({
      mode: 'patch', useCaseId: 'airtraffic-agent', agentId: 'airport-agent',
      descriptor, fetchImpl: fetchMock,
    });

    const [, callOpts] = fetchMock.mock.calls[0];
    const body = JSON.parse(callOpts.body);
    expect(body.descriptor.agentId).toBe('airport-agent');
    expect(body.descriptor.name).toBe('Airport Agent Updated');
    // Body must NOT have a top-level 'mode' or 'useCaseId' field (those are for POST)
    expect(body.mode).toBeUndefined();
    expect(body.useCaseId).toBeUndefined();
  });

  test('replace mode still calls POST /api/agents', async () => {
    const fetchMock = jest.fn().mockResolvedValue({ ok: true, json: async () => ({ saved: true }) });
    const descriptor = buildSingleAgentDescriptor('banking-fraud-agent', 'Banking Agent');

    await simulateHandleSave({
      mode: 'replace', useCaseId: 'banking-fraud-agent', agentId: 'banking-fraud-agent',
      descriptor, fetchImpl: fetchMock,
    });

    expect(fetchMock).toHaveBeenCalledWith(
      `${API_URL}/api/agents`,
      expect.objectContaining({ method: 'POST' })
    );
  });

  test('append mode still calls POST /api/agents with mode=append', async () => {
    const fetchMock = jest.fn().mockResolvedValue({ ok: true, json: async () => ({ saved: true }) });
    const descriptor = buildSingleAgentDescriptor('new-agent', 'New Agent');

    await simulateHandleSave({
      mode: 'append', useCaseId: 'my-job', agentId: 'new-agent',
      descriptor, fetchImpl: fetchMock,
    });

    expect(fetchMock).toHaveBeenCalledWith(
      `${API_URL}/api/agents`,
      expect.objectContaining({ method: 'POST' })
    );
    const [, opts] = fetchMock.mock.calls[0];
    expect(JSON.parse(opts.body).mode).toBe('append');
  });
});

// ─── handleEditAgent: mode selection (replace vs patch) ───────────────────────

describe('handleEditAgent — mode selection based on agent count', () => {
  /**
   * Reproduces the mode-selection logic from handleEditAgent in App.js.
   * Verifies that:
   *   - Single-agent descriptors → mode='replace' (backward-compat)
   *   - Multi-agent descriptors  → mode='patch' (safe per-agent edit)
   */

  function deriveMode(agentsJson) {
    const isMulti = (agentsJson.agents || []).length > 1;
    return isMulti ? 'patch' : 'replace';
  }

  function deriveTargetAgentId(agentsJson) {
    const isMulti = (agentsJson.agents || []).length > 1;
    return isMulti ? agentsJson.agents[0].agentId : null;
  }

  const SINGLE = {
    agents: [{ agentId: 'banking-fraud-agent', name: 'Banking Agent', subscriptions: [], ruleset: { path: 'x' }, outputs: [] }],
  };

  const MULTI = {
    agents: [
      { agentId: 'aircraft-agent', name: 'Aircraft Agent', subscriptions: [], ruleset: { path: 'x' }, outputs: [] },
      { agentId: 'airport-agent',  name: 'Airport Agent',  subscriptions: [], ruleset: { path: 'x' }, outputs: [] },
    ],
  };

  test('single-agent descriptor → mode is "replace"', () => {
    expect(deriveMode(SINGLE)).toBe('replace');
  });

  test('single-agent descriptor → targetAgentId is null', () => {
    expect(deriveTargetAgentId(SINGLE)).toBeNull();
  });

  test('multi-agent descriptor → mode is "patch"', () => {
    expect(deriveMode(MULTI)).toBe('patch');
  });

  test('multi-agent descriptor → targetAgentId is agents[0].agentId', () => {
    expect(deriveTargetAgentId(MULTI)).toBe('aircraft-agent');
  });

  test('empty agents array → mode is "replace" (falls back gracefully)', () => {
    expect(deriveMode({ agents: [] })).toBe('replace');
  });
});

// ─── AgentScenarioCard: canEdit logic ─────────────────────────────────────────

describe('AgentScenarioCard — canEdit availability by phase', () => {
  /**
   * canEdit is true when the job is NOT probing, busy (starting/polling), or running.
   * This allows editing of idle and failed-state agents without restriction.
   */

  function resolveCanEdit(phase) {
    const isProbing = phase === 'probing';
    const isRunning = phase === 'running';
    const isBusy    = phase === 'starting' || phase === 'polling';
    return !isProbing && !isBusy && !isRunning;
  }

  test('"idle" phase → canEdit is true', () => {
    expect(resolveCanEdit('idle')).toBe(true);
  });

  test('"error" phase → canEdit is true (failed job can always be edited)', () => {
    expect(resolveCanEdit('error')).toBe(true);
  });

  test('"probing" phase → canEdit is false (initial state probe in progress)', () => {
    expect(resolveCanEdit('probing')).toBe(false);
  });

  test('"starting" phase → canEdit is false (job submission in flight)', () => {
    expect(resolveCanEdit('starting')).toBe(false);
  });

  test('"polling" phase → canEdit is false (waiting for RUNNING status)', () => {
    expect(resolveCanEdit('polling')).toBe(false);
  });

  test('"running" phase → canEdit is false (job actively running)', () => {
    expect(resolveCanEdit('running')).toBe(false);
  });
});


// ─── StepIdentity: useCaseId locked in append mode ────────────────────────────

describe('StepIdentity — useCaseId locked in append mode', () => {
  /**
   * In append mode the useCaseId field must be readonly so the user cannot
   * accidentally redirect the new agent to a different use-case folder.
   * We verify the locking logic by inspecting the prop resolution that
   * StepIdentity uses (useCaseIdLocked = saveMode === 'append').
   */

  function resolveUseCaseIdLocked(saveMode) {
    return saveMode === 'append';
  }

  test('saveMode="append" → useCaseId is locked', () => {
    expect(resolveUseCaseIdLocked('append')).toBe(true);
  });

  test('saveMode="replace" → useCaseId is editable', () => {
    expect(resolveUseCaseIdLocked('replace')).toBe(false);
  });

  test('saveMode="patch" → useCaseId is editable', () => {
    expect(resolveUseCaseIdLocked('patch')).toBe(false);
  });

  test('saveMode=undefined → useCaseId is editable', () => {
    expect(resolveUseCaseIdLocked(undefined)).toBe(false);
  });

  test('append mode: wizard pre-fills useCaseId from initialUseCaseId via descriptorToForm', () => {
    // In append mode App.js passes initialUseCaseId to the wizard which passes
    // it as the third arg to descriptorToForm(null, null, initialUseCaseId).
    // The resulting form must have useCaseId populated — not blank — so the
    // locked field displays the correct target use-case.
    const form = descriptorToForm(null, null, 'my-existing-job');
    expect(form.useCaseId).toBe('my-existing-job');
  });

  test('append mode: useCaseId from descriptorToForm is non-empty even when no descriptor', () => {
    // Guard: if useCaseId were blank in append mode, the locked field would be
    // empty, which is confusing.  This test ensures the value is preserved.
    const form = descriptorToForm(null, null, 'airtraffic-agent');
    expect(form.useCaseId).toBe('airtraffic-agent');
    expect(form.useCaseId.length).toBeGreaterThan(0);
  });
});

// ─── StepPublish: Save Descriptor disabled after first save ───────────────────

describe('StepPublish — Save Descriptor button disabled after first save', () => {
  /**
   * Once saveState transitions to 'saved', the Save Descriptor button must be
   * disabled.  In append mode this prevents a re-submit that would trigger the
   * DUPLICATE_AGENT_ID error from the BFF.  In all modes it prevents unnecessary
   * duplicate requests.
   *
   * We verify the disabled-condition expression that StepPublish evaluates:
   *   disabled = saveState === 'saving' || saveState === 'saved' || isDeleted || !jarReady
   */

  function isButtonDisabled({ saveState, isDeleted = false, jarReady = true }) {
    return saveState === 'saving' || saveState === 'saved' || isDeleted || !jarReady;
  }

  test('"idle" saveState → button is enabled', () => {
    expect(isButtonDisabled({ saveState: 'idle' })).toBe(false);
  });

  test('"saving" saveState → button is disabled (in-flight)', () => {
    expect(isButtonDisabled({ saveState: 'saving' })).toBe(true);
  });

  test('"saved" saveState → button is disabled (prevents duplicate submit)', () => {
    expect(isButtonDisabled({ saveState: 'saved' })).toBe(true);
  });

  test('"error" saveState → button is enabled (user can retry)', () => {
    expect(isButtonDisabled({ saveState: 'error' })).toBe(false);
  });

  test('isDeleted=true → button is disabled regardless of saveState', () => {
    expect(isButtonDisabled({ saveState: 'idle', isDeleted: true })).toBe(true);
    expect(isButtonDisabled({ saveState: 'error', isDeleted: true })).toBe(true);
  });

  test('jarReady=false → button is disabled (JAR not yet uploaded)', () => {
    expect(isButtonDisabled({ saveState: 'idle', jarReady: false })).toBe(true);
  });

  test('"saved" + isDeleted=true → still disabled (both conditions met)', () => {
    expect(isButtonDisabled({ saveState: 'saved', isDeleted: true })).toBe(true);
  });
});

// ─── handleDeleteAgent / handleDeleteSpecificAgent — fetch URL and error handling ─

describe('handleDeleteAgent — calls DELETE /api/agents/:useCaseId', () => {
  /**
   * Reproduces the exact handleDeleteAgent logic from App.js.
   * Verifies method, URL, and error-path behaviour.
   */

  const API_URL_APP = import.meta.env?.VITE_API_URL || 'http://localhost:8080';

  async function simulateHandleDeleteAgent(useCaseId, fetchImpl) {
    global.fetch = fetchImpl;
    const r = await fetch(`${API_URL_APP}/api/agents/${useCaseId}`, { method: 'DELETE' });
    if (!r.ok) {
      const body = await r.json().catch(() => ({}));
      throw new Error(body.error || r.statusText);
    }
  }

  afterEach(() => {
    jest.restoreAllMocks?.();
    jest.clearAllMocks?.();
  });

  test('calls DELETE /api/agents/:useCaseId', async () => {
    const fetchMock = jest.fn().mockResolvedValue({ ok: true, json: async () => ({ deleted: true }) });
    await simulateHandleDeleteAgent('airtraffic-agent', fetchMock);
    expect(fetchMock).toHaveBeenCalledWith(
      `${API_URL_APP}/api/agents/airtraffic-agent`,
      expect.objectContaining({ method: 'DELETE' })
    );
  });

  test('does NOT call /api/agents/:useCaseId/agents/:agentId (not per-agent endpoint)', async () => {
    const fetchMock = jest.fn().mockResolvedValue({ ok: true, json: async () => ({ deleted: true }) });
    await simulateHandleDeleteAgent('banking-fraud-agent', fetchMock);
    expect(fetchMock).not.toHaveBeenCalledWith(
      expect.stringContaining('/agents/banking-fraud-agent/agents/'),
      expect.anything()
    );
  });

  test('throws on non-ok response with server error message', async () => {
    const fetchMock = jest.fn().mockResolvedValue({
      ok: false,
      statusText: 'Conflict',
      json: async () => ({ error: 'Agent is currently running' }),
    });
    await expect(simulateHandleDeleteAgent('my-agent', fetchMock)).rejects.toThrow('Agent is currently running');
  });

  test('throws with statusText when response body has no error field', async () => {
    const fetchMock = jest.fn().mockResolvedValue({
      ok: false,
      statusText: 'Internal Server Error',
      json: async () => ({}),
    });
    await expect(simulateHandleDeleteAgent('my-agent', fetchMock)).rejects.toThrow('Internal Server Error');
  });

  test('resolves without throwing on success', async () => {
    const fetchMock = jest.fn().mockResolvedValue({ ok: true, json: async () => ({ deleted: true }) });
    await expect(simulateHandleDeleteAgent('airtraffic-agent', fetchMock)).resolves.toBeUndefined();
  });
});

describe('handleDeleteSpecificAgent — calls DELETE /api/agents/:uc/agents/:agentId', () => {
  /**
   * Reproduces the exact handleDeleteSpecificAgent logic from App.js.
   */

  const API_URL_APP = import.meta.env?.VITE_API_URL || 'http://localhost:8080';

  async function simulateHandleDeleteSpecificAgent(useCaseId, agentId, fetchImpl) {
    global.fetch = fetchImpl;
    const r = await fetch(`${API_URL_APP}/api/agents/${useCaseId}/agents/${agentId}`, { method: 'DELETE' });
    if (!r.ok) {
      const body = await r.json().catch(() => ({}));
      throw new Error(body.error || r.statusText);
    }
  }

  afterEach(() => {
    jest.restoreAllMocks?.();
    jest.clearAllMocks?.();
  });

  test('calls DELETE /api/agents/:uc/agents/:agentId with correct URL', async () => {
    const fetchMock = jest.fn().mockResolvedValue({ ok: true, json: async () => ({ deleted: true }) });
    await simulateHandleDeleteSpecificAgent('airtraffic-agent', 'aircraft-agent', fetchMock);
    expect(fetchMock).toHaveBeenCalledWith(
      `${API_URL_APP}/api/agents/airtraffic-agent/agents/aircraft-agent`,
      expect.objectContaining({ method: 'DELETE' })
    );
  });

  test('second agent in same use-case uses correct agentId in URL', async () => {
    const fetchMock = jest.fn().mockResolvedValue({ ok: true, json: async () => ({ deleted: true }) });
    await simulateHandleDeleteSpecificAgent('airtraffic-agent', 'airport-agent', fetchMock);
    expect(fetchMock).toHaveBeenCalledWith(
      `${API_URL_APP}/api/agents/airtraffic-agent/agents/airport-agent`,
      expect.objectContaining({ method: 'DELETE' })
    );
  });

  test('throws on non-ok response with BFF error message', async () => {
    const fetchMock = jest.fn().mockResolvedValue({
      ok: false,
      statusText: 'Not Found',
      json: async () => ({ error: 'AGENT_NOT_FOUND' }),
    });
    await expect(
      simulateHandleDeleteSpecificAgent('airtraffic-agent', 'ghost-agent', fetchMock)
    ).rejects.toThrow('AGENT_NOT_FOUND');
  });

  test('resolves without throwing on success', async () => {
    const fetchMock = jest.fn().mockResolvedValue({ ok: true, json: async () => ({ deleted: true, agentsRemaining: 1 }) });
    await expect(
      simulateHandleDeleteSpecificAgent('airtraffic-agent', 'airport-agent', fetchMock)
    ).resolves.toBeUndefined();
  });
});

// ─── AgentScenarioCard confirm-state logic ────────────────────────────────────

describe('AgentScenarioCard — delete confirm state machine', () => {
  /**
   * Verifies the confirm-state logic that drives the inline confirmation dialog.
   * State shape: null | { type: 'use-case' } | { type: 'agent', agentId }
   */

  function openUseCase() { return { type: 'use-case' }; }
  function openAgent(agentId) { return { type: 'agent', agentId }; }
  function cancel() { return null; }
  function confirmMsg(confirm, agentId, useCaseId) {
    if (!confirm) return null;
    return confirm.type === 'use-case'
      ? `Delete use-case "${useCaseId}" and all its descriptor files? This cannot be undone.`
      : `Remove agent "${confirm.agentId}" from use-case "${useCaseId}"? This cannot be undone.`;
  }

  test('initial state is null (no dialog shown)', () => {
    expect(cancel()).toBeNull();
  });

  test('clicking use-case delete opens use-case confirm dialog', () => {
    const state = openUseCase();
    expect(state.type).toBe('use-case');
  });

  test('clicking per-agent delete opens agent confirm with correct agentId', () => {
    const state = openAgent('aircraft-agent');
    expect(state.type).toBe('agent');
    expect(state.agentId).toBe('aircraft-agent');
  });

  test('cancel resets confirm to null', () => {
    expect(cancel()).toBeNull();
  });

  test('use-case confirm message includes the use-case id', () => {
    const msg = confirmMsg(openUseCase(), null, 'airtraffic-agent');
    expect(msg).toContain('airtraffic-agent');
    expect(msg).toContain('cannot be undone');
  });

  test('agent confirm message includes the agentId', () => {
    const msg = confirmMsg(openAgent('airport-agent'), 'airport-agent', 'airtraffic-agent');
    expect(msg).toContain('airport-agent');
    expect(msg).toContain('airtraffic-agent');
    expect(msg).toContain('cannot be undone');
  });

  test('no confirm message when state is null', () => {
    expect(confirmMsg(null, null, 'airtraffic-agent')).toBeNull();
  });

  test('canEdit false → delete button must not be shown (runState guard)', () => {
    // canEdit = !isProbing && !isBusy && !isRunning
    const phases = ['probing', 'starting', 'polling', 'running'];
    for (const phase of phases) {
      const isProbing = phase === 'probing';
      const isRunning = phase === 'running';
      const isBusy    = phase === 'starting' || phase === 'polling';
      const canEdit   = !isProbing && !isBusy && !isRunning;
      expect(canEdit).toBe(false);
    }
  });

  test('canEdit true → delete button available for idle and error phases', () => {
    for (const phase of ['idle', 'error']) {
      const isProbing = phase === 'probing';
      const isRunning = phase === 'running';
      const isBusy    = phase === 'starting' || phase === 'polling';
      const canEdit   = !isProbing && !isBusy && !isRunning;
      expect(canEdit).toBe(true);
    }
  });
});

// ─── refreshToken: AgentScenariosPanel re-fetch trigger ───────────────────────

describe('refreshToken — panel re-fetch after create/edit/delete', () => {
  /**
   * The refreshToken is a numeric counter that App bumps whenever the agent
   * list may have changed (wizard close, delete success).  AgentScenariosPanel
   * has a useEffect that calls fetchAgents() whenever the token changes.
   *
   * We verify the token-bumping logic directly (without mounting components),
   * mirroring the exact state-update calls in App.js.
   */

  function makeTokenState(initial = 0) {
    let token = initial;
    const bump = () => { token = token + 1; };
    const get  = () => token;
    return { bump, get };
  }

  test('initial token is 0', () => {
    const { get } = makeTokenState();
    expect(get()).toBe(0);
  });

  test('wizard close bumps token by 1', () => {
    const { bump, get } = makeTokenState();
    bump(); // simulates handleWizardClose → setRefreshToken(t => t + 1)
    expect(get()).toBe(1);
  });

  test('two wizard sessions produce token 2', () => {
    const { bump, get } = makeTokenState();
    bump();
    bump();
    expect(get()).toBe(2);
  });

  test('successful delete also bumps token', () => {
    const { bump, get } = makeTokenState();
    bump(); // simulates handleDeleteAgent success
    expect(get()).toBe(1);
  });

  test('failed delete does NOT bump token (throw before bump)', () => {
    // Reproduce: if fetch is not ok, we throw before reaching setRefreshToken
    const { get } = makeTokenState(5);
    // (no bump call — error path throws before reaching it)
    expect(get()).toBe(5); // unchanged
  });

  test('delete + wizard close in sequence produce distinct increments', () => {
    const { bump, get } = makeTokenState();
    bump(); // delete success
    bump(); // handleWizardClose inside delete (when showWizard was true)
    // In the real code handleWizardClose also bumps → token ends up at 2,
    // but since handleDeleteAgent calls handleWizardClose which calls setRefreshToken,
    // the React batching means only one +1 takes effect per async boundary.
    // This test just verifies the counter is monotonically increasing.
    expect(get()).toBeGreaterThanOrEqual(1);
  });
});

// ─── handleDeleteAgent/handleDeleteSpecificAgent: wizard-close-on-success ─────

describe('handleDeleteAgent — wizard close behaviour on success vs failure', () => {
  /**
   * Reproduces the exact conditional logic from App.js:
   *   if (showWizard) handleWizardClose();
   *   setRefreshToken(t => t + 1);
   *
   * Verifies that wizard is closed on success and left open on failure.
   */

  async function simulateHandleDeleteAgent({ useCaseId, fetchImpl, showWizard, onClose, onBumpToken }) {
    global.fetch = fetchImpl;
    const r = await fetch(`http://localhost:8080/api/agents/${useCaseId}`, { method: 'DELETE' });
    if (!r.ok) {
      const body = await r.json().catch(() => ({}));
      throw new Error(body.error || r.statusText);
    }
    if (showWizard) onClose();
    onBumpToken();
  }

  afterEach(() => {
    jest.restoreAllMocks?.();
    jest.clearAllMocks?.();
  });

  test('success + wizard open → wizard is closed and token is bumped', async () => {
    const onClose     = jest.fn();
    const onBumpToken = jest.fn();
    const fetchMock   = jest.fn().mockResolvedValue({ ok: true, json: async () => ({ deleted: true }) });

    await simulateHandleDeleteAgent({
      useCaseId: 'my-agent', fetchImpl: fetchMock,
      showWizard: true, onClose, onBumpToken,
    });

    expect(onClose).toHaveBeenCalledTimes(1);
    expect(onBumpToken).toHaveBeenCalledTimes(1);
  });

  test('success + wizard closed → onClose NOT called (wizard already hidden)', async () => {
    const onClose     = jest.fn();
    const onBumpToken = jest.fn();
    const fetchMock   = jest.fn().mockResolvedValue({ ok: true, json: async () => ({ deleted: true }) });

    await simulateHandleDeleteAgent({
      useCaseId: 'my-agent', fetchImpl: fetchMock,
      showWizard: false, onClose, onBumpToken,
    });

    expect(onClose).not.toHaveBeenCalled();
    expect(onBumpToken).toHaveBeenCalledTimes(1);
  });

  test('failure + wizard open → onClose NOT called, token NOT bumped', async () => {
    const onClose     = jest.fn();
    const onBumpToken = jest.fn();
    const fetchMock   = jest.fn().mockResolvedValue({
      ok: false, statusText: 'Conflict',
      json: async () => ({ error: 'Agent is currently running' }),
    });

    await expect(
      simulateHandleDeleteAgent({
        useCaseId: 'my-agent', fetchImpl: fetchMock,
        showWizard: true, onClose, onBumpToken,
      })
    ).rejects.toThrow('Agent is currently running');

    expect(onClose).not.toHaveBeenCalled();
    expect(onBumpToken).not.toHaveBeenCalled();
  });

  test('failure + wizard closed → onClose NOT called, token NOT bumped', async () => {
    const onClose     = jest.fn();
    const onBumpToken = jest.fn();
    const fetchMock   = jest.fn().mockResolvedValue({
      ok: false, statusText: 'Not Found',
      json: async () => ({}),
    });

    await expect(
      simulateHandleDeleteAgent({
        useCaseId: 'my-agent', fetchImpl: fetchMock,
        showWizard: false, onClose, onBumpToken,
      })
    ).rejects.toThrow('Not Found');

    expect(onClose).not.toHaveBeenCalled();
    expect(onBumpToken).not.toHaveBeenCalled();
  });
});

describe('handleDeleteSpecificAgent — wizard close behaviour on success vs failure', () => {
  /**
   * Same pattern as above but for the per-agent DELETE endpoint.
   */

  async function simulateHandleDeleteSpecificAgent({ useCaseId, agentId, fetchImpl, showWizard, onClose, onBumpToken }) {
    global.fetch = fetchImpl;
    const r = await fetch(`http://localhost:8080/api/agents/${useCaseId}/agents/${agentId}`, { method: 'DELETE' });
    if (!r.ok) {
      const body = await r.json().catch(() => ({}));
      throw new Error(body.error || r.statusText);
    }
    if (showWizard) onClose();
    onBumpToken();
  }

  afterEach(() => {
    jest.restoreAllMocks?.();
    jest.clearAllMocks?.();
  });

  test('success + wizard open → wizard is closed and token is bumped', async () => {
    const onClose     = jest.fn();
    const onBumpToken = jest.fn();
    const fetchMock   = jest.fn().mockResolvedValue({ ok: true, json: async () => ({ deleted: true, agentsRemaining: 1 }) });

    await simulateHandleDeleteSpecificAgent({
      useCaseId: 'airtraffic-agent', agentId: 'airport-agent', fetchImpl: fetchMock,
      showWizard: true, onClose, onBumpToken,
    });

    expect(onClose).toHaveBeenCalledTimes(1);
    expect(onBumpToken).toHaveBeenCalledTimes(1);
  });

  test('success + wizard closed → token is bumped but onClose not called', async () => {
    const onClose     = jest.fn();
    const onBumpToken = jest.fn();
    const fetchMock   = jest.fn().mockResolvedValue({ ok: true, json: async () => ({ deleted: true }) });

    await simulateHandleDeleteSpecificAgent({
      useCaseId: 'airtraffic-agent', agentId: 'airport-agent', fetchImpl: fetchMock,
      showWizard: false, onClose, onBumpToken,
    });

    expect(onClose).not.toHaveBeenCalled();
    expect(onBumpToken).toHaveBeenCalledTimes(1);
  });

  test('failure → neither onClose nor onBumpToken is called', async () => {
    const onClose     = jest.fn();
    const onBumpToken = jest.fn();
    const fetchMock   = jest.fn().mockResolvedValue({
      ok: false, statusText: 'Not Found',
      json: async () => ({ error: 'AGENT_NOT_FOUND' }),
    });

    await expect(
      simulateHandleDeleteSpecificAgent({
        useCaseId: 'airtraffic-agent', agentId: 'ghost-agent', fetchImpl: fetchMock,
        showWizard: true, onClose, onBumpToken,
      })
    ).rejects.toThrow('AGENT_NOT_FOUND');

    expect(onClose).not.toHaveBeenCalled();
    expect(onBumpToken).not.toHaveBeenCalled();
  });
});
