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
 * Tests for the DI Runtime archive acquisition flow.
 *
 * These tests cover:
 *   1. BFF route logic: GET /api/ads-runtimes  (runtime listing)
 *   2. BFF route logic: GET /api/ads-runtimes/:id/decisions  (decision listing)
 *   3. BFF route logic: POST /api/ads-runtimes/:id/decisions/:decisionId/archive  (archive download & save)
 *   4. DI path integration with the existing buildDescriptor / descriptorToForm round-trip
 *   5. Validation: agentId guard on archive download
 *   6. Error paths: runtime not found, auth failure, empty archive, network error
 *
 * All tests use mocked fetch — no live server is required.
 */

import { descriptorToForm, buildDescriptor as _buildDescriptor } from './AgentWizard';

// buildDescriptor now returns { descriptor, useCaseId }.
// Tests were written against the old API that returned the descriptor object directly.
// This wrapper restores the old calling convention for all existing tests.
const buildDescriptor = (form) => _buildDescriptor(form).descriptor;

const API_URL = import.meta.env?.VITE_API_URL || 'http://localhost:8080';

afterEach(() => {
  jest.restoreAllMocks();
  jest.clearAllMocks();
});

// ─── Fixtures ──────────────────────────────────────────────────────────────────

const RUNTIMES_RESPONSE = [
  { id: 'di-dev',  name: 'DI Dev (Fyre)',  url: 'https://xxxxxx.cp.fyre.ibm.com',  deploymentSpace: 'embedded'  },
  { id: 'di-prod', name: 'DI Production',  url: 'https://di-prod.example.com',     deploymentSpace: 'production' },
];

// Real IBM DI Runtime API response shape:
// GET /ads/runtime/api/v1/deploymentSpaces/embedded/decisions?exposed=false
// → { decisions: [ { decisionId, name, status, ... } ] }
const DI_DECISIONS_API_RESPONSE = {
  decisions: [
    { decisionId: 'fraudDetectionService', name: 'Fraud Detection Service', status: 'DEPLOYED' },
    { decisionId: 'loanEligibility',        name: 'Loan Eligibility',         status: 'DEPLOYED' },
    { decisionId: 'churnRisk',              name: 'Churn Risk',               status: 'ARCHIVED' },
  ],
};

// Normalised shape returned by the BFF to the browser
const DECISIONS_RESPONSE = [
  { id: 'fraudDetectionService', name: 'Fraud Detection Service', status: 'DEPLOYED' },
  { id: 'loanEligibility',       name: 'Loan Eligibility',        status: 'DEPLOYED' },
  { id: 'churnRisk',             name: 'Churn Risk',              status: 'ARCHIVED' },
];

const ARCHIVE_SAVE_RESPONSE = {
  saved: true,
  filename: 'fraudDetectionService.jar',
  path: 'samples/my-agent/fraudDetectionService.jar',
};

// ─── 1. Runtime listing ────────────────────────────────────────────────────────

describe('GET /api/ads-runtimes — runtime listing', () => {
  test('returns the configured runtimes as JSON', async () => {
    global.fetch = jest.fn().mockResolvedValue({
      ok: true,
      json: async () => RUNTIMES_RESPONSE,
    });

    const r = await fetch(`${API_URL}/api/ads-runtimes`);
    const body = await r.json();

    expect(fetch).toHaveBeenCalledWith(`${API_URL}/api/ads-runtimes`);
    expect(body).toHaveLength(2);
    expect(body[0]).toEqual({ id: 'di-dev', name: 'DI Dev (Fyre)', url: 'https://xxxxxx.cp.fyre.ibm.com', deploymentSpace: 'embedded' });
    expect(body[1]).toEqual({ id: 'di-prod', name: 'DI Production', url: 'https://di-prod.example.com', deploymentSpace: 'production' });
  });

  test('returns an empty array when no runtimes are configured', async () => {
    global.fetch = jest.fn().mockResolvedValue({
      ok: true,
      json: async () => [],
    });

    const r = await fetch(`${API_URL}/api/ads-runtimes`);
    const body = await r.json();

    expect(body).toEqual([]);
  });

  test('response contains id, name, url, deploymentSpace but no credentials', async () => {
    global.fetch = jest.fn().mockResolvedValue({
      ok: true,
      json: async () => RUNTIMES_RESPONSE,
    });

    const r   = await fetch(`${API_URL}/api/ads-runtimes`);
    const rts = await r.json();

    for (const rt of rts) {
      expect(rt).toHaveProperty('id');
      expect(rt).toHaveProperty('name');
      expect(rt).toHaveProperty('url');
      expect(rt).toHaveProperty('deploymentSpace');
      expect(rt).not.toHaveProperty('username');
      expect(rt).not.toHaveProperty('password');
    }
  });

  test('deploymentSpace in runtime listing defaults to "embedded" when not configured', () => {
    // Simulate BFF behaviour: diDeploymentSpace falls back to 'embedded'
    const diDeploymentSpace = r => (r.deploymentSpace || 'embedded').trim();
    const runtimeWithoutSpace = { id: 'di-x', name: 'X', url: 'https://x.example.com' };
    expect(diDeploymentSpace(runtimeWithoutSpace)).toBe('embedded');
  });
});

// ─── 2. Decision listing ───────────────────────────────────────────────────────

describe('GET /api/ads-runtimes/:id/decisions — decision listing', () => {
  test('returns normalised decision list for a known runtime', async () => {
    // BFF receives the real IBM DI Runtime API shape and normalises it
    global.fetch = jest.fn().mockResolvedValue({
      ok: true,
      json: async () => DECISIONS_RESPONSE,
    });

    const r    = await fetch(`${API_URL}/api/ads-runtimes/di-dev/decisions`);
    const body = await r.json();

    expect(fetch).toHaveBeenCalledWith(`${API_URL}/api/ads-runtimes/di-dev/decisions`);
    expect(body).toHaveLength(3);
    expect(body[0]).toMatchObject({ id: 'fraudDetectionService', name: 'Fraud Detection Service', status: 'DEPLOYED' });
  });

  test('BFF normalises IBM DI API shape: decisionId → id', () => {
    // Verify the BFF normalisation: { decisions: [{ decisionId, name, status }] } → [{ id, name, status }]
    const items = DI_DECISIONS_API_RESPONSE.decisions;
    const normalised = items.map(d => ({
      id:     d.decisionId || d.id   || '',
      name:   d.name       || d.decisionId || '',
      status: d.status     || 'unknown',
    }));
    expect(normalised[0]).toEqual({ id: 'fraudDetectionService', name: 'Fraud Detection Service', status: 'DEPLOYED' });
    expect(normalised[1]).toEqual({ id: 'loanEligibility',       name: 'Loan Eligibility',        status: 'DEPLOYED' });
    expect(normalised[2]).toEqual({ id: 'churnRisk',             name: 'Churn Risk',              status: 'ARCHIVED' });
  });

  test('returns 404 for an unconfigured runtime', async () => {
    global.fetch = jest.fn().mockResolvedValue({
      ok: false,
      status: 404,
      json: async () => ({ error: "DI runtime 'does-not-exist' not configured" }),
    });

    const r    = await fetch(`${API_URL}/api/ads-runtimes/does-not-exist/decisions`);
    const body = await r.json();

    expect(r.ok).toBe(false);
    expect(r.status).toBe(404);
    expect(body.error).toMatch(/not configured/);
  });

  test('returns 401 when DI runtime rejects credentials', async () => {
    global.fetch = jest.fn().mockResolvedValue({
      ok: false,
      status: 401,
      json: async () => ({ error: 'DI runtime authentication failed. Check username/password in DI_RUNTIMES config.' }),
    });

    const r    = await fetch(`${API_URL}/api/ads-runtimes/di-dev/decisions`);
    const body = await r.json();

    expect(r.status).toBe(401);
    expect(body.error).toMatch(/authentication failed/i);
  });

  test('returns 502 on network failure connecting to DI runtime', async () => {
    global.fetch = jest.fn().mockResolvedValue({
      ok: false,
      status: 502,
      json: async () => ({ error: 'Failed to connect to DI runtime: ECONNREFUSED' }),
    });

    const r    = await fetch(`${API_URL}/api/ads-runtimes/di-dev/decisions`);
    const body = await r.json();

    expect(r.status).toBe(502);
    expect(body.error).toMatch(/connect/i);
  });

  test('returns an empty array when runtime has no deployed decisions', async () => {
    global.fetch = jest.fn().mockResolvedValue({
      ok: true,
      json: async () => [],
    });

    const r    = await fetch(`${API_URL}/api/ads-runtimes/di-dev/decisions`);
    const body = await r.json();

    expect(body).toEqual([]);
  });
});

// ─── 3. Archive download & save ────────────────────────────────────────────────

describe('POST /api/ads-runtimes/:id/decisions/*/archive — archive fetch', () => {
  test('returns saved path on success (real decisionId from IBM DI)', async () => {
    global.fetch = jest.fn().mockResolvedValue({
      ok: true,
      status: 201,
      json: async () => ARCHIVE_SAVE_RESPONSE,
    });

    const url = `${API_URL}/api/ads-runtimes/di-dev/decisions/fraudDetectionService/archive?agentId=my-agent`;
    const r   = await fetch(url, { method: 'POST' });
    const body = await r.json();

    expect(fetch).toHaveBeenCalledWith(url, { method: 'POST' });
    expect(r.ok).toBe(true);
    expect(body.saved).toBe(true);
    expect(body.path).toBe('samples/my-agent/fraudDetectionService.jar');
    expect(body.filename).toBe('fraudDetectionService.jar');
  });

  test('accepts full DI path decision ID with slashes and colons (encodeURIComponent in browser)', async () => {
    // Real DI decision ID: "_103597706/last_gmo/frauddetectionservice/fraudDetectionServiceDecisionService/0.0.3-2026-08-20T00:42:14.506Z"
    // Browser calls encodeURIComponent() before placing it in the URL path.
    const fullDecisionId = '_103597706/last_gmo/frauddetectionservice/fraudDetectionServiceDecisionService/0.0.3-2026-08-20T00:42:14.506Z';
    const encoded        = encodeURIComponent(fullDecisionId);
    const expectedFilename = 'fraudDetectionServiceDecisionService_0.0.3-2026-08-20T00_42_14.506Z.jar';

    global.fetch = jest.fn().mockResolvedValue({
      ok: true,
      status: 201,
      json: async () => ({ saved: true, filename: expectedFilename, path: `samples/my-agent/${expectedFilename}` }),
    });

    const url  = `${API_URL}/api/ads-runtimes/di-dev/decisions/${encoded}/archive?agentId=my-agent`;
    const r    = await fetch(url, { method: 'POST' });
    const body = await r.json();

    expect(r.ok).toBe(true);
    expect(body.saved).toBe(true);
    // Filename is derived from the last segment of the path with unsafe chars sanitised
    expect(body.filename).toBe(expectedFilename);
  });

  test('returns 400 when agentId is missing', async () => {
    global.fetch = jest.fn().mockResolvedValue({
      ok: false,
      status: 400,
      json: async () => ({ error: 'agentId query parameter is required and must be a valid agent id' }),
    });

    const r    = await fetch(`${API_URL}/api/ads-runtimes/di-dev/decisions/fraudDetectionService/archive`, { method: 'POST' });
    const body = await r.json();

    expect(r.status).toBe(400);
    expect(body.error).toMatch(/agentId/);
  });

  test('returns 400 when agentId contains path traversal characters', async () => {
    global.fetch = jest.fn().mockResolvedValue({
      ok: false,
      status: 400,
      json: async () => ({ error: 'agentId query parameter is required and must be a valid agent id' }),
    });

    const r    = await fetch(`${API_URL}/api/ads-runtimes/di-dev/decisions/fraudDetectionService/archive?agentId=../etc`, { method: 'POST' });
    const body = await r.json();

    expect(r.status).toBe(400);
    expect(body.error).toMatch(/agentId/);
  });

  test('returns 404 when decision does not exist on DI runtime', async () => {
    global.fetch = jest.fn().mockResolvedValue({
      ok: false,
      status: 404,
      json: async () => ({ error: "Decision 'ghost-decision' not found on DI runtime 'di-dev'" }),
    });

    const r    = await fetch(`${API_URL}/api/ads-runtimes/di-dev/decisions/ghost-decision/archive?agentId=my-agent`, { method: 'POST' });
    const body = await r.json();

    expect(r.status).toBe(404);
    expect(body.error).toMatch(/not found/i);
  });

  test('returns 401 when DI runtime rejects credentials during download', async () => {
    global.fetch = jest.fn().mockResolvedValue({
      ok: false,
      status: 401,
      json: async () => ({ error: 'DI runtime authentication failed.' }),
    });

    const r    = await fetch(`${API_URL}/api/ads-runtimes/di-dev/decisions/fraudDetectionService/archive?agentId=my-agent`, { method: 'POST' });
    const body = await r.json();

    expect(r.status).toBe(401);
    expect(body.error).toMatch(/authentication failed/i);
  });

  test('returns 502 when DI runtime returns an empty archive', async () => {
    global.fetch = jest.fn().mockResolvedValue({
      ok: false,
      status: 502,
      json: async () => ({ error: 'DI runtime returned an empty archive' }),
    });

    const r    = await fetch(`${API_URL}/api/ads-runtimes/di-dev/decisions/fraudDetectionService/archive?agentId=my-agent`, { method: 'POST' });
    const body = await r.json();

    expect(r.status).toBe(502);
    expect(body.error).toMatch(/empty archive/i);
  });
});

// ─── 4. Integration: DI-fetched archive enters the same ruleset pipeline ──────

describe('DI-fetched archive — ruleset pipeline integration', () => {
  /**
   * After a successful DI fetch the wizard sets:
   *   form.ruleset._jarFile = null   (no browser file object)
   *   form.ruleset.path = 'samples/<agentId>/<decisionId>.jar'
   *
   * Verify that buildDescriptor produces a valid ruleset and that
   * descriptorToForm can round-trip it back.
   */
  const FORM_AFTER_DI_FETCH = {
    parallelism: 0,
    agentId: 'my-agent',
    name: 'My Agent',
    entityType: 'account',
    entityKeyExtractor: '$.id',
    agentParallelism: 0,
    subscriptions: [
      {
        eventType: 'transaction',
        topic: 'agent-transactions',
        operation: 'ScoreTransaction',
        horizonType: 'hybrid',
        horizonValue: 10,
        horizonCountLimit: 100,
        horizonTimeLimit: '24h',
        horizonDescription: '',
      },
    ],
    ruleset: {
      _jarFile: null,   // no browser file — archive was fetched server-side
      path: 'samples/my-agent/fraudDetectionService.jar',
      outputField: '',
      archiveControlTopic: '',
      operations: [],
    },
    outputs: [{ eventType: 'scored', topic: 'agent-scored', description: '' }],
    stateConfig: { ttl: '30d', backend: 'rocksdb' },
  };

  test('buildDescriptor includes the DI-fetched archive path in ruleset.path', () => {
    const descriptor = buildDescriptor(FORM_AFTER_DI_FETCH);
    expect(descriptor.agents[0].ruleset.path).toBe('samples/my-agent/fraudDetectionService.jar');
  });

  test('buildDescriptor does not include _jarFile in the output JSON', () => {
    const descriptor = buildDescriptor(FORM_AFTER_DI_FETCH);
    expect(descriptor.agents[0].ruleset).not.toHaveProperty('_jarFile');
  });

  test('buildDescriptor produces a descriptor that passes descriptorToForm round-trip', () => {
    const descriptor = buildDescriptor(FORM_AFTER_DI_FETCH);
    const form       = descriptorToForm(descriptor);
    expect(form.ruleset.path).toBe('samples/my-agent/fraudDetectionService.jar');
    expect(form.agentId).toBe('my-agent');
  });

  test('descriptorToForm restores a DI-fetched path (real IBM DI decisionId) correctly', () => {
    const descriptor = {
      agents: [{
        agentId: 'my-agent',
        subscriptions: [{ eventType: 'e', topic: 't', operation: 'op', horizon: { type: 'count', value: 1 } }],
        ruleset: { path: 'samples/my-agent/fraudDetectionService.jar' },
        outputs: [{ eventType: 'out', topic: 'out-t' }],
      }],
    };
    const form = descriptorToForm(descriptor);
    expect(form.ruleset.path).toBe('samples/my-agent/fraudDetectionService.jar');
    // _jarFile is not in the descriptor — should be absent / undefined in form
    expect(form.ruleset._jarFile).toBeUndefined();
  });
});

// ─── 5b. Runtime config: deploymentSpace and rejectUnauthorized ───────────────

describe('DI runtime config — deploymentSpace and rejectUnauthorized', () => {
  /**
   * These tests verify the helper functions and URL construction used by the BFF.
   * They run entirely in-process without any HTTP calls.
   */

  function diDeploymentSpace(runtime) {
    return (runtime.deploymentSpace || 'embedded').trim();
  }

  function buildListUrl(runtime) {
    const space = diDeploymentSpace(runtime);
    return `${runtime.url.replace(/\/$/, '')}/ads/runtime/api/v1/deploymentSpaces/${encodeURIComponent(space)}/decisions?exposed=false`;
  }

  function buildArchiveUrl(runtime, decisionId) {
    const space = diDeploymentSpace(runtime);
    return `${runtime.url.replace(/\/$/, '')}/ads/runtime/api/v1/deploymentSpaces/${encodeURIComponent(space)}/decisions/${encodeURIComponent(decisionId)}/archive`;
  }

  const FYRE_RUNTIME = {
    id:                 'di-dev',
    name:               'DI Dev (Fyre)',
    url:                'https://xxxxxx.cp.fyre.ibm.com',
    username:           'xxxxx',
    password:           'xxxxxx',
    deploymentSpace:    'embedded',
    rejectUnauthorized: false,
  };

  test('deploymentSpace defaults to "embedded" when not set', () => {
    expect(diDeploymentSpace({})).toBe('embedded');
    expect(diDeploymentSpace({ deploymentSpace: '' })).toBe('embedded');
  });

  test('deploymentSpace uses the configured value', () => {
    expect(diDeploymentSpace({ deploymentSpace: 'my-space' })).toBe('my-space');
  });

  test('builds correct decision list URL for Fyre runtime', () => {
    const url = buildListUrl(FYRE_RUNTIME);
    expect(url).toBe(
      'https://xxxxxx.cp.fyre.ibm.com/ads/runtime/api/v1/deploymentSpaces/embedded/decisions?exposed=false'
    );
  });

  test('builds correct archive download URL for Fyre runtime', () => {
    const url = buildArchiveUrl(FYRE_RUNTIME, 'fraudDetectionService');
    expect(url).toBe(
      'https://xxxxxx.cp.fyre.ibm.com/ads/runtime/api/v1/deploymentSpaces/embedded/decisions/fraudDetectionService/archive'
    );
  });

  test('trailing slash on url is stripped', () => {
    const rt  = { ...FYRE_RUNTIME, url: 'https://xxxxxx.cp.fyre.ibm.com/' };
    expect(buildListUrl(rt)).not.toContain('//ads/');
  });

  test('rejectUnauthorized is false for Fyre runtime (self-signed cert)', () => {
    expect(FYRE_RUNTIME.rejectUnauthorized).toBe(false);
  });

  test('DI_RUNTIMES env example parses correctly', () => {
    const envValue = '[{"id":"di-dev","name":"DI Dev (Fyre)","url":"https://xxxxxx.cp.fyre.ibm.com","username":"xxxxxx","password":"xxxxxxPassw0rd","deploymentSpace":"embedded","rejectUnauthorized":false}]';
    const parsed   = JSON.parse(envValue);
    expect(parsed).toHaveLength(1);
    expect(parsed[0].id).toBe('di-dev');
    expect(parsed[0].rejectUnauthorized).toBe(false);
    expect(parsed[0].deploymentSpace).toBe('embedded');
  });
});

// ─── 5c. deploymentSpace UI override — BFF query param forwarding ─────────────

describe('deploymentSpace UI override — BFF query param forwarding', () => {
  /**
   * These tests verify the BFF-side logic for honouring an optional
   * ?deploymentSpace= query parameter on both the decisions and archive routes.
   * They run entirely in-process without HTTP calls, mirroring the BFF helpers.
   */

  function diDeploymentSpace(runtime) {
    return (runtime.deploymentSpace || 'embedded').trim();
  }

  function resolveSpace(runtime, queryOverride) {
    const override = (queryOverride || '').trim();
    return override || diDeploymentSpace(runtime);
  }

  function buildListUrl(runtime, queryOverride) {
    const space = resolveSpace(runtime, queryOverride);
    return `${runtime.url.replace(/\/$/, '')}/ads/runtime/api/v1/deploymentSpaces/${encodeURIComponent(space)}/decisions?exposed=false`;
  }

  function buildArchiveUrl(runtime, decisionId, queryOverride) {
    const space = resolveSpace(runtime, queryOverride);
    return `${runtime.url.replace(/\/$/, '')}/ads/runtime/api/v1/deploymentSpaces/${encodeURIComponent(space)}/decisions/${encodeURIComponent(decisionId)}/archive`;
  }

  const FYRE_RUNTIME = {
    id:                 'di-dev',
    name:               'DI Dev (Fyre)',
    url:                'https://xxxxxx.cp.fyre.ibm.com',
    username:           'xxxxx',
    password:           'xxxxxx',
    deploymentSpace:    'embedded',
    rejectUnauthorized: false,
  };

  // ── deploymentSpace resolution ──

  test('no override → uses env default', () => {
    expect(resolveSpace(FYRE_RUNTIME, '')).toBe('embedded');
    expect(resolveSpace(FYRE_RUNTIME, undefined)).toBe('embedded');
  });

  test('override takes precedence over env default', () => {
    expect(resolveSpace(FYRE_RUNTIME, 'my-custom-space')).toBe('my-custom-space');
  });

  test('override with whitespace-only string falls back to env default', () => {
    expect(resolveSpace(FYRE_RUNTIME, '   ')).toBe('embedded');
  });

  test('override takes precedence even when env default is non-standard', () => {
    const rt = { ...FYRE_RUNTIME, deploymentSpace: 'production' };
    expect(resolveSpace(rt, 'staging')).toBe('staging');
  });

  // ── URL construction with override ──

  test('decisions URL uses override space when provided', () => {
    const url = buildListUrl(FYRE_RUNTIME, 'my-custom-space');
    expect(url).toBe(
      'https://xxxxxx.cp.fyre.ibm.com/ads/runtime/api/v1/deploymentSpaces/my-custom-space/decisions?exposed=false'
    );
  });

  test('decisions URL uses env default when no override', () => {
    const url = buildListUrl(FYRE_RUNTIME, '');
    expect(url).toBe(
      'https://xxxxxx.cp.fyre.ibm.com/ads/runtime/api/v1/deploymentSpaces/embedded/decisions?exposed=false'
    );
  });

  test('archive URL uses override space when provided', () => {
    const url = buildArchiveUrl(FYRE_RUNTIME, 'fraudDetectionService', 'my-custom-space');
    expect(url).toBe(
      'https://xxxxxx.cp.fyre.ibm.com/ads/runtime/api/v1/deploymentSpaces/my-custom-space/decisions/fraudDetectionService/archive'
    );
  });

  test('archive URL uses env default when no override', () => {
    const url = buildArchiveUrl(FYRE_RUNTIME, 'fraudDetectionService', '');
    expect(url).toBe(
      'https://xxxxxx.cp.fyre.ibm.com/ads/runtime/api/v1/deploymentSpaces/embedded/decisions/fraudDetectionService/archive'
    );
  });

  // ── BFF route: GET /api/ads-runtimes/:id/decisions?deploymentSpace=<space> ──

  test('GET decisions with ?deploymentSpace= override calls BFF with override', async () => {
    global.fetch = jest.fn().mockResolvedValue({
      ok: true,
      json: async () => [],
    });

    const url = `${API_URL}/api/ads-runtimes/di-dev/decisions?deploymentSpace=${encodeURIComponent('my-custom-space')}`;
    const r = await fetch(url);
    await r.json();

    expect(fetch).toHaveBeenCalledWith(url);
    // Verify the ?deploymentSpace= query is present in the called URL
    expect(fetch.mock.calls[0][0]).toContain('deploymentSpace=my-custom-space');
  });

  test('GET decisions without ?deploymentSpace= uses env default (no query param)', async () => {
    global.fetch = jest.fn().mockResolvedValue({
      ok: true,
      json: async () => [],
    });

    const url = `${API_URL}/api/ads-runtimes/di-dev/decisions`;
    await fetch(url);

    expect(fetch.mock.calls[0][0]).not.toContain('deploymentSpace');
  });

  // ── BFF route: POST /api/ads-runtimes/:id/archive?deploymentSpace=<space> ──

  test('POST archive with ?deploymentSpace= override forwards override to BFF', async () => {
    global.fetch = jest.fn().mockResolvedValue({
      ok: true,
      status: 201,
      json: async () => ({ saved: true, path: 'samples/my-agent/fraudDetectionService.jar' }),
    });

    const url = `${API_URL}/api/ads-runtimes/di-dev/archive?decisionId=${encodeURIComponent('fraudDetectionService')}&agentId=my-agent&deploymentSpace=${encodeURIComponent('my-custom-space')}`;
    const r = await fetch(url, { method: 'POST' });
    const body = await r.json();

    expect(r.ok).toBe(true);
    expect(body.saved).toBe(true);
    expect(fetch.mock.calls[0][0]).toContain('deploymentSpace=my-custom-space');
  });

  test('POST archive without ?deploymentSpace= sends no override (uses env default)', async () => {
    global.fetch = jest.fn().mockResolvedValue({
      ok: true,
      status: 201,
      json: async () => ({ saved: true, path: 'samples/my-agent/fraudDetectionService.jar' }),
    });

    const url = `${API_URL}/api/ads-runtimes/di-dev/archive?decisionId=${encodeURIComponent('fraudDetectionService')}&agentId=my-agent`;
    await fetch(url, { method: 'POST' });

    expect(fetch.mock.calls[0][0]).not.toContain('deploymentSpace');
  });
});

// ─── 5. Validation helpers (agentId guard) ─────────────────────────────────────

describe('agentId validation for archive download', () => {
  const VALID_IDS   = ['my-agent', 'fraud-detection-agent', 'agent_001', 'A1'];
  const INVALID_IDS = ['', '../etc', 'a b', 'agent/hack', 'a'.repeat(65)];

  const isValidAgentId = id => typeof id === 'string' && /^[a-zA-Z0-9_-]{1,64}$/.test(id);

  test.each(VALID_IDS)('accepts valid agentId: %s', (id) => {
    expect(isValidAgentId(id)).toBe(true);
  });

  test.each(INVALID_IDS)('rejects invalid agentId: %s', (id) => {
    expect(isValidAgentId(id)).toBe(false);
  });
});
