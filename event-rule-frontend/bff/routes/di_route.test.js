/**
 * Unit & functional tests for DI Runtime (IBM Decision Intelligence) router (routes/di_route.js).
 */

'use strict';

const express = require('express');
const { createAdsRouter } = require('./di_route');
const fs = require('fs').promises;
const path = require('path');
const os = require('os');

describe('DI Runtime (IBM Decision Intelligence) Router', () => {
  let tmpDir;
  let isValidAgentId;
  let diRuntimes;
  let router;
  let app;

  beforeEach(async () => {
    tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), 'ads-test-'));
    isValidAgentId = (id) => typeof id === 'string' && /^[a-zA-Z0-9_-]{1,64}$/.test(id);
    diRuntimes = [
      {
        id: 'runtime-1',
        name: 'Runtime One',
        url: 'http://localhost:9999',
        deploymentSpace: 'custom-space',
        username: 'admin',
        apiKey: 'secret-api-key' // pragma: allowlist secret (dummy test fixture)
      },
      {
        id: 'runtime-2',
        url: 'http://localhost:9998',
        // default deploymentSpace is 'embedded'
      }
    ];

    router = createAdsRouter({
      diRuntimes,
      samplesDir: tmpDir,
      isValidAgentId
    });

    app = express();
    app.use(express.json());
    app.use('/api/ads-runtimes', router);
  });

  afterEach(async () => {
    try {
      await fs.rm(tmpDir, { recursive: true, force: true });
    } catch {}
  });

  describe('GET /api/ads-runtimes', () => {
    test('lists configured runtimes without exposing passwords/credentials', async () => {
      // Mock req & res for direct router handling or supertest
      const req = { method: 'GET', url: '/' };
      let resJson;
      const res = {
        json: (data) => { resJson = data; }
      };

      // Call route handler
      const match = router.stack.find(s => s.route && s.route.path === '/' && s.route.methods.get);
      expect(match).toBeDefined();
      match.route.stack[0].handle(req, res);

      expect(resJson).toHaveLength(2);
      expect(resJson[0]).toEqual({
        id: 'runtime-1',
        name: 'Runtime One',
        url: 'http://localhost:9999',
        deploymentSpace: 'custom-space'
      });
      expect(resJson[0].apiKey).toBeUndefined();
      expect(resJson[0].username).toBeUndefined();

      expect(resJson[1]).toEqual({
        id: 'runtime-2',
        name: 'runtime-2',
        url: 'http://localhost:9998',
        deploymentSpace: 'embedded'
      });
    });
  });

  describe('GET /api/ads-runtimes/:id/decisions', () => {
    test('returns 404 if runtime is not found', async () => {
      const req = { method: 'GET', params: { id: 'unknown-runtime' }, query: {} };
      let resStatus;
      let resJson;
      const res = {
        status: (s) => { resStatus = s; return res; },
        json: (data) => { resJson = data; }
      };

      const match = router.stack.find(s => s.route && s.route.path === '/:id/decisions' && s.route.methods.get);
      await match.route.stack[0].handle(req, res);

      expect(resStatus).toBe(404);
      expect(resJson.error).toContain('not configured');
    });
  });

  describe('POST /api/ads-runtimes/:id/archive', () => {
    test('returns 404 if runtime is not found', async () => {
      const req = { method: 'POST', params: { id: 'unknown-runtime' }, query: { agentId: 'agent-1', decisionId: 'dec-1' } };
      let resStatus;
      let resJson;
      const res = {
        status: (s) => { resStatus = s; return res; },
        json: (data) => { resJson = data; }
      };

      const match = router.stack.find(s => s.route && s.route.path === '/:id/archive' && s.route.methods.post);
      await match.route.stack[0].handle(req, res);

      expect(resStatus).toBe(404);
      expect(resJson.error).toContain('not configured');
    });

    test('validates agentId parameter', async () => {
      const req = { method: 'POST', params: { id: 'runtime-1' }, query: { agentId: 'invalid/id', decisionId: 'dec-1' } };
      let resStatus;
      let resJson;
      const res = {
        status: (s) => { resStatus = s; return res; },
        json: (data) => { resJson = data; }
      };

      const match = router.stack.find(s => s.route && s.route.path === '/:id/archive' && s.route.methods.post);
      await match.route.stack[0].handle(req, res);

      expect(resStatus).toBe(400);
      expect(resJson.error).toContain('agentId query parameter is required');
    });

    test('validates decisionId parameter', async () => {
      const req = { method: 'POST', params: { id: 'runtime-1' }, query: { agentId: 'valid-agent' } };
      let resStatus;
      let resJson;
      const res = {
        status: (s) => { resStatus = s; return res; },
        json: (data) => { resJson = data; }
      };

      const match = router.stack.find(s => s.route && s.route.path === '/:id/archive' && s.route.methods.post);
      await match.route.stack[0].handle(req, res);

      expect(resStatus).toBe(400);
      expect(resJson.error).toContain('decisionId is required');
    });
  });

  describe('GET /api/ads-runtimes/:id/archive', () => {
    const getHandler = () => router.stack.find(s => s.route && s.route.path === '/:id/archive' && s.route.methods.get);

    test('is registered', () => {
      expect(getHandler()).toBeDefined();
    });

    test('returns 404 if runtime is not found', async () => {
      const req = { method: 'GET', params: { id: 'unknown-runtime' }, query: { decisionId: 'dec-1' } };
      let resStatus;
      let resJson;
      const res = {
        status: (s) => { resStatus = s; return res; },
        json: (data) => { resJson = data; },
      };
      await getHandler().route.stack[0].handle(req, res);
      expect(resStatus).toBe(404);
      expect(resJson.error).toContain('not configured');
    });

    test('validates decisionId and does not require an agentId', async () => {
      const req = { method: 'GET', params: { id: 'runtime-1' }, query: {} };
      let resStatus;
      let resJson;
      const res = {
        status: (s) => { resStatus = s; return res; },
        json: (data) => { resJson = data; },
      };
      await getHandler().route.stack[0].handle(req, res);
      expect(resStatus).toBe(400);
      expect(resJson.error).toContain('decisionId is required');
    });
  });

  describe('POST /api/ads-runtimes/:id/refresh-rules', () => {
    const http = require('http');
    const crypto = require('crypto');
    const ARCHIVE = Buffer.from('fake-jar-content');
    let server;
    let published;

    const refresh = async (query, { runtimeId = 'rt', publish } = {}) => {
      const url = `http://127.0.0.1:${server.address().port}`;
      const r = createAdsRouter({
        diRuntimes: [{ id: 'rt', url }],
        samplesDir: tmpDir,
        isValidAgentId,
        publishControlMessage: publish === undefined
          ? async (topic, value) => { published.push({ topic, value }); }
          : publish,
        flinkSamplesMount: '/opt/flink/samples',
      });
      const match = r.stack.find(s => s.route && s.route.path === '/:id/refresh-rules' && s.route.methods.post);
      const out = { status: 200 };
      const res = { status: (s) => { out.status = s; return res; }, json: (d) => { out.body = d; return res; } };
      await match.route.stack[0].handle({ params: { id: runtimeId }, query }, res);
      return out;
    };

    const writeDescriptor = async (agents) => {
      await fs.mkdir(path.join(tmpDir, 'uc'), { recursive: true });
      await fs.writeFile(path.join(tmpDir, 'uc', 'agents.json'), JSON.stringify({ agents }));
    };

    beforeEach(async () => {
      published = [];
      server = http.createServer((req, res) => { res.end(ARCHIVE); });
      await new Promise(r => server.listen(0, '127.0.0.1', r));
    });

    afterEach(async () => {
      await new Promise(r => server.close(r));
    });

    test('saves the archive in the use-case folder and publishes a LOAD command', async () => {
      await writeDescriptor([{ agentId: 'a', ruleset: { archiveControlTopic: 'uc-control' } }]);
      const out = await refresh({ decisionId: 'proj/svc/1.0', useCaseId: 'uc' });

      expect(out.status).toBe(200);
      expect(out.body.refreshed).toBe(true);
      expect(out.body.path).toBe('samples/uc/svc_1.0.jar');
      expect(await fs.readFile(path.join(tmpDir, 'uc', 'svc_1.0.jar'))).toEqual(ARCHIVE);

      expect(published).toHaveLength(1);
      expect(published[0].topic).toBe('uc-control');
      const cmd = JSON.parse(published[0].value);
      expect(cmd).toMatchObject({
        version: 'svc_1.0',
        archiveUrl: 'file:///opt/flink/samples/uc/svc_1.0.jar',
        sha256: crypto.createHash('sha256').update(ARCHIVE).digest('hex'),
        commandType: 'LOAD',
      });
      expect(cmd.requestId).toMatch(/^refresh-rules-/);
    });

    test('publishes once per distinct archive-control topic', async () => {
      await writeDescriptor([
        { agentId: 'a', ruleset: { archiveControlTopic: 't1' } },
        { agentId: 'b', ruleset: { archiveControlTopic: 't1' } },
        { agentId: 'c', ruleset: { archiveControlTopic: 't2' } },
      ]);
      await refresh({ decisionId: 'proj/svc/1.0', useCaseId: 'uc' });
      expect(published.map(p => p.topic)).toEqual(['t1', 't2']);
    });

    test('falls back to agent-<useCase>-archive-control when the descriptor declares none', async () => {
      await writeDescriptor([{ agentId: 'a', ruleset: {} }]);
      await refresh({ decisionId: 'proj/svc/1.0', useCaseId: 'uc' });
      expect(published.map(p => p.topic)).toEqual(['agent-uc-archive-control']);
    });

    test('rejects an invalid use case id', async () => {
      const out = await refresh({ decisionId: 'proj/svc/1.0', useCaseId: '../etc' });
      expect(out.status).toBe(400);
      expect(published).toHaveLength(0);
    });

    test('returns 404 when agents.json does not exist', async () => {
      const out = await refresh({ decisionId: 'proj/svc/1.0', useCaseId: 'missing' });
      expect(out.status).toBe(404);
      expect(published).toHaveLength(0);
    });

    test('returns 404 for an unknown runtime', async () => {
      const out = await refresh({ decisionId: 'proj/svc/1.0', useCaseId: 'uc' }, { runtimeId: 'nope' });
      expect(out.status).toBe(404);
    });

    test('returns 503 when no Kafka publisher is available', async () => {
      await writeDescriptor([{ agentId: 'a', ruleset: { archiveControlTopic: 't' } }]);
      const out = await refresh({ decisionId: 'proj/svc/1.0', useCaseId: 'uc' }, { publish: null });
      expect(out.status).toBe(503);
    });
  });

  describe('authentication towards the DI runtime', () => {
    const http = require('http');
    let server;
    let seenAuth;

    beforeEach(async () => {
      seenAuth = undefined;
      server = http.createServer((req, res) => {
        seenAuth = req.headers.authorization;
        res.setHeader('Content-Type', 'application/json');
        res.end(JSON.stringify(['proj/svc/1.0']));
      });
      await new Promise(r => server.listen(0, '127.0.0.1', r));
    });

    afterEach(async () => {
      await new Promise(r => server.close(r));
    });

    const listDecisions = async (runtime) => {
      const r = createAdsRouter({ diRuntimes: [runtime], samplesDir: tmpDir, isValidAgentId });
      const match = r.stack.find(s => s.route && s.route.path === '/:id/decisions' && s.route.methods.get);
      let resJson;
      const res = { status: () => res, json: (d) => { resJson = d; } };
      await match.route.stack[0].handle({ params: { id: runtime.id }, query: {} }, res);
      return resJson;
    };

    test('sends the Zen API key as "Authorization: ZenApiKey base64(username:apiKey)"', async () => {
      const url = `http://127.0.0.1:${server.address().port}`;
      // pragma: allowlist nextline secret (dummy test fixture)
      await listDecisions({ id: 'rt', url, username: 'cpadmin', apiKey: 'abc123' });
      expect(seenAuth).toBe(`ZenApiKey ${Buffer.from('cpadmin:abc123').toString('base64')}`);
    });

    test('sends no Authorization header when username or apiKey is missing', async () => {
      const url = `http://127.0.0.1:${server.address().port}`;
      await listDecisions({ id: 'rt', url, username: 'cpadmin' });
      expect(seenAuth).toBeUndefined();
    });
  });
});
