/**
 * Unit & functional tests for DI Runtime (IBM Decision Intelligence) router (routes/di_route.js).
 */

'use strict';

const express = require('express');
const { createAdsRouter } = require('./di_route');
const fs = require('fs').promises;
const path = require('path');
const os = require('os');

// Fake of archive_status.createArchiveAckWaiter(): the job answers `status` for every expected agent.
const fakeWaiter = (status = 'LOADED', { record, subtasks = 1, error } = {}) => async (args) => {
  if (record) record.push(args);
  return {
    done: async () => args.expectedAgents.flatMap(agentId => Array.from({ length: subtasks }, (_, i) => ({
      agentId, requestId: args.requestId, subtask: i, parallelism: subtasks,
      status: Array.isArray(status) ? status[i] : status, ...(error ? { error } : {}),
    }))),
    close: async () => {},
  };
};

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
      expect(resJson[0].token).toBeUndefined();

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

    const refresh = async (query, { runtimeId = 'rt', publish, waiter = fakeWaiter() } = {}) => {
      const url = `http://127.0.0.1:${server.address().port}`;
      const r = createAdsRouter({
        diRuntimes: [{ id: 'rt', url }],
        samplesDir: tmpDir,
        isValidAgentId,
        publishControlMessage: publish === undefined
          ? async (topic, value) => { published.push({ topic, value }); }
          : publish,
        awaitArchiveAcks: waiter,
        ackTimeoutMs: 50,
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
      await writeDescriptor([{ agentId: 'a', ruleset: { path: 'samples/uc/svc_1.0.jar', archiveControlTopic: 'uc-control' } }]);
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
        { agentId: 'a', ruleset: { path: 'samples/uc/svc_1.0.jar', archiveControlTopic: 't1' } },
        { agentId: 'b', ruleset: { path: 'samples/uc/svc_1.0.jar', archiveControlTopic: 't1' } },
        { agentId: 'c', ruleset: { path: 'samples/uc/svc_1.0.jar', archiveControlTopic: 't2' } },
      ]);
      await refresh({ decisionId: 'proj/svc/1.0', useCaseId: 'uc' });
      expect(published.map(p => p.topic)).toEqual(['t1', 't2']);
    });

    test('falls back to agent-<useCase>-archive-control when the descriptor declares none', async () => {
      await writeDescriptor([{ agentId: 'a', ruleset: { path: 'samples/uc/svc_1.0.jar' } }]);
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
      await writeDescriptor([{ agentId: 'a', ruleset: { path: 'samples/uc/svc_1.0.jar', archiveControlTopic: 't' } }]);
      const out = await refresh({ decisionId: 'proj/svc/1.0', useCaseId: 'uc' }, { publish: null });
      expect(out.status).toBe(503);
    });

    describe('use-case binding (server-side validation)', () => {
      const bound = (ruleset) => writeDescriptor([{ agentId: 'a', ruleset: { archiveControlTopic: 't', ...ruleset } }]);
      const expectNothingDone = async (out) => {
        expect(published).toHaveLength(0);
        await expect(fs.readdir(path.join(tmpDir, 'uc'))).resolves.toEqual(['agents.json']);
        expect(out.body.error).toBeTruthy();
      };

      test('allows an archive of the service bound by ruleset.path', async () => {
        await bound({ path: 'samples/uc/AirlineDecisionService_1.6.0.jar' });
        const out = await refresh({ decisionId: 'proj/AirlineDecisionService/2.0', useCaseId: 'uc' });
        expect(out.status).toBe(200);
        expect(out.body.filename).toBe('AirlineDecisionService_2.0.jar');
        expect(published).toHaveLength(1);
      });

      test('rejects an archive of an unrelated service with 403 and a clear message', async () => {
        await bound({ path: 'samples/uc/AirlineDecisionService_1.6.0.jar' });
        const out = await refresh({ decisionId: 'proj/RetailDecisionService/1.0', useCaseId: 'uc' });
        expect(out.status).toBe(403);
        expect(out.body.error).toMatch(/RetailDecisionService/);
        expect(out.body.error).toMatch(/AirlineDecisionService/);
        await expectNothingDone(out);
      });

      test('rejects a decision id without a service segment', async () => {
        await bound({ path: 'samples/uc/svc_1.0.jar' });
        const out = await refresh({ decisionId: 'svc', useCaseId: 'uc' });
        expect(out.status).toBe(403);
        await expectNothingDone(out);
      });

      test('allows any of several bound services and rejects others', async () => {
        await writeDescriptor([
          { agentId: 'a', ruleset: { path: 'samples/uc/Alpha_1.0.jar', archiveControlTopic: 't' } },
          { agentId: 'b', ruleset: { path: 'samples/uc/Beta_2.0.jar', archiveControlTopic: 't' } },
        ]);
        expect((await refresh({ decisionId: 'p/Alpha/3.0', useCaseId: 'uc' })).status).toBe(200);
        expect((await refresh({ decisionId: 'p/Beta/3.0', useCaseId: 'uc' })).status).toBe(200);
        const out = await refresh({ decisionId: 'p/Gamma/3.0', useCaseId: 'uc' });
        expect(out.status).toBe(403);
      });

      test('uses ruleset.project when ruleset.path is absent', async () => {
        await bound({ project: 'samples/uc/di-project/FraudDetectionService.zip' });
        expect((await refresh({ decisionId: 'p/FraudDetectionService/1.0', useCaseId: 'uc' })).status).toBe(200);
        expect((await refresh({ decisionId: 'p/Other/1.0', useCaseId: 'uc' })).status).toBe(403);
      });

      test('trusts ruleset.path when it disagrees with ruleset.project', async () => {
        await bound({ path: 'samples/uc/PathService_1.0.jar', project: 'samples/uc/di-project/ProjectService.zip' });
        expect((await refresh({ decisionId: 'p/ProjectService/1.0', useCaseId: 'uc' })).status).toBe(403);
        expect((await refresh({ decisionId: 'p/PathService/2.0', useCaseId: 'uc' })).status).toBe(200);
      });

      test('rejects with 422 when no binding can be resolved (no path, no project)', async () => {
        await bound({});
        const out = await refresh({ decisionId: 'p/svc/1.0', useCaseId: 'uc' });
        expect(out.status).toBe(422);
        expect(out.body.error).toMatch(/ruleset\.path/);
        await expectNothingDone(out);
      });

      test('rejects with 422 for an unrecognizable jar name, without falling back to project', async () => {
        await bound({ path: 'samples/uc/rules.jar', project: 'samples/uc/di-project/svc.zip' });
        const out = await refresh({ decisionId: 'p/svc/1.0', useCaseId: 'uc' });
        expect(out.status).toBe(422);
        await expectNothingDone(out);
      });

      test('rejects with 422 when the descriptor has no agents', async () => {
        await writeDescriptor([]);
        const out = await refresh({ decisionId: 'p/svc/1.0', useCaseId: 'uc' });
        expect(out.status).toBe(422);
      });

      test('a missing decisionId is still answered with 400', async () => {
        await bound({ path: 'samples/uc/svc_1.0.jar' });
        const out = await refresh({ useCaseId: 'uc' });
        expect(out.status).toBe(400);
        expect(out.body.error).toMatch(/decisionId is required/);
      });

      test('a repeated decisionId parameter is rejected', async () => {
        await bound({ path: 'samples/uc/svc_1.0.jar' });
        const out = await refresh({ decisionId: ['p/svc/1.0', 'p/other/1.0'], useCaseId: 'uc' });
        expect(out.status).toBe(400);
        expect(published).toHaveLength(0);
      });

      test('a malformed agents.json is a 500, nothing is published', async () => {
        await fs.mkdir(path.join(tmpDir, 'uc'), { recursive: true });
        await fs.writeFile(path.join(tmpDir, 'uc', 'agents.json'), '{ not json');
        const out = await refresh({ decisionId: 'p/svc/1.0', useCaseId: 'uc' });
        expect(out.status).toBe(500);
        expect(published).toHaveLength(0);
      });
    });
  });

  describe('GET /api/ads-runtimes/:id/decisions?useCaseId= (use-case filtering)', () => {
    const http = require('http');
    let server;
    let listCalls;
    let listing;

    const ALL = [
      'p/AirlineDecisionService/1', 'p/AirlineDecisionService/2', 'p/AirlineDecisionService/3',
      'p/RetailDecisionService/1', 'p/FraudDecisionService/1', 'p/OtherDecisionService/7',
      'q/Air/1', 'q/Air_Traffic_Control_Service/1.6.1',
    ];

    beforeEach(async () => {
      listCalls = 0;
      listing = ALL;
      server = http.createServer((req, res) => {
        listCalls += 1;
        res.setHeader('Content-Type', 'application/json');
        res.end(JSON.stringify(listing));
      });
      await new Promise(r => server.listen(0, '127.0.0.1', r));
    });

    afterEach(async () => {
      await new Promise(r => server.close(r));
    });

    const writeUc = async (id, agents) => {
      await fs.mkdir(path.join(tmpDir, id), { recursive: true });
      await fs.writeFile(path.join(tmpDir, id, 'agents.json'), JSON.stringify({ agents }));
    };

    const list = async (query, { runtimeId = 'rt' } = {}) => {
      const url = `http://127.0.0.1:${server.address().port}`;
      const r = createAdsRouter({ diRuntimes: [{ id: 'rt', url }], samplesDir: tmpDir, isValidAgentId });
      const match = r.stack.find(s => s.route && s.route.path === '/:id/decisions' && s.route.methods.get);
      const out = { status: 200, headers: {} };
      const res = {
        status: (s) => { out.status = s; return res; },
        set: (k, v) => { out.headers[k] = v; return res; },
        json: (d) => { out.body = d; return res; },
      };
      await match.route.stack[0].handle({ params: { id: runtimeId }, query }, res);
      return out;
    };

    const ids = (out) => out.body.map(d => d.id);

    test('a use case bound to one service lists only that service\'s versions', async () => {
      await writeUc('airline', [{ ruleset: { path: 'samples/airline/AirlineDecisionService_1.6.0.jar' } }]);
      const out = await list({ useCaseId: 'airline' });
      expect(out.status).toBe(200);
      expect(ids(out)).toEqual(['p/AirlineDecisionService/1', 'p/AirlineDecisionService/2', 'p/AirlineDecisionService/3']);
      expect(out.body[0]).toMatchObject({ name: 'AirlineDecisionService  1', status: 'deployed' });
    });

    test('unrelated services are never returned', async () => {
      await writeUc('airline', [{ ruleset: { path: 'samples/airline/AirlineDecisionService_1.6.0.jar' } }]);
      const all = ids(await list({ useCaseId: 'airline' })).join(' ');
      for (const unrelated of ['Retail', 'Fraud', 'Other']) expect(all).not.toContain(unrelated);
    });

    test('a use case bound to several services lists the union (duplicates collapse)', async () => {
      await writeUc('multi', [
        { ruleset: { path: 'samples/multi/AirlineDecisionService_1.6.0.jar' } },
        { ruleset: { path: 'samples/multi/RetailDecisionService_2.0.jar' } },
        { ruleset: { path: 'samples/multi/RetailDecisionService_2.0.jar' } },
      ]);
      const out = await list({ useCaseId: 'multi' });
      expect(ids(out)).toEqual([
        'p/AirlineDecisionService/1', 'p/AirlineDecisionService/2', 'p/AirlineDecisionService/3',
        'p/RetailDecisionService/1',
      ]);
    });

    test('a service whose name is a prefix of a bound service is not matched', async () => {
      await writeUc('atc', [{ ruleset: { path: 'samples/atc/Air_Traffic_Control_Service_1.6.1.jar' } }]);
      expect(ids(await list({ useCaseId: 'atc' }))).toEqual(['q/Air_Traffic_Control_Service/1.6.1']);
    });

    test('matches a service whose runtime name contains spaces against the sanitised jar name', async () => {
      listing = ['proj/Fraud Detection Service/1.0', 'proj/Other/1.0'];
      await writeUc('fraud', [{ ruleset: { path: 'samples/fraud/Fraud_Detection_Service_1.6.0.jar' } }]);
      expect(ids(await list({ useCaseId: 'fraud' }))).toEqual(['proj/Fraud Detection Service/1.0']);
    });

    test('valid ruleset.path: only matching archives are returned, project is ignored', async () => {
      await writeUc('uc', [{ ruleset: {
        path: 'samples/uc/AirlineDecisionService_1.6.0.jar',
        project: 'samples/uc/di-project/RetailDecisionService.zip',
      } }]);
      expect(ids(await list({ useCaseId: 'uc' }))).toEqual([
        'p/AirlineDecisionService/1', 'p/AirlineDecisionService/2', 'p/AirlineDecisionService/3',
      ]);
    });

    test('missing ruleset.path, valid ruleset.project: project is the fallback', async () => {
      await writeUc('uc', [{ ruleset: { project: 'samples/uc/di-project/RetailDecisionService.zip' } }]);
      expect(ids(await list({ useCaseId: 'uc' }))).toEqual(['p/RetailDecisionService/1']);
    });

    test('blank ruleset.path counts as absent, so project is the fallback', async () => {
      await writeUc('uc', [{ ruleset: { path: '  ', project: 'di-project/RetailDecisionService.zip' } }]);
      expect(ids(await list({ useCaseId: 'uc' }))).toEqual(['p/RetailDecisionService/1']);
    });

    test('path and project disagree: the path wins', async () => {
      await writeUc('uc', [{ ruleset: {
        path: 'samples/uc/FraudDecisionService_1.0.jar',
        project: 'samples/uc/di-project/OtherDecisionService.zip',
      } }]);
      expect(ids(await list({ useCaseId: 'uc' }))).toEqual(['p/FraudDecisionService/1']);
    });

    test.each([
      ['no ruleset binding at all', [{ ruleset: { archiveControlTopic: 't' } }]],
      ['no ruleset', [{ agentId: 'a' }]],
      ['no agents', []],
      ['an unrecognizable jar name', [{ ruleset: { path: 'samples/uc/rules.jar', project: 'di-project/AirlineDecisionService.zip' } }]],
      ['a non-jar path', [{ ruleset: { path: 'samples/uc/AirlineDecisionService_1.0.zip' } }]],
      ['a non-string path', [{ ruleset: { path: 42, project: 'di-project/AirlineDecisionService.zip' } }]],
    ])('fails closed with [] for %s, without querying the runtime', async (_label, agents) => {
      await writeUc('uc', agents);
      const out = await list({ useCaseId: 'uc' });
      expect(out.status).toBe(200);
      expect(out.body).toEqual([]);
      expect(listCalls).toBe(0);
    });

    test('returns [] when the runtime has no version of the bound service', async () => {
      listing = ['p/RetailDecisionService/1', 'p/FraudDecisionService/1'];
      await writeUc('airline', [{ ruleset: { path: 'samples/airline/AirlineDecisionService_1.6.0.jar' } }]);
      const out = await list({ useCaseId: 'airline' });
      expect(out.status).toBe(200);
      expect(out.body).toEqual([]);
    });

    test('decisions without a service segment are never listed for a use case', async () => {
      listing = ['AirlineDecisionService', 'p/AirlineDecisionService/1'];
      await writeUc('airline', [{ ruleset: { path: 'samples/airline/AirlineDecisionService_1.6.0.jar' } }]);
      expect(ids(await list({ useCaseId: 'airline' }))).toEqual(['p/AirlineDecisionService/1']);
    });

    test('without useCaseId the full list is returned (backward compatible)', async () => {
      const out = await list({});
      expect(ids(out)).toEqual(ALL);
    });

    test.each(['../etc', 'a/b', ''])('rejects the invalid useCaseId %p with 400, no listing is exposed', async (bad) => {
      const out = await list({ useCaseId: bad });
      expect(out.status).toBe(400);
      expect(out.body.error).toBeTruthy();
      expect(listCalls).toBe(0);
    });

    test('rejects a repeated useCaseId parameter with 400', async () => {
      const out = await list({ useCaseId: ['a', 'b'] });
      expect(out.status).toBe(400);
      expect(listCalls).toBe(0);
    });

    test('an unknown use case is a 404, never the full list', async () => {
      const out = await list({ useCaseId: 'missing' });
      expect(out.status).toBe(404);
      expect(out.body.error).toMatch(/agents\.json not found/);
      expect(listCalls).toBe(0);
    });

    test('a malformed agents.json fails closed with []', async () => {
      await fs.mkdir(path.join(tmpDir, 'uc'), { recursive: true });
      await fs.writeFile(path.join(tmpDir, 'uc', 'agents.json'), '{ not json');
      const out = await list({ useCaseId: 'uc' });
      expect(out.body).toEqual([]);
      expect(listCalls).toBe(0);
    });

    test('an unknown runtime is still a 404', async () => {
      const out = await list({ useCaseId: 'uc' }, { runtimeId: 'nope' });
      expect(out.status).toBe(404);
    });

    test('other use cases are unaffected by each other\'s binding', async () => {
      await writeUc('airline', [{ ruleset: { path: 'samples/airline/AirlineDecisionService_1.6.0.jar' } }]);
      await writeUc('retail', [{ ruleset: { path: 'samples/retail/RetailDecisionService_1.6.0.jar' } }]);
      expect(ids(await list({ useCaseId: 'retail' }))).toEqual(['p/RetailDecisionService/1']);
      expect(ids(await list({ useCaseId: 'airline' })).every(i => i.includes('Airline'))).toBe(true);
    });
  });

  // Regression: the ids a real DI runtime returns are
  //   <tenant>/<repository>/<service>/<service>DecisionService/<version>-<build timestamp>
  // (taken from META-INF/decisionServiceArchive.json of the sample jars). The service used to be read
  // from the second-to-last segment ("retailPromotionServiceDecisionService"), so no version of a
  // correctly bound use case matched and the UI said "No version of this use case's decision service".
  describe('real runtime decision ids (Retail — Promotion Agent regression)', () => {
    const http = require('http');
    const { boundServices, isDecisionBound, resolveRulesetService } = require('./di_route');
    let server;
    let listing;
    let listCalls;

    const R = (svc, ver, ts = '2026-11-12T13:43:37.259Z') => {
      const lower = svc.replace(/[^A-Za-z0-9]/g, '').toLowerCase();
      const lowerFirst = svc.replace(/[^A-Za-z0-9]/g, '').replace(/^./, c => c.toLowerCase());
      return `cpmanager/banking_demo/${lower}/${lowerFirst}DecisionService/${ver}-${ts}`;
    };
    const RETAIL_V1 = R('RetailPromotionService', '1.0.0', '2026-10-01T08:00:00.000Z');
    const RETAIL_V2 = R('RetailPromotionService', '1.5.0', '2026-11-01T08:00:00.000Z');
    const RETAIL_V3 = R('RetailPromotionService', '1.6.0');
    const AIRLINE   = R('AirlineDecisionService', '1.6.0', '2026-11-09T12:16:25.033Z');
    const FRAUD     = R('Fraud Detection Service', '1.6.0', '2026-11-12T13:43:31.785Z');
    const ATC       = R('Air Traffic Control Service', '1.6.1', '2026-11-13T23:04:19.716Z');

    beforeEach(async () => {
      listCalls = 0;
      listing = [AIRLINE, RETAIL_V1, FRAUD, RETAIL_V2, ATC, RETAIL_V3];
      server = http.createServer((req, res) => {
        listCalls += 1;
        res.setHeader('Content-Type', 'application/json');
        res.end(JSON.stringify(listing));
      });
      await new Promise(r => server.listen(0, '127.0.0.1', r));
    });

    afterEach(async () => { await new Promise(r => server.close(r)); });

    const writeUc = async (id, agents) => {
      await fs.mkdir(path.join(tmpDir, id), { recursive: true });
      await fs.writeFile(path.join(tmpDir, id, 'agents.json'), JSON.stringify({ agents }));
    };
    const call = async (routePath, method, req) => {
      const url = `http://127.0.0.1:${server.address().port}`;
      const published = [];
      const r = createAdsRouter({
        diRuntimes: [{ id: 'rt', url }], samplesDir: tmpDir, isValidAgentId,
        publishControlMessage: async (topic, value) => { published.push({ topic, value }); },
        awaitArchiveAcks: fakeWaiter(), ackTimeoutMs: 50,
      });
      const match = r.stack.find(s => s.route && s.route.path === routePath && s.route.methods[method]);
      const out = { status: 200, headers: {}, published };
      const res = {
        status: (s) => { out.status = s; return res; },
        set: (k, v) => { out.headers[k] = v; return res; },
        json: (d) => { out.body = d; return res; },
      };
      await match.route.stack[0].handle({ params: { id: 'rt' }, ...req }, res);
      return out;
    };
    const list = (query) => call('/:id/decisions', 'get', { query });
    const looked = (out) => JSON.parse(decodeURIComponent(out.headers['X-Decision-Services']));
    const refresh = (query) => call('/:id/refresh-rules', 'post', { query });
    const RETAIL_AGENT = { agentId: 'retail', ruleset: { path: 'samples/retail-agent/RetailPromotionService_1.6.0.jar',
      project: 'samples/retail-agent/di-project/RetailPromotionService.zip', archiveControlTopic: 'agent-retail-archive-control' } };

    test('Retail — Promotion Agent: every version of its Decision Service is returned', async () => {
      await writeUc('retail-agent', [RETAIL_AGENT]);
      const out = await list({ useCaseId: 'retail-agent' });
      expect(out.status).toBe(200);
      expect(out.body.map(d => d.id).sort()).toEqual([RETAIL_V1, RETAIL_V2, RETAIL_V3].sort());
      expect(looked(out)).toEqual(['RetailPromotionService']);
    });

    test('each version exposes service, version and build parsed from the real id', async () => {
      await writeUc('retail-agent', [RETAIL_AGENT]);
      const out = await list({ useCaseId: 'retail-agent' });
      const v3 = out.body.find(d => d.id === RETAIL_V3);
      expect(v3).toMatchObject({
        service: 'retailpromotionservice', version: '1.6.0', build: '2026-11-12T13:43:37.259Z',
        group: 'banking_demo', status: 'deployed',
      });
      expect(out.body.map(d => d.version).sort()).toEqual(['1.0.0', '1.5.0', '1.6.0']);
    });

    test('unrelated Decision Services (Airline, Fraud, Air Traffic) are excluded', async () => {
      await writeUc('retail-agent', [RETAIL_AGENT]);
      const all = (await list({ useCaseId: 'retail-agent' })).body.map(d => d.id).join(' ').toLowerCase();
      for (const other of ['airline', 'fraud', 'airtraffic', 'air_traffic']) expect(all).not.toContain(other);
    });

    test.each([
      ['airline-agent', 'samples/airline-agent/AirlineDecisionService_1.6.0.jar', [AIRLINE]],
      ['banking-fraud-agent', 'samples/banking-fraud-agent/Fraud_Detection_Service_1.6.0.jar', [FRAUD]],
      ['airtraffic-agent', 'samples/airtraffic-agent/Air_Traffic_Control_Service_1.6.1.jar', [ATC]],
    ])('%s sees only its own service', async (uc, jar, expected) => {
      await writeUc(uc, [{ ruleset: { path: jar } }]);
      expect((await list({ useCaseId: uc })).body.map(d => d.id)).toEqual(expected);
    });

    test('a bound service without any version on the runtime: [] and the looked-for service is reported', async () => {
      listing = [AIRLINE, FRAUD, ATC];
      await writeUc('retail-agent', [RETAIL_AGENT]);
      const out = await list({ useCaseId: 'retail-agent' });
      expect(out.status).toBe(200);
      expect(out.body).toEqual([]);
      expect(looked(out)).toEqual(['RetailPromotionService']); // UI: "looked for X", not "no binding"
    });

    test('no binding: [] and an empty service list is reported (distinct from "no versions")', async () => {
      await writeUc('retail-agent', [{ ruleset: { archiveControlTopic: 't' } }]);
      const out = await list({ useCaseId: 'retail-agent' });
      expect(out.body).toEqual([]);
      expect(looked(out)).toEqual([]);
      expect(listCalls).toBe(0);
    });

    test('several bound services: versions of all of them, nothing else', async () => {
      await writeUc('multi', [
        RETAIL_AGENT,
        { agentId: 'fraud', ruleset: { path: 'samples/multi/Fraud_Detection_Service_1.6.0.jar' } },
      ]);
      const out = await list({ useCaseId: 'multi' });
      expect(out.body.map(d => d.id).sort()).toEqual([FRAUD, RETAIL_V1, RETAIL_V2, RETAIL_V3].sort());
      expect(looked(out)).toEqual(['RetailPromotionService', 'Fraud_Detection_Service']);
    });

    test('plain <project>/<service>/<version> ids keep working', async () => {
      listing = ['proj/RetailPromotionService/1', 'proj/RetailPromotionService/2', 'proj/Other/1'];
      await writeUc('retail-agent', [RETAIL_AGENT]);
      const out = await list({ useCaseId: 'retail-agent' });
      expect(out.body.map(d => d.id)).toEqual(['proj/RetailPromotionService/1', 'proj/RetailPromotionService/2']);
      expect(out.body[0]).toMatchObject({ service: 'RetailPromotionService', version: '1', build: '' });
    });

    test('a different service whose name merely contains the bound one is not matched', async () => {
      listing = [R('RetailPromotionServiceLegacy', '1.0.0'), R('Promotion', '1.0.0'), RETAIL_V3];
      await writeUc('retail-agent', [RETAIL_AGENT]);
      expect((await list({ useCaseId: 'retail-agent' })).body.map(d => d.id)).toEqual([RETAIL_V3]);
    });

    test('without useCaseId the full list is returned and carries the parsed identity', async () => {
      const out = await list({});
      expect(out.body).toHaveLength(6);
      expect(out.body.find(d => d.id === AIRLINE)).toMatchObject({ service: 'airlinedecisionservice', version: '1.6.0' });
    });

    test('refresh-rules accepts any version of the bound service, rejects unrelated ones', async () => {
      await writeUc('retail-agent', [RETAIL_AGENT]);
      for (const id of [RETAIL_V1, RETAIL_V2, RETAIL_V3]) {
        const ok = await refresh({ decisionId: id, useCaseId: 'retail-agent' });
        expect(ok.status).toBe(200);
      }
      const bad = await refresh({ decisionId: AIRLINE, useCaseId: 'retail-agent' });
      expect(bad.status).toBe(403);
    });

    describe('binding from archives saved by the editor / refresh (named after the decision id)', () => {
      // downloadArchive names the file <segment[-2]>_<segment[-1]> sanitised, e.g.
      // retailPromotionServiceDecisionService_1.6.0-2026-11-12T13_43_37.259Z.jar
      const saved = 'samples/retail-agent/retailPromotionServiceDecisionService_1.6.0-2026-11-12T13_43_37.259Z.jar';

      test('resolves to the service, not to "<service>DecisionService_<version>…"', () => {
        expect(resolveRulesetService({ path: saved })).toBe('retailPromotionService');
        expect(isDecisionBound(RETAIL_V1, boundServices({ agents: [{ ruleset: { path: saved } }] }))).toBe(true);
        expect(isDecisionBound(AIRLINE, boundServices({ agents: [{ ruleset: { path: saved } }] }))).toBe(false);
      });

      test('a refresh keeps the use case bound to the same service', async () => {
        await writeUc('retail-agent', [{ agentId: 'retail', ruleset: { path: saved, archiveControlTopic: 't' } }]);
        expect((await list({ useCaseId: 'retail-agent' })).body).toHaveLength(3);
        expect((await refresh({ decisionId: RETAIL_V3, useCaseId: 'retail-agent' })).status).toBe(200);
      });
    });

    test.each([
      ['RetailPromotionService_1.6.0.jar', 'RetailPromotionService'],
      ['Air_Traffic_Control_Service_1.6.1.jar', 'Air_Traffic_Control_Service'],
      ['Fraud_Detection_Service_1.6.0-SNAPSHOT.jar', 'Fraud_Detection_Service'],
      ['svc_1.0.0-2026-11-12T13_43_37.259Z.jar', 'svc'],
    ])('jar name %s resolves to %s', (jar, expected) => {
      expect(resolveRulesetService({ path: `samples/x/${jar}` })).toBe(expected);
    });
  });

  describe('listing and refresh-rules share one resolution (consistency)', () => {
    const { boundServices, isDecisionBound, resolveRulesetService } = require('./di_route');

    test.each([
      [{ path: 'samples/a/Svc_1.2.3.jar' }, 'Svc'],
      [{ path: 'samples/a/Svc_v2.jar' }, 'Svc'],
      [{ path: 'samples/a/My_Service_Name_1.6.1.jar' }, 'My_Service_Name'],
      [{ path: 'C:\\x\\Svc_1.0.jar' }, 'Svc'],
      [{ project: 'di-project/Svc.zip' }, 'Svc'],
      [{ path: 'samples/a/rules.jar' }, null],
      [{ path: 'samples/a/Svc_final.jar' }, null],
      [{ path: 'samples/a/Svc_1.0.war' }, null],
      [{ path: 'samples/a/Svc_1.0.jar/' }, 'Svc'],
      [{}, null],
      [null, null],
      ['x', null],
    ])('resolveRulesetService(%j) = %p', (ruleset, expected) => {
      expect(resolveRulesetService(ruleset)).toBe(expected);
    });

    test('a decision is bound exactly when the listing would include it', () => {
      const services = boundServices({ agents: [{ ruleset: { path: 'x/Air_Traffic_Control_Service_1.6.1.jar' } }] });
      expect(isDecisionBound('q/Air_Traffic_Control_Service/1.6.1', services)).toBe(true);
      expect(isDecisionBound('q/Air/1', services)).toBe(false);
      expect(isDecisionBound('Air_Traffic_Control_Service', services)).toBe(false);
      expect(isDecisionBound('', services)).toBe(false);
    });
  });

  describe('real sample use cases keep a resolvable binding', () => {
    const { boundServices } = require('./di_route');
    const samplesRoot = path.resolve(__dirname, '../../../samples');
    const fsSync = require('fs');
    const useCases = fsSync.existsSync(samplesRoot)
      ? fsSync.readdirSync(samplesRoot).filter(n => fsSync.existsSync(path.join(samplesRoot, n, 'agents.json')))
      : [];

    test('samples exist', () => { expect(useCases.length).toBeGreaterThan(0); });

    test.each(useCases)('%s resolves to at least one Decision Service', (name) => {
      const descriptor = JSON.parse(fsSync.readFileSync(path.join(samplesRoot, name, 'agents.json'), 'utf8'));
      expect(boundServices(descriptor).length).toBeGreaterThan(0);
    });

    test('airline-agent is bound to AirlineDecisionService only', () => {
      const descriptor = JSON.parse(fsSync.readFileSync(path.join(samplesRoot, 'airline-agent', 'agents.json'), 'utf8'));
      expect(boundServices(descriptor)).toEqual(['AirlineDecisionService']);
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

    test('sends the Bearer token as "Authorization: Bearer <token>" when token is configured', async () => {
      const url = `http://127.0.0.1:${server.address().port}`;
      // pragma: allowlist nextline secret (dummy test fixture)
      await listDecisions({ id: 'rt', url, token: 'jwt-or-bearer-token' });
      expect(seenAuth).toBe('Bearer jwt-or-bearer-token');
    });

    test('prefers token over apiKey if both are configured', async () => {
      const url = `http://127.0.0.1:${server.address().port}`;
      // pragma: allowlist nextline secret (dummy test fixture)
      await listDecisions({ id: 'rt', url, username: 'cpadmin', apiKey: 'abc123', token: 'jwt-or-bearer-token' });
      expect(seenAuth).toBe('Bearer jwt-or-bearer-token');
    });

    test('sends no Authorization header when credentials are missing', async () => {
      const url = `http://127.0.0.1:${server.address().port}`;
      await listDecisions({ id: 'rt', url, username: 'cpadmin' });
      expect(seenAuth).toBeUndefined();
    });
  });
});

// ── Active archive: acknowledgement, agents.json sync, restore default ────────────────────────
describe('active archive tracking (acknowledged loads, agents.json, restore default)', () => {
  const http = require('http');
  const crypto = require('crypto');
  const ARCHIVE = Buffer.from('fake-jar-content');
  const DEFAULT_JAR = 'samples/uc/Svc_1.0.jar';
  const DECISION = 'tenant/repo/svc/svcDecisionService/2.0-2026-11-12T13:43:37.259Z';
  const LOADED_FILE = 'svcDecisionService_2.0-2026-11-12T13_43_37.259Z.jar';
  const isValidAgentId = (id) => typeof id === 'string' && /^[a-zA-Z0-9_-]{1,64}$/.test(id);

  let tmpDir; let server; let published; let waits;

  beforeEach(async () => {
    tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), 'ads-active-'));
    published = []; waits = [];
    server = http.createServer((req, res) => {
      res.setHeader('Content-Type', 'application/json');
      res.end(req.url.includes('/archive') ? ARCHIVE : JSON.stringify([DECISION]));
    });
    await new Promise(r => server.listen(0, '127.0.0.1', r));
  });
  afterEach(async () => {
    await new Promise(r => server.close(r));
    await fs.rm(tmpDir, { recursive: true, force: true });
  });

  const writeUc = async (agents, extra = {}) => {
    await fs.mkdir(path.join(tmpDir, 'uc'), { recursive: true });
    await fs.writeFile(path.join(tmpDir, 'uc', 'agents.json'), JSON.stringify({ useCase: 'uc', ...extra, agents }, null, 2));
  };
  const agentsJson = async () => JSON.parse(await fs.readFile(path.join(tmpDir, 'uc', 'agents.json'), 'utf8'));
  const AGENTS = () => [
    { agentId: 'a', ruleset: { path: DEFAULT_JAR, archiveControlTopic: 't', operations: [{ name: 'op' }] } },
    { agentId: 'b', ruleset: { path: DEFAULT_JAR, archiveControlTopic: 't' } },
  ];

  const call = async (routePath, method, req, { waiter = fakeWaiter('LOADED', { record: waits }), publish } = {}) => {
    const url = `http://127.0.0.1:${server.address().port}`;
    const r = createAdsRouter({
      diRuntimes: [{ id: 'rt', url }], samplesDir: tmpDir, isValidAgentId,
      publishControlMessage: publish || (async (topic, value) => { published.push({ topic, value }); }),
      awaitArchiveAcks: waiter, ackTimeoutMs: 50,
    });
    const match = r.stack.find(s => s.route && s.route.path === routePath && s.route.methods[method]);
    const out = { status: 200, headers: {} };
    const res = {
      status: (s) => { out.status = s; return res; },
      set: (k, v) => { out.headers[k] = v; return res; },
      json: (d) => { out.body = d; return res; },
    };
    await match.route.stack[0].handle({ params: { id: 'rt' }, query: {}, ...req }, res);
    return out;
  };
  const refresh = (opts, updateAgentsJson = 'true') => call(
    '/:id/refresh-rules', 'post',
    { query: { decisionId: DECISION, useCaseId: 'uc', ...(updateAgentsJson === null ? {} : { updateAgentsJson }) } },
    opts,
  );
  const restore = (opts) => call('/restore-default', 'post', { query: { useCaseId: 'uc' } }, opts);
  const status = () => call('/rules-status', 'get', { query: { useCaseId: 'uc' } });

  describe('refresh-rules', () => {
    test('waits for the acknowledgement of every agent BEFORE publishing', async () => {
      await writeUc(AGENTS());
      const order = [];
      const out = await refresh({
        waiter: async (args) => { order.push('listen'); waits.push(args); return fakeWaiter()(args); },
        publish: async () => { order.push('publish'); },
      });
      expect(out.status).toBe(200);
      expect(order).toEqual(['listen', 'publish']);
      expect(waits[0].expectedAgents).toEqual(['a', 'b']);
      expect(waits[0].topics).toEqual(['t']);
    });

    test('a confirmed load writes ruleset.path and keeps the shipped archive as defaultPath', async () => {
      await writeUc(AGENTS(), { extra: 'kept' });
      const out = await refresh();
      expect(out.status).toBe(200);
      expect(out.body).toMatchObject({ refreshed: true, state: 'loaded', path: `samples/uc/${LOADED_FILE}`, agents: ['a', 'b'] });

      const json = await agentsJson();
      expect(json.extra).toBe('kept');
      for (const a of json.agents) {
        expect(a.ruleset.path).toBe(`samples/uc/${LOADED_FILE}`);
        expect(a.ruleset.defaultPath).toBe(DEFAULT_JAR);
      }
      expect(json.agents[0].ruleset.operations).toEqual([{ name: 'op' }]); // untouched
      expect(JSON.parse(published[0].value).sha256).toBe(crypto.createHash('sha256').update(ARCHIVE).digest('hex'));
    });

    test('updateAgentsJson=false: job loads the archive, agents.json byte-identical, jar discarded', async () => {
      await writeUc(AGENTS());
      const before = await fs.readFile(path.join(tmpDir, 'uc', 'agents.json'), 'utf8');
      const out = await refresh(undefined, 'false');
      expect(out.status).toBe(200);
      expect(out.body).toMatchObject({
        refreshed: true, state: 'loaded', agentsUpdated: false, archiveDeleted: true, path: `samples/uc/${LOADED_FILE}`,
      });
      expect(published).toHaveLength(1);
      await expect(fs.access(path.join(tmpDir, 'uc', LOADED_FILE))).rejects.toThrow();
      expect(await fs.readFile(path.join(tmpDir, 'uc', 'agents.json'), 'utf8')).toBe(before);
    });

    test('updateAgentsJson omitted defaults to leaving agents.json untouched and discarding the jar', async () => {
      await writeUc(AGENTS());
      const before = await fs.readFile(path.join(tmpDir, 'uc', 'agents.json'), 'utf8');
      const out = await refresh(undefined, null);
      expect(out.status).toBe(200);
      expect(out.body).toMatchObject({ agentsUpdated: false, archiveDeleted: true });
      await expect(fs.access(path.join(tmpDir, 'uc', LOADED_FILE))).rejects.toThrow();
      expect(await fs.readFile(path.join(tmpDir, 'uc', 'agents.json'), 'utf8')).toBe(before);
    });

    test('updateAgentsJson=true reports agentsUpdated and writes ruleset.path', async () => {
      await writeUc(AGENTS());
      const out = await refresh(undefined, 'true');
      expect(out.body.agentsUpdated).toBe(true);
      expect((await agentsJson()).agents[0].ruleset.path).toBe(`samples/uc/${LOADED_FILE}`);
    });

    describe('archive lifecycle: persistence and no premature deletion', () => {
      const jars = async () => (await fs.readdir(path.join(tmpDir, 'uc'))).filter(f => f.endsWith('.jar')).sort();
      const SHA8 = crypto.createHash('sha256').update(ARCHIVE).digest('hex').slice(0, 8);

      test('updateAgentsJson=true: the loaded archive AND agents.json are kept, byte for byte', async () => {
        await writeUc(AGENTS());
        await fs.writeFile(path.join(tmpDir, 'uc', 'Svc_1.0.jar'), 'default-jar');
        const out = await refresh(undefined, 'true');
        expect(out.status).toBe(200);
        expect(out.body.archiveDeleted).toBeUndefined();
        expect(await jars()).toEqual([LOADED_FILE, 'Svc_1.0.jar'].sort());
        expect(await fs.readFile(path.join(tmpDir, 'uc', LOADED_FILE))).toEqual(ARCHIVE);
        expect(await fs.readFile(path.join(tmpDir, 'uc', 'Svc_1.0.jar'), 'utf8')).toBe('default-jar');
        expect((await agentsJson()).agents[0].ruleset.path).toBe(`samples/uc/${LOADED_FILE}`);
      });

      test('updateAgentsJson=false: only the archive of this refresh is removed, the shipped one stays', async () => {
        await writeUc(AGENTS());
        await fs.writeFile(path.join(tmpDir, 'uc', 'Svc_1.0.jar'), 'default-jar');
        const out = await refresh(undefined, 'false');
        expect(out.body.archiveDeleted).toBe(true);
        expect(await jars()).toEqual(['Svc_1.0.jar']);
        expect(await fs.readFile(path.join(tmpDir, 'uc', 'Svc_1.0.jar'), 'utf8')).toBe('default-jar');
      });

      test('updateAgentsJson=false: a jar that already existed (identical) is kept', async () => {
        await writeUc(AGENTS());
        await fs.writeFile(path.join(tmpDir, 'uc', LOADED_FILE), ARCHIVE);
        const out = await refresh(undefined, 'false');
        expect(out.status).toBe(200);
        expect(out.body.archiveDeleted).toBe(false);
        expect(await jars()).toEqual([LOADED_FILE]);
      });

      test('updateAgentsJson=false: a jar still referenced by agents.json is never deleted', async () => {
        await writeUc(AGENTS());
        const waiter = async (args) => { // agents.json edited to point at the new jar while loading
          const json = await agentsJson();
          json.agents.forEach(a => { a.ruleset.path = `samples/uc/${LOADED_FILE}`; });
          await fs.writeFile(path.join(tmpDir, 'uc', 'agents.json'), JSON.stringify(json));
          return fakeWaiter()(args);
        };
        const out = await refresh({ waiter }, 'false');
        expect(out.status).toBe(200);
        expect(out.body.archiveDeleted).toBe(false);
        expect(await jars()).toEqual([LOADED_FILE]);
      });

      test('updateAgentsJson=false: a deletion failure does not fail the confirmed refresh', async () => {
        await writeUc(AGENTS());
        const waiter = async (args) => { // jar removed behind our back -> unlink fails with ENOENT
          await fs.rm(path.join(tmpDir, 'uc', LOADED_FILE), { force: true });
          return fakeWaiter()(args);
        };
        const out = await refresh({ waiter }, 'false');
        expect(out.status).toBe(200);
        expect(out.body).toMatchObject({ refreshed: true, state: 'loaded', archiveDeleted: false });
      });

      test('the archive is deleted only AFTER the job confirmed (still present while waiting)', async () => {
        await writeUc(AGENTS());
        let presentWhileWaiting;
        const waiter = async (args) => {
          const w = await fakeWaiter()(args);
          return { ...w, done: async () => {
            presentWhileWaiting = await fs.access(path.join(tmpDir, 'uc', LOADED_FILE)).then(() => true, () => false);
            return w.done();
          } };
        };
        await refresh({ waiter }, 'false');
        expect(presentWhileWaiting).toBe(true);
        expect(await jars()).toEqual([]);
      });

      test('a second refresh keeps the archive loaded by the first one (rollback target)', async () => {
        await writeUc(AGENTS());
        await refresh();
        const first = await jars();
        expect(first).toEqual([LOADED_FILE]);
        const json = await agentsJson();
        json.agents.forEach(a => { a.ruleset.path = `samples/uc/${LOADED_FILE}`; });
        await fs.writeFile(path.join(tmpDir, 'uc', 'agents.json'), JSON.stringify(json));
        await refresh();
        expect(await jars()).toEqual(first); // same content: reused, nothing removed or duplicated
      });

      test('an archive with the same name but other content is never overwritten', async () => {
        await writeUc(AGENTS());
        await fs.writeFile(path.join(tmpDir, 'uc', LOADED_FILE), 'previous-build');
        const out = await refresh();
        const renamed = LOADED_FILE.replace(/\.jar$/, `-${SHA8}.jar`);
        expect(out.status).toBe(200);
        expect(out.body.filename).toBe(renamed);
        expect(await fs.readFile(path.join(tmpDir, 'uc', LOADED_FILE), 'utf8')).toBe('previous-build');
        expect(await fs.readFile(path.join(tmpDir, 'uc', renamed))).toEqual(ARCHIVE);
        expect(JSON.parse(published[0].value).archiveUrl).toMatch(new RegExp(`${renamed}$`));
        expect((await agentsJson()).agents[0].ruleset.path).toBe(`samples/uc/${renamed}`);
      });

      test.each([
        ['rejected', fakeWaiter('FAILED', { error: 'boom' })],
        ['partial', fakeWaiter(['LOADED', 'FAILED'], { subtasks: 2, error: 'boom' })],
        ['timeout', async () => ({ done: async () => [], close: async () => {} })],
      ])('%s load (updateAgentsJson=false): nothing is deleted, previous archive intact, agents.json unchanged', async (_l, waiter) => {
        await writeUc(AGENTS());
        await fs.writeFile(path.join(tmpDir, 'uc', 'Svc_1.0.jar'), 'default-jar');
        const before = await fs.readFile(path.join(tmpDir, 'uc', 'agents.json'), 'utf8');
        const out = await refresh({ waiter }, 'false');
        expect(out.body.archiveDeleted).toBeUndefined();
        expect(out.status).toBeGreaterThanOrEqual(500);
        // the job may still load the file late, so it must remain; the running one is untouched
        expect(await jars()).toEqual([LOADED_FILE, 'Svc_1.0.jar'].sort());
        expect(await fs.readFile(path.join(tmpDir, 'uc', 'Svc_1.0.jar'), 'utf8')).toBe('default-jar');
        expect(await fs.readFile(path.join(tmpDir, 'uc', 'agents.json'), 'utf8')).toBe(before);
      });

      test('a failed agents.json write leaves the loaded archive on disk and flags out-of-sync', async () => {
        await writeUc(AGENTS());
        // make the descriptor unreadable between the first read and the persist step
        const waiter = async (args) => {
          await fs.writeFile(path.join(tmpDir, 'uc', 'agents.json'), '{ broken');
          return fakeWaiter()(args);
        };
        const out = await refresh({ waiter });
        expect(out.status).toBe(500);
        expect(out.body).toMatchObject({ state: 'loaded', outOfSync: true });
        expect(await fs.readFile(path.join(tmpDir, 'uc', LOADED_FILE))).toEqual(ARCHIVE);
      });

      test('no temporary files are left behind in the use-case folder', async () => {
        await writeUc(AGENTS());
        await refresh();
        const files = await fs.readdir(path.join(tmpDir, 'uc'));
        expect(files.filter(f => f.endsWith('.tmp'))).toEqual([]);
      });
    });

    test('defaultPath is set once: a second refresh does not overwrite it', async () => {
      await writeUc(AGENTS());
      await refresh();
      const json = await agentsJson();
      json.agents.forEach(a => { a.ruleset.path = 'samples/uc/Other_9.9.jar'; });
      await fs.writeFile(path.join(tmpDir, 'uc', 'agents.json'), JSON.stringify(json));
      await refresh();
      expect((await agentsJson()).agents[0].ruleset.defaultPath).toBe(DEFAULT_JAR);
    });

    test('every agent on a published control topic must acknowledge', async () => {
      await writeUc([
        { agentId: 'a', ruleset: { path: DEFAULT_JAR, archiveControlTopic: 't' } },
        { agentId: 'c', ruleset: { path: DEFAULT_JAR, archiveControlTopic: 'other' } },
      ]);
      const out = await refresh();
      expect(out.body.agents).toEqual(['a', 'c']);
    });

    test.each([
      ['the job rejects the archive', fakeWaiter('FAILED', { error: 'SHA-256 mismatch' }), 502, 'failed', /rejected the archive.*SHA-256 mismatch/s],
      ['only some parallel instances load it', fakeWaiter(['LOADED', 'FAILED'], { subtasks: 2, error: 'boom' }), 502, 'partial', /only some/],
    ])('%s: agents.json is left untouched', async (_label, waiter, httpStatus, state, message) => {
      await writeUc(AGENTS());
      const before = await fs.readFile(path.join(tmpDir, 'uc', 'agents.json'), 'utf8');
      const out = await refresh({ waiter });
      expect(out.status).toBe(httpStatus);
      expect(out.body.state).toBe(state);
      expect(out.body.error).toMatch(message);
      expect(await fs.readFile(path.join(tmpDir, 'uc', 'agents.json'), 'utf8')).toBe(before);
    });

    test('a partial load is flagged as out of sync', async () => {
      await writeUc(AGENTS());
      const out = await refresh({ waiter: fakeWaiter(['LOADED', 'FAILED'], { subtasks: 2, error: 'boom' }) });
      expect(out.body.outOfSync).toBe(true);
    });

    test('no acknowledgement in time: 504 and agents.json is left untouched', async () => {
      await writeUc(AGENTS());
      const before = await fs.readFile(path.join(tmpDir, 'uc', 'agents.json'), 'utf8');
      const silent = async () => ({ done: async () => [], close: async () => {} });
      const out = await refresh({ waiter: silent });
      expect(out.status).toBe(504);
      expect(out.body.state).toBe('timeout');
      expect(out.body.missing).toEqual(['a', 'b']);
      expect(await fs.readFile(path.join(tmpDir, 'uc', 'agents.json'), 'utf8')).toBe(before);
    });

    test('one agent silent: still a timeout, nothing written', async () => {
      await writeUc(AGENTS());
      const before = await fs.readFile(path.join(tmpDir, 'uc', 'agents.json'), 'utf8');
      const onlyA = async (args) => ({
        done: async () => [{ agentId: 'a', requestId: args.requestId, subtask: 0, parallelism: 1, status: 'LOADED' }],
        close: async () => {},
      });
      const out = await refresh({ waiter: onlyA });
      expect(out.status).toBe(504);
      expect(out.body.missing).toEqual(['b']);
      expect(await fs.readFile(path.join(tmpDir, 'uc', 'agents.json'), 'utf8')).toBe(before);
    });

    test('a publish failure is a 502, agents.json untouched, listener closed', async () => {
      await writeUc(AGENTS());
      const before = await fs.readFile(path.join(tmpDir, 'uc', 'agents.json'), 'utf8');
      let closed = false;
      const out = await refresh({
        waiter: async () => ({ done: async () => [], close: async () => { closed = true; } }),
        publish: async () => { throw new Error('kafka down'); },
      });
      expect(out.status).toBe(502);
      expect(out.body.error).toMatch(/kafka down/);
      expect(closed).toBe(true);
      expect(await fs.readFile(path.join(tmpDir, 'uc', 'agents.json'), 'utf8')).toBe(before);
    });

    test('an unavailable listener is a 502 and nothing is published', async () => {
      await writeUc(AGENTS());
      const out = await refresh({ waiter: async () => { throw new Error('no broker'); } });
      expect(out.status).toBe(502);
      expect(published).toHaveLength(0);
    });

    test('503 without an acknowledgement listener', async () => {
      await writeUc(AGENTS());
      const out = await refresh({ waiter: null });
      expect(out.status).toBe(503);
    });

    test('a second refresh of the same use case while one runs is a 409', async () => {
      await writeUc(AGENTS());
      let release;
      const gate = new Promise(r => { release = r; });
      const url = `http://127.0.0.1:${server.address().port}`;
      const r = createAdsRouter({
        diRuntimes: [{ id: 'rt', url }], samplesDir: tmpDir, isValidAgentId,
        publishControlMessage: async () => {},
        awaitArchiveAcks: async (args) => ({ done: async () => { await gate; return fakeWaiter()(args).then(w => w.done()); }, close: async () => {} }),
        ackTimeoutMs: 1000,
      });
      const handler = r.stack.find(s => s.route && s.route.path === '/:id/refresh-rules').route.stack[0].handle;
      const mk = () => { const o = { status: 200 }; const res = { status: (s) => { o.status = s; return res; }, json: (d) => { o.body = d; return res; } }; return { o, res }; };
      const first = mk(); const second = mk();
      const running = handler({ params: { id: 'rt' }, query: { decisionId: DECISION, useCaseId: 'uc' } }, first.res);
      await new Promise(r2 => setTimeout(r2, 50));
      await handler({ params: { id: 'rt' }, query: { decisionId: DECISION, useCaseId: 'uc' } }, second.res);
      expect(second.o.status).toBe(409);
      release(); await running;
      expect(first.o.status).toBe(200);
    });

    test('403 for an unrelated archive is unchanged and publishes nothing', async () => {
      await writeUc(AGENTS());
      const out = await call('/:id/refresh-rules', 'post', { query: { decisionId: 'tenant/repo/other/otherDecisionService/1.0-2026-11-12T13:43:37.259Z', useCaseId: 'uc' } });
      expect(out.status).toBe(403);
      expect(published).toHaveLength(0);
    });

    test('the shipped service stays bound after a version of it was loaded', async () => {
      await writeUc(AGENTS());
      await refresh();
      const out = await call('/:id/decisions', 'get', { query: { useCaseId: 'uc' } });
      expect(out.body.map(d => d.id)).toEqual([DECISION]);
    });
  });

  describe('restore-default', () => {
    const afterRefresh = async () => {
      await writeUc(AGENTS());
      await fs.writeFile(path.join(tmpDir, 'uc', 'Svc_1.0.jar'), 'default-jar');
      await refresh();
      published.length = 0; waits.length = 0;
    };

    test('reloads the default jar and writes path back, keeping defaultPath', async () => {
      await afterRefresh();
      const out = await restore();
      expect(out.status).toBe(200);
      expect(out.body).toMatchObject({ restored: true, state: 'loaded', path: DEFAULT_JAR });
      const cmd = JSON.parse(published[0].value);
      expect(cmd).toMatchObject({
        version: 'Svc_1.0', archiveUrl: 'file:///opt/flink/samples/uc/Svc_1.0.jar', commandType: 'LOAD',
        sha256: crypto.createHash('sha256').update('default-jar').digest('hex'),
      });
      expect(cmd.requestId).toMatch(/^restore-default-/);
      const json = await agentsJson();
      expect(json.agents.map(a => a.ruleset.path)).toEqual([DEFAULT_JAR, DEFAULT_JAR]);
      expect(json.agents.map(a => a.ruleset.defaultPath)).toEqual([DEFAULT_JAR, DEFAULT_JAR]);
    });

    test('a failed restore leaves agents.json (and the active archive) unchanged', async () => {
      await afterRefresh();
      const before = await fs.readFile(path.join(tmpDir, 'uc', 'agents.json'), 'utf8');
      const out = await restore({ waiter: fakeWaiter('FAILED', { error: 'cannot load' }) });
      expect(out.status).toBe(502);
      expect(await fs.readFile(path.join(tmpDir, 'uc', 'agents.json'), 'utf8')).toBe(before);
      expect((await status()).body.active.filename).toBe(LOADED_FILE);
    });

    test('404 when the default jar is no longer on disk', async () => {
      await writeUc(AGENTS());
      const out = await restore();
      expect(out.status).toBe(404);
      expect(published).toHaveLength(0);
    });

    test('422 when there is no usable default', async () => {
      await writeUc([{ agentId: 'a', ruleset: { archiveControlTopic: 't' } }]);
      expect((await restore()).status).toBe(422);
    });

    test('422 when the default is outside the use-case folder', async () => {
      await writeUc([{ agentId: 'a', ruleset: { path: '../elsewhere/Svc_1.0.jar', archiveControlTopic: 't' } }]);
      expect((await restore()).status).toBe(422);
      expect(published).toHaveLength(0);
    });

    test('400 for an invalid use case id', async () => {
      const out = await call('/restore-default', 'post', { query: { useCaseId: '../x' } });
      expect(out.status).toBe(400);
    });
  });

  describe('rules-status', () => {
    test('a use case that never refreshed runs its default', async () => {
      await writeUc(AGENTS());
      const out = await status();
      expect(out.body).toMatchObject({
        useCaseId: 'uc', isDefault: true, inSync: true,
        active: { filename: 'Svc_1.0.jar', service: 'Svc', version: '1.0' },
        default: { filename: 'Svc_1.0.jar' },
      });
    });

    test('after a confirmed refresh the loaded archive is active and the default is kept', async () => {
      await writeUc(AGENTS());
      await refresh();
      const out = await status();
      expect(out.body.isDefault).toBe(false);
      expect(out.body.active.filename).toBe(LOADED_FILE);
      expect(out.body.default.filename).toBe('Svc_1.0.jar');
    });

    test('a failed refresh does not change the active archive', async () => {
      await writeUc(AGENTS());
      await refresh({ waiter: fakeWaiter('FAILED', { error: 'x' }) });
      expect((await status()).body).toMatchObject({ isDefault: true, active: { filename: 'Svc_1.0.jar' } });
    });

    test('agents that disagree are reported as not in sync', async () => {
      await writeUc([
        { agentId: 'a', ruleset: { path: 'samples/uc/Svc_1.0.jar' } },
        { agentId: 'b', ruleset: { path: 'samples/uc/Svc_2.0.jar' } },
      ]);
      expect((await status()).body.inSync).toBe(false);
    });

    test('active and default are null without a ruleset', async () => {
      await writeUc([{ agentId: 'a' }]);
      expect((await status()).body).toMatchObject({ active: null, default: null, isDefault: false });
    });

    test('404 / 400', async () => {
      expect((await call('/rules-status', 'get', { query: { useCaseId: 'nope' } })).status).toBe(404);
      expect((await call('/rules-status', 'get', { query: {} })).status).toBe(400);
    });
  });

  describe('listing', () => {
    test('each decision carries the jar file name it is stored under', async () => {
      await writeUc(AGENTS());
      const out = await call('/:id/decisions', 'get', { query: { useCaseId: 'uc' } });
      expect(out.body[0].filename).toBe(LOADED_FILE);
    });
  });
});
