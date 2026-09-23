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
        password: 'secretpassword'
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
      expect(resJson[0].password).toBeUndefined();
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
});
