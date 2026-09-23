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
 * DI Runtime (IBM Decision Intelligence) routes.
 *
 * Exposes three endpoints for browsing and downloading IBM Decision Intelligence
 * decision archives:
 *
 *   GET  /api/ads-runtimes
 *        → list configured runtimes (id, name, url; credentials omitted)
 *   GET  /api/ads-runtimes/:id/decisions[?deploymentSpace=<space>]
 *        → proxy decision list from the DI runtime
 *   POST /api/ads-runtimes/:id/archive?decisionId=<id>&agentId=<id>[&deploymentSpace=<space>]
 *        → download decision archive and save under samples/<agentId>/
 *
 * Factory export: createAdsRouter({ diRuntimes, samplesDir, isValidAgentId })
 */

'use strict';

const express = require('express');
const https   = require('https');
const http    = require('http');
const fs      = require('fs').promises;
const path    = require('path');

// ── Helpers ───────────────────────────────────────────────────────────────────

/** Returns the deploymentSpace for a runtime entry (defaults to "embedded"). */
function diDeploymentSpace(runtime) {
  return (runtime.deploymentSpace || 'embedded').trim();
}

/**
 * Makes an HTTP/HTTPS GET request to the given URL with optional Basic auth.
 * Returns a Promise that resolves with { statusCode, body } where body is a
 * Buffer for binary responses or a parsed JS object for JSON.
 *
 * @param {string}  url                   Full URL to request
 * @param {string}  [username]
 * @param {string}  [password]
 * @param {boolean} [binary]              If true, resolve body as Buffer (for archive download)
 * @param {boolean} [rejectUnauthorized]  Set false to allow self-signed TLS certs
 * @returns {Promise<{statusCode: number, body: Buffer|object}>}
 */
function diRequest(url, username, password, binary = false, rejectUnauthorized = true) {
  return new Promise((resolve, reject) => {
    const parsed  = new URL(url);
    const lib     = parsed.protocol === 'https:' ? https : http;
    const options = {
      hostname:           parsed.hostname,
      port:               parsed.port || (parsed.protocol === 'https:' ? 443 : 80),
      path:               parsed.pathname + parsed.search,
      method:             'GET',
      headers:            { Accept: binary ? 'application/octet-stream' : 'application/json' },
      rejectUnauthorized: rejectUnauthorized !== false,
    };
    if (username) {
      const creds = Buffer.from(`${username}:${password || ''}`).toString('base64');
      options.headers['Authorization'] = `Basic ${creds}`;
    }
    const req = lib.request(options, (res) => {
      const chunks = [];
      res.on('data', chunk => chunks.push(chunk));
      res.on('end', () => {
        const buf = Buffer.concat(chunks);
        if (binary) {
          resolve({ statusCode: res.statusCode, body: buf });
        } else {
          try {
            resolve({ statusCode: res.statusCode, body: JSON.parse(buf.toString('utf8')) });
          } catch {
            resolve({ statusCode: res.statusCode, body: buf.toString('utf8') });
          }
        }
      });
    });
    req.on('error', reject);
    req.setTimeout(60_000, () => { req.destroy(new Error('DI request timed out')); });
    req.end();
  });
}

// ── Router factory ────────────────────────────────────────────────────────────

/**
 * @param {object} opts
 * @param {Array}    opts.diRuntimes     - parsed DI_RUNTIMES array
 * @param {string}   opts.samplesDir     - absolute path to the samples directory
 * @param {Function} opts.isValidAgentId - id validation predicate
 */
function createAdsRouter({ diRuntimes, samplesDir, isValidAgentId }) {
  const router = express.Router();

  const findRuntime = (id) => diRuntimes.find(r => r.id === id);

  /**
   * GET /api/ads-runtimes
   * Lists all configured DI runtime instances (credentials are never returned).
   * Includes deploymentSpace so the UI can pre-populate the field with the env default.
   */
  router.get('/', (req, res) => {
    const runtimes = diRuntimes.map(r => ({
      id:              r.id,
      name:            r.name || r.id,
      url:             r.url,
      deploymentSpace: diDeploymentSpace(r),
    }));
    res.json(runtimes);
  });

  /**
   * GET /api/ads-runtimes/:id/decisions
   * Proxies the decision listing from the DI runtime.
   * Accepts an optional ?deploymentSpace= query param to override the env default.
   * Returns: [{ id, name, status }]
   */
  router.get('/:id/decisions', async (req, res) => {
    const runtime = findRuntime(req.params.id);
    if (!runtime) return res.status(404).json({ error: `DI runtime '${req.params.id}' not configured` });

    const spaceOverride = (req.query.deploymentSpace || '').trim();
    const space = spaceOverride || diDeploymentSpace(runtime);
    const listUrl = `${runtime.url.replace(/\/$/, '')}/ads/runtime/api/v1/deploymentSpaces/${encodeURIComponent(space)}/decisions?exposed=false`;

    try {
      const { statusCode, body } = await diRequest(
        listUrl,
        runtime.username,
        runtime.password,
        /* binary */ false,
        runtime.rejectUnauthorized,
      );
      if (statusCode === 401) {
        return res.status(401).json({ error: 'DI runtime authentication failed. Check username/password in DI_RUNTIMES config.' });
      }
      if (statusCode < 200 || statusCode >= 300) {
        return res.status(502).json({ error: `DI runtime returned status ${statusCode}` });
      }
      // IBM ADS Runtime API (v1/deploymentSpaces/.../decisions) returns a flat
      // string[] where each element is the full decision path used as the decision ID.
      const rawItems = Array.isArray(body) ? body
        : Array.isArray(body?.decisions) ? body.decisions
        : [];

      const decisions = rawItems.map(item => {
        if (typeof item === 'string') {
          const parts  = item.split('/');
          const ver    = parts[parts.length - 1] || item;
          const svc    = parts[parts.length - 2] || '';
          const proj   = parts[parts.length - 3] || '';
          const label  = svc ? `${svc}  ${ver}` : item;
          const group  = proj || svc || item;
          return { id: item, name: label, group, status: 'deployed' };
        }
        return {
          id:     item.decisionId || item.id   || '',
          name:   item.name       || item.decisionId || '',
          group:  '',
          status: item.status     || 'unknown',
        };
      });
      res.json(decisions);
    } catch (err) {
      console.error(`[di] Failed to list decisions from runtime '${req.params.id}':`, err.message);
      res.status(502).json({ error: `Failed to connect to DI runtime: ${err.message}` });
    }
  });

  /**
   * POST /api/ads-runtimes/:id/archive?decisionId=<encoded>&agentId=<agentId>[&deploymentSpace=<space>]
   * Downloads the decision archive from the DI runtime and saves it under
   * samples/<agentId>/<safeId>.jar.
   */
  router.post('/:id/archive', async (req, res) => {
    const { id: runtimeId } = req.params;
    const { agentId, decisionId, deploymentSpace: deploymentSpaceOverride } = req.query;

    const runtime = findRuntime(runtimeId);
    if (!runtime) return res.status(404).json({ error: `DI runtime '${runtimeId}' not configured` });

    if (!agentId || !isValidAgentId(agentId)) {
      return res.status(400).json({ error: 'agentId query parameter is required and must be a valid agent id' });
    }
    if (!decisionId || decisionId.length > 512) {
      return res.status(400).json({ error: 'decisionId is required and must not exceed 512 characters' });
    }

    const spaceOverride = (deploymentSpaceOverride || '').trim();
    const space         = spaceOverride || diDeploymentSpace(runtime);
    const archiveUrl    = `${runtime.url.replace(/\/$/, '')}/ads/runtime/api/v1/deploymentSpaces/${encodeURIComponent(space)}/decisions/${encodeURIComponent(decisionId)}/archive`;

    try {
      const { statusCode, body: archiveBuffer } = await diRequest(
        archiveUrl,
        runtime.username,
        runtime.password,
        /* binary */ true,
        runtime.rejectUnauthorized,
      );
      if (statusCode === 401) {
        return res.status(401).json({ error: 'DI runtime authentication failed.' });
      }
      if (statusCode === 404) {
        return res.status(404).json({ error: `Decision '${decisionId}' not found on DI runtime '${runtimeId}'` });
      }
      if (statusCode < 200 || statusCode >= 300) {
        return res.status(502).json({ error: `DI runtime returned status ${statusCode} for archive download` });
      }
      if (!archiveBuffer || archiveBuffer.length === 0) {
        return res.status(502).json({ error: 'DI runtime returned an empty archive' });
      }

      // Derive a safe filename from the last two path segments of the decision ID.
      const parts    = decisionId.split('/');
      const ver      = parts[parts.length - 1] || 'archive';
      const svc      = parts[parts.length - 2] || '';
      const base     = svc ? `${svc}_${ver}` : ver;
      const safeId   = base.replace(/[^a-zA-Z0-9._-]/g, '_');
      const filename = `${safeId}.jar`;
      const dir      = path.join(samplesDir, agentId);
      const filePath = path.join(dir, filename);
      const relPath  = `samples/${agentId}/${filename}`;
      await fs.mkdir(dir, { recursive: true });
      await fs.writeFile(filePath, archiveBuffer);

      console.log(`[di] Archive for decision '${decisionId}' saved to ${relPath} (${archiveBuffer.length} bytes)`);
      res.status(201).json({ saved: true, filename, path: relPath });
    } catch (err) {
      console.error(`[di] Archive download failed for decision '${decisionId}' from runtime '${runtimeId}':`, err.message);
      res.status(502).json({ error: `Archive download failed: ${err.message}` });
    }
  });

  return router;
}

module.exports = { createAdsRouter };
