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
 * Exposes endpoints for browsing and downloading IBM Decision Intelligence
 * decision archives:
 *
 *   GET  /api/ads-runtimes
 *        → list configured runtimes (id, name, url; credentials omitted)
 *   GET  /api/ads-runtimes/:id/decisions[?deploymentSpace=<space>][&useCaseId=<id>]
 *        → proxy decision list from the DI runtime. With useCaseId, only the versions of the
 *          Decision Service(s) bound to that use case are returned (see "Use-case binding")
 *   POST /api/ads-runtimes/:id/archive?decisionId=<id>&agentId=<id>[&deploymentSpace=<space>]
 *        → download decision archive and save under samples/<agentId>/
 *   GET  /api/ads-runtimes/:id/archive?decisionId=<id>[&deploymentSpace=<space>]
 *        → stream decision archive to the caller without storing it
 *   POST /api/ads-runtimes/:id/refresh-rules?decisionId=<id>&useCaseId=<id>[&deploymentSpace=<space>][&updateAgentsJson=true]
 *        → download decision archive, save it under samples/<useCaseId>/, publish a LOAD
 *          command to the archive-control topic(s) of the use case (live rules refresh) and wait
 *          for the job's acknowledgement. The archive must belong to a Decision Service bound
 *          to the use case. agents.json (ruleset.path) is updated only when updateAgentsJson=true
 *          (default: not updated) and only once the job confirmed the load; on failure, partial
 *          load or timeout it is left untouched. An archive is never overwritten with different
 *          content (same name + other bytes is stored as <name>-<sha8>.jar).
 *          Archive lifecycle after the job confirmed the load:
 *            updateAgentsJson=true  → the jar is kept and agents.json points at it;
 *            updateAgentsJson=false → the jar stored by THIS request is deleted (response:
 *              archiveDeleted=true), unless agents.json still references it or an identical file
 *              already existed. On any failed/partial/timed-out load nothing is deleted.
 *          Note: the job keeps its own SHA-keyed copy (FLINK_ARCHIVE_CACHE_DIR); without the jar
 *          in samples/, a checkpoint restore or ROLLBACK to this archive depends on that cache.
 *   POST /api/ads-runtimes/restore-default?useCaseId=<id>
 *        → reload the default archive (ruleset.defaultPath) the same way
 *   GET  /api/ads-runtimes/rules-status?useCaseId=<id>
 *        → { active, default, isDefault, inSync } read from agents.json
 *
 * Use-case binding (single source of truth, shared by the listing and refresh-rules):
 *   The Decision Service(s) of a use case are derived from samples/<useCaseId>/agents.json,
 *   agents[].ruleset:
 *     1. ruleset.path  (authoritative) — jar named <Service>_<version>.jar, the convention used
 *        when an archive is downloaded from a runtime; <Service> is the bound service.
 *     2. ruleset.project — only when ruleset.path is absent/blank; basename without ".zip".
 *   A path that is present but unrecognizable is NOT overridden by ruleset.project.
 *   The binding is the union over all agents (and over ruleset.defaultPath, see refresh-rules). It fails closed: when it cannot be resolved,
 *   nothing is listed and nothing can be refreshed.
 *
 * Acknowledgements: see archive_status.js. ruleset.defaultPath is added by the first refresh and
 * keeps the archive the use case shipped with.
 *
 * Factory export: createAdsRouter({ diRuntimes, samplesDir, isValidAgentId,
 *                                   publishControlMessage, flinkSamplesMount,
 *                                   awaitArchiveAcks, ackTimeoutMs })
 */

'use strict';

const crypto  = require('crypto');
const express = require('express');
const https   = require('https');
const http    = require('http');
const fs      = require('fs').promises;
const path    = require('path');

const { evaluateAcks } = require('./archive_status');

// ── Helpers ───────────────────────────────────────────────────────────────────

/** Returns the deploymentSpace for a runtime entry (defaults to "embedded"). */
function diDeploymentSpace(runtime) {
  return (runtime.deploymentSpace || 'embedded').trim();
}

/**
 * Makes an HTTP/HTTPS GET request to the given URL, authenticated with a Bearer token
 * ("Authorization: Bearer <token>") or a Zen API key
 * ("Authorization: ZenApiKey base64(<username>:<apiKey>)") when provided.
 * Returns a Promise that resolves with { statusCode, body } where body is a
 * Buffer for binary responses or a parsed JS object for JSON.
 *
 * @param {string}  url                   Full URL to request
 * @param {string}  [username]            Zen user name the API key belongs to
 * @param {string}  [apiKey]              Zen API key
 * @param {boolean} [binary]              If true, resolve body as Buffer (for archive download)
 * @param {boolean} [rejectUnauthorized]  Set false to allow self-signed TLS certs
 * @param {string}  [token]               Bearer token
 * @returns {Promise<{statusCode: number, body: Buffer|object}>}
 */
function diRequest(url, username, apiKey, binary = false, rejectUnauthorized = true, token = null) {
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
    if (token) {
      options.headers['Authorization'] = `Bearer ${token}`;
    } else if (username && apiKey) {
      const creds = Buffer.from(`${username}:${apiKey}`).toString('base64');
      options.headers['Authorization'] = `ZenApiKey ${creds}`;
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

// ── Use-case binding ──────────────────────────────────────────────────────────

/**
 * `<Service>_<version>[-<build>].jar`. The service is everything before the FIRST "_<digits>"
 * that is followed by a version; the build suffix may itself contain "_" (an archive downloaded
 * from a runtime is named after its decision id, e.g. `svc_1.6.0-2026-11-12T13_43_37.259Z.jar`).
 */
const JAR_NAME_RE = /^(.+?)_v?\d+(?:\.\d+)*((?:[-+][A-Za-z0-9._+-]*)?)\.jar$/i;
/** Build suffix that only a decision id (runtime archive) carries: `-YYYY-MM-DDT…`. */
const RUNTIME_BUILD_RE = /^-\d{4}-\d{2}-\d{2}T/;
const DECISION_SERVICE_SUFFIX_RE = /DecisionService$/i;

const serviceKey = (name) => String(name ?? '').toLowerCase().replace(/[^a-z0-9]/g, '');

const baseName = (p) => String(p).split(/[\\/]/).filter(Boolean).pop() || '';

/**
 * Identity of a decision (archive version) listed by a runtime.
 *
 * A runtime decision id looks like
 *   <tenant>/<repository>/<service>/<service>DecisionService/<version>-<build timestamp>
 * i.e. the service is the third segment from the end and the one before the version is its
 * generated "<service>DecisionService" decision name. Plain ids `<project>/<service>/<version>`
 * are also accepted. Returns { service, key, version, build, group }.
 */
function describeDecision(decisionId) {
  const parts = String(decisionId).split('/');
  const n = parts.length;
  const last = parts[n - 1] || '';
  const runtimeShape = n >= 4 && parts[n - 3]
    && serviceKey(parts[n - 2]) === `${serviceKey(parts[n - 3])}decisionservice`;

  const service = runtimeShape ? parts[n - 3] : (n >= 2 ? parts[n - 2] : '');
  const group = runtimeShape ? (parts[n - 4] || '') : (parts[n - 3] || service || '');
  const m = /^(.*?)-(\d{4}-\d{2}-\d{2}T.*)$/.exec(last);
  return {
    service,
    key: serviceKey(service),
    version: m ? m[1] : last,
    build: m ? m[2] : '',
    group,
  };
}

/**
 * Decision Service bound by one `ruleset` block, or null when it cannot be resolved.
 * ruleset.path is authoritative; ruleset.project is used only when the path is absent.
 */
function resolveRulesetService(ruleset) {
  if (!ruleset || typeof ruleset !== 'object') return null;
  const { path: jarPath, project } = ruleset;

  const pathPresent = jarPath != null && !(typeof jarPath === 'string' && !jarPath.trim());
  if (pathPresent) {
    if (typeof jarPath !== 'string') return null;
    const m = JAR_NAME_RE.exec(baseName(jarPath.trim()));
    if (!m) return null; // never fall back to project
    // A jar saved from a runtime decision id is named after the generated "<service>DecisionService"
    // decision; the service itself is what precedes that suffix.
    const generated = RUNTIME_BUILD_RE.test(m[2]);
    const service = generated ? m[1].replace(DECISION_SERVICE_SUFFIX_RE, '') : m[1];
    return service || null;
  }
  if (typeof project === 'string' && project.trim()) {
    return baseName(project.trim()).replace(/\.zip$/i, '') || null;
  }
  return null;
}

/**
 * Names of the Decision Services bound to a descriptor (union over its agents, one entry per
 * distinct service). Empty when no agent has a resolvable binding.
 */
function boundServices(descriptor) {
  const byKey = new Map();
  for (const agent of (Array.isArray(descriptor?.agents) ? descriptor.agents : [])) {
    const ruleset = agent?.ruleset;
    // ruleset.defaultPath (the archive the use case shipped with, kept by refresh-rules) binds too,
    // so that loading a version never drops a service from the binding.
    const candidates = [resolveRulesetService(ruleset)];
    if (typeof ruleset?.defaultPath === 'string' && ruleset.defaultPath.trim()) {
      candidates.push(resolveRulesetService({ path: ruleset.defaultPath }));
    }
    for (const svc of candidates) {
      if (svc && serviceKey(svc) && !byKey.has(serviceKey(svc))) byKey.set(serviceKey(svc), svc);
    }
  }
  return [...byKey.values()];
}

/** True when the decision is a version of one of the bound services. */
function isDecisionBound(decisionId, services) {
  const { key } = describeDecision(decisionId);
  return key !== '' && services.some(s => serviceKey(s) === key);
}

/** Jar file name an archive of `decisionId` is stored under (last two id segments). */
function archiveFileName(decisionId) {
  const parts = String(decisionId).split('/');
  const ver   = parts[parts.length - 1] || 'archive';
  const svc   = parts[parts.length - 2] || '';
  const base  = svc ? `${svc}_${ver}` : ver;
  return `${base.replace(/[^a-zA-Z0-9._-]/g, '_')}.jar`;
}

/** The archive "Restore default" returns to: ruleset.defaultPath, else the current ruleset.path. */
function defaultJarPath(ruleset) {
  for (const candidate of [ruleset?.defaultPath, ruleset?.path]) {
    if (typeof candidate === 'string' && candidate.trim()) return candidate.trim();
  }
  return null;
}

/** { filename, path, service, version } of a jar path, or null. */
function describeJar(jarPath) {
  if (typeof jarPath !== 'string' || !jarPath.trim()) return null;
  const filename = baseName(jarPath.trim());
  const m = /^(.+?)_v?(\d+(?:\.\d+)*)((?:[-+][A-Za-z0-9._+-]*)?)\.jar$/i.exec(filename);
  return { filename, path: jarPath.trim(), service: m ? m[1] : '', version: m ? m[2] : '' };
}

/**
 * Archive a use case runs and its default. agents.json is written only after the job confirmed a
 * load, so ruleset.path IS the loaded archive. inSync is false when the agents disagree.
 */
function rulesStatus(descriptor) {
  const rulesets = (Array.isArray(descriptor?.agents) ? descriptor.agents : [])
    .map(a => a?.ruleset).filter(r => r && typeof r === 'object');
  const paths = rulesets.map(r => (typeof r.path === 'string' ? r.path.trim() : '')).filter(Boolean);
  const defaults = rulesets.map(defaultJarPath).filter(Boolean);
  const active = describeJar(paths[0]);
  const dflt = describeJar(defaults[0]) || active;
  return {
    active,
    default: dflt,
    isDefault: !!active && !!dflt && active.filename === dflt.filename,
    inSync: new Set(paths).size <= 1 && new Set(defaults).size <= 1,
  };
}

// ── Router factory ────────────────────────────────────────────────────────────

/**
 * Distinct archive-control topics declared by the agents of a descriptor
 * (agents[].ruleset.archiveControlTopic), same discovery as scripts/broadcast-reload.sh.
 */
function archiveControlTopics(descriptor) {
  const topics = new Set();
  for (const agent of (descriptor?.agents || [])) {
    const t = agent?.ruleset?.archiveControlTopic;
    if (typeof t === 'string' && t.trim()) topics.add(t.trim());
  }
  return [...topics];
}

/**
 * @param {object} opts
 * @param {Array}    opts.diRuntimes            - parsed DI_RUNTIMES array
 * @param {string}   opts.samplesDir            - absolute path to the samples directory
 * @param {Function} opts.isValidAgentId        - id validation predicate
 * @param {Function} [opts.publishControlMessage] - async (topic, jsonString) => void, Kafka publisher
 * @param {string}   [opts.flinkSamplesMount]   - path where samples/ is mounted in the Flink containers
 * @param {Function} [opts.awaitArchiveAcks]    - archive_status.createArchiveAckWaiter(): starts listening
 *                                                for the job's acknowledgement of one command
 * @param {number}   [opts.ackTimeoutMs]        - how long to wait for the acknowledgement
 */
function createAdsRouter({
  diRuntimes, samplesDir, isValidAgentId,
  publishControlMessage, flinkSamplesMount = '/opt/flink/samples',
  awaitArchiveAcks, ackTimeoutMs = 30000,
}) {
  const router = express.Router();

  const findRuntime = (id) => diRuntimes.find(r => r.id === id);

  /**
   * Reads samples/<useCaseId>/agents.json (useCaseId must already be validated).
   * Resolves { descriptor } — null when the file is not valid JSON — or { status, error }.
   */
  async function readUseCaseDescriptor(useCaseId) {
    let raw;
    try {
      raw = await fs.readFile(path.join(samplesDir, useCaseId, 'agents.json'), 'utf8');
    } catch (err) {
      if (err.code === 'ENOENT') return { status: 404, error: `agents.json not found for '${useCaseId}'` };
      return { status: 500, error: `Could not read agents.json: ${err.message}` };
    }
    try {
      return { descriptor: JSON.parse(raw) };
    } catch (err) {
      return { descriptor: null, parseError: err.message };
    }
  }

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
   * Accepts an optional ?useCaseId= query param: only the versions of the Decision Service(s)
   * bound to that use case are returned (fail closed: [] when the binding cannot be resolved).
   * Returns: [{ id, name, status }]
   */
  router.get('/:id/decisions', async (req, res) => {
    const runtime = findRuntime(req.params.id);
    if (!runtime) return res.status(404).json({ error: `DI runtime '${req.params.id}' not configured` });

    // Resolve the use-case binding first: it needs no runtime call and decides whether one is useful.
    const { useCaseId } = req.query;
    let services = null; // null = no use-case filtering requested
    if (useCaseId !== undefined) {
      if (typeof useCaseId !== 'string' || !isValidAgentId(useCaseId)) {
        return res.status(400).json({ error: 'useCaseId query parameter must be a valid use case id' });
      }
      const loaded = await readUseCaseDescriptor(useCaseId);
      if (loaded.error) return res.status(loaded.status).json({ error: loaded.error });
      services = boundServices(loaded.descriptor);
      // Tells the UI which service(s) were looked for, so that "no binding" and "bound service
      // without any version" can be told apart (the body stays a plain array).
      res.set('X-Decision-Services', encodeURIComponent(JSON.stringify(services)));
      res.set('Access-Control-Expose-Headers', 'X-Decision-Services');
      if (services.length === 0) return res.json([]);
    }

    const spaceOverride = (req.query.deploymentSpace || '').trim();
    const space = spaceOverride || diDeploymentSpace(runtime);
    const listUrl = `${runtime.url.replace(/\/$/, '')}/ads/runtime/api/v1/deploymentSpaces/${encodeURIComponent(space)}/decisions?exposed=false`;

    try {
      const { statusCode, body } = await diRequest(
        listUrl,
        runtime.username,
        runtime.apiKey,
        /* binary */ false,
        runtime.rejectUnauthorized,
        runtime.token,
      );
      if (statusCode === 401) {
        return res.status(401).json({ error: 'DI runtime authentication failed. Check username/apiKey in DI_RUNTIMES config.' });
      }
      if (statusCode < 200 || statusCode >= 300) {
        return res.status(502).json({ error: `DI runtime returned status ${statusCode}` });
      }
      // IBM ADS Runtime API (v1/deploymentSpaces/.../decisions) returns a flat
      // string[] where each element is the full decision path used as the decision ID
      // (see describeDecision for its shape).
      const rawItems = Array.isArray(body) ? body
        : Array.isArray(body?.decisions) ? body.decisions
        : [];

      const decisions = rawItems.map(item => {
        const id = typeof item === 'string' ? item : (item.decisionId || item.id || '');
        const d  = describeDecision(id);
        if (typeof item === 'string') {
          return {
            id,
            name:    d.service ? `${d.service}  ${d.version}` : item,
            group:   d.group || d.service || item,
            status:  'deployed',
            service: d.service, version: d.version, build: d.build, filename: archiveFileName(id),
          };
        }
        return {
          id,
          name:    item.name || item.decisionId || '',
          group:   '',
          status:  item.status || 'unknown',
          service: d.service, version: d.version, build: d.build, filename: archiveFileName(id),
        };
      });
      res.json(services ? decisions.filter(d => isDecisionBound(d.id, services)) : decisions);
    } catch (err) {
      console.error(`[di] Failed to list decisions from runtime '${req.params.id}':`, err.message);
      res.status(502).json({ error: `Failed to connect to DI runtime: ${err.message}` });
    }
  });

  /**
   * Downloads the archive of req.query.decisionId from the DI runtime req.params.id.
   * Returns { buffer, filename } on success; on failure it has already answered
   * the request and returns null.
   */
  async function downloadArchive(req, res) {
    const { id: runtimeId } = req.params;
    const { decisionId, deploymentSpace: deploymentSpaceOverride } = req.query;

    const runtime = findRuntime(runtimeId);
    if (!runtime) { res.status(404).json({ error: `DI runtime '${runtimeId}' not configured` }); return null; }

    if (!decisionId || decisionId.length > 512) {
      res.status(400).json({ error: 'decisionId is required and must not exceed 512 characters' });
      return null;
    }

    const spaceOverride = (deploymentSpaceOverride || '').trim();
    const space         = spaceOverride || diDeploymentSpace(runtime);
    const archiveUrl    = `${runtime.url.replace(/\/$/, '')}/ads/runtime/api/v1/deploymentSpaces/${encodeURIComponent(space)}/decisions/${encodeURIComponent(decisionId)}/archive`;

    try {
      const { statusCode, body: archiveBuffer } = await diRequest(
        archiveUrl,
        runtime.username,
        runtime.apiKey,
        /* binary */ true,
        runtime.rejectUnauthorized,
        runtime.token,
      );
      if (statusCode === 401) {
        res.status(401).json({ error: 'DI runtime authentication failed.' });
        return null;
      }
      if (statusCode === 404) {
        res.status(404).json({ error: `Decision '${decisionId}' not found on DI runtime '${runtimeId}'` });
        return null;
      }
      if (statusCode < 200 || statusCode >= 300) {
        res.status(502).json({ error: `DI runtime returned status ${statusCode} for archive download` });
        return null;
      }
      if (!archiveBuffer || archiveBuffer.length === 0) {
        res.status(502).json({ error: 'DI runtime returned an empty archive' });
        return null;
      }

      // Derive a safe filename from the last two path segments of the decision ID.
      return { buffer: archiveBuffer, filename: archiveFileName(decisionId) };
    } catch (err) {
      console.error(`[di] Archive download failed for decision '${decisionId}' from runtime '${runtimeId}':`, err.message);
      res.status(502).json({ error: `Archive download failed: ${err.message}` });
      return null;
    }
  }

  /**
   * POST /api/ads-runtimes/:id/archive?decisionId=<encoded>&agentId=<agentId>[&deploymentSpace=<space>]
   * Downloads the decision archive from the DI runtime and saves it under
   * samples/<agentId>/<safeId>.jar.
   */
  router.post('/:id/archive', async (req, res) => {
    const { agentId, decisionId } = req.query;
    if (!findRuntime(req.params.id)) {
      return res.status(404).json({ error: `DI runtime '${req.params.id}' not configured` });
    }
    if (!agentId || !isValidAgentId(agentId)) {
      return res.status(400).json({ error: 'agentId query parameter is required and must be a valid agent id' });
    }
    const archive = await downloadArchive(req, res);
    if (!archive) return undefined;
    try {
      const dir      = path.join(samplesDir, agentId);
      const relPath  = `samples/${agentId}/${archive.filename}`;
      await fs.mkdir(dir, { recursive: true });
      await fs.writeFile(path.join(dir, archive.filename), archive.buffer);
      console.log(`[di] Archive for decision '${decisionId}' saved to ${relPath} (${archive.buffer.length} bytes)`);
      return res.status(201).json({ saved: true, filename: archive.filename, path: relPath });
    } catch (err) {
      console.error(`[di] Could not store archive for decision '${decisionId}':`, err.message);
      return res.status(500).json({ error: `Could not store archive: ${err.message}` });
    }
  });

  /**
   * GET /api/ads-runtimes/:id/archive?decisionId=<encoded>[&deploymentSpace=<space>]
   * Streams the decision archive to the caller without storing it, so the Agent
   * Diagram Editor can keep it pending until the diagram is saved.
   * The file name is returned in the X-Archive-Filename header.
   */
  router.get('/:id/archive', async (req, res) => {
    const archive = await downloadArchive(req, res);
    if (!archive) return undefined;
    res.set('Content-Type', 'application/java-archive');
    res.set('X-Archive-Filename', archive.filename);
    res.set('Access-Control-Expose-Headers', 'X-Archive-Filename');
    return res.status(200).send(archive.buffer);
  });

  /**
   * Agents of the descriptor that listen on one of `topics` (the ones a command published there
   * reaches). When no agent declares a control topic (fallback topic), every agent with a ruleset.
   */
  function targetAgents(descriptor, topics) {
    const declared = archiveControlTopics(descriptor).length > 0;
    return (Array.isArray(descriptor?.agents) ? descriptor.agents : []).filter(a => {
      if (!a || typeof a.agentId !== 'string' || !a.ruleset || typeof a.ruleset !== 'object') return false;
      if (!declared) return true;
      const t = a.ruleset.archiveControlTopic;
      return typeof t === 'string' && topics.includes(t.trim());
    });
  }

  /**
   * Writes the confirmed archive into agents.json. The descriptor is re-read first so that edits
   * made since the command was published are not overwritten. The first time the path changes,
   * the previous one is kept as ruleset.defaultPath (the archive "Restore default" goes back to).
   */
  async function persistActiveArchive(useCaseId, agentIds, relPath) {
    const file = path.join(samplesDir, useCaseId, 'agents.json');
    const current = JSON.parse(await fs.readFile(file, 'utf8'));
    for (const agent of (Array.isArray(current.agents) ? current.agents : [])) {
      if (!agentIds.includes(agent?.agentId) || !agent.ruleset) continue;
      const previous = agent.ruleset.path;
      if (typeof agent.ruleset.defaultPath !== 'string' || !agent.ruleset.defaultPath.trim()) {
        if (typeof previous === 'string' && previous.trim()) agent.ruleset.defaultPath = previous;
      }
      agent.ruleset.path = relPath;
    }
    // Atomic replace: a crash mid-write must never leave a truncated agents.json behind.
    const tmp = `${file}.${process.pid}.tmp`;
    try {
      await fs.writeFile(tmp, JSON.stringify(current, null, 2));
      await fs.rename(tmp, file);
    } catch (err) {
      await fs.unlink(tmp).catch(() => {});
      throw err;
    }
    return current;
  }

  /**
   * Stores a downloaded archive in samples/<useCaseId>/ WITHOUT ever overwriting a different one.
   *
   * A stored archive can still be needed after a refresh: it may be the one the job runs, the
   * "Restore default" target (ruleset.defaultPath), the rollback target, or the archiveUrl a
   * checkpoint restore re-downloads and re-verifies by SHA-256. Rewriting it with other bytes
   * would break all of those, and the refresh itself may still fail afterwards. So:
   *   - same name, same content  -> reused as is (nothing written);
   *   - same name, other content -> stored next to it as <name>-<sha8>.jar;
   *   - otherwise                -> written to a temp file and renamed (never a partial jar).
   * Returns { filename, reused, renamed }.
   */
  async function storeArchive(useCaseId, desiredName, buffer, sha256) {
    const dir = path.join(samplesDir, useCaseId);
    await fs.mkdir(dir, { recursive: true });
    const sha = (buf) => crypto.createHash('sha256').update(buf).digest('hex');
    /** true = same bytes, false = other bytes, null = no such file */
    const compare = async (name) => {
      try {
        return sha(await fs.readFile(path.join(dir, name))) === sha256;
      } catch (err) {
        if (err.code === 'ENOENT') return null;
        throw err;
      }
    };

    let filename = desiredName;
    let renamed = false;
    let state = await compare(filename);
    if (state === false) {
      filename = desiredName.replace(/\.jar$/i, `-${sha256.slice(0, 8)}.jar`);
      renamed = true;
      console.warn(`[di] '${useCaseId}': ${desiredName} already exists with different content; `
        + `keeping it and storing the new archive as ${filename}`);
      state = await compare(filename);
      if (state === false) throw new Error(`${filename} already exists with different content`);
    }
    if (state === true) {
      console.log(`[di] '${useCaseId}': archive ${filename} already stored (identical content), reused`);
      return { filename, reused: true, renamed };
    }

    const tmp = path.join(dir, `.${filename}.${process.pid}.tmp`);
    try {
      await fs.writeFile(tmp, buffer);
      await fs.rename(tmp, path.join(dir, filename));
    } catch (err) {
      await fs.unlink(tmp).catch(() => {});
      throw err;
    }
    console.log(`[di] '${useCaseId}': archive ${filename} stored (${buffer.length} bytes)`);
    return { filename, reused: false, renamed };
  }

  /**
   * Deletes an archive stored by a refresh that did NOT update agents.json. Called only after the
   * job confirmed the load. Never throws: a failed deletion must not turn a confirmed refresh
   * into an error. Returns { deleted, reason } (reason set when kept).
   */
  async function discardArchive(useCaseId, filename) {
    const file = path.join(samplesDir, useCaseId, filename);
    try {
      // Re-read agents.json: never delete a jar it (still) points at as active or default.
      const loaded = await readUseCaseDescriptor(useCaseId);
      if (loaded.error || loaded.parseError) {
        console.warn(`[di] '${useCaseId}': ${filename} kept — agents.json could not be checked `
          + `(${loaded.error || loaded.parseError})`);
        return { deleted: false, reason: 'agents.json could not be checked' };
      }
      const referenced = (Array.isArray(loaded.descriptor?.agents) ? loaded.descriptor.agents : [])
        .flatMap(a => [a?.ruleset?.path, a?.ruleset?.defaultPath])
        .filter(p => typeof p === 'string' && p.trim())
        .some(p => baseName(p.trim().replace(/\\/g, '/')) === filename);
      if (referenced) {
        console.log(`[di] '${useCaseId}': ${filename} kept — referenced by agents.json`);
        return { deleted: false, reason: 'referenced by agents.json' };
      }
      await fs.unlink(file);
      console.log(`[di] '${useCaseId}': ${filename} deleted after the job confirmed the load `
        + '(agents.json not updated)');
      return { deleted: true };
    } catch (err) {
      console.warn(`[di] '${useCaseId}': could not delete ${filename}: ${err.message}`);
      return { deleted: false, reason: err.message };
    }
  }

  const rulesInFlight = new Set(); // use cases with a refresh/restore running

  /**
   * Publishes a LOAD command and waits until the running job confirms it. agents.json — and with
   * it the "active archive" the UI shows — changes ONLY when every subtask of every expected
   * agent acknowledged LOADED: the rules loaded in the job are the source of truth.
   * Resolves { status, body } for the HTTP response.
   */
  async function loadAndConfirm({ useCaseId, descriptor, topics, filename, sha256, requestPrefix, updateAgentsJson = true }) {
    const targets = targetAgents(descriptor, topics);
    const agentIds = targets.map(a => a.agentId);
    if (agentIds.length === 0) {
      return { status: 422, body: { error: `Use case '${useCaseId}' has no agent with a ruleset to refresh.` } };
    }

    const version   = filename.replace(/\.jar$/, '');
    const requestId = `${requestPrefix}-${Date.now()}`;
    const relPath   = `samples/${useCaseId}/${filename}`;
    const payload   = JSON.stringify({
      version,
      archiveUrl:  `file://${flinkSamplesMount.replace(/\/$/, '')}/${useCaseId}/${filename}`,
      sha256,
      commandType: 'LOAD',
      requestId,
    });

    // Listen BEFORE publishing so that no acknowledgement can be missed.
    let waiter;
    try {
      waiter = await awaitArchiveAcks({ topics, requestId, expectedAgents: agentIds });
    } catch (err) {
      return { status: 502, body: { error: `Could not listen for the job's acknowledgement: ${err.message}` } };
    }

    let acks;
    try {
      for (const topic of topics) await publishControlMessage(topic, payload);
      acks = await waiter.done(ackTimeoutMs);
    } catch (err) {
      await waiter.close();
      return { status: 502, body: { error: `Could not publish the archive command: ${err.message}` } };
    }

    const result = evaluateAcks(acks, agentIds);
    const detail = { requestId, version, filename, path: relPath, topics };
    if (result.state !== 'loaded') {
      // Never delete on failure: the job may still load it late (timeout) or run it partially.
      console.warn(`[di] Load of ${filename} for '${useCaseId}' not confirmed (${result.state}); `
        + `archive kept at ${relPath}, agents.json unchanged, previous archive untouched`);
    }
    if (result.state === 'pending') {
      return { status: 504, body: {
        ...detail, state: 'timeout', missing: result.missing,
        error: `The job did not confirm the new rules within ${Math.round(ackTimeoutMs / 1000)}s `
          + `(no answer from: ${result.missing.join(', ')}). agents.json and the active archive were left unchanged. `
          + 'Check that the job is running a build that acknowledges archive commands, then retry.',
      } };
    }
    if (result.state !== 'loaded') {
      const reasons = [...new Set(result.failures.map(f => f.error))].join('; ');
      const partial = result.state === 'partial';
      return { status: 502, body: {
        ...detail, state: result.state, failures: result.failures, outOfSync: partial,
        error: partial
          ? `The new rules were loaded by only some of the job's parallel instances (${reasons}). agents.json was left `
            + 'unchanged, but the job now runs mixed rules: retry the refresh or restore the default archive.'
          : `The job rejected the archive and keeps its previous rules: ${reasons}. agents.json was left unchanged.`,
      } };
    }

    // The archive jar is already stored in samples/<useCaseId>/; only agents.json is optional.
    if (updateAgentsJson) {
      try {
        await persistActiveArchive(useCaseId, agentIds, relPath);
      } catch (err) {
        console.error(`[di] Rules loaded for '${useCaseId}' but agents.json was not updated:`, err.message);
        return { status: 500, body: {
          ...detail, state: 'loaded', outOfSync: true,
          error: `The job loaded ${filename} but agents.json could not be updated: ${err.message}`,
        } };
      }
    }
    console.log(`[di] Rules for '${useCaseId}' now ${filename} sha256=${sha256} (confirmed by ${agentIds.join(', ')})`
      + `${updateAgentsJson ? '' : '; agents.json not updated'}`);
    return { status: 200, body: {
      ...detail, state: 'loaded', refreshed: true, sha256, agents: agentIds, agentsUpdated: updateAgentsJson,
    } };
  }

  /** Common checks of the endpoints that load an archive into a running job. */
  async function prepareLoad(req, res) {
    const { useCaseId } = req.query;
    if (!useCaseId || typeof useCaseId !== 'string' || !isValidAgentId(useCaseId)) {
      res.status(400).json({ error: 'useCaseId query parameter is required and must be a valid use case id' });
      return null;
    }
    if (typeof publishControlMessage !== 'function' || typeof awaitArchiveAcks !== 'function') {
      res.status(503).json({ error: 'Kafka publishing is not available' });
      return null;
    }
    const loaded = await readUseCaseDescriptor(useCaseId);
    if (loaded.error) { res.status(loaded.status).json({ error: loaded.error }); return null; }
    if (loaded.parseError) {
      res.status(500).json({ error: `Could not read agents.json: ${loaded.parseError}` });
      return null;
    }
    const topics = archiveControlTopics(loaded.descriptor);
    if (topics.length === 0) topics.push(`agent-${useCaseId}-archive-control`);
    return { useCaseId, descriptor: loaded.descriptor, topics };
  }

  /** Runs `task` unless a refresh/restore of the same use case is already running (409). */
  async function exclusive(useCaseId, res, task) {
    if (rulesInFlight.has(useCaseId)) {
      return res.status(409).json({ error: `A rules refresh of '${useCaseId}' is already in progress.` });
    }
    rulesInFlight.add(useCaseId);
    try {
      return await task();
    } finally {
      rulesInFlight.delete(useCaseId);
    }
  }

  /**
   * POST /api/ads-runtimes/:id/refresh-rules?decisionId=<encoded>&useCaseId=<id>[&deploymentSpace=<space>][&updateAgentsJson=true|false]
   * Live refresh of the rules of a deployed job: downloads the decision archive, stores it in
   * samples/<useCaseId>/, publishes a LOAD ArchiveControlCmd (see scripts/broadcast-reload.sh) to
   * the archive-control topic(s) of the use case and waits for the job's acknowledgement.
   * The archive is always stored; agents.json is updated only when updateAgentsJson=true (default
   * false) and only after the job confirmed the load.
   */
  router.post('/:id/refresh-rules', async (req, res) => {
    if (!findRuntime(req.params.id)) {
      return res.status(404).json({ error: `DI runtime '${req.params.id}' not configured` });
    }
    const ctx = await prepareLoad(req, res);
    if (!ctx) return undefined;
    const { useCaseId, descriptor, topics } = ctx;

    // The archive must belong to the Decision Service bound to the use case (fail closed).
    // A missing decisionId is left to downloadArchive, which answers 400.
    const { decisionId } = req.query;
    if (decisionId) {
      if (typeof decisionId !== 'string') {
        return res.status(400).json({ error: 'decisionId must be a single value' });
      }
      const services = boundServices(descriptor);
      if (services.length === 0) {
        return res.status(422).json({
          error: `Cannot determine the Decision Service of use case '${useCaseId}': set ruleset.path `
            + '(<Service>_<version>.jar) or ruleset.project in its agents.json.',
        });
      }
      if (!isDecisionBound(decisionId, services)) {
        return res.status(403).json({
          error: `Archive '${decisionId}' does not belong to the Decision Service of use case '${useCaseId}' `
            + `(${services.join(', ')}). Select a version of that service.`,
        });
      }
    }

    return exclusive(useCaseId, res, async () => {
      const archive = await downloadArchive(req, res);
      if (!archive) return undefined;
      try {
        const sha256 = crypto.createHash('sha256').update(archive.buffer).digest('hex');
        const stored = await storeArchive(useCaseId, archive.filename, archive.buffer, sha256);
        const updateAgentsJson = req.query.updateAgentsJson === 'true';
        const { status, body } = await loadAndConfirm({
          useCaseId, descriptor, topics, filename: stored.filename, sha256, requestPrefix: 'refresh-rules',
          updateAgentsJson,
        });
        // Cleanup only after a fully confirmed load (200); every failure path keeps the file.
        // Only a file written by this request is removed: an identical file that already existed
        // may be the running, default or rollback archive.
        if (status === 200 && !updateAgentsJson) {
          if (stored.reused) {
            body.archiveDeleted = false;
            console.log(`[di] '${useCaseId}': ${stored.filename} kept — it already existed before this refresh`);
          } else {
            body.archiveDeleted = (await discardArchive(useCaseId, stored.filename)).deleted;
          }
        }
        return res.status(status).json(body);
      } catch (err) {
        console.error(`[di] Rules refresh failed for '${useCaseId}':`, err.message);
        return res.status(500).json({ error: `Rules refresh failed: ${err.message}` });
      }
    });
  });

  /**
   * POST /api/ads-runtimes/restore-default?useCaseId=<id>
   * Reloads the default archive (agents[].ruleset.defaultPath — the jar the use case shipped
   * with) into the running job; same acknowledgement rule as refresh-rules.
   */
  router.post('/restore-default', async (req, res) => {
    const ctx = await prepareLoad(req, res);
    if (!ctx) return undefined;
    const { useCaseId, descriptor, topics } = ctx;

    const defaults = new Set(targetAgents(descriptor, topics).map(a => defaultJarPath(a.ruleset)).filter(Boolean));
    if (defaults.size !== 1) {
      return res.status(422).json({
        error: defaults.size === 0
          ? `Use case '${useCaseId}' has no default archive (ruleset.path).`
          : `The agents of use case '${useCaseId}' have different default archives; restore cannot pick one.`,
      });
    }
    const [defaultPath] = defaults;
    const normalized = defaultPath.replace(/\\/g, '/');
    const filename = baseName(normalized);
    if (normalized !== `samples/${useCaseId}/${filename}` || !/\.jar$/i.test(filename)) {
      return res.status(422).json({
        error: `The default archive '${defaultPath}' must be a .jar under samples/${useCaseId}/ to be reloaded.`,
      });
    }

    return exclusive(useCaseId, res, async () => {
      let buffer;
      try {
        buffer = await fs.readFile(path.join(samplesDir, useCaseId, filename));
      } catch (err) {
        return res.status(404).json({ error: `The default archive ${filename} is not available: ${err.message}` });
      }
      try {
        const sha256 = crypto.createHash('sha256').update(buffer).digest('hex');
        const { status, body } = await loadAndConfirm({
          useCaseId, descriptor, topics, filename, sha256, requestPrefix: 'restore-default',
        });
        return res.status(status).json(status === 200 ? { ...body, restored: true } : body);
      } catch (err) {
        console.error(`[di] Restoring the default archive of '${useCaseId}' failed:`, err.message);
        return res.status(500).json({ error: `Restoring the default archive failed: ${err.message}` });
      }
    });
  });

  /**
   * GET /api/ads-runtimes/rules-status?useCaseId=<id>
   * The archive the use case runs (agents.json ruleset.path — written only once the job confirmed
   * a load) and its default (ruleset.defaultPath, else the current path).
   * Returns { useCaseId, active, default, isDefault, inSync } — active/default are
   * { filename, path, service, version } or null.
   */
  router.get('/rules-status', async (req, res) => {
    const { useCaseId } = req.query;
    if (!useCaseId || typeof useCaseId !== 'string' || !isValidAgentId(useCaseId)) {
      return res.status(400).json({ error: 'useCaseId query parameter is required and must be a valid use case id' });
    }
    const loaded = await readUseCaseDescriptor(useCaseId);
    if (loaded.error) return res.status(loaded.status).json({ error: loaded.error });
    if (loaded.parseError) return res.status(500).json({ error: `Could not read agents.json: ${loaded.parseError}` });
    return res.json({ useCaseId, ...rulesStatus(loaded.descriptor) });
  });

  return router;
}

module.exports = {
  createAdsRouter, resolveRulesetService, boundServices, isDecisionBound, rulesStatus, archiveFileName,
};
