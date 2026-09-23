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
const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');
const { Kafka } = require('kafkajs');
const cors = require('cors');
const fs = require('fs').promises;
const path = require('path');
const multer = require('multer');

const app = express();
app.use(cors());
app.use(express.json());

const server = http.createServer(app);

// ── WebSocket push server ─────────────────────────────────────────────────────
// Clients connect to ws://<host>/ws and send:
//   { type: 'subscribe', useCase: '<id>' }
// The server then pushes:
//   { type: 'events', useCase: '<id>', data: { [topicName]: Array<event> } }
// whenever new Kafka messages arrive for that use-case.
// A 'pong' message is sent in response to any 'ping' to keep the connection alive.
const wss = new WebSocketServer({ server, path: '/ws' });

// Map<useCase, Set<WebSocket>> — tracks which sockets are subscribed to each use-case
const wsSubscriptions = new Map();

wss.on('connection', (ws) => {
  let subscribedUseCase = null;

  ws.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw.toString());
      if (msg.type === 'subscribe' && msg.useCase) {
        // Unsubscribe from any previous use-case
        if (subscribedUseCase && wsSubscriptions.has(subscribedUseCase)) {
          wsSubscriptions.get(subscribedUseCase).delete(ws);
        }
        subscribedUseCase = msg.useCase;
        if (!wsSubscriptions.has(subscribedUseCase)) {
          wsSubscriptions.set(subscribedUseCase, new Set());
        }
        wsSubscriptions.get(subscribedUseCase).add(ws);

        // Start the Kafka consumer for this use-case (idempotent — safe to call multiple times).
        // Without this, the consumer would only be started by the HTTP-poll fallback path
        // (GET /api/use-cases/:useCase/events), meaning events published while a WebSocket
        // client is connected would never be stored or broadcast.
        ensureConsumerForUseCase(subscribedUseCase)
          .then(async () => {
            // Send the current event snapshot immediately so the client does not have to wait
            // for the next Kafka message before seeing already-stored events.
            const descriptor = await getUseCaseDescriptor(subscribedUseCase).catch(() => null);
            if (!descriptor) return;
            const useCaseStore = eventStore.get(subscribedUseCase) || new Map();
            const data = {};
            for (const t of descriptor.topics ?? []) {
              data[t.name] = useCaseStore.get(t.name) || [];
            }
            for (const ot of descriptor.outboundTopics ?? []) {
              if (ot.name) data[ot.name] = useCaseStore.get(ot.name) || [];
            }
            if (ws.readyState === ws.OPEN) {
              ws.send(JSON.stringify({ type: 'events', useCase: subscribedUseCase, data }));
            }
          })
          .catch(err => console.error(`[ws] Failed to start consumer for '${subscribedUseCase}':`, err.message));
      } else if (msg.type === 'ping') {
        ws.send(JSON.stringify({ type: 'pong' }));
      }
    } catch { /* ignore malformed frames */ }
  });

  ws.on('close', () => {
    if (subscribedUseCase && wsSubscriptions.has(subscribedUseCase)) {
      wsSubscriptions.get(subscribedUseCase).delete(ws);
    }
  });
});

/**
 * Broadcasts the current event snapshot for a use-case to all subscribed WebSocket clients.
 * Called by the Kafka consumer each time a new event is stored.
 * Silently drops dead connections (readyState !== OPEN).
 */
function broadcastEvents(useCase, descriptor) {
  const subs = wsSubscriptions.get(useCase);
  if (!subs || subs.size === 0) return;

  const useCaseStore = eventStore.get(useCase) || new Map();
  const data = {};
  if (descriptor) {
    for (const t of descriptor.topics ?? []) {
      data[t.name] = useCaseStore.get(t.name) || [];
    }
    for (const ot of descriptor.outboundTopics ?? []) {
      if (ot.name) data[ot.name] = useCaseStore.get(ot.name) || [];
    }
  }
  const payload = JSON.stringify({ type: 'events', useCase, data });

  for (const ws of subs) {
    if (ws.readyState === ws.OPEN) {
      ws.send(payload);
    } else {
      subs.delete(ws); // prune stale connection
    }
  }
}

// Configuration
const KAFKA_BROKER = process.env.KAFKA_BOOTSTRAP_SERVERS || 'localhost:9092';
const PORT = process.env.PORT || 8080;
const SAMPLES_DIR = process.env.SAMPLES_DIR || path.join(__dirname, '../../samples');
const FLINK_URL   = process.env.FLINK_URL || null; // e.g. http://jobmanager:8081

// ── DI Runtime configuration ─────────────────────────────────────────────────
// DI_RUNTIMES is a JSON array of runtime objects:
//   [{ "id": "di-prod", "name": "DI Production", "url": "https://di.example.com",
//      "username": "...", "password": "..." }]
// username/password are optional and used for HTTP Basic auth.
// When omitted the env var defaults to an empty array (no runtimes configured).
const DI_RUNTIMES = (() => {
  const raw = process.env.DI_RUNTIMES;
  if (!raw || !raw.trim()) return [];
  try {
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) {
      console.warn('[ads] DI_RUNTIMES must be a JSON array — ignoring');
      return [];
    }
    return parsed.filter(r => r && typeof r.id === 'string' && typeof r.url === 'string');
  } catch (e) {
    console.warn('[ads] Failed to parse DI_RUNTIMES JSON:', e.message);
    return [];
  }
})();

// Fat JAR path — explicit env var wins (Docker mount); otherwise auto-resolved from the
// local build output so local-dev works without any extra env configuration.
const JAR_PATH = (() => {
  if (process.env.JAR_PATH) return process.env.JAR_PATH;
  const targetDir = path.join(__dirname, '../../event-rule-backend/target');
  try {
    const entries = require('fs').readdirSync(targetDir);
    const jar = entries.find(f => /^event-rule-backend-.*(?<!original)\.jar$/.test(f));
    if (jar) return path.join(targetDir, jar);
  } catch { /* target dir not built yet — JAR_PATH stays null */ }
  return null;
})();

// Topic configuration applied to every topic created by this service
const TOPIC_PARTITIONS        = parseInt(process.env.TOPIC_PARTITIONS        || '3', 10);
const TOPIC_REPLICATION       = parseInt(process.env.TOPIC_REPLICATION_FACTOR || '1', 10);

// Initialize Kafka
const kafka = new Kafka({
  clientId: 'event-rule-bff',
  brokers: [KAFKA_BROKER],
  retry: {
    initialRetryTime: 100,
    retries: 8
  }
});

// ── Shared Kafka producer ─────────────────────────────────────────────────────
// A single long-lived producer is reused across all event-injection requests.
// Creating a producer per request (connect + send + disconnect) wastes TCP
// connections and is slow; this is connected once at startup and torn down on
// graceful server shutdown.
const sharedProducer = kafka.producer();
let sharedProducerReady = false;

async function ensureProducerConnected() {
  if (!sharedProducerReady) {
    await sharedProducer.connect();
    sharedProducerReady = true;
    console.log('[kafka] Shared producer connected');
  }
}

// Maximum number of events retained per topic (ring-buffer cap)
const MAX_EVENTS_PER_TOPIC = 50;

// Maximum entity snapshots retained per agent in entityStateStore.
// When the cap is reached the oldest-updated entity is evicted (LRU-style).
// Override via MAX_ENTITIES_PER_AGENT env var.
const MAX_ENTITIES_PER_AGENT = parseInt(process.env.MAX_ENTITIES_PER_AGENT || '1000', 10);

// Store for latest events (in-memory cache) - organized by use-case and topic
const eventStore = new Map(); // Map<useCase, Map<topicName, Array<events>>>

// ── Entity state store ────────────────────────────────────────────────────────
// Persistent map of the latest entity snapshots received from changelog topics.
// The value for a given entity is simply overwritten each time a newer changelog
// message arrives.  When an agent's entity map exceeds MAX_ENTITIES_PER_AGENT
// the oldest (by timestamp) entry is evicted to prevent unbounded memory growth.
//
// Structure: Map<useCaseId, Map<agentId, Map<entityId, snapshot>>>
//   snapshot = { entityId, entityType, agentId, entityData, timestamp }
//
// Scoped by agentId so that two agents in the same use-case that happen to share
// the same entityType string (e.g. two pipelines both processing "Account") remain
// completely independent.
const entityStateStore = new Map(); // Map<useCaseId, Map<agentId, Map<entityId, snapshot>>>

/**
 * Returns (creating if needed) the inner Map<entityId, snapshot> for a given
 * use-case + agent combination.
 */
function getEntityAgentMap(useCaseId, agentId) {
  if (!entityStateStore.has(useCaseId)) entityStateStore.set(useCaseId, new Map());
  const byAgent = entityStateStore.get(useCaseId);
  if (!byAgent.has(agentId)) byAgent.set(agentId, new Map());
  return byAgent.get(agentId);
}

/**
 * Upserts a changelog envelope into entityStateStore.
 * Keeps only the snapshot with the highest timestamp for each entityId.
 *
 * @param {string} useCaseId
 * @param {string} agentId      - owning agent (from agents.json)
 * @param {string} entityType   - e.g. "Account", "Aircraft"
 * @param {object} envelope     - raw Kafka value as parsed JSON
 *                                (EventEnvelope shape: { entityId, timestamp, payload: { entity, ... } })
 */
function upsertEntityState(useCaseId, agentId, entityType, envelope) {
  // EventEnvelope shape: { agentEventType, entityId, sourceTopic, timestamp, payload: { entityId, entityType, timestamp, entity } }
  const inner   = envelope.payload || envelope;
  const entityId = inner.entityId || envelope.entityId;
  const entity   = inner.entity;
  if (!entityId || !entity) return;

  const tsMs = typeof inner.timestamp === 'number' && inner.timestamp > 0
    ? inner.timestamp
    : (typeof envelope.timestamp === 'number' && envelope.timestamp > 0 ? envelope.timestamp : null);

  const agentMap = getEntityAgentMap(useCaseId, agentId);
  const existing = agentMap.get(entityId);

  // Replace when the incoming snapshot is newer or equal-timestamp (last-write-wins).
  // Using >= (not >) ensures that scenario replays with identical timestamps correctly
  // overwrite the previous snapshot rather than silently retaining stale state.
  if (!existing || !existing.timestamp || (tsMs !== null && tsMs >= existing.timestamp)) {
    agentMap.set(entityId, {
      entityId,
      entityType:  inner.entityType || entityType || null,
      agentId,
      entityData:  entity,
      timestamp:   tsMs,
    });

    // LRU eviction: when cap is exceeded, remove the entity with the oldest timestamp.
    if (agentMap.size > MAX_ENTITIES_PER_AGENT) {
      let oldestKey = null;
      let oldestTs  = Infinity;
      for (const [key, snap] of agentMap) {
        const t = snap.timestamp || 0;
        if (t < oldestTs) { oldestTs = t; oldestKey = key; }
      }
      if (oldestKey !== null) agentMap.delete(oldestKey);
    }
  }
}

// Active Kafka consumers by use-case — started lazily on first poll, kept alive permanently
const activeConsumers = new Map(); // Map<useCase, consumer>
const consumerInitPromises = new Map(); // Map<useCase, Promise<void>>

// ── Agent job-state registry ──────────────────────────────────────────────────
// Map<agentId, { jobId: string, startedAt: number }>
// Persisted to samples/<id>/.job-state.json so it survives BFF restarts.
const agentJobState = new Map();

async function loadAgentJobStates() {
  try {
    const entries = await fs.readdir(SAMPLES_DIR, { withFileTypes: true });
    for (const e of entries) {
      if (!e.isDirectory()) continue;
      const stateFile = path.join(SAMPLES_DIR, e.name, '.job-state.json');
      try {
        const raw = await fs.readFile(stateFile, 'utf8');
        agentJobState.set(e.name, JSON.parse(raw));
      } catch { /* no state file yet — skip */ }
    }
  } catch { /* SAMPLES_DIR not readable yet — skip */ }
}

async function persistAgentJobState(agentId, state) {
  const stateFile = path.join(SAMPLES_DIR, agentId, '.job-state.json');
  await fs.writeFile(stateFile, JSON.stringify(state, null, 2), 'utf8');
}

async function removeAgentJobState(agentId) {
  agentJobState.delete(agentId);
  const stateFile = path.join(SAMPLES_DIR, agentId, '.job-state.json');
  await fs.unlink(stateFile).catch(() => {});
}

/**
 * ensureConsumerForUseCase
 *
 * Lazily starts the Kafka consumer + historical backfill for a use case the
 * first time it is polled.  Subsequent calls are no-ops.  The consumer is
 * kept alive for the lifetime of the BFF process — there is no per-client
 * reference counting and no stop/restart cycle, which eliminates the
 * duplicated-event bug that arose when a restarting consumer replayed
 * already-backfilled messages.
 */
async function ensureConsumerForUseCase(useCase) {
  if (activeConsumers.has(useCase)) return; // already running
  if (consumerInitPromises.has(useCase)) {
    return consumerInitPromises.get(useCase); // join in-flight initialization
  }

  const initPromise = (async () => {
    const descriptor = await getUseCaseDescriptor(useCase);
    if (!descriptor) throw new Error(`Use case '${useCase}' not found`);

    if (!eventStore.has(useCase)) {
      eventStore.set(useCase, new Map());
    }

    // Build changelog topic → { agentId, entityType } lookup for this use-case.
    // agents.json is optional (wizard-created agents may only have a descriptor);
    // if it can't be read we fall back to an empty map — no entityStateStore updates.
    let changelogTopicAgentMap = new Map(); // topicName → { agentId, entityType }
    try {
      const raw = await fs.readFile(path.join(SAMPLES_DIR, useCase, 'agents.json'), 'utf8');
      const agentsJson = JSON.parse(raw);
      for (const agent of (agentsJson.agents || [])) {
        const clf = agent.stateConfig?.changeLogTopic;
        if (clf && clf.trim() && agent.agentId) {
          changelogTopicAgentMap.set(clf.trim(), {
            agentId:    agent.agentId,
            entityType: agent.entityType || null,
          });
        }
      }
    } catch { /* agents.json absent or malformed — proceed without changelog upserts */ }

    await fetchHistoricalEvents(useCase, descriptor, changelogTopicAgentMap).catch(err =>
      console.warn(`[history] Backfill failed for '${useCase}' (non-fatal): ${err.message}`)
    );
    await startKafkaConsumerForUseCase(useCase, descriptor, changelogTopicAgentMap);
  })().finally(() => {
    consumerInitPromises.delete(useCase);
  });

  consumerInitPromises.set(useCase, initPromise);
  return initPromise;
}

// Get list of available use cases
// ─── Flink job probe ─────────────────────────────────────────────────────────
// Returns a Set of RUNNING Flink job names, or null when Flink is unreachable.
// null means "no filter" — all use cases are shown (safe fallback).

async function getRunningFlinkJobs() {
  if (!FLINK_URL) return null;
  try {
    const res = await fetch(`${FLINK_URL}/jobs/overview`, { signal: AbortSignal.timeout(3000) });
    if (!res.ok) return null;
    const { jobs } = await res.json();
    const running = jobs.filter(j => j.state === 'RUNNING');
    return {
      // Set of job names — used by UseCaseSelector Flink poll and existing descriptors
      names: new Set(running.map(j => j.name)),
      // Set of job IDs (jid) — used for exact matching of wizard-created agents
      ids:   new Set(running.map(j => j.jid)),
    };
  } catch {
    // Flink not reachable — degrade gracefully, show all tiles
    return null;
  }
}

// Backwards-compat wrapper: returns Set<name> or null (used by /api/flink/status)
async function getRunningFlinkJobNames() {
  const result = await getRunningFlinkJobs();
  return result ? result.names : null;
}

// ── Active-agent guard ────────────────────────────────────────────────────────

/**
 * Returns true when the agent identified by `id` is currently running in Flink.
 * Always returns false when Flink is unreachable so that operations are never
 * silently blocked due to infra issues.
 *
 * @param {string} id  use-case folder name / agentId
 * @returns {Promise<boolean>}
 */
async function isAgentActivelyRunning(id) {
  if (!FLINK_URL) return false;
  try {
    const stored = agentJobState.get(id);
    const overviewRes = await fetch(`${FLINK_URL}/jobs/overview`, { signal: AbortSignal.timeout(5000) });
    if (!overviewRes.ok) return false;
    const { jobs } = await overviewRes.json();
    const ACTIVE = new Set(['RUNNING', 'RESTARTING', 'CREATED', 'INITIALIZING', 'RECONCILING']);

    if (stored) {
      // Wizard-created agent: match by the exact job ID we submitted.
      return (jobs || []).some(j => j.jid === stored.jobId && ACTIVE.has(j.state));
    }
    // Agent without a stored job ID: match by the flinkJobName declared in the descriptor.
    const descriptor = await getUseCaseDescriptor(id);
    if (!descriptor?.flinkJobName) return false;
    return (jobs || []).some(j => j.name === descriptor.flinkJobName && ACTIVE.has(j.state));
  } catch {
    // Flink not reachable — do not block operations.
    return false;
  }
}

async function getUseCases() {
  try {
    const [entries, flinkRunning] = await Promise.all([
      fs.readdir(SAMPLES_DIR, { withFileTypes: true }),
      getRunningFlinkJobs(),
    ]);
    const useCases = [];
    
    for (const entry of entries) {
      if (entry.isDirectory()) {
        const descriptorPath = path.join(SAMPLES_DIR, entry.name, 'descriptor.json');
        try {
          await fs.access(descriptorPath);
          const descriptor = await getUseCaseDescriptor(entry.name);
          if (!descriptor) continue;

          // Gate: only show tile when the corresponding Flink job is RUNNING.
          // If Flink is unreachable (flinkRunning === null) show all tiles as fallback.
          if (flinkRunning !== null && descriptor.flinkJobName) {
            const stored = agentJobState.get(entry.name);
            let isRunning = false;

            if (stored) {
              // Wizard-created agent: match by the exact job ID we submitted.
              // The Flink display name is always the compiled-in DEFAULT_JOB_NAME
              // ("Agent-Based Event Processor") regardless of pipeline.name config,
              // so name-based matching never works for agent jobs.
              isRunning = flinkRunning.ids.has(stored.jobId);
            } else {
              // Static/pre-configured use case (agent-banking, agent-retail):
              // no stored job ID — match by the flinkJobName declared in descriptor.json.
              isRunning = flinkRunning.names.has(descriptor.flinkJobName);
            }

            if (!isRunning) continue;
          }

          useCases.push({
            id:          entry.name,
            name:        descriptor.name,
            topics:      descriptor.topics,
            // Optional display metadata — passed through from descriptor.json when present.
            // Hand-authored descriptors can set these; wizard-created ones inherit them from
            // buildStubDescriptor which derives them from agents.json.
            ...(descriptor.description && { description: descriptor.description }),
            ...(descriptor.icon        && { icon:        descriptor.icon }),
          });
        } catch (err) {
          // Skip directories without descriptor.json
          console.log(`Skipping ${entry.name}: no descriptor.json found`);
        }
      }
    }
    
    return useCases;
  } catch (error) {
    console.error('Error reading use cases:', error);
    return [];
  }
}

// Get descriptor for a specific use case
async function getUseCaseDescriptor(useCase) {
  try {
    const descriptorPath = path.join(SAMPLES_DIR, useCase, 'descriptor.json');
    const content = await fs.readFile(descriptorPath, 'utf8');
    return JSON.parse(content);
  } catch (error) {
    console.error(`Error reading descriptor for ${useCase}:`, error);
    return null;
  }
}

/**
 * fetchHistoricalEvents
 *
 * Reads every message currently stored in Kafka (up to MAX_EVENTS_PER_TOPIC
 * per topic type) and populates the in-memory eventStore for the given use
 * case.  Must be called BEFORE the live consumer starts so that the
 * `initial` WebSocket snapshot contains all pre-existing events.
 *
 * Strategy:
 *  1. Use the admin client to fetch the current high-water mark (end offset)
 *     for every partition of every topic.
 *  2. If all partitions are empty (high == '0') there is nothing to fetch —
 *     return immediately.
 *  3. Spin up a throwaway consumer with a unique group ID (a UUID suffix) so
 *     it never shares committed offsets with the live consumer group.
 *  4. Manually seek each partition to offset 0, consume up to the high-water
 *     mark, then stop.  Events are written into the eventStore exactly like
 *     the live consumer does (ring-buffer cap, sentinel suppression).
 *  5. Disconnect and discard the throwaway consumer — it leaves no trace in
 *     Kafka's consumer-group registry that would affect the live consumer.
 */
async function fetchHistoricalEvents(useCase, descriptor, changelogTopicAgentMap = new Map()) {
  const outboundTopics = descriptor.outboundTopics || [];
  const topicNames = [
    ...descriptor.topics.map(t => t.name),
    ...outboundTopics.filter(ot => ot.name).map(ot => ot.name),
  ];

  // Build topic → type mapping (same as the live consumer uses)
  const topicTypeMap = new Map();
  descriptor.topics.forEach(t => topicTypeMap.set(t.name, t.type));
  outboundTopics.forEach(ot => { if (ot.name) topicTypeMap.set(ot.name, ot.type); });

  // ── Step 1: fetch high-water marks via admin ──────────────────────────────
  const admin = kafka.admin();
  let offsetsByTopic; // Map<topicName, Map<partition, { low: bigint, high: bigint }>>
  try {
    await admin.connect();
    offsetsByTopic = new Map();
    for (const topic of topicNames) {
      let offsets;
      try {
        offsets = await admin.fetchTopicOffsets(topic);
      } catch (err) {
        if (isUnknownTopicError(err)) continue; // topic not yet created
        throw err;
      }
      const partMap = new Map();
      for (const { partition, low, high } of offsets) {
        partMap.set(partition, {
          low:  BigInt(low ?? '0'),
          high: BigInt(high ?? '0'),
        });
      }
      offsetsByTopic.set(topic, partMap);
    }
  } finally {
    await admin.disconnect().catch(() => {});
  }

  // If every partition has low === high (or high === 0) there are no accessible messages to replay
  const hasMessages = [...offsetsByTopic.values()].some(pm =>
    [...pm.values()].some(({ low, high }) => high > low)
  );
  if (!hasMessages) {
    console.log(`[history] No pre-existing messages for use case '${useCase}' — skipping backfill`);
    return;
  }

  // ── Step 2: create a throwaway consumer ──────────────────────────────────
  // Unique group ID → no committed offsets → safe to seek from 0
  const throwawayGroupId = `event-rule-bff-history-${useCase}-${Date.now()}`;
  const histConsumer = kafka.consumer({
    groupId: throwawayGroupId,
    sessionTimeout: 30000,
    heartbeatInterval: 3000,
    // Reading from the beginning, we want low latency — small fetch wait
    maxWaitTimeInMs: 100,
  });

  try {
    await histConsumer.connect();

    // Subscribe with fromBeginning so KafkaJS assigns all partitions
    const topicsWithMessages = [...offsetsByTopic.keys()].filter(t =>
      [...offsetsByTopic.get(t).values()].some(({ low, high }) => high > low)
    );
    await histConsumer.subscribe({ topics: topicsWithMessages, fromBeginning: true });

    const useCaseStore = eventStore.get(useCase);

    // Track partitions that still have messages to consume up to high-water mark
    const remaining = new Map(); // topic → Set<partition>
    for (const [topic, partMap] of offsetsByTopic) {
      if (!topicsWithMessages.includes(topic)) continue;
      const parts = new Set();
      for (const [partition, { low, high }] of partMap) {
        if (high > low) parts.add(partition);
      }
      if (parts.size > 0) remaining.set(topic, parts);
    }

    let totalFetched = 0;

    await new Promise((resolve, reject) => {
      // Seek every assigned partition to its low-water mark (or 0) once the consumer joins the group
      histConsumer.on(histConsumer.events.GROUP_JOIN, () => {
        for (const [topic, partMap] of offsetsByTopic) {
          for (const [partition, { low }] of partMap) {
            histConsumer.seek({ topic, partition, offset: low.toString() });
          }
        }
      });

      histConsumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          try {
            const topicType = topicTypeMap.get(topic);
            if (!topicType) return;

            const msgOffset = BigInt(message.offset);
            const partOffsets = offsetsByTopic.get(topic)?.get(partition);
            const hwm = partOffsets?.high ?? 0n;

            // Skip any message that arrived after the HWM snapshot was taken.
            if (msgOffset >= hwm) return;

            const value = JSON.parse(message.value.toString());

            // Suppress reset sentinels — same rule as the live consumer
            if (value.__eventType !== '__ENTITY_RESET__') {
              if (!useCaseStore.has(topic)) useCaseStore.set(topic, []);
              const topicEvents = useCaseStore.get(topic);
              topicEvents.push({ ...value });
              if (topicEvents.length > MAX_EVENTS_PER_TOPIC) {
                topicEvents.splice(0, topicEvents.length - MAX_EVENTS_PER_TOPIC);
              }
              totalFetched++;

              // Upsert entityStateStore for changelog topics
              const changelogAgent = changelogTopicAgentMap.get(topic);
              if (changelogAgent) {
                upsertEntityState(useCase, changelogAgent.agentId, changelogAgent.entityType, value);
              }
            }

            // Mark partition done when we reach its last pre-HWM message (offset hwm - 1)
            const parts = remaining.get(topic);
            if (parts && msgOffset >= hwm - 1n) {
              parts.delete(partition);
              if (parts.size === 0) remaining.delete(topic);
            }

            // All partitions of all topics consumed up to their HWM → done
            if (remaining.size === 0) resolve();
          } catch (err) {
            reject(err);
          }
        },
      }).catch(reject);
    });

    console.log(`[history] Backfilled ${totalFetched} event(s) for use case '${useCase}'`);
  } finally {
    await histConsumer.disconnect().catch(() => {});
  }
}

// Kafka consumer setup for a specific use case
async function startKafkaConsumerForUseCase(useCase, descriptor, changelogTopicAgentMap = new Map()) {
  // Use a session-unique group ID so this consumer always starts from the
  // latest offset (fromBeginning: false) and never replays messages that
  // were already loaded into the store by fetchHistoricalEvents.
  // A stable group ID retains committed offsets across restarts, which
  // causes those already-backfilled messages to be delivered again and
  // duplicated in the event panels.
  const consumerGroupId = `event-rule-bff-${useCase}-${Date.now()}`;
  console.log(`Creating Kafka consumer with group ID: ${consumerGroupId}`);
  
  const consumer = kafka.consumer({
    groupId: consumerGroupId,
    sessionTimeout: 30000,
    heartbeatInterval: 3000
  });

  await consumer.connect();
  console.log(`Kafka consumer connected for use case: ${useCase}`);

  // Subscribe to inbound topics
  const topicNames = descriptor.topics.map(t => t.name);
  
  // Add all outbound topics
  const outboundTopics = descriptor.outboundTopics || [];
  for (const ot of outboundTopics) {
    if (ot.name) topicNames.push(ot.name);
  }
  console.log(`Subscribing to topics: ${topicNames.join(', ')}`);
  
  await consumer.subscribe({
    topics: topicNames,
    fromBeginning: false
  });
  
  console.log(`Successfully subscribed to ${topicNames.length} topics`);

  // Create topic type mapping for inbound topics
  const topicTypeMap = new Map();
  descriptor.topics.forEach(topic => {
    topicTypeMap.set(topic.name, topic.type);
  });
  
  // Add all outbound topic mappings
  for (const ot of outboundTopics) {
    if (ot.name) topicTypeMap.set(ot.name, ot.type);
  }

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const value = JSON.parse(message.value.toString());
        const topicType = topicTypeMap.get(topic);
        
        if (!topicType) {
          console.warn(`Unknown topic: ${topic}`);
          return;
        }

        // Suppress reset sentinels from the eventStore and UI.
        // Additionally clear any stale entityStateStore snapshot for the reset entity
        // so that the next changelog event after the reset is always accepted,
        // regardless of whether its timestamp equals the pre-reset snapshot's timestamp.
        if (value.__eventType === '__ENTITY_RESET__') {
          const keyField = Object.keys(value).find(k => k !== '__eventType');
          if (keyField) {
            const resetEntityId = String(value[keyField]);
            for (const [, agentDesc] of changelogTopicAgentMap) {
              const agentMap = entityStateStore.get(useCase)?.get(agentDesc.agentId);
              if (agentMap && agentMap.has(resetEntityId)) {
                agentMap.delete(resetEntityId);
              }
            }
          }
          return;
        }
        
        // Get or create event store for this use case and topic name.
        // The store entry may have been deleted by reset-topics while the consumer
        // keeps running — re-initialise it transparently so incoming events are
        // never silently dropped.
        if (!eventStore.has(useCase)) {
          eventStore.set(useCase, new Map());
        }
        const useCaseStore = eventStore.get(useCase);
        if (!useCaseStore.has(topic)) {
          useCaseStore.set(topic, []);
        }

        // Store the event — trim to ring-buffer cap to prevent unbounded growth
        const event = { ...value };
        const topicEvents = useCaseStore.get(topic);
        topicEvents.push(event);
        if (topicEvents.length > MAX_EVENTS_PER_TOPIC) {
          topicEvents.splice(0, topicEvents.length - MAX_EVENTS_PER_TOPIC);
        }

        // Upsert entityStateStore for changelog topics — live path
        const changelogAgent = changelogTopicAgentMap.get(topic);
        if (changelogAgent) {
          upsertEntityState(useCase, changelogAgent.agentId, changelogAgent.entityType, value);
        }

        // Push the updated snapshot to all subscribed WebSocket clients.
        broadcastEvents(useCase, descriptor);

        console.log(`Received ${topicType} event for ${useCase}`);
      } catch (error) {
        console.error('Error processing message:', error);
      }
    },
  });

  activeConsumers.set(useCase, consumer);
}

// ── Modular route registrations ──────────────────────────────────────────────
// Self-contained route groups are extracted into routes/ modules.
// Each factory receives only the dependencies it needs, keeping routes testable
// in isolation without requiring the full server context.
const { createAdsRouter }       = require('./routes/di_route');
const { createScenariosRouter } = require('./routes/scenarios');

app.use('/api/ads-runtimes',                  createAdsRouter({
  diRuntimes:    DI_RUNTIMES,
  samplesDir:    SAMPLES_DIR,
  isValidAgentId,
}));
app.use('/api/use-cases/:useCase/scenarios',  createScenariosRouter({
  samplesDir:    SAMPLES_DIR,
  isValidAgentId,
}));

// REST endpoints

// GET /api/flink/status
// Returns the set of currently RUNNING Flink job names.
// The frontend polls this to detect when a new job starts and re-fetch use cases.
app.get('/api/flink/status', async (req, res) => {
  const running = await getRunningFlinkJobNames();
  // running is null when Flink is unreachable — return empty array so the
  // client knows "no confirmed running jobs" without treating it as an error.
  res.json({ jobs: running ? [...running] : [] });
});

app.get('/api/use-cases', async (req, res) => {
  try {
    const useCases = await getUseCases();
    res.json(useCases);
  } catch (error) {
    console.error('Error fetching use cases:', error);
    res.status(500).json({ error: 'Failed to fetch use cases' });
  }
});

app.get('/api/use-cases/:useCase/descriptor', async (req, res) => {
  const { useCase } = req.params;
  if (!isValidAgentId(useCase)) return res.status(400).json({ error: 'Invalid use case id' });
  try {
    const descriptor = await getUseCaseDescriptor(useCase);
    if (descriptor) {
      res.json(descriptor);
    } else {
      res.status(404).json({ error: 'Use case not found' });
    }
  } catch (error) {
    console.error('Error fetching descriptor:', error);
    res.status(500).json({ error: 'Failed to fetch descriptor' });
  }
});

/**
 * GET /api/use-cases/:useCase/events
 * Returns the current in-memory event snapshot for every topic of the use case.
 * Lazily starts the Kafka consumer + historical backfill on the first call.
 * The UI polls this endpoint every 2 seconds instead of using WebSockets.
 *
 * Response: { [topicType]: Array<event> }
 */
app.get('/api/use-cases/:useCase/events', async (req, res) => {
  const { useCase } = req.params;
  if (!isValidAgentId(useCase)) return res.status(400).json({ error: 'Invalid use case id' });
  try {
    await ensureConsumerForUseCase(useCase);
    const descriptor = await getUseCaseDescriptor(useCase);
    const useCaseStore = eventStore.get(useCase) || new Map();
    const data = {};
    descriptor.topics.forEach(t => {
      data[t.name] = useCaseStore.get(t.name) || [];
    });
    const outboundTopics = descriptor.outboundTopics || [];
    for (const ot of outboundTopics) {
      if (ot.name) data[ot.name] = useCaseStore.get(ot.name) || [];
    }
    res.json(data);
  } catch (err) {
    console.error(`[events] Error fetching events for '${useCase}':`, err);
    res.status(500).json({ error: err.message || 'Failed to fetch events' });
  }
});

/**
 * POST /api/use-cases/:useCase/events
 * Body: { events: [ { topic: string, payload: {...} }, ... ], delayMs?: number }
 *
 * Publishes each event to its designated Kafka topic in declaration order so
 * the Flink pipeline receives them in the exact sequence defined by the scenario.
 * Used by the frontend "Run Scenario" button.
 *
 * delayMs (optional, default 0): milliseconds to wait between consecutive
 * events, so the Flink pipeline can process each one before the next arrives.
 */
app.post('/api/use-cases/:useCase/events', async (req, res) => {
  const { useCase } = req.params;
  if (!isValidAgentId(useCase)) return res.status(400).json({ error: 'Invalid use case id' });
  const { events, delayMs } = req.body;

  if (!Array.isArray(events) || events.length === 0) {
    return res.status(400).json({ error: 'events must be a non-empty array' });
  }
  if (events.some(e => typeof e?.topic !== 'string' || !e.topic)) {
    return res.status(400).json({ error: 'each event must have a non-empty topic field' });
  }

  try {
    const descriptor = await getUseCaseDescriptor(useCase);
    if (!descriptor) {
      return res.status(404).json({ error: `Use case '${useCase}' not found` });
    }

    // Validate all referenced topics are declared in the descriptor
    const knownTopics = new Set(descriptor.topics.map(t => t.name));
    const unknownTopics = [...new Set(events.map(e => e.topic))].filter(t => !knownTopics.has(t));
    if (unknownTopics.length > 0) {
      return res.status(400).json({
        error: `Unknown topic(s) for use case '${useCase}': ${unknownTopics.join(', ')}`,
        knownTopics: [...knownTopics]
      });
    }

    await ensureProducerConnected();

    // Publish events one at a time in declaration order, with an optional delay
    // between each send so the Flink pipeline can process each event before the
    // next one arrives — regardless of which topic it targets.
    const interEventDelay = typeof delayMs === 'number' && delayMs > 0 ? delayMs : 0;
    let published = 0;
    for (const event of events) {
      if (published > 0 && interEventDelay > 0) {
        await new Promise(resolve => setTimeout(resolve, interEventDelay));
      }
      await sharedProducer.send({
        topic: event.topic,
        messages: [{ value: JSON.stringify(event.payload) }]
      });
      published++;
    }

    console.log(`Injected ${published} event(s) for use case '${useCase}'`);
    res.json({ published });
  } catch (error) {
    console.error('Error injecting events:', error);
    res.status(500).json({ error: 'Failed to publish events' });
  }
});

/**
 * POST /api/use-cases/:useCase/reset-entities
 * Resets Flink entity state for every distinct entity key touched by a scenario.
 *
 * The sentinel is published to the same Kafka topic as the scenario's first event
 * (resolved from the event's explicit "topic" field, then by matching eventType in
 * agents.json subscriptions, then falling back to subscriptions[0].topic).  This
 * guarantees that Flink's per-partition ordering delivers the sentinel before the
 * first real event of the same scenario — regardless of which topics other events
 * use — eliminating the race condition that arose when the sentinel and the first
 * scenario event were published to different topics.
 *
 * Body: { scenarioId: string }
 */
app.post('/api/use-cases/:useCase/reset-entities', async (req, res) => {
  const { useCase } = req.params;
  if (!isValidAgentId(useCase)) return res.status(400).json({ error: 'Invalid use case id' });

  const { scenarioId } = req.body || {};
  if (!scenarioId || typeof scenarioId !== 'string') {
    return res.status(400).json({ error: 'scenarioId is required' });
  }

  try {
    // Read agents.json to find entityKeyExtractor and subscription topics
    const agentsPath = path.join(SAMPLES_DIR, useCase, 'agents.json');
    let agentsJson;
    try {
      agentsJson = JSON.parse(await fs.readFile(agentsPath, 'utf8'));
    } catch (err) {
      if (err.code === 'ENOENT') return res.status(404).json({ error: `agents.json not found for '${useCase}'` });
      throw err;
    }

    const agent = (agentsJson.agents || [])[0];
    if (!agent) return res.status(404).json({ error: `No agent defined in agents.json for '${useCase}'` });

    // Validate entityKeyExtractor: only simple top-level fields (e.g. "$.accountNumber")
    // are supported.  Nested paths like "$.customer.id" cannot be resolved with a plain
    // bracket lookup against the flat payload object.
    const extractor = agent.entityKeyExtractor || '';
    const keyFieldMatch = extractor.match(/^\$\.([a-zA-Z_][a-zA-Z0-9_]*)$/);
    if (!keyFieldMatch) {
      return res.status(400).json({
        error: `entityKeyExtractor '${extractor}' is not a supported simple top-level field ` +
               `(expected form: '$.fieldName').  Entity reset is not possible for nested paths.`
      });
    }
    const keyField = keyFieldMatch[1];

    // Read demo-scenarios.json and find the target scenario
    const scenariosPath = path.join(SAMPLES_DIR, useCase, 'demo-scenarios.json');
    let scenarios;
    try {
      scenarios = JSON.parse(await fs.readFile(scenariosPath, 'utf8'));
    } catch (err) {
      if (err.code === 'ENOENT') return res.status(404).json({ error: `demo-scenarios.json not found for '${useCase}'` });
      throw err;
    }

    const scenario = scenarios.find(s => s.id === scenarioId);
    if (!scenario) return res.status(404).json({ error: `Scenario '${scenarioId}' not found` });

    // Resolve the topic for the reset sentinel.  The sentinel MUST travel on the same
    // Kafka topic as the scenario's first event so that Flink's per-partition ordering
    // guarantees the sentinel is processed before that event.
    //
    // Resolution order (most- to least-specific):
    //   1. Explicit "topic" field on the first event in the scenario
    //   2. agents.json subscription whose eventType matches the first event's type
    //   3. agents.json subscriptions[0].topic  (fallback)
    const firstEvent = (scenario.events || [])[0];
    const subscriptions = agent.subscriptions || [];
    const fallbackTopic = subscriptions[0]?.topic;

    let sentinelTopic = firstEvent?.topic;
    if (!sentinelTopic && firstEvent?.type) {
      const matchedSub = subscriptions.find(s => s.eventType === firstEvent.type);
      sentinelTopic = matchedSub?.topic;
    }
    sentinelTopic = sentinelTopic || fallbackTopic;

    if (!sentinelTopic) {
      return res.status(400).json({ error: `No subscriptions found for agent in '${useCase}'` });
    }

    // Collect distinct entity key values from all event payloads in this scenario
    const entityIds = [...new Set(
      (scenario.events || [])
        .map(e => e.payload?.[keyField])
        .filter(v => v != null && v !== '')
    )];

    if (entityIds.length === 0) {
      return res.json({ reset: 0, message: 'No entity keys found in scenario — nothing to reset' });
    }

    await ensureProducerConnected();

    // Publish one sentinel per entity key to the resolved topic, sequentially so
    // Kafka's intra-partition ordering is preserved end-to-end.
    for (const entityId of entityIds) {
      await sharedProducer.send({
        topic: sentinelTopic,
        messages: [{ value: JSON.stringify({ __eventType: '__ENTITY_RESET__', [keyField]: String(entityId) }) }]
      });
    }

    console.log(`[reset-entities] Reset ${entityIds.length} entity/entities (${entityIds.join(', ')}) ` +
      `via topic '${sentinelTopic}' for use case '${useCase}' scenario '${scenarioId}'`);
    res.json({ reset: entityIds.length, entityIds, topic: sentinelTopic });
  } catch (error) {
    console.error('[reset-entities] Error:', error);
    res.status(500).json({ error: 'Failed to reset entities' });
  }
});

/**
 * POST /api/use-cases/:useCase/reset-topics
 * Purges all messages from every Kafka topic associated with the use case
 * (both inbound and outbound topics from agents.json) and clears the
 * in-memory event store so the UI topic panels are immediately emptied.
 *
 * The topics are NOT deleted — they remain with the same configuration,
 * only their message history is wiped via deleteTopicRecords.
 */
app.post('/api/use-cases/:useCase/reset-topics', async (req, res) => {
  const { useCase } = req.params;
  if (!isValidAgentId(useCase)) return res.status(400).json({ error: 'Invalid use case id' });

  try {
    const descriptor = await getUseCaseDescriptor(useCase).catch(() => null);
    let agentsJson = null;
    try {
      const agentsPath = path.join(SAMPLES_DIR, useCase, 'agents.json');
      agentsJson = JSON.parse(await fs.readFile(agentsPath, 'utf8'));
    } catch (err) {
      if (err.code !== 'ENOENT') throw err;
    }

    if (!descriptor && !agentsJson) {
      return res.status(404).json({ error: `Use case descriptor not found for '${useCase}'` });
    }

    // Collect all topics from both agents.json and descriptor.json (including error and outbound topics)
    const topicSet = new Set();
    if (agentsJson) {
      for (const t of collectTopicsFromAgentsJson(agentsJson)) topicSet.add(t);
    }
    if (descriptor?.topics) {
      for (const t of descriptor.topics) if (t.name) topicSet.add(t.name);
    }
    if (descriptor?.outboundTopics) {
      for (const t of descriptor.outboundTopics) if (t.name) topicSet.add(t.name);
    }

    const topics = [...topicSet];
    if (topics.length > 0) {
      const admin = kafka.admin();
      try {
        await admin.connect();
        for (const topic of topics) {
          await purgeTopicMessages(admin, topic);
        }
      } finally {
        await admin.disconnect().catch(() => {});
      }
    }

    // Clear the in-memory event cache and entity state snapshot cache for this use case
    eventStore.delete(useCase);
    entityStateStore.delete(useCase);

    // Immediately push empty event snapshot to all connected WebSocket subscribers
    if (descriptor) {
      broadcastEvents(useCase, descriptor);
    }

    console.log(`[reset-topics] Purged ${topics.length} topic(s) for use case '${useCase}': ${topics.join(', ')}`);
    res.json({ purged: topics.length, topics });
  } catch (error) {
    console.error('[reset-topics] Error:', error);
    res.status(500).json({ error: 'Failed to reset topics' });
  }
});



app.get('/health', (req, res) => {
  const stats = {
    status: 'ok',
    activeUseCases: activeConsumers.size,
    useCases: {}
  };
  
  eventStore.forEach((topicMap, useCase) => {
    stats.useCases[useCase] = {};
    topicMap.forEach((events, topicType) => {
      stats.useCases[useCase][topicType] = events.length;
    });
  });
  
  res.json(stats);
});

// ─── Kafka topic initialisation ───────────────────────────────────────────────
//
// Reads every use-case descriptor found in SAMPLES_DIR and ensures all inbound
// and outbound topics exist in Kafka before the BFF begins serving traffic.
// Uses the KafkaJS Admin client with `createTopics({ validateOnly: false })`
// so existing topics are silently skipped (allowAutoTopicCreation is unrelated
// to this call — this is an explicit admin-level create, not a consumer-side
// auto-create).
//
// The function retries the admin connection up to ADMIN_MAX_RETRIES times so
// the BFF can start in parallel with Kafka without failing immediately.

const ADMIN_MAX_RETRIES  = 10;
const ADMIN_RETRY_DELAY  = 3000; // ms between attempts

async function initTopics() {
  const admin = kafka.admin();

  // Retry loop: Kafka may not be ready when the BFF container starts
  let connected = false;
  for (let attempt = 1; attempt <= ADMIN_MAX_RETRIES; attempt++) {
    try {
      await admin.connect();
      connected = true;
      break;
    } catch (err) {
      console.warn(`[topics] Kafka admin connect attempt ${attempt}/${ADMIN_MAX_RETRIES} failed: ${err.message}`);
      if (attempt < ADMIN_MAX_RETRIES) {
        await new Promise(r => setTimeout(r, ADMIN_RETRY_DELAY));
      }
    }
  }

  if (!connected) {
    console.error('[topics] Could not connect to Kafka admin after retries — skipping topic initialisation');
    return;
  }

  try {
    // Collect all topic names from every descriptor in SAMPLES_DIR
    const topicsToCreate = new Map(); // name → { numPartitions, replicationFactor }

    let entries;
    try {
      entries = await fs.readdir(SAMPLES_DIR, { withFileTypes: true });
    } catch (err) {
      console.warn(`[topics] Cannot read SAMPLES_DIR (${SAMPLES_DIR}): ${err.message}`);
      return;
    }

    for (const entry of entries) {
      if (!entry.isDirectory()) continue;
      const descriptorPath = path.join(SAMPLES_DIR, entry.name, 'descriptor.json');
      let descriptor;
      try {
        const content = await fs.readFile(descriptorPath, 'utf8');
        descriptor = JSON.parse(content);
      } catch {
        continue; // no descriptor.json — skip
      }

      // Inbound topics
      for (const topic of (descriptor.topics || [])) {
        if (topic.name && !topicsToCreate.has(topic.name)) {
          topicsToCreate.set(topic.name, {
            topic:             topic.name,
            numPartitions:     TOPIC_PARTITIONS,
            replicationFactor: TOPIC_REPLICATION,
          });
        }
      }

      // Outbound topics (all of them)
      for (const ot of (descriptor.outboundTopics || [])) {
        if (ot.name && !topicsToCreate.has(ot.name)) {
          topicsToCreate.set(ot.name, {
            topic:             ot.name,
            numPartitions:     TOPIC_PARTITIONS,
            replicationFactor: TOPIC_REPLICATION,
          });
        }
      }
    }

    if (topicsToCreate.size === 0) {
      console.log('[topics] No topics found in any descriptor — nothing to create');
      return;
    }

    console.log(`[topics] Ensuring ${topicsToCreate.size} topic(s) exist: ${[...topicsToCreate.keys()].join(', ')}`);

    // createTopics silently skips topics that already exist
    const result = await admin.createTopics({
      waitForLeaders: true,
      topics: [...topicsToCreate.values()],
    });

    if (result) {
      console.log('[topics] Topic(s) created successfully');
    } else {
      console.log('[topics] All topics already existed — no changes made');
    }

    // Log current topic list for visibility
    const existing = await admin.listTopics();
    const relevant = existing.filter(t => topicsToCreate.has(t));
    console.log(`[topics] Verified present: ${relevant.join(', ')}`);

  } finally {
    await admin.disconnect().catch(() => {});
  }
}

// Start server — load persisted job states, initialise topics, then accept connections
// Placed at bottom of file after all route registrations.

// ─── Agent descriptor management + Flink job lifecycle ───────────────────────

// ── Multer: disk storage that places the JAR next to the agent descriptor ─────
// storage destination is resolved per-request once the agentId is known.
const multerStorage = multer.diskStorage({
  destination: async (req, file, cb) => {
    const id = req.params.id;
    if (!isValidAgentIdRaw(id)) return cb(new Error('Invalid agent id'));
    const dir = path.join(SAMPLES_DIR, id);
    try {
      await fs.mkdir(dir, { recursive: true });
      cb(null, dir);
    } catch (err) {
      cb(err);
    }
  },
  filename: (req, file, cb) => {
    // Preserve the original filename; strip directory traversal characters
    const safe = path.basename(file.originalname).replace(/[^a-zA-Z0-9._-]/g, '_');
    cb(null, safe);
  },
});

const jarUpload = multer({
  storage: multerStorage,
  limits: { fileSize: 256 * 1024 * 1024 }, // 256 MB max
  fileFilter: (req, file, cb) => {
    if (!file.originalname.endsWith('.jar')) {
      return cb(new Error('Only .jar files are accepted'));
    }
    cb(null, true);
  },
});

/**
 * Validates a use-case / agent ID: alphanumeric, hyphens, underscores only
 * (used as a filesystem directory name).  Applied to both :id and :useCase
 * route params before they are used in path.join() calls.
 */
function isValidAgentId(id) {
  return typeof id === 'string' && /^[a-zA-Z0-9_-]{1,64}$/.test(id);
}

/** Alias kept for the Multer destination callback above (same logic). */
const isValidAgentIdRaw = isValidAgentId;

// Build a stub descriptor.json from the agents.json content so the selector tile works.
// Aggregates topics and outboundTopics from ALL agents so that multi-agent use-cases
// expose every subscribed/output topic in the dashboard tile and Kafka consumer.
function buildStubDescriptor(useCaseId, agentsJson) {
  const allAgents = agentsJson.agents || [];
  const first = allAgents[0] || {};

  // Use a Map keyed by topic name to deduplicate topics that appear in multiple agents
  // (e.g. a shared error topic declared in every agent's outputs).
  const topicsMap = new Map();
  const outboundMap = new Map();

  for (const agent of allAgents) {
    for (const s of (agent.subscriptions || [])) {
      const name = s.topic || s.eventType;
      if (name && !topicsMap.has(name)) {
        topicsMap.set(name, { name, type: s.eventType, label: s.eventType });
      }
    }
    for (const o of (agent.outputs || [])) {
      if (o.topic && !outboundMap.has(o.topic)) {
        outboundMap.set(o.topic, {
          name:  o.topic,
          type:  o.eventType,
          label: o.description || o.eventType,
          ...(o.filterKey   && { filterKey:   o.filterKey   }),
          ...(o.filterValue && { filterValue: o.filterValue }),
          ...(o.filterLabel && { filterLabel: o.filterLabel }),
        });
      }
    }
    // Changelog topic: expose it as an outbound topic so the consumer subscribes to it.
    // Each agent that declares a changeLogTopic gets a distinct type key so multi-agent
    // use-cases don't collide in the eventStore.  The type key is 'entity-changelog' for
    // the first/only agent, and 'entity-changelog-<agentId>' for subsequent agents.
    const clf = agent.stateConfig?.changeLogTopic;
    if (clf && clf.trim() && !outboundMap.has(clf.trim())) {
      const isFirst = !([...outboundMap.values()].some(e => e.type?.startsWith('entity-changelog')));
      const typeKey = isFirst ? 'entity-changelog' : `entity-changelog-${agent.agentId}`;
      outboundMap.set(clf.trim(), {
        name:  clf.trim(),
        type:  typeKey,
        label: `Entity Changelog (${agent.entityType || agent.agentId})`,
      });
    }
  }

  // Derive a display name: use the single agent's name for single-agent use-cases
  // (preserving the original behaviour); fall back to a title-cased use-case ID for
  // multi-agent use-cases where no single agent name is representative.
  const name = allAgents.length === 1
    ? (first.name || useCaseId.replace(/[-_]+/g, ' ').replace(/\b\w/g, c => c.toUpperCase()))
    : useCaseId.replace(/[-_]+/g, ' ').replace(/\b\w/g, c => c.toUpperCase());

  return {
    name,
    flinkJobName:    `${useCaseId}-job`,
    consumerGroupId: `event-rule-bff-${useCaseId}`,
    topics:          [...topicsMap.values()],
    outboundTopics:  [...outboundMap.values()],
  };
}

// Ensure all topics referenced by an agents.json exist in Kafka
async function ensureTopicsForDescriptor(descriptor) {
  const admin = kafka.admin();
  try {
    await admin.connect();
    const topicsToCreate = new Map();
    const addTopic = name => {
      if (name && !topicsToCreate.has(name))
        topicsToCreate.set(name, { topic: name, numPartitions: TOPIC_PARTITIONS, replicationFactor: TOPIC_REPLICATION });
    };
    for (const t of (descriptor.topics || [])) addTopic(t.name);
    for (const ot of (descriptor.outboundTopics || [])) addTopic(ot.name);
    if (topicsToCreate.size === 0) return;
    await admin.createTopics({ waitForLeaders: true, topics: [...topicsToCreate.values()] });
    console.log(`[topics] Ensured: ${[...topicsToCreate.keys()].join(', ')}`);
  } catch (err) {
    console.warn(`[topics] ensureTopicsForDescriptor failed: ${err.message}`);
  } finally {
    await admin.disconnect().catch(() => {});
  }
}

// Collect every Kafka topic name referenced by an agents.json object.
// Covers: all subscription topics (all entries, not just [0]), all output
// topics, and the optional archiveControlTopic on each agent's ruleset.
// Handles both sub.topic (current single-string model) and legacy sub.topics (array).
function collectTopicsFromAgentsJson(agentsJson) {
  const topics = new Set();
  for (const agent of (agentsJson.agents || [])) {
    for (const sub of (agent.subscriptions || [])) {
      // Current model: single-string topic field
      if (sub.topic && sub.topic.trim()) topics.add(sub.topic.trim());
      // Legacy / hand-authored: array of topics
      for (const t of (sub.topics || [])) {
        if (t && t.trim()) topics.add(t.trim());
      }
    }
    for (const out of (agent.outputs || [])) {
      if (out.topic && out.topic.trim()) topics.add(out.topic.trim());
    }
    if (agent.ruleset?.archiveControlTopic?.trim())
      topics.add(agent.ruleset.archiveControlTopic.trim());
    if (agent.stateConfig?.changeLogTopic?.trim())
      topics.add(agent.stateConfig.changeLogTopic.trim());
  }
  return topics;
}

// Returns true when a KafkaJS error signals that a topic is already absent.
function isUnknownTopicError(err) {
  return (
    err.type === 'UNKNOWN_TOPIC_OR_PARTITION' ||
    (err.message || '').includes('UnknownTopicOrPartition')
  );
}

// Purge all retained messages from every partition of `topic` by advancing
// each partition's low-water mark to its current high-water mark.
// This is a strict operation: it throws on any error other than
// "topic already gone" (which is treated as already-purged).
//
// Uses admin.fetchTopicOffsets() to discover all partition high-water marks,
// then admin.deleteTopicRecords() to truncate up to those offsets.
async function purgeTopicMessages(admin, topic) {
  let offsets;
  try {
    offsets = await admin.fetchTopicOffsets(topic);
  } catch (err) {
    if (isUnknownTopicError(err)) {
      // Topic already deleted — nothing to purge
      console.log(`[topics] Purge skipped for topic=${topic} (already absent)`);
      return;
    }
    throw err;
  }

  // Build the per-partition seek entries: move the log-start-offset to the
  // current high-water mark so all existing messages become inaccessible.
  const partitions = offsets.map(({ partition, high }) => ({
    partition,
    offset: high, // advance log-start-offset to high watermark → all messages deleted
  }));

  // A topic that exists but has never received a message has high === '0' on all
  // partitions.  deleteTopicRecords with offset '0' is a no-op (log-start is
  // already 0), so we skip the call to avoid an unnecessary admin round-trip.
  const nonEmpty = partitions.filter(p => p.offset !== '0' && p.offset !== 0);
  if (nonEmpty.length === 0) {
    console.log(`[topics] Purge skipped for topic=${topic} (no messages)`);
    return;
  }

  try {
    await admin.deleteTopicRecords({ topic, partitions: nonEmpty });
    console.log(`[topics] Purged topic=${topic} (${nonEmpty.length} partition(s))`);
  } catch (err) {
    if (isUnknownTopicError(err)) {
      // Topic was deleted between the offset fetch and the purge call — fine
      console.log(`[topics] Purge skipped for topic=${topic} (topic disappeared during purge)`);
      return;
    }
    throw err;
  }
}

// Purge messages from all topics owned by an agent, then delete the topics.
// Sequence (per topic):
//   1. fetchTopicOffsets — discover partition high-water marks
//   2. deleteTopicRecords — advance log-start-offset to high-water mark
//   3. deleteTopics       — remove the topic entirely
//
// Returns the list of topic names that were targeted.
// Throws if the purge step fails so that the caller can decide whether to
// proceed with topic deletion (the DELETE handler treats this as fatal).
async function deleteTopicsForAgent(agentId) {
  const agentsPath = path.join(SAMPLES_DIR, agentId, 'agents.json');
  let agentsJson;
  try {
    agentsJson = JSON.parse(await fs.readFile(agentsPath, 'utf8'));
  } catch (err) {
    if (err.code === 'ENOENT') return []; // file already gone — nothing to do
    throw err;
  }
  const topics = [...collectTopicsFromAgentsJson(agentsJson)];
  if (topics.length === 0) return [];

  const admin = kafka.admin();
  try {
    await admin.connect();

    // ── Step A: Purge messages from every topic ────────────────────────────
    // All purges run sequentially so a failure on one topic stops the loop
    // and prevents the subsequent deleteTopics call.
    for (const topic of topics) {
      await purgeTopicMessages(admin, topic);
    }

    // ── Step B: Delete the topics ─────────────────────────────────────────
    try {
      await admin.deleteTopics({ topics });
      console.log(`[topics] Deleted for agentId=${agentId}: ${topics.join(', ')}`);
    } catch (err) {
      if (isUnknownTopicError(err)) {
        console.log(`[topics] Some topics already absent for agentId=${agentId} (non-fatal)`);
      } else {
        throw err;
      }
    }

    return topics;
  } finally {
    await admin.disconnect().catch(() => {});
  }
}

// Poll GET /jobs/:jobId until the job reaches a terminal state or the timeout
// expires.  Returns { terminal: true, state } on success, or
// { terminal: false, state, reason } when the timeout is reached.
//
// Terminal Flink states: CANCELED, FINISHED, FAILED, SUSPENDED.
// If the job no longer exists (Flink 404) it is treated as already-terminal
// (CANCELED) so that callers can proceed with cleanup.
//
// timeout defaults to 60 000 ms (1 minute); pollIntervalMs defaults to 1 000 ms.
async function waitForFlinkJobTerminal(jobId, { timeoutMs = 60_000, pollIntervalMs = 1_000 } = {}) {
  const TERMINAL = new Set(['CANCELED', 'FINISHED', 'FAILED', 'SUSPENDED']);
  const deadline = Date.now() + timeoutMs;
  let lastState  = null;

  while (Date.now() < deadline) {
    try {
      const res = await fetch(`${FLINK_URL}/jobs/${jobId}`, {
        signal: AbortSignal.timeout(5000),
      });

      if (res.status === 404) {
        // Job no longer exists — treat as already terminated
        console.log(`[agents] Flink job ${jobId} not found (already gone) — treating as CANCELED`);
        return { terminal: true, state: 'CANCELED' };
      }

      if (res.ok) {
        const job = await res.json();
        lastState = job.state;
        if (TERMINAL.has(lastState)) {
          console.log(`[agents] Flink job ${jobId} reached terminal state ${lastState}`);
          return { terminal: true, state: lastState };
        }
        console.log(`[agents] Flink job ${jobId} state=${lastState} — waiting...`);
      }
    } catch (err) {
      // Network hiccup — keep polling
      console.warn(`[agents] Poll error for Flink job ${jobId}: ${err.message}`);
    }

    await new Promise(r => setTimeout(r, pollIntervalMs));
  }

  return { terminal: false, state: lastState, reason: `timed out after ${timeoutMs}ms` };
}

// Resolve the Flink JAR id already uploaded, or upload it now
async function resolveFlinkJarId() {
  if (!FLINK_URL) throw new Error('FLINK_URL is not configured');
  if (!JAR_PATH)  throw new Error('JAR_PATH is not configured');

  // Check for already-uploaded JAR
  const listRes = await fetch(`${FLINK_URL}/jars`, { signal: AbortSignal.timeout(5000) });
  if (listRes.ok) {
    const { files } = await listRes.json();
    const existing = (files || []).find(f => (f.name || '').includes('event-rule-backend'));
    if (existing) {
      console.log(`[flink] Reusing uploaded JAR id=${existing.id}`);
      return existing.id;
    }
  }

  // Upload the JAR
  console.log(`[flink] Uploading JAR from ${JAR_PATH}`);
  const jarBuffer = await fs.readFile(JAR_PATH);
  const { FormData, Blob } = await import('node:buffer').catch(() => ({}));
  // FormData / Blob available in Node 18+; fall back to raw multipart if not
  let uploadRes;
  try {
    const form = new (globalThis.FormData || FormData)();
    form.append('jarfile', new (globalThis.Blob || Blob)([jarBuffer], { type: 'application/java-archive' }), 'event-rule-backend.jar');
    uploadRes = await fetch(`${FLINK_URL}/jars/upload`, { method: 'POST', body: form, signal: AbortSignal.timeout(30000) });
  } catch {
    // Node <18 fallback: manual multipart
    const boundary = '----FlinkJarUpload';
    const header = Buffer.from(
      `--${boundary}\r\nContent-Disposition: form-data; name="jarfile"; filename="event-rule-backend.jar"\r\nContent-Type: application/java-archive\r\n\r\n`
    );
    const footer = Buffer.from(`\r\n--${boundary}--\r\n`);
    const body = Buffer.concat([header, jarBuffer, footer]);
    uploadRes = await fetch(`${FLINK_URL}/jars/upload`, {
      method: 'POST',
      headers: { 'Content-Type': `multipart/form-data; boundary=${boundary}` },
      body,
      signal: AbortSignal.timeout(30000),
    });
  }

  if (!uploadRes.ok) {
    const text = await uploadRes.text();
    throw new Error(`JAR upload failed ${uploadRes.status}: ${text}`);
  }
  const { filename } = await uploadRes.json();
  // Flink's run endpoint expects the full basename including the .jar extension,
  // e.g. "620679bf-1d7e-4ffb-ab68-447f2aa45c14_event-rule-backend.jar"
  // path.basename without a second arg preserves the extension — matches GET /jars[].id
  const jarId = path.basename(filename);
  console.log(`[flink] JAR uploaded successfully, id=${jarId}`);
  return jarId;
}

// ── POST /api/agents — save descriptor files ──────────────────────────────────
/**
 * Body: { useCaseId: string, descriptor: object, mode?: "replace"|"append" }
 *
 * mode="replace" (default): overwrites agents.json with the submitted descriptor.
 *   Behaviour is identical to the original implementation.
 *
 * mode="append": merges the submitted descriptor's agents into an existing
 *   agents.json.  If the file does not yet exist, falls back to "replace".
 *   Returns 409 when an incoming agentId already exists in the file.
 *   Does NOT block on a running job (the file write succeeds; the caller must
 *   restart the job).  Returns restartRequired:true when a job is running.
 *
 * Writes samples/<useCaseId>/agents.json + stub descriptor.json,
 * then ensures all referenced Kafka topics exist.
 */
app.post('/api/agents', async (req, res) => {
  const { useCaseId, descriptor, mode = 'replace' } = req.body || {};

  if (!isValidAgentId(useCaseId))
    return res.status(400).json({ error: 'useCaseId must be 1-64 alphanumeric/hyphen/underscore characters' });
  if (!descriptor || typeof descriptor !== 'object')
    return res.status(400).json({ error: 'descriptor must be a non-null object' });
  if (mode !== 'replace' && mode !== 'append')
    return res.status(400).json({ error: "mode must be \"replace\" or \"append\"" });

  // In replace mode, block while the job is running (existing behaviour).
  // In append mode, allow the file write and surface restartRequired instead.
  if (mode === 'replace' && await isAgentActivelyRunning(useCaseId)) {
    return res.status(409).json({
      error: `Agent '${useCaseId}' is currently running. Stop the Flink job before publishing a new descriptor.`,
    });
  }

  try {
    const dir = path.join(SAMPLES_DIR, useCaseId);
    await fs.mkdir(dir, { recursive: true });

    let finalDescriptor = descriptor;

    if (mode === 'append') {
      const agentsPath = path.join(dir, 'agents.json');
      let existing = null;
      try {
        existing = JSON.parse(await fs.readFile(agentsPath, 'utf8'));
      } catch (err) {
        if (err.code !== 'ENOENT') {
          return res.status(400).json({ error: 'CORRUPT_AGENTS_JSON', detail: err.message });
        }
        // File does not exist — fall back to replace behaviour
        existing = null;
      }

      if (existing) {
        // Duplicate agentId guard: reject if any incoming agentId already exists
        const existingIds = new Set((existing.agents || []).map(a => a.agentId));
        for (const incoming of (descriptor.agents || [])) {
          if (existingIds.has(incoming.agentId)) {
            return res.status(409).json({
              error: 'DUPLICATE_AGENT_ID',
              agentId: incoming.agentId,
              detail: `Agent '${incoming.agentId}' already exists in use-case '${useCaseId}'. Choose a different Agent ID.`,
            });
          }
        }
        // Merge: submitted descriptor's agents appended after existing agents.
        // Submitted top-level parallelism takes precedence when provided.
        const mergedAgents = [...(existing.agents || []), ...(descriptor.agents || [])];
        finalDescriptor = { ...existing, agents: mergedAgents };
        if (descriptor.parallelism != null) finalDescriptor.parallelism = descriptor.parallelism;
      }
    }

    await fs.writeFile(path.join(dir, 'agents.json'), JSON.stringify(finalDescriptor, null, 2), 'utf8');
    const stub = buildStubDescriptor(useCaseId, finalDescriptor);
    await fs.writeFile(path.join(dir, 'descriptor.json'), JSON.stringify(stub, null, 2), 'utf8');

    await ensureTopicsForDescriptor(stub).catch(err =>
      console.warn(`[agents] topic ensure failed (non-fatal): ${err.message}`)
    );

    // Inform the caller whether the Flink job needs a restart to pick up the change.
    const restartRequired = mode === 'append' && (await isAgentActivelyRunning(useCaseId));

    console.log(`[agents] Saved descriptor for useCaseId=${useCaseId} mode=${mode}`);
    res.status(201).json({
      saved: true,
      useCaseId,
      path: `samples/${useCaseId}/agents.json`,
      restartRequired,
    });
  } catch (err) {
    console.error('[agents] Save error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── GET /api/agents — list all saved agent descriptors ───────────────────────
/**
 * Returns an array of every samples/<id>/agents.json that exists, shaped as:
 *   [{ id, name, agentId, agentCount, agents: [{agentId, name}] }]
 * where `id` is the folder name (use-case key), `name` is the human display
 * name (first agent for single-agent, use-case ID for multi-agent), `agentId`
 * is agents[0].agentId (backward compat), `agentCount` is the total number of
 * agents, and `agents` is the lightweight list of all agents.
 */
app.get('/api/agents', async (req, res) => {
  try {
    const entries = await fs.readdir(SAMPLES_DIR, { withFileTypes: true });
    const result = [];
    for (const entry of entries) {
      if (!entry.isDirectory()) continue;
      try {
        const raw = await fs.readFile(path.join(SAMPLES_DIR, entry.name, 'agents.json'), 'utf8');
        const parsed = JSON.parse(raw);
        const allAgents = parsed.agents || [];
        const first = allAgents[0] || {};
        const agentCount = allAgents.length;
        // Prefer the name declared in descriptor.json (mirrors the use-case selector tile),
        // then fall back to agents[0].name for single-agent jobs, then to the folder name.
        let descriptorName;
        let descriptorDesc;
        try {
          const dp = path.join(SAMPLES_DIR, entry.name, 'descriptor.json');
          const dr = JSON.parse(await fs.readFile(dp, 'utf8'));
          descriptorName = dr.name || undefined;
          descriptorDesc = dr.description || undefined;
        } catch { /* descriptor.json absent or unreadable — proceed without it */ }
        const name = descriptorName
          || (agentCount === 1 ? (first.name || entry.name) : entry.name.replace(/[-_]+/g, ' ').replace(/\b\w/g, c => c.toUpperCase()));
        result.push({
          id:          entry.name,
          name,
          description: descriptorDesc,
          agentId:     first.agentId || entry.name,   // backward compat field
          agentCount,
          agents:      allAgents.map(a => ({ agentId: a.agentId || '', name: a.name || '' })),
        });
      } catch { /* no agents.json — skip */ }
    }
    res.json(result);
  } catch (err) {
    console.error('[agents] List error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── GET /api/agents/:id — read back the saved agents.json ────────────────────
app.get('/api/agents/:id', async (req, res) => {
  const { id } = req.params;
  if (!isValidAgentId(id)) return res.status(400).json({ error: 'Invalid agent id' });
  try {
    const content = await fs.readFile(path.join(SAMPLES_DIR, id, 'agents.json'), 'utf8');
    res.json(JSON.parse(content));
  } catch (err) {
    if (err.code === 'ENOENT') return res.status(404).json({ error: `Agent '${id}' not found` });
    res.status(500).json({ error: err.message });
  }
});

// ── PATCH /api/agents/:useCaseId/agents/:agentId — replace one agent in-place ─
/**
 * Replaces a single agent (identified by :agentId) within an existing
 * agents.json without disturbing sibling agents.  Used by wizard edit mode.
 *
 * Body: { descriptor: <single AgentDescriptor object> }
 *
 * Blocks while the use-case job is actively running (same guard as POST).
 * Returns 404 when the use-case or the agentId does not exist.
 * Returns 409 when the job is running.
 */
app.patch('/api/agents/:useCaseId/agents/:agentId', async (req, res) => {
  const { useCaseId, agentId } = req.params;
  if (!isValidAgentId(useCaseId)) return res.status(400).json({ error: 'Invalid useCaseId' });
  if (!isValidAgentId(agentId))   return res.status(400).json({ error: 'Invalid agentId' });

  const { descriptor } = req.body || {};
  if (!descriptor || typeof descriptor !== 'object')
    return res.status(400).json({ error: 'descriptor must be a non-null object' });

  if (await isAgentActivelyRunning(useCaseId)) {
    return res.status(409).json({
      error: `Use-case '${useCaseId}' is currently running. Stop the Flink job before editing an agent.`,
    });
  }

  const agentsPath = path.join(SAMPLES_DIR, useCaseId, 'agents.json');
  let existing;
  try {
    existing = JSON.parse(await fs.readFile(agentsPath, 'utf8'));
  } catch (err) {
    if (err.code === 'ENOENT') return res.status(404).json({ error: `Use-case '${useCaseId}' not found` });
    return res.status(500).json({ error: err.message });
  }

  const idx = (existing.agents || []).findIndex(a => a.agentId === agentId);
  if (idx === -1) return res.status(404).json({ error: 'AGENT_NOT_FOUND', agentId });

  // Replace only the matched agent; leave all others untouched.
  existing.agents[idx] = descriptor;

  try {
    await fs.writeFile(agentsPath, JSON.stringify(existing, null, 2), 'utf8');
    const stub = buildStubDescriptor(useCaseId, existing);
    await fs.writeFile(path.join(SAMPLES_DIR, useCaseId, 'descriptor.json'), JSON.stringify(stub, null, 2), 'utf8');
    await ensureTopicsForDescriptor(stub).catch(err =>
      console.warn(`[agents] topic ensure failed (non-fatal): ${err.message}`)
    );
    console.log(`[agents] Updated agentId=${agentId} in useCaseId=${useCaseId}`);
    res.json({ updated: true, useCaseId, agentId });
  } catch (err) {
    console.error('[agents] Patch error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── DELETE /api/agents/:useCaseId/agents/:agentId — remove one agent ──────────
/**
 * Removes a single agent (identified by :agentId) from an existing agents.json.
 *
 * Lifecycle semantics:
 *   • agents remain  → write updated agents.json + descriptor.json; do NOT stop
 *     the job or delete topics; return restartRequired:true.
 *   • last agent removed → delegates to the full use-case delete path (stops job,
 *     purges + deletes topics, removes files and directory).
 *
 * Blocks while the use-case job is running (the user must stop first).
 */
app.delete('/api/agents/:useCaseId/agents/:agentId', async (req, res) => {
  const { useCaseId, agentId } = req.params;
  if (!isValidAgentId(useCaseId)) return res.status(400).json({ error: 'Invalid useCaseId' });
  if (!isValidAgentId(agentId))   return res.status(400).json({ error: 'Invalid agentId' });

  if (await isAgentActivelyRunning(useCaseId)) {
    return res.status(409).json({
      error: `Use-case '${useCaseId}' is currently running. Stop the Flink job before removing an agent.`,
    });
  }

  const dir        = path.join(SAMPLES_DIR, useCaseId);
  const agentsPath = path.join(dir, 'agents.json');
  let existing;
  try {
    existing = JSON.parse(await fs.readFile(agentsPath, 'utf8'));
  } catch (err) {
    if (err.code === 'ENOENT') return res.status(404).json({ error: `Use-case '${useCaseId}' not found` });
    return res.status(500).json({ error: err.message });
  }

  const before = existing.agents || [];
  const after  = before.filter(a => a.agentId !== agentId);
  if (after.length === before.length)
    return res.status(404).json({ error: 'AGENT_NOT_FOUND', agentId });

  // ── Last agent: full use-case teardown ─────────────────────────────────────
  if (after.length === 0) {
    // Purge + delete Kafka topics, then remove all descriptor files.
    let topicsDeleted = [];
    try {
      topicsDeleted = await deleteTopicsForAgent(useCaseId);
    } catch (err) {
      return res.status(500).json({ error: `Kafka topic purge/delete failed: ${err.message}` });
    }
    const filesToRemove = ['agents.json', 'descriptor.json', '.job-state.json'];
    const removed = [];
    for (const fname of filesToRemove) {
      try { await fs.unlink(path.join(dir, fname)); removed.push(fname); }
      catch (e) { if (e.code !== 'ENOENT') console.warn(`[agents] Could not remove ${fname}: ${e.message}`); }
    }
    try { const rem = await fs.readdir(dir); if (rem.length === 0) await fs.rmdir(dir); } catch { /* ignore */ }
    console.log(`[agents] Removed last agent '${agentId}' from useCaseId=${useCaseId}; deleted use-case`);
    return res.json({ deleted: true, useCaseId, agentId, agentsRemaining: 0, topicsDeleted });
  }

  // ── Sibling agents remain: update files only ───────────────────────────────
  try {
    existing.agents = after;
    await fs.writeFile(agentsPath, JSON.stringify(existing, null, 2), 'utf8');
    const stub = buildStubDescriptor(useCaseId, existing);
    await fs.writeFile(path.join(dir, 'descriptor.json'), JSON.stringify(stub, null, 2), 'utf8');
    console.log(`[agents] Removed agentId=${agentId} from useCaseId=${useCaseId}; ${after.length} agent(s) remain`);
    res.json({ deleted: true, useCaseId, agentId, agentsRemaining: after.length, restartRequired: true });
  } catch (err) {
    console.error('[agents] Per-agent delete error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── POST /api/agents/:id/start — submit Flink job ────────────────────────────
app.post('/api/agents/:id/start', async (req, res) => {
  const { id } = req.params;
  if (!isValidAgentId(id)) return res.status(400).json({ error: 'Invalid agent id' });
  if (!FLINK_URL) return res.status(503).json({ error: 'FLINK_URL not configured — Flink unavailable' });

  // Block deploy while the agent is already actively running
  if (await isAgentActivelyRunning(id)) {
    return res.status(409).json({
      error: `Agent '${id}' is currently running. Stop the Flink job before deploying a new version.`,
    });
  }

  try {
    await fs.access(path.join(SAMPLES_DIR, id, 'agents.json'));
  } catch {
    return res.status(404).json({ error: `agents.json not found for '${id}' — save first` });
  }

  try {
    // Ensure all topics referenced by this agent's descriptor exist before starting.
    // createTopics() is idempotent — it silently skips topics that already exist.
    try {
      const agentsJson = JSON.parse(await fs.readFile(path.join(SAMPLES_DIR, id, 'agents.json'), 'utf8'));
      const topicNames = [...collectTopicsFromAgentsJson(agentsJson)];
      if (topicNames.length > 0) {
        const admin = kafka.admin();
        try {
          await admin.connect();
          await admin.createTopics({
            waitForLeaders: true,
            topics: topicNames.map(t => ({ topic: t, numPartitions: TOPIC_PARTITIONS, replicationFactor: TOPIC_REPLICATION })),
          });
          console.log(`[topics] Pre-start ensure for agentId=${id}: ${topicNames.join(', ')}`);
        } finally {
          await admin.disconnect().catch(() => {});
        }
      }
    } catch (err) {
      console.warn(`[topics] Pre-start topic ensure failed (non-fatal): ${err.message}`);
    }

    const jarId = await resolveFlinkJarId();

    const submitRes = await fetch(`${FLINK_URL}/jars/${jarId}/run`, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        entryClass:      'com.ibm.di.event.agent.AgentBasedDataStreamJob',
        // args[0] = Kafka bootstrap servers, args[1] = descriptor path
        // AgentBasedDataStreamJob.main() reads args[1] via envOr("AGENT_DESCRIPTOR_PATH", args[1])
        // Passing via programArgsList is the only way to reach System.getenv() fallback args
        // from the REST API — flinkConfiguration keys go into Flink config, not the JVM env.
        // args[0] = Kafka bootstrap servers
        // args[1] = descriptor path  (samples/<id>/agents.json)
        // args[2] = Flink job name   (<id>-job) — must match descriptor.flinkJobName
        //           FLINK_JOB_NAME env var takes priority if set on the task-manager;
        //           programArgsList is the only channel to pass it from the REST API.
        programArgsList: [KAFKA_BROKER, `samples/${id}/agents.json`, `${id}-job`],
        flinkConfiguration: {
          'pipeline.name': `${id}-job`,
        },
      }),
      signal: AbortSignal.timeout(30000),
    });

    if (!submitRes.ok) {
      const text = await submitRes.text();
      return res.status(submitRes.status).json({ error: `Flink submit failed: ${text}` });
    }

    const { jobid } = await submitRes.json();
    const state = { jobId: jobid, startedAt: Date.now() };
    agentJobState.set(id, state);
    await persistAgentJobState(id, state);

    console.log(`[agents] Started Flink job for agentId=${id} jobId=${jobid}`);
    res.status(202).json({ started: true, jobId: jobid, agentId: id });
  } catch (err) {
    console.error('[agents] Start error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── GET /api/agents/:id/status — query Flink job state ───────────────────────
app.get('/api/agents/:id/status', async (req, res) => {
  const { id } = req.params;
  if (!isValidAgentId(id)) return res.status(400).json({ error: 'Invalid agent id' });

  const stored = agentJobState.get(id);
  if (!stored) return res.json({ agentId: id, jobId: null, state: 'NOT_STARTED' });

  if (!FLINK_URL) return res.json({ agentId: id, jobId: stored.jobId, state: 'UNKNOWN', reason: 'FLINK_URL not configured' });

  try {
    const flinkRes = await fetch(`${FLINK_URL}/jobs/${stored.jobId}`, { signal: AbortSignal.timeout(5000) });
    if (!flinkRes.ok) {
      if (flinkRes.status === 404) {
        // Job no longer exists in Flink (container restart or manual cancel).
        // Clear the stale state so the agent is treated as NOT_STARTED and the
        // tile becomes visible again on the selector screen.
        await removeAgentJobState(id);
        console.log(`[agents] Cleared stale job state for agentId=${id} (Flink 404)`);
        return res.json({ agentId: id, jobId: null, state: 'NOT_STARTED' });
      }
      return res.status(flinkRes.status).json({ error: `Flink status check failed: ${flinkRes.status}` });
    }
    const job = await flinkRes.json();
    res.json({ agentId: id, jobId: stored.jobId, state: job.state, name: job.name, startedAt: stored.startedAt });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── POST /api/agents/:id/stop — cancel the Flink job and clean up Kafka topics ─
//
// Cancellation sequence:
//   1. Resolve the Flink jobId (agentJobState → Flink overview by job name).
//   2. Purge and delete all Kafka topics owned by this agent BEFORE cancelling
//      the Flink job.  Wiping topics first ensures:
//      • No retained messages survive on any topic regardless of what Flink
//        writes during its subsequent shutdown window.
//      • Any in-flight Flink sink writes after this point target topics that
//        have already been deleted (Flink will log producer errors, which is
//        acceptable during a deliberate cancel).
//      Missing topics are silently ignored (idempotent).
//   3. Cancel the Flink job via PATCH /jobs/:jobId?mode=cancel.
//      Flink 202 = accepted; 404 = already gone — both treated as success.
//   4. Poll GET /jobs/:jobId until the job reaches a terminal state (CANCELED,
//      FINISHED, FAILED, SUSPENDED) or a 60-second timeout is reached.
//      If the poll times out the handler returns 500 so the caller can retry.
//   5. Clear all agent-scoped runtime state:
//        • agentJobState (in-memory + .job-state.json on disk)
//        • activeConsumers (Kafka consumer used by the polling event feed)
//        • eventStore (in-memory event ring-buffer shown in the dashboard)
//
// Resolution order for the Flink jobId:
//   1. BFF agentJobState (set when the job was started via /api/agents/:id/start)
//   2. Flink /jobs/overview lookup by descriptor.flinkJobName (covers jobs started
//      externally — e.g. by a Docker job-submitter — that bypassed the BFF start
//      endpoint and therefore have no entry in agentJobState)
//
// Idempotency: if no running Flink job is found the handler skips steps 3/4 and
// proceeds directly to step 5 so that re-invoking stop after a partial failure
// completes the cleanup rather than returning 404.
app.post('/api/agents/:id/stop', async (req, res) => {
  const { id } = req.params;
  if (!isValidAgentId(id)) return res.status(400).json({ error: 'Invalid agent id' });
  if (!FLINK_URL) return res.status(503).json({ error: 'FLINK_URL not configured — Flink unavailable' });

  // ── Step 1: Resolve Flink jobId ──────────────────────────────────────────────
  let jobId = agentJobState.get(id)?.jobId ?? null;

  if (!jobId) {
    // No BFF-managed state — look up by job name in Flink
    try {
      const descriptor = await getUseCaseDescriptor(id);
      const jobName    = descriptor?.flinkJobName;
      if (jobName) {
        const overviewRes = await fetch(`${FLINK_URL}/jobs/overview`, { signal: AbortSignal.timeout(5000) });
        if (overviewRes.ok) {
          const { jobs } = await overviewRes.json();
          const ACTIVE = new Set(['RUNNING', 'RESTARTING', 'CREATED', 'INITIALIZING', 'RECONCILING']);
          const match  = (jobs || []).find(j => j.name === jobName && ACTIVE.has(j.state));
          if (match) jobId = match.jid;
        }
      }
    } catch (err) {
      console.warn(`[agents] Flink overview lookup failed for agentId=${id}: ${err.message}`);
    }
  }

  // ── Step 2: Purge and delete Kafka topics BEFORE cancelling Flink ────────────
  // Wiping topics first guarantees no retained messages survive, even if Flink
  // produces a few more events during its shutdown window after the cancel
  // signal is sent.  Those late writes target topics that are already gone, so
  // they are discarded by Kafka and never retained.
  let topicsDeleted = [];
  let topicsError   = null;
  try {
    topicsDeleted = await deleteTopicsForAgent(id);
    if (topicsDeleted.length > 0) {
      console.log(`[agents] Pre-cancel Kafka cleanup for agentId=${id}: removed ${topicsDeleted.length} topic(s)`);
    }
  } catch (err) {
    topicsError = err.message;
    console.error(`[topics] Kafka cleanup failed for agentId=${id}: ${err.message}`);
  }

  if (topicsError) {
    return res.status(500).json({
      stopped: false,
      jobId,
      agentId: id,
      flinkCancelled: false,
      flinkState: null,
      topicsDeleted,
      error: `Kafka topic cleanup failed: ${topicsError}`,
    });
  }

  // ── Step 3: Cancel Flink job (skip when already gone) ────────────────────────
  let flinkCancelled = false;
  let flinkState     = null;   // terminal state observed during the poll (null when no job)
  if (jobId) {
    try {
      const cancelRes = await fetch(`${FLINK_URL}/jobs/${jobId}?mode=cancel`, {
        method: 'PATCH',
        signal: AbortSignal.timeout(10000),
      });
      // Flink 202 = accepted cancel; 404 = already gone — both are success
      if (!cancelRes.ok && cancelRes.status !== 404) {
        const text = await cancelRes.text();
        return res.status(cancelRes.status).json({ error: `Flink cancel failed: ${text}` });
      }
      flinkCancelled = true;
      console.log(`[agents] Cancelled Flink job for agentId=${id} jobId=${jobId}`);
    } catch (err) {
      console.error('[agents] Stop error:', err.message);
      return res.status(500).json({ error: err.message });
    }

    // ── Step 4: Poll until Flink job reaches a terminal state ──────────────────
    // The PATCH is fire-and-forget (Flink returns 202 immediately).  We wait for
    // the job to fully wind down so callers get a reliable "job is gone" signal.
    const pollResult = await waitForFlinkJobTerminal(jobId);
    if (!pollResult.terminal) {
      console.error(`[agents] Flink job ${jobId} did not terminate in time: ${pollResult.reason} (last state: ${pollResult.state})`);
      return res.status(500).json({
        stopped: false,
        jobId,
        agentId: id,
        flinkCancelled,
        flinkState: pollResult.state,
        topicsDeleted,
        error: `Flink job did not reach terminal state: ${pollResult.reason} (last state: ${pollResult.state})`,
      });
    }
    flinkState = pollResult.state;
  } else {
    // No running job found — Flink job is already gone (idempotent stop)
    console.log(`[agents] No running Flink job found for agentId=${id} — skipping Flink cancel`);
  }

  // ── Step 5: Clear all agent-scoped runtime state ─────────────────────────────
  // a) Persisted job-state file + in-memory agentJobState entry
  await removeAgentJobState(id);

  // b) Active Kafka consumer for this use-case (used by the polling event feed)
  const consumer = activeConsumers.get(id);
  if (consumer) {
    try {
      await consumer.disconnect();
      console.log(`[kafka] Disconnected consumer for agentId=${id} on stop`);
    } catch (err) {
      console.warn(`[kafka] Could not disconnect consumer for agentId=${id} (non-fatal): ${err.message}`);
    }
    activeConsumers.delete(id);
  }

  // c) In-memory event ring-buffer (events shown in the dashboard event feed)
  eventStore.delete(id);


  console.log(`[agents] Stopped agentId=${id} jobId=${jobId ?? '(none)'} flinkState=${flinkState ?? '(none)'} topicsDeleted: ${topicsDeleted.join(', ') || 'none'}`);
  res.json({ stopped: true, jobId, agentId: id, flinkCancelled, flinkState, topicsDeleted });
});

// ── DELETE /api/agents/:id — stop job + delete all descriptor files ───────────
/**
 * Cancels any running Flink job for this agent, then removes:
 *   samples/<id>/agents.json
 *   samples/<id>/descriptor.json
 *   samples/<id>/.job-state.json
 * The directory itself is removed only when it is empty afterwards.
 */
app.delete('/api/agents/:id', async (req, res) => {
  const { id } = req.params;
  if (!isValidAgentId(id)) return res.status(400).json({ error: 'Invalid agent id' });

  // Block delete while the agent is actively running
  if (await isAgentActivelyRunning(id)) {
    return res.status(409).json({
      error: `Agent '${id}' is currently running. Stop the Flink job before deleting the agent.`,
    });
  }

  const dir = path.join(SAMPLES_DIR, id);

  // 0. Clear all agent-scoped runtime state
  const existingConsumer = activeConsumers.get(id);
  if (existingConsumer) {
    try {
      await existingConsumer.disconnect();
      console.log(`[kafka] Disconnected consumer for agentId=${id}`);
    } catch (err) {
      console.warn(`[kafka] Could not disconnect consumer for agentId=${id} (non-fatal): ${err.message}`);
    }
    activeConsumers.delete(id);
  }
  eventStore.delete(id);

  // 1. Cancel job if running (best-effort — do not fail delete if Flink is unreachable)
  const stored = agentJobState.get(id);
  if (stored && FLINK_URL) {
    try {
      await fetch(`${FLINK_URL}/jobs/${stored.jobId}?mode=cancel`, {
        method: 'PATCH', signal: AbortSignal.timeout(8000),
      });
      console.log(`[agents] Cancelled job ${stored.jobId} during delete of agentId=${id}`);
    } catch (err) {
      console.warn(`[agents] Could not cancel job during delete (non-fatal): ${err.message}`);
    }
    await removeAgentJobState(id);
  }

  // 1.5 Purge and delete Kafka topics owned by this agent.
  //     The purge (deleteTopicRecords) runs first; only on success are the
  //     topics deleted.  A purge failure is surfaced as a 500 so the caller
  //     can retry — file deletion does not proceed in that case.
  let topicsDeleted = [];
  try {
    topicsDeleted = await deleteTopicsForAgent(id);
  } catch (err) {
    console.error(`[topics] Purge/delete failed for agentId=${id}: ${err.message}`);
    return res.status(500).json({
      deleted: false,
      agentId: id,
      error: `Kafka topic purge/delete failed: ${err.message}`,
    });
  }

  // 2. Remove descriptor files (fixed list + any co-located .jar files)
  const filesToRemove = ['agents.json', 'descriptor.json', '.job-state.json'];
  try {
    const allFiles = await fs.readdir(dir);
    for (const f of allFiles) {
      if (f.endsWith('.jar')) filesToRemove.push(f);
    }
  } catch { /* directory may not exist — fine */ }
  const removed = [];
  const errors  = [];
  for (const fname of filesToRemove) {
    try {
      await fs.unlink(path.join(dir, fname));
      removed.push(fname);
    } catch (err) {
      if (err.code !== 'ENOENT') errors.push(`${fname}: ${err.message}`);
      // ENOENT = file already gone — not an error
    }
  }

  // 3. Remove the directory if now empty
  try {
    const remaining = await fs.readdir(dir);
    if (remaining.length === 0) await fs.rmdir(dir);
  } catch { /* ignore — directory may not exist or may have other files */ }

  if (errors.length > 0) {
    return res.status(500).json({ deleted: false, removed, errors });
  }
  if (removed.length === 0) {
    return res.status(404).json({ error: `Agent '${id}' not found` });
  }

  console.log(`[agents] Deleted agentId=${id}, removed: ${removed.join(', ')}, topicsDeleted: ${topicsDeleted.join(', ') || 'none'}`);
  res.json({ deleted: true, agentId: id, removed, topicsDeleted });
});

// ── POST /api/agents/:id/jar — upload Decision Intelligence ruleset JAR ──────
/**
 * Multipart upload (field name: "jar"). Saves the .jar file to
 * samples/<id>/<originalname> and returns:
 *   { saved: true, filename: string, path: string }
 * where `path` is the value to use as `ruleset.path` in agents.json.
 */
app.post('/api/agents/:id/jar', jarUpload.single('jar'), (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'No jar file received' });
  const { id } = req.params;
  const relPath = `samples/${id}/${req.file.filename}`;
  console.log(`[agents] JAR uploaded for agentId=${id}: ${relPath}`);
  res.status(201).json({ saved: true, filename: req.file.filename, path: relPath });
});


// Graceful shutdown
process.on('SIGTERM', async () => {
  console.log('SIGTERM received, shutting down gracefully');

  // Disconnect all active Kafka consumers
  for (const [useCase, consumer] of activeConsumers) {
    try {
      await consumer.disconnect();
      console.log(`[kafka] Disconnected consumer for useCase=${useCase}`);
    } catch (err) {
      console.warn(`[kafka] Error disconnecting consumer for useCase=${useCase}: ${err.message}`);
    }
  }

  // Disconnect shared producer
  if (sharedProducerReady) {
    try {
      await sharedProducer.disconnect();
      console.log('[kafka] Shared producer disconnected');
    } catch (err) {
      console.warn(`[kafka] Error disconnecting shared producer: ${err.message}`);
    }
  }

  server.close(() => {
    console.log('Server closed');
    process.exit(0);
  });
});


// ── GET /api/agents/:id/entity-state — Flink entity state snapshot ────────────
/**
 * Returns a snapshot of entity state currently visible in the BFF event store.
 *
 * Entity IDs are extracted from inbound events using each agent's configured
 * `entityKeyExtractor` JSONPath (the same path the Flink job uses), ensuring the
 * UI sees the same entity keys as Flink.
 *
 * Outbound events (serialized EventEnvelope objects) carry `entityId` at the top
 * level — populated since the toEnvelope() fix in StatefulAgentProcessor — and
 * are looked up by the descriptor.json `outboundTopics[].type` keys that the
 * eventStore uses.
 *
 * For each entity we collect:
 *   - inbound event counts per subscription eventType
 *   - the most recent rule-output envelope (lastPayload) from any outbound topic
 *   - last-seen timestamp and total inbound event count
 *   - entityType from the specific agent that owns that entity
 *
 * Query params:
 *   ?agentId=<id>    — serve from entityStateStore (changelog-based, always current).
 *                      Returns { agentId, entityType, entities: [snapshot, ...] }
 *                      sorted newest-first.  Falls back to empty list when no changelog
 *                      data exists yet.
 *   ?entityId=<id>   — return only that entity (single object instead of array)
 *
 * Response (all entities):
 *   { entityType: string|null, entities: [ { entityId, entityType, lastSeen, eventCount,
 *       lastPayload, inboundCounts: { [topicType]: number } } ] }
 *
 * Response (single entity, ?entityId=xxx):
 *   same shape as one entity item, or 404 when not found
 */
app.get('/api/agents/:id/entity-state', async (req, res) => {
  const { id } = req.params;
  const { entityId: filterEntityId, agentId: filterAgentId, summary: summaryParam } = req.query;
  const wantsAgentSummary = summaryParam === 'true';
  if (!isValidAgentId(id)) return res.status(400).json({ error: 'Invalid agent id' });

  // ── Fast path: agentId param → serve directly from entityStateStore ──────────
  // This path is used by the diagram entity detail panel.  Keep it for entity
  // state requests, but let summary requests use the event-store path below so
  // the result can include only this agent's inbound and outbound topics.
  if (filterAgentId && !wantsAgentSummary) {
    const agentMap = entityStateStore.get(id)?.get(filterAgentId);
    const snapshots = agentMap ? [...agentMap.values()] : [];

    // Sort newest-first by timestamp
    snapshots.sort((a, b) => {
      if (!a.timestamp && !b.timestamp) return 0;
      if (!a.timestamp) return 1;
      if (!b.timestamp) return -1;
      return b.timestamp - a.timestamp;
    });

    // Derive entityType from the first snapshot (all share the same type per agent)
    const entityType = snapshots[0]?.entityType || null;

    return res.json({ agentId: filterAgentId, entityType, entities: snapshots });
  }

  // Load agents.json — source of entityKeyExtractor, entityType, subscriptions, outputs
  let agentsJson;
  try {
    const raw = await fs.readFile(path.join(SAMPLES_DIR, id, 'agents.json'), 'utf8');
    agentsJson = JSON.parse(raw);
  } catch (err) {
    if (err.code === 'ENOENT') return res.status(404).json({ error: `Agent '${id}' not found` });
    return res.status(500).json({ error: err.message });
  }

  // Load descriptor.json to get the eventStore topic-type keys for outbound topics.
  // descriptor.json is the authoritative mapping: topicName → topicType (eventStore key).
  // For wizard-created agents it may not exist on disk; fall back to building it from agents.json
  // the same way buildStubDescriptor() does so the keys always match the eventStore.
  let descriptor = await getUseCaseDescriptor(id);
  if (!descriptor) {
    descriptor = buildStubDescriptor(id, agentsJson);
  }

  const agents = agentsJson.agents || [];
  const selectedAgent = filterAgentId
    ? agents.find(agent => agent.agentId === filterAgentId)
    : null;
  if (filterAgentId && !selectedAgent) {
    return res.status(404).json({ error: `Agent '${filterAgentId}' not found` });
  }
  const scopedAgents = selectedAgent ? [selectedAgent] : agents;

  // Build a map: outbound topicName → entityType of the owning agent.
  // Keyed by topic name (not type) so two outbound topics with the same type (e.g. two
  // "Alert" topics owned by different agents) are stored and retrieved independently.
  const outboundNameToEntityType = new Map();
  // Only scan outbound topics declared by the selected agent.
  const outboundTopicNames = new Set(
    scopedAgents.flatMap(agent => (agent.outputs || [])
      .map(out => out.topic)
      .filter(Boolean))
  );
  for (const agent of scopedAgents) {
    for (const out of agent.outputs || []) {
      // Match agent output eventType against descriptor outboundTopics by name
      const match = (descriptor.outboundTopics || []).find(
        ot => ot.name && out.topic && ot.name === out.topic
      );
      if (match) {
        outboundNameToEntityType.set(match.name, agent.entityType || null);
      }
    }
  }

  // Build per-agent inbound topicName → agent descriptor map so we know how to
  // extract entity IDs from each inbound event in the eventStore.
  const inboundNameToAgent = new Map(); // topicName → agent descriptor object
  for (const agent of scopedAgents) {
    for (const sub of agent.subscriptions || []) {
      if (sub.topic) inboundNameToAgent.set(sub.topic, agent);
    }
  }

  // Build changelog topic name set — one per agent that declares changeLogTopic.
  // Keyed by topic name so two agents with distinct changelog topics (both typed
  // 'entity-changelog') are never collapsed into the same eventStore bucket.
  const changelogTopicNames = new Set(); // Set of eventStore topicName keys for changelog topics
  const changelogNameToAgent = new Map(); // topicName → agent descriptor
  for (const agent of scopedAgents) {
    const clf = agent.stateConfig?.changeLogTopic;
    if (clf && clf.trim()) {
      changelogTopicNames.add(clf.trim());
      changelogNameToAgent.set(clf.trim(), agent);
    }
  }

  const useCaseStore = eventStore.get(id);

  // Entity map: entityId → { entityId, entityType, lastSeen, eventCount, lastPayload, inboundCounts, entityData }
  const entityMap = new Map();

  // Tracks the most-recent inbound event per entity for use as a fallback payload
  // when the operation has no outbounds (e.g. HandleAccountEvent — entity-creation only).
  const latestInboundByEntity = new Map(); // entityId → { ts: string|null, event: object }

  const getOrCreate = (eid, entityType) => {
    if (!entityMap.has(eid)) {
      entityMap.set(eid, {
        entityId:      eid,
        entityType:    entityType || null,
        lastSeen:      null,
        eventCount:    0,
        lastPayload:   null,
        entityData:    null,   // latest entity snapshot from changelog topic (null when not available)
        inboundCounts: {},
      });
    }
    return entityMap.get(eid);
  };

  if (useCaseStore) {
    // ── Inbound topics ────────────────────────────────────────────────────────
    // Extract entity ID from each raw domain event using the agent's entityKeyExtractor.
    // The same JSONPath that Flink uses — ensures entity IDs match Flink's keyed partitions.
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

        // Track the most recent inbound event per entity so we can fall back to it
        // when an operation produces no outbound events (e.g. entity-creation operations
        // like HandleAccountEvent that only update Flink keyed state without emitting).
        const prev = latestInboundByEntity.get(eid);
        // Use >= so scenario replays with identical timestamps keep the last inbound
        // event as the fallback payload rather than the pre-reset one.
        if (!prev || (ts && (!prev.ts || ts >= prev.ts))) {
          latestInboundByEntity.set(eid, { ts, eventType: topicName, event: ev });
        }
      }
    }

    // ── Outbound topics ───────────────────────────────────────────────────────
    // Outbound events are serialized EventEnvelope objects written by Flink.
    // Since the toEnvelope() fix they carry entityId at the top level.
    // eventStore keys outbound events by topic name.
    for (const outboundTopicName of outboundTopicNames) {
      const events = useCaseStore.get(outboundTopicName) || [];
      const ownerEntityType = outboundNameToEntityType.get(outboundTopicName) || null;
      for (const ev of events) {
        // EventEnvelope serialization emits entityId at top level (agentEventType, entityId, ...)
        const eid = ev.entityId || null;
        if (!eid) continue;
        const entry = getOrCreate(eid, ownerEntityType);
        // Prefer the outbound event timestamp; fall back to 0 (epoch) as sentinel to detect
        // un-timestamped events from before the toEnvelope() fix.
        const tsMs = (typeof ev.timestamp === 'number' && ev.timestamp > 0) ? ev.timestamp : null;
        const ts   = tsMs ? new Date(tsMs).toISOString() : null;
        // Use >= so that same-timestamp outbound events from a scenario replay
        // overwrite the pre-reset lastPayload (last-write-wins).  Without this,
        // the ring buffer's first (old) outbound event permanently captures
        // lastPayload and the "Last Rule Engine Result" never updates after reset.
        const isNewerOrEqual = ts && (!entry.lastSeen || ts >= entry.lastSeen);
        if (isNewerOrEqual) {
          entry.lastSeen    = ts;
          entry.lastPayload = ev;
        } else if (!entry.lastPayload) {
          entry.lastPayload = ev;
        }
      }
    }

    // ── Changelog topics ──────────────────────────────────────────────────────
    // entity-changelog events are emitted by StatefulAgentProcessor's side-output
    // whenever the entity data changes after processing an event.
    // Payload shape: { entityId, entityType, timestamp, entity: { ... } }
    // We keep only the most-recent snapshot per entity (newest timestamp wins).
    for (const changelogTopicName of changelogTopicNames) {
      const agentDesc = changelogNameToAgent.get(changelogTopicName);
      const events = useCaseStore.get(changelogTopicName) || [];
      for (const ev of events) {
        // Changelog events are stored as raw Kafka values (the EventEnvelope's payload field).
        // The Flink EnvelopeSerializer wraps the payload under a 'payload' key.
        // The inner payload has: { entityId, entityType, timestamp, entity }
        const inner = ev.payload || ev;
        const eid = inner.entityId || ev.entityId || null;
        if (!eid) continue;
        const entry = getOrCreate(eid, inner.entityType || agentDesc?.entityType || null);
        const tsMs = typeof inner.timestamp === 'number' && inner.timestamp > 0
          ? inner.timestamp : null;
        const ts = tsMs ? new Date(tsMs).toISOString() : null;
        // Keep the snapshot with the newest-or-equal timestamp (last-write-wins for same-ts).
        // Using >= ensures scenario replays with identical timestamps overwrite stale entityData.
        if (inner.entity && (entry.entityData === null || (ts && ts >= (entry._changelogTs || '')))) {
          entry.entityData    = inner.entity;
          entry._changelogTs  = ts || '';
          if (ts && (!entry.lastSeen || ts > entry.lastSeen)) entry.lastSeen = ts;
        }
      }
    }

    // ── Inbound-only fallback ─────────────────────────────────────────────────
    // Operations with no outbounds (e.g. HandleAccountEvent) never emit Kafka messages,
    // so the entity's Flink state is never observable through the outbound topic stream.
    // For these entities the inbound event itself IS the observable entity payload:
    // it contains all the fields Flink stores as entity state (account attributes, etc.).
    // Synthesize a lastPayload envelope so the Entity tab can display the domain data.
    for (const [eid, latest] of latestInboundByEntity) {
      const entry = entityMap.get(eid);
      if (!entry || entry.lastPayload !== null) continue; // outbound already set it
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

  // Strip internal tracking fields before returning
  for (const entry of entityMap.values()) {
    delete entry._changelogTs;
  }

  // Sort newest-first; entities with no lastSeen go to the end.
  const entities = [...entityMap.values()].sort((a, b) => {
    if (!a.lastSeen && !b.lastSeen) return 0;
    if (!a.lastSeen) return 1;
    if (!b.lastSeen) return -1;
    return b.lastSeen.localeCompare(a.lastSeen);
  });

  if (filterEntityId) {
    const found = entities.find(e => e.entityId === filterEntityId);
    if (!found) return res.status(404).json({ error: `Entity '${filterEntityId}' not found` });
    return res.json(found);
  }

  // Agent summary is assembled only from topics owned by the selected agent.
  // This prevents a multi-agent use-case from showing another agent's last output.
  if (wantsAgentSummary && selectedAgent) {
    const agentSummary = entities.reduce((summary, entry) => {
      for (const [eventType, count] of Object.entries(entry.inboundCounts || {})) {
        summary.inboundCounts[eventType] =
          (summary.inboundCounts[eventType] || 0) + count;
      }
      // Use >= so that all entities with the same lastSeen timestamp contribute:
      // in single-entity scenarios (typical demo) the entity always updates the
      // summary; in multi-entity scenarios the last-seen entity wins.
      if (entry.lastPayload && (!summary.lastPayload ||
          (entry.lastSeen && entry.lastSeen >= (summary.lastSeen || '')))) {
        summary.lastPayload = entry.lastPayload;
        summary.lastSeen = entry.lastSeen;
      }
      return summary;
    }, { inboundCounts: {}, lastPayload: null, lastSeen: null });

    return res.json({
      agentId: selectedAgent.agentId,
      entityType: selectedAgent.entityType || null,
      agentSummary,
      entities,
    });
  }

  // Top-level entityType: meaningful only for single-agent use-cases; null for multi-agent.
  const topLevelEntityType = agents.length === 1 ? (agents[0]?.entityType || null) : null;
  res.json({ entityType: topLevelEntityType, entities });
});

/**
 * Extracts an entity ID from a raw Kafka event object using a JSONPath extractor string.
 *
 * Mirrors the logic of AgentBasedDataStreamJob.EnvelopeDeserializer.extractEntityId():
 *   - bare "fieldName"  → "$.fieldName"
 *   - "$.a.b.c"         → traverse the object
 *
 * Returns null when extractor is blank, the path doesn't match, or the value is "unknown".
 * No external JSONPath library is required — simple dot-path traversal is sufficient for
 * the patterns used in practice (e.g. "$.accountNumber", "$.aircraftId", "$.airportCode").
 *
 * @param {object} event     - raw event object from eventStore
 * @param {string|null} extractor - JSONPath string from agents.json entityKeyExtractor
 * @returns {string|null}    - extracted entity ID, or null if not found
 */
function extractEntityIdFromEvent(event, extractor) {
  if (!extractor || !event || typeof event !== 'object') return null;
  // Normalise: strip leading "$." or "$" to get the dot-separated field path
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

// ── GET /api/agents/:id/sample-input — generate a sample event payload ────────
/**
 * Invokes SampleInputGenerator from the fat JAR as a child process.
 * SampleInputGenerator calls DecisionRunner.createInputExample() (public Decision Intelligence API),
 * which returns the archive's own built-in example values. The Decision Intelligence input JSON is
 * then unwrapped to the raw per-topic Kafka event shape — StatefulAgentProcessor rebuilds
 * the Decision Intelligence wrapper internally on each event, so users must send the raw domain event.
 *
 * JAR_PATH is resolved at startup: the explicit JAR_PATH env var wins (Docker mount);
 * otherwise the fat JAR is located automatically in event-rule-backend/target/.
 *
 * Response: { topic: string, sample: object, samplesByTopic: object, topics: [{name, type, label}] }
 */
app.get('/api/agents/:id/sample-input', async (req, res) => {
  const { id } = req.params;
  if (!isValidAgentId(id)) return res.status(400).json({ error: 'Invalid agent id' });

  let agentsJson;
  try {
    const raw = await fs.readFile(path.join(SAMPLES_DIR, id, 'agents.json'), 'utf8');
    agentsJson = JSON.parse(raw);
  } catch (err) {
    if (err.code === 'ENOENT') return res.status(404).json({ error: `Agent '${id}' not found` });
    return res.status(500).json({ error: err.message });
  }

  const agent = (agentsJson.agents || [])[0];
  if (!agent) return res.status(404).json({ error: `No agents defined in agents.json for '${id}'` });

  const ruleset = agent.ruleset || {};

  // Collect inbound subscription topics so the UI can let the user pick which topic to send to
  const inboundTopics = (agent.subscriptions || [])
    .filter(sub => sub.topic)
    .map(sub => ({ name: sub.topic, type: sub.eventType, label: sub.eventType, operation: sub.operation }));

  const defaultTopic = inboundTopics[0]?.name || 'events';

  if (!JAR_PATH) {
    return res.status(503).json({
      error: 'Sample generation unavailable: fat JAR not found. Run `mvn package -DskipTests` in event-rule-backend/ first.',
    });
  }

  const archivePath = ruleset.path;

  // ruleset.operations[] is the canonical list of per-operation configs (agents.json schema).
  // Each entry has an `operation` string that matches a subscription's `operation` field.
  // Fall back gracefully when the archive path or operations list is absent (e.g. wizard
  // agents that haven't had a JAR uploaded yet).
  const rulesetOperations = Array.isArray(ruleset.operations) ? ruleset.operations : [];

  if (!archivePath || rulesetOperations.length === 0) {
    // No archive configured — return empty per-topic stubs so the UI still opens
    // and the user can type a payload manually.
    const samplesByTopic = {};
    for (const t of inboundTopics) {
      samplesByTopic[t.name] = {};
    }
    return res.json({ topic: defaultTopic, sample: {}, samplesByTopic, topics: inboundTopics });
  }

  try {
    // Build a map of operationName → subscription topic so we can route each
    // generated sample to the correct inbound Kafka topic.
    const topicByOperation = {};
    for (const sub of inboundTopics) {
      if (sub.operation) topicByOperation[sub.operation] = sub.name;
    }

    // Invoke the generator once per ruleset operation, then collate by topic.
    // Multiple operations may share the same topic (e.g. two event types routed
    // to the same subscription) — last write wins, which is fine for a sample.
    const samplesByTopic = {};
    let firstSample = null;
    let firstTopic  = defaultTopic;

    for (const opConfig of rulesetOperations) {
      const opName = opConfig.operation;
      if (!opName) continue;

      let adsJson;
      try {
        adsJson = await invokeArchiveSampleGenerator(JAR_PATH, archivePath, opName);
      } catch (err) {
        console.warn(`[sample-input] Generator skipped operation '${opName}' for '${id}': ${err.message}`);
        continue;
      }

      const sample = unwrapAdsSample(adsJson);

      // Determine which topic this operation's events arrive on.
      const topic = topicByOperation[opName] || defaultTopic;
      samplesByTopic[topic] = sample;

      if (firstSample === null) {
        firstSample = sample;
        firstTopic  = topic;
      }
    }

    // If every operation call failed, return an empty stub rather than a 500.
    if (firstSample === null) {
      firstSample = {};
      for (const t of inboundTopics) {
        samplesByTopic[t.name] = samplesByTopic[t.name] || {};
      }
    }

    const sample = samplesByTopic[defaultTopic] || firstSample;
    return res.json({ topic: firstTopic, sample, samplesByTopic, topics: inboundTopics });
  } catch (err) {
    console.error(`[sample-input] Archive generator failed for '${id}': ${err.message}`);
    return res.status(500).json({ error: `Sample generation failed: ${err.message}` });
  }
});

/**
 * Invokes com.ibm.di.event.rule.SampleInputGenerator as a Java child process.
 * SampleInputGenerator.main() calls DecisionRunner.createInputExample() (public API)
 * and prints the result as pretty-printed JSON to stdout.
 *
 * @param {string} jarPath     path to the fat JAR
 * @param {string} archivePath path to the decision archive JAR (from agents.json ruleset.path)
 * @param {string} operation   Decision Intelligence operation name (from agents.json ruleset.operations[].operation)
 * @returns {Promise<object>}  parsed Decision Intelligence input JSON
 */
function invokeArchiveSampleGenerator(jarPath, archivePath, operation) {
  return new Promise((resolve, reject) => {
    const { spawn } = require('child_process');
    const child = spawn('java', [
      // Suppress Log4j2 entirely so stdout carries only the JSON output.
      // Without this flag the bundled log4j2.properties configures a
      // ConsoleAppender (stdout) at DEBUG for RuleEngineRunner, which
      // prepends log lines to the JSON and breaks JSON.parse(stdout).
      '-Dlog4j2.configurationFile=NONE',
      '-cp', jarPath,
      'com.ibm.di.event.rule.SampleInputGenerator',
      archivePath,
      operation,
    ], { timeout: 30000 });

    let stdout = '';
    let stderr = '';
    child.stdout.on('data', d => { stdout += d; });
    child.stderr.on('data', d => { stderr += d; });

    child.on('close', code => {
      if (code !== 0) return reject(new Error(`exit ${code}: ${stderr.trim()}`));
      try {
        resolve(JSON.parse(stdout));
      } catch (e) {
        reject(new Error(`JSON parse failed: ${e.message}`));
      }
    });

    child.on('error', err => reject(err));
  });
}

/**
 * Extracts the raw Kafka event payload from the Decision Intelligence example input object.
 *
 * SampleInputGenerator returns { "input": { "currentEvent": { … }, … } }.
 * The raw Kafka event the user sends IS the currentEvent payload, so we extract it here.
 * Falls back to the full input object, then to adsJson itself, as a last resort.
 *
 * Called once per ruleset operation so the correct per-operation sample is mapped to
 * its subscription topic by the caller.
 *
 * @param {object} adsJson  parsed output of SampleInputGenerator for one operation
 * @returns {object}        raw Kafka event sample for that operation
 */
function unwrapAdsSample(adsJson) {
  const inputInner   = adsJson.input || {};
  const currentEvent = inputInner.currentEvent || {};
  if (Object.keys(currentEvent).length > 0) return currentEvent;
  if (Object.keys(inputInner).length  > 0) return inputInner;
  return adsJson;
}


// ── Test-only exports ─────────────────────────────────────────────────────────
// Exposes pure helper functions so unit tests can import them without starting
// the server or connecting to Kafka/Express.  Only the three whitebox helpers
// that have no side-effects are exported; the server itself is not started when
// this module is require()'d in a Jest context (the listen() call is guarded by
// the require.main check below).
if (typeof module !== 'undefined') {
  module.exports = { isValidAgentId, buildStubDescriptor, collectTopicsFromAgentsJson };
}

if (require.main === module) {
  loadAgentJobStates()
    .then(() => console.log(`[agents] Loaded ${agentJobState.size} persisted job state(s)`))
    .catch(err => console.warn('[agents] Job state load failed (non-fatal):', err.message));

  initTopics()
    .catch(err => console.error('[topics] Initialisation error (non-fatal):', err.message))
    .finally(() => {
      server.listen(PORT, () => {
        console.log(`BFF server listening on port ${PORT}`);
        console.log(`REST + polling API ready`);
        console.log(`Kafka broker: ${KAFKA_BROKER}`);
        console.log(`Samples directory: ${SAMPLES_DIR}`);
        console.log('Waiting for clients to select use cases...');
      });
    });
}
