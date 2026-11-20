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
 * Archive-status acknowledgements.
 *
 * Every LOAD / ROLLBACK command published on an agent's archive-control topic is answered by
 * each subtask of the running Flink job with one `archive-status` message on
 * `<archiveControlTopic>-status` (see StatefulAgentProcessor#emitArchiveStatus). The message is
 * the only proof of which archive the job really runs, so a refresh is considered done — and
 * agents.json is updated — only once every expected subtask has acknowledged it.
 *
 * Ack payload: { agentId, status: LOADED|FAILED, requestId, requestedVersion, activeVersion,
 *                subtask, parallelism, error? }
 */

'use strict';

/** Topic carrying the acknowledgements of an archive-control topic (mirrors the Java job). */
const archiveStatusTopic = (controlTopic) => `${controlTopic}-status`;

/**
 * Summarises the acknowledgements received so far.
 *
 * @param {Array<object>} acks            ack payloads (any order, any requestId already filtered)
 * @param {string[]}      expectedAgents  agentIds that must answer (every subtask of each)
 * @returns {{state: 'pending'|'loaded'|'failed'|'partial', loaded: number,
 *            failures: Array<{agentId: string, subtask: number, error: string}>, missing: string[]}}
 *   pending — some expected subtask has not answered yet; loaded — all LOADED;
 *   failed — all answered FAILED; partial — all answered, some LOADED and some FAILED.
 */
function evaluateAcks(acks, expectedAgents) {
  const latest = new Map(); // `${agentId}#${subtask}` → ack (the last one wins)
  for (const ack of acks) {
    if (ack && expectedAgents.includes(ack.agentId)) latest.set(`${ack.agentId}#${ack.subtask}`, ack);
  }

  const missing = [];
  let loaded = 0;
  const failures = [];
  for (const agentId of expectedAgents) {
    const mine = [...latest.values()].filter(a => a.agentId === agentId);
    const parallelism = mine.reduce((m, a) => Math.max(m, Number(a.parallelism) || 1), 1);
    if (mine.length === 0 || mine.length < parallelism) missing.push(agentId);
    for (const a of mine) {
      if (a.status === 'LOADED') loaded += 1;
      else failures.push({ agentId, subtask: a.subtask, error: a.error || 'unknown error' });
    }
  }

  let state = 'pending';
  if (expectedAgents.length > 0 && missing.length === 0) {
    if (failures.length === 0) state = 'loaded';
    else state = loaded === 0 ? 'failed' : 'partial';
  }
  return { state, loaded, failures, missing };
}

/**
 * Builds the function that starts listening for the acknowledgements of one command.
 *
 * It must be awaited BEFORE the command is published: the consumer starts at the end of the
 * status topic, so an ack can never be missed.
 *
 * @param {object}   opts
 * @param {object}   opts.kafka       a KafkaJS client
 * @param {object}  [opts.log]        console-like logger
 * @returns {(args: {topics: string[], requestId: string, expectedAgents: string[]}) =>
 *           Promise<{done: (timeoutMs: number) => Promise<object[]>, close: () => Promise<void>}>}
 */
function createArchiveAckWaiter({ kafka, log = console }) {
  return async function beginArchiveAckWait({ topics, requestId, expectedAgents }) {
    const statusTopics = [...new Set(topics.map(archiveStatusTopic))];

    // The job's sink creates the topic too; creating it here first makes the subscription safe.
    const admin = kafka.admin();
    try {
      await admin.connect();
      await admin.createTopics({
        waitForLeaders: true,
        topics: statusTopics.map(topic => ({ topic, numPartitions: 1 })),
      });
    } catch (err) {
      log.warn(`[di] could not ensure archive-status topic(s) (non-fatal): ${err.message}`);
    } finally {
      await admin.disconnect().catch(() => {});
    }

    const acks = [];
    let notify = () => {};
    const consumer = kafka.consumer({ groupId: `event-rule-bff-archive-ack-${requestId}` });
    let closed = false;
    const close = async () => {
      if (closed) return;
      closed = true;
      await consumer.disconnect().catch(() => {});
    };

    try {
      await consumer.connect();
      for (const topic of statusTopics) await consumer.subscribe({ topic, fromBeginning: false });
      await consumer.run({
        eachMessage: async ({ message }) => {
          try {
            const body = JSON.parse(message.value.toString());
            const ack = body && body.payload ? body.payload : body; // EnvelopeSerializer wraps it
            if (ack && ack.requestId === requestId) {
              acks.push(ack);
              notify();
            }
          } catch (err) {
            log.warn(`[di] ignoring unreadable archive-status message: ${err.message}`);
          }
        },
      });
    } catch (err) {
      await close();
      throw err;
    }

    return {
      /** Resolves with the acks received, as soon as all expected ones are in or on timeout. */
      done: async (timeoutMs) => {
        try {
          await new Promise((resolve) => {
            const timer = setTimeout(resolve, timeoutMs);
            notify = () => {
              if (evaluateAcks(acks, expectedAgents).state !== 'pending') {
                clearTimeout(timer);
                resolve();
              }
            };
            notify(); // acks may already be complete
          });
          return acks.slice();
        } finally {
          await close();
        }
      },
      close,
    };
  };
}

module.exports = { archiveStatusTopic, evaluateAcks, createArchiveAckWaiter };
