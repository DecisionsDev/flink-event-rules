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
import { useEffect, useState } from 'react';
import { API_URL } from './config.js';

/**
 * Deployment mode of an agent — the single place that decides it.
 *
 * Source of truth: the `runtime` the BFF stored with the job when it was submitted
 * (`samples/<id>/.job-state.json`), served by GET /api/agents/:id/status.  It is NOT the
 * global "Flink Runtime" toggle: that toggle only selects the runtime of the *next*
 * deployment, and the BFF echoes it in the status response when no job exists.
 *
 *   'ptf'        — job runs the Process Table Function engine (SQL topology)
 *   'datastream' — job runs the DataStream engine
 *   null         — nothing deployed (or not known yet): no mode, no badge
 */
export const MODE_PTF        = 'ptf';
export const MODE_DATASTREAM = 'datastream';

export const MODE_LABELS = {
  [MODE_PTF]:        'PTF',
  [MODE_DATASTREAM]: 'DataStream',
};

export const MODE_DESCRIPTIONS = {
  [MODE_PTF]:        'Running on the Flink Process Table Function (PTF) engine',
  [MODE_DATASTREAM]: 'Running on the Flink DataStream engine',
};

/** Interval at which the deployed mode is re-read while a dashboard is open. */
export const MODE_POLL_MS = 5000;

/**
 * Deployment mode described by a /status response, or null when no job is deployed.
 * Anything the dashboard does not know how to present is treated as "no mode" rather
 * than guessed.
 */
export function resolveDeploymentMode(status) {
  if (!status || typeof status !== 'object') return null;
  if (!status.jobId || status.state === 'NOT_STARTED') return null;
  const runtime = String(status.runtime ?? '').toLowerCase();
  return runtime === MODE_PTF || runtime === MODE_DATASTREAM ? runtime : null;
}

/** The SQL view exists for PTF deployments only. */
export function supportsSqlView(mode) {
  return mode === MODE_PTF;
}

const NO_DEPLOYMENT = { mode: null, jobId: null };

/**
 * Tracks the deployment mode of a use case.
 *
 * Re-reads /status on mount, every MODE_POLL_MS and whenever `refreshKey` changes, so a
 * redeploy, an undeploy or a switch of runtime shows up without a page reload.  Switching
 * to another use case discards the previous answer immediately, so one agent's mode is
 * never shown for another.  A transient failure keeps the last known value of the same
 * use case: the job is still what it was.
 *
 * @returns {{ mode: ('ptf'|'datastream'|null), jobId: (string|null) }}
 */
export function useDeploymentMode(useCaseId, refreshKey) {
  // The answer is stored with the use case it belongs to, so a stale one is ignored
  // during the render that follows a change of use case.
  const [answer, setAnswer] = useState({ useCaseId: null, ...NO_DEPLOYMENT });

  useEffect(() => {
    if (!useCaseId) return undefined;
    let cancelled = false;
    let controller = null;

    const read = async () => {
      controller?.abort();
      controller = new AbortController();
      try {
        const r = await fetch(`${API_URL}/api/agents/${encodeURIComponent(useCaseId)}/status`, { signal: controller.signal });
        if (cancelled || !r.ok) return;
        const status = await r.json();
        if (cancelled) return;
        setAnswer({ useCaseId, mode: resolveDeploymentMode(status), jobId: status?.jobId ?? null });
      } catch { /* aborted or unreachable — keep the last known value */ }
    };

    read();
    const timer = setInterval(read, MODE_POLL_MS);
    return () => { cancelled = true; clearInterval(timer); controller?.abort(); };
  }, [useCaseId, refreshKey]);

  return answer.useCaseId === useCaseId ? { mode: answer.mode, jobId: answer.jobId } : NO_DEPLOYMENT;
}
