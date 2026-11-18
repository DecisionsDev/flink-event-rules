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
import { useEffect, useRef, useState } from 'react';
import { API_URL } from './config.js';

export const LIVE_POLL_MS = 5000;
const HISTORY_LEN = 24;

/**
 * Polls the BFF for each agent's processed-event counters so the diagram can show live traffic.
 *
 *   GET /api/agents/:useCaseId/entity-state?agentId=<id>&summary=true
 *     -> { agentSummary: { inboundCounts: { [eventType]: n } } }
 *
 * Returns { [agentId]: { counts, total, history } } — `history` is the last cumulative totals,
 * used for the sparkline. Failures are ignored (the diagram simply shows no live data).
 * Polling pauses while the tab is hidden.
 */
export function useAgentLiveStats(useCaseId, agentIds) {
  const [live, setLive] = useState({});
  const histories = useRef({});
  const idsKey = (agentIds || []).join('\u0000');

  useEffect(() => {
    if (!useCaseId || !idsKey || typeof fetch !== 'function') return undefined;
    const ids = idsKey.split('\u0000');
    let cancelled = false;

    const pollOnce = async () => {
      if (typeof document !== 'undefined' && document.hidden) return;
      const next = {};
      await Promise.all(ids.map(async (agentId) => {
        try {
          const r = await fetch(`${API_URL}/api/agents/${useCaseId}/entity-state?agentId=${encodeURIComponent(agentId)}&summary=true`);
          if (!r.ok) return;
          const json = await r.json();
          const counts = json?.agentSummary?.inboundCounts || {};
          const total = Object.values(counts).reduce((s, n) => s + (Number(n) || 0), 0);
          const hist = [...(histories.current[agentId] || []), total].slice(-HISTORY_LEN);
          histories.current[agentId] = hist;
          next[agentId] = { counts, total, history: hist };
        } catch { /* live data is optional */ }
      }));
      if (!cancelled && Object.keys(next).length) setLive(prev => ({ ...prev, ...next }));
    };

    pollOnce();
    const timer = setInterval(pollOnce, LIVE_POLL_MS);
    return () => { cancelled = true; clearInterval(timer); };
  }, [useCaseId, idsKey]);

  return live;
}
