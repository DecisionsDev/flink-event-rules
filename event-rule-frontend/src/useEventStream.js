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
 * useEventStream
 *
 * Subscribes to live event updates for a use-case.  Prefers the WebSocket push
 * channel (`ws://<host>/ws`) for instant updates; falls back to HTTP polling
 * when the BFF WebSocket endpoint is unavailable (e.g. behind a proxy that does
 * not support upgrade).
 *
 * The hook is a drop-in replacement for the inline polling loop in App.jsx:
 *
 *   const { eventsByType, error } = useEventStream(selectedUseCase, apiUrl);
 *
 * @param {string|null} useCase   - BFF use-case folder key, or null to suspend.
 * @param {string}      apiUrl    - REST base URL (e.g. 'http://localhost:8080').
 * @param {object}      [options]
 * @param {number}      [options.pollIntervalMs=2000]  - poll interval in ms (fallback mode).
 * @param {number}      [options.wsPingIntervalMs=30000] - keepalive ping interval in ms.
 */

import { useState, useEffect, useRef } from 'react';

const DEFAULT_POLL_MS    = 2000;
const DEFAULT_PING_MS    = 30000;
const WS_RECONNECT_MS    = 5000;  // backoff before reconnect attempt
const WS_FAILURE_LIMIT   = 3;     // failed *initial* connects before giving up on WS
// Maximum total reconnects after a successful open — prevents infinite retry noise
// when the BFF is shut down permanently after a session.
const WS_RECONNECT_LIMIT = 20;

export function useEventStream(useCase, apiUrl, {
  pollIntervalMs   = DEFAULT_POLL_MS,
  wsPingIntervalMs = DEFAULT_PING_MS,
} = {}) {
  const [eventsByType, setEventsByType] = useState({});
  const [error, setError]               = useState(null);

  // Stable refs so the effect cleanup works across re-renders
  const wsRef            = useRef(null);
  const pollTimerRef     = useRef(null);
  const pingTimerRef     = useRef(null);
  const cancelledRef     = useRef(false);
  const wsFailureRef     = useRef(0);
  const wsReconnectCount = useRef(0);

  useEffect(() => {
    if (!useCase) {
      // Clear stale events when the use case is deselected so the previous use
      // case's data never bleeds into the next one.
      setEventsByType({});
      return;
    }

    // Clear immediately on use-case change so the panel does not flash stale events
    // from the previous use case while the new connection is being established.
    setEventsByType({});

    cancelledRef.current  = false;
    wsFailureRef.current  = 0;

    // ── Derive WebSocket URL from apiUrl ────────────────────────────────────
    // Strip any path prefix so the WS endpoint is always at the server root /ws,
    // matching the BFF WebSocketServer registration (path: '/ws').
    const wsOrigin = apiUrl
      .replace(/^http:\/\//, 'ws://')
      .replace(/^https:\/\//, 'wss://')
      // Strip single-segment path suffix if present (e.g. /bff → '').
      // The capture group (\/\/[^/]+) anchors to the authority so that bare
      // origins like 'ws://host:8080' (no path at all) are left untouched.
      .replace(/(\/\/[^/]+)(\/[^/]*)$/, '$1');
    const wsUrl = `${wsOrigin}/ws`;

    // ── HTTP fallback poll ──────────────────────────────────────────────────
    function startPolling() {
      function poll() {
        if (cancelledRef.current) return;
        fetch(`${apiUrl}/api/use-cases/${useCase}/events`)
          .then(r => { if (!r.ok) throw new Error(r.statusText); return r.json(); })
          .then(data => {
            if (!cancelledRef.current) { setEventsByType(data); setError(null); }
          })
          .catch(err => {
            if (!cancelledRef.current) setError(`Polling error: ${err.message}`);
          })
          .finally(() => {
            if (!cancelledRef.current)
              pollTimerRef.current = setTimeout(poll, pollIntervalMs);
          });
      }
      poll();
    }

    // ── WebSocket connection ────────────────────────────────────────────────
    function connectWs() {
      if (cancelledRef.current) return;
      if (wsFailureRef.current >= WS_FAILURE_LIMIT) {
        // Give up on WS; fall back to polling permanently
        console.info('[useEventStream] WebSocket unavailable — switching to HTTP polling');
        startPolling();
        return;
      }

      let ws;
      try {
        ws = new WebSocket(wsUrl);
      } catch {
        wsFailureRef.current++;
        startPolling();
        return;
      }
      wsRef.current = ws;
      // Track whether this socket ever successfully opened.
      // onerror that fires after onopen (e.g. network drop mid-session) should
      // NOT count as a connection failure — we already know WS works.
      let everOpened = false;

      ws.onopen = () => {
        everOpened = true;
        wsFailureRef.current = 0;
        ws.send(JSON.stringify({ type: 'subscribe', useCase }));
        // Keepalive ping
        pingTimerRef.current = setInterval(() => {
          if (ws.readyState === WebSocket.OPEN)
            ws.send(JSON.stringify({ type: 'ping' }));
        }, wsPingIntervalMs);
      };

      ws.onmessage = (e) => {
        try {
          const msg = JSON.parse(e.data);
          if (msg.type === 'events' && msg.useCase === useCase && msg.data) {
            if (!cancelledRef.current) { setEventsByType(msg.data); setError(null); }
          }
        } catch { /* ignore malformed frame */ }
      };

      ws.onerror = () => {
        // Only count as a failure when the connection was never established.
        // Errors after a successful open are transient drops — onclose will
        // handle reconnection with backoff.
        if (!everOpened) wsFailureRef.current++;
      };

      ws.onclose = () => {
        clearInterval(pingTimerRef.current);
        if (!cancelledRef.current) {
          wsReconnectCount.current++;
          if (wsReconnectCount.current > WS_RECONNECT_LIMIT) {
            // Too many reconnects — BFF is likely shut down. Fall back to polling.
            console.info('[useEventStream] WebSocket reconnect limit reached — switching to HTTP polling');
            startPolling();
            return;
          }
          // Reconnect with a short backoff
          setTimeout(connectWs, WS_RECONNECT_MS);
        }
      };
    }

    connectWs();

    return () => {
      cancelledRef.current = true;
      wsReconnectCount.current = 0;
      clearTimeout(pollTimerRef.current);
      clearInterval(pingTimerRef.current);
      if (wsRef.current) {
        wsRef.current.onclose = null; // prevent reconnect on intentional close
        wsRef.current.close();
        wsRef.current = null;
      }
    };
  }, [useCase, apiUrl, pollIntervalMs, wsPingIntervalMs]);

  return { eventsByType, setEventsByType, error };
}
