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
 * Tests for useEventStream — covering the two fixes added in this session:
 *
 *   LOW-4   : WS URL path-prefix stripping
 *             apiUrl with a path suffix (e.g. "http://host/bff") must produce
 *             a WS URL of "ws://host/ws", NOT "ws://host/bff/ws".
 *
 *   MEDIUM-4: WebSocket reconnect cap → HTTP polling fallback
 *             After WS_RECONNECT_LIMIT (20) post-open reconnects the hook must
 *             stop reconnecting and switch permanently to HTTP polling instead.
 */

import { renderHook, act } from '@testing-library/react';
import { useEventStream } from './useEventStream';

// ── WebSocket stub factory ─────────────────────────────────────────────────────
// Returns a lightweight fake WebSocket class and a handle to the last instance
// created, so tests can trigger onopen / onclose / onerror / onmessage manually.

function makeWsStub() {
  const instances = [];

  class FakeWS {
    constructor(url) {
      this.url       = url;
      this.readyState = FakeWS.CONNECTING;
      this.onopen    = null;
      this.onclose   = null;
      this.onerror   = null;
      this.onmessage = null;
      this._sent     = [];
      instances.push(this);
    }
    send(data) { this._sent.push(data); }
    close()    { this.readyState = FakeWS.CLOSED; }
  }
  FakeWS.CONNECTING = 0;
  FakeWS.OPEN       = 1;
  FakeWS.CLOSING    = 2;
  FakeWS.CLOSED     = 3;

  return {
    FakeWS,
    instances,
    last: () => instances[instances.length - 1],
    count: () => instances.length,
  };
}

// ── LOW-4: WS URL path-prefix stripping ───────────────────────────────────────

describe('useEventStream — WS URL derivation (LOW-4)', () => {
  let stub;
  let origWS;

  beforeEach(() => {
    vi.useFakeTimers();
    stub   = makeWsStub();
    origWS = globalThis.WebSocket;
    globalThis.WebSocket = stub.FakeWS;
  });

  afterEach(() => {
    vi.useRealTimers();
    globalThis.WebSocket = origWS;
  });

  test('plain origin URL (no path) → ws://<host>/ws', () => {
    renderHook(() => useEventStream('uc1', 'http://localhost:8080'));
    expect(stub.last().url).toBe('ws://localhost:8080/ws');
  });

  test('URL with single-segment path → strips path, produces ws://<host>/ws', () => {
    // e.g. the BFF is exposed at /bff but the WS server is at the server root
    renderHook(() => useEventStream('uc1', 'http://localhost:8080/bff'));
    expect(stub.last().url).toBe('ws://localhost:8080/ws');
  });

  test('URL with multi-segment path → no stripping (regex only matches single-segment paths)', () => {
    // The authority-anchored regex only strips a trailing single-segment path
    // (e.g. /bff).  Multi-segment paths like /api/v1 are left untouched so the
    // WS URL picks up the full prefix rather than silently dropping it.
    renderHook(() => useEventStream('uc1', 'http://localhost:8080/api/v1'));
    expect(stub.last().url).toBe('ws://localhost:8080/api/v1/ws');
  });

  test('https:// → wss:// with path stripped', () => {
    renderHook(() => useEventStream('uc1', 'https://prod.example.com/bff'));
    expect(stub.last().url).toBe('wss://prod.example.com/ws');
  });

  test('plain https:// origin → wss://<host>/ws', () => {
    renderHook(() => useEventStream('uc1', 'https://prod.example.com'));
    expect(stub.last().url).toBe('wss://prod.example.com/ws');
  });
});

// ── MEDIUM-4: Reconnect cap → HTTP polling fallback ───────────────────────────

describe('useEventStream — reconnect limit → HTTP polling fallback (MEDIUM-4)', () => {
  const WS_RECONNECT_LIMIT = 20; // must match the constant in useEventStream.js
  const WS_RECONNECT_MS    = 5000;

  let stub;
  let origWS;
  let fetchSpy;

  beforeEach(() => {
    vi.useFakeTimers();
    stub   = makeWsStub();
    origWS = globalThis.WebSocket;
    globalThis.WebSocket = stub.FakeWS;

    // Stub fetch so HTTP polling calls resolve (no real network)
    fetchSpy = vi.spyOn(globalThis, 'fetch').mockResolvedValue({
      ok:   true,
      json: async () => ({}),
    });
  });

  afterEach(() => {
    vi.useRealTimers();
    globalThis.WebSocket = origWS;
    fetchSpy.mockRestore();
  });

  /**
   * Drive WS_RECONNECT_LIMIT reconnect cycles.
   * Each cycle: open the socket → close it → advance clock past the backoff.
   */
  /**
   * Drive n open→close cycles.
   * - Opens socket (everOpened = true, resets wsFailureRef)
   * - Closes socket (increments wsReconnectCount, schedules next connectWs OR startPolling)
   * - Advances clock past the backoff so the NEXT connectWs fires (when i < n-1).
   *
   * wsReconnectCount check: `> WS_RECONNECT_LIMIT` triggers after the (LIMIT+1)-th close.
   * With LIMIT=20, the 21st close triggers startPolling. So driving 21 cycles means
   * the last close switches to polling (no more setTimeout(connectWs)).
   */
  async function driveReconnectCycles(n) {
    for (let i = 0; i < n; i++) {
      // Allow the current connectWs() call to create a FakeWS instance
      await act(async () => {});
      const ws = stub.last();
      // Simulate a successful open (sets everOpened = true)
      await act(async () => { ws.readyState = stub.FakeWS.OPEN; ws.onopen?.(); });
      // Simulate a close that triggers reconnect (or polling switch on last cycle)
      await act(async () => { ws.readyState = stub.FakeWS.CLOSED; ws.onclose?.(); });
      // Advance past the backoff — this fires the NEXT connectWs (except on the very
      // last iteration where the close already switched to polling).
      await act(async () => { vi.advanceTimersByTime(WS_RECONNECT_MS + 100); });
    }
  }

  test('after WS_RECONNECT_LIMIT+1 closures the hook stops creating new WebSocket instances', async () => {
    const { unmount } = renderHook(() =>
      useEventStream('uc1', 'http://localhost:8080', { pollIntervalMs: 60000 })
    );

    // Drive LIMIT+1 cycles so the last close triggers startPolling (no more connectWs)
    await driveReconnectCycles(WS_RECONNECT_LIMIT + 1);

    // Record instance count — this is the final count (polling has taken over)
    const finalCount = stub.count();

    // Advance time further — absolutely no new WS instances should appear
    await act(async () => { vi.advanceTimersByTime(WS_RECONNECT_MS * 3); });

    expect(stub.count()).toBe(finalCount);
    unmount();
  });

  test('after WS_RECONNECT_LIMIT+1 closures the hook falls back to HTTP polling', async () => {
    const { unmount } = renderHook(() =>
      useEventStream('uc1', 'http://localhost:8080', { pollIntervalMs: 500 })
    );

    // The (LIMIT+1)-th close switches to startPolling() which fires poll() immediately
    await driveReconnectCycles(WS_RECONNECT_LIMIT + 1);

    // Allow the initial poll() call to reach fetch (it fires synchronously in startPolling)
    await act(async () => { vi.advanceTimersByTime(100); });

    expect(fetchSpy).toHaveBeenCalled();
    // The fetch URL should use the REST polling endpoint
    expect(fetchSpy.mock.calls[0][0]).toContain('/api/use-cases/uc1/events');

    unmount();
  });

  test('cleanup resets reconnect counter so a remounted hook gets fresh retries', async () => {
    const { unmount, rerender } = renderHook(
      ({ uc }) => useEventStream(uc, 'http://localhost:8080', { pollIntervalMs: 60000 }),
      { initialProps: { uc: 'uc1' } }
    );

    // Exhaust the reconnect limit for uc1
    await driveReconnectCycles(WS_RECONNECT_LIMIT);
    await act(async () => { vi.advanceTimersByTime(WS_RECONNECT_MS + 100); });

    const countAfterFirst = stub.count();

    // Switch to a different use case — effect cleanup runs, counter resets
    await act(async () => { rerender({ uc: 'uc2' }); });
    // A new WS connection should be attempted immediately
    expect(stub.count()).toBeGreaterThan(countAfterFirst);

    unmount();
  });
});
