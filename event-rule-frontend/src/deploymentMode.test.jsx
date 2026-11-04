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
import React from 'react';
import { describe, test, expect, vi, afterEach, beforeEach } from 'vitest';
import { render, screen, act, renderHook, waitFor } from '@testing-library/react';
import RuntimeBadge from './RuntimeBadge';
import {
  resolveDeploymentMode, supportsSqlView, useDeploymentMode, MODE_POLL_MS,
} from './deploymentMode.js';

afterEach(() => { vi.useRealTimers(); vi.restoreAllMocks(); });

describe('resolveDeploymentMode', () => {
  test.each([
    ['a running PTF job', { jobId: 'j', state: 'RUNNING', runtime: 'ptf' }, 'ptf'],
    ['a running DataStream job', { jobId: 'j', state: 'RUNNING', runtime: 'datastream' }, 'datastream'],
    ['case-insensitively', { jobId: 'j', state: 'RUNNING', runtime: 'PTF' }, 'ptf'],
    ['a job that is not RUNNING (still deployed)', { jobId: 'j', state: 'FAILED', runtime: 'ptf' }, 'ptf'],
  ])('%s', (_n, status, expected) => {
    expect(resolveDeploymentMode(status)).toBe(expected);
  });

  test.each([
    ['NOT_STARTED echoing the global toggle', { jobId: null, state: 'NOT_STARTED', runtime: 'ptf' }],
    ['a status without jobId', { state: 'RUNNING', runtime: 'ptf' }],
    ['an unknown runtime', { jobId: 'j', state: 'RUNNING', runtime: 'spark' }],
    ['a missing runtime', { jobId: 'j', state: 'RUNNING' }],
    ['null', null],
    ['undefined', undefined],
    ['a non-object', 'ptf'],
  ])('is null for %s', (_n, status) => {
    expect(resolveDeploymentMode(status)).toBeNull();
  });
});

describe('supportsSqlView', () => {
  test('only PTF', () => {
    expect(supportsSqlView('ptf')).toBe(true);
    expect(supportsSqlView('datastream')).toBe(false);
    expect(supportsSqlView(null)).toBe(false);
    expect(supportsSqlView(undefined)).toBe(false);
    expect(supportsSqlView('anything')).toBe(false);
  });
});

describe('RuntimeBadge', () => {
  test('renders PTF', () => {
    render(<RuntimeBadge mode="ptf" />);
    const b = screen.getByText('PTF');
    expect(b).toHaveAttribute('data-runtime-mode', 'ptf');
    expect(b).toHaveClass('runtime-badge--ptf');
    expect(b.title).toMatch(/Process Table Function/);
  });

  test('renders DataStream, and not PTF', () => {
    render(<RuntimeBadge mode="datastream" />);
    expect(screen.getByText('DataStream')).toHaveClass('runtime-badge--datastream');
    expect(screen.queryByText('PTF')).not.toBeInTheDocument();
  });

  test.each([null, undefined, '', 'spark', 'NOT_STARTED'])('renders nothing for %s', (mode) => {
    const { container } = render(<RuntimeBadge mode={mode} />);
    expect(container).toBeEmptyDOMElement();
  });

  test('renders exactly one badge', () => {
    const { container } = render(<RuntimeBadge mode="ptf" />);
    expect(container.querySelectorAll('[data-runtime-mode]')).toHaveLength(1);
  });
});

describe('useDeploymentMode', () => {
  function respond(map) {
    global.fetch = vi.fn((url) => {
      const id = decodeURIComponent(/agents\/([^/]+)\/status/.exec(url)[1]);
      const body = map[id];
      if (body instanceof Error) return Promise.reject(body);
      if (body === undefined) return Promise.resolve({ ok: false, status: 500, json: () => Promise.resolve({}) });
      return Promise.resolve({ ok: true, json: () => Promise.resolve(body) });
    });
  }
  const PTF = { jobId: 'j1', state: 'RUNNING', runtime: 'ptf' };
  const DS  = { jobId: 'j2', state: 'RUNNING', runtime: 'datastream' };
  const NONE = { jobId: null, state: 'NOT_STARTED', runtime: 'ptf' };

  test('reads the mode of the deployed job from /status', async () => {
    respond({ a: PTF });
    const { result } = renderHook(() => useDeploymentMode('a'));
    expect(result.current).toEqual({ mode: null, jobId: null });
    await waitFor(() => expect(result.current).toEqual({ mode: 'ptf', jobId: 'j1' }));
    expect(global.fetch.mock.calls[0][0]).toMatch(/\/api\/agents\/a\/status$/);
  });

  test('the global toggle echoed for an undeployed agent is not a mode', async () => {
    respond({ a: NONE });
    const { result } = renderHook(() => useDeploymentMode('a'));
    await waitFor(() => expect(global.fetch).toHaveBeenCalled());
    await act(async () => {});
    expect(result.current.mode).toBeNull();
  });

  test('changing the use case drops the previous answer at once', async () => {
    respond({ a: PTF, b: DS });
    const { result, rerender } = renderHook(({ id }) => useDeploymentMode(id), { initialProps: { id: 'a' } });
    await waitFor(() => expect(result.current.mode).toBe('ptf'));

    rerender({ id: 'b' });
    // Same render pass: nothing of 'a' may be visible for 'b'.
    expect(result.current).toEqual({ mode: null, jobId: null });
    await waitFor(() => expect(result.current).toEqual({ mode: 'datastream', jobId: 'j2' }));
  });

  test('a late answer for the previous use case is ignored', async () => {
    let resolveA;
    global.fetch = vi.fn((url) => (url.includes('/agents/a/')
      ? new Promise((res) => { resolveA = () => res({ ok: true, json: () => Promise.resolve(PTF) }); })
      : Promise.resolve({ ok: true, json: () => Promise.resolve(DS) })));
    const { result, rerender } = renderHook(({ id }) => useDeploymentMode(id), { initialProps: { id: 'a' } });
    rerender({ id: 'b' });
    await waitFor(() => expect(result.current.mode).toBe('datastream'));
    await act(async () => { resolveA(); });
    expect(result.current.mode).toBe('datastream');
  });

  test('follows a redeploy: polls, and re-reads when refreshKey changes', async () => {
    vi.useFakeTimers();
    let current = PTF;
    global.fetch = vi.fn(() => Promise.resolve({ ok: true, json: () => Promise.resolve(current) }));
    const { result, rerender } = renderHook(({ k }) => useDeploymentMode('a', k), { initialProps: { k: 'ptf' } });
    await act(async () => {});
    expect(result.current.mode).toBe('ptf');

    current = DS;
    await act(async () => { vi.advanceTimersByTime(MODE_POLL_MS); });
    expect(result.current.mode).toBe('datastream');

    current = NONE;
    const calls = global.fetch.mock.calls.length;
    rerender({ k: 'datastream' });
    await act(async () => {});
    expect(global.fetch.mock.calls.length).toBeGreaterThan(calls);
    expect(result.current.mode).toBeNull();
  });

  test('keeps the last known mode of the same use case on a transient failure', async () => {
    vi.useFakeTimers();
    respond({ a: PTF });
    const { result } = renderHook(() => useDeploymentMode('a'));
    await act(async () => {});
    expect(result.current.mode).toBe('ptf');

    respond({ a: new TypeError('network down') });
    await act(async () => { vi.advanceTimersByTime(MODE_POLL_MS); });
    expect(result.current.mode).toBe('ptf');

    respond({});                       // HTTP 500
    await act(async () => { vi.advanceTimersByTime(MODE_POLL_MS); });
    expect(result.current.mode).toBe('ptf');
  });

  test('stops polling on unmount and without a use case', async () => {
    vi.useFakeTimers();
    respond({ a: PTF });
    const { unmount } = renderHook(() => useDeploymentMode('a'));
    await act(async () => {});
    unmount();
    const calls = global.fetch.mock.calls.length;
    await act(async () => { vi.advanceTimersByTime(MODE_POLL_MS * 3); });
    expect(global.fetch.mock.calls.length).toBe(calls);

    global.fetch.mockClear();
    const { result } = renderHook(() => useDeploymentMode(null));
    await act(async () => { vi.advanceTimersByTime(MODE_POLL_MS * 2); });
    expect(global.fetch).not.toHaveBeenCalled();
    expect(result.current.mode).toBeNull();
  });

  beforeEach(() => { vi.restoreAllMocks(); });
});
