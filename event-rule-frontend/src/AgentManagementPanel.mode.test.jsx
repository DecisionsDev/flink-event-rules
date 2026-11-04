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
import { describe, test, expect, vi, afterEach } from 'vitest';
import { render, screen, fireEvent, waitFor, act } from '@testing-library/react';
import { AgentManagementPanel } from './App';
import { MODE_POLL_MS } from './deploymentMode.js';
import airline from './__fixtures__/ptf-airline.json';

/**
 * Deployment mode badge and PTF-only SQL View of the dashboard's Agent Management panel.
 * `status` is what GET /api/agents/:id/status answers; the global `flinkRuntime` prop is
 * deliberately set to something different to prove that the panel does not follow it.
 */
let status;
let sqlResponse;

function installFetch() {
  global.fetch = vi.fn((url) => {
    if (/\/status$/.test(url)) {
      return Promise.resolve({ ok: true, json: () => Promise.resolve(status) });
    }
    if (/\/sql$/.test(url)) {
      return Promise.resolve(sqlResponse());
    }
    return Promise.resolve({ ok: true, json: () => Promise.resolve({}) });
  });
}

const PTF  = { agentId: 'uc', jobId: 'job-ptf-1', state: 'RUNNING', runtime: 'ptf' };
const DS   = { agentId: 'uc', jobId: 'job-ds-1',  state: 'RUNNING', runtime: 'datastream' };
const NONE = { agentId: 'uc', jobId: null, state: 'NOT_STARTED', runtime: 'ptf' };

function panel(props = {}) {
  return (
    <AgentManagementPanel
      agentId="uc" useCaseId="uc" onEdit={vi.fn()} onNavigateBack={vi.fn()}
      agentsJson={{ agents: [] }} flinkRuntime="datastream" {...props}
    />
  );
}

const badges = (container) => [...container.querySelectorAll('[data-runtime-mode]')].map(b => b.textContent);
const sqlButton = () => screen.queryByRole('button', { name: 'SQL View' });

afterEach(() => { vi.useRealTimers(); vi.restoreAllMocks(); });

describe('AgentManagementPanel — deployment mode', () => {
  test('PTF agent: PTF badge and SQL View button', async () => {
    status = PTF; installFetch();
    const { container } = render(panel());
    await waitFor(() => expect(badges(container)).toEqual(['PTF']));
    expect(sqlButton()).toBeInTheDocument();
  });

  test('PTF agent stays PTF whatever the global toggle says', async () => {
    status = PTF; installFetch();
    const { container } = render(panel({ flinkRuntime: 'datastream' }));
    await waitFor(() => expect(badges(container)).toEqual(['PTF']));
    expect(badges(container)).not.toContain('DataStream');
  });

  test('DataStream agent: DataStream badge, no PTF badge, no SQL View — even if the toggle says ptf', async () => {
    status = DS; installFetch();
    const { container } = render(panel({ flinkRuntime: 'ptf' }));
    await waitFor(() => expect(badges(container)).toEqual(['DataStream']));
    expect(screen.queryByText('PTF')).not.toBeInTheDocument();
    expect(sqlButton()).not.toBeInTheDocument();
  });

  test('not deployed: no badge and no SQL View (hidden, not disabled)', async () => {
    status = NONE; installFetch();
    const { container } = render(panel({ flinkRuntime: 'ptf' }));
    await waitFor(() => expect(global.fetch).toHaveBeenCalled());
    await act(async () => {});
    expect(badges(container)).toEqual([]);
    expect(sqlButton()).not.toBeInTheDocument();
    expect(screen.queryByText('SQL View')).not.toBeInTheDocument();
  });

  test('status request failing: no badge and no SQL View', async () => {
    global.fetch = vi.fn(() => Promise.reject(new TypeError('network down')));
    const { container } = render(panel());
    await act(async () => {});
    expect(badges(container)).toEqual([]);
    expect(sqlButton()).not.toBeInTheDocument();
  });

  test('the other dashboard actions are unaffected in every mode', async () => {
    for (const s of [PTF, DS, NONE]) {
      status = s; installFetch();
      const { unmount } = render(panel());
      await act(async () => {});
      for (const name of ['Edit Agent', 'Diagram View', 'Delete Agent', 'Undeploy Agent']) {
        expect(screen.getByRole('button', { name })).toBeInTheDocument();
      }
      unmount();
    }
  });
});

describe('AgentManagementPanel — switching deployment modes', () => {
  test('PTF → DataStream: badge flips, button disappears, and an open SQL view is closed', async () => {
    vi.useFakeTimers();
    status = PTF;
    sqlResponse = () => ({ ok: true, json: () => Promise.resolve({ script: 'S', statements: airline.statements, stale: false }) });
    installFetch();
    const { container } = render(panel());
    await act(async () => {});
    expect(badges(container)).toEqual(['PTF']);

    fireEvent.click(sqlButton());
    await act(async () => {});
    expect(screen.getByRole('dialog', { name: 'SQL View' })).toBeInTheDocument();

    status = DS;                                      // the job is redeployed on DataStream
    await act(async () => { vi.advanceTimersByTime(MODE_POLL_MS); });
    expect(badges(container)).toEqual(['DataStream']);
    expect(screen.queryByRole('dialog', { name: 'SQL View' })).not.toBeInTheDocument();
    expect(sqlButton()).not.toBeInTheDocument();

    // Back to PTF: the SQL view must NOT pop back open on its own.
    status = PTF;
    await act(async () => { vi.advanceTimersByTime(MODE_POLL_MS); });
    expect(badges(container)).toEqual(['PTF']);
    expect(sqlButton()).toBeInTheDocument();
    expect(screen.queryByRole('dialog', { name: 'SQL View' })).not.toBeInTheDocument();
  });

  test('PTF → undeployed: no badge, no button', async () => {
    vi.useFakeTimers();
    status = PTF; installFetch();
    const { container } = render(panel());
    await act(async () => {});
    expect(badges(container)).toEqual(['PTF']);

    status = NONE;
    await act(async () => { vi.advanceTimersByTime(MODE_POLL_MS); });
    expect(badges(container)).toEqual([]);
    expect(sqlButton()).not.toBeInTheDocument();
  });

  test('another use case never shows the previous one\'s mode', async () => {
    const byId = { a: { ...PTF, agentId: 'a' }, b: { ...DS, agentId: 'b' } };
    global.fetch = vi.fn((url) => {
      const id = /agents\/([^/]+)\/status/.exec(url)[1];
      return Promise.resolve({ ok: true, json: () => Promise.resolve(byId[id]) });
    });
    const { container, rerender } = render(panel({ useCaseId: 'a' }));
    await waitFor(() => expect(badges(container)).toEqual(['PTF']));

    rerender(panel({ useCaseId: 'b' }));
    expect(badges(container)).toEqual([]);            // same render pass: nothing stale
    expect(sqlButton()).not.toBeInTheDocument();
    await waitFor(() => expect(badges(container)).toEqual(['DataStream']));
    expect(sqlButton()).not.toBeInTheDocument();
  });

  test('changing the global toggle re-reads the deployed mode without changing it by itself', async () => {
    status = PTF; installFetch();
    const { container, rerender } = render(panel({ flinkRuntime: 'ptf' }));
    await waitFor(() => expect(badges(container)).toEqual(['PTF']));
    const calls = global.fetch.mock.calls.length;

    rerender(panel({ flinkRuntime: 'datastream' }));  // only the next deployment will change
    await waitFor(() => expect(global.fetch.mock.calls.length).toBeGreaterThan(calls));
    expect(badges(container)).toEqual(['PTF']);
    expect(sqlButton()).toBeInTheDocument();
  });
});
