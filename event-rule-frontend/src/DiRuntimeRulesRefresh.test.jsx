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
import { render, screen, fireEvent, waitFor, within } from '@testing-library/react';
import DiRuntimeRulesRefresh from './DiRuntimeRulesRefresh';
import { AgentManagementPanel } from './App';

const RUNTIMES  = [{ id: 'rt1', name: 'Runtime One', url: 'http://x', deploymentSpace: 'embedded' }];
const DECISIONS = [{ id: 'proj/svc/1.0', name: 'svc  1.0', status: 'deployed' }];

let status = { jobId: 'j1', state: 'RUNNING', runtime: 'datastream' };

function installFetch(refreshResponse) {
  global.fetch = vi.fn((url) => {
    if (/\/refresh-rules/.test(url)) return Promise.resolve(refreshResponse);
    if (/\/decisions/.test(url)) return Promise.resolve({ ok: true, json: () => Promise.resolve(DECISIONS) });
    if (/\/api\/ads-runtimes$/.test(url)) return Promise.resolve({ ok: true, json: () => Promise.resolve(RUNTIMES) });
    if (/\/status$/.test(url)) return Promise.resolve({ ok: true, json: () => Promise.resolve(status) });
    return Promise.resolve({ ok: true, json: () => Promise.resolve({}) });
  });
}

const expand = () => fireEvent.click(screen.getByRole('button', { name: 'Expand Refresh Rules' }));
const refreshButton = () => screen.getByRole('button', { name: 'Refresh Rules' });

async function selectDecision() {
  await waitFor(() => expect(screen.getByRole('option', { name: 'Runtime One' })).toBeInTheDocument());
  fireEvent.change(screen.getByLabelText('Select DI runtime'), { target: { value: 'rt1' } });
  await screen.findByLabelText('Select decision');
  fireEvent.change(screen.getByLabelText('Select decision'), { target: { value: 'proj/svc/1.0' } });
}

afterEach(() => { vi.restoreAllMocks(); });

describe('DiRuntimeRulesRefresh', () => {
  test('is always visible and sends nothing until the Refresh Rules button is pressed', async () => {
    installFetch({ ok: true, json: () => Promise.resolve({ filename: 'svc_1.0.jar', topics: ['uc-control'] }) });
    render(<DiRuntimeRulesRefresh useCaseId="uc" />);
    expand();

    expect(screen.getByLabelText('Select DI runtime')).toBeInTheDocument();
    expect(refreshButton()).toBeDisabled(); // nothing selected yet

    await selectDecision();
    expect(global.fetch.mock.calls.some(([u]) => /refresh-rules/.test(u))).toBe(false);
    expect(refreshButton()).toBeEnabled();

    fireEvent.click(refreshButton());

    await screen.findByText(/reload command sent to uc-control/);
    const call = global.fetch.mock.calls.find(([u]) => /refresh-rules/.test(u));
    expect(call[0]).toContain('/api/ads-runtimes/rt1/refresh-rules?decisionId=proj%2Fsvc%2F1.0&useCaseId=uc&deploymentSpace=embedded');
    expect(call[1]).toEqual({ method: 'POST' });
  });

  test('shows the error returned by the BFF', async () => {
    installFetch({ ok: false, statusText: 'Bad', json: () => Promise.resolve({ error: 'boom' }) });
    render(<DiRuntimeRulesRefresh useCaseId="uc" />);
    expand();
    await selectDecision();
    fireEvent.click(refreshButton());
    await screen.findByText('Refresh failed: boom');
  });

  test('disabled: every control is disabled and no runtime is requested', () => {
    installFetch({ ok: true, json: () => Promise.resolve({}) });
    render(<DiRuntimeRulesRefresh useCaseId="uc" disabled disabledReason="nope" />);
    expand();
    expect(screen.getByLabelText('Select DI runtime')).toBeDisabled();
    expect(refreshButton()).toBeDisabled();
    expect(screen.getByText('nope')).toBeInTheDocument();
    expect(global.fetch).not.toHaveBeenCalled();
  });

  test('can be collapsed and expanded', async () => {
    installFetch({ ok: true, json: () => Promise.resolve({}) });
    render(<DiRuntimeRulesRefresh useCaseId="uc" />);
    await waitFor(() => expect(screen.getByRole('option', { name: 'Runtime One', hidden: true })).toBeInTheDocument());
    const toggle = screen.getByRole('button', { name: 'Expand Refresh Rules' });
    expect(toggle).toHaveAttribute('aria-expanded', 'false');
    expect(screen.getByLabelText('Select DI runtime')).not.toBeVisible();

    fireEvent.click(toggle);
    expect(screen.getByRole('button', { name: 'Collapse Refresh Rules' })).toHaveAttribute('aria-expanded', 'true');
    expect(screen.getByLabelText('Select DI runtime')).toBeVisible();

    fireEvent.click(screen.getByRole('button', { name: 'Collapse Refresh Rules' }));
    expect(screen.getByLabelText('Select DI runtime')).not.toBeVisible();
  });
});

describe('AgentManagementPanel — Refresh Rules sub-section', () => {
  const panel = () => (
    <AgentManagementPanel agentId="uc" useCaseId="uc" onNavigateBack={vi.fn()} flinkRuntime="datastream" />
  );
  const section = () => screen.getByTestId('refresh-rules-section');

  test('is part of the Agent Management panel and enabled for a DataStream job', async () => {
    status = { jobId: 'j1', state: 'RUNNING', runtime: 'datastream' };
    installFetch({ ok: true, json: () => Promise.resolve({}) });
    const { container } = render(panel());
    expect(container.querySelector('.agent-mgmt-panel')).toContainElement(section());
    await waitFor(() => expect(within(section()).getByLabelText('Select DI runtime')).toBeEnabled());
  });

  test('disabled for a PTF job', async () => {
    status = { jobId: 'j1', state: 'RUNNING', runtime: 'ptf' };
    installFetch({ ok: true, json: () => Promise.resolve({}) });
    render(panel());
    await screen.findByText('Rules refresh requires the DataStream runtime');
    fireEvent.click(within(section()).getByRole('button', { name: 'Expand Refresh Rules' }));
    expect(within(section()).getByLabelText('Select DI runtime')).toBeDisabled();
    expect(within(section()).getByRole('button', { name: 'Refresh Rules' })).toBeDisabled();
  });

  test('disabled when no job is deployed', async () => {
    status = { jobId: null, state: 'NOT_STARTED', runtime: 'datastream' };
    installFetch({ ok: true, json: () => Promise.resolve({}) });
    render(panel());
    await screen.findByText('No job deployed');
    expect(within(section()).getByLabelText('Select DI runtime')).toBeDisabled();
  });
});
