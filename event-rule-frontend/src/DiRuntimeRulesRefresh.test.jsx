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

let rulesStatus = null; // body of GET /rules-status (null = endpoint unavailable)
let restoreResponse = { ok: true, json: () => Promise.resolve({ filename: 'Svc_1.0.jar' }) };

function installFetch(refreshResponse) {
  global.fetch = vi.fn((url) => {
    if (/\/rules-status/.test(url)) {
      return Promise.resolve(rulesStatus
        ? { ok: true, json: () => Promise.resolve(rulesStatus) }
        : { ok: false, json: () => Promise.resolve({ error: 'x' }) });
    }
    if (/\/restore-default/.test(url)) return Promise.resolve(restoreResponse);
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
  const group = await screen.findByRole('radiogroup', { name: 'Select decision' });
  fireEvent.click(within(group).getByRole('radio', { name: 'svc 1.0' }));
}

afterEach(() => {
  vi.restoreAllMocks();
  rulesStatus = null;
  restoreResponse = { ok: true, json: () => Promise.resolve({ filename: 'Svc_1.0.jar' }) };
});

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

    await screen.findByText(/svc_1\.0\.jar is now running — confirmed by the job \(uc-control\)/);
    const call = global.fetch.mock.calls.find(([u]) => /refresh-rules/.test(u));
    expect(call[0]).toContain('/api/ads-runtimes/rt1/refresh-rules?decisionId=proj%2Fsvc%2F1.0&useCaseId=uc&deploymentSpace=embedded');
    expect(call[0]).toContain('&updateAgentsJson=false');
    expect(call[1]).toEqual({ method: 'POST' });
  });

  test('the "Persist Rules" checkbox is unchecked by default and propagated in the request', async () => {
    installFetch({ ok: true, json: () => Promise.resolve({ filename: 'svc_1.0.jar', topics: ['uc-control'] }) });
    render(<DiRuntimeRulesRefresh useCaseId="uc" />);
    expand();
    const box = screen.getByRole('checkbox', { name: 'Persist Rules' });
    expect(box).not.toBeChecked();

    fireEvent.click(box);
    expect(box).toBeChecked();
    await selectDecision();
    fireEvent.click(refreshButton());

    await screen.findByText(/svc_1\.0\.jar is now running/);
    const call = global.fetch.mock.calls.find(([u]) => /refresh-rules/.test(u));
    expect(call[0]).toContain('&updateAgentsJson=true');
  });

  test('shows the error returned by the BFF', async () => {
    installFetch({ ok: false, statusText: 'Bad', json: () => Promise.resolve({ error: 'boom' }) });
    render(<DiRuntimeRulesRefresh useCaseId="uc" />);
    expand();
    await selectDecision();
    fireEvent.click(refreshButton());
    await screen.findByText('Refresh failed: boom');
  });

  test('shows the selected archive in a summary and lets the user clear it', async () => {
    installFetch({ ok: true, json: () => Promise.resolve({}) });
    render(<DiRuntimeRulesRefresh useCaseId="uc" />);
    expand();
    expect(screen.getByText('Select an archive to continue')).toBeInTheDocument();
    await selectDecision();

    expect(screen.getByText('Ready to apply')).toBeInTheDocument();
    expect(screen.getByRole('radio', { name: 'svc 1.0' })).toBeChecked();

    fireEvent.click(screen.getByRole('button', { name: 'Clear' }));
    expect(screen.getByRole('radio', { name: 'svc 1.0' })).not.toBeChecked();
    expect(refreshButton()).toBeDisabled();
  });

  describe('READY TO APPLY disclosure', () => {
    const toggle = () => screen.getByRole('button', { name: /Ready to apply/ });
    const details = () => document.getElementById('rr-summary-details');

    test('stays collapsed when an archive is selected', async () => {
      installFetch({ ok: true, json: () => Promise.resolve({}) });
      render(<DiRuntimeRulesRefresh useCaseId="uc" />);
      expand();
      await selectDecision();

      expect(toggle()).toHaveAttribute('aria-expanded', 'false');
      expect(details()).toHaveAttribute('hidden');
      expect(toggle()).toHaveTextContent('svc 1.0'); // compact line still names the archive
    });

    test('can still be expanded and collapsed manually', async () => {
      installFetch({ ok: true, json: () => Promise.resolve({}) });
      render(<DiRuntimeRulesRefresh useCaseId="uc" />);
      expand();
      await selectDecision();

      fireEvent.click(toggle());
      expect(toggle()).toHaveAttribute('aria-expanded', 'true');
      expect(details()).not.toHaveAttribute('hidden');
      expect(details()).toHaveTextContent('proj/svc/1.0');

      fireEvent.click(toggle());
      expect(toggle()).toHaveAttribute('aria-expanded', 'false');
      expect(details()).toHaveAttribute('hidden');
    });

    test('keeps the user\'s choice when the selection is cleared and made again', async () => {
      installFetch({ ok: true, json: () => Promise.resolve({}) });
      render(<DiRuntimeRulesRefresh useCaseId="uc" />);
      expand();
      await selectDecision();
      fireEvent.click(toggle()); // user opens it

      fireEvent.click(screen.getByRole('button', { name: 'Clear' }));
      fireEvent.click(screen.getByRole('radio', { name: 'svc 1.0' }));
      expect(toggle()).toHaveAttribute('aria-expanded', 'true');

      fireEvent.click(toggle()); // user closes it, then re-selects: stays closed
      fireEvent.click(screen.getByRole('button', { name: 'Clear' }));
      fireEvent.click(screen.getByRole('radio', { name: 'svc 1.0' }));
      expect(toggle()).toHaveAttribute('aria-expanded', 'false');
    });
  });

  test('empty state when the runtime has no decisions', async () => {
    global.fetch = vi.fn((url) => {
      if (/\/decisions/.test(url)) return Promise.resolve({ ok: true, json: () => Promise.resolve([]) });
      return Promise.resolve({ ok: true, json: () => Promise.resolve(RUNTIMES) });
    });
    render(<DiRuntimeRulesRefresh useCaseId="uc" />);
    expand();
    await waitFor(() => expect(screen.getByRole('option', { name: 'Runtime One' })).toBeInTheDocument());
    fireEvent.change(screen.getByLabelText('Select DI runtime'), { target: { value: 'rt1' } });
    await screen.findByText("No version of this use case's decision service on this runtime");
  });

  describe('empty states are told apart by the services the BFF resolved', () => {
    const withBinding = (services, list) => {
      global.fetch = vi.fn((url) => {
        if (/\/decisions/.test(url)) {
          return Promise.resolve({
            ok: true,
            headers: { get: (h) => (h === 'X-Decision-Services' ? encodeURIComponent(JSON.stringify(services)) : null) },
            json: () => Promise.resolve(list),
          });
        }
        return Promise.resolve({ ok: true, json: () => Promise.resolve(RUNTIMES) });
      });
    };
    const openRuntime = async () => {
      render(<DiRuntimeRulesRefresh useCaseId="retail-agent" />);
      expand();
      await waitFor(() => expect(screen.getByRole('option', { name: 'Runtime One' })).toBeInTheDocument());
      fireEvent.change(screen.getByLabelText('Select DI runtime'), { target: { value: 'rt1' } });
    };

    test('bound service without versions: names the service that was looked for', async () => {
      withBinding(['RetailPromotionService'], []);
      await openRuntime();
      await screen.findByText("No version of this use case's decision service on this runtime");
      expect(screen.getByText('RetailPromotionService')).toBeInTheDocument();
      expect(screen.queryByText('This use case has no decision service binding')).not.toBeInTheDocument();
    });

    test('no binding at all: says so instead of "no versions"', async () => {
      withBinding([], []);
      await openRuntime();
      await screen.findByText('This use case has no decision service binding');
      expect(screen.queryByText("No version of this use case's decision service on this runtime")).not.toBeInTheDocument();
    });

    test('cards show service, version and build date from the BFF fields', async () => {
      withBinding(['RetailPromotionService'], [{
        id: 'cpmanager/banking_demo/retailpromotionservice/retailPromotionServiceDecisionService/1.6.0-2026-11-12T13:43:37.259Z',
        service: 'RetailPromotionService', version: '1.6.0', build: '2026-11-12T13:43:37.259Z',
        group: 'banking_demo', status: 'deployed',
      }]);
      await openRuntime();
      const group = await screen.findByRole('radiogroup', { name: 'Select decision' });
      expect(within(group).getByRole('radio', { name: 'RetailPromotionService 1.6.0' })).toBeInTheDocument();
      expect(within(group).getByText('v1.6.0')).toBeInTheDocument();
      expect(within(group).getByText('2026-11-12')).toBeInTheDocument();
    });
  });

  test('requests only the decision service of the use case (useCaseId on the listing)', async () => {
    installFetch({ ok: true, json: () => Promise.resolve({}) });
    render(<DiRuntimeRulesRefresh useCaseId="my-uc" />);
    expand();
    await selectDecision();
    const listing = global.fetch.mock.calls.map(([u]) => u).find(u => /\/decisions/.test(u));
    expect(listing).toContain('/api/ads-runtimes/rt1/decisions?useCaseId=my-uc');
    expect(listing).toContain('deploymentSpace=embedded');

    // changing the space reloads with the same use case filter
    fireEvent.change(screen.getByLabelText('Deployment space'), { target: { value: 'prod' } });
    fireEvent.click(screen.getByRole('button', { name: 'Load Decisions' }));
    await waitFor(() => {
      const urls = global.fetch.mock.calls.map(([u]) => u).filter(u => /\/decisions/.test(u));
      expect(urls[urls.length - 1]).toMatch(/useCaseId=my-uc&deploymentSpace=prod$/);
    });
  });

  test('shows the BFF refusal when the archive does not belong to the use case', async () => {
    installFetch({
      ok: false,
      statusText: 'Forbidden',
      json: () => Promise.resolve({ error: "Archive 'p/Other/1' does not belong to the Decision Service of use case 'uc'" }),
    });
    render(<DiRuntimeRulesRefresh useCaseId="uc" />);
    expand();
    await selectDecision();
    fireEvent.click(refreshButton());
    await screen.findByText(/Refresh failed: Archive 'p\/Other\/1' does not belong/);
  });

  test('load error offers a retry', async () => {
    let fail = true;
    global.fetch = vi.fn((url) => {
      if (/\/decisions/.test(url)) {
        return fail
          ? Promise.resolve({ ok: false, statusText: 'Bad', json: () => Promise.resolve({ error: 'down' }) })
          : Promise.resolve({ ok: true, json: () => Promise.resolve(DECISIONS) });
      }
      return Promise.resolve({ ok: true, json: () => Promise.resolve(RUNTIMES) });
    });
    render(<DiRuntimeRulesRefresh useCaseId="uc" />);
    expand();
    await waitFor(() => expect(screen.getByRole('option', { name: 'Runtime One' })).toBeInTheDocument());
    fireEvent.change(screen.getByLabelText('Select DI runtime'), { target: { value: 'rt1' } });
    await screen.findByText('Could not load decisions: down');

    fail = false;
    fireEvent.click(screen.getByRole('button', { name: 'Retry' }));
    await screen.findByRole('radio', { name: 'svc 1.0' });
  });

  test('filter appears for long lists and narrows the archives', async () => {
    const many = Array.from({ length: 8 }, (_, i) => ({ id: `proj/svc${i}/1.0`, name: `svc${i}  1.0`, status: 'deployed' }));
    global.fetch = vi.fn((url) => {
      if (/\/decisions/.test(url)) return Promise.resolve({ ok: true, json: () => Promise.resolve(many) });
      return Promise.resolve({ ok: true, json: () => Promise.resolve(RUNTIMES) });
    });
    render(<DiRuntimeRulesRefresh useCaseId="uc" />);
    expand();
    await waitFor(() => expect(screen.getByRole('option', { name: 'Runtime One' })).toBeInTheDocument());
    fireEvent.change(screen.getByLabelText('Select DI runtime'), { target: { value: 'rt1' } });
    const filter = await screen.findByLabelText('Filter archives');
    expect(screen.getAllByRole('radio')).toHaveLength(8);

    fireEvent.change(filter, { target: { value: 'svc3' } });
    expect(screen.getAllByRole('radio')).toHaveLength(1);
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

describe('active archive indicator and restore default', () => {
  const jar = (filename, service = '', version = '') => ({ filename, path: `samples/uc/${filename}`, service, version });
  const DEFAULT = jar('svc_1.0.jar', 'svc', '1.0');
  const CUSTOM = jar('svcDecisionService_2.0-2026-11-12T13_43_37.259Z.jar', 'svcDecisionService', '2.0');
  const TWO = [
    { id: 'proj/svc/1.0', name: 'svc  1.0', status: 'deployed', service: 'svc', version: '1.0', filename: 'svc_1.0.jar' },
    { id: 'proj/svc/2.0', name: 'svc  2.0', status: 'deployed', service: 'svc', version: '2.0',
      filename: 'svcDecisionService_2.0-2026-11-12T13_43_37.259Z.jar' },
  ];
  const withDecisions = (list) => {
    const base = global.fetch;
    global.fetch = vi.fn((url, init) => (/\/decisions/.test(url)
      ? Promise.resolve({ ok: true, json: () => Promise.resolve(list) })
      : base(url, init)));
  };
  const open = async () => {
    expand();
    await waitFor(() => expect(screen.getByRole('option', { name: 'Runtime One' })).toBeInTheDocument());
    fireEvent.change(screen.getByLabelText('Select DI runtime'), { target: { value: 'rt1' } });
    await screen.findByRole('radiogroup', { name: 'Select decision' });
  };
  const running = () => screen.queryByTestId('rr-running');

  test('shows nothing when the status is unavailable', async () => {
    installFetch({ ok: true, json: () => Promise.resolve({}) });
    render(<DiRuntimeRulesRefresh useCaseId="uc" />);
    await open();
    expect(running()).toBeNull();
    expect(screen.queryByRole('button', { name: 'Restore default' })).toBeNull();
  });

  test('the archive the job runs is highlighted and marked default, without a restore action', async () => {
    rulesStatus = { active: DEFAULT, default: DEFAULT, isDefault: true, inSync: true };
    installFetch({ ok: true, json: () => Promise.resolve({}) });
    render(<DiRuntimeRulesRefresh useCaseId="uc" />);
    await open();
    withDecisions(TWO);
    fireEvent.change(screen.getByLabelText('Select DI runtime'), { target: { value: '' } });
    fireEvent.change(screen.getByLabelText('Select DI runtime'), { target: { value: 'rt1' } });

    const card = (await screen.findByRole('radio', { name: 'svc 1.0 (running now)' })).closest('label');
    expect(card).toHaveClass('rr-option--active');
    expect(within(card).getByText('Running')).toBeInTheDocument();
    expect(within(card).getByText('Default')).toBeInTheDocument();
    expect(screen.getByRole('radio', { name: 'svc 2.0' }).closest('label')).not.toHaveClass('rr-option--active');
    expect(within(running()).getByText('svc_1.0.jar')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Restore default' })).toBeNull();
  });

  test('a custom archive is highlighted by its jar name; the default keeps its marker and a way back', async () => {
    rulesStatus = { active: CUSTOM, default: DEFAULT, isDefault: false, inSync: true };
    installFetch({ ok: true, json: () => Promise.resolve({}) });
    render(<DiRuntimeRulesRefresh useCaseId="uc" />);
    await open();
    withDecisions(TWO);
    fireEvent.change(screen.getByLabelText('Select DI runtime'), { target: { value: '' } });
    fireEvent.change(screen.getByLabelText('Select DI runtime'), { target: { value: 'rt1' } });

    const active = (await screen.findByRole('radio', { name: 'svc 2.0 (running now)' })).closest('label');
    expect(active).toHaveClass('rr-option--active');
    const dflt = screen.getByRole('radio', { name: 'svc 1.0' }).closest('label');
    expect(dflt).not.toHaveClass('rr-option--active');
    expect(within(dflt).getByText('Default')).toBeInTheDocument();
    expect(within(running()).getByText('Custom')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Restore default' })).toBeEnabled();
  });

  test('restoring the default moves the highlight back only after the job confirmed it', async () => {
    rulesStatus = { active: CUSTOM, default: DEFAULT, isDefault: false, inSync: true };
    installFetch({ ok: true, json: () => Promise.resolve({}) });
    render(<DiRuntimeRulesRefresh useCaseId="uc" />);
    await open();
    withDecisions(TWO);
    fireEvent.change(screen.getByLabelText('Select DI runtime'), { target: { value: '' } });
    fireEvent.change(screen.getByLabelText('Select DI runtime'), { target: { value: 'rt1' } });
    await screen.findByRole('radio', { name: 'svc 2.0 (running now)' });

    rulesStatus = { active: DEFAULT, default: DEFAULT, isDefault: true, inSync: true }; // what agents.json says afterwards
    fireEvent.click(screen.getByRole('button', { name: 'Restore default' }));

    await screen.findByText(/Default archive Svc_1\.0\.jar restored — confirmed by the job/);
    const call = global.fetch.mock.calls.find(([u]) => /restore-default/.test(u));
    expect(call[0]).toContain('/api/ads-runtimes/restore-default?useCaseId=uc');
    expect(call[1]).toEqual({ method: 'POST' });
    expect(await screen.findByRole('radio', { name: 'svc 1.0 (running now)' })).toBeInTheDocument();
    expect(screen.getByRole('radio', { name: 'svc 2.0' }).closest('label')).not.toHaveClass('rr-option--active');
    expect(screen.queryByRole('button', { name: 'Restore default' })).toBeNull();
  });

  test('a failed restore keeps the previous indicator and shows the error', async () => {
    rulesStatus = { active: CUSTOM, default: DEFAULT, isDefault: false, inSync: true };
    restoreResponse = { ok: false, statusText: 'Bad Gateway', json: () => Promise.resolve({ error: 'The job rejected the archive' }) };
    installFetch({ ok: true, json: () => Promise.resolve({}) });
    render(<DiRuntimeRulesRefresh useCaseId="uc" />);
    await open();
    withDecisions(TWO);
    fireEvent.change(screen.getByLabelText('Select DI runtime'), { target: { value: '' } });
    fireEvent.change(screen.getByLabelText('Select DI runtime'), { target: { value: 'rt1' } });
    await screen.findByRole('radio', { name: 'svc 2.0 (running now)' });

    fireEvent.click(screen.getByRole('button', { name: 'Restore default' }));

    await screen.findByText('Restore failed: The job rejected the archive');
    expect(screen.getByRole('radio', { name: 'svc 2.0 (running now)' })).toBeInTheDocument();
    expect(within(running()).getByText('Custom')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Restore default' })).toBeEnabled();
  });

  test('a successful refresh re-reads the status; a failed one does not move the indicator', async () => {
    rulesStatus = { active: DEFAULT, default: DEFAULT, isDefault: true, inSync: true };
    installFetch({ ok: false, statusText: 'Gateway Timeout', json: () => Promise.resolve({ error: 'no answer from: a' }) });
    render(<DiRuntimeRulesRefresh useCaseId="uc" />);
    await open();
    withDecisions(TWO);
    fireEvent.change(screen.getByLabelText('Select DI runtime'), { target: { value: '' } });
    fireEvent.change(screen.getByLabelText('Select DI runtime'), { target: { value: 'rt1' } });
    fireEvent.click(await screen.findByRole('radio', { name: 'svc 2.0' }));

    const statusCalls = () => global.fetch.mock.calls.filter(([u]) => /rules-status/.test(u)).length;
    const before = statusCalls();
    fireEvent.click(refreshButton());
    await screen.findByText('Refresh failed: no answer from: a');

    expect(statusCalls()).toBe(before); // nothing re-read, nothing changed
    expect(screen.getByRole('radio', { name: 'svc 1.0 (running now)' })).toBeInTheDocument();
    expect(screen.getByRole('radio', { name: 'svc 2.0' }).closest('label')).not.toHaveClass('rr-option--active');
  });

  test('after a confirmed refresh the highlight follows the loaded archive', async () => {
    rulesStatus = { active: DEFAULT, default: DEFAULT, isDefault: true, inSync: true };
    installFetch({ ok: true, json: () => Promise.resolve({ filename: CUSTOM.filename, topics: ['uc-control'] }) });
    render(<DiRuntimeRulesRefresh useCaseId="uc" />);
    await open();
    withDecisions(TWO);
    fireEvent.change(screen.getByLabelText('Select DI runtime'), { target: { value: '' } });
    fireEvent.change(screen.getByLabelText('Select DI runtime'), { target: { value: 'rt1' } });
    fireEvent.click(await screen.findByRole('radio', { name: 'svc 2.0' }));

    rulesStatus = { active: CUSTOM, default: DEFAULT, isDefault: false, inSync: true };
    fireEvent.click(refreshButton());

    await screen.findByText(/is now running — confirmed by the job/);
    expect(await screen.findByRole('radio', { name: 'svc 2.0 (running now)' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Restore default' })).toBeInTheDocument();
  });

  test('does not highlight when the running jar matches several listed archives', async () => {
    rulesStatus = { active: DEFAULT, default: DEFAULT, isDefault: true, inSync: true };
    installFetch({ ok: true, json: () => Promise.resolve({}) });
    render(<DiRuntimeRulesRefresh useCaseId="uc" />);
    await open();
    withDecisions([
      { id: 'a/svc/1.0', name: 'x', status: 'deployed', service: 'svc', version: '1.0', filename: 'a.jar' },
      { id: 'b/svc/1.0', name: 'y', status: 'deployed', service: 'svc', version: '1.0', filename: 'b.jar' },
    ]);
    fireEvent.change(screen.getByLabelText('Select DI runtime'), { target: { value: '' } });
    fireEvent.change(screen.getByLabelText('Select DI runtime'), { target: { value: 'rt1' } });
    await screen.findAllByRole('radio');
    expect(screen.queryByText('Running')).toBeNull();
    expect(within(running()).getByText('svc_1.0.jar')).toBeInTheDocument(); // still reported in the banner
  });

  test('warns when the agents of the use case disagree', async () => {
    rulesStatus = { active: DEFAULT, default: DEFAULT, isDefault: true, inSync: false };
    installFetch({ ok: true, json: () => Promise.resolve({}) });
    render(<DiRuntimeRulesRefresh useCaseId="uc" />);
    expand();
    expect(await screen.findByText(/do not use the same archive/)).toBeInTheDocument();
  });
});
