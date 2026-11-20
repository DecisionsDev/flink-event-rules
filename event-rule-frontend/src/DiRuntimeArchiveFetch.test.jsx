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
import DiRuntimeArchiveFetch from './DiRuntimeArchiveFetch';

const RUNTIMES = [{ id: 'rt1', name: 'Runtime One', deploymentSpace: 'embedded' }];
const DECISIONS = [
  { id: 'proj/svc/1.0', name: 'svc  1.0', status: 'deployed', service: 'svc', version: '1.0', build: '2026-01-02T10:00:00Z', group: 'proj', filename: 'svc_1.0-2026.jar' },
  { id: 'proj/svc/2.0', name: 'svc  2.0', status: 'deployed', service: 'svc', version: '2.0', group: 'proj', filename: 'svc_2.0.jar' },
];
const ZIP = new Blob([new Uint8Array([0x50, 0x4b, 0x03, 0x04, 9])]);

function installFetch({ runtimes, decisions, archive } = {}) {
  global.fetch = vi.fn((url) => {
    if (/\/archive\?/.test(url)) return archive ? archive(url) : Promise.resolve({ ok: true, headers: { get: () => 'svc_1.0.jar' }, blob: async () => ZIP });
    if (/\/decisions/.test(url)) return decisions ? decisions() : Promise.resolve({ ok: true, json: async () => DECISIONS });
    if (/\/api\/ads-runtimes$/.test(url)) return runtimes ? runtimes() : Promise.resolve({ ok: true, json: async () => RUNTIMES });
    return Promise.resolve({ ok: true, json: async () => ({}) });
  });
}

const open = () => fireEvent.click(screen.getByRole('button', { name: /Fetch from DI Runtime/i }));
const pickRuntime = async () => {
  fireEvent.change(await screen.findByLabelText('Select DI runtime'), { target: { value: 'rt1' } });
};
const pick = async (name) => {
  const group = await screen.findByRole('radiogroup', { name: 'Select decision' });
  fireEvent.click(within(group).getByRole('radio', { name }));
};
const fetchButton = () => screen.getByRole('button', { name: /Fetch Archive|Fetching/ });

afterEach(() => vi.restoreAllMocks());

describe('DiRuntimeArchiveFetch', () => {
  test('initial state: closed, then a guided empty panel with a disabled primary action', async () => {
    installFetch();
    render(<DiRuntimeArchiveFetch onFetched={vi.fn()} />);
    expect(screen.queryByLabelText('Select DI runtime')).toBeNull();
    open();
    expect(screen.getByRole('button', { name: /Fetch from DI Runtime/i })).toHaveAttribute('aria-expanded', 'true');
    await screen.findByLabelText('Select DI runtime');
    expect(screen.getByText('Select a runtime to browse archives')).toBeInTheDocument();
    expect(screen.getByText('Select an archive to continue')).toBeInTheDocument();
    expect(fetchButton()).toBeDisabled();
  });

  test('lists archives as cards with metadata, selection is summarised and fetching succeeds', async () => {
    installFetch();
    const onFetched = vi.fn();
    render(<DiRuntimeArchiveFetch onFetched={onFetched} />);
    open();
    await pickRuntime();
    expect(await screen.findByText('2 available')).toBeInTheDocument();
    expect(screen.getByText('v1.0')).toBeInTheDocument();
    expect(screen.getByText('2026-01-02')).toBeInTheDocument();
    await pick('svc 2.0');
    expect(screen.getByText('Ready to fetch')).toBeInTheDocument();
    expect(fetchButton()).toBeEnabled();
    fireEvent.click(fetchButton());
    expect(await screen.findByText(/Archive fetched \(svc_1\.0\.jar\)/)).toBeInTheDocument();
    expect(onFetched).toHaveBeenCalledTimes(1);
    expect(onFetched.mock.calls[0][0].name).toBe('svc_1.0.jar');
  });

  test('tags the archive matching the current jar and selects nothing for it', async () => {
    installFetch();
    render(<DiRuntimeArchiveFetch currentJar="samples/x/svc_2.0.jar" onFetched={vi.fn()} />);
    open();
    await pickRuntime();
    const group = await screen.findByRole('radiogroup', { name: 'Select decision' });
    const current = within(group).getByRole('radio', { name: 'svc 2.0 (current)' });
    expect(current).not.toBeChecked();
    expect(within(group).getByText('Current')).toBeInTheDocument();
  });

  test('Clear removes the selection', async () => {
    installFetch();
    render(<DiRuntimeArchiveFetch onFetched={vi.fn()} />);
    open();
    await pickRuntime();
    await pick('svc 1.0');
    fireEvent.click(screen.getByRole('button', { name: 'Clear' }));
    expect(screen.getByText('Select an archive to continue')).toBeInTheDocument();
    expect(fetchButton()).toBeDisabled();
  });

  test('shows a loading placeholder while archives are loading', async () => {
    let release;
    installFetch({ decisions: () => new Promise(r => { release = () => r({ ok: true, json: async () => DECISIONS }); }) });
    render(<DiRuntimeArchiveFetch onFetched={vi.fn()} />);
    open();
    await pickRuntime();
    expect(await screen.findByText('Loading archives…')).toBeInTheDocument();
    release();
    expect(await screen.findByText('2 available')).toBeInTheDocument();
  });

  test('empty list: explains that no archive is deployed', async () => {
    installFetch({ decisions: () => Promise.resolve({ ok: true, json: async () => [] }) });
    render(<DiRuntimeArchiveFetch onFetched={vi.fn()} />);
    open();
    await pickRuntime();
    expect(await screen.findByText('No decision service archive on this runtime')).toBeInTheDocument();
  });

  test('no runtime configured', async () => {
    installFetch({ runtimes: () => Promise.resolve({ ok: true, json: async () => [] }) });
    render(<DiRuntimeArchiveFetch onFetched={vi.fn()} />);
    open();
    expect(await screen.findByText(/No DI runtimes configured/)).toBeInTheDocument();
  });

  test('runtime list failure can be retried', async () => {
    let calls = 0;
    installFetch({
      runtimes: () => (++calls === 1
        ? Promise.resolve({ ok: false, statusText: 'x', json: async () => ({ error: 'runtime down' }) })
        : Promise.resolve({ ok: true, json: async () => RUNTIMES })),
    });
    render(<DiRuntimeArchiveFetch onFetched={vi.fn()} />);
    open();
    expect(await screen.findByRole('alert')).toHaveTextContent('Could not load runtimes: runtime down');
    fireEvent.click(screen.getByRole('button', { name: 'Retry' }));
    expect(await screen.findByLabelText('Select DI runtime')).toBeInTheDocument();
  });

  test('archive list failure can be retried', async () => {
    let calls = 0;
    installFetch({
      decisions: () => (++calls === 1
        ? Promise.resolve({ ok: false, statusText: 'x', json: async () => ({ error: 'boom' }) })
        : Promise.resolve({ ok: true, json: async () => DECISIONS })),
    });
    render(<DiRuntimeArchiveFetch onFetched={vi.fn()} />);
    open();
    await pickRuntime();
    expect(await screen.findByText(/Could not load decisions: boom/)).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Retry' }));
    expect(await screen.findByText('2 available')).toBeInTheDocument();
  });

  test('fetch failure is reported and nothing is handed over', async () => {
    installFetch({ archive: () => Promise.resolve({ ok: false, statusText: 'x', json: async () => ({ error: 'gone' }) }) });
    const onFetched = vi.fn();
    render(<DiRuntimeArchiveFetch onFetched={onFetched} />);
    open();
    await pickRuntime();
    await pick('svc 1.0');
    fireEvent.click(fetchButton());
    expect(await screen.findByRole('alert')).toHaveTextContent('Fetch failed: gone');
    expect(onFetched).not.toHaveBeenCalled();
    expect(fetchButton()).toBeEnabled();
  });

  test('an invalid archive is rejected', async () => {
    installFetch({ archive: () => Promise.resolve({ ok: true, headers: { get: () => 'svc_1.0.jar' }, blob: async () => new Blob(['<html>']) }) });
    const onFetched = vi.fn();
    render(<DiRuntimeArchiveFetch onFetched={onFetched} />);
    open();
    await pickRuntime();
    await pick('svc 1.0');
    fireEvent.click(fetchButton());
    expect(await screen.findByRole('alert')).toHaveTextContent(/Invalid archive.*not a valid Java archive/);
    expect(onFetched).not.toHaveBeenCalled();
  });

  test('a non-jar file name is rejected', async () => {
    installFetch({ archive: () => Promise.resolve({ ok: true, headers: { get: () => 'rules.zip' }, blob: async () => ZIP }) });
    const onFetched = vi.fn();
    render(<DiRuntimeArchiveFetch onFetched={onFetched} />);
    open();
    await pickRuntime();
    await pick('svc 1.0');
    fireEvent.click(fetchButton());
    expect(await screen.findByRole('alert')).toHaveTextContent(/not a \.jar file/);
    expect(onFetched).not.toHaveBeenCalled();
  });

  test('shows progress while fetching and Cancel aborts the download', async () => {
    installFetch({
      archive: (url) => new Promise((_, reject) => {
        // the component passes the abort signal as 2nd argument of fetch
        const signal = global.fetch.mock.calls.find(c => c[0] === url)[1].signal;
        signal.addEventListener('abort', () => reject(new DOMException('aborted', 'AbortError')));
      }),
    });
    const onFetched = vi.fn();
    render(<DiRuntimeArchiveFetch onFetched={onFetched} />);
    open();
    await pickRuntime();
    await pick('svc 1.0');
    fireEvent.click(fetchButton());
    expect(await screen.findByText(/Downloading svc 1\.0 from the runtime/)).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Fetching…' })).toBeDisabled();
    fireEvent.click(screen.getByRole('button', { name: 'Cancel' }));
    expect(await screen.findByText(/Fetch cancelled/)).toBeInTheDocument();
    expect(onFetched).not.toHaveBeenCalled();
    expect(fetchButton()).toBeEnabled();
  });

  test('Cancel on an idle panel closes it and discards the selection', async () => {
    installFetch();
    render(<DiRuntimeArchiveFetch onFetched={vi.fn()} />);
    open();
    await pickRuntime();
    await pick('svc 1.0');
    fireEvent.click(screen.getByRole('button', { name: 'Cancel' }));
    await waitFor(() => expect(screen.queryByLabelText('Select DI runtime')).toBeNull());
    open();
    expect(await screen.findByLabelText('Select DI runtime')).toHaveValue('');
  });
});
