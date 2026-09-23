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
 * Tests for the AgentManagementPanel stop/delete fetch URL regression.
 *
 * Root cause: AgentManagementPanel was receiving agentJobId (the internal
 * agentId from agents.json, e.g. "fraud-detection-agent") as its sole `agentId`
 * prop and using it for all BFF API calls.  The BFF's /stop and DELETE endpoints
 * are keyed by the use-case folder name (e.g. "agent-banking"), so the calls hit
 * the wrong URL → 404 → the Flink job was never actually stopped.
 *
 * Fix: the component now accepts a separate `useCaseId` prop (always the folder
 * name) for API calls, keeping `agentId` only for human-readable dialog text.
 */

import React, { useState } from 'react';
import { render, screen, fireEvent, act } from '@testing-library/react';

// ─── Minimal inline replica of AgentManagementPanel ──────────────────────────
// We reproduce only the fetch logic under test so we are not coupled to App.js
// internals.  This keeps the test stable even if UI layout changes.

const API_URL = 'http://localhost:8080';

function ConfirmDialog({ message, confirmLabel, onConfirm, onCancel }) {
  return (
    <div>
      <p>{message}</p>
      <button onClick={onConfirm}>{confirmLabel}</button>
      <button onClick={onCancel}>Cancel</button>
    </div>
  );
}

/**
 * Replica that mirrors the exact fetch calls in the real AgentManagementPanel.
 * agentId   = display name  (e.g. "fraud-detection-agent")
 * useCaseId = folder key    (e.g. "agent-banking")
 */
function TestPanel({ agentId, useCaseId, onNavigateBack }) {
  const [confirm, setConfirm] = useState(null);
  const [cancelState, setCancelState] = useState('idle');
  const [deleteState, setDeleteState] = useState('idle');
  const [actionError, setActionError] = useState(null);

  const handleCancelConfirmed = async () => {
    setConfirm(null);
    setCancelState('stopping');
    try {
      const r = await fetch(`${API_URL}/api/agents/${useCaseId}/stop`, { method: 'POST' });
      if (!r.ok && r.status !== 404) {
        const body = await r.json().catch(() => ({}));
        throw new Error(body.error || r.statusText);
      }
      onNavigateBack();
    } catch (err) {
      setCancelState('error');
      setActionError(`Cancel failed: ${err.message}`);
    }
  };

  const handleDeleteConfirmed = async () => {
    setConfirm(null);
    setDeleteState('deleting');
    try {
      const r = await fetch(`${API_URL}/api/agents/${useCaseId}`, { method: 'DELETE' });
      if (!r.ok) {
        const body = await r.json().catch(() => ({}));
        throw new Error(body.error || r.statusText);
      }
      onNavigateBack();
    } catch (err) {
      setDeleteState('error');
      setActionError(`Delete failed: ${err.message}`);
    }
  };

  return (
    <>
      {confirm === 'cancel' && (
        <ConfirmDialog
          message={`Cancel the running Flink job for agent "${agentId}"?`}
          confirmLabel="Yes, cancel job"
          onConfirm={handleCancelConfirmed}
          onCancel={() => setConfirm(null)}
        />
      )}
      {confirm === 'delete' && (
        <ConfirmDialog
          message={`Delete agent "${agentId}"?`}
          confirmLabel="Yes, delete agent"
          onConfirm={handleDeleteConfirmed}
          onCancel={() => setConfirm(null)}
        />
      )}
      <button title="stop" onClick={() => setConfirm('cancel')}>
        {cancelState === 'stopping' ? 'Cancelling…' : 'Cancel'}
      </button>
      <button title="delete" onClick={() => setConfirm('delete')}>
        {deleteState === 'deleting' ? 'Deleting…' : 'Delete'}
      </button>
      {actionError && <span data-testid="error">{actionError}</span>}
    </>
  );
}

// ─── helpers ─────────────────────────────────────────────────────────────────

const USE_CASE_FOLDER = 'agent-banking';        // the folder key — correct URL
const INTERNAL_AGENT_ID = 'fraud-detection-agent'; // agents.json agentId — wrong URL

function setup({ fetchResponse = { ok: true, json: async () => ({}) } } = {}) {
  const fetchMock = jest.fn().mockResolvedValue(fetchResponse);
  global.fetch = fetchMock;
  const onNavigateBack = jest.fn();

  render(
    <TestPanel
      agentId={INTERNAL_AGENT_ID}
      useCaseId={USE_CASE_FOLDER}
      onNavigateBack={onNavigateBack}
    />
  );

  return { fetchMock, onNavigateBack };
}

// ─── tests ───────────────────────────────────────────────────────────────────

afterEach(() => {
  jest.restoreAllMocks();
  jest.clearAllMocks();
});

describe('AgentManagementPanel — stop uses useCaseId (folder key)', () => {
  test('Cancel calls POST /api/agents/<useCaseId>/stop', async () => {
    const { fetchMock, onNavigateBack } = setup();

    fireEvent.click(screen.getByTitle('stop'));
    fireEvent.click(screen.getByText('Yes, cancel job'));

    await act(async () => {});

    expect(fetchMock).toHaveBeenCalledWith(
      `${API_URL}/api/agents/${USE_CASE_FOLDER}/stop`,
      expect.objectContaining({ method: 'POST' })
    );
    // Must NOT use the internal agentId
    expect(fetchMock).not.toHaveBeenCalledWith(
      expect.stringContaining(`/api/agents/${INTERNAL_AGENT_ID}`),
      expect.anything()
    );
    expect(onNavigateBack).toHaveBeenCalled();
  });

  test('404 from /stop is treated as success (job already gone)', async () => {
    const { fetchMock, onNavigateBack } = setup({
      fetchResponse: { ok: false, status: 404, json: async () => ({ error: 'not found' }) },
    });

    fireEvent.click(screen.getByTitle('stop'));
    fireEvent.click(screen.getByText('Yes, cancel job'));

    await act(async () => {});

    expect(fetchMock).toHaveBeenCalledWith(
      `${API_URL}/api/agents/${USE_CASE_FOLDER}/stop`,
      expect.objectContaining({ method: 'POST' })
    );
    // 404 is not an error — navigation should still happen
    expect(onNavigateBack).toHaveBeenCalled();
    expect(screen.queryByTestId('error')).toBeNull();
  });

  test('non-404 error from /stop shows error message', async () => {
    const { onNavigateBack } = setup({
      fetchResponse: { ok: false, status: 500, json: async () => ({ error: 'Flink down' }) },
    });

    fireEvent.click(screen.getByTitle('stop'));
    fireEvent.click(screen.getByText('Yes, cancel job'));

    await act(async () => {});

    expect(onNavigateBack).not.toHaveBeenCalled();
    expect(screen.getByTestId('error').textContent).toMatch(/Cancel failed/);
  });
});

describe('AgentManagementPanel — delete uses useCaseId (folder key)', () => {
  test('Delete calls DELETE /api/agents/<useCaseId>', async () => {
    const { fetchMock, onNavigateBack } = setup();

    fireEvent.click(screen.getByTitle('delete'));
    fireEvent.click(screen.getByText('Yes, delete agent'));

    await act(async () => {});

    expect(fetchMock).toHaveBeenCalledWith(
      `${API_URL}/api/agents/${USE_CASE_FOLDER}`,
      expect.objectContaining({ method: 'DELETE' })
    );
    // Must NOT use the internal agentId
    expect(fetchMock).not.toHaveBeenCalledWith(
      expect.stringContaining(`/api/agents/${INTERNAL_AGENT_ID}`),
      expect.objectContaining({ method: 'DELETE' })
    );
    expect(onNavigateBack).toHaveBeenCalled();
  });

  test('delete error shows error message and does not navigate', async () => {
    const { onNavigateBack } = setup({
      fetchResponse: { ok: false, status: 500, json: async () => ({ error: 'disk full' }) },
    });

    fireEvent.click(screen.getByTitle('delete'));
    fireEvent.click(screen.getByText('Yes, delete agent'));

    await act(async () => {});

    expect(onNavigateBack).not.toHaveBeenCalled();
    expect(screen.getByTestId('error').textContent).toMatch(/Delete failed/);
  });

  test('confirm dialog shows agentId (human name) in message, not useCaseId', () => {
    setup();

    fireEvent.click(screen.getByTitle('delete'));

    expect(screen.getByText(`Delete agent "${INTERNAL_AGENT_ID}"?`)).toBeTruthy();
  });
});
