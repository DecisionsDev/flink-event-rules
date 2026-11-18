/**
 * Tests for AgentScenariosPanel component.
 */

import React from 'react';
import { describe, test, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import AgentScenariosPanel from './AgentScenariosPanel.jsx';

describe('AgentScenariosPanel', () => {
  const originalFetch = globalThis.fetch;

  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    globalThis.fetch = originalFetch;
  });

  const sampleAgents = [
    {
      id: 'banking-fraud-agent',
      name: 'Banking Fraud Agent',
      description: 'Real-time banking fraud detection',
      lastModified: Date.now(),
      agents: [
        { agentId: 'banking-fraud', name: 'Banking Fraud Agent' },
      ],
    },
  ];

  test('surfaces detailed error message when deployment fails synchronously', async () => {
    globalThis.fetch = vi.fn().mockImplementation((url, opts) => {
      if (url.includes('/api/agents/banking-fraud-agent/status')) {
        return Promise.resolve({
          ok: true,
          json: () => Promise.resolve({ agentId: 'banking-fraud-agent', state: 'NOT_STARTED' }),
        });
      }
      if (url.endsWith('/api/agents')) {
        return Promise.resolve({
          ok: true,
          json: () => Promise.resolve(sampleAgents),
        });
      }
      if (url.includes('/api/agents/banking-fraud-agent/start') && opts?.method === 'POST') {
        return Promise.resolve({
          ok: false,
          status: 400,
          json: () => Promise.resolve({
            error: "Agent descriptor validation failed: Agent 'banking-fraud': Decision Archive Control Topic (ruleset.archiveControlTopic) is required",
          }),
        });
      }
      return Promise.reject(new Error(`Unhandled fetch: ${url}`));
    });

    render(<AgentScenariosPanel onSelectUseCase={vi.fn()} />);

    // Wait for agent card to appear and finish probing
    const deployBtn = await screen.findByRole('button', { name: /Deploy Banking Fraud Agent/i });
    await waitFor(() => expect(deployBtn).not.toBeDisabled());

    // Click Deploy
    fireEvent.click(deployBtn);

    // Error message must be rendered
    const errMsg = await screen.findByText(/Decision Archive Control Topic \(ruleset\.archiveControlTopic\) is required/i);
    expect(errMsg).toBeInTheDocument();

    // The deploy button should now show "Retry Deploy" and not be hidden
    const retryBtn = await screen.findByRole('button', { name: /Retry Deploy/i });
    expect(retryBtn).toBeInTheDocument();
  });

  test('surfaces failureReason when Flink status transitions to FAILED', async () => {
    let callCount = 0;
    globalThis.fetch = vi.fn().mockImplementation((url, opts) => {
      if (url.includes('/api/agents/banking-fraud-agent/status')) {
        callCount++;
        if (callCount === 1) {
          return Promise.resolve({
            ok: true,
            json: () => Promise.resolve({ agentId: 'banking-fraud-agent', state: 'NOT_STARTED' }),
          });
        }
        return Promise.resolve({
          ok: true,
          json: () => Promise.resolve({
            agentId: 'banking-fraud-agent',
            jobId: 'job-1234',
            state: 'FAILED',
            failureReason: 'UnknownTopicOrPartitionException: Topic agent-banking-archive-control not found',
          }),
        });
      }
      if (url.endsWith('/api/agents')) {
        return Promise.resolve({
          ok: true,
          json: () => Promise.resolve(sampleAgents),
        });
      }
      if (url.includes('/api/agents/banking-fraud-agent/start') && opts?.method === 'POST') {
        return Promise.resolve({
          ok: true,
          status: 202,
          json: () => Promise.resolve({ started: true, jobId: 'job-1234', runtime: 'datastream' }),
        });
      }
      return Promise.reject(new Error(`Unhandled fetch: ${url}`));
    });

    render(<AgentScenariosPanel onSelectUseCase={vi.fn()} />);

    const deployBtn = await screen.findByRole('button', { name: /Deploy Banking Fraud Agent/i });
    await waitFor(() => expect(deployBtn).not.toBeDisabled());
    fireEvent.click(deployBtn);

    // Polling will detect FAILED state and show failureReason
    const errMsg = await screen.findByText(/Job failed: UnknownTopicOrPartitionException: Topic agent-banking-archive-control not found/i, {}, { timeout: 5000 });
    expect(errMsg).toBeInTheDocument();
  });

  test('allows undeploy/reset when job is in error state', async () => {
    let stopped = false;
    globalThis.fetch = vi.fn().mockImplementation((url, opts) => {
      if (url.includes('/api/agents/banking-fraud-agent/status')) {
        return Promise.resolve({
          ok: true,
          json: () => Promise.resolve({
            agentId: 'banking-fraud-agent',
            state: stopped ? 'NOT_STARTED' : 'FAILED',
            failureReason: 'Deployment startup failed',
          }),
        });
      }
      if (url.endsWith('/api/agents')) {
        return Promise.resolve({
          ok: true,
          json: () => Promise.resolve(sampleAgents),
        });
      }
      if (url.includes('/api/agents/banking-fraud-agent/start') && opts?.method === 'POST') {
        return Promise.resolve({
          ok: false,
          status: 500,
          json: () => Promise.resolve({ error: 'Flink submit failed: Fatal error' }),
        });
      }
      if (url.includes('/api/agents/banking-fraud-agent/stop') && opts?.method === 'POST') {
        stopped = true;
        return Promise.resolve({
          ok: true,
          json: () => Promise.resolve({ stopped: true }),
        });
      }
      return Promise.reject(new Error(`Unhandled fetch: ${url}`));
    });

    render(<AgentScenariosPanel onSelectUseCase={vi.fn()} />);

    // Trigger deployment error
    const deployBtn = await screen.findByRole('button', { name: /Deploy Banking Fraud Agent/i });
    await waitFor(() => expect(deployBtn).not.toBeDisabled());
    fireEvent.click(deployBtn);

    await screen.findByText(/Flink submit failed: Fatal error/i);

    // Open options dropdown menu
    const optionsBtn = screen.getByRole('button', { name: /Options/i });
    fireEvent.click(optionsBtn);

    // Undeploy button should be enabled even when failed
    const undeployMenuItem = screen.getByRole('menuitem', { name: /Undeploy Job/i });
    expect(undeployMenuItem).not.toBeDisabled();
    fireEvent.click(undeployMenuItem);

    // Confirm modal should appear
    const confirmBtn = await screen.findByRole('button', { name: /^Undeploy$/i });
    fireEvent.click(confirmBtn);

    // After stop, card resets to idle and Deploy button is back to "Deploy"
    await waitFor(() => {
      expect(screen.queryByText(/Flink submit failed: Fatal error/i)).not.toBeInTheDocument();
    });
  });
});
