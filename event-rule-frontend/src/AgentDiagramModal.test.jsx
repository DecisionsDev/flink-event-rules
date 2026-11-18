// @vitest-environment jsdom
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
import fs from 'node:fs';
import path from 'node:path';
import React from 'react';
import { describe, test, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent, cleanup } from '@testing-library/react';
import AgentDiagramModal from './AgentDiagramModal';

const sample = (name) => JSON.parse(fs.readFileSync(
  path.resolve(__dirname, `../../samples/${name}/agents.json`), 'utf8'));

describe('AgentDiagramModal', () => {
  beforeEach(() => {
    globalThis.fetch = vi.fn(() => Promise.resolve({ ok: false, json: () => Promise.resolve({}) }));
  });
  afterEach(() => { cleanup(); vi.restoreAllMocks(); });

  test('shows a summary of the architecture and the view controls', () => {
    render(<AgentDiagramModal agentsJson={sample('airtraffic-agent')} useCaseId="airtraffic-agent" onClose={() => {}} />);
    expect(screen.getByRole('dialog', { name: 'Agent Diagram View' })).toBeInTheDocument();
    expect(screen.getByText(/2 agents · 7 topics/)).toBeInTheDocument();
    expect(screen.getByText(/1 chained/)).toBeInTheDocument();
    for (const name of ['Search the diagram', 'Zoom in', 'Zoom out', 'Fit to screen', 'Show minimap', 'Animate event flow']) {
      expect(screen.getAllByLabelText(name).length).toBeGreaterThan(0);
    }
  });

  test('hiding alerts and errors updates the summary', () => {
    render(<AgentDiagramModal agentsJson={sample('airtraffic-agent')} useCaseId="airtraffic-agent" onClose={() => {}} />);
    const alerts = screen.getByRole('button', { name: 'Show alert outputs' });
    expect(alerts).toHaveAttribute('aria-pressed', 'true');
    fireEvent.click(alerts);
    fireEvent.click(screen.getByRole('button', { name: 'Show error outputs' }));
    expect(alerts).toHaveAttribute('aria-pressed', 'false');
    expect(screen.getByText(/2 agents · 5 topics/)).toBeInTheDocument();
  });

  test('search reports matches and no-match state', () => {
    render(<AgentDiagramModal agentsJson={sample('airtraffic-agent')} useCaseId="airtraffic-agent" onClose={() => {}} />);
    const box = screen.getByLabelText('Search the diagram');
    fireEvent.change(box, { target: { value: 'airport init' } });
    expect(screen.getByText(/\d+ matches?/)).toBeInTheDocument();
    fireEvent.change(box, { target: { value: 'zzz-nothing' } });
    expect(screen.getByText('0 matches')).toBeInTheDocument();
    expect(screen.getByRole('status')).toHaveTextContent('Nothing matches');
  });

  test('Escape closes the modal; clicking the overlay closes it too', () => {
    const onClose = vi.fn();
    const { container } = render(<AgentDiagramModal agentsJson={sample('banking-fraud-agent')} useCaseId="u" onClose={onClose} />);
    fireEvent.keyDown(document, { key: 'Escape' });
    expect(onClose).toHaveBeenCalledTimes(1);
    fireEvent.click(container.querySelector('.agent-diagram-overlay'));
    expect(onClose).toHaveBeenCalledTimes(2);
    fireEvent.click(screen.getByLabelText('Close diagram'));
    expect(onClose).toHaveBeenCalledTimes(3);
  });

  test('renders an empty state without agents', () => {
    render(<AgentDiagramModal agentsJson={{ agents: [] }} useCaseId="u" onClose={() => {}} />);
    expect(screen.getByText('No agent data available.')).toBeInTheDocument();
  });

  describe('card interactions', () => {
    const agentsJson = sample('airtraffic-agent');
    const renderModal = () => render(<AgentDiagramModal agentsJson={agentsJson} useCaseId="airtraffic-agent" onClose={() => {}} />);

    test('React Flow node wrappers accept pointer events so card buttons are clickable', () => {
      const { container } = renderModal();
      const wrappers = container.querySelectorAll('.react-flow__node');
      expect(wrappers.length).toBeGreaterThan(0);
      wrappers.forEach(w => expect(w.style.pointerEvents).toBe('all'));
    });

    test('card toggle expands/collapses only that card and stays in sync with the Operations button', () => {
      renderModal();
      const toggles = screen.getAllByRole('button', { name: /(Expand|Collapse) decision operations of/ });
      expect(toggles.length).toBeGreaterThan(1);
      const all = screen.getByRole('button', { name: /(Expand|Collapse) all agents/ });
      expect(all).toHaveAttribute('aria-pressed', 'false');

      fireEvent.click(toggles[0]);
      expect(screen.getAllByRole('button', { name: /Collapse decision operations of/ })).toHaveLength(1);
      expect(screen.getAllByText('Decision operations')).toHaveLength(1);
      expect(all).toHaveAttribute('aria-pressed', 'false');

      // Expanding the remaining cards one by one flips the toolbar button to "all expanded".
      screen.getAllByRole('button', { name: /Expand decision operations of/ }).forEach(b => fireEvent.click(b));
      expect(screen.getByRole('button', { name: 'Collapse all agents' })).toHaveAttribute('aria-pressed', 'true');

      fireEvent.click(screen.getAllByRole('button', { name: /Collapse decision operations of/ })[0]);
      expect(screen.getByRole('button', { name: 'Expand all agents' })).toHaveAttribute('aria-pressed', 'false');

      // The toolbar button drives the very same state.
      fireEvent.click(screen.getByRole('button', { name: 'Expand all agents' }));
      expect(screen.queryAllByRole('button', { name: /Expand decision operations of/ })).toHaveLength(0);
      fireEvent.click(screen.getByRole('button', { name: 'Collapse all agents' }));
      expect(screen.queryAllByRole('button', { name: /Collapse decision operations of/ })).toHaveLength(0);
    });

    test('clicking an entity chip loads entity-change-log state and lets the user pick between entities', async () => {
      const entities = [
        { entityId: 'AC-1', entityData: { callsign: 'first' }, timestamp: 2 },
        { entityId: 'AC-2', entityData: { callsign: 'second' }, timestamp: 1 },
      ];
      globalThis.fetch = vi.fn(() => Promise.resolve({ ok: true, json: () => Promise.resolve({ entities }) }));
      renderModal();
      const chip = screen.getAllByTitle(/^Inspect .* entity state$/)[0];
      fireEvent.click(chip);

      expect(await screen.findByRole('complementary', { name: 'Selection detail' })).toBeInTheDocument();
      expect(await screen.findByText(/"first"/)).toBeInTheDocument();
      expect(globalThis.fetch.mock.calls.some(([url]) => /\/entity-state\?agentId=/.test(url) && !/summary=true/.test(url))).toBe(true);

      fireEvent.click(screen.getByRole('button', { name: 'AC-2' }));
      expect(await screen.findByText(/"second"/)).toBeInTheDocument();
      expect(screen.queryByText(/"first"/)).not.toBeInTheDocument();
    });
  });
});
