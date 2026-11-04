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
import { render, screen, fireEvent, waitFor, cleanup } from '@testing-library/react';
import SqlViewModal, { tokenizeSqlLine, groupStatements } from './SqlViewModal';
import airline from './__fixtures__/ptf-airline.json';
import airtraffic from './__fixtures__/ptf-airtraffic-chained.json';
import airtrafficUnchained from './__fixtures__/ptf-airtraffic-unchained.json';

const SNAPSHOT = {
  agentId: 'airline-agent',
  jobId: 'abcdef0123456789',
  script: "CREATE TEMPORARY VIEW `a_in` AS\nSELECT 'x''y' AS `c` -- note\n",
  dialect: 'open-source',
  generatedAt: 1760000000000,
  chainingEnabled: false,
  stale: false,
};

function mockFetch(response) {
  global.fetch = vi.fn(() => Promise.resolve(response));
}

afterEach(() => { vi.restoreAllMocks(); });

describe('tokenizeSqlLine', () => {
  test('concatenating the tokens gives back the original line', () => {
    const line = "  `raw` STRING, WATERMARK FOR `event_ts` AS `event_ts` - INTERVAL '5.000' SECOND -- c;";
    expect(tokenizeSqlLine(line).map(t => t.text).join('')).toBe(line);
  });

  test('classifies keywords, identifiers, strings, numbers and comments', () => {
    const types = Object.fromEntries(
      tokenizeSqlLine("SELECT `a` FROM t WHERE x = 'it''s' AND n = 42 -- end").map(t => [t.text, t.type]),
    );
    expect(types.SELECT).toBe('keyword');
    expect(types['`a`']).toBe('ident');
    expect(types["'it''s'"]).toBe('string');
    expect(types['42']).toBe('number');
    expect(types['-- end']).toBe('comment');
  });

  test('does not treat a semicolon or JSON inside a string as structure', () => {
    const tokens = tokenizeSqlLine(`agent_config => '{"a":"b;c"}',`);
    expect(tokens.filter(t => t.type === 'string')).toHaveLength(1);
  });

  test('copes with unterminated strings and empty lines', () => {
    expect(tokenizeSqlLine("'oops").map(t => t.text).join('')).toBe("'oops");
    expect(tokenizeSqlLine('')).toEqual([]);
  });
});

describe('groupStatements', () => {
  test('groups by agent in order of first appearance, null agent as its own group', () => {
    const groups = groupStatements([
      { kind: 'table', agentId: 'a', sql: 'T1;' },
      { kind: 'function', agentId: null, sql: 'F;' },
      { kind: 'insert', agentId: 'a', sql: 'I1;' },
      { kind: 'table', agentId: 'b', sql: 'T2;' },
    ]);
    expect(groups.map(g => g.agentId)).toEqual(['a', null, 'b']);
    expect(groups[0].sql).toBe('T1;\n\nI1;');
    expect(groups[0].items).toHaveLength(2);
  });
});

describe('SqlViewModal', () => {
  test('renders one section per agent when statements are provided', async () => {
    const statements = [
      { kind: 'table', agentId: 'a1', sql: 'CREATE TEMPORARY TABLE `a1_in` (x INT);' },
      { kind: 'insert', agentId: 'a1', sql: 'INSERT INTO `a1_out` SELECT 1;' },
      { kind: 'table', agentId: 'a2', sql: 'CREATE TEMPORARY TABLE `a2_in` (x INT);' },
    ];
    mockFetch({ ok: true, json: () => Promise.resolve({ ...SNAPSHOT, statements }) });
    render(<SqlViewModal useCaseId="airline-agent" onClose={vi.fn()} />);

    expect(await screen.findByRole('heading', { name: /a1\s*2 statements/ })).toBeInTheDocument();
    expect(screen.getByRole('heading', { name: /a2\s*1 statement$/ })).toBeInTheDocument();
    expect(screen.getByLabelText('SQL of a1').textContent).toContain('INSERT INTO');
    expect(screen.getByLabelText('SQL of a2').textContent).not.toContain('INSERT INTO');
    expect(screen.queryByLabelText('Generated SQL script')).not.toBeInTheDocument();
  });

  test('falls back to the flat script for snapshots without statements', async () => {
    mockFetch({ ok: true, json: () => Promise.resolve({ ...SNAPSHOT, statements: null }) });
    render(<SqlViewModal useCaseId="airline-agent" onClose={vi.fn()} />);
    expect(await screen.findByLabelText('Generated SQL script')).toBeInTheDocument();
    expect(screen.queryByRole('heading')).not.toBeInTheDocument();
  });

  test('requests the SQL of the use case and renders it line by line', async () => {
    mockFetch({ ok: true, json: () => Promise.resolve(SNAPSHOT) });
    render(<SqlViewModal useCaseId="airline-agent" onClose={vi.fn()} />);

    expect(screen.getByText('Loading SQL…')).toBeInTheDocument();
    const code = await screen.findByLabelText('Generated SQL script');

    expect(global.fetch.mock.calls[0][0]).toMatch(/\/api\/agents\/airline-agent\/sql$/);
    expect(code.textContent).toContain('CREATE TEMPORARY VIEW');
    expect(code.textContent).toContain("'x''y'");
    expect(screen.getByRole('dialog', { name: 'SQL View' })).toBeInTheDocument();
    expect(screen.getByText(/Chaining disabled/)).toBeInTheDocument();
    expect(screen.queryByRole('status')).not.toBeInTheDocument();
  });

  test('never interprets the SQL as HTML', async () => {
    mockFetch({ ok: true, json: () => Promise.resolve({ ...SNAPSHOT, script: "SELECT '<img src=x onerror=alert(1)>';\n" }) });
    const { container } = render(<SqlViewModal useCaseId="airline-agent" onClose={vi.fn()} />);
    await screen.findByLabelText('Generated SQL script');
    expect(container.querySelector('img')).toBeNull();
  });

  test('shows a notice when the descriptor changed since deployment', async () => {
    mockFetch({ ok: true, json: () => Promise.resolve({ ...SNAPSHOT, stale: true }) });
    render(<SqlViewModal useCaseId="airline-agent" onClose={vi.fn()} />);
    expect(await screen.findByRole('status')).toHaveTextContent(/descriptor has changed/i);
  });

  test('shows the server error for 404 and 409 responses', async () => {
    mockFetch({ ok: false, status: 404, statusText: 'Not Found', json: () => Promise.resolve({ error: 'No SQL available' }) });
    render(<SqlViewModal useCaseId="airline-agent" onClose={vi.fn()} />);
    expect(await screen.findByRole('alert')).toHaveTextContent('No SQL available');
    expect(screen.getByRole('button', { name: 'Copy' })).toBeDisabled();
  });

  test('shows a message when the server cannot be reached', async () => {
    global.fetch = vi.fn(() => Promise.reject(new TypeError('network down')));
    render(<SqlViewModal useCaseId="airline-agent" onClose={vi.fn()} />);
    expect(await screen.findByRole('alert')).toHaveTextContent('Could not reach the server.');
  });

  test('closes on Escape, on the close button and on overlay click, but not on modal click', async () => {
    mockFetch({ ok: true, json: () => Promise.resolve(SNAPSHOT) });
    const onClose = vi.fn();
    const { container } = render(<SqlViewModal useCaseId="airline-agent" onClose={onClose} />);
    await screen.findByLabelText('Generated SQL script');

    fireEvent.click(screen.getByRole('dialog'));
    expect(onClose).not.toHaveBeenCalled();

    fireEvent.keyDown(document, { key: 'Escape' });
    fireEvent.click(screen.getByRole('button', { name: 'Close SQL view' }));
    fireEvent.click(container.querySelector('.sql-view-overlay'));
    expect(onClose).toHaveBeenCalledTimes(3);
  });

  test('copy falls back to execCommand outside a secure context', async () => {
    mockFetch({ ok: true, json: () => Promise.resolve(SNAPSHOT) });
    Object.defineProperty(window, 'isSecureContext', { value: false, configurable: true });
    document.execCommand = vi.fn(() => true);

    render(<SqlViewModal useCaseId="airline-agent" onClose={vi.fn()} />);
    await screen.findByLabelText('Generated SQL script');
    fireEvent.click(screen.getByRole('button', { name: 'Copy' }));

    await waitFor(() => expect(screen.getByRole('button', { name: 'Copied' })).toBeInTheDocument());
    expect(document.execCommand).toHaveBeenCalledWith('copy');
  });
});

describe('SqlViewModal — Raw SQL / Graphical toggle', () => {
  const withStatements = (statements, extra = {}) => ({ ...SNAPSHOT, script: 'S', statements, ...extra });
  const open = async (body) => {
    mockFetch({ ok: true, json: () => Promise.resolve(body) });
    render(<SqlViewModal useCaseId="airline-agent" onClose={vi.fn()} />);
    await screen.findByRole('dialog', { name: 'SQL View' });
  };
  const rawBtn   = () => screen.getByRole('button', { name: 'Raw SQL' });
  const graphBtn = () => screen.getByRole('button', { name: 'Graphical' });

  test('opens on Raw SQL; the toggle shows which presentation is active', async () => {
    await open(withStatements(airline.statements));
    await screen.findByLabelText('SQL of airline-agent');
    expect(rawBtn()).toHaveAttribute('aria-pressed', 'true');
    expect(graphBtn()).toHaveAttribute('aria-pressed', 'false');
    expect(screen.queryByLabelText('Data flow of the deployed SQL')).not.toBeInTheDocument();
  });

  test('the Graphical button is disabled while loading and when the request failed', async () => {
    mockFetch({ ok: false, status: 404, statusText: 'x', json: () => Promise.resolve({ error: 'No SQL available' }) });
    render(<SqlViewModal useCaseId="airline-agent" onClose={vi.fn()} />);
    expect(graphBtn()).toBeDisabled();
    await screen.findByRole('alert');
    expect(graphBtn()).toBeDisabled();
  });

  test('switches to the diagram and back without losing the SQL', async () => {
    await open(withStatements(airline.statements));
    await screen.findByLabelText('SQL of airline-agent');

    fireEvent.click(graphBtn());
    expect(screen.getByLabelText('Data flow of the deployed SQL')).toBeInTheDocument();
    expect(screen.queryByLabelText('SQL of airline-agent')).not.toBeInTheDocument();
    expect(graphBtn()).toHaveAttribute('aria-pressed', 'true');
    expect(global.fetch).toHaveBeenCalledTimes(1);          // no refetch

    fireEvent.click(rawBtn());
    expect(screen.getByLabelText('SQL of airline-agent').textContent).toContain('CREATE TEMPORARY VIEW');
    expect(global.fetch).toHaveBeenCalledTimes(1);
  });

  test('the diagram shows the topics and processor of the SQL', async () => {
    await open(withStatements(airline.statements));
    await screen.findByLabelText('SQL of airline-agent');
    fireEvent.click(graphBtn());

    for (const topic of ['agent-flight-events', 'agent-catering-events', 'agent-passenger-events',
      'agent-flight-notifications', 'agent-passenger-notifications', 'agent-airline-errors', 'agent-flight-entity-changelog']) {
      expect(screen.getByRole('button', { name: new RegExp(`: ${topic}$`) })).toBeInTheDocument();
    }
    expect(screen.getByRole('button', { name: 'Processor (PTF): event_rule_airline_agent' })).toBeInTheDocument();
    expect(screen.getByText(/13 of 13 statements drawn/)).toBeInTheDocument();
  });

  test('selecting a box explains it in words and shows its SQL; selecting again clears it', async () => {
    await open(withStatements(airline.statements));
    await screen.findByLabelText('SQL of airline-agent');
    fireEvent.click(graphBtn());

    const node = screen.getByRole('button', { name: 'Kafka source: agent-flight-events' });
    fireEvent.click(node);
    const details = screen.getByRole('complementary', { name: 'Details' });
    expect(details).toHaveTextContent('Reads records from the Kafka topic ‘agent-flight-events’.');
    expect(details).toHaveTextContent('agent-airline-agent');
    expect(details.querySelector('pre').textContent).toContain("'topic' = 'agent-flight-events'");
    expect(node).toHaveAttribute('aria-pressed', 'true');

    fireEvent.click(node);
    expect(screen.getByRole('complementary', { name: 'Details' })).toHaveTextContent(/Select a box/);
  });

  test('keyboard: Enter selects a node', async () => {
    await open(withStatements(airline.statements));
    await screen.findByLabelText('SQL of airline-agent');
    fireEvent.click(graphBtn());
    fireEvent.keyDown(screen.getByRole('button', { name: 'Processor (PTF): event_rule_airline_agent' }), { key: 'Enter' });
    expect(screen.getByRole('complementary', { name: 'Details' })).toHaveTextContent('Runs the Process Table Function');
  });

  test('"Show in raw SQL" returns to the raw view with that statement marked', async () => {
    await open(withStatements(airline.statements));
    await screen.findByLabelText('SQL of airline-agent');
    fireEvent.click(graphBtn());
    fireEvent.click(screen.getByRole('button', { name: 'Processor (PTF): event_rule_airline_agent' }));
    fireEvent.click(screen.getByRole('button', { name: 'Show in raw SQL' }));

    expect(rawBtn()).toHaveAttribute('aria-pressed', 'true');
    const code = screen.getByLabelText('SQL of airline-agent');
    const marked = [...code.querySelectorAll('.sql-view-line--marked')].map(l => l.textContent);
    expect(marked.join('\n')).toContain('event_rule_airline_agent');
    expect(marked.join('\n')).not.toContain('airline_agent_src_agent_flight_events');
    expect(marked.length).toBe(airline.statements[4].sql.split('\n').length);
  });

  test('an edge label explains the filter of an INSERT', async () => {
    await open(withStatements(airline.statements));
    await screen.findByLabelText('SQL of airline-agent');
    fireEvent.click(graphBtn());
    fireEvent.click(screen.getByRole('button', { name: /Flow airline_agent_out to airline_agent_sink_agent_flight_notifications/ }));
    expect(screen.getByRole('complementary', { name: 'Details' })).toHaveTextContent(
      "Writes the rows of ‘airline_agent_out’ where out_type = 'FlightNotification'");
  });

  test('multiple agents get one lane each', async () => {
    await open(withStatements(airtraffic.statements));
    await screen.findByLabelText('SQL of aircraft-agent');
    fireEvent.click(graphBtn());
    const lanes = [...document.querySelectorAll('.sqg-band text')].map(t => t.textContent);
    expect(lanes).toEqual(['aircraft-agent', 'airport-agent']);
  });

  test('unsupported statements are listed with the reason, the rest is drawn', async () => {
    const odd = { kind: 'view', agentId: 'airline-agent', sql: 'CREATE VIEW j AS SELECT a FROM x JOIN y ON x.a = y.a' };
    await open(withStatements([...airline.statements, odd]));
    await screen.findByLabelText('SQL of airline-agent');
    fireEvent.click(graphBtn());

    const note = screen.getByRole('status');
    expect(note).toHaveTextContent('1 of 14 statements could not be drawn');
    expect(note).toHaveTextContent(/unsupported clause 'JOIN'/);
    expect(note.querySelector('pre').textContent).toBe(odd.sql);
    expect(screen.getByRole('button', { name: 'Processor (PTF): event_rule_airline_agent' })).toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: 'Show in raw SQL' }));
    expect(rawBtn()).toHaveAttribute('aria-pressed', 'true');
    expect(screen.getByLabelText('SQL of airline-agent').querySelector('.sql-view-line--marked').textContent).toContain('CREATE VIEW j');
  });

  test('when nothing can be drawn, it says why and offers the raw SQL', async () => {
    const cyc = [
      { kind: 'view', agentId: 'a', sql: 'CREATE VIEW a AS SELECT x FROM b' },
      { kind: 'view', agentId: 'a', sql: 'CREATE VIEW b AS SELECT x FROM a' },
    ];
    await open(withStatements(cyc));
    await screen.findByLabelText('SQL of a');
    fireEvent.click(graphBtn());
    expect(screen.getByRole('alert')).toHaveTextContent(/graphical view is not available: circular reference/);
    fireEvent.click(screen.getByRole('button', { name: 'Show raw SQL' }));
    expect(screen.getByLabelText('SQL of a')).toBeInTheDocument();
  });

  test('a snapshot without statements (older deployment) falls back to the raw script', async () => {
    await open({ ...SNAPSHOT, statements: null });
    await screen.findByLabelText('Generated SQL script');
    fireEvent.click(graphBtn());
    expect(screen.getByRole('alert')).toHaveTextContent(/no SQL statements to draw/);
    fireEvent.click(screen.getByRole('button', { name: 'Show raw SQL' }));
    expect(screen.getByLabelText('Generated SQL script')).toBeInTheDocument();
  });

  test('an empty statement list is handled', async () => {
    await open(withStatements([]));
    await screen.findByLabelText('Generated SQL script');
    fireEvent.click(graphBtn());
    expect(screen.getByRole('alert')).toHaveTextContent(/no SQL statements to draw/);
  });

  test('Copy and Download still use the full script in the graphical view', async () => {
    await open(withStatements(airline.statements, { script: 'THE FULL SCRIPT' }));
    await screen.findByLabelText('SQL of airline-agent');
    Object.defineProperty(window, 'isSecureContext', { value: true, configurable: true });
    const writeText = vi.fn(() => Promise.resolve());
    Object.defineProperty(navigator, 'clipboard', { value: { writeText }, configurable: true });
    fireEvent.click(graphBtn());
    fireEvent.click(screen.getByRole('button', { name: 'Copy' }));
    await waitFor(() => expect(writeText).toHaveBeenCalledWith('THE FULL SCRIPT'));
  });

  test('SQL text from the statements is never interpreted as HTML in the diagram', async () => {
    const evil = [{ kind: 'table', agentId: 'x', sql: "CREATE TABLE `<img src=x onerror=alert(1)>` (a INT) WITH ('connector' = 'kafka', 'topic' = '<script>alert(1)</script>')" }];
    await open(withStatements(evil));
    await screen.findByLabelText('SQL of x');
    fireEvent.click(graphBtn());
    expect(document.querySelector('img')).toBeNull();
    expect(document.querySelector('script')).toBeNull();
  });
});

describe('SqlViewModal — graphical view zoom', () => {
  const open = async (statements) => {
    mockFetch({ ok: true, json: () => Promise.resolve({ ...SNAPSHOT, script: 'S', statements }) });
    render(<SqlViewModal useCaseId="airline-agent" onClose={vi.fn()} />);
    await screen.findByRole('dialog', { name: 'SQL View' });
    await screen.findByRole('group', { name: 'SQL presentation' });
    await waitFor(() => expect(screen.getByRole('button', { name: 'Graphical' })).not.toBeDisabled());
    fireEvent.click(screen.getByRole('button', { name: 'Graphical' }));
  };
  const svg = () => document.querySelector('.sqg-svg');
  const level = () => screen.getByTestId('zoom-level').textContent;
  const width = () => Number(svg().getAttribute('width'));
  const viewBoxWidth = () => Number(svg().getAttribute('viewBox').split(' ')[2]);

  let clientWidth;
  beforeEach(() => {
    clientWidth = 1124;     // 1100 usable + 24 padding
    vi.spyOn(HTMLElement.prototype, 'clientWidth', 'get').mockImplementation(function () {
      return this.classList.contains('sqg-canvas') ? clientWidth : 0;
    });
  });

  test('a diagram that fits is shown at 100%', async () => {
    await open(airline.statements.slice(0, 3));         // three sources: narrow
    expect(level()).toBe('100%');
    expect(width()).toBe(viewBoxWidth());
  });

  test('a diagram wider than the window is scaled to fit, but kept readable', async () => {
    await open(airtraffic.statements);                   // ~2450px
    expect(level()).toBe(`${Math.round(Math.max(0.6, 1100 / viewBoxWidth()) * 100)}%`);
    expect(width()).toBeLessThan(viewBoxWidth());

    cleanup();
    await open(airtrafficUnchained.statements);          // ~3400px: below the readable limit
    expect(level()).toBe('60%');
  });

  test('zoom in / out change the size, stay within the limits, and the viewBox never changes', async () => {
    clientWidth = 4000;                                  // wide window: starts at 100%
    await open(airline.statements);
    expect(level()).toBe('100%');
    const vb = svg().getAttribute('viewBox');
    const before = width();

    fireEvent.click(screen.getByRole('button', { name: 'Zoom in' }));
    expect(width()).toBeGreaterThan(before);
    expect(level()).toBe('125%');

    for (let i = 0; i < 20; i += 1) fireEvent.click(screen.getByRole('button', { name: 'Zoom in' }));
    expect(level()).toBe('200%');
    expect(screen.getByRole('button', { name: 'Zoom in' })).toBeDisabled();

    for (let i = 0; i < 30; i += 1) fireEvent.click(screen.getByRole('button', { name: 'Zoom out' }));
    expect(level()).toBe('20%');
    expect(screen.getByRole('button', { name: 'Zoom out' })).toBeDisabled();
    expect(svg().getAttribute('viewBox')).toBe(vb);
  });

  test('Fit fills the window width, 100% restores the actual size', async () => {
    await open(airtraffic.statements);
    fireEvent.click(screen.getByRole('button', { name: '100%' }));
    expect(width()).toBe(viewBoxWidth());
    expect(level()).toBe('100%');

    fireEvent.click(screen.getByRole('button', { name: 'Fit' }));
    expect(Math.abs(width() - 1100)).toBeLessThanOrEqual(1);
  });

  test('Fit follows a resize of the window', async () => {
    await open(airtraffic.statements);
    clientWidth = 824;
    // ResizeObserver is a no-op in jsdom: remount via the toggle to re-measure.
    fireEvent.click(screen.getByRole('button', { name: 'Raw SQL' }));
    fireEvent.click(screen.getByRole('button', { name: 'Graphical' }));
    fireEvent.click(screen.getByRole('button', { name: 'Fit' }));
    expect(Math.abs(width() - 800)).toBeLessThanOrEqual(1);
  });

  test('selection and the details panel survive zooming', async () => {
    await open(airline.statements);
    fireEvent.click(screen.getByRole('button', { name: 'Processor (PTF): event_rule_airline_agent' }));
    fireEvent.click(screen.getByRole('button', { name: 'Zoom in' }));
    expect(screen.getByRole('button', { name: 'Processor (PTF): event_rule_airline_agent' })).toHaveAttribute('aria-pressed', 'true');
    expect(screen.getByRole('complementary', { name: 'Details' })).toHaveTextContent('Runs the Process Table Function');
  });

  test('every zoom control is reachable by keyboard (real buttons with names)', async () => {
    await open(airline.statements);
    for (const name of ['Zoom out', 'Zoom in', 'Fit', '100%']) {
      expect(screen.getByRole('button', { name }).tagName).toBe('BUTTON');
    }
  });
});

