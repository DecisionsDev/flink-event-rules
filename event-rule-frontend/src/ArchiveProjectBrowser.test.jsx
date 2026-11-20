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
import React, { useState } from 'react';
import { describe, test, expect, vi } from 'vitest';
import { render, screen, fireEvent, within } from '@testing-library/react';
import ArchiveProjectBrowser, { UNGROUPED } from './ArchiveProjectBrowser';

const make = (id, service, version, project, extra = {}) => ({
  id, service, version, project, name: `${service}  ${version}`, label: `${service} ${version}`, status: 'deployed', ...extra,
});

const ARCHIVES = [
  make('banking/fraud/1.0', 'fraud', '1.0', 'banking'),
  make('banking/fraud/2.0', 'fraud', '2.0', 'banking'),
  make('retail/promo/1.0', 'promo', '1.0', 'retail'),
  make('loose/1.0', 'loose', '1.0', ''),
];

function Harness({ archives = ARCHIVES, initial = '', onSelectSpy }) {
  const [id, setId] = useState(initial);
  return (
    <ArchiveProjectBrowser
      archives={archives}
      decisionId={id}
      radioName="t"
      onSelect={(v) => { onSelectSpy?.(v); setId(v); }}
    />
  );
}

const head = (name) => screen.getByRole('button', { name: new RegExp(`^${name}`) });
const expandedHeads = () => screen.queryAllByRole('button', { expanded: true });
const decisionRadios = () => screen.queryAllByRole('radio');

describe('ArchiveProjectBrowser — project-first navigation', () => {
  test('groups decisions by project, projects listed alphabetically with ungrouped last', () => {
    render(<Harness />);
    const items = within(screen.getByRole('list', { name: 'Projects' })).getAllByRole('listitem');
    expect(items).toHaveLength(3);
    expect(within(items[0]).getByText('banking')).toBeInTheDocument();
    expect(within(items[0]).getByText('2 decisions')).toBeInTheDocument();
    expect(within(items[1]).getByText('retail')).toBeInTheDocument();
    expect(within(items[1]).getByText('1 decision')).toBeInTheDocument();
    expect(within(items[2]).getByText(UNGROUPED)).toBeInTheDocument();
  });

  test('nothing is expanded and no decision is shown until a project is selected', () => {
    render(<Harness />);
    expect(expandedHeads()).toHaveLength(0);
    expect(decisionRadios()).toHaveLength(0);
    expect(screen.queryByRole('radiogroup')).toBeNull();
  });

  test('selecting a project expands it, marks it selected and shows only its decisions', () => {
    render(<Harness />);
    fireEvent.click(head('banking'));
    expect(head('banking')).toHaveAttribute('aria-expanded', 'true');
    expect(head('banking')).toHaveAttribute('aria-current', 'true');
    expect(screen.getAllByTestId('rr-project')[0]).toHaveClass('rr-project--selected');
    expect(decisionRadios().map(r => r.getAttribute('aria-label'))).toEqual(['fraud 1.0', 'fraud 2.0']);
    expect(screen.queryByRole('radio', { name: 'promo 1.0' })).toBeNull();
    expect(screen.queryByRole('radio', { name: 'loose 1.0' })).toBeNull();
  });

  test('only one project is expanded: selecting another collapses the previous one', () => {
    render(<Harness />);
    fireEvent.click(head('banking'));
    fireEvent.click(head('retail'));
    expect(expandedHeads()).toHaveLength(1);
    expect(head('retail')).toHaveAttribute('aria-expanded', 'true');
    expect(head('banking')).toHaveAttribute('aria-expanded', 'false');
    expect(head('banking')).not.toHaveAttribute('aria-current');
    expect(screen.getAllByTestId('rr-project')[0]).not.toHaveClass('rr-project--selected');
    expect(decisionRadios().map(r => r.getAttribute('aria-label'))).toEqual(['promo 1.0']);
    expect(screen.getAllByRole('radiogroup')).toHaveLength(1);
  });

  test('the expanded project is always the selected one, across several switches', () => {
    render(<Harness />);
    for (const name of ['retail', 'banking', UNGROUPED, 'retail']) {
      fireEvent.click(head(name));
      const selected = screen.getAllByTestId('rr-project').filter(p => p.classList.contains('rr-project--selected'));
      expect(selected).toHaveLength(1);
      expect(within(selected[0]).getByRole('button', { name: new RegExp(`^${name}`) })).toHaveAttribute('aria-expanded', 'true');
      expect(expandedHeads()).toHaveLength(1);
    }
  });

  test('decisions without a project are listed under "Ungrouped"', () => {
    render(<Harness />);
    fireEvent.click(head(UNGROUPED));
    expect(decisionRadios().map(r => r.getAttribute('aria-label'))).toEqual(['loose 1.0']);
  });

  test('clicking the selected project again keeps it expanded (a project is always in focus)', () => {
    render(<Harness />);
    fireEvent.click(head('banking'));
    fireEvent.click(head('banking'));
    expect(head('banking')).toHaveAttribute('aria-expanded', 'true');
  });

  test('selecting a decision works inside the project, and changing project clears a decision left behind', () => {
    const spy = vi.fn();
    render(<Harness onSelectSpy={spy} />);
    fireEvent.click(head('banking'));
    fireEvent.click(screen.getByRole('radio', { name: 'fraud 2.0' }));
    expect(spy).toHaveBeenLastCalledWith('banking/fraud/2.0');
    expect(screen.getByRole('radio', { name: 'fraud 2.0' })).toBeChecked();

    fireEvent.click(head('retail'));
    expect(spy).toHaveBeenLastCalledWith('');
    expect(screen.getByRole('radio', { name: 'promo 1.0' })).not.toBeChecked();
    fireEvent.click(head('banking'));
    expect(screen.getByRole('radio', { name: 'fraud 2.0' })).not.toBeChecked();
  });

  test('a single project is selected and expanded automatically', () => {
    render(<Harness archives={ARCHIVES.slice(0, 2)} />);
    expect(head('banking')).toHaveAttribute('aria-expanded', 'true');
    expect(decisionRadios()).toHaveLength(2);
  });

  test('disabled locks the project headers and the decisions', () => {
    render(
      <ArchiveProjectBrowser archives={ARCHIVES.slice(0, 2)} decisionId="" radioName="t" onSelect={vi.fn()} disabled />,
    );
    expect(head('banking')).toBeDisabled();
    expect(screen.getAllByRole('radio')[0]).toBeDisabled();
  });
});

describe('ArchiveProjectBrowser — empty projects', () => {
  const many = [
    ...Array.from({ length: 5 }, (_, i) => make(`a/s${i}/1.0`, `s${i}`, '1.0', 'alpha')),
    make('b/t/1.0', 't', '1.0', 'beta'),
    make('c/u/1.0', 'u', '1.0', 'gamma'),
  ];

  test('a project whose decisions are all filtered out stays visible, flagged and cannot be expanded', () => {
    render(<Harness archives={many} />);
    fireEvent.click(head('alpha'));
    expect(decisionRadios()).toHaveLength(5);

    fireEvent.change(screen.getByLabelText('Filter archives'), { target: { value: 'beta' } });
    // alpha is still listed, marked empty, collapsed, and shows no accordion area
    const alpha = screen.getAllByTestId('rr-project')[0];
    expect(alpha).toHaveClass('rr-project--empty');
    expect(within(alpha).getByText('No matching decisions')).toBeInTheDocument();
    expect(head('alpha')).toHaveAttribute('aria-expanded', 'false');
    expect(alpha.querySelector('.rr-project-body')).toBeNull();
    expect(screen.queryByRole('radiogroup')).toBeNull();

    fireEvent.click(head('alpha'));
    expect(head('alpha')).toHaveAttribute('aria-expanded', 'false');
    expect(screen.queryByRole('radiogroup')).toBeNull();

    // other projects keep working
    fireEvent.click(head('beta'));
    expect(decisionRadios().map(r => r.getAttribute('aria-label'))).toEqual(['t 1.0']);
  });

  test('a filter that matches nothing keeps every project listed and says so', () => {
    render(<Harness archives={many} />);
    fireEvent.change(screen.getByLabelText('Filter archives'), { target: { value: 'zzz' } });
    expect(screen.getAllByTestId('rr-project')).toHaveLength(3);
    expect(screen.getAllByText('No matching decisions')).toHaveLength(3);
    expect(screen.getByText(/No archives match/)).toBeInTheDocument();
  });

  test('the filter narrows decisions inside the selected project and by project name', () => {
    render(<Harness archives={many} />);
    fireEvent.click(head('alpha'));
    fireEvent.change(screen.getByLabelText('Filter archives'), { target: { value: 's3' } });
    expect(decisionRadios().map(r => r.getAttribute('aria-label'))).toEqual(['s3 1.0']);
    expect(head('alpha')).toHaveTextContent('1 decision');

    fireEvent.change(screen.getByLabelText('Filter archives'), { target: { value: 'gamma' } });
    expect(screen.getAllByTestId('rr-project')[2]).not.toHaveClass('rr-project--empty');
    expect(screen.getAllByTestId('rr-project')[0]).toHaveClass('rr-project--empty');
  });

  test('the filter box only appears for long lists', () => {
    render(<Harness archives={ARCHIVES} />);
    expect(screen.queryByLabelText('Filter archives')).toBeNull();
  });
});
