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
import React, { useId, useMemo, useState } from 'react';

/** Show the filter box once the list is long enough to need it. */
export const FILTER_THRESHOLD = 6;

/** Heading of the decisions whose id carries no project. */
export const UNGROUPED = 'Ungrouped';

export function statusTone(status) {
  if (status === 'deployed') return 'ok';
  if (!status || status === 'unknown') return 'neutral';
  return 'warn';
}

const projectOf = (a) => a.project || UNGROUPED;

/** Projects in alphabetical order, the ungrouped decisions last. */
function compareProjects(a, b) {
  if (a === UNGROUPED) return 1;
  if (b === UNGROUPED) return -1;
  return a.localeCompare(b);
}

/**
 * Project-first navigation of the decision archives listed by a DI runtime:
 * Projects → select a project → its decisions.
 *
 * Accordion: at most one project is expanded and it is always the selected project. Selecting a
 * project expands it and collapses the previous one; only its decisions are rendered. Projects are
 * derived from the decisions, so a project is empty only when the filter hides all of its decisions:
 * it then stays listed, flagged "No matching decisions", and cannot be expanded, so no empty area
 * is ever shown. A single project is selected automatically.
 *
 * Mounted only while a list is shown (the parents unmount it while loading), so a reload starts
 * from a clean project selection and filter.
 *
 * Props
 *   archives       decisions enriched by describeArchive ({ id, label, service, version, build,
 *                  project, status, … })
 *   decisionId     id of the selected decision ('' = none)
 *   onSelect       (id) => void — called with '' when the selected decision is left behind by a
 *                  change of project
 *   disabled       lock every control (e.g. while fetching)
 *   radioName      name of the radio group
 *   ariaLabelFor   (archive) => accessible name of a decision
 *   isHighlighted  (archive) => card gets the "active" accent (running / current archive)
 *   tagsFor        (archive) => extra tags shown on a card
 */
export default function ArchiveProjectBrowser({
  archives,
  decisionId,
  onSelect,
  disabled = false,
  radioName,
  ariaLabelFor = (a) => a.label,
  isHighlighted = () => false,
  tagsFor = () => null,
}) {
  const uid = useId();
  const [filter, setFilter] = useState('');
  const [chosen, setChosen] = useState(null);

  const projects = useMemo(() => {
    const q = filter.trim().toLowerCase();
    const byProject = new Map();
    for (const a of archives) {
      const name = projectOf(a);
      if (!byProject.has(name)) byProject.set(name, []);
      byProject.get(name).push(a);
    }
    return [...byProject.keys()].sort(compareProjects).map(name => {
      const all = byProject.get(name);
      const visible = q
        ? all.filter(a => `${a.id} ${a.name || ''} ${name}`.toLowerCase().includes(q))
        : all;
      return { name, visible };
    });
  }, [archives, filter]);

  // The selected project: the user's choice, or the only project there is.
  const selectedProject = projects.some(p => p.name === chosen)
    ? chosen
    : (projects.length === 1 ? projects[0].name : null);

  const selectProject = (name) => {
    if (name === selectedProject) return;
    setChosen(name);
    const target = projects.find(p => p.name === name);
    if (decisionId && !target?.visible.some(a => a.id === decisionId)) onSelect('');
  };

  const showFilter = archives.length > FILTER_THRESHOLD;
  const noMatch = filter.trim() !== '' && projects.every(p => p.visible.length === 0);

  return (
    <>
      {showFilter && (
        <input
          type="search"
          className="rr-input rr-filter"
          aria-label="Filter archives"
          placeholder="Filter by name, project or version"
          value={filter}
          onChange={e => setFilter(e.target.value)}
        />
      )}
      <div className="rr-projects" role="list" aria-label="Projects">
        {projects.map((p, i) => {
          const empty = p.visible.length === 0;
          const selected = p.name === selectedProject;
          const expanded = selected && !empty;
          const panelId = `${uid}-project-${i}`;
          return (
            <div
              key={p.name}
              role="listitem"
              className={`rr-project${selected ? ' rr-project--selected' : ''}${empty ? ' rr-project--empty' : ''}`}
              data-testid="rr-project"
            >
              <button
                type="button"
                className="rr-project-head"
                aria-expanded={expanded}
                aria-controls={expanded ? panelId : undefined}
                aria-disabled={empty || undefined}
                aria-current={selected ? 'true' : undefined}
                disabled={disabled}
                onClick={() => { if (!empty) selectProject(p.name); }}
              >
                <span className="rr-project-caret" aria-hidden="true">{expanded ? '▾' : '▸'}</span>
                <span className="rr-project-name">{p.name}</span>
                <span className="rr-project-count">
                  {empty
                    ? 'No matching decisions'
                    : `${p.visible.length} ${p.visible.length === 1 ? 'decision' : 'decisions'}`}
                </span>
              </button>
              {expanded && (
                <div id={panelId} className="rr-project-body">
                  <div className="rr-grid" role="radiogroup" aria-label="Select decision">
                    {p.visible.map(a => (
                      <label key={a.id} className={`rr-option${isHighlighted(a) ? ' rr-option--active' : ''}`} title={a.id}>
                        <input
                          type="radio"
                          name={radioName}
                          className="rr-radio"
                          value={a.id}
                          aria-label={ariaLabelFor(a)}
                          checked={a.id === decisionId}
                          disabled={disabled}
                          onChange={() => onSelect(a.id)}
                        />
                        <span className="rr-card">
                          <span className="rr-card-top">
                            <span className="rr-check" aria-hidden="true" />
                            <span className={`rr-badge rr-badge--${statusTone(a.status)}`}>{a.status || 'unknown'}</span>
                          </span>
                          <span className="rr-card-title">{a.service}</span>
                          <span className="rr-card-tags">
                            {a.version && <span className="rr-card-version">v{a.version}</span>}
                            {a.build && <span className="rr-card-build">{a.build.replace(/T.*$/, '')}</span>}
                            {tagsFor(a)}
                          </span>
                        </span>
                      </label>
                    ))}
                  </div>
                </div>
              )}
            </div>
          );
        })}
      </div>
      {noMatch && <div className="rr-empty"><strong>No archives match “{filter}”</strong></div>}
    </>
  );
}
