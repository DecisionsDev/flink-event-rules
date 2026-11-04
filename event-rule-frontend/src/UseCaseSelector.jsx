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
import './UseCaseSelector.css';
import { FLINK_DASHBOARD_URL } from './config';
import AgentScenariosPanel from './AgentScenariosPanel';

function UseCaseSelector({ onSelectUseCase, onCreateDescriptor, onCreateDiagram, onEditDiagram, onEditAgent, onEditSpecificAgent, onAddAgentToJob, onDeleteAgent, onDeleteSpecificAgent, refreshToken, flinkRuntime = 'datastream', onFlinkRuntimeChange }) {
  const [searchQuery, setSearchQuery] = useState('');
  const [sortBy, setSortBy] = useState('recently_updated');
  const [viewMode, setViewMode] = useState('grid'); // 'grid' | 'list'

  return (
    <div className="di-page-container">
      {/* ── Top Decision Intelligence Header Bar ── */}
      <header className="di-header">
        <div className="di-header-left">
          <div className="di-header-brand">
            <span className="di-header-brand-ibm">IBM</span>
            <span className="di-header-brand-title">Decision Intelligence - Event Rules</span>
          </div>
        </div>
        <div className="di-header-right">
          <a
            className="di-header-action"
            href={FLINK_DASHBOARD_URL}
            target="_blank"
            rel="noopener noreferrer"
            title="Open Flink Dashboard"
            aria-label="Open Flink Dashboard in a new tab"
          >
            <svg width="18" height="18" viewBox="0 0 32 32" fill="currentColor">
              <path d="M26 28H6a2 2 0 01-2-2V6a2 2 0 012-2h10v2H6v20h20V16h2v10a2 2 0 01-2 2z"/>
              <path d="M20 2v2h6.586L18 12.586 19.414 14 28 5.414V12h2V2H20z"/>
            </svg>
          </a>
          <button className="di-header-action" title="Settings" aria-label="Settings">
            <svg width="18" height="18" viewBox="0 0 32 32" fill="currentColor">
              <path d="M29.41 14.16l-2.61-.43a11.13 11.13 0 00-.91-2.19l1.55-2.18a1 1 0 00-.14-1.3l-2.67-2.67a1 1 0 00-1.3-.14l-2.18 1.55a11.13 11.13 0 00-2.19-.91l-.43-2.61A1 1 0 0017.65 2h-3.3a1 1 0 00-.99.84l-.43 2.61a11.13 11.13 0 00-2.19.91L8.56 4.81a1 1 0 00-1.3.14L4.59 7.62a1 1 0 00-.14 1.3l1.55 2.18a11.13 11.13 0 00-.91 2.19l-2.61.43A1 1 0 001.5 14.72v3.3a1 1 0 00.84.99l2.61.43a11.13 11.13 0 00.91 2.19L4.31 23.81a1 1 0 00.14 1.3l2.67 2.67a1 1 0 001.3.14l2.18-1.55a11.13 11.13 0 002.19.91l.43 2.61a1 1 0 00.99.84h3.3a1 1 0 00.99-.84l.43-2.61a11.13 11.13 0 002.19-.91l2.18 1.55a1 1 0 001.3-.14l2.67-2.67a1 1 0 00.14-1.3l-1.55-2.18a11.13 11.13 0 00.91-2.19l2.61-.43a1 1 0 00.84-.99v-3.3a1 1 0 00-.84-.86zM16 21a5 5 0 115-5 5 5 0 01-5 5z"/>
            </svg>
          </button>
          <button className="di-header-action" title="Theme" aria-label="Theme">
            <svg width="18" height="18" viewBox="0 0 32 32" fill="currentColor">
              <path d="M16 2A14 14 0 1030 16 14 14 0 0016 2zm0 25.8V4.2A11.8 11.8 0 114.2 16 11.81 11.81 0 0116 27.8z"/>
            </svg>
          </button>
          <div className="di-user-avatar" title="John Doe">
            JD
          </div>
        </div>
      </header>

      {/* ── Title Bar ── */}
      <div className="di-title-bar">
        <h1 className="di-page-title">Event Rules Applications</h1>
        <div style={{ display: 'flex', gap: 8 }}>
          <button className="di-btn-primary" onClick={onCreateDescriptor}>
            <span>New Event Rules Application</span>
            <span className="di-btn-icon">+</span>
          </button>
          <button className="di-btn-primary" onClick={onCreateDiagram} style={{ background: '#6929c4', borderColor: '#6929c4' }} title="Open the graphical Agent Diagram Editor">
            <svg width="14" height="14" viewBox="0 0 32 32" fill="currentColor" style={{ flexShrink: 0 }}>
              <path d="M27 22.14V9.86A4 4 0 1022.14 5H9.86A4 4 0 105 9.86v12.28A4 4 0 109.86 27h12.28A4 4 0 1027 22.14zM25 4a2 2 0 11-2 2 2 2 0 012-2zM4 7a2 2 0 112 2A2 2 0 014 7zm3 18a2 2 0 11-2-2 2 2 0 012 2zm18.14-4A4 4 0 0022 23.86v.14H9.86A4 4 0 008 22.14V9.86A4 4 0 0010.14 8H22a4 4 0 001.86.14V22a4 4 0 00-.72.14zM25 28a2 2 0 112-2 2 2 0 01-2 2z"/>
            </svg>
            <span>New Event Rules Application (Diagram)</span>
          </button>
        </div>
      </div>

      {/* ── Toolbar Bar ── */}
      <div className="di-toolbar-bar">
        <div className="di-toolbar-left">
          <div className="di-sort-dropdown">
            <span className="di-sort-label">Sort by:</span>
            <select
              className="di-sort-select"
              value={sortBy}
              onChange={(e) => setSortBy(e.target.value)}
            >
              <option value="recently_updated">Recently updated</option>
              <option value="name_asc">Name (A-Z)</option>
              <option value="name_desc">Name (Z-A)</option>
            </select>
          </div>

          <div className="di-search-box">
            <span className="di-search-icon">
              <svg width="16" height="16" viewBox="0 0 32 32" fill="currentColor">
                <path d="M29.414 28l-6.84-6.84a11.94 11.94 0 10-1.414 1.414L28 29.414zM4 14a10 10 0 1110 10A10.011 10.011 0 014 14z"/>
              </svg>
            </span>
            <input
              type="text"
              className="di-search-input"
              placeholder="Search"
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
            />
          </div>

          <div className="di-flink-runtime-selector">
            <span className="di-flink-runtime-label">Flink Runtime:</span>
            <div className="di-radio-group" role="radiogroup" aria-label="Flink Runtime Implementation">
              <label className="di-radio-label">
                <input
                  type="radio"
                  name="flink-runtime"
                  value="datastream"
                  checked={flinkRuntime === 'datastream'}
                  onChange={() => onFlinkRuntimeChange && onFlinkRuntimeChange('datastream')}
                  className="di-radio-input"
                />
                <span className="di-radio-text">DataStream</span>
              </label>
              <label className="di-radio-label">
                <input
                  type="radio"
                  name="flink-runtime"
                  value="ptf"
                  checked={flinkRuntime === 'ptf'}
                  onChange={() => onFlinkRuntimeChange && onFlinkRuntimeChange('ptf')}
                  className="di-radio-input"
                />
                <span className="di-radio-text">Process Table Function</span>
              </label>
            </div>
          </div>
        </div>

        <div className="di-toolbar-right">
          <div className="di-view-toggle">
            <button
              className={`di-view-btn ${viewMode === 'list' ? 'active' : ''}`}
              onClick={() => setViewMode('list')}
              title="List view"
              aria-label="List view"
            >
              <svg width="18" height="18" viewBox="0 0 32 32" fill="currentColor">
                <path d="M4 6h24v2H4zm0 6h24v2H4zm0 6h24v2H4zm0 6h24v2H4z"/>
              </svg>
            </button>
            <button
              className={`di-view-btn ${viewMode === 'grid' ? 'active' : ''}`}
              onClick={() => setViewMode('grid')}
              title="Grid view"
              aria-label="Grid view"
            >
              <svg width="18" height="18" viewBox="0 0 32 32" fill="currentColor">
                <path d="M4 4h10v10H4zm2 2v6h6V6zm12-2h10v10H18zm2 2v6h6V6zM4 18h10v10H4zm2 2v6h6v-6zm12-2h10v10H18zm2 2v6h6v-6z"/>
              </svg>
            </button>
          </div>
        </div>
      </div>

      {/* ── Main Content Area: Agent Scenarios in Decision Automation style ── */}
      <div className="di-main-content">
        <AgentScenariosPanel
          onEditAgent={onEditAgent}
          onEditDiagram={onEditDiagram}
          onEditSpecificAgent={onEditSpecificAgent}
          onAddAgentToJob={onAddAgentToJob}
          onDeleteAgent={onDeleteAgent}
          onDeleteSpecificAgent={onDeleteSpecificAgent}
          onSelectUseCase={onSelectUseCase}
          refreshToken={refreshToken}
          searchQuery={searchQuery}
          sortBy={sortBy}
          viewMode={viewMode}
          flinkRuntime={flinkRuntime}
        />
      </div>
    </div>
  );
}

export default UseCaseSelector;
