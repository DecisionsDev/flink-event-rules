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
import AgentScenariosPanel from './AgentScenariosPanel';

function UseCaseSelector({ onSelectUseCase, onCreateDescriptor, onEditAgent, onEditSpecificAgent, onAddAgentToJob, onDeleteAgent, onDeleteSpecificAgent, refreshToken }) {
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
          <button className="di-header-action" title="Connections" aria-label="Connections">
            <svg width="18" height="18" viewBox="0 0 32 32" fill="currentColor">
              <path d="M26 14v-4h-4V8h4V4h2v4h4v2h-4v4h-2zm-8 4v-4h-4v-2h4V8h2v4h4v2h-4v4h-2zM4 28v-4h4v-2H4v-4H2v4H0v2h2v4h2zM12 28v-4h4v-2h-4v-4h-2v4H8v2h2v4h2z"/>
            </svg>
          </button>
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
        <button className="di-btn-primary" onClick={onCreateDescriptor}>
          <span>New Event Rules Application</span>
          <span className="di-btn-icon">+</span>
        </button>
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
          onEditSpecificAgent={onEditSpecificAgent}
          onAddAgentToJob={onAddAgentToJob}
          onDeleteAgent={onDeleteAgent}
          onDeleteSpecificAgent={onDeleteSpecificAgent}
          onSelectUseCase={onSelectUseCase}
          refreshToken={refreshToken}
          searchQuery={searchQuery}
          sortBy={sortBy}
          viewMode={viewMode}
        />
      </div>
    </div>
  );
}

export default UseCaseSelector;
