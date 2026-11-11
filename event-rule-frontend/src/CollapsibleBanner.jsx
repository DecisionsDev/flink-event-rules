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
import './CollapsibleBanner.css';

const STORAGE_KEY = 'di_banner_collapsed';

export default function CollapsibleBanner({ onNewApplication }) {
  const [isCollapsed, setIsCollapsed] = useState(() => {
    try {
      return localStorage.getItem(STORAGE_KEY) === 'true';
    } catch {
      return false;
    }
  });

  const toggleCollapse = () => {
    setIsCollapsed(prev => {
      const next = !prev;
      try {
        localStorage.setItem(STORAGE_KEY, String(next));
      } catch {
        // ignore localStorage access errors
      }
      return next;
    });
  };

  if (isCollapsed) {
    return (
      <div
        className="di-banner-collapsed"
        onClick={toggleCollapse}
        role="region"
        aria-label="Event Rules Application Overview (Collapsed)"
      >
        <div className="di-banner-collapsed-content">
          <span className="di-banner-sticker di-banner-sticker--compact">Tech Asset</span>
          <span className="di-banner-collapsed-title">
            IBM Decision Intelligence - Event Rules — Real-time stateful decision agents powered by Apache Flink &amp; Kafka.
          </span>
        </div>
        <button
          type="button"
          className="di-banner-toggle-btn"
          onClick={(e) => {
            e.stopPropagation();
            toggleCollapse();
          }}
          aria-expanded="false"
          aria-label="Expand banner"
          title="Expand banner"
        >
          <svg width="20" height="20" viewBox="0 0 32 32" fill="currentColor">
            <path d="M16 22L6 12l1.41-1.41L16 19.17l8.59-8.58L26 12z" />
          </svg>
        </button>
      </div>
    );
  }

  return (
    <section
      className="di-banner"
      aria-label="Event Rules Application Overview"
    >
      <button
        type="button"
        className="di-banner-toggle-btn di-banner-toggle-btn--expanded"
        onClick={toggleCollapse}
        aria-expanded="true"
        aria-label="Collapse banner"
        title="Collapse banner"
      >
        <svg width="20" height="20" viewBox="0 0 32 32" fill="currentColor">
          <path d="M16 10l10 10-1.41 1.41L16 12.83l-8.59 8.58L6 20z" />
        </svg>
      </button>

      <div className="di-banner-container">
        {/* Left column: Hero headline, description, and action links */}
        <div className="di-banner-hero">
          <div className="di-banner-sticker-wrapper">
            <span className="di-banner-sticker">
              <svg width="12" height="12" viewBox="0 0 32 32" fill="currentColor" aria-hidden="true" style={{ flexShrink: 0 }}>
                <path d="M28 6H4a2 2 0 00-2 2v16a2 2 0 002 2h24a2 2 0 002-2V8a2 2 0 00-2-2zm0 18H4V8h24v16z"/>
                <path d="M9 12h14v2H9zm0 5h8v2H9z"/>
              </svg>
              Tech Asset
            </span>
          </div>
          <h1 className="di-banner-title">
            Confluent + IBM Decision Intelligence: Turn streaming events into intelligent, real-time decisions.
          </h1>
          <p className="di-banner-desc">
            Event Rules ingest Kafka event streams, maintain bounded entity state in Apache Flink, and evaluate business policies in-process using IBM Decision Intelligence rulesets — delivering low-latency, transparent, and auditable event-based decisions at scale.
          </p>
          <div className="di-banner-links">
            <button
              type="button"
              className="di-banner-link-btn"
              onClick={() => onNewApplication && onNewApplication()}
            >
              <span>Build Event Rule Decision Agents with the Diagram Editor</span>
              <span className="di-banner-link-arrow">→</span>
            </button>
            <a
              href="#scenarios-panel"
              className="di-banner-link"
              onClick={(e) => {
                const el = document.querySelector('.di-main-content');
                if (el) {
                  e.preventDefault();
                  el.scrollIntoView({ behavior: 'smooth' });
                }
              }}
            >
              <span>Explore pre-built sample applications and live scenarios</span>
              <span className="di-banner-link-arrow">→</span>
            </a>
            <a
              href="https://www.ibm.com/products/decision-intelligence"
              target="_blank"
              rel="noopener noreferrer"
              className="di-banner-link"
            >
              <span>{"See what's ahead: Explore IBM Decision Intelligence"}</span>
              <svg width="14" height="14" viewBox="0 0 32 32" fill="currentColor" className="di-banner-external-icon" aria-hidden="true">
                <path d="M26 28H6a2 2 0 01-2-2V6a2 2 0 012-2h10v2H6v20h20V16h2v10a2 2 0 01-2 2z" />
                <path d="M20 2v2h6.586L18 12.586 19.414 14 28 5.414V12h2V2H20z" />
              </svg>
            </a>
          </div>
        </div>

        {/* Right column: 2x2 Feature highlight grid reflecting application architecture */}
        <div className="di-banner-features">
          {/* Feature 1: Stateful Stream Processing */}
          <div className="di-banner-feature-item">
            <div className="di-banner-feature-icon" aria-hidden="true">
              <svg width="32" height="32" viewBox="0 0 32 32" fill="none">
                <path d="M4 15c4-5 8-5 12 0s8 5 12 0" stroke="#0f62fe" strokeWidth="2" strokeLinecap="round" />
                <path d="M4 22c4-5 8-5 12 0s8 5 12 0" stroke="#0f62fe" strokeWidth="2" strokeLinecap="round" strokeOpacity="0.4" />
                <circle cx="10" cy="12.5" r="2.5" fill="#0f62fe" />
                <circle cx="22" cy="17.5" r="2.5" fill="#0f62fe" />
              </svg>
            </div>
            <h2 className="di-banner-feature-title">Stateful Stream Processing</h2>
            <p className="di-banner-feature-desc">
              Maintains a stateful situational context in Apache Flink, evaluating incoming events in rich historical context.
            </p>
          </div>

          {/* Feature 2: Rules in Plain Business Language */}
          <div className="di-banner-feature-item">
            <div className="di-banner-feature-icon" aria-hidden="true">
              <svg width="32" height="32" viewBox="0 0 32 32" fill="none">
                <text x="3" y="10" fontSize="9" fontWeight="600" fill="#0f62fe" fontFamily="'IBM Plex Sans', sans-serif">A</text>
                <text x="18" y="10" fontSize="9" fontWeight="600" fill="#0f62fe" fontFamily="'IBM Plex Sans', sans-serif">文</text>
                <circle cx="16" cy="22" r="2.5" fill="#0f62fe" />
                <path d="M16 19.5V12M13 14.5L16 11.5L19 14.5" stroke="#0f62fe" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
              </svg>
            </div>
            <h2 className="di-banner-feature-title">Rules in Plain Business Language</h2>
            <p className="di-banner-feature-desc">
              Author natural-language rules in IBM Decision Designer to apply time-based reasoning on events and detect emerging situations.
            </p>
          </div>

          {/* Feature 3: Zero-Downtime Hot Reload */}
          <div className="di-banner-feature-item">
            <div className="di-banner-feature-icon" aria-hidden="true">
              <svg width="32" height="32" viewBox="0 0 32 32" fill="none">
                <path d="M26 16A10 10 0 1118 6.32" stroke="#0f62fe" strokeWidth="2" strokeLinecap="round" />
                <path d="M18 2v5h5" stroke="#0f62fe" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
                <circle cx="16" cy="16" r="3" fill="#0f62fe" />
              </svg>
            </div>
            <h2 className="di-banner-feature-title">Zero-Downtime Rule Hot Reload</h2>
            <p className="di-banner-feature-desc">
              Deploy updated event processing logic dynamically via Kafka broadcast state with zero restarts or downtime.
            </p>
          </div>

          {/* Feature 4: Dual Runtime Architecture */}
          <div className="di-banner-feature-item">
            <div className="di-banner-feature-icon" aria-hidden="true">
              <svg width="32" height="32" viewBox="0 0 32 32" fill="none">
                <rect x="4" y="22" width="5" height="8" rx="1" fill="#a6c8ff" />
                <rect x="11" y="16" width="5" height="14" rx="1" fill="#78a9ff" />
                <rect x="18" y="10" width="5" height="20" rx="1" fill="#4589ff" />
                <rect x="25" y="4" width="5" height="26" rx="1" fill="#0f62fe" />
              </svg>
            </div>
            <h2 className="di-banner-feature-title">Dual Runtime Architecture</h2>
            <p className="di-banner-feature-desc">
              Execute agent chains on high-throughput DataStream API or declarative Flink SQL Process Table Functions.
            </p>
          </div>
        </div>
      </div>
    </section>
  );
}
