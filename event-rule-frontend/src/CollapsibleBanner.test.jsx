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
import { describe, test, expect, beforeEach, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import CollapsibleBanner from './CollapsibleBanner';

describe('CollapsibleBanner Component', () => {
  beforeEach(() => {
    localStorage.clear();
  });

  test('renders full banner content when expanded by default', () => {
    render(<CollapsibleBanner />);

    // Tech asset sticker
    expect(screen.getByText('Tech Asset')).toBeInTheDocument();

    // Hero title
    expect(
      screen.getByRole('heading', {
        level: 1,
        name: /Turn streaming events into intelligent, real-time decisions/i,
      })
    ).toBeInTheDocument();

    // Hero description
    expect(
      screen.getByText(/Event Rules ingest Kafka event streams, maintain bounded entity state in Apache Flink/i)
    ).toBeInTheDocument();

    // 3 action links
    expect(
      screen.getByText(/Build Event Rule Decision Agents with the Diagram Editor/i)
    ).toBeInTheDocument();
    expect(
      screen.getByText(/Explore pre-built sample applications and live scenarios/i)
    ).toBeInTheDocument();
    expect(
      screen.getByText(/See what's ahead: Explore IBM Decision Intelligence/i)
    ).toBeInTheDocument();

    // 4 feature items reflecting the application intent
    expect(screen.getByText('Stateful Stream Processing')).toBeInTheDocument();
    expect(
      screen.getByText(/Maintains a stateful situational context in Apache Flink/i)
    ).toBeInTheDocument();

    expect(screen.getByText('Rules in Plain Business Language')).toBeInTheDocument();
    expect(
      screen.getByText(/Author natural-language rules in IBM Decision Designer/i)
    ).toBeInTheDocument();

    expect(screen.getByText('Zero-Downtime Rule Hot Reload')).toBeInTheDocument();
    expect(
      screen.getByText(/Deploy updated event processing logic dynamically via Kafka broadcast state/i)
    ).toBeInTheDocument();

    expect(screen.getByText('Dual Runtime Architecture')).toBeInTheDocument();
    expect(
      screen.getByText(/Execute agent chains on high-throughput DataStream API or declarative Flink SQL/i)
    ).toBeInTheDocument();
  });

  test('collapses and expands when toggle button is clicked', () => {
    render(<CollapsibleBanner />);

    const collapseBtn = screen.getByRole('button', { name: /collapse banner/i });
    expect(collapseBtn).toHaveAttribute('aria-expanded', 'true');

    // Click collapse
    fireEvent.click(collapseBtn);

    // Full hero and features are hidden
    expect(
      screen.queryByRole('heading', { level: 1, name: /Turn streaming events/i })
    ).not.toBeInTheDocument();
    expect(screen.queryByText('Stateful Stream Processing')).not.toBeInTheDocument();

    // Collapsed region is shown with expand button
    const expandBtn = screen.getByRole('button', { name: /expand banner/i });
    expect(expandBtn).toHaveAttribute('aria-expanded', 'false');

    // Click expand
    fireEvent.click(expandBtn);

    // Full banner is restored
    expect(
      screen.getByRole('heading', { level: 1, name: /Turn streaming events/i })
    ).toBeInTheDocument();
  });

  test('clicking collapsed strip expands the banner', () => {
    localStorage.setItem('di_banner_collapsed', 'true');
    render(<CollapsibleBanner />);

    const collapsedRegion = screen.getByRole('region', {
      name: /Event Rules Application Overview \(Collapsed\)/i,
    });
    expect(collapsedRegion).toBeInTheDocument();

    fireEvent.click(collapsedRegion);

    expect(
      screen.getByRole('heading', { level: 1, name: /Turn streaming events/i })
    ).toBeInTheDocument();
  });

  test('persists collapsed state to localStorage', () => {
    render(<CollapsibleBanner />);

    const collapseBtn = screen.getByRole('button', { name: /collapse banner/i });
    fireEvent.click(collapseBtn);
    expect(localStorage.getItem('di_banner_collapsed')).toBe('true');

    const expandBtn = screen.getByRole('button', { name: /expand banner/i });
    fireEvent.click(expandBtn);
    expect(localStorage.getItem('di_banner_collapsed')).toBe('false');
  });

  test('calls onNewApplication callback when clicking "Build Decision Agents" link', () => {
    const onNewAppMock = vi.fn();
    render(<CollapsibleBanner onNewApplication={onNewAppMock} />);

    const buildLinkBtn = screen.getByRole('button', {
      name: /Build Event Rule Decision Agents with the Diagram Editor/i,
    });
    fireEvent.click(buildLinkBtn);

    expect(onNewAppMock).toHaveBeenCalledTimes(1);
  });

  test('renders external roadmap link with target="_blank"', () => {
    render(<CollapsibleBanner />);

    const roadmapLink = screen.getByRole('link', {
      name: /See what's ahead: Explore IBM Decision Intelligence/i,
    });
    expect(roadmapLink).toHaveAttribute('href', 'https://www.ibm.com/products/decision-intelligence');
    expect(roadmapLink).toHaveAttribute('target', '_blank');
    expect(roadmapLink).toHaveAttribute('rel', 'noopener noreferrer');
  });
});
