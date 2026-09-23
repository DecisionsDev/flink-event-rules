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
import JsonCardRenderer from './JsonCardRenderer';

/**
 * EventCardErrorBoundary
 *
 * Wraps individual event card renders.  When a card throws during rendering
 * (e.g. a malformed JSON payload produces an unexpected shape), this boundary
 * catches the error and renders a minimal fallback instead of propagating
 * the failure up to the root App and blanking the entire UI.
 *
 * Usage:
 *   <EventCardErrorBoundary eventType="fraud-alert" index={0}>
 *     {renderCard(event, topicType, index)}
 *   </EventCardErrorBoundary>
 */
export class EventCardErrorBoundary extends React.Component {
  constructor(props) {
    super(props);
    this.state = { hasError: false, error: null };
  }

  static getDerivedStateFromError(error) {
    return { hasError: true, error };
  }

  componentDidCatch(error, info) {
    console.error(
      '[EventCardErrorBoundary] Card render failed:',
      error,
      info.componentStack
    );
  }

  render() {
    if (this.state.hasError) {
      const { eventType, rawEvent } = this.props;
      return (
        <div className="data-item" style={{ borderLeft: '3px solid #f59e0b' }}>
          <div style={{ fontSize: '11px', color: '#b45309', marginBottom: 4 }}>
            ⚠ Card render failed for event type <strong>{eventType || 'unknown'}</strong>
          </div>
          {rawEvent && <JsonCardRenderer data={rawEvent} />}
        </div>
      );
    }
    return this.props.children;
  }
}

export default EventCardErrorBoundary;
