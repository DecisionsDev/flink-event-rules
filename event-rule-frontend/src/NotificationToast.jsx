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
import React, { useEffect, useRef, useState } from 'react';

/**
 * NotificationToast
 *
 * Renders a single generic event notification.  The component is completely
 * domain-agnostic: it depends only on the generic notification record shape
 * produced by useKafkaNotifications.
 *
 * Props:
 *   notification  — { id, type, label, timestamp, message }
 *   onDismiss     — () => void  called when the user or timer dismisses it
 *   autoDismissMs — number, default 5000
 */
function NotificationToast({ notification, onDismiss, autoDismissMs = 5000 }) {
  const [visible, setVisible] = useState(false);
  const [leaving, setLeaving] = useState(false);
  const timerRef = useRef(null);

  // Keep a stable ref to onDismiss and notification.id so the auto-dismiss
  // timer always calls the latest callback without adding them to the effect
  // dependency array (which would reset the timer on every render).
  const onDismissRef      = useRef(onDismiss);
  const notificationIdRef = useRef(notification.id);
  useEffect(() => { onDismissRef.current      = onDismiss;       }, [onDismiss]);
  useEffect(() => { notificationIdRef.current = notification.id; }, [notification.id]);

  // Entry animation: mount → schedule a microtask to flip `visible` so the
  // CSS transition can play from the hidden state.
  useEffect(() => {
    const raf = requestAnimationFrame(() => setVisible(true));
    return () => cancelAnimationFrame(raf);
  }, []);

  // Auto-dismiss after autoDismissMs.  Uses refs so the timer always invokes
  // the current onDismiss without needing to be reset on every render.
  useEffect(() => {
    timerRef.current = setTimeout(() => handleDismiss(), autoDismissMs);
    return () => clearTimeout(timerRef.current);
  }, [autoDismissMs]);

  function handleDismiss() {
    clearTimeout(timerRef.current);
    setLeaving(true);
    // Wait for exit transition before removing from DOM
    setTimeout(() => onDismissRef.current(notificationIdRef.current), 280);
  }

  const timestamp = notification.timestamp
    ? new Date(notification.timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' })
    : '';

  const classNames = [
    'kafka-notification',
    visible && !leaving ? 'kafka-notification--visible' : '',
    leaving ? 'kafka-notification--leaving' : '',
  ].filter(Boolean).join(' ');

  return (
    <div className={classNames} role="status" aria-live="polite">
      <div className="kafka-notification__header">
        <span className="kafka-notification__label">{notification.label}</span>
        {timestamp && (
          <span className="kafka-notification__time">{timestamp}</span>
        )}
        <button
          className="kafka-notification__close"
          onClick={handleDismiss}
          aria-label="Dismiss notification"
        >
          ×
        </button>
      </div>
      <div className="kafka-notification__message">{notification.message}</div>
      {autoDismissMs > 0 && (
        <div
          className="kafka-notification__progress"
          style={{ animationDuration: `${autoDismissMs}ms` }}
        />
      )}
    </div>
  );
}

export default NotificationToast;
