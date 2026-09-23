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
import NotificationToast from './NotificationToast';

/**
 * NotificationContainer
 *
 * Renders the stack of active notification toasts in a fixed overlay position.
 * Completely domain-agnostic — it delegates all rendering to NotificationToast
 * and receives only the generic notification list + callbacks from the caller.
 *
 * Props:
 *   notifications  — notification[] from useKafkaNotifications
 *   onDismiss      — (id: string) => void
 *   autoDismissMs  — number forwarded to each toast
 */
function NotificationContainer({ notifications, onDismiss, autoDismissMs }) {
  if (!notifications || notifications.length === 0) return null;

  return (
    <div className="kafka-notification-container" aria-label="Event notifications">
      {notifications.map(n => (
        <NotificationToast
          key={n.id}
          notification={n}
          onDismiss={onDismiss}
          autoDismissMs={autoDismissMs}
        />
      ))}
    </div>
  );
}

export default NotificationContainer;
