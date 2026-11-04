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
import { MODE_LABELS, MODE_DESCRIPTIONS } from './deploymentMode.js';
import './RuntimeBadge.css';

/**
 * RuntimeBadge
 *
 * The deployment-mode badge shown on every dashboard: "PTF" or "DataStream".  Renders
 * nothing for any other value (no deployment, unknown runtime), so it can never show two
 * modes at once or a mode that is not known.
 *
 * `mode` must come from deploymentMode.js (resolveDeploymentMode / useDeploymentMode) or
 * from a job's own `runtime` — never from the global runtime toggle.
 */
export default function RuntimeBadge({ mode, className = '' }) {
  const label = MODE_LABELS[mode];
  if (!label) return null;
  return (
    <span
      className={`runtime-badge runtime-badge--${mode}${className ? ` ${className}` : ''}`}
      title={MODE_DESCRIPTIONS[mode]}
      data-runtime-mode={mode}
    >
      {label}
    </span>
  );
}
