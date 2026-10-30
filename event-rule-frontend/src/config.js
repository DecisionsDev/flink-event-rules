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

/**
 * Centralised application configuration.
 *
 * API_URL — base URL of the BFF.  Injected at build time via the
 * VITE_API_URL environment variable; falls back to the local Vite dev-server
 * default so development works without extra configuration.
 *
 * Importing from this module instead of repeating the inline expression in
 * every component ensures that all components use the same resolved value and
 * that the default only needs to be changed in one place (L-2).
 */
// When VITE_API_URL is unset, assume the BFF runs on port 8080 of the host that
// served the UI.  A hard-coded "localhost" would point a remote user's browser
// at their own machine.  On a local machine this still resolves to
// http://localhost:8080.
const defaultApiUrl = () => {
  const loc = typeof window !== 'undefined' ? window.location : null;
  return loc && loc.hostname ? `${loc.protocol}//${loc.hostname}:8080` : 'http://localhost:8080';
};

export const API_URL = import.meta.env.VITE_API_URL || defaultApiUrl();

/**
 * FLINK_DASHBOARD_URL — browser-reachable URL of the Flink web dashboard.
 * Override at build time with VITE_FLINK_DASHBOARD_URL; otherwise assumes the
 * JobManager REST/UI port (8081) on the host that served the UI.
 */
const defaultFlinkDashboardUrl = () => {
  const loc = typeof window !== 'undefined' ? window.location : null;
  return loc && loc.hostname ? `${loc.protocol}//${loc.hostname}:8081` : 'http://localhost:8081';
};

export const FLINK_DASHBOARD_URL =
  import.meta.env.VITE_FLINK_DASHBOARD_URL || defaultFlinkDashboardUrl();

