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
 * Returns a random RFC 4122 version-4 UUID string.
 *
 * `crypto.randomUUID()` is only exposed in secure contexts (HTTPS or
 * localhost).  When the UI is served over plain HTTP from a remote host
 * (e.g. http://myhost:3010) the function is undefined and calling it throws,
 * which aborts module evaluation and leaves a blank page.
 *
 * `crypto.getRandomValues()` is available in insecure contexts as well and is
 * cryptographically secure, so it is used as the fallback.  The values are used
 * as React list keys only (never as credentials), but they are still generated
 * from a CSPRNG so collision guarantees are the same as the native API.
 */
export function newKey() {
  const c = globalThis.crypto;
  if (c && typeof c.randomUUID === 'function') {
    return c.randomUUID();
  }
  if (c && typeof c.getRandomValues === 'function') {
    const b = c.getRandomValues(new Uint8Array(16));
    b[6] = (b[6] & 0x0f) | 0x40; // version 4
    b[8] = (b[8] & 0x3f) | 0x80; // RFC 4122 variant
    const h = Array.from(b, (x) => x.toString(16).padStart(2, '0'));
    return `${h.slice(0, 4).join('')}-${h.slice(4, 6).join('')}-${h.slice(6, 8).join('')}-${h.slice(8, 10).join('')}-${h.slice(10).join('')}`;
  }
  throw new Error('No secure random source (crypto.getRandomValues) is available in this browser.');
}
