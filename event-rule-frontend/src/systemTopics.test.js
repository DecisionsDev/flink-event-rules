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
import { describe, test, expect } from 'vitest';
import { isSystemTopicType, isChangelogTopicType } from './systemTopics';

describe('isSystemTopicType', () => {
  test.each([
    'entity-changelog',
    'entity-changelog-airport-agent',
    'entity-changelog-a2',
    'agent-error',
  ])('%s is a system topic type', (type) => {
    expect(isSystemTopicType(type)).toBe(true);
  });

  test.each(['Alert', 'AircraftEvent', 'entity-changelogs', 'my-entity-changelog', '', undefined, null, 42])(
    '%s is not a system topic type',
    (type) => {
      expect(isSystemTopicType(type)).toBe(false);
    }
  );
});

describe('isChangelogTopicType', () => {
  test('matches the base and per-agent changelog types only', () => {
    expect(isChangelogTopicType('entity-changelog')).toBe(true);
    expect(isChangelogTopicType('entity-changelog-airport-agent')).toBe(true);
    expect(isChangelogTopicType('agent-error')).toBe(false);
    expect(isChangelogTopicType(undefined)).toBe(false);
  });
});
