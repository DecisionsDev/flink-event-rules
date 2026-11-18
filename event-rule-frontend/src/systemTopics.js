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
 * System topics are the platform-generated outbound topics: entity change-logs
 * and agent errors. They are hidden in the dashboard unless "Show system topics"
 * is enabled.
 *
 * The BFF (buildUseCaseDescriptor) types the first change-log topic
 * 'entity-changelog' and every further one 'entity-changelog-<agentId>', so a
 * multi-agent use case (e.g. Air Traffic Control) has several change-log types.
 */
export const ENTITY_CHANGELOG_TYPE = 'entity-changelog';
export const AGENT_ERROR_TYPE      = 'agent-error';

/** True when a descriptor topic `type` denotes a system topic. */
export function isSystemTopicType(type) {
  if (typeof type !== 'string') return false;
  return type === AGENT_ERROR_TYPE
    || type === ENTITY_CHANGELOG_TYPE
    || type.startsWith(`${ENTITY_CHANGELOG_TYPE}-`);
}

/** True when a descriptor topic `type` denotes an entity change-log topic. */
export function isChangelogTopicType(type) {
  return typeof type === 'string'
    && (type === ENTITY_CHANGELOG_TYPE || type.startsWith(`${ENTITY_CHANGELOG_TYPE}-`));
}
