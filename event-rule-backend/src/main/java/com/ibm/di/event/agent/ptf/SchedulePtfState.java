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
package com.ibm.di.event.agent.ptf;

import java.util.HashMap;
import java.util.Map;

/**
 * Per-entity schedule state of the Process Table Function engine.
 *
 * <p>It is kept apart from {@link AgentPtfState} because the entity state is cleared by the
 * state time-to-live emulation while pending schedules must survive it (an expired schedule would
 * silently never fire).
 */
public class SchedulePtfState {

    /** The entity id, kept so that a timer can build a {@code Schedule} event without a row. */
    public String entityId = null;

    /** Pending {@code ScheduleRequest}s, schedule id to the request serialized as JSON. */
    public Map<String, String> requests = new HashMap<>();

    public SchedulePtfState() {}
}
