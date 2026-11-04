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
package com.ibm.di.event.agent.datastream;

import com.ibm.di.event.agent.EventEnvelope;
import com.ibm.di.event.agent.ScheduleRequest;
import com.ibm.di.event.agent.ScheduleRequests;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.streaming.api.TimerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Keeps the {@link ScheduleRequest}s of the current entity in keyed state and keeps the Flink
 * event-time timers in sync with them.
 *
 * <p>Two {@link MapState}s are used, both scoped to the current key (the entity):
 * <ul>
 *   <li>{@code byId}: schedule id to request, used to replace or cancel a schedule by id;</li>
 *   <li>{@code idsByTime}: timepoint (epoch ms) to the ids registered at that timepoint, used to
 *       know when the last request of a timepoint is gone and its timer can be deleted, and to
 *       find the requests to fire in {@code onTimer}.</li>
 * </ul>
 *
 * <p>Several requests may share a timepoint; the Flink timer for a timepoint is deleted only when
 * no request is left for it.
 *
 * <p>This class has no dependency on the Flink runtime beyond the {@link MapState} and
 * {@link TimerService} interfaces, so it can be unit-tested with in-memory implementations.
 */
public class ScheduleManager {

    private static final Logger LOG = LoggerFactory.getLogger(ScheduleManager.class);

    private final MapState<String, ScheduleRequest> byId;
    private final MapState<Long, List<String>>      idsByTime;
    private final String                            tag;

    public ScheduleManager(MapState<String, ScheduleRequest> byId,
                           MapState<Long, List<String>> idsByTime,
                           String tag) {
        this.byId      = byId;
        this.idsByTime = idsByTime;
        this.tag       = tag;
    }

    // ── Applying requests ─────────────────────────────────────────────────────

    /** Applies the requests in order. */
    public void apply(List<ScheduleRequest> requests, TimerService timerService) throws Exception {
        for (ScheduleRequest r : requests) {
            switch (r.getStatus()) {
                case START     -> start(r, timerService);
                case CANCEL    -> {
                    if (!cancel(r.getId(), timerService)) {
                        LOG.debug("[{}] CANCEL ignored: no schedule with id={}", tag, r.getId());
                    }
                }
                case CANCELALL -> cancelAll(timerService);
            }
        }
    }

    /** Registers the schedule; an existing schedule with the same id is cancelled first. */
    private void start(ScheduleRequest r, TimerService timerService) throws Exception {
        cancel(r.getId(), timerService);
        long ms = r.timepointMs();
        byId.put(r.getId(), r);
        List<String> ids = idsByTime.get(ms);
        List<String> updated = ids == null ? new ArrayList<>() : new ArrayList<>(ids);
        updated.add(r.getId());
        idsByTime.put(ms, updated);
        timerService.registerEventTimeTimer(ms);
        LOG.debug("[{}] Schedule started id={} timepoint={}", tag, r.getId(), r.getTimepoint());
    }

    /**
     * Removes the schedule with the given id. The Flink timer is deleted only when no other
     * request is registered at the same timepoint.
     *
     * @return {@code true} if a schedule with this id existed
     */
    private boolean cancel(String id, TimerService timerService) throws Exception {
        ScheduleRequest existing = byId.get(id);
        if (existing == null) return false;
        byId.remove(id);
        long ms = existing.timepointMs();
        List<String> ids = idsByTime.get(ms);
        if (ids != null) {
            List<String> remaining = new ArrayList<>(ids);
            remaining.remove(id);
            if (remaining.isEmpty()) {
                idsByTime.remove(ms);
                timerService.deleteEventTimeTimer(ms);
            } else {
                idsByTime.put(ms, remaining);
            }
        }
        LOG.debug("[{}] Schedule cancelled id={}", tag, id);
        return true;
    }

    /** Cancels every schedule of the current entity. */
    public void cancelAll(TimerService timerService) throws Exception {
        List<Long> timepoints = new ArrayList<>();
        for (Long ms : idsByTime.keys()) timepoints.add(ms);
        for (Long ms : timepoints) timerService.deleteEventTimeTimer(ms);
        idsByTime.clear();
        byId.clear();
        LOG.debug("[{}] All schedules cancelled ({} timepoint(s))", tag, timepoints.size());
    }

    // ── Firing ────────────────────────────────────────────────────────────────

    /**
     * Removes the requests registered at {@code timestamp} from state and returns one
     * {@code Schedule} event per request. Called from {@code onTimer}; Flink has already
     * discarded the fired timer so it is not deleted here.
     *
     * <p>The state is updated <em>before</em> the events are processed by the caller, so a rule
     * that reacts to a {@code Schedule} event by starting a new schedule sees consistent state.
     *
     * @param timestamp the timer timestamp passed to {@code onTimer}
     * @param entityId  the entity key of the current timer
     */
    public List<EventEnvelope> fire(long timestamp, String entityId) throws Exception {
        List<String> ids = idsByTime.get(timestamp);
        if (ids == null) return Collections.emptyList();
        idsByTime.remove(timestamp);

        List<EventEnvelope> events = new ArrayList<>();
        for (String id : ids) {
            ScheduleRequest r = byId.get(id);
            if (r == null) continue;
            byId.remove(id);
            events.add(ScheduleRequests.buildEvent(r, timestamp, entityId));
        }
        return events;
    }

    /**
     * Return the list of planned scheduled requests still registered a timestamp and prior timepoints.
     * Return an ordered list, from the oldest timestamp to the newest
     */
    public List<Long> getAllScheduledTimestamps(long timestamp, String currentKey) throws Exception {
        List<Long> result = new ArrayList<>();
        for (Long ms : idsByTime.keys()) {
            if (ms <= timestamp) result.add(ms);
        }
        Collections.sort(result);
        return result;
    }
}
