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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.di.event.agent.ScheduleRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * The pending {@link ScheduleRequest}s of one entity, backed by a {@link SchedulePtfState}.
 *
 * <p>Process Table Functions have no {@code MapState}, so the requests live in one
 * {@code Map<String,String>} (schedule id to request JSON). Timepoints are derived from the
 * requests, so the map is the single source of truth and cannot drift from the timers: the caller
 * registers one named Flink timer per distinct timepoint.
 *
 * <p>Semantics match {@code ScheduleRequests}: starting a schedule replaces one with the same id,
 * several requests may share a timepoint, and cancelling an unknown id is ignored.
 */
public final class ScheduleBook {

    private static final Logger LOG = LoggerFactory.getLogger(ScheduleBook.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final SchedulePtfState state;

    public ScheduleBook(SchedulePtfState state) {
        this.state = state;
    }

    public String getEntityId() {
        return state.entityId;
    }

    /** Remembers the entity id, which a timer needs to build a {@code Schedule} event. */
    public void setEntityId(String entityId) {
        if (entityId != null) state.entityId = entityId;
    }

    /** True when no schedule is pending. */
    public boolean isEmpty() {
        return state.requests.isEmpty();
    }

    /** Applies the requests in order. */
    public void apply(List<ScheduleRequest> requests) {
        for (ScheduleRequest r : requests) {
            switch (r.getStatus()) {
                case START     -> start(r);
                case CANCEL    -> {
                    if (state.requests.remove(r.getId()) == null) {
                        LOG.debug("CANCEL ignored: no schedule with id={}", r.getId());
                    }
                }
                case CANCELALL -> cancelAll();
            }
        }
    }

    /** Cancels every schedule of the entity. */
    public void cancelAll() {
        state.requests.clear();
    }

    /** Distinct pending timepoints (epoch ms), ascending. */
    public List<Long> timepoints() {
        TreeSet<Long> result = new TreeSet<>();
        for (ScheduleRequest r : parsed()) result.add(r.timepointMs());
        return new ArrayList<>(result);
    }

    /** Distinct pending timepoints {@code <= timestamp}, ascending. */
    public List<Long> dueTimepoints(long timestamp) {
        List<Long> result = new ArrayList<>();
        for (Long ms : timepoints()) {
            if (ms <= timestamp) result.add(ms);
        }
        return result;
    }

    /** Removes and returns the requests registered at {@code timestamp}. */
    public List<ScheduleRequest> fire(long timestamp) {
        List<ScheduleRequest> fired = new ArrayList<>();
        for (ScheduleRequest r : parsed()) {
            if (r.timepointMs() == timestamp) {
                state.requests.remove(r.getId());
                fired.add(r);
            }
        }
        return fired;
    }

    private void start(ScheduleRequest r) {
        state.requests.put(r.getId(), write(r));
    }

    private List<ScheduleRequest> parsed() {
        if (state.requests.isEmpty()) return Collections.emptyList();
        List<ScheduleRequest> result = new ArrayList<>(state.requests.size());
        for (Map.Entry<String, String> e : state.requests.entrySet()) {
            try {
                result.add(MAPPER.readValue(e.getValue(), ScheduleRequest.class));
            } catch (Exception ex) {
                throw new IllegalStateException("Corrupted schedule state for id=" + e.getKey(), ex);
            }
        }
        return result;
    }

    private static String write(ScheduleRequest r) {
        try {
            return MAPPER.writeValueAsString(r);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot serialize " + r, e);
        }
    }
}
