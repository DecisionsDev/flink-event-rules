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
package com.ibm.di.event.agent;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.io.Serial;
import java.io.Serializable;
import java.util.Map;

/**
 * One field copied by an enrichment: the reference column {@link #getSource()} written under the
 * name {@link #getTarget()}.
 *
 * <p>JSON forms: {@code "name"} (same name on both sides) or {@code {"checkInStatus":
 * "check-inStatus"}} (rename).
 */
public final class FieldMapping implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private final String source;
    private final String target;

    public FieldMapping(String source, String target) {
        if (source == null || source.isBlank()) {
            throw new IllegalArgumentException("Enrichment field name must not be blank");
        }
        this.source = source;
        this.target = target == null || target.isBlank() ? source : target;
    }

    @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
    public static FieldMapping fromJson(Object value) {
        if (value instanceof String s) return new FieldMapping(s, s);
        if (value instanceof Map<?, ?> m && m.size() == 1) {
            Map.Entry<?, ?> e = m.entrySet().iterator().next();
            if (e.getKey() instanceof String k && e.getValue() instanceof String v) {
                return new FieldMapping(k, v);
            }
        }
        throw new IllegalArgumentException("An enrichment field is either \"name\" or "
            + "{\"column\": \"newName\"}, got: " + value);
    }

    /** Column of the reference data. */
    public String getSource() { return source; }

    /** Name of the property written in the event. */
    public String getTarget() { return target; }

    @JsonValue
    public Object toJson() {
        return source.equals(target) ? source : Map.of(source, target);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof FieldMapping f && source.equals(f.source) && target.equals(f.target);
    }

    @Override
    public int hashCode() {
        return 31 * source.hashCode() + target.hashCode();
    }

    @Override
    public String toString() {
        return source.equals(target) ? source : source + "->" + target;
    }
}
