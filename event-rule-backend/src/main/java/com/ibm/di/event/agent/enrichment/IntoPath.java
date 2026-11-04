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
package com.ibm.di.event.agent.enrichment;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The restricted JSONPath accepted as {@code into} target of an enrichment: {@code $},
 * {@code $.a.b} or {@code $.a.b[*]}. Only the last element may be {@code [*]}. Property names are
 * letters, digits, {@code _} and {@code -}.
 *
 * @param segments property names from the root, empty for {@code $}
 * @param each     whether the target is every element of the array at {@code segments}
 */
record IntoPath(List<String> segments, boolean each) {

    private static final Pattern SEGMENT = Pattern.compile("\\.([A-Za-z0-9_\\-]+)");

    /** {@code true} for {@code $}. */
    boolean isRoot() {
        return segments.isEmpty() && !each;
    }

    /**
     * @throws IllegalArgumentException when {@code into} is not in the supported grammar
     */
    static IntoPath parse(String into) {
        if (into == null || !into.startsWith("$")) {
            throw new IllegalArgumentException("'into' must be a path starting with '$': " + into);
        }
        String rest = into.substring(1);
        boolean each = false;
        if (rest.endsWith("[*]")) {
            each = true;
            rest = rest.substring(0, rest.length() - 3);
        }
        List<String> segments = new ArrayList<>();
        int pos = 0;
        Matcher m = SEGMENT.matcher(rest);
        while (pos < rest.length()) {
            m.region(pos, rest.length());
            if (!m.lookingAt()) {
                throw new IllegalArgumentException("'into' supports only '$', '$.a.b' and '$.a.b[*]': " + into);
            }
            segments.add(m.group(1));
            pos = m.end();
        }
        if (each && segments.isEmpty()) {
            throw new IllegalArgumentException("'into' [*] needs an array property, as in '$.items[*]': " + into);
        }
        return new IntoPath(List.copyOf(segments), each);
    }

    /**
     * The property names of a plain dotted path ({@code $.a.b}), or {@code null} when {@code path}
     * contains anything else (filter, wildcard, index, recursive descent).
     */
    static List<String> simpleSegments(String path) {
        if (path == null || !path.startsWith("$")) return null;
        String rest = path.substring(1);
        List<String> segments = new ArrayList<>();
        int pos = 0;
        Matcher m = SEGMENT.matcher(rest);
        while (pos < rest.length()) {
            m.region(pos, rest.length());
            if (!m.lookingAt()) return segments.isEmpty() ? null : segments;
            segments.add(m.group(1));
            pos = m.end();
        }
        return segments;
    }

    boolean startsWith(List<String> prefix) {
        return segments.size() >= prefix.size() && segments.subList(0, prefix.size()).equals(prefix);
    }

    @Override
    public String toString() {
        return "$" + segments.stream().map(s -> "." + s).reduce("", String::concat) + (each ? "[*]" : "");
    }
}
