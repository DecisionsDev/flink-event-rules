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

import com.ibm.di.event.agent.EnrichmentConfig;

import java.util.ArrayList;
import java.util.List;

/**
 * Orders the steps of an enrichment so that every step runs after the steps whose result it
 * needs. The order is otherwise the declared one.
 *
 * <p>Step B depends on step A when
 * <ul>
 *   <li>B writes into every element of an array ({@code $.a.b[*]}) and A creates that array
 *       ({@code into} is exactly {@code $.a.b}); or</li>
 *   <li>B reads ({@code on}, a plain dotted path) below the object A writes ({@code into} is
 *       {@code $.a} or {@code $.a.x}, and B reads {@code $.a.x.y}).</li>
 * </ul>
 * A step into {@code $} creates no dependency by itself.
 */
final class EnrichmentPlanner {

    private EnrichmentPlanner() {}

    /**
     * @return the indexes of {@code steps} in execution order
     * @throws IllegalArgumentException when the steps depend on each other in a cycle
     */
    static List<Integer> order(List<EnrichmentConfig> steps) {
        int n = steps.size();
        IntoPath[] into = new IntoPath[n];
        List<List<String>> reads = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            into[i] = IntoPath.parse(steps.get(i).getInto());
            reads.add(IntoPath.simpleSegments(steps.get(i).getOn()));
        }

        boolean[][] dependsOn = new boolean[n][n];
        for (int b = 0; b < n; b++) {
            for (int a = 0; a < n; a++) {
                if (a == b || into[a].isRoot() || into[a].each()) continue;
                boolean fillsArray = into[b].each() && into[b].segments().equals(into[a].segments());
                List<String> read = reads.get(b);
                boolean readsResult = read != null && !into[b].each()
                    && startsWith(read, into[a].segments());
                dependsOn[b][a] = fillsArray || readsResult;
            }
        }

        List<Integer> order = new ArrayList<>();
        boolean[] done = new boolean[n];
        while (order.size() < n) {
            int next = -1;
            for (int i = 0; i < n && next < 0; i++) {
                if (done[i]) continue;
                boolean ready = true;
                for (int a = 0; a < n; a++) {
                    if (dependsOn[i][a] && !done[a]) { ready = false; break; }
                }
                if (ready) next = i;
            }
            if (next < 0) {
                throw new IllegalArgumentException("The enrichment steps depend on each other in a cycle: "
                    + steps.stream().filter(s -> !done[steps.indexOf(s)]).map(EnrichmentConfig::toString).toList());
            }
            done[next] = true;
            order.add(next);
        }
        return order;
    }

    private static boolean startsWith(List<String> path, List<String> prefix) {
        return path.size() > prefix.size() && path.subList(0, prefix.size()).equals(prefix);
    }
}
