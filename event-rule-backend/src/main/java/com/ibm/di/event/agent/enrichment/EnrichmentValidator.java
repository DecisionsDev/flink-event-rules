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

import com.ibm.di.event.agent.AgentDescriptor;
import com.ibm.di.event.agent.EnrichmentConfig;
import com.ibm.di.event.agent.EventSubscription;
import com.ibm.di.event.agent.OutputConfig;
import com.ibm.di.event.agent.ReferenceConfig;
import com.jayway.jsonpath.InvalidPathException;
import com.jayway.jsonpath.JsonPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Checks the {@code references} and {@code enrichment} sections of an agent descriptor.
 *
 * <p>{@link #validate(AgentDescriptor, boolean)} with {@code loadData = false} is structural and
 * touches no file; with {@code true} it also builds an {@link Enricher} per enrichment, which
 * reads the reference files and checks the columns the steps use.
 */
public final class EnrichmentValidator {

    private static final Logger LOG = LoggerFactory.getLogger(EnrichmentValidator.class);

    private EnrichmentValidator() {}

    /**
     * @throws IllegalArgumentException describing the first problem found
     */
    public static void validate(AgentDescriptor agent, boolean loadData) {
        String who = "agentId=" + agent.getAgentId() + ": ";
        List<ReferenceConfig> references = agent.getReferences();

        if (references != null) {
            Set<String> names = new HashSet<>();
            for (ReferenceConfig r : references) {
                if (r == null || r.getName() == null || r.getName().isBlank()) {
                    throw new IllegalArgumentException(who + "every reference needs a 'name'");
                }
                if (!names.add(r.getName())) {
                    throw new IllegalArgumentException(who + "duplicate reference name '" + r.getName() + "'");
                }
                if (!ReferenceConfig.TYPE_CSV.equalsIgnoreCase(r.getType())) {
                    throw new IllegalArgumentException(who + "reference '" + r.getName()
                        + "': unsupported type '" + r.getType() + "' (only 'csv')");
                }
                if (r.getFile() == null || r.getFile().isBlank()) {
                    throw new IllegalArgumentException(who + "reference '" + r.getName() + "' needs a 'file'");
                }
            }
        }

        if (agent.getSubscriptions() != null) {
            for (EventSubscription sub : agent.getSubscriptions()) {
                if (sub.hasEnrichment()) {
                    check(who + "subscription '" + sub.getEventType() + "'", "", sub.getEnrichment(),
                        references, loadData);
                }
            }
        }
        if (agent.getOutputs() != null) {
            for (OutputConfig out : agent.getOutputs()) {
                if (!out.hasEnrichment()) continue;
                if (out.getEventType() == null || out.getEventType().isBlank()) {
                    throw new IllegalArgumentException(who + "an output with 'enrichment' needs an 'eventType'");
                }
                check(who + "output '" + out.getEventType() + "'", "payload", out.getEnrichment(),
                    references, loadData);
            }
        }
    }

    private static void check(String where, String root, List<EnrichmentConfig> steps,
                              List<ReferenceConfig> references, boolean loadData) {
        for (EnrichmentConfig s : steps) {
            if (s == null) throw new IllegalArgumentException(where + ": null enrichment step");
            require(where, s, "reference", s.getReference());
            require(where, s, "key", s.getKey());
            require(where, s, "on", s.getOn());
            require(where, s, "into", s.getInto());
            if (s.getFields().isEmpty()) {
                throw new IllegalArgumentException(where + ": step " + s + " lists no 'fields'");
            }
            try {
                IntoPath.parse(s.getInto());
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(where + ": step " + s + ": " + e.getMessage());
            }
            compilePath(where, s, "on", s.getOn());
            if (s.getWhen() != null && !s.getWhen().isBlank()) {
                compilePath(where, s, "when", s.getWhen());
            }
        }

        EnrichmentSpec spec;
        try {
            spec = EnrichmentSpec.of(root, steps, references);
            EnrichmentPlanner.order(steps);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(where + ": " + e.getMessage());
        }

        for (EnrichmentConfig s : steps) {
            IntoPath into = IntoPath.parse(s.getInto());
            if (into.each() && steps.stream().noneMatch(o -> o != s
                    && !IntoPath.parse(o.getInto()).each()
                    && IntoPath.parse(o.getInto()).segments().equals(into.segments()))) {
                LOG.warn("{}: step {} enriches the elements of {}, which no step fills: the event must "
                    + "carry that array itself.", where, s, s.getInto());
            }
        }

        if (loadData) {
            try {
                new Enricher(spec);
            } catch (RuntimeException e) {
                throw new IllegalArgumentException(where + ": " + e.getMessage(), e);
            }
        }
    }

    private static void compilePath(String where, EnrichmentConfig s, String name, String path) {
        try {
            JsonPath.compile(path);
        } catch (InvalidPathException e) {
            throw new IllegalArgumentException(where + ": step " + s + ": invalid '" + name + "' path '"
                + path + "': " + e.getMessage());
        }
    }

    private static void require(String where, EnrichmentConfig s, String name, String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(where + ": enrichment step " + s + " needs a '" + name + "'");
        }
    }
}
