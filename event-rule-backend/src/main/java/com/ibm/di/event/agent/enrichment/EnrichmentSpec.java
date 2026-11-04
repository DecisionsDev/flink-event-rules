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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.di.event.agent.EnrichmentConfig;
import com.ibm.di.event.agent.ReferenceConfig;

import java.util.ArrayList;
import java.util.List;

/**
 * Self-contained description of one enrichment call, the argument of the {@code EVENT_ENRICH} SQL
 * function: the steps, the reference data sets they use, and where in the JSON value they apply.
 *
 * <p>JSON: {@code {"root": "payload", "references": [...], "steps": [...]}}. {@code root} is the
 * property of the value that holds the document to enrich; empty (the default) means the value
 * itself.
 */
public final class EnrichmentSpec {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Property of the JSON value to enrich, {@code ""} for the value itself. */
    public String root = "";
    public List<ReferenceConfig> references = new ArrayList<>();
    public List<EnrichmentConfig> steps = new ArrayList<>();

    public EnrichmentSpec() {}

    /**
     * @param all every reference of the agent; only the ones {@code steps} use are kept
     * @throws IllegalArgumentException when a step names a reference that does not exist
     */
    public static EnrichmentSpec of(String root, List<EnrichmentConfig> steps, List<ReferenceConfig> all) {
        EnrichmentSpec spec = new EnrichmentSpec();
        spec.root = root == null ? "" : root;
        spec.steps = new ArrayList<>(steps);
        for (EnrichmentConfig step : steps) {
            ReferenceConfig ref = all == null ? null : all.stream()
                .filter(r -> r.getName() != null && r.getName().equals(step.getReference()))
                .findFirst().orElse(null);
            if (ref == null) {
                throw new IllegalArgumentException("Enrichment step " + step + " uses the reference '"
                    + step.getReference() + "', which is not declared in 'references'");
            }
            if (spec.references.stream().noneMatch(r -> r.getName().equals(ref.getName()))) {
                spec.references.add(ref);
            }
        }
        return spec;
    }

    public String toJson() {
        try {
            return MAPPER.writeValueAsString(this);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot serialize the enrichment spec: " + e.getMessage(), e);
        }
    }

    public static EnrichmentSpec fromJson(String json) {
        try {
            return MAPPER.readerFor(EnrichmentSpec.class).readValue(json);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid enrichment spec: " + e.getMessage(), e);
        }
    }
}
