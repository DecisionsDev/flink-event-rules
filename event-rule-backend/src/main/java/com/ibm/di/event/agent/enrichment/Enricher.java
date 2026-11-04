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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ibm.di.event.agent.EnrichmentConfig;
import com.ibm.di.event.agent.FieldMapping;
import com.ibm.di.event.agent.ReferenceConfig;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.InvalidPathException;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Applies reference-data enrichment steps to a JSON document. It does not depend on Flink.
 *
 * <p>Semantics (see {@link EnrichmentConfig}):
 * <ul>
 *   <li>the steps run in dependency order ({@link EnrichmentPlanner});</li>
 *   <li>a step whose {@code when} does not match, whose {@code on} value is missing, whose
 *       lookup finds no row, or whose {@code [*]} target is not an array, does nothing and the
 *       following steps still run;</li>
 *   <li>a {@code [*]} step enriches each object element of the array independently: an element
 *       without a row is left as it is;</li>
 *   <li>an empty CSV cell is not written;</li>
 *   <li>configuration errors (unknown reference, column, invalid path) are thrown by the
 *       constructor; a failure while enriching a document is logged and leaves that step's
 *       target unchanged, so a bad record never stops the stream.</li>
 * </ul>
 * Instances are immutable and thread-safe.
 */
public final class Enricher {

    private static final Logger LOG = LoggerFactory.getLogger(Enricher.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Configuration JSON_PATH = Configuration.builder()
        .jsonProvider(new JacksonJsonNodeJsonProvider())
        .mappingProvider(new JacksonMappingProvider())
        .build();

    private final String root;
    private final List<Step> steps = new ArrayList<>();

    /**
     * A step with everything resolved at construction. {@code keyColumns} are the names of the
     * columns used as lookup key by any step of the spec: a column of that name is always written
     * as a string (unless its type is declared), so that an identifier is never turned into a
     * number.
     */
    private record Step(EnrichmentConfig config, ReferenceTable table, IntoPath into, JsonPath on,
                        JsonPath when, boolean many, Set<String> keyColumns) {
        String name() { return config.getReference() + "->" + config.getInto(); }
    }

    /**
     * @throws IllegalArgumentException for an invalid configuration
     * @throws IllegalStateException    when a reference file cannot be read
     */
    public Enricher(EnrichmentSpec spec) {
        this.root = spec.root == null ? "" : spec.root;

        Map<String, ReferenceConfig> references = new HashMap<>();
        for (ReferenceConfig r : spec.references) references.put(r.getName(), r);
        Set<String> keyColumns = new HashSet<>();
        for (EnrichmentConfig c : spec.steps) keyColumns.add(c.getKey());

        for (int index : EnrichmentPlanner.order(spec.steps)) {
            EnrichmentConfig c = spec.steps.get(index);
            ReferenceConfig ref = references.get(c.getReference());
            if (ref == null) {
                throw new IllegalArgumentException("Step " + c + " uses the unknown reference '" + c.getReference() + "'");
            }
            ReferenceTable table = ReferenceRegistry.get(ref);
            steps.add(resolve(c, table, keyColumns));
        }
    }

    private static Step resolve(EnrichmentConfig c, ReferenceTable table, Set<String> keyColumns) {
        IntoPath into = IntoPath.parse(c.getInto());
        if (!table.hasColumn(c.getKey())) {
            throw new IllegalArgumentException("Step " + c + ": the reference has no key column '" + c.getKey()
                + "' (columns: " + table.columns() + ")");
        }
        if (c.getFields().isEmpty()) {
            throw new IllegalArgumentException("Step " + c + " lists no 'fields'");
        }
        for (FieldMapping f : c.getFields()) {
            if (!table.hasColumn(f.getSource())) {
                throw new IllegalArgumentException("Step " + c + ": the reference has no column '"
                    + f.getSource() + "' (columns: " + table.columns() + ")");
            }
        }
        boolean many = resolveCardinality(c, into, table);
        return new Step(c, table, into, compile(c.getOn(), "on", c), c.getWhen() == null || c.getWhen().isBlank()
            ? null : compile(c.getWhen(), "when", c), many, keyColumns);
    }

    private static boolean resolveCardinality(EnrichmentConfig c, IntoPath into, ReferenceTable table) {
        String cardinality = c.getCardinality();
        if (cardinality != null && !cardinality.isBlank()
                && !EnrichmentConfig.ONE.equals(cardinality) && !EnrichmentConfig.MANY.equals(cardinality)) {
            throw new IllegalArgumentException("Step " + c + ": cardinality must be 'one' or 'many'");
        }
        if (into.each() || into.isRoot()) {
            if (EnrichmentConfig.MANY.equals(cardinality)) {
                throw new IllegalArgumentException("Step " + c + ": cardinality 'many' cannot be used with into '"
                    + c.getInto() + "'");
            }
            return false;
        }
        if (cardinality == null || cardinality.isBlank()) return table.hasDuplicateKeys(c.getKey());
        return EnrichmentConfig.MANY.equals(cardinality);
    }

    private static JsonPath compile(String path, String what, EnrichmentConfig c) {
        if (path == null || path.isBlank()) {
            throw new IllegalArgumentException("Step " + c + " has no '" + what + "'");
        }
        try {
            return JsonPath.compile(path);
        } catch (InvalidPathException e) {
            throw new IllegalArgumentException("Step " + c + ": invalid '" + what + "' path '" + path + "': " + e.getMessage());
        }
    }

    // ── Enrichment ────────────────────────────────────────────────────────────

    /**
     * Enriches {@code json}.
     *
     * @return the enriched document, or {@code json} itself when it is null, blank, not an object
     *         (or has no object at {@code root}), unparseable, or no step changed anything
     */
    public String enrich(String json) {
        if (json == null || json.isBlank() || steps.isEmpty()) return json;
        JsonNode tree;
        try {
            tree = MAPPER.readTree(json);
        } catch (Exception e) {
            LOG.warn("Not enriching a value that is not valid JSON: {}", e.getMessage());
            return json;
        }
        return enrichTree(tree) ? tree.toString() : json;
    }

    /**
     * Enriches {@code tree} in place. This is the entry point of engines that already hold a
     * parsed document (the DataStream engine); {@link #enrich(String)} is built on it.
     *
     * @return whether the document was changed; {@code false} when it is null, not an object
     *         (or has no object at {@code root}), or no step changed anything
     */
    public boolean enrichTree(JsonNode tree) {
        if (tree == null || steps.isEmpty()) return false;
        JsonNode doc = root.isEmpty() ? tree : tree.get(root);
        if (!(doc instanceof ObjectNode document)) return false;

        boolean changed = false;
        for (Step step : steps) {
            try {
                changed |= apply(step, document);
            } catch (RuntimeException e) {
                LOG.warn("Enrichment step {} failed and was skipped: {}", step.name(), e.toString());
            }
        }
        return changed;
    }

    private boolean apply(Step step, ObjectNode document) {
        if (step.into().each()) {
            JsonNode array = navigate(document, step.into().segments());
            if (!(array instanceof ArrayNode elements)) return false;
            boolean changed = false;
            for (JsonNode element : elements) {
                if (element instanceof ObjectNode object && applies(step, object)) {
                    List<String[]> rows = rowsFor(step, object);
                    if (!rows.isEmpty()) changed |= merge(step, object, rows.get(0));
                }
            }
            return changed;
        }

        if (!applies(step, document)) return false;
        List<String[]> rows = rowsFor(step, document);
        if (rows.isEmpty()) return false;

        if (step.many()) {
            ArrayNode array = MAPPER.createArrayNode();
            for (String[] row : rows) {
                ObjectNode element = MAPPER.createObjectNode();
                merge(step, element, row);
                array.add(element);
            }
            parent(document, step.into().segments()).set(last(step.into().segments()), array);
            return true;
        }
        ObjectNode target = step.into().isRoot() ? document : objectAt(document, step.into().segments());
        return merge(step, target, rows.get(0));
    }

    /** Whether the {@code when} filter selects something (a non-empty result, or {@code true}). */
    private static boolean applies(Step step, JsonNode node) {
        if (step.when() == null) return true;
        try {
            // The JSON provider unwraps scalars to Java values and returns JsonNode containers.
            Object result = step.when().read(node, JSON_PATH);
            if (result == null) return false;
            if (result instanceof JsonNode n) {
                if (n.isNull() || n.isMissingNode()) return false;
                if (n.isArray() || n.isObject()) return !n.isEmpty();
                if (n.isBoolean()) return n.booleanValue();
                return true;
            }
            if (result instanceof Boolean b) return b;
            return true;
        } catch (PathNotFoundException e) {
            return false;
        }
    }

    private static List<String[]> rowsFor(Step step, JsonNode node) {
        String value = lookupValue(step.on(), node);
        return value == null ? List.of() : step.table().lookup(step.config().getKey(), value);
    }

    /** The scalar found by {@code on} as a string; {@code null} when absent, null or not a scalar. */
    private static String lookupValue(JsonPath on, JsonNode node) {
        try {
            Object v = on.read(node, JSON_PATH);
            if (v == null) return null;
            if (v instanceof JsonNode n) return n.isValueNode() && !n.isNull() ? n.asText() : null;
            return v.toString();
        } catch (PathNotFoundException e) {
            return null;
        }
    }

    /** Writes the step's fields of {@code row} into {@code target}; whether anything was written. */
    private static boolean merge(Step step, ObjectNode target, String[] row) {
        boolean changed = false;
        for (FieldMapping f : step.config().getFields()) {
            JsonNode value = step.table().value(f.getSource(), row, step.keyColumns().contains(f.getSource()));
            if (value != null) {
                target.set(f.getTarget(), value);
                changed = true;
            }
        }
        return changed;
    }

    // ── Document navigation ───────────────────────────────────────────────────

    private static JsonNode navigate(JsonNode from, List<String> segments) {
        JsonNode n = from;
        for (String s : segments) {
            n = n == null ? null : n.get(s);
        }
        return n;
    }

    /** The object at {@code segments}, created (replacing a non-object value) when absent. */
    private static ObjectNode objectAt(ObjectNode document, List<String> segments) {
        ObjectNode n = document;
        for (String s : segments) {
            JsonNode child = n.get(s);
            if (child instanceof ObjectNode o) {
                n = o;
            } else {
                n = n.putObject(s);
            }
        }
        return n;
    }

    private static ObjectNode parent(ObjectNode document, List<String> segments) {
        return objectAt(document, segments.subList(0, segments.size() - 1));
    }

    private static String last(List<String> segments) {
        return segments.get(segments.size() - 1);
    }
}
