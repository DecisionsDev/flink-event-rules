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

import com.ibm.di.event.agent.enrichment.Enricher;
import com.ibm.di.event.agent.enrichment.EnrichmentSpec;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * SQL scalar function {@code EVENT_ENRICH(json, spec)}: applies the reference-data enrichment
 * described by {@code spec} (an {@link EnrichmentSpec} as JSON, a constant of the generated
 * query) to the JSON string {@code json} and returns the enriched JSON string.
 *
 * <p>The reference data (CSV files) is read from the classpath first, so it can be bundled in the
 * function JAR, which is the only way to deliver it to Confluent Cloud. An {@link Enricher} (and
 * the tables behind it) is built once per distinct {@code spec} and JVM.
 *
 * <p>One {@code EVENT_ENRICH} call is one SQL step: the engine of the pipeline builder chains
 * calls in {@code CREATE VIEW}s. A record that cannot be enriched is returned unchanged; a
 * configuration error (unknown column, unreadable file) fails the statement on the first record.
 *
 * <p>Registered under {@link #NAME}. Public class with a public no-argument constructor, as
 * Confluent Cloud requires for {@code CREATE FUNCTION ... USING JAR}.
 */
public class EventEnrichFunction extends ScalarFunction {

    /** SQL name of the function. */
    public static final String NAME = "EVENT_ENRICH";

    private static final Map<String, Enricher> ENRICHERS = new ConcurrentHashMap<>();

    public EventEnrichFunction() {}

    @FunctionHint(output = @DataTypeHint("STRING"))
    public String eval(String json, String spec) {
        if (json == null || spec == null) return json;
        return ENRICHERS.computeIfAbsent(spec, s -> new Enricher(EnrichmentSpec.fromJson(s))).enrich(json);
    }
}
