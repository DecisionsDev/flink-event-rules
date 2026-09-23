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

import com.fasterxml.jackson.databind.JsonNode;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shared JSONPath utility for entity-ID extraction.
 *
 * <p>This class is the <strong>single authoritative implementation</strong> of the
 * entity-ID extraction logic used in two places:
 * <ol>
 *   <li>{@link AgentBasedDataStreamJob.EnvelopeDeserializer} — at Kafka deserialization time.</li>
 *   <li>{@link AgentChainBuilder.DeferredChainedInputs} — during chain envelope normalisation.</li>
 * </ol>
 *
 * <p>Keeping both callers on the same code path guarantees that the entity ID produced for a
 * given payload and extractor expression is identical regardless of whether the event arrived
 * via Kafka or via an in-process chain edge.  Previously the logic was copy-pasted in two
 * places, creating a risk of silent divergence (CF-5 / Weakness 5 in the investigation report).
 *
 * <p>Behaviour:
 * <ul>
 *   <li>A bare field name (no leading {@code $}) is normalized to {@code $.fieldName}.</li>
 *   <li>Returns {@code "unknown"} when the extractor is null/blank, the payload is null, or
 *       the path does not match any field.</li>
 *   <li>A {@link PathNotFoundException} is caught; a WARN is logged once per call.</li>
 * </ul>
 */
public final class JsonPathUtils {

    private static final Logger LOG = LoggerFactory.getLogger(JsonPathUtils.class);

    private JsonPathUtils() {}

    /**
     * Extracts the entity ID from {@code payload} using the given JSONPath {@code extractor}.
     *
     * <p>Supports arbitrary nesting depth (e.g. {@code "$.a.b.c.d"}).
     *
     * @param payload   the event JSON payload; may be {@code null}
     * @param extractor JSONPath expression, e.g. {@code "$.accountId"} or {@code "$.a.b.c"};
     *                  a bare field name is normalised to {@code $.fieldName} automatically
     * @return the extracted string value, or {@code "unknown"} when not found or on error
     */
    public static String extractEntityId(JsonNode payload, String extractor) {
        if (extractor == null || extractor.isBlank() || payload == null) return "unknown";
        // Normalise: bare "fieldName" → "$.fieldName"
        String path = extractor.startsWith("$") ? extractor : "$." + extractor;
        try {
            Object value = JsonPath.read(payload.toString(), path);
            return value != null ? value.toString() : "unknown";
        } catch (PathNotFoundException e) {
            LOG.warn("[JsonPathUtils] entityKeyExtractor '{}' did not match any field in payload; "
                + "entity will key to 'unknown'. Check the entityKeyExtractor expression "
                + "in your agent descriptor.", extractor);
            return "unknown";
        }
    }
}
