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

import com.fasterxml.jackson.databind.JsonNode;
import com.ibm.di.event.agent.EventEnvelope;
import com.ibm.di.event.agent.enrichment.Enricher;
import com.ibm.di.event.agent.enrichment.EnrichmentSpec;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;

import java.io.Serial;
import java.util.HashMap;

/**
 * DataStream operator applying the reference-data enrichment of a subscription or of an output
 * to the payload of each {@link EventEnvelope}. It is the DataStream counterpart of the SQL
 * function {@code EVENT_ENRICH} of the PTF engine and shares its engine, {@link Enricher}, so
 * both runtimes enrich identically.
 *
 * <p>The envelope received is never modified: when the payload changes, a new envelope carrying
 * the enriched payload (same type, topic, timestamp, entity id and metadata) is returned, so an
 * envelope shared with other operators stays intact. The entity id and the timestamp are not
 * recomputed. Neither the key nor event time depends on enrichment.
 *
 * <p>The {@link Enricher} (hence the CSV files) is built in {@link #open}, on the task manager,
 * from the serializable {@link EnrichmentSpec} JSON.
 */
public class EnrichEnvelopeFunction extends RichMapFunction<EventEnvelope, EventEnvelope> {

    @Serial
    private static final long serialVersionUID = 1L;

    private final String specJson;
    private transient Enricher enricher;

    /** @param spec what to apply; {@code root} must be empty (the payload is the document) */
    public EnrichEnvelopeFunction(EnrichmentSpec spec) {
        this.specJson = spec.toJson();
    }

    @Override
    public void open(OpenContext openContext) {
        enricher = new Enricher(EnrichmentSpec.fromJson(specJson));
    }

    @Override
    public EventEnvelope map(EventEnvelope in) {
        JsonNode payload = in.getPayload();
        if (payload == null) return in;
        JsonNode copy = payload.deepCopy();
        if (!enricher.enrichTree(copy)) return in;

        EventEnvelope out = EventEnvelope.from(in.getEventType(), in.getSourceTopic(),
            in.getTimestamp(), copy, in.getEntityId());
        if (in.getMetadata() != null) out.setMetadata(new HashMap<>(in.getMetadata()));
        return out;
    }
}
