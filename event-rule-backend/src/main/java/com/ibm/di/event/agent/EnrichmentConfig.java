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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serial;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * One reference-data enrichment step of a subscription (inbound event) or of an output (outbound
 * event). It is executed by both engines (SQL function {@code EVENT_ENRICH} in the Process Table
 * Function engine, a {@code map} operator in the DataStream engine). The Table API based
 * alternative of the DataStream engine is {@link SqlEnrichmentConfig}: where a subscription or
 * an output declares both, the DataStream engine applies {@code sqlEnrichment} only and ignores
 * these steps (with a warning).
 *
 * <p>For the event (or, with {@code into} ending in {@code [*]}, for every element of an array of
 * the event):
 * <ol>
 *   <li>if {@link #when} is set and does not match, the step is skipped;</li>
 *   <li>the value found by {@link #on} is looked up in the column {@link #key} of
 *       {@link #reference};</li>
 *   <li>when a row is found, its {@link #fields} are written at {@link #into}. When no row is
 *       found nothing is written and the next steps run.</li>
 * </ol>
 * Steps are executed in an order that respects their dependencies, not necessarily the declared
 * order: a step into {@code $.a.b[*]} runs after the step that fills {@code $.a.b}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class EnrichmentConfig implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    public static final String ONE  = "one";
    public static final String MANY = "many";

    /** Name of an entry of the agent's {@code references}. */
    private String reference;

    /** Column of the reference that is matched against the value found by {@link #on}. */
    private String key;

    /**
     * JSONPath of the lookup value in the event. With a {@code [*]} {@link #into} it is relative
     * to each array element ({@code $} is the element).
     */
    private String on;

    /**
     * Optional JSONPath filter, for example {@code $[?(@.type == 'FLIGHTOPEN')]}: the step runs
     * when it selects something. Evaluated on the event, or on each element for a {@code [*]}
     * {@link #into}. Attributes are written {@code @.name}.
     */
    private String when;

    /**
     * Where the fields are written: {@code $} (the event), {@code $.a.b} (an object, or an array
     * when the reference has several rows per key) or {@code $.a.b[*]} (every element of an
     * existing array).
     */
    private String into;

    /** Reference columns to copy. */
    private List<FieldMapping> fields = new ArrayList<>();

    /**
     * {@value #ONE}: the first matching row is merged into an object. {@value #MANY}: all
     * matching rows become an array. Absent: {@value #MANY} when the reference holds several rows
     * for one key, else {@value #ONE}. Must not be {@value #MANY} for a {@code [*]} target.
     */
    private String cardinality;

    public EnrichmentConfig() {}

    public String getReference()                   { return reference; }
    public void   setReference(String r)           { this.reference = r; }

    public String getKey()                         { return key; }
    public void   setKey(String k)                 { this.key = k; }

    public String getOn()                          { return on; }
    public void   setOn(String o)                  { this.on = o; }

    public String getWhen()                        { return when; }
    public void   setWhen(String w)                { this.when = w; }

    public String getInto()                        { return into; }
    public void   setInto(String i)                { this.into = i; }

    public List<FieldMapping> getFields()          { return fields; }
    public void setFields(List<FieldMapping> f)    { this.fields = f != null ? f : new ArrayList<>(); }

    public String getCardinality()                 { return cardinality; }
    public void   setCardinality(String c)         { this.cardinality = c; }

    @Override
    public String toString() {
        return "EnrichmentConfig{reference='" + reference + "', key='" + key + "', on='" + on
            + "', into='" + into + "'}";
    }
}
