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

/**
 * Configuration for optional Flink SQL enrichment attached to an {@link EventSubscription}.
 *
 * <p>When present on a subscription, incoming Kafka events are registered as a temporary
 * view (named {@link #inputViewName}) in a Flink {@code StreamTableEnvironment} based on the
 * topic's JSON Schema. The DDL and queries in {@link #sqlFile} are executed, and the resulting
 * table (named {@link #outputViewName}) is converted back into {@link EventEnvelope}s before
 * entering the {@link StatefulAgentProcessor}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SqlEnrichmentConfig implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    /** Path to the .sql file containing DDL (e.g. reference tables) and enrichment views. */
    private String sqlFile;

    /** Name of the temporary input view registered for the incoming stream (default: {@code "raw_events"}). */
    private String inputViewName = "raw_events";

    /** Name of the view/table producing enriched rows (default: {@code "enriched_events"}). */
    private String outputViewName = "enriched_events";

    public SqlEnrichmentConfig() {}

    public SqlEnrichmentConfig(String sqlFile, String inputViewName, String outputViewName) {
        this.sqlFile = sqlFile;
        this.inputViewName = (inputViewName != null && !inputViewName.isBlank()) ? inputViewName : "raw_events";
        this.outputViewName = (outputViewName != null && !outputViewName.isBlank()) ? outputViewName : "enriched_events";
    }

    public String getSqlFile() {
        return sqlFile;
    }

    public void setSqlFile(String sqlFile) {
        this.sqlFile = sqlFile;
    }

    public String getInputViewName() {
        return inputViewName != null && !inputViewName.isBlank() ? inputViewName : "raw_events";
    }

    public void setInputViewName(String inputViewName) {
        this.inputViewName = inputViewName;
    }

    public String getOutputViewName() {
        return outputViewName != null && !outputViewName.isBlank() ? outputViewName : "enriched_events";
    }

    public void setOutputViewName(String outputViewName) {
        this.outputViewName = outputViewName;
    }

    public boolean hasSqlFile() {
        return sqlFile != null && !sqlFile.isBlank();
    }
}
