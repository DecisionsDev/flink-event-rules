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

import com.ibm.di.event.agent.datastream.AgentBasedDataStreamJob;

/**
 * Entry point selecting the runtime of the Event Rule system.
 *
 * <p>The runtime is chosen with the {@code EVENT_RULE_RUNTIME} environment variable:
 * <ul>
 *   <li>{@code datastream} (default) — {@link AgentBasedDataStreamJob}, the DataStream API job;</li>
 *   <li>{@code ptf} — {@link AgentBasedPTFJob}, the Flink SQL / Process Table Function job.</li>
 * </ul>
 * Both entry points stay directly runnable; this class only dispatches to them.
 */
public final class AgentJobLauncher {

    /** Environment variable selecting the runtime. */
    public static final String RUNTIME_ENV = "EVENT_RULE_RUNTIME";

    /** The runtimes the system can run on. */
    public enum Runtime { DATASTREAM, PTF }

    private AgentJobLauncher() {}

    public static void main(String[] args) throws Exception {
        Runtime runtime = resolve(System.getenv(RUNTIME_ENV));
        switch (runtime) {
            case PTF -> AgentBasedPTFJob.main(args);
            case DATASTREAM -> AgentBasedDataStreamJob.main(args);
        }
    }

    /**
     * Maps the value of {@link #RUNTIME_ENV} to a runtime.
     *
     * @param value the raw value; {@code null} or blank selects the DataStream runtime
     * @throws IllegalArgumentException if the value names no known runtime
     */
    public static Runtime resolve(String value) {
        if (value == null || value.isBlank()) return Runtime.DATASTREAM;
        return switch (value.trim().toLowerCase(java.util.Locale.ROOT)) {
            case "datastream" -> Runtime.DATASTREAM;
            case "ptf", "sql" -> Runtime.PTF;
            default -> throw new IllegalArgumentException(
                RUNTIME_ENV + " must be 'datastream' or 'ptf', got: " + value);
        };
    }
}
