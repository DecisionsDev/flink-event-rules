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

/**
 * Per-entity state of the Process Table Function engine: the entity profile and the bounded event
 * histories, serialized by {@link AgentStateCodec} into a single JSON string.
 *
 * <p>Confluent Cloud requires state classes to be POJOs with public fields, default values and a
 * default constructor, and it serializes the whole object on every access. Holding one JSON string
 * keeps the schema stable (no Flink type evolution when the entity shape changes) and the size
 * bounded by the configured horizons.
 */
public class AgentPtfState {

    /** The serialized {@code AgentState}, or {@code null} when the entity has no state. */
    public String data = null;

    public AgentPtfState() {}
}
