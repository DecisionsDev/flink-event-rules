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

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * SQL scalar function {@code EVENT_FIELD(raw, field, defaultType, extractor)} used by the generated
 * pipeline to normalize raw Kafka values. See {@link EventNormalizer} for the rules.
 *
 * <p>Registered under the name {@link #NAME}. Public class with a public no-argument constructor,
 * as Confluent Cloud requires for {@code CREATE FUNCTION ... USING JAR}.
 */
public class EventEnvelopeFunction extends ScalarFunction {

    /** SQL name of the function. */
    public static final String NAME = "EVENT_FIELD";

    public EventEnvelopeFunction() {}

    @FunctionHint(output = @DataTypeHint("STRING"))
    public String eval(String raw, String field, String defaultType, String extractor) {
        return EventNormalizer.field(raw, field, defaultType, extractor);
    }
}
