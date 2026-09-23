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
package com.ibm.di.event.agent.context;

import java.util.HashMap;
import java.util.Map;

/**
 * Registry of {@link ContextBuilder} implementations keyed by their
 * {@code contextFormat} string.
 */
public final class ContextBuilderRegistry {

    private static final Map<String, ContextBuilder> REGISTRY = new HashMap<>();

    static {
        REGISTRY.put("generic",     new GenericContextBuilder());
    }

    private ContextBuilderRegistry() {}

    /**
     * Returns the {@link ContextBuilder} for the given {@code format} string.
     * Falls back to the {@code generic} builder for unknown or null formats.
     *
     * @param format the {@code contextFormat} string from the agent descriptor's ruleset
     *               configuration (e.g. {@code "generic"}); {@code null} or blank selects
     *               the default {@code "generic"} builder
     * @return a non-null {@link ContextBuilder}
     */
    public static ContextBuilder get(String format) {
        if (format == null || format.isBlank()) {
            return REGISTRY.get("generic");
        }
        return REGISTRY.getOrDefault(format, REGISTRY.get("generic"));
    }

    /**
     * Registers (or replaces) a {@link ContextBuilder} for the given {@code format} key.
     *
     * <p>Thread-safety: this method modifies a shared map.  For production use, call it
     * during application startup before any Flink operator is opened.  In tests, call it
     * before the processor under test is constructed.
     *
     * @param format  the {@code contextFormat} string (must not be null or blank)
     * @param builder the builder to register
     */
    public static void register(String format, ContextBuilder builder) {
        if (format == null || format.isBlank()) throw new IllegalArgumentException("format must not be blank");
        if (builder == null)                    throw new IllegalArgumentException("builder must not be null");
        REGISTRY.put(format, builder);
    }
}

