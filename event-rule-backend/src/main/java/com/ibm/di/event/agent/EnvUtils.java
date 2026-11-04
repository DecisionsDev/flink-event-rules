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

/**
 * Environment-variable helpers shared by the DataStream and PTF job entry points.
 */
public final class EnvUtils {

    private EnvUtils() {}

    public static String envOr(String name, String def) {
        String v = System.getenv(name);
        return (v != null && !v.isBlank()) ? v : def;
    }

    public static int intEnvOr(String name, int def) {
        String v = System.getenv(name);
        if (v == null || v.isBlank()) return def;
        try { return Integer.parseInt(v.trim()); }
        catch (NumberFormatException e) { return def; }
    }

    public static long longEnvOr(String name, long def) {
        String v = System.getenv(name);
        if (v == null || v.isBlank()) return def;
        try { return Long.parseLong(v.trim()); }
        catch (NumberFormatException e) { return def; }
    }

    /**
     * Reads a boolean environment variable.  Only the literal string {@code "false"}
     * (case-insensitive) returns {@code false}; everything else (including an unset or blank
     * value) returns the provided default.  This avoids the common pitfall where a non-empty
     * string like {@code "0"} or {@code "no"} would be treated as {@code true} by a naïve
     * {@code Boolean.parseBoolean} call.
     */
    public static boolean boolEnvOr(String name, boolean def) {
        String v = System.getenv(name);
        if (v == null || v.isBlank()) return def;
        return !"false".equalsIgnoreCase(v.trim());
    }
}
