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
package com.ibm.di.event.agent.enrichment;

import com.ibm.di.event.agent.ReferenceConfig;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Loads and caches {@link ReferenceTable}s.
 *
 * <p>A file is looked up on the <b>classpath</b> first, then on the filesystem (as given, then
 * relative to the working directory and its parent, like the agent descriptor). Bundling the
 * files in the function JAR is how the data reaches Confluent Cloud, whose functions have no
 * filesystem. A table is loaded once per JVM and shared by every function instance, keyed by file
 * and declared types.
 */
public final class ReferenceRegistry {

    private static final Map<String, ReferenceTable> CACHE = new ConcurrentHashMap<>();

    private ReferenceRegistry() {}

    /** The table of {@code reference}; loads it on first use. */
    public static ReferenceTable get(ReferenceConfig reference) {
        if (!ReferenceConfig.TYPE_CSV.equalsIgnoreCase(reference.getType())) {
            throw new IllegalArgumentException("Reference '" + reference.getName() + "': unsupported type '"
                + reference.getType() + "' (only 'csv')");
        }
        String file = reference.getFile();
        if (file == null || file.isBlank()) {
            throw new IllegalArgumentException("Reference '" + reference.getName() + "' has no file");
        }
        String key = file + "|" + new TreeMap<>(reference.getTypes());
        return CACHE.computeIfAbsent(key, k -> {
            try {
                return ReferenceTable.parse(reference.getName(), read(file), reference.getTypes());
            } catch (IOException e) {
                throw new IllegalStateException("Cannot read reference '" + reference.getName()
                    + "' from " + file + ": " + e.getMessage(), e);
            }
        });
    }

    /** Whether {@code file} can be found on the classpath or the filesystem. */
    public static boolean exists(String file) {
        return file != null && !file.isBlank()
            && (classpathResource(file) != null || locate(file) != null);
    }

    /** Drops every cached table (tests). */
    public static void clear() {
        CACHE.clear();
    }

    private static String read(String file) throws IOException {
        java.net.URL url = classpathResource(file);
        if (url != null) {
            try (InputStream in = url.openStream()) {
                return new String(in.readAllBytes(), StandardCharsets.UTF_8);
            }
        }
        File f = locate(file);
        if (f == null) {
            throw new IOException("not found on the classpath nor on the filesystem (working dir: "
                + System.getProperty("user.dir") + ")");
        }
        return java.nio.file.Files.readString(f.toPath(), StandardCharsets.UTF_8);
    }

    private static java.net.URL classpathResource(String file) {
        String resource = file.startsWith("/") ? file.substring(1) : file;
        java.net.URL url = ReferenceRegistry.class.getClassLoader().getResource(resource);
        if (url == null) {
            ClassLoader cl = Thread.currentThread().getContextClassLoader();
            if (cl != null) url = cl.getResource(resource);
        }
        return url;
    }

    private static File locate(String file) {
        File f = new File(file);
        if (f.isFile()) return f;
        String userDir = System.getProperty("user.dir");
        if (userDir != null) {
            f = new File(userDir, file);
            if (f.isFile()) return f;
            File parent = new File(userDir).getParentFile();
            if (parent != null) {
                f = new File(parent, file);
                if (f.isFile()) return f;
            }
        }
        return null;
    }
}
