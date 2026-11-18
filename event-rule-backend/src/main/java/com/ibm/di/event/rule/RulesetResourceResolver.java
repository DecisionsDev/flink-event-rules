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
package com.ibm.di.event.rule;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Resolves a ruleset archive path to a {@link URI} that can be loaded by {@link RuleEngineRunner}.
 *
 * <p>Supports:
 * <ul>
 *   <li>Standard URIs ({@code http://}, {@code https://}, etc.)</li>
 *   <li>Explicit classpath schemes: {@code classpath:ruleset.jar}, {@code resource:/ruleset.jar}</li>
 *   <li>Direct filesystem paths (absolute, relative to working directory or project root)</li>
 *   <li>Classpath resources embedded inside the application fat JAR:
 *       extracts the resource to an ephemeral file in {@code java.io.tmpdir} so that
 *       {@link java.net.URLClassLoader} can load it without requiring a persistent filesystem mount
 *       in managed environments like Confluent Cloud.</li>
 * </ul>
 */
public final class RulesetResourceResolver {

    private static final Logger LOG = LoggerFactory.getLogger(RulesetResourceResolver.class);

    /** Cache of extracted resource URIs: candidatePath -> file URI */
    private static final Map<String, URI> EXTRACTED_CACHE = new ConcurrentHashMap<>();

    private RulesetResourceResolver() {}

    /**
     * Resolves the given ruleset path into a loadable {@link URI}.
     *
     * @param path ruleset path, URI, or classpath resource name
     * @return a {@link URI} pointing to the ruleset archive
     * @throws IllegalArgumentException if the ruleset cannot be found on disk or classpath
     */
    public static URI resolve(String path) {
        if (path == null || path.isBlank()) {
            throw new IllegalArgumentException("Ruleset path must not be null or blank");
        }

        String trimmed = path.trim();

        // 1. Explicit classpath / resource schemes
        if (trimmed.startsWith("classpath:") || trimmed.startsWith("resource:")) {
            String resourcePath = trimmed.replaceFirst("^(classpath:|resource:)", "");
            return resolveClasspathResource(resourcePath, trimmed);
        }

        // 2. Non-file URI schemes (http://, https://, etc.)
        if (trimmed.contains("://") && !trimmed.startsWith("file://")) {
            return URI.create(trimmed);
        }

        // 3. File URI scheme: file:///path/to/jar
        String filePath = trimmed.startsWith("file://") ? URI.create(trimmed).getPath() : trimmed;

        // 4. Try resolving on the filesystem (absolute, cwd, parent)
        Path fsPath = resolveFileSystemPath(filePath);
        if (fsPath != null) {
            return fsPath.toUri();
        }

        // 5. Try resolving as a bundled classpath resource (fallback for containerized/serverless environments like Confluent Cloud)
        URI resourceUri = tryResolveClasspathResource(filePath);
        if (resourceUri != null) {
            return resourceUri;
        }

        // 6. Not found anywhere
        throw new IllegalArgumentException(
            "Ruleset archive not found on filesystem or classpath: '" + path + "' (working dir: "
                + System.getProperty("user.dir") + ")");
    }

    private static Path resolveFileSystemPath(String path) {
        try {
            Path p = Paths.get(path);
            if (p.isAbsolute() && Files.isRegularFile(p)) {
                return p;
            }

            // Relative path: try cwd first
            Path cwdPath = Paths.get(System.getProperty("user.dir")).resolve(path);
            if (Files.isRegularFile(cwdPath)) {
                return cwdPath;
            }

            // Then try parent directory of cwd (project root)
            Path parent = Paths.get(System.getProperty("user.dir")).getParent();
            if (parent != null) {
                Path parentPath = parent.resolve(path);
                if (Files.isRegularFile(parentPath)) {
                    return parentPath;
                }
            }
        } catch (Exception e) {
            LOG.debug("Error checking filesystem path '{}': {}", path, e.getMessage());
        }
        return null;
    }

    private static URI resolveClasspathResource(String resourcePath, String originalPath) {
        URI uri = tryResolveClasspathResource(resourcePath);
        if (uri != null) {
            return uri;
        }
        throw new IllegalArgumentException(
            "Classpath ruleset resource not found: '" + originalPath + "'");
    }

    private static URI tryResolveClasspathResource(String path) {
        // Generate candidate resource paths to check
        List<String> candidates = buildCandidateResourcePaths(path);

        for (String candidate : candidates) {
            URI cached = EXTRACTED_CACHE.get(candidate);
            if (cached != null) {
                // Verify the cached temp file still exists
                try {
                    if (Files.isRegularFile(Paths.get(cached))) {
                        return cached;
                    }
                } catch (Exception ignored) {}
            }

            InputStream in = getResourceStream(candidate);
            if (in != null) {
                try (in) {
                    Path tempFile = extractToTempFile(candidate, in);
                    URI uri = tempFile.toUri();
                    EXTRACTED_CACHE.put(candidate, uri);
                    LOG.info("Extracted bundled ruleset resource '{}' to temporary file: {}", candidate, tempFile);
                    return uri;
                } catch (IOException e) {
                    LOG.error("Failed to extract ruleset resource '{}' to temp file: {}", candidate, e.getMessage(), e);
                    throw new RuntimeException("Failed to extract ruleset resource '" + candidate + "': " + e.getMessage(), e);
                }
            }
        }
        return null;
    }

    private static List<String> buildCandidateResourcePaths(String path) {
        List<String> list = new ArrayList<>();
        String normalized = path.replace('\\', '/').trim();
        String withoutLeadingSlash = normalized.startsWith("/") ? normalized.substring(1) : normalized;
        String withLeadingSlash = "/" + withoutLeadingSlash;

        list.add(withoutLeadingSlash);
        list.add(withLeadingSlash);

        // Strip "samples/<use-case>/" prefix if present
        if (withoutLeadingSlash.startsWith("samples/")) {
            String stripped = withoutLeadingSlash.replaceFirst("^samples/[^/]+/", "");
            list.add(stripped);
            list.add("/" + stripped);
        }

        // Just filename
        int lastSlash = withoutLeadingSlash.lastIndexOf('/');
        if (lastSlash >= 0) {
            String filename = withoutLeadingSlash.substring(lastSlash + 1);
            list.add(filename);
            list.add("/" + filename);
            list.add("rulesets/" + filename);
            list.add("/rulesets/" + filename);
        }

        // Generic convention fallback
        list.add("ruleset.jar");
        list.add("/ruleset.jar");
        list.add("rulesets/ruleset.jar");
        list.add("/rulesets/ruleset.jar");

        return list;
    }

    private static InputStream getResourceStream(String name) {
        // Try context class loader first
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        if (cl != null) {
            String lookup = name.startsWith("/") ? name.substring(1) : name;
            InputStream in = cl.getResourceAsStream(lookup);
            if (in != null) return in;
        }

        // Then this class's loader
        cl = RulesetResourceResolver.class.getClassLoader();
        if (cl != null) {
            String lookup = name.startsWith("/") ? name.substring(1) : name;
            InputStream in = cl.getResourceAsStream(lookup);
            if (in != null) return in;
        }

        // Class.getResourceAsStream allows leading slash
        return RulesetResourceResolver.class.getResourceAsStream(name);
    }

    private static Path extractToTempFile(String resourceName, InputStream in) throws IOException {
        String suffix = resourceName.endsWith(".dsa") ? ".dsa" : ".jar";
        Path tempFile = Files.createTempFile("ruleset-extracted-", suffix);
        tempFile.toFile().deleteOnExit();

        Files.copy(in, tempFile, StandardCopyOption.REPLACE_EXISTING);
        return tempFile;
    }

    /** Clears the extracted cache (mainly for testing). */
    static void clearCache() {
        EXTRACTED_CACHE.clear();
    }
}
