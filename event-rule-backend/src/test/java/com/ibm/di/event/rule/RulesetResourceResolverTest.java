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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("RulesetResourceResolver Tests")
class RulesetResourceResolverTest {

    @AfterEach
    void tearDown() {
        RulesetResourceResolver.clearCache();
    }

    @Test
    @DisplayName("rejects null, empty, or blank path")
    void rejectsInvalidPath() {
        assertThrows(IllegalArgumentException.class, () -> RulesetResourceResolver.resolve(null));
        assertThrows(IllegalArgumentException.class, () -> RulesetResourceResolver.resolve(""));
        assertThrows(IllegalArgumentException.class, () -> RulesetResourceResolver.resolve("   "));
    }

    @Test
    @DisplayName("resolves non-file URI schemes directly")
    void resolvesHttpUris() {
        URI httpUri = RulesetResourceResolver.resolve("http://example.com/ruleset.jar");
        assertEquals(URI.create("http://example.com/ruleset.jar"), httpUri);

        URI httpsUri = RulesetResourceResolver.resolve("https://storage.googleapis.com/bucket/ruleset.jar");
        assertEquals(URI.create("https://storage.googleapis.com/bucket/ruleset.jar"), httpsUri);
    }

    @Test
    @DisplayName("resolves existing filesystem paths (absolute and relative)")
    void resolvesExistingFileSystemPath() throws IOException {
        Path tempJar = Files.createTempFile("test-ruleset-", ".jar");
        tempJar.toFile().deleteOnExit();

        URI resolved = RulesetResourceResolver.resolve(tempJar.toAbsolutePath().toString());
        assertEquals(tempJar.toUri(), resolved);

        // Also test file:// prefix
        URI fileUriResolved = RulesetResourceResolver.resolve(tempJar.toUri().toString());
        assertEquals(tempJar.toUri(), fileUriResolved);
    }

    @Test
    @DisplayName("resolves bundled classpath resource and extracts to ephemeral temp file")
    void resolvesBundledClasspathResource() throws IOException {
        // application.properties exists on classpath
        URI resolved = RulesetResourceResolver.resolve("classpath:application.properties");
        assertNotNull(resolved);
        assertTrue(resolved.getScheme().equalsIgnoreCase("file"));

        Path extractedPath = Paths.get(resolved);
        assertTrue(Files.exists(extractedPath), "Extracted file should exist on disk");
        assertTrue(Files.size(extractedPath) > 0, "Extracted file should not be empty");

        // Subsequent resolution should hit cache
        URI cached = RulesetResourceResolver.resolve("classpath:application.properties");
        assertEquals(resolved, cached);
    }

    @Test
    @DisplayName("resolves resource with resource: prefix and fallback candidate paths")
    void resolvesResourcePrefixAndCandidates() {
        URI resolved = RulesetResourceResolver.resolve("resource:/application.properties");
        assertNotNull(resolved);
        assertTrue(Files.exists(Paths.get(resolved)));
    }

    @Test
    @DisplayName("extracts classpath resource fallback when file is not on filesystem")
    void resolvesFallbackClasspathResourceWhenFileNotOnDisk() {
        // Given a path like "samples/some-agent/application.properties", it tries candidate paths
        // including stripping the samples/ prefix or trying filename alone
        URI resolved = RulesetResourceResolver.resolve("samples/some-agent/application.properties");
        assertNotNull(resolved);
        assertTrue(Files.exists(Paths.get(resolved)));
    }

    @Test
    @DisplayName("throws IllegalArgumentException when resource is not found anywhere")
    void throwsWhenNotFound() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
            () -> RulesetResourceResolver.resolve("non-existent-ruleset-1234567.jar"));
        assertTrue(ex.getMessage().contains("Ruleset archive not found"));
    }
}
