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
package com.ibm.di.event.rule.broadcast;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Functional and regression unit tests for {@link ArchiveDownloader} and {@link ArchiveVerificationException}.
 */
@DisplayName("ArchiveDownloader Unit Tests")
class ArchiveDownloaderTest {

    private static String sha256Hex(byte[] bytes) throws Exception {
        byte[] hash = MessageDigest.getInstance("SHA-256").digest(bytes);
        return HexFormat.of().formatHex(hash);
    }

    @Nested
    @DisplayName("Input Validation")
    class InputValidation {

        @Test
        @DisplayName("Rejects null or blank URL")
        void rejectsBlankUrl() {
            assertThrows(IllegalArgumentException.class, () -> ArchiveDownloader.downloadAndVerify(null, "somehash"));
            assertThrows(IllegalArgumentException.class, () -> ArchiveDownloader.downloadAndVerify("", "somehash"));
            assertThrows(IllegalArgumentException.class, () -> ArchiveDownloader.downloadAndVerify("   ", "somehash"));
        }

        @Test
        @DisplayName("Rejects null or blank SHA-256")
        void rejectsBlankSha256() {
            assertThrows(IllegalArgumentException.class, () -> ArchiveDownloader.downloadAndVerify("file:///tmp/test.jar", null));
            assertThrows(IllegalArgumentException.class, () -> ArchiveDownloader.downloadAndVerify("file:///tmp/test.jar", ""));
            assertThrows(IllegalArgumentException.class, () -> ArchiveDownloader.downloadAndVerify("file:///tmp/test.jar", "   "));
        }
    }

    @Nested
    @DisplayName("Local File Download & Verification")
    class LocalFileTests {

        @Test
        @DisplayName("Successfully downloads, caches, and verifies file:// URL")
        void downloadAndVerifyLocalFile(@TempDir Path tempDir) throws Exception {
            byte[] content = "dummy-decision-archive-payload".getBytes();
            String sha = sha256Hex(content);

            Path sourceFile = tempDir.resolve("sample.jar");
            Files.write(sourceFile, content);

            // First download
            Path downloaded = ArchiveDownloader.downloadAndVerify(sourceFile.toUri().toString(), sha);
            assertNotNull(downloaded);
            assertTrue(Files.exists(downloaded));
            assertArrayEquals(content, Files.readAllBytes(downloaded));

            // Cache hit
            Path cached = ArchiveDownloader.downloadAndVerify(sourceFile.toUri().toString(), sha);
            assertEquals(downloaded, cached);

            // Evict and download again
            ArchiveDownloader.evict(sha);
            Path reDownloaded = ArchiveDownloader.downloadAndVerify(sourceFile.toUri().toString(), sha);
            assertEquals(downloaded, reDownloaded);
        }

        @Test
        @DisplayName("Fails verification when SHA-256 mismatches")
        void sha256MismatchThrows(@TempDir Path tempDir) throws Exception {
            byte[] content = "valid-content".getBytes();
            Path sourceFile = tempDir.resolve("mismatch.jar");
            Files.write(sourceFile, content);

            String incorrectSha = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

            IOException ioe = assertThrows(IOException.class, () ->
                ArchiveDownloader.downloadAndVerify(sourceFile.toUri().toString(), incorrectSha)
            );
            assertTrue(ioe.getMessage().contains("SHA-256 mismatch") || (ioe.getCause() != null && ioe.getCause().getMessage().contains("SHA-256 mismatch")));
        }

        @Test
        @DisplayName("Rejects unsupported schemes like s3:// or cos://")
        void rejectsUnsupportedScheme() {
            String fakeSha = "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
            assertThrows(Exception.class, () ->
                ArchiveDownloader.downloadAndVerify("s3://my-bucket/archive.jar", fakeSha)
            );
            assertThrows(Exception.class, () ->
                ArchiveDownloader.downloadAndVerify("cos://my-bucket/archive.jar", fakeSha)
            );
        }
    }

    @Nested
    @DisplayName("ArchiveVerificationException Tests")
    class ExceptionTests {

        @Test
        @DisplayName("Constructors and message retention")
        void exceptionConstructors() {
            ArchiveVerificationException ex1 = new ArchiveVerificationException("verification failed");
            assertEquals("verification failed", ex1.getMessage());

            Throwable cause = new RuntimeException("root cause");
            ArchiveVerificationException ex2 = new ArchiveVerificationException("failed with cause", cause);
            assertEquals("failed with cause", ex2.getMessage());
            assertEquals(cause, ex2.getCause());
        }
    }
}
