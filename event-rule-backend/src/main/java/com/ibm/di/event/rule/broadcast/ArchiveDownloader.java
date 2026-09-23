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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Downloads and verifies Decision Intelligence archive JARs from object storage.
 *
 * <p>Downloads are keyed by SHA-256 digest so that multiple subtasks running on the
 * same JVM share a single download — the static {@link #DOWNLOAD_CACHE} is a
 * JVM-level deduplication guard.  The local copy is written to a configurable path
 * (see {@link #CACHE_DIR}) so it survives across poll cycles until the JVM exits.
 *
 * <h3>Supported URL schemes</h3>
 * <ul>
 *   <li>{@code file://} — local filesystem (for development and CI)</li>
 *   <li>{@code http://} / {@code https://} — standard HTTP(S), including cloud
 *       object storage pre-signed URLs (the recommended production path for S3/COS)</li>
 *   <li>{@code s3://} / {@code cos://} — <strong>not natively supported</strong>.
 *       The JDK has no built-in handler for these schemes; using them will throw
 *       {@link java.net.MalformedURLException}.  Use HTTPS pre-signed URLs instead:
 *       both AWS S3 and IBM COS support them via their respective consoles and CLIs.</li>
 * </ul>
 *
 * <h3>Cache directory</h3>
 * <p>Defaults to {@code $TMPDIR/flink-archive-cache}.  Override with the environment
 * variable {@code FLINK_ARCHIVE_CACHE_DIR} to point at a persistent volume — useful
 * in Kubernetes where {@code /tmp} is an ephemeral in-memory tmpfs that is wiped on
 * pod restart, defeating the PREFETCH→LOAD optimisation.
 *
 * <p>Thread-safety: all public methods are thread-safe.
 */
public final class ArchiveDownloader {

    private static final Logger LOG = LoggerFactory.getLogger(ArchiveDownloader.class);

    /** Default download timeout in seconds. */
    private static final int DOWNLOAD_TIMEOUT_S = 120;

    /**
     * Base directory for the local archive cache.
     * Reads {@code FLINK_ARCHIVE_CACHE_DIR} first; falls back to the JVM temp dir.
     */
    private static final String CACHE_DIR = resolveCacheDir();

    private static String resolveCacheDir() {
        String envVal = System.getenv("FLINK_ARCHIVE_CACHE_DIR");
        if (envVal != null && !envVal.isBlank()) {
            return envVal;
        }
        return System.getProperty("java.io.tmpdir") + java.io.File.separator + "flink-archive-cache";
    }

    /**
     * JVM-level download deduplication cache.
     * Key = SHA-256 hex; Value = future that completes with the local Path.
     */
    private static final ConcurrentHashMap<String, CompletableFuture<Path>> DOWNLOAD_CACHE =
        new ConcurrentHashMap<>();

    private ArchiveDownloader() {}

    /**
     * Downloads the archive at {@code url}, verifies its SHA-256, and returns the
     * local {@link Path} to the downloaded file.
     *
     * <p>If another thread (or subtask on the same JVM) is already downloading the same
     * digest, this call blocks until that download completes and reuses the result.
     *
     * @param url            URL of the archive JAR
     * @param expectedSha256 lowercase hex SHA-256; the download is rejected if it does not match
     * @return local path to the verified archive JAR
     * @throws ArchiveVerificationException if the SHA-256 does not match
     * @throws IOException                  if the download fails
     */
    public static Path downloadAndVerify(String url, String expectedSha256)
            throws Exception {
        if (url == null || url.isBlank()) {
            throw new IllegalArgumentException("archive URL must not be blank");
        }
        if (expectedSha256 == null || expectedSha256.isBlank()) {
            throw new IllegalArgumentException("expectedSha256 must not be blank");
        }
        String key = expectedSha256.toLowerCase();
        CompletableFuture<Path> future = DOWNLOAD_CACHE.computeIfAbsent(key,
            k -> CompletableFuture.supplyAsync(() -> {
                try {
                    return doDownload(url, k);
                } catch (Exception e) {
                    throw new RuntimeException("Archive download failed for " + url + ": " + e.getMessage(), e);
                }
            }));
        try {
            return future.get(DOWNLOAD_TIMEOUT_S, TimeUnit.SECONDS);
        } catch (java.util.concurrent.ExecutionException ee) {
            // Remove failed entry so the next call retries
            DOWNLOAD_CACHE.remove(key, future);
            Throwable cause = ee.getCause();
            if (cause instanceof ArchiveVerificationException ave) throw ave;
            if (cause instanceof IOException ioe) throw ioe;
            throw new IOException("Archive download failed: " + cause.getMessage(), cause);
        } catch (java.util.concurrent.TimeoutException te) {
            // B6/H2: evict the stuck future so the next retry starts a fresh download
            DOWNLOAD_CACHE.remove(key, future);
            throw new IOException("Archive download timed out after " + DOWNLOAD_TIMEOUT_S
                + "s for " + url, te);
        }
    }

    /**
     * Evicts a cached entry.  Call this after a failed reload so the next control
     * message re-downloads instead of hitting the poisoned cache entry.
     */
    public static void evict(String sha256) {
        if (sha256 != null) DOWNLOAD_CACHE.remove(sha256.toLowerCase());
    }

    // ── Private implementation ────────────────────────────────────────────────

    private static Path doDownload(String url, String expectedSha256) throws Exception {
        // Stable local path keyed by digest — avoids re-downloading the same version
        Path localPath = localPathFor(expectedSha256);
        if (Files.exists(localPath)) {
            // Verify the cached copy has not been corrupted
            String actual = sha256Hex(localPath);
            if (actual.equals(expectedSha256)) {
                LOG.info("[ArchiveDownloader] Cache hit for sha256={}", expectedSha256);
                return localPath;
            }
            LOG.warn("[ArchiveDownloader] Cached file SHA-256 mismatch — re-downloading (expected={} actual={})",
                expectedSha256, actual);
            Files.deleteIfExists(localPath);
        }

        LOG.info("[ArchiveDownloader] Downloading archive from {}", url);
        Path tmpPath = localPath.resolveSibling(localPath.getFileName() + ".tmp");

        try (InputStream in = openStream(URI.create(url));
             OutputStream out = Files.newOutputStream(tmpPath)) {
            in.transferTo(out);
        }

        // Verify digest before making the file visible
        String actual = sha256Hex(tmpPath);
        if (!actual.equals(expectedSha256)) {
            Files.deleteIfExists(tmpPath);
            throw new ArchiveVerificationException(
                "SHA-256 mismatch for " + url + ": expected=" + expectedSha256 + " actual=" + actual);
        }

        // Atomic rename so no subtask ever sees a partial file
        Files.move(tmpPath, localPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        LOG.info("[ArchiveDownloader] Downloaded and verified: {}", localPath);
        return localPath;
    }

    /**
     * Opens an {@link InputStream} for the given URI with explicit connect/read timeouts.
     *
     * <p>Supports {@code file://}, {@code http://}, and {@code https://}.
     * {@code s3://} and {@code cos://} schemes are <strong>not supported</strong> —
     * the JDK has no built-in handler for them.  Use HTTPS pre-signed URLs instead.
     *
     * @throws java.net.MalformedURLException if the URI scheme is not handled by the JDK
     *         (e.g. {@code s3://} or {@code cos://})
     */
    private static InputStream openStream(URI uri) throws IOException {
        String scheme = uri.getScheme();
        if (scheme == null || "file".equals(scheme)) {
            return Files.newInputStream(Paths.get(uri));
        }
        // For http/https: open with explicit timeouts — never block indefinitely.
        // NOTE: s3:// and cos:// are not supported here; use https:// pre-signed URLs.
        java.net.URLConnection conn = uri.toURL().openConnection();
        conn.setConnectTimeout(15_000);   // 15 s to establish TCP connection
        conn.setReadTimeout(60_000);      // 60 s between consecutive read() calls
        return conn.getInputStream();
    }

    private static Path localPathFor(String sha256) throws IOException {
        Path dir = Paths.get(CACHE_DIR);
        Files.createDirectories(dir);
        return dir.resolve("archive-" + sha256 + ".jar");
    }

    private static String sha256Hex(Path path) throws Exception {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        try (InputStream in = Files.newInputStream(path)) {
            byte[] buf = new byte[65536];
            int n;
            while ((n = in.read(buf)) != -1) md.update(buf, 0, n);
        }
        return HexFormat.of().formatHex(md.digest());
    }
}

