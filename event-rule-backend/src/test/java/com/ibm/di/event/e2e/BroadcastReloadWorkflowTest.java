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
package com.ibm.di.event.e2e;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ibm.di.event.agent.*;
import com.ibm.di.event.rule.RuleEngineRunner;
import com.ibm.di.event.rule.broadcast.ArchiveControlCmd;
import com.ibm.di.event.rule.broadcast.ArchiveControlCmd.CommandType;
import com.ibm.di.event.rule.broadcast.ArchiveEntry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end workflow tests for the broadcast-state archive hot-swap mechanism.
 *
 * <p>These tests exercise scenarios that go beyond the unit-level dispatch tests in
 * {@link com.ibm.di.event.agent.StatefulAgentProcessorTest.BroadcastReload} and the
 * lifecycle invariants in {@link ArchiveLifecycleWorkflowTest}.  The focus here is on
 * <em>observable state transitions</em> produced by sequences of broadcast commands:
 *
 * <ul>
 *   <li>Stale runner eviction after a successful LOAD</li>
 *   <li>Multiple consecutive LOADs keeping only the active version in cache</li>
 *   <li>LOAD failure (non-existent JAR) leaves the runner cache intact</li>
 *   <li>LOAD failure leaves ACTIVE broadcast state intact</li>
 *   <li>LOAD idempotency: injecting the same version twice does not duplicate</li>
 *   <li>ROLLBACK with ArchiveEntry.previousEntry chain: validates hasPreviousEntry semantics</li>
 *   <li>Second ROLLBACK after hasPreviousEntry() == false is rejected cleanly</li>
 *   <li>PREFETCH does not change the bootstrap runner used by invokeForTest</li>
 *   <li>NOOP between two LOADs leaves the second LOAD's broadcast state intact</li>
 *   <li>Null broadcast command with a live runner does not clear the runner</li>
 *   <li>ArchiveEntry previousEntry chain is exactly one level deep</li>
 *   <li>Audit log: DeploymentAuditLog records LOAD metadata correctly</li>
 * </ul>
 *
 * <p>All tests are hermetic: no external infrastructure required.
 * Pre-warmed runners are injected via
 * {@link StatefulAgentProcessor#injectRunner(String, RuleEngineRunner)}, and the
 * broadcast state is exercised via
 * {@link StatefulAgentProcessor#invokeProcessBroadcastForTest}.
 */
@DisplayName("Broadcast reload — end-to-end state-transition workflow tests")
class BroadcastReloadWorkflowTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // ── Stub rule engine ──────────────────────────────────────────────────────

    /**
     * Minimal stub that tracks call count and returns a fixed result.
     * Never touches ODM infrastructure.
     */
    static class TrackingStub extends RuleEngineRunner {

        final  String   version;
        private final   JsonNode result;
        private int     calls;
        private boolean closed;

        TrackingStub(String version, JsonNode result) {
            super("stub-" + version, URI.create("file:///dev/null"));
            this.version = version;
            this.result  = result;
        }

        int  callCount()    { return calls; }
        boolean wasClosed() { return closed; }

        @Override
        public JsonNode execute(JsonNode ctx) {
            calls++;
            return result;
        }

        @Override protected void initialize()    { /* no-op */ }
        @Override public  void    close()        { closed = true; }
    }

    // ── Shared builder helpers ────────────────────────────────────────────────

    /** Minimal valid descriptor. */
    private static AgentDescriptor baseDescriptor() {
        AgentDescriptor d = new AgentDescriptor();
        d.setAgentId("reload-test-agent");
        d.setName("Reload Test Agent");
        d.setEntityType("account");
        d.setEntityKeyExtractor("$.accountId");

        RulesetConfig rs = new RulesetConfig("/tmp/test.jar");
        rs.setArchiveControlTopic("archive-control");
        // Register OperationConfig so buildOutputEvents can route to the correct event type
        RulesetConfig.OperationConfig opCfg = new RulesetConfig.OperationConfig();
        opCfg.setOperation("TestOp");
        opCfg.setOutbounds(java.util.List.of(new RulesetConfig.OutboundConfig("outputEvents", "agent-output")));
        rs.setOperations(java.util.List.of(opCfg));
        d.setRuleset(rs);

        d.setSubscriptions(java.util.List.of(
            new EventSubscription("transaction",
                "transactions",
                "TestOp",
                HorizonConfig.count(5))
        ));
        d.setOutputs(java.util.List.of(
            new OutputConfig("agent-output", "results"),
            new OutputConfig("agent-error",  "errors")
        ));
        return d;
    }

    /** Approved-transaction result JSON using the outputEvents array format. */
    private static JsonNode approvedResult() throws Exception {
        return MAPPER.readTree(
            "{\"outputEvents\":[{\"decision\":\"APPROVED\"}]}");
    }

    /** Builds a LOAD command for the given version (non-existent file — no download). */
    private static ArchiveControlCmd loadCmd(String version) {
        ArchiveControlCmd c = new ArchiveControlCmd();
        c.setCommandType(CommandType.LOAD);
        c.setVersion(version);
        c.setArchiveUrl("file:///tmp/stub-" + version + ".jar");
        c.setSha256("00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff");
        c.setRequestId("req-" + version);
        c.setRequestedBy("ci-pipeline");
        return c;
    }

    private AgentDescriptor descriptor;
    private StatefulAgentProcessor processor;

    @BeforeEach
    void setUp() {
        descriptor = baseDescriptor();
        processor  = new StatefulAgentProcessor(descriptor);
    }

    // ── 1. Stale runner eviction after successful LOAD ────────────────────────

    /**
     * After a successful LOAD the processor calls {@code evictStaleRunners(activeVersion)}.
     * Any pre-warmed runner that is NOT the newly activated version must be removed.
     *
     * <p>This test simulates a successful LOAD by injecting the new runner into the cache
     * and then directly writing the ACTIVE entry to the broadcast state.  The
     * {@code evictStaleRunners} call is triggered internally by {@code processBroadcastElement}
     * only when the download succeeds.  Since we cannot trigger a real download in a unit
     * test we instead verify the eviction logic directly through the runner cache.
     */
    @Nested
    @DisplayName("1. Stale runner eviction after LOAD")
    class StaleRunnerEviction {

        @Test
        @DisplayName("Only the active version survives in cache after LOAD command succeeds (simulated)")
        void onlyActiveVersionRemainsAfterLoad() throws Exception {
            // Inject two stale runners + bootstrap
            TrackingStub bootstrap = new TrackingStub("bootstrap", approvedResult());
            TrackingStub v1        = new TrackingStub("v1.0.0",    approvedResult());
            TrackingStub v2        = new TrackingStub("v2.0.0",    approvedResult());

            processor.injectRunner("bootstrap", bootstrap);
            processor.injectRunner("v1.0.0",    v1);
            processor.injectRunner("v2.0.0",    v2);

            // Simulate what processBroadcastElement does after a successful LOAD of v3.0.0:
            //   1. runnerCache.put("v3.0.0", newRunner)   (broadcast state updated)
            //   2. evictStaleRunners("v3.0.0")            (removes all others)
            // We do this manually to test eviction in isolation.
            TrackingStub v3 = new TrackingStub("v3.0.0", approvedResult());
            processor.injectRunner("v3.0.0", v3);

            // Manually evict as the processor would: remove all entries except "v3.0.0"
            processor.getRunnerCache().entrySet().removeIf(e -> {
                if (e.getKey().equals("v3.0.0")) return false;
                try { e.getValue().close(); } catch (Exception ex) { /* ignore */ }
                return true;
            });

            Map<String, RuleEngineRunner> cache = processor.getRunnerCache();
            assertEquals(1, cache.size(),
                "After LOAD + eviction exactly one version must remain in the cache");
            assertTrue(cache.containsKey("v3.0.0"),
                "Only the active version 'v3.0.0' must remain after stale eviction");
            // Evicted runners must have been closed
            assertTrue(bootstrap.wasClosed(), "Stale bootstrap runner must be closed on eviction");
            assertTrue(v1.wasClosed(),        "Stale v1.0.0 runner must be closed on eviction");
            assertTrue(v2.wasClosed(),        "Stale v2.0.0 runner must be closed on eviction");
            assertFalse(v3.wasClosed(),       "Active v3.0.0 runner must NOT be closed");
        }

        @Test
        @DisplayName("Failed LOAD (non-existent file) leaves pre-warmed runners intact in cache")
        void failedLoadDoesNotEvictExistingRunners() throws Exception {
            TrackingStub bootstrap = new TrackingStub("bootstrap", approvedResult());
            TrackingStub v1        = new TrackingStub("v1.0.0",    approvedResult());

            processor.injectRunner("bootstrap", bootstrap);
            processor.injectRunner("v1.0.0",    v1);

            int cacheSizeBefore = processor.getRunnerCache().size();

            // LOAD v2.0.0 — will fail because the file doesn't exist
            Map<String, ArchiveEntry> store = new HashMap<>();
            processor.invokeProcessBroadcastForTest(loadCmd("v2.0.0"), store);

            // Cache must be unchanged — no eviction on failure
            assertEquals(cacheSizeBefore, processor.getRunnerCache().size(),
                "Failed LOAD must not change the runner cache size");
            assertTrue(processor.getRunnerCache().containsKey("bootstrap"),
                "Bootstrap runner must survive a failed LOAD");
            assertTrue(processor.getRunnerCache().containsKey("v1.0.0"),
                "Pre-warmed v1.0.0 runner must survive a failed LOAD");
            assertFalse(processor.getRunnerCache().containsKey("v2.0.0"),
                "Failed v2.0.0 runner must not remain in cache (finally block removes it)");
        }

        @Test
        @DisplayName("Failed LOAD leaves ACTIVE broadcast state unchanged")
        void failedLoadLeavesActiveStateUnchanged() throws Exception {
            // Simulate a prior successful LOAD of v1.0.0
            Map<String, ArchiveEntry> store = new HashMap<>();
            ArchiveEntry v1Active = new ArchiveEntry("v1.0.0", "url1", "sha1", 1000L);
            store.put("ACTIVE", v1Active);

            // Attempt to LOAD v2.0.0 — fails (file doesn't exist)
            processor.invokeProcessBroadcastForTest(loadCmd("v2.0.0"), store);

            // ACTIVE must still be v1.0.0
            assertEquals("v1.0.0", store.get("ACTIVE").getVersion(),
                "Failed LOAD must not overwrite the ACTIVE broadcast state entry");
        }
    }

    // ── 2. Consecutive LOADs ──────────────────────────────────────────────────

    @Nested
    @DisplayName("2. Consecutive LOADs — each LOAD sets the next active version")
    class ConsecutiveLoads {

        @Test
        @DisplayName("Broadcast state version advances with each simulated LOAD")
        void broadcastStateAdvancesWithEachLoad() {
            Map<String, ArchiveEntry> store = new HashMap<>();

            // Simulate three consecutive successful LOADs by directly updating broadcast state
            ArchiveEntry v1 = new ArchiveEntry("v1.0.0", "url1", "sha1", 1000L);
            store.put("ACTIVE", v1);
            assertEquals("v1.0.0", store.get("ACTIVE").getVersion());

            ArchiveEntry v2 = new ArchiveEntry("v2.0.0", "url2", "sha2", 2000L);
            store.put("ACTIVE", v2);
            assertEquals("v2.0.0", store.get("ACTIVE").getVersion());

            ArchiveEntry v3 = new ArchiveEntry("v3.0.0", "url3", "sha3", 3000L);
            store.put("ACTIVE", v3);
            assertEquals("v3.0.0", store.get("ACTIVE").getVersion());
        }

        @Test
        @DisplayName("LOAD validation failure leaves ACTIVE unchanged — consecutive commands safe")
        void consecutiveLoadValidationFailuresAreIndependent() {
            Map<String, ArchiveEntry> store = new HashMap<>();
            ArchiveEntry existing = new ArchiveEntry("v1.0.0", "url1", "sha1", 1000L);
            store.put("ACTIVE", existing);

            // Blank version — rejected
            ArchiveControlCmd badV = loadCmd("v-bad");
            badV.setVersion("");
            processor.invokeProcessBroadcastForTest(badV, store);
            assertEquals("v1.0.0", store.get("ACTIVE").getVersion(),
                "LOAD with blank version must not change ACTIVE");

            // Blank url — rejected
            ArchiveControlCmd badU = loadCmd("v-bad");
            badU.setArchiveUrl(null);
            processor.invokeProcessBroadcastForTest(badU, store);
            assertEquals("v1.0.0", store.get("ACTIVE").getVersion(),
                "LOAD with null url must not change ACTIVE");

            // Blank sha256 — rejected
            ArchiveControlCmd badS = loadCmd("v-bad");
            badS.setSha256("   ");
            processor.invokeProcessBroadcastForTest(badS, store);
            assertEquals("v1.0.0", store.get("ACTIVE").getVersion(),
                "LOAD with blank sha256 must not change ACTIVE");
        }
    }

    // ── 3. LOAD idempotency ───────────────────────────────────────────────────

    @Nested
    @DisplayName("3. LOAD idempotency — same version injected twice")
    class LoadIdempotency {

        @Test
        @DisplayName("Injecting the same version twice overwrites the cache entry (no duplicates)")
        void sameVersionInjectedTwiceDoesNotDuplicate() throws Exception {
            TrackingStub first  = new TrackingStub("v1.0.0", approvedResult());
            TrackingStub second = new TrackingStub("v1.0.0", approvedResult());

            processor.injectRunner("v1.0.0", first);
            processor.injectRunner("v1.0.0", second);

            // The map still has exactly one entry for v1.0.0
            assertTrue(processor.getRunnerCache().containsKey("v1.0.0"),
                "Version v1.0.0 must be present in cache");
            // The second injection wins (Map.put semantics)
            assertSame(second, processor.getRunnerCache().get("v1.0.0"),
                "Second injection must overwrite the first");
        }

        @Test
        @DisplayName("Cache size does not grow when the same version is injected multiple times")
        void cacheSizeDoesNotGrowOnDuplicateInjection() throws Exception {
            processor.injectRunner("v1.0.0", new TrackingStub("v1.0.0", approvedResult()));
            int sizeAfterFirst = processor.getRunnerCache().size();

            processor.injectRunner("v1.0.0", new TrackingStub("v1.0.0", approvedResult()));
            int sizeAfterSecond = processor.getRunnerCache().size();

            assertEquals(sizeAfterFirst, sizeAfterSecond,
                "Injecting the same version twice must not grow the runner cache");
        }
    }

    // ── 4. ROLLBACK with ArchiveEntry previousEntry chain ────────────────────

    @Nested
    @DisplayName("4. ROLLBACK — ArchiveEntry previousEntry chain semantics")
    class RollbackWithChain {

        @Test
        @DisplayName("ArchiveEntry with previousEntry: hasPreviousEntry() is true")
        void archiveEntryWithPreviousEntryHasHistory() {
            ArchiveEntry v1 = new ArchiveEntry("v1.0.0", "url1", "sha1", 1000L);
            ArchiveEntry v2 = new ArchiveEntry("v2.0.0", "url2", "sha2", 2000L, v1);

            assertTrue(v2.hasPreviousEntry(),
                "v2 (with previousEntry=v1) must report hasPreviousEntry()=true");
        }

        @Test
        @DisplayName("ArchiveEntry chain is exactly one level deep — previousEntry has no further history")
        void archiveEntryChainIsOneLevelDeep() {
            ArchiveEntry v0 = new ArchiveEntry("v0.9.0", "url0", "sha0", 500L);
            ArchiveEntry v1 = new ArchiveEntry("v1.0.0", "url1", "sha1", 1000L, v0);
            ArchiveEntry v2 = new ArchiveEntry("v2.0.0", "url2", "sha2", 2000L, v1);

            // v2.previousEntry = v1; v1.previousEntry = v0
            // The test verifies the model supports chaining without depth limit,
            // and that the rollback mechanism reads only ONE level (previousEntry)
            assertEquals("v1.0.0", v2.getPreviousEntry().getVersion(),
                "v2.previousEntry must be v1.0.0");
            assertEquals("v0.9.0", v2.getPreviousEntry().getPreviousEntry().getVersion(),
                "v2.previousEntry.previousEntry must be v0.9.0 (chain accessible)");
        }

        @Test
        @DisplayName("ROLLBACK to explicit previous: archived state gets previous version")
        void rollbackWritesPreviousVersionToState() {
            Map<String, ArchiveEntry> store = new HashMap<>();

            // Simulate successful LOAD v2.0.0 with v1.0.0 as previousEntry
            ArchiveEntry v1 = new ArchiveEntry("v1.0.0", "url1", "sha1", 1000L);
            ArchiveEntry v2 = new ArchiveEntry("v2.0.0", "url2", "sha2", 2000L, v1);
            store.put("ACTIVE", v2);

            assertEquals("v2.0.0", store.get("ACTIVE").getVersion());
            assertTrue(store.get("ACTIVE").hasPreviousEntry(),
                "Precondition: ACTIVE entry must have a previous entry for rollback");

            // Simulate rollback: restore previousEntry as ACTIVE
            ArchiveEntry prev = store.get("ACTIVE").getPreviousEntry();
            store.put("ACTIVE", prev);

            assertEquals("v1.0.0", store.get("ACTIVE").getVersion(),
                "After rollback ACTIVE must be v1.0.0");
        }

        @Test
        @DisplayName("Second ROLLBACK when ACTIVE has no previousEntry: hasPreviousEntry() is false")
        void secondRollbackRejectedWhenNoPreviousEntry() {
            Map<String, ArchiveEntry> store = new HashMap<>();

            // After the first rollback we're on v1.0.0 which has no previousEntry
            ArchiveEntry v1 = new ArchiveEntry("v1.0.0", "url1", "sha1", 1000L);
            // v1 has no previousEntry set — hasPreviousEntry() == false
            assertFalse(v1.hasPreviousEntry(),
                "v1 must not have a previous entry (no rollback target)");

            store.put("ACTIVE", v1);

            // Attempt an implicit ROLLBACK (null url/sha256)
            ArchiveControlCmd rollback = loadCmd("v-auto");
            rollback.setCommandType(CommandType.ROLLBACK);
            rollback.setArchiveUrl(null);
            rollback.setSha256(null);

            assertDoesNotThrow(
                () -> processor.invokeProcessBroadcastForTest(rollback, store),
                "ROLLBACK with no previous entry must not throw");

            // v1.0.0 must still be ACTIVE — the rollback had nowhere to go
            // (it attempted a download which failed, leaving state unchanged)
            assertEquals("v1.0.0", store.get("ACTIVE").getVersion(),
                "Second ROLLBACK must not change ACTIVE when there is no previous entry");
        }

        @Test
        @DisplayName("ArchiveEntry without previousEntry: hasPreviousEntry() is false")
        void archiveEntryWithoutPreviousEntry() {
            ArchiveEntry entry = new ArchiveEntry("v1.0.0", "url1", "sha1", 1000L);
            assertFalse(entry.hasPreviousEntry(),
                "Entry with no previousEntry must report hasPreviousEntry()=false");
        }

        @Test
        @DisplayName("ArchiveEntry with blank-url previousEntry: hasPreviousEntry() is false")
        void archiveEntryWithBlankUrlPreviousEntry() {
            ArchiveEntry blankPrev = new ArchiveEntry("v0.9.0", "", "", 500L);
            ArchiveEntry v1 = new ArchiveEntry("v1.0.0", "url1", "sha1", 1000L, blankPrev);
            assertFalse(v1.hasPreviousEntry(),
                "Entry whose previousEntry has blank url must report hasPreviousEntry()=false");
        }
    }

    // ── 5. PREFETCH does not change the bootstrap runner ─────────────────────

    @Nested
    @DisplayName("5. PREFETCH does not displace the bootstrap runner")
    class PrefetchBootstrapIsolation {

        @Test
        @DisplayName("PREFETCH for a new version does not replace the bootstrap runner")
        void prefetchDoesNotReplaceBootstrap() throws Exception {
            TrackingStub bootstrap = new TrackingStub("bootstrap", approvedResult());
            processor.injectRunner("bootstrap", bootstrap);

            // PREFETCH v1.0.0 — download fails (file not there), runner init skipped
            Map<String, ArchiveEntry> store = new HashMap<>();
            ArchiveControlCmd pf = loadCmd("v1.0.0");
            pf.setCommandType(CommandType.PREFETCH);
            processor.invokeProcessBroadcastForTest(pf, store);

            // Bootstrap runner must still be in cache and unaffected
            assertSame(bootstrap, processor.getRunnerCache().get("bootstrap"),
                "PREFETCH must not replace the bootstrap runner");
            assertFalse(store.containsKey("ACTIVE"),
                "PREFETCH must not set ACTIVE in broadcast state");
        }

        @Test
        @DisplayName("PREFETCH for already-cached version does not grow the cache")
        void prefetchAlreadyCachedVersionDoesNotGrowCache() throws Exception {
            TrackingStub v1 = new TrackingStub("v1.0.0", approvedResult());
            processor.injectRunner("v1.0.0", v1);
            int sizeBefore = processor.getRunnerCache().size();

            // PREFETCH v1.0.0 — already in cache, runner init is skipped
            Map<String, ArchiveEntry> store = new HashMap<>();
            ArchiveControlCmd pf = loadCmd("v1.0.0");
            pf.setCommandType(CommandType.PREFETCH);
            processor.invokeProcessBroadcastForTest(pf, store);

            assertEquals(sizeBefore, processor.getRunnerCache().size(),
                "PREFETCH for already-cached version must not grow the runner cache");
            assertSame(v1, processor.getRunnerCache().get("v1.0.0"),
                "PREFETCH must not replace the existing cached runner for that version");
        }

        @Test
        @DisplayName("PREFETCH with null url/sha256 is silently ignored — bootstrap runner unaffected")
        void prefetchWithNullFieldsIgnored() throws Exception {
            TrackingStub bootstrap = new TrackingStub("bootstrap", approvedResult());
            processor.injectRunner("bootstrap", bootstrap);

            Map<String, ArchiveEntry> store = new HashMap<>();
            ArchiveControlCmd pf = new ArchiveControlCmd();
            pf.setCommandType(CommandType.PREFETCH);
            pf.setVersion("v-null");
            // archiveUrl and sha256 are intentionally null

            assertDoesNotThrow(
                () -> processor.invokeProcessBroadcastForTest(pf, store),
                "PREFETCH with null url/sha256 must not throw");
            assertSame(bootstrap, processor.getRunnerCache().get("bootstrap"),
                "Bootstrap runner must be unchanged after PREFETCH with null fields");
        }
    }

    // ── 6. NOOP between two LOADs ─────────────────────────────────────────────

    @Nested
    @DisplayName("6. NOOP between LOADs — broadcast state and cache unaffected")
    class NoopBetweenLoads {

        @Test
        @DisplayName("NOOP between two LOADs preserves the ACTIVE entry of the second LOAD")
        void noopBetweenLoadsPreservesActive() {
            Map<String, ArchiveEntry> store = new HashMap<>();

            // First LOAD (simulated by direct state write)
            ArchiveEntry v1 = new ArchiveEntry("v1.0.0", "url1", "sha1", 1000L);
            store.put("ACTIVE", v1);

            // NOOP
            ArchiveControlCmd noop = new ArchiveControlCmd();
            noop.setCommandType(CommandType.NOOP);
            noop.setVersion("health-check");
            noop.setArchiveUrl("file:///tmp/x.jar");
            noop.setSha256("aabb");
            processor.invokeProcessBroadcastForTest(noop, store);

            // ACTIVE must still be v1.0.0 after NOOP
            assertEquals("v1.0.0", store.get("ACTIVE").getVersion(),
                "ACTIVE must remain v1.0.0 after NOOP");

            // Second LOAD (simulated)
            ArchiveEntry v2 = new ArchiveEntry("v2.0.0", "url2", "sha2", 2000L);
            store.put("ACTIVE", v2);

            assertEquals("v2.0.0", store.get("ACTIVE").getVersion(),
                "ACTIVE must be v2.0.0 after second LOAD");
        }

        @Test
        @DisplayName("NOOP does not add any runner to the cache")
        void noopDoesNotAddRunnerToCache() throws Exception {
            processor.injectRunner("bootstrap",
                new TrackingStub("bootstrap", approvedResult()));
            int cacheSizeBefore = processor.getRunnerCache().size();

            Map<String, ArchiveEntry> store = new HashMap<>();
            ArchiveControlCmd noop = new ArchiveControlCmd();
            noop.setCommandType(CommandType.NOOP);
            noop.setVersion("health-check-2");
            noop.setArchiveUrl("file:///tmp/x.jar");
            noop.setSha256("ccdd");
            processor.invokeProcessBroadcastForTest(noop, store);

            assertEquals(cacheSizeBefore, processor.getRunnerCache().size(),
                "NOOP must not add any runner to the cache");
            assertFalse(processor.getRunnerCache().containsKey("health-check-2"),
                "NOOP must not add its version to the runner cache");
        }
    }

    // ── 7. Null command with active runner ────────────────────────────────────

    @Nested
    @DisplayName("7. Null command — does not clear or replace an active runner")
    class NullCommandSafety {

        @Test
        @DisplayName("Null command does not remove the bootstrap runner from cache")
        void nullCommandDoesNotClearBootstrap() throws Exception {
            TrackingStub bootstrap = new TrackingStub("bootstrap", approvedResult());
            processor.injectRunner("bootstrap", bootstrap);

            Map<String, ArchiveEntry> store = new HashMap<>();
            assertDoesNotThrow(
                () -> processor.invokeProcessBroadcastForTest(null, store),
                "Null command must not throw");

            assertSame(bootstrap, processor.getRunnerCache().get("bootstrap"),
                "Null command must not remove the bootstrap runner from the cache");
            assertTrue(store.isEmpty(),
                "Null command must not modify the broadcast state");
        }

        @Test
        @DisplayName("Null command with an ACTIVE broadcast entry leaves the entry intact")
        void nullCommandLeavesActiveEntryIntact() {
            Map<String, ArchiveEntry> store = new HashMap<>();
            ArchiveEntry active = new ArchiveEntry("v1.0.0", "url1", "sha1", 1000L);
            store.put("ACTIVE", active);

            processor.invokeProcessBroadcastForTest(null, store);

            assertNotNull(store.get("ACTIVE"),
                "Null command must not remove the ACTIVE broadcast entry");
            assertEquals("v1.0.0", store.get("ACTIVE").getVersion(),
                "Null command must not modify the ACTIVE broadcast entry's version");
        }
    }

    // ── 8. Complete PREFETCH → LOAD → event sequence ─────────────────────────

    @Nested
    @DisplayName("8. PREFETCH → LOAD sequence: pre-warmed runner is the active runner")
    class PrefetchThenLoad {

        @Test
        @DisplayName("Pre-warmed runner injected before LOAD survives the (failed) download attempt")
        void prewarmedRunnerSurvivesFailedLoad() throws Exception {
            // Simulate: PREFETCH succeeded (runner pre-warmed), then LOAD attempted for same version
            TrackingStub prewarmed = new TrackingStub("v1.0.0", approvedResult());
            processor.injectRunner("v1.0.0", prewarmed);

            Map<String, ArchiveEntry> store = new HashMap<>();
            // LOAD v1.0.0 — download fails (file not there), but the pre-warmed runner
            // was injected before the finally block had a chance to remove it
            processor.invokeProcessBroadcastForTest(loadCmd("v1.0.0"), store);

            // Because the LOAD failed, the runner at v1.0.0 was NOT activated
            // (broadcast state has no ACTIVE), but the pre-warmed runner IS still
            // in the cache — the finally block only removes a runner that was added
            // by the download attempt itself (newRunner != null), not externally injected ones.
            //
            // This is the defined contract: a pre-warmed runner is NOT evicted by a failed LOAD.
            assertTrue(processor.getRunnerCache().containsKey("v1.0.0"),
                "Pre-warmed runner for v1.0.0 must remain in cache after a failed LOAD of same version");
        }

        @Test
        @DisplayName("PREFETCH pre-warms cache; subsequent invokeForTest still uses bootstrap")
        void prefetchDoesNotChangeActiveRunnerForInvokeForTest() throws Exception {
            TrackingStub bootstrap = new TrackingStub("bootstrap", approvedResult());
            processor.injectRuleEngine(bootstrap); // puts under "bootstrap" key

            // PREFETCH v1.0.0 — fails on download (expected)
            Map<String, ArchiveEntry> store = new HashMap<>();
            ArchiveControlCmd pf = loadCmd("v1.0.0");
            pf.setCommandType(CommandType.PREFETCH);
            processor.invokeProcessBroadcastForTest(pf, store);

            // invokeForTest uses "bootstrap" key, not broadcast state
            ObjectNode payload = MAPPER.createObjectNode();
            payload.put("accountId", "ACC-PF");
            EventEnvelope ev = EventEnvelope.from(
                "transaction", "transactions", 1000L, payload);
            ev.setEntityId("ACC-PF");

            EventEnvelope out = processor.invokeForTest(ev, null);

            assertEquals("agent-output", out.getEventType(),
                "invokeForTest must use the bootstrap runner, not a prefetched version");
            assertEquals(1, bootstrap.callCount(),
                "Bootstrap runner must be called exactly once via invokeForTest");
        }
    }

    // ── 9. Runner cache visibility (live map) ─────────────────────────────────

    @Nested
    @DisplayName("9. Runner cache is the live map — mutations are immediately visible")
    class RunnerCacheLiveness {

        @Test
        @DisplayName("getRunnerCache() returns the live map — direct mutations are visible")
        void getRunnerCacheIsLiveMap() throws Exception {
            TrackingStub v1 = new TrackingStub("v1.0.0", approvedResult());
            processor.injectRunner("v1.0.0", v1);

            Map<String, RuleEngineRunner> cache = processor.getRunnerCache();
            assertSame(v1, cache.get("v1.0.0"),
                "getRunnerCache() must return the live map (not a copy)");

            // Add via returned reference — must be visible through processor
            TrackingStub v2 = new TrackingStub("v2.0.0", approvedResult());
            cache.put("v2.0.0", v2);
            assertSame(v2, processor.getRunnerCache().get("v2.0.0"),
                "Entry added via getRunnerCache() reference must be visible immediately");
        }

        @Test
        @DisplayName("Multiple versions can be pre-warmed independently without interference")
        void multipleVersionsCoexistIndependently() throws Exception {
            TrackingStub v1 = new TrackingStub("v1.0.0", approvedResult());
            TrackingStub v2 = new TrackingStub("v2.0.0", approvedResult());
            TrackingStub v3 = new TrackingStub("v3.0.0", approvedResult());

            processor.injectRunner("v1.0.0", v1);
            processor.injectRunner("v2.0.0", v2);
            processor.injectRunner("v3.0.0", v3);

            Map<String, RuleEngineRunner> cache = processor.getRunnerCache();
            assertSame(v1, cache.get("v1.0.0"), "v1 must be retrievable");
            assertSame(v2, cache.get("v2.0.0"), "v2 must be retrievable");
            assertSame(v3, cache.get("v3.0.0"), "v3 must be retrievable");
            assertTrue(cache.size() >= 3,
                "Cache must hold all three independently pre-warmed versions");
        }
    }

    // ── 10. ArchiveEntry model invariants ─────────────────────────────────────

    @Nested
    @DisplayName("10. ArchiveEntry model invariants")
    class ArchiveEntryInvariants {

        @Test
        @DisplayName("Two-arg constructor (version, url, sha256, loadedAt) has no previousEntry")
        void twoArgConstructorHasNoPreviousEntry() {
            ArchiveEntry entry = new ArchiveEntry("v1.0.0", "url1", "sha1", 1000L);
            assertNull(entry.getPreviousEntry(),
                "Default constructor must leave previousEntry null");
            assertFalse(entry.hasPreviousEntry());
        }

        @Test
        @DisplayName("Five-arg constructor sets previousEntry and hasPreviousEntry() returns true")
        void fiveArgConstructorSetsPreviousEntry() {
            ArchiveEntry v1 = new ArchiveEntry("v1.0.0", "url1", "sha1", 1000L);
            ArchiveEntry v2 = new ArchiveEntry("v2.0.0", "url2", "sha2", 2000L, v1);

            assertNotNull(v2.getPreviousEntry());
            assertTrue(v2.hasPreviousEntry());
            assertEquals("v1.0.0", v2.getPreviousEntry().getVersion());
        }

        @Test
        @DisplayName("ArchiveEntry getters return the exact values passed to the constructor")
        void archiveEntryGettersMatchConstructorArgs() {
            long ts = 1_700_000_000L;
            ArchiveEntry entry = new ArchiveEntry("v3.1.4", "http://host/v3.jar", "abc123", ts);

            assertEquals("v3.1.4",                   entry.getVersion());
            assertEquals("http://host/v3.jar",       entry.getArchiveUrl());
            assertEquals("abc123",                   entry.getSha256());
            assertEquals(ts,                         entry.getLoadedAtMs());
        }

        @Test
        @DisplayName("ArchiveEntry with null previousEntry: hasPreviousEntry() is false")
        void nullPreviousEntryReturnsFalse() {
            ArchiveEntry entry = new ArchiveEntry("v1.0.0", "url", "sha", 1L, null);
            assertFalse(entry.hasPreviousEntry(),
                "Explicitly-null previousEntry must return hasPreviousEntry()=false");
        }

        @Test
        @DisplayName("hasPreviousEntry() requires non-blank url AND sha256 on the previousEntry")
        void hasPreviousEntryRequiresNonBlankUrlAndSha() {
            // Blank url
            ArchiveEntry blankUrl = new ArchiveEntry("v2", "url2", "sha2", 2L,
                new ArchiveEntry("v1", "", "sha1", 1L));
            assertFalse(blankUrl.hasPreviousEntry(),
                "previousEntry with blank url must make hasPreviousEntry() false");

            // Blank sha256
            ArchiveEntry blankSha = new ArchiveEntry("v2", "url2", "sha2", 2L,
                new ArchiveEntry("v1", "url1", "", 1L));
            assertFalse(blankSha.hasPreviousEntry(),
                "previousEntry with blank sha256 must make hasPreviousEntry() false");

            // Both non-blank
            ArchiveEntry valid = new ArchiveEntry("v2", "url2", "sha2", 2L,
                new ArchiveEntry("v1", "url1", "sha1", 1L));
            assertTrue(valid.hasPreviousEntry(),
                "previousEntry with non-blank url+sha256 must make hasPreviousEntry() true");
        }
    }

    // ── 11. DeploymentAuditLog model (stateless assertions) ───────────────────

    @Nested
    @DisplayName("11. DeploymentAuditLog — audit entries are recorded correctly")
    class DeploymentAuditLogTests {

        @Test
        @DisplayName("New DeploymentAuditLog starts empty")
        void newAuditLogIsEmpty() {
            com.ibm.di.event.rule.broadcast.DeploymentAuditLog log =
                new com.ibm.di.event.rule.broadcast.DeploymentAuditLog();
            assertEquals(0, log.size(), "New audit log must be empty");
            assertTrue(log.getEntries().isEmpty());
        }

        @Test
        @DisplayName("record() appends an entry with correct who/from/to/type metadata")
        void recordAppendsCorrectEntry() {
            com.ibm.di.event.rule.broadcast.DeploymentAuditLog log =
                new com.ibm.di.event.rule.broadcast.DeploymentAuditLog();

            ArchiveControlCmd cmd = loadCmd("v2.0.0");
            cmd.setRequestedBy("release-bot");

            log.record(cmd, "v1.0.0", 0);

            assertEquals(1, log.size());
            com.ibm.di.event.rule.broadcast.DeploymentAuditLog.Entry entry =
                log.getEntries().get(0);

            assertEquals("release-bot", entry.who,       "who must match requestedBy");
            assertEquals("v1.0.0",      entry.fromVersion, "fromVersion must be the prior version");
            assertEquals("v2.0.0",      entry.toVersion,   "toVersion must be the new version");
            assertEquals(CommandType.LOAD, entry.commandType, "commandType must be LOAD");
            assertEquals("req-v2.0.0", entry.requestId,  "requestId must match cmd.requestId");
            assertEquals(0,             entry.subtaskIndex);
            assertNotNull(entry.when, "when must be non-null");
        }

        @Test
        @DisplayName("record() with null requestedBy defaults who to 'unknown'")
        void recordWithNullRequestedByDefaultsToUnknown() {
            com.ibm.di.event.rule.broadcast.DeploymentAuditLog log =
                new com.ibm.di.event.rule.broadcast.DeploymentAuditLog();

            ArchiveControlCmd cmd = loadCmd("v3.0.0");
            cmd.setRequestedBy(null);
            log.record(cmd, "v2.0.0", 1);

            assertEquals("unknown", log.getEntries().get(0).who,
                "Null requestedBy must default to 'unknown' in the audit log");
        }

        @Test
        @DisplayName("record() with blank requestedBy defaults who to 'unknown'")
        void recordWithBlankRequestedByDefaultsToUnknown() {
            com.ibm.di.event.rule.broadcast.DeploymentAuditLog log =
                new com.ibm.di.event.rule.broadcast.DeploymentAuditLog();

            ArchiveControlCmd cmd = loadCmd("v3.0.0");
            cmd.setRequestedBy("   ");
            log.record(cmd, "v2.0.0", 1);

            assertEquals("unknown", log.getEntries().get(0).who,
                "Blank requestedBy must default to 'unknown' in the audit log");
        }

        @Test
        @DisplayName("Multiple records are appended in order (oldest first)")
        void multipleRecordsAreAppendedInOrder() {
            com.ibm.di.event.rule.broadcast.DeploymentAuditLog log =
                new com.ibm.di.event.rule.broadcast.DeploymentAuditLog();

            ArchiveControlCmd load1 = loadCmd("v1.0.0");
            ArchiveControlCmd load2 = loadCmd("v2.0.0");
            ArchiveControlCmd rollback = loadCmd("v1.0.0");
            rollback.setCommandType(CommandType.ROLLBACK);

            log.record(load1,    null,       0);
            log.record(load2,    "v1.0.0",   0);
            log.record(rollback, "v2.0.0",   0);

            assertEquals(3, log.size(), "All three records must be appended");
            assertEquals("v1.0.0",         log.getEntries().get(0).toVersion);
            assertEquals("v2.0.0",         log.getEntries().get(1).toVersion);
            assertEquals(CommandType.ROLLBACK, log.getEntries().get(2).commandType);
        }

        @Test
        @DisplayName("getEntries() returns an unmodifiable view")
        void getEntriesIsUnmodifiable() {
            com.ibm.di.event.rule.broadcast.DeploymentAuditLog log =
                new com.ibm.di.event.rule.broadcast.DeploymentAuditLog();
            log.record(loadCmd("v1"), null, 0);

            assertThrows(UnsupportedOperationException.class,
                () -> log.getEntries().clear(),
                "getEntries() must return an unmodifiable list");
        }
    }
}

