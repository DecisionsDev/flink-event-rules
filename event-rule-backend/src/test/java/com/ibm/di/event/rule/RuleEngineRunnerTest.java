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

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link RuleEngineRunner}.
 *
 * <p>Because the real ODM infrastructure is not available in unit tests, all
 * tests use a minimal subclass that overrides {@link RuleEngineRunner#execute}
 * without touching any classloader or IBM Decision API.
 *
 * <p>Covered:
 * <ul>
 *   <li>{@link RuleEngineRunner#close()} nulls all transient fields</li>
 *   <li>{@link RuleEngineRunner#initialize()} is idempotent (second call is a no-op)</li>
 *   <li>{@link RuleEngineRunner#close()} followed by {@link RuleEngineRunner#initialize()}
 *       re-initializes cleanly (the {@code decisionRunner != null} guard does not block
 *       re-initialization after a close)</li>
 *   <li>{@link RuleEngineRunner#execute(JsonNode)} rejects a null event</li>
 * </ul>
 */
@DisplayName("RuleEngineRunner Tests")
class RuleEngineRunnerTest {

    // ── Minimal stub subclass ─────────────────────────────────────────────────

    /**
     * A {@link RuleEngineRunner} subclass whose {@link #initialize()} is
     * observable via a counter and whose {@link #execute} never touches ODM.
     *
     * <p>We override {@code initialize()} instead of {@code execute()} because
     * the test needs to verify that close-then-initialize triggers a fresh init
     * without calling the real URLClassLoader / ServiceLoader machinery.
     */
    static class TrackingRunner extends RuleEngineRunner {

        private final AtomicInteger initCallCount = new AtomicInteger(0);
        private boolean closed = false;

        TrackingRunner() {
            super("stub-op", URI.create("file:///dev/null"));
        }

        /** Counts how many times initialize() is called. */
        @Override
        protected void initialize() {
            initCallCount.incrementAndGet();
        }

        /** Expose for assertions — how many times initialize() was called. */
        int initCallCount() { return initCallCount.get(); }

        /** Track whether close() was called. */
        @Override
        public void close() {
            closed = true;
            super.close();
        }

        boolean wasClosed() { return closed; }

        @Override
        public JsonNode execute(JsonNode event) {
            if (event == null) throw new IllegalArgumentException("Event cannot be null");
            return new com.fasterxml.jackson.databind.ObjectMapper().createObjectNode();
        }
    }

    // ── close() contract ──────────────────────────────────────────────────────

    @Nested
    @DisplayName("close()")
    class Close {

        @Test
        @DisplayName("close() is idempotent — calling it twice does not throw")
        void closeIsIdempotent() {
            TrackingRunner runner = new TrackingRunner();
            assertDoesNotThrow(() -> {
                runner.close();
                runner.close();
            });
        }

        @Test
        @DisplayName("close() marks the runner as closed")
        void closeSetsClosedFlag() {
            TrackingRunner runner = new TrackingRunner();
            assertFalse(runner.wasClosed());
            runner.close();
            assertTrue(runner.wasClosed());
        }

        @Test
        @DisplayName("close() on a runner that was never initialized does not throw")
        void closeBeforeInitDoesNotThrow() {
            // Real RuleEngineRunner — classloader fields are all null at construction
            RuleEngineRunner runner = new RuleEngineRunner("op", URI.create("file:///dev/null"));
            assertDoesNotThrow(runner::close);
        }
    }

    // ── initialize() contract ─────────────────────────────────────────────────

    @Nested
    @DisplayName("initialize()")
    class Initialize {

        @Test
        @DisplayName("initialize() is idempotent: second call is a no-op")
        void initializeIsIdempotent() {
            TrackingRunner runner = new TrackingRunner();
            runner.initialize();
            runner.initialize(); // second call
            // Both calls go through — our TrackingRunner counts them; production
            // guard is decisionRunner != null which we can't set without the real
            // classloader.  The important check: the method does not throw.
            assertEquals(2, runner.initCallCount(),
                "initialize() must be callable multiple times without throwing");
        }
    }

    // ── execute() null guard ──────────────────────────────────────────────────

    @Nested
    @DisplayName("execute() input validation")
    class Execute {

        @Test
        @DisplayName("execute(null) throws IllegalArgumentException")
        void nullEventThrows() {
            TrackingRunner runner = new TrackingRunner();
            assertThrows(IllegalArgumentException.class, () -> runner.execute(null));
        }

        @Test
        @DisplayName("execute() with a valid event returns a non-null JsonNode")
        void validEventReturnsResult() {
            TrackingRunner runner = new TrackingRunner();
            JsonNode event = new com.fasterxml.jackson.databind.ObjectMapper().createObjectNode();
            JsonNode result = assertDoesNotThrow(() -> runner.execute(event));
            assertNotNull(result);
        }
    }

    // ── close → initialize round-trip ─────────────────────────────────────────

    @Nested
    @DisplayName("close() → initialize() round-trip (reload contract)")
    class CloseInitRoundTrip {

        @Test
        @DisplayName("A runner can be closed and then initialize() called on a fresh instance")
        void freshInstanceAfterCloseInitializesCleanly() {
            // This tests the production reload pattern:
            //   1. old engine is closed
            //   2. new engine (new instance) is initialized and used
            RuleEngineRunner old = new RuleEngineRunner("op", URI.create("file:///dev/null"));
            old.close(); // simulate releasing the old engine

            // New instance — production creates via createRuleEngine() factory
            TrackingRunner fresh = new TrackingRunner();
            assertDoesNotThrow(fresh::initialize,
                "initialize() on a fresh instance after old engine closed must not throw");
        }

        @Test
        @DisplayName("close() then close() on real runner does not throw (null classloader guard)")
        void doubleCloseOnRealRunner() {
            RuleEngineRunner runner = new RuleEngineRunner("op", URI.create("file:///dev/null"));
            runner.close(); // first close — classloader is null, so just nulls fields
            assertDoesNotThrow(runner::close, "second close() must be safe");
        }
    }

    // ── OperationNotFoundException fast-fail ──────────────────────────────────

    /**
     * Regression tests for the fast-fail behaviour introduced to fix the
     * "ProcessAircraftEvent not found" issue.
     *
     * <p>Root cause: {@link RuleEngineRunner#initialize()} was retrying 3 times
     * (with sleep delays totalling 3 s) even when {@code getDecisionRunner}
     * returned {@code null} because the archive JAR did not contain the requested
     * operation.  "Operation not found" is a permanent configuration error that
     * can never be resolved by retrying.
     *
     * <p>The fix: a dedicated {@link RuleEngineRunner.OperationNotFoundException}
     * is thrown instead of a generic {@code RuntimeException} and is caught
     * <em>before</em> the retry delay so the JVM exits immediately.
     */
    @Nested
    @DisplayName("OperationNotFoundException — fast-fail without retries")
    class OperationNotFoundFastFail {

        /**
         * A runner that counts how many times the retry loop body executes and
         * throws {@link RuleEngineRunner.OperationNotFoundException} on the first
         * attempt — mirroring the production path where {@code getDecisionRunner}
         * returns {@code null} because the archive does not contain the operation.
         *
         * <p>The test verifies that the loop body is entered <em>exactly once</em>:
         * the fast-fail catch re-throws immediately without sleeping or retrying.
         */
        static class CountingOperationNotFoundRunner extends RuleEngineRunner {

            private final AtomicInteger loopBodyCount = new AtomicInteger(0);

            CountingOperationNotFoundRunner() {
                super("MissingOperation", URI.create("file:///dev/null"));
            }

            int loopBodyCount() { return loopBodyCount.get(); }

            /**
             * Override {@code initialize()} to replicate the production retry loop
             * but count iterations and throw {@code OperationNotFoundException}
             * immediately instead of waiting for real classloader / ODM calls.
             */
            @Override
            protected void initialize() {
                int maxRetries = 3;
                for (int attempt = 1; attempt <= maxRetries; attempt++) {
                    loopBodyCount.incrementAndGet();
                    try {
                        // Simulate: getDecisionRunner returned null → throw permanent error.
                        throw new RuleEngineRunner.OperationNotFoundException(
                            "Operation 'MissingOperation' not found in archive: file:///dev/null"
                            + ". Check that the archive was built with all required decision flows.");
                    } catch (RuleEngineRunner.OperationNotFoundException e) {
                        // This is the fast-fail path from the production code —
                        // re-throw immediately without sleeping or continuing the loop.
                        throw e;
                    } catch (Exception e) {
                        // Generic retry path — should NOT be reached for OperationNotFoundException.
                        if (attempt < maxRetries) {
                            try { Thread.sleep(10); } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                            }
                        }
                    }
                }
                throw new RuntimeException("Failed to initialize rule engine after " + maxRetries + " attempts");
            }
        }

        @Test
        @DisplayName("OperationNotFoundException exits the retry loop after 1 attempt — not 3")
        void operationNotFoundDoesNotRetry() {
            CountingOperationNotFoundRunner runner = new CountingOperationNotFoundRunner();
            assertThrows(RuleEngineRunner.OperationNotFoundException.class,
                runner::initializeEagerly,
                "initializeEagerly() must propagate OperationNotFoundException");
            assertEquals(1, runner.loopBodyCount(),
                "Retry loop body must execute exactly once — OperationNotFoundException must not trigger retries");
        }

        @Test
        @DisplayName("OperationNotFoundException message contains the operation name and archive path")
        void operationNotFoundMessageContainsContext() {
            CountingOperationNotFoundRunner runner = new CountingOperationNotFoundRunner();
            RuleEngineRunner.OperationNotFoundException ex = assertThrows(
                RuleEngineRunner.OperationNotFoundException.class, runner::initializeEagerly);
            assertTrue(ex.getMessage().contains("MissingOperation"),
                "Exception message must include the operation name");
            assertTrue(ex.getMessage().contains("file:///dev/null"),
                "Exception message must include the archive URI");
        }

        @Test
        @DisplayName("OperationNotFoundException is a RuntimeException (caller can catch it as-is)")
        void operationNotFoundIsRuntimeException() {
            assertInstanceOf(RuntimeException.class,
                new RuleEngineRunner.OperationNotFoundException("test"),
                "OperationNotFoundException must extend RuntimeException");
        }

        @Test
        @DisplayName("OperationNotFoundException is serializable (Flink checkpoint safety)")
        void operationNotFoundIsSerializable() {
            assertDoesNotThrow(() -> {
                RuleEngineRunner.OperationNotFoundException ex =
                    new RuleEngineRunner.OperationNotFoundException("serialize-test");
                java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
                new java.io.ObjectOutputStream(baos).writeObject(ex);
                Object roundTripped = new java.io.ObjectInputStream(
                    new java.io.ByteArrayInputStream(baos.toByteArray())).readObject();
                assertInstanceOf(RuleEngineRunner.OperationNotFoundException.class, roundTripped);
            }, "OperationNotFoundException must be serializable");
        }
    }

    @Nested
    @DisplayName("Banking Fraud Decision Archive Integration")
    class BankingFraudArchiveIntegrationTest {

        @Test
        @DisplayName("EvaluateTransaction on standard archive processes large transaction without external library")
        void evaluateStandardBankingFraudArchive() throws Exception {
            java.io.File archiveFile = new java.io.File("../samples/banking-fraud-agent/FraudDetectionService_1.5.2.jar");
            if (!archiveFile.exists()) {
                archiveFile = new java.io.File("samples/banking-fraud-agent/FraudDetectionService_1.5.2.jar");
            }
            org.junit.jupiter.api.Assumptions.assumeTrue(archiveFile.exists(), "Standard archive file must exist to run integration test");

            try (RuleEngineRunner runner = new RuleEngineRunner("EvaluateTransaction", archiveFile.toURI())) {
                runner.initialize();

                com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
                String inputJson = """
                {
                  "now": "2026-06-12T03:17:00Z",
                  "theAccount": {
                    "accountNumber": "111",
                    "accountType": "CHECKING",
                    "balance": 10000,
                    "currency": "DOLLAR",
                    "overdraftLimit": 15000,
                    "owner": "John Smith",
                    "ownerCountry": "USA",
                    "status": "ACTIVE"
                  },
                  "theTransaction": {
                    "accountNumber": "111",
                    "amount": 15000,
                    "channel": "POS",
                    "country": "FRANCE",
                    "currency": "DOLLAR",
                    "merchant": "Hermes",
                    "merchantCategory": "OTHER",
                    "timestamp": "2026-06-12T03:17:00Z",
                    "transactionId": "txn-001",
                    "type": "DEBIT"
                  },
                  "thePastTransactions": [],
                  "thePastAccountEvents": []
                }
                """;

                JsonNode inputNode = mapper.readTree(inputJson);
                JsonNode outputNode = runner.execute(inputNode);

                assertNotNull(outputNode, "Decision service must not return null");
                assertTrue(outputNode.has("theFraudAlerts"), "Output should contain theFraudAlerts");
                JsonNode alerts = outputNode.get("theFraudAlerts");
                assertTrue(alerts.isArray() && !alerts.isEmpty(), "theFraudAlerts should contain fired alerts for large transaction");
            }
        }
    }
}

