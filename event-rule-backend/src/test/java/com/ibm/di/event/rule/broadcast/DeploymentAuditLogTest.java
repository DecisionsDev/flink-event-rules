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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link DeploymentAuditLog}.
 *
 * <p>Covered:
 * <ul>
 *   <li>Empty log on construction</li>
 *   <li>Single entry: who / when / fromVersion / toVersion / commandType / requestId / subtaskIndex</li>
 *   <li>{@code requestedBy = null} falls back to {@code "unknown"}</li>
 *   <li>{@code requestedBy = blank} falls back to {@code "unknown"}</li>
 *   <li>Multiple entries accumulate in insertion order</li>
 *   <li>Cold-start: {@code fromVersion = null} when no previous entry existed</li>
 *   <li>ROLLBACK command type is recorded correctly</li>
 *   <li>Returned list is unmodifiable</li>
 *   <li>{@code Entry.toString()} contains all key fields</li>
 * </ul>
 */
@DisplayName("DeploymentAuditLog Unit Tests")
class DeploymentAuditLogTest {

    private DeploymentAuditLog log;

    @BeforeEach
    void setUp() {
        log = new DeploymentAuditLog();
    }

    // ── Construction ──────────────────────────────────────────────────────────

    @Nested
    @DisplayName("Construction")
    class Construction {

        @Test
        @DisplayName("New log is empty")
        void emptyOnConstruction() {
            assertEquals(0, log.size());
            assertTrue(log.getEntries().isEmpty());
        }

        @Test
        @DisplayName("getEntries() returns an unmodifiable view")
        void entriesListIsUnmodifiable() {
            List<DeploymentAuditLog.Entry> view = log.getEntries();
            assertThrows(UnsupportedOperationException.class,
                () -> view.add(null),
                "getEntries() must return an unmodifiable list");
        }
    }

    // ── Single entry ──────────────────────────────────────────────────────────

    @Nested
    @DisplayName("Single entry")
    class SingleEntry {

        @Test
        @DisplayName("Entry captures who, when, fromVersion, toVersion, commandType, requestId, subtaskIndex")
        void allFieldsRecorded() {
            ArchiveControlCmd cmd = cmd("v2", "ci-pipeline", "req-001", ArchiveControlCmd.CommandType.LOAD);

            Instant before = Instant.now();
            log.record(cmd, "v1", 2);
            Instant after  = Instant.now();

            assertEquals(1, log.size());
            DeploymentAuditLog.Entry e = log.getEntries().get(0);

            assertEquals("ci-pipeline",                       e.who);
            assertEquals("v1",                                e.fromVersion);
            assertEquals("v2",                                e.toVersion);
            assertEquals(ArchiveControlCmd.CommandType.LOAD,  e.commandType);
            assertEquals("req-001",                           e.requestId);
            assertEquals(2,                                   e.subtaskIndex);

            assertFalse(e.when.isBefore(before), "when must be >= test start");
            assertFalse(e.when.isAfter(after),   "when must be <= test end");
        }

        @Test
        @DisplayName("null requestedBy is recorded as 'unknown'")
        void nullRequestedByBecomesUnknown() {
            ArchiveControlCmd cmd = cmd("v3", null, "req-002", ArchiveControlCmd.CommandType.LOAD);
            log.record(cmd, "v2", 0);
            assertEquals("unknown", log.getEntries().get(0).who);
        }

        @Test
        @DisplayName("blank requestedBy is recorded as 'unknown'")
        void blankRequestedByBecomesUnknown() {
            ArchiveControlCmd cmd = cmd("v3", "  ", "req-003", ArchiveControlCmd.CommandType.LOAD);
            log.record(cmd, "v2", 0);
            assertEquals("unknown", log.getEntries().get(0).who);
        }

        @Test
        @DisplayName("null fromVersion is allowed (cold-start scenario)")
        void nullFromVersionAllowed() {
            ArchiveControlCmd cmd = cmd("v1", "operator", "req-cold", ArchiveControlCmd.CommandType.LOAD);
            log.record(cmd, null, 0);
            assertNull(log.getEntries().get(0).fromVersion,
                "fromVersion must be null on cold start");
        }

        @Test
        @DisplayName("ROLLBACK command type is preserved in the entry")
        void rollbackCommandTypePreserved() {
            ArchiveControlCmd cmd = cmd("v1", "operator", "req-rb", ArchiveControlCmd.CommandType.ROLLBACK);
            log.record(cmd, "v3", 1);
            assertEquals(ArchiveControlCmd.CommandType.ROLLBACK,
                log.getEntries().get(0).commandType);
        }

        @Test
        @DisplayName("null requestId is preserved in the entry")
        void nullRequestIdPreserved() {
            ArchiveControlCmd cmd = cmd("v2", "operator", null, ArchiveControlCmd.CommandType.LOAD);
            log.record(cmd, "v1", 0);
            assertNull(log.getEntries().get(0).requestId);
        }
    }

    // ── Multiple entries ──────────────────────────────────────────────────────

    @Nested
    @DisplayName("Multiple entries")
    class MultipleEntries {

        @Test
        @DisplayName("Entries accumulate in insertion order")
        void entriesAccumulateInOrder() {
            log.record(cmd("v2", "alice", "r1", ArchiveControlCmd.CommandType.LOAD),    "v1", 0);
            log.record(cmd("v3", "bob",   "r2", ArchiveControlCmd.CommandType.LOAD),    "v2", 0);
            log.record(cmd("v2", "carol", "r3", ArchiveControlCmd.CommandType.ROLLBACK), "v3", 0);

            assertEquals(3, log.size());

            List<DeploymentAuditLog.Entry> entries = log.getEntries();
            assertEquals("alice", entries.get(0).who);
            assertEquals("bob",   entries.get(1).who);
            assertEquals("carol", entries.get(2).who);
        }

        @Test
        @DisplayName("size() matches number of record() calls")
        void sizeMatchesRecordCount() {
            for (int i = 0; i < 5; i++) {
                log.record(cmd("v" + (i + 1), "op", "r" + i, ArchiveControlCmd.CommandType.LOAD),
                    "v" + i, 0);
            }
            assertEquals(5, log.size());
        }
    }

    // ── Entry.toString() ──────────────────────────────────────────────────────

    @Nested
    @DisplayName("Entry.toString()")
    class EntryToString {

        @Test
        @DisplayName("toString() contains all key fields")
        void toStringContainsKeyFields() {
            ArchiveControlCmd cmd = cmd("v9", "deploy-bot", "req-99", ArchiveControlCmd.CommandType.LOAD);
            log.record(cmd, "v8", 3);
            String s = log.getEntries().get(0).toString();

            assertTrue(s.contains("deploy-bot"), "must contain who");
            assertTrue(s.contains("v8"),         "must contain fromVersion");
            assertTrue(s.contains("v9"),         "must contain toVersion");
            assertTrue(s.contains("req-99"),     "must contain requestId");
            assertTrue(s.contains("3"),          "must contain subtaskIndex");
            assertTrue(s.contains("LOAD"),       "must contain commandType");
        }
    }

    // ── Helper ────────────────────────────────────────────────────────────────

    private static ArchiveControlCmd cmd(String version, String requestedBy,
                                         String requestId, ArchiveControlCmd.CommandType type) {
        ArchiveControlCmd cmd = new ArchiveControlCmd();
        cmd.setVersion(version);
        cmd.setRequestedBy(requestedBy);
        cmd.setRequestId(requestId);
        cmd.setCommandType(type);
        cmd.setArchiveUrl("file:///tmp/archive-" + version + ".jar");
        cmd.setSha256("aabbcc" + version);
        return cmd;
    }
}

