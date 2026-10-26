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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Edge-case and boundary tests for {@link HorizonConfig#timeLimitMs()}.
 *
 * <p>The main happy-path tests (24h, 7d, 30m) already exist in
 * {@link AgentDescriptorTest}.  This class covers:
 *
 * <ul>
 *   <li>Seconds unit ({@code "90s"}, {@code "1s"}) — parseable, not covered in main tests</li>
 *   <li>Numeric boundary values: {@code "0d"}, {@code "1d"}, {@code "365d"}</li>
 *   <li>Whitespace handling: leading/trailing spaces are tolerated (trimmed)</li>
 *   <li>Case-insensitivity: {@code "24H"}, {@code "7D"}, {@code "30M"}, {@code "60S"}</li>
 *   <li>Missing unit suffix throws {@link IllegalArgumentException}</li>
 *   <li>Non-numeric prefix throws {@link IllegalArgumentException}</li>
 *   <li>Blank / null timeLimit returns 0 (no time ceiling)</li>
 *   <li>{@code effectiveCountLimit()} boundary: countLimit=0 → falls back to value</li>
 *   <li>Factory methods: {@code count()}, {@code time()}, {@code hybrid()} produce expected state</li>
 *   <li>{@code hybrid()} keeps {@code value} and {@code countLimit} in sync</li>
 *   <li>{@code toString()} contains all key fields</li>
 * </ul>
 */
@DisplayName("HorizonConfig edge cases and boundary conditions")
class HorizonConfigEdgeCaseTest {

    // ── timeLimitMs: all four units ───────────────────────────────────────────

    @Nested
    @DisplayName("timeLimitMs() — all supported unit suffixes")
    class TimeLimitAllUnits {

        @Test
        @DisplayName("'s' unit: '90s' → 90 000 ms")
        void secondsUnit90() {
            HorizonConfig h = HorizonConfig.time("90s");
            assertEquals(90_000L, h.timeLimitMs());
        }

        @Test
        @DisplayName("'s' unit: '1s' → 1 000 ms")
        void secondsUnitOne() {
            HorizonConfig h = HorizonConfig.time("1s");
            assertEquals(1_000L, h.timeLimitMs());
        }

        @Test
        @DisplayName("'mn' unit: '60mn' → 3 600 000 ms (1 hour)")
        void minutesUnit60() {
            HorizonConfig h = HorizonConfig.time("60mn");
            assertEquals(3_600_000L, h.timeLimitMs());
        }

        @Test
        @DisplayName("'mn' unit: '1mn' → 60 000 ms")
        void minutesUnitOne() {
            HorizonConfig h = HorizonConfig.time("1mn");
            assertEquals(60_000L, h.timeLimitMs());
        }

        @Test
        @DisplayName("'m' unit: '1m' → 2 592 000 000 ms (30 days / 1 month)")
        void monthsUnitOne() {
            HorizonConfig h = HorizonConfig.time("1m");
            assertEquals(2_592_000_000L, h.timeLimitMs());
        }

        @Test
        @DisplayName("'m' unit: '7m' → 7 × 30 × 86 400 000 ms (7 months)")
        void monthsUnitSeven() {
            HorizonConfig h = HorizonConfig.time("7m");
            assertEquals(7L * 30L * 86_400_000L, h.timeLimitMs());
        }

        @Test
        @DisplayName("'h' unit: '1h' → 3 600 000 ms")
        void hoursUnitOne() {
            HorizonConfig h = HorizonConfig.time("1h");
            assertEquals(3_600_000L, h.timeLimitMs());
        }

        @Test
        @DisplayName("'d' unit: '1d' → 86 400 000 ms")
        void daysUnitOne() {
            HorizonConfig h = HorizonConfig.time("1d");
            assertEquals(86_400_000L, h.timeLimitMs());
        }

        @Test
        @DisplayName("'d' unit: '365d' → 365 × 86 400 000 ms")
        void daysUnit365() {
            HorizonConfig h = HorizonConfig.time("365d");
            assertEquals(365L * 86_400_000L, h.timeLimitMs());
        }

        @Test
        @DisplayName("'d' unit: '0d' → 0 ms (zero-length window)")
        void daysUnitZero() {
            HorizonConfig h = HorizonConfig.time("0d");
            assertEquals(0L, h.timeLimitMs());
        }
    }

    // ── timeLimitMs: case-insensitivity ───────────────────────────────────────

    @Nested
    @DisplayName("timeLimitMs() — case-insensitive unit suffix")
    class TimeLimitCaseInsensitivity {

        @Test
        @DisplayName("'24H' (uppercase) → same as '24h'")
        void upperCaseH() {
            assertEquals(
                HorizonConfig.time("24h").timeLimitMs(),
                HorizonConfig.time("24H").timeLimitMs());
        }

        @Test
        @DisplayName("'7D' (uppercase) → same as '7d'")
        void upperCaseD() {
            assertEquals(
                HorizonConfig.time("7d").timeLimitMs(),
                HorizonConfig.time("7D").timeLimitMs());
        }

        @Test
        @DisplayName("'30MN' (uppercase) → same as '30mn'")
        void upperCaseMN() {
            assertEquals(
                HorizonConfig.time("30mn").timeLimitMs(),
                HorizonConfig.time("30MN").timeLimitMs());
        }

        @Test
        @DisplayName("'6M' (uppercase) → same as '6m'")
        void upperCaseM() {
            assertEquals(
                HorizonConfig.time("6m").timeLimitMs(),
                HorizonConfig.time("6M").timeLimitMs());
        }

        @Test
        @DisplayName("'60S' (uppercase) → same as '60s'")
        void upperCaseS() {
            assertEquals(
                HorizonConfig.time("60s").timeLimitMs(),
                HorizonConfig.time("60S").timeLimitMs());
        }
    }

    // ── timeLimitMs: whitespace handling ──────────────────────────────────────

    @Nested
    @DisplayName("timeLimitMs() — whitespace tolerance")
    class TimeLimitWhitespace {

        @Test
        @DisplayName("Leading and trailing spaces are trimmed before parsing")
        void leadingTrailingSpacesTrimmed() {
            HorizonConfig h = new HorizonConfig();
            h.setTimeLimit("  24h  ");
            assertEquals(86_400_000L, h.timeLimitMs(),
                "Leading/trailing whitespace must be trimmed before parsing");
        }
    }

    // ── timeLimitMs: blank/null → 0 ──────────────────────────────────────────

    @Nested
    @DisplayName("timeLimitMs() — blank or null timeLimit returns 0")
    class TimeLimitBlankOrNull {

        @Test
        @DisplayName("null timeLimit → 0 (no ceiling)")
        void nullTimeLimitReturnsZero() {
            HorizonConfig h = new HorizonConfig();
            // timeLimit is null by default
            assertEquals(0L, h.timeLimitMs());
        }

        @ParameterizedTest(name = "blank=''{0}'' → 0")
        @ValueSource(strings = {"", " ", "   ", "\t"})
        @DisplayName("Blank timeLimit → 0 (no ceiling)")
        void blankTimeLimitReturnsZero(String blank) {
            HorizonConfig h = new HorizonConfig();
            h.setTimeLimit(blank);
            assertEquals(0L, h.timeLimitMs());
        }
    }

    // ── timeLimitMs: invalid input throws ────────────────────────────────────

    @Nested
    @DisplayName("timeLimitMs() — invalid input throws IllegalArgumentException")
    class TimeLimitInvalidInput {

        @Test
        @DisplayName("No unit suffix throws IllegalArgumentException")
        void noUnitSuffixThrows() {
            HorizonConfig h = new HorizonConfig();
            h.setTimeLimit("24");
            assertThrows(IllegalArgumentException.class, h::timeLimitMs,
                "Missing unit suffix must throw IllegalArgumentException");
        }

        @Test
        @DisplayName("Non-numeric prefix throws IllegalArgumentException")
        void nonNumericPrefixThrows() {
            HorizonConfig h = new HorizonConfig();
            h.setTimeLimit("abch");
            assertThrows(IllegalArgumentException.class, h::timeLimitMs,
                "Non-numeric prefix must throw IllegalArgumentException");
        }

        @Test
        @DisplayName("Invalid unit letter throws IllegalArgumentException")
        void invalidUnitLetterThrows() {
            HorizonConfig h = new HorizonConfig();
            h.setTimeLimit("10x"); // 'x' is not a valid unit
            assertThrows(IllegalArgumentException.class, h::timeLimitMs,
                "Unsupported unit letter must throw IllegalArgumentException");
        }

        @Test
        @DisplayName("Exception message includes the offending timeLimit value")
        void exceptionMessageContainsValue() {
            HorizonConfig h = new HorizonConfig();
            h.setTimeLimit("bad-input");
            IllegalArgumentException ex =
                assertThrows(IllegalArgumentException.class, h::timeLimitMs);
            assertTrue(ex.getMessage().contains("bad-input"),
                "Exception message must quote the offending timeLimit");
        }
    }

    // ── effectiveCountLimit edge cases ────────────────────────────────────────

    @Nested
    @DisplayName("effectiveCountLimit() — edge cases")
    class EffectiveCountLimit {

        @Test
        @DisplayName("Both countLimit=0 and value=0 → 0 (unlimited)")
        void bothZeroReturnsZero() {
            HorizonConfig h = new HorizonConfig();
            h.setType("count");
            h.setValue(0);
            h.setCountLimit(0);
            assertEquals(0, h.effectiveCountLimit());
        }

        @Test
        @DisplayName("countLimit negative treated as 0 — falls back to value")
        void negativeCountLimitFallsBackToValue() {
            HorizonConfig h = new HorizonConfig();
            h.setType("count");
            h.setValue(10);
            h.setCountLimit(-1); // negative: should not be preferred
            // countLimit > 0 is false for -1, so value is used
            assertEquals(10, h.effectiveCountLimit(),
                "Negative countLimit must not satisfy countLimit>0 guard; value must be used");
        }

        @Test
        @DisplayName("countLimit=1 → 1 (lowest meaningful limit)")
        void countLimitOneReturnsOne() {
            HorizonConfig h = HorizonConfig.count(1);
            assertEquals(1, h.effectiveCountLimit());
        }
    }

    // ── Factory methods ───────────────────────────────────────────────────────

    @Nested
    @DisplayName("Factory methods — count(), time(), hybrid()")
    class FactoryMethods {

        @Test
        @DisplayName("count(N) sets type='count' and value=N; timeLimit is null")
        void countFactory() {
            HorizonConfig h = HorizonConfig.count(25);
            assertEquals("count", h.getType());
            assertEquals(25,      h.getValue());
            assertEquals(25,      h.effectiveCountLimit());
            assertNull(h.getTimeLimit(), "count() must leave timeLimit null");
        }

        @Test
        @DisplayName("time(tl) sets type='time' and timeLimit; value and countLimit are 0")
        void timeFactory() {
            HorizonConfig h = HorizonConfig.time("12h");
            assertEquals("time", h.getType());
            assertEquals("12h",  h.getTimeLimit());
            assertEquals(0,      h.getValue());
            assertEquals(0,      h.getCountLimit());
        }

        @Test
        @DisplayName("hybrid(n, tl) sets type='hybrid', countLimit=n, value=n, and timeLimit=tl")
        void hybridFactory() {
            HorizonConfig h = HorizonConfig.hybrid(50, "7d");
            assertEquals("hybrid", h.getType());
            assertEquals(50,       h.getCountLimit());
            assertEquals(50,       h.getValue(), "hybrid() must keep value in sync with countLimit");
            assertEquals("7d",     h.getTimeLimit());
            assertEquals(50,       h.effectiveCountLimit());
        }
    }

    // ── toString() ────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("toString() — contains key fields")
    class ToStringTests {

        @Test
        @DisplayName("toString includes type, value, countLimit, and timeLimit")
        void toStringContainsKeyFields() {
            HorizonConfig h = HorizonConfig.hybrid(10, "24h");
            String s = h.toString();

            assertTrue(s.contains("hybrid"), "toString must contain type");
            assertTrue(s.contains("10"),     "toString must contain countLimit/value");
            assertTrue(s.contains("24h"),    "toString must contain timeLimit");
        }
    }
}

