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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.di.event.agent.EnrichmentConfig;
import com.ibm.di.event.agent.FieldMapping;
import com.ibm.di.event.agent.ReferenceConfig;
import com.ibm.di.event.agent.enrichment.ReferenceTable.ColumnType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Reference data: CSV, table, registry, into path, planner, field mapping")
class ReferenceTableTest {

    private static EnrichmentConfig step(String into, String on) {
        EnrichmentConfig c = new EnrichmentConfig();
        c.setReference("r");
        c.setKey("k");
        c.setOn(on);
        c.setInto(into);
        c.setFields(List.of(new FieldMapping("f", "f")));
        return c;
    }

    // ── CSV ───────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("CSV: quotes, escaped quotes, embedded commas and line breaks, CRLF, BOM, blank lines")
    void csv() {
        List<String[]> rows = CsvReader.parse("\uFEFFa,b\r\n\"x,1\",\"he said \"\"hi\"\"\"\r\n\r\n\"multi\nline\",z\r\n");
        assertEquals(3, rows.size());
        assertArrayEquals(new String[] {"a", "b"}, rows.get(0));
        assertArrayEquals(new String[] {"x,1", "he said \"hi\""}, rows.get(1));
        assertArrayEquals(new String[] {"multi\nline", "z"}, rows.get(2));
    }

    // ── Table ─────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("types are inferred from all values of a column, a declared type wins")
    void types() {
        ReferenceTable t = ReferenceTable.parse("t",
            "k,n,d,b,s,mixed\nA,1,1.5,true,x,1\nB,2,2,false,y,abc\nC,,3.25,TRUE,,2\n", Map.of());
        assertEquals(ColumnType.LONG, t.typeOf("n"));
        assertEquals(ColumnType.DOUBLE, t.typeOf("d"));
        assertEquals(ColumnType.BOOLEAN, t.typeOf("b"));
        assertEquals(ColumnType.STRING, t.typeOf("s"));
        assertEquals(ColumnType.STRING, t.typeOf("mixed"));

        ReferenceTable declared = ReferenceTable.parse("t", "k,n\nA,007\n", Map.of("n", "string"));
        assertEquals("007", declared.value("n", declared.lookup("k", "A").get(0), false).asText());
        assertTrue(declared.value("n", declared.lookup("k", "A").get(0), false).isTextual());
    }

    @Test
    @DisplayName("an empty cell gives no value, a key column is a string")
    void valuesAndKeyColumns() {
        ReferenceTable t = ReferenceTable.parse("t", "id,n\n1,\n2,5\n", Map.of());
        String[] row1 = t.lookup("id", "1").get(0);
        assertNull(t.value("n", row1, false));
        assertTrue(t.value("id", row1, true).isTextual());
        assertTrue(t.value("id", row1, false).isIntegralNumber());
    }

    @Test
    @DisplayName("lookup and duplicate keys")
    void lookup() {
        ReferenceTable t = ReferenceTable.parse("t", "k,v\nA,1\nA,2\nB,3\n", Map.of());
        assertEquals(2, t.lookup("k", "A").size());
        assertEquals("2", t.lookup("k", "A").get(1)[1]);
        assertTrue(t.lookup("k", "Z").isEmpty());
        assertTrue(t.hasDuplicateKeys("k"));
        assertFalse(t.hasDuplicateKeys("v"));
        assertEquals(3, t.size());
        assertThrows(IllegalArgumentException.class, () -> t.lookup("nope", "A"));
    }

    @Test
    @DisplayName("short rows are padded")
    void shortRows() {
        ReferenceTable t = ReferenceTable.parse("t", "k,a,b\nA,1\n", Map.of());
        assertNull(t.value("b", t.lookup("k", "A").get(0), false));
    }

    @Test
    @DisplayName("bad files are rejected")
    void badFiles() {
        assertThrows(IllegalArgumentException.class, () -> ReferenceTable.parse("t", "", Map.of()));
        assertThrows(IllegalArgumentException.class, () -> ReferenceTable.parse("t", "a,,b\n1,2,3\n", Map.of()));
        assertThrows(IllegalArgumentException.class, () -> ReferenceTable.parse("t", "a,a\n1,2\n", Map.of()));
        assertThrows(IllegalArgumentException.class, () -> ReferenceTable.parse("t", "a\n1\n", Map.of("zz", "long")));
        assertThrows(IllegalArgumentException.class, () -> ReferenceTable.parse("t", "a\n1\n", Map.of("a", "date")));
        assertThrows(IllegalArgumentException.class, () -> ReferenceTable.parse("t", "a\nx\n", Map.of("a", "long")));
    }

    // ── Registry ──────────────────────────────────────────────────────────────

    @Test
    @DisplayName("the registry loads a file once and rejects missing files and unsupported types")
    void registry() {
        ReferenceConfig ref = new ReferenceConfig("p", "csv", "samples/airline-agent/enrichment/passengers.csv");
        ReferenceTable first = ReferenceRegistry.get(ref);
        assertSame(first, ReferenceRegistry.get(ref));
        assertTrue(first.hasColumn("passengerId"));
        assertTrue(ReferenceRegistry.exists(ref.getFile()));

        assertThrows(IllegalStateException.class,
            () -> ReferenceRegistry.get(new ReferenceConfig("x", "csv", "samples/does-not-exist.csv")));
        assertFalse(ReferenceRegistry.exists("samples/does-not-exist.csv"));
        assertThrows(IllegalArgumentException.class,
            () -> ReferenceRegistry.get(new ReferenceConfig("x", "json", "a.json")));
        assertThrows(IllegalArgumentException.class,
            () -> ReferenceRegistry.get(new ReferenceConfig("x", "csv", " ")));
    }

    // ── Into path ─────────────────────────────────────────────────────────────

    @Test
    @DisplayName("into: $, $.a.b and $.a.b[*] are accepted, the rest is not")
    void intoPath() {
        assertTrue(IntoPath.parse("$").isRoot());
        IntoPath nested = IntoPath.parse("$.flight.cateringInformation");
        assertEquals(List.of("flight", "cateringInformation"), nested.segments());
        assertFalse(nested.each());
        IntoPath each = IntoPath.parse("$.flight.passengers[*]");
        assertTrue(each.each());
        assertEquals(List.of("flight", "passengers"), each.segments());
        assertEquals("$.flight.passengers[*]", each.toString());
        assertEquals(List.of("check-in"), IntoPath.parse("$.check-in").segments());

        for (String bad : new String[] {"flight", "$.a[0]", "$.a[*].b", "$[*]", "$..a", "$.a.", "", "$.a b"}) {
            assertThrows(IllegalArgumentException.class, () -> IntoPath.parse(bad), bad);
        }
        assertThrows(IllegalArgumentException.class, () -> IntoPath.parse(null));
    }

    @Test
    @DisplayName("simple segments of a plain dotted path only")
    void simpleSegments() {
        assertEquals(List.of("a", "b"), IntoPath.simpleSegments("$.a.b"));
        assertEquals(List.of(), IntoPath.simpleSegments("$"));
        assertNull(IntoPath.simpleSegments("$[?(@.x)]"));
        assertNull(IntoPath.simpleSegments("a.b"));
    }

    // ── Planner ───────────────────────────────────────────────────────────────

    @Test
    @DisplayName("planner: declared order is kept when nothing depends on anything")
    void plannerKeepsOrder() {
        List<EnrichmentConfig> steps = List.of(step("$.a", "$.x"), step("$.b", "$.x"), step("$", "$.x"));
        assertEquals(List.of(0, 1, 2), EnrichmentPlanner.order(steps));
    }

    @Test
    @DisplayName("planner: a [*] step runs after the step that fills the array")
    void plannerEach() {
        List<EnrichmentConfig> steps = List.of(step("$.flight.passengers[*]", "$.passengerId"),
            step("$.flight", "$.flightNumber"), step("$.flight.passengers", "$.flightNumber"));
        List<Integer> order = EnrichmentPlanner.order(steps);
        assertTrue(order.indexOf(2) < order.indexOf(0), "array filled before its elements are enriched");
    }

    @Test
    @DisplayName("planner: a step that reads below another step's target runs after it")
    void plannerReads() {
        List<EnrichmentConfig> steps = List.of(step("$.b", "$.a.id"), step("$.a", "$.x"));
        assertEquals(List.of(1, 0), EnrichmentPlanner.order(steps));
    }

    @Test
    @DisplayName("planner: a cycle is an error")
    void plannerCycle() {
        List<EnrichmentConfig> steps = List.of(step("$.a", "$.b.id"), step("$.b", "$.a.id"));
        assertThrows(IllegalArgumentException.class, () -> EnrichmentPlanner.order(steps));
    }

    // ── Field mapping ─────────────────────────────────────────────────────────

    @Test
    @DisplayName("a field is \"name\" or {\"column\":\"newName\"}")
    void fieldMapping() throws Exception {
        ObjectMapper m = new ObjectMapper();
        FieldMapping plain = m.readValue("\"name\"", FieldMapping.class);
        assertEquals("name", plain.getSource());
        assertEquals("name", plain.getTarget());
        assertEquals("\"name\"", m.writeValueAsString(plain));

        FieldMapping renamed = m.readValue("{\"checkInStatus\":\"check-inStatus\"}", FieldMapping.class);
        assertEquals("checkInStatus", renamed.getSource());
        assertEquals("check-inStatus", renamed.getTarget());
        assertEquals("{\"checkInStatus\":\"check-inStatus\"}", m.writeValueAsString(renamed));

        assertThrows(Exception.class, () -> m.readValue("{\"a\":\"b\",\"c\":\"d\"}", FieldMapping.class));
        assertThrows(Exception.class, () -> m.readValue("3", FieldMapping.class));
    }
}
