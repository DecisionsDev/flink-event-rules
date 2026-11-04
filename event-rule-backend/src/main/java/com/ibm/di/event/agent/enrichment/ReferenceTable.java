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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.TextNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * An in-memory reference data set loaded from CSV, indexed by key column on demand.
 *
 * <p>Cells are kept as strings and converted to JSON when a field is written, with the column
 * type (declared, else inferred from all of the column's values, see
 * {@link com.ibm.di.event.agent.ReferenceConfig#getTypes()}). An empty cell yields no value: the
 * property is not written. Thread-safe once built.
 */
public final class ReferenceTable {

    /** JSON type of a column. */
    public enum ColumnType { STRING, LONG, DOUBLE, BOOLEAN }

    private static final Pattern LONG_RE   = Pattern.compile("-?(0|[1-9][0-9]{0,17})");
    private static final Pattern DOUBLE_RE = Pattern.compile("-?(0|[1-9][0-9]*)\\.[0-9]+([eE][+-]?[0-9]+)?");

    private final String name;
    private final List<String> columns;
    private final Map<String, Integer> columnIndex = new HashMap<>();
    private final List<String[]> rows;
    private final Map<String, ColumnType> declared = new HashMap<>();
    private final Map<String, ColumnType> types = new HashMap<>();
    private final Map<String, Map<String, List<String[]>>> indexes = new ConcurrentHashMap<>();

    private ReferenceTable(String name, List<String> columns, List<String[]> rows,
                           Map<String, String> declaredTypes) {
        this.name = name;
        this.columns = List.copyOf(columns);
        this.rows = rows;
        for (int i = 0; i < columns.size(); i++) columnIndex.put(columns.get(i), i);
        if (declaredTypes != null) {
            for (Map.Entry<String, String> e : declaredTypes.entrySet()) {
                if (!columnIndex.containsKey(e.getKey())) {
                    throw new IllegalArgumentException("Reference '" + name + "': 'types' names the unknown column '"
                        + e.getKey() + "' (columns: " + columns + ")");
                }
                declared.put(e.getKey(), parseType(name, e.getKey(), e.getValue()));
            }
        }
        for (int c = 0; c < columns.size(); c++) {
            String col = columns.get(c);
            types.put(col, declared.containsKey(col) ? declared.get(col) : infer(c));
        }
        validateDeclared();
    }

    /**
     * Parses {@code csv}, whose first line holds the column names.
     *
     * @throws IllegalArgumentException for an empty file, a blank or duplicate column name, an
     *                                  unknown declared type or a value that does not fit it
     */
    public static ReferenceTable parse(String name, String csv, Map<String, String> declaredTypes) {
        List<String[]> raw = CsvReader.parse(csv);
        if (raw.isEmpty()) {
            throw new IllegalArgumentException("Reference '" + name + "' is empty: the first line must name the columns");
        }
        List<String> columns = new ArrayList<>();
        for (String header : raw.get(0)) {
            String h = header.trim();
            if (h.isEmpty()) throw new IllegalArgumentException("Reference '" + name + "' has a blank column name");
            if (columns.contains(h)) throw new IllegalArgumentException("Reference '" + name + "' has the column '" + h + "' twice");
            columns.add(h);
        }
        List<String[]> rows = new ArrayList<>();
        for (int i = 1; i < raw.size(); i++) {
            rows.add(Arrays.copyOf(raw.get(i), columns.size()));   // pads short rows, drops extra cells
        }
        for (String[] r : rows) {
            for (int i = 0; i < r.length; i++) if (r[i] == null) r[i] = "";
        }
        return new ReferenceTable(name, columns, rows, declaredTypes);
    }

    public String name()               { return name; }
    public List<String> columns()      { return columns; }
    public int size()                  { return rows.size(); }
    public boolean hasColumn(String c) { return columnIndex.containsKey(c); }
    public ColumnType typeOf(String c) { return types.get(c); }

    /** The rows whose {@code keyColumn} equals {@code value}, in file order. */
    public List<String[]> lookup(String keyColumn, String value) {
        return index(keyColumn).getOrDefault(value, List.of());
    }

    /** Whether some value of {@code keyColumn} appears on more than one row. */
    public boolean hasDuplicateKeys(String keyColumn) {
        return index(keyColumn).values().stream().anyMatch(l -> l.size() > 1);
    }

    /**
     * The JSON value of {@code column} in {@code row}, or {@code null} for an empty cell.
     *
     * @param keyColumn whether the column is used as lookup key by some enrichment: an inferred
     *                  type is then ignored and the value is a string (a declared type is kept),
     *                  so that an identifier never turns into a number
     */
    public JsonNode value(String column, String[] row, boolean keyColumn) {
        String raw = row[columnIndex.get(column)];
        if (raw.isEmpty()) return null;
        ColumnType type = declared.containsKey(column) ? declared.get(column)
            : keyColumn ? ColumnType.STRING : types.get(column);
        return switch (type) {
            case LONG    -> LongNode.valueOf(Long.parseLong(raw));
            case DOUBLE  -> DoubleNode.valueOf(Double.parseDouble(raw));
            case BOOLEAN -> BooleanNode.valueOf(Boolean.parseBoolean(raw));
            case STRING  -> TextNode.valueOf(raw);
        };
    }

    // ── Internals ─────────────────────────────────────────────────────────────

    private Map<String, List<String[]>> index(String keyColumn) {
        Integer k = columnIndex.get(keyColumn);
        if (k == null) {
            throw new IllegalArgumentException("Reference '" + name + "' has no column '" + keyColumn
                + "' (columns: " + columns + ")");
        }
        return indexes.computeIfAbsent(keyColumn, c -> {
            Map<String, List<String[]>> m = new LinkedHashMap<>();
            for (String[] r : rows) {
                if (!r[k].isEmpty()) m.computeIfAbsent(r[k], x -> new ArrayList<>()).add(r);
            }
            return m;
        });
    }

    private ColumnType infer(int c) {
        boolean any = false, allLong = true, allNumber = true, allBoolean = true;
        for (String[] r : rows) {
            String v = r[c];
            if (v.isEmpty()) continue;
            any = true;
            boolean isLong = LONG_RE.matcher(v).matches();
            allLong &= isLong;
            allNumber &= isLong || DOUBLE_RE.matcher(v).matches();
            allBoolean &= "true".equalsIgnoreCase(v) || "false".equalsIgnoreCase(v);
        }
        if (!any) return ColumnType.STRING;
        if (allLong) return ColumnType.LONG;
        if (allNumber) return ColumnType.DOUBLE;
        if (allBoolean) return ColumnType.BOOLEAN;
        return ColumnType.STRING;
    }

    private void validateDeclared() {
        for (Map.Entry<String, ColumnType> e : declared.entrySet()) {
            int c = columnIndex.get(e.getKey());
            for (String[] r : rows) {
                String v = r[c];
                if (v.isEmpty()) continue;
                try {
                    switch (e.getValue()) {
                        case LONG    -> Long.parseLong(v);
                        case DOUBLE  -> Double.parseDouble(v);
                        case BOOLEAN -> {
                            if (!"true".equalsIgnoreCase(v) && !"false".equalsIgnoreCase(v)) {
                                throw new NumberFormatException(v);
                            }
                        }
                        case STRING  -> { }
                    }
                } catch (NumberFormatException ex) {
                    throw new IllegalArgumentException("Reference '" + name + "': the value '" + v
                        + "' of column '" + e.getKey() + "' is not a valid " + e.getValue().name().toLowerCase(Locale.ROOT));
                }
            }
        }
    }

    private static ColumnType parseType(String reference, String column, String type) {
        String t = type == null ? "" : type.trim().toLowerCase(Locale.ROOT);
        return switch (t) {
            case "string"                        -> ColumnType.STRING;
            case "long", "int", "integer"        -> ColumnType.LONG;
            case "double", "float", "number"     -> ColumnType.DOUBLE;
            case "boolean"                       -> ColumnType.BOOLEAN;
            default -> throw new IllegalArgumentException("Reference '" + reference + "': unknown type '"
                + type + "' for column '" + column + "' (string, long, double or boolean)");
        };
    }
}
