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

import java.util.ArrayList;
import java.util.List;

/**
 * Minimal RFC 4180 CSV reader: comma separated, {@code "} quoting with {@code ""} as escaped
 * quote, quoted values may contain commas and line breaks, {@code \n}, {@code \r\n} or {@code \r}
 * line ends, blank lines skipped, a leading byte order mark ignored.
 *
 * <p>It exists so that the reference-data function does not depend on {@code flink-csv}, which a
 * Confluent Cloud artifact must not bundle.
 */
final class CsvReader {

    private final String text;
    private final List<String[]> rows = new ArrayList<>();
    private final List<String> row = new ArrayList<>();
    private final StringBuilder cell = new StringBuilder();
    private boolean rowStarted;

    private CsvReader(String text) {
        this.text = text.startsWith("\uFEFF") ? text.substring(1) : text;
    }

    static List<String[]> parse(String text) {
        return new CsvReader(text).read();
    }

    private List<String[]> read() {
        boolean quoted = false;
        int n = text.length();
        for (int i = 0; i < n; i++) {
            char c = text.charAt(i);
            if (quoted) {
                if (c == '"') {
                    if (i + 1 < n && text.charAt(i + 1) == '"') {
                        cell.append('"');
                        i++;
                    } else {
                        quoted = false;
                    }
                } else {
                    cell.append(c);
                }
            } else if (c == '"' && cell.length() == 0) {
                quoted = true;
                rowStarted = true;
            } else if (c == ',') {
                endCell();
            } else if (c == '\n' || c == '\r') {
                if (c == '\r' && i + 1 < n && text.charAt(i + 1) == '\n') i++;
                endRow();
            } else {
                cell.append(c);
                rowStarted = true;
            }
        }
        endRow();
        return rows;
    }

    private void endCell() {
        row.add(cell.toString());
        cell.setLength(0);
        rowStarted = true;
    }

    private void endRow() {
        if (rowStarted || cell.length() > 0 || !row.isEmpty()) {
            endCell();
            rows.add(row.toArray(new String[0]));
        }
        row.clear();
        rowStarted = false;
    }
}
