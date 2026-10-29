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
package com.ibm.di.event.sql;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class JsonToRowMapperTest {

    @Test
    @DisplayName("Maps flat and nested JSON string to Flink Row")
    void testJsonToRowMapping() throws Exception {
        String schemaJson = """
            {
              "type": "object",
              "properties": {
                "timestamp": { "type": "string" },
                "flightNumber": { "type": "string" },
                "flight": {
                  "type": "object",
                  "properties": {
                    "aircraftType": { "type": "string" },
                    "origin": { "type": "string" }
                  }
                }
              }
            }
            """;

        String eventJson = """
            {
              "timestamp": "2026-10-01T20:29:16Z",
              "flightNumber": "EY082",
              "flight": {
                "aircraftType": "A380",
                "origin": "CDG"
              }
            }
            """;

        JsonToRowMapper mapper = new JsonToRowMapper(schemaJson);
        Row row = mapper.map(eventJson);

        assertNotNull(row);
        assertEquals("2026-10-01T20:29:16Z", row.getField(0));
        assertEquals("EY082", row.getField(1));

        Object nestedObj = row.getField(2);
        assertInstanceOf(Row.class, nestedObj);
        Row nestedRow = (Row) nestedObj;
        assertEquals("A380", nestedRow.getField(0));
        assertEquals("CDG", nestedRow.getField(1));

        // raw_payload is last field
        assertEquals(eventJson, row.getField(3));
    }
}
