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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.api.Schema;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class JsonSchemaToFlinkConverterTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    @DisplayName("Converts basic and nested properties to Flink Table Schema")
    void testBasicConversion() throws Exception {
        String schemaJson = """
            {
              "$schema": "http://json-schema.org/draft-07/schema#",
              "type": "object",
              "properties": {
                "timestamp": { "type": "string" },
                "flightNumber": { "type": "string" },
                "count": { "type": "integer" },
                "flight": {
                  "type": "object",
                  "properties": {
                    "aircraftType": { "type": "string" },
                    "duration": { "type": "string" }
                  }
                }
              }
            }
            """;

        JsonNode root = MAPPER.readTree(schemaJson);
        Schema schema = JsonSchemaToFlinkConverter.convertToTableSchema(root);

        assertTrue(schema.getColumns().stream().anyMatch(c -> c.getName().equals("timestamp")));
        assertTrue(schema.getColumns().stream().anyMatch(c -> c.getName().equals("flightNumber")));
        assertTrue(schema.getColumns().stream().anyMatch(c -> c.getName().equals("count")));
        assertTrue(schema.getColumns().stream().anyMatch(c -> c.getName().equals("flight")));
        assertTrue(schema.getColumns().stream().anyMatch(c -> c.getName().equals("raw_payload")));
    }
}
