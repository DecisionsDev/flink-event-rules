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
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;

import java.io.Serial;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Generic Flink {@link MapFunction} that parses raw JSON strings from Kafka into
 * Flink {@link Row} objects conforming to a declared JSON Schema, plus an appended
 * {@code raw_payload} field.
 */
public class JsonToRowMapper implements MapFunction<String, Row> {

    @Serial
    private static final long serialVersionUID = 1L;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String schemaJsonString;
    private transient List<FieldSpec> fieldSpecs;

    public record FieldSpec(String name, String type, List<FieldSpec> nestedFields, FieldSpec itemSpec) implements Serializable {}

    public JsonToRowMapper(String schemaJsonString) {
        this.schemaJsonString = schemaJsonString;
    }

    private List<FieldSpec> getFieldSpecs() {
        if (fieldSpecs == null) {
            try {
                JsonNode root = MAPPER.readTree(schemaJsonString);
                fieldSpecs = parseProperties(root.path("properties"));
            } catch (Exception e) {
                fieldSpecs = new ArrayList<>();
            }
        }
        return fieldSpecs;
    }

    private static List<FieldSpec> parseProperties(JsonNode properties) {
        List<FieldSpec> list = new ArrayList<>();
        if (properties.isObject()) {
            Iterator<String> names = properties.fieldNames();
            while (names.hasNext()) {
                String name = names.next();
                JsonNode prop = properties.get(name);
                String type = prop.path("type").asText("string");
                if (type.isBlank() && prop.has("properties")) {
                    type = "object";
                }
                List<FieldSpec> nested = "object".equalsIgnoreCase(type) ? parseProperties(prop.path("properties")) : null;
                FieldSpec itemSpec = "array".equalsIgnoreCase(type) ? parsePropertyItem(prop.path("items")) : null;
                list.add(new FieldSpec(name, type.toLowerCase(), nested, itemSpec));
            }
        }
        return list;
    }

    private static FieldSpec parsePropertyItem(JsonNode itemNode) {
        if (itemNode == null || itemNode.isMissingNode() || itemNode.isNull()) {
            return new FieldSpec("item", "string", null, null);
        }
        String type = itemNode.path("type").asText("string");
        if (type.isBlank() && itemNode.has("properties")) {
            type = "object";
        }
        List<FieldSpec> nested = "object".equalsIgnoreCase(type) ? parseProperties(itemNode.path("properties")) : null;
        return new FieldSpec("item", type.toLowerCase(), nested, null);
    }

    @Override
    public Row map(String jsonString) throws Exception {
        JsonNode root = MAPPER.readTree(jsonString);
        List<FieldSpec> specs = getFieldSpecs();

        // Row has fields for all schema properties + 1 for raw_payload
        Row row = new Row(specs.size() + 1);
        for (int i = 0; i < specs.size(); i++) {
            FieldSpec spec = specs.get(i);
            JsonNode child = root.get(spec.name());
            row.setField(i, convertValue(child, spec));
        }
        // Append raw json string
        row.setField(specs.size(), jsonString);
        return row;
    }

    private Object convertValue(JsonNode node, FieldSpec spec) {
        if (node == null || node.isNull() || node.isMissingNode()) {
            return null;
        }

        return switch (spec.type()) {
            case "string" -> node.isTextual() ? node.asText() : node.toString();
            case "integer" -> node.asLong();
            case "number" -> node.asDouble();
            case "boolean" -> node.asBoolean();
            case "array" -> {
                if (!node.isArray()) yield null;
                List<Object> items = new ArrayList<>();
                for (JsonNode elem : node) {
                    items.add(spec.itemSpec() != null ? convertValue(elem, spec.itemSpec()) : elem.asText());
                }
                yield items.toArray();
            }
            case "object" -> {
                if (spec.nestedFields() != null && !spec.nestedFields().isEmpty()) {
                    Row nestedRow = new Row(spec.nestedFields().size());
                    for (int i = 0; i < spec.nestedFields().size(); i++) {
                        FieldSpec childSpec = spec.nestedFields().get(i);
                        nestedRow.setField(i, convertValue(node.get(childSpec.name()), childSpec));
                    }
                    yield nestedRow;
                }
                yield node.toString();
            }
            default -> node.asText();
        };
    }
}
