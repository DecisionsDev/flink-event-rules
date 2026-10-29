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
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.types.DataType;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Generic, use-case agnostic converter from standard JSON Schema (draft-07 / draft-2020-12)
 * to Apache Flink Table {@link Schema} and {@link DataType}.
 */
public class JsonSchemaToFlinkConverter {

    /** Name of the synthesized column containing the unparsed raw JSON string. */
    public static final String RAW_PAYLOAD_COLUMN = "raw_payload";

    /**
     * Converts a root JSON Schema {@link JsonNode} into a Flink {@link Schema} for
     * registering input streams as temporary views. Automatically adds a {@code raw_payload STRING}
     * column at the end for pass-through support.
     *
     * @param rootSchema parsed JSON Schema
     * @return Flink Table Schema
     */
    public static Schema convertToTableSchema(JsonNode rootSchema) {
        Schema.Builder builder = Schema.newBuilder();
        JsonNode properties = rootSchema.path("properties");
        if (properties.isObject()) {
            Iterator<String> fieldNames = properties.fieldNames();
            while (fieldNames.hasNext()) {
                String name = fieldNames.next();
                builder.column(name, convertProperty(properties.get(name)));
            }
        }
        // Always append the raw payload column for passthrough capability in SQL
        builder.column(RAW_PAYLOAD_COLUMN, DataTypes.STRING());
        // Append processing time attribute for temporal lookup joins
        builder.columnByExpression("proc_time", "PROCTIME()");
        return builder.build();
    }

    /**
     * Builds Flink {@link RowTypeInfo} from the JSON Schema properties plus {@code raw_payload},
     * ensuring Flink's Table API knows the physical field names and types without falling back to GenericType.
     *
     * @param rootSchema parsed JSON Schema
     * @return Flink RowTypeInfo
     */
    public static RowTypeInfo convertToRowTypeInfo(JsonNode rootSchema) {
        List<String> fieldNames = new ArrayList<>();
        List<TypeInformation<?>> types = new ArrayList<>();

        JsonNode properties = rootSchema.path("properties");
        if (properties.isObject()) {
            Iterator<String> names = properties.fieldNames();
            while (names.hasNext()) {
                String name = names.next();
                fieldNames.add(name);
                types.add(convertPropertyToTypeInfo(properties.get(name)));
            }
        }
        fieldNames.add(RAW_PAYLOAD_COLUMN);
        types.add(BasicTypeInfo.STRING_TYPE_INFO);

        return new RowTypeInfo(
            types.toArray(new TypeInformation<?>[0]),
            fieldNames.toArray(new String[0])
        );
    }

    private static TypeInformation<?> convertPropertyToTypeInfo(JsonNode node) {
        if (node == null || node.isMissingNode() || node.isNull()) {
            return BasicTypeInfo.STRING_TYPE_INFO;
        }

        String type = node.path("type").asText("");
        if (type.isBlank() && node.has("properties")) {
            type = "object";
        }

        return switch (type.toLowerCase()) {
            case "string" -> BasicTypeInfo.STRING_TYPE_INFO;
            case "integer" -> BasicTypeInfo.LONG_TYPE_INFO;
            case "number" -> BasicTypeInfo.DOUBLE_TYPE_INFO;
            case "boolean" -> BasicTypeInfo.BOOLEAN_TYPE_INFO;
            case "object" -> {
                JsonNode props = node.path("properties");
                if (props.isObject() && props.size() > 0) {
                    List<String> childNames = new ArrayList<>();
                    List<TypeInformation<?>> childTypes = new ArrayList<>();
                    Iterator<String> it = props.fieldNames();
                    while (it.hasNext()) {
                        String childName = it.next();
                        childNames.add(childName);
                        childTypes.add(convertPropertyToTypeInfo(props.get(childName)));
                    }
                    yield new RowTypeInfo(
                        childTypes.toArray(new TypeInformation<?>[0]),
                        childNames.toArray(new String[0])
                    );
                }
                yield BasicTypeInfo.STRING_TYPE_INFO;
            }
            default -> BasicTypeInfo.STRING_TYPE_INFO;
        };
    }

    /**
     * Converts a JSON Schema property node into a Flink {@link DataType}.
     *
     * @param node JSON Schema property definition
     * @return corresponding Flink DataType
     */
    public static DataType convertProperty(JsonNode node) {
        if (node == null || node.isMissingNode() || node.isNull()) {
            return DataTypes.STRING();
        }

        String type = node.path("type").asText("");

        // If type is not explicitly set, check if properties exist -> object
        if (type.isBlank() && node.has("properties")) {
            type = "object";
        }

        return switch (type.toLowerCase()) {
            case "string" -> DataTypes.STRING();
            case "integer" -> DataTypes.BIGINT();
            case "number" -> DataTypes.DOUBLE();
            case "boolean" -> DataTypes.BOOLEAN();
            case "array" -> {
                JsonNode items = node.path("items");
                yield DataTypes.ARRAY(convertProperty(items));
            }
            case "object" -> {
                JsonNode props = node.path("properties");
                if (props.isObject() && props.size() > 0) {
                    List<DataTypes.Field> fields = new ArrayList<>();
                    Iterator<String> names = props.fieldNames();
                    while (names.hasNext()) {
                        String childName = names.next();
                        fields.add(DataTypes.FIELD(childName, convertProperty(props.get(childName))));
                    }
                    yield DataTypes.ROW(fields.toArray(new DataTypes.Field[0]));
                }
                yield DataTypes.STRING();
            }
            default -> DataTypes.STRING();
        };
    }
}
