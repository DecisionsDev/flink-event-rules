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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serial;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A named reference data set an {@link EnrichmentConfig} can look rows up in.
 *
 * <p>Only {@code type = "csv"} exists. The first line of the file holds the column names. The
 * {@code file} is resolved from the classpath first (this is how the data reaches Confluent
 * Cloud, which has no filesystem), then from the filesystem like the agent descriptor itself.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ReferenceConfig implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    public static final String TYPE_CSV = "csv";

    /** Name the enrichment steps refer to. Unique within the agent. */
    private String name;

    /** Format of the file; {@value #TYPE_CSV} when absent. */
    private String type = TYPE_CSV;

    /** Path of the data file. */
    private String file;

    /**
     * Optional column types: column name to {@code string}, {@code long}, {@code double} or
     * {@code boolean}. A column that is not listed is typed from its values: a number when every
     * value is a canonical number, a boolean when every value is {@code true}/{@code false},
     * otherwise a string. A key column is never typed from its values.
     */
    private Map<String, String> types = new LinkedHashMap<>();

    public ReferenceConfig() {}

    public ReferenceConfig(String name, String type, String file) {
        this.name = name;
        this.type = type;
        this.file = file;
    }

    public String getName()                      { return name; }
    public void   setName(String n)              { this.name = n; }

    public String getType()                      { return type == null || type.isBlank() ? TYPE_CSV : type; }
    public void   setType(String t)              { this.type = t; }

    public String getFile()                      { return file; }
    public void   setFile(String f)              { this.file = f; }

    public Map<String, String> getTypes()        { return types; }
    public void setTypes(Map<String, String> t)  { this.types = t != null ? t : new LinkedHashMap<>(); }

    @Override
    public String toString() {
        return "ReferenceConfig{name='" + name + "', type='" + getType() + "', file='" + file + "'}";
    }
}
