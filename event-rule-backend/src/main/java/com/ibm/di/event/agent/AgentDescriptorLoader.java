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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Loads the agents descriptor file ({@code agents.json}). Shared by the DataStream and PTF jobs.
 */
public final class AgentDescriptorLoader {

    /** Source idleness used when a topic in {@code agents.json} does not set {@code idleness}. */
    public static final long DEFAULT_IDLENESS_MS = 60_000L;

    private AgentDescriptorLoader() {}

    /**
     * Container for the agents descriptor file contents.
     *
     * @param parallelism job-level parallelism declared in the file ({@code 0} = not set)
     * @param agents      list of agent descriptors
     */
    public record AgentFileDescriptor(int parallelism, List<AgentDescriptor> agents) {}

    /**
     * Reads the per-topic source idleness from the top-level {@code topics[]} section of the
     * agents file.
     *
     * <p>Each entry of {@code topics[]} may carry an {@code idleness} (same format as a horizon
     * {@code timeLimit}: {@code 30s}, {@code 1mn}, {@code 2h}…).  Topics without it use
     * {@link #DEFAULT_IDLENESS_MS}.  A file without a {@code topics} section (or a plain JSON
     * array of agents) yields an empty map.
     *
     * @return topic name → idleness in milliseconds
     * @throws IllegalArgumentException if an {@code idleness} value is invalid or not positive
     */
    public static Map<String, Long> loadTopicIdleness(String agentDescriptorPath) throws IOException {
        Map<String, Long> result = new HashMap<>();

        JsonNode topics = new ObjectMapper().readTree(readFile(agentDescriptorPath)).get("topics");
        if (topics == null || !topics.isArray()) return result;
        for (JsonNode topic : topics) {
            JsonNode name = topic.get("name");
            JsonNode idleness = topic.get("idleness");
            if (name == null || !name.isTextual() || idleness == null || idleness.isNull()) continue;
            long ms = HorizonConfig.time(idleness.asText()).timeLimitMs();
            if (ms <= 0) {
                throw new IllegalArgumentException("agents.json: topic '" + name.asText()
                    + "' has a non-positive idleness '" + idleness.asText() + "'");
            }
            result.put(name.asText(), ms);
        }
        return result;
    }

    /**
     * Reads declared JSON Schema paths from the top-level {@code topics[]} section of the agents file.
     *
     * @return topic name → schema file path
     */
    public static Map<String, String> loadTopicSchemas(String agentDescriptorPath) throws IOException {
        Map<String, String> result = new HashMap<>();

        JsonNode topics = new ObjectMapper().readTree(readFile(agentDescriptorPath)).get("topics");
        if (topics == null || !topics.isArray()) return result;
        for (JsonNode topic : topics) {
            JsonNode name = topic.get("name");
            JsonNode schema = topic.get("schema");
            if (name != null && name.isTextual() && schema != null && schema.isTextual() && !schema.asText().isBlank()) {
                result.put(name.asText(), schema.asText());
            }
        }
        return result;
    }

    /**
     * Loads the agent descriptor file and returns an {@link AgentFileDescriptor}.
     * Accepts:
     * <ul>
     *   <li>A JSON array: {@code [ { … }, { … } ]}</li>
     *   <li>A wrapper object with an optional top-level {@code "parallelism"} field and an
     *       {@code "agents"} array: {@code { "parallelism": 4, "agents": [ … ] }}</li>
     * </ul>
     */
    public static AgentFileDescriptor loadFileDescriptor(String descriptorPath) throws IOException {
        String json = readFile(descriptorPath);
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(json);

        int jobParallelism = 0;
        JsonNode arrayNode;
        if (root.isArray()) {
            arrayNode = root;
        } else if (root.has("agents") && root.get("agents").isArray()) {
            arrayNode = root.get("agents");
            // Read optional top-level parallelism field
            if (root.has("parallelism") && root.get("parallelism").isInt()) {
                jobParallelism = root.get("parallelism").asInt(0);
            }
        } else {
            throw new IllegalArgumentException(
                "Agent descriptor file must be a JSON array or an object with an 'agents' array: "
                    + descriptorPath);
        }

        List<AgentDescriptor> agents = new ArrayList<>();
        for (JsonNode node : arrayNode) {
            AgentDescriptor agent = mapper.treeToValue(node, AgentDescriptor.class);
            agents.add(agent);
        }
        return new AgentFileDescriptor(jobParallelism, agents);
    }

    /**
     * Convenience method that returns only the agent list.
     * Kept for callers that do not need the job-level parallelism.
     */
    public static List<AgentDescriptor> loadAgentDescriptors(String descriptorPath) throws IOException {
        return loadFileDescriptor(descriptorPath).agents();
    }

    private static String readFile(String path) throws IOException {
        // Try classpath first
        try (InputStream in = AgentDescriptorLoader.class.getClassLoader()
                .getResourceAsStream(path)) {
            if (in != null) {
                return new String(in.readAllBytes(), StandardCharsets.UTF_8);
            }
        }
        // Then filesystem
        File f = new File(path);
        if (!f.exists()) f = new File(System.getProperty("user.dir"), path);
        if (!f.exists()) f = new File("..", path);
        if (!f.exists() && System.getProperty("user.dir") != null) {
            File userDir = new File(System.getProperty("user.dir"));
            if (userDir.getParentFile() != null) {
                f = new File(userDir.getParentFile(), path);
            }
        }
        if (!f.exists()) {
            throw new IllegalArgumentException("Agent descriptor not found: " + path
                + " (working dir: " + System.getProperty("user.dir") + ")");
        }
        try (InputStream in = new FileInputStream(f)) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
