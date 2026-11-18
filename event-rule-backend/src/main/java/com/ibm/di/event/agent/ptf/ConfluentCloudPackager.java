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
package com.ibm.di.event.agent.ptf;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ibm.di.event.agent.AgentDescriptor;
import com.ibm.di.event.agent.AgentDescriptorLoader;
import com.ibm.di.event.agent.AgentDescriptorValidator;
import com.ibm.di.event.agent.EnvUtils;
import com.ibm.di.event.agent.ReferenceConfig;
import com.ibm.di.event.agent.ptf.AgentSqlPipelineBuilder.Dialect;
import com.ibm.di.event.agent.ptf.AgentSqlPipelineBuilder.Options;
import com.ibm.di.event.agent.ptf.AgentSqlPipelineBuilder.Pipeline;
import com.ibm.di.event.agent.ptf.AgentSqlPipelineBuilder.Statement;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Build-time step of the {@code ccloud} Maven profile: prepares what a use case needs to run as a
 * Process Table Function on Confluent Cloud for Apache Flink.
 *
 * <p>Confluent Cloud has no volume to mount and no {@code main()} to run, so a use case is
 * deployed as two things, both produced from its {@code agents.json}:
 * <ol>
 *   <li>a function JAR: the shaded backend <em>plus</em> the files the agents read at runtime
 *       (the {@code agents.json} itself, the ruleset archives and the reference-data CSV files,
 *       all at the path they have in the repository, for example {@code samples/<use-case>/}),
 *       which this class copies into the compiled classes directory before the JAR is built; the
 *       SQL names the file and the agent ({@code agents_file}, {@code agent_id}) instead of
 *       inlining the descriptor;</li>
 *   <li>the SQL program that registers the function and starts the pipeline, written under
 *       {@code <outDir>/sql} together with a {@code manifest.json} that lists the statements in
 *       execution order, so that a deployment script needs no SQL parsing.</li>
 * </ol>
 *
 * <p>The SQL is the {@link Dialect#CONFLUENT} output of {@link AgentSqlPipelineBuilder}. The
 * {@code CREATE FUNCTION} statements keep the {@value AgentSqlPipelineBuilder#ARTIFACT_URI_PLACEHOLDER}
 * placeholder unless an artifact URI is given: the artifact id exists only after the upload.
 *
 * <p>Inputs are system properties (an empty argument is dropped by some launchers):
 * <ul>
 *   <li>{@code ccloud.descriptor}: the {@code agents.json} to package (required);</li>
 *   <li>{@code ccloud.baseDir}: directory the paths of the descriptor are relative to (the
 *       repository root);</li>
 *   <li>{@code ccloud.classesDir}: where the runtime files are copied to (the module's compiled
 *       classes);</li>
 *   <li>{@code ccloud.outDir}: where {@code sql/} and {@code manifest.json} are written;</li>
 *   <li>{@code ccloud.jar}: file name of the function JAR, recorded in the manifest;</li>
 *   <li>{@code ccloud.artifactUri}: optional, replaces the artifact placeholder.</li>
 * </ul>
 * The {@code AGENT_ERROR_TOPIC*} and {@code CHAINING_ENABLED} environment variables apply as they
 * do for {@link AgentBasedPTFJob}.
 */
public final class ConfluentCloudPackager {

    /** Result of {@link #run}. */
    public record Result(Path sqlDir, Path manifest, List<String> bundled, Pipeline pipeline) {}

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Pattern OBJECT_NAME = Pattern.compile("`((?:[^`]|``)+)`");

    private ConfluentCloudPackager() {}

    public static void main(String[] args) throws Exception {
        String descriptor = System.getProperty("ccloud.descriptor", "");
        if (descriptor.isBlank()) {
            throw new IllegalArgumentException("Set -Dccloud.descriptor=<path of the agents.json to "
                + "package>, for example -Dccloud.descriptor=samples/banking-fraud-agent/agents.json");
        }
        Result r = run(Path.of(descriptor),
            Path.of(System.getProperty("ccloud.baseDir", ".")),
            Path.of(System.getProperty("ccloud.classesDir", "target/classes")),
            Path.of(System.getProperty("ccloud.outDir", "target/ccloud")),
            System.getProperty("ccloud.jar", "event-rule-ccloud.jar"),
            System.getProperty("ccloud.artifactUri", AgentSqlPipelineBuilder.ARTIFACT_URI_PLACEHOLDER));
        System.out.println("[ccloud] bundled " + r.bundled().size() + " runtime file(s) into the function JAR");
        System.out.println("[ccloud] SQL program: " + r.sqlDir().toAbsolutePath());
        System.out.println("[ccloud] manifest   : " + r.manifest().toAbsolutePath());
    }

    /**
     * @param descriptor  the {@code agents.json}; relative paths are resolved against {@code baseDir}
     * @param baseDir     directory the paths inside the descriptor are relative to
     * @param classesDir  directory the runtime files are copied to, keeping the path they have in
     *                    the descriptor (which is where the runtime looks them up on the classpath)
     * @param outDir      receives {@code sql/} and {@code manifest.json}
     * @param jarName     file name of the function JAR, recorded in the manifest
     * @param artifactUri URI written in the {@code USING JAR} clauses
     * @throws IllegalArgumentException when the descriptor is invalid or a runtime file is missing
     */
    public static Result run(Path descriptor, Path baseDir, Path classesDir, Path outDir,
                             String jarName, String artifactUri) throws IOException {
        Path descriptorFile = descriptor.isAbsolute() || Files.isRegularFile(descriptor)
            ? descriptor : baseDir.resolve(descriptor);
        List<AgentDescriptor> agents = AgentDescriptorLoader
            .loadFileDescriptor(descriptorFile.toString()).agents();
        if (agents.isEmpty()) {
            throw new IllegalArgumentException("No agent descriptors found in: " + descriptorFile);
        }
        AgentDescriptorValidator.validateUniqueAgentIds(agents);
        AgentDescriptorValidator.validateUniqueOperatorUids(agents);
        boolean chaining = EnvUtils.boolEnvOr("CHAINING_ENABLED", true);
        for (AgentDescriptor agent : agents) {
            AgentDescriptorValidator.validate(agent);
            if (!chaining) agent.setChainingEnabled(false);
        }

        List<String> bundled = bundleRuntimeFiles(agents, baseDir, classesDir);
        String agentsResource = bundleDescriptor(descriptorFile, baseDir, classesDir);
        bundled.add(0, agentsResource);

        Options options = new Options();
        options.dialect = Dialect.CONFLUENT;
        options.bundledAgentsFile = agentsResource;
        options.chainingEnabled = chaining;
        options.env = System.getenv();
        options.artifactUri = artifactUri == null || artifactUri.isBlank()
            ? AgentSqlPipelineBuilder.ARTIFACT_URI_PLACEHOLDER : artifactUri;
        Pipeline pipeline = new AgentSqlPipelineBuilder(options).build(agents);

        Path sqlDir = outDir.resolve("sql");
        Path manifest = writeSql(pipeline, descriptor, jarName, options.artifactUri, bundled, outDir, sqlDir);
        return new Result(sqlDir, manifest, bundled, pipeline);
    }

    // ── Runtime files ─────────────────────────────────────────────────────────

    /**
     * Copies the ruleset archives and the CSV reference files into {@code classesDir}, at the
     * path the descriptor gives (without a leading slash). That is a path both
     * {@code RulesetResourceResolver} and {@code ReferenceRegistry} look up on the classpath when
     * the file does not exist on the filesystem.
     */
    private static List<String> bundleRuntimeFiles(List<AgentDescriptor> agents, Path baseDir,
                                                   Path classesDir) throws IOException {
        Set<String> paths = new LinkedHashSet<>();
        for (AgentDescriptor agent : agents) {
            if (agent.getRuleset() != null) paths.add(agent.getRuleset().getPath());
            if (agent.getReferences() != null) {
                for (ReferenceConfig ref : agent.getReferences()) paths.add(ref.getFile());
            }
        }
        List<String> bundled = new ArrayList<>();
        for (String declared : paths) {
            if (declared == null || declared.isBlank() || declared.contains("://")) continue;
            String resource = declared.replace('\\', '/').replaceFirst("^/+", "");
            if (Path.of(resource).normalize().startsWith("..")) {
                throw new IllegalArgumentException("Cannot bundle '" + declared + "': it points outside "
                    + "the base directory, use a path below it");
            }
            Path source = Path.of(declared);
            if (!source.isAbsolute()) source = baseDir.resolve(declared);
            if (!Files.isRegularFile(source)) {
                throw new IllegalArgumentException("File '" + declared + "' of the descriptor not found: "
                    + source.toAbsolutePath().normalize() + " (paths are relative to " + baseDir.toAbsolutePath().normalize()
                    + "; set -Dccloud.baseDir)");
            }
            Path target = classesDir.resolve(resource);
            Files.createDirectories(target.getParent());
            Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
            bundled.add(resource);
        }
        return bundled;
    }

    /**
     * Copies the {@code agents.json} into {@code classesDir}, at its path relative to
     * {@code baseDir} (for example {@code samples/<use-case>/agents.json}), where the function
     * finds it through its {@code agents_file} argument.
     *
     * @return that path, with forward slashes
     */
    private static String bundleDescriptor(Path descriptorFile, Path baseDir, Path classesDir) throws IOException {
        Path relative = baseDir.toAbsolutePath().normalize()
            .relativize(descriptorFile.toAbsolutePath().normalize());
        if (relative.startsWith("..")) {
            throw new IllegalArgumentException("Cannot bundle the descriptor " + descriptorFile
                + ": it is outside the base directory " + baseDir.toAbsolutePath().normalize()
                + " (set -Dccloud.baseDir)");
        }
        String resource = relative.toString().replace('\\', '/');
        Path target = classesDir.resolve(resource);
        Files.createDirectories(target.getParent());
        Files.copy(descriptorFile, target, StandardCopyOption.REPLACE_EXISTING);
        return resource;
    }

    // ── SQL program ───────────────────────────────────────────────────────────

    private static Path writeSql(Pipeline pipeline, Path descriptor, String jarName, String artifactUri,
                                 List<String> bundled, Path outDir, Path sqlDir) throws IOException {
        // A previous run may have produced more statements: do not leave stale files behind.
        if (Files.isDirectory(sqlDir)) {
            try (var files = Files.list(sqlDir)) {
                for (Path f : (Iterable<Path>) files::iterator) Files.delete(f);
            }
        }
        Files.createDirectories(sqlDir);

        ObjectNode root = MAPPER.createObjectNode();
        root.put("descriptor", descriptor.toString().replace('\\', '/'));
        ObjectNode artifact = root.putObject("artifact");
        artifact.put("file", jarName);
        artifact.put("uri", artifactUri);
        artifact.put("uriPlaceholder", AgentSqlPipelineBuilder.ARTIFACT_URI_PLACEHOLDER);
        ArrayNode bundledNode = root.putArray("bundled");
        bundled.forEach(bundledNode::add);

        ArrayNode statements = root.putArray("statements");
        int order = 0;
        for (Statement s : pipeline.statements) {
            if (s.kind().equals("insert")) continue;
            order++;
            String name = objectName(s.sql());
            String file = String.format("%03d-%s-%s.sql", order, s.kind(), fileSafe(name));
            Files.writeString(sqlDir.resolve(file), s.sql() + ";\n", StandardCharsets.UTF_8);
            addStatement(statements, order, s.kind(), name, s.agentId(), "sql/" + file);
        }
        String statementSet = pipeline.toStatementSet();
        if (!statementSet.isEmpty()) {
            order++;
            String file = String.format("%03d-statement-set.sql", order);
            Files.writeString(sqlDir.resolve(file), statementSet, StandardCharsets.UTF_8);
            addStatement(statements, order, "statement-set", "statement-set", null, "sql/" + file);
        }

        Files.writeString(outDir.resolve("pipeline.sql"), pipeline.toScript(), StandardCharsets.UTF_8);
        root.put("script", "pipeline.sql");
        Path manifest = outDir.resolve("manifest.json");
        Files.writeString(manifest, MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(root) + "\n",
            StandardCharsets.UTF_8);
        return manifest;
    }

    private static void addStatement(ArrayNode statements, int order, String kind, String name,
                                     String agentId, String file) {
        ObjectNode n = statements.addObject();
        n.put("order", order);
        n.put("kind", kind);
        n.put("name", name);
        n.put("agentId", agentId);
        n.put("file", file);
    }

    /** The first back-quoted identifier of a CREATE statement: the object it creates. */
    static String objectName(String sql) {
        Matcher m = OBJECT_NAME.matcher(Pipeline.stripLeadingComments(sql));
        return m.find() ? m.group(1).replace("``", "`") : "statement";
    }

    private static String fileSafe(String name) {
        return name.replaceAll("[^A-Za-z0-9_.-]", "_");
    }
}
