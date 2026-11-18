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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("ConfluentCloudPackager")
class ConfluentCloudPackagerTest {

    /** The repository root: the tests run from event-rule-backend. */
    private static final Path ROOT = Path.of("..");
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @TempDir
    Path tmp;

    private ConfluentCloudPackager.Result run(String useCase, String artifactUri) throws Exception {
        return ConfluentCloudPackager.run(Path.of("samples/" + useCase + "/agents.json"), ROOT,
            tmp.resolve("classes"), tmp.resolve("out"), useCase + ".jar", artifactUri);
    }

    private List<String> sqlFiles(ConfluentCloudPackager.Result r) throws Exception {
        try (Stream<Path> s = Files.list(r.sqlDir())) {
            return s.map(p -> p.getFileName().toString()).sorted().toList();
        }
    }

    @Test
    @DisplayName("bundles the ruleset archive at the path of the descriptor, where the runtime looks it up")
    void bundlesRuleset() throws Exception {
        ConfluentCloudPackager.Result r = run("banking-fraud-agent", null);

        String path = "samples/banking-fraud-agent/Fraud_Detection_Service_1.6.0.jar";
        assertEquals(List.of("samples/banking-fraud-agent/agents.json", path), r.bundled());
        assertArrayEquals(Files.readAllBytes(ROOT.resolve(path)),
            Files.readAllBytes(tmp.resolve("classes").resolve(path)));
    }

    @Test
    @DisplayName("bundles the agents.json in samples/<use-case>, and the SQL names it instead of inlining it")
    void bundlesDescriptor() throws Exception {
        ConfluentCloudPackager.Result r = run("banking-fraud-agent", null);

        String path = "samples/banking-fraud-agent/agents.json";
        assertArrayEquals(Files.readAllBytes(ROOT.resolve(path)),
            Files.readAllBytes(tmp.resolve("classes").resolve(path)));
        String script = r.pipeline().toScript();
        assertTrue(script.contains("agents_file => '" + path + "'"));
        assertTrue(script.contains("agent_id => '"));
        assertFalse(script.contains("agent_config"));
    }

    @Test
    @DisplayName("bundles the reference-data CSV files, and each file once")
    void bundlesReferenceData() throws Exception {
        ConfluentCloudPackager.Result r = run("airline-agent", null);

        assertTrue(r.bundled().contains("samples/airline-agent/AirlineDecisionService_1.6.0.jar"));
        assertTrue(r.bundled().contains("samples/airline-agent/enrichment/flights.csv"));
        assertEquals(r.bundled().size(), r.bundled().stream().distinct().count());
        for (String resource : r.bundled()) {
            assertTrue(Files.isRegularFile(tmp.resolve("classes").resolve(resource)), resource);
        }
    }

    @Test
    @DisplayName("writes one file per setup statement, in order, then the statement set")
    void writesStatementsInOrder() throws Exception {
        ConfluentCloudPackager.Result r = run("banking-fraud-agent", null);

        List<String> files = sqlFiles(r);
        assertTrue(files.get(0).startsWith("001-function-"), files.toString());
        assertTrue(files.get(files.size() - 1).endsWith("-statement-set.sql"), files.toString());
        assertEquals(r.pipeline().setup.size() + 1, files.size());
        for (int i = 0; i < r.pipeline().setup.size(); i++) {
            assertEquals(r.pipeline().setup.get(i) + ";\n",
                Files.readString(r.sqlDir().resolve(files.get(i)), StandardCharsets.UTF_8));
        }
        assertEquals(r.pipeline().toStatementSet(),
            Files.readString(r.sqlDir().resolve(files.get(files.size() - 1)), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("writes the Confluent dialect: persistent views, key/val sinks, no Kafka DDL")
    void writesConfluentDialect() throws Exception {
        ConfluentCloudPackager.Result r = run("banking-fraud-agent", null);

        String script = Files.readString(r.manifest().getParent().resolve("pipeline.sql"), StandardCharsets.UTF_8);
        assertEquals(r.pipeline().toScript(), script);
        assertTrue(script.contains("CREATE FUNCTION `event_rule_agent`"));
        assertTrue(script.contains("CREATE VIEW "));
        assertFalse(script.contains("TEMPORARY"));
        assertFalse(script.contains("'connector'"));
        assertTrue(script.contains("(`key`, `val`)"));
    }

    @Test
    @DisplayName("the manifest lists the statements in execution order with their files, and the artifact")
    void writesManifest() throws Exception {
        ConfluentCloudPackager.Result r = run("banking-fraud-agent", null);

        JsonNode m = MAPPER.readTree(r.manifest().toFile());
        assertEquals("banking-fraud-agent.jar", m.at("/artifact/file").asText());
        assertEquals(AgentSqlPipelineBuilder.ARTIFACT_URI_PLACEHOLDER, m.at("/artifact/uri").asText());
        assertEquals(AgentSqlPipelineBuilder.ARTIFACT_URI_PLACEHOLDER, m.at("/artifact/uriPlaceholder").asText());
        assertEquals("pipeline.sql", m.get("script").asText());
        assertEquals(2, m.get("bundled").size());
        assertEquals("samples/banking-fraud-agent/agents.json", m.at("/bundled/0").asText());

        JsonNode statements = m.get("statements");
        List<String> files = sqlFiles(r);
        assertEquals(files.size(), statements.size());
        List<String> kinds = new ArrayList<>();
        for (int i = 0; i < statements.size(); i++) {
            assertEquals(i + 1, statements.get(i).get("order").asInt());
            assertEquals("sql/" + files.get(i), statements.get(i).get("file").asText());
            assertTrue(Files.isRegularFile(r.manifest().getParent().resolve(statements.get(i).get("file").asText())));
            kinds.add(statements.get(i).get("kind").asText());
        }
        assertEquals("function", kinds.get(0));
        assertEquals("statement-set", kinds.get(kinds.size() - 1));
        // A view is created after the functions it calls and before the statement set that reads it.
        assertTrue(kinds.indexOf("view") > kinds.lastIndexOf("function"));
        assertEquals("event_rule_agent", statements.get(0).get("name").asText());
        assertTrue(statements.get(0).get("agentId").isNull());
    }

    @Test
    @DisplayName("an artifact URI replaces the placeholder in the functions and in the manifest")
    void artifactUri() throws Exception {
        String uri = "confluent-artifact://cfa-123/ver-456";
        ConfluentCloudPackager.Result r = run("banking-fraud-agent", uri);

        String script = Files.readString(r.manifest().getParent().resolve("pipeline.sql"), StandardCharsets.UTF_8);
        assertTrue(script.contains("USING JAR '" + uri + "'"));
        assertFalse(script.contains("<artifact-id>"));
        assertEquals(uri, MAPPER.readTree(r.manifest().toFile()).at("/artifact/uri").asText());
    }

    @Test
    @DisplayName("a second run removes the statement files of the first")
    void rerunLeavesNoStaleFiles() throws Exception {
        ConfluentCloudPackager.Result first = run("airtraffic-agent", null);
        int firstCount = sqlFiles(first).size();
        Files.writeString(first.sqlDir().resolve("999-stale.sql"), "SELECT 1;");

        ConfluentCloudPackager.Result second = run("airtraffic-agent", null);
        assertEquals(firstCount, sqlFiles(second).size());
        assertFalse(sqlFiles(second).contains("999-stale.sql"));
    }

    @Test
    @DisplayName("a file of the descriptor that does not exist fails the build, naming the file")
    void missingFileFails() throws Exception {
        Path base = Files.createDirectories(tmp.resolve("base"));
        Files.copy(ROOT.resolve("samples/banking-fraud-agent/agents.json"), base.resolve("agents.json"));

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
            () -> ConfluentCloudPackager.run(Path.of("agents.json"), base, tmp.resolve("classes"),
                tmp.resolve("out"), "x.jar", null));
        assertTrue(e.getMessage().contains("Fraud_Detection_Service_1.6.0.jar"), e.getMessage());
        assertTrue(e.getMessage().contains("ccloud.baseDir"), e.getMessage());
    }

    @Test
    @DisplayName("an object name is the first back-quoted identifier after the comments")
    void objectName() {
        assertEquals("event_rule_agent", ConfluentCloudPackager.objectName(
            "-- Register once\nCREATE FUNCTION `event_rule_agent` AS 'x' USING JAR 'y'"));
        assertEquals("a`b", ConfluentCloudPackager.objectName("CREATE VIEW `a``b` AS SELECT `c`"));
        assertEquals("statement", ConfluentCloudPackager.objectName("SELECT 1"));
    }
}
