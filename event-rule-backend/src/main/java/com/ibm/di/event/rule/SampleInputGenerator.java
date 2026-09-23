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
package com.ibm.di.event.rule;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;

/**
 * Generates a sample JSON input payload for a given IBM Decision Intelligence Archive operation.
 *
 * <h3>How it works</h3>
 * <p>Delegates entirely to {@link RuleEngineRunner#generateSampleInput()}, which calls
 * the <strong>public</strong> {@link com.ibm.decision.run.DecisionRunner#createInputExample()}
 * API.  Internally the archive's generated {@code Input} POJO is instantiated and populated
 * via its own {@code initExample()} method — the result is then serialised with the
 * archive-aware {@link ObjectMapper} so every field name and type exactly matches
 * what the live rule engine expects.
 *
 * <h3>API classification</h3>
 * <ul>
 *   <li>{@code DecisionRunner.createInputExample()} — <strong>PUBLIC</strong> interface method
 *       (engine-de-api).  No internal Decision Intelligence classes are referenced here.</li>
 *   <li>{@code AbstractDecisionProvider.createObjectMapper()} — public static, already used
 *       throughout {@link RuleEngineRunner}.</li>
 * </ul>
 *
 * <h3>CLI usage</h3>
 * <pre>
 *   java -cp event-rule-backend-1.0-SNAPSHOT.jar \
 *        com.ibm.di.event.rule.SampleInputGenerator \
 *        &lt;archive-path-or-uri&gt; &lt;operation&gt;
 *
 *   # Example (Banking Fraud Agent):
 *   java -cp event-rule-backend-1.0-SNAPSHOT.jar \
 *        com.ibm.di.event.rule.SampleInputGenerator \
 *        samples/banking-fraud-agent/FraudDetectionService_1.3.1.jar EvaluateTransaction
 * </pre>
 * <p>Prints the pretty-printed JSON to stdout; exits with code 1 on failure.
 * This is the entry point called by the BFF via a Node.js child process.
 *
 * <h3>Logging and stdout purity</h3>
 * <p>This class writes <strong>only JSON</strong> to {@code stdout}.  All error
 * text is written to {@code stderr}.  The caller <em>must</em> suppress Log4j2
 * output (e.g. by passing {@code -Dlog4j2.configurationFile=NONE}) so that the
 * bundled {@code log4j2.properties} ConsoleAppender does not contaminate
 * {@code stdout} with log lines that would break {@code JSON.parse(stdout)}.
 */
public class SampleInputGenerator {

    private SampleInputGenerator() {}

    /**
     * Generates a pretty-printed sample JSON input string for the given Decision Archive
     * operation.
     *
     * <p>Opens a dedicated {@link java.net.URLClassLoader} for the archive JAR, resolves
     * the {@link com.ibm.decision.run.DecisionServiceProvider} via {@link java.util.ServiceLoader},
     * calls {@code runner.createInputExample()} (public API), and serialises the result
     * to JSON using the archive-aware {@link ObjectMapper}.
     *
     * <p>The classloader is closed after serialisation so no class-leaking occurs when
     * this method is called from a long-lived process.
     *
     * @param archiveUri URI pointing to the Decision Archive (.jar / .dsa)
     * @param operation  Decision Intelligence operation name (e.g. {@code "fraudDetectionService"})
     * @return pretty-printed JSON string
     * @throws IOException              if JSON serialisation fails
     * @throws IllegalArgumentException if no runner is found for the given operation
     */
    public static String generateSampleInputJson(URI archiveUri, String operation)
            throws IOException {
        // RuleEngineRunner already manages a URLClassLoader per archive URI and
        // exposes the archive-aware ObjectMapper — reuse it to avoid duplication.
        try (RuleEngineRunner runner = new RuleEngineRunner(operation, archiveUri)) {
            runner.initializeEagerly();
            JsonNode sample = runner.generateSampleInput();
            return new ObjectMapper()
                .writerWithDefaultPrettyPrinter()
                .writeValueAsString(sample);
        }
    }

    // ── CLI entry point ───────────────────────────────────────────────────────

    /**
     * CLI entry point invoked by the BFF Node.js process via {@code child_process.spawn}.
     *
     * <p>Exits 0 and prints JSON to stdout on success; exits 1 and prints the error
     * message to stderr on failure.
     *
     * @param args {@code args[0]} = archive path or URI, {@code args[1]} = operation name
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println(
                "Usage: SampleInputGenerator <archive-path-or-uri> <operation>");
            System.exit(1);
        }

        String archiveArg = args[0];
        String operation  = args[1];

        URI archiveUri = archiveArg.contains("://")
            ? URI.create(archiveArg)
            : java.nio.file.Paths.get(archiveArg).toUri();

        try {
            System.out.println(generateSampleInputJson(archiveUri, operation));
        } catch (Exception e) {
            System.err.println("SampleInputGenerator failed: " + e.getMessage());
            System.exit(1);
        }
    }
}

