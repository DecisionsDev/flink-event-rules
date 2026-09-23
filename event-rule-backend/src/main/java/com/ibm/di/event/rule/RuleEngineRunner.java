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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.decision.run.DecisionRunner;
import com.ibm.decision.run.DecisionServiceProvider;
import com.ibm.decision.run.RunContext;
import com.ibm.decision.run.impl.AbstractDecisionProvider;
import com.ibm.rules.engine.observer.Observable;
import com.ibm.rules.engine.ruledef.runtime.RuleEngineObserver;
import com.ibm.rules.engine.ruledef.runtime.RuleInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serial;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * Loads and executes an IBM Decision Intelligence archive ({@code .jar}/{@code .dsa}) for a
 * single named operation, accepting and returning {@link JsonNode}.
 *
 * <p>Each instance manages one {@link URLClassLoader} and one {@link DecisionRunner} for the
 * given archive URI and operation name.  Initialization is lazy (or explicit via
 * {@link #initializeEagerly()}) and is retried up to three times with linear back-off to
 * tolerate transient filesystem delays after an atomic archive rename.
 *
 * <p>Instances are obtained and lifecycle-managed by {@link com.ibm.di.event.agent.StatefulAgentProcessor};
 * callers should not share instances across threads.  After {@link #close()}, the instance
 * must not be reused — create a new one for the replacement archive.
 */
public class RuleEngineRunner implements Serializable, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(RuleEngineRunner.class);

    @Serial
    private static final long serialVersionUID = 1L;

    private final String rulesetOperation;
    private final URI rulesetJarUri;
    private transient DecisionRunner decisionRunner;
    private transient ObjectMapper objectMapper;
    private transient URLClassLoader rulesetClassLoader;

    /**
     * Creates a runner for the given operation and archive URI.
     * Initialization is deferred until the first {@link #execute(JsonNode)} call
     * or an explicit {@link #initializeEagerly()} call.
     *
     * @param rulesetOperation IBM Decision Intelligence operation name
     * @param rulesetJarUri    URI to the archive JAR/DSA
     */
    public RuleEngineRunner(String rulesetOperation, URI rulesetJarUri) {
        this.rulesetOperation = rulesetOperation;
        this.rulesetJarUri    = rulesetJarUri;
    }

    /**
     * Eagerly initializes the engine so it is fully warm before being swapped in.
     * Delegates to {@link #initialize()}; subclasses may override {@link #initialize()}.
     */
    public final void initializeEagerly() {
        initialize();
    }

    /**
     * Initializes the {@link URLClassLoader}, {@link ObjectMapper}, and {@link DecisionRunner}
     * for this archive.  Subsequent calls after a successful initialization are no-ops.
     * Retried up to 3 times with linear back-off.
     */
    protected void initialize() {
        if (decisionRunner != null) {
            return;
        }
        
        int maxRetries = 3;
        int retryDelayMs = 1000;
        Exception lastException = null;
        
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                LOG.info("Initializing rule engine (attempt {}/{})...", attempt, maxRetries);
                
                // Create classloader for the ruleset
                rulesetClassLoader = createClassLoader(rulesetJarUri);
                
                // Create object mapper with ruleset classloader
                objectMapper = createObjectMapper(rulesetClassLoader);
                
                // Get the decision runner
                decisionRunner = getDecisionRunner(rulesetClassLoader, rulesetOperation);
                
                if (decisionRunner == null) {
                    // "Operation not found" is a permanent configuration error: the archive
                    // JAR does not contain a DecisionServiceProvider for this operation name.
                    // Retrying will never resolve it — fail fast so the BFF gets the error
                    // immediately and does not wait through 3× retry delays.
                    throw new OperationNotFoundException(
                        "Operation '" + rulesetOperation + "' not found in archive: " + rulesetJarUri
                        + ". Check that the archive was built with all required decision flows.");
                }
                
                LOG.info("Rule engine initialized successfully");
                return;
                
            } catch (OperationNotFoundException e) {
                // Permanent: the operation does not exist in this archive — do not retry.
                LOG.error("Operation not found (no retry): {}", e.getMessage());
                throw e;
            } catch (Exception e) {
                lastException = e;
                LOG.error("Failed to initialize rule engine (attempt {}/{}): {}", attempt, maxRetries, e.getMessage());
                
                if (attempt < maxRetries) {
                    try {
                        Thread.sleep((long) retryDelayMs * attempt); // linear back-off
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Initialization interrupted", ie);
                    }
                }
            }
        }
        
        throw new RuntimeException("Failed to initialize rule engine after " + maxRetries + " attempts", lastException);
    }

    /**
     * Thrown when the archive JAR does not contain a {@link DecisionRunner} for the
     * requested operation name.  This is a permanent configuration error — the archive
     * must be rebuilt to include the missing decision flow.  The retry loop in
     * {@link #initialize()} fast-fails on this exception instead of retrying.
     */
    public static final class OperationNotFoundException extends RuntimeException {
        @Serial
        private static final long serialVersionUID = 1L;

        public OperationNotFoundException(String message) {
            super(message);
        }
    }
    
    /**
     * Executes the configured operation on {@code event}.
     *
     * @param event JSON input node (must not be null)
     * @return rule execution result as a {@link JsonNode}
     * @throws IllegalArgumentException if {@code event} is null
     */
    public JsonNode execute(JsonNode event) {
        if (event == null) {
            throw new IllegalArgumentException("Event cannot be null");
        }
        
        initialize();

        // ── Invocation details ────────────────────────────────────────────────
        String eventId = resolveEventId(event);
        LOG.debug("[IBM DI] ▶ Invoking operation='{}' eventId={}\n{}",
            rulesetOperation, eventId, event.toPrettyString());
        
        long startNs = System.nanoTime();
        try {
            // Create run context with a collecting observer that records fired rules
            RuleObserver observer = new RuleObserver();
            RunContext runContext = decisionRunner.createRunContext(rulesetOperation);
            runContext.addObserver(observer);

            // Deserialize the JSON directly into the Decision Intelligence-generated Input class.
            // Input.set(Map) does a raw cast (not JSON deserialization), so we must
            // deserialize via Jackson using the ruleset classloader's type factory.
            Class<?> inputClass = decisionRunner.getInputClass();
            Object input = objectMapper.treeToValue(event, inputClass);

            LOG.debug("Input object sent to the engine: " + objectMapper.writeValueAsString(input));


            Object result = decisionRunner.execute(input, runContext);

            long elapsedMs = (System.nanoTime() - startNs) / 1_000_000;

            // Convert result back to JsonNode
            JsonNode resultNode = objectMapper.valueToTree(result);

            // ── Result summary ────────────────────────────────────────────────
            LOG.debug("[IBM DI] ◀ Complete  eventId={} elapsedMs={} firedRules={}\n{}",
                eventId, elapsedMs, observer.getFiredRuleCount(), resultNode.toPrettyString());
            if (!observer.getFiredRuleNames().isEmpty()) {
                LOG.debug("[IBM DI]   fired: {}", observer.getFiredRuleNames());
            }

            return resultNode;
            
        } catch (IllegalArgumentException e) {
            long elapsedMs = (System.nanoTime() - startNs) / 1_000_000;
            LOG.error("[IBM DI] ✗ Invalid input eventId={} elapsedMs={} error={}", eventId, elapsedMs, e.getMessage());
            throw e;
        } catch (ClassCastException e) {
            long elapsedMs = (System.nanoTime() - startNs) / 1_000_000;
            LOG.error("[IBM DI] ✗ Type error eventId={} elapsedMs={} error={}", eventId, elapsedMs, e.getMessage());
            throw new RuntimeException("Failed to convert event data types", e);
        } catch (Exception e) {
            long elapsedMs = (System.nanoTime() - startNs) / 1_000_000;
            LOG.error("[IBM DI] ✗ Error    eventId={} elapsedMs={} error={}", eventId, elapsedMs, e.getMessage());
            throw new RuntimeException("Failed to execute rules on event", e);
        }
    }

    /**
     * Generates a sample JSON input for this runner's operation using the archive's
     * built-in example data.
     *
     * <p>Calls the <strong>public</strong> {@link DecisionRunner#createInputExample()} API,
     * which internally instantiates the archive's generated {@code Input} POJO and populates
     * it via {@code ConvenientInput.initExample()} (an archive-side generated method).
     * No internal Decision Intelligence classes are referenced here.
     *
     * <p>Must be called after {@link #initialize()} or {@link #initializeEagerly()} —
     * {@link #initialize()} is called automatically if the runner has not yet been warmed up.
     *
     * @return a {@link JsonNode} representing the fully-populated example input, serialised
     *         with the archive-aware {@link ObjectMapper} so all field names and types match
     *         exactly what the live {@link #execute(JsonNode)} expects
     */
    public JsonNode generateSampleInput() {
        initialize(); // no-op if already warm
        Object example = decisionRunner.createInputExample();
        LOG.debug("[IBM DI] generateSampleInput op='{}' type={}", rulesetOperation,
            example.getClass().getName());
        return objectMapper.valueToTree(example);
    }

    /**
     * Executes the operation with a raw input map (alternative to JSON-based {@link #execute}).
     *
     * @param inputs map of input parameter names to values (must not be null)
     * @return rule execution result
     * @throws IllegalArgumentException if {@code inputs} is null
     */
    public Object executeWithInputs(Map<String, Object> inputs) {
        if (inputs == null) {
            throw new IllegalArgumentException("Inputs map cannot be null");
        }
        
        initialize();
        
        try {
            RunContext runContext = decisionRunner.createRunContext(rulesetOperation);
            runContext.addObserver(new RuleObserver());
            
            Object input = decisionRunner.createInput(inputs);
            return decisionRunner.execute(input, runContext);
            
        } catch (Exception e) {
            throw new RuntimeException("Failed to execute rules", e);
        }
    }
    
    /**
     * Resolves a short event identifier for log lines.
     * Checks common domain ID fields in order; falls back to the first 40 characters of the JSON.
     */
    private static String resolveEventId(JsonNode event) {
        for (String field : new String[]{"eventId", "entityId", "id", "correlationId"}) {
            JsonNode n = event.path(field);
            if (!n.isMissingNode() && !n.isNull() && !n.asText("").isBlank()) {
                return n.asText();
            }
        }
        String json = event.toString();
        return json.length() > 40 ? json.substring(0, 40) + "…" : json;
    }

    /** Creates an isolated {@link URLClassLoader} for the archive JAR. */
    private URLClassLoader createClassLoader(URI jarUri) throws MalformedURLException {
        URL jarUrl = jarUri.toURL();
        return new URLClassLoader(new URL[]{jarUrl}, this.getClass().getClassLoader());
    }
    
    /**
     * Closes the rule engine and releases all resources.
     *
     * <p>After this call the instance is in a "closed" state:
     * {@code decisionRunner}, {@code objectMapper}, and {@code rulesetClassLoader}
     * are all null.  The instance must not be used for {@link #execute(JsonNode)}
     * after closing; create a new {@link RuleEngineRunner} for the replacement archive
     * using the constructor directly.
     */
    @Override
    public void close() {
        try {
            if (rulesetClassLoader != null) {
                rulesetClassLoader.close();
            }
        } catch (Exception e) {
            LOG.error("Error closing ruleset classloader: {}", e.getMessage());
        } finally {
            // Null all transient fields so this instance cannot be accidentally reused
            // after close() and so that the decisionRunner != null guard in initialize()
            // does not give a false "already initialized" result if close() is somehow
            // followed by initialize() on the same instance.
            rulesetClassLoader = null;
            decisionRunner     = null;
            objectMapper       = null;
        }
    }
    
    /**
     * Create an ObjectMapper configured for the ruleset.
     */
    private ObjectMapper createObjectMapper(ClassLoader classLoader) {
        ObjectMapper mapper = AbstractDecisionProvider.createObjectMapper();
        mapper.setTypeFactory(mapper.getTypeFactory().withClassLoader(classLoader));
        // Decision Intelligence-generated classes may not have all Java-model fields (e.g. dailyTransactionLimit).
        // Allow unknown properties so extra fields are silently dropped during deserialization.
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        // Allow numeric strings like "188.23" to be coerced into double fields.
        mapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);
        return mapper;
    }
    
    /**
     * Get the DecisionRunner from the ruleset classloader.
     * Manually parses service provider files from the JAR to bypass ServiceLoader limitations.
     */
    private DecisionRunner getDecisionRunner(ClassLoader classLoader, String decisionOperation) {
        LOG.debug("Looking for DecisionRunner with operation: '{}'", decisionOperation);
        LOG.debug("Classloader: {}", classLoader);
        
        // First, try the standard ServiceLoader approach
        DecisionRunner runner = tryServiceLoader(classLoader, decisionOperation);
        if (runner != null) {
            return runner;
        }
        
        // If ServiceLoader fails, manually parse and load providers from the JAR
        LOG.debug("ServiceLoader approach failed, trying manual provider discovery...");
        return tryManualProviderDiscovery(classLoader, decisionOperation);
    }
    
    /**
     * Try to find DecisionRunner using standard ServiceLoader.
     */
    private DecisionRunner tryServiceLoader(ClassLoader classLoader, String decisionOperation) {
        ClassLoader originalContextClassLoader = Thread.currentThread().getContextClassLoader();
        
        try {
            Thread.currentThread().setContextClassLoader(classLoader);
            
            ServiceLoader<DecisionServiceProvider> serviceLoader =
                ServiceLoader.load(DecisionServiceProvider.class, classLoader);
            serviceLoader.reload();
            
            int providerCount = 0;
            for (DecisionServiceProvider provider : serviceLoader) {
                providerCount++;
                try {
                    String providerClassName = provider.getClass().getName();
                    LOG.debug("Discovered DecisionServiceProvider #{}: {}", providerCount, providerClassName);

                    // getDecisionOperationRunner matches by operationName (from META-INF/decisions/*.json).
                    // getDecisionRunner matches by decisionFunction (the class-registration key), which is
                    // the model name (e.g. "FraudDetectionModel-FraudDetectionModel") — not what agents.json uses.
                    DecisionRunner runner = provider.getDecisionOperationRunner(decisionOperation);
                    if (runner != null) {
                        LOG.info("✓ Successfully resolved DecisionRunner for operation: {}", decisionOperation);
                        LOG.info("  Using provider: {}", providerClassName);
                        return runner;
                    }
                } catch (Throwable t) {
                    LOG.warn("Failed while querying DecisionServiceProvider: {} error={}",
                        provider.getClass().getName(), t.getMessage());
                }
            }
            
            LOG.warn("ServiceLoader found {} provider(s), but none matched the operation", providerCount);
            return null;
            
        } finally {
            Thread.currentThread().setContextClassLoader(originalContextClassLoader);
        }
    }
    
    /**
     * Manually discover and load service providers from the JAR.
     * This bypasses ServiceLoader's limitations with dynamically loaded JARs.
     */
    private DecisionRunner tryManualProviderDiscovery(ClassLoader classLoader, String decisionOperation) {
        String serviceFile = "META-INF/services/com.ibm.decision.run.DecisionServiceProvider";
        
        try {
            List<String> providerClassNames = new ArrayList<>();
            
            // For URLClassLoader, directly access the JAR file
            if (classLoader instanceof URLClassLoader urlClassLoader) {
                URL[] urls = urlClassLoader.getURLs();
                
                LOG.debug("Scanning {} URL(s) for service providers...", urls.length);
                
                for (URL jarUrl : urls) {
                    LOG.debug("  Checking JAR: {}", jarUrl);
                    
                    // Use JarFile to properly read from the JAR
                    try {
                        String jarPath = jarUrl.getPath();
                        // Remove file: prefix if present
                        if (jarPath.startsWith("file:")) {
                            jarPath = jarPath.substring(5);
                        }

                        // try-with-resources ensures the JarFile is closed even if an
                        // IOException is thrown while reading the service-file entry.
                        try (java.util.jar.JarFile jarFile = new java.util.jar.JarFile(jarPath)) {
                            java.util.jar.JarEntry serviceEntry = jarFile.getJarEntry(serviceFile);

                            if (serviceEntry != null) {
                                LOG.debug("  Found service file in JAR: {}", serviceFile);

                                try (InputStream is = jarFile.getInputStream(serviceEntry);
                                     BufferedReader reader = new BufferedReader(
                                         new InputStreamReader(is, StandardCharsets.UTF_8))) {

                                    String line;
                                    while ((line = reader.readLine()) != null) {
                                        // Remove comments and trim whitespace
                                        int commentIndex = line.indexOf('#');
                                        if (commentIndex >= 0) {
                                            line = line.substring(0, commentIndex);
                                        }
                                        line = line.trim();

                                        // Skip empty lines
                                        if (!line.isEmpty()) {
                                            providerClassNames.add(line);
                                            LOG.debug("    Found provider class: {}", line);
                                        }
                                    }
                                }
                            } else {
                                LOG.debug("  Service file not found in this JAR");
                            }
                        }

                    } catch (IOException e) {
                        LOG.error("  Error reading JAR file: {}", e.getMessage());
                    }
                }
            } else {
                LOG.warn("Classloader is not a URLClassLoader, cannot manually discover providers");
                return null;
            }
            
            if (providerClassNames.isEmpty()) {
                LOG.warn("No service provider classes found in any JAR");
                return null;
            }
            
            // Try to instantiate and query each provider
            for (String providerClassName : providerClassNames) {
                try {
                    LOG.debug("Attempting to load provider: {}", providerClassName);
                    
                    // Load the provider class using the ruleset classloader
                    Class<?> providerClass = Class.forName(providerClassName, true, classLoader);
                    LOG.debug("  Provider class loaded successfully");
                    
                    // Instantiate the provider
                    Object providerInstance = providerClass.getDeclaredConstructor().newInstance();
                    LOG.debug("  Provider instance created successfully");
                    
                    // Cast to DecisionServiceProvider
                    if (providerInstance instanceof DecisionServiceProvider provider) {
                        LOG.debug("  Provider implements DecisionServiceProvider interface");

                        // getDecisionOperationRunner matches by operationName (from META-INF/decisions/*.json).
                        // getDecisionRunner matches by decisionFunction (model-registration key) — not operationName.
                        DecisionRunner runner = provider.getDecisionOperationRunner(decisionOperation);
                        if (runner != null) {
                            LOG.info("✓ Successfully resolved DecisionRunner for operation: {}", decisionOperation);
                            LOG.info("  Using manually loaded provider: {}", providerClassName);
                            return runner;
                        } else {
                            LOG.debug("✗ Provider did not resolve operation: {}", decisionOperation);
                        }
                    } else {
                        LOG.warn("  Provider does not implement DecisionServiceProvider interface");
                    }
                    
                } catch (ClassNotFoundException e) {
                    LOG.error("  Failed to load provider class: {}", e.getMessage());
                } catch (NoSuchMethodException e) {
                    LOG.error("  Provider has no default constructor: {}", e.getMessage());
                } catch (Exception e) {
                    LOG.error("  Failed to instantiate provider: {}", e.getMessage());
                }
            }
            
            LOG.warn("Manual provider discovery found {} provider(s), but none matched the operation",
                providerClassNames.size());
            return null;
            
        } catch (Exception e) {
            LOG.error("Error during manual provider discovery: {}", e.getMessage());
            return null;
        }
    }
    
    /**
     * Observer for rule execution events.
     *
     * <p>Collects the names of every rule that fires during a single invocation
     * so they can be included in the post-execution log line.
     */
    private static class RuleObserver implements RuleEngineObserver, Serializable {
        
        @Serial
        private static final long serialVersionUID = 1L;

        private final List<String> firedRuleNames = new ArrayList<>();

        /** Returns an unmodifiable view of every rule name that fired. */
        List<String> getFiredRuleNames() {
            return Collections.unmodifiableList(firedRuleNames);
        }

        /** Returns the number of rules that fired. */
        int getFiredRuleCount() {
            return firedRuleNames.size();
        }
        
        @Override
        public void exceptionRaisedInAction(Observable observable, Exception exception,
                                            RuleInstance ruleInstance, boolean b) {
            LOG.error("[IBM DI]   ✗ Exception in rule action [{}]: {}",
                ruleInstance.getRule().getName(), exception.getMessage());
        }
        
        @Override
        public void exceptionRaisedInCondition(Observable observable, Exception exception, boolean b) {
            LOG.error("[IBM DI]   ✗ Exception in rule condition: {}", exception.getMessage());
        }
        
        @Override
        public void ruleExecutionStarted(Observable observable, RuleInstance ruleInstance) {
            String name = ruleInstance.getRule().getName();
            LOG.debug("[IBM DI]   → Rule started:  {}", name);
        }

        @Override
        public void ruleExecutionEnded(Observable observable, RuleInstance ruleInstance) {
            String name = ruleInstance.getRule().getName();
            firedRuleNames.add(name);
            LOG.debug("[IBM DI]   ← Rule finished: {}", name);
        }
    }
}

