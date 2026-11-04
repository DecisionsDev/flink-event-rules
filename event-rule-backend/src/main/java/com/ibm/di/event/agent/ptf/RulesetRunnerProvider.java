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

import com.ibm.di.event.agent.AgentDescriptor;
import com.ibm.di.event.agent.EventSubscription;
import com.ibm.di.event.rule.RuleEngineRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.file.Paths;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Creates and caches one {@link RuleEngineRunner} per operation from the ruleset archive named by
 * {@code ruleset.path} of the descriptor.
 *
 * <p>The archive is fixed for the life of the function: unlike {@code StatefulAgentProcessor} there
 * is no broadcast channel to receive a new one. The path is a URI when it contains {@code ://},
 * otherwise a file path.
 */
public final class RulesetRunnerProvider implements AgentEventProcessor.RunnerProvider {

    private static final Logger LOG = LoggerFactory.getLogger(RulesetRunnerProvider.class);

    private final AgentDescriptor descriptor;
    private transient Map<String, RuleEngineRunner> runners;

    public RulesetRunnerProvider(AgentDescriptor descriptor) {
        this.descriptor = descriptor;
    }

    /** Initializes the runner of every operation now, so a bad archive fails the function at open. */
    public void warmUp() {
        Set<String> operations = new LinkedHashSet<>();
        if (descriptor.getSubscriptions() != null) {
            for (EventSubscription sub : descriptor.getSubscriptions()) {
                if (sub.hasOperation()) operations.add(sub.getOperation());
            }
        }
        for (String operation : operations) {
            runnerFor(operation);
        }
    }

    @Override
    public RuleEngineRunner runnerFor(String operation) {
        if (runners == null) runners = new ConcurrentHashMap<>();
        return runners.computeIfAbsent(operation, op -> {
            RuleEngineRunner runner = new RuleEngineRunner(op, rulesetUri());
            runner.initializeEagerly();
            LOG.info("[{}] Runner initialised. operation={}", descriptor.getAgentId(), op);
            return runner;
        });
    }

    /** Releases every runner. */
    public void close() {
        if (runners == null) return;
        for (RuleEngineRunner runner : runners.values()) {
            try {
                runner.close();
            } catch (RuntimeException e) {
                LOG.warn("[{}] Runner close failed: {}", descriptor.getAgentId(), e.getMessage());
            }
        }
        runners.clear();
    }

    private URI rulesetUri() {
        String path = descriptor.getRuleset().getPath();
        if (path.contains("://")) return URI.create(path);
        // Absolute path — use as-is.
        java.nio.file.Path p = Paths.get(path);
        if (p.isAbsolute()) return p.toUri();
        // Relative path: try cwd first, then cwd's parent (project root).
        // This mirrors AgentDescriptorLoader's resolution strategy so that a path like
        // "samples/banking-fraud-agent/Foo.jar" works whether the script is launched from
        // the project root or from the event-rule-backend sub-directory.
        java.nio.file.Path resolved = Paths.get(System.getProperty("user.dir")).resolve(path);
        if (!resolved.toFile().exists()) {
            java.nio.file.Path parent = Paths.get(System.getProperty("user.dir")).getParent();
            if (parent != null) {
                java.nio.file.Path viaParent = parent.resolve(path);
                if (viaParent.toFile().exists()) resolved = viaParent;
            }
        }
        return resolved.toUri();
    }
}
