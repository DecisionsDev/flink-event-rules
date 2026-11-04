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
import com.ibm.di.event.agent.AgentDescriptorLoader;
import com.ibm.di.event.agent.AgentDescriptorValidator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static com.ibm.di.event.agent.EnvUtils.boolEnvOr;
import static com.ibm.di.event.agent.EnvUtils.envOr;
import static com.ibm.di.event.agent.EnvUtils.longEnvOr;

/**
 * Entry point of the Process Table Function engine: runs the agents of an {@code agents.json} on
 * Flink SQL, an alternative to {@link com.ibm.di.event.agent.datastream.AgentBasedDataStreamJob} that reads the same descriptor file
 * and honours the same environment variables.
 *
 * <p>It loads and validates the descriptors, generates the SQL topology with
 * {@link AgentSqlPipelineBuilder}, registers {@link StatefulAgentProcessTableFunction} (one
 * instance per agent, holding its descriptor) and {@link EventEnvelopeFunction}, and submits the
 * inserts as one statement set.
 *
 * <p>Environment variables: {@code KAFKA_BOOTSTRAP_SERVERS}, {@code AGENT_DESCRIPTOR_PATH},
 * {@code FLINK_JOB_NAME}, {@code CHECKPOINT_INTERVAL_MS}, {@code WATERMARK_OUT_OF_ORDER_MS},
 * {@code CHAINING_ENABLED}, {@code FLINK_PARALLELISM}, {@code AGENT_ERROR_TOPIC[_<AGENT>]}. The
 * command line arguments are the same as the DataStream job: bootstrap servers, descriptor path,
 * job name. {@code PTF_PRINT_SQL=true} prints the generated script instead of running it, which is
 * the way to obtain the statements to paste in a Confluent Cloud SQL workspace
 * ({@code PTF_DIALECT=confluent}). {@code PTF_PRINT_SQL=json} prints the same statements as JSON
 * ({@link AgentSqlPipelineBuilder.Pipeline#toJson()}), each tagged with its agent, for tools.
 *
 * <p>Source idleness: a SQL job has one idleness timeout for all sources
 * ({@code table.exec.source.idle-timeout}); it is the smallest idleness declared in the
 * {@code topics} section, or {@value AgentDescriptorLoader#DEFAULT_IDLENESS_MS} ms.
 */
public final class AgentBasedPTFJob {

    private static final Logger LOG = LoggerFactory.getLogger(AgentBasedPTFJob.class);

    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String DEFAULT_DESCRIPTOR_PATH   = "samples/agent/agents.json";
    private static final String DEFAULT_JOB_NAME          = "Agent-Based Event Processor (PTF)";

    private AgentBasedPTFJob() {}

    public static void main(String[] args) throws Exception {
        LOG.info("{}", "=".repeat(80));
        LOG.info("Agent-Based Event Processing Job — Apache Flink Process Table Functions");
        LOG.info("{}", "=".repeat(80));

        String bootstrapServers = envOr("KAFKA_BOOTSTRAP_SERVERS",
            args.length > 0 ? args[0] : DEFAULT_BOOTSTRAP_SERVERS);
        String descriptorPath = envOr("AGENT_DESCRIPTOR_PATH",
            args.length > 1 ? args[1] : DEFAULT_DESCRIPTOR_PATH);
        String jobName = envOr("FLINK_JOB_NAME", args.length > 2 ? args[2] : DEFAULT_JOB_NAME);
        long checkpointMs = longEnvOr("CHECKPOINT_INTERVAL_MS", 60_000L);
        long outOfOrderMs = longEnvOr("WATERMARK_OUT_OF_ORDER_MS", 5_000L);
        boolean chaining = boolEnvOr("CHAINING_ENABLED", true);
        String printMode = envOr("PTF_PRINT_SQL", "").trim().toLowerCase(java.util.Locale.ROOT);
        boolean printJson = "json".equals(printMode);
        boolean printOnly = printJson || "true".equals(printMode);

        AgentDescriptorLoader.AgentFileDescriptor file =
            AgentDescriptorLoader.loadFileDescriptor(descriptorPath);
        List<AgentDescriptor> agents = file.agents();
        if (agents.isEmpty()) {
            throw new IllegalStateException(
                "No agent descriptors found in: " + descriptorPath
                    + " — check that the file exists and contains a non-empty \"agents\" array.");
        }
        AgentDescriptorValidator.validateUniqueAgentIds(agents);
        AgentDescriptorValidator.validateUniqueOperatorUids(agents);
        for (AgentDescriptor agent : agents) {
            AgentDescriptorValidator.validate(agent);
            if (!chaining) agent.setChainingEnabled(false);
        }

        int parallelism = file.parallelism();
        if (parallelism <= 0) parallelism = (int) longEnvOr("FLINK_PARALLELISM", 0);
        if (parallelism <= 0) parallelism = 1;

        long idleMs = AgentDescriptorLoader.loadTopicIdleness(descriptorPath).values().stream()
            .mapToLong(Long::longValue).min().orElse(AgentDescriptorLoader.DEFAULT_IDLENESS_MS);

        AgentSqlPipelineBuilder.Options options = new AgentSqlPipelineBuilder.Options();
        options.dialect = "confluent".equalsIgnoreCase(System.getenv("PTF_DIALECT"))
            ? AgentSqlPipelineBuilder.Dialect.CONFLUENT : AgentSqlPipelineBuilder.Dialect.OPEN_SOURCE;
        options.bootstrapServers = bootstrapServers;
        options.outOfOrderMs = outOfOrderMs;
        options.chainingEnabled = chaining;
        options.env = System.getenv();
        AgentSqlPipelineBuilder.Pipeline pipeline = new AgentSqlPipelineBuilder(options).build(agents);

        if (printOnly) {
            System.out.println(printJson ? pipeline.toJson() : pipeline.toScript());
            return;
        }
        if (options.dialect == AgentSqlPipelineBuilder.Dialect.CONFLUENT) {
            throw new IllegalStateException("PTF_DIALECT=confluent only generates SQL for Confluent "
                + "Cloud: set PTF_PRINT_SQL=true and run the script in its SQL workspace.");
        }

        LOG.info("  Bootstrap servers  : {}", bootstrapServers);
        LOG.info("  Descriptor path    : {}", descriptorPath);
        LOG.info("  Checkpoint interval: {} ms", checkpointMs);
        LOG.info("  Watermark OOO bound: {} ms", outOfOrderMs);
        LOG.info("  Source idleness    : {} ms", idleMs);
        LOG.info("  Parallelism        : {}", parallelism);

        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.newInstance()
            .inStreamingMode().withConfiguration(configuration(jobName, checkpointMs, idleMs,
                parallelism)).build());
        execute(tEnv, pipeline);
    }

    /** The Flink configuration of the job. */
    static Configuration configuration(String jobName, long checkpointMs, long idleMs, int parallelism) {
        Configuration c = new Configuration();
        c.setString("pipeline.name", jobName);
        c.setString("parallelism.default", Integer.toString(parallelism));
        c.setString("execution.checkpointing.interval", checkpointMs + " ms");
        c.setString("execution.checkpointing.mode", "AT_LEAST_ONCE");
        c.setString("table.exec.source.idle-timeout", idleMs + " ms");
        return c;
    }

    /**
     * Registers the functions of {@code pipeline}, runs its setup statements and returns its inserts
     * as one (not yet executed) statement set.
     */
    static StatementSet register(TableEnvironment tEnv, AgentSqlPipelineBuilder.Pipeline pipeline) {
        tEnv.createTemporarySystemFunction(EventEnvelopeFunction.NAME, new EventEnvelopeFunction());
        tEnv.createTemporarySystemFunction(EventEnrichFunction.NAME, new EventEnrichFunction());
        for (Map.Entry<String, AgentDescriptor> fn : pipeline.functions.entrySet()) {
            tEnv.createTemporarySystemFunction(fn.getKey(),
                new StatefulAgentProcessTableFunction(fn.getValue()));
        }
        for (String statement : pipeline.setup) {
            LOG.debug("SQL: {}", statement);
            tEnv.executeSql(statement);
        }
        StatementSet set = tEnv.createStatementSet();
        for (String insert : pipeline.inserts) {
            set.addInsertSql(insert);
        }
        return set;
    }

    private static void execute(TableEnvironment tEnv, AgentSqlPipelineBuilder.Pipeline pipeline)
            throws Exception {
        StatementSet set = register(tEnv, pipeline);
        LOG.info("Starting Flink job");
        TableResult result = set.execute();
        if (isLocalExecution(result)) {
            // Local (mini-cluster) execution: keep the JVM alive for as long as the job runs.
            result.await();
        } else {
            // Cluster submission (CLI `flink run -d` or Flink REST API `/jars/:id/run`): the job is
            // running on the cluster and should run detached.
            LOG.info("Job submitted to Flink cluster - not waiting for its result");
        }
    }

    private static boolean isLocalExecution(TableResult result) {
        return result.getJobClient()
            .map(client -> client.getClass().getSimpleName().equals("MiniClusterJobClient"))
            .orElse(false);
    }
}
