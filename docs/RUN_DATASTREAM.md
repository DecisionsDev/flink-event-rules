# Running AgentBasedDataStreamJob from Command Line

This guide explains how to run the `AgentBasedDataStreamJob` application from the command line using the provided script.

## Prerequisites

- Java 21 or higher
- Maven 3.6 or higher
- Kafka running (default: localhost:9092)

## Quick Start

1. **Compile the project** (if not already compiled):
   ```bash
   cd event-rule-backend
   mvn clean compile
   ```

2. **Run with default settings**:
   ```bash
   ./run-datastream.sh
   ```

   This uses:
   - Bootstrap servers: `localhost:9092`
   - Agents JSON: `../samples/banking-fraud-agent/agents.json` (must be supplied; no built-in default)

3. **Run with custom settings**:
   ```bash
   ./run-datastream.sh <bootstrap-servers> <agents-json-path>
   ```

   Example:
   ```bash
   ./run-datastream.sh localhost:9092 ../samples/banking-fraud-agent/agents.json
   ```

## What the Script Does

The `run-datastream.sh` script:

1. **Builds Maven classpath**: Uses `mvn dependency:build-classpath` with the `dev` profile to include all dependencies (including Flink provided dependencies)

2. **Adds to classpath**:
   - `target/classes` - Compiled application classes
   - `target/test-classes` - Test classes
   - Maven dependencies (all compile-scope dependencies)
   - `.` - Current directory

3. **Runs AgentBasedDataStreamJob**: Executes the main class with proper classpath and log4j configuration

## Classpath Details

The script ensures the following are in the classpath:

- **Maven Dependencies**:
  - Apache Flink (streaming-java, clients)
  - Flink Kafka Connector
  - Jackson JSON processing
  - IBM Decision Intelligence Execution API
  - Log4j logging framework

## Troubleshooting

### Classpath Issues

If you encounter classpath-related errors:

1. Ensure Maven dependencies are downloaded:
   ```bash
   mvn dependency:resolve
   ```

2. Verify the classpath is built correctly:
   ```bash
   mvn -Pdev dependency:build-classpath
   ```

### Agents JSON Not Found

If the agents descriptor cannot be loaded:

1. Check the agents JSON path is correct
2. Verify the file exists (e.g. `samples/banking-fraud-agent/agents.json`)

### Kafka Connection Issues

If Kafka connection fails:

1. Verify Kafka is running: `docker ps` or check your Kafka service
2. Check the bootstrap servers address is correct
3. Ensure topics defined in `agents.json` subscriptions exist in Kafka

## Development Profile

The script uses Maven's `dev` profile which changes Flink dependencies from `provided` to `compile` scope. This ensures Flink libraries are included in the classpath for local execution.

## Alternative: Using Maven Exec Plugin

You can also run using Maven directly:

```bash
mvn -Pdev compile exec:java \
  -Dexec.mainClass="com.ibm.di.event.agent.AgentBasedDataStreamJob" \
  -Dexec.args="localhost:9092 ../samples/banking-fraud-agent/agents.json" \
  -Dexec.classpathScope=compile
```
