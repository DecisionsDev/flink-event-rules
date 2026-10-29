# Runtime image for Flink backend
# IMPORTANT: Build the JAR locally first with: mvn clean install -DskipTests
# This Dockerfile assumes the application has been built and the JAR exists in target/

ARG FLINK_IMAGE=flink:2.2.0-scala_2.12-java21
FROM ${FLINK_IMAGE}

# Accept build argument for descriptor path
ARG DESCRIPTOR_PATH=samples/banking/agents.json

# Copy the pre-built jar from local target directory
# Note: Kafka connector is already included in the shaded JAR, no need to download separately
COPY event-rule-backend/target/event-rule-backend-1.0-SNAPSHOT.jar /opt/flink/usrlib/event-rule-backend.jar

# Copy application properties
COPY event-rule-backend/src/main/resources/application.properties /opt/flink/conf/application.properties

# Copy Flink configuration (proper YAML format for Flink 2.1.3)
COPY event-rule-backend/src/main/resources/flink-conf.yaml /opt/flink/conf/flink-conf.yaml

# Set environment variables
# FLINK_PROPERTIES must be valid YAML - use proper multi-line format or key=value pairs
# For Flink 2.1.3, use flink-conf.yaml format or avoid FLINK_PROPERTIES for simple configs
ENV DESCRIPTOR_PATH=${DESCRIPTOR_PATH}

# Expose Flink Web UI port
EXPOSE 8081

# Start Flink in standalone cluster mode
# First start JobManager, then TaskManager, then submit the job
CMD ["/bin/bash", "-c", "/docker-entrypoint.sh jobmanager & sleep 10 && /docker-entrypoint.sh taskmanager & sleep 5 && /opt/flink/bin/flink run -d /opt/flink/usrlib/event-rule-backend.jar && tail -f /dev/null"]

