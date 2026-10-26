# Docker Compose setup

This document provides detailed instructions for building and running the Event Rule
application in Docker. For a quick entry point, see
[`README.md` §8 (Running with Docker)](README.md#8-running-with-docker).

## Prerequisites

- Docker Engine 20.10 or later
- Docker Compose V2 or later
- At least 4GB of available RAM

## Architecture overview

The Docker Compose setup includes:

1. **Zookeeper** - Coordination service for Kafka (port 2181)
2. **Kafka** - Message broker (ports 9092 for host, 29092 for inter-container)
3. **Flink JobManager** - Job coordination and REST API (port 8081)
4. **Flink TaskManager** - Stream processing execution
5. **BFF** - Backend-for-Frontend Node.js service bridging Kafka to WebSocket (port 8080)
6. **Frontend** - React application served by Nginx (port 3010)

Flink jobs are submitted separately after the infrastructure is healthy, using the scripts in `scripts/`.

## Configuration

### Image versions — `.env`

All Docker image versions are centralised in [`.env`](.env), which `docker compose`
loads automatically. Edit this file to upgrade any image across the entire stack at once.

| Variable | Default | Description |
|---|---|---|
| `ZOOKEEPER_IMAGE` | `confluentinc/cp-zookeeper:7.5.0` | Zookeeper image |
| `KAFKA_IMAGE` | `confluentinc/cp-kafka:7.5.0` | Kafka broker + init image |
| `FLINK_IMAGE` | See `.env` file | Flink JobManager and TaskManager |
| `BFF_BASE_IMAGE` | `node:18-alpine` | Base image for the BFF Dockerfile |
| `FRONTEND_BASE_IMAGE` | `nginx:alpine` | Base image for the frontend Dockerfile |

## Quick start

### 1. Start all services

```bash
./run.sh up
```

### 2. Submit a Flink job

Once the services are healthy (Flink UI at http://localhost:8081 shows a registered TaskManager):

```bash
# Banking fraud detection
./samples/banking-fraud-agent/scripts/deploy.sh
```

### 3. Check service status

```bash
docker compose ps
```

### 4. View logs

```bash
# All services
docker compose logs -f

# Specific service
docker compose logs -f jobmanager
docker compose logs -f taskmanager
docker compose logs -f kafka
```

### 5. Access the applications

- **Frontend**: http://localhost:3010
- **BFF WebSocket**: ws://localhost:8080
- **BFF REST API**: http://localhost:8080/api
- **BFF Health**: http://localhost:8080/health
- **Flink Web UI**: http://localhost:8081
- **Kafka**: localhost:9092

## Managing services

### Stop all services

```bash
docker compose down
```

### Stop and remove volumes (clean slate)

```bash
# Docker Compose volume removal
docker compose down -v

# Or use the project cleanup script for complete teardown including checkpoints and build artifacts
./run.sh clean
```

### Restart a specific service

```bash
# Restart BFF
docker compose restart bff

# Restart frontend
docker compose restart frontend
```

### Rebuild after code changes

To rebuild all images before running the app:

```bash
docker compose up -d --build
```

```bash
# Rebuild and restart BFF
docker compose up -d --build bff

# Rebuild and restart frontend
docker compose up -d --build frontend
```

## Troubleshooting

### Flink job not running

The Flink job is submitted as a transient job — it is not a persistent Docker service.

1. Check the TaskManager is registered (Flink UI at http://localhost:8081):
   ```bash
   docker compose ps taskmanager
   ```

2. View TaskManager logs:
   ```bash
   docker compose logs taskmanager
   ```

3. Re-submit the Flink job:
   ```bash
   ./samples/banking-fraud-agent/scripts/deploy.sh
   ```

### BFF not starting

1. Check if Kafka is healthy:
   ```bash
   docker compose ps kafka
   ```

2. View BFF logs:
   ```bash
   docker compose logs bff
   ```

3. Restart BFF:
   ```bash
   docker compose restart bff
   ```

### Frontend not accessible

1. Check if the container is running:
   ```bash
   docker compose ps frontend
   ```

2. Check if BFF is running (frontend depends on it):
   ```bash
   docker compose ps bff
   ```

3. View frontend logs:
   ```bash
   docker compose logs frontend
   ```

### Kafka connection issues

1. Verify Kafka is running:
   ```bash
   docker compose ps kafka
   ```

2. Check Kafka logs:
   ```bash
   docker compose logs kafka
   ```

3. Test Kafka connectivity from TaskManager:
   ```bash
   docker exec -it event-rule-backend-taskmanager ping kafka
   ```

### Clean restart

If you encounter persistent issues:

```bash
# Stop everything and remove volumes
docker compose down -v

# Remove all containers and images
docker compose down --rmi all

# Rebuild and start fresh
docker compose up -d --build
```

## Development workflow

### Making backend changes

The Flink job is submitted as a transient job to the Flink cluster — it is not a persistent
Docker service. To pick up Java code changes:

1. Modify Java code in `event-rule-backend/src/`.
2. Rebuild the fat JAR:
   ```bash
   cd event-rule-backend && mvn package -DskipTests
   ```
3. Re-submit the Flink job:
   ```bash
   ./samples/banking-fraud-agent/scripts/deploy.sh
   ```

The TaskManager volume-mounts the `event-rule-backend/target/` directory, so no Docker
image rebuild is needed for backend code changes.

### Making BFF changes

1. Modify Node.js code in `event-rule-frontend/bff/`.
2. Rebuild and restart:
   ```bash
   docker compose up -d --build bff
   ```

### Making frontend changes

1. Modify React code in `event-rule-frontend/src/`.
2. Rebuild and restart:
   ```bash
   docker compose up -d --build frontend
   ```

## Resource management

### View resource usage

```bash
docker stats
```

### Adjust resource limits

Resource limits are already defined in `docker-compose.yml` for each service. Edit the `deploy.resources` block for the relevant service (`jobmanager`, `taskmanager`, `kafka`, etc.) to tune CPU and memory allocation.

## Network

All services communicate through the `event-rule-network` bridge network:
- Services can reach each other using service names (e.g., `kafka:29092`, `bff:8080`)
- Host machine can access services via localhost and exposed ports

### Service Communication Flow

```
Kafka (port 29092) → Flink TaskManager (event stream processing)
                   ↓
Kafka (port 29092) → BFF (port 8080) → Frontend (port 3010)
                     WebSocket/REST
```

The BFF service:
- Consumes events from Kafka topics
- Maintains an in-memory cache of events
- Streams events to the frontend via WebSocket
- Provides REST API fallback for initial data fetch

## Stopping the application

```bash
# Stop services but keep data
docker compose stop

# Stop and remove containers (keeps images and volumes)
docker compose down

# Complete project cleanup (removes containers, volumes, checkpoints, build artifacts, state)
./run.sh clean
# (or bypass prompt with ./run.sh clean --force)
```