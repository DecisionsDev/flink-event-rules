# Use Cases

This directory contains sample Agents, demo scenarios, and Decision Intelligence archives,
organized one directory per use case, demonstrating the event-processing capabilities of the
Event Rule application.

Each use case provides:
- **`agents.json`** — Agent descriptor consumed by `AgentBasedDataStreamJob` (Flink)
- **`descriptor.json`** — BFF use-case descriptor for dashboard auto-discovery
- **Decision Intelligence archive** — pre-built JAR/ZIP with the ruleset
- **`demo-scenarios.json`** — curated demo scenarios for the UI
- **`di-project/`** — IBM Decision Intelligence project source
- **`README.md`** — use-case-specific documentation

For the descriptor schema and how these files relate to each other, see
[`../docs/AGENT_PATTERN_DESIGN.md`](../docs/AGENT_PATTERN_DESIGN.md). To build your own use case
from scratch, see [`../TUTORIAL.md`](../TUTORIAL.md).

## Available Use Cases

| Use case                                              | Description                                                                                                                                     |
|-------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------|
| [Banking Fraud Agent](banking-fraud-agent/README.md) | Fraud detection pipeline: evaluates `Transaction` events against account history and outputs alerts. `chainingEnabled: false`                   |
| [Air Traffic Control](airtraffic-agent/README.md)    | Cross-domain agent chaining: Aircraft Agent feeds Airport Agent directly in-process. `chainingEnabled: true` on both agents.                    |

## Directory Structure

```
samples/
├── README.md                              # This file
├── banking-fraud-agent/                   # Fraud detection (chainingEnabled: false)
│   ├── README.md                          # Use-case documentation
│   ├── agents.json                        # Agent descriptor
│   ├── descriptor.json                    # BFF use-case descriptor
│   ├── FraudDetectionService_1.5.2.jar    # Pre-built Decision Intelligence archive
│   ├── demo-scenarios.json                # Demo scenarios for the UI
│   ├── di-project/                        # IBM Decision Intelligence project source
│   └── scripts/
│       └── deploy.sh                      # Submit the Flink job for this use case
└── airtraffic-agent/                      # Air Traffic Control — cross-domain chaining (chainingEnabled: true)
    ├── README.md                          # Use-case documentation
    ├── agents.json                        # Agent descriptor (Aircraft + Airport agents)
    └── descriptor.json                    # BFF use-case descriptor
```

## Running a use case

See [**`README.md` §3 Quick Start**](../README.md#3-quick-start) for the complete build,
infrastructure start, deploy, and verification steps.

A quick reference for the most common operations:

```bash
# Build
./build.sh

# Start infrastructure
./run.sh up

# Deploy the banking fraud agent
./samples/banking-fraud-agent/scripts/deploy.sh

# Run scenarios from the CLI
./scripts/launch-demo-scenario.sh banking-fraud-agent
./scripts/launch-demo-scenario.sh banking-fraud-agent transaction-on-suspended-account
```

See [`banking-fraud-agent/README.md`](banking-fraud-agent/README.md) for use-case-specific rules,
event shapes, demo scenarios, and an end-to-end walkthrough.

## Creating a new use case

For the full step-by-step guide — directory structure, `agents.json`, `descriptor.json`,
Decision Intelligence archive design, deploy script, and validation — see
[**`TUTORIAL.md`**](../TUTORIAL.md).

## Prerequisites

- Docker and Docker Compose (V2 or later)
- `jq` command-line JSON processor
  - macOS: `brew install jq`
  - Linux: `apt-get install jq`

## Verifying output events

After submitting events, verify output was produced:

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic agent-fraud-alerts \
  --from-beginning \
  --max-messages 10
```

