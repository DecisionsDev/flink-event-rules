# Use Cases

This directory contains sample Agents, demo scenarios, and Decision Intelligence archives,
organized one directory per use case, demonstrating the event-processing capabilities of the
Event Rule application.

Each use case provides:
- **`agents.json`** — Agent descriptor consumed by `AgentBasedDataStreamJob` (Flink) and by the BFF
  for dashboard auto-discovery (optional top-level use-case properties: `name`, `description`, `topics`, ...)
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
| [Retail — Next Best Offer Agent](retail-agent/README.md) | Real-time offer decisioning: evaluates customer interactions against accumulated customer eligibility, product relevance, inventory, and active promotions to surface the best offer. Stateful decision service with multi-context event processing. |
| [Transport — Airline Agent](airline-agent/README.md) | Stateful airline operations pipeline: correlates flight, passenger, and catering events per flight; raises notifications and schedules timers (check-in open 48 h before departure). |

## Directory Structure

```
samples/
├── README.md                              # This file
├── banking-fraud-agent/                   # Fraud detection (chainingEnabled: false)
│   ├── README.md                          # Use-case documentation
│   ├── agents.json                        # Agent descriptor + use-case properties
│   ├── FraudDetectionService_1.5.2.jar    # Pre-built Decision Intelligence archive
│   ├── demo-scenarios.json                # Demo scenarios for the UI
│   ├── di-project/                        # IBM Decision Intelligence project source
│   └── scripts/
│       └── deploy.sh                      # Submit the Flink job for this use case
├── airtraffic-agent/                      # Air Traffic Control — cross-domain chaining (chainingEnabled: true)
│   ├── README.md                          # Use-case documentation
│   └── agents.json                        # Agent descriptor (Aircraft + Airport agents) + use-case properties
├── retail-agent/                          # Retail Next Best Offer — stateful decision service
│   ├── README.md                          # Use-case documentation
│   ├── agents.json                        # Agent descriptor + use-case properties
│   ├── RealTimeNextBestOffer.zip          # IBM Decision Intelligence project source archive
│   ├── demo-scenarios.json                # Demo scenarios for the UI
│   └── scripts/
│       └── deploy.sh                      # Submit the Flink job for this use case
└── airline-agent/                         # Transport Airline — scheduling and multi-event correlation
    ├── README.md                          # Use-case documentation
    ├── agents.json                        # Agent descriptor + use-case properties
    ├── AirlineDecisionService_1.1.0.jar   # Pre-built Decision Intelligence archive
    ├── demo-scenarios.json                # Demo scenarios for the UI
    ├── di-project/                        # IBM Decision Intelligence project source
    └── scripts/
        ├── deploy.sh                      # Submit the Flink job for this use case
        └── get-dsa.sh                     # Download a new archive version from a DI runtime
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

For the full step-by-step guide — directory structure, `agents.json`,
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

