# AGENT.md — Repository guide for coding agents

Goal: find the right file fast and avoid reading large or irrelevant files.

## What this project is

Event Rules: JSON-configured "agents" that consume Kafka events, keep per-entity state in Flink, and
call IBM Decision Intelligence (DI) rulesets in-process. A single `agents.json` descriptor drives
everything, so new use cases need no Java.

Stack: Java 21 + Flink 2.2 (backend), Node/Express BFF + React 18/Vite dashboard (frontend), Kafka, Docker Compose.

## Layout (only what matters)

| Path | Purpose |
|---|---|
| `event-rule-backend/src/main/java/com/ibm/di/event/` | All Java source (see map below) |
| `event-rule-backend/src/test/java/...` | Unit tests mirror the main package tree; `e2e/` and `functional/` need live infra |
| `event-rule-frontend/bff/server.js` | BFF: Kafka→WebSocket bridge, REST API, Flink job submission (~3.8k lines) |
| `event-rule-frontend/bff/routes/` | Extracted BFF routes: `di_route.js` (DI runtimes), `scenarios.js` |
| `event-rule-frontend/src/` | Flat React app (`*.jsx`), tests sit next to sources (`*.test.js[x]`) |
| `samples/<use-case>/` | `agents.json`, `demo-scenarios.json`, `scripts/deploy.sh`, `di-project/`, prebuilt DI `*.jar` |
| `docs/` | Design docs (see "Docs to read instead of code") |
| `scripts/` | Ops helper shell scripts (topics, DI archive download, monitoring) |
| `functional-tests/` | Bash-driven end-to-end suite (`run-functional-tests.sh`, `bin/use_cases`, `fixtures/`) |
| `run.sh`, `build.sh`, `Makefile`, `docker-compose*.yml` | Lifecycle, build, test entry points |

## Backend package map (`com.ibm.di.event`)

- `agent/` — descriptor model and shared logic: `AgentDescriptor`, `AgentDescriptorLoader`, `AgentDescriptorValidator`, `AgentState`, `EventEnvelope`, `EventSubscription`, `RulesetConfig`, `AgentChainBuilder` (inter-agent wiring), `*Config` classes, `JsonPathUtils`, `EnvUtils`.
- `agent/datastream/` — **DataStream backend (default)**. `AgentBasedDataStreamJob` (DAG), `StatefulAgentProcessor` (core logic, 1.6k lines — read by method, not whole), `ScheduleManager`, `EnrichEnvelopeFunction`.
- `agent/ptf/` — **PTF / Flink SQL backend**. `AgentBasedPTFJob`, `AgentSqlPipelineBuilder` (generates SQL), `AgentEventProcessor` (Flink-free port of the processor pipeline), `StatefulAgentProcessTableFunction`, `AgentJobLauncher` (shaded JAR main class; picks backend via `EVENT_RULE_RUNTIME=datastream|ptf`).
- `agent/context/` — rule input `ContextBuilder` / `GenericContextBuilder`.
- `agent/enrichment/` — CSV reference-data enrichment; `enrichment/sql/` — SQL enrichment (DataStream only).
- `rule/` — `RuleEngineRunner` (DI `DecisionRunner` adapter, per-archive `URLClassLoader`), `SampleInputGenerator` (CLI spawned by BFF); `rule/broadcast/` — archive hot reload (control cmds, download + SHA-256).
- `api/` — `DeploymentApiServer` (JDK `HttpServer`) for archive deployment.

**Parity rule:** logic changes to `StatefulAgentProcessor` (DataStream) usually need the matching change in `AgentEventProcessor` (PTF), and vice versa. Hot reload and `sqlEnrichment` are DataStream-only.

## Commands

```bash
./build.sh                                   # full build (backend fat JAR + frontend); uses settings.xml
cd event-rule-backend && mvn package -DskipTests
cd event-rule-backend && mvn test -DskipTests=false                       # unit tests only
cd event-rule-backend && mvn test -DskipTests=false -Dtest=ClassNameTest  # single class
cd event-rule-frontend && npm test -- --run                               # vitest (React src/)
cd event-rule-frontend/bff && npm test                                    # jest (BFF; run from bff/)
cd event-rule-frontend && npm run lint
./run.sh up | down | logs | log | status | clean   # Docker stack (Kafka, Flink, BFF, UI on :3010)
./samples/<use-case>/scripts/deploy.sh             # submit a use case's Flink job
make functional-tests                              # needs Docker + built JAR; slow
```

- Surefire excludes `**/functional/**` and `**/*E2ETest.java`; those need `-Pfunctional` / `-Pe2e` and a running stack. Do not run them unless asked.
- JaCoCo enforces coverage thresholds (~0.65 / 0.60) on the backend build.
- Building against DI artifacts may need `-s settings.xml -P maven-designer` (README §8).
- Prefer running a single test class over the whole suite.

## Docs to read instead of code

- `docs/AGENT_PATTERN_DESIGN.md` — authoritative `agents.json` schema, state, context contract, scheduling, enrichment (1.6k lines: `grep -n "^#"` for the TOC, then read a range).
- `docs/ADVANCED_PATTERNS.md` — chaining, multi-input, worked examples.
- `docs/RULE_ENGINE.md` — DI class hierarchy and `RuleEngineRunner`.
- `docs/RUN_DATASTREAM.md`, `docs/RUN_PTF.md` — local runs without Docker.
- `README.md` — long (1.2k lines). Use its TOC: §4 architecture, §5 backends, §8 build/test, §15 troubleshooting. Its §14 project structure is outdated; use the maps above.
- `TUTORIAL.md` — building a new use case; `DOCKER.md` — compose reference; `event-rule-frontend/bff/README.md` — BFF API.
- `samples/PROMPT.md`, `samples/Event Rule Application Template.md` — templates for generating a new use case.

## Where to change what

| Task | Start here |
|---|---|
| New/changed descriptor field | `agent/AgentDescriptor.java` + relevant `*Config`, `AgentDescriptorValidator`, then both backends, `AgentWizard.jsx`, docs |
| Per-event processing behaviour | `StatefulAgentProcessor.processElement` and `AgentEventProcessor` |
| Rule engine / archive loading | `rule/RuleEngineRunner.java`, `rule/broadcast/` |
| Enrichment | `agent/enrichment/` (+ `enrichment/sql/`) |
| REST endpoint for UI | `bff/server.js` (find via `grep -n "app\.\(get\|post\|put\|patch\|delete\)"`) |
| Dashboard UI | `event-rule-frontend/src/*.jsx`; event cards registered in `eventCardRegistry.jsx` |
| Agent diagram editor | `AgentDiagram*.jsx`, `agentDiagramDescriptor.js`, `useDiagramHistory.js` |
| SQL pipeline view | `sqlGraph.js`, `SqlGraphView.jsx`, `SqlViewModal.jsx` |
| Descriptor wizard | `AgentWizard.jsx` (+ `.test.jsx`) |
| New sample use case | copy a `samples/*` dir; follow `TUTORIAL.md` |

## Token-saving rules

1. **Never read or list** these (generated, binary, or huge): `node_modules/`, `target/`, `build/`, `.git/`, `*.jar`, `package-lock.json`, `samples/*/di-project/`, `event-rule-backend/${sys:log.file}*`, `checkpoints/`, `savepoints/`, `functional-tests/reports/`.
2. Do not run `list_files` recursively at the repo root (tens of thousands of entries because of `.git` and `node_modules`). List one subdirectory at a time, or use `glob` with a narrow pattern.
3. For big files (`bff/server.js`, `StatefulAgentProcessor.java`, `AgentBasedDataStreamJob.java`, `README.md`, `AGENT_PATTERN_DESIGN.md`) use symbol tools or `grep -n` first, then read a line range.
4. Java: use `GetSymbolsOverview` / `FindSymbol` rather than reading whole classes. Tests mirror the main package path (`FooBar` → `src/test/.../FooBarTest.java`).
5. Sample `agents.json` files are the best real examples of the descriptor; read one (e.g. `samples/banking-fraud-agent/agents.json`) instead of inferring the schema from code.
6. Run one test class, not the whole suite; use `-q` on Maven and `--run` on vitest to cut output.
7. Don't touch `.env.local` (local secrets/config); `.env.example` documents available variables.

## Conventions

- Source files carry the header `Copyright IBM Corp. 2026` (Apache 2.0); keep it on new files.
- Java 21; Jackson versions are pinned in the root `pom.xml` `dependencyManagement`; add shared versions there.
- Frontend tests: Vitest + Testing Library, co-located with sources. BFF tests: Jest, `bff/test/` and `bff/routes/*.test.js`.
- Keep changes minimal; update the matching doc in `docs/` when changing descriptor semantics.
