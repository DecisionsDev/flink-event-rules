# Air Traffic Control Agent

This sample demonstrates **cross-domain agent chaining** in an Air Traffic Control scenario:
situation detection on aircraft and airports, with the Aircraft Agent feeding the Airport Agent
directly in-process — no Kafka hop between them.

## Table of Contents

1. [Overview](#1-overview)
2. [Agent Chaining Architecture](#2-agent-chaining-architecture)
3. [Data Model](#3-data-model)
4. [Agents](#4-agents)
5. [Demo Scenarios](#5-demo-scenarios)
6. [State Configuration](#6-state-configuration)
7. [Troubleshooting](#7-troubleshooting)

---

## 1. Overview

Monitoring air traffic and identifying situations where flight safety might be compromised is a
critical activity performed by national and international Air Traffic Control agencies. In this
sample, rule-based event-processing agents monitor inbound and outbound flights to detect
situations worth proper handling by Air Traffic Control staff.

Example detections:

- **Touch-and-go** — an aircraft cancels a landing and takes off again to ensure a safe final
  approach.
- **Simultaneous take-off** — two aircraft taking off simultaneously on parallel runways, which
  may require extra attention on the tarmac.

---

## 2. Agent Chaining Architecture

This sample is the canonical example of **cross-domain agent chaining**: two agents manage
different entity types (Aircraft vs. Airport) with different entity key spaces
(`$.aircraftId` vs. `$.airportCode`).

```
[agent-aircraft-init-events] ──► Aircraft Agent (keyed by $.aircraftId)
[agent-aircraft-events]      ──►      │
                                      │  ProcessAircraftEvent
                                      │  emits TrackedAircraftEvent
                                      │  (payload contains $.airportCode)
                                      │
                             ┌────────┴──────────────────────────────────────────┐
                             │  filter(eventType == TrackedAircraftEvent)        │
                             │  map(re-key entityId → $.airportCode,             │  in-process
                             │      set sourceTopic =                            │  no Kafka hop
                             │        agent-tracked-aircraft-events)             │
                             └────────────────┬─────────────────────────────────┘
                                              │
[agent-airport-init-events] ──► Airport Agent (keyed by $.airportCode)
[agent-airport-events]      ──►      │  (Kafka sources + chain stream unioned)
                                     │
                                     │  ProcessTrackedAircraftEvent
                                     │  (full airport entity state + 1h history)
                                     ▼
                                [agent-atc-alerts]
```

The chaining layer automatically handles three correctness concerns:

1. **Entity-key re-extraction** — the `TrackedAircraftEvent` envelope produced by the Aircraft
   Agent carries `entityId = aircraftId`. Before reaching the Airport Agent, a `map()` step
   re-extracts `$.airportCode` from the payload and sets it as the new `entityId`. This ensures
   Flink's `keyBy(entityId)` routes the event to the correct airport partition, not the aircraft
   partition.

2. **`sourceTopic` correction** — the `sourceTopic` field is set to
   `agent-tracked-aircraft-events` so the Airport Agent's operation resolver and event-history
   updater find the right subscription (`ProcessTrackedAircraftEvent`).

3. **Kafka source suppression** — the Airport Agent's Kafka consumer for
   `agent-tracked-aircraft-events` is **disabled** when the chain is active, preventing each
   `TrackedAircraftEvent` from being processed twice (once via the in-memory chain, once after
   the Kafka round-trip).

The `agent-tracked-aircraft-events` topic still exists as a sink for the Aircraft Agent.
External consumers (monitoring, replay, audit) can still read from it.

### Shared archive-control topic

Both agents share the same `archiveControlTopic` (`agent-atc-archive-control`) because they use
the same JAR (`AirTrafficControlService`). A single `ArchiveControlCmd` message therefore reloads
**both agents simultaneously**. If the agents ever diverge to use different JARs, assign each
agent its own archive-control topic to prevent cross-agent reload interference.

---

## 3. Data Model

### Airport

| Field         | Type     | Description                                |
|---------------|----------|--------------------------------------------|
| `airportCode` | string   | IATA airport code (entity key)             |
| `airportName` | string   | Full name                                  |
| `country`     | string   | Country                                    |
| `location`    | Position | Latitude, longitude, elevation (m)         |
| `status`      | enum     | `OPEN`, `CLOSED`, `CLOSING`, `UNDER_STORM` |

### Aircraft

| Field                | Type     | Description                                                    |
|----------------------|----------|----------------------------------------------------------------|
| `aircraftId`         | string   | Aircraft registration (entity key)                             |
| `flightNumber`       | string   | Current flight number                                          |
| `originAirport`      | string   | Airport code of departure                                      |
| `destinationAirport` | string   | Airport code of destination                                    |
| `position`           | Position | Last known position                                            |
| `status`             | enum     | `TAKING_OFF`, `ASCENDING`, `IN_FLIGHT`, `DESCENDING`, `LANDING`, `PARKED`, `TAXIING`, `AT_GATE` |
| `groundSpeed`        | number   | km/h                                                           |
| `trackingAirport`    | string   | Airport code currently tracking this aircraft                  |

### Events

| Event                          | Topic                           | Entity key field                    |
|--------------------------------|---------------------------------|-------------------------------------|
| `AircraftInitializationEvent`  | `agent-aircraft-init-events`    | `aircraftId`                        |
| `AircraftEvent`                | `agent-aircraft-events`         | `aircraftId`                        |
| `AirportInitializationEvent`   | `agent-airport-init-events`     | `airportCode`                       |
| `AirportEvent`                 | `agent-airport-events`          | `airportCode`                       |
| `TrackedAircraftEvent`         | `agent-tracked-aircraft-events` | `airportCode` (downstream key after chain re-keying) |
| `Alert`                        | `agent-atc-alerts`              | —                                   |

**`TrackedAircraftEvent` payload fields:**

| Field          | Type     | Description                              |
|----------------|----------|------------------------------------------|
| `timestamp`    | datetime |                                          |
| `airportCode`  | string   | Airport currently tracking this aircraft |
| `aircraftId`   | string   |                                          |
| `flightNumber` | string   |                                          |
| `position`     | Position | Latitude, longitude, elevation           |
| `groundSpeed`  | number   | km/h                                     |
| `status`       | enum     | Aircraft status at time of event         |

**`Alert` payload fields:**

| Field          | Type     | Description                        |
|----------------|----------|------------------------------------|
| `timestamp`    | datetime |                                    |
| `aircraftId`   | string   |                                    |
| `flightNumber` | string   |                                    |
| `airportCode`  | string   |                                    |
| `position`     | Position |                                    |
| `message`      | string   | Human-readable description         |
| `severity`     | enum     | `LOW`, `MEDIUM`, `HIGH`, `CRITICAL`|

---

## 4. Agents

### Aircraft Agent

**Entity:** Aircraft — keyed by `$.aircraftId`

| Subscription | Topic | Operation | Horizon |
|---|---|---|---|
| `AircraftInitializationEvent` | `agent-aircraft-init-events`  | `InitializeAircraft`   | none          |
| `AircraftEvent`               | `agent-aircraft-events`       | `ProcessAircraftEvent` | time: last 2h |

| Output | Topic | Description |
|---|---|---|
| `TrackedAircraftEvent` | `agent-tracked-aircraft-events` | Chained to Airport Agent; also available on Kafka |
| `Alert`                | `agent-atc-alerts`              | Touch-and-go and other aircraft-level or airport-level alerts      |
| `agent-error`          | `agent-atc-errors`              | Processing errors                                 |

#### Aircraft status logic (`ProcessAircraftEvent`)

| Condition | New status | Side effect |
|---|---|---|
| Event type `TAXIING` | `TAXIING` | |
| Event type `AT_GATE` | `AT_GATE` | |
| Event type `IN_FLIGHT` | `IN_FLIGHT` | |
| Altitude rising | `ASCENDING` | |
| Altitude rising AND distance from origin < 1 000 m | `TAKING_OFF` | `trackingAirport` = origin |
| Altitude falling | `DESCENDING` | |
| Altitude falling AND distance from destination < 1 000 m | `LANDING` | `trackingAirport` = destination |
| Status was `LANDING` AND altitude equals destination airport elevation | `GROUNDED` | |
| `trackingAirport` is set | — | Emits `TrackedAircraftEvent` |

#### Touch-and-go detection

When status is `LANDING` or `GROUNDED`, altitude starts rising, and the time since the last
event is less than 2 minutes → emits an `Alert` with type `TOUCH_AND_GO`.

---

### Airport Agent

**Entity:** Airport — keyed by `$.airportCode`

| Subscription | Topic | Operation | Horizon |
|---|---|---|---|
| `AirportInitializationEvent` | `agent-airport-init-events`     | `InitializeAirport`           | none          |
| `AirportEvent`               | `agent-airport-events`          | `ProcessAirportEvent`         | none          |
| `TrackedAircraftEvent`       | `agent-tracked-aircraft-events` | `ProcessTrackedAircraftEvent` | time: last 1h |

> **Note:** The `TrackedAircraftEvent` Kafka consumer is **suppressed** when agent chaining is
> active (the default). Events arrive via the Aircraft Agent → Airport Agent chain instead.

| Output | Topic | Description |
|---|---|---|
| `Alert`       | `agent-atc-alerts` | Airport-level and aircraft-proximity alerts |
| `agent-error` | `agent-atc-errors` | Processing errors                           |

#### Simultaneous take-off detection (`ProcessTrackedAircraftEvent`)

When a `TrackedAircraftEvent` arrives with status `TAKING_OFF` and there is at least one other
`TrackedAircraftEvent` with status `TAKING_OFF` within the past 30 seconds whose runway is
parallel to the current runway → emits an `Alert` with type `SIMULTANEOUS_TAKING_OFF`.

---

## 5. Demo Scenarios

The file [`demo-scenarios.json`](demo-scenarios.json) contains six ready-to-run scenarios:

| ID | Label | What it demonstrates |
|----|-------|----------------------|
| `airport-creation` | Airport creation from event | Single `AirportInitializationEvent` |
| `aircraft-creation` | Aircraft creation from event | Single `AircraftInitializationEvent` |
| `several-aircraft-creation` | Multiple Aircraft creation | Four aircraft initialised at once |
| `several-airport-creation` | Multiple Airport creation | Four airports initialised at once |
| `agent-chaining-aircraft-to-airport` | **Agent chaining: aircraft position update triggers airport processing** | Full end-to-end chain: init airport + aircraft, send `AircraftEvent` → aircraft-agent emits `TrackedAircraftEvent` → chains directly to airport-agent (no Kafka hop). Primary regression check for entity-key re-keying and Kafka source suppression. |
| `agent-chaining-multi-aircraft-airport-view` | **Agent chaining: multiple aircraft tracked at one airport** | Two airports (CDG, NCE) + three aircraft. Position updates route `TrackedAircraftEvents` to the correct airport via `$.airportCode` re-keying. Verifies each airport accumulates independent state. |

---

## 6. State Configuration

Both agents persist entity state in RocksDB and publish entity-state change events to a
dedicated Kafka topic via `stateConfig.changeLogTopic`:

| Agent          | Entity type | State TTL | Changelog topic                    |
|----------------|-------------|-----------|-------------------------------------|
| Aircraft Agent | `Aircraft`  | 4 hours   | `agent-aircraft-entity-changelog`  |
| Airport Agent  | `Airport`   | 24 hours  | `agent-airport-entity-changelog`   |

Whenever a rule execution updates the entity (aircraft or airport), Flink emits a side-output
record to the changelog topic with the form:

```json
{
  "entityId":   "<aircraftId or airportCode>",
  "entityType": "<Aircraft or Airport>",
  "timestamp":  1750000000000,
  "entity":     { ... }
}
```

The Kafka record key is the entity ID (UTF-8). This allows downstream consumers to track live
aircraft and airport state without reading Flink checkpoints.

See [`../../docs/AGENT_PATTERN_DESIGN.md`](../../docs/AGENT_PATTERN_DESIGN.md#9-agent-descriptor-field-reference)
for the full `stateConfig` field reference.

---

## 7. Troubleshooting

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| `Operation 'ProcessAircraftEvent' not found in archive` | The JAR's `META-INF/services/com.ibm.decision.run.DecisionServiceProvider` file does not include the `ProcessAircraftEvent` flow's provider class | Rebuild `AirTrafficControlService` to include all required decision flows and redeploy the JAR |
| `TrackedAircraftEvent` never reaches Airport Agent | `ProcessAircraftEvent` fails (see above), or `trackingAirport` is never set in the aircraft entity | Ensure the `AircraftEvent` payload includes `airportCode` so the ruleset can populate `trackingAirport` and emit `TrackedAircraftEvent` |
| Airport Agent receives events but state is always empty | Entity-key mismatch: the envelope's `entityId` is the aircraft ID, not the airport code | Verify that both `aircraft-agent` and `airport-agent` declare `"chainingEnabled": true` in the descriptor, and that `airport-agent` declares `"entityKeyExtractor": "$.airportCode"` |
| Both chain and Kafka deliver the same `TrackedAircraftEvent` (duplicate processing) | Kafka source suppression not active | Check that `resolveTopology()` runs before wiring and that `chainedEventTypes` is passed to `wireAgent()` |
| `pendingCommittables` metric warnings in TaskManager logs | Benign Flink metrics collision on Kafka sink committer re-registration at checkpoint | No action needed — informational only |
