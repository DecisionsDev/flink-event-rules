# Air Traffic Control

## Introduction

Event Rules Application for Air Traffic Control. Track aircraft and airport events to detect situations that impact flight safety.

## Application

Application Name: Travel — Air Traffic Control

List of inbound topics:
* `agent-aircraft-events`, receiving events of type `AircraftEvent`
* `agent-tracked-aircraft-events`, receiving events of type `TrackedAircraftEvent`
* `agent-aircraft-init-events`, receiving events of type `AircraftInitializationEvent`
* `agent-airport-events`, receiving events of type `AirportEvent`
* `agent-airport-init-events`, receiving events of type `AirportInitializationEvent`

List of outbound topics:
* `agent-atc-alerts`, sending events of type `Alert`
* `agent-tracked-aircraft-events`, sending events of type `TrackedAircraftEvent`
* `agent-atc-errors`, sending events of type `agent-error`
* `agent-aircraft-entity-changelog`, sending events of type `entity-changelog`
* `agent-airport-entity-changelog`, sending events of type `entity-changelog`

consumerGroupId: `event-rule-bff-atc-agent`

## Agents

This section describes the Agents of the application. Each section below describes an agent.

### Aircraft Agent

#### Specification

`Aircraft Agent` is an agent related to an `Aircraft` subscribing to:
   * `AircraftInitializationEvent` from `agent-aircraft-init-events`, processed by `InitializeAircraft` operation
   * `AircraftEvent` from `agent-aircraft-events`, keeping them for 2 hours and processed by `ProcessAircraftEvent` operation

#### Ruleset

The processing ruleset is `AirTrafficControlService_1.2.1.jar`, defining the following operations:

* `InitializeAircraft` taking as input:
   * `aircraft` as the entity,
   * `aircraftInitializationEvent` as the event

* `ProcessAircraftEvent` taking as input:
   * `aircraft` as the entity,
   * `aircraftEvent` as the event,
   * history of `AircraftEvent` as `pastAircraftEvents`

  generating:
   * a collection of `TrackedAircraftEvent` as `trackedAircraftEvents`
   * a collection of `Alert` as `alerts`

The ruleset control topic is `agent-atc-archive-control`.

Entity changes are published to `agent-aircraft-entity-changelog`.

Agent Errors are published to `agent-atc-errors`.

#### Output

The agent generates the following output:
* `TrackedAircraftEvent` events are sent to `agent-tracked-aircraft-events` (aircraft position and status updates forwarded to the relevant airport agent)
* `Alert` events are sent to `agent-atc-alerts` (Air Traffic Control alerts detected for an aircraft)
* `agent-error` events are sent to `agent-atc-errors` (processing errors for investigation)

#### Configuration

The state is stored on `rocksdb` with a time-to-live of 4 hours.

---

### Airport Agent

#### Specification

`Airport Agent` is an agent related to an `Airport` subscribing to:
   * `AirportInitializationEvent` from `agent-airport-init-events`, processed by `InitializeAirport` operation
   * `AirportEvent` from `agent-airport-events`, processed by `ProcessAirportEvent` operation
   * `TrackedAircraftEvent` from `agent-tracked-aircraft-events`, keeping them for 1 hour and processed by `ProcessTrackedAircraftEvent` operation

#### Ruleset

The processing ruleset is `AirTrafficControlService_1.2.1.jar`, defining the following operations:

* `InitializeAirport` taking as input:
   * `airport` as the entity,
   * `airportInitializationEvent` as the event

* `ProcessAirportEvent` taking as input:
   * `airport` as the entity,
   * `airportEvent` as the event,
   * history of `AirportEvent` as `pastAirportEvents`
   * history of `TrackedAircraftEvent` as `pastTrackedAircraftEvents`

  generating:
   * a collection of `Alert` as `alerts`

* `ProcessTrackedAircraftEvent` taking as input:
   * `airport` as the entity,
   * `trackedAircraftEvent` as the event,
   * history of `AirportEvent` as `pastAirportEvents`
   * history of `TrackedAircraftEvent` as `pastTrackedAircraftEvents`

  generating:
   * a collection of `Alert` as `alerts`

The ruleset control topic is `agent-atc-archive-control`.

Entity changes are published to `agent-airport-entity-changelog`.

Agent Errors are published to `agent-atc-errors`.

#### Output

The agent generates the following output:
* `Alert` events are sent to `agent-atc-alerts` (Air Traffic Control alerts detected for an airport or an aircraft)
* `agent-error` events are sent to `agent-atc-errors` (processing errors for investigation)

#### Configuration

The state is stored on `rocksdb` with a time-to-live of 24 hours.
