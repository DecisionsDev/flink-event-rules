# Retail Event Application

## Introduction

Event Rules Application for real-time promotion in retail. Track Orders to generate Promotions based on order patterns.

## Application

Application Name: Retail - Real-time Promotion

List of inbound topics:
* `agent-order-events`, receiving events of type `Order`
* `agent-webclick-events`, receiving events of type `WebClick`
* `agent-customer-init-events`, receiving events of type `CustomerInitializationEvent`

List of outbound topics:
* `agent-promotions`, sending events of type `Promotion`
* `agent-promotion-errors`, sending events of type `agent-error`
* `agent-customer-entity-changelog`, sending events of type `entity-changelog`

consumerGroupId: `event-rule-promotion-agent`

## Agents

This section describes the Agents of the application. Each section below describes an agent.

### Customer Agent

#### Specification

`Customer Agent` is an agent related to a `Customer` subscribing to:
   * `CustomerInitializationEvent` from `agent-customer-init-events`, processed by `InitializeCustomer` operation
   * `Order` from `agent-order-events`, keeping 100 occurences for 3 days and processed by `ProcessOrder` operation

By default, the entity is extracted from events through the `customer` field. 

#### Ruleset

The processing ruleset is `PromotionService_1.0.0.jar`, defining the following operations:

* `InitializeCustomer` taking as input:
   * `customer` as the entity,
   * `customerInitializationEvent` as the event

* `Order` taking as input:
   * `customer` as the entity,
   * `order` as the event,
   * history of `Order` as `pastOrders`

  generating:
   * a collection of `Promotion` as `promotions`

The ruleset control topic is `agent-promotion-archive-control`.

Entity changes are published to `agent-customer-entity-changelog`.

Agent Errors are published to `agent-promotion-errors`.

#### Output

The agent generates the following output:
* `Promotion` events are sent to `agent-promotion-events`
* `agent-error` events are sent to `agent-promotion-errors` (processing errors for investigation)

#### Configuration

The state is stored on `rocksdb` with a time-to-live of 5 days.

