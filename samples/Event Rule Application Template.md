# <<Event Rule Application>>

## Introduction

<<use as the description of the application>>

## Application

Application Name: <<ApplicationName>>

List of inbound topics:
* <<TopicName>>, receiving events of type <<EventType>>

List of outbound topics:
* <<TopicName>>, sending events of type <<EventType>>

(Optional) consumerGroupId: <<consumerGroupId>>

## Agents 

This section describes the Agents of the application. Each section below describes an agent. 

### <<Agent>>

#### Specification

<<Agent>> is an agent related to a <<EntityType>> subscribing to:
   * <<EventType>> from <<TopicName>>, <<horizon-specification>> and processed by <<OperationName>> operation

```
horizon-specification:
| keeping them for <<duration>>
| keeping <<n>> occurences
| keeping <<n>> occurences for <<duration>>
```
(Optional) By default, the entity is extracted from events through the <<EntityField>> field. 

#### Ruleset

The processing ruleset is <<Ruleset JAR File>>, defining the following operations:
* <<OperationName>> taking as input: 
   * <<EntityField>> as the entity, 
   * <<EventField>> as the event, 
   * history of <<EventType>> as <<EventHistoryField>> (1)
generating: (2)
   * a collection of <<EventType>> as <<OutboundEventsField>> (1)

Note (1): zero or more 
Note (2): optional

The ruleset control topic is <<ArchiveControlTopic>>.

Entity changes are published to <<ChangeLogTopic>>.

Agent Errors are published to <<ErrorTopic>>.

#### Output

The application generates the following output:
* <<EventType>> events are sent to <<OutboundTopicName>>

#### Configuration

The state is stored on <<Store (1)>> with a time-to-live of <<duration>>

Note (1): `rocksdb` by default.




