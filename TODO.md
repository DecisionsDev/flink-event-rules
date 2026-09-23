# TODO on Event Rule

Here the list of things remaining †o do for this application:

## Unit tests

Restore unit tests: let's make sure we only have test that don't depend on Kafka/Flink and that can be invoked during `mvn clean install`

If we want, we can also implement an E2E test running on top of a deployed (in Docker) Event Rule application that validates the Banking Fraud Agent use-case run end-to-end (assuming the application has been built, the Docker topology runs, assert that we can deploy the samples, send events, verify we got back the expected events). But we should not run it during build. 

## Documentation

Review the documentation. 

I suggest to have one README at the top of the project with the following sections:
   * Introduction
   * Table of Content
   * Prerequisites
   * Quick Start (based on the Banking Fraud Agent use-case)
   * Architecture Overview
   * How Event Rule Works (pointing to docs/AGENT_PATTERN_DESIGN.md which is the reference of descriptor.json and agents.json. Can also point to a MD file documenting the Advanced Patterns)
   * Building and Testing (how to build/test the application)
   * Run in Docker, point to a DOCKER.md file with more information about building/running Event Rule in Docker
   * Run locally (optional, we don't have to do it now)
   * Use Cases (point to a README.md in Samples. It itself point to the README.md in each use-case located in `samples`)

I suggest we have a TUTORIAL.md which is a step-by-step tutorial to build a new use-case

We also need to document what the user have to do with DI CMS:
- create a Decision Service and operations, respecting the expected signature
- Share/Version/Deploy
- get the Decision Service Archive ID in the deployment report 
- use DI CMS Swagger API to retrieve the Decision Service Archive and put it in the location specified in the Agent Descriptor
(or use a script/curl to do that. In that case, we need to explain how the user creates an API key to be used with the REST API or point to the documentation)

## Retail Agent use-case

We need to reimplement the Retail use-case on the new foundation

## Banking Fraud Agent use-case

Beef-up the event processing logic with more pattern of fraud, including some velocity checks, correlation between transaction and events on the account....

## Code

* See TODO in the code. 
* when building the DAG we union all data stream from event sources. The problem is that watermark does not work on the union. So the ordering of events from 2 different sources is random (even if a Transaction event is sent after an Account Event, it can be processed before). Figure out if it is the right way to do. 
* Figure out how it should be packaged to be public (on public github)
   * assumption is that the user has DI CMS installed on his machine
   * adjust POM...
   * See https://localhost:3000/ads/download to access the Jar, and the doc: https://www.ibm.com/docs/en/cloud-paks/cp-biz-automation/26.0.0?topic=api-application-packaging

* We need to figure out if we can:
- export a Decision Service from DI SaaS
- used the DI CMS command line to build the DSA
- use the generated DSA in our Flink Agent

* [X] The current version does not support more than one output for an operation. We need to be able to send several type of outbound events. 

* Review how agent chaining works. If an inbound event is partitionned using a key, is processed by one Decision Operation and emit a new event, how this outbound event is partitionned in Flink? It only works to chain agents based on the same partition, so related to the same entity. We might need to revisit the concept of chained agents. 

* the chaining of agent is not working (uid for Agent Processor is not unique when 2 agents are chained together). Need to fix.

* We need to figure out if we can deploy the Job without having access to the Ruleset jar and explicitly deploy it (through the management topic) when the job is running. 

* We need to investigate how Confluent Flink SaaS offering is working and what are the APIs to submit a Job.

## Code documentation

Let's clean up the documentation in the code. A lot is obsolete. Let's point to the Documentation (in `docs` folder) instead of writting down the spec in Javadoc. 

## Build

[INFO] No artifact matching filter com.ibm.aid.engine:decisionJar

==========================================
Building Backend (Apache Flink)
==========================================
WARNING: A restricted method in java.lang.System has been called
WARNING: java.lang.System::load has been called by org.fusesource.jansi.internal.JansiLoader in an unnamed module (file:/Users/smery/Dev/install/maven/lib/jansi-2.4.0.jar)
WARNING: Use --enable-native-access=ALL-UNNAMED to avoid a warning for callers in this module
WARNING: Restricted methods will be blocked in a future release unless native access is enabled



## UI

Let's make sure the UI is aligned to the new code and works. Let's remove anything which is use-case specific in the code of the UI. If we want things to be nicer, we could base them on naming conventions or things we're putting in the `agents.json`.

* [X] For a scenario execution, display the created or modified entity if any
* Provide a way to clear the view of a topic (not emptying it, just clear the view). Add a way to re-initialize it from the Kafka topic
* Generate Sample Event is misleading as it generate an example payload for an operation and call the Decision Runtime. If we want to keep the feature we need to move it elsewhere, in a separate section, and have an operation selector.
* New scenario creation. It's quite hard to know what to put into the payload. We have all information to generate examples of events that can be put in the payload. For instance, we could generate an example of AccountEvent and an example of Transaction when we are on the Banking Fraud Agent use-case and put them in the array of events. It would allow the user to copy/paste and describe the events he wants to send as part of the scenario
* Review the New Agent wizard
   * may be we should specify the ruleset before the subsription. When specifying the description we could select an operation that has been specified 
   * Choose jar: can we pick up a file from anywhere and it will be copied in the agent directory?
   * in last step, what means Upload Jar?

## Potential use-cases

See https://simon-aubury.medium.com/when-plans-change-at-500-feet-complex-event-processing-of-ads-b-aviation-data-with-apache-flink-52b6381d6a20 and code here: https://github.com/saubury/plane_track

------
# Ongoing tasks

* [ ] **Code**
* [X] **Unit tests**
* [/] **Documentation**
* [/] **Code documentation**
* [ ] **Retail Agent use case**
* [X] **Banking Fraud Agent use case**
* [/] **UI**



   


