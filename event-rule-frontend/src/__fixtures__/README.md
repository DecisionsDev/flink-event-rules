Real output of `AgentBasedPTFJob` with `PTF_PRINT_SQL=json` (`PTF_DIALECT` unset), used to test
`sqlGraph.js` against what the backend actually emits:

    PTF_PRINT_SQL=json CHAINING_ENABLED=true|false java -Dconsole.log.level=OFF \
      -cp event-rule-backend/target/event-rule-backend-1.0-SNAPSHOT.jar \
      com.ibm.di.event.agent.ptf.AgentBasedPTFJob localhost:9092 samples/<id>/agents.json

`ptf-airtraffic-chained.json` has `CHAINING_ENABLED=true` (agents connected inside the job),
`ptf-airtraffic-unchained.json` has it `false` (agents connected through a Kafka topic).
Regenerate them if the SQL generator changes.
