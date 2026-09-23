# Event Rule Application creation prompt

To develop a new Event Rule Application, create a document that describes the application as a MD file, following the template `Event Rule Application Template.md`. 

Then use a prompt like this to get your AI to generate the corresponding `descriptor.json` file and the `agents.json` file.

Prompt:
```
From <<Event Rule Application.md>> file, generate an event rule descriptor JSON file and an agents JSON file in samples/<DIRECTORY>. 

Use samples/banking-fraud-agent/descriptor.json and  samples/banking-fraud-agent/agents.json as example of the files that need to be generated.
```


