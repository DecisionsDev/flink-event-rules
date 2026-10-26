# Event Rule Application creation prompt

To develop a new Event Rule Application, create a document that describes the application as a MD file, following the template `Event Rule Application Template.md`. 

Then use a prompt like this to get your AI to generate the corresponding `agents.json` file (agents plus use-case properties such as `name`, `description`, `topics`).

Prompt:
```
From <<Event Rule Application.md>> file, generate an agents JSON file in samples/<DIRECTORY>.

Use samples/banking-fraud-agent/agents.json and samples/airtraffic-agent/agents.json as example of the file that needs to be generated.
```


