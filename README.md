# AgentMQ: An agent based distributed network

AgentMQ is an agent-based, distributed network framework on top of MQTT. It has several features as follows:
+ It is designed to be modulable so that users can add components at runtime.
+ Each component functions as an individual agent that performs certain task
+ Each component can run on different machine, hence distributed

### Developing Environment
+ Python 3.2+
+ paho-mqtt
+ MQTT broker

### MQTT Topic Mapping
By design most topic used in the system has 5 levels, `DB_Tag,/Query/AGENT_NAME/API_KEY/QUERY_ID`. For instance, `SQL/Query/Request/test_api_key/1234` is a legal topic string. 
The system uses request id internally, which consists of api key and query id. 

### Available Agents:
+ QueryAgent: base class for all database query agent. If you want to developer your own database agent, you can inherits your class from QueryAgent and override `_query_data` method.
+ SimpleSQLAgent: a simple SQLite agent. You can check its implementation about how to write your own query agent.
+ WindowAgent: an agent listening to stream data and batch the result on demand. If the stream query contains compute unit, it will forward the raw data to compute agent.
+ ComputeAgent: a agent processing batched data based on given compute unit
+ LoggingAgent: a logging agent keeping track on internal traffic and log message.
