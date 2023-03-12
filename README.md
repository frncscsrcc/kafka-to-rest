Kafka-To-Rest
---


It forwards (async) Kafka messages to (sync) REST API.


**Currently implemented:**
- Concurrent implementation
- Configuration based
- It is possible to define multiple proxies
- Each proxy listens to one or more kafka topics
- Each proxy POSTs to a specific REST API endpoint
- Configurable retry mechanism based on the returned API status code
- Configurable DLQ behaviour
- Authorisation via shared token (read from the file system) or static token saved in the configuration
- Graceful shutdown


**Missing:**
- Tests
- Decent logging