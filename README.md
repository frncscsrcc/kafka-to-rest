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

**Config Example:**

```
{
    "group": "my-group",
    "proxies": [
        {
            "name": "First Proxy",
            "consumer": {
                "host":   "localhost",
                "port":   9092,
                "topics": ["topic1", "topic2", "topic3"],
                "dlq":    "proxy1.dlq"
            },
            "api-caller": {
                "auth": {
                    "stored-token-filename": "token.txt"
                },
                "protocol": "https",
                "host": "webhookme.zhitty.com",
                "port": 443,
                "path": "send/blablabla",
                "retry-delay": 1,
                "retry-max": 5
            },
            "retry-on-status": ["5**", "6**", "40*"],
            "commit-on-status": ["2**"]
        }
    ]
}
```

**Compile and run:**

```
go build -o kafka-to-rest main.go

./kafka-to-rest start --config config.json
```