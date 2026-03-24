# sarama-easy

A lightweight Go wrapper around [IBM/sarama](https://github.com/IBM/sarama) for Apache Kafka. Provides simplified APIs for async producers and consumers (including auto-rebalancing consumer groups) with built-in Avro serialization via Confluent Schema Registry.

## Features

- **Async Producer** with context-based graceful shutdown
- **Consumer Groups** with auto-rebalancing and configurable strategies
- **Avro Support** using Confluent wire format and Schema Registry
- **Schema Registry Client** with thread-safe caching
- **TLS/mTLS** and **SASL SCRAM** (SHA-256/SHA-512) authentication
- **Environment-based configuration** via `envconfig`

## Installation

```bash
go get github.com/Cuebiq/sarama-easy
```

## Quick Start

### Producer

```go
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

conf := kafka.NewKafkaConfig()
conf.Brokers = "localhost:9092"

logger := log.New(os.Stdout, "[producer] ", log.LstdFlags)

producer, err := kafka.NewProducer(ctx, conf, logger)
if err != nil {
    log.Fatal(err)
}

// Start the background process and error consumer
processor, errors := producer.Background()
go processor()
go func() {
    for err := range errors {
        log.Printf("error: %s", err)
    }
}()

// Send messages
producer.Send(kafka.ProducerMessage{
    Topic: "my-topic",
    Key:   []byte("key"),
    Value: []byte("value"),
})

// Trigger graceful shutdown
cancel()
```

### Consumer

```go
ctx, cancel := context.WithCancel(context.Background())

handler := &myHandler{cancel: cancel}

consumer, err := kafka.NewConsumer(ctx, conf, handler, logger)
if err != nil {
    log.Fatal(err)
}

processor, errors := consumer.Background()
go processor()

for err := range errors {
    log.Printf("error: %s", err)
}
```

### Handler

Implement the `Handler` interface to process incoming messages:

```go
type myHandler struct {
    cancel context.CancelFunc
}

func (h *myHandler) Handle(msg *kafka.Message) error {
    fmt.Printf("topic=%s key=%s value=%s\n", msg.Topic, msg.Key, msg.Value)
    return nil // return error to trigger consumer shutdown
}
```

## Configuration

All configuration fields can be set via environment variables, code, or CLI flags.

| Field | Env Variable | Default | Description |
|-------|-------------|---------|-------------|
| `Brokers` | `KAFKA_BROKERS` | `localhost:9092` | CSV list of Kafka broker addresses |
| `Version` | `KAFKA_VERSION` | `1.1.0` | Kafka broker version for API compatibility |
| `ClientID` | `KAFKA_CLIENT_ID` | `sarama-easy` | Client identifier sent to brokers |
| `Topics` | `KAFKA_TOPICS` | | CSV list of topics to consume |
| `Group` | `KAFKA_GROUP` | `default-group` | Consumer group ID |
| `RebalanceStrategy` | `KAFKA_REBALANCE_STRATEGY` | `roundrobin` | `roundrobin` or `range` |
| `RebalanceTimeout` | `KAFKA_REBALANCE_TIMEOUT` | `1m` | Consumer rebalance timeout |
| `InitOffsets` | `KAFKA_INIT_OFFSETS` | `latest` | `latest` or `earliest` |
| `CommitInterval` | `KAFKA_COMMIT_INTERVAL` | `10s` | Auto-commit interval |
| `FlushInterval` | `KAFKA_FLUSH_INTERVAL` | `1s` | Producer flush interval |
| `IsolationLevel` | `KAFKA_ISOLATION_LEVEL` | `ReadUncommitted` | `ReadUncommitted` or `ReadCommitted` |
| `Verbose` | `KAFKA_VERBOSE` | `false` | Enable Sarama internal logging |

### TLS

| Field | Env Variable | Description |
|-------|-------------|-------------|
| `TLSEnabled` | `KAFKA_TLS_ENABLED` | Enable TLS |
| `TLSKey` | `KAFKA_TLS_KEY` | Path to client private key |
| `TLSCert` | `KAFKA_TLS_CERT` | Path to client certificate |
| `CACerts` | `KAFKA_CA_CERTS` | Path to CA certificate bundle |

### SASL

| Field | Env Variable | Description |
|-------|-------------|-------------|
| `SaslEnabled` | `KAFKA_SASL_ENABLED` | Enable SASL authentication |
| `SaslMechanism` | `KAFKA_SASL_MECHANISM` | `SCRAM-SHA-256` or `SCRAM-SHA-512` |
| `Username` | `KAFKA_USERNAME` | SASL username |
| `Password` | `KAFKA_PASSWORD` | SASL password |

### Schema Registry

| Field | Env Variable | Description |
|-------|-------------|-------------|
| `SchemaRegistryServers` | `KAFKA_SCHEMA_REGISTRY_SERVERS` | CSV list of Schema Registry URLs |

## Avro Support

### Producing Avro Messages

```go
codec, err := goavro.NewCodec(avroSchema)
schemaID, err := producer.GetSchemaId("my-topic", codec)

native, err := codec.BinaryFromNative(nil, record)
msg := &sarama.ProducerMessage{
    Topic: "my-topic",
    Key:   sarama.StringEncoder("key"),
    Value: &kafka.AvroEncoder{SchemaID: schemaID, Content: native},
}
producer.SendAvroMsg(msg)
```

### Consuming Avro Messages

Avro decoding is handled automatically by the consumer. The `Message.Value` field contains the JSON-encoded Avro data, and `Message.SchemaId` holds the schema ID from the registry.

## Running the Examples

```bash
# Start Kafka and Schema Registry
docker compose up -d

# Wait for services to be healthy, then:
go build -o bin/ ./...

# In one terminal:
bin/consumer-example -brokers localhost:9092 -topic mytopic -version 3.5.0

# In another terminal:
bin/producer-example -brokers localhost:9092 -topic mytopic -version 3.5.0

# Clean up
docker compose down -v
```

The producer sends 10 messages and a sentinel ("poison pill"). The consumer receives all messages and shuts down when it sees the sentinel.

## Architecture

```
┌─────────────────────────────────────────────┐
│                  Application                 │
│                                              │
│  ┌──────────┐  Handler   ┌──────────────┐   │
│  │ Consumer ├───────────►│ Your Handler │   │
│  └────┬─────┘            └──────────────┘   │
│       │                                      │
│  ┌────┴─────┐  Send()   ┌──────────────┐   │
│  │ Producer ├───────────►│ Kafka Broker │   │
│  └────┬─────┘            └──────────────┘   │
│       │                                      │
│  ┌────┴─────────────────┐                   │
│  │ CachedSchemaRegistry │◄──► Schema Reg.   │
│  └──────────────────────┘                   │
└─────────────────────────────────────────────┘
```

## License

Apache License 2.0 - see [LICENSE](LICENSE) for details.
