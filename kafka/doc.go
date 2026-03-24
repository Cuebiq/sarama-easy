// Package kafka provides a lightweight wrapper around IBM/sarama for building
// Apache Kafka producers and consumers in Go.
//
// It simplifies common patterns such as async producing, consumer group
// management with auto-rebalancing, and Avro serialization via Confluent
// Schema Registry.
//
// # Quick Start
//
// Create a producer:
//
//	ctx, cancel := context.WithCancel(context.Background())
//	defer cancel()
//
//	conf := kafka.NewKafkaConfig()
//	conf.Brokers = "localhost:9092"
//
//	producer, err := kafka.NewProducer(ctx, conf, logger)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	processor, errors := producer.Background()
//	go processor()
//
//	producer.Send(kafka.ProducerMessage{
//	    Topic: "my-topic",
//	    Key:   []byte("key"),
//	    Value: []byte("value"),
//	})
//
// Create a consumer:
//
//	consumer, err := kafka.NewConsumer(ctx, conf, myHandler, logger)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	processor, errors := consumer.Background()
//	go processor()
//
//	for err := range errors {
//	    log.Printf("error: %s", err)
//	}
//
// # Configuration
//
// Config can be populated from environment variables using FromEnv(),
// from code using NewKafkaConfig() with manual field assignment, or
// via CLI flags. See the Config type for available fields and their
// corresponding environment variable names.
//
// # Avro Support
//
// Both producer and consumer support Avro serialization using the Confluent
// wire format (magic byte + 4-byte schema ID + binary Avro data). The
// CachedSchemaRegistryClient provides a thread-safe, caching client for
// Confluent Schema Registry.
//
// # Shutdown
//
// Cancel the context passed to NewProducer or NewConsumer to initiate
// graceful shutdown. The error channel returned by Background() will be
// closed once shutdown is complete.
package kafka
