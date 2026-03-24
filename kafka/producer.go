package kafka

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/IBM/sarama"
	"github.com/linkedin/goavro/v2"
)

// ProducerMessage is a simplified message type for sending data to Kafka.
// At least one of Key or Value must be non-empty.
type ProducerMessage struct {
	Topic string
	Key   []byte
	Value []byte
}

// Producer provides a high-level interface for asynchronously producing
// messages to Kafka.
type Producer interface {
	// Background returns a blocking function and an error channel. The caller
	// should run the function in a goroutine and consume the error channel
	// until it is closed (which signals shutdown is complete).
	Background() (func(), chan error)

	// Send queues a message for async delivery to Kafka. Returns an error
	// if the message fails validation or if the context has been cancelled.
	Send(ProducerMessage) error

	// SendAvroMsg queues a pre-built sarama ProducerMessage (typically Avro-encoded
	// using AvroEncoder) for async delivery. Returns an error if the context
	// has been cancelled.
	SendAvroMsg(*sarama.ProducerMessage) error
}

// Option configures optional parameters for NewProducer and NewConsumer.
type Option func(*options)

type options struct {
	schemaRegistryClient SchemaRegistryClientInterface
}

// WithSchemaRegistryClient injects a pre-existing schema registry client,
// allowing multiple producers/consumers to share the same client and cache.
func WithSchemaRegistryClient(client SchemaRegistryClientInterface) Option {
	return func(o *options) { o.schemaRegistryClient = client }
}

// internal type implementing kafka.Producer contract
type kafkaProducer struct {
	ctx  context.Context
	conf Config

	producer             sarama.AsyncProducer
	schemaRegistryClient SchemaRegistryClientInterface
	errors               chan error
	logger               *log.Logger
}

// NewProducer creates a Kafka producer. The caller can cancel the supplied context to initiate shutdown.
// Pass WithSchemaRegistryClient to share a schema registry client across multiple producers/consumers.
func NewProducer(ctx context.Context, conf *Config, logger *log.Logger, opts ...Option) (Producer, error) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}

	producer, err := createProducer(conf, logger)
	if err != nil {
		return nil, err
	}

	srClient := o.schemaRegistryClient
	if srClient == nil {
		schemaRegistryServers := strings.Split(conf.SchemaRegistryServers, ",")
		if len(schemaRegistryServers) == 0 || schemaRegistryServers[0] == "" {
			return nil, errors.New("at least one Schema Registry server is required")
		}
		srClient = NewCachedSchemaRegistryClientWithOptions(
			schemaRegistryServers, len(schemaRegistryServers), conf.SchemaRegistryTimeout, 0,
		)
	}
	return &kafkaProducer{
		conf:                 *conf,
		ctx:                  ctx,
		producer:             producer,
		schemaRegistryClient: srClient,
		errors:               make(chan error, errorQueueSize),
		logger:               logger,
	}, nil
}

// user-facing event emit API
func (kp *kafkaProducer) Send(msg ProducerMessage) error {
	if len(msg.Topic) == 0 {
		return errors.New("message topic is required")
	}

	if len(msg.Key) == 0 && len(msg.Value) == 0 {
		return errors.New("at least one of message fields key or value is required")
	}

	kmsg := &sarama.ProducerMessage{
		Topic: msg.Topic,
		Key:   sarama.ByteEncoder(msg.Key),
		Value: sarama.ByteEncoder(msg.Value),
	}

	// if shutdown is triggered, drop the message
	select {
	case <-kp.ctx.Done():
		return fmt.Errorf("message lost: shutdown triggered during send: %w", kp.ctx.Err())

	case kp.producer.Input() <- kmsg:
		// fall through, msg has been queued for write
	}

	return nil
}

// user-facing event emit API
func (kp *kafkaProducer) SendAvroMsg(msg *sarama.ProducerMessage) error {

	// if shutdown is triggered, drop the message
	select {
	case <-kp.ctx.Done():
		return fmt.Errorf("message lost: shutdown triggered during send: %w", kp.ctx.Err())

	case kp.producer.Input() <- msg:
		// fall through, msg has been queued for write
	}

	return nil
}

// caller should run the returned function in a goroutine, and consume
// the returned error channel until it's closed at shutdown.
func (kp *kafkaProducer) Background() (func(), chan error) {
	// proxy all Sarama errors to the caller until Close() drains and closes it
	go func() {
		for err := range kp.producer.Errors() {
			kp.errors <- err
		}

		kp.logger.Printf("Kafka producer: shutting down error reporter")
	}()

	return func() {
		defer func() {
			if err := kp.producer.Close(); err != nil {
				// Non-blocking send: during shutdown the caller may have stopped
				// consuming the error channel, so avoid deadlock.
				select {
				case kp.errors <- err:
				default:
					kp.logger.Printf("Kafka producer: close error dropped (channel full): %s", err)
				}
			}
			close(kp.errors)
		}()

		<-kp.ctx.Done()
		kp.logger.Printf("Kafka producer: shutdown triggered")
	}, kp.errors
}

func createProducer(conf *Config, logger *log.Logger) (sarama.AsyncProducer, error) {
	if logger != nil {
		sarama.Logger = logger
	}

	cfg, err := configureProducer(conf)
	if err != nil {
		return nil, err
	}

	brokers := strings.Split(conf.Brokers, ",")
	producer, err := sarama.NewAsyncProducer(brokers, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka producer: %w", err)
	}

	return producer, nil
}

// AvroEncoder implements sarama.Encoder for Avro messages using the Confluent
// wire format: 1 magic byte (0x00) + 4-byte big-endian schema ID + binary Avro payload.
// Use this with sarama.ProducerMessage to send Avro-encoded messages.
type AvroEncoder struct {
	SchemaID int
	Content  []byte
}

// Notice: the Confluent schema registry has special requirements for the Avro serialization rules,
// not only need to serialize the specific content, but also attach the Schema ID and Magic Byte.
// Ref: https://docs.confluent.io/current/schema-registry/serializer-formatter.html#wire-format
func (a *AvroEncoder) Encode() ([]byte, error) {
	var binaryMsg []byte
	// Confluent serialization format version number; currently always 0.
	binaryMsg = append(binaryMsg, byte(0))
	// 4-byte schema ID as returned by Schema Registry
	binarySchemaId := make([]byte, 4)
	binary.BigEndian.PutUint32(binarySchemaId, uint32(a.SchemaID))
	binaryMsg = append(binaryMsg, binarySchemaId...)
	// Avro serialized data in Avro's binary encoding
	binaryMsg = append(binaryMsg, a.Content...)
	return binaryMsg, nil
}

// Length of schemaId and Content.
func (a *AvroEncoder) Length() int {
	return 5 + len(a.Content)
}

// GetSchemaId registers or retrieves the schema ID for the given topic from
// the schema registry. The subject name is derived as "<topic>-value".
func (ap *kafkaProducer) GetSchemaId(topic string, avroCodec *goavro.Codec) (int, error) {
	schemaId, err := ap.schemaRegistryClient.CreateSubject(topic+"-value", avroCodec)
	if err != nil {
		return 0, err
	}
	return schemaId, nil
}
