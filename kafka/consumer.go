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

// ConsumerMessage is a type alias for sarama.ConsumerMessage, abstracting
// the Sarama-specific message type from end users of this package.
type ConsumerMessage sarama.ConsumerMessage

// Consumer provides a high-level interface for consuming messages from Kafka
// using consumer groups with auto-rebalancing.
type Consumer interface {
	// Background returns a blocking function and an error channel. The caller
	// should run the function in a goroutine and consume the error channel
	// until it is closed (which signals shutdown is complete).
	Background() (func(), chan error)
}

type kafkaConsumer struct {
	ctx  context.Context
	conf Config

	consumer             sarama.ConsumerGroup
	schemaRegistryClient SchemaRegistryClientInterface
	handler              Handler
	errors               chan error

	logger *log.Logger
}

// Message is the decoded representation of a Kafka message, passed to Handler
// implementations. For Avro messages, Value contains the JSON-encoded Avro data
// and SchemaId holds the Confluent Schema Registry schema ID. For non-Avro
// messages (nil value), SchemaId is set to -1.
type Message struct {
	SchemaId            int
	Topic               string
	Partition           int32
	Offset              int64
	Key                 string
	Value               string
	HighWaterMarkOffset int64
}

// NewConsumer creates a Kafka consumer. The caller should cancel the supplied context for graceful shutdown.
// Pass WithSchemaRegistryClient to share a schema registry client across multiple producers/consumers.
func NewConsumer(ctx context.Context, conf *Config, handler Handler, logger *log.Logger, opts ...Option) (Consumer, error) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}

	// set the internal Sarama client logging
	if conf.Verbose {
		sarama.Logger = logger
	}

	saramaConf, err := configureConsumer(conf)
	if err != nil {
		return nil, err
	}

	// config should have a CSV list of brokers
	brokers := strings.Split(conf.Brokers, ",")

	// create consumer group w/underlying managed client.
	// docs recommend 1 client per producer or consumer for throughput
	consumer, err := sarama.NewConsumerGroup(brokers, conf.Group, saramaConf)
	if err != nil {
		return nil, fmt.Errorf("error creating consumer group: %w", err)
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

	return &kafkaConsumer{
		ctx:                  ctx,
		conf:                 *conf,
		consumer:             consumer,
		schemaRegistryClient: srClient,
		handler:              handler,
		errors:               make(chan error, errorQueueSize),
		logger:               logger,
	}, nil
}

// caller should run the returned function in a goroutine, and consume
// the returned error channel until it's closed at shutdown.
func (kc *kafkaConsumer) Background() (func(), chan error) {
	// pass errors from Sarama to end user; closed by Sarama during shutdown
	go func() {
		for err := range kc.consumer.Errors() {
			kc.errors <- err
		}
	}()

	return func() {
		defer func() {
			if err := kc.consumer.Close(); err != nil {
				// Non-blocking send: during shutdown the caller may have stopped
				// consuming the error channel, so avoid deadlock.
				select {
				case kc.errors <- err:
				default:
					kc.logger.Printf("Kafka consumer: close error dropped (channel full): %s", err)
				}
			}
			// this releases the caller who should be consuming this channel
			close(kc.errors)
		}()

		// the main consume loop, parent of the ConsumerClaim() partition message loops.
		topics := strings.Split(kc.conf.Topics, ",")
		for kc.ctx.Err() == nil {
			kc.logger.Printf("Kafka consumer: beginning parent Consume() loop for topic(s): %s", kc.conf.Topics)

			// if Consume() returns nil, a rebalance is in progress and it should be called again.
			// if Consume() returns an error, the consumer should break the loop, shutting down.
			if err := kc.consumer.Consume(kc.ctx, topics, kc); err != nil {
				kc.errors <- err
				return
			}
		}
	}, kc.errors
}

// Implements the sarama.ConsumerGroupHandler contract
func (kc *kafkaConsumer) Setup(session sarama.ConsumerGroupSession) error {
	kc.logger.Printf("Kafka client: consumer.Setup() called: session=%+v", session)
	return nil
}

// Implements the sarama.ConsumerGroupHandler contract
func (kc *kafkaConsumer) Cleanup(session sarama.ConsumerGroupSession) error {
	kc.logger.Printf("Kafka client: consumer.Cleanup() called: session=%+v", session)
	return nil
}

// Implements the sarama.ConsumerGroupHandler contract - Sarama runs this in a goroutine for you.
// Called once per partition assigned to this consumer group member.
func (kc *kafkaConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	kc.logger.Printf("Kafka client: consuming partition %d of topic %s", claim.Partition(), claim.Topic())

	// consume each partitions' messages async and pass to supplied handler. Messages() closed for us on shutdown
	for msg := range claim.Messages() {
		saramaMsg := (*ConsumerMessage)(msg)
		message, err := kc.ProcessAvroMsg(saramaMsg)
		if err != nil {
			return err
		}

		message.HighWaterMarkOffset = claim.HighWaterMarkOffset()
		if err := kc.handler.Handle(message); err != nil {
			return err
		}

		// if the message handler didn't return an error, mark this message offset as consumed
		session.MarkMessage(msg, "")
	}

	return nil
}

// ProcessAvroMsg decodes an Avro-encoded Kafka message using the Confluent wire
// format (magic byte + 4-byte schema ID + binary Avro payload). It fetches the
// schema from the registry, deserializes the binary Avro data, and returns a
// Message with the JSON-encoded value. Returns a Message with SchemaId=-1 for
// nil-value messages (e.g. tombstones).
func (ac *kafkaConsumer) ProcessAvroMsg(m *ConsumerMessage) (*Message, error) {
	if m.Value == nil {
		return &Message{
			SchemaId:  int(-1),
			Topic:     m.Topic,
			Partition: m.Partition,
			Offset:    m.Offset,
			Key:       string(m.Key),
			Value:     string("")}, nil
	}
	if len(m.Value) < 5 {
		return &Message{}, errors.New("message too short for Avro wire format (need at least 5 bytes)")
	}
	if m.Value[0] != 0x00 {
		return &Message{}, fmt.Errorf("invalid Avro magic byte: expected 0x00, got 0x%02x", m.Value[0])
	}
	schemaId := binary.BigEndian.Uint32(m.Value[1:5])
	ac.logger.Printf("SchemaID: %d", schemaId)
	codec, err := ac.GetSchema(int(schemaId))
	if err != nil {
		ac.logger.Printf("Error: %s", err)
		return &Message{}, err
	}
	// Convert binary Avro data back to native Go form
	native, _, err := codec.NativeFromBinary(m.Value[5:])
	if err != nil {
		ac.logger.Printf("Error: %s", err)
		return &Message{}, err
	}

	// Convert native Go form to textual Avro data
	textual, err := codec.TextualFromNative(nil, native)
	if err != nil {
		ac.logger.Printf("Error: %s", err)
		return &Message{}, err
	}
	msg := Message{
		SchemaId:  int(schemaId),
		Topic:     m.Topic,
		Partition: m.Partition,
		Offset:    m.Offset,
		Key:       string(m.Key),
		Value:     string(textual)}
	return &msg, nil
}

// GetSchema retrieves an Avro codec from the schema registry by its unique ID.
func (ac *kafkaConsumer) GetSchema(id int) (*goavro.Codec, error) {
	codec, err := ac.schemaRegistryClient.GetSchema(id)
	if err != nil {
		return nil, err
	}
	return codec, nil
}
