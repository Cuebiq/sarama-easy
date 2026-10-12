package kafka

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

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

	client               sarama.Client
	consumer             sarama.ConsumerGroup
	schemaRegistryClient SchemaRegistryClientInterface
	handler              Handler
	observer             PartitionObserver
	errors               chan error

	// oldestOffset resolves the first available offset of a partition
	oldestOffset func(topic string, partition int32) (int64, error)

	logger *log.Logger
}

const (
	defaultRetryBackoff    = 500 * time.Millisecond
	defaultMaxRetryBackoff = 30 * time.Second
)

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

	// create consumer group on a dedicated client, also used to resolve partition offsets.
	// docs recommend 1 client per producer or consumer for throughput
	client, err := sarama.NewClient(brokers, saramaConf)
	if err != nil {
		return nil, fmt.Errorf("error creating kafka client: %w", err)
	}
	consumer, err := sarama.NewConsumerGroupFromClient(conf.Group, client)
	if err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("error creating consumer group: %w", err)
	}

	srClient := o.schemaRegistryClient
	if srClient == nil {
		schemaRegistryServers := strings.Split(conf.SchemaRegistryServers, ",")
		if len(schemaRegistryServers) == 0 || schemaRegistryServers[0] == "" {
			_ = consumer.Close()
			_ = client.Close()
			return nil, errors.New("at least one Schema Registry server is required")
		}
		srClient = NewCachedSchemaRegistryClientWithOptions(
			schemaRegistryServers, len(schemaRegistryServers), conf.SchemaRegistryTimeout, 0,
		)
	}

	return &kafkaConsumer{
		ctx:                  ctx,
		conf:                 *conf,
		client:               client,
		consumer:             consumer,
		schemaRegistryClient: srClient,
		handler:              handler,
		observer:             o.partitionObserver,
		errors:               make(chan error, errorQueueSize),
		oldestOffset: func(topic string, partition int32) (int64, error) {
			return client.GetOffset(topic, partition, sarama.OffsetOldest)
		},
		logger: logger,
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
			// a consumer group built from a client does not close it
			if kc.client != nil && !kc.client.Closed() {
				if err := kc.client.Close(); err != nil {
					kc.logger.Printf("Kafka consumer: client close error: %s", err)
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
	if kc.observer != nil {
		kc.observer.Assigned(session.Claims())
	}
	return nil
}

// Implements the sarama.ConsumerGroupHandler contract
func (kc *kafkaConsumer) Cleanup(session sarama.ConsumerGroupSession) error {
	kc.logger.Printf("Kafka client: consumer.Cleanup() called: session=%+v", session)
	return nil
}

// Implements the sarama.ConsumerGroupHandler contract - Sarama runs this in a goroutine for you.
// Called once per partition assigned to this consumer group member.
//
// Messages are processed strictly in order and never skipped: transient errors
// are retried with backoff, and a permanent error halts the partition on that
// message until the session ends (rebalance or shutdown).
func (kc *kafkaConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	kc.logger.Printf("Kafka client: consuming partition %d of topic %s", claim.Partition(), claim.Topic())
	if kc.observer != nil {
		kc.observer.ClaimStarted(PartitionInfo{
			Topic:               claim.Topic(),
			Partition:           claim.Partition(),
			StartOffset:         kc.startOffset(claim),
			HighWaterMarkOffset: claim.HighWaterMarkOffset(),
		})
	}

	ctx := session.Context()
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			if err := kc.processMessage(ctx, claim, msg); err != nil {
				if IsPermanent(err) {
					kc.halt(ctx, msg, err)
				}
				// session ended (or halted): leave the message unmarked
				return nil
			}
			session.MarkMessage(msg, "")
		}
	}
}

// processMessage decodes and handles one message, retrying transient errors.
// Returns nil on success, a PermanentError, or ctx.Err() if the session ended.
func (kc *kafkaConsumer) processMessage(ctx context.Context, claim sarama.ConsumerGroupClaim, msg *sarama.ConsumerMessage) error {
	var message *Message
	err := kc.retry(ctx, msg, "decode", func() error {
		var err error
		message, err = kc.ProcessAvroMsg((*ConsumerMessage)(msg))
		return err
	})
	if err != nil {
		return err
	}
	return kc.retry(ctx, msg, "handle", func() error {
		message.HighWaterMarkOffset = claim.HighWaterMarkOffset()
		return kc.handler.Handle(message)
	})
}

// retry runs fn until it succeeds, returns a permanent error, or ctx is done.
func (kc *kafkaConsumer) retry(ctx context.Context, msg *sarama.ConsumerMessage, stage string, fn func() error) error {
	backoff := kc.conf.RetryBackoff
	if backoff <= 0 {
		backoff = defaultRetryBackoff
	}
	maxBackoff := kc.conf.MaxRetryBackoff
	if maxBackoff <= 0 {
		maxBackoff = defaultMaxRetryBackoff
	}
	for attempt := 1; ; attempt++ {
		err := fn()
		if err == nil || IsPermanent(err) {
			return err
		}
		kc.logger.Printf("Kafka client: transient %s error on %s/%d offset %d (attempt %d), retrying in %s: %s",
			stage, msg.Topic, msg.Partition, msg.Offset, attempt, backoff, err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, maxBackoff)
	}
}

// halt reports a partition stopped on msg and blocks until the session ends.
func (kc *kafkaConsumer) halt(ctx context.Context, msg *sarama.ConsumerMessage, err error) {
	h := PartitionHalt{
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
		Key:       string(msg.Key),
		SchemaId:  schemaIDOf(msg.Value),
		Err:       err,
	}
	kc.logger.Printf("Kafka client: partition %d of topic %s HALTED at offset %d (key %q, schema id %d): %s",
		h.Partition, h.Topic, h.Offset, h.Key, h.SchemaId, err)
	if kc.observer != nil {
		kc.observer.PartitionHalted(h)
	}
	select {
	case kc.errors <- fmt.Errorf("partition %s/%d halted at offset %d: %w", h.Topic, h.Partition, h.Offset, err):
	default:
	}
	<-ctx.Done()
}

// startOffset returns the first offset the claim reads, or -1 if unknown.
func (kc *kafkaConsumer) startOffset(claim sarama.ConsumerGroupClaim) int64 {
	switch off := claim.InitialOffset(); {
	case off >= 0:
		return off
	case off == sarama.OffsetNewest:
		return claim.HighWaterMarkOffset()
	case off == sarama.OffsetOldest && kc.oldestOffset != nil:
		oldest, err := kc.oldestOffset(claim.Topic(), claim.Partition())
		if err != nil {
			kc.logger.Printf("Kafka client: cannot resolve oldest offset of %s/%d: %s", claim.Topic(), claim.Partition(), err)
			return -1
		}
		return oldest
	}
	return -1
}

// schemaIDOf extracts the Confluent schema id from a raw value, or -1.
func schemaIDOf(value []byte) int {
	if len(value) < 5 || value[0] != 0x00 {
		return -1
	}
	return int(binary.BigEndian.Uint32(value[1:5]))
}

// ProcessAvroMsg decodes an Avro-encoded Kafka message using the Confluent wire
// format (magic byte + 4-byte schema ID + binary Avro payload). It fetches the
// schema from the registry, deserializes the binary Avro data, and returns a
// Message with the JSON-encoded value. Returns a Message with SchemaId=-1 for
// nil-value messages (e.g. tombstones). Malformed or undecodable records return
// a PermanentError; schema registry failures other than "not found" are transient.
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
		return &Message{}, Permanent(errors.New("message too short for Avro wire format (need at least 5 bytes)"))
	}
	if m.Value[0] != 0x00 {
		return &Message{}, Permanent(fmt.Errorf("invalid Avro magic byte: expected 0x00, got 0x%02x", m.Value[0]))
	}
	schemaId := binary.BigEndian.Uint32(m.Value[1:5])
	ac.logger.Printf("SchemaID: %d", schemaId)
	codec, err := ac.GetSchema(int(schemaId))
	if err != nil {
		if !IsPermanent(err) {
			err = classifySchemaError(int(schemaId), err)
		}
		return &Message{}, err
	}
	// Convert binary Avro data back to native Go form
	native, _, err := codec.NativeFromBinary(m.Value[5:])
	if err != nil {
		return &Message{}, Permanent(fmt.Errorf("decoding record with schema id %d: %w", schemaId, err))
	}

	// Convert native Go form to textual Avro data
	textual, err := codec.TextualFromNative(nil, native)
	if err != nil {
		return &Message{}, Permanent(fmt.Errorf("encoding record with schema id %d as JSON: %w", schemaId, err))
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
