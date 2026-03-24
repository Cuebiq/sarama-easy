package kafka

import (
	"encoding/binary"
	"log"
	"os"
	"testing"

	"github.com/IBM/sarama"
)

var testLogger = log.New(os.Stderr, "[test] ", log.LstdFlags)

func TestProcessAvroMsg_NilValue(t *testing.T) {
	kc := &kafkaConsumer{logger: testLogger}

	msg := &ConsumerMessage{
		Topic:     "test-topic",
		Partition: 0,
		Offset:    10,
		Key:       []byte("test-key"),
		Value:     nil,
	}

	result, err := kc.ProcessAvroMsg(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.SchemaId != -1 {
		t.Errorf("expected SchemaId -1, got %d", result.SchemaId)
	}
	if result.Topic != "test-topic" {
		t.Errorf("expected Topic 'test-topic', got '%s'", result.Topic)
	}
	if result.Partition != 0 {
		t.Errorf("expected Partition 0, got %d", result.Partition)
	}
	if result.Offset != 10 {
		t.Errorf("expected Offset 10, got %d", result.Offset)
	}
	if result.Key != "test-key" {
		t.Errorf("expected Key 'test-key', got '%s'", result.Key)
	}
	if result.Value != "" {
		t.Errorf("expected empty Value, got '%s'", result.Value)
	}
}

func TestProcessAvroMsg_InvalidSchemaId(t *testing.T) {
	// Create a mock cached schema registry that will fail
	mockServers := []string{"http://localhost:1"}
	client := NewCachedSchemaRegistryClient(mockServers)

	kc := &kafkaConsumer{
		schemaRegistryClient: client,
		logger:               testLogger,
	}

	// Build a fake Avro message with magic byte + schema ID
	var value []byte
	value = append(value, byte(0)) // magic byte
	schemaIdBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(schemaIdBytes, 99999)
	value = append(value, schemaIdBytes...)
	value = append(value, []byte("fake-data")...)

	msg := &ConsumerMessage{
		Topic:     "test-topic",
		Partition: 0,
		Offset:    1,
		Key:       []byte("key"),
		Value:     value,
	}

	// Should fail because schema registry is not reachable
	_, err := kc.ProcessAvroMsg(msg)
	if err == nil {
		t.Fatal("expected error when schema registry is unreachable")
	}
}

func TestProcessAvroMsg_TooShort(t *testing.T) {
	kc := &kafkaConsumer{logger: testLogger}

	msg := &ConsumerMessage{
		Topic: "test-topic",
		Value: []byte{0x00, 0x01}, // only 2 bytes, need at least 5
	}

	_, err := kc.ProcessAvroMsg(msg)
	if err == nil {
		t.Fatal("expected error for message shorter than 5 bytes")
	}
	if err.Error() != "message too short for Avro wire format (need at least 5 bytes)" {
		t.Errorf("unexpected error: %s", err)
	}
}

func TestProcessAvroMsg_InvalidMagicByte(t *testing.T) {
	kc := &kafkaConsumer{logger: testLogger}

	msg := &ConsumerMessage{
		Topic: "test-topic",
		Value: []byte{0x01, 0x00, 0x00, 0x00, 0x01, 0x02}, // magic byte 0x01 instead of 0x00
	}

	_, err := kc.ProcessAvroMsg(msg)
	if err == nil {
		t.Fatal("expected error for invalid magic byte")
	}
	if err.Error() != "invalid Avro magic byte: expected 0x00, got 0x01" {
		t.Errorf("unexpected error: %s", err)
	}
}

func TestConsumerMessage_TypeAlias(t *testing.T) {
	saramaMsg := &sarama.ConsumerMessage{
		Topic:     "test",
		Partition: 1,
		Offset:    42,
		Key:       []byte("key"),
		Value:     []byte("value"),
	}

	// Verify type conversion works
	msg := (*ConsumerMessage)(saramaMsg)
	if msg.Topic != "test" {
		t.Errorf("expected Topic 'test', got '%s'", msg.Topic)
	}
	if msg.Partition != 1 {
		t.Errorf("expected Partition 1, got %d", msg.Partition)
	}
	if msg.Offset != 42 {
		t.Errorf("expected Offset 42, got %d", msg.Offset)
	}
}

func TestMessage_Fields(t *testing.T) {
	msg := Message{
		SchemaId:            1,
		Topic:               "topic",
		Partition:           2,
		Offset:              100,
		Key:                 "key",
		Value:               "value",
		HighWaterMarkOffset: 200,
	}

	if msg.SchemaId != 1 {
		t.Errorf("expected SchemaId 1, got %d", msg.SchemaId)
	}
	if msg.HighWaterMarkOffset != 200 {
		t.Errorf("expected HighWaterMarkOffset 200, got %d", msg.HighWaterMarkOffset)
	}
}
