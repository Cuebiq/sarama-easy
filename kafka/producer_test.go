package kafka

import (
	"context"
	"encoding/binary"
	"testing"
)

func TestSend_EmptyTopic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	kp := &kafkaProducer{
		ctx: ctx,
	}

	err := kp.Send(ProducerMessage{
		Topic: "",
		Key:   []byte("key"),
		Value: []byte("value"),
	})
	if err == nil {
		t.Fatal("expected error for empty topic")
	}
	if err.Error() != "message topic is required" {
		t.Errorf("unexpected error message: %s", err)
	}
}

func TestSend_EmptyKeyAndValue(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	kp := &kafkaProducer{
		ctx: ctx,
	}

	err := kp.Send(ProducerMessage{
		Topic: "test-topic",
		Key:   []byte{},
		Value: []byte{},
	})
	if err == nil {
		t.Fatal("expected error for empty key and value")
	}
	if err.Error() != "at least one of message fields key or value is required" {
		t.Errorf("unexpected error message: %s", err)
	}
}

func TestSend_ValidationBeforeProducer(t *testing.T) {
	// Validation should happen before touching the producer,
	// so even with nil producer these should return proper errors
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	kp := &kafkaProducer{ctx: ctx}

	err := kp.Send(ProducerMessage{Topic: "", Key: []byte("k")})
	if err == nil {
		t.Fatal("expected error for empty topic")
	}

	err = kp.Send(ProducerMessage{Topic: "t"})
	if err == nil {
		t.Fatal("expected error for empty key and value")
	}
}

func TestAvroEncoder_Encode(t *testing.T) {
	content := []byte("test-avro-data")
	encoder := &AvroEncoder{
		SchemaID: 42,
		Content:  content,
	}

	encoded, err := encoder.Encode()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// First byte should be magic byte 0
	if encoded[0] != 0 {
		t.Errorf("expected magic byte 0, got %d", encoded[0])
	}

	// Bytes 1-4 should be schema ID in big-endian
	schemaId := binary.BigEndian.Uint32(encoded[1:5])
	if schemaId != 42 {
		t.Errorf("expected schema ID 42, got %d", schemaId)
	}

	// Remaining bytes should be the content
	if string(encoded[5:]) != "test-avro-data" {
		t.Errorf("expected content 'test-avro-data', got '%s'", string(encoded[5:]))
	}
}

func TestAvroEncoder_Length(t *testing.T) {
	encoder := &AvroEncoder{
		SchemaID: 1,
		Content:  []byte("hello"),
	}

	// 5 bytes header (1 magic + 4 schema ID) + content length
	expected := 5 + len("hello")
	if encoder.Length() != expected {
		t.Errorf("expected length %d, got %d", expected, encoder.Length())
	}
}

func TestAvroEncoder_EmptyContent(t *testing.T) {
	encoder := &AvroEncoder{
		SchemaID: 1,
		Content:  []byte{},
	}

	encoded, err := encoder.Encode()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(encoded) != 5 {
		t.Errorf("expected 5 bytes for empty content, got %d", len(encoded))
	}

	if encoder.Length() != 5 {
		t.Errorf("expected length 5, got %d", encoder.Length())
	}
}
