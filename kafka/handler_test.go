package kafka

import "testing"

func TestNoOpHandler_Handle(t *testing.T) {
	handler := NoOpHandler
	msg := &Message{
		Topic: "test",
		Key:   "key",
		Value: "value",
	}

	err := handler.Handle(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNoOpHandler_NilMessage(t *testing.T) {
	handler := NoOpHandler
	err := handler.Handle(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
