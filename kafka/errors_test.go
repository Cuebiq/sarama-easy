package kafka

import (
	"bytes"
	"io"
	"net/http"
	"testing"
)

func TestError_Error(t *testing.T) {
	err := &Error{ErrorCode: 40401, Message: "Subject not found"}
	expected := "40401 - Subject not found"
	if err.Error() != expected {
		t.Errorf("expected '%s', got '%s'", expected, err.Error())
	}
}

func TestNewError_ValidJSON(t *testing.T) {
	body := `{"error_code":42202,"message":"Invalid Avro schema"}`
	resp := &http.Response{
		StatusCode: 422,
		Body:       io.NopCloser(bytes.NewBufferString(body)),
	}

	err := newError(resp)
	if err.ErrorCode != 42202 {
		t.Errorf("expected error code 42202, got %d", err.ErrorCode)
	}
	if err.Message != "Invalid Avro schema" {
		t.Errorf("expected message 'Invalid Avro schema', got '%s'", err.Message)
	}
}

func TestNewError_InvalidJSON(t *testing.T) {
	body := `not json`
	resp := &http.Response{
		StatusCode: 500,
		Body:       io.NopCloser(bytes.NewBufferString(body)),
	}

	err := newError(resp)
	if err.ErrorCode != 500 {
		t.Errorf("expected error code 500, got %d", err.ErrorCode)
	}
	if err.Message != "Unrecognized error found" {
		t.Errorf("expected fallback message, got '%s'", err.Message)
	}
}
