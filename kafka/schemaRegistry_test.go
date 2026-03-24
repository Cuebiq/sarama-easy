package kafka

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/linkedin/goavro/v2"
)

func newTestServer(handler http.HandlerFunc) *httptest.Server {
	return httptest.NewServer(handler)
}

func TestGetSchema_Success(t *testing.T) {
	avroSchema := `{"type":"string"}`
	server := newTestServer(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			t.Errorf("expected GET, got %s", r.Method)
		}
		if r.URL.Path != "/schemas/ids/1" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		resp := schemaResponse{Schema: avroSchema}
		json.NewEncoder(w).Encode(resp)
	})
	defer server.Close()

	client := NewSchemaRegistryClient([]string{server.URL})
	codec, err := client.GetSchema(1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if codec == nil {
		t.Fatal("expected non-nil codec")
	}
}

func TestGetSchema_NotFound(t *testing.T) {
	server := newTestServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(Error{ErrorCode: 40403, Message: "Schema not found"})
	})
	defer server.Close()

	client := NewSchemaRegistryClient([]string{server.URL})
	_, err := client.GetSchema(999)
	if err == nil {
		t.Fatal("expected error for missing schema")
	}
}

func TestGetSubjects_Success(t *testing.T) {
	server := newTestServer(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/subjects" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		json.NewEncoder(w).Encode([]string{"subject1", "subject2"})
	})
	defer server.Close()

	client := NewSchemaRegistryClient([]string{server.URL})
	subjects, err := client.GetSubjects()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(subjects) != 2 {
		t.Errorf("expected 2 subjects, got %d", len(subjects))
	}
	if subjects[0] != "subject1" {
		t.Errorf("expected 'subject1', got '%s'", subjects[0])
	}
}

func TestGetVersions_Success(t *testing.T) {
	server := newTestServer(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/subjects/test-subject/versions" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		json.NewEncoder(w).Encode([]int{1, 2, 3})
	})
	defer server.Close()

	client := NewSchemaRegistryClient([]string{server.URL})
	versions, err := client.GetVersions("test-subject")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(versions) != 3 {
		t.Errorf("expected 3 versions, got %d", len(versions))
	}
}

func TestGetSchemaByVersion_Success(t *testing.T) {
	avroSchema := `{"type":"string"}`
	server := newTestServer(func(w http.ResponseWriter, r *http.Request) {
		resp := schemaVersionResponse{
			Subject: "test",
			Version: 1,
			Schema:  avroSchema,
			ID:      10,
		}
		json.NewEncoder(w).Encode(resp)
	})
	defer server.Close()

	client := NewSchemaRegistryClient([]string{server.URL})
	codec, err := client.GetSchemaByVersion("test", 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if codec == nil {
		t.Fatal("expected non-nil codec")
	}
}

func TestGetLatestSchema_Success(t *testing.T) {
	avroSchema := `{"type":"string"}`
	server := newTestServer(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/subjects/test/versions/latest" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		resp := schemaVersionResponse{
			Subject: "test",
			Version: 3,
			Schema:  avroSchema,
			ID:      10,
		}
		json.NewEncoder(w).Encode(resp)
	})
	defer server.Close()

	client := NewSchemaRegistryClient([]string{server.URL})
	codec, err := client.GetLatestSchema("test")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if codec == nil {
		t.Fatal("expected non-nil codec")
	}
}

func TestCreateSubject_Success(t *testing.T) {
	server := newTestServer(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("expected POST, got %s", r.Method)
		}
		json.NewEncoder(w).Encode(idResponse{ID: 42})
	})
	defer server.Close()

	client := NewSchemaRegistryClient([]string{server.URL})

	// goavro needs a valid schema to create a codec
	codec, _ := newTestCodec()
	id, err := client.CreateSubject("test-subject", codec)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if id != 42 {
		t.Errorf("expected ID 42, got %d", id)
	}
}

func TestDeleteSubject_Success(t *testing.T) {
	server := newTestServer(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "DELETE" {
			t.Errorf("expected DELETE, got %s", r.Method)
		}
		if r.URL.Path != "/subjects/test-subject" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		json.NewEncoder(w).Encode([]int{1, 2})
	})
	defer server.Close()

	client := NewSchemaRegistryClient([]string{server.URL})
	err := client.DeleteSubject("test-subject")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDeleteVersion_Success(t *testing.T) {
	server := newTestServer(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "DELETE" {
			t.Errorf("expected DELETE, got %s", r.Method)
		}
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "1")
	})
	defer server.Close()

	client := NewSchemaRegistryClient([]string{server.URL})
	err := client.DeleteVersion("test-subject", 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestHttpCall_Retry5XX(t *testing.T) {
	attempts := 0
	server := newTestServer(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		json.NewEncoder(w).Encode([]string{"ok"})
	})
	defer server.Close()

	client := NewSchemaRegistryClientWithRetries([]string{server.URL}, 3)
	subjects, err := client.GetSubjects()
	if err != nil {
		t.Fatalf("unexpected error after retries: %v", err)
	}
	if len(subjects) != 1 {
		t.Errorf("expected 1 subject, got %d", len(subjects))
	}
	if attempts != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts)
	}
}

func TestHttpCall_ContentType(t *testing.T) {
	server := newTestServer(func(w http.ResponseWriter, r *http.Request) {
		ct := r.Header.Get("Content-Type")
		if ct != contentType {
			t.Errorf("expected content type '%s', got '%s'", contentType, ct)
		}
		json.NewEncoder(w).Encode([]string{})
	})
	defer server.Close()

	client := NewSchemaRegistryClient([]string{server.URL})
	client.GetSubjects()
}

func TestRetriable(t *testing.T) {
	tests := []struct {
		code     int
		expected bool
	}{
		{200, false},
		{404, false},
		{500, true},
		{502, true},
		{503, true},
		{599, true},
		{600, false},
	}

	for _, tt := range tests {
		resp := &http.Response{StatusCode: tt.code}
		if retriable(resp) != tt.expected {
			t.Errorf("retriable(%d) = %v, want %v", tt.code, !tt.expected, tt.expected)
		}
	}
}

func TestOkStatus(t *testing.T) {
	tests := []struct {
		code     int
		expected bool
	}{
		{200, true},
		{201, true},
		{301, true},
		{399, true},
		{400, false},
		{404, false},
		{500, false},
	}

	for _, tt := range tests {
		resp := &http.Response{StatusCode: tt.code}
		if okStatus(resp) != tt.expected {
			t.Errorf("okStatus(%d) = %v, want %v", tt.code, !tt.expected, tt.expected)
		}
	}
}

// --- Cached Schema Registry Tests ---

func TestCachedGetSchema_CachesResult(t *testing.T) {
	avroSchema := `{"type":"string"}`
	calls := 0
	server := newTestServer(func(w http.ResponseWriter, r *http.Request) {
		calls++
		resp := schemaResponse{Schema: avroSchema}
		json.NewEncoder(w).Encode(resp)
	})
	defer server.Close()

	client := NewCachedSchemaRegistryClient([]string{server.URL})

	// First call should hit the server
	codec1, err := client.GetSchema(1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if codec1 == nil {
		t.Fatal("expected non-nil codec")
	}
	if calls != 1 {
		t.Errorf("expected 1 server call, got %d", calls)
	}

	// Second call should be cached
	codec2, err := client.GetSchema(1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if codec2 == nil {
		t.Fatal("expected non-nil codec from cache")
	}
	if calls != 1 {
		t.Errorf("expected still 1 server call (cached), got %d", calls)
	}
}

func TestCachedCreateSubject_CachesResult(t *testing.T) {
	calls := 0
	server := newTestServer(func(w http.ResponseWriter, r *http.Request) {
		calls++
		json.NewEncoder(w).Encode(idResponse{ID: 7})
	})
	defer server.Close()

	client := NewCachedSchemaRegistryClient([]string{server.URL})
	codec, _ := newTestCodec()

	// First call
	id1, err := client.CreateSubject("test", codec)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if id1 != 7 {
		t.Errorf("expected ID 7, got %d", id1)
	}

	// Second call should be cached
	id2, err := client.CreateSubject("test", codec)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if id2 != 7 {
		t.Errorf("expected cached ID 7, got %d", id2)
	}
	if calls != 1 {
		t.Errorf("expected 1 server call (cached), got %d", calls)
	}
}

func TestCachedSchemaRegistryClientWithRetries(t *testing.T) {
	client := NewCachedSchemaRegistryClientWithRetries([]string{"http://localhost:1"}, 5)
	if client == nil {
		t.Fatal("expected non-nil client")
	}
	if client.schemaRegistryClient.retries != 5 {
		t.Errorf("expected 5 retries, got %d", client.schemaRegistryClient.retries)
	}
}

func TestCachedGetSchema_ConcurrentDoubleCheck(t *testing.T) {
	avroSchema := `{"type":"string"}`
	calls := 0
	server := newTestServer(func(w http.ResponseWriter, r *http.Request) {
		calls++
		// Simulate slow response to increase contention window
		resp := schemaResponse{Schema: avroSchema}
		json.NewEncoder(w).Encode(resp)
	})
	defer server.Close()

	client := NewCachedSchemaRegistryClient([]string{server.URL})

	// Launch multiple goroutines requesting the same schema concurrently
	const goroutines = 20
	errs := make(chan error, goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			codec, err := client.GetSchema(1)
			if err != nil {
				errs <- err
				return
			}
			if codec == nil {
				errs <- fmt.Errorf("expected non-nil codec")
				return
			}
			errs <- nil
		}()
	}

	for i := 0; i < goroutines; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("goroutine error: %v", err)
		}
	}

	// With double-check locking, only 1 HTTP call should be made
	// (the first goroutine to acquire the write lock fetches, others find it cached)
	if calls != 1 {
		t.Errorf("expected 1 server call with double-check locking, got %d", calls)
	}
}

func TestCachedGetSchema_TTLExpiration(t *testing.T) {
	avroSchema := `{"type":"string"}`
	calls := 0
	server := newTestServer(func(w http.ResponseWriter, r *http.Request) {
		calls++
		resp := schemaResponse{Schema: avroSchema}
		json.NewEncoder(w).Encode(resp)
	})
	defer server.Close()

	client := NewCachedSchemaRegistryClientWithOptions(
		[]string{server.URL}, 1, 2*time.Second, 50*time.Millisecond,
	)

	// First call hits server
	_, err := client.GetSchema(1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected 1 call, got %d", calls)
	}

	// Second call is cached
	_, err = client.GetSchema(1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected still 1 call (cached), got %d", calls)
	}

	// Wait for TTL to expire
	time.Sleep(60 * time.Millisecond)

	// Third call should re-fetch
	_, err = client.GetSchema(1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 2 {
		t.Errorf("expected 2 calls after TTL expiry, got %d", calls)
	}
}

func TestNewSchemaRegistryClientWithOptions_CustomTimeout(t *testing.T) {
	client := NewSchemaRegistryClientWithOptions([]string{"http://localhost:1"}, 3, 5*time.Second)
	if client.httpClient.Timeout != 5*time.Second {
		t.Errorf("expected 5s timeout, got %v", client.httpClient.Timeout)
	}
	if client.retries != 3 {
		t.Errorf("expected 3 retries, got %d", client.retries)
	}
}

func TestNewSchemaRegistryClientWithOptions_DefaultTimeout(t *testing.T) {
	client := NewSchemaRegistryClientWithOptions([]string{"http://localhost:1"}, 1, 0)
	if client.httpClient.Timeout != 2*time.Second {
		t.Errorf("expected default 2s timeout, got %v", client.httpClient.Timeout)
	}
}

// helper to create a valid goavro Codec for testing
func newTestCodec() (*goavro.Codec, error) {
	return goavro.NewCodec(`{"type":"string"}`)
}
