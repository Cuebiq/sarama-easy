package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
)

type fakeSession struct {
	ctx    context.Context
	claims map[string][]int32

	mu     sync.Mutex
	marked []int64
}

func (s *fakeSession) Claims() map[string][]int32               { return s.claims }
func (s *fakeSession) MemberID() string                         { return "member" }
func (s *fakeSession) GenerationID() int32                      { return 1 }
func (s *fakeSession) MarkOffset(string, int32, int64, string)  {}
func (s *fakeSession) Commit()                                  {}
func (s *fakeSession) ResetOffset(string, int32, int64, string) {}
func (s *fakeSession) Context() context.Context                 { return s.ctx }
func (s *fakeSession) MarkMessage(msg *sarama.ConsumerMessage, _ string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.marked = append(s.marked, msg.Offset)
}

func (s *fakeSession) markedOffsets() []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]int64(nil), s.marked...)
}

type fakeClaim struct {
	initialOffset int64
	hwm           int64
	messages      chan *sarama.ConsumerMessage
}

func (c *fakeClaim) Topic() string                            { return "t" }
func (c *fakeClaim) Partition() int32                         { return 1 }
func (c *fakeClaim) InitialOffset() int64                     { return c.initialOffset }
func (c *fakeClaim) HighWaterMarkOffset() int64               { return c.hwm }
func (c *fakeClaim) Messages() <-chan *sarama.ConsumerMessage { return c.messages }

func newFakeClaim(msgs ...*sarama.ConsumerMessage) *fakeClaim {
	ch := make(chan *sarama.ConsumerMessage, len(msgs))
	for _, m := range msgs {
		ch <- m
	}
	return &fakeClaim{initialOffset: sarama.OffsetOldest, hwm: int64(len(msgs)), messages: ch}
}

// tombstone builds a nil-value message, decoded without a schema registry.
func tombstone(offset int64) *sarama.ConsumerMessage {
	return &sarama.ConsumerMessage{Topic: "t", Partition: 1, Offset: offset, Key: []byte(fmt.Sprintf("k%d", offset))}
}

type recordingHandler struct {
	mu       sync.Mutex
	seen     []int64
	failures map[int64][]error // errors returned, in order, before succeeding
}

func (h *recordingHandler) Handle(m *Message) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.seen = append(h.seen, m.Offset)
	if errs := h.failures[m.Offset]; len(errs) > 0 {
		h.failures[m.Offset] = errs[1:]
		return errs[0]
	}
	return nil
}

func (h *recordingHandler) seenOffsets() []int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]int64(nil), h.seen...)
}

type recordingObserver struct {
	mu       sync.Mutex
	assigned map[string][]int32
	started  []PartitionInfo
	halted   []PartitionHalt
}

func (o *recordingObserver) Assigned(a map[string][]int32) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.assigned = a
}

func (o *recordingObserver) ClaimStarted(p PartitionInfo) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.started = append(o.started, p)
}

func (o *recordingObserver) PartitionHalted(h PartitionHalt) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.halted = append(o.halted, h)
}

func (o *recordingObserver) haltedEvents() []PartitionHalt {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]PartitionHalt(nil), o.halted...)
}

func newTestConsumer(h Handler, o PartitionObserver) *kafkaConsumer {
	return &kafkaConsumer{
		conf:     Config{RetryBackoff: time.Millisecond, MaxRetryBackoff: 5 * time.Millisecond},
		handler:  h,
		observer: o,
		errors:   make(chan error, errorQueueSize),
		logger:   testLogger,
		oldestOffset: func(string, int32) (int64, error) {
			return 0, nil
		},
	}
}

// runClaim runs ConsumeClaim in the background and returns a channel closed when it returns.
func runClaim(kc *kafkaConsumer, s *fakeSession, c *fakeClaim) chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := kc.ConsumeClaim(s, c); err != nil {
			panic(err)
		}
	}()
	return done
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(time.Millisecond)
	}
}

func equalOffsets(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestConsumeClaim_RetriesTransientHandlerErrorInOrder(t *testing.T) {
	h := &recordingHandler{failures: map[int64][]error{1: {errors.New("opa down"), errors.New("opa down")}}}
	kc := newTestConsumer(h, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &fakeSession{ctx: ctx}
	done := runClaim(kc, s, newFakeClaim(tombstone(0), tombstone(1), tombstone(2)))

	waitFor(t, func() bool { return len(s.markedOffsets()) == 3 })
	cancel()
	<-done

	if got := s.markedOffsets(); !equalOffsets(got, []int64{0, 1, 2}) {
		t.Errorf("marked offsets = %v, want [0 1 2]", got)
	}
	if got := h.seenOffsets(); !equalOffsets(got, []int64{0, 1, 1, 1, 2}) {
		t.Errorf("handled offsets = %v, want [0 1 1 1 2]", got)
	}
}

func TestConsumeClaim_PermanentHandlerErrorHaltsPartition(t *testing.T) {
	h := &recordingHandler{failures: map[int64][]error{1: {Permanent(errors.New("bad payload"))}}}
	o := &recordingObserver{}
	kc := newTestConsumer(h, o)
	ctx, cancel := context.WithCancel(context.Background())
	s := &fakeSession{ctx: ctx}
	done := runClaim(kc, s, newFakeClaim(tombstone(0), tombstone(1), tombstone(2)))

	waitFor(t, func() bool { return len(o.haltedEvents()) == 1 })
	select {
	case <-done:
		t.Fatal("ConsumeClaim must block on a halted partition until the session ends")
	case <-time.After(20 * time.Millisecond):
	}
	cancel()
	<-done

	if got := s.markedOffsets(); !equalOffsets(got, []int64{0}) {
		t.Errorf("marked offsets = %v, want [0]", got)
	}
	if got := h.seenOffsets(); !equalOffsets(got, []int64{0, 1}) {
		t.Errorf("handled offsets = %v, want [0 1] (nothing after the halted message)", got)
	}
	halt := o.haltedEvents()[0]
	if halt.Topic != "t" || halt.Partition != 1 || halt.Offset != 1 || halt.Key != "k1" || !IsPermanent(halt.Err) {
		t.Errorf("unexpected halt event: %+v", halt)
	}
	select {
	case err := <-kc.errors:
		if !IsPermanent(err) {
			t.Errorf("expected the halt to be reported on the error channel, got: %s", err)
		}
	default:
		t.Error("expected the halt to be reported on the error channel")
	}
}

func TestConsumeClaim_UndecodableRecordHaltsPartition(t *testing.T) {
	h := &recordingHandler{}
	o := &recordingObserver{}
	kc := newTestConsumer(h, o)
	ctx, cancel := context.WithCancel(context.Background())
	s := &fakeSession{ctx: ctx}
	bad := &sarama.ConsumerMessage{Topic: "t", Partition: 1, Offset: 1, Key: []byte("bad"), Value: []byte{0x01, 0, 0, 0, 7, 9}}
	done := runClaim(kc, s, newFakeClaim(tombstone(0), bad, tombstone(2)))

	waitFor(t, func() bool { return len(o.haltedEvents()) == 1 })
	cancel()
	<-done

	if got := h.seenOffsets(); !equalOffsets(got, []int64{0}) {
		t.Errorf("handled offsets = %v, want [0]", got)
	}
	if got := s.markedOffsets(); !equalOffsets(got, []int64{0}) {
		t.Errorf("marked offsets = %v, want [0]", got)
	}
	if halt := o.haltedEvents()[0]; halt.Offset != 1 || halt.Key != "bad" {
		t.Errorf("unexpected halt event: %+v", halt)
	}
}

func TestConsumeClaim_SessionEndDuringRetryLeavesMessageUnmarked(t *testing.T) {
	h := &recordingHandler{failures: map[int64][]error{0: make([]error, 1000)}}
	for i := range h.failures[0] {
		h.failures[0][i] = errors.New("opa down")
	}
	o := &recordingObserver{}
	kc := newTestConsumer(h, o)
	ctx, cancel := context.WithCancel(context.Background())
	s := &fakeSession{ctx: ctx}
	done := runClaim(kc, s, newFakeClaim(tombstone(0)))

	waitFor(t, func() bool { return len(h.seenOffsets()) >= 2 })
	cancel()
	<-done

	if got := s.markedOffsets(); len(got) != 0 {
		t.Errorf("marked offsets = %v, want none", got)
	}
	if len(o.haltedEvents()) != 0 {
		t.Error("a transient error must never halt the partition")
	}
}

func TestConsumeClaim_ReportsClaimStart(t *testing.T) {
	o := &recordingObserver{}
	kc := newTestConsumer(&recordingHandler{}, o)
	kc.oldestOffset = func(topic string, partition int32) (int64, error) { return 47, nil }
	ctx, cancel := context.WithCancel(context.Background())
	s := &fakeSession{ctx: ctx}
	c := newFakeClaim()
	c.hwm = 4442
	done := runClaim(kc, s, c)
	cancel()
	<-done

	want := PartitionInfo{Topic: "t", Partition: 1, StartOffset: 47, HighWaterMarkOffset: 4442}
	if len(o.started) != 1 || o.started[0] != want {
		t.Errorf("started = %+v, want [%+v]", o.started, want)
	}
}

func TestStartOffset(t *testing.T) {
	kc := newTestConsumer(nil, nil)
	kc.oldestOffset = func(string, int32) (int64, error) { return 10, nil }
	cases := []struct {
		initial, want int64
	}{
		{initial: 25, want: 25},
		{initial: sarama.OffsetOldest, want: 10},
		{initial: sarama.OffsetNewest, want: 100},
	}
	for _, tc := range cases {
		if got := kc.startOffset(&fakeClaim{initialOffset: tc.initial, hwm: 100}); got != tc.want {
			t.Errorf("startOffset(initial=%d) = %d, want %d", tc.initial, got, tc.want)
		}
	}
	kc.oldestOffset = func(string, int32) (int64, error) { return 0, errors.New("broker down") }
	if got := kc.startOffset(&fakeClaim{initialOffset: sarama.OffsetOldest, hwm: 100}); got != -1 {
		t.Errorf("startOffset with failing lookup = %d, want -1", got)
	}
}

func TestSetup_ReportsAssignment(t *testing.T) {
	o := &recordingObserver{}
	kc := newTestConsumer(nil, o)
	claims := map[string][]int32{"a": {0, 1, 2}, "b": {0}}
	if err := kc.Setup(&fakeSession{ctx: context.Background(), claims: claims}); err != nil {
		t.Fatal(err)
	}
	if len(o.assigned) != 2 || len(o.assigned["a"]) != 3 {
		t.Errorf("assigned = %v, want %v", o.assigned, claims)
	}
}

func TestClassifySchemaError(t *testing.T) {
	if err := classifySchemaError(183, &Error{ErrorCode: 40403, Message: "Schema not found"}); !IsPermanent(err) {
		t.Errorf("schema not found must be permanent, got: %s", err)
	}
	if err := classifySchemaError(183, &Error{ErrorCode: 404, Message: "Unrecognized error found"}); !IsPermanent(err) {
		t.Errorf("HTTP 404 must be permanent, got: %s", err)
	}
	if err := classifySchemaError(183, &Error{ErrorCode: 50001, Message: "store error"}); IsPermanent(err) {
		t.Errorf("registry server errors must be transient, got: %s", err)
	}
	if err := classifySchemaError(183, errors.New("connection refused")); IsPermanent(err) {
		t.Errorf("transport errors must be transient, got: %s", err)
	}
}

func TestPermanent(t *testing.T) {
	if Permanent(nil) != nil {
		t.Error("Permanent(nil) must be nil")
	}
	base := errors.New("boom")
	wrapped := fmt.Errorf("context: %w", Permanent(base))
	if !IsPermanent(wrapped) {
		t.Error("IsPermanent must see through wrapping")
	}
	if !errors.Is(wrapped, base) {
		t.Error("PermanentError must unwrap to the original error")
	}
	if IsPermanent(base) {
		t.Error("a plain error is not permanent")
	}
}

func TestSchemaIDOf(t *testing.T) {
	if got := schemaIDOf([]byte{0, 0, 0, 0, 183, 1}); got != 183 {
		t.Errorf("schemaIDOf = %d, want 183", got)
	}
	if got := schemaIDOf([]byte{1, 0, 0, 0, 183}); got != -1 {
		t.Errorf("schemaIDOf with bad magic byte = %d, want -1", got)
	}
	if got := schemaIDOf(nil); got != -1 {
		t.Errorf("schemaIDOf(nil) = %d, want -1", got)
	}
}
