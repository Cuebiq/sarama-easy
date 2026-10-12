package kafka

// Handler is the interface that consumers use to process incoming Kafka messages.
// Implementations receive a decoded Message. A returned error is retried with
// backoff (transient) unless wrapped with Permanent, which halts the partition
// on that message. The message is marked consumed only when Handle returns nil.
type Handler interface {
	Handle(*Message) error
}

// NoOpHandler is a Handler implementation that discards all messages.
// Useful for testing or when only side effects of consuming (e.g. offset commits) matter.
var NoOpHandler = &noOpHandler{}

type noOpHandler struct{}

func (h *noOpHandler) Handle(kmsg *Message) error {
	return nil
}
