package kafka

// PartitionInfo describes a partition claim when the consumer starts reading it.
type PartitionInfo struct {
	Topic     string
	Partition int32
	// StartOffset is the first offset the claim will read, or -1 if unknown.
	StartOffset int64
	// HighWaterMarkOffset is the offset of the next message to be produced
	// when the claim started.
	HighWaterMarkOffset int64
}

// PartitionHalt describes a partition stopped on a message that could not be
// processed. The consumer does not read past Offset until the next session.
type PartitionHalt struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       string
	SchemaId  int
	Err       error
}

// PartitionObserver receives partition lifecycle events. Implementations must
// be safe for concurrent use: claims run in parallel goroutines.
type PartitionObserver interface {
	// Assigned is called once per session, before any claim starts, with the
	// full assignment (topic -> partitions).
	Assigned(map[string][]int32)
	// ClaimStarted is called when a partition claim starts.
	ClaimStarted(PartitionInfo)
	// PartitionHalted is called when a partition stops on a permanent error.
	PartitionHalted(PartitionHalt)
}

// WithPartitionObserver registers a PartitionObserver on a consumer.
func WithPartitionObserver(o PartitionObserver) Option {
	return func(opts *options) { opts.partitionObserver = o }
}
