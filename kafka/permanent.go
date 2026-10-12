package kafka

import (
	"errors"
	"fmt"
)

// PermanentError marks a message error that retrying cannot fix (e.g. an
// undecodable record). The consumer halts the partition on it instead of
// retrying. Any other error returned while processing a message is treated
// as transient and retried with backoff.
type PermanentError struct {
	Err error
}

func (e *PermanentError) Error() string { return "permanent: " + e.Err.Error() }

func (e *PermanentError) Unwrap() error { return e.Err }

// Permanent wraps err as a PermanentError. Handlers return it for messages
// that must not be retried. Returns nil if err is nil.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &PermanentError{Err: err}
}

// IsPermanent reports whether err, or any error it wraps, is a PermanentError.
func IsPermanent(err error) bool {
	var p *PermanentError
	return errors.As(err, &p)
}

// classifySchemaError marks schema registry lookups that will never succeed
// (unknown schema id) as permanent; everything else (transport, 5xx) stays transient.
func classifySchemaError(id int, err error) error {
	var regErr *Error
	if errors.As(err, &regErr) && (regErr.ErrorCode == 404 || regErr.ErrorCode/100 == 404) {
		return Permanent(fmt.Errorf("schema id %d not found in registry: %w", id, err))
	}
	return fmt.Errorf("fetching schema id %d: %w", id, err)
}
