package worker

import "errors"

// permanentError wraps a handler error to signal that retrying it can never
// succeed (e.g. a malformed payload), so the pool should mark the job
// StatusFailed immediately instead of scheduling a retry or, once retries
// are exhausted, dead-lettering it.
type permanentError struct {
	err error
}

// Permanent wraps err so that, when returned from a Handler, the pool marks
// the job StatusFailed right away and skips both retry scheduling and
// dead-lettering — even if the job has retries remaining. Use it when a
// handler can tell an error is not transient: no amount of retrying will
// make it succeed.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

func (e *permanentError) Error() string { return e.err.Error() }

func (e *permanentError) Unwrap() error { return e.err }

// IsPermanent reports whether err, or anything it wraps, was produced by
// Permanent. The pool uses it to decide a failed job's fate; it is
// exported so handler middleware -- something that logs, or retries, or
// converts errors around an inner handler -- can tell a permanent failure
// from a transient one without knowing how the marker is spelled.
func IsPermanent(err error) bool {
	var pe *permanentError
	return errors.As(err, &pe)
}
