package protonats

import "errors"

// Disposition is what happens to a JetStream message whose handler returned an
// error: retry it, or give up on it.
type Disposition int

const (
	// Retry naks the message so JetStream redelivers it.
	Retry Disposition = iota
	// Terminate stops redelivery. For a task method this is also what triggers
	// the rollback.
	Terminate
)

func (d Disposition) String() string {
	if d == Terminate {
		return "terminate"
	}
	return "retry"
}

// RetryPolicy decides whether a handler error is worth retrying. It is only
// consulted for non-nil errors.
type RetryPolicy func(err error) Disposition

// DefaultRetryPolicy retries everything except an explicit ErrTerminate.
// Combined with a consumer's MaxDeliver this is at-least-once with a bounded
// number of attempts; on a consumer with unlimited MaxDeliver (the JetStream
// default) a permanently failing handler retries forever, which is why
// TerminateOnClientError exists.
func DefaultRetryPolicy(err error) Disposition {
	if errors.Is(err, ErrTerminate) {
		return Terminate
	}
	return Retry
}

// TerminateOnClientError additionally gives up on any *Error with a 4xx code.
// Those describe a request that is wrong rather than a system that is
// struggling — a malformed payload or an unknown id will fail identically on
// every redelivery, so retrying it only delays the rollback.
func TerminateOnClientError(err error) Disposition {
	var pnErr *Error
	if errors.As(err, &pnErr) && pnErr.Code >= 400 && pnErr.Code < 500 {
		return Terminate
	}
	return DefaultRetryPolicy(err)
}

// retryPolicyOr returns the configured policy, or DefaultRetryPolicy.
func (ho HandlerOptions) retryPolicyOr() RetryPolicy {
	if ho.retryPolicy != nil {
		return ho.retryPolicy
	}
	return DefaultRetryPolicy
}
