package protonats

import (
	"time"

	"github.com/nats-io/nats.go"
)

// CallOption configures a single client call.
type CallOption func(*callOptions)

type callOptions struct {
	timeout *time.Duration
	headers nats.Header
	subject string
	method  string
}

func applyCallOptions(opts []CallOption) callOptions {
	var co callOptions
	for _, o := range opts {
		o(&co)
	}
	return co
}

// methodOr returns the method name reported to client interceptors,
// falling back to the given subject for calls made directly on Conn.
func (co callOptions) methodOr(subject string) string {
	if co.method != "" {
		return co.method
	}
	return subject
}

// WithTimeout overrides the request timeout for this call.
func WithTimeout(d time.Duration) CallOption {
	return func(o *callOptions) { o.timeout = &d }
}

// WithHeaders adds NATS headers to this message.
func WithHeaders(h nats.Header) CallOption {
	return func(o *callOptions) { o.headers = h }
}

// WithSubject completely overrides the resolved subject for this call.
func WithSubject(s string) CallOption {
	return func(o *callOptions) { o.subject = s }
}

// WithMethodName sets the fully qualified method name reported to client
// interceptors. Set by generated code; rarely useful directly.
func WithMethodName(name string) CallOption {
	return func(o *callOptions) { o.method = name }
}
