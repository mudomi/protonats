package protonats

import (
	"sync"

	"github.com/nats-io/nats.go"
)

// Registration holds active subscriptions for a registered handler.
type Registration struct {
	mu   sync.Mutex
	subs []*nats.Subscription
}

// AddSubscription tracks a subscription for later cleanup.
// Called by generated code.
func (r *Registration) AddSubscription(sub *nats.Subscription) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.subs = append(r.subs, sub)
}

// Drain gracefully stops all subscriptions (finishes in-flight, stops accepting new).
func (r *Registration) Drain() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var firstErr error
	for _, sub := range r.subs {
		if err := sub.Drain(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Unsubscribe immediately stops all subscriptions.
func (r *Registration) Unsubscribe() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var firstErr error
	for _, sub := range r.subs {
		if err := sub.Unsubscribe(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// HandlerOption configures handler registration.
type HandlerOption func(*HandlerOptions)

// HandlerOptions holds resolved handler configuration. Exported for generated code.
type HandlerOptions struct {
	QueueGroup          string
	HandlerInterceptors []HandlerInterceptor
}

// ApplyHandlerOptions resolves handler options. Called by generated code.
func ApplyHandlerOptions(opts []HandlerOption) HandlerOptions {
	var ho HandlerOptions
	for _, o := range opts {
		o(&ho)
	}
	return ho
}

// WithQueueGroup sets the queue group for all handler subscriptions.
func WithQueueGroup(q string) HandlerOption {
	return func(o *HandlerOptions) { o.QueueGroup = q }
}

// WithHandlerInterceptor adds a handler interceptor.
func WithHandlerInterceptor(i HandlerInterceptor) HandlerOption {
	return func(o *HandlerOptions) { o.HandlerInterceptors = append(o.HandlerInterceptors, i) }
}
