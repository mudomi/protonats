package protonats

import (
	"sync"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Registration holds the active subscriptions and JetStream consumers for a
// registered handler.
type Registration struct {
	mu        sync.Mutex
	subs      []*nats.Subscription
	consumers []jetstream.ConsumeContext
}

// AddSubscription tracks a subscription for later cleanup. Called by generated code.
func (r *Registration) AddSubscription(sub *nats.Subscription) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.subs = append(r.subs, sub)
}

// AddConsumeContext tracks a JetStream consumer for later cleanup. Called by generated code.
func (r *Registration) AddConsumeContext(cc jetstream.ConsumeContext) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.consumers = append(r.consumers, cc)
}

// Drain starts a graceful stop of every subscription and consumer: each stops
// accepting new messages and finishes what it already has.
//
// It returns as soon as the drain is requested, without waiting for in-flight
// handlers. Before exiting the process, drain the underlying *nats.Conn too —
// that wait is what lets the handlers finish.
func (r *Registration) Drain() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var firstErr error
	for _, sub := range r.subs {
		if err := sub.Drain(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	for _, cc := range r.consumers {
		cc.Drain()
	}
	return firstErr
}

// Unsubscribe immediately stops all subscriptions and consumers.
func (r *Registration) Unsubscribe() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var firstErr error
	for _, sub := range r.subs {
		if err := sub.Unsubscribe(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	for _, cc := range r.consumers {
		cc.Stop()
	}
	return firstErr
}

// HandlerOption configures handler registration.
type HandlerOption func(*HandlerOptions)

// HandlerOptions holds resolved handler configuration. Constructed via
// ApplyHandlerOptions and passed through by generated code.
type HandlerOptions struct {
	queueGroup      *string
	interceptors    []HandlerInterceptor
	consumerConfigs map[string]jetstream.ConsumerConfig
}

// queueGroupOr returns the configured queue group, or def when unset.
func (ho HandlerOptions) queueGroupOr(def string) string {
	if ho.queueGroup != nil {
		return *ho.queueGroup
	}
	return def
}

// ApplyHandlerOptions resolves handler options. Called by generated code.
func ApplyHandlerOptions(opts []HandlerOption) HandlerOptions {
	var ho HandlerOptions
	for _, o := range opts {
		o(&ho)
	}
	return ho
}

// WithQueueGroup overrides the per-method default queue group, applying one
// name to every subscription in the registration.
//
// Only use this when you want every method of the service in a single group.
// If any two of its subjects can match the same message (a wildcard subject
// alongside a literal one), NATS delivers such a message to just one of them,
// so requests silently reach the wrong handler. The per-method default has no
// such hazard.
func WithQueueGroup(q string) HandlerOption {
	return func(o *HandlerOptions) { o.queueGroup = &q }
}

// WithNoQueueGroup disables queue groups for all handler subscriptions:
// every running instance receives every message, including request/reply
// (which then gets duplicate processing — use for broadcast-style handlers only).
func WithNoQueueGroup() HandlerOption {
	none := ""
	return func(o *HandlerOptions) { o.queueGroup = &none }
}

// WithHandlerInterceptor adds a handler interceptor.
func WithHandlerInterceptor(i HandlerInterceptor) HandlerOption {
	return func(o *HandlerOptions) { o.interceptors = append(o.interceptors, i) }
}

// WithConsumerConfig overrides the JetStream consumer configuration for one
// JETSTREAM_CONSUME method, identified by its Go method name (e.g.
// "ProcessOrderEvent"). The consumer is created or updated with this exact
// config; the proto-defined durable name and filter subject fill in when the
// config leaves them empty.
func WithConsumerConfig(method string, cfg jetstream.ConsumerConfig) HandlerOption {
	return func(o *HandlerOptions) {
		if o.consumerConfigs == nil {
			o.consumerConfigs = map[string]jetstream.ConsumerConfig{}
		}
		o.consumerConfigs[method] = cfg
	}
}
