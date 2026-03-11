package protonats

import (
	"context"
	"fmt"
	"log"
	"runtime/debug"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

const defaultTimeout = 5 * time.Second

// Logger defines the interface for logging within protonats.
type Logger interface {
	Printf(format string, v ...any)
}

// stdLogger wraps the standard log package.
type stdLogger struct{}

func (stdLogger) Printf(format string, v ...any) { log.Printf(format, v...) }

// Conn wraps a *nats.Conn with protobuf serialization, error handling, and interceptors.
type Conn struct {
	nc                 *nats.Conn
	codec              Codec
	defaultTimeout     time.Duration
	clientInterceptors []ClientInterceptor
	logger             Logger
}

// Option configures a Conn.
type Option func(*Conn)

// New creates a ProtoNats connection wrapping an existing NATS connection.
// The caller owns the *nats.Conn lifecycle.
func New(nc *nats.Conn, opts ...Option) (*Conn, error) {
	if nc == nil {
		return nil, fmt.Errorf("protonats: nats connection is nil")
	}
	pn := &Conn{
		nc:             nc,
		codec:          ProtoCodec{},
		defaultTimeout: defaultTimeout,
		logger:         stdLogger{},
	}
	for _, o := range opts {
		o(pn)
	}
	return pn, nil
}

// WithCodec sets the serialization codec.
func WithCodec(c Codec) Option {
	return func(pn *Conn) { pn.codec = c }
}

// WithDefaultTimeout sets the default request timeout.
func WithDefaultTimeout(d time.Duration) Option {
	return func(pn *Conn) { pn.defaultTimeout = d }
}

// WithClientInterceptor adds a client interceptor to the chain.
func WithClientInterceptor(i ClientInterceptor) Option {
	return func(pn *Conn) { pn.clientInterceptors = append(pn.clientInterceptors, i) }
}

// WithLogger sets a custom logger. Pass nil to disable logging.
func WithLogger(l Logger) Option {
	return func(pn *Conn) { pn.logger = l }
}

// NatsConn returns the underlying *nats.Conn for direct NATS access.
func (pn *Conn) NatsConn() *nats.Conn { return pn.nc }

// Codec returns the configured codec.
func (pn *Conn) Codec() Codec { return pn.codec }

// Request performs a request/reply call with proto serialization and error header handling.
// The context is used for cancellation and deadline propagation.
func (pn *Conn) Request(ctx context.Context, subject string, req, resp proto.Message, opts ...CallOption) error {
	co := applyCallOptions(opts)
	if co.subject != "" {
		subject = co.subject
	}

	data, err := pn.codec.Marshal(req)
	if err != nil {
		return fmt.Errorf("protonats: marshal request: %w", err)
	}

	msg := &nats.Msg{Subject: subject, Data: data}
	if co.headers != nil {
		msg.Header = co.headers
	}

	// Apply timeout as a context deadline if the context doesn't already have one,
	// or if an explicit timeout was provided via WithTimeout.
	timeout := pn.defaultTimeout
	if co.timeout != nil {
		timeout = *co.timeout
	}
	if _, hasDeadline := ctx.Deadline(); !hasDeadline || co.timeout != nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	reply, err := pn.nc.RequestMsgWithContext(ctx, msg)
	if err != nil {
		return fmt.Errorf("protonats: request to %s: %w", subject, err)
	}

	if herr := errorFromHeaders(reply.Header); herr != nil {
		return herr
	}

	if err := pn.codec.Unmarshal(reply.Data, resp); err != nil {
		return fmt.Errorf("protonats: unmarshal response: %w", err)
	}
	return nil
}

// Publish sends a fire-and-forget message with proto serialization.
// Returns an error if the context is already cancelled.
func (pn *Conn) Publish(ctx context.Context, subject string, req proto.Message, opts ...CallOption) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("protonats: publish to %s: %w", subject, err)
	}

	co := applyCallOptions(opts)
	if co.subject != "" {
		subject = co.subject
	}

	data, err := pn.codec.Marshal(req)
	if err != nil {
		return fmt.Errorf("protonats: marshal: %w", err)
	}

	msg := &nats.Msg{Subject: subject, Data: data}
	if co.headers != nil {
		msg.Header = co.headers
	}

	if err := pn.nc.PublishMsg(msg); err != nil {
		return fmt.Errorf("protonats: publish to %s: %w", subject, err)
	}
	return nil
}

// Subscribe creates a request/reply subscription with proto deserialization.
// Used by generated registration code.
func (pn *Conn) Subscribe(subject, queueGroup string, newMsg func() proto.Message, handler func(context.Context, *nats.Msg, proto.Message) (proto.Message, error)) (*nats.Subscription, error) {
	cb := func(m *nats.Msg) {
		defer func() {
			if r := recover(); r != nil {
				if pn.logger != nil {
					pn.logger.Printf("protonats: panic handling %s: %v\n%s", m.Subject, r, debug.Stack())
				}
				if m.Reply != "" {
					resp := &nats.Msg{Subject: m.Reply, Header: nats.Header{}}
					setErrorHeaders(resp.Header, Errorf(500, "internal error"))
					_ = pn.nc.PublishMsg(resp)
				}
			}
		}()

		ctx := context.Background()
		ctx = contextWithSubject(ctx, m.Subject)
		ctx = contextWithMsg(ctx, m)

		req := newMsg()
		if err := pn.codec.Unmarshal(m.Data, req); err != nil {
			if m.Reply != "" {
				resp := &nats.Msg{Subject: m.Reply, Header: nats.Header{}}
				setErrorHeaders(resp.Header, Errorf(400, "invalid request: %v", err))
				_ = pn.nc.PublishMsg(resp)
			}
			return
		}

		result, err := handler(ctx, m, req)
		if m.Reply == "" {
			return
		}

		reply := &nats.Msg{Subject: m.Reply, Header: nats.Header{}}
		if err != nil {
			setErrorHeaders(reply.Header, err)
			_ = pn.nc.PublishMsg(reply)
			return
		}

		if result != nil {
			data, merr := pn.codec.Marshal(result)
			if merr != nil {
				setErrorHeaders(reply.Header, Errorf(500, "marshal response: %v", merr))
				_ = pn.nc.PublishMsg(reply)
				return
			}
			reply.Data = data
		}
		_ = pn.nc.PublishMsg(reply)
	}

	if queueGroup != "" {
		return pn.nc.QueueSubscribe(subject, queueGroup, cb)
	}
	return pn.nc.Subscribe(subject, cb)
}

// SubscribePublish creates a fire-and-forget subscription with proto deserialization.
// Used by generated registration code.
func (pn *Conn) SubscribePublish(subject, queueGroup string, newMsg func() proto.Message, handler func(context.Context, proto.Message) error) (*nats.Subscription, error) {
	cb := func(m *nats.Msg) {
		defer func() {
			if r := recover(); r != nil {
				if pn.logger != nil {
					pn.logger.Printf("protonats: panic handling %s: %v\n%s", m.Subject, r, debug.Stack())
				}
			}
		}()

		ctx := context.Background()
		ctx = contextWithSubject(ctx, m.Subject)
		ctx = contextWithMsg(ctx, m)

		req := newMsg()
		if err := pn.codec.Unmarshal(m.Data, req); err != nil {
			if pn.logger != nil {
				pn.logger.Printf("protonats: unmarshal error on %s: %v", m.Subject, err)
			}
			return
		}
		if err := handler(ctx, req); err != nil {
			if pn.logger != nil {
				pn.logger.Printf("protonats: handler error on %s: %v", m.Subject, err)
			}
		}
	}

	if queueGroup != "" {
		return pn.nc.QueueSubscribe(subject, queueGroup, cb)
	}
	return pn.nc.Subscribe(subject, cb)
}
