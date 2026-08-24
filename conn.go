package protonats

import (
	"context"
	"fmt"
	"log"
	"runtime/debug"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"
)

const defaultTimeout = 5 * time.Second

// Handlers join a queue group named after their fully qualified proto method
// (e.g. "myapp.orders.OrderService.GetOrder") unless one is configured
// explicitly. The name must be unique per method, NOT a shared constant: NATS
// groups queue subscribers by queue name across every subject pattern matching
// the delivered subject, not per subscription subject. A service with both
// "orders.{order_id}" (subscribing "orders.*") and "orders.Create" would
// otherwise put both in one group, and roughly half of all Create requests
// would be delivered to the GetOrder handler instead.
//
// Deriving the name per method gives the right behavior on every axis:
// instances of one method load-balance, distinct methods never steal from each
// other, and separate services consuming the same event each keep their copy.

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

	jsOnce sync.Once
	js     jetstream.JetStream
	jsErr  error
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

// WithClientInterceptor adds a client interceptor to the chain. Interceptors
// run in registration order around every Request, Publish, and PublishJetStream.
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

// JetStream returns the JetStream context for the underlying connection,
// creating it on first use.
func (pn *Conn) JetStream() (jetstream.JetStream, error) {
	pn.jsOnce.Do(func() {
		pn.js, pn.jsErr = jetstream.New(pn.nc)
		if pn.jsErr != nil {
			pn.jsErr = fmt.Errorf("protonats: jetstream: %w", pn.jsErr)
		}
	})
	return pn.js, pn.jsErr
}

func (pn *Conn) logf(format string, v ...any) {
	if pn.logger != nil {
		pn.logger.Printf(format, v...)
	}
}

// Request performs a request/reply call with proto serialization and error
// header handling. The context is used for cancellation and deadline
// propagation; without a deadline the default timeout applies.
func (pn *Conn) Request(ctx context.Context, subject string, req, resp proto.Message, opts ...CallOption) error {
	co := applyCallOptions(opts)
	if co.subject != "" {
		subject = co.subject
	}

	result, err := invokeClientChain(ctx, pn.clientInterceptors, co.methodOr(subject), subject, req,
		func(ctx context.Context, subject string, req proto.Message) (proto.Message, error) {
			if err := pn.doRequest(ctx, subject, req, resp, co); err != nil {
				return nil, err
			}
			return resp, nil
		})
	if err != nil {
		return err
	}
	return adoptResponse(resp, result)
}

// adoptResponse copies an interceptor-supplied response into the caller's
// message. An interceptor that short-circuits the chain (cache hit, mock,
// circuit-breaker fallback) returns its own message instead of the one
// doRequest filled in, so without this the caller is left holding a zero value.
func adoptResponse(dst, src proto.Message) error {
	if src == nil || src == dst {
		return nil
	}
	srcDesc, dstDesc := src.ProtoReflect().Descriptor(), dst.ProtoReflect().Descriptor()
	if srcDesc != dstDesc {
		return fmt.Errorf("protonats: interceptor returned %s, want %s", srcDesc.FullName(), dstDesc.FullName())
	}
	proto.Reset(dst)
	proto.Merge(dst, src)
	return nil
}

func (pn *Conn) doRequest(ctx context.Context, subject string, req, resp proto.Message, co callOptions) error {
	data, err := pn.codec.Marshal(req)
	if err != nil {
		return fmt.Errorf("protonats: marshal request: %w", err)
	}

	ctx, cancel := pn.withTimeout(ctx, co)
	defer cancel()

	reply, err := pn.nc.RequestMsgWithContext(ctx, &nats.Msg{Subject: subject, Data: data, Header: co.headers})
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

// withTimeout applies the default or per-call timeout as a context deadline.
// An explicit WithTimeout narrows an existing deadline; otherwise an existing
// deadline is left alone.
func (pn *Conn) withTimeout(ctx context.Context, co callOptions) (context.Context, context.CancelFunc) {
	timeout := pn.defaultTimeout
	if co.timeout != nil {
		timeout = *co.timeout
	}
	if _, hasDeadline := ctx.Deadline(); hasDeadline && co.timeout == nil {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, timeout)
}

// Publish sends a fire-and-forget message with proto serialization.
// Returns an error if the context is already cancelled.
func (pn *Conn) Publish(ctx context.Context, subject string, req proto.Message, opts ...CallOption) error {
	co := applyCallOptions(opts)
	if co.subject != "" {
		subject = co.subject
	}

	_, err := invokeClientChain(ctx, pn.clientInterceptors, co.methodOr(subject), subject, req,
		func(ctx context.Context, subject string, req proto.Message) (proto.Message, error) {
			if err := ctx.Err(); err != nil {
				return nil, fmt.Errorf("protonats: publish to %s: %w", subject, err)
			}

			data, err := pn.codec.Marshal(req)
			if err != nil {
				return nil, fmt.Errorf("protonats: marshal: %w", err)
			}

			if err := pn.nc.PublishMsg(&nats.Msg{Subject: subject, Data: data, Header: co.headers}); err != nil {
				return nil, fmt.Errorf("protonats: publish to %s: %w", subject, err)
			}
			return nil, nil
		})
	return err
}

// PublishJetStream publishes to a JetStream stream and waits for the ack.
// The default or per-call timeout bounds the ack wait.
func (pn *Conn) PublishJetStream(ctx context.Context, subject string, req proto.Message, opts ...CallOption) (*jetstream.PubAck, error) {
	co := applyCallOptions(opts)
	if co.subject != "" {
		subject = co.subject
	}

	js, err := pn.JetStream()
	if err != nil {
		return nil, err
	}

	var ack *jetstream.PubAck
	_, err = invokeClientChain(ctx, pn.clientInterceptors, co.methodOr(subject), subject, req,
		func(ctx context.Context, subject string, req proto.Message) (proto.Message, error) {
			data, err := pn.codec.Marshal(req)
			if err != nil {
				return nil, fmt.Errorf("protonats: marshal: %w", err)
			}

			ctx, cancel := pn.withTimeout(ctx, co)
			defer cancel()

			ack, err = js.PublishMsg(ctx, &nats.Msg{Subject: subject, Data: data, Header: co.headers})
			if err != nil {
				return nil, fmt.Errorf("protonats: jetstream publish to %s: %w", subject, err)
			}
			return nil, nil
		})
	if err != nil {
		return nil, err
	}
	if ack == nil {
		// An interceptor returned without calling next. There is no ack to
		// report, and callers dereference the result, so fail loudly.
		return nil, fmt.Errorf("protonats: jetstream publish to %s short-circuited by an interceptor: no PubAck", subject)
	}
	return ack, nil
}

// replyError publishes an error reply. No-op when there is no reply subject.
func (pn *Conn) replyError(replySubject string, err error) {
	if replySubject == "" {
		return
	}
	msg := &nats.Msg{Subject: replySubject, Header: nats.Header{}}
	setErrorHeaders(msg.Header, err)
	if perr := pn.nc.PublishMsg(msg); perr != nil {
		pn.logf("protonats: error reply to %s failed: %v", replySubject, perr)
	}
}

// Subscribe creates a request/reply subscription with proto deserialization
// and handler interceptors. Used by generated registration code.
func (pn *Conn) Subscribe(method, subject string, ho HandlerOptions, newMsg func() proto.Message, handle HandlerInvoker) (*nats.Subscription, error) {
	cb := func(m *nats.Msg) {
		defer func() {
			if r := recover(); r != nil {
				pn.logf("protonats: panic handling %s: %v\n%s", m.Subject, r, debug.Stack())
				pn.replyError(m.Reply, Errorf(500, "internal error"))
			}
		}()

		ctx := handlerContext(m)
		req := newMsg()
		if err := pn.codec.Unmarshal(m.Data, req); err != nil {
			pn.replyError(m.Reply, Errorf(400, "invalid request: %v", err))
			return
		}

		result, err := invokeHandlerChain(ctx, ho.interceptors, method, m.Subject, req, handle)
		if m.Reply == "" {
			if err != nil {
				pn.logf("protonats: handler error on %s: %v", m.Subject, err)
			}
			return
		}
		if err != nil {
			pn.replyError(m.Reply, err)
			return
		}

		reply := &nats.Msg{Subject: m.Reply}
		if result != nil {
			data, merr := pn.codec.Marshal(result)
			if merr != nil {
				pn.replyError(m.Reply, Errorf(500, "marshal response: %v", merr))
				return
			}
			reply.Data = data
		}
		if perr := pn.nc.PublishMsg(reply); perr != nil {
			pn.logf("protonats: reply on %s failed: %v", m.Subject, perr)
		}
	}

	return pn.subscribe(subject, ho.queueGroupOr(method), cb)
}

// SubscribePublish creates a fire-and-forget subscription with proto
// deserialization and handler interceptors. Used by generated registration code.
func (pn *Conn) SubscribePublish(method, subject string, ho HandlerOptions, newMsg func() proto.Message, handle func(context.Context, proto.Message) error) (*nats.Subscription, error) {
	cb := func(m *nats.Msg) {
		defer func() {
			if r := recover(); r != nil {
				pn.logf("protonats: panic handling %s: %v\n%s", m.Subject, r, debug.Stack())
			}
		}()

		ctx := handlerContext(m)
		req := newMsg()
		if err := pn.codec.Unmarshal(m.Data, req); err != nil {
			pn.logf("protonats: unmarshal error on %s: %v", m.Subject, err)
			return
		}

		_, err := invokeHandlerChain(ctx, ho.interceptors, method, m.Subject, req,
			func(ctx context.Context, req proto.Message) (proto.Message, error) {
				return nil, handle(ctx, req)
			})
		if err != nil {
			pn.logf("protonats: handler error on %s: %v", m.Subject, err)
		}
	}

	return pn.subscribe(subject, ho.queueGroupOr(method), cb)
}

func (pn *Conn) subscribe(subject, queueGroup string, cb nats.MsgHandler) (*nats.Subscription, error) {
	if queueGroup != "" {
		return pn.nc.QueueSubscribe(subject, queueGroup, cb)
	}
	return pn.nc.Subscribe(subject, cb)
}

func handlerContext(m *nats.Msg) context.Context {
	ctx := contextWithSubject(context.Background(), m.Subject)
	return contextWithMsg(ctx, m)
}
