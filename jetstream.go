package protonats

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"
)

// ConsumeConfig identifies the stream and consumer for a JETSTREAM_CONSUME
// method. Populated by generated registration code from proto options.
type ConsumeConfig struct {
	// Method is the fully qualified proto method name, e.g.
	// "myapp.orders.OrderService.ProcessOrderEvent".
	Method string
	// Subject the consumer filters on (template tokens become wildcards).
	Subject string
	// Stream is the JetStream stream name.
	Stream string
	// Consumer is the durable consumer name; empty means ephemeral.
	Consumer string
}

// ConsumeJetStream starts consuming a JetStream consumer, decoding each
// message and applying handler interceptors. Ack semantics on handler return:
// nil acks, ErrTerminate terminates (no redelivery), any other error naks.
// A handler that acked explicitly can return nil; the redundant ack is ignored.
// Used by generated registration code.
func (pn *Conn) ConsumeJetStream(cfg ConsumeConfig, ho HandlerOptions, newMsg func() proto.Message, handle func(context.Context, proto.Message, Acker) error) (jetstream.ConsumeContext, error) {
	js, err := pn.JetStream()
	if err != nil {
		return nil, err
	}

	// Registration has no caller context; bound consumer setup by the
	// connection's default timeout.
	setupCtx, cancel := context.WithTimeout(context.Background(), pn.defaultTimeout)
	defer cancel()

	cons, err := pn.resolveConsumer(setupCtx, js, cfg, ho)
	if err != nil {
		return nil, fmt.Errorf("protonats: consumer for %s: %w", cfg.Method, err)
	}

	return cons.Consume(func(m jetstream.Msg) {
		defer func() {
			if r := recover(); r != nil {
				pn.logf("protonats: panic consuming %s: %v", m.Subject(), r)
				_ = m.Nak()
			}
		}()

		req := newMsg()
		if err := pn.codec.Unmarshal(m.Data(), req); err != nil {
			// A payload that cannot decode never will; terminate instead of
			// redelivering forever.
			pn.logf("protonats: unmarshal error on %s, terminating message: %v", m.Subject(), err)
			_ = m.Term()
			return
		}

		ctx := contextWithSubject(context.Background(), m.Subject())
		_, err := invokeHandlerChain(ctx, ho.interceptors, cfg.Method, m.Subject(), req,
			func(ctx context.Context, req proto.Message) (proto.Message, error) {
				return nil, handle(ctx, req, m)
			})

		switch {
		case errors.Is(err, ErrTerminate):
			_ = m.Term()
		case err != nil:
			pn.logf("protonats: handler error on %s: %v", m.Subject(), err)
			_ = m.Nak()
		default:
			// Redundant after an explicit handler ack; that error is expected.
			_ = m.Ack()
		}
	})
}

// resolveConsumer binds to or creates the consumer for a consume method.
// Priority: an explicit WithConsumerConfig override, then an existing durable
// consumer (so operator-tuned settings are never clobbered), then a created
// durable or ephemeral consumer with defaults.
func (pn *Conn) resolveConsumer(ctx context.Context, js jetstream.JetStream, cfg ConsumeConfig, ho HandlerOptions) (jetstream.Consumer, error) {
	if override, ok := ho.consumerConfigs[shortMethodName(cfg.Method)]; ok {
		if override.Durable == "" && override.Name == "" {
			override.Durable = cfg.Consumer
		}
		if override.FilterSubject == "" && len(override.FilterSubjects) == 0 {
			override.FilterSubject = cfg.Subject
		}
		return js.CreateOrUpdateConsumer(ctx, cfg.Stream, override)
	}

	defaults := jetstream.ConsumerConfig{
		Durable:       cfg.Consumer,
		FilterSubject: cfg.Subject,
		AckPolicy:     jetstream.AckExplicitPolicy,
	}

	if cfg.Consumer == "" {
		return js.CreateConsumer(ctx, cfg.Stream, defaults)
	}

	cons, err := js.Consumer(ctx, cfg.Stream, cfg.Consumer)
	if errors.Is(err, jetstream.ErrConsumerNotFound) {
		return js.CreateConsumer(ctx, cfg.Stream, defaults)
	}
	return cons, err
}

// shortMethodName reduces "pkg.Service.Method" to "Method", the key
// WithConsumerConfig uses.
func shortMethodName(full string) string {
	if i := strings.LastIndex(full, "."); i >= 0 {
		return full[i+1:]
	}
	return full
}
