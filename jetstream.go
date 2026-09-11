package protonats

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
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

// RollbackSuffix is appended to a task's subject and consumer name to derive
// its rollback half. The runtime and the code generator both derive the names
// through the two helpers below, so the two halves cannot disagree.
const RollbackSuffix = "rollback"

// RollbackSubjectFor returns the subject a failed task is republished to.
// "orders.Reserve" becomes "orders.Reserve.rollback"; a task with subject
// tokens rolls back on the matching concrete subject.
func RollbackSubjectFor(taskSubject string) string {
	return taskSubject + "." + RollbackSuffix
}

// RollbackConsumerFor returns the durable consumer name for a task's rollback.
// An empty task consumer stays empty: an ephemeral task has no durable
// rollback either.
func RollbackConsumerFor(taskConsumer string) string {
	if taskConsumer == "" {
		return ""
	}
	return taskConsumer + "-" + RollbackSuffix
}

// ConsumeJetStream starts consuming a JetStream consumer, decoding each
// message and applying handler interceptors. Ack semantics on handler return:
// nil acks, an error the retry policy terminates stops redelivery, any other
// error naks. A handler that acked explicitly can return nil; the redundant
// ack is ignored. Used by generated registration code.
func (pn *Conn) ConsumeJetStream(cfg ConsumeConfig, ho HandlerOptions, newMsg func() proto.Message, handle func(context.Context, proto.Message, Acker) error) (jetstream.ConsumeContext, error) {
	return pn.consume(cfg, ho, newMsg, noRollback, func(ctx context.Context, req proto.Message, m jetstream.Msg) error {
		return handle(ctx, req, m)
	})
}

// ConsumeTask is ConsumeJetStream plus a rollback: when the work is given up
// on, the message that triggered it is republished to its rollback subject
// with the cause attached, so the rollback handler can undo it. Used by
// generated registration code for the worker half of a JETSTREAM_TASK method.
func (pn *Conn) ConsumeTask(cfg ConsumeConfig, ho HandlerOptions, newMsg func() proto.Message, handle func(context.Context, proto.Message, Acker) error) (jetstream.ConsumeContext, error) {
	return pn.consume(cfg, ho, newMsg, withRollback, func(ctx context.Context, req proto.Message, m jetstream.Msg) error {
		return handle(ctx, req, m)
	})
}

// ConsumeRollback consumes the rollback side of a task, handing the handler
// the message that failed along with the error that ended it. Used by
// generated registration code for the rollback half of a JETSTREAM_TASK
// method.
//
// A rollback has no rollback of its own: when one is given up on, it is
// terminated and reported through WithOnTerminate, and undoing it is a matter
// for an operator.
func (pn *Conn) ConsumeRollback(cfg ConsumeConfig, ho HandlerOptions, newMsg func() proto.Message, handle func(context.Context, proto.Message, *Error, Acker) error) (jetstream.ConsumeContext, error) {
	return pn.consume(cfg, ho, newMsg, noRollback, func(ctx context.Context, req proto.Message, m jetstream.Msg) error {
		return handle(ctx, req, causeFromHeaders(m.Headers()), m)
	})
}

// consumeFunc is the internal handler shape: it sees the raw message, which
// the rollback path needs in order to read the cause off its headers.
type consumeFunc func(ctx context.Context, req proto.Message, m jetstream.Msg) error

// Whether a consumer publishes a rollback when it gives up. Named rather than
// a bare bool so the call sites read as intent.
const (
	noRollback   = false
	withRollback = true
)

func (pn *Conn) consume(cfg ConsumeConfig, ho HandlerOptions, newMsg func() proto.Message, rollback bool, handle consumeFunc) (jetstream.ConsumeContext, error) {
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

	// MaxDeliver is read once at registration to recognise the final attempt.
	// A consumer reconfigured underneath a running process keeps the value it
	// started with, which is the conservative direction: at worst the last
	// attempt is treated as retryable and JetStream drops it, exactly as it
	// would have without this.
	maxDeliver, backoff := 0, []time.Duration(nil)
	if info := cons.CachedInfo(); info != nil {
		maxDeliver, backoff = info.Config.MaxDeliver, info.Config.BackOff
	}

	policy := ho.retryPolicyOr()

	return cons.Consume(func(m jetstream.Msg) {
		defer func() {
			if r := recover(); r != nil {
				pn.logf("protonats: panic consuming %s: %v\n%s", m.Subject(), r, debug.Stack())
				_ = m.Nak()
			}
		}()

		req := newMsg()
		if err := pn.codec.Unmarshal(m.Data(), req); err != nil {
			// A payload that cannot decode never will, so there is nothing to
			// retry. There is also no decoded message to hand a rollback, so
			// this path terminates without one.
			pn.logf("protonats: unmarshal error on %s, terminating message: %v", m.Subject(), err)
			_ = m.Term()
			return
		}

		ctx := contextWithSubject(context.Background(), m.Subject())
		_, err := invokeHandlerChain(ctx, ho.interceptors, cfg.Method, m.Subject(), req,
			func(ctx context.Context, req proto.Message) (proto.Message, error) {
				return nil, handle(ctx, req, m)
			})

		if err == nil {
			// Redundant after an explicit handler ack; that error is expected.
			_ = m.Ack()
			return
		}

		if pn.dispositionOf(err, m, policy, maxDeliver) == Retry {
			pn.logf("protonats: handler error on %s, retrying: %v", m.Subject(), err)
			pn.nak(m, backoff)
			return
		}

		pn.giveUp(ctx, cfg, rollback, ho, m, req, err)
	})
}

// nak returns the message for redelivery, spaced by the consumer's configured
// BackOff. A plain Nak asks for redelivery *now*: JetStream applies BackOff to
// an ack that never arrives, not to one that explicitly says "try again", so
// naking a handler that fails fast would burn every attempt in milliseconds
// and defeat the configured backoff entirely.
func (pn *Conn) nak(m jetstream.Msg, backoff []time.Duration) {
	if len(backoff) == 0 {
		_ = m.Nak()
		return
	}
	meta, err := m.Metadata()
	if err != nil {
		_ = m.Nak()
		return
	}
	_ = m.NakWithDelay(backoffFor(backoff, meta.NumDelivered))
}

// backoffFor picks the wait after a given number of deliveries, holding at the
// last entry once the list runs out — the same way JetStream itself walks it.
func backoffFor(backoff []time.Duration, numDelivered uint64) time.Duration {
	i := int(numDelivered) - 1
	if i < 0 {
		i = 0
	}
	if i >= len(backoff) {
		i = len(backoff) - 1
	}
	return backoff[i]
}

// dispositionOf applies the retry policy, then upgrades a retry to a terminate
// on the consumer's final attempt. Without that, a message whose handler keeps
// failing is dropped by JetStream when MaxDeliver runs out — silently, and
// without ever running a rollback.
func (pn *Conn) dispositionOf(err error, m jetstream.Msg, policy RetryPolicy, maxDeliver int) Disposition {
	if policy(err) == Terminate {
		return Terminate
	}
	if maxDeliver <= 0 {
		return Retry // unlimited redelivery; there is no final attempt
	}
	meta, metaErr := m.Metadata()
	if metaErr != nil {
		// Without metadata the attempt count is unknown. Retrying is the safe
		// guess: JetStream still enforces MaxDeliver.
		pn.logf("protonats: metadata unavailable on %s: %v", m.Subject(), metaErr)
		return Retry
	}
	if meta.NumDelivered >= uint64(maxDeliver) {
		return Terminate
	}
	return Retry
}

// giveUp runs the terminal callback, hands the message to its rollback if it
// has one, and only then terminates it.
func (pn *Conn) giveUp(ctx context.Context, cfg ConsumeConfig, rollback bool, ho HandlerOptions, m jetstream.Msg, req proto.Message, cause error) {
	pn.logf("protonats: giving up on %s: %v", m.Subject(), cause)

	if ho.onTerminate != nil {
		ho.onTerminate(ctx, cfg.Method, m.Subject(), req, cause)
	}

	if !rollback {
		_ = m.Term()
		return
	}

	// Publish the rollback before terminating. The reverse order would lose
	// the rollback entirely if this process died in between, whereas failing
	// here leaves the message to be redelivered and tried again. That makes
	// rollbacks at-least-once, so rollback handlers must be idempotent — the
	// Nats-Msg-Id below lets JetStream collapse the common duplicate.
	if err := pn.publishRollback(ctx, cfg, m, cause); err != nil {
		pn.logf("protonats: rollback publish for %s failed, leaving message for redelivery: %v", cfg.Method, err)
		_ = m.Nak()
		return
	}
	_ = m.Term()
}

func (pn *Conn) publishRollback(ctx context.Context, cfg ConsumeConfig, m jetstream.Msg, cause error) error {
	js, err := pn.JetStream()
	if err != nil {
		return err
	}

	header := nats.Header{}
	setErrorHeaders(header, cause)
	header.Set(headerRollbackOf, cfg.Method)

	// Deduplicate on the failed message's stream sequence, which is stable
	// across its redeliveries, so a redelivered original cannot enqueue a
	// second rollback within the stream's duplicate window.
	if meta, err := m.Metadata(); err == nil {
		header.Set(nats.MsgIdHdr, fmt.Sprintf("%s.rollback.%d", cfg.Method, meta.Sequence.Stream))
		header.Set(headerRollbackAttempts, strconv.FormatUint(meta.NumDelivered, 10))
	}

	ctx, cancel := context.WithTimeout(ctx, pn.defaultTimeout)
	defer cancel()

	// Derived from the concrete subject the message arrived on, so a task with
	// dynamic subject tokens rolls back on the matching concrete subject.
	subject := RollbackSubjectFor(m.Subject())

	_, err = js.PublishMsg(ctx, &nats.Msg{Subject: subject, Data: m.Data(), Header: header})
	return err
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
