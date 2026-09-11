// Package e2e exercises the checked-in generated code (internal/gen/testdata/gen)
// against a real NATS server, covering every supported communication pattern.
// Tests skip when NATS is unreachable; JetStream tests also need -js enabled.
package e2e

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	protonats "github.com/mudomi/protonats"
	testgen "github.com/mudomi/protonats/internal/gen/testdata/gen"
	"github.com/mudomi/protonats/internal/natstest"
)

func connect(t *testing.T) *protonats.Conn {
	t.Helper()
	pn, err := protonats.New(natstest.Connect(t))
	require.NoError(t, err)
	return pn
}

// freshEventsStream (re)creates the EVENTS stream test.proto's JetStream
// methods reference, skipping the test when JetStream is disabled.
//
// The stream name comes from the proto, so every test here shares it and none
// of them may call t.Parallel(): they would delete each other's stream.
func freshEventsStream(t *testing.T, pn *protonats.Conn) jetstream.JetStream {
	t.Helper()
	js, err := pn.JetStream()
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	if _, err := js.AccountInfo(ctx); err != nil {
		natstest.Unavailable(t, "JetStream not available: %v", err)
	}

	_ = js.DeleteStream(ctx, "EVENTS")
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name: "EVENTS",
		// ChargeCard's rollback subject is derived, so the stream has to
		// capture it too — hence the wildcard rather than a literal.
		Subjects: []string{"testpkg.EmitEvent", "testpkg.ProcessEvent", "testpkg.ChargeCard", "testpkg.ChargeCard.>"},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = js.DeleteStream(ctx, "EVENTS")
	})
	return js
}

// handler implements TestServiceHandler with pluggable functions.
type handler struct {
	testgen.UnimplementedTestServiceHandler
	echo    func(context.Context, *testgen.EchoRequest) (*testgen.EchoResponse, error)
	getItem func(context.Context, *testgen.GetItemRequest) (*testgen.Item, error)
	notify  func(context.Context, *testgen.NotifyRequest) error
	process func(context.Context, *testgen.EventPayload, protonats.Acker) error
}

func (h *handler) Echo(ctx context.Context, req *testgen.EchoRequest) (*testgen.EchoResponse, error) {
	return h.echo(ctx, req)
}

func (h *handler) GetItem(ctx context.Context, req *testgen.GetItemRequest) (*testgen.Item, error) {
	return h.getItem(ctx, req)
}

func (h *handler) Notify(ctx context.Context, req *testgen.NotifyRequest) error {
	return h.notify(ctx, req)
}

func (h *handler) ProcessEvent(ctx context.Context, req *testgen.EventPayload, ack protonats.Acker) error {
	return h.process(ctx, req, ack)
}

func register(t *testing.T, pn *protonats.Conn, h testgen.TestServiceHandler, opts ...protonats.HandlerOption) *protonats.Registration {
	t.Helper()
	reg, err := testgen.RegisterTestServiceHandler(pn, h, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reg.Unsubscribe() })
	return reg
}

func echoOK(_ context.Context, req *testgen.EchoRequest) (*testgen.EchoResponse, error) {
	return &testgen.EchoResponse{Message: req.Message + " back"}, nil
}

func TestRequestReply_Generated(t *testing.T) {
	pn := connect(t)
	freshEventsStream(t, pn)
	register(t, pn, &handler{
		echo: echoOK,
		process: func(_ context.Context, _ *testgen.EventPayload, _ protonats.Acker) error {
			return nil
		},
	})

	client := testgen.NewTestServiceClient(pn)
	resp, err := client.Echo(context.Background(), &testgen.EchoRequest{Message: "hi"})
	require.NoError(t, err)
	assert.Equal(t, "hi back", resp.Message)
}

func TestRequestReply_NoJetStreamService(t *testing.T) {
	// PrefixedService has no JetStream methods, so it registers without a stream.
	pn := connect(t)

	type pingHandler struct {
		testgen.UnimplementedPrefixedServiceHandler
	}
	reg, err := testgen.RegisterPrefixedServiceHandler(pn, &pingHandler{})
	require.NoError(t, err)
	defer reg.Unsubscribe()

	client := testgen.NewPrefixedServiceClient(pn)
	_, err = client.Ping(context.Background(), &testgen.PingRequest{})

	// Unimplemented handlers respond with 501.
	var pnErr *protonats.Error
	require.ErrorAs(t, err, &pnErr)
	assert.Equal(t, 501, pnErr.Code)
}

func TestDynamicSubject_Generated(t *testing.T) {
	pn := connect(t)
	freshEventsStream(t, pn)

	var gotSubject string
	register(t, pn, &handler{
		getItem: func(ctx context.Context, req *testgen.GetItemRequest) (*testgen.Item, error) {
			gotSubject = protonats.SubjectFromContext(ctx)
			return &testgen.Item{ItemId: req.ItemId, Name: "widget"}, nil
		},
		process: func(_ context.Context, _ *testgen.EventPayload, _ protonats.Acker) error {
			return nil
		},
	})

	client := testgen.NewTestServiceClient(pn)
	item, err := client.GetItem(context.Background(), &testgen.GetItemRequest{ItemId: "abc123"})
	require.NoError(t, err)
	assert.Equal(t, "abc123", item.ItemId)
	assert.Equal(t, "items.abc123", gotSubject)
}

// Token values are interpolated into the subject verbatim — nothing escapes or
// rejects them. This pins how each hazardous value actually behaves, which is
// what docs/proto-definition.md promises callers.
func TestDynamicSubject_HazardousTokenValues(t *testing.T) {
	pn := connect(t)
	freshEventsStream(t, pn)

	var seenSubject atomic.Value
	seenSubject.Store("")
	register(t, pn, &handler{
		getItem: func(ctx context.Context, req *testgen.GetItemRequest) (*testgen.Item, error) {
			seenSubject.Store(protonats.SubjectFromContext(ctx))
			return &testgen.Item{ItemId: req.ItemId}, nil
		},
		process: func(_ context.Context, _ *testgen.EventPayload, _ protonats.Acker) error {
			return nil
		},
	})
	client := testgen.NewTestServiceClient(pn)

	// A "." adds a token, so the subject no longer matches "items.*" and the
	// call fails loudly instead of reaching some other handler.
	_, err := client.GetItem(context.Background(), &testgen.GetItemRequest{ItemId: "a.b"},
		protonats.WithTimeout(time.Second))
	require.Error(t, err)
	assert.ErrorIs(t, err, nats.ErrNoResponders, "a dotted id must not silently reach a handler")

	// An empty id leaves a trailing empty token, which fails the same way.
	_, err = client.GetItem(context.Background(), &testgen.GetItemRequest{ItemId: ""},
		protonats.WithTimeout(time.Second))
	require.Error(t, err)
	assert.ErrorIs(t, err, nats.ErrNoResponders, "an empty id must not silently reach a handler")

	// A wildcard character is NOT a wildcard here: NATS treats it as a literal
	// token, so the call succeeds and the handler sees it as the id. Callers
	// must validate ids themselves — this is the case that does not fail.
	seenSubject.Store("")
	item, err := client.GetItem(context.Background(), &testgen.GetItemRequest{ItemId: "*"},
		protonats.WithTimeout(2*time.Second))
	require.NoError(t, err)
	assert.Equal(t, "*", item.ItemId)
	assert.Equal(t, "items.*", seenSubject.Load(), "the wildcard arrives as a literal subject token")
}

func TestErrorPropagation_Generated(t *testing.T) {
	pn := connect(t)
	freshEventsStream(t, pn)
	register(t, pn, &handler{
		echo: func(_ context.Context, _ *testgen.EchoRequest) (*testgen.EchoResponse, error) {
			return nil, protonats.Errorf(404, "gone")
		},
		process: func(_ context.Context, _ *testgen.EventPayload, _ protonats.Acker) error {
			return nil
		},
	})

	client := testgen.NewTestServiceClient(pn)
	_, err := client.Echo(context.Background(), &testgen.EchoRequest{Message: "x"})

	var pnErr *protonats.Error
	require.ErrorAs(t, err, &pnErr)
	assert.Equal(t, 404, pnErr.Code)
	assert.Equal(t, "gone", pnErr.Message)
}

func TestPublish_Generated(t *testing.T) {
	pn := connect(t)
	freshEventsStream(t, pn)

	received := make(chan string, 1)
	register(t, pn, &handler{
		notify: func(_ context.Context, req *testgen.NotifyRequest) error {
			received <- req.Text
			return nil
		},
		process: func(_ context.Context, _ *testgen.EventPayload, _ protonats.Acker) error {
			return nil
		},
	})

	client := testgen.NewTestServiceClient(pn)
	require.NoError(t, client.Notify(context.Background(), &testgen.NotifyRequest{Text: "ping"}))

	select {
	case text := <-received:
		assert.Equal(t, "ping", text)
	case <-time.After(2 * time.Second):
		t.Fatal("notify not received")
	}
}

func TestDefaultQueueGroup_Generated(t *testing.T) {
	pn := connect(t)
	freshEventsStream(t, pn)

	var handled atomic.Int32
	h := &handler{
		echo: func(_ context.Context, req *testgen.EchoRequest) (*testgen.EchoResponse, error) {
			handled.Add(1)
			return &testgen.EchoResponse{Message: req.Message}, nil
		},
		process: func(_ context.Context, _ *testgen.EventPayload, _ protonats.Acker) error {
			return nil
		},
	}
	register(t, pn, h)
	register(t, pn, h)

	client := testgen.NewTestServiceClient(pn)
	const requests = 10
	for i := 0; i < requests; i++ {
		_, err := client.Echo(context.Background(), &testgen.EchoRequest{Message: "x"})
		require.NoError(t, err)
	}

	// Two registrations share the default queue group: each request is
	// handled exactly once.
	assert.Equal(t, int32(requests), handled.Load())
}

func TestInterceptors_SeeFullMethodName(t *testing.T) {
	nc, err := nats.Connect(nats.DefaultURL, nats.Timeout(2*time.Second))
	if err != nil {
		t.Skipf("NATS not available: %v", err)
	}
	defer nc.Close()

	var clientMethod string
	pn, err := protonats.New(nc, protonats.WithClientInterceptor(
		func(ctx context.Context, method, subject string, req proto.Message, next protonats.ClientInvoker) (proto.Message, error) {
			clientMethod = method
			return next(ctx, subject, req)
		}))
	require.NoError(t, err)
	freshEventsStream(t, pn)

	var handlerMethod string
	register(t, pn, &handler{
		echo: echoOK,
		process: func(_ context.Context, _ *testgen.EventPayload, _ protonats.Acker) error {
			return nil
		},
	}, protonats.WithHandlerInterceptor(
		func(ctx context.Context, method, subject string, req proto.Message, next protonats.HandlerInvoker) (proto.Message, error) {
			handlerMethod = method
			return next(ctx, req)
		}))

	client := testgen.NewTestServiceClient(pn)
	_, err = client.Echo(context.Background(), &testgen.EchoRequest{Message: "x"})
	require.NoError(t, err)

	assert.Equal(t, "testpkg.TestService.Echo", clientMethod)
	assert.Equal(t, "testpkg.TestService.Echo", handlerMethod)
}

// ── JetStream ──

func TestJetStreamPublish_ReturnsAck(t *testing.T) {
	pn := connect(t)
	freshEventsStream(t, pn)

	client := testgen.NewTestServiceClient(pn)
	ack, err := client.EmitEvent(context.Background(), &testgen.EventPayload{EventId: "e1", Data: "d"})
	require.NoError(t, err)
	assert.Equal(t, "EVENTS", ack.Stream)
	assert.EqualValues(t, 1, ack.Sequence)
}

func TestJetStreamConsume_AutoAck(t *testing.T) {
	pn := connect(t)
	freshEventsStream(t, pn)

	received := make(chan string, 1)
	register(t, pn, &handler{
		process: func(_ context.Context, req *testgen.EventPayload, _ protonats.Acker) error {
			received <- req.EventId
			return nil
		},
	})

	_, err := pn.PublishJetStream(context.Background(), "testpkg.ProcessEvent", &testgen.EventPayload{EventId: "evt-1"})
	require.NoError(t, err)

	select {
	case id := <-received:
		assert.Equal(t, "evt-1", id)
	case <-time.After(5 * time.Second):
		t.Fatal("event not consumed")
	}
}

func TestJetStreamConsume_NakRedelivers(t *testing.T) {
	pn := connect(t)
	freshEventsStream(t, pn)

	var deliveries atomic.Int32
	done := make(chan struct{}, 1)
	register(t, pn, &handler{
		process: func(_ context.Context, _ *testgen.EventPayload, _ protonats.Acker) error {
			if deliveries.Add(1) == 1 {
				return errors.New("transient failure")
			}
			done <- struct{}{}
			return nil
		},
	})

	_, err := pn.PublishJetStream(context.Background(), "testpkg.ProcessEvent", &testgen.EventPayload{EventId: "evt-nak"})
	require.NoError(t, err)

	select {
	case <-done:
		assert.GreaterOrEqual(t, deliveries.Load(), int32(2))
	case <-time.After(10 * time.Second):
		t.Fatal("message was not redelivered after nak")
	}
}

func TestJetStreamConsume_ErrTerminateStopsRedelivery(t *testing.T) {
	pn := connect(t)
	freshEventsStream(t, pn)

	var deliveries atomic.Int32
	register(t, pn, &handler{
		process: func(_ context.Context, _ *testgen.EventPayload, _ protonats.Acker) error {
			deliveries.Add(1)
			return protonats.ErrTerminate
		},
	})

	_, err := pn.PublishJetStream(context.Background(), "testpkg.ProcessEvent", &testgen.EventPayload{EventId: "evt-term"})
	require.NoError(t, err)

	require.Eventually(t, func() bool { return deliveries.Load() == 1 }, 5*time.Second, 50*time.Millisecond)
	time.Sleep(500 * time.Millisecond)
	assert.Equal(t, int32(1), deliveries.Load(), "terminated message must not be redelivered")
}

func TestJetStreamConsume_ConsumerConfigOverride(t *testing.T) {
	pn := connect(t)
	js := freshEventsStream(t, pn)

	register(t, pn, &handler{
		process: func(_ context.Context, _ *testgen.EventPayload, _ protonats.Acker) error {
			return nil
		},
	}, protonats.WithConsumerConfig("ProcessEvent", jetstream.ConsumerConfig{
		MaxDeliver: 3,
	}))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cons, err := js.Consumer(ctx, "EVENTS", "event-processor")
	require.NoError(t, err)

	info, err := cons.Info(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, info.Config.MaxDeliver)
	assert.Equal(t, "testpkg.ProcessEvent", info.Config.FilterSubject)
}

// TestJetStreamConsume_ExplicitAckerTerm covers a handler driving the Acker
// itself: Term() must stop redelivery, and the nil return afterwards must not
// undo it — the auto-ack on a nil return is redundant once the message has
// been acked, and JetStream ignores it.
func TestJetStreamConsume_ExplicitAckerTerm(t *testing.T) {
	pn := connect(t)
	freshEventsStream(t, pn)

	var deliveries atomic.Int32
	register(t, pn, &handler{
		process: func(_ context.Context, _ *testgen.EventPayload, ack protonats.Acker) error {
			deliveries.Add(1)
			require.NoError(t, ack.InProgress())
			require.NoError(t, ack.Term())
			return nil
		},
	})

	_, err := pn.PublishJetStream(context.Background(), "testpkg.ProcessEvent", &testgen.EventPayload{EventId: "evt-explicit"})
	require.NoError(t, err)

	require.Eventually(t, func() bool { return deliveries.Load() == 1 }, 5*time.Second, 50*time.Millisecond)
	time.Sleep(500 * time.Millisecond)
	assert.Equal(t, int32(1), deliveries.Load(), "an explicitly terminated message must not be redelivered")
}

// A payload that cannot decode never will, so it must be terminated rather
// than redelivered until MaxDeliver — otherwise one poison message blocks the
// consumer and reappears forever.
func TestJetStreamConsume_UndecodablePayloadIsTerminated(t *testing.T) {
	pn := connect(t)
	js := freshEventsStream(t, pn)

	var delivered atomic.Int32
	register(t, pn, &handler{
		process: func(_ context.Context, _ *testgen.EventPayload, _ protonats.Acker) error {
			delivered.Add(1)
			return nil
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := js.Publish(ctx, "testpkg.ProcessEvent", []byte{0xff, 0xfe, 0xfd})
	require.NoError(t, err)

	// Give the consumer time to take it, fail to decode, and terminate it.
	time.Sleep(time.Second)
	assert.Equal(t, int32(0), delivered.Load(), "the handler must never see an undecodable payload")

	// A terminated message leaves no unacked work behind; a naked nak loop
	// would show up here as a pending count that never drains.
	cons, err := js.Consumer(ctx, "EVENTS", "event-processor")
	require.NoError(t, err)
	info, err := cons.Info(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, info.NumAckPending, "the poison message must not stay pending")
	assert.EqualValues(t, 0, info.NumRedelivered, "the poison message must not be redelivered")
}

// A valid message published after a poison one must still be delivered: the
// terminate path may not wedge the consumer.
func TestJetStreamConsume_SurvivesAPoisonMessage(t *testing.T) {
	pn := connect(t)
	js := freshEventsStream(t, pn)

	received := make(chan string, 1)
	register(t, pn, &handler{
		process: func(_ context.Context, req *testgen.EventPayload, _ protonats.Acker) error {
			received <- req.EventId
			return nil
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := js.Publish(ctx, "testpkg.ProcessEvent", []byte{0xff, 0xfe, 0xfd})
	require.NoError(t, err)

	_, err = pn.PublishJetStream(context.Background(), "testpkg.ProcessEvent",
		&testgen.EventPayload{EventId: "after-poison"})
	require.NoError(t, err)

	select {
	case id := <-received:
		assert.Equal(t, "after-poison", id)
	case <-time.After(5 * time.Second):
		t.Fatal("consumer stalled after an undecodable message")
	}
}

// TestJetStreamConsume_HandlerInterceptor checks that handler interceptors are
// applied on the consume path, not only on request/reply.
func TestJetStreamConsume_HandlerInterceptor(t *testing.T) {
	pn := connect(t)
	freshEventsStream(t, pn)

	seen := make(chan string, 1)
	register(t, pn, &handler{
		process: func(_ context.Context, _ *testgen.EventPayload, _ protonats.Acker) error {
			return nil
		},
	}, protonats.WithHandlerInterceptor(
		func(ctx context.Context, method, subject string, req proto.Message, next protonats.HandlerInvoker) (proto.Message, error) {
			select {
			case seen <- method:
			default:
			}
			return next(ctx, req)
		}))

	_, err := pn.PublishJetStream(context.Background(), "testpkg.ProcessEvent", &testgen.EventPayload{EventId: "evt-ic"})
	require.NoError(t, err)

	select {
	case method := <-seen:
		assert.Equal(t, "testpkg.TestService.ProcessEvent", method)
	case <-time.After(5 * time.Second):
		t.Fatal("handler interceptor did not run on the consume path")
	}
}

// TestJetStreamConsume_BindsExistingDurable covers the documented promise that
// an existing durable consumer is bound as-is, so settings tuned by an operator
// (or by earlier infrastructure code) survive a handler registration.
func TestJetStreamConsume_BindsExistingDurable(t *testing.T) {
	pn := connect(t)
	js := freshEventsStream(t, pn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Pre-create the durable with a distinctive, non-default setting.
	_, err := js.CreateConsumer(ctx, "EVENTS", jetstream.ConsumerConfig{
		Durable:       "event-processor",
		FilterSubject: "testpkg.ProcessEvent",
		AckPolicy:     jetstream.AckExplicitPolicy,
		MaxDeliver:    7,
	})
	require.NoError(t, err)

	register(t, pn, &handler{
		process: func(_ context.Context, _ *testgen.EventPayload, _ protonats.Acker) error {
			return nil
		},
	})

	cons, err := js.Consumer(ctx, "EVENTS", "event-processor")
	require.NoError(t, err)
	info, err := cons.Info(ctx)
	require.NoError(t, err)
	assert.Equal(t, 7, info.Config.MaxDeliver, "registration must not clobber an existing durable's config")
}

// TestRegistrationRollback verifies that when a later registration step fails
// (here: the JetStream consumer, because the stream does not exist), the
// already-created core subscriptions are torn down again.
// ── Tasks and rollbacks ──

// taskHandler implements both halves of the ChargeCard task, recording what
// each one saw.
type taskHandler struct {
	work     func(context.Context, *testgen.ChargeRequest) error
	rolled   chan *testgen.ChargeRequest
	causes   chan *protonats.Error
	attempts atomic.Int32
}

func newTaskHandler(work func(context.Context, *testgen.ChargeRequest) error) *taskHandler {
	return &taskHandler{
		work:   work,
		rolled: make(chan *testgen.ChargeRequest, 4),
		causes: make(chan *protonats.Error, 4),
	}
}

func (h *taskHandler) ChargeCard(ctx context.Context, req *testgen.ChargeRequest, _ protonats.Acker) error {
	h.attempts.Add(1)
	return h.work(ctx, req)
}

// The rollback half satisfies TestServiceRollback via a separate receiver so
// one struct can register as both roles in a test.
type rollbackHandler struct{ h *taskHandler }

func (r rollbackHandler) ChargeCard(_ context.Context, req *testgen.ChargeRequest, cause *protonats.Error, _ protonats.Acker) error {
	r.h.rolled <- req
	r.h.causes <- cause
	return nil
}

// registerTask wires both halves against one connection. Real deployments split
// them across services; one process is enough to prove the wire contract.
func registerTask(t *testing.T, pn *protonats.Conn, h *taskHandler, opts ...protonats.HandlerOption) {
	t.Helper()
	worker, err := testgen.RegisterTestServiceWorker(pn, h, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = worker.Unsubscribe() })

	rollback, err := testgen.RegisterTestServiceRollback(pn, rollbackHandler{h})
	require.NoError(t, err)
	t.Cleanup(func() { _ = rollback.Unsubscribe() })
}

func TestTask_SucceedsWithoutRollback(t *testing.T) {
	pn := connect(t)
	freshEventsStream(t, pn)

	done := make(chan string, 1)
	h := newTaskHandler(func(_ context.Context, req *testgen.ChargeRequest) error {
		done <- req.ChargeId
		return nil
	})
	registerTask(t, pn, h)

	client := testgen.NewTestServiceClient(pn)
	_, err := client.ChargeCard(context.Background(), &testgen.ChargeRequest{ChargeId: "ok-1", AmountCents: 500})
	require.NoError(t, err)

	select {
	case id := <-done:
		assert.Equal(t, "ok-1", id)
	case <-time.After(5 * time.Second):
		t.Fatal("task never ran")
	}

	// Work that succeeded must not be undone.
	select {
	case req := <-h.rolled:
		t.Fatalf("a successful task was rolled back: %+v", req)
	case <-time.After(time.Second):
	}
}

// ErrTerminate says the work will never succeed, so the rollback runs at once
// rather than after a round of pointless retries.
func TestTask_TerminalErrorRollsBackImmediately(t *testing.T) {
	pn := connect(t)
	freshEventsStream(t, pn)

	h := newTaskHandler(func(_ context.Context, _ *testgen.ChargeRequest) error {
		return fmt.Errorf("card declined: %w", protonats.ErrTerminate)
	})
	registerTask(t, pn, h)

	client := testgen.NewTestServiceClient(pn)
	_, err := client.ChargeCard(context.Background(), &testgen.ChargeRequest{ChargeId: "declined-1", AmountCents: 900})
	require.NoError(t, err)

	select {
	case req := <-h.rolled:
		assert.Equal(t, "declined-1", req.ChargeId, "the rollback receives the message that failed")
		assert.EqualValues(t, 900, req.AmountCents)
	case <-time.After(10 * time.Second):
		t.Fatal("rollback never ran")
	}
	assert.EqualValues(t, 1, h.attempts.Load(), "a terminal error must not be retried first")
}

// The cause travels with the rollback, so the handler can tell a declined card
// from a crashed dependency.
func TestTask_RollbackReceivesCause(t *testing.T) {
	pn := connect(t)
	freshEventsStream(t, pn)

	h := newTaskHandler(func(_ context.Context, _ *testgen.ChargeRequest) error {
		return protonats.Errorf(402, "insufficient funds")
	})
	registerTask(t, pn, h, protonats.WithRetryPolicy(protonats.TerminateOnClientError))

	client := testgen.NewTestServiceClient(pn)
	_, err := client.ChargeCard(context.Background(), &testgen.ChargeRequest{ChargeId: "broke-1"})
	require.NoError(t, err)

	select {
	case cause := <-h.causes:
		require.NotNil(t, cause)
		assert.Equal(t, 402, cause.Code)
		assert.Equal(t, "insufficient funds", cause.Message)
	case <-time.After(10 * time.Second):
		t.Fatal("rollback never ran")
	}
}

// A 4xx is the request's fault and will fail identically every time, so
// TerminateOnClientError gives up on the first attempt.
func TestTask_ClientErrorPolicySkipsRetries(t *testing.T) {
	pn := connect(t)
	freshEventsStream(t, pn)

	h := newTaskHandler(func(_ context.Context, _ *testgen.ChargeRequest) error {
		return protonats.Errorf(400, "malformed charge")
	})
	registerTask(t, pn, h, protonats.WithRetryPolicy(protonats.TerminateOnClientError))

	client := testgen.NewTestServiceClient(pn)
	_, err := client.ChargeCard(context.Background(), &testgen.ChargeRequest{ChargeId: "bad-1"})
	require.NoError(t, err)

	select {
	case <-h.rolled:
	case <-time.After(10 * time.Second):
		t.Fatal("rollback never ran")
	}
	assert.EqualValues(t, 1, h.attempts.Load())
}

// The important case: a handler that keeps failing for an ordinary reason.
// JetStream would silently drop the message once MaxDeliver ran out, so the
// runtime treats the final attempt as terminal and rolls back instead.
func TestTask_RollsBackAfterRetriesAreExhausted(t *testing.T) {
	pn := connect(t)
	freshEventsStream(t, pn)

	h := newTaskHandler(func(_ context.Context, _ *testgen.ChargeRequest) error {
		return fmt.Errorf("payment gateway timeout")
	})
	registerTask(t, pn, h, protonats.WithConsumerConfig("ChargeCard", jetstream.ConsumerConfig{
		Durable:    "card-charger",
		MaxDeliver: 3,
		BackOff:    []time.Duration{200 * time.Millisecond, 200 * time.Millisecond},
	}))

	client := testgen.NewTestServiceClient(pn)
	started := time.Now()
	_, err := client.ChargeCard(context.Background(), &testgen.ChargeRequest{ChargeId: "flaky-1"})
	require.NoError(t, err)

	select {
	case req := <-h.rolled:
		assert.Equal(t, "flaky-1", req.ChargeId)
	case <-time.After(15 * time.Second):
		t.Fatal("rollback never ran after retries were exhausted")
	}
	assert.EqualValues(t, 3, h.attempts.Load(), "every delivery MaxDeliver allows should be tried before giving up")

	// Two retries at 200ms each. A plain Nak asks for redelivery immediately,
	// so without the backoff being applied the whole thing finishes in
	// milliseconds and a failing dependency gets hammered.
	assert.GreaterOrEqual(t, time.Since(started), 400*time.Millisecond,
		"retries must be spaced by the consumer's configured BackOff")
}

// A transient failure that later succeeds must not roll back: the retry is the
// whole point of not giving up early.
func TestTask_RecoveryBeforeExhaustionSkipsRollback(t *testing.T) {
	pn := connect(t)
	freshEventsStream(t, pn)

	var h *taskHandler
	h = newTaskHandler(func(_ context.Context, _ *testgen.ChargeRequest) error {
		if h.attempts.Load() == 1 {
			return fmt.Errorf("transient blip")
		}
		return nil
	})
	registerTask(t, pn, h, protonats.WithConsumerConfig("ChargeCard", jetstream.ConsumerConfig{
		Durable:    "card-charger",
		MaxDeliver: 3,
		BackOff:    []time.Duration{200 * time.Millisecond, 200 * time.Millisecond},
	}))

	client := testgen.NewTestServiceClient(pn)
	_, err := client.ChargeCard(context.Background(), &testgen.ChargeRequest{ChargeId: "blip-1"})
	require.NoError(t, err)

	require.Eventually(t, func() bool { return h.attempts.Load() >= 2 }, 10*time.Second, 50*time.Millisecond)

	select {
	case req := <-h.rolled:
		t.Fatalf("a task that recovered was rolled back: %+v", req)
	case <-time.After(time.Second):
	}
}

// WithOnTerminate is the observability hook: it fires for anything given up on,
// including a plain consume method that has no rollback at all.
func TestOnTerminate_FiresForConsumeWithoutRollback(t *testing.T) {
	pn := connect(t)
	freshEventsStream(t, pn)

	type terminal struct {
		method string
		cause  error
	}
	seen := make(chan terminal, 1)

	register(t, pn, &handler{
		process: func(_ context.Context, _ *testgen.EventPayload, _ protonats.Acker) error {
			return protonats.ErrTerminate
		},
	}, protonats.WithOnTerminate(func(_ context.Context, method, _ string, _ proto.Message, cause error) {
		select {
		case seen <- terminal{method, cause}:
		default:
		}
	}))

	_, err := pn.PublishJetStream(context.Background(), "testpkg.ProcessEvent",
		&testgen.EventPayload{EventId: "term-hook"})
	require.NoError(t, err)

	select {
	case got := <-seen:
		assert.Equal(t, "testpkg.TestService.ProcessEvent", got.method)
		assert.ErrorIs(t, got.cause, protonats.ErrTerminate)
	case <-time.After(5 * time.Second):
		t.Fatal("terminal hook never fired")
	}
}

func TestRegistrationRollback(t *testing.T) {
	pn := connect(t)
	js, err := pn.JetStream()
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := js.AccountInfo(ctx); err != nil {
		natstest.Unavailable(t, "JetStream not available: %v", err)
	}
	_ = js.DeleteStream(ctx, "EVENTS")

	_, err = testgen.RegisterTestServiceHandler(pn, &handler{
		echo: echoOK,
		process: func(_ context.Context, _ *testgen.EventPayload, _ protonats.Acker) error {
			return nil
		},
	})
	require.Error(t, err, "registration must fail without the EVENTS stream")

	// The Echo subscription created before the failure must be gone.
	client := testgen.NewTestServiceClient(pn)
	_, err = client.Echo(context.Background(), &testgen.EchoRequest{Message: "x"}, protonats.WithTimeout(time.Second))
	require.Error(t, err)
	assert.ErrorIs(t, err, nats.ErrNoResponders)
}

func TestRegistration_DrainWithConsumer(t *testing.T) {
	pn := connect(t)
	freshEventsStream(t, pn)

	reg := register(t, pn, &handler{
		echo: echoOK,
		process: func(_ context.Context, _ *testgen.EventPayload, _ protonats.Acker) error {
			return nil
		},
	})
	require.NoError(t, reg.Drain())
}
