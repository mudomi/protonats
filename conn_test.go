package protonats

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/mudomi/protonats/internal/natstest"
)

func connectNats(t *testing.T) *nats.Conn {
	t.Helper()
	return natstest.Connect(t)
}

// testLogger captures log output for assertions.
type testLogger struct {
	mu    sync.Mutex
	lines []string
}

func (l *testLogger) Printf(format string, v ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, fmt.Sprintf(format, v...))
}

func (l *testLogger) contains(substr string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, line := range l.lines {
		if strings.Contains(line, substr) {
			return true
		}
	}
	return false
}

func echoHandler(ctx context.Context, req proto.Message) (proto.Message, error) {
	return wrapperspb.String(req.(*wrapperspb.StringValue).Value + " back"), nil
}

func newStringValue() proto.Message { return &wrapperspb.StringValue{} }

func subscribeEcho(t *testing.T, pn *Conn, subject string, opts ...HandlerOption) {
	t.Helper()
	sub, err := pn.Subscribe("test.Method", subject, ApplyHandlerOptions(opts), newStringValue, echoHandler)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Unsubscribe() })
}

func TestNew_NilConn(t *testing.T) {
	_, err := New(nil)
	require.Error(t, err)
}

func TestNew_Defaults(t *testing.T) {
	pn, err := New(connectNats(t))
	require.NoError(t, err)
	assert.Equal(t, ProtoCodec{}, pn.Codec())
	assert.Equal(t, defaultTimeout, pn.defaultTimeout)
}

func TestNew_Options(t *testing.T) {
	pn, err := New(connectNats(t), WithDefaultTimeout(time.Second), WithCodec(JSONCodec))
	require.NoError(t, err)
	assert.Equal(t, time.Second, pn.defaultTimeout)
	assert.Equal(t, JSONCodec, pn.Codec())
}

func TestRequestReply(t *testing.T) {
	pn, _ := New(connectNats(t))
	subscribeEcho(t, pn, "test.echo")

	resp := &wrapperspb.StringValue{}
	require.NoError(t, pn.Request(context.Background(), "test.echo", wrapperspb.String("hello"), resp))
	assert.Equal(t, "hello back", resp.Value)
}

func TestRequestReply_HandlerError(t *testing.T) {
	pn, _ := New(connectNats(t))
	sub, err := pn.Subscribe("test.Method", "test.err", HandlerOptions{}, newStringValue,
		func(ctx context.Context, req proto.Message) (proto.Message, error) {
			return nil, Errorf(404, "nope")
		})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	err = pn.Request(context.Background(), "test.err", wrapperspb.String("x"), &wrapperspb.StringValue{})
	var pnErr *Error
	require.ErrorAs(t, err, &pnErr)
	assert.Equal(t, 404, pnErr.Code)
	assert.Equal(t, "nope", pnErr.Message)
}

func TestRequestReply_PlainErrorBecomes500(t *testing.T) {
	pn, _ := New(connectNats(t))
	sub, err := pn.Subscribe("test.Method", "test.plainerr", HandlerOptions{}, newStringValue,
		func(ctx context.Context, req proto.Message) (proto.Message, error) {
			return nil, fmt.Errorf("plain failure")
		})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	err = pn.Request(context.Background(), "test.plainerr", wrapperspb.String("x"), &wrapperspb.StringValue{})
	var pnErr *Error
	require.ErrorAs(t, err, &pnErr)
	assert.Equal(t, 500, pnErr.Code)
	assert.Equal(t, "plain failure", pnErr.Message)
}

func TestRequestReply_HandlerPanic(t *testing.T) {
	logger := &testLogger{}
	pn, _ := New(connectNats(t), WithLogger(logger))
	sub, err := pn.Subscribe("test.Method", "test.panic", HandlerOptions{}, newStringValue,
		func(ctx context.Context, req proto.Message) (proto.Message, error) {
			panic("kaboom")
		})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	err = pn.Request(context.Background(), "test.panic", wrapperspb.String("x"), &wrapperspb.StringValue{})
	var pnErr *Error
	require.ErrorAs(t, err, &pnErr)
	assert.Equal(t, 500, pnErr.Code)
	assert.True(t, logger.contains("panic"))
}

// The end-to-end half of TestErrorFromHeaders_EmptyMessageIsStillAnError: a
// handler failure with no message must not reach the caller as a success.
func TestRequestReply_HandlerErrorWithEmptyMessage(t *testing.T) {
	pn, _ := New(connectNats(t))
	sub, err := pn.Subscribe("test.Method", "test.emptyerr", HandlerOptions{}, newStringValue,
		func(ctx context.Context, req proto.Message) (proto.Message, error) {
			return nil, Errorf(404, "")
		})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	resp := &wrapperspb.StringValue{}
	err = pn.Request(context.Background(), "test.emptyerr", wrapperspb.String("x"), resp)
	var pnErr *Error
	require.ErrorAs(t, err, &pnErr)
	assert.Equal(t, 404, pnErr.Code)
}

func TestRequest_Timeout(t *testing.T) {
	pn, _ := New(connectNats(t))
	sub, err := pn.Subscribe("test.Method", "test.slow", HandlerOptions{}, newStringValue,
		func(ctx context.Context, req proto.Message) (proto.Message, error) {
			time.Sleep(2 * time.Second)
			return req, nil
		})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	err = pn.Request(context.Background(), "test.slow", wrapperspb.String("x"), &wrapperspb.StringValue{}, WithTimeout(100*time.Millisecond))
	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

// ── Timeout resolution ──

// A caller's own deadline wins over the connection default, in both
// directions: it is neither ignored nor clipped to the default.
func TestRequest_CallerDeadlineOverridesDefault(t *testing.T) {
	pn, _ := New(connectNats(t), WithDefaultTimeout(50*time.Millisecond))
	sub, err := pn.Subscribe("test.Method", "test.slowdefault", HandlerOptions{}, newStringValue,
		func(ctx context.Context, req proto.Message) (proto.Message, error) {
			time.Sleep(300 * time.Millisecond)
			return req, nil
		})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	resp := &wrapperspb.StringValue{}
	require.NoError(t, pn.Request(ctx, "test.slowdefault", wrapperspb.String("x"), resp),
		"a 3s caller deadline must not be clipped to the 50ms connection default")
	assert.Equal(t, "x", resp.Value)
}

// WithTimeout narrows an existing deadline; it can never extend past it,
// because a child context cannot outlive its parent.
func TestWithTimeout_NarrowsButNeverExtends(t *testing.T) {
	pn, _ := New(connectNats(t))

	parent, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	parentDeadline, _ := parent.Deadline()

	extended, cancelExtended := pn.withTimeout(parent, applyCallOptions([]CallOption{WithTimeout(time.Hour)}))
	defer cancelExtended()
	got, ok := extended.Deadline()
	require.True(t, ok)
	assert.Equal(t, parentDeadline, got, "an hour-long call timeout cannot outlive a 100ms parent")

	narrowed, cancelNarrowed := pn.withTimeout(parent, applyCallOptions([]CallOption{WithTimeout(10 * time.Millisecond)}))
	defer cancelNarrowed()
	got, ok = narrowed.Deadline()
	require.True(t, ok)
	assert.True(t, got.Before(parentDeadline), "a shorter call timeout must win")
}

func TestRequest_CancelledContext(t *testing.T) {
	pn, _ := New(connectNats(t))
	subscribeEcho(t, pn, "test.reqcancelled")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := pn.Request(ctx, "test.reqcancelled", wrapperspb.String("x"), &wrapperspb.StringValue{})
	assert.ErrorIs(t, err, context.Canceled)
}

func TestRequest_NoResponders(t *testing.T) {
	pn, _ := New(connectNats(t))
	err := pn.Request(context.Background(), "test.nobody.home", wrapperspb.String("x"), &wrapperspb.StringValue{}, WithTimeout(time.Second))
	require.Error(t, err)
	assert.ErrorIs(t, err, nats.ErrNoResponders)
}

func TestRequest_SubjectOverride(t *testing.T) {
	pn, _ := New(connectNats(t))
	subscribeEcho(t, pn, "test.actual")

	resp := &wrapperspb.StringValue{}
	require.NoError(t, pn.Request(context.Background(), "test.ignored", wrapperspb.String("x"), resp, WithSubject("test.actual")))
	assert.Equal(t, "x back", resp.Value)
}

func TestRequest_WithHeaders(t *testing.T) {
	pn, _ := New(connectNats(t))

	var gotHeader string
	sub, err := pn.Subscribe("test.Method", "test.headers", HandlerOptions{}, newStringValue,
		func(ctx context.Context, req proto.Message) (proto.Message, error) {
			gotHeader = MsgFromContext(ctx).Header.Get("X-Test")
			return req, nil
		})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	h := nats.Header{}
	h.Set("X-Test", "v1")
	require.NoError(t, pn.Request(context.Background(), "test.headers", wrapperspb.String("x"), &wrapperspb.StringValue{}, WithHeaders(h)))
	assert.Equal(t, "v1", gotHeader)
}

func TestRequest_EmptyMessageRoundtrip(t *testing.T) {
	pn, _ := New(connectNats(t))
	subscribeEcho(t, pn, "test.empty")

	resp := &wrapperspb.StringValue{}
	require.NoError(t, pn.Request(context.Background(), "test.empty", wrapperspb.String(""), resp))
	assert.Equal(t, " back", resp.Value)
}

func TestRequest_Concurrent(t *testing.T) {
	pn, _ := New(connectNats(t))
	subscribeEcho(t, pn, "test.concurrent")

	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp := &wrapperspb.StringValue{}
			if err := pn.Request(context.Background(), "test.concurrent", wrapperspb.String(fmt.Sprint(i)), resp); err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestRequest_JSONCodec(t *testing.T) {
	pn, _ := New(connectNats(t), WithCodec(JSONCodec))
	subscribeEcho(t, pn, "test.json")

	resp := &wrapperspb.StringValue{}
	require.NoError(t, pn.Request(context.Background(), "test.json", wrapperspb.String("hello"), resp))
	assert.Equal(t, "hello back", resp.Value)
}

func TestSubscribe_BadPayload(t *testing.T) {
	pn, _ := New(connectNats(t))
	subscribeEcho(t, pn, "test.badpayload")

	reply, err := pn.NatsConn().Request("test.badpayload", []byte{0xff, 0xfe, 0xfd}, time.Second)
	require.NoError(t, err)
	herr := errorFromHeaders(reply.Header)
	var pnErr *Error
	require.ErrorAs(t, herr, &pnErr)
	assert.Equal(t, 400, pnErr.Code)
}

func TestSubscribe_NilResponse(t *testing.T) {
	pn, _ := New(connectNats(t))
	sub, err := pn.Subscribe("test.Method", "test.nilresp", HandlerOptions{}, newStringValue,
		func(ctx context.Context, req proto.Message) (proto.Message, error) {
			return nil, nil
		})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	resp := &wrapperspb.StringValue{}
	require.NoError(t, pn.Request(context.Background(), "test.nilresp", wrapperspb.String("x"), resp))
	assert.Empty(t, resp.Value)
}

func TestSubscribe_ContextValues(t *testing.T) {
	pn, _ := New(connectNats(t))

	var gotSubject string
	var gotMsg *nats.Msg
	sub, err := pn.Subscribe("test.Method", "test.ctx", HandlerOptions{}, newStringValue,
		func(ctx context.Context, req proto.Message) (proto.Message, error) {
			gotSubject = SubjectFromContext(ctx)
			gotMsg = MsgFromContext(ctx)
			return req, nil
		})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	require.NoError(t, pn.Request(context.Background(), "test.ctx", wrapperspb.String("x"), &wrapperspb.StringValue{}))
	assert.Equal(t, "test.ctx", gotSubject)
	require.NotNil(t, gotMsg)
	assert.Equal(t, "test.ctx", gotMsg.Subject)
}

func TestSubscribe_HandlerErrorWithoutReplyIsLogged(t *testing.T) {
	logger := &testLogger{}
	pn, _ := New(connectNats(t), WithLogger(logger))
	sub, err := pn.Subscribe("test.Method", "test.noreply", HandlerOptions{}, newStringValue,
		func(ctx context.Context, req proto.Message) (proto.Message, error) {
			return nil, fmt.Errorf("dropped error")
		})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	// Plain publish: no reply subject, so the error has nowhere to go but the log.
	require.NoError(t, pn.Publish(context.Background(), "test.noreply", wrapperspb.String("x")))
	require.Eventually(t, func() bool { return logger.contains("dropped error") }, 2*time.Second, 10*time.Millisecond)
}

// ── Queue groups ──

func TestSubscribe_DefaultQueueGroupLoadBalances(t *testing.T) {
	pn, _ := New(connectNats(t))

	var handled atomic.Int32
	countingHandler := func(ctx context.Context, req proto.Message) (proto.Message, error) {
		handled.Add(1)
		return req, nil
	}
	for i := 0; i < 2; i++ {
		sub, err := pn.Subscribe("test.Method", "test.qgdefault", HandlerOptions{}, newStringValue, countingHandler)
		require.NoError(t, err)
		defer sub.Unsubscribe()
	}

	const requests = 10
	for i := 0; i < requests; i++ {
		require.NoError(t, pn.Request(context.Background(), "test.qgdefault", wrapperspb.String("x"), &wrapperspb.StringValue{}))
	}

	// With the default queue group, each request is handled exactly once even
	// with two live subscriptions.
	assert.Equal(t, int32(requests), handled.Load())
}

func TestSubscribe_WithNoQueueGroupFansOut(t *testing.T) {
	pn, _ := New(connectNats(t))

	var handled atomic.Int32
	ho := ApplyHandlerOptions([]HandlerOption{WithNoQueueGroup()})
	for i := 0; i < 2; i++ {
		sub, err := pn.Subscribe("test.Method", "test.qgnone", ho, newStringValue,
			func(ctx context.Context, req proto.Message) (proto.Message, error) {
				handled.Add(1)
				return req, nil
			})
		require.NoError(t, err)
		defer sub.Unsubscribe()
	}

	require.NoError(t, pn.Request(context.Background(), "test.qgnone", wrapperspb.String("x"), &wrapperspb.StringValue{}))

	require.Eventually(t, func() bool { return handled.Load() == 2 }, 2*time.Second, 10*time.Millisecond)
}

// ── Publish / SubscribePublish ──

func TestPublish(t *testing.T) {
	pn, _ := New(connectNats(t))

	done := make(chan string, 1)
	sub, err := pn.SubscribePublish("test.Method", "test.pub", HandlerOptions{}, newStringValue,
		func(ctx context.Context, req proto.Message) error {
			done <- req.(*wrapperspb.StringValue).Value
			return nil
		})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	require.NoError(t, pn.Publish(context.Background(), "test.pub", wrapperspb.String("fire")))

	select {
	case v := <-done:
		assert.Equal(t, "fire", v)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}
}

func TestPublish_CancelledContext(t *testing.T) {
	pn, _ := New(connectNats(t))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := pn.Publish(ctx, "test.cancelled", wrapperspb.String("x"))
	assert.ErrorIs(t, err, context.Canceled)
}

func TestSubscribePublish_DistinctMethodsEachGetACopy(t *testing.T) {
	pn, _ := New(connectNats(t))

	// Two different services consuming the same event: distinct method names
	// mean distinct queue groups, so each must receive every message.
	var a, b atomic.Int32
	counters := map[string]*atomic.Int32{"pkg.A.OnEvent": &a, "pkg.B.OnEvent": &b}
	for method, counter := range counters {
		sub, err := pn.SubscribePublish(method, "test.fanout", HandlerOptions{}, newStringValue,
			func(ctx context.Context, req proto.Message) error {
				counter.Add(1)
				return nil
			})
		require.NoError(t, err)
		defer sub.Unsubscribe()
	}

	require.NoError(t, pn.Publish(context.Background(), "test.fanout", wrapperspb.String("x")))
	require.Eventually(t, func() bool { return a.Load() == 1 && b.Load() == 1 }, 2*time.Second, 10*time.Millisecond)
}

func TestSubscribePublish_SameMethodLoadBalances(t *testing.T) {
	pn, _ := New(connectNats(t))

	// Two instances of the same service: one method name, so the default queue
	// group balances instead of double-processing.
	var received atomic.Int32
	for i := 0; i < 2; i++ {
		sub, err := pn.SubscribePublish("pkg.Svc.OnEvent", "test.pubbalance", HandlerOptions{}, newStringValue,
			func(ctx context.Context, req proto.Message) error {
				received.Add(1)
				return nil
			})
		require.NoError(t, err)
		defer sub.Unsubscribe()
	}

	const messages = 10
	for i := 0; i < messages; i++ {
		require.NoError(t, pn.Publish(context.Background(), "test.pubbalance", wrapperspb.String("x")))
	}
	require.Eventually(t, func() bool { return received.Load() == messages }, 2*time.Second, 10*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int32(messages), received.Load())
}

// TestSubscribe_OverlappingSubjectsDoNotSteal is the regression guard for the
// queue-group scoping rule: NATS groups queue subscribers by queue name across
// every subject pattern matching the delivered subject. A shared queue name
// would put the wildcard and literal handlers in one group, and the wildcard
// handler would swallow roughly half of the literal subject's requests.
//
// The subjects are namespaced to this test: `go test ./...` runs packages in
// parallel against one server, and internal/e2e subscribes "items.*".
func TestSubscribe_OverlappingSubjectsDoNotSteal(t *testing.T) {
	pn, _ := New(connectNats(t))

	var wildcard, literal atomic.Int32
	subs := []struct {
		method, subject string
		counter         *atomic.Int32
	}{
		{"pkg.Svc.GetItem", "overlap.*", &wildcard},
		{"pkg.Svc.ListItems", "overlap.list", &literal},
	}
	for _, s := range subs {
		counter := s.counter
		sub, err := pn.Subscribe(s.method, s.subject, HandlerOptions{}, newStringValue,
			func(ctx context.Context, req proto.Message) (proto.Message, error) {
				counter.Add(1)
				return req, nil
			})
		require.NoError(t, err)
		defer sub.Unsubscribe()
	}

	const requests = 20
	for i := 0; i < requests; i++ {
		require.NoError(t, pn.Request(context.Background(), "overlap.list", wrapperspb.String("x"), &wrapperspb.StringValue{}))
	}

	// Both subscriptions match, so both must see every message. A request
	// returns on whichever reply arrives first, so the other handler can still
	// be running — wait for both counts rather than reading them immediately.
	require.Eventually(t, func() bool {
		return literal.Load() == requests && wildcard.Load() == requests
	}, 2*time.Second, 10*time.Millisecond,
		"nothing may be stolen: literal=%d wildcard=%d, want %d each", literal.Load(), wildcard.Load(), requests)
}

func TestSubscribePublish_QueueGroupLoadBalances(t *testing.T) {
	pn, _ := New(connectNats(t))

	var received atomic.Int32
	ho := ApplyHandlerOptions([]HandlerOption{WithQueueGroup("workers")})
	for i := 0; i < 2; i++ {
		sub, err := pn.SubscribePublish("test.Method", "test.qgpub", ho, newStringValue,
			func(ctx context.Context, req proto.Message) error {
				received.Add(1)
				return nil
			})
		require.NoError(t, err)
		defer sub.Unsubscribe()
	}

	for i := 0; i < 10; i++ {
		require.NoError(t, pn.Publish(context.Background(), "test.qgpub", wrapperspb.String("x")))
	}
	require.Eventually(t, func() bool { return received.Load() == 10 }, 2*time.Second, 10*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int32(10), received.Load())
}

func TestSubscribePublish_BadPayloadLogged(t *testing.T) {
	logger := &testLogger{}
	pn, _ := New(connectNats(t), WithLogger(logger))

	handled := false
	sub, err := pn.SubscribePublish("test.Method", "test.pubbad", HandlerOptions{}, newStringValue,
		func(ctx context.Context, req proto.Message) error {
			handled = true
			return nil
		})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	require.NoError(t, pn.NatsConn().Publish("test.pubbad", []byte{0xff, 0xfe}))
	require.Eventually(t, func() bool { return logger.contains("unmarshal error") }, 2*time.Second, 10*time.Millisecond)
	assert.False(t, handled)
}

func TestSubscribePublish_PanicRecovered(t *testing.T) {
	logger := &testLogger{}
	pn, _ := New(connectNats(t), WithLogger(logger))

	sub, err := pn.SubscribePublish("test.Method", "test.pubpanic", HandlerOptions{}, newStringValue,
		func(ctx context.Context, req proto.Message) error {
			panic("pub kaboom")
		})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	require.NoError(t, pn.Publish(context.Background(), "test.pubpanic", wrapperspb.String("x")))
	require.Eventually(t, func() bool { return logger.contains("panic") }, 2*time.Second, 10*time.Millisecond)
}

// ── Interceptors ──

func TestClientInterceptors_RunInOrderWithMethodName(t *testing.T) {
	nc := connectNats(t)

	var order []string
	mkInterceptor := func(name string) ClientInterceptor {
		return func(ctx context.Context, method, subject string, req proto.Message, next ClientInvoker) (proto.Message, error) {
			order = append(order, name+":"+method+":"+subject)
			return next(ctx, subject, req)
		}
	}

	pn, err := New(nc, WithClientInterceptor(mkInterceptor("outer")), WithClientInterceptor(mkInterceptor("inner")))
	require.NoError(t, err)
	subscribeEcho(t, pn, "test.ic")

	resp := &wrapperspb.StringValue{}
	require.NoError(t, pn.Request(context.Background(), "test.ic", wrapperspb.String("x"), resp, WithMethodName("pkg.Svc.Do")))

	require.Equal(t, []string{"outer:pkg.Svc.Do:test.ic", "inner:pkg.Svc.Do:test.ic"}, order)
	assert.Equal(t, "x back", resp.Value)
}

func TestClientInterceptor_MethodDefaultsToSubject(t *testing.T) {
	var gotMethod string
	pn, err := New(connectNats(t), WithClientInterceptor(
		func(ctx context.Context, method, subject string, req proto.Message, next ClientInvoker) (proto.Message, error) {
			gotMethod = method
			return next(ctx, subject, req)
		}))
	require.NoError(t, err)
	subscribeEcho(t, pn, "test.icdefault")

	require.NoError(t, pn.Request(context.Background(), "test.icdefault", wrapperspb.String("x"), &wrapperspb.StringValue{}))
	assert.Equal(t, "test.icdefault", gotMethod)
}

func TestClientInterceptor_ShortCircuit(t *testing.T) {
	pn, err := New(connectNats(t), WithClientInterceptor(
		func(ctx context.Context, method, subject string, req proto.Message, next ClientInvoker) (proto.Message, error) {
			return nil, Errorf(403, "blocked")
		}))
	require.NoError(t, err)

	// No subscription exists; the interceptor must block before the wire.
	err = pn.Request(context.Background(), "test.blocked", wrapperspb.String("x"), &wrapperspb.StringValue{})
	var pnErr *Error
	require.ErrorAs(t, err, &pnErr)
	assert.Equal(t, 403, pnErr.Code)
}

func TestClientInterceptor_Publish(t *testing.T) {
	var called bool
	pn, err := New(connectNats(t), WithClientInterceptor(
		func(ctx context.Context, method, subject string, req proto.Message, next ClientInvoker) (proto.Message, error) {
			called = true
			return next(ctx, subject, req)
		}))
	require.NoError(t, err)

	require.NoError(t, pn.Publish(context.Background(), "test.icpub", wrapperspb.String("x")))
	assert.True(t, called)
}

func TestHandlerInterceptors_RunInOrder(t *testing.T) {
	pn, _ := New(connectNats(t))

	var order []string
	mkInterceptor := func(name string) HandlerInterceptor {
		return func(ctx context.Context, method, subject string, req proto.Message, next HandlerInvoker) (proto.Message, error) {
			order = append(order, name+":"+method+":"+subject)
			return next(ctx, req)
		}
	}
	ho := ApplyHandlerOptions([]HandlerOption{
		WithHandlerInterceptor(mkInterceptor("outer")),
		WithHandlerInterceptor(mkInterceptor("inner")),
	})

	sub, err := pn.Subscribe("pkg.Svc.Do", "test.hic", ho, newStringValue, echoHandler)
	require.NoError(t, err)
	defer sub.Unsubscribe()

	resp := &wrapperspb.StringValue{}
	require.NoError(t, pn.Request(context.Background(), "test.hic", wrapperspb.String("x"), resp))
	require.Equal(t, []string{"outer:pkg.Svc.Do:test.hic", "inner:pkg.Svc.Do:test.hic"}, order)
}

func TestHandlerInterceptor_ShortCircuit(t *testing.T) {
	pn, _ := New(connectNats(t))

	handlerCalled := false
	ho := ApplyHandlerOptions([]HandlerOption{
		WithHandlerInterceptor(func(ctx context.Context, method, subject string, req proto.Message, next HandlerInvoker) (proto.Message, error) {
			return nil, Errorf(401, "denied")
		}),
	})
	sub, err := pn.Subscribe("test.Method", "test.hicblock", ho, newStringValue,
		func(ctx context.Context, req proto.Message) (proto.Message, error) {
			handlerCalled = true
			return req, nil
		})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	err = pn.Request(context.Background(), "test.hicblock", wrapperspb.String("x"), &wrapperspb.StringValue{})
	var pnErr *Error
	require.ErrorAs(t, err, &pnErr)
	assert.Equal(t, 401, pnErr.Code)
	assert.False(t, handlerCalled)
}

func TestContextHelpers_ZeroValues(t *testing.T) {
	ctx := context.Background()
	assert.Equal(t, "", SubjectFromContext(ctx))
	assert.Nil(t, MsgFromContext(ctx))
}

func TestClientInterceptor_SubstitutesResponse(t *testing.T) {
	// An interceptor that short-circuits (cache hit, fallback) supplies its own
	// response; it must reach the caller instead of leaving them a zero value.
	pn, err := New(connectNats(t), WithClientInterceptor(
		func(ctx context.Context, method, subject string, req proto.Message, next ClientInvoker) (proto.Message, error) {
			return wrapperspb.String("from cache"), nil
		}))
	require.NoError(t, err)

	resp := &wrapperspb.StringValue{}
	// No subscription exists: only the interceptor can produce this answer.
	require.NoError(t, pn.Request(context.Background(), "test.substituted", wrapperspb.String("x"), resp))
	assert.Equal(t, "from cache", resp.Value)
}

func TestClientInterceptor_SubstitutesWrongType(t *testing.T) {
	pn, err := New(connectNats(t), WithClientInterceptor(
		func(ctx context.Context, method, subject string, req proto.Message, next ClientInvoker) (proto.Message, error) {
			return wrapperspb.Int32(42), nil
		}))
	require.NoError(t, err)

	err = pn.Request(context.Background(), "test.wrongtype", wrapperspb.String("x"), &wrapperspb.StringValue{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Int32Value")
}

func TestPublishJetStream_InterceptorShortCircuitIsAnError(t *testing.T) {
	// Without an ack there is nothing to return, and callers dereference the
	// result — so this must be an error rather than (nil, nil).
	pn, err := New(connectNats(t), WithClientInterceptor(
		func(ctx context.Context, method, subject string, req proto.Message, next ClientInvoker) (proto.Message, error) {
			return nil, nil
		}))
	require.NoError(t, err)

	ack, err := pn.PublishJetStream(context.Background(), "test.js.shortcircuit", wrapperspb.String("x"))
	assert.Nil(t, ack)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "short-circuited")
}
