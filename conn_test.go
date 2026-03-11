package protonats

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func connectNats(t *testing.T) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(nats.DefaultURL, nats.Timeout(2*time.Second))
	if err != nil {
		t.Skipf("NATS not available at %s: %v", nats.DefaultURL, err)
	}
	t.Cleanup(func() { nc.Close() })
	return nc
}

func TestNew_NilConn(t *testing.T) {
	_, err := New(nil)
	require.Error(t, err)
}

func TestNew_WithOptions(t *testing.T) {
	nc := connectNats(t)
	pn, err := New(nc, WithCodec(JSONCodec), WithDefaultTimeout(10*time.Second))
	require.NoError(t, err)
	assert.Equal(t, "application/json", pn.Codec().ContentType())
	assert.Equal(t, nc, pn.NatsConn())
}

func TestRequestReply(t *testing.T) {
	nc := connectNats(t)
	pn, err := New(nc)
	require.NoError(t, err)

	sub, err := pn.Subscribe("test.echo", "", func() proto.Message {
		return &wrapperspb.StringValue{}
	}, func(ctx context.Context, m *nats.Msg, req proto.Message) (proto.Message, error) {
		return wrapperspb.String(req.(*wrapperspb.StringValue).Value + " reply"), nil
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	resp := &wrapperspb.StringValue{}
	require.NoError(t, pn.Request(context.Background(), "test.echo", wrapperspb.String("hello"), resp))
	assert.Equal(t, "hello reply", resp.Value)
}

func TestRequestReply_HandlerError(t *testing.T) {
	nc := connectNats(t)
	pn, err := New(nc)
	require.NoError(t, err)

	sub, err := pn.Subscribe("test.err", "", func() proto.Message {
		return &wrapperspb.StringValue{}
	}, func(ctx context.Context, m *nats.Msg, req proto.Message) (proto.Message, error) {
		return nil, Errorf(404, "not found")
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	err = pn.Request(context.Background(), "test.err", wrapperspb.String("x"), &wrapperspb.StringValue{})
	var pnErr *Error
	require.ErrorAs(t, err, &pnErr)
	assert.Equal(t, 404, pnErr.Code)
}

func TestRequestReply_HandlerPanic(t *testing.T) {
	nc := connectNats(t)
	pn, err := New(nc)
	require.NoError(t, err)

	sub, err := pn.Subscribe("test.panic", "", func() proto.Message {
		return &wrapperspb.StringValue{}
	}, func(ctx context.Context, m *nats.Msg, req proto.Message) (proto.Message, error) {
		panic("boom")
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	err = pn.Request(context.Background(), "test.panic", wrapperspb.String("x"), &wrapperspb.StringValue{})
	var pnErr *Error
	require.ErrorAs(t, err, &pnErr)
	assert.Equal(t, 500, pnErr.Code)
}

func TestPublish(t *testing.T) {
	nc := connectNats(t)
	pn, err := New(nc)
	require.NoError(t, err)

	var received string
	var mu sync.Mutex
	done := make(chan struct{})

	sub, err := pn.SubscribePublish("test.fire", "", func() proto.Message {
		return &wrapperspb.StringValue{}
	}, func(ctx context.Context, req proto.Message) error {
		mu.Lock()
		received = req.(*wrapperspb.StringValue).Value
		mu.Unlock()
		close(done)
		return nil
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	require.NoError(t, pn.Publish(context.Background(), "test.fire", wrapperspb.String("fire")))

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}

	mu.Lock()
	assert.Equal(t, "fire", received)
	mu.Unlock()
}

func TestRequestReply_Timeout(t *testing.T) {
	nc := connectNats(t)
	pn, err := New(nc)
	require.NoError(t, err)

	err = pn.Request(context.Background(), "test.nobody", wrapperspb.String("x"), &wrapperspb.StringValue{},
		WithTimeout(100*time.Millisecond))
	require.Error(t, err)
}

func TestRequestReply_SubjectOverride(t *testing.T) {
	nc := connectNats(t)
	pn, err := New(nc)
	require.NoError(t, err)

	sub, err := pn.Subscribe("real.subject", "", func() proto.Message {
		return &wrapperspb.StringValue{}
	}, func(ctx context.Context, m *nats.Msg, req proto.Message) (proto.Message, error) {
		return wrapperspb.String("ok"), nil
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	resp := &wrapperspb.StringValue{}
	require.NoError(t, pn.Request(context.Background(), "wrong", wrapperspb.String("x"), resp,
		WithSubject("real.subject")))
	assert.Equal(t, "ok", resp.Value)
}

func TestQueueGroup(t *testing.T) {
	nc := connectNats(t)
	pn, err := New(nc)
	require.NoError(t, err)

	var count int
	var mu sync.Mutex

	handler := func(ctx context.Context, m *nats.Msg, req proto.Message) (proto.Message, error) {
		mu.Lock()
		count++
		mu.Unlock()
		return wrapperspb.String("ok"), nil
	}

	sub1, _ := pn.Subscribe("test.q", "w", func() proto.Message { return &wrapperspb.StringValue{} }, handler)
	sub2, _ := pn.Subscribe("test.q", "w", func() proto.Message { return &wrapperspb.StringValue{} }, handler)
	defer sub1.Unsubscribe()
	defer sub2.Unsubscribe()

	require.NoError(t, pn.Request(context.Background(), "test.q", wrapperspb.String("x"), &wrapperspb.StringValue{}))
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	assert.Equal(t, 1, count)
	mu.Unlock()
}

func TestContextValues(t *testing.T) {
	nc := connectNats(t)
	pn, err := New(nc)
	require.NoError(t, err)

	var gotSubject string
	sub, err := pn.Subscribe("test.ctx", "", func() proto.Message {
		return &wrapperspb.StringValue{}
	}, func(ctx context.Context, m *nats.Msg, req proto.Message) (proto.Message, error) {
		gotSubject = SubjectFromContext(ctx)
		return wrapperspb.String("ok"), nil
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	require.NoError(t, pn.Request(context.Background(), "test.ctx", wrapperspb.String("x"), &wrapperspb.StringValue{}))
	assert.Equal(t, "test.ctx", gotSubject)
}

func TestRegistration(t *testing.T) {
	nc := connectNats(t)
	sub1, _ := nc.Subscribe("test.r.1", func(*nats.Msg) {})
	sub2, _ := nc.Subscribe("test.r.2", func(*nats.Msg) {})

	reg := &Registration{}
	reg.AddSubscription(sub1)
	reg.AddSubscription(sub2)

	assert.True(t, sub1.IsValid())
	require.NoError(t, reg.Unsubscribe())
	assert.False(t, sub1.IsValid())
	assert.False(t, sub2.IsValid())
}

func TestHandlerOptions(t *testing.T) {
	ho := ApplyHandlerOptions([]HandlerOption{WithQueueGroup("grp")})
	assert.Equal(t, "grp", ho.QueueGroup)
}


// Edge cases

func TestNew_DefaultCodec(t *testing.T) {
	nc := connectNats(t)
	pn, err := New(nc)
	require.NoError(t, err)
	assert.Equal(t, "application/protobuf", pn.Codec().ContentType())
}

func TestNew_DefaultTimeout(t *testing.T) {
	nc := connectNats(t)
	pn, err := New(nc)
	require.NoError(t, err)
	assert.Equal(t, 5*time.Second, pn.defaultTimeout)
}

func TestNew_CustomTimeout(t *testing.T) {
	nc := connectNats(t)
	pn, err := New(nc, WithDefaultTimeout(10*time.Second))
	require.NoError(t, err)
	assert.Equal(t, 10*time.Second, pn.defaultTimeout)
}

func TestPublish_WithHeaders(t *testing.T) {
	nc := connectNats(t)
	pn, err := New(nc)
	require.NoError(t, err)

	var gotHeaders nats.Header
	done := make(chan struct{})

	sub, err := nc.Subscribe("test.pub.headers", func(m *nats.Msg) {
		gotHeaders = m.Header
		close(done)
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	err = pn.Publish(context.Background(), "test.pub.headers", wrapperspb.String("hi"),
		WithHeaders(nats.Header{"X-Custom": []string{"val"}}))
	require.NoError(t, err)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}
	assert.Equal(t, "val", gotHeaders.Get("X-Custom"))
}

func TestPublish_SubjectOverride(t *testing.T) {
	nc := connectNats(t)
	pn, err := New(nc)
	require.NoError(t, err)

	done := make(chan struct{})
	sub, err := nc.Subscribe("actual.subject", func(m *nats.Msg) {
		close(done)
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	err = pn.Publish(context.Background(), "wrong.subject", wrapperspb.String("x"),
		WithSubject("actual.subject"))
	require.NoError(t, err)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}
}

func TestRequest_WithHeaders(t *testing.T) {
	nc := connectNats(t)
	pn, err := New(nc)
	require.NoError(t, err)

	var gotHeader string
	sub, err := pn.Subscribe("test.req.headers", "", func() proto.Message {
		return &wrapperspb.StringValue{}
	}, func(ctx context.Context, m *nats.Msg, req proto.Message) (proto.Message, error) {
		gotHeader = m.Header.Get("X-Test")
		return wrapperspb.String("ok"), nil
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	resp := &wrapperspb.StringValue{}
	err = pn.Request(context.Background(), "test.req.headers", wrapperspb.String("x"), resp,
		WithHeaders(nats.Header{"X-Test": []string{"hello"}}))
	require.NoError(t, err)
	assert.Equal(t, "hello", gotHeader)
}

func TestRequest_HandlerPlainError(t *testing.T) {
	nc := connectNats(t)
	pn, err := New(nc)
	require.NoError(t, err)

	sub, err := pn.Subscribe("test.planerr", "", func() proto.Message {
		return &wrapperspb.StringValue{}
	}, func(ctx context.Context, m *nats.Msg, req proto.Message) (proto.Message, error) {
		return nil, assert.AnError // plain Go error, not *Error
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	err = pn.Request(context.Background(), "test.planerr", wrapperspb.String("x"), &wrapperspb.StringValue{})
	var pnErr *Error
	require.ErrorAs(t, err, &pnErr)
	assert.Equal(t, 500, pnErr.Code) // plain errors become 500
}

func TestSubscribe_BadPayload(t *testing.T) {
	nc := connectNats(t)
	pn, err := New(nc)
	require.NoError(t, err)

	// Handler for request/reply that receives invalid protobuf
	sub, err := pn.Subscribe("test.badpayload", "", func() proto.Message {
		return &wrapperspb.StringValue{}
	}, func(ctx context.Context, m *nats.Msg, req proto.Message) (proto.Message, error) {
		t.Fatal("handler should not be called with bad payload")
		return nil, nil
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	// Send garbage data as a request
	resp, err := nc.Request("test.badpayload", []byte("not protobuf!@#$"), time.Second)
	require.NoError(t, err)
	// Should get a 400 error back
	assert.Equal(t, "400", resp.Header.Get(headerErrorCode))
}

func TestSubscribePublish_BadPayload(t *testing.T) {
	nc := connectNats(t)
	pn, err := New(nc)
	require.NoError(t, err)

	handlerCalled := false
	sub, err := pn.SubscribePublish("test.badpub", "", func() proto.Message {
		return &wrapperspb.StringValue{}
	}, func(ctx context.Context, req proto.Message) error {
		handlerCalled = true
		return nil
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	// Publish garbage
	require.NoError(t, nc.Publish("test.badpub", []byte("garbage")))
	nc.Flush()
	time.Sleep(100 * time.Millisecond)
	assert.False(t, handlerCalled)
}

func TestSubscribePublish_WithQueueGroup(t *testing.T) {
	nc := connectNats(t)
	pn, err := New(nc)
	require.NoError(t, err)

	var count int
	var mu sync.Mutex
	done := make(chan struct{}, 2)

	newHandler := func() (*nats.Subscription, error) {
		return pn.SubscribePublish("test.pubq", "workers", func() proto.Message {
			return &wrapperspb.StringValue{}
		}, func(ctx context.Context, req proto.Message) error {
			mu.Lock()
			count++
			mu.Unlock()
			done <- struct{}{}
			return nil
		})
	}

	sub1, err := newHandler()
	require.NoError(t, err)
	sub2, err := newHandler()
	require.NoError(t, err)
	defer sub1.Unsubscribe()
	defer sub2.Unsubscribe()

	require.NoError(t, pn.Publish(context.Background(), "test.pubq", wrapperspb.String("x")))
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}
	time.Sleep(50 * time.Millisecond) // extra wait for potential duplicate

	mu.Lock()
	assert.Equal(t, 1, count) // queue group ensures only one gets it
	mu.Unlock()
}

func TestSubscribePublish_HandlerPanic(t *testing.T) {
	nc := connectNats(t)
	pn, err := New(nc)
	require.NoError(t, err)

	done := make(chan struct{})
	sub, err := pn.SubscribePublish("test.pubpanic", "", func() proto.Message {
		return &wrapperspb.StringValue{}
	}, func(ctx context.Context, req proto.Message) error {
		defer close(done)
		panic("boom")
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	require.NoError(t, pn.Publish(context.Background(), "test.pubpanic", wrapperspb.String("x")))
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}
	// If we get here, the panic was recovered (no crash)
}

func TestSubscribe_NoReply(t *testing.T) {
	// Test that a Subscribe handler receiving a message without reply subject
	// does not crash — it just calls the handler and discards the result.
	nc := connectNats(t)
	pn, err := New(nc)
	require.NoError(t, err)

	done := make(chan struct{})
	sub, err := pn.Subscribe("test.noreply", "", func() proto.Message {
		return &wrapperspb.StringValue{}
	}, func(ctx context.Context, m *nats.Msg, req proto.Message) (proto.Message, error) {
		close(done)
		return wrapperspb.String("result"), nil
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	// Publish (not Request) so there's no reply subject
	data, _ := proto.Marshal(wrapperspb.String("hello"))
	require.NoError(t, nc.Publish("test.noreply", data))

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}
}

func TestSubscribe_HandlerReturnsNilResponse(t *testing.T) {
	nc := connectNats(t)
	pn, err := New(nc)
	require.NoError(t, err)

	sub, err := pn.Subscribe("test.nilresp", "", func() proto.Message {
		return &wrapperspb.StringValue{}
	}, func(ctx context.Context, m *nats.Msg, req proto.Message) (proto.Message, error) {
		return nil, nil // nil response, no error
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	resp, err := nc.Request("test.nilresp", []byte{}, time.Second)
	require.NoError(t, err)
	// Should get an empty reply (no error headers, no data)
	assert.Empty(t, resp.Header.Get(headerError))
	assert.Empty(t, resp.Data)
}

func TestSubscribe_ContextHasSubjectAndMsg(t *testing.T) {
	nc := connectNats(t)
	pn, err := New(nc)
	require.NoError(t, err)

	var gotSubject string
	var gotMsg *nats.Msg
	sub, err := pn.Subscribe("test.ctxfull", "", func() proto.Message {
		return &wrapperspb.StringValue{}
	}, func(ctx context.Context, m *nats.Msg, req proto.Message) (proto.Message, error) {
		gotSubject = SubjectFromContext(ctx)
		gotMsg = MsgFromContext(ctx)
		return wrapperspb.String("ok"), nil
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	require.NoError(t, pn.Request(context.Background(), "test.ctxfull", wrapperspb.String("x"), &wrapperspb.StringValue{}))
	assert.Equal(t, "test.ctxfull", gotSubject)
	assert.NotNil(t, gotMsg)
	assert.Equal(t, "test.ctxfull", gotMsg.Subject)
}

func TestClientInterceptor(t *testing.T) {
	nc := connectNats(t)

	var interceptedMethod string
	interceptor := func(ctx context.Context, method, subject string, req proto.Message, next ClientInvoker) (proto.Message, error) {
		interceptedMethod = method
		return next(ctx, subject, req)
	}

	pn, err := New(nc, WithClientInterceptor(interceptor))
	require.NoError(t, err)
	// Verify interceptor was added
	assert.Len(t, pn.clientInterceptors, 1)

	// The interceptor is stored but the current Request/Publish don't call it
	// (interceptor chain is wired in generated code). Just verify it's registered.
	_ = interceptedMethod
}

func TestRequest_JSONCodec(t *testing.T) {
	nc := connectNats(t)
	pn, err := New(nc, WithCodec(JSONCodec))
	require.NoError(t, err)

	sub, err := pn.Subscribe("test.json", "", func() proto.Message {
		return &wrapperspb.StringValue{}
	}, func(ctx context.Context, m *nats.Msg, req proto.Message) (proto.Message, error) {
		return wrapperspb.String(req.(*wrapperspb.StringValue).Value + " json"), nil
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	resp := &wrapperspb.StringValue{}
	require.NoError(t, pn.Request(context.Background(), "test.json", wrapperspb.String("hello"), resp))
	assert.Equal(t, "hello json", resp.Value)
}

func TestPublish_JSONCodec(t *testing.T) {
	nc := connectNats(t)
	pn, err := New(nc, WithCodec(JSONCodec))
	require.NoError(t, err)

	done := make(chan string)
	sub, err := pn.SubscribePublish("test.jsonpub", "", func() proto.Message {
		return &wrapperspb.StringValue{}
	}, func(ctx context.Context, req proto.Message) error {
		done <- req.(*wrapperspb.StringValue).Value
		return nil
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	require.NoError(t, pn.Publish(context.Background(), "test.jsonpub", wrapperspb.String("json-msg")))

	select {
	case v := <-done:
		assert.Equal(t, "json-msg", v)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}
}
