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
