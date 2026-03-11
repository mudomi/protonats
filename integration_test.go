package protonats

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestRequest_EmptyProtoMessage(t *testing.T) {
	nc := connectNats(t)
	pn, _ := New(nc)

	sub, err := pn.Subscribe("test.empty", "", func() proto.Message {
		return &emptypb.Empty{}
	}, func(ctx context.Context, m *nats.Msg, req proto.Message) (proto.Message, error) {
		return &emptypb.Empty{}, nil
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	resp := &emptypb.Empty{}
	require.NoError(t, pn.Request(context.Background(), "test.empty", &emptypb.Empty{}, resp))
}

func TestRequest_ConcurrentRequests(t *testing.T) {
	nc := connectNats(t)
	pn, _ := New(nc)

	sub, err := pn.Subscribe("test.concurrent", "", func() proto.Message {
		return &wrapperspb.StringValue{}
	}, func(ctx context.Context, m *nats.Msg, req proto.Message) (proto.Message, error) {
		return wrapperspb.String(req.(*wrapperspb.StringValue).Value + "-reply"), nil
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	var wg sync.WaitGroup
	errs := make([]error, 50)
	results := make([]string, 50)
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp := &wrapperspb.StringValue{}
			errs[i] = pn.Request(context.Background(), "test.concurrent",
				wrapperspb.String("msg"), resp)
			if errs[i] == nil {
				results[i] = resp.Value
			}
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		assert.NoError(t, err, "request %d failed", i)
		assert.Equal(t, "msg-reply", results[i])
	}
}

func TestPublish_ConcurrentPublishes(t *testing.T) {
	nc := connectNats(t)
	pn, _ := New(nc)

	var count atomic.Int32
	done := make(chan struct{})

	sub, err := pn.SubscribePublish("test.concpub", "", func() proto.Message {
		return &wrapperspb.StringValue{}
	}, func(ctx context.Context, req proto.Message) error {
		if count.Add(1) == 20 {
			close(done)
		}
		return nil
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pn.Publish(context.Background(), "test.concpub", wrapperspb.String("x"))
		}()
	}
	wg.Wait()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout — not all messages received")
	}
	assert.Equal(t, int32(20), count.Load())
}

func TestSubscribe_HandlerReturnsWrappedError(t *testing.T) {
	nc := connectNats(t)
	pn, _ := New(nc)

	sub, err := pn.Subscribe("test.wraperr", "", func() proto.Message {
		return &wrapperspb.StringValue{}
	}, func(ctx context.Context, m *nats.Msg, req proto.Message) (proto.Message, error) {
		return nil, Errorf(422, "validation failed")
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	resp := &wrapperspb.StringValue{}
	err = pn.Request(context.Background(), "test.wraperr", wrapperspb.String("x"), resp)
	var pnErr *Error
	require.ErrorAs(t, err, &pnErr)
	assert.Equal(t, 422, pnErr.Code)
	assert.Equal(t, "validation failed", pnErr.Message)
}

func TestSubscribe_EmptyDataRequest(t *testing.T) {
	nc := connectNats(t)
	pn, _ := New(nc)

	sub, err := pn.Subscribe("test.emptyreq", "", func() proto.Message {
		return &wrapperspb.StringValue{}
	}, func(ctx context.Context, m *nats.Msg, req proto.Message) (proto.Message, error) {
		// Empty bytes unmarshal to zero-value message.
		return wrapperspb.String(req.(*wrapperspb.StringValue).Value), nil
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	// Send request with empty data (valid proto).
	resp, err := nc.Request("test.emptyreq", []byte{}, time.Second)
	require.NoError(t, err)
	assert.Empty(t, resp.Header.Get(headerError))
}

func TestRegistration_DrainThenUnsubscribe(t *testing.T) {
	nc := connectNats(t)
	sub1, _ := nc.Subscribe("test.dl.1", func(*nats.Msg) {})
	sub2, _ := nc.Subscribe("test.dl.2", func(*nats.Msg) {})

	reg := &Registration{}
	reg.AddSubscription(sub1)
	reg.AddSubscription(sub2)

	assert.NoError(t, reg.Drain())
	// Unsubscribe after Drain should not panic.
	_ = reg.Unsubscribe()
}

func TestRegistration_DoubleDrain(t *testing.T) {
	nc := connectNats(t)
	sub, _ := nc.Subscribe("test.dd", func(*nats.Msg) {})

	reg := &Registration{}
	reg.AddSubscription(sub)

	assert.NoError(t, reg.Drain())
	// Second drain on already-drained subscription.
	_ = reg.Drain()
}

func TestRegistration_ConcurrentAddAndDrain(t *testing.T) {
	nc := connectNats(t)

	reg := &Registration{}
	var wg sync.WaitGroup

	// Add subscriptions concurrently.
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sub, _ := nc.Subscribe("test.conc."+string(rune('a'+i)), func(*nats.Msg) {})
			reg.AddSubscription(sub)
		}(i)
	}
	wg.Wait()
	assert.NoError(t, reg.Drain())
}

func TestRequest_ZeroTimeout(t *testing.T) {
	nc := connectNats(t)
	pn, _ := New(nc)

	// Zero timeout should fail immediately.
	err := pn.Request(context.Background(), "test.zero.timeout",
		wrapperspb.String("x"), &wrapperspb.StringValue{},
		WithTimeout(0))
	assert.Error(t, err)
}

func TestRequest_VeryShorTimeout(t *testing.T) {
	nc := connectNats(t)
	pn, _ := New(nc)

	// 1 nanosecond timeout — should always timeout.
	err := pn.Request(context.Background(), "test.nano.timeout",
		wrapperspb.String("x"), &wrapperspb.StringValue{},
		WithTimeout(1*time.Nanosecond))
	assert.Error(t, err)
}

func TestSubscribe_MultipleHandlersNoQueueGroup(t *testing.T) {
	nc := connectNats(t)
	pn, _ := New(nc)

	var count atomic.Int32
	done := make(chan struct{})

	newHandler := func() *nats.Subscription {
		sub, _ := pn.Subscribe("test.fanout", "", func() proto.Message {
			return &wrapperspb.StringValue{}
		}, func(ctx context.Context, m *nats.Msg, req proto.Message) (proto.Message, error) {
			if count.Add(1) == 2 {
				close(done)
			}
			return wrapperspb.String("ok"), nil
		})
		return sub
	}

	sub1 := newHandler()
	sub2 := newHandler()
	defer sub1.Unsubscribe()
	defer sub2.Unsubscribe()

	// Without queue group, both handlers should receive the message.
	pn.Publish(context.Background(), "test.fanout", wrapperspb.String("x"))

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout — expected fan-out to both handlers")
	}
	assert.Equal(t, int32(2), count.Load())
}

func TestSubscribe_PanicWithNilReply(t *testing.T) {
	nc := connectNats(t)
	pn, _ := New(nc)

	done := make(chan struct{})
	sub, err := pn.Subscribe("test.panicnoreply", "", func() proto.Message {
		return &wrapperspb.StringValue{}
	}, func(ctx context.Context, m *nats.Msg, req proto.Message) (proto.Message, error) {
		defer close(done)
		panic("boom")
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	// Publish without reply — panic should be recovered without crash.
	data, _ := proto.Marshal(wrapperspb.String("x"))
	nc.Publish("test.panicnoreply", data)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}
}

func TestPublish_EmptyMessage(t *testing.T) {
	nc := connectNats(t)
	pn, _ := New(nc)

	done := make(chan struct{})
	sub, err := pn.SubscribePublish("test.emptypub", "", func() proto.Message {
		return &emptypb.Empty{}
	}, func(ctx context.Context, req proto.Message) error {
		close(done)
		return nil
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	require.NoError(t, pn.Publish(context.Background(), "test.emptypub", &emptypb.Empty{}))

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}
}

func TestSubscribePublish_ContextHasCorrectValues(t *testing.T) {
	nc := connectNats(t)
	pn, _ := New(nc)

	var gotSubject string
	var gotMsg *nats.Msg
	done := make(chan struct{})

	sub, _ := pn.SubscribePublish("test.pubctx", "", func() proto.Message {
		return &wrapperspb.StringValue{}
	}, func(ctx context.Context, req proto.Message) error {
		gotSubject = SubjectFromContext(ctx)
		gotMsg = MsgFromContext(ctx)
		close(done)
		return nil
	})
	defer sub.Unsubscribe()

	pn.Publish(context.Background(), "test.pubctx", wrapperspb.String("x"))
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}
	assert.Equal(t, "test.pubctx", gotSubject)
	assert.NotNil(t, gotMsg)
}
