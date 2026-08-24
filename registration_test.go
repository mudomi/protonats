package protonats

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestRegistration_DrainEmpty(t *testing.T) {
	reg := &Registration{}
	require.NoError(t, reg.Drain())
}

func TestRegistration_UnsubscribeEmpty(t *testing.T) {
	reg := &Registration{}
	require.NoError(t, reg.Unsubscribe())
}

func TestRegistration_DrainWithNats(t *testing.T) {
	pn, _ := New(connectNats(t))

	reg := &Registration{}
	sub, err := pn.Subscribe("test.Method", "test.reg.drain", HandlerOptions{}, newStringValue, echoHandler)
	require.NoError(t, err)
	reg.AddSubscription(sub)

	require.NoError(t, reg.Drain())
	require.NoError(t, reg.Drain(), "draining twice must stay a no-op, so deferred cleanup is safe")
}

func TestRegistration_UnsubscribeStopsDelivery(t *testing.T) {
	pn, _ := New(connectNats(t))

	reg := &Registration{}
	sub, err := pn.Subscribe("test.Method", "test.reg.unsub", HandlerOptions{}, newStringValue, echoHandler)
	require.NoError(t, err)
	reg.AddSubscription(sub)
	require.NoError(t, reg.Unsubscribe())

	err = pn.Request(context.Background(), "test.reg.unsub", wrapperspb.String("x"), &wrapperspb.StringValue{}, WithTimeout(time.Second))
	require.Error(t, err)
}

func TestRegistration_ConcurrentAdd(t *testing.T) {
	pn, _ := New(connectNats(t))
	reg := &Registration{}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sub, err := pn.Subscribe("test.Method", "test.reg.concurrent", HandlerOptions{}, newStringValue, echoHandler)
			if err == nil {
				reg.AddSubscription(sub)
			}
		}()
	}
	wg.Wait()
	require.NoError(t, reg.Unsubscribe())
}

func TestHandlerOptions_QueueGroupDefaults(t *testing.T) {
	const method = "pkg.Svc.Do"

	// Unset: the caller's default (the method name) wins.
	ho := ApplyHandlerOptions(nil)
	assert.Equal(t, method, ho.queueGroupOr(method))

	ho = ApplyHandlerOptions([]HandlerOption{WithQueueGroup("custom")})
	assert.Equal(t, "custom", ho.queueGroupOr(method))

	ho = ApplyHandlerOptions([]HandlerOption{WithNoQueueGroup()})
	assert.Equal(t, "", ho.queueGroupOr(method))
}

func TestHandlerOptions_Interceptors(t *testing.T) {
	ic := func(ctx context.Context, method, subject string, req proto.Message, next HandlerInvoker) (proto.Message, error) {
		return next(ctx, req)
	}
	ho := ApplyHandlerOptions([]HandlerOption{WithHandlerInterceptor(ic), WithHandlerInterceptor(ic)})
	assert.Len(t, ho.interceptors, 2)
}

func TestHandlerOptions_ConsumerConfig(t *testing.T) {
	cfg := jetstream.ConsumerConfig{MaxDeliver: 3}
	ho := ApplyHandlerOptions([]HandlerOption{WithConsumerConfig("ProcessEvent", cfg)})

	got, ok := ho.consumerConfigs["ProcessEvent"]
	require.True(t, ok)
	assert.Equal(t, 3, got.MaxDeliver)
}

func TestShortMethodName(t *testing.T) {
	assert.Equal(t, "ProcessEvent", shortMethodName("pkg.Svc.ProcessEvent"))
	assert.Equal(t, "Bare", shortMethodName("Bare"))
}
