package protonats

import (
	"context"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

func TestRegistration_DrainEmpty(t *testing.T) {
	reg := &Registration{}
	assert.NoError(t, reg.Drain())
}

func TestRegistration_UnsubscribeEmpty(t *testing.T) {
	reg := &Registration{}
	assert.NoError(t, reg.Unsubscribe())
}

func TestRegistration_DrainWithNats(t *testing.T) {
	nc := connectNats(t)
	sub1, _ := nc.Subscribe("test.drain.1", func(m *nats.Msg) {})
	sub2, _ := nc.Subscribe("test.drain.2", func(m *nats.Msg) {})

	reg := &Registration{}
	reg.AddSubscription(sub1)
	reg.AddSubscription(sub2)

	assert.NoError(t, reg.Drain())
}

func TestHandlerOptions_Multiple(t *testing.T) {
	interceptor := func(_ context.Context, _, _ string, _ proto.Message, next HandlerInvoker) (proto.Message, error) {
		return next(context.Background(), nil)
	}

	ho := ApplyHandlerOptions([]HandlerOption{
		WithQueueGroup("grp"),
		WithHandlerInterceptor(interceptor),
	})
	assert.Equal(t, "grp", ho.QueueGroup)
	assert.Len(t, ho.HandlerInterceptors, 1)
}

func TestHandlerOptions_Defaults(t *testing.T) {
	ho := ApplyHandlerOptions(nil)
	assert.Empty(t, ho.QueueGroup)
	assert.Nil(t, ho.HandlerInterceptors)
}
