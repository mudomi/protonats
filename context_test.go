package protonats

import (
	"context"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
)

func TestContextHelpers(t *testing.T) {
	ctx := context.Background()
	assert.Equal(t, "", SubjectFromContext(ctx))
	assert.Nil(t, MsgFromContext(ctx))

	ctx = contextWithSubject(ctx, "orders.abc123")
	ctx = contextWithMsg(ctx, &nats.Msg{Subject: "orders.abc123"})

	assert.Equal(t, "orders.abc123", SubjectFromContext(ctx))
	assert.Equal(t, "orders.abc123", MsgFromContext(ctx).Subject)
}
