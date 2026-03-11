package protonats

import (
	"context"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
)

func TestSubjectFromContext_WrongType(t *testing.T) {
	// Store a non-string at the subject key.
	ctx := context.WithValue(context.Background(), ctxKeySubject, 12345)
	assert.Equal(t, "", SubjectFromContext(ctx))
}

func TestMsgFromContext_WrongType(t *testing.T) {
	ctx := context.WithValue(context.Background(), ctxKeyMsg, "not a msg")
	assert.Nil(t, MsgFromContext(ctx))
}

func TestContextWithSubject_EmptyString(t *testing.T) {
	ctx := contextWithSubject(context.Background(), "")
	assert.Equal(t, "", SubjectFromContext(ctx))
}

func TestContextWithMsg_NilMsg(t *testing.T) {
	ctx := contextWithMsg(context.Background(), nil)
	assert.Nil(t, MsgFromContext(ctx))
}

func TestContext_OverwriteSubject(t *testing.T) {
	ctx := contextWithSubject(context.Background(), "first")
	ctx = contextWithSubject(ctx, "second")
	assert.Equal(t, "second", SubjectFromContext(ctx))
}

func TestContext_SubjectAndMsgIndependent(t *testing.T) {
	ctx := context.Background()
	ctx = contextWithSubject(ctx, "orders.abc")
	ctx = contextWithMsg(ctx, &nats.Msg{Subject: "different.subject"})

	// Both values are independent.
	assert.Equal(t, "orders.abc", SubjectFromContext(ctx))
	assert.Equal(t, "different.subject", MsgFromContext(ctx).Subject)
}
