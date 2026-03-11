package protonats

import (
	"context"

	"github.com/nats-io/nats.go"
)

type contextKey int

const (
	ctxKeySubject contextKey = iota
	ctxKeyMsg
)

// SubjectFromContext returns the NATS subject from a handler context.
func SubjectFromContext(ctx context.Context) string {
	v, _ := ctx.Value(ctxKeySubject).(string)
	return v
}

// MsgFromContext returns the raw *nats.Msg from a handler context.
func MsgFromContext(ctx context.Context) *nats.Msg {
	v, _ := ctx.Value(ctxKeyMsg).(*nats.Msg)
	return v
}

func contextWithSubject(ctx context.Context, subject string) context.Context {
	return context.WithValue(ctx, ctxKeySubject, subject)
}

func contextWithMsg(ctx context.Context, msg *nats.Msg) context.Context {
	return context.WithValue(ctx, ctxKeyMsg, msg)
}
