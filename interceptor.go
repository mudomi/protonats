package protonats

import (
	"context"

	"google.golang.org/protobuf/proto"
)

// ClientInvoker performs the actual client call.
type ClientInvoker func(ctx context.Context, subject string, req proto.Message) (proto.Message, error)

// ClientInterceptor intercepts outbound client calls.
type ClientInterceptor func(ctx context.Context, method, subject string, req proto.Message, next ClientInvoker) (proto.Message, error)

// HandlerInvoker performs the actual handler call.
type HandlerInvoker func(ctx context.Context, req proto.Message) (proto.Message, error)

// HandlerInterceptor intercepts inbound handler calls.
type HandlerInterceptor func(ctx context.Context, method, subject string, req proto.Message, next HandlerInvoker) (proto.Message, error)
