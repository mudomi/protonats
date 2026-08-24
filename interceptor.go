package protonats

import (
	"context"

	"google.golang.org/protobuf/proto"
)

// ClientInvoker performs the actual client call.
type ClientInvoker func(ctx context.Context, subject string, req proto.Message) (proto.Message, error)

// ClientInterceptor intercepts outbound client calls. method is the fully
// qualified proto method name (or the subject, for calls made directly on
// Conn); subject may still be overridden by the interceptor when calling next.
type ClientInterceptor func(ctx context.Context, method, subject string, req proto.Message, next ClientInvoker) (proto.Message, error)

// HandlerInvoker performs the actual handler call.
type HandlerInvoker func(ctx context.Context, req proto.Message) (proto.Message, error)

// HandlerInterceptor intercepts inbound handler calls. method is the fully
// qualified proto method name; subject is the concrete subject the message
// arrived on (wildcards resolved).
type HandlerInterceptor func(ctx context.Context, method, subject string, req proto.Message, next HandlerInvoker) (proto.Message, error)

// invokeClientChain runs a client call through the interceptors, first
// registered outermost, ending at final.
func invokeClientChain(ctx context.Context, ics []ClientInterceptor, method, subject string, req proto.Message, final ClientInvoker) (proto.Message, error) {
	next := final
	for i := len(ics) - 1; i >= 0; i-- {
		ic, inner := ics[i], next
		next = func(ctx context.Context, subject string, req proto.Message) (proto.Message, error) {
			return ic(ctx, method, subject, req, inner)
		}
	}
	return next(ctx, subject, req)
}

// invokeHandlerChain runs an inbound message through the interceptors, first
// registered outermost, ending at final.
func invokeHandlerChain(ctx context.Context, ics []HandlerInterceptor, method, subject string, req proto.Message, final HandlerInvoker) (proto.Message, error) {
	next := final
	for i := len(ics) - 1; i >= 0; i-- {
		ic, inner := ics[i], next
		next = func(ctx context.Context, req proto.Message) (proto.Message, error) {
			return ic(ctx, method, subject, req, inner)
		}
	}
	return next(ctx, req)
}
