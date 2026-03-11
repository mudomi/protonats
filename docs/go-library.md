# ProtoNats Go Library Guide

This document describes how to use the generated Go code and the `protonats` runtime package.

## Installation

```bash
go get github.com/you/protonats
go install github.com/you/protonats/cmd/protoc-gen-protonats@latest
```

## Code Generation

```bash
protoc \
  --go_out=gen --go_opt=paths=source_relative \
  --protonats_out=gen --protonats_opt=paths=source_relative \
  -I proto \
  proto/myapp/orders/orders.proto
```

Produces per proto file:
- `orders.pb.go` -- Standard protobuf types (from `protoc-gen-go`)
- `orders_protonats.pb.go` -- ProtoNats clients, handlers, subjects, KV helpers

---

## Connection Setup

ProtoNats wraps a `*nats.Conn`. It does not own the connection -- you create it, you close it.

```go
import (
    "github.com/nats-io/nats.go"
    "github.com/you/protonats"
)

func main() {
    nc, err := nats.Connect("nats://localhost:4222",
        nats.Name("my-service"),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer nc.Drain()

    pn, err := protonats.New(nc)
    if err != nil {
        log.Fatal(err)
    }

    // ... use pn to create clients and register handlers
}
```

If you need JetStream (for `JETSTREAM_PUBLISH`, `JETSTREAM_CONSUME`, or KV):

```go
pn, err := protonats.New(nc, protonats.WithJetStream())
```

### Accessing Raw NATS

ProtoNats never hides the underlying connections:

```go
nc := pn.NatsConn()       // *nats.Conn
js := pn.JetStream()      // jetstream.JetStream (nil if WithJetStream not used)
```

This is the escape hatch for anything ProtoNats doesn't cover: object stores, complex stream management, raw subscriptions, or any other NATS feature.

---

## Using Generated Clients

For each service, ProtoNats generates a client struct.

### Request/Response

```go
client := orders.NewOrderServiceClient(pn)

resp, err := client.GetOrder(ctx, &orders.GetOrderRequest{
    OrderId: "ord_abc123",
})
if err != nil {
    log.Fatal(err)
}
log.Printf("Order status: %s", resp.Status)
```

The generated code handles serialization, subject construction (including dynamic segment interpolation), the NATS request/reply exchange, error header checking, and response deserialization.

### Publish (fire-and-forget)

```go
err := client.OrderCreated(ctx, &orders.OrderCreatedEvent{
    Order:     myOrder,
    Timestamp: time.Now().UnixMilli(),
})
// err is only from serialization or publish failure
```

### JetStream Publish

```go
ack, err := client.OrderCreated(ctx, &orders.OrderCreatedEvent{
    Order: myOrder,
})
if err != nil {
    log.Fatal(err)
}
log.Printf("Persisted to %s seq %d", ack.Stream, ack.Sequence)
```

### Per-Call Options

Override defaults on individual calls:

```go
resp, err := client.GetOrder(ctx, req,
    protonats.WithTimeout(2 * time.Second),
    protonats.WithHeaders(nats.Header{
        "X-Request-Id": []string{requestId},
    }),
)
```

| Option | Description |
|--------|-------------|
| `WithTimeout(d)` | Override request timeout for this call |
| `WithHeaders(h)` | Add NATS headers to this message |
| `WithSubject(s)` | Completely override the resolved subject |

---

## Implementing Handlers

For each service, ProtoNats generates a handler interface and a registration function.

### The Handler Interface

```go
// Generated
type OrderServiceHandler interface {
    GetOrder(ctx context.Context, req *GetOrderRequest) (*Order, error)
    Create(ctx context.Context, req *CreateOrderRequest) (*Order, error)
    ProcessOrderEvent(ctx context.Context, msg *OrderCreatedEvent, ack protonats.Acker) error
}
```

Note: `OrderCreated` (JETSTREAM_PUBLISH) does not appear -- it's client/publish-only.

### Forward Compatibility

ProtoNats generates an `UnimplementedOrderServiceHandler` struct (following the gRPC pattern). Embed it to avoid breakage when new methods are added to the service:

```go
type orderHandler struct {
    orders.UnimplementedOrderServiceHandler
    db *sql.DB
}
```

Methods on `Unimplemented*` return `protonats.Errorf(501, "not implemented")`.

### Implementation

```go
func (h *orderHandler) GetOrder(ctx context.Context, req *orders.GetOrderRequest) (*orders.Order, error) {
    order, err := h.db.GetOrder(ctx, req.OrderId)
    if err != nil {
        return nil, protonats.Errorf(404, "order not found: %s", req.OrderId)
    }
    return order, nil
}

func (h *orderHandler) Create(ctx context.Context, req *orders.CreateOrderRequest) (*orders.Order, error) {
    order, err := h.db.CreateOrder(ctx, req)
    if err != nil {
        return nil, protonats.Errorf(500, "failed to create: %v", err)
    }
    return order, nil
}

func (h *orderHandler) ProcessOrderEvent(ctx context.Context, msg *orders.OrderCreatedEvent, ack protonats.Acker) error {
    if err := h.indexOrder(ctx, msg.Order); err != nil {
        return err // NAK'd for redelivery
    }
    return nil // auto-acked
}
```

### Registration

```go
reg, err := orders.RegisterOrderServiceHandler(pn, handler)
if err != nil {
    log.Fatal(err)
}
defer reg.Drain()
```

### Registration Options

All operational configuration happens here, not in proto:

```go
reg, err := orders.RegisterOrderServiceHandler(pn, handler,
    // Queue group for load balancing (all request/reply + publish handlers)
    protonats.WithQueueGroup("order-svc"),

    // JetStream consumer configuration
    protonats.WithConsumerConfig("ProcessOrderEvent", jetstream.ConsumerConfig{
        AckPolicy:     jetstream.AckExplicitPolicy,
        MaxDeliver:    5,
        MaxAckPending: 100,
        AckWait:       30 * time.Second,
        DeliverPolicy: jetstream.DeliverNewPolicy,
        FilterSubject: "events.orders.>",
    }),

    // Override subject for a specific method
    protonats.WithSubjectOverride("GetOrder", "legacy.orders.{order_id}"),
)
```

| Option | Description |
|--------|-------------|
| `WithQueueGroup(q)` | Set queue group for all handler subscriptions. If empty, no queue group. |
| `WithConsumerConfig(method, cfg)` | Override JetStream consumer config for a specific JETSTREAM_CONSUME method. |
| `WithSubjectOverride(method, subj)` | Override the subject for a specific method. |
| `WithAutoAck(bool)` | For JetStream consumers: auto-ack on nil return. Default: true. |

---

## Error Handling

### Structured Errors

Handlers return errors using `protonats.Errorf(code, format, args...)`:

```go
return nil, protonats.Errorf(404, "order %s not found", req.OrderId)
```

The generated handler wrapper sets `Nats-Service-Error` and `Nats-Service-Error-Code` headers on the response. The generated client checks these headers and returns a `*protonats.Error`.

```go
resp, err := client.GetOrder(ctx, req)
if err != nil {
    var pnErr *protonats.Error
    if errors.As(err, &pnErr) {
        log.Printf("Service error %d: %s", pnErr.Code, pnErr.Message)
    }
}
```

Plain Go errors (not `protonats.Errorf`) are wrapped as code 500 automatically.

### JetStream Acker

For `JETSTREAM_CONSUME` handlers, the `protonats.Acker` interface provides:

| Method | Effect |
|--------|--------|
| `Ack()` | Acknowledge -- message processed successfully |
| `Nak()` | Negative ack -- redeliver immediately |
| `NakWithDelay(d)` | Redeliver after delay |
| `Term()` | Terminate -- no more redeliveries (dead letter) |
| `InProgress()` | Extend ack wait (still working on it) |

Default behavior based on handler return:

| Handler returns | Auto behavior |
|----------------|---------------|
| `nil` | `Ack()` |
| `error` | `Nak()` |
| `protonats.ErrTerminate` | `Term()` |

Call `ack` methods directly for fine-grained control:

```go
func (h *handler) ProcessEvent(ctx context.Context, msg *Event, ack protonats.Acker) error {
    ack.InProgress() // extend timeout

    if err := h.longProcess(ctx, msg); err != nil {
        ack.NakWithDelay(5 * time.Second)
        return nil // nil because we handled ack ourselves
    }

    ack.Ack()
    return nil
}
```

---

## KV Store Helpers

For messages with `kv` options:

```go
kvStore, err := orders.NewOrderStateKV(pn)
if err != nil {
    log.Fatal(err)
}
```

The bucket must already exist. Create it with the `nats` CLI, Terraform, or Go setup code:

```go
js := pn.JetStream()
_, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
    Bucket:  "order-state",
    History: 10,
    TTL:     7 * 24 * time.Hour,
})
```

### Operations

```go
// Get
state, revision, err := kvStore.Get(ctx, "ord_abc123")

// Put (key derived from key_template)
revision, err := kvStore.Put(ctx, &orders.OrderState{
    OrderId: "ord_abc123",
    Status:  orders.ORDER_STATUS_CONFIRMED,
})

// Create (only if key doesn't exist)
revision, err := kvStore.Create(ctx, state)

// Update (optimistic concurrency -- fails if revision changed)
newRev, err := kvStore.Update(ctx, state, revision)

// Delete
err := kvStore.Delete(ctx, "ord_abc123")

// Watch a single key
watcher, err := kvStore.Watch(ctx, "ord_abc123")
defer watcher.Stop()
for entry := range watcher.Updates() {
    if entry == nil { continue } // end of initial values
    log.Printf("Changed: %s → %s", entry.Key, entry.Value.Status)
}

// Watch all keys
watcher, err := kvStore.WatchAll(ctx)
```

The `Watcher[T]` provides:
- `Updates() <-chan *protonats.KVEntry[T]`
- `Stop()`

`KVEntry[T]` contains: `Key`, `Value *T`, `Revision uint64`, `Operation` (Put/Delete/Purge), `Created time.Time`.

---

## Middleware / Interceptors

### Client Interceptors

```go
type ClientInterceptor func(
    ctx context.Context,
    method string,      // "OrderService.GetOrder"
    subject string,     // resolved NATS subject
    req proto.Message,
    next ClientInvoker,
) (proto.Message, error)
```

Example:

```go
func logging(ctx context.Context, method, subject string, req proto.Message, next protonats.ClientInvoker) (proto.Message, error) {
    start := time.Now()
    resp, err := next(ctx, subject, req)
    log.Printf("%s %s %v err=%v", method, subject, time.Since(start), err)
    return resp, err
}

pn, _ := protonats.New(nc, protonats.WithClientInterceptor(logging))
```

### Handler Interceptors

```go
type HandlerInterceptor func(
    ctx context.Context,
    method string,
    subject string,
    req proto.Message,
    next HandlerInvoker,
) (proto.Message, error)
```

Add at registration time:

```go
reg, _ := orders.RegisterOrderServiceHandler(pn, handler,
    protonats.WithHandlerInterceptor(recoveryInterceptor),
    protonats.WithHandlerInterceptor(loggingInterceptor),
)
```

### Context Values

Handlers can access NATS metadata from context:

```go
subject := protonats.SubjectFromContext(ctx)    // actual NATS subject
natsMsg := protonats.MsgFromContext(ctx)         // raw *nats.Msg
meta := protonats.JetStreamMetaFromContext(ctx)  // JetStream metadata (nil for core NATS)
```

---

## Request-Many Helper

Request-Many (scatter-gather) is not a generated method type, but the runtime library provides a generic helper:

```go
responses, err := protonats.RequestMany[orders.SearchOrdersResponse](ctx, pn,
    "orders.search",
    &orders.SearchOrdersRequest{Query: "widget"},
    protonats.RequestManyOpts{
        MaxResponses: 10,
        Timeout:      2 * time.Second,
        Stall:        500 * time.Millisecond,
    },
)
```

This is a runtime-only helper -- no proto definition needed. You provide the subject and message directly.

---

## Serialization

Default: protobuf binary (`proto.Marshal`/`proto.Unmarshal`). Fast, compact, schema-evolved.

For JSON interop:

```go
pn, _ := protonats.New(nc, protonats.WithCodec(protonats.JSONCodec))
```

Custom codecs:

```go
type Codec interface {
    Marshal(proto.Message) ([]byte, error)
    Unmarshal([]byte, proto.Message) error
    ContentType() string  // "application/protobuf" or "application/json"
}
```

---

## Graceful Shutdown

```go
reg1, _ := orders.RegisterOrderServiceHandler(pn, orderHandler)
reg2, _ := users.RegisterUserServiceHandler(pn, userHandler)

// Wait for signal
<-ctx.Done()

// Drain handlers (finish in-flight, stop accepting new)
reg1.Drain()
reg2.Drain()

// Drain the NATS connection
nc.Drain()
```

---

## Pattern Decision Guide

```
Need a response?
├─ No → Need persistence?
│        ├─ Yes → JETSTREAM_PUBLISH (proto)
│        └─ No  → PUBLISH (proto)
└─ Yes → REQUEST_REPLY (proto, default)

Consuming from a stream?
└─ Yes → JETSTREAM_CONSUME (proto)

Need scatter-gather?
└─ Yes → protonats.RequestMany[T]() (runtime helper, no proto)

Need typed KV?
└─ Yes → kv message option (proto)

Anything else?
└─ Use pn.NatsConn() or pn.JetStream() directly
```
