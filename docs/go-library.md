# Go Library Guide

Using the generated Go code and the `protonats` runtime. See the
[README](../README.md) for installation and code generation.

## Connection

ProtoNats wraps a `*nats.Conn` and never takes ownership — you create it, you
close it.

```go
nc, err := nats.Connect("nats://localhost:4222", nats.Name("my-service"))
if err != nil {
    log.Fatal(err)
}
defer nc.Drain()

pn, err := protonats.New(nc)
```

| Option | Description |
|---|---|
| `WithDefaultTimeout(d)` | Timeout for requests and JetStream acks (default 5s) |
| `WithCodec(c)` | Serialization; `protonats.JSONCodec` for JSON interop |
| `WithClientInterceptor(i)` | Add a client interceptor (repeatable) |
| `WithLogger(l)` | Logger for errors with no caller to return to; `nil` disables |

Nothing is hidden: `pn.NatsConn()` returns the `*nats.Conn` and
`pn.JetStream()` the JetStream context (created on first use). Use them for
anything ProtoNats does not cover — KV, object store, stream management.

## Clients

```go
client := orders.NewOrderServiceClient(pn)

// Request/reply
order, err := client.GetOrder(ctx, &orders.GetOrderRequest{OrderId: "abc"})

// Publish — err covers serialization and publish failure only
err = client.OrderCreated(ctx, &orders.OrderCreatedEvent{Order: order})

// JetStream publish — waits for the server ack
ack, err := client.EmitOrderEvent(ctx, &orders.OrderEvent{})
log.Printf("persisted to %s seq %d", ack.Stream, ack.Sequence)
```

Subject construction, serialization, error headers, and response decoding are
all handled by the generated code.

Per-call options: `WithTimeout(d)`, `WithHeaders(h)`, and `WithSubject(s)` to
override the resolved subject entirely.

## Handlers

Implement the generated interface and register it. Embed the generated
`Unimplemented…Handler` so that adding a method to the service does not break
compilation; its methods return `protonats.Errorf(501, "not implemented")`.

```go
type orderHandler struct {
    orders.UnimplementedOrderServiceHandler
    db *sql.DB
}

func (h *orderHandler) GetOrder(ctx context.Context, req *orders.GetOrderRequest) (*orders.Order, error) {
    order, err := h.db.GetOrder(ctx, req.OrderId)
    if err != nil {
        return nil, protonats.Errorf(404, "order not found: %s", req.OrderId)
    }
    return order, nil
}

reg, err := orders.RegisterOrderServiceHandler(pn, &orderHandler{db: db})
if err != nil {
    log.Fatal(err)
}
defer reg.Drain()
```

Registration is atomic: if any subscription or consumer fails to start,
everything already registered is torn down and an error is returned.

Handlers can read NATS metadata from the context:

```go
subject := protonats.SubjectFromContext(ctx)  // concrete subject, wildcards resolved
msg := protonats.MsgFromContext(ctx)          // raw *nats.Msg; nil in JetStream consumers
```

### Registration options

| Option | Description |
|---|---|
| `WithQueueGroup(q)` | One queue group for every method (see the caveat below) |
| `WithNoQueueGroup()` | No queue groups: every instance receives every message |
| `WithHandlerInterceptor(i)` | Add a handler interceptor (repeatable) |
| `WithConsumerConfig(method, cfg)` | JetStream consumer config for one method, by Go method name |
| `WithRetryPolicy(p)` | How handler errors are classified: retry or give up |
| `WithOnTerminate(fn)` | Called for every message given up on |

## Queue Groups

Each handler joins a queue group named after its fully qualified proto method,
so several instances of a service load-balance that method's messages while
different methods — and different services — never take each other's traffic.
This needs no configuration and is almost always what you want.

The name has to be per method rather than a shared constant because NATS groups
queue subscribers by queue *name* across every subject pattern matching the
delivered subject, not per subscription subject. That is also why
`WithQueueGroup` deserves care: it puts every method of the service in one
group, so if any two of its subjects can match the same message, that message
reaches only one of them. (Code generation rejects overlapping subjects, so
this only bites if you also override subjects at runtime.)

## Errors

Return `protonats.Errorf(code, format, args...)`. The runtime writes the
`Nats-Service-Error` and `Nats-Service-Error-Code` headers, and clients turn
them back into `*protonats.Error`. Plain Go errors become code 500.

```go
resp, err := client.GetOrder(ctx, req)
var pnErr *protonats.Error
if errors.As(err, &pnErr) {
    log.Printf("service error %d: %s", pnErr.Code, pnErr.Message)
}
```

Errors that have no caller to reach — a publish handler failing, a reply that
cannot be sent — go to the configured logger rather than disappearing.

## JetStream

`JETSTREAM_CONSUME` handlers take a `protonats.Acker`:

```go
func (h *orderHandler) ProcessOrderEvent(ctx context.Context, msg *orders.OrderEvent, ack protonats.Acker) error {
    if err := h.index(ctx, msg); err != nil {
        return err  // NAK'd, redelivered
    }
    return nil      // acked
}
```

| Return | Effect |
|---|---|
| `nil` | `Ack()` |
| an error the retry policy terminates | `Term()` — no redelivery |
| any other error | `Nak()` — redelivered |

`Acker` also exposes `Ack`, `Nak`, `NakWithDelay`, `Term`, and `InProgress` for
direct control. Acking yourself and then returning `nil` is safe — the
redundant auto-ack is ignored. A payload that fails to deserialize is
terminated, since it would never decode on redelivery either.

Consumers are resolved in this order: a `WithConsumerConfig` override (created
or updated with exactly that config); an existing durable with the name from
the proto, bound as-is so operator tuning is never clobbered; otherwise a new
consumer with explicit-ack defaults, durable if the proto names one.

```go
reg, err := orders.RegisterOrderServiceHandler(pn, handler,
    protonats.WithConsumerConfig("ProcessOrderEvent", jetstream.ConsumerConfig{
        MaxDeliver:    5,
        MaxAckPending: 100,
        AckWait:       30 * time.Second,
    }),
)
```

### Retries and giving up

A handler error is classified by the connection's retry policy:

| Policy | Terminates on | Retries |
|---|---|---|
| `DefaultRetryPolicy` (default) | `ErrTerminate` | everything else |
| `TerminateOnClientError` | `ErrTerminate`, any `*Error` with a 4xx code | everything else |

```go
protonats.WithRetryPolicy(protonats.TerminateOnClientError)
```

A 4xx describes a request that is wrong rather than a system that is
struggling — a malformed payload or an unknown id fails identically on every
redelivery, so retrying it only delays the outcome.

Two things happen on top of the policy:

- **The final attempt terminates.** When the consumer's `MaxDeliver` is
  reached, the runtime treats that delivery as terminal rather than naking it.
  Otherwise JetStream would drop the message when the attempts ran out —
  silently, and without running a rollback.
- **Naks honour `BackOff`.** JetStream applies `BackOff` to an ack that never
  arrives, not to one that explicitly asks to retry, so a plain nak would
  redeliver immediately and burn every attempt in milliseconds. The runtime
  naks with the delay the consumer is configured for.

Note the default `MaxDeliver` is unlimited, so a permanently failing handler
under `DefaultRetryPolicy` retries forever. Set `MaxDeliver`, choose a policy
that can terminate, or both.

`WithOnTerminate` observes everything that is given up on, whether or not it
has a rollback:

```go
protonats.WithOnTerminate(func(ctx context.Context, method, subject string, req proto.Message, cause error) {
    metrics.TaskAbandoned(method, cause)
})
```

## Tasks and Rollbacks

A `JETSTREAM_TASK` generates a worker and a rollback, registered separately —
usually by different services.

```go
// The service that does the work.
worker, err := shop.RegisterPaymentsWorker(pn, &payments{},
    protonats.WithRetryPolicy(protonats.TerminateOnClientError),
    protonats.WithConsumerConfig("ChargeCard", jetstream.ConsumerConfig{
        Durable:    "payments",
        MaxDeliver: 3,
        BackOff:    []time.Duration{time.Second, 5 * time.Second},
    }),
)

// The service that owns the undo.
rollback, err := shop.RegisterPaymentsRollback(pn, &orders{})
```

When the worker is given up on, the runtime republishes the message that failed
to the derived rollback subject with the cause attached, and only then
terminates the original. The rollback handler receives both:

```go
func (r *orders) ChargeCard(ctx context.Context, req *shop.Charge, cause *protonats.Error, ack protonats.Acker) error {
    if cause.Code == 402 {
        return r.notifyDeclined(ctx, req.OrderId)
    }
    return r.refund(ctx, req.OrderId)
}
```

Three properties worth designing around:

- **Rollbacks are at-least-once, so they must be idempotent.** The rollback is
  published *before* the original is terminated: the other order would lose the
  rollback entirely if the process died in between, whereas failing this way
  leaves the message to be redelivered and tried again. A `Nats-Msg-Id` derived
  from the failed message's stream sequence lets JetStream collapse the common
  duplicate, but the stream's duplicate window is not a guarantee.
- **A rollback has no rollback of its own.** One that keeps failing is retried
  like any other consumer and then terminated; wire `WithOnTerminate` to alert
  someone, because the regress has to stop at a person.
- **This is per-step, not a distributed transaction.** Each task undoes itself.
  Unwinding a chain of several services in reverse order needs a coordinator
  holding saga state, which protonats does not provide.

## Interceptors

Client interceptors wrap every `Request`, `Publish`, and `PublishJetStream`;
handler interceptors wrap every handler call, including publish and JetStream
consume. Both run outermost-first in registration order.

```go
func logging(ctx context.Context, method, subject string, req proto.Message, next protonats.ClientInvoker) (proto.Message, error) {
    start := time.Now()
    resp, err := next(ctx, subject, req)
    log.Printf("%s %s %v err=%v", method, subject, time.Since(start), err)
    return resp, err
}

pn, _ := protonats.New(nc, protonats.WithClientInterceptor(logging))
```

`method` is the fully qualified proto method name — use it as a metrics key,
since dynamic subjects have unbounded cardinality. An interceptor may
short-circuit by returning a response without calling `next`; that response is
copied into the caller's message and must be of the same type.

## Shutdown

```go
<-ctx.Done()

reg.Drain()   // stop accepting, let in-flight work finish
nc.Drain()    // waits for it, then closes
```

`reg.Drain()` returns as soon as the drain is requested. Draining the
connection afterwards is what actually waits for in-flight handlers.

## Not Implemented

`micro` service discovery and generated `kv` helpers are reserved in
`options.proto` but not built; the plugin fails if either option is set. For
scatter-gather, KV, or object store, use `pn.NatsConn()` and `pn.JetStream()`
directly.
