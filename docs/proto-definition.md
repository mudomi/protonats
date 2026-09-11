# Proto Definition Guide

How to write `.proto` files for ProtoNats. The authoritative option definitions
live in [`proto/protonats/options.proto`](../proto/protonats/options.proto);
this guide explains what they mean.

## Setup

Copy `proto/protonats/options.proto` into your include path and import it:

```protobuf
import "protonats/options.proto";
```

Everything works without any option set — request/reply on a subject derived
from the package and method name. Options exist only for what proto cannot
otherwise express: subject routing and the communication pattern.

## What Goes In Proto

Proto files define the API contract: what messages look like, what operations
exist, which subjects they route to, and which stream carries them. Operational
values — timeouts, queue group names, consumer tuning, retry counts — belong in
runtime configuration, because changing them should not require regenerating
code and redeploying every caller.

| In proto (contract) | In code (operational) |
|---|---|
| Subject pattern | Timeout, retries |
| Communication pattern | Queue group override |
| Stream and durable consumer name | Consumer tuning (ack policy, max deliver) |
| Dynamic subject field markers | Connection, TLS, auth |

## Subjects

The default subject is `{proto package}.{MethodName}`:

```protobuf
package myapp.orders;
service OrderService {
  rpc GetOrder(GetOrderRequest) returns (Order);   // → myapp.orders.GetOrder
}
```

The service name is deliberately excluded — the package already namespaces, and
including it produces needlessly long subjects. If two services in one package
have colliding method names, disambiguate with `subject_prefix`:

```protobuf
service InvoiceService {
  option (protonats.service) = { subject_prefix: "billing.invoices" };
  rpc Get(GetInvoiceRequest) returns (Invoice);    // → billing.invoices.Get
}
```

### Dynamic subjects

A subject may interpolate request fields. Each referenced field must be marked
`subject_token`:

```protobuf
message GetOrderRequest {
  string order_id = 1 [(protonats.field).subject_token = true];
}

rpc GetOrder(GetOrderRequest) returns (Order) {
  option (protonats.method).subject = "orders.{order_id}";
}
```

A client call publishes to `orders.abc123`; the handler subscribes to the
wildcard `orders.*` and reads the concrete subject with
`protonats.SubjectFromContext(ctx)`. Multiple segments work the same way:
`"events.{region}.{event_type}"`.

Token fields must be a scalar string, integer, or bool. Their *values* become
subject tokens verbatim, so validate them before the call — nothing escapes or
rejects them, and each hazardous value fails in its own way:

| Value | Resulting subject | What happens |
|---|---|---|
| `a.b` | `orders.a.b` | extra token; no longer matches `orders.*`, so the call fails with *no responders* |
| `""` | `orders.` | trailing empty token; same *no responders* failure |
| `a b` | `orders.a b` | the server rejects the publish, and the caller only sees the request time out |
| `*` or `>` | `orders.*` | delivered as a **literal** token — the handler runs and sees that string as the id |

The last row is the one to watch: it does not fail, so an unvalidated id
reaches the handler as a subject a person would read as a wildcard.

### Subjects must not overlap

Code generation fails if two methods subscribe to subjects that can both match
one message — for example `orders.{order_id}` (subscribing `orders.*`) next to
`orders.Create`. NATS delivers such a message to both subscriptions, so both
handlers would answer and the caller would take whichever reply arrived first.
Give them distinct subjects, e.g. `orders.get.{order_id}` and `orders.create`.

Some further guidance: keep subjects short, and keep event streams out of the
request/reply namespace so a stream capturing `orders.>` does not also swallow
request traffic. Prefer `events.orders.>` for the stream.

## Method Options

Set with `option (protonats.method) = { ... };` inside an `rpc`.

### `type`

| Value | Meaning |
|---|---|
| `REQUEST_REPLY` (default) | Client sends a request and waits for one response. |
| `PUBLISH` | Fire-and-forget. Use `google.protobuf.Empty` as the response type. |
| `JETSTREAM_PUBLISH` | Publish to a stream and wait for the ack. Requires `stream`. |
| `JETSTREAM_CONSUME` | Consume from a stream. Handler-only. Requires `stream`. |
| `JETSTREAM_TASK` | Work with a paired rollback. Requires `stream` and `consumer`. |

```protobuf
rpc OrderCreated(OrderCreatedEvent) returns (google.protobuf.Empty) {
  option (protonats.method).type = PUBLISH;
}

rpc ProcessOrderEvent(OrderEvent) returns (google.protobuf.Empty) {
  option (protonats.method) = {
    type: JETSTREAM_CONSUME
    stream: "ORDER_EVENTS"
    consumer: "order-processor"
  };
}
```

A `PUBLISH` handler's error return is for logging and metrics only — there is no
reply subject to send it to.

### Tasks

`JETSTREAM_TASK` declares a unit of work together with its undo. One rpc
generates three roles:

```protobuf
rpc ChargeCard(Charge) returns (google.protobuf.Empty) {
  option (protonats.method) = {
    type: JETSTREAM_TASK
    stream: "SHOP"
    consumer: "payments"
  };
}
```

| Role | Generated | Implemented by |
|---|---|---|
| Trigger | `PaymentsClient.ChargeCard` | whoever starts the work |
| Worker | `PaymentsWorker` + `RegisterPaymentsWorker` | the service that does it |
| Rollback | `PaymentsRollback` + `RegisterPaymentsRollback` | the service that undoes it |

The rollback's subject and durable consumer are **derived**, never configured:
`shop.ChargeCard` gets `shop.ChargeCard.rollback` and `payments-rollback`. That
is the point of declaring both halves in one rpc — they share a message type
and cannot disagree about where to find each other.

The stream must capture the rollback subject as well as the task subject, so
give it a subtree (`shop.>`) rather than a list of literals. protonats creates
consumers but never streams.

`consumer` is required: an ephemeral consumer would hand every running instance
its own copy of the rollback, and each would undo the same work.

See the [Go Library Guide](go-library.md) for when a rollback fires, and
[`examples/rollback`](../examples/rollback) for a runnable walkthrough.

### `subject`

Overrides the subject, with `{field}` interpolation as described above.

### `stream` and `consumer`

`stream` is required for both JetStream types and rejected on the others.
`consumer` names the durable consumer for `JETSTREAM_CONSUME`; if omitted, an
ephemeral consumer is created, which is rarely what you want in production.
`JETSTREAM_TASK` requires both.
All other consumer tuning happens at registration time — see the
[Go Library Guide](go-library.md).

## Service Options

Set with `option (protonats.service) = { ... };`.

- **`subject_prefix`** — replaces the proto package as the subject prefix.
- **`micro`, `version`, `description`** — reserved for NATS micro service
  discovery. Not implemented; the plugin fails if `micro` is set.

## Field Options

- **`subject_token`** — marks a field usable as a `{token}` in a subject
  template. The proto field name is the template variable; there is no separate
  naming layer, so mismatches are impossible.

## Message Options

- **`kv`** — reserved for typed KV store helpers. Not implemented; the plugin
  fails if it is set. Use the jetstream KV API through `pn.JetStream()`.

## Example

```protobuf
syntax = "proto3";
package myapp.orders;

option go_package = "github.com/myorg/myapp/gen/orders;orders";

import "google/protobuf/empty.proto";
import "protonats/options.proto";

service OrderService {
  option (protonats.service) = { subject_prefix: "orders" };

  // → orders.get.{order_id}, handler subscribes orders.get.*
  rpc GetOrder(GetOrderRequest) returns (Order) {
    option (protonats.method).subject = "orders.get.{order_id}";
  }

  // → orders.Create
  rpc Create(CreateOrderRequest) returns (Order);

  // Persisted event, published with an ack.
  rpc OrderCreated(OrderCreatedEvent) returns (google.protobuf.Empty) {
    option (protonats.method) = {
      type: JETSTREAM_PUBLISH
      subject: "events.orders.created"
      stream: "ORDER_EVENTS"
    };
  }

  // Consumed from the same stream by this service.
  rpc ProcessOrderEvent(OrderCreatedEvent) returns (google.protobuf.Empty) {
    option (protonats.method) = {
      type: JETSTREAM_CONSUME
      subject: "events.orders.created"
      stream: "ORDER_EVENTS"
      consumer: "order-processor"
    };
  }
}

message GetOrderRequest {
  string order_id = 1 [(protonats.field).subject_token = true];
}

message Order {
  string order_id = 1;
  string status = 2;
}

message CreateOrderRequest { string customer_id = 1; }
message OrderCreatedEvent { Order order = 1; int64 timestamp = 2; }
```

## What the Plugin Rejects

Generation fails, rather than emitting misleading code, when:

- a subject references a field that does not exist or is not marked `subject_token`
- a token field is not a scalar string, integer, or bool
- two methods subscribe to overlapping subjects
- `stream` is missing on a JetStream method, or set on a non-JetStream one
- `micro` or `kv` is set (both unimplemented)
- an RPC is streaming (`stream` keyword on the request or response)
