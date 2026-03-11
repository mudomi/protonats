# ProtoNats Proto Definition Guide

This document describes how to write `.proto` files for ProtoNats. The custom options control subject naming and communication patterns. Operational configuration (timeouts, queue groups, consumer tuning) lives in Go code, not proto files.

## Setup

Every `.proto` file that uses ProtoNats features must import the options file:

```protobuf
syntax = "proto3";
import "protonats/options.proto";
```

## Design Principles

**Proto files define the API contract.** They answer: what messages exist, what operations are available, what NATS subjects they route to.

**Proto files do NOT define operational config.** Timeouts, queue groups, consumer buffer sizes, ack policies, retry counts -- these change between environments and callers. They belong in Go code.

**Convention over configuration.** If you don't set any options, the generated code still works. Subjects are derived from the proto package + method name. The default communication pattern is request/response. Options exist only for things conventions can't express.

## The Options Proto File

All options live under the `protonats` package using extension number `50100` (within the 50000-99999 range reserved for organizational use). One extension number per descriptor type, using the sub-message pattern (recommended by protobuf docs for projects needing many options).

```protobuf
syntax = "proto3";
package protonats;

option go_package = "github.com/mudomi/protonats;protonats";

import "google/protobuf/descriptor.proto";

// ─── Service-level options ───────────────────────────────────────────

message ServiceOptions {
  // Subject prefix for all methods in this service.
  // Default: "{proto_package}" (dots preserved as NATS subject tokens)
  // Example: "api.v1.orders"
  //
  // The service name is NOT included in the default prefix. The proto
  // package already provides namespacing. If a package has multiple
  // services with colliding method names, set this to disambiguate.
  string subject_prefix = 1;

  // Whether to generate NATS micro-compatible service discovery
  // (PING/INFO/STATS on $SRV subjects).
  // Default: false
  //
  // When true, the generated handler registers with the NATS micro
  // framework. Other NATS tooling (nats micro ls, nats micro info)
  // can discover and monitor the service.
  bool micro = 2;

  // Service version for micro discovery. Required if micro is true.
  // Must be valid semver (e.g., "1.0.0").
  string version = 3;

  // Human-readable description for micro discovery.
  string description = 4;
}

extend google.protobuf.ServiceOptions {
  optional ServiceOptions service = 50100;
}

// ─── Method-level options ────────────────────────────────────────────

message MethodOptions {
  // The communication pattern for this method.
  // Default: REQUEST_REPLY
  MethodType type = 1;

  // The NATS subject for this method.
  // Default: "{service_prefix}.{MethodName}"
  //
  // Dynamic segments from request message fields:
  //   "users.{user_id}.orders"  →  "users.abc123.orders"
  //
  // Fields referenced in {braces} must have the subject_token field
  // option set. On the handler side, the subscription automatically
  // uses wildcards: "users.*.orders"
  //
  // Multiple dynamic segments are supported:
  //   "events.{region}.{event_type}"
  string subject = 2;

  // Stream name for JetStream methods.
  // Required when type is JETSTREAM_PUBLISH or JETSTREAM_CONSUME.
  // This identifies which stream to publish to or consume from.
  string stream = 3;

  // Durable consumer name for JETSTREAM_CONSUME.
  // If set, creates/binds a durable consumer with this name.
  // If empty, an ephemeral consumer is used.
  string consumer = 4;
}

extend google.protobuf.MethodOptions {
  optional MethodOptions method = 50100;
}

// ─── Method types ────────────────────────────────────────────────────

enum MethodType {
  // Unary request/response (default).
  // Client sends a request, waits for a single response.
  // Maps to: nc.Request() / msg.Respond()
  REQUEST_REPLY = 0;

  // Fire-and-forget publish.
  // Client publishes a message, no response expected.
  // The response type in the rpc definition is ignored
  // (use google.protobuf.Empty).
  // Maps to: nc.Publish()
  PUBLISH = 1;

  // JetStream publish with acknowledgment.
  // Client publishes to a JetStream-enabled subject and receives
  // a PubAck confirming persistence. Requires `stream` to be set.
  // Maps to: js.Publish()
  JETSTREAM_PUBLISH = 2;

  // JetStream consume.
  // Handler consumes messages from a JetStream consumer.
  // Handler-only: no client is generated (the "client" is whatever
  // published to the stream). Requires `stream` to be set.
  // Maps to: consumer.Consume()
  JETSTREAM_CONSUME = 3;
}

// ─── Field-level options ─────────────────────────────────────────────

message FieldOptions {
  // Marks this field as a dynamic NATS subject token.
  //
  // When the message is used as a request in a method with a dynamic
  // subject template, this field's value replaces the {name} placeholder.
  //
  // Example:
  //   Field: string user_id = 1 [(protonats.field).subject_token = true];
  //   Subject template: "users.{user_id}.orders"
  //   → On publish: "users.abc123.orders"
  //   → On subscribe: "users.*.orders"
  //
  // Constraints:
  //   - Field must be string, int32, int64, uint32, or uint64
  //   - Value must not contain '.', '*', '>', or spaces (NATS rules)
  //   - Empty values are a runtime error
  //
  // The template variable name is always the proto field name.
  // No separate naming is needed.
  bool subject_token = 1;
}

extend google.protobuf.FieldOptions {
  optional FieldOptions field = 50100;
}

// ─── Message-level options ───────────────────────────────────────────

message MessageOptions {
  // KV store configuration for this message type.
  // Generates type-safe get/put/delete/watch helpers.
  KVOptions kv = 1;
}

message KVOptions {
  // The KV bucket name.
  // Required. Must follow NATS KV naming rules (alphanumeric, dash, underscore).
  string bucket = 1;

  // The key template. Uses the same {field_name} interpolation as subjects.
  // Fields referenced must have subject_token = true.
  //
  // Examples:
  //   "{user_id}"               → simple entity key
  //   "users.{user_id}"         → namespaced key
  //   "{tenant}.{user_id}"      → multi-tenant key
  //
  // If empty, the key must be provided at call time as a string argument.
  string key_template = 2;
}

extend google.protobuf.MessageOptions {
  optional MessageOptions message = 50100;
}
```

---

## What's In Proto vs. What's In Go Code

This is the most important design decision in ProtoNats. The line is drawn based on a simple rule: **if it's part of the API contract (callers need to know it), it goes in proto. If it's operational (varies by environment or caller), it goes in Go.**

| In Proto (API contract) | In Go code (operational) |
|------------------------|--------------------------|
| Subject pattern | Timeout duration |
| Communication pattern (req/resp, publish, stream) | Queue group name |
| Stream name (which stream a method publishes to / consumes from) | Consumer config (ack policy, max deliver, max ack pending) |
| Consumer name (durable identity) | Deliver policy (all, new, last) |
| Dynamic subject field markers | Ack wait, backoff |
| KV bucket name and key pattern | KV history depth, TTL |
| Micro service flag | Connection options, TLS, auth |

### Why stream name is in proto

The stream name is part of the contract. A publisher and consumer must agree on which stream the data flows through. If you change the stream name, both sides must be updated. This is a schema-level concern.

### Why consumer config is NOT in proto

Consumer configuration (ack policy, max ack pending, deliver policy) is deployment config. The same consumer might run with `max_ack_pending: 10` in staging and `max_ack_pending: 1000` in production. Baking it into proto means regenerating code to tune performance.

### Why queue group is NOT in proto

Queue groups are a handler-side deployment concern. The same handler might run in queue group `"orders-v1"` in one deployment and `"orders-v2"` during a migration. The client doesn't know or care about queue groups.

### Why timeout is NOT in proto

Timeouts depend on the caller's context, network conditions, and SLA requirements. A CLI tool might use 30s timeout; a web request handler might use 2s. Hardcoding timeouts in proto forces all callers to use the same value.

---

## Service-Level Options

Applied with `option (protonats.service) = { ... };` inside a `service` block.

### `subject_prefix`

Overrides the default subject prefix. By default, the prefix is the proto package name with dots preserved as NATS subject tokens.

```protobuf
package myapp.orders;

// Default prefix: "myapp.orders"
// Methods would be: "myapp.orders.GetOrder", "myapp.orders.CreateOrder"

service OrderService {
  option (protonats.service) = {
    subject_prefix: "orders"  // shorter: "orders.GetOrder", "orders.CreateOrder"
  };
}
```

The service name is intentionally excluded from the default. The proto package provides namespacing. Including it produces unnecessarily long subjects like `myapp.orders.OrderService.GetOrder`.

If a proto package has multiple services with colliding method names, use `subject_prefix` to disambiguate:

```protobuf
package myapp.billing;

service InvoiceService {
  option (protonats.service) = { subject_prefix: "billing.invoices" };
  rpc Get(GetInvoiceRequest) returns (Invoice);  // → billing.invoices.Get
}

service PaymentService {
  option (protonats.service) = { subject_prefix: "billing.payments" };
  rpc Get(GetPaymentRequest) returns (Payment);  // → billing.payments.Get
}
```

### `micro`

When `true`, the generated handler code registers with the NATS micro framework, enabling automatic service discovery via `$SRV.PING`, `$SRV.INFO`, and `$SRV.STATS`.

```protobuf
service OrderService {
  option (protonats.service) = {
    micro: true
    version: "1.0.0"
    description: "Order lifecycle management"
  };
}
```

Requires `version` (valid semver). This is the only place where version appears in proto -- it's the service discovery version, not a schema version.

---

## Method-Level Options

Applied with `option (protonats.method) = { ... };` inside an `rpc` block.

### `type` (MethodType)

Determines the communication pattern. Default is `REQUEST_REPLY`.

#### `REQUEST_REPLY` (default)

Standard request/response. No option needed if this is what you want.

```protobuf
// No protonats options needed -- REQUEST_REPLY is the default
rpc GetOrder(GetOrderRequest) returns (Order);
```

Generated client returns `(*Order, error)`.
Generated handler signature: `func(ctx, *GetOrderRequest) (*Order, error)`.

#### `PUBLISH`

Fire-and-forget. Use `google.protobuf.Empty` as the response type.

```protobuf
import "google/protobuf/empty.proto";

rpc OrderCreated(OrderCreatedEvent) returns (google.protobuf.Empty) {
  option (protonats.method).type = PUBLISH;
}
```

Generated client returns `error` (serialization/publish failure only).
Generated handler signature: `func(ctx, *OrderCreatedEvent) error`.

The handler's error return is for internal bookkeeping (logging, metrics). There's no reply subject, so errors can't be sent back.

#### `JETSTREAM_PUBLISH`

Publish to a JetStream stream with persistence acknowledgment.

```protobuf
rpc EmitOrderEvent(OrderEvent) returns (google.protobuf.Empty) {
  option (protonats.method) = {
    type: JETSTREAM_PUBLISH
    stream: "ORDER_EVENTS"
  };
}
```

Generated client returns `(*jetstream.PubAck, error)`.
No handler is generated -- the message goes into the stream. Consumption is defined separately.

#### `JETSTREAM_CONSUME`

Consume from a JetStream stream. Handler-only (no client generated).

```protobuf
rpc ProcessOrderEvent(OrderEvent) returns (google.protobuf.Empty) {
  option (protonats.method) = {
    type: JETSTREAM_CONSUME
    stream: "ORDER_EVENTS"
    consumer: "order-processor"
  };
}
```

No client generated.
Generated handler signature: `func(ctx, *OrderEvent, protonats.Acker) error`.

The `consumer` field names the durable consumer. If omitted, an ephemeral consumer is created at runtime (useful for development, rarely what you want in production).

All consumer tuning (ack policy, max deliver, max ack pending, deliver policy, ack wait, filter subjects) is configured in Go code at handler registration time. See the Go Library Guide.

### `subject`

Override the NATS subject. Supports `{field_name}` interpolation for dynamic segments.

#### Static override

```protobuf
rpc GetOrder(GetOrderRequest) returns (Order) {
  option (protonats.method).subject = "orders.get";
}
```

#### Dynamic subjects

```protobuf
message GetOrderRequest {
  string order_id = 1 [(protonats.field).subject_token = true];
}

rpc GetOrder(GetOrderRequest) returns (Order) {
  option (protonats.method).subject = "orders.{order_id}";
}
```

When the client calls `GetOrder(&GetOrderRequest{OrderId: "abc123"})`, the message is published to `orders.abc123`.

On the handler side, the subscription uses a wildcard: `orders.*`. The handler receives the full deserialized request (including `order_id` from the payload). The actual subject is available via `protonats.SubjectFromContext(ctx)`.

#### Multiple dynamic segments

```protobuf
message RegionalEvent {
  string region = 1 [(protonats.field).subject_token = true];
  string event_type = 2 [(protonats.field).subject_token = true];
}

rpc EmitEvent(RegionalEvent) returns (google.protobuf.Empty) {
  option (protonats.method) = {
    type: PUBLISH
    subject: "events.{region}.{event_type}"
  };
}
```

### `stream` and `consumer`

Required for JetStream methods. Stream identifies which JetStream stream; consumer names the durable consumer.

```protobuf
rpc ProcessEvent(Event) returns (google.protobuf.Empty) {
  option (protonats.method) = {
    type: JETSTREAM_CONSUME
    stream: "EVENTS"
    consumer: "event-processor"
  };
}
```

---

## Field-Level Options

Applied with `[(protonats.field).subject_token = true]` on a message field.

### `subject_token`

Marks a field as a dynamic component of a NATS subject. The **proto field name** is used as the template variable (no separate naming layer).

```protobuf
message OrderQuery {
  string customer_id = 1 [(protonats.field).subject_token = true];
  string order_id = 2 [(protonats.field).subject_token = true];
  int32 limit = 3;  // not a subject token
}
```

Subject template: `"orders.{customer_id}.{order_id}"` uses the field names directly.

Constraints:
- Field must be `string`, `int32`, `int64`, `uint32`, or `uint64`
- Value must not contain `.`, `*`, `>`, or spaces (NATS subject naming rules)
- Empty/zero values are a runtime error (the generated client validates before publishing)

**Why `bool` instead of `string`:** In the first design, `subject_token` was a string that named the template variable separately from the field name. This created an indirection layer with no benefit -- the field name is already the natural template variable name. A boolean is simpler and eliminates the possibility of naming mismatches.

---

## Message-Level Options: KV Store

Applied with `option (protonats.message) = { ... };` inside a `message` block.

The `kv` option generates type-safe helpers for storing and retrieving this message type in a NATS KV bucket.

```protobuf
message UserProfile {
  option (protonats.message) = {
    kv: {
      bucket: "user-profiles"
      key_template: "{user_id}"
    }
  };

  string user_id = 1 [(protonats.field).subject_token = true];
  string display_name = 2;
  string email = 3;
}
```

This generates:
- `UserProfileKV.Get(ctx, userId) (*UserProfile, uint64, error)` -- returns value and revision
- `UserProfileKV.Put(ctx, profile) (uint64, error)` -- key derived from `key_template`
- `UserProfileKV.Create(ctx, profile) (uint64, error)` -- put only if key doesn't exist
- `UserProfileKV.Update(ctx, profile, revision) (uint64, error)` -- optimistic concurrency
- `UserProfileKV.Delete(ctx, userId) error`
- `UserProfileKV.Watch(ctx, userId) (protonats.Watcher[UserProfile], error)`
- `UserProfileKV.WatchAll(ctx) (protonats.Watcher[UserProfile], error)`

**Bucket configuration (history, TTL, replicas, storage type) is not in proto.** These are operational parameters configured in Go code or via infrastructure tooling. The generated code does not auto-create buckets -- you provision them separately (nats CLI, Terraform, or Go setup code). The generated code binds to an existing bucket by name.

---

## Complete Example

```protobuf
syntax = "proto3";
package myapp.orders;

option go_package = "github.com/myorg/myapp/gen/orders;orders";

import "google/protobuf/empty.proto";
import "protonats/options.proto";

// ─── Service ─────────────────────────────────────────────────────────

service OrderService {
  option (protonats.service) = {
    subject_prefix: "orders"
    micro: true
    version: "1.0.0"
    description: "Order lifecycle management"
  };

  // Request/Response: get an order by ID
  // Subject: orders.{order_id} (dynamic)
  rpc GetOrder(GetOrderRequest) returns (Order) {
    option (protonats.method).subject = "orders.{order_id}";
  }

  // Request/Response: create an order
  // Subject: orders.Create (default: prefix + method name)
  rpc Create(CreateOrderRequest) returns (Order);

  // Publish event to JetStream
  // Subject: events.orders.created
  rpc OrderCreated(OrderCreatedEvent) returns (google.protobuf.Empty) {
    option (protonats.method) = {
      type: JETSTREAM_PUBLISH
      subject: "events.orders.created"
      stream: "ORDER_EVENTS"
    };
  }

  // Consume events from JetStream
  rpc ProcessOrderEvent(OrderCreatedEvent) returns (google.protobuf.Empty) {
    option (protonats.method) = {
      type: JETSTREAM_CONSUME
      stream: "ORDER_EVENTS"
      consumer: "order-processor"
    };
  }
}

// ─── Messages ────────────────────────────────────────────────────────

message GetOrderRequest {
  string order_id = 1 [(protonats.field).subject_token = true];
}

message CreateOrderRequest {
  string customer_id = 1;
  repeated OrderItem items = 2;
}

message Order {
  string order_id = 1 [(protonats.field).subject_token = true];
  string customer_id = 2;
  repeated OrderItem items = 3;
  OrderStatus status = 4;
  int64 created_at = 5;
}

message OrderItem {
  string product_id = 1;
  int32 quantity = 2;
  int64 price_cents = 3;
}

enum OrderStatus {
  ORDER_STATUS_UNSPECIFIED = 0;
  ORDER_STATUS_PENDING = 1;
  ORDER_STATUS_CONFIRMED = 2;
  ORDER_STATUS_SHIPPED = 3;
  ORDER_STATUS_DELIVERED = 4;
  ORDER_STATUS_CANCELLED = 5;
}

message OrderCreatedEvent {
  Order order = 1;
  int64 timestamp = 2;
}

// ─── KV-backed message ──────────────────────────────────────────────

message OrderState {
  option (protonats.message) = {
    kv: {
      bucket: "order-state"
      key_template: "{order_id}"
    }
  };

  string order_id = 1 [(protonats.field).subject_token = true];
  OrderStatus status = 2;
  string last_updated_by = 3;
  int64 updated_at = 4;
}
```

---

## Subject Design Guidelines

### Keep subjects short

NATS subjects are dot-delimited tokens. Each token adds overhead in the trie-based subscription matcher.

Good: `orders.abc123`
Bad: `myapp.production.api.v2.orders.service.get-order.abc123`

### Separate event streams from request/reply

If a JetStream stream captures `orders.>`, every subject starting with `orders.` goes into the stream -- including request/reply traffic. Use a dedicated namespace:

```
orders.Create        ← request/reply (core NATS)
orders.{id}          ← request/reply (core NATS)
events.orders.>      ← JetStream stream captures these
```

### Design for wildcard subscriptions

Put dynamic segments in predictable positions:
- `orders.*` -- all direct order lookups
- `events.orders.>` -- all order events
- `events.*.created` -- all "created" events across domains

### Don't encode operational concerns in subjects

No queue groups, instance IDs, environment names, or deployment versions in subjects. These are routing/deployment concerns, not API concerns.

---

## Proto File Organization

```
proto/
  protonats/
    options.proto          # ProtoNats options (provided by the library)
  myapp/
    orders/
      orders.proto         # Order service + messages
    users/
      users.proto          # User service + messages
    common/
      types.proto          # Shared message types (no services)
```

Generated Go code:

```
gen/
  myapp/
    orders/
      orders.pb.go           # Standard protobuf types
      orders_protonats.pb.go # ProtoNats clients, handlers, subjects
    users/
      users.pb.go
      users_protonats.pb.go
```
