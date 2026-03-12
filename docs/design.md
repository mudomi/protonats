# ProtoNats Design Overview

## Philosophy

ProtoNats makes the common case trivial and the complex case possible. For a simple request/response or publish/subscribe, the developer writes a proto service definition and gets generated Go code where sending a message is a single function call and receiving one is a single handler function. No manual subject construction, no manual serialization, no boilerplate.

When you need more control -- custom queue groups, dynamic subjects, raw NATS access, JetStream tuning -- ProtoNats exposes escape hatches at every level without forcing you to abandon the generated code.

### Convention Over Configuration

Most successful protoc plugins (gRPC, Twirp, ConnectRPC) use zero custom proto options. They derive everything from standard `service`/`rpc` definitions. ProtoNats follows this principle: **if no options are set, the generated code still works.** Custom options exist only for things that standard proto schema cannot express (NATS subject routing, communication pattern selection).

### Keep Operational Config Out of Proto

Proto files define the **API contract** -- what messages look like, what operations exist, what subjects they route to. Proto files do **not** define operational parameters like timeouts, retry counts, consumer buffer sizes, or queue group names. Those change between environments and belong in runtime configuration.

The line: if changing a value requires regenerating code and redeploying, it probably shouldn't be in the proto file.

## What ProtoNats Is Not

ProtoNats is **not gRPC over NATS**. gRPC assumes point-to-point HTTP/2 connections with bidirectional streams. NATS is a fundamentally different messaging topology: pub/sub, fan-out, queue groups, persistence via JetStream, key-value stores, and more. ProtoNats embraces NATS-native patterns rather than shoehorning gRPC semantics.

ProtoNats also does not try to wrap every NATS feature. For things like object stores, complex KV operations, or direct JetStream stream management, you use the NATS Go client directly. ProtoNats exposes the underlying `*nats.Conn` and `jetstream.JetStream` handles so you can always drop down.

## Components

```
                          protoc
  .proto files ──────────────────────────> Generated Go code
  (messages + services    protoc-gen-go         (types)
   + protonats options)   protoc-gen-protonats   (clients, handlers, subjects)
                                │
                                ▼
                         Go runtime library
                         (protonats package)
                                │
                                ▼
                          nats.go client
```

1. **`protonats/options.proto`** - Proto file defining custom options (service, method, field, message level)
2. **`protoc-gen-protonats`** - Protoc plugin that reads proto files and generates Go code
3. **`protonats` Go package** - Runtime library providing connection management, middleware, and helpers

## Supported Communication Patterns

### Tier 1: Full code generation

These patterns get full code generation with type-safe clients and handlers.

| Pattern | How it's expressed | Description |
|---------|-------------------|-------------|
| **Request/Response** | `rpc Foo(Req) returns (Resp)` | Unary. Client sends request, gets one response. Maps to NATS Request/Reply. |
| **Publish (fire-and-forget)** | `rpc Foo(Msg) returns (google.protobuf.Empty)` + `type: PUBLISH` | Client publishes, no response. Maps to NATS Publish. |
| **JetStream Publish** | `rpc Foo(Msg) returns (google.protobuf.Empty)` + `type: JETSTREAM_PUBLISH` | Publish to stream with ack. Maps to JetStream Publish. |
| **JetStream Consume** | `rpc Foo(Msg) returns (google.protobuf.Empty)` + `type: JETSTREAM_CONSUME` | Consume from stream. Handler-only. Maps to JetStream Consumer. |

### Tier 2: Typed helpers

These get generated type-safe serialization wrappers around existing NATS APIs.

| Pattern | How it's expressed | Description |
|---------|-------------------|-------------|
| **KV Store** | `kv` message option | Type-safe get/put/delete/watch for a protobuf message in a NATS KV bucket. |

### Tier 3: Use NATS directly

These patterns are too varied or operational to warrant code generation. ProtoNats provides the serialized message types; you use `*nats.Conn` or `jetstream.JetStream` directly.

| Pattern | Why not generated |
|---------|------------------|
| **Request-Many (scatter-gather)** | Niche pattern with many tuning knobs (timeouts, stall timers, max responses). Better served by a runtime helper function than code generation. |
| **Object Store** | Streaming large binary blobs doesn't benefit from protobuf typing. |
| **Complex stream topologies** | Mirrors, sources, subject transforms -- infrastructure config, not service definitions. |
| **Batch publishing** | Highly application-specific batching logic. |

## Subject Naming

ProtoNats derives NATS subjects from the proto package and method name by default:

```
{package}.{MethodName}
```

For example, given:
```protobuf
package myapp.orders;
service OrderService {
  rpc GetOrder(GetOrderRequest) returns (Order);
}
```

The default subject is: `myapp.orders.GetOrder`

The service name is intentionally **not** included by default. The proto package already provides namespacing, and including the service name produces unnecessarily long subjects. If a package contains multiple services and there's a name collision, override with `subject_prefix`.

This default can be overridden at every level (service prefix, method subject) via custom options. Dynamic subject components (like entity IDs) are supported via template syntax.

## Error Handling

ProtoNats follows the NATS Service API convention (`Nats-Service-Error` / `Nats-Service-Error-Code` headers). This means:

- Any NATS tooling that understands the Service API can interpret ProtoNats errors.
- The generated client checks these headers and returns structured Go errors.
- Handlers return errors via `protonats.Errorf(code, format, args...)`.
- For JetStream: standard ack semantics (Ack/Nak/Term) apply.

## Middleware / Interceptors

Both clients and handlers support middleware chains (similar to gRPC interceptors):

- **Client interceptors**: Run before sending / after receiving. Use for logging, tracing, retry.
- **Handler interceptors**: Run before / after the handler function. Use for auth, logging, metrics, panic recovery.

Interceptors have access to the NATS message metadata (subject, headers, reply subject) in addition to the typed proto message.
