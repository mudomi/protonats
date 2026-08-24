# Design Notes

Rationale and scope. For usage see the [README](../README.md) and the
language guides.

## Philosophy

Make the common case trivial and the complex case possible. A request/response
or publish should be one function call to send and one function to implement,
with no manual subject building or serialization — and when you need queue
groups, dynamic subjects, JetStream tuning, or raw NATS, there is an escape
hatch at every level that does not require abandoning the generated code.

Following gRPC, Twirp, and ConnectRPC, options are a last resort: if none are
set, the generated code still works. They exist only for what a standard proto
schema cannot express — subject routing and the communication pattern.

## Not gRPC over NATS

gRPC assumes point-to-point HTTP/2 connections with bidirectional streams. NATS
is a different topology: pub/sub, fan-out, queue groups, persistence through
JetStream, key-value. ProtoNats targets NATS-native patterns rather than
reshaping them into gRPC semantics, which is why there is no streaming and why
publish and JetStream are first-class method types.

## Components

```
                        protoc
.proto  ────────────────────────────────────→  Go: types + clients/handlers
(messages, services,    protoc-gen-go              ↓
 protonats options)     protoc-gen-protonats    protonats runtime → nats.go
                        protoc-gen-es              ↓
                        protoc-gen-protonats-ts TS: types + clients/handlers
                                                   ↓
                                                @protonats/runtime → nats.js
```

Both sides speak binary protobuf on identical subjects with identical error
headers, so a Go handler can serve a TypeScript client and vice versa.

## Scope

Generated in full: request/reply, publish, JetStream publish, JetStream
consume (Go only for the JetStream pair).

Deliberately not generated, because they are too varied or too operational to
benefit from codegen — use `pn.NatsConn()` or `pn.JetStream()` instead:

- **Request-many (scatter-gather)** — many tuning knobs, better as a call-site helper.
- **Object store** — streaming large blobs gains nothing from protobuf typing.
- **Stream topologies** — mirrors, sources, subject transforms are infrastructure.
- **Batch publishing** — application-specific.

Reserved in `options.proto` but not implemented; the plugin fails if set:
**`micro`** service discovery and **`kv`** typed store helpers.

## Two Rules Worth Knowing

**Queue group names must be unique per method.** NATS groups queue subscribers
by queue name across every subject pattern that matches the delivered subject,
not per subscription subject. A shared name would let a handler on `orders.*`
swallow messages meant for one on `orders.Create`. Naming the group after the
proto method gives load-balancing within a method and isolation between them.

**Subjects must not overlap.** For the same matching reason, two handlers whose
subjects can both match one message will both receive it, and a caller would
take whichever reply came first. Code generation rejects this rather than
letting it become a heisenbug in production.
