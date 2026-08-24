# ProtoNats

[![CI](https://github.com/mudomi/protonats/actions/workflows/ci.yml/badge.svg)](https://github.com/mudomi/protonats/actions/workflows/ci.yml)
[![Docker](https://github.com/mudomi/protonats/actions/workflows/docker.yml/badge.svg)](https://github.com/mudomi/protonats/actions/workflows/docker.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](./LICENSE)

A protoc plugin and runtime that generate type-safe [NATS](https://nats.io) clients and handlers from Protocol Buffer service definitions, for Go and TypeScript.

```protobuf
service OrderService {
  rpc GetOrder(GetOrderRequest) returns (Order);
}
```

```go
// Client — one call, no manual subjects or serialization
client := orders.NewOrderServiceClient(pn)
order, err := client.GetOrder(ctx, &orders.GetOrderRequest{OrderId: "abc"})

// Handler — implement the interface, register it
func (h *handler) GetOrder(ctx context.Context, req *orders.GetOrderRequest) (*orders.Order, error) {
    return fetchOrder(ctx, req.OrderId)
}
reg, _ := orders.RegisterOrderServiceHandler(pn, &handler{})
defer reg.Drain()
```

## Install

```bash
go install github.com/mudomi/protonats/cmd/protoc-gen-protonats@latest
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go get github.com/mudomi/protonats
```

You also need [protoc](https://github.com/protocolbuffers/protobuf/releases). Alternatively the Docker image bundles everything — see [Docker Usage](./docs/docker.md).

## Quick Start

**1. Define the service.** Copy `proto/protonats/options.proto` from this repo into your include path.

```protobuf
syntax = "proto3";
package myapp.orders;

option go_package = "github.com/myorg/myapp/gen/orders;orders";

import "protonats/options.proto";

service OrderService {
  // Request/reply is the default — no options needed.
  rpc GetOrder(GetOrderRequest) returns (Order);
}

message GetOrderRequest { string order_id = 1; }
message Order { string order_id = 1; string name = 2; }
```

**2. Generate.**

```bash
protoc \
  --go_out=gen --go_opt=paths=source_relative \
  --protonats_out=gen --protonats_opt=paths=source_relative \
  -I proto \
  proto/myapp/orders/orders.proto
```

This writes `orders.pb.go` (message types) and `orders_protonats.pb.go` (client, handler interface, registration).

**3. Connect, then use the generated code as shown at the top.**

```go
nc, _ := nats.Connect("nats://localhost:4222")
defer nc.Drain()

pn, _ := protonats.New(nc)
```

## Supported Patterns

| Pattern | Proto | Client returns |
|---|---|---|
| **Request/Reply** (default) | `rpc Foo(Req) returns (Resp)` | `(*Resp, error)` |
| **Publish** | `type: PUBLISH` | `error` |
| **JetStream Publish** | `type: JETSTREAM_PUBLISH` + `stream` | `(*jetstream.PubAck, error)` |
| **JetStream Consume** | `type: JETSTREAM_CONSUME` + `stream` | handler-only |

Subjects default to `{proto package}.{MethodName}` and can be overridden, including dynamic segments like `orders.{order_id}`. See the [Proto Definition Guide](./docs/proto-definition.md).

## Scaling Out

Handlers join a queue group named after their proto method, so running several
instances of a service load-balances its messages automatically. Nothing to
configure, and two different methods can never take each other's traffic.

## Errors

Handlers return structured errors; clients receive them as `*protonats.Error`, using the same NATS Service API headers in both languages.

```go
return nil, protonats.Errorf(404, "order %s not found", req.OrderId)
```

## Documentation

- [Proto Definition Guide](./docs/proto-definition.md) — options, subjects, patterns
- [Go Library Guide](./docs/go-library.md) — generated code and runtime
- [TypeScript Library Guide](./docs/ts-library.md) — the TS runtime
- [Docker Usage](./docs/docker.md) — toolchain image
- [Design Notes](./docs/design.md) — rationale and scope

## License

[MIT](./LICENSE)
