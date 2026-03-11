# ProtoNats

[![CI](https://github.com/mudomi/protonats/actions/workflows/ci.yml/badge.svg)](https://github.com/mudomi/protonats/actions/workflows/ci.yml)
[![Docker](https://github.com/mudomi/protonats/actions/workflows/docker.yml/badge.svg)](https://github.com/mudomi/protonats/actions/workflows/docker.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](./LICENSE)

A protoc plugin and Go runtime that generates type-safe [NATS](https://nats.io) clients and handlers from Protocol Buffer service definitions.

```protobuf
service OrderService {
  rpc GetOrder(GetOrderRequest) returns (Order);
}
```

```go
// Client — one function call, no manual subjects or serialization
client := orders.NewOrderServiceClient(pn)
order, err := client.GetOrder(ctx, &orders.GetOrderRequest{OrderId: "abc"})

// Handler — implement an interface, register it
type handler struct { orders.UnimplementedOrderServiceHandler }
func (h *handler) GetOrder(ctx context.Context, req *orders.GetOrderRequest) (*orders.Order, error) {
    return fetchOrder(ctx, req.OrderId)
}
reg, _ := orders.RegisterOrderServiceHandler(pn, &handler{})
```

## Install

```bash
go install github.com/mudomi/protonats/cmd/protoc-gen-protonats@latest
```

Add the runtime library to your project:

```bash
go get github.com/mudomi/protonats
```

You also need [protoc](https://github.com/protocolbuffers/protobuf/releases) and [protoc-gen-go](https://pkg.go.dev/google.golang.org/protobuf/cmd/protoc-gen-go):

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
```

Or use the Docker image which bundles everything:

```bash
docker pull ghcr.io/mudomi/protonats
```

## Quick Start

### 1. Define your service in a `.proto` file

```protobuf
syntax = "proto3";
package myapp.orders;

option go_package = "github.com/myorg/myapp/gen/orders;orders";

import "protonats/options.proto";

service OrderService {
  // Request/Reply (default pattern — no options needed)
  rpc GetOrder(GetOrderRequest) returns (Order);
}

message GetOrderRequest {
  string order_id = 1;
}

message Order {
  string order_id = 1;
  string name = 2;
}
```

Copy `proto/protonats/options.proto` from this repo into your proto include path. This file defines the custom options ProtoNats uses.

### 2. Generate code

With local tools:

```bash
protoc \
  --go_out=gen --go_opt=paths=source_relative \
  --protonats_out=gen --protonats_opt=paths=source_relative \
  -I proto \
  proto/myapp/orders/orders.proto
```

Or with Docker (no local installs needed):

```bash
docker run --rm -v $(pwd):/work ghcr.io/mudomi/protonats \
  --go_out=/work/gen --go_opt=paths=source_relative \
  --protonats_out=/work/gen --protonats_opt=paths=source_relative \
  -I /work/proto \
  /work/proto/myapp/orders/orders.proto
```

This produces two files:
- `orders.pb.go` — standard protobuf types (from `protoc-gen-go`)
- `orders_protonats.pb.go` — typed client, handler interface, and registration function

### 3. Connect to NATS

```go
import (
    "github.com/nats-io/nats.go"
    "github.com/mudomi/protonats"
)

nc, _ := nats.Connect("nats://localhost:4222")
defer nc.Drain()

pn, _ := protonats.New(nc)
```

### 4. Implement a handler

```go
type orderHandler struct {
    orders.UnimplementedOrderServiceHandler
}

func (h *orderHandler) GetOrder(ctx context.Context, req *orders.GetOrderRequest) (*orders.Order, error) {
    // Your business logic here
    return &orders.Order{OrderId: req.OrderId, Name: "Widget"}, nil
}

reg, _ := orders.RegisterOrderServiceHandler(pn, &orderHandler{})
defer reg.Drain()
```

### 5. Call from a client

```go
client := orders.NewOrderServiceClient(pn)
order, err := client.GetOrder(ctx, &orders.GetOrderRequest{OrderId: "abc123"})
```

## Supported Patterns

| Pattern | Proto | Client returns |
|---------|-------|---------------|
| **Request/Reply** (default) | `rpc Foo(Req) returns (Resp)` | `(*Resp, error)` |
| **Publish** (fire-and-forget) | `rpc Foo(Msg) returns (Empty)` + `type: PUBLISH` | `error` |
| **JetStream Publish** | `type: JETSTREAM_PUBLISH` + `stream: "NAME"` | `error` |
| **JetStream Consume** | `type: JETSTREAM_CONSUME` + `stream: "NAME"` | handler-only |

Request/Reply is the default — no options needed.

## Subject Naming

Subjects are derived automatically from the proto package and method name:

```
{package}.{MethodName}  →  myapp.orders.GetOrder
```

Override with custom subjects, including dynamic segments:

```protobuf
rpc GetOrder(GetOrderRequest) returns (Order) {
  option (protonats.method).subject = "orders.{order_id}";
}

message GetOrderRequest {
  string order_id = 1 [(protonats.field).subject_token = true];
}
```

## Error Handling

Handlers return structured errors compatible with the NATS Service API:

```go
return nil, protonats.Errorf(404, "order %s not found", req.OrderId)
```

Clients receive them as `*protonats.Error`:

```go
var pnErr *protonats.Error
if errors.As(err, &pnErr) {
    log.Printf("code=%d msg=%s", pnErr.Code, pnErr.Message)
}
```

## Project Structure

```
cmd/protoc-gen-protonats/   protoc plugin binary
internal/gen/               code generation logic
proto/protonats/            options.proto definition
*.go                        runtime library (package protonats)
```

## Documentation

- [Design Overview](./docs/design.md) — architecture, philosophy, patterns
- [Proto Definition Guide](./docs/proto-definition.md) — writing `.proto` files for ProtoNats
- [Go Library Guide](./docs/go-library.md) — using generated code and the runtime
- [Docker Usage](./docs/docker.md) — using the Docker image locally and in CI

## License

[MIT](./LICENSE)
