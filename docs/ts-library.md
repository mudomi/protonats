# ProtoNats TypeScript Library Guide

This document describes how to use the generated TypeScript code and the `@protonats/runtime` package.

## Prerequisites

You need four protoc plugins to generate both Go and TypeScript from the same `.proto` files:

| Plugin | Generates | Install |
|--------|-----------|---------|
| `protoc-gen-go` | Go message types (`_pb.go`) | `go install google.golang.org/protobuf/cmd/protoc-gen-go@latest` |
| `protoc-gen-protonats` | Go clients/handlers (`_protonats.pb.go`) | `go install github.com/mudomi/protonats/cmd/protoc-gen-protonats@latest` |
| `protoc-gen-es` | TS message types + schemas (`_pb.ts`) | `npm install -g @bufbuild/protoc-gen-es` |
| `protoc-gen-protonats-ts` | TS clients/handlers (`_protonats.ts`) | `go install github.com/mudomi/protonats/cmd/protoc-gen-protonats-ts@latest` |

All four plugins are Go binaries except `protoc-gen-es`, which is a Node.js package from the Buf team. The Docker image includes everything except `protoc-gen-es`.

## Installation

Install the runtime dependencies in your Node.js project:

```bash
npm install @protonats/runtime @bufbuild/protobuf @nats-io/nats-core @nats-io/transport-node
```

## Code Generation

```bash
protoc \
  --es_out=gen-ts --es_opt=target=ts \
  --protonats-ts_out=gen-ts --protonats-ts_opt=paths=source_relative \
  -I proto \
  proto/myapp/orders/orders.proto
```

Produces per proto file:
- `orders_pb.ts` -- Protobuf types and schema descriptors (from `protoc-gen-es`)
- `orders_protonats.ts` -- ProtoNats clients, handlers, registration (from `protoc-gen-protonats-ts`)

The `_protonats.ts` file imports message types and schemas from the `_pb.ts` file. Both plugins must target the same output directory.

---

## Connection Setup

ProtoNats wraps a NATS connection. You create and own the connection lifecycle.

```typescript
import { connect } from "@nats-io/transport-node";
import { ProtoNatsConn } from "@protonats/runtime";

const nc = await connect({ servers: "nats://localhost:4222" });
const pn = new ProtoNatsConn(nc);

// ... use pn to create clients and register handlers

// Cleanup
await nc.drain();
```

### Options

```typescript
const pn = new ProtoNatsConn(nc, {
  defaultTimeout: 10_000, // 10 seconds (default: 5000)
});
```

### Accessing Raw NATS

```typescript
const nc = pn.natsConn(); // NatsConnection
```

---

## Using Generated Clients

For each service, ProtoNats generates a client class.

### Request/Response

```typescript
import { OrderServiceClient } from "./gen-ts/myapp/orders/orders_protonats.js";

const client = new OrderServiceClient(pn);

const order = await client.getOrder({ orderId: "ord_abc123" });
console.log("Order status:", order.status);
```

### Publish (fire-and-forget)

```typescript
client.notify({ text: "order created" });
```

### Per-Call Options

```typescript
import { headers } from "@nats-io/nats-core";

const h = headers();
h.set("X-Request-Id", requestId);

const order = await client.getOrder(
  { orderId: "ord_abc123" },
  { timeout: 2000, headers: h },
);
```

| Option | Type | Description |
|--------|------|-------------|
| `timeout` | `number` | Request timeout in milliseconds |
| `headers` | `MsgHdrs` | Additional NATS headers |
| `signal` | `AbortSignal` | Cancellation signal |

---

## Implementing Handlers

For each service, ProtoNats generates a handler interface and a registration function.

### The Handler Interface

```typescript
// Generated
export interface OrderServiceHandler {
  getOrder(ctx: HandlerContext, req: GetOrderRequest): Promise<Order>;
  create(ctx: HandlerContext, req: CreateOrderRequest): Promise<Order>;
  notify(ctx: HandlerContext, req: NotifyRequest): Promise<void>;
}
```

### Implementation

```typescript
import { ProtoNatsError } from "@protonats/runtime";
import type { HandlerContext } from "@protonats/runtime";
import type { OrderServiceHandler } from "./gen-ts/myapp/orders/orders_protonats.js";
import type { GetOrderRequest, Order } from "./gen-ts/myapp/orders/orders_pb.js";

const handler: OrderServiceHandler = {
  async getOrder(ctx: HandlerContext, req: GetOrderRequest): Promise<Order> {
    const order = await db.getOrder(req.orderId);
    if (!order) {
      throw new ProtoNatsError(404, `order not found: ${req.orderId}`);
    }
    return order;
  },

  async create(ctx, req) {
    return db.createOrder(req);
  },

  async notify(ctx, req) {
    console.log("Notification:", req.text);
  },
};
```

### Registration

```typescript
import { registerOrderServiceHandler } from "./gen-ts/myapp/orders/orders_protonats.js";

const reg = registerOrderServiceHandler(pn, handler);

// Later, graceful shutdown:
await reg.drain();
```

### Registration Options

```typescript
const reg = registerOrderServiceHandler(pn, handler, {
  queueGroup: "order-svc",
});
```

| Option | Type | Description |
|--------|------|-------------|
| `queueGroup` | `string` | Queue group for load-balanced subscriptions |

---

## Error Handling

### Structured Errors

Handlers throw `ProtoNatsError` for typed errors:

```typescript
throw new ProtoNatsError(404, "order not found");
```

The runtime sets `Nats-Service-Error` and `Nats-Service-Error-Code` headers on the response, matching the Go behavior exactly. The generated client checks these headers and throws a `ProtoNatsError`.

```typescript
import { ProtoNatsError } from "@protonats/runtime";

try {
  const order = await client.getOrder({ orderId: "missing" });
} catch (err) {
  if (err instanceof ProtoNatsError) {
    console.log(`Service error ${err.code}: ${err.message}`);
  }
}
```

Plain errors (not `ProtoNatsError`) thrown from handlers are sent as code 500.

---

## Handler Context

The `HandlerContext` provides access to NATS metadata:

```typescript
const handler: OrderServiceHandler = {
  async getOrder(ctx, req) {
    console.log("Subject:", ctx.subject);       // actual NATS subject
    console.log("Headers:", ctx.msg.headers);    // raw NATS Msg
    // ...
  },
};
```

| Property | Type | Description |
|----------|------|-------------|
| `subject` | `string` | The NATS subject the message was received on |
| `msg` | `Msg` | The raw NATS message |

---

## Interop with Go

TypeScript and Go services are wire-compatible. A TypeScript client can call a Go handler and vice versa -- both use:

- Binary protobuf serialization
- Identical NATS subjects (derived from the same proto definitions)
- The same error header convention (`Nats-Service-Error` / `Nats-Service-Error-Code`)

No configuration needed. Just point both at the same NATS cluster.

```
┌─────────────┐       NATS       ┌─────────────┐
│   TS Client  │ ──── request ──→ │  Go Handler  │
│              │ ←── response ─── │              │
└─────────────┘                   └─────────────┘

┌─────────────┐       NATS       ┌─────────────┐
│  Go Client   │ ──── request ──→ │  TS Handler  │
│              │ ←── response ─── │              │
└─────────────┘                   └─────────────┘
```

---

## Naming Conventions

Proto names are converted to idiomatic TypeScript:

| Proto | TypeScript | Example |
|-------|-----------|---------|
| Method name | `camelCase` | `GetOrder` → `getOrder` |
| Field name | `camelCase` | `order_id` → `orderId` |
| Message type | `PascalCase` (unchanged) | `GetOrderRequest` → `GetOrderRequest` |
| Dynamic subject | Template literal | `orders.{order_id}` → `` `orders.${req.orderId}` `` |

These match the conventions used by `protoc-gen-es` (Protobuf-ES).

---

## What's Not Covered (Yet)

The TypeScript plugin currently supports **REQUEST_REPLY** and **PUBLISH** patterns. The following are deferred:

- **JetStream Publish / Consume** -- use nats.js JetStream API directly
- **KV Store helpers** -- use nats.js KV API directly
- **Interceptors / middleware** -- not yet available in the TS runtime
- **NATS Micro service discovery** -- not yet supported

For any of these, you can access the raw NATS connection via `pn.natsConn()`.

---

## Graceful Shutdown

```typescript
const reg1 = registerOrderServiceHandler(pn, orderHandler);
const reg2 = registerUserServiceHandler(pn, userHandler);

// Wait for signal
process.on("SIGTERM", async () => {
  await reg1.drain();
  await reg2.drain();
  await nc.drain();
});
```
