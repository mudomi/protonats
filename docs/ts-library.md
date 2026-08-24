# TypeScript Library Guide

Using the generated TypeScript code and the `@protonats/runtime` package.

## Setup

Generating both languages from one set of `.proto` files needs four plugins.
All are Go binaries except `protoc-gen-es`, the Buf team's Node package that
produces the message types; the Docker image bundles everything except that one.

| Plugin | Generates |
|---|---|
| `protoc-gen-go` | Go message types (`_pb.go`) |
| `protoc-gen-protonats` | Go clients and handlers (`_protonats.pb.go`) |
| `protoc-gen-es` | TS message types and schemas (`_pb.ts`) |
| `protoc-gen-protonats-ts` | TS clients and handlers (`_protonats.ts`) |

```bash
npm install @protonats/runtime @bufbuild/protobuf @nats-io/nats-core @nats-io/transport-node
npm install -g @bufbuild/protoc-gen-es

protoc \
  --es_out=gen-ts --es_opt=target=ts \
  --protonats-ts_out=gen-ts --protonats-ts_opt=paths=source_relative \
  -I proto \
  proto/myapp/orders/orders.proto
```

Both plugins must write to the same directory — the generated `_protonats.ts`
imports its types from the neighbouring `_pb.ts`.

## Connection

```ts
import { connect } from "@nats-io/transport-node";
import { ProtoNatsConn } from "@protonats/runtime";

const nc = await connect({ servers: "nats://localhost:4222" });
const pn = new ProtoNatsConn(nc);
```

| Option | Description |
|---|---|
| `defaultTimeout` | Request timeout in ms (default 5000) |
| `onError` | Called for errors with no caller to return to; defaults to `console.error` |

`pn.natsConn()` returns the underlying connection for anything not covered here.

## Clients

```ts
import { OrderServiceClient } from "./gen-ts/orders_protonats.js";

const client = new OrderServiceClient(pn);

const order = await client.getOrder({ orderId: "abc" });   // request/reply
client.notify({ text: "order created" });                  // publish
```

Per-call options: `{ timeout, headers }`.

## Handlers

```ts
import { ProtoNatsError } from "@protonats/runtime";
import type { HandlerContext } from "@protonats/runtime";
import { registerOrderServiceHandler } from "./gen-ts/orders_protonats.js";

const handler: OrderServiceHandler = {
  async getOrder(ctx: HandlerContext, req) {
    const order = await db.getOrder(req.orderId);
    if (!order) throw new ProtoNatsError(404, `order not found: ${req.orderId}`);
    return order;
  },
  async notify(ctx, req) {
    console.log("notification:", req.text);
  },
};

const reg = registerOrderServiceHandler(pn, handler);
// on shutdown:
await reg.drain();
```

`ctx` carries the concrete `subject` and the raw `msg`.

Registration takes `{ queueGroup }`. By default each handler joins a queue
group named after its proto method, so several instances of a service
load-balance while different methods never take each other's traffic. Pass
`null` to disable queue groups entirely and have every instance receive every
message.

## Errors

Handlers throw `ProtoNatsError`; clients receive it back:

```ts
try {
  await client.getOrder({ orderId: "missing" });
} catch (err) {
  if (err instanceof ProtoNatsError) console.log(err.code, err.message);
}
```

This uses the same `Nats-Service-Error` / `Nats-Service-Error-Code` headers as
the Go runtime. Errors that cannot be returned to a caller — a publish handler
throwing, an undecodable payload with no reply subject — go to `onError`.

## Interop with Go

Both runtimes use binary protobuf, the same subjects derived from the same
proto files, and the same error headers, so a TypeScript client can call a Go
handler and vice versa with no configuration. Point them at the same cluster.

## Naming

Proto names map to Protobuf-ES conventions: methods and fields become
`camelCase` (`GetOrder` → `getOrder`, `order_id` → `orderId`), message types
keep their `PascalCase`, and a dynamic subject becomes a template literal
(`orders.{order_id}` → `` `orders.${req.orderId}` ``).

## Not Covered

The TypeScript side supports request/reply and publish. JetStream methods are
skipped during generation, with a comment in the output saying so — use the
nats.js JetStream API directly. Interceptors, KV helpers, and micro service
discovery are not available in the TS runtime either. The Go side supports
JetStream fully, so one proto file can still serve both languages.
