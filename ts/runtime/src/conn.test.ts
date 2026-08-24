import { describe, expect, it, vi } from "vitest";
import { create, toBinary } from "@bufbuild/protobuf";
import { StringValueSchema } from "@bufbuild/protobuf/wkt";
import type { Msg, MsgHdrs, NatsConnection } from "@nats-io/nats-core";
import { headers } from "@nats-io/nats-core";
import { ProtoNatsConn } from "./conn.js";
import { ProtoNatsError, setErrorHeaders } from "./errors.js";

interface SubscribeCall {
  subject: string;
  queue: string | undefined;
  callback: (err: Error | null, msg: Msg) => void;
}

// Minimal in-memory NatsConnection double capturing subscriptions and replies.
function fakeNats() {
  const subscriptions: SubscribeCall[] = [];
  const published: { subject: string; data: Uint8Array; headers?: MsgHdrs }[] = [];

  const nc = {
    subscribe(subject: string, opts: { queue?: string; callback: SubscribeCall["callback"] }) {
      subscriptions.push({ subject, queue: opts.queue, callback: opts.callback });
      return { drain: async () => {}, unsubscribe: () => {} };
    },
    publish(subject: string, data: Uint8Array, opts?: { headers?: MsgHdrs }) {
      published.push({ subject, data, headers: opts?.headers });
    },
    async request(): Promise<Msg> {
      throw new Error("not wired in this test");
    },
  } as unknown as NatsConnection;

  return { nc, subscriptions, published };
}

function inboundMsg(subject: string, data: Uint8Array, reply?: string) {
  const replies: { data: Uint8Array; headers?: MsgHdrs }[] = [];
  const msg = {
    subject,
    data,
    reply,
    headers: undefined,
    respond(data?: Uint8Array, opts?: { headers?: MsgHdrs }) {
      replies.push({ data: data ?? new Uint8Array(), headers: opts?.headers });
      return true;
    },
  } as unknown as Msg;
  return { msg, replies };
}

const encoded = (value: string) => toBinary(StringValueSchema, create(StringValueSchema, { value }));

// Handlers default to a queue group named after their proto method.
const METHOD = "pkg.Svc.Do";

async function settled() {
  // Let pending promise callbacks (handler .then/.catch chains) run.
  await new Promise((r) => setTimeout(r, 0));
}

describe("subscribe (request/reply)", () => {
  it("defaults the queue group to the method name and replies with the handler result", async () => {
    const { nc, subscriptions } = fakeNats();
    const pn = new ProtoNatsConn(nc);

    pn.subscribe(METHOD, "t.echo", undefined, StringValueSchema, async (_ctx, req) =>
      encoded(`${req.value} back`),
    );

    expect(subscriptions[0].queue).toBe(METHOD);

    const { msg, replies } = inboundMsg("t.echo", encoded("hi"), "reply.1");
    subscriptions[0].callback(null, msg);
    await settled();

    expect(replies).toHaveLength(1);
    expect(replies[0].headers).toBeUndefined();
  });

  it("disables the queue group when null is passed", () => {
    const { nc, subscriptions } = fakeNats();
    const pn = new ProtoNatsConn(nc);

    pn.subscribe(METHOD, "t.echo", null, StringValueSchema, async () => new Uint8Array());

    expect(subscriptions[0].queue).toBeUndefined();
  });

  it("routes a decode failure with no reply subject to onError", async () => {
    const { nc, subscriptions } = fakeNats();
    const onError = vi.fn();
    const pn = new ProtoNatsConn(nc, { onError });

    pn.subscribe(METHOD, "t.echo", undefined, StringValueSchema, async () => new Uint8Array());

    const { msg } = inboundMsg("t.echo", new Uint8Array([0xff, 0xff]));
    subscriptions[0].callback(null, msg);
    await settled();

    expect(onError).toHaveBeenCalledWith("t.echo", expect.anything());
  });

  it("replies 400 on an undecodable payload", async () => {
    const { nc, subscriptions } = fakeNats();
    const pn = new ProtoNatsConn(nc);
    const handler = vi.fn();

    pn.subscribe(METHOD, "t.echo", undefined, StringValueSchema, handler);

    const bad = new Uint8Array([0xff, 0xff, 0xff]);
    const { msg, replies } = inboundMsg("t.echo", bad, "reply.1");
    subscriptions[0].callback(null, msg);
    await settled();

    expect(handler).not.toHaveBeenCalled();
    expect(replies[0].headers?.get("Nats-Service-Error-Code")).toBe("400");
  });

  it("maps a thrown ProtoNatsError to error headers", async () => {
    const { nc, subscriptions } = fakeNats();
    const pn = new ProtoNatsConn(nc);

    pn.subscribe(METHOD, "t.echo", undefined, StringValueSchema, async () => {
      throw new ProtoNatsError(404, "missing");
    });

    const { msg, replies } = inboundMsg("t.echo", encoded("x"), "reply.1");
    subscriptions[0].callback(null, msg);
    await settled();

    expect(replies[0].headers?.get("Nats-Service-Error")).toBe("missing");
    expect(replies[0].headers?.get("Nats-Service-Error-Code")).toBe("404");
  });

  it("reports handler errors without a reply subject to onError", async () => {
    const { nc, subscriptions } = fakeNats();
    const onError = vi.fn();
    const pn = new ProtoNatsConn(nc, { onError });

    pn.subscribe(METHOD, "t.echo", undefined, StringValueSchema, async () => {
      throw new Error("boom");
    });

    const { msg } = inboundMsg("t.echo", encoded("x"));
    subscriptions[0].callback(null, msg);
    await settled();

    expect(onError).toHaveBeenCalledWith("t.echo", expect.any(Error));
  });
});

describe("subscribePublish (fire-and-forget)", () => {
  it("defaults the queue group to the method name", () => {
    const { nc, subscriptions } = fakeNats();
    const pn = new ProtoNatsConn(nc);

    pn.subscribePublish(METHOD, "t.event", undefined, StringValueSchema, async () => {});

    expect(subscriptions[0].queue).toBe(METHOD);
  });

  it("disables the queue group when null is passed", () => {
    const { nc, subscriptions } = fakeNats();
    const pn = new ProtoNatsConn(nc);

    pn.subscribePublish(METHOD, "t.event", null, StringValueSchema, async () => {});

    expect(subscriptions[0].queue).toBeUndefined();
  });

  it("uses an explicit queue group", () => {
    const { nc, subscriptions } = fakeNats();
    const pn = new ProtoNatsConn(nc);

    pn.subscribePublish(METHOD, "t.event", "workers", StringValueSchema, async () => {});

    expect(subscriptions[0].queue).toBe("workers");
  });

  it("reports handler and decode errors to onError instead of swallowing them", async () => {
    const { nc, subscriptions } = fakeNats();
    const onError = vi.fn();
    const pn = new ProtoNatsConn(nc, { onError });

    pn.subscribePublish(METHOD, "t.event", undefined, StringValueSchema, async () => {
      throw new Error("handler failed");
    });

    const { msg } = inboundMsg("t.event", encoded("x"));
    subscriptions[0].callback(null, msg);
    await settled();
    expect(onError).toHaveBeenCalledTimes(1);

    const { msg: badMsg } = inboundMsg("t.event", new Uint8Array([0xff, 0xff]));
    subscriptions[0].callback(null, badMsg);
    await settled();
    expect(onError).toHaveBeenCalledTimes(2);
  });
});

describe("publish", () => {
  it("encodes the message and forwards headers", () => {
    const { nc, published } = fakeNats();
    const pn = new ProtoNatsConn(nc);

    const h = headers();
    h.set("X-Test", "v1");
    pn.publish("t.event", create(StringValueSchema, { value: "hi" }), StringValueSchema, { headers: h });

    expect(published).toHaveLength(1);
    expect(published[0].subject).toBe("t.event");
    expect(published[0].data).toEqual(encoded("hi"));
    expect(published[0].headers?.get("X-Test")).toBe("v1");
  });
});

describe("request", () => {
  it("throws when the reply carries error headers", async () => {
    const h = headers();
    setErrorHeaders(h, new ProtoNatsError(503, "unavailable"));
    const reply = { data: new Uint8Array(), headers: h } as unknown as Msg;

    const nc = {
      async request() {
        return reply;
      },
    } as unknown as NatsConnection;

    const pn = new ProtoNatsConn(nc);
    await expect(
      pn.request("t.echo", create(StringValueSchema, { value: "x" }), StringValueSchema, StringValueSchema),
    ).rejects.toMatchObject({ code: 503, message: "unavailable" });
  });

  // The Go runtime may reply with an error carrying no message; the client
  // must still throw rather than hand back a zero-valued response.
  it("throws on an error reply whose message is empty", async () => {
    const h = headers();
    setErrorHeaders(h, new ProtoNatsError(404, ""));
    const reply = { data: new Uint8Array(), headers: h } as unknown as Msg;

    const nc = {
      async request() {
        return reply;
      },
    } as unknown as NatsConnection;

    const pn = new ProtoNatsConn(nc);
    await expect(
      pn.request("t.echo", create(StringValueSchema, { value: "x" }), StringValueSchema, StringValueSchema),
    ).rejects.toBeInstanceOf(ProtoNatsError);
  });

  it("passes the call timeout and headers through, defaulting the timeout", async () => {
    const calls: { subject: string; opts: { timeout: number; headers?: MsgHdrs } }[] = [];
    const nc = {
      async request(subject: string, _data: Uint8Array, opts: { timeout: number; headers?: MsgHdrs }) {
        calls.push({ subject, opts });
        return { data: encoded("ok"), headers: undefined } as unknown as Msg;
      },
    } as unknown as NatsConnection;

    const pn = new ProtoNatsConn(nc, { defaultTimeout: 1234 });
    const req = create(StringValueSchema, { value: "x" });

    await pn.request("t.a", req, StringValueSchema, StringValueSchema);
    expect(calls[0].opts.timeout).toBe(1234);

    const h = headers();
    h.set("X-Test", "v1");
    await pn.request("t.b", req, StringValueSchema, StringValueSchema, { timeout: 50, headers: h });
    expect(calls[1].opts.timeout).toBe(50);
    expect(calls[1].opts.headers?.get("X-Test")).toBe("v1");
  });

  it("decodes a successful reply", async () => {
    const reply = { data: encoded("pong"), headers: undefined } as unknown as Msg;
    const nc = {
      async request() {
        return reply;
      },
    } as unknown as NatsConnection;

    const pn = new ProtoNatsConn(nc);
    const resp = await pn.request(
      "t.echo",
      create(StringValueSchema, { value: "ping" }),
      StringValueSchema,
      StringValueSchema,
    );
    expect(resp.value).toBe("pong");
  });
});
