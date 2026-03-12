import type { NatsConnection, Msg, Subscription } from "@nats-io/nats-core";
import { headers } from "@nats-io/nats-core";
import type { DescMessage, MessageShape } from "@bufbuild/protobuf";
import { encode, decode } from "./codec.js";
import { errorFromHeaders, setErrorHeaders, ProtoNatsError } from "./errors.js";
import type { CallOptions, HandlerContext, HandlerOptions } from "./types.js";
import { Registration } from "./registration.js";

const DEFAULT_TIMEOUT = 5000;

/** ProtoNats connection wrapping a NATS connection with protobuf serialization. */
export class ProtoNatsConn {
  private readonly nc: NatsConnection;
  private readonly defaultTimeout: number;

  constructor(nc: NatsConnection, opts?: { defaultTimeout?: number }) {
    this.nc = nc;
    this.defaultTimeout = opts?.defaultTimeout ?? DEFAULT_TIMEOUT;
  }

  /** Returns the underlying NATS connection. */
  natsConn(): NatsConnection {
    return this.nc;
  }

  /** Perform a request/reply call with protobuf serialization. */
  async request<ReqDesc extends DescMessage, RespDesc extends DescMessage>(
    subject: string,
    req: MessageShape<ReqDesc>,
    reqSchema: ReqDesc,
    respSchema: RespDesc,
    opts?: CallOptions,
  ): Promise<MessageShape<RespDesc>> {
    const data = encode(reqSchema, req);
    const timeout = opts?.timeout ?? this.defaultTimeout;

    const reply = await this.nc.request(subject, data, {
      timeout,
      headers: opts?.headers,
    });

    const err = errorFromHeaders(reply.headers);
    if (err) throw err;

    return decode(respSchema, reply.data ?? new Uint8Array());
  }

  /** Send a fire-and-forget message with protobuf serialization. */
  publish<ReqDesc extends DescMessage>(
    subject: string,
    req: MessageShape<ReqDesc>,
    reqSchema: ReqDesc,
    opts?: CallOptions,
  ): void {
    const data = encode(reqSchema, req);
    this.nc.publish(subject, data, {
      headers: opts?.headers,
    });
  }

  /** Create a request/reply subscription. Used by generated registration code. */
  subscribe<ReqDesc extends DescMessage>(
    subject: string,
    queueGroup: string | undefined,
    reqSchema: ReqDesc,
    handler: (ctx: HandlerContext, req: MessageShape<ReqDesc>) => Promise<Uint8Array>,
  ): Subscription {
    const sub = this.nc.subscribe(subject, {
      queue: queueGroup,
      callback: (_err: Error | null, msg: Msg) => {
        const ctx: HandlerContext = { subject: msg.subject, msg };

        let req: MessageShape<ReqDesc>;
        try {
          req = decode(reqSchema, msg.data ?? new Uint8Array());
        } catch (e) {
          if (msg.reply) {
            const h = headers();
            setErrorHeaders(h, new ProtoNatsError(400, `invalid request: ${e}`));
            msg.respond(new Uint8Array(), { headers: h });
          }
          return;
        }

        handler(ctx, req)
          .then((respData) => {
            if (msg.reply) {
              msg.respond(respData);
            }
          })
          .catch((err) => {
            if (msg.reply) {
              const h = headers();
              setErrorHeaders(h, err);
              msg.respond(new Uint8Array(), { headers: h });
            }
          });
      },
    });
    return sub;
  }

  /** Create a fire-and-forget subscription. Used by generated registration code. */
  subscribePublish<ReqDesc extends DescMessage>(
    subject: string,
    queueGroup: string | undefined,
    reqSchema: ReqDesc,
    handler: (ctx: HandlerContext, req: MessageShape<ReqDesc>) => Promise<void>,
  ): Subscription {
    const sub = this.nc.subscribe(subject, {
      queue: queueGroup,
      callback: (_err: Error | null, msg: Msg) => {
        const ctx: HandlerContext = { subject: msg.subject, msg };

        let req: MessageShape<ReqDesc>;
        try {
          req = decode(reqSchema, msg.data ?? new Uint8Array());
        } catch {
          return;
        }

        handler(ctx, req).catch(() => {});
      },
    });
    return sub;
  }
}
