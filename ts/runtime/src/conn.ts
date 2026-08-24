import type { NatsConnection, Msg, Subscription } from "@nats-io/nats-core";
import { headers } from "@nats-io/nats-core";
import type { DescMessage, MessageShape } from "@bufbuild/protobuf";
import { encode, decode } from "./codec.js";
import { errorFromHeaders, setErrorHeaders, ProtoNatsError } from "./errors.js";
import type { CallOptions, HandlerContext, HandlerOptions } from "./types.js";

const DEFAULT_TIMEOUT = 5000;

/** Called when a handler or decode error cannot be reported to any caller. */
export type ErrorHandler = (subject: string, err: unknown) => void;

/** ProtoNats connection wrapping a NATS connection with protobuf serialization. */
export class ProtoNatsConn {
  private readonly nc: NatsConnection;
  private readonly defaultTimeout: number;
  private readonly onError: ErrorHandler;

  constructor(
    nc: NatsConnection,
    opts?: { defaultTimeout?: number; onError?: ErrorHandler },
  ) {
    this.nc = nc;
    this.defaultTimeout = opts?.defaultTimeout ?? DEFAULT_TIMEOUT;
    this.onError =
      opts?.onError ??
      ((subject, err) => console.error(`protonats: error on ${subject}:`, err));
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
    method: string,
    subject: string,
    queueGroup: string | null | undefined,
    reqSchema: ReqDesc,
    handler: (ctx: HandlerContext, req: MessageShape<ReqDesc>) => Promise<Uint8Array>,
  ): Subscription {
    return this.nc.subscribe(subject, {
      queue: resolveQueue(queueGroup, method),
      callback: (_err: Error | null, msg: Msg) => {
        const ctx: HandlerContext = { subject: msg.subject, msg };

        let req: MessageShape<ReqDesc>;
        try {
          req = decode(reqSchema, msg.data ?? new Uint8Array());
        } catch (e) {
          if (msg.reply) {
            this.respondError(msg, new ProtoNatsError(400, `invalid request: ${e}`));
          } else {
            this.onError(msg.subject, e);
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
              this.respondError(msg, err);
            } else {
              this.onError(msg.subject, err);
            }
          });
      },
    });
  }

  /** Create a fire-and-forget subscription. Used by generated registration code. */
  subscribePublish<ReqDesc extends DescMessage>(
    method: string,
    subject: string,
    queueGroup: string | null | undefined,
    reqSchema: ReqDesc,
    handler: (ctx: HandlerContext, req: MessageShape<ReqDesc>) => Promise<void>,
  ): Subscription {
    return this.nc.subscribe(subject, {
      queue: resolveQueue(queueGroup, method),
      callback: (_err: Error | null, msg: Msg) => {
        const ctx: HandlerContext = { subject: msg.subject, msg };

        let req: MessageShape<ReqDesc>;
        try {
          req = decode(reqSchema, msg.data ?? new Uint8Array());
        } catch (e) {
          this.onError(msg.subject, e);
          return;
        }

        handler(ctx, req).catch((err) => this.onError(msg.subject, err));
      },
    });
  }

  private respondError(msg: Msg, err: unknown): void {
    if (!msg.reply) return;
    const h = headers();
    setErrorHeaders(h, err);
    msg.respond(new Uint8Array(), { headers: h });
  }
}

/**
 * Handlers join a queue group named after their fully qualified proto method
 * unless one is configured explicitly. The name must be unique per method, NOT
 * a shared constant: NATS groups queue subscribers by queue name across every
 * subject pattern matching the delivered subject, not per subscription subject,
 * so a shared name lets a wildcard handler swallow another method's messages.
 * `null` opts out entirely and every instance receives every message.
 */
function resolveQueue(
  queueGroup: string | null | undefined,
  method: string,
): string | undefined {
  if (queueGroup === null) return undefined;
  return queueGroup ?? method;
}
