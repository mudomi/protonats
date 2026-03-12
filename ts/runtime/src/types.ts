import type { MsgHdrs, Msg } from "@nats-io/nats-core";

/** Options for a single client call. */
export interface CallOptions {
  /** Request timeout in milliseconds. */
  timeout?: number;
  /** Additional NATS headers. */
  headers?: MsgHdrs;
  /** Abort signal for cancellation. */
  signal?: AbortSignal;
}

/** Context passed to handler functions. */
export interface HandlerContext {
  /** The NATS subject the message was received on. */
  subject: string;
  /** The raw NATS message. */
  msg: Msg;
}

/** Options for handler registration. */
export interface HandlerOptions {
  /** Queue group for load-balanced subscriptions. */
  queueGroup?: string;
}
