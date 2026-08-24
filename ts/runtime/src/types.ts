import type { MsgHdrs, Msg } from "@nats-io/nats-core";

/** Options for a single client call. */
export interface CallOptions {
  /** Request timeout in milliseconds. */
  timeout?: number;
  /** Additional NATS headers. */
  headers?: MsgHdrs;
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
  /**
   * Queue group for every subscription in the registration. Left unset, each
   * handler joins a group named after its own proto method, so instances of a
   * service load-balance while distinct methods never take each other's
   * traffic. Pass null to disable queue groups and have every instance receive
   * every message. A shared explicit name is only safe when no two of the
   * service's subjects can match the same message.
   */
  queueGroup?: string | null;
}
