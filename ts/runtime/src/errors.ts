import type { MsgHdrs } from "@nats-io/nats-core";

const HEADER_ERROR = "Nats-Service-Error";
const HEADER_ERROR_CODE = "Nats-Service-Error-Code";

/** Structured error with a numeric code, compatible with the NATS Service API. */
export class ProtoNatsError extends Error {
  readonly code: number;

  constructor(code: number, message: string) {
    super(message);
    this.name = "ProtoNatsError";
    this.code = code;
  }
}

/** Write error information to NATS headers. */
export function setErrorHeaders(headers: MsgHdrs, err: unknown): void {
  if (err instanceof ProtoNatsError) {
    headers.set(HEADER_ERROR, err.message);
    headers.set(HEADER_ERROR_CODE, String(err.code));
  } else if (err instanceof Error) {
    headers.set(HEADER_ERROR, err.message);
    headers.set(HEADER_ERROR_CODE, "500");
  } else {
    headers.set(HEADER_ERROR, String(err));
    headers.set(HEADER_ERROR_CODE, "500");
  }
}

/** Read error information from NATS headers. Returns null if no error. */
export function errorFromHeaders(h: MsgHdrs | undefined): ProtoNatsError | null {
  if (!h) return null;
  // Presence of the header, not its value, marks a failure: a handler may
  // return an error whose message is empty, and testing the value would hand
  // the caller a zero-valued response and no error instead.
  if (!h.has(HEADER_ERROR)) return null;
  const msg = h.get(HEADER_ERROR);
  const codeStr = h.get(HEADER_ERROR_CODE);
  const code = codeStr ? parseInt(codeStr, 10) : 500;
  return new ProtoNatsError(isNaN(code) ? 500 : code, msg);
}
