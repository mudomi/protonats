import { describe, expect, it } from "vitest";
import { headers } from "@nats-io/nats-core";
import { ProtoNatsError, setErrorHeaders, errorFromHeaders } from "./errors.js";

describe("error headers", () => {
  it("round-trips a structured error", () => {
    const h = headers();
    setErrorHeaders(h, new ProtoNatsError(404, "not found"));

    const err = errorFromHeaders(h);
    expect(err).toBeInstanceOf(ProtoNatsError);
    expect(err?.code).toBe(404);
    expect(err?.message).toBe("not found");
  });

  it("wraps a plain Error as code 500", () => {
    const h = headers();
    setErrorHeaders(h, new Error("boom"));

    const err = errorFromHeaders(h);
    expect(err?.code).toBe(500);
    expect(err?.message).toBe("boom");
  });

  it("stringifies non-Error values", () => {
    const h = headers();
    setErrorHeaders(h, "just a string");

    const err = errorFromHeaders(h);
    expect(err?.code).toBe(500);
    expect(err?.message).toBe("just a string");
  });

  it("returns null without error headers", () => {
    expect(errorFromHeaders(headers())).toBeNull();
    expect(errorFromHeaders(undefined)).toBeNull();
  });

  // Keying off the header's value instead of its presence silently turned a
  // failure into a successful, zero-valued response.
  it("still reports an error when the message is empty", () => {
    const h = headers();
    setErrorHeaders(h, new ProtoNatsError(404, ""));

    const err = errorFromHeaders(h);
    expect(err).toBeInstanceOf(ProtoNatsError);
    expect(err?.code).toBe(404);
    expect(err?.message).toBe("");
  });

  it("defaults to 500 on a malformed code", () => {
    const h = headers();
    h.set("Nats-Service-Error", "oops");
    h.set("Nats-Service-Error-Code", "not-a-number");

    expect(errorFromHeaders(h)?.code).toBe(500);
  });
});
