import { describe, expect, it, vi } from "vitest";
import type { Subscription } from "@nats-io/nats-core";
import { Registration } from "./registration.js";

function fakeSub() {
  return {
    drain: vi.fn(async () => {}),
    unsubscribe: vi.fn(() => {}),
  } as unknown as Subscription & { drain: ReturnType<typeof vi.fn>; unsubscribe: ReturnType<typeof vi.fn> };
}

describe("Registration", () => {
  it("drains every tracked subscription", async () => {
    const reg = new Registration();
    const a = fakeSub();
    const b = fakeSub();
    reg.addSubscription(a);
    reg.addSubscription(b);

    await reg.drain();

    expect(a.drain).toHaveBeenCalledTimes(1);
    expect(b.drain).toHaveBeenCalledTimes(1);
  });

  it("unsubscribes every tracked subscription", () => {
    const reg = new Registration();
    const a = fakeSub();
    const b = fakeSub();
    reg.addSubscription(a);
    reg.addSubscription(b);

    reg.unsubscribe();

    expect(a.unsubscribe).toHaveBeenCalledTimes(1);
    expect(b.unsubscribe).toHaveBeenCalledTimes(1);
  });

  it("is a no-op when empty", async () => {
    const reg = new Registration();
    await expect(reg.drain()).resolves.toBeUndefined();
    expect(() => reg.unsubscribe()).not.toThrow();
  });

  it("propagates a drain failure rather than hiding it", async () => {
    const reg = new Registration();
    const failing = {
      drain: vi.fn(async () => {
        throw new Error("drain failed");
      }),
      unsubscribe: vi.fn(),
    } as unknown as Subscription;
    reg.addSubscription(failing);

    await expect(reg.drain()).rejects.toThrow("drain failed");
  });
});
