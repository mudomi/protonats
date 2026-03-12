import type { Subscription } from "@nats-io/nats-core";

/** Holds active subscriptions for a registered handler. */
export class Registration {
  private subs: Subscription[] = [];

  /** Track a subscription for later cleanup. Called by generated code. */
  addSubscription(sub: Subscription): void {
    this.subs.push(sub);
  }

  /** Gracefully drain all subscriptions (finish in-flight, stop accepting new). */
  async drain(): Promise<void> {
    await Promise.all(this.subs.map((sub) => sub.drain()));
  }

  /** Immediately unsubscribe all subscriptions. */
  unsubscribe(): void {
    for (const sub of this.subs) {
      sub.unsubscribe();
    }
  }
}
