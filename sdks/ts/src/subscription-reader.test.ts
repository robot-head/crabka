import { describe, expect, test } from "vitest";
import type { Message } from "./messaging.ts";
import { SubscriptionReader } from "./subscription-reader.ts";

describe("SubscriptionReader", () => {
  test("close does not wait behind a pending read", async () => {
    let returnWasCalled = false;
    const iterator = {
      next: () => new Promise<IteratorResult<Message>>(() => undefined),
      return: () => {
        returnWasCalled = true;
        return new Promise<IteratorResult<Message>>(() => undefined);
      },
    } satisfies AsyncIterator<Message>;
    const reader = new SubscriptionReader(iterator);

    void reader.read();
    await expect(reader.close()).resolves.toBeUndefined();

    expect(returnWasCalled).toBe(true);
    await expect(reader.read()).resolves.toEqual({ done: true, value: undefined });
  });
});
