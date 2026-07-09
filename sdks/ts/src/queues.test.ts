import { describe, expect, test } from "vitest";
import { createClient } from "./client.ts";
import { InvalidArgumentError } from "./errors.ts";

const encoder = new TextEncoder();

describe("queues", () => {
  test("mock acquire returns queue v1.1 message shape", async () => {
    const client = createClient({ endpoint: "mock://gateway" });
    await client.messaging.publish({
      topic: "queue-acquire",
      value: encoder.encode("work"),
      headers: [{ name: "kind", value: encoder.encode("queue") }],
    });

    await expect(
      client.queues.acquire("queue-acquire", { group: "workers", max: 1, lockDurationMs: 30_000 }),
    ).resolves.toMatchObject({
      sessionId: "queue-session-1",
      messages: [
        {
          messageId: "queue-acquire:0:0",
          topic: "queue-acquire",
          partition: 0,
          offset: 0,
          value: encoder.encode("work"),
          headers: [{ name: "kind", value: encoder.encode("queue") }],
          deliveryCount: 1,
        },
      ],
    });
  });

  test("mock acknowledge and renew return per-entry result shapes", async () => {
    const client = createClient({ endpoint: "mock://gateway" });
    await client.messaging.publish({ topic: "queue-ack", value: encoder.encode("ack") });
    const acquired = await client.queues.acquire("queue-ack", { group: "workers", max: 1, lockDurationMs: 30_000 });

    await expect(client.queues.renew(acquired.sessionId, [{ messageId: "queue-ack:0:0" }])).resolves.toEqual({
      results: [{ messageId: "queue-ack:0:0", error: null }],
    });
    await expect(
      client.queues.acknowledge(acquired.sessionId, [
        { messageId: "queue-ack:0:0", ackType: "accept" },
        { messageId: "missing:0:0", ackType: "accept" },
      ]),
    ).resolves.toEqual({
      results: [
        { messageId: "queue-ack:0:0", error: null },
        {
          messageId: "missing:0:0",
          error: { kind: "invalid_argument", message: "queue message is not acquired" },
        },
      ],
    });
  });

  test("mock acquire validates group and lock duration", async () => {
    const client = createClient({ endpoint: "mock://gateway" });

    await expect(client.queues.acquire("queue", { group: "" })).rejects.toThrow(InvalidArgumentError);
    await expect(client.queues.acquire("queue", { group: "workers", lockDurationMs: 1_000 })).rejects.toMatchObject({
      message: "queue lock_duration_ms must be 30000; per-acquire lock durations are not supported",
    });
  });
});
