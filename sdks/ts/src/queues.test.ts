import { describe, expect, test } from "vitest";
import { createClient } from "./client.ts";
import { InvalidArgumentError } from "./errors.ts";
import { createQueuesModule } from "./queues.ts";

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
          error: { kind: "invalid_argument", message: "queue message is not acquired", retriable: false },
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

  test("live acknowledge preserves per-entry gateway error classification", async () => {
    const gateway = {
      async queueAcknowledge() {
        return {
          results: [
            { error: { code: 13, message: "commit failed", retriable: false } },
            { error: { code: 13, message: "commit retryable", retriable: true } },
            { error: { code: 9, message: "record is not acquired by this session", retriable: false } },
          ],
        };
      },
    } as unknown as NonNullable<Parameters<typeof createQueuesModule>[0]["gateway"]>;
    const queues = createQueuesModule({ endpoint: "http://gateway", gateway });

    await expect(
      queues.acknowledge("queue-session", [
        { messageId: "queue-ack:0:0", ackType: "accept" },
        { messageId: "queue-ack:0:1", ackType: "accept" },
        { messageId: "queue-ack:0:2", ackType: "accept" },
      ]),
    ).resolves.toEqual({
      results: [
        { messageId: "queue-ack:0:0", error: { kind: "server_error", message: "commit failed", retriable: false } },
        { messageId: "queue-ack:0:1", error: { kind: "transport", message: "commit retryable", retriable: true } },
        {
          messageId: "queue-ack:0:2",
          error: { kind: "invalid_argument", message: "record is not acquired by this session", retriable: false },
        },
      ],
    });
  });

  test("live renew preserves per-entry gateway error classification", async () => {
    const gateway = {
      async queueRenew() {
        return { results: [{ error: { code: 13, message: "renew retryable", retriable: true } }] };
      },
    } as unknown as NonNullable<Parameters<typeof createQueuesModule>[0]["gateway"]>;
    const queues = createQueuesModule({ endpoint: "http://gateway", gateway });

    await expect(queues.renew("queue-session", [{ messageId: "queue-renew:0:0" }])).resolves.toEqual({
      results: [
        { messageId: "queue-renew:0:0", error: { kind: "transport", message: "renew retryable", retriable: true } },
      ],
    });
  });
});
