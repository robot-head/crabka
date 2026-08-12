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
          offset: 0n,
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

  test("live queue offsets and message ids preserve bigint precision", async () => {
    const offset = 9_007_199_254_740_993n;
    const gateway = {
      async queueAcquire(request: { sessionId: string }) {
        expect(request.sessionId).toBe("session");
        return { sessionId: "session", messages: [{ topic: "large", partition: 0, offset, value: new Uint8Array(), headers: [], deliveryCount: 1 }] };
      },
      async queueAcknowledge(request: { entries: { offset: bigint }[] }) {
        expect(request.entries[0]?.offset).toBe(offset);
        return { results: [{ entry: { topic: "large", partition: 0, offset } }] };
      },
    } as unknown as NonNullable<Parameters<typeof createQueuesModule>[0]["gateway"]>;
    const queues = createQueuesModule({ endpoint: "http://gateway", gateway });

    const acquired = await queues.acquire("large", { group: "workers", sessionId: "session" });
    expect(acquired.messages[0]).toMatchObject({ messageId: `large:0:${offset}`, offset });
    await queues.acknowledge("session", [{ messageId: `large:0:${offset}`, ackType: "accept" }]);
  });

  test("live queue payloads distinguish tombstones from empty values", async () => {
    const gateway = {
      async queueAcquire() {
        return {
          sessionId: "session",
          messages: [
            { topic: "queue", partition: 0, offset: 0n, headers: [], deliveryCount: 1 },
            { topic: "queue", partition: 0, offset: 1n, value: new Uint8Array(), headers: [], deliveryCount: 1 },
          ],
        };
      },
    } as unknown as NonNullable<Parameters<typeof createQueuesModule>[0]["gateway"]>;
    const queues = createQueuesModule({ endpoint: "http://gateway", gateway });

    const acquired = await queues.acquire("queue", { group: "workers", sessionId: "session" });
    expect(acquired.messages[0]?.value).toBeUndefined();
    expect(acquired.messages[1]?.value).toEqual(new Uint8Array());
  });

  test("mock acquire validates group and lock duration", async () => {
    const client = createClient({ endpoint: "mock://gateway" });

    await expect(client.queues.acquire("queue", { group: "" })).rejects.toThrow(InvalidArgumentError);
    await expect(client.queues.acquire("queue", { group: "workers", lockDurationMs: 1_000 })).rejects.toMatchObject({
      message: "queue lock_duration_ms must be 30000; per-acquire lock durations are not supported",
    });
    await expect(
      client.queues.acknowledge("session", [{ messageId: "queue:0:0", ackType: "relese" as "release" }]),
    ).rejects.toMatchObject({
      kind: "invalid_argument",
      message: "queue ack_type must be accept, release, or reject",
    });
  });

  test("mock sessions own delivered coordinates and reject unknown reuse", async () => {
    const client = createClient({ endpoint: "mock://gateway" });
    await client.messaging.publish({ topic: "queue", value: encoder.encode("job") });
    const first = await client.queues.acquire("queue", { group: "workers", max: 1 });
    const second = await client.queues.acquire("queue", { group: "workers", max: 1 });

    await expect(
      client.queues.acknowledge(second.sessionId, [{ messageId: first.messages[0]!.messageId, ackType: "accept" }]),
    ).resolves.toEqual({
      results: [
        {
          messageId: first.messages[0]!.messageId,
          error: { kind: "invalid_argument", message: "queue message is not acquired", retriable: false },
        },
      ],
    });
    await expect(
      client.queues.renew(second.sessionId, [{ messageId: first.messages[0]!.messageId }]),
    ).resolves.toEqual({
      results: [
        {
          messageId: first.messages[0]!.messageId,
          error: { kind: "invalid_argument", message: "queue message is not acquired", retriable: false },
        },
      ],
    });
    await expect(
      client.queues.acquire("queue", { group: "workers", sessionId: "missing-session" }),
    ).rejects.toMatchObject({ message: "queue session expired; re-acquire" });
    await expect(
      client.queues.acquire("queue", { group: "other-workers", sessionId: first.sessionId }),
    ).rejects.toMatchObject({ message: "group_id and topics are fixed when a queue session is created" });
  });

  test("mock queue state is independent per group", async () => {
    const client = createClient({ endpoint: "mock://gateway" });
    await client.messaging.publish({ topic: "queue", value: encoder.encode("job") });
    const first = await client.queues.acquire("queue", { group: "first-workers", max: 1 });
    const second = await client.queues.acquire("queue", { group: "second-workers", max: 1 });

    expect(first.messages).toMatchObject([{ messageId: "queue:0:0", deliveryCount: 1 }]);
    expect(second.messages).toMatchObject([{ messageId: "queue:0:0", deliveryCount: 1 }]);
    await expect(
      client.queues.acknowledge(first.sessionId, [{ messageId: "queue:0:0", ackType: "release" }]),
    ).resolves.toEqual({ results: [{ messageId: "queue:0:0", error: null }] });
    await expect(client.queues.renew(second.sessionId, [{ messageId: "queue:0:0" }])).resolves.toEqual({
      results: [{ messageId: "queue:0:0", error: null }],
    });

    const redelivered = await client.queues.acquire("queue", {
      group: "first-workers",
      max: 1,
      sessionId: first.sessionId,
    });
    expect(redelivered.messages).toMatchObject([{ messageId: "queue:0:0", deliveryCount: 2 }]);
    await expect(
      client.queues.acknowledge(first.sessionId, [{ messageId: "queue:0:0", ackType: "accept" }]),
    ).resolves.toEqual({ results: [{ messageId: "queue:0:0", error: null }] });
    await expect(
      client.queues.acknowledge(second.sessionId, [{ messageId: "queue:0:0", ackType: "accept" }]),
    ).resolves.toEqual({ results: [{ messageId: "queue:0:0", error: null }] });
  });

  test("live acknowledge preserves per-entry gateway error classification", async () => {
    const gateway = {
      async queueAcknowledge() {
        return {
          results: [
            { entry: { topic: "queue-ack", partition: 0, offset: 3n }, error: { code: 13, message: "commit failed", retriable: false } },
            { entry: { topic: "queue-ack", partition: 0, offset: 2n }, error: { code: 13, message: "commit retryable", retriable: true } },
            { entry: { topic: "queue-ack", partition: 0, offset: 1n }, error: { code: 9, message: "record is not acquired by this session", retriable: false } },
            { entry: { topic: "queue-ack", partition: 0, offset: 0n }, error: { code: 9, message: "coordinator retry", retriable: true } },
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
        { messageId: "queue-ack:0:3", ackType: "accept" },
      ]),
    ).resolves.toEqual({
      results: [
        { messageId: "queue-ack:0:3", error: { kind: "server_error", message: "commit failed", retriable: false } },
        { messageId: "queue-ack:0:2", error: { kind: "transport", message: "commit retryable", retriable: true } },
        {
          messageId: "queue-ack:0:1",
          error: { kind: "invalid_argument", message: "record is not acquired by this session", retriable: false },
        },
        { messageId: "queue-ack:0:0", error: { kind: "transport", message: "coordinator retry", retriable: true } },
      ],
    });
  });

  test("live renew preserves per-entry gateway error classification", async () => {
    const gateway = {
      async queueRenew() {
        return {
          results: [
            {
              entry: { topic: "queue-renew", partition: 0, offset: 7n },
              error: { code: 13, message: "renew retryable", retriable: true },
            },
          ],
        };
      },
    } as unknown as NonNullable<Parameters<typeof createQueuesModule>[0]["gateway"]>;
    const queues = createQueuesModule({ endpoint: "http://gateway", gateway });

    await expect(queues.renew("queue-session", [{ messageId: "queue-renew:0:0" }])).resolves.toEqual({
      results: [
        { messageId: "queue-renew:0:7", error: { kind: "transport", message: "renew retryable", retriable: true } },
      ],
    });
  });

  test("live queue results require authoritative response entries", async () => {
    const gateway = {
      async queueAcknowledge() {
        return { results: [{}] };
      },
    } as unknown as NonNullable<Parameters<typeof createQueuesModule>[0]["gateway"]>;
    const queues = createQueuesModule({ endpoint: "http://gateway", gateway });

    await expect(
      queues.acknowledge("queue-session", [{ messageId: "queue-ack:0:0", ackType: "accept" }]),
    ).rejects.toMatchObject({
      kind: "transport",
      message: "queue response result did not include an entry",
    });
  });
});
