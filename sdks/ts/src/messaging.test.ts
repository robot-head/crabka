import { describe, expect, test } from "vitest";
import { createClient } from "./client.ts";
import type { SubscribeFrame } from "../gen/crabka/gateway/v1/gateway_pb.ts";
import { cloudEventHeaders, createMessagingModule, type Filter } from "./messaging.ts";

const encoder = new TextEncoder();

describe("messaging", () => {
  test("CloudEvents binary mapping uses pinned headers", () => {
    const headers = cloudEventHeaders({
      id: "evt-1",
      source: "/orders",
      type: "order.created",
      specversion: "1.0",
      datacontenttype: "application/json",
      data: encoder.encode('{"n":7}'),
    });

    expect(headers.map((header) => [header.name, new TextDecoder().decode(header.value)])).toEqual([
      ["ce_id", "evt-1"],
      ["ce_source", "/orders"],
      ["ce_type", "order.created"],
      ["ce_specversion", "1.0"],
      ["content-type", "application/json"],
    ]);
    expect(headers.map((header) => header.name)).not.toContain("ce_datacontenttype");
  });

  test("mock publish and subscribe round trip", async () => {
    const client = createClient({ endpoint: "mock://gateway" });
    await expect(client.messaging.publish({ topic: "roundtrip", value: encoder.encode("hello") })).resolves.toEqual({
      partition: 0,
      offset: 0,
      deduplicated: false,
    });

    const subscription = client.messaging.subscribe(["roundtrip"], { group: "reader" });
    await expect(subscription[Symbol.asyncIterator]().next()).resolves.toMatchObject({
      value: { topic: "roundtrip", partition: 0, offset: 0, value: encoder.encode("hello"), headers: [] },
      done: false,
    });
  });

  test("mock subscribe filter delivers matches only", async () => {
    const client = createClient({ endpoint: "mock://gateway" });
    await client.messaging.publish({ topic: "filtered", value: encoder.encode('{"kind":"skip"}') });
    await client.messaging.publish({ topic: "filtered", value: encoder.encode('{"kind":"keep"}') });

    const subscription = client.messaging.subscribe(["filtered"], {
      group: "reader",
      filter: { path: "$.kind", op: "equals", value: "keep" },
    });
    await expect(subscription[Symbol.asyncIterator]().next()).resolves.toMatchObject({
      value: { topic: "filtered", partition: 0, offset: 1, value: encoder.encode('{"kind":"keep"}'), headers: [] },
      done: false,
    });
  });

  test.each([
    { value: "C:\\tmp", expected: "path = 'C:\\\\tmp'" },
    { value: "C:\\tmp\\", expected: "path = 'C:\\\\tmp\\\\'" },
    { value: "C:\\tmp\\O'Brien", expected: "path = 'C:\\\\tmp\\\\O\\'Brien'" },
  ])("live subscribe string filter escapes backslashes and quotes for $value", async ({ value, expected }) => {
    await expect(renderedLiveFilterFor({ path: "$.path", op: "equals", value })).resolves.toBe(expected);
  });
});

async function renderedLiveFilterFor(filter: Filter): Promise<string> {
  let renderedFilter: string | undefined;
  const gateway = {
    subscribe(frames: AsyncIterable<SubscribeFrame>) {
      return {
        async *[Symbol.asyncIterator]() {
          const startFrame = await frames[Symbol.asyncIterator]().next();
          if (startFrame.done || startFrame.value.frame.case !== "start") {
            throw new Error("subscribe start frame is required");
          }
          renderedFilter = startFrame.value.frame.value.filter;
        },
      };
    },
  } as unknown as NonNullable<Parameters<typeof createMessagingModule>[0]["gateway"]>;

  const subscription = createMessagingModule({ endpoint: "http://gateway", gateway }).subscribe(["filtered"], {
    group: "reader",
    filter,
  });
  await subscription[Symbol.asyncIterator]().next();
  if (renderedFilter === undefined) {
    throw new Error("subscribe filter was not rendered");
  }
  return renderedFilter;
}
