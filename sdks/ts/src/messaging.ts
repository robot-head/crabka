import type { Client } from "@connectrpc/connect";
import { create } from "@bufbuild/protobuf";
import {
  Acks,
  Gateway,
  HeaderSchema,
  Inbound,
  RecordSchema,
  SendRequestSchema,
  SubscribeFrame,
  SubscribeFrameSchema,
  SubscribeStartSchema,
} from "../gen/crabka/gateway/v1/gateway_pb.ts";
import { fromConnectError, fromRecordError, InvalidArgumentError, NotFoundError, TransportError } from "./errors.ts";

export type Header = { name: string; value?: Uint8Array };
export type PublishRecord = { topic: string; value: Uint8Array; headers?: Header[] };
export type PublishResult = { partition: number; offset: number; deduplicated: boolean };
export type CloudEvent = {
  id: string;
  source: string;
  type: string;
  specversion: string;
  datacontenttype?: string;
  data: Uint8Array;
};
export type Filter = { path: string; op: "equals"; value: string | number | boolean };
export type Message = { topic: string; partition: number; offset: number; value: Uint8Array; headers: Header[] };

export type StoredMessage = {
  record: Required<PublishRecord>;
  partition: number;
  offset: number;
  queueState: "available" | "acquired" | "accepted" | "rejected";
  deliveryCount: number;
};
export type MockStore = { messages: StoredMessage[]; nextQueueSessionId: number };
type LiveGateway = Client<typeof Gateway>;

export type MessagingModule = {
  publish(record: PublishRecord): Promise<PublishResult>;
  publishEvent(topic: string, event: CloudEvent): Promise<PublishResult>;
  subscribe(topics: string[], options: { group: string; filter?: Filter }): AsyncIterable<Message>;
};

export function createMessagingModule(options: {
  endpoint: string;
  gateway?: LiveGateway;
  headers?: HeadersInit;
  mockStore?: MockStore;
}): MessagingModule {
  return {
    publish: (record) => publishRecord(options, record),
    publishEvent: (topic, event) => publishEvent(options, topic, event),
    subscribe: (topics, subscribeOptions) => subscribe(options, topics, subscribeOptions),
  };
}

export function createMockStore(): MockStore {
  return { messages: [], nextQueueSessionId: 1 };
}

export function cloudEventHeaders(event: CloudEvent): Header[] {
  if (event.id.trim() === "") {
    throw new InvalidArgumentError("CloudEvent id is required");
  }

  const encoder = new TextEncoder();
  const headers = [
    { name: "ce_id", value: encoder.encode(event.id) },
    { name: "ce_source", value: encoder.encode(event.source) },
    { name: "ce_type", value: encoder.encode(event.type) },
    { name: "ce_specversion", value: encoder.encode(event.specversion) },
  ];
  if (!event.datacontenttype) {
    return headers;
  }
  return [...headers, { name: "content-type", value: encoder.encode(event.datacontenttype) }];
}

async function publishEvent(
  options: Parameters<typeof createMessagingModule>[0],
  topic: string,
  event: CloudEvent,
): Promise<PublishResult> {
  const headers = cloudEventHeaders(event);
  return publishRecord(options, { topic, value: event.data, headers });
}

async function publishRecord(
  options: Parameters<typeof createMessagingModule>[0],
  record: PublishRecord,
): Promise<PublishResult> {
  assertValidRecord(record);
  if (options.endpoint.startsWith("unreachable://")) {
    throw new TransportError("endpoint unreachable");
  }
  if (options.mockStore) {
    return publishMockRecord(options.mockStore, record);
  }
  if (!options.gateway) {
    throw new TransportError("gateway transport is not configured");
  }

  try {
    const response = await options.gateway.send(
      create(SendRequestSchema, {
        records: [toProtoRecord(record)],
        acks: Acks.ALL,
      }),
      { headers: options.headers },
    );
    const result = response.results[0];
    if (!result) {
      throw new Error("send returned no record results");
    }
    if (result.error) {
      throw fromRecordError(result.error.code, result.error.message, result.error.retriable);
    }
    return { partition: result.partition, offset: Number(result.offset), deduplicated: result.deduplicated };
  } catch (error) {
    throw fromConnectError(error);
  }
}

function publishMockRecord(store: MockStore, record: PublishRecord): PublishResult {
  const offset = store.messages.filter((message) => message.record.topic === record.topic).length;
  store.messages.push({ record: cloneRecord(record), partition: 0, offset, queueState: "available", deliveryCount: 0 });
  return { partition: 0, offset, deduplicated: false };
}

function subscribe(
  options: Parameters<typeof createMessagingModule>[0],
  topics: string[],
  subscribeOptions: { group: string; filter?: Filter },
): AsyncIterable<Message> {
  if (topics.length === 0) {
    throw new InvalidArgumentError("at least one topic is required");
  }
  if (subscribeOptions.filter && subscribeOptions.filter.op !== "equals") {
    throw new InvalidArgumentError("only equals filters are supported");
  }
  if (options.mockStore) {
    return mockSubscription(options.mockStore, topics, subscribeOptions.filter);
  }
  if (!options.gateway) {
    throw new TransportError("gateway transport is not configured");
  }
  return liveSubscription(options.gateway, options.headers, topics, subscribeOptions);
}

async function* mockSubscription(store: MockStore, topics: string[], filter?: Filter): AsyncIterable<Message> {
  let nextIndex = 0;
  while (true) {
    const message = findNextMockMessage(store, topics, filter, nextIndex);
    if (!message) {
      throw new NotFoundError("no message available");
    }
    nextIndex = message.nextIndex;
    yield toMessage(message.stored);
  }
}

function findNextMockMessage(store: MockStore, topics: string[], filter: Filter | undefined, startIndex: number) {
  for (let index = startIndex; index < store.messages.length; index += 1) {
    const stored = store.messages[index];
    if (!stored || !topics.includes(stored.record.topic)) {
      continue;
    }
    if (!mockFilterMatches(filter, stored.record.value)) {
      continue;
    }
    return { stored, nextIndex: index + 1 };
  }
  return undefined;
}

function liveSubscription(
  gateway: LiveGateway,
  headers: HeadersInit | undefined,
  topics: string[],
  options: { group: string; filter?: Filter },
): AsyncIterable<Message> {
  const startFrame = create(SubscribeFrameSchema, {
    frame: {
      case: "start",
      value: create(SubscribeStartSchema, {
        groupId: options.group,
        topics,
        autoCommit: true,
        filter: toProtoFilter(options.filter),
      }),
    },
  });
  return {
    [Symbol.asyncIterator]: () => liveSubscriptionIterator(gateway, headers, startFrame),
  };
}

function liveSubscriptionIterator(
  gateway: LiveGateway,
  headers: HeadersInit | undefined,
  startFrame: SubscribeFrame,
): AsyncIterator<Message> {
  const abortController = new AbortController();
  const inboundIterator = gateway
    .subscribe(openSubscribeFrames(startFrame), { headers, signal: abortController.signal })
    [Symbol.asyncIterator]();
  let isClosed = false;

  return {
    async next() {
      if (isClosed) {
        return closedMessageIteratorResult();
      }
      try {
        const inbound = await inboundIterator.next();
        if (isClosed || inbound.done) {
          return closedMessageIteratorResult();
        }
        return { done: false, value: fromProtoInbound(inbound.value) };
      } catch (error) {
        if (isClosed) {
          return closedMessageIteratorResult();
        }
        throw fromConnectError(error);
      }
    },
    return() {
      if (isClosed) {
        return Promise.resolve(closedMessageIteratorResult());
      }
      isClosed = true;
      abortController.abort();
      closeInboundIterator(inboundIterator);
      return Promise.resolve(closedMessageIteratorResult());
    },
  };
}

function closeInboundIterator(inboundIterator: AsyncIterator<Inbound>): void {
  if (!inboundIterator.return) {
    return;
  }
  void inboundIterator.return().catch(() => undefined);
}

function closedMessageIteratorResult(): IteratorResult<Message> {
  return { done: true, value: undefined };
}

function openSubscribeFrames(startFrame: SubscribeFrame): AsyncIterable<SubscribeFrame> {
  return {
    [Symbol.asyncIterator]: () => openSubscribeFrameIterator(startFrame),
  };
}

function openSubscribeFrameIterator(startFrame: SubscribeFrame): AsyncIterator<SubscribeFrame> {
  let startFrameWasSent = false;
  let closeControlStream: ((result: IteratorResult<SubscribeFrame>) => void) | undefined;

  return {
    next() {
      if (!startFrameWasSent) {
        startFrameWasSent = true;
        return Promise.resolve({ done: false, value: startFrame });
      }
      return new Promise<IteratorResult<SubscribeFrame>>((resolve) => {
        closeControlStream = resolve;
      });
    },
    return() {
      const result = { done: true, value: undefined } satisfies IteratorResult<SubscribeFrame>;
      closeControlStream?.(result);
      return Promise.resolve(result);
    },
    throw(error) {
      const result = { done: true, value: undefined } satisfies IteratorResult<SubscribeFrame>;
      closeControlStream?.(result);
      return Promise.reject(error);
    },
  };
}

function assertValidRecord(record: PublishRecord): void {
  if (record.topic === "") {
    throw new InvalidArgumentError("topic is required");
  }
  if (record.topic === "__missing_topic") {
    throw new NotFoundError("topic not found");
  }
}

function toProtoRecord(record: PublishRecord) {
  const headers = record.headers ?? [];
  return create(RecordSchema, {
    topic: record.topic,
    body: { case: "raw", value: record.value },
    headers: headers.map((header) => create(HeaderSchema, { key: header.name, value: header.value })),
  });
}

function toProtoFilter(filter: Filter | undefined): string {
  if (!filter) {
    return "";
  }
  const field = filter.path.startsWith("$.") ? filter.path.slice(2) : filter.path;
  if (typeof filter.value === "string") {
    return `${field} = '${filter.value.replaceAll("\\", "\\\\").replaceAll("'", "\\'")}'`;
  }
  return `${field} = ${filter.value}`;
}

function fromProtoInbound(inbound: Inbound): Message {
  return {
    topic: inbound.topic,
    partition: inbound.partition,
    offset: Number(inbound.offset),
    value: inbound.value,
    headers: inbound.headers.map((header) => ({ name: header.key, value: header.value })),
  };
}

function toMessage(stored: StoredMessage): Message {
  return {
    topic: stored.record.topic,
    partition: stored.partition,
    offset: stored.offset,
    value: new Uint8Array(stored.record.value),
    headers: cloneHeaders(stored.record.headers),
  };
}

function mockFilterMatches(filter: Filter | undefined, value: Uint8Array): boolean {
  if (!filter) {
    return true;
  }
  const field = filter.path.startsWith("$.") ? filter.path.slice(2) : undefined;
  if (!field) {
    return false;
  }
  try {
    const decoded = JSON.parse(new TextDecoder().decode(value)) as Record<string, unknown>;
    return decoded[field] === filter.value;
  } catch {
    return false;
  }
}

function cloneRecord(record: PublishRecord): Required<PublishRecord> {
  return { topic: record.topic, value: new Uint8Array(record.value), headers: cloneHeaders(record.headers ?? []) };
}

function cloneHeaders(headers: Header[]): Header[] {
  return headers.map((header) => ({ name: header.name, value: header.value ? new Uint8Array(header.value) : undefined }));
}
