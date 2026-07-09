import type { Client } from "@connectrpc/connect";
import { create } from "@bufbuild/protobuf";
import {
  Gateway,
  Header as ProtoHeader,
  QueueAckEntrySchema,
  QueueAckType,
  QueueAcquireRequestSchema,
  QueueAcknowledgeRequestSchema,
  QueueRenewRequestSchema,
} from "../gen/crabka/gateway/v1/gateway_pb.ts";
import { fromConnectError, InvalidArgumentError, TransportError } from "./errors.ts";
import type { Header, MockStore, StoredMessage } from "./messaging.ts";

const DEFAULT_QUEUE_LOCK_DURATION_MS = 30_000;
const QUEUE_MESSAGE_ID_PARTS = 3;
const QUEUE_MESSAGE_NOT_ACQUIRED = "queue message is not acquired";

type LiveGateway = Client<typeof Gateway>;

export type QueueAckTypeName = "accept" | "release" | "reject";
export type QueueAckEntry = { messageId: string; ackType: QueueAckTypeName };
export type QueueRenewEntry = { messageId: string };
export type QueueOperationError = { kind: "invalid_argument"; message: string };
export type QueueResult = { messageId: string; error: QueueOperationError | null };
export type QueueMessage = {
  messageId: string;
  topic: string;
  partition: number;
  offset: number;
  value: Uint8Array;
  headers: Header[];
  deliveryCount: number;
};
export type QueueAcquireResult = { sessionId: string; messages: QueueMessage[] };
export type QueueBatchResult = { results: QueueResult[] };

export type QueuesModule = {
  acquire(topic: string, options: { group?: string; max?: number; lockDurationMs?: number }): Promise<QueueAcquireResult>;
  acknowledge(sessionId: string, entries: QueueAckEntry[]): Promise<QueueBatchResult>;
  renew(sessionId: string, entries: QueueRenewEntry[]): Promise<QueueBatchResult>;
};

export function createQueuesModule(options: {
  endpoint: string;
  gateway?: LiveGateway;
  headers?: HeadersInit;
  mockStore?: MockStore;
}): QueuesModule {
  return {
    acquire: (topic, acquireOptions) => acquire(options, topic, acquireOptions),
    acknowledge: (sessionId, entries) => acknowledge(options, sessionId, entries),
    renew: (sessionId, entries) => renew(options, sessionId, entries),
  };
}

async function acquire(
  options: Parameters<typeof createQueuesModule>[0],
  topic: string,
  acquireOptions: { group?: string; max?: number; lockDurationMs?: number },
): Promise<QueueAcquireResult> {
  assertSupportedAcquireOptions(acquireOptions);
  if (options.endpoint.startsWith("unreachable://")) {
    throw new TransportError("endpoint unreachable");
  }
  if (options.mockStore) {
    return acquireMockMessages(options.mockStore, topic, acquireOptions.max ?? 1);
  }
  if (!options.gateway) {
    throw new TransportError("gateway transport is not configured");
  }

  try {
    const response = await options.gateway.queueAcquire(
      create(QueueAcquireRequestSchema, {
        groupId: acquireOptions.group,
        topics: [topic],
        maxMessages: acquireOptions.max ?? 1,
        waitMs: 0,
        lockDurationMs: BigInt(DEFAULT_QUEUE_LOCK_DURATION_MS),
      }),
      { headers: options.headers },
    );
    return {
      sessionId: response.sessionId,
      messages: response.messages.map((message) => ({
        messageId: messageIdFromParts(message.topic, message.partition, Number(message.offset)),
        topic: message.topic,
        partition: message.partition,
        offset: Number(message.offset),
        value: message.value,
        headers: fromProtoHeaders(message.headers),
        deliveryCount: message.deliveryCount,
      })),
    };
  } catch (error) {
    throw fromConnectError(error);
  }
}

async function acknowledge(
  options: Parameters<typeof createQueuesModule>[0],
  sessionId: string,
  entries: QueueAckEntry[],
): Promise<QueueBatchResult> {
  assertSessionId(sessionId);
  if (options.mockStore) {
    const store = options.mockStore;
    return { results: entries.map((entry) => acknowledgeMockMessage(store, entry)) };
  }
  if (!options.gateway) {
    throw new TransportError("gateway transport is not configured");
  }

  try {
    const response = await options.gateway.queueAcknowledge(
      create(QueueAcknowledgeRequestSchema, { sessionId, entries: entries.map(toProtoAckEntry) }),
      { headers: options.headers },
    );
    return { results: response.results.map((result, index) => fromProtoQueueResult(entries[index], result.error)) };
  } catch (error) {
    throw fromConnectError(error);
  }
}

async function renew(
  options: Parameters<typeof createQueuesModule>[0],
  sessionId: string,
  entries: QueueRenewEntry[],
): Promise<QueueBatchResult> {
  assertSessionId(sessionId);
  if (options.mockStore) {
    const store = options.mockStore;
    return { results: entries.map((entry) => renewMockMessage(store, entry)) };
  }
  if (!options.gateway) {
    throw new TransportError("gateway transport is not configured");
  }

  try {
    const response = await options.gateway.queueRenew(
      create(QueueRenewRequestSchema, {
        sessionId,
        entries: entries.map((entry) => toProtoAckEntry({ messageId: entry.messageId, ackType: "accept" })),
      }),
      { headers: options.headers },
    );
    return { results: response.results.map((result, index) => fromProtoQueueResult(entries[index], result.error)) };
  } catch (error) {
    throw fromConnectError(error);
  }
}

function assertSupportedAcquireOptions(options: { group?: string; lockDurationMs?: number }): void {
  if (!options.group) {
    throw new InvalidArgumentError("queue group is required");
  }
  if (options.lockDurationMs !== undefined && options.lockDurationMs !== DEFAULT_QUEUE_LOCK_DURATION_MS) {
    throw new InvalidArgumentError(
      "queue lock_duration_ms must be 30000; per-acquire lock durations are not supported",
    );
  }
}

function assertSessionId(sessionId: string): void {
  if (sessionId === "") {
    throw new InvalidArgumentError("queue session_id is required");
  }
}

function acquireMockMessages(store: MockStore, topic: string, maxMessages: number): QueueAcquireResult {
  const sessionId = `queue-session-${store.nextQueueSessionId}`;
  store.nextQueueSessionId += 1;
  const messages = store.messages
    .filter((message) => message.record.topic === topic && message.queueState === "available")
    .slice(0, maxMessages)
    .map((message) => acquireMockMessage(message));
  return { sessionId, messages };
}

function acquireMockMessage(message: StoredMessage): QueueMessage {
  message.queueState = "acquired";
  message.deliveryCount += 1;
  return {
    messageId: messageIdFromStoredMessage(message),
    topic: message.record.topic,
    partition: message.partition,
    offset: message.offset,
    value: new Uint8Array(message.record.value),
    headers: message.record.headers.map((header) => ({
      name: header.name,
      value: header.value ? new Uint8Array(header.value) : undefined,
    })),
    deliveryCount: message.deliveryCount,
  };
}

function acknowledgeMockMessage(store: MockStore, entry: QueueAckEntry): QueueResult {
  const message = findAcquiredMockMessage(store, entry.messageId);
  if (!message) {
    return queueEntryError(entry.messageId);
  }
  message.queueState = mockQueueStateForAck(entry.ackType);
  return { messageId: entry.messageId, error: null };
}

function renewMockMessage(store: MockStore, entry: QueueRenewEntry): QueueResult {
  if (!findAcquiredMockMessage(store, entry.messageId)) {
    return queueEntryError(entry.messageId);
  }
  return { messageId: entry.messageId, error: null };
}

function findAcquiredMockMessage(store: MockStore, messageId: string): StoredMessage | undefined {
  return store.messages.find(
    (message) => message.queueState === "acquired" && messageIdFromStoredMessage(message) === messageId,
  );
}

function mockQueueStateForAck(ackType: QueueAckTypeName): StoredMessage["queueState"] {
  if (ackType === "release") {
    return "available";
  }
  if (ackType === "reject") {
    return "rejected";
  }
  return "accepted";
}

function queueEntryError(messageId: string): QueueResult {
  return { messageId, error: { kind: "invalid_argument", message: QUEUE_MESSAGE_NOT_ACQUIRED } };
}

function toProtoAckEntry(entry: QueueAckEntry) {
  const messageId = parseMessageId(entry.messageId);
  return create(QueueAckEntrySchema, {
    topic: messageId.topic,
    partition: messageId.partition,
    offset: BigInt(messageId.offset),
    type: toProtoAckType(entry.ackType),
  });
}

function parseMessageId(messageId: string): { topic: string; partition: number; offset: number } {
  const parts = messageId.split(":");
  if (parts.length !== QUEUE_MESSAGE_ID_PARTS) {
    throw new InvalidArgumentError("queue message_id must be <topic>:<partition>:<offset>");
  }
  const [topic, partitionText, offsetText] = parts;
  const partition = Number(partitionText);
  const offset = Number(offsetText);
  if (!topic || !Number.isInteger(partition) || !Number.isInteger(offset)) {
    throw new InvalidArgumentError("queue message_id must be <topic>:<partition>:<offset>");
  }
  return { topic, partition, offset };
}

function toProtoAckType(ackType: QueueAckTypeName): QueueAckType {
  if (ackType === "release") {
    return QueueAckType.RELEASE;
  }
  if (ackType === "reject") {
    return QueueAckType.REJECT;
  }
  return QueueAckType.ACCEPT;
}

function fromProtoQueueResult(entry: { messageId: string } | undefined, error: { message: string } | undefined): QueueResult {
  const messageId = entry?.messageId ?? "";
  if (!error) {
    return { messageId, error: null };
  }
  return { messageId, error: { kind: "invalid_argument", message: QUEUE_MESSAGE_NOT_ACQUIRED } };
}

function fromProtoHeaders(headers: ProtoHeader[]): Header[] {
  return headers.map((header) => ({ name: header.key, value: header.value }));
}

function messageIdFromStoredMessage(message: StoredMessage): string {
  return messageIdFromParts(message.record.topic, message.partition, message.offset);
}

function messageIdFromParts(topic: string, partition: number, offset: number): string {
  return `${topic}:${partition}:${offset}`;
}
