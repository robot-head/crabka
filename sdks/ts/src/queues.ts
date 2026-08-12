import type { Client } from "@connectrpc/connect";
import { create } from "@bufbuild/protobuf";
import {
  type ErrorInfo,
  Gateway,
  Header as ProtoHeader,
  QueueAckEntrySchema,
  type QueueAckResult as ProtoQueueAckResult,
  QueueAckType,
  QueueAcquireRequestSchema,
  QueueAcknowledgeRequestSchema,
  QueueRenewRequestSchema,
} from "../gen/crabka/gateway/v1/gateway_pb.ts";
import { fromConnectError, fromRecordError, type ErrorKind, InvalidArgumentError, TransportError } from "./errors.ts";
import type { Header, MockStore, StoredMessage, StoredQueueDelivery } from "./messaging.ts";

const DEFAULT_QUEUE_LOCK_DURATION_MS = 30_000;
const QUEUE_MESSAGE_ID_PARTS = 3;
const QUEUE_MESSAGE_NOT_ACQUIRED = "queue message is not acquired";
const QUEUE_SESSION_EXPIRED = "queue session expired; re-acquire";

type LiveGateway = Client<typeof Gateway>;

export type QueueAckTypeName = "accept" | "release" | "reject";
export type QueueAckEntry = { messageId: string; ackType: QueueAckTypeName };
export type QueueRenewEntry = { messageId: string };
export type QueueOperationError = { kind: ErrorKind; message: string; retriable: boolean };
export type QueueResult = { messageId: string; error: QueueOperationError | null };
export type QueueMessage = {
  messageId: string;
  topic: string;
  partition: number;
  offset: bigint;
  value?: Uint8Array;
  headers: Header[];
  deliveryCount: number;
};
export type QueueAcquireResult = { sessionId: string; messages: QueueMessage[] };
export type QueueBatchResult = { results: QueueResult[] };

export type QueueAcquireOptions = { group?: string; max?: number; lockDurationMs?: number; sessionId?: string };
export type QueuesModule = {
  acquire(topic: string, options: QueueAcquireOptions): Promise<QueueAcquireResult>;
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
  acquireOptions: QueueAcquireOptions,
): Promise<QueueAcquireResult> {
  if (!topic) {
    throw new InvalidArgumentError("queue topic is required");
  }
  assertSupportedAcquireOptions(acquireOptions);
  if (options.endpoint.startsWith("unreachable://")) {
    throw new TransportError("endpoint unreachable");
  }
  if (options.mockStore) {
    return acquireMockMessages(
      options.mockStore,
      topic,
      acquireOptions.group ?? "",
      acquireOptions.max ?? 1,
      acquireOptions.sessionId,
    );
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
        sessionId: acquireOptions.sessionId,
        lockDurationMs: BigInt(DEFAULT_QUEUE_LOCK_DURATION_MS),
      }),
      { headers: options.headers },
    );
    return {
      sessionId: response.sessionId,
      messages: response.messages.map((message) => ({
        messageId: messageIdFromParts(message.topic, message.partition, message.offset),
        topic: message.topic,
        partition: message.partition,
        offset: message.offset,
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
  entries.forEach((entry) => assertAckType(entry.ackType));
  if (options.mockStore) {
    const store = options.mockStore;
    const group = assertMockSession(store, sessionId).group;
    return { results: entries.map((entry) => acknowledgeMockMessage(store, group, sessionId, entry)) };
  }
  if (!options.gateway) {
    throw new TransportError("gateway transport is not configured");
  }

  try {
    const response = await options.gateway.queueAcknowledge(
      create(QueueAcknowledgeRequestSchema, { sessionId, entries: entries.map(toProtoAckEntry) }),
      { headers: options.headers },
    );
    return { results: response.results.map(fromProtoQueueResult) };
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
    const group = assertMockSession(store, sessionId).group;
    return { results: entries.map((entry) => renewMockMessage(store, group, sessionId, entry)) };
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
    return { results: response.results.map(fromProtoQueueResult) };
  } catch (error) {
    throw fromConnectError(error);
  }
}

function assertSupportedAcquireOptions(options: QueueAcquireOptions): void {
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

function assertAckType(ackType: string): void {
  if (ackType !== "accept" && ackType !== "release" && ackType !== "reject") {
    throw new InvalidArgumentError("queue ack_type must be accept, release, or reject");
  }
}

function acquireMockMessages(
  store: MockStore,
  topic: string,
  group: string,
  maxMessages: number,
  sessionId?: string,
): QueueAcquireResult {
  const effectiveMax = Math.min(Math.max(maxMessages, 1), 500);
  if (!sessionId) {
    sessionId = `queue-session-${store.nextQueueSessionId}`;
    store.nextQueueSessionId += 1;
    store.queueSessions.set(sessionId, { topic, group, max: effectiveMax });
  } else {
    const session = assertMockSession(store, sessionId);
    if (session.topic !== topic || session.group !== group) {
      throw new InvalidArgumentError("group_id and topics are fixed when a queue session is created");
    }
    if (maxMessages !== 0 && effectiveMax !== session.max) {
      throw new InvalidArgumentError("max_messages is fixed when a queue session is created");
    }
  }
  const messages = store.messages
    .filter((message) => message.record.topic === topic && mockDelivery(message, group).state === "available")
    .slice(0, effectiveMax)
    .map((message) => acquireMockMessage(message, group, sessionId));
  return { sessionId, messages };
}

function acquireMockMessage(message: StoredMessage, group: string, sessionId: string): QueueMessage {
  const delivery = mockDelivery(message, group);
  delivery.state = "acquired";
  delivery.sessionId = sessionId;
  delivery.deliveryCount += 1;
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
    deliveryCount: delivery.deliveryCount,
  };
}

function acknowledgeMockMessage(store: MockStore, group: string, sessionId: string, entry: QueueAckEntry): QueueResult {
  const delivery = findAcquiredMockMessage(store, group, sessionId, entry.messageId);
  if (!delivery) {
    return queueEntryError(entry.messageId);
  }
  delivery.state = mockQueueStateForAck(entry.ackType);
  delete delivery.sessionId;
  return { messageId: entry.messageId, error: null };
}

function renewMockMessage(store: MockStore, group: string, sessionId: string, entry: QueueRenewEntry): QueueResult {
  if (!findAcquiredMockMessage(store, group, sessionId, entry.messageId)) {
    return queueEntryError(entry.messageId);
  }
  return { messageId: entry.messageId, error: null };
}

function findAcquiredMockMessage(
  store: MockStore,
  group: string,
  sessionId: string,
  messageId: string,
): StoredQueueDelivery | undefined {
  const message = store.messages.find((candidate) => messageIdFromStoredMessage(candidate) === messageId);
  const delivery = message?.queueDeliveries.get(group);
  return delivery?.state === "acquired" && delivery.sessionId === sessionId ? delivery : undefined;
}

function mockDelivery(message: StoredMessage, group: string): StoredQueueDelivery {
  let delivery = message.queueDeliveries.get(group);
  if (!delivery) {
    delivery = { state: "available", deliveryCount: 0 };
    message.queueDeliveries.set(group, delivery);
  }
  return delivery;
}

function assertMockSession(store: MockStore, sessionId: string): { topic: string; group: string; max: number } {
  const session = store.queueSessions.get(sessionId);
  if (!session) {
    throw new InvalidArgumentError(QUEUE_SESSION_EXPIRED);
  }
  return session;
}

function mockQueueStateForAck(ackType: QueueAckTypeName): StoredQueueDelivery["state"] {
  if (ackType === "release") {
    return "available";
  }
  if (ackType === "reject") {
    return "rejected";
  }
  return "accepted";
}

function queueEntryError(messageId: string): QueueResult {
  return { messageId, error: { kind: "invalid_argument", message: QUEUE_MESSAGE_NOT_ACQUIRED, retriable: false } };
}

function toProtoAckEntry(entry: QueueAckEntry) {
  const messageId = parseMessageId(entry.messageId);
  return create(QueueAckEntrySchema, {
    topic: messageId.topic,
    partition: messageId.partition,
    offset: messageId.offset,
    type: toProtoAckType(entry.ackType),
  });
}

function parseMessageId(messageId: string): { topic: string; partition: number; offset: bigint } {
  const parts = messageId.split(":");
  if (parts.length !== QUEUE_MESSAGE_ID_PARTS) {
    throw new InvalidArgumentError("queue message_id must be <topic>:<partition>:<offset>");
  }
  const [topic, partitionText, offsetText] = parts;
  if (topic === undefined || partitionText === undefined || offsetText === undefined) {
    throw new InvalidArgumentError("queue message_id must be <topic>:<partition>:<offset>");
  }
  if (!/^-?\d+$/.test(partitionText) || !/^-?\d+$/.test(offsetText)) {
    throw new InvalidArgumentError("queue message_id must be <topic>:<partition>:<offset>");
  }
  const partition = Number(partitionText);
  let offset: bigint;
  try {
    offset = BigInt(offsetText);
  } catch {
    throw new InvalidArgumentError("queue message_id must be <topic>:<partition>:<offset>");
  }
  if (!topic || !Number.isInteger(partition) || partition < -2_147_483_648 || partition > 2_147_483_647) {
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

function fromProtoQueueResult(result: ProtoQueueAckResult): QueueResult {
  if (!result.entry) {
    throw new TransportError("queue response result did not include an entry");
  }
  const messageId = messageIdFromParts(result.entry.topic, result.entry.partition, result.entry.offset);
  if (!result.error) {
    return { messageId, error: null };
  }
  return { messageId, error: queueOperationErrorFromProto(result.error) };
}

function queueOperationErrorFromProto(error: ErrorInfo): QueueOperationError {
  const mappedError = fromRecordError(error.code, error.message, error.retriable);
  return { kind: mappedError.kind, message: mappedError.message, retriable: error.retriable };
}

function fromProtoHeaders(headers: ProtoHeader[]): Header[] {
  return headers.map((header) => ({ name: header.key, value: header.value }));
}

function messageIdFromStoredMessage(message: StoredMessage): string {
  return messageIdFromParts(message.record.topic, message.partition, message.offset);
}

function messageIdFromParts(topic: string, partition: number, offset: bigint): string {
  return `${topic}:${partition}:${offset}`;
}
