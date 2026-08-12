#!/usr/bin/env node
import { createInterface } from "node:readline/promises";
import { stdin as input, stdout as output } from "node:process";
import { pathToFileURL } from "node:url";
import { createClient, CrabkaClient } from "./client.ts";
import { CrabkaError, ErrorKind, InvalidArgumentError, NotFoundError, ServerError, UnimplementedError } from "./errors.ts";
import type { Filter, Header, Message } from "./messaging.ts";
import type { QueueAckTypeName } from "./queues.ts";
import { SubscriptionReader } from "./subscription-reader.ts";

type HeaderWire = { name: string; value_b64: string | null };

const CONTRACT_MAJOR = 1;
const CONTRACT_MINOR = 1;
const GATEWAY_QUEUE_MESSAGE_NOT_ACQUIRED = "record is not acquired by this session";
const CONTRACT_QUEUE_MESSAGE_NOT_ACQUIRED = "queue message is not acquired";

type Command = {
  cmd: string;
  endpoint?: string;
  bearer?: string | null;
  topic?: string;
  value_b64?: string;
  headers?: HeaderWire[];
  event?: {
    id: string;
    source: string;
    type: string;
    specversion: string;
    datacontenttype?: string;
    data_b64: string;
  };
  topics?: string[];
  group?: string;
  filter?: Filter | null;
  timeout_ms?: number;
  message_id?: string;
  session_id?: string;
  max?: number;
  lock_duration_ms?: number;
  entries?: QueueCommandEntry[];
  name?: string;
  username?: string;
  password?: string;
  key?: string;
};

type QueueCommandEntry = { message_id: string; ack_type?: QueueAckTypeName };

class Adapter {
  private client: CrabkaClient = createClient({ endpoint: "mock://gateway" });
  private subscription?: SubscriptionReader;
  private nextQueueSessionId = 1;
  private queueSessionAliases = new Map<string, string>();

  async close(): Promise<void> {
    await this.closeCurrentSubscription();
  }

  async handle(command: Command): Promise<unknown> {
    switch (command.cmd) {
      case "hello":
        return { hello: { contract_major: CONTRACT_MAJOR, contract_minor: CONTRACT_MINOR, language: "ts" } };
      case "configure":
        return this.configure(command);
      case "publish":
        return this.publish(command);
      case "publish_event":
        return this.publishEvent(command);
      case "subscribe":
        return this.subscribe(command);
      case "next_message":
        return this.nextMessage(command);
      case "queue_acquire":
        return this.queueAcquire(command);
      case "queue_ack":
        return sdkError(new UnimplementedError("queues", "gateway-sharegroup-rpc"));
      case "queue_acknowledge":
        return this.queueAcknowledge(command);
      case "queue_renew":
        return this.queueRenew(command);
      case "db_connect":
        return errorResponse(this.client.database.connect(command.name ?? ""));
      case "auth_sign_in":
        return errorResponse(this.client.auth.signIn(command.username ?? "", command.password ?? ""));
      case "blob_put":
        return errorResponse(this.client.blob.put(command.key ?? "", decodeBase64(command.value_b64 ?? "")));
      case "blob_get":
        return errorResponse(this.client.blob.get(command.key ?? ""));
      default:
        return sdkError(new InvalidArgumentError("unknown command"));
    }
  }

  private async configure(command: Command): Promise<unknown> {
    if (!command.endpoint) {
      return sdkError(new InvalidArgumentError("endpoint is required"));
    }
    await this.closeCurrentSubscription();
    this.client = createClient({ endpoint: command.endpoint, bearerToken: command.bearer ?? undefined });
    this.nextQueueSessionId = 1;
    this.queueSessionAliases.clear();
    return ok({ bearer_configured: command.bearer !== null && command.bearer !== undefined });
  }

  private async publish(command: Command): Promise<unknown> {
    const result = await this.client.messaging.publish({
      topic: command.topic ?? "",
      value: decodeBase64(command.value_b64 ?? ""),
      headers: decodeHeaders(command.headers ?? []),
    });
    return ok({ ...result, offset: contractSafeOffset(result.offset) });
  }

  private async publishEvent(command: Command): Promise<unknown> {
    if (!command.event) {
      return sdkError(new InvalidArgumentError("event is required"));
    }
    const result = await this.client.messaging.publishEvent(command.topic ?? "", {
      id: command.event.id,
      source: command.event.source,
      type: command.event.type,
      specversion: command.event.specversion,
      datacontenttype: command.event.datacontenttype,
      data: decodeBase64(command.event.data_b64),
    });
    return ok({ ...result, offset: contractSafeOffset(result.offset) });
  }

  private async subscribe(command: Command): Promise<unknown> {
    await this.closeCurrentSubscription();
    const iterable = this.client.messaging.subscribe(command.topics ?? [], {
      group: command.group ?? "",
      filter: command.filter ?? undefined,
    });
    this.subscription = new SubscriptionReader(iterable[Symbol.asyncIterator]());
    return ok({});
  }

  private async queueAcquire(command: Command): Promise<unknown> {
    const result = await this.client.queues.acquire(command.topic ?? "", {
      group: command.group,
      max: command.max,
      lockDurationMs: command.lock_duration_ms,
      sessionId: this.actualQueueSessionId(command.session_id ?? "") || undefined,
    });
    const publicSessionId = this.rememberQueueSession(result.sessionId);
    return ok({
      session_id: publicSessionId,
      messages: result.messages.map((message) => ({
        message_id: message.messageId,
        topic: message.topic,
        partition: message.partition,
        offset: contractSafeOffset(message.offset),
        value_b64: message.value === undefined ? null : Buffer.from(message.value).toString("base64"),
        headers: encodeHeaders(message.headers),
        delivery_count: message.deliveryCount,
      })),
    });
  }

  private async queueAcknowledge(command: Command): Promise<unknown> {
    const result = await this.client.queues.acknowledge(
      this.actualQueueSessionId(command.session_id ?? ""),
      (command.entries ?? []).map((entry) => ({ messageId: entry.message_id, ackType: entry.ack_type ?? "accept" })),
    );
    return ok(encodeQueueBatchResult(result.results));
  }

  private async queueRenew(command: Command): Promise<unknown> {
    const result = await this.client.queues.renew(
      this.actualQueueSessionId(command.session_id ?? ""),
      (command.entries ?? []).map((entry) => ({ messageId: entry.message_id })),
    );
    return ok(encodeQueueBatchResult(result.results));
  }

  private rememberQueueSession(actualSessionId: string): string {
    for (const [publicSessionId, storedActualSessionId] of this.queueSessionAliases) {
      if (storedActualSessionId === actualSessionId) {
        return publicSessionId;
      }
    }
    const publicSessionId = `queue-session-${this.nextQueueSessionId}`;
    this.nextQueueSessionId += 1;
    this.queueSessionAliases.set(publicSessionId, actualSessionId);
    return publicSessionId;
  }

  private actualQueueSessionId(publicSessionId: string): string {
    return this.queueSessionAliases.get(publicSessionId) ?? publicSessionId;
  }

  private async closeCurrentSubscription(): Promise<void> {
    if (!this.subscription) {
      return;
    }
    const subscription = this.subscription;
    this.subscription = undefined;
    await subscription.close();
  }

  private async nextMessage(command: Command): Promise<unknown> {
    if (!this.subscription) {
      return sdkError(new InvalidArgumentError("subscribe before next_message"));
    }
    const timeoutMs = command.timeout_ms ?? 0;
    const message = await withTimeout(this.subscription.read(), timeoutMs);
    this.subscription.consume(message);
    if (message.done || !message.value) {
      return sdkError(new ServerError("subscription ended"));
    }
    return { message: encodeMessage(message.value) };
  }
}

async function main(): Promise<void> {
  const adapter = new Adapter();
  const lines = createInterface({ input });
  try {
    for await (const line of lines) {
      const command = JSON.parse(line) as Command;
      const response = await safeHandle(adapter, command);
      output.write(`${JSON.stringify(response)}\n`);
    }
  } finally {
    await adapter.close();
  }
}

async function safeHandle(adapter: Adapter, command: Command): Promise<unknown> {
  try {
    return await adapter.handle(command);
  } catch (error) {
    return sdkError(toCrabkaError(error));
  }
}

async function errorResponse(promise: Promise<unknown>): Promise<unknown> {
  try {
    await promise;
    return ok({});
  } catch (error) {
    return sdkError(toCrabkaError(error));
  }
}

function ok(value: unknown): unknown {
  return { ok: value };
}

function sdkError(error: CrabkaError): unknown {
  const body: Record<string, unknown> = { kind: error.kind };
  if (error instanceof UnimplementedError && error.module) {
    body.module = error.module;
  }
  if (error instanceof UnimplementedError && error.gatedOn) {
    body.gated_on = error.gatedOn;
  }
  if (!(error instanceof UnimplementedError) || (!error.module && error.message !== "unimplemented")) {
    body.message = error.message;
  }
  return { error: body };
}

function toCrabkaError(error: unknown): CrabkaError {
  if (error instanceof CrabkaError) {
    return error;
  }
  if (error instanceof Error) {
    return new ServerError(error.message);
  }
  return new ServerError(String(error));
}

async function withTimeout<T>(promise: Promise<T>, timeoutMs: number): Promise<T> {
  if (timeoutMs <= 0) {
    return promise;
  }
  return Promise.race([
    promise,
    new Promise<T>((_, reject) => setTimeout(() => reject({ kind: "not_found" satisfies ErrorKind }), timeoutMs)),
  ]).catch((error) => {
    if (error && typeof error === "object" && "kind" in error && error.kind === "not_found") {
      throw new NotFoundError("no message available");
    }
    throw error;
  });
}

function decodeBase64(value: string): Uint8Array {
  return Buffer.from(value, "base64");
}

function decodeHeaders(headers: HeaderWire[]): Header[] {
  return headers.map((header) => ({ name: header.name, value: header.value_b64 === null ? undefined : decodeBase64(header.value_b64) }));
}

function encodeHeaders(headers: Header[]): HeaderWire[] {
  return headers.map((header) => ({
    name: header.name,
    value_b64: header.value ? Buffer.from(header.value).toString("base64") : null,
  }));
}

function encodeQueueBatchResult(results: { messageId: string; error: { kind: string; message: string } | null }[]): unknown {
  return {
    results: results.map((result) => ({
      message_id: result.messageId,
      error: encodeQueueOperationError(result.error),
    })),
  };
}

export function encodeQueueOperationError(error: { kind: string; message: string } | null): unknown {
  if (!error) {
    return null;
  }
  return { kind: error.kind, message: queueOperationErrorMessageForContract(error.message) };
}

function queueOperationErrorMessageForContract(message: string): string {
  if (message === GATEWAY_QUEUE_MESSAGE_NOT_ACQUIRED) {
    return CONTRACT_QUEUE_MESSAGE_NOT_ACQUIRED;
  }
  return message;
}

function encodeMessage(message: Message): unknown {
  return {
    topic: message.topic,
    partition: message.partition,
    offset: contractSafeOffset(message.offset),
    value_b64: Buffer.from(message.value).toString("base64"),
    headers: encodeHeaders(message.headers),
  };
}

function contractSafeOffset(offset: bigint): number {
  const value = Number(offset);
  if (!Number.isSafeInteger(value) || BigInt(value) !== offset) {
    throw new RangeError(`conformance offset ${offset} is outside the JSON safe-integer range`);
  }
  return value;
}

if (isAdapterEntrypoint()) {
  await main();
}

function isAdapterEntrypoint(): boolean {
  if (!process.argv[1]) {
    return false;
  }
  if (import.meta.url === pathToFileURL(process.argv[1]).href) {
    return true;
  }
  return process.argv[1].endsWith("/bin/conformance-adapter");
}
