import type { Message } from "./messaging.ts";

export class SubscriptionReader {
  private pendingRead?: Promise<IteratorResult<Message>>;
  private bufferedRead?: IteratorResult<Message>;
  private isClosed = false;

  constructor(private readonly subscription: AsyncIterator<Message>) {}

  read(): Promise<IteratorResult<Message>> {
    if (this.isClosed) {
      return Promise.resolve(closedIteratorResult());
    }
    if (this.bufferedRead) {
      return Promise.resolve(this.bufferedRead);
    }
    if (this.pendingRead) {
      return this.pendingRead;
    }

    const pendingRead = this.subscription.next().then(
      (message) => this.bufferMessage(pendingRead, message),
      (error: unknown) => {
        this.clearPendingRead(pendingRead);
        if (this.isClosed) {
          return closedIteratorResult();
        }
        throw error;
      },
    );
    this.pendingRead = pendingRead;
    return pendingRead;
  }

  consume(message: IteratorResult<Message>): void {
    if (this.bufferedRead !== message) {
      return;
    }
    this.bufferedRead = undefined;
  }

  close(): Promise<void> {
    if (this.isClosed) {
      return Promise.resolve();
    }
    this.isClosed = true;
    this.pendingRead = undefined;
    this.bufferedRead = undefined;
    this.cancelSubscription();
    return Promise.resolve();
  }

  private cancelSubscription(): void {
    if (!this.subscription.return) {
      return;
    }
    void this.subscription.return().catch(() => undefined);
  }

  private bufferMessage(
    pendingRead: Promise<IteratorResult<Message>>,
    message: IteratorResult<Message>,
  ): IteratorResult<Message> {
    if (this.isClosed) {
      return closedIteratorResult();
    }
    if (this.pendingRead !== pendingRead) {
      return message;
    }
    this.pendingRead = undefined;
    this.bufferedRead = message;
    return message;
  }

  private clearPendingRead(pendingRead: Promise<IteratorResult<Message>>): void {
    if (this.pendingRead !== pendingRead) {
      return;
    }
    this.pendingRead = undefined;
  }
}

function closedIteratorResult(): IteratorResult<Message> {
  return { done: true, value: undefined };
}
