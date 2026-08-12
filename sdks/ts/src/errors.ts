import { Code, ConnectError } from "@connectrpc/connect";

export type ErrorKind =
  | "transport"
  | "unauthenticated"
  | "invalid_argument"
  | "not_found"
  | "server_error"
  | "unimplemented";

export class CrabkaError extends Error {
  constructor(
    public readonly kind: ErrorKind,
    message: string,
  ) {
    super(message);
    this.name = new.target.name;
  }
}

export class TransportError extends CrabkaError {
  constructor(message = "transport error") {
    super("transport", message);
  }
}

export class UnauthenticatedError extends CrabkaError {
  constructor(message = "unauthenticated") {
    super("unauthenticated", message);
  }
}

export class InvalidArgumentError extends CrabkaError {
  constructor(message = "invalid argument") {
    super("invalid_argument", message);
  }
}

export class NotFoundError extends CrabkaError {
  constructor(message = "not found") {
    super("not_found", message);
  }
}

export class ServerError extends CrabkaError {
  constructor(message = "server error") {
    super("server_error", message);
  }
}

export class UnimplementedError extends CrabkaError {
  constructor(
    public readonly module?: string,
    public readonly gatedOn?: string,
    message = gatedOn && module ? `${module} is gated on ${gatedOn}` : "unimplemented",
  ) {
    super("unimplemented", message);
  }
}

export function fromConnectError(error: unknown): CrabkaError {
  if (error instanceof CrabkaError) {
    return error;
  }

  const connectError = ConnectError.from(error);
  const message = connectError.rawMessage;
  const code = connectError.code;
  switch (code) {
    case Code.PermissionDenied:
    case Code.Unauthenticated:
      return new UnauthenticatedError(message);
    case Code.InvalidArgument:
    case Code.FailedPrecondition:
    case Code.OutOfRange:
      return new InvalidArgumentError(message);
    case Code.NotFound:
      return new NotFoundError(message);
    case Code.Unimplemented:
      return new UnimplementedError(undefined, undefined, message);
    case Code.Unavailable:
    case Code.DeadlineExceeded:
    case Code.Canceled:
      return new TransportError(message);
    default:
      return new ServerError(message);
  }
}

export function fromRecordError(code: number, message: string, retriable = false): CrabkaError {
  if (retriable) {
    return new TransportError(message);
  }
  switch (code) {
    case 3:
    case 9:
    case 11:
      return new InvalidArgumentError(message);
    case 5:
      return new NotFoundError(message);
    case 7:
    case 16:
      return new UnauthenticatedError(message);
    case 12:
      return new UnimplementedError(undefined, undefined, message);
    case 13:
      return new ServerError(message);
    case 14:
      return new TransportError(message);
    default:
      return new ServerError(message);
  }
}
