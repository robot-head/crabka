import { describe, expect, test } from "vitest";
import { encodeQueueOperationError } from "./conformance-adapter.ts";

describe("conformance adapter", () => {
  test("normalizes gateway queue not-acquired wording in batch operation output", () => {
    expect(
      encodeQueueOperationError({
        kind: "invalid_argument",
        message: "record is not acquired by this session",
      }),
    ).toEqual({ kind: "invalid_argument", message: "queue message is not acquired" });
  });

  test("preserves other queue operation error messages", () => {
    expect(encodeQueueOperationError({ kind: "server_error", message: "commit failed" })).toEqual({
      kind: "server_error",
      message: "commit failed",
    });
  });
});
