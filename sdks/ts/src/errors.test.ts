import { Code, ConnectError } from "@connectrpc/connect";
import { describe, expect, test } from "vitest";
import { createClient } from "./client.ts";
import { fromConnectError, NotFoundError, TransportError } from "./errors.ts";

describe("error taxonomy and stubs", () => {
  test("connect codes map to SDK taxonomy", () => {
    expect(fromConnectError(new ConnectError("missing", Code.NotFound))).toBeInstanceOf(NotFoundError);
    expect(fromConnectError(new ConnectError("down", Code.Unavailable))).toBeInstanceOf(TransportError);
  });

  test("stub errors carry pinned slugs", async () => {
    const client = createClient({ endpoint: "mock://gateway" });
    await expect(client.database.connect("db")).rejects.toMatchObject({
      module: "database",
      gatedOn: "chapter-f-control-plane",
    });
    await expect(client.blob.put("key", new Uint8Array())).rejects.toMatchObject({
      module: "blob",
      gatedOn: "chapter-b-blob-api",
    });
  });

  test("auth sign in remains unauthenticated", async () => {
    const client = createClient({ endpoint: "mock://gateway", bearerToken: "token" });
    expect(client.auth.bearerToken).toBe("token");
    await expect(client.auth.signIn("u", "p")).rejects.toMatchObject({ kind: "unauthenticated" });
  });
});
