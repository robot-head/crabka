import { UnauthenticatedError, UnimplementedError } from "./errors.ts";

export const QUEUES_GATE = "gateway-sharegroup-rpc";
export const DATABASE_GATE = "chapter-f-control-plane";
export const BLOB_GATE = "chapter-b-blob-api";

export type DatabaseModule = {
  connect(name: string): Promise<never>;
};

export type AuthModule = {
  bearerToken?: string;
  signIn(username: string, password: string): Promise<never>;
};

export type BlobModule = {
  put(key: string, value: Uint8Array): Promise<never>;
  get(key: string): Promise<never>;
};

export function createDatabaseModule(): DatabaseModule {
  return {
    async connect() {
      throw new UnimplementedError("database", DATABASE_GATE);
    },
  };
}

export function createAuthModule(bearerToken?: string): AuthModule {
  return {
    bearerToken,
    async signIn() {
      throw new UnauthenticatedError("identity APIs are not part of contract v1");
    },
  };
}

export function createBlobModule(): BlobModule {
  return {
    async put() {
      throw new UnimplementedError("blob", BLOB_GATE);
    },
    async get() {
      throw new UnimplementedError("blob", BLOB_GATE);
    },
  };
}
