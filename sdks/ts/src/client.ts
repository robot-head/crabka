import { createClient as createConnectClient } from "@connectrpc/connect";
import { createConnectTransport } from "@connectrpc/connect-node";
import { Gateway } from "../gen/crabka/gateway/v1/gateway_pb.ts";
import { createMockStore, createMessagingModule, MessagingModule } from "./messaging.ts";
import { createQueuesModule, QueuesModule } from "./queues.ts";
import {
  AuthModule,
  BlobModule,
  createAuthModule,
  createBlobModule,
  createDatabaseModule,
  DatabaseModule,
} from "./stubs.ts";

export type ClientOptions = {
  endpoint: string;
  bearerToken?: string;
};

export type CrabkaClient = {
  messaging: MessagingModule;
  queues: QueuesModule;
  database: DatabaseModule;
  auth: AuthModule;
  blob: BlobModule;
};

export function createClient(options: ClientOptions): CrabkaClient {
  const headers = authHeaders(options.bearerToken);
  const mockStore = isMockEndpoint(options.endpoint) ? createMockStore() : undefined;
  const gateway = mockStore ? undefined : createLiveGateway(options.endpoint);

  return {
    messaging: createMessagingModule({ endpoint: options.endpoint, gateway, headers, mockStore }),
    queues: createQueuesModule({ endpoint: options.endpoint, gateway, headers, mockStore }),
    database: createDatabaseModule(),
    auth: createAuthModule(options.bearerToken),
    blob: createBlobModule(),
  };
}

function isMockEndpoint(endpoint: string): boolean {
  return endpoint.startsWith("mock://") || endpoint.startsWith("unreachable://");
}

function createLiveGateway(endpoint: string) {
  const transport = createConnectTransport({
    baseUrl: endpoint,
    httpVersion: "2",
  });
  return createConnectClient(Gateway, transport);
}

function authHeaders(bearerToken: string | undefined): HeadersInit | undefined {
  if (!bearerToken) {
    return undefined;
  }
  return { Authorization: `Bearer ${bearerToken}` };
}
