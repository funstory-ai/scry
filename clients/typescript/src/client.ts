import * as fs from "node:fs";
import * as grpc from "@grpc/grpc-js";
import type { ServiceError } from "@grpc/grpc-js";
import {
  AdminClient,
  EventsClient,
  FilesClient,
  MutationClient,
  NamespaceClient,
  SearchClient,
} from "./gen/scry/v1/workspace.js";
import type {
  GetFileChunk,
  PutFileRequest,
  SearchRequest,
  SearchResponse,
  SubscribeRequest,
} from "./gen/scry/v1/workspace.js";
import { NodeKind, SearchMode } from "./gen/scry/v1/workspace.js";

export type ScryClientOptions = {
  endpoint: string;
  token?: string;
  tls?: boolean;
  caCertPath?: string;
  clientCertPath?: string;
  clientKeyPath?: string;
};

function readPemOptional(path?: string): Buffer | undefined {
  if (!path) {
    return undefined;
  }
  return fs.readFileSync(path);
}

function channelCredentials(opts: ScryClientOptions): grpc.ChannelCredentials {
  if (!opts.tls) {
    return grpc.credentials.createInsecure();
  }
  const rootCerts = readPemOptional(opts.caCertPath);
  const privateKey = readPemOptional(opts.clientKeyPath);
  const certChain = readPemOptional(opts.clientCertPath);
  return grpc.credentials.createSsl(rootCerts, privateKey, certChain);
}

function bearerInterceptor(token: string): grpc.Interceptor {
  return (options, nextCall) =>
    new grpc.InterceptingCall(nextCall(options), {
      start: (metadata, listener, next) => {
        metadata.set("authorization", `Bearer ${token}`);
        next(metadata, listener);
      },
    });
}

function clientOptions(opts: ScryClientOptions): grpc.ClientOptions | undefined {
  if (!opts.token) {
    return undefined;
  }
  return { interceptors: [bearerInterceptor(opts.token)] };
}

function grpcHost(endpoint: string): string {
  return endpoint.replace(/^https?:\/\//, "");
}

async function readStreamChunks<T>(stream: grpc.ClientReadableStream<T>): Promise<T[]> {
  const out: T[] = [];
  return await new Promise((resolve, reject) => {
    stream.on("data", (msg: T) => out.push(msg));
    stream.on("error", (err) => reject(err));
    stream.on("end", () => resolve(out));
  });
}

export class WorkspaceHandle {
  constructor(
    private readonly workspaceId: string,
    private readonly namespace: NamespaceClient,
    private readonly mutation: MutationClient,
    private readonly files: FilesClient,
    private readonly searchClient: SearchClient,
    private readonly eventsClient: EventsClient,
  ) {}

  /** Ensure a directory exists under `parentPath` (posix, relative to workspace root). */
  async mkdirp(path: string, mode = 0o755): Promise<void> {
    const segments = path.split("/").filter(Boolean);
    let parent = "";
    for (const name of segments) {
      const parentId = await this.resolvePathToNodeId(parent);
      await new Promise<void>((resolve, reject) => {
        this.mutation.create(
          {
            workspaceId: this.workspaceId,
            parentNodeId: parentId,
            name,
            kind: NodeKind.NODE_KIND_DIR,
            mode,
            exclusive: false,
          },
          (err: ServiceError | null) => {
            if (!err) {
              resolve();
              return;
            }
            if (err.code === grpc.status.ALREADY_EXISTS) {
              resolve();
              return;
            }
            reject(err);
          },
        );
      });
      parent = parent ? `${parent}/${name}` : name;
    }
  }

  private async resolvePathToNodeId(path: string): Promise<string> {
    return await new Promise((resolve, reject) => {
      this.namespace.resolvePath({ workspaceId: this.workspaceId, path }, (err, resp) => {
        if (err || !resp) {
          reject(err ?? new Error("missing resolve response"));
          return;
        }
        if (!resp.exists || !resp.nodeId) {
          reject(new Error(`path does not exist: ${path || "<root>"}`));
          return;
        }
        resolve(resp.nodeId);
      });
    });
  }

  filesApi = {
    put: async (path: string, data: Buffer | Uint8Array): Promise<void> => {
      const segments = path.split("/").filter(Boolean);
      if (segments.length === 0) {
        throw new Error("path must be non-empty");
      }
      const name = segments.pop()!;
      const parentPath = segments.join("/");
      const parentNodeId = await this.resolvePathToNodeId(parentPath);
      const content = data instanceof Buffer ? new Uint8Array(data) : data;
      const req: PutFileRequest = {
        workspaceId: this.workspaceId,
        path: "",
        content,
        ifVersion: 0n,
        mode: 0o644,
        putFilePath: { parentNodeId, name },
      };
      await new Promise<void>((resolve, reject) => {
        this.files.putFile(req, (err: ServiceError | null) => {
          if (err) {
            reject(err);
          } else {
            resolve();
          }
        });
      });
    },
    get: async (path: string): Promise<Buffer> => {
      const stream = this.files.getFile({ workspaceId: this.workspaceId, path });
      const chunks = await readStreamChunks<GetFileChunk>(stream);
      return Buffer.concat(chunks.map((c) => Buffer.from(c.data)));
    },
  };

  search = async (
    query: string,
    opts?: { mode?: SearchMode; limit?: number },
  ): Promise<SearchResponse> => {
    const req: SearchRequest = {
      workspaceId: this.workspaceId,
      query,
      mode: opts?.mode ?? SearchMode.SEARCH_MODE_HYBRID,
      limit: opts?.limit ?? 20,
    };
    return await new Promise((resolve, reject) => {
      this.searchClient.search(req, (err: ServiceError | null, resp) => {
        if (err || !resp) {
          reject(err ?? new Error("empty search response"));
        } else {
          resolve(resp);
        }
      });
    });
  };

  events = {
    subscribe: (req: Partial<SubscribeRequest>) => {
      const full: SubscribeRequest = {
        workspaceId: this.workspaceId,
        sinceCursor: req.sinceCursor ?? "0",
        subscriberId: req.subscriberId ?? "ts-sdk",
        filter: req.filter,
      };
      return this.eventsClient.subscribe(full);
    },
  };
}

export class ScryClient {
  private readonly host: string;
  private readonly creds: grpc.ChannelCredentials;
  private readonly copts: grpc.ClientOptions | undefined;
  private readonly admin: AdminClient;
  private readonly namespace: NamespaceClient;
  private readonly mutation: MutationClient;
  private readonly files: FilesClient;
  private readonly search: SearchClient;
  private readonly events: EventsClient;

  constructor(private readonly opts: ScryClientOptions) {
    this.host = grpcHost(opts.endpoint);
    this.creds = channelCredentials(opts);
    this.copts = clientOptions(opts);
    this.admin = new AdminClient(this.host, this.creds, this.copts);
    this.namespace = new NamespaceClient(this.host, this.creds, this.copts);
    this.mutation = new MutationClient(this.host, this.creds, this.copts);
    this.files = new FilesClient(this.host, this.creds, this.copts);
    this.search = new SearchClient(this.host, this.creds, this.copts);
    this.events = new EventsClient(this.host, this.creds, this.copts);
  }

  workspace(workspaceId: string): WorkspaceHandle {
    return new WorkspaceHandle(
      workspaceId,
      this.namespace,
      this.mutation,
      this.files,
      this.search,
      this.events,
    );
  }

  async createWorkspace(name: string): Promise<{ workspaceId: string; rootNodeId: string }> {
    return await new Promise((resolve, reject) => {
      this.admin.createWorkspace(
        {
          name,
          config: { embeddingModel: "", embeddingDim: 32, maxFileSize: 0n },
        },
        (err: ServiceError | null, resp) => {
          if (err || !resp) {
            reject(err ?? new Error("empty create response"));
          } else {
            resolve({ workspaceId: resp.workspaceId, rootNodeId: resp.rootNodeId });
          }
        },
      );
    });
  }

  close(): void {
    this.admin.close();
    this.namespace.close();
    this.mutation.close();
    this.files.close();
    this.search.close();
    this.events.close();
  }
}

export function createScryClient(opts: ScryClientOptions): ScryClient {
  return new ScryClient(opts);
}
