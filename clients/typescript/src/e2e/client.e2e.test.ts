import { spawn } from "node:child_process";
import * as fs from "node:fs";
import * as net from "node:net";
import * as path from "node:path";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createScryClient } from "../client.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(__dirname, "../../../..");

function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const s = net.createServer();
    s.listen(0, "127.0.0.1", () => {
      const a = s.address();
      s.close(() => {
        if (a && typeof a === "object") {
          resolve(a.port);
        } else {
          reject(new Error("no port"));
        }
      });
    });
    s.on("error", reject);
  });
}

function run(cmd: string, args: string[], env: NodeJS.ProcessEnv): Promise<string> {
  return new Promise((resolve, reject) => {
    const child = spawn(cmd, args, { env: { ...process.env, ...env }, encoding: "utf8" });
    let out = "";
    let err = "";
    child.stdout?.on("data", (d) => (out += d));
    child.stderr?.on("data", (d) => (err += d));
    child.on("error", reject);
    child.on("close", (code) => {
      if (code === 0) {
        resolve(out.trim());
      } else {
        reject(new Error(`${cmd} ${args.join(" ")} failed (${code}): ${err}`));
      }
    });
  });
}

describe("createScryClient e2e", () => {
  let port: number;
  let tmp: string;
  let scryd: ReturnType<typeof spawn> | undefined;
  let secret: string;
  let adminToken: string;
  let workspaceId: string;
  let workspaceToken: string;

  beforeAll(async () => {
    port = await freePort();
    tmp = fs.mkdtempSync(path.join(process.env.TMPDIR || "/tmp", "scry-ts-e2e-"));
    const content = path.join(tmp, "content");
    const indexRoot = path.join(tmp, "index");
    fs.mkdirSync(content, { recursive: true });
    fs.mkdirSync(indexRoot, { recursive: true });
    secret = `ts-sdk-e2e-secret-${Math.random().toString(16).slice(2)}`;
    const scrydBin = path.join(repoRoot, "target", "debug", "scryd");
    if (!fs.existsSync(scrydBin)) {
      throw new Error(`missing ${scrydBin}; run cargo build -p scryd`);
    }
    scryd = spawn(
      scrydBin,
      [],
      {
        env: {
          ...process.env,
          SCRYD_DEV_MODE: "1",
          SCRYD_AUTH_SECRET: secret,
          SCRYD_ALLOW_LOCAL_NO_AUTH: "1",
          SCRYD_CONTENT_ROOT: content,
          SCRYD_INDEX_ROOT: indexRoot,
          SCRYD_GRPC_ADDR: `127.0.0.1:${port}`,
          SCRYD_EMBED_PROVIDER: "mock",
          SCRYD_CATALOG_BACKEND: "sqlite",
        },
        stdio: "ignore",
      },
    );

    const addr = `127.0.0.1:${port}`;
    for (let i = 0; i < 100; i++) {
      try {
        await new Promise<void>((resolve, reject) => {
          const c = net.createConnection({ host: "127.0.0.1", port }, () => {
            c.end();
            resolve();
          });
          c.on("error", () => reject(new Error("retry")));
        });
        break;
      } catch {
        await new Promise((r) => setTimeout(r, 50));
        if (i === 99) {
          throw new Error(`scryd did not open ${addr}`);
        }
      }
    }

    adminToken = await run(
      scrydBin,
      [
        "token",
        "mint",
        "--kind",
        "admin",
        "--scope",
        "admin.workspace.create admin.workspace.list",
        "--ttl-secs",
        "3600",
        "--secret",
        secret,
      ],
      { SCRYD_AUTH_SECRET: secret },
    );

    const admin = createScryClient({ endpoint: `http://${addr}`, token: adminToken });
    const created = await admin.createWorkspace("ts-sdk-e2e");
    workspaceId = created.workspaceId;
    admin.close();

    workspaceToken = await run(
      scrydBin,
      [
        "token",
        "mint",
        "--kind",
        "workspace",
        "--workspace-id",
        workspaceId,
        "--scope",
        "workspace.access",
        "--ttl-secs",
        "3600",
        "--secret",
        secret,
      ],
      { SCRYD_AUTH_SECRET: secret },
    );
  });

  afterAll(async () => {
    if (scryd) {
      scryd.kill("SIGTERM");
      await new Promise((r) => setTimeout(r, 200));
      scryd.kill("SIGKILL");
    }
    fs.rmSync(tmp, { recursive: true, force: true });
  });

  it("puts a file, reads it back, and search returns a hit", async () => {
    const addr = `127.0.0.1:${port}`;
    const client = createScryClient({ endpoint: `http://${addr}`, token: workspaceToken });
    const ws = client.workspace(workspaceId);
    await ws.mkdirp("docs");
    const phrase = `ts-sdk-e2e-phrase-${Date.now()}`;
    await ws.filesApi.put("docs/plan.md", Buffer.from(`${phrase}\n`, "utf8"));
    const bytes = await ws.filesApi.get("docs/plan.md");
    expect(bytes.toString("utf8")).toContain(phrase);

    let hits = 0;
    for (let i = 0; i < 60; i++) {
      const resp = await ws.search(phrase, { limit: 10 });
      hits = resp.hits.filter((h) => h.path.endsWith("docs/plan.md")).length;
      if (hits > 0) {
        break;
      }
      await new Promise((r) => setTimeout(r, 100));
    }
    expect(hits).toBeGreaterThan(0);
    client.close();
  });
});
