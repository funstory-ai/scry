# AGENTS — Scry 仓库的协作指南

本文件面向 **AI 编码 agent** 与 **新加入的人类 contributor**：在动手改 Scry 之前需要知道的最少信息，以及"哪些约定不能踩"。

> 文档分工：
>
> - 「这个仓库现在长什么样、未来要做什么」→ [`docs/spec/architecture.md`](docs/spec/architecture.md)（living spec）
> - 「怎么用 / 怎么跑 / 部署须知」→ [`README.md`](README.md)
> - 「怎么在 staging 验证可以上线」→ [`docs/hardening.md`](docs/hardening.md)
> - 「**Agent 在仓库里怎么干活、写什么、不写什么**」→ 本文件

---

## 1. 仓库速览

Scry 是 **agent-native workspace 基础设施**：用户看到的是一个 FUSE 目录，系统看到的是一套 gRPC 契约。所有 client（FUSE / TypeScript SDK / 未来的 MCP / Python / `.scry/rg`）都是 `proto/scry/v1/workspace.proto` 的薄壳。

```
proto/scry/v1/workspace.proto    单一权威 proto
crates/
  scry-proto/        tonic 生成代码
  scry-storage/      ContentStore（LocalFs / JuiceFs）
  scry-index/        SQLite + FTS5 + sqlite-vec + chunking
  scry-fuse/         FUSE 客户端（feature `fuse`）
  scryd/             gRPC 服务端主二进制
clients/typescript/  @scry/sdk 预览
config/              scryd YAML 示例
ops/                 backup / recovery / drill 脚本
docs/
  spec/architecture.md   living spec
  hardening.md           staging 验收清单
  runbooks/              备份恢复 / secret / TLS
  ops/                   install / alerts / Prometheus rules
  archive/               历史快照（**禁止修改**）
```

**入口提示**：`scryd` 主二进制 = `crates/scryd/src/main.rs`；CLI 在 `crates/scryd/src/cli.rs`；server wiring 在 `crates/scryd/src/server/`。`scry-index` 不依赖 tokio，async 留在 `scryd` 侧。

---

## 2. 不可妥协的设计原则

来自 spec §1，任何 PR 都要遵守：

1. **零迁移**：不承诺旁路写入捕获；用户工具链不变。FUSE 与 RPC 是唯一两条进入路径。
2. **稳定引用**：`stable_id == node_id`（UUIDv7）。rename / 内容大改不能让旧 `node_id` 失效。
3. **接口一致性**：`proto/scry/v1/workspace.proto` 是唯一 API 定义。FUSE / SDK / CLI 都是 client，**不要**给 client 加上 proto 没有的语义。
4. **metadata 与 content 物理分离**：`index.db`（SQLite）必须本地 SSD/NVMe；content 可在 JuiceFS 等共享 FS 上。任何会让 SQLite 落到远程 FS 的改动需要在 PR 描述里专门说明并征求 maintainer 同意。

---

## 3. 启动一个改动前的 checklist

1. 读 [`docs/spec/architecture.md`](docs/spec/architecture.md) 对应章节，确认你要改的东西在 spec 里是怎么描述的。
2. 如果改动会影响 RPC 语义、存储布局、auth 模型、客户端清单中的任意一项，**必须同 PR 更新 spec**（不要堆叠"补丁文档"）。
3. 如果改动跨多个 PR、需要 phase gate，才在 `docs/plan/N-xxx.md` 起一份新计划；日常 forward-looking 工作直接进 spec §11。
4. 如果是一次安全 / 合规审计，新文件以 `docs/audit/YYYY-MM-DD.md` 命名，结论被 spec 吸收后整体移到 `docs/archive/audit/`。

---

## 4. 必须在本地跑通的检查（与 CI 对齐）

```bash
cargo check --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all -- --check
```

工作流定义：[`.github/workflows/ci.yml`](.github/workflows/ci.yml)。

附加场景：

| 场景 | 命令 |
|------|------|
| 只跑 `scryd` 的测试 | `cargo test -p scryd` |
| FUSE 客户端静态检查（不依赖宿主 libfuse3） | `cargo check -p scry-fuse` |
| FUSE 真实挂载编译 | `cargo check -p scry-fuse --features fuse` |
| TypeScript SDK 构建 + 单测 | `cd clients/typescript && npm ci && npm test` |
| 行覆盖率（与 CI 阈值一致） | `cargo llvm-cov --package scryd --fail-under-lines 50` |
| 端到端 staging 验证 | 跟着 [`docs/hardening.md`](docs/hardening.md) 一路跑 |

> 在 cloud agent / sandbox 环境里，如果一开始缺工具（`grpcurl` / `sqlite3` / `python3` / Rust toolchain）请先安装好；不要为了跳过依赖而跳过验证。

---

## 5. CLI 速查

`scryd` 的 CLI 由 clap 驱动（`crates/scryd/src/cli.rs`）。常用子命令：

```bash
# 启动服务（默认）
cargo run -p scryd
cargo run -p scryd -- serve

# 健康检查
cargo run -p scryd -- ping

# Token（HMAC-SHA256，三类）
cargo run -p scryd -- token mint --kind admin    --scope '*'                  --ttl-secs 3600
cargo run -p scryd -- token mint --kind workspace --workspace-id <ws-id> --scope 'workspace.access' --ttl-secs 3600
cargo run -p scryd -- token mint --kind local    --scope '*'                  --ttl-secs 3600

# Admin（远端调，需要 --token 或 SCRYD_ADMIN_TOKEN）
cargo run -p scryd -- admin create-workspace  --name foo
cargo run -p scryd -- admin list-workspaces   --limit 10
cargo run -p scryd -- admin stats             --id <ws-id>
cargo run -p scryd -- admin reindex           --id <ws-id>
cargo run -p scryd -- admin delete-workspace  --id <ws-id>

# Catalog 一致性
cargo run -p scryd -- catalog reconcile          # dry-run（默认）
cargo run -p scryd -- catalog reconcile --apply
```

> 旧别名 `mint-token` / `reconcile-catalog` 仍隐藏可用，新代码请使用 `token mint` / `catalog reconcile`。

`--config ./path/to.yaml` 是全局参数，可以放在任何子命令前。`SCRYD_CONFIG_PATH` 等价。**优先级**：命令行 / 环境变量 > YAML 文件 > 内置默认值。

---

## 6. 配置与环境变量

YAML schema 见 [`config/scryd.example.yaml`](config/scryd.example.yaml)；以下是 agent 经常需要在 PR 里改 / 用的 env 一览：

| 变量 | 含义 | 默认 |
|------|------|------|
| `SCRYD_CONFIG_PATH` | YAML 配置路径 | — |
| `SCRYD_GRPC_ADDR` | TCP 监听 | `127.0.0.1:50051` |
| `SCRYD_GRPC_UNIX_SOCKET` | Unix socket 路径（mode `0600`） | — |
| `SCRYD_CONTENT_ROOT` | content 根目录 | `/tmp/scry/content` |
| `SCRYD_INDEX_ROOT` | per-workspace 布局的根目录（必填，唯一支持的布局） | — |
| `SCRYD_CATALOG_BACKEND` | `sqlite` \| `postgres` | `sqlite` |
| `SCRYD_CATALOG_DB_PATH` | sqlite catalog 路径（仅 `sqlite` backend） | `{SCRYD_INDEX_ROOT}/_catalog.db` |
| `SCRYD_CATALOG_POSTGRES_URL` | postgres catalog 连接串（`postgres` backend 必填） | — |
| `SCRYD_HANDLE_IDLE_TIMEOUT_MS` | IO handle 空闲回收阈值 | `300000` |
| `SCRYD_IO_HANDLE_MEMORY_LIMIT` | 单 handle 内存缓冲上限，超过溢出 tempfile | `32 MiB` |
| `SCRYD_IO_STREAM_CHUNK_BYTES` | `Files.GetFile` 流帧窗口 | `1 MiB` |
| `SCRYD_INDEX_STREAM_CHUNK_BYTES` | flush 后流式 blake3 / utf8 校验窗口 | — |
| `SCRYD_INDEX_SKIP_CHUNKING_OVER_BYTES` | 超过此值的 UTF-8 文件走 headline-only | `64 MiB` |
| `SCRYD_MAX_DECODING_MESSAGE_BYTES` | 单帧 gRPC 解码上限（影响 `Files.PutFile` 安全阈值） | `16 MiB` |
| `SCRYD_AUTH_SECRET` | HMAC 主密钥（默认值是 insecure 占位，必须替换） | — |
| `SCRYD_ALLOW_LOCAL_NO_AUTH` | 仅 loopback 允许免 token | `false` |
| `SCRYD_DEV_MODE` | 跳过部分启动安全断言（仅本地） | `false` |
| `SCRYD_METRICS_ADDR` | `/metrics` HTTP 监听 | — |
| `SCRYD_METRICS_ALLOW_PUBLIC` | 允许 metrics 绑非 loopback | `false` |
| `SCRYD_EMBED_PROVIDER` | `mock` \| `openai-compatible` | `mock` |
| `SCRYD_EMBED_BASE_URL` / `_API_KEY` / `_MODEL` / `_TIMEOUT_MS` | openai-compatible 端点参数 | — |
| `SCRYD_EMBED_DOCUMENT_TASK` / `_QUERY_TASK` / `_NORMALIZED` | Jina 等扩展字段 | — |
| `SCRYD_EMBED_BATCH_SIZE` / `_BATCH_MAX_BYTES` | 单批 embedding 行数 / 字节上限 | `64` / `256 KiB` |

> 任何新增的 `SCRYD_*` 都必须同 PR 更新本表 + spec §6/§7/§9 + `docs/hardening.md` 的相关章节。

---

## 7. 写代码时的硬约束

1. **gRPC 契约是单一真相**：proto 字段编号 / 字段名变更视为破坏性改动。新增字段优先用未占用 tag。
2. **`scryd/src/main.rs` 控制在 1000 LoC 以内**：wiring 之外的逻辑放 `crates/scryd/src/server/`。
3. **`scry-index` 不引入 tokio**：保持纯同步、无 runtime 依赖；async 留在 `scryd`。
4. **写路径必须经过事务**：`Flush` / `Mutation.*` / index pipeline 的 SQLite 写都走 single writer 连接（`StoreConnections::writer_connection`）+ 单事务 commit；reader 走连接池。
5. **大文件路径**：`>= SCRYD_MAX_DECODING_MESSAGE_BYTES - 1 MiB` 的写必须走 `Io.Open + Io.Write + Io.Flush`，不要让 client 直接调 `Files.PutFile`。SDK / FUSE 内部应自动切换。
6. **handle 一致性**：read handle 是 open-time snapshot，dirty buffer 超 `SCRYD_IO_HANDLE_MEMORY_LIMIT` 溢出 tempfile；handle 5 分钟 idle 自动回收（写 handle 先 flush）。改这块要先看 spec §8。
7. **不要引入 fanotify / 旁路写入捕获**：与「零迁移」原则有张力，需要单独 spec（spec §11.6 列为后续工作项）。
8. **代码注释**：只解释非显然意图、tradeoff、约束；不要复述代码做了什么。
9. **commit 粒度**：小步提交，commit message 描述 *为什么* 改，不只是 *改了什么*。一个逻辑变更一个 commit。

---

## 8. 文档与 backlog 维护

- 现行 spec [`docs/spec/architecture.md`](docs/spec/architecture.md) 是 **living** 的：发现描述错了，**直接改 spec**，别开补丁文档。
- spec §11 的 backlog 项完成后：
  1. 把现状描述补进对应章节（§5 / §6 / §7 / §8 / §9 / §10）；
  2. 从 §11 删掉该条；
  3. 在 §13 变更记录里加一行。
- 新审计 / 新跨 PR 计划的开闭流程见 [`docs/README.md`](docs/README.md) §「新审计 / 新计划怎么开？」。
- 归档目录 [`docs/archive/`](docs/archive/) **不修改**——用于追溯历史决策动机；新 PR 不要往里面写。

---

## 9. Git / PR 流程（cloud agent 适用）

- 在 `main` 之上创建 feature 分支，命名采用 `cursor/<short-kebab>-<suffix>`（cloud agent 会自动加后缀）。
- 推送前确保本地 lint + test 全绿。
- 实施 → 测试两段都要 push；测试阶段产生的修复仍要 commit 回同一分支并 update PR。
- PR 描述要回答三个问题：动机、行为变化、回滚方式；涉及 proto / 存储格式时显式注明兼容性。
- 不要 force push / amend 已 push 的 commit；不要切走当前分支，除非用户明确要求。

---

## 10. 安全与敏感信息

- **不要**把 `SCRYD_AUTH_SECRET` / TLS 私钥 / Postgres 凭证写进 fixture / commit / PR 描述。
- 调试时优先用 `mock` embedding provider；外部 API key 通过 cloud agent secret 注入，不在仓库内出现。
- 涉及鉴权 / 传输的改动要在 PR 描述里说明对默认安全姿态（loopback-only、`allow_local_no_auth=false`、`enforce_scopes=true`）有没有影响。
- 发现安全漏洞通过 maintainer 私密渠道报告，**不要**在公开 issue 里披露可利用细节。

---

## 11. 当你卡住时

1. 先回到 [`docs/spec/architecture.md`](docs/spec/architecture.md)，确认问题对应的章节怎么定义。
2. 在 `docs/archive/` 找历史决策（**只读**）。
3. 在 `crates/scryd/src/server/tests/` 与 `tests/` 找现成的测试 fixture。
4. 仍卡住：在 PR 描述里写下你的疑问 + 你尝试过的 N 条路径 + 倾向选择，让 reviewer 决策。

—— *本文件随仓库演进。任何改变 agent 工作流、CLI 形态、文档分工的 PR 都应同步更新本文件。*
