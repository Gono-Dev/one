# 21 - 简易存储服务与 S3 兼容后端规划

状态：未来待做

## 目标

让本项目在保留现有本地文件存储能力的基础上，增加 S3 兼容对象存储后端。第一版目标是让 Nextcloud Desktop 和通用 WebDAV 客户端在 `local` 与 `s3` 两种后端下都能继续完成连接、上传、下载、复制、移动、删除、增量同步、收藏属性和 chunking v2 上传。

本规划不做自动迁移、不做多租户按用户选择后端、不做本地与 S3 双写。第一版采用启动时二选一的后端模式。

## 核心决策

- 后端模式：启动时通过配置选择 `local` 或 `s3`，单实例只使用一种存储后端。
- S3 范围：优先支持 S3-compatible，包括 AWS S3、MinIO、Cloudflare R2、Ceph RGW 等兼容 endpoint。
- 元数据真理源：S3 后端以 SQLite 为真理源，S3 只保存对象内容。
- WebDAV 协议层：继续复用 `dav-server 0.11`，通过新增 `DavFileSystem` 后端实现接入 S3。
- 普通读写策略：GET/HEAD 使用 S3 HEAD 和 Range GET；PUT 使用本地临时文件暂存，完成后提交到 S3。
- chunking v2 策略：分片继续落到本地临时目录，`MOVE .file` 合并后一次性 PUT 到 S3。
- 本地后端保持现状：继续使用本地文件系统、xattr、SQLite cache、同分区 chunk rename 检查。

## 配置设计

建议扩展配置如下：

```toml
[storage]
backend = "local" # local | s3
data_dir = "data"
xattr_ns = "user.nc"

[storage.s3]
bucket = "gono-one"
prefix = "files"
endpoint = "https://s3.example.com"
region = "auto"
path_style = true
allow_http = false
temp_dir = "data/s3-tmp"
```

凭据默认从环境变量读取：

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_SESSION_TOKEN`
- `AWS_REGION`

配置文件中暂不强制保存密钥，避免安装脚本和 systemd unit 直接泄露长期凭据。后续可以通过环境文件或 secret manager 集成。

## S3 对象布局

- 文件对象：`{prefix}/{rel_path}`
- 目录 marker：`{prefix}/{rel_path}/`
- 根目录不需要 marker。
- WebDAV 视图必须过滤内部临时对象和系统保留前缀。
- 路径仍使用项目现有的 WebDAV 路径规范化逻辑，禁止 `..`、NUL、绝对路径和编码穿越。

目录语义建议：

- `MKCOL /a/b` 创建 `files/a/b/` marker。
- `PROPFIND Depth 1 /a/` 使用 S3 delimiter list 合成直接子文件和子目录。
- 删除目录时递归删除该目录 marker 及所有子对象。
- 空目录必须依靠 marker 保留。

## 元数据模型

S3 后端不依赖 object metadata、object tags 或 sidecar JSON 作为主元数据。以下数据继续存在 SQLite：

- `file_id`
- `favorite`
- `permissions`
- dead props
- sync token
- change log
- upload sessions

S3 对象自身只提供内容状态输入：

- object size
- last modified
- provider ETag
- version id，如果 provider 支持

WebDAV ETag 生成优先级建议：

1. S3 version id
2. S3 object ETag
3. `size + last_modified`

COPY 与 MOVE 规则：

- COPY 目标必须分配新的 `file_id`，不能复用源 `file_id`。
- MOVE 必须保持原 `file_id`，并更新 SQLite 中的 `rel_path`。
- favorite、permissions、dead props 的复制/移动语义与当前本地后端保持一致。

## 实现拆分

### 1. 抽象存储后端

- 在 `storage` 或 `dav_handler` 下新增统一后端边界，用于构造 `dav-server` 需要的 `DavFileSystem`。
- 保留 `NcLocalFs` 作为本地实现。
- 新增 `NcS3Fs` 作为 S3 实现。
- `router` 根据 `AppState.storage_backend` 构造对应 filesystem。
- `AppState` 不再让所有上层逻辑直接依赖 `files_root/uploads_root/xattr_ns`，S3 后端只保留必要的本地临时目录。

### 2. 解耦 xattr 元数据

当前 `db::ensure_file_record` 会读取和写入 xattr。S3 后端需要拆出后端无关版本：

- 本地后端：继续读取 xattr，SQLite 作为 read-through cache。
- S3 后端：SQLite 为真理源，按 S3 object stat 刷新 ETag、size、mtime。
- `set_favorite` 改为按后端策略更新：本地写 xattr + SQLite，S3 只写 SQLite。

### 3. 实现 S3 DavFileSystem

`NcS3Fs` 需要实现：

- `metadata` / `symlink_metadata`
- `read_dir`
- `open`
- `create_dir`
- `remove_dir`
- `remove_file`
- `rename`
- `copy`
- `patch_props`
- `get_props`
- `get_prop`
- `get_quota`

读文件：

- `DavFile::seek` 只维护当前 offset。
- `DavFile::read_bytes(count)` 发起 S3 Range GET。
- `metadata()` 返回缓存或 HEAD 结果。

写文件：

- `DavFile::write_bytes/write_buf` 写入本地 temp file。
- `flush()` 将 temp file PUT 到 S3。
- 写入成功后刷新 SQLite file record。
- 写入失败必须返回错误，不能假装成功。

### 4. chunking v2 适配

- 本地后端继续使用当前 `uploads_root`。
- S3 后端使用 `storage.s3.temp_dir` 保存 upload session 和 chunk 文件。
- `MOVE .file` 时合并本地 chunk 到临时文件，再上传到 S3 目标对象。
- 上传成功后更新 SQLite 元数据和 change_log。
- 失败时保留 upload session，便于客户端重试或后续清理。

### 5. 启动探测与后端身份

本地后端启动探测保持：

- data dir canonicalize
- xattr probe
- files/uploads 同分区检查

S3 后端启动探测新增：

- temp dir 可创建、可写、可清理。
- bucket/prefix 可写入、读取、删除 probe object。
- endpoint、region、path-style 配置可连通。

建议在 SQLite 保存 storage identity：

- local：`local:<canonical_data_root>`
- s3：`s3:<endpoint>/<bucket>/<prefix>`

启动时如果检测到现有 DB 的 storage identity 与当前配置不一致，默认拒绝启动，避免本地数据和 S3 数据混用。迁移工具后续单独设计。

## 指标与运维

新增或调整指标：

- `gono_one_storage_backend_info{backend="local|s3"} 1`
- 本地后端继续暴露 `gono_one_storage_files_available_bytes` 和 `gono_one_storage_files_total_bytes`。
- S3 后端不假装知道 bucket 总容量，改为暴露本地 temp dir 可用空间。
- 增加 S3 请求错误计数、PUT/GET/COPY/DELETE 操作计数和延迟 histogram，可后续实现。

备份策略：

- local：备份 SQLite、`data/files`、xattr。
- s3：同时备份 SQLite 和 bucket prefix。
- S3 第一版不支持用户绕过本服务直接修改 bucket；如果发生，需要未来的扫描/修复工具重建 SQLite 索引。

## 测试计划

基础测试：

- `cargo check`
- `cargo test`
- 现有 Phase 0 WebDAV 测试继续覆盖本地后端。
- 新增 S3 fake 或 MinIO 测试覆盖相同协议矩阵。

S3 必测场景：

- `/` 和 `/remote.php/dav/` 两种路径入口。
- Basic Auth 401 和成功路径。
- PROPFIND Depth 0/1。
- PUT 后返回 `OC-Etag` 和 `OC-FileId`。
- GET 使用 Range 读取正确内容。
- MKCOL 创建空目录 marker。
- COPY 生成新 `file_id`。
- MOVE 保持 `file_id`。
- DELETE 文件和递归删除目录。
- PROPPATCH favorite 可读回。
- REPORT sync-collection 返回正确 change log。
- SEARCH 和 `oc:filter-files` 仍按 SQLite 元数据工作。
- chunking v2 `MKCOL + PUT chunks + MOVE .file` 能上传到 S3。

兼容性测试：

- `scripts/compat-smoke.sh` 增加 `RUN_S3_SMOKE=1`。
- S3 smoke 推荐使用 MinIO。
- Litmus 本地后端继续必跑；S3 后端在 MinIO smoke 稳定后作为可选项加入。

## 实施阶段

### Phase A：文档与配置打样

- 更新计划文档、配置示例和安装说明。
- 增加 `storage.backend` 与 `[storage.s3]` 配置解析。
- 启动时先只识别配置，不改变运行后端。

### Phase B：元数据解耦

- 拆分 xattr 依赖。
- 让 SQLite-only 元数据路径可测试。
- 保证本地后端行为不变。

### Phase C：S3 文件系统 MVP

- 接入 `object_store` 的 aws feature。
- 实现 `NcS3Fs` 的基本 WebDAV 方法。
- 完成 PUT/GET/PROPFIND/MKCOL/DELETE/COPY/MOVE。

### Phase D：Nextcloud 扩展与 chunking

- 适配 REPORT、SEARCH、filter-files。
- 适配 S3 后端 chunking v2。
- 完成 MinIO smoke。

### Phase E：加固与运维

- 增加 storage identity 防误启动。
- 增加 S3 指标。
- 补充部署文档、备份/恢复注意事项。
- 评估是否需要本地到 S3 的迁移工具。

## 明确不做

- 不在第一版支持运行时切换后端。
- 不在第一版支持本地与 S3 双写。
- 不在第一版支持每用户不同后端。
- 不在第一版把 S3 object metadata/tags 作为元数据真理源。
- 不在第一版承诺可安全处理用户绕过服务直接改 bucket 的情况。
- 不在第一版实现完整迁移工具；只保留未来扩展入口。

