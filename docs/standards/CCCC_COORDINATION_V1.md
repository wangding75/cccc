# CCCC Coordination Contract (CCCC-CORE-01 冻结)

Status: Frozen (CCCC-CORE-01)

本标准冻结 CCCC One-shot 多智能体协作改造（CCCC-CORE-01 ~ CCCC-CORE-12）共同遵守的数据与业务规则。

设计原则：**兼容演进**。在保留现有 v0.4.30 公开契约的前提下，新增两个独立维度与一组任务消息契约，
不删除、不重命名、不破坏现有 Group / foreman / peer / 广播 / 多接收者 / PTY / headless 能力。

约束优先级：`CLAUDE.md` → `docs/reference/architecture.md` → 现有 contracts/代码事实 →
`cccc-development-task-v3-clear.md` → 本文档。

---

## 1. 术语映射

任务书 §9 的术语按以下方式映射到现有 CCCC 公开契约，不新建并存的核心模型：

| 任务书术语 | 现有契约 | 说明 |
|---|---|---|
| Workspace | Group | 不新增 Workspace 核心模型；`workspace_id` 即 `group_id` |
| Foreman | foreman | 首位可见 actor，由稳定位置自动决定 |
| Worker | peer | 不把 `peer` 全局重命名为 `worker` |

冻结实现位于 `src/cccc/contracts/v1/coordination.py`。

## 2. 现有架构事实（保留基线）

1. **Group**：`~/.cccc/groups/<group_id>/`，元数据 `group.yaml`，事实流 `ledger.jsonl`（append-only），状态 `active/idle/paused/stopped`。
2. **Actor**：`ActorRole = Literal["foreman","peer"]`，role 由可见 actor 列表稳定位置自动决定，不持久化。`RunnerKind = Literal["pty","headless"]`。
3. **Message**：`ChatMessageData.to: list[str]`，空=广播，支持 `@all` / `@peers` / `@foreman` / `user` / 明确 actor id。广播与多接收者是一等语义。
4. **Event**：`Event`（v/id/ts/kind/group_id/scope_key/by/data），kind 为 `EventKind` 白名单。
5. **Ledger**：单写者，daemon 通过 IPC 唯一写入。
6. **Runner**：`runners/pty.py`（长期交互进程）、`runners/headless.py`（MCP 驱动）。无 One-shot Run / Session / StandardEvent / Artifact。

## 3. 新增维度（独立、默认兼容）

### 3.1 CoordinationMode

```python
CoordinationMode = Literal["legacy", "foreman_managed"]
DEFAULT_COORDINATION_MODE = "legacy"
```

- `legacy`（默认）：保持现有 CCCC 行为。普通消息支持广播与多接收者，不强制任务消息路由规则，不启用 Delivery/Run/Session 契约。
- `foreman_managed`：显式启用后，任务消息（`task_instruction` / `task_result`）必须满足严格单接收者路由规则，并启用 Delivery/Run/RuntimeSession 契约。普通消息（`chat`）行为不变。

### 3.2 ExecutionMode

```python
ExecutionMode = Literal["pty", "headless", "one_shot"]
DEFAULT_EXECUTION_MODE = "pty"
```

- `pty` / `headless`：保持现有 runner 语义。
- `one_shot`：新增执行模式。每条任务消息创建一个 Run，启动一次智能体进程，保存全部输出，进程退出后 Run 结束。Actor 仍是逻辑角色，不绑定长期进程。仅当显式启用时生效，不替换现有 PTY/headless/app-server 代码。

> 持久化位置（group.yaml 新字段或 settings）留待 CCCC-CORE-02 数据模型实现时确定；本契约只冻结枚举与默认值。

## 4. 任务消息语义

```python
TaskMessageKind = Literal["chat", "task_instruction", "task_result", "system"]
DEFAULT_TASK_MESSAGE_KIND = "chat"
```

### 4.1 chat

保持现有 `ChatMessageData` 全部语义，包括广播与多接收者。

### 4.2 task_instruction（Foreman → 指定 peer）

必须满足：

- `sender.role = foreman`
- recipient 数量 = 1
- `recipient.role = peer`
- recipient actor id 明确存在
- sender 与 recipient 属于同一 Group

### 4.3 task_result（peer → Foreman）

必须满足：

- `sender.role = peer`
- recipient 数量 = 1
- recipient = 当前 Group 的 foreman
- sender 与 recipient 属于同一 Group

### 4.4 task 消息禁止项

- 广播
- 多接收者
- 空接收者
- peer → peer
- 自动选择空闲 peer
- 根据 Runtime 模糊匹配接收者

CCCC 必须依据明确 Actor ID 投递。消息路由只依据明确 `recipient_actor_id`，不依据 `session_id` 决定接收者。

## 5. 可靠投递契约（foreman_managed 模式）

复用现有 Message 与 Ledger，不建立重复消息系统。以下模型描述标准化视图，实际存储仍以 ledger 事件为准。

### 5.1 TaskMessage

```python
class TaskMessage:
    group_id: str
    message_id: str
    sender_actor_id: str
    recipient_actor_id: str
    kind: Literal["task_instruction", "task_result"]
    payload: dict
    created_at: str
```

### 5.2 Delivery

```python
class Delivery:
    delivery_id: str
    group_id: str
    message_id: str
    recipient_actor_id: str
    state: DeliveryState = "queued"
    attempts: int = 0
    locked_by: str | None
    locked_at: str | None
    last_error: str | None
    created_at: str
    updated_at: str
```

- 每个 Actor 独立逻辑队列。发给 a1 的任务只进入 a1 的队列，a2 不得消费、查询或触发该任务的 Run。
- 消费条件必须包含明确的 `group_id` 与 `recipient_actor_id`。
- 一条任务消息对一个 Actor 最多创建一个 Run。
- 守护进程重启后不能重复启动同一任务。
- 投递失败必须保留状态和错误原因。

### 5.3 Run

```python
class Run:
    run_id: str
    group_id: str
    actor_id: str
    message_id: str
    delivery_id: str = ""
    session_id: str = ""
    runtime: str = ""
    execution_mode: ExecutionMode = "one_shot"
    state: RunState = "pending"
    started_at: str | None
    ended_at: str | None
    exit_code: int | None
    error_code: str | None
```

### 5.4 RuntimeSession

```python
class RuntimeSession:
    session_id: str
    group_id: str
    owner_actor_id: str
    runtime: str = ""
    work_dir: str = ""
    execution_mode: ExecutionMode = "one_shot"
    state: RuntimeSessionState = "idle"
    current_run_id: str | None
    created_at: str
    updated_at: str
```

- 无 `session_id`：创建新会话。有 `session_id`：继续指定会话。
- 校验：`session.owner_actor_id = run.actor_id`，且 Group / Runtime / 工作目录一致。
- 禁止：a1 使用 a2 的 Session；同一 Session 并发执行多个 Run；Session 恢复失败后静默创建新会话。

### 5.5 StandardEvent

```python
class StandardEvent:
    event_id: str
    group_id: str
    actor_id: str
    run_id: str
    session_id: str = ""
    runtime: str = ""
    sequence: int = 0
    stream: Literal["stdout","stderr","event","system"]
    event_type: StandardEventType
    content: str = ""
    received_at: str
    source_reference: str = ""
```

- 标准化结构，不修改内容。
- Web、Ledger 上层视图和其他 Actor 优先消费标准事件，不直接消费杂乱的 Runtime 原始格式。
- 无法识别的输出封装为 `raw.stdout` / `raw.stderr`。

### 5.6 RunArtifact

```python
class RunArtifact:
    artifact_id: str
    run_id: str
    kind: Literal["raw.stdout","raw.stderr","artifact"]
    path: str = ""
    sha256: str = ""
    bytes: int = 0
    mime_type: str = ""
```

- 原始 stdout / stderr 分开保存、不修改内容、不删除重复、不以摘要替代、不因解析失败而丢弃。

## 6. 状态机（冻结）

### 6.1 Delivery

```
queued → reserved → delivered
                   → failed
reserved → queued  (回退，如锁过期)
```

终态：`delivered`、`failed`。

### 6.2 Run

```
pending → running → succeeded
                 → failed
                 → cancelled
                 → timed_out
```

终态：`succeeded`、`failed`、`cancelled`、`timed_out`。

### 6.3 RuntimeSession

```
idle ↔ busy
busy  → closed
busy  → failed
```

`busy` 期间禁止并发 Run。

## 7. 错误码（冻结）

| 错误码 | 语义 |
|---|---|
| `GROUP_FOREMAN_REQUIRED` | Group 需要一个 foreman |
| `GROUP_MULTIPLE_FOREMEN` | Group 只能有一个 foreman |
| `TASK_RECIPIENT_REQUIRED` | 任务消息需要单个明确接收者 |
| `TASK_MULTIPLE_RECIPIENTS_NOT_ALLOWED` | 任务消息禁止多接收者 |
| `TASK_ROUTE_NOT_ALLOWED` | 任务消息路由不被允许 |
| `ACTOR_NOT_FOUND` | Actor 不存在 |
| `ACTOR_DISABLED` | Actor 已禁用 |
| `SESSION_NOT_FOUND` | Session 不存在 |
| `SESSION_ACTOR_MISMATCH` | Session 归属 Actor 不匹配 |
| `SESSION_RUNTIME_MISMATCH` | Session runtime 不匹配 |
| `SESSION_GROUP_MISMATCH` | Session group 不匹配 |
| `SESSION_WORKSPACE_PATH_MISMATCH` | Session 工作目录不匹配 |
| `SESSION_BUSY` | Session 正忙于另一个 Run |
| `DELIVERY_ALREADY_EXISTS` | 该消息与接收者的投递已存在 |
| `RUN_ALREADY_EXISTS` | 该消息的 Run 已存在 |
| `RUN_CANCELLED` | Run 已取消 |
| `RUN_TIMED_OUT` | Run 超时 |
| `SESSION_RESUME_FAILED` | Session 恢复失败 |
| `OUTPUT_CAPTURE_FAILED` | 输出采集失败 |

错误码常量与 `error_message()` 位于 `coordination.py`。路由/所有权校验失败抛出 `RouteValidationError(code, detail)`。

## 8. 兼容与迁移策略

1. v0.4.30 Group 数据仍可读取（新字段均为 `Optional` + 默认值）。
2. foreman / peer 不重命名。
3. 普通 Message 广播与多接收者保持。
4. 未启用 `foreman_managed` 的 Group 行为不变。
5. 新字段有兼容默认值；旧客户端不识别新字段时不立即失效。
6. `one_shot` 通过新增配置显式启用；不删除现有 PTY / headless / app-server 代码。
7. 破坏性变更：**无**。所有变更均为新增（additive）。

## 9. 契约测试方案

- 文件：`tests/test_coordination_contracts.py`
- 覆盖：维度默认值、错误码冻结、状态机终态、模型字段与默认值、任务路由规则（legacy/foreman_managed × chat/task_instruction/task_result × 广播/多接收者/跨 Group/角色错配）、Session 所有权校验、向后兼容（ActorRole 仍为 foreman/peer、ChatMessageData 仍支持广播/多接收者、RunnerKind 仍为 pty/headless）。
- 验证结果：45 passed。

## 10. 后续任务边界

本契约冻结后，后续任务的实施顺序与边界：

| 任务 | 边界 |
|---|---|
| CCCC-CORE-02 数据模型 | 持久化 coordination_mode/execution_mode 及 Delivery/Run/Session/Artifact（新增表/索引/约束/迁移/CRUD），不破坏现有 Group 表 |
| CCCC-CORE-03 路由控制 | 实现 RouteValidator，复用 `validate_task_route`，仅在 foreman_managed 模式强制 |
| CCCC-CORE-04 消息投递 | Actor 独立 Mailbox、queued/reserved/delivered 状态、消息锁、防重复消费 |
| CCCC-CORE-05 One-shot Runner | 创建 Run、启动一次进程、保存结果、退出释放资源 |
| CCCC-CORE-06 Session 管理 | 新建/继续会话、所有权校验（复用 `validate_session_owner`） |
| CCCC-CORE-07 进程管理 | 取消/超时/异常恢复/清理子进程 |
| CCCC-CORE-08 输出采集 | 保存原始 stdout/stderr、有序 stream 记录 |
| CCCC-CORE-09 输出标准化 | StandardEvent 与 Adapter 接口，第一阶段原样透传 |
| CCCC-CORE-10 协作闭环 | Foreman↔peer 完整工作流、消息/Run 关联 |
| CCCC-CORE-11 API 接口 | Workspace/Actor/Message/Run/Session/Event 查询 API |
| CCCC-CORE-12 测试验收 | 串线/重复投递/Session 错误/重启/大输出/取消测试 |
