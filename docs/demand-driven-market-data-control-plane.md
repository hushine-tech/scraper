# 需求驱动的市场数据控制面

状态：`kline` v1 已实现，当前仍处在 rollout / 验证阶段。

本文档记录当前已经落地的控制面模型，以及仍然保留的 rollout 边界。它不再只是提案草稿，但也不代表管理员 UI、告警平台和多 runtime 隔离都已经完成。

## 当前实现范围

当前 workspace 已经具备这些能力：

- 用户可以声明 / 取消自己需要的 `kline` 流
- `control-panel-service` 持久化 `request / stream / lease` 三类状态
- `scraper` 可在 `market_data.control_plane.enabled=true` 时按控制面状态管理 `kline` collector
- `strategy-service` 会在 `mode=2` 启动前做 readiness preflight，并在运行中续租 lease
- `strategy-service` 提供 live-consumption diagnostics，用于观察 session 到 stream 的绑定和 unroutable live K-line 异常

当前明确不在范围内的内容：

- `orderbook / funding / oi`
- 独立管理员 UI
- 强制 start / stop 某条共享流的人工 override 面板

更具体的 rollout / rollback 操作见：

- [market-data-control-plane-rollout.md](./market-data-control-plane-rollout.md)

## 三类状态各自是什么意思

### Request

`request` 表示“某个用户声明需要这条流”。

- 它是用户意图
- 它有 ownership
- 取消 request 不等于立即停流

### Stream

`stream` 表示“这条共享物理流当前整体处于什么状态”。

- 它按 `(exchange, market, kind, symbol, interval)` 聚合
- 它没有用户 ownership
- 它持有 `desired_state / actual_state / effective_live_delivery / freshness / last_error`

### Lease

`lease` 表示“某个 live session 当前仍在实际消费这条流”。

- 它由 `strategy-service` 创建和续租
- 它带 TTL
- 它的作用是防止 stop-path 失败时把流永久泄漏

## 背景

当前 `scraper` 采用静态配置模型：

- 启动时读取 `config.yaml`
- 按配置创建各类 collector
- 常驻采集并写入 TimescaleDB
- Live K-line Kafka 是否开启也由进程配置决定

这条路径足以支撑当前开发和 `C3` smoke，但不适合后续面向用户开放 live/testnet 策略运行。主要问题有：

- 前端无法声明“我需要新的数据流”
- `scraper` 不能按需求动态 start / stop 某条流
- 启动 live 策略前，后端无法严格校验依赖数据流是否真的可用
- 如果只靠“策略停止时改数据库”释放流，写库失败会导致流长期无法关闭

## 目标

将 `scraper` 演进为“需求驱动”的市场数据采集系统：

- 用户声明“需要什么流”，而不是直接操作 `scraper`
- 数据库保存期望状态，作为控制面的唯一真相源
- `scraper` 自己 reconcile 期望状态与实际运行状态
- `strategy-service` 在 live 启动前做硬校验
- 活跃 session 通过 lease / heartbeat 维持依赖流
- 无人需要且 lease 过期后，`scraper` 才允许停流

## 核心原则

### 1. 用户声明需求，不直接控制采集器

用户侧只表达：

- 需要哪个交易所
- 哪个市场：`spot` / `futures`
- 哪类数据：初期只考虑 `kline`
- 哪个 `symbol`
- 哪个 `interval`

用户不直接做这些事：

- 不直接向 `scraper` 发 start / stop 指令
- 不直接决定某条共享流是否发布到 Kafka
- 不直接修改 collector 进程内部状态

### 2. 数据库是控制面的唯一真相源

控制面不能依赖一次性通知。原因很简单：

- `scraper` 重启后不能丢状态
- `strategy-service` 重启后也不能丢状态
- 单次 RPC / HTTP 调用失败不能让系统进入不一致状态

因此，所有需求、租约、流状态都必须持久化到数据库，再由各服务自行 reconcile。

### 3. Kafka 是否发布属于“流级聚合状态”，不是“某个用户的开关”

同一条数据流可能被多个用户、多个 session 共享。例如：

- `binance + futures + BTCUSDT + 1m + kline`

如果把 Kafka 发布做成“某个用户自己的勾选”，很容易误伤其他 live session。更合理的语义是：

- 用户可以声明“我需要 live 消费能力”
- 后端把多个需求聚合成流级别的有效状态
- 只要仍有任何 live 需求或活跃 lease 依赖该流，该流就必须继续发布 Kafka

## 建议的数据模型

以下是建议方向，不是最终 schema：

### `market_data_requests`

记录用户或后台声明的需求。

建议字段：

- `request_id`
- `user_id`
- `portfolio_id` 或其他归属标识
- `exchange`
- `market`
- `kind`
- `symbol`
- `interval`
- `needs_live_delivery`
- `status`：`pending / active / cancelled`
- `created_at`
- `updated_at`

### `market_data_streams`

记录物理流的聚合状态。按流维度唯一：

- `exchange + market + kind + symbol + interval`

建议字段：

- `stream_id`
- `exchange`
- `market`
- `kind`
- `symbol`
- `interval`
- `desired_state`：`running / stopped`
- `actual_state`：`pending / starting / running / draining / stopped / error`
- `effective_live_delivery`
- `last_data_at`
- `last_error`
- `updated_at`

### `market_data_leases`

记录活跃策略 session 对流的占用关系，由 `strategy-service` 周期性续租。

建议字段：

- `lease_id`
- `session_id`
- `strategy_id`
- `portfolio_id`
- `stream_id`
- `expires_at`
- `last_heartbeat_at`

这个表的目的不是替代 request，而是避免下面这种情况：

- session 停止时写库失败
- request 已经不再需要，但 lease 还没被正常清理
- 如果没有 TTL，流可能永远关不掉

lease + heartbeat + TTL 可以把这个问题降为“最多延迟释放”，而不是“永久泄漏”。

## 控制流程

### 1. 用户提交数据需求

当前前端已经有“数据管理”入口。用户提交需求后：

- 请求进入 gateway / backend
- 后端写入 `market_data_requests`
- 不直接通知 `scraper`

### 2. `scraper` 定时 reconcile

`scraper` 周期性读取数据库，计算每条流的目标状态：

- 是否至少有一个有效 request
- 是否需要 live Kafka 发布
- 是否有未过期 lease

然后决定：

- `start`
- `keep running`
- `enter draining`
- `stop`

### 3. `strategy-service` 启动前做硬校验

用户点击“开始运行 live/testnet 策略”后，后端必须先校验所需流是否可用，再决定是否创建 session。

最少要检查：

- 所需流是否存在
- `scraper` 是否已经把流跑到 `running`
- 数据是否足够新鲜
- 对于 live 模式，Kafka 发布是否有效

若不满足，应直接返回 `FAILED_PRECONDITION` 或对应 HTTP 错误，而不是创建一个注定收不到数据的 session。

### 4. 运行中的 session 续租 lease

当 session 真正进入 live 运行态后，`strategy-service` 应为所依赖的流创建 lease，并按固定周期 heartbeat。

这一步的目标是：

- 反映“这条流现在确实正在被消费”
- 即使 stop 流程写库失败，只要 heartbeat 停止，lease 也会自然过期

### 5. 停流必须满足双重条件

`scraper` 不应仅因为“request 被取消”就马上停流。更稳的规则是：

- 没有有效 request
- 没有未过期 lease
- 经过一个短暂的 draining / grace period

同时满足时，才允许真正 stop。

## `strategy-service` 诊断接口

除了数据库 lease，还需要一个面向诊断的 `strategy-service` 接口，用来回答：

- 当前内存里有哪些 live session
- 每个 session 正在消费哪些流
- 某条流当前是否真的路由到了至少一个活跃策略

这个接口的定位是：

- 管理后台诊断
- 告警核对
- 排障审计

它不应成为停流的唯一真相源。停流主判断仍应来自数据库中的 request + lease。

## 告警建议

至少需要覆盖以下几类告警：

- 有 request，但流长时间未进入 `running`
- 有未过期 lease，但数据长时间不新鲜
- `strategy-service` 诊断结果显示“有 session 预期消费”，但实际没有路由命中
- `scraper` 持续推送某条流，但诊断接口显示没有任何活跃 session 在消费
- 数据库 lease 与 `strategy-service` 内存态长时间不一致

其中：

- “拿到 K 线却没有路由到任何策略”应视为异常
- 这类异常不一定立即停流，但必须给管理员可见

## 前台与后台边界

后续建议分成两类界面；但当前还没有单独管理员 UI：

### 用户界面

用户只能看到与自己相关的内容：

- 我申请了哪些流
- 当前状态是否 `running`
- 是否支持 live 启动
- 哪些流正在被自己的 session 使用

### 管理员界面

管理员可以看到全局状态：

- 全部流
- 聚合需求
- lease
- 错误与告警
- 诊断接口结果
- `scraper` 实际运行状态

管理员 / 用户界面分离仍是后续工作。本 change 当前只有最小用户界面和诊断 API，没有独立管理员操作台。

## 与当前 `C3` 的关系

这套控制面不是 `C3` smoke 的硬前置。`C3` 当前仍可依赖静态配置完成验证。

但在真正把 `mode=2` 能力开放到用户可操作的前端前，这套控制面是必要前置，否则会出现：

- 策略能启动但根本收不到数据
- 共享流被误停
- 流资源无法安全回收
- 管理后台缺少定位依据

## 非目标

本文档当前不覆盖：

- 多 runtime / 多 `strategy-service` 部署下的路由细节
- `orderbook / funding / oi` 的精细化控制规则
- 管理后台具体页面设计
- 最终数据库 schema 与 migration 细节

这些内容会在正式 proposal / design 中进一步细化。
