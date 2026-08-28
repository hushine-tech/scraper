# 需求驱动的市场数据控制面

最后核验：2026-08-28。

本文描述当前已经落地的控制面契约。控制面由 control-panel-service 持久化，scraper
负责 reconcile 和写入交易所 year-sharded TimescaleDB；用户和 session 不直接启停
scraper 内部 collector。

## 当前范围

| Scope | Kind | 当前能力 |
|---|---|---|
| `live` | `kline` | request 聚合、共享 stream、session lease、freshness、可选 finalized-bar Kafka delivery |
| `historical` | `kline` | 有限时间窗 backfill、逐年 coverage、完整行数校验 |
| `historical` | `funding_rate` | Futures 有限时间窗 backfill、逐年 coverage；interval 必须为空，显式零行 segment 合法 |

`orderbook`、`open_interest` 和 live Funding 尚未进入需求驱动生命周期。它们不能伪装成
上述 scope 已受支持。OKX 仍 fail-closed；当前可用的 historical Funding adapter 是
Binance。

## Live 状态

### Request

`request` 是某个用户对一条 live K 线的需求，包含 ownership 和
`needs_live_delivery`。取消 request 只撤销该用户意图，不直接停止共享物理流。

### Stream

`stream` 按 `(exchange, market, kind, symbol, interval)` 聚合，保存
`desired_state / actual_state / effective_live_delivery / freshness / last_error`。scraper
只根据控制面状态执行 `start / keep running / drain / stop`。

### Lease

`lease` 表示 active demo/live session 正在消费 stream。strategy-service 创建并续租 TTL；
session 停止或 heartbeat 消失后 lease 到期。只有没有有效 request、没有有效 lease，并且
draining grace 已结束，scraper 才能停止 collector。

### Live 启动与投递

demo/live session 启动前必须验证所有声明的 K 线 stream 已存在、处于 running、freshness
合格，并在需要实时投递时确认 `effective_live_delivery=true`。不满足时 fail closed，不创建
注定收不到数据的 session。

managed K 线始终先写 `{exchange}_{year}`。只有 closed/finalized bar 且 stream 的有效聚合
状态要求 live delivery 时，才发布到 `md.kline.{exchange}.{market}.{interval}`。Kafka topic
不带 demo/live 环境段。

## Historical request 与 coverage

historical request 必须带非空 UTC `[requested_start_at, requested_end_at)` 窗口，且不允许
`needs_live_delivery=true`。它不创建 live lease，也不依赖 live Kafka。

scraper 的 historical runtime 周期性读取 request：

1. 将状态报告为 `running`；
2. 按交易所 adapter 执行 K 线或 Funding backfill；
3. 按事件时间写入 `{exchange}_{year}`；
4. 向 control-panel-service 报告逐年 coverage segment；
5. coverage 写入成功且窗口验证完整后才报告 `ready`，否则报告 `error`。

K 线 key 必须包含 interval，coverage 行数必须精确匹配窗口与 interval。Funding key 必须是
Futures `funding_rate` 且 interval 为空；交易所确实没有结算事件的窗口必须上报显式零行
segment，不能把“没有请求或请求失败”误当成完整 coverage。

创建 Futures K 线 historical request 时，control-panel-service 会为同一 symbol/window 配套
创建 Funding historical request。Backtest 预检分别检查 K 线与 Funding coverage；RuntimeChannel
backtest page 同时携带 K 线和 Funding facts，并明确 `funding_coverage_complete`。

## 服务边界

- control-panel-service：request/stream/lease/history/coverage 的唯一控制状态 owner。
- scraper：交易所 adapter、backfill、collector、year-sharded store 和 coverage report。
- strategy-service：解析 `INPUTS`，执行 demo/live readiness、session lease 与 runtime diagnostics。
- quant-handler/frontend：用户 request/status、Backtest 缺口下载与进度展示。

scraper 不能读取 Portfolio、Venue credential、订单或 session 钱包。Runtime 也不能收到内部
数据库、Kafka、账户或订单地址。

## 重启与一致性

控制面不依赖一次性通知。scraper 重启后从持久化状态恢复 live reconcile 和未完成的
historical request；strategy-service 重启后由 session/lease heartbeat 恢复消费关系。
request 取消、lease 到期、collector 停止和 historical error 都必须通过 owner API/状态表
收敛，不能用进程内 map 作为唯一事实。

同一个 year shard 的写入受 writer lease 保护，避免多 scraper 实例同时拥有相同写域。
historical coverage 只有在数据写入和 coverage report 都成功后才可见为完成。

## 操作检查

- live request 长时间未从 starting 进入 running；
- active lease 对应 stream 不新鲜；
- session diagnostics 声明消费，但没有 K 线路由命中；
- historical request 长时间停在 running/verifying 或进入 error；
- K 线行数与 coverage 不一致；
- Funding 缺少显式 coverage segment，或 interval 非空；
- writer lease owner 与实际 scraper instance 不一致。

当前没有独立管理员 override UI。排障使用用户 `/market-data` 页面、control-panel stream/history/
coverage 状态、strategy runtime diagnostics、日志与 tracing。任何人工修复都不得绕过 owner
API 直接制造 ready 状态。
