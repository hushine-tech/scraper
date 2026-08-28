# Scraper

> 更新时间：2026-08-28

## 概述

Binance 交易所行情采集服务。采集结果按事件时间写入 TimescaleDB；满足实时投递条件的已完成 K 线还可发布到 Kafka。运行日志使用独立的 `log-config.json` 配置。

---

## 采集能力

当前 collector 支持：

- 现货 / 期货 K 线
- 现货 / 期货 order book
- 期货 funding rate
- 期货 open interest

Funding rate 使用 REST 轮询。Open interest 的 forward collector 优先使用 REST 轮询，REST 失败时回退到当前 WebSocket 路径。

`config.yaml` 默认关闭所有静态 forward collector；当前 K 线流由 control-panel 的用户需求聚合结果驱动。配置中的 symbol 列表是静态采集模式的候选范围，不表示这些币种始终都会启动采集。

---

## 数据表

当前读写约定是“按事件时间分 year database，按表分 symbol/interval”：

- live continuous ingestion 和 historical backfill 都写入 `{exchange}_{year}`，例如 `binance_2026`
- K 线表名包含 interval：`{market}_klines_{symbol_lower}_{interval_lower}`
- orderbook / funding / OI 表名不含 interval：`{market}_{datatype}_{symbol_lower}`
- 固定 `binance` / `okx` database 不是读写目标，也不作为 fallback

```
spot_klines_{symbol}_{interval}
futures_klines_{symbol}_{interval}
spot_orderbook_{symbol}
futures_orderbook_{symbol}
futures_open_interest_{symbol}
futures_funding_rates_{symbol}
```

示例：
```
spot_klines_btcusdt_1m
futures_klines_btcusdt_1m
futures_open_interest_btcusdt
futures_funding_rates_btcusdt
spot_orderbook_btcusdt
futures_orderbook_btcusdt
```

旧环境可能仍有 `{market}_{datatype}_{SYMBOL}_{YEAR}` 表；这些不是当前新写入目标。需要清理固定库时，可在 dev 环境运行 `scripts/drop-fixed-market-data-dbs.sql`。

---

## 架构

```
control-panel-service（live K 线 stream/lease + K 线/Funding 历史 request/coverage）
    |
    v
Scraper (Go) -- REST / WebSocket collectors --> {exchange}_{year} TimescaleDB
    |
    +-- finalized live K-line（effective_live_delivery=true）--> Kafka

Scraper runtime logging -- log-config.json --> local files / optional Kafka
```

---

## 配置

行情采集配置文件是 `config.yaml`。仓库默认值关闭 Binance 的静态 forward collectors，并启用 control-panel 驱动的 managed K-line：

```yaml
exchanges:
  binance:
    forward:
      spot_kline: false
      futures_kline: false
      spot_orderbook: false
      futures_orderbook: false
      funding_rate: false
      futures_open_interest: false

market_data:
  control_plane:
    enabled: true
```

---

## 日志

运行时日志与行情采集分开配置，日志配置文件是 `log-config.json`。仓库默认值：

- 本地文件：启用，输出目录 `./logs`
- Kafka：禁用；启用后可通过配置指定 broker、topic 和 topic prefix
- tracing：禁用

---

## 启动

```bash
# Docker 部署
docker compose up -d

# 本地编译运行
go build -o bin/scraper ./cmd/scraper
./bin/scraper
```

`docker compose up -d` 是有效的启动方式，但 Compose 引用的外部 `app-logs-net` 网络以及 TimescaleDB、Kafka 等基础设施必须预先存在。

---

## 状态

- 交易所：Binance（OKX 待实现）
- TimescaleDB 写入：✅
- 本地日志：✅（仓库默认启用）
- Kafka 日志：可选（仓库默认禁用）
- Kafka 市场数据：✅ Live K-line 已接通（`md.kline.{exchange}.{market}.{interval}`，仅 closed bar）

## Live K-line Kafka

- Topic 规则：`md.kline.{exchange}.{market}.{interval}`，不带 `testnet` / `prod` 之类的环境段
- 当前范围：只发布 forward/live Binance K-line
- 发布条件：只在 K 线 closed/finalized 后写入 Kafka
- Reverse/backfill：只回写 TimescaleDB，不会发到 live Kafka topic

## 需求驱动采集控制面

市场数据控制面已经接进 workspace，当前模型是：

- 用户只声明“需要什么流”，不直接 start / stop `scraper`
- `control-panel-service` 持久化三类状态：
  - `request`：某个用户声明的需求
  - `stream`：共享物理流的聚合状态
  - `lease`：某个 live session 当前仍在消费该流的 TTL 租约
- `scraper` 在 `market_data.control_plane.enabled=true` 时按数据库状态 reconcile live K 线的
  `starting / running / draining / stopped / error`
- historical runtime 按有限窗口处理 `kline` 与 Futures `funding_rate` request，先写
  year-sharded TimescaleDB 并上报 coverage，再将 request 标记 ready
- 运行中的 live `kline` 仍然总是写 TimescaleDB；只有
  `effective_live_delivery=true` 时才发 Kafka
- `strategy-service` 的 demo/live session 启动前会做 readiness preflight，并在运行中续租 lease

当前产品边界：

- live 动态 collector/lease 只覆盖 `kline`
- historical request/coverage 覆盖 `kline` 和 Futures `funding_rate`；Funding 的 interval 必须为空
- `orderbook`、`open_interest` 尚未进入需求驱动控制面
- 还没有独立管理员 UI
- 当前 UI 重点是用户提交/取消自己的 stream request，并查看 readiness / freshness / live-delivery 状态
- 运维排障主要依赖 `strategy-service` diagnostics、日志和 control-plane 状态

详细说明见：

- [docs/demand-driven-market-data-control-plane.md](./docs/demand-driven-market-data-control-plane.md)
- [docs/market-data-control-plane-rollout.md](./docs/market-data-control-plane-rollout.md)

## Historical vs Live Scope

`market-data` 现在分成两类 scope：

- `live`
  - 从当前时间开始持续采集
  - finalized K-line 总是写入交易所 live store
  - 只有用户勾选 `needs_live_delivery=true` 时才会继续发 Kafka
- `historical`
  - 声明一个有限时间窗口
  - `scraper` 会把窗口自动拆到 year-sharded 历史库
  - 支持 `kline`，并为 Futures K 线需求配套创建 interval 为空的 `funding_rate` 请求
  - 例如 `binance_2025`、`binance_2026`
  - scraper 完成 backfill 并成功上报 coverage 后才会进入 `ready`；K 线要求完整行数，
    Funding 允许用显式零行 segment 表示该时间段确实没有结算记录

当前库关系：

- live continuous ingestion / historical authoritative store:
  - `{exchange}_{year}`
  - 按 record timestamp/open_time/funding_time/event timestamp 路由到对应年份库
  - 面向实时 collector、freshness、backfill、coverage verify、backtest / demo-live session

这意味着：

- backtest 只看 historical coverage，不看 live collector / Kafka
- demo/live session 只看 live readiness / freshness；历史请求不会替代 live stream

## 部署检查

当前系统未上线，不用旧协议或静态 K 线回退掩盖控制面失败。部署顺序、live/historical
验收和失败处理见
[Market Data Control-Plane Deployment](./docs/market-data-control-plane-rollout.md)。验收不通过时
停止本环境的 session admission，保留 owner 状态进行修复；不要手工制造 ready coverage，
也不要删除共享数据库或 volume。
