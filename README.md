# Scraper

> 更新时间：2026-05-16

## 概述

Binance 交易所数据采集服务，将采集的数据写入 TimescaleDB 时序数据库，运行时日志通过 Kafka 接入 ELK。

---

## 采集数据

6 类数据，9 个交易币种：BTC / ETH / BNB / XRP / SOL / ADA / DOGE / DOT / LTC

| 数据类型       | 采集方式   | 说明                         |
|--------------|----------|------------------------------|
| 现货 K线      | WebSocket | Binance `@kline_1s`         |
| 期货 K线      | REST轮询  | 1分钟K线，5秒轮询             |
| 现货盘口      | WebSocket | Binance `@depth20@100ms`    |
| 期货盘口      | REST轮询  | 2秒轮询                       |
| Funding Fee  | WebSocket | Binance `@funding`           |
| Open Interest| WebSocket | Binance `@open_interest`     |

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
Scraper (Go)
    |
    +-- WebSocket 实时数据 --> TimescaleDB
    |                               |
    |   现货K线 @kline_1s            |
    |   现货盘口 @depth20@100ms      |
    |   资金费率 @funding            |
    |   持仓量 @open_interest        |
    |
    +-- REST 轮询 --> TimescaleDB
    |                               |
    |   期货K线 (5秒间隔)             |
    |   期货盘口 (2秒间隔)            |
    |
    +-- Kafka --> ELK
        日志 (app-logs topic)
```

---

## 配置

配置文件：`config.yaml`

```yaml
symbols:
  - btcusdt
  - ethusdt
  - bnbusdt
  - xrpusdt
  - solusdt
  - adausdt
  - dogeusdt
  - dotusdt
  - ltcusdt

kline_interval: 2s
orderbook_interval: 2s
```

---

## 日志

运行时日志通过 Kafka 接入 ELK，不写本地文件。

- Topic: `app-logs`
- Broker: `kafka:29092`（Docker网络内）
- Partition: 8
- Consumer: 2 个 kafka-es-bridge 实例

---

## 启动

```bash
# Docker 部署
docker compose up -d

# 本地编译运行
go build -o bin/scraper ./cmd/scraper
./bin/scraper
```

---

## 状态

- 交易所：Binance（OKX 待实现）
- TimescaleDB 写入：✅
- Kafka 日志：✅
- Kafka 市场数据：✅ Live K-line 已接通（`md.kline.{exchange}.{market}.{interval}`，仅 closed bar）

## Live K-line Kafka

- Topic 规则：`md.kline.{exchange}.{market}.{interval}`，不带 `testnet` / `prod` 之类的环境段
- 当前范围：只发布 forward/live Binance K-line
- 发布条件：只在 K 线 closed/finalized 后写入 Kafka
- Reverse/backfill：只回写 TimescaleDB，不会发到 live Kafka topic

## 需求驱动采集控制面

`kline` v1 控制面已经接进 workspace，当前模型是：

- 用户只声明“需要什么流”，不直接 start / stop `scraper`
- `control-panel-service` 持久化三类状态：
  - `request`：某个用户声明的需求
  - `stream`：共享物理流的聚合状态
  - `lease`：某个 live session 当前仍在消费该流的 TTL 租约
- `scraper` 在 `market_data.control_plane.enabled=true` 时按数据库状态 reconcile `starting / running / draining / stopped / error`
- 运行中的 `kline` 仍然总是写 TimescaleDB；只有 `effective_live_delivery=true` 时才发 Kafka
- `strategy-service` 的 `mode=2` 启动前会做 readiness preflight，并在运行中续租 lease

当前产品边界：

- 只覆盖 `kline`
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
  - 例如 `binance_2025`、`binance_2026`
  - 请求只有在目标窗口可从 authoritative historical store 读出时才会进入 `ready`

当前库关系：

- live continuous ingestion / historical authoritative store:
  - `{exchange}_{year}`
  - 按 record timestamp/open_time/funding_time/event timestamp 路由到对应年份库
  - 面向实时 collector、freshness、backfill、coverage verify、`mode=0` / `mode=2`

这意味着：

- `mode=0` 只看 historical coverage，不看 live collector / Kafka
- `mode=2` 只看 live readiness / freshness；历史请求不会替代 live stream

## Rollout / Rollback

Rollout:

1. 先升级 `control-panel-service`，确保 market-data 控制面表和 RPC 已生效。
2. 再升级 `scraper`，让 historical worker 可以轮询请求并回报 `running / verifying / ready / error`。
3. 最后升级 `quant-handler` / `quant-frontend`，开放 `historical` / `live` 两类入口。
4. 验证：
   - historical 请求会落到 `{exchange}_{year}` 库
   - live 请求持续写 `{exchange}_{year}` 库并按 writer lease 保护写入所有权
   - `mode=0` 能通过历史 coverage preflight
   - `mode=2` 仍按 live readiness + Kafka delivery 判定

Rollback:

1. 如果 historical worker 不稳定，可以先停 `scraper` 的 historical runtime，只保留 live runtime。
2. 页面可以回退为只开放 `live` scope；现有 live request / stream / lease 不受影响。
3. 已写入的 `{exchange}_{year}` 历史库保持只读，不需要清理。
4. `mode=0` 仍可以直接按 Timescale 历史库运行，只是不会再通过 market-data 页面主动补数。
