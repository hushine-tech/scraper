# Market Data Control-Plane Deployment

最后核验：2026-08-28。

本文是当前部署检查清单，不是旧控制面兼容或回退方案。当前产品路径为：

- live：需求驱动的 managed K 线、session lease、finalized-bar Kafka delivery；
- historical：有限窗口的 K 线与 Futures Funding request/backfill/coverage；
- control-panel-service 是 request/stream/lease/history/coverage 状态 owner。

## 配置

`scraper/config.yaml`：

```yaml
market_data:
  control_plane:
    enabled: true
    market_data_control_panel_grpc: "127.0.0.1:50054"
    reconcile_interval_seconds: 5
    draining_grace_period_seconds: 60
    request_timeout_seconds: 15
```

control plane 启用时，Binance 的静态 forward K 线 collector 必须关闭，避免同一个 writer
domain 被静态和 managed collector 同时拥有。Historical runtime 仍由同一个 scraper 进程
轮询有限窗口 request；它不使用 live Kafka。

strategy Runtime 的 live readiness 和 lease 参数由当前 strategy-service/control-panel-service
配置生成。Hosted/self-hosted/bare Runtime 不能收到数据库、Kafka、账户或订单地址。

## 部署顺序

1. 对空 `control_panel` 执行当前 baseline，确认 market-data request/stream/lease/history/
   coverage 和 writer-lease 对象存在。
2. 启动 control-panel-service，验证 `:50054` 及 market-data RPC 可用。
3. 启动 scraper，确认它取得唯一 writer ownership，并同时启动 live reconcile 与 historical
   reconcile。
4. 启动 strategy-service、quant-handler 和 frontend。
5. 创建一条 live K 线 request，验证 stream 从 starting 进入 running、freshness 前进；请求
   live delivery 时还要验证 closed K 线进入正确 Kafka topic。
6. 创建一个 Futures K 线 historical request，确认系统配套创建 interval 为空的 Funding
   request；两者分别完成 backfill、coverage 和 ready。
7. 启动 demo/live session，确认 readiness 通过、lease 周期续租，停止后 lease 被释放或按
   TTL 到期。
8. 启动 Backtest，确认缺口只通过 historical request 补齐，RuntimeChannel page 同时携带
   K 线及 Funding coverage facts。

## 验收失败时

系统未上线，不保留旧协议或静态 K 线回退路径来掩盖控制面缺陷。出现下面任一情况时停止
本次环境的 session admission，保留 owner 状态和日志进行修复：

- 同一 writer domain 有多个 scraper owner；
- live stream 不新鲜或要求投递却没有 Kafka delivery；
- historical request 在数据/coverage 未完成时进入 ready；
- Funding request 带非空 interval，或把查询失败记录为零行 coverage；
- session 已停止但 lease 无法通过 cleanup/TTL 收敛；
- OKX 或不支持的 kind 没有 fail closed。

不要手工把 request/coverage 改成 ready，也不要删除共享数据库或 volume。只取消本次用户
拥有的 request/session；修复服务后让 reconcile 从持久化状态恢复。

## 观测面

- 用户 `/market-data` request/status 页面；
- control-panel-service stream/history/coverage/writer-lease 状态；
- strategy runtime diagnostics 与 session lease；
- scraper system log、Kafka/TimescaleDB 指标和 Jaeger trace。

当前没有管理员 override UI。任何后续管理员页面也必须调用 owner API，不能直接启停
scraper collector 或绕过 coverage 校验。
