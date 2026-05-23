# Market Data Control-Plane Rollout

这份文档记录 `kline` v1 控制面的 rollout / rollback 操作约定，重点覆盖：

- `scraper` 如何在需求驱动模式和静态配置模式之间切换
- `strategy-service` 如何开启 / 关闭 `mode=2` readiness preflight 与 lease 管理
- 第一次上线不稳定时，如何快速回退到“静态采集 + 无强制 preflight”的旧路径

## 当前范围

当前只覆盖：

- `kline`
- `mode=2`
- `request / stream / lease`
- `scraper` reconcile runtime
- `strategy-service` preflight / lease / diagnostics

当前不覆盖：

- `orderbook / funding / oi`
- 独立管理员 UI
- 强制 start / stop 某条共享流的人工 override

## 配置开关

### Scraper

`scraper/config.yaml`

```yaml
market_data:
  control_plane:
    enabled: true
    # Phase D2: control plane moved from account-service to control-panel-service.
    market_data_control_panel_grpc: "127.0.0.1:50054"
    reconcile_interval_seconds: 5
    draining_grace_period_seconds: 60
    request_timeout_seconds: 15
```

语义：

- `enabled=true`
  `scraper` 通过 control-plane state 管理受控 `kline` collector
- `enabled=false`
  `scraper` 回到静态 symbol 列表驱动；不再按 request / lease 动态启停

### Strategy-Service

`strategy-service/config.yaml`

```yaml
market_data:
  preflight_enabled: true
  lease_management_enabled: true
  lease_heartbeat_seconds: 30
  lease_ttl_seconds: 90
  freshness_grace_seconds: 30
```

语义：

- `preflight_enabled=true`
  `mode=2` 启动前强制校验 stream 存在、`running`、freshness 合格、`effective_live_delivery=true`
- `preflight_enabled=false`
  `mode=2` 跳过 control-plane readiness gate，恢复到旧的“直接尝试启动 live session”路径
- `lease_management_enabled=true`
  `strategy-service` 为运行中的 `mode=2` session 创建 / 续租 / 最佳努力释放 stream lease
- `lease_management_enabled=false`
  不再创建或续租 lease；已有 lease 行仅靠 TTL 自然过期

对应环境变量：

- `MARKET_DATA_PREFLIGHT_ENABLED`
- `MARKET_DATA_LEASE_MANAGEMENT_ENABLED`
- `MARKET_DATA_LEASE_HEARTBEAT_SECONDS`
- `MARKET_DATA_LEASE_TTL_SECONDS`
- `MARKET_DATA_FRESHNESS_GRACE_SECONDS`

## 首次 Rollout 建议

1. 保留 `scraper/config.yaml` 里的静态 `spot_symbols / futures_symbols / kline_intervals`
2. 打开 `scraper.market_data.control_plane.enabled=true`
3. 打开 `strategy-service.market_data.preflight_enabled=true`
4. 打开 `strategy-service.market_data.lease_management_enabled=true`
5. 通过产品页提交一个新的 `kline` request，确认 `stream.actual_state` 从 `starting` 进入 `running`
6. 确认 `effective_live_delivery=true` 时 `mode=2` 启动成功
7. 观察 `strategy-service` diagnostics 是否出现 live session / stream binding / lease heartbeat

建议第一次上线时不要删除静态 symbol 配置。即使 control-plane 出问题，也能在切回静态模式后立刻恢复采集。

## 快速 Rollback

如果首轮 rollout 不稳定，按下面顺序回退：

1. 在 `strategy-service` 上关闭强制 preflight

```yaml
market_data:
  preflight_enabled: false
  lease_management_enabled: false
```

2. 重启 `strategy-service`
3. 在 `scraper` 上关闭 control-plane runtime

```yaml
market_data:
  control_plane:
    enabled: false
```

4. 重启 `scraper`
5. 保留 `control-panel-service` / `control_panel` 库中已有的 `request / stream / lease` 记录，不做数据删除
6. 用静态配置继续保障基础 K-line 采集和 Kafka 发布

## Rollback 后的预期行为

- `mode=2` 不再因为 control-plane readiness 而返回 `FAILED_PRECONDITION`
- `strategy-service` 不再新建或续租 stream lease
- `scraper` 不再根据 request / lease 动态 start / drain / stop managed collector
- 已有 lease 会在 TTL 过后自然失效
- 控制面表仍然保留，但只作为惰性状态，不再驱动 collector lifecycle

## Rollback 后应检查的内容

- `scraper` 日志中不再持续出现 control-plane reconcile 相关事件
- 静态配置中的 `spot_symbols / futures_symbols / kline_intervals` 仍然能产出 K-line
- TimescaleDB 持续入库
- closed K-line 仍然按 `md.kline.{exchange}.{market}.{interval}` 发布
- `mode=2` live session 至少恢复到 rollout 前的行为，不再被 preflight 卡死

## Dedicated Admin UI

当前没有独立管理员控制台。第一阶段的真实观测面来自：

- 用户侧 `/market-data` request/status 页面
- `control-panel-service` 中的 stream state
- `strategy-service` diagnostics
- 各服务日志和 tracing

后续如果增加管理员 UI，应建立在这些接口之上，而不是绕过控制面真相源直接操作 `scraper`。
