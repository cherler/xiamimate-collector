# DuckDB 锁与同步任务编排

本文档记录 ECS2 collector 上 live DuckDB、PostgreSQL 同步、theme serving 同步、补池外部触发任务之间的锁、互斥关系和优先级。目标是避免补池任务进入 `completed` 后，`serving.theme_*` 没有补齐，最终停在 `serving_sync_pending`。

## 任务类型

### 实时采集任务

- systemd: `xiamimate-auto-collect.service`
- 入口: `scripts/run_auto_collect_foreground.sh`
- 主要资源: `/data/xiamimate/duckdb/live/local_analytics.duckdb`
- 职责: Keepa/Trends 采集、补池 hydration、写入 live DuckDB。
- 锁: 进入 `run_auto_collect()` 后持有 `XIAMIMATE_DUCKDB_ACCESS_LOCK_FILE`，退出本轮采集后释放。

### 高频 PG sync 定时任务

- systemd: `xiamimate-pg-sync-snapshot.service` + `xiamimate-pg-sync-snapshot.timer`
- 入口: `scripts/run_pg_sync_once.sh`
- 当前 ECS2 形态: `PG_SYNC_DUCKDB_SOURCE=live`，`PG_SYNC_REFRESH_SNAPSHOT=false`。
- 职责: 将 live DuckDB 中的 registry/snapshot/history/mapping 等同步到 PostgreSQL，并把补池 job 从 `syncing` 协调到 `completed`。
- 锁: 读取 live DuckDB 前申请 `XIAMIMATE_DUCKDB_ACCESS_LOCK_FILE`；同步进程自身用 `sync_duckdb_to_pg.lock` 保证同类 PG sync 单实例。

### 通用 theme-sync 定时任务

- systemd: `xiamimate-theme-sync-snapshot.service` + `xiamimate-theme-sync-snapshot.timer`
- 入口: `scripts/run_theme_feature_sync_once.sh`
- 默认调度: 每天 `01:00`。
- 数据源: `/data/xiamimate/duckdb/snapshots/current/local_analytics.duckdb`，必要时先发布 snapshot。
- 职责: 面向全局 serving 的日常 `serving.theme_base_daily`、`serving.theme_trends_daily`、`serving.theme_cross_daily` 刷新。
- 说明: 这不是补池 completed 后触发的 scoped serving sync。两者复用同一个底层 theme feature sync 脚本，但调用方、数据源、目标 ASIN 范围和日志归属不同。

### 补池 completed 后的 scoped serving sync

- 触发方: `scripts/run_pg_sync_once.sh` 在 PG sync 完成并释放 live DuckDB 锁后触发。
- 入口: `scripts/run_candidate_expansion_refresh_once.sh`
- 数据源: live DuckDB 先构建补池 job 的 mini subset DuckDB，然后 scoped PG sync 和 scoped theme sync 都读这个 subset。
- 范围: 只针对 `CANDIDATE_EXPANSION_JOB_IDS` 对应 job 的 ASIN/domain。
- 职责: 让刚补池完成的 ASIN 尽快进入 `serving.theme_*`，使 theme-api 的 `data_readiness.analysis_ready` 变为 true。
- 日志: 由 PG sync 服务触发时，输出进入 `pg_sync.timer.log`；不是 `xiamimate-theme-sync-snapshot.service` 的一次 systemd run。

## 锁文件

### `duckdb_live_access.lock`

路径由 `XIAMIMATE_DUCKDB_ACCESS_LOCK_FILE` 指定，ECS2 默认是 `/data/xiamimate/collector/logs/duckdb_live_access.lock`。

用途:

- 保护 live DuckDB 文件，避免 auto-collect 写入、PG sync live 读取、snapshot publish、补池 subset build 同时打开 live DB。
- 这是跨任务的 live DuckDB 访问锁，不是业务任务锁。

规则:

- 任何直接访问 live DuckDB 的流程必须持有这把锁。
- 已持有这把锁的父流程不能再同步调用一个会重新 `flock -x` 同一文件的子流程，否则会造成父子自锁。
- 如果确实需要在同一个 live DuckDB critical section 内调用子脚本，子脚本只能使用 `CANDIDATE_EXPANSION_DUCKDB_LOCK_MODE=inherit`，并且只能由确定已持锁的父流程设置。
- 当前唯一受支持的补池→serving 触发链路是 deferred：PG sync Python 把 completed job ids 写入临时文件后退出，释放 live DuckDB 锁与 `sync_duckdb_to_pg.lock`，再由 `run_pg_sync_once.sh` 调用 `run_candidate_expansion_refresh_once.sh`。inline 触发路径已下线，避免任何父子自锁回退。
- 锁文件以 append 模式（`exec 8>>"$LOCK_FILE"`）打开后再 `flock -x -w`，等待时不会清空当前持有者写入的 `pid/role/acquired_at`，`cat duckdb_live_access.lock` 在排障时一直可用。

### `sync_duckdb_to_pg.lock`

用途:

- 保证 `sync_duckdb_to_pg.py` 同类进程单实例，避免多个 PG sync 同时写同一批 PostgreSQL 表。

规则:

- 普通 PG sync 默认不等待，发现已有进程就退出。
- 补池 scoped PG sync 由 `run_candidate_expansion_refresh_once.sh` 设置 `PG_SYNC_PROCESS_LOCK_TIMEOUT_SECONDS=900`，撞上定时 PG sync 时等待，而不是立即失败。
- PG sync completed 后的 scoped refresh 已经移到父 PG sync 进程锁释放之后触发，避免自己调用自己导致 `sync_duckdb_to_pg.lock` 自锁。

### `sync_theme_features_to_pg.lock`

用途:

- 保证 `sync_theme_features_to_pg.py` 同类进程单实例，避免通用 theme-sync 和 scoped theme-sync 同时改 `serving.theme_*`。

规则:

- 普通 theme-sync 默认不等待，发现已有进程就退出。
- 补池 scoped theme-sync 由 `run_candidate_expansion_refresh_once.sh` 设置 `THEME_FEATURE_SYNC_PROCESS_LOCK_TIMEOUT_SECONDS=900`，撞上通用 theme-sync 时等待。
- scoped theme-sync 的数据源是 mini subset DuckDB，并带 `THEME_FEATURE_TARGET_ASINS` / `THEME_FEATURE_TARGET_DOMAINS`，不是全局 snapshot refresh。

### `publish_duckdb_snapshot.lock`

用途:

- 串行化 snapshot 发布，避免多个 snapshot publisher 同时改 `/data/xiamimate/duckdb/snapshots/current`。

规则:

- 只有需要发布 snapshot 的任务进入这把锁。
- ECS2 当前推荐由 PG sync 或明确的 snapshot publisher 负责发布 current snapshot；补池 scoped refresh 默认 `CANDIDATE_EXPANSION_REFRESH_SNAPSHOT=false`，不发布 snapshot。

### `auto_collect.lock`

用途:

- 保证 auto-collect 服务自身单实例。
- 它不是 live DuckDB 访问锁，不能替代 `duckdb_live_access.lock`。

## 优先级与互斥口径

1. live DuckDB 文件一致性优先级最高。任何 live 访问都必须走 `duckdb_live_access.lock`。
2. 用户触发补池属于交互链路，优先级高于普通 daily serving refresh；但它不能绕过 live DuckDB 保护，只能缩短 live 锁持有时间。
3. auto-collect 是唯一 live DuckDB writer，应通过小批次和短 critical section 降低对 PG sync / 补池 subset build 的阻塞。
4. 高频 PG sync 负责把补池 job 从 `syncing` 协调到 `completed`，但它不在 Python 进程内部直接跑 scoped serving sync。completed job ids 交回 shell wrapper，释放锁后再触发。
5. 通用 theme-sync 定时任务与补池 scoped theme-sync 共享 `sync_theme_features_to_pg.lock`。如果冲突，补池 scoped sync 等待默认 900 秒。
6. 补池 scoped refresh 默认流程是: acquire live DuckDB lock -> build mini subset -> release live DuckDB lock -> scoped PG sync from subset -> scoped theme sync from subset。
7. 不要手动把 `CANDIDATE_EXPANSION_DUCKDB_LOCK_MODE=inherit` 用在 standalone 命令上。standalone 补池 refresh 应使用默认 `acquire`。

## 补池 completed 后的正确链路

正常情况下，链路应为:

1. auto-collect 完成补池发现与 hydration，写 live DuckDB。
2. 高频 PG sync 申请 `duckdb_live_access.lock`，读取 live DuckDB，同步 registry/history 到 PostgreSQL。
3. `sync_duckdb_to_pg.py` 将满足条件的 job 标记为 `completed`，并把 completed job ids 写到 `PG_SYNC_RECONCILED_EXPANSION_JOB_IDS_FILE`。
4. `run_pg_sync_once.sh` 释放 `duckdb_live_access.lock`，Python 进程退出并释放 `sync_duckdb_to_pg.lock`。
5. `run_pg_sync_once.sh` 调用 `run_candidate_expansion_refresh_once.sh`。
6. `run_candidate_expansion_refresh_once.sh` 申请 `duckdb_live_access.lock`，从 live DuckDB 构建 job ASIN mini subset，然后释放 live 锁。
7. scoped PG sync 从 subset 同步必要表，且禁用二次 theme trigger。
8. scoped theme-sync 从 subset 写 `serving.theme_base_daily`、`serving.theme_trends_daily`、`serving.theme_cross_daily`。
9. theme-api 再查 job readiness 时，`serving_base_hit_count >= ready_threshold`，`analysis_ready=true`。

注意:

- reconcile pass 1 只把 `status='syncing'` 的 job 推进到 `completed`，不会把已经 `completed` 的 job 误回退到 `hydrating`。
- subset DuckDB 文件由 `run_candidate_expansion_refresh_once.sh` 的 EXIT trap 清理。万一 wrapper 崩溃留下垃圾文件，`check_ecs2_collector_health.sh` 每 5 分钟会删除 `CANDIDATE_EXPANSION_DUCKDB_SUBSET_DIR` 下超过 `CANDIDATE_EXPANSION_DUCKDB_SUBSET_TTL_MINUTES`（默认 360 分钟）的 `*.duckdb*` / `*.manifest.json`。

## 排查命令

查看当前锁持有者:

```bash
cat /data/xiamimate/collector/logs/duckdb_live_access.lock || true
lsof /data/xiamimate/collector/logs/duckdb_live_access.lock || true
```

查看补池 serving trigger 是否完成:

```bash
tail -120 /data/xiamimate/collector/logs/pg_sync.timer.log
```

查看通用 theme-sync 定时任务:

```bash
systemctl status xiamimate-theme-sync-snapshot.service --no-pager -l
tail -120 /data/xiamimate/collector/logs/theme_sync.timer.log
```

手动补跑某个 completed job 的 scoped refresh:

```bash
cd /opt/xiamimate/xiamimate-collector
CANDIDATE_EXPANSION_JOB_IDS=kexp_xxx \
  CANDIDATE_EXPANSION_DUCKDB_SOURCE=live \
  CANDIDATE_EXPANSION_REFRESH_SNAPSHOT=false \
  bash scripts/run_candidate_expansion_refresh_once.sh
```

手动命令不要设置 `CANDIDATE_EXPANSION_DUCKDB_LOCK_MODE=inherit`。
