# XiaMimate Collector

这个仓库是 XiaMimate 拆分后的离线采集与特征工程仓库。

当前状态：

- 由原始单仓库 `xiamimate` 在 2026-04-15 启动 Phase 2 时创建。
- 当前正式采集与同步已经切到本仓启动。
- 当前正式运行已经切到共享运行时根目录 `/path/to/xiamimate-runtime` 的 Python / DuckDB / raw products 路径；旧仓仅保留兼容 symlink。

当前已迁入内容：

- `data_collector/`
- `doc/`
- `scripts/manage_auto_collect.sh`
- `scripts/manage_pg_sync.sh`
- `scripts/manage_theme_sync.sh`
- `scripts/cleanup_raw_products.sh`
- `scripts/normalize_product_recall_query.py`

当前运行边界：

1. `auto-collect`、`sync_duckdb_to_pg`、`sync_theme_features_to_pg` 当前都应从本仓管理脚本启动。
2. 当前 PID / lock / log 已切到本仓 `logs/`。
3. DuckDB 与 raw JSON 已迁到仓库外共享运行时根目录；旧仓只保留路径兼容，不再承载运行态资产所有权。

Phase 2 已补齐：

1. 统一环境入口使用 `data_collector/.env`，模板见 `data_collector/.env.example`。
2. 三个管理脚本都支持 `preview`，只打印解析后的命令与共享路径，不启动进程。
3. 已提供 `bash scripts/dry_run_validate_collector.sh` 作为 Phase 2 只读校验入口。
4. 如果 `data_collector/.env` 没填完整，phase 2 会优先尝试回落到同级共享运行时根目录 `../xiamimate-runtime`，其次才回落到旧仓 `../xiamimate` 的兼容 symlink，以及同级 `../xiamimate-data-infra` 的 `init_sync_tables.sql`。

推荐环境模板：

1. 复制 `data_collector/.env.example` 为本地 `data_collector/.env`。
2. 至少填写以下路径变量：
   - `XIAMIMATE_RUNTIME_ROOT`
   - `XIAMIMATE_PYTHON_BIN`
   - `XIAMIMATE_DUCKDB_PATH`
   - `XIAMIMATE_RAW_PRODUCTS_DIR`
   - `XIAMIMATE_LOG_DIR`
   - `XIAMIMATE_INIT_SYNC_TABLES_SQL`
3. 如果当前目录结构仍是 `xiamimate-collector`、`xiamimate-runtime`、`xiamimate-data-infra` 同级，最少可以只填：
   - `XIAMIMATE_RUNTIME_ROOT=/path/to/xiamimate-runtime`
4. 如果当前目录结构还包含同级 `xiamimate-data-infra`，建议同时填：
   - `XIAMIMATE_DATA_INFRA_ROOT=/path/to/xiamimate-data-infra`
5. 当前正式运行推荐分别指向：
   - Python: `xiamimate-runtime/python/.venv/bin/python`
   - DuckDB: `xiamimate-runtime/duckdb/warehouse/local_analytics.duckdb`
   - raw products: `xiamimate-runtime/raw/json/products`
   - logs: 本仓 `logs/`
   - init sync SQL: `xiamimate-data-infra/postgres/init_sync_tables.sql`

说明：`xiamimate-data-infra` 现在已经承接 `postgres/init_sync_tables.sql` 这份当前 PostgreSQL bootstrap DDL。collector phase 2 默认会优先使用 data-infra 里的这份 SQL，同时优先使用 `xiamimate-runtime` 的共享 Python / DuckDB / raw 路径。
说明：当前不建议复制 `.venv` 目录。虚拟环境通常带有创建时的绝对路径，直接 copy 很容易失效；当前已验证的做法是把它整体迁到共享运行时根目录，并通过稳定路径引用。

只读校验：

1. `bash scripts/dry_run_validate_collector.sh`
2. `bash scripts/manage_auto_collect.sh preview`
3. `bash scripts/manage_pg_sync.sh preview`
4. `bash scripts/manage_theme_sync.sh preview`

当前正式运行入口：

1. `bash scripts/manage_auto_collect.sh {install|start|stop|status|logs}`
2. `bash scripts/manage_pg_sync.sh {install|start|stop|restart|status|uninstall|logs}`
3. `bash scripts/manage_theme_sync.sh {install|start|stop|restart|status|uninstall|logs}`

ECS2 systemd 入口：

1. 复制 `data_collector/.env.ecs2.example` 为 ECS2 上的 `data_collector/.env`，填入实际 Python、DuckDB、raw、日志和 PostgreSQL 目标配置。
2. 只读核对：`bash scripts/dry_run_validate_collector.sh`
3. 命令预览：
   - `bash scripts/manage_auto_collect.sh preview`
   - `bash scripts/run_pg_sync_once.sh`
   - `bash scripts/run_theme_feature_sync_once.sh`
4. 安装 systemd：`sudo bash scripts/manage_ecs2_collector_services.sh install all`
5. 查看状态：`sudo bash scripts/manage_ecs2_collector_services.sh status all`
6. 查看日志：
   - `sudo bash scripts/manage_ecs2_collector_services.sh logs auto`
   - `sudo bash scripts/manage_ecs2_collector_services.sh logs pg-sync`
   - `sudo bash scripts/manage_ecs2_collector_services.sh logs theme-sync`

统一管理入口：

1. 推荐优先使用 `bash scripts/manage_collector_jobs.sh help`
2. 查看所有任务状态：`bash scripts/manage_collector_jobs.sh status`
3. 预览所有任务命令：`bash scripts/manage_collector_jobs.sh preview`
4. 按任务转发：
   - `bash scripts/manage_collector_jobs.sh auto status`
   - `bash scripts/manage_collector_jobs.sh pg-sync preview`
   - `bash scripts/manage_collector_jobs.sh theme-sync logs`
   - `bash scripts/manage_collector_jobs.sh pg-tunnel status`
   - `bash scripts/manage_collector_jobs.sh week1 preview`

脚本职责边界：

1. `manage_pg_sync.sh` 是“DuckDB -> PostgreSQL 主同步循环”，同步规范化表和聚合表。
2. `manage_theme_sync.sh` 是“DuckDB -> PostgreSQL 主题特征同步循环”，同步在线 serving 用的主题特征子集（base/trends/cross）。
3. `manage_pg_ssh_tunnel.sh` 只负责 SSH 隧道，不做任何数据同步。
4. `manage_pg_sync.sh` 和 `manage_theme_sync.sh` 是否写本地 PostgreSQL、直连 RDS、还是通过 SSH 隧道写 RDS，不由脚本名决定，而由 `data_collector/.env` 里的 `PG_*` / `PG_TUNNEL_*` 配置决定。

RDS cutover 说明：

1. DuckDB 仍然是离线采集主库，迁移的是下游 PostgreSQL 镜像目标，不是把采集写入口改成 PostgreSQL。
2. 从“本地 DuckDB -> 本地 PostgreSQL”切到“ECS DuckDB -> RDS PostgreSQL”时，先停掉本地旧 writer，再启 ECS writer，避免双写：
   - `bash scripts/manage_pg_sync.sh stop`
   - `bash scripts/manage_theme_sync.sh stop`
3. 在 ECS 的 `data_collector/.env` 中显式填写 `PG_HOST`、`PG_PORT`、`PG_DB`、`PG_USER`、`PG_PASSWORD` 指向 RDS；现在 `preview` / `status` 会直接打印 `pg_target=...`，启动前先核对目标库。
4. `bash scripts/manage_pg_sync.sh preview` 与 `bash scripts/manage_theme_sync.sh preview` 现在会在目标库配置缺失时直接失败，避免脚本静默回落到 Python 默认的 `localhost`。
5. 如果要改成系统托管，优先执行：
   - `bash scripts/manage_pg_sync.sh install`
   - `bash scripts/manage_theme_sync.sh install`

ECS2 cutover 说明：

1. ECS2 不再依赖 macOS `launchd`，改用 `scripts/manage_ecs2_collector_services.sh` 生成并托管：
   - `xiamimate-auto-collect.service`
   - `xiamimate-pg-sync.service`
   - `xiamimate-theme-sync.service`
2. `auto-collect` 通过 `scripts/run_auto_collect_foreground.sh` 以前台方式交给 systemd 托管；崩溃后由 systemd 重启。
3. `pg-sync` 与 `theme-sync` 通过 `scripts/run_pg_sync_loop.sh`、`scripts/run_theme_sync_loop.sh` 按“单次执行 + sleep”循环运行，沿用现有 `run_*_once.sh` 的业务入口，不重写同步逻辑。
4. ECS2 推荐直接写 PostgreSQL 目标库，默认 `PG_TUNNEL_ENABLED=0`；只有 ECS2 到目标库仍然不通时，才再切回 SSH 隧道模式。
5. ECS2 的 live DuckDB 锁、PG/theme 定时任务互斥、补池 completed 后 scoped serving sync 的触发关系见 `doc/duckdb-locking-and-sync-orchestration.md`。

SSH 隧道同步模式：

1. 如果本地开发机不能直连 RDS，但可以 SSH 到 ECS，可在 `data_collector/.env` 中启用 `PG_TUNNEL_ENABLED=1`。
2. 建议保留 `PG_HOST` / `PG_PORT` 为真实 RDS 地址，再额外填写：
   - `PG_TUNNEL_SSH_HOST=your-ssh-host`
   - `PG_TUNNEL_LOCAL_HOST=127.0.0.1`
   - `PG_TUNNEL_REMOTE_HOST=your-instance.pg.rds.aliyuncs.com`
   - `PG_TUNNEL_REMOTE_PORT=5432`
3. 如果多个本地服务都启用 SSH 隧道，默认按服务分配独立本地端口，避免同时启动时争抢同一个 listen port：
   - `auto-collect`: `PG_TUNNEL_LOCAL_PORT=15432`
   - `pg-sync`: `PG_SYNC_TUNNEL_LOCAL_PORT=15433`
   - `theme-sync`: `THEME_FEATURE_SYNC_TUNNEL_LOCAL_PORT=15434`
   - `theme-api`: 建议 `PG_PORT=15435`
   - `chat-backend`: 建议 `PG_PORT=15436`
4. 启用后，各同步任务会在每一轮真正执行前确保自己的 SSH 隧道可用；默认 keepalive，不在每轮结束后关闭。只有显式设置 `THEME_FEATURE_SYNC_TUNNEL_KEEPALIVE=false` 时，theme-sync 才会按单次任务生命周期关闭自己的隧道。
5. 当 `pg sync` 将补池任务从 `syncing` 协调为 `completed` 后，外层 `run_pg_sync_once.sh` 会在释放 live DuckDB 锁后触发一次补池 scoped refresh，让 `serving.theme_base_daily` 尽快追上最新补池 ASIN。互斥细节见 `doc/duckdb-locking-and-sync-orchestration.md`。
6. auto-collect 的通用隧道也可以单独管理：
   - `bash scripts/manage_pg_ssh_tunnel.sh preview`
   - `bash scripts/manage_pg_ssh_tunnel.sh status`
   - `bash scripts/manage_pg_ssh_tunnel.sh start`
   - `bash scripts/manage_pg_ssh_tunnel.sh stop`
7. 当前 `manage_pg_sync.sh preview` / `manage_theme_sync.sh preview` 会打印“单次同步命令”和“循环命令”，方便确认是否已经切到“按轮次开关隧道”的运行模式。

PG sync 性能说明：

1. 当前 `sync_duckdb_to_pg.py` 的主要瓶颈通常不是 `keepa_product_history` 小增量，而是两个聚合表：
   - `sync.keepa_history_domain_daily`
   - `sync.keepa_history_root_category_daily`
2. 这两个表当前按 `PG_AGG_REFRESH_INTERVAL_SECONDS` 控制最小重刷间隔，默认 `3600` 秒；在间隔未到时，日志会显示 `skipped (refresh interval ... not reached)`，这是预期行为，不是异常。
3. PostgreSQL 批量写入页大小由 `PG_SYNC_BATCH_SIZE` 控制，默认 `2000`；如果本地机器和 RDS 链路稳定、单批数据量大，可以继续调高测试。
4. DuckDB 结果读取批次由 `PG_SYNC_FETCH_BATCH_SIZE` 控制，默认 `10000`；它决定 `sync_duckdb_to_pg.py` 每次从 DuckDB 拉多少行进入内存，再拆成 `PG_SYNC_BATCH_SIZE` 小批写入 PostgreSQL。通常应保持 `PG_SYNC_FETCH_BATCH_SIZE >= PG_SYNC_BATCH_SIZE`。

Theme feature sync 性能说明：

1. `THEME_FEATURE_REFRESH_OVERLAP_DAYS` 控制 PostgreSQL 增量回补窗口；如果任务是每天跑一次，通常不需要保留到 `35` 天这么大，先降到 `7` 或 `14` 更合理。
2. `THEME_FEATURE_DUCKDB_THREADS` 控制 `sync_theme_features_to_pg.py` 在 DuckDB 内部构建临时特征表时使用的线程数，默认 `4`；如果本地机器负载高，可以降到 `2` 或 `1`。
3. 当前 `sync_theme_features_to_pg.py` 会按“本轮最早 refresh_start + 7 天 lookback”构建 DuckDB 临时特征表，而不是再按整段 retention 窗口构建，从而减少临时表规模与本地内存/磁盘压力。
