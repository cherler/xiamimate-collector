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
- `scripts/manage_theme_feature_sync.sh`
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
4. `bash scripts/manage_theme_feature_sync.sh preview`

当前正式运行入口：

1. `bash scripts/manage_auto_collect.sh {install|start|stop|status|logs}`
2. `bash scripts/manage_pg_sync.sh {start|stop|restart|status|logs}`
3. `bash scripts/manage_theme_feature_sync.sh {start|stop|restart|status|logs}`
