# 销量预测数据集构建

这个模块只负责一件事：把已经采集好的跨境电商 CSV 数据，整理成可直接用于销量预测建模的数据集。

## 当前目标

当前默认预测目标是 `estimated_sales`，也就是可观测销量代理值。

这不是平台后台真实销量，因此应理解为：

- 模型预测的是“销量估算值 / 销量代理值”
- 更适合做排序、选品优先级判断、相对销量判断
- 不适合直接当财务级收入预测

## 支持的输入

### 1. 商品监控数据

来自标准化后的 `product_tracking_data`：

- Keepa
- 卖家精灵导出后的标准化结果
- 其他兼容同字段结构的商品监控 CSV

### 2. 趋势数据

来自标准化后的 `traffic_trend_data`：

- Google Trends

### 3. 宏观贸易数据

来自标准化后的 `macro_trade_data`：

- UN Comtrade
- Eurostat

### 4. 商品元数据映射

用于补齐建模时必须的映射字段：

- `asin`
- `keyword`
- `country`
- `hs_code`
- `product_group_id`

样例文件：

- `data_collector/examples/sales_forecast_product_metadata_sample.csv`

## 输出说明

### 1. `base_training_dataset.csv`

这是所有来源合并后的基础训练表，保留了绝大多数中间字段，适合做数据排查和特征复查。

### 2. `pytorch_forecasting_dataset.csv`

这是偏长表结构的数据，已经包含：

- `series_id`
- `time_idx`
- `target`
- 静态特征
- 已知时间特征
- 动态商品特征
- 滞后与滚动特征

后续适合直接接 `pytorch_forecasting.TimeSeriesDataSet`。

### 3. `xgboost_dataset.csv`

这是偏宽表结构的数据，适合直接做表格回归。常见用法是：

- `target` 作为标签
- 其余数值特征作为输入
- 类别特征后续做编码

### 4. `dataset_manifest.json`

记录本次构建的：

- 样本量
- 时间范围
- 分组数量
- 特征清单
- 目标列定义

## 命令示例

```bash
python -m data_collector.sales_forecast build-dataset \
  --product-file data_collector/outputs/smoke_sellersprite.csv \
  --metadata-file data_collector/examples/sales_forecast_product_metadata_sample.csv \
  --output-dir data_collector/forecast_datasets/demo
```

## 当前特征工程策略

### 商品侧

- `price`
- `list_price`
- `bsr`
- `rating`
- `review_count`
- `seller_count`
- `price_discount_ratio`

### 趋势侧

- `trend_index`
- `search_volume`
- `estimated_traffic`

### 宏观侧

- `macro_trade_value`
- `macro_quantity`

### 时间侧

- `month`
- `week_of_year`
- `day_of_week`
- `day_of_month`
- `time_idx`

### 历史统计侧

- `estimated_sales_lag_1`
- `estimated_sales_lag_7`
- `price_lag_1`
- `price_lag_7`
- `bsr_lag_1`
- `bsr_lag_7`
- `review_count_lag_1`
- `review_count_lag_7`
- `trend_index_lag_1`
- `trend_index_lag_7`
- `estimated_sales_roll_mean_7`
- `estimated_sales_roll_mean_14`
- `price_roll_mean_7`
- `bsr_roll_mean_7`
- `review_count_roll_mean_7`

## 注意事项

1. 如果 `keyword`、`country`、`hs_code` 在商品表里缺失，建议一定提供元数据映射文件。
2. 如果某个来源的字段不是每日更新，构建器会按日期尽量对齐，但不会伪造目标值。
3. 当前默认按 `group_id` 建序列；如果你有父子体逻辑，建议在元数据里显式设置 `product_group_id`。
4. 训练集只保留满足最少历史行数且目标列非空的样本。
