# 预测特征矩阵 — 技术说明文档

## 一、整体架构

```
┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐
│   Keepa API      │    │  Google Trends    │    │  商品元数据       │
│  (历史时间序列)    │    │  (搜索热度)       │    │  (keyword/HS编码) │
└──────┬───────────┘    └──────┬───────────┘    └──────┬───────────┘
       │                       │                       │
       ▼                       ▼                       ▼
┌──────────────────────────────────────────────────────────────────┐
│              FeatureMatrixBuilder                                 │
│  1. 数据清洗 & 类型转换                                            │
│  2. 前向填充 (ffill) 稀疏数据                                      │
│  3. BSR → 日销量估算 (幂律模型)                                    │
│  4. 派生特征 (折扣率, BSR变化率, 评论增速...)                       │
│  5. 合并 Google Trends (周→日 插值)                                │
│  6. 时间特征 (day_of_week, month, is_weekend...)                  │
│  7. 滞后 & 滚动特征 (lag_1/7/14/30, rolling_mean/std)            │
└──────────────────────────┬───────────────────────────────────────┘
                           ▼
              prediction_feature_matrix.csv
              (63 列, 每日一行, 每个 ASIN 独立时间序列)
```

---

## 二、Keepa API 字段含义与空值说明

### 2.1 核心字段含义

| 字段 | Keepa 来源 | 含义 | 单位 |
|------|-----------|------|------|
| `amazon_price` | csv[0] AMAZON | Amazon 自营的价格 | 美元 (已从分转换) |
| `new_price` | csv[1] NEW | 第三方卖家新品最低价 | 美元 |
| `used_price` | csv[2] USED | 二手商品最低价 | 美元 |
| `buy_box_price` | csv[18] BUY_BOX_SHIPPING | Buy Box 价格 (含运费) | 美元 |
| `list_price` | csv[4] LISTPRICE | 标价/建议零售价 (MSRP) | 美元 |
| `bsr` | csv[3] SALES | Best Sellers Rank (销量排名) | 排名数字 |
| `rating` | csv[16] RATING | 商品评分 | 0.0-5.0 (已从0-50转换) |
| `review_count` | csv[17] COUNT_REVIEWS | 累计评论数 | 个 |
| `monthly_sold` | monthlySoldHistory | Amazon "X+ bought in past month" 标签 | 月销量 (阶梯值) |
| `new_offer_count` | csv[11] COUNT_NEW | 新品卖家数量 | 个 |
| `used_offer_count` | csv[12] COUNT_USED | 二手卖家数量 | 个 |

### 2.2 为什么字段值经常为空？

**关键概念: Keepa 的"变化点记录"机制**

Keepa **不是**每天记录一次所有字段。它采用的是**事件驱动记录**: 只有当某个字段的值发生变化时, 才记录一条 `[时间戳, 新值]`。

这意味着:
- 某一天可能只有 `bsr` 变了 → 只有 `bsr` 有值, 其他都是空
- 某一天可能只有 `amazon_price` 变了 → 只有价格有值, BSR 是空的
- 连续几天没有任何变化 → 这几天完全没有记录

**空值的 3 种含义:**

| 情况 | 含义 | 处理方式 |
|------|------|---------|
| 空但之前有值 | 值没变, Keepa 没记录 | 前向填充 (ffill): 沿用上一个已知值 |
| 值 = `-1` | 该指标**不可用** (商品下架/缺货/价格不存在) | 转为 `None` |
| 从未出现过值 | 该商品/站点不支持该指标 | 保持 `None` |

**具体到每个字段:**

- `amazon_price` 空 = Amazon 不是卖家 (第三方卖家商品), 或者 Amazon 暂时缺货
- `buy_box_price` 空 = 没有 Buy Box (多卖家竞争中, 或商品下架)
- `used_price` 空 = 没有二手在售
- `list_price` 空 = 卖家没设置建议零售价
- `monthly_sold` 空 = Amazon 没有展示 "bought in past month" 标签 (销量太低, 或该品类不展示)
- `rating` 空 = 评分还没变化 (评分更新频率远低于价格)
- `new_offer_count` 空 = 卖家数没变

### 2.3 Keepa API 其他重要结构

**salesRanks (多品类排名)**
```json
{
  "172282": [时间戳, BSR, ...],    // 根品类 Electronics 的排名
  "1232597011": [时间戳, BSR, ...], // 子品类 Tablets 的排名
}
```
csv[3] 中的 BSR 是 **根品类排名** (salesRankReference 指定的品类)。
`salesRanks` 对象包含所有品类的排名, 更精细。

**salesRankDrops (BSR 下降次数)**
- `salesRankDrops30/90/180/365`: 过去 N 天内 BSR 下降的次数
- BSR 下降通常意味着有销售 → 近似为成交笔数
- 但会低估: 同一小时多笔成交只算一次 drop

**monthlySoldHistory (月销量标签)**
- Amazon 页面上的 "2K+ bought in past month" 标签
- 阶梯值: 50, 100, 200, 300, 400, 500, 1K, 2K, 5K, 10K...
- 不是精确值, 但是最接近真实销量的公开数据
- 很多商品没有这个标签 (销量太低或刚上架)

**stats (统计摘要)**
- `stats.current[i]`: 各 csv 指标的当前值
- `stats.avg30[i]`: 30天均值
- `stats.salesRankDrops30`: 30天 BSR 下降次数

---

## 三、BSR → 日销量转换

### 3.1 为什么 BSR 能推算销量？

Amazon BSR 的更新机制:
1. 每次有订单 → BSR 下降 (排名提升)
2. 没有订单 → BSR 缓慢上升 (排名下降)
3. BSR 与销量之间存在**幂律关系**: `daily_sales = a × BSR^b`

### 3.2 三种估算方法 (按优先级)

```
  优先级1: monthly_sold (Amazon官方标签, 最可信, 但只有部分商品有)
     ↓ 没有
  优先级2: salesRankDrops30 (Keepa统计的BSR下降次数, × 1.3 校正)
     ↓ 没有
  优先级3: BSR 幂律模型 (品类回归系数)
```

### 3.3 幂律模型系数

| 品类ID | 品类名 | 系数 a | 系数 b | BSR=100 时日销量 | BSR=1000 时日销量 |
|--------|-------|--------|--------|----------------|-----------------|
| 172282 | Electronics | 54670 | -0.822 | 1245 | 199 |
| 1055398 | Home & Kitchen | 33600 | -0.740 | 1018 | 193 |
| 468642 | Toys & Games | 74890 | -0.850 | 1491 | 211 |
| 283155 | Books | 102200 | -0.900 | 1620 | 204 |
| - | General (fallback) | 32000 | -0.750 | 1000 | 178 |

> 注意: 系数是 Amazon US 的近似值。不同站点有销量倍率调整 (UK×0.3, DE×0.35, JP×0.4)。

### 3.4 验证: BSR模型 vs monthly_sold

从测试数据看:
- BSR=136, 品类=Electronics → BSR模型估算 ≈ 1079/天, monthly_sold标签=500/月(≈17/天)
- 差异较大! 原因: BSR模型给出的是**该品类整体**的近似, 而 monthly_sold 是该**具体商品**的

**重要**: BSR 幂律模型的绝对值不够准确, 但**趋势变化**是可靠的:
- BSR 从 1000降到500 → 销量翻倍, 这个相对变化是稳定的
- 建模时建议使用 `log_bsr` 和 `bsr_change_pct` 而非绝对销量值

---

## 四、特征矩阵字段说明 (63 列)

### 4.1 原始数据 (17列)
来自 Keepa 历史 CSV, 经过前向填充:
```
asin, product_title, brand, category, marketplace, date,
amazon_price, new_price, used_price, buy_box_price, list_price,
bsr, rating, review_count, monthly_sold, new_offer_count, used_offer_count
```

### 4.2 销量估算 (4列)
```
est_daily_sales_bsr      — 仅基于 BSR 幂律模型
est_daily_sales_monthly  — 仅基于 monthly_sold ÷ 30
estimated_daily_sales    — 综合估算 (优先 monthly_sold > BSR)
sales_estimation_method  — 使用的方法 ("monthly_sold"/"bsr_power_law"/"no_data")
```

### 4.3 派生特征 (8列)
```
price_discount_pct  — 折扣百分比: (list_price - amazon_price) / list_price × 100
effective_price     — 实际售价: buy_box > amazon_price > new_price
bsr_change          — BSR 日变化量 (正=排名下降/销量减少, 负=排名上升/销量增加)
bsr_change_pct      — BSR 日变化百分比
price_change        — 价格日变化量
price_change_pct    — 价格日变化百分比
review_velocity     — 评论日增量
log_bsr             — BSR 对数值 (幂律关系在对数空间是线性的, 更适合建模)
```

### 4.4 Google Trends (2列)
```
trend_index     — 搜索热度 (0-100), 从周级数据线性插值到日级
search_volume   — 搜索量 (如果有)
```

### 4.5 时间特征 (6列)
```
day_of_week   — 星期几 (0=周一, 6=周日)
day_of_month  — 几号
week_of_year  — 第几周
month         — 月份
is_weekend    — 是否周末
time_idx      — 距时间序列起点的天数
```

### 4.6 滞后特征 (16列)
```
{指标}_lag_1/7/14/30  — 1/7/14/30天前的值
适用指标: estimated_daily_sales, bsr, effective_price, review_count
```

### 4.7 滚动特征 (10列)
```
{指标}_roll_mean_7/14/30  — 7/14/30天滚动均值
{指标}_roll_std_7         — 7天滚动标准差 (波动性)
trend_index_lag_7         — 搜索热度7天前值
trend_index_roll_mean_7   — 搜索热度7天滚动均值
```

> 所有 lag/rolling 特征使用 `shift(1)` 避免未来信息泄漏。

---

## 五、使用方法

### 5.1 步骤1: 采集 Keepa 历史数据
```bash
python -m data_collector.cross_border_data keepa-history \
  --asin B09V3KXJPB \
  --domain 1 \
  --output outputs/keepa_B09V3KXJPB.csv \
  --raw-output outputs/keepa_B09V3KXJPB_raw.json
```

### 5.2 步骤2: 采集 Google Trends 数据
```bash
python -m data_collector.cross_border_data google-trends \
  --keyword "ipad air" \
  --geo US \
  --timeframe "today 12-m" \
  --output outputs/trends_ipad_air.csv
```

### 5.3 步骤3: 构建特征矩阵
```bash
python -m data_collector.sales_forecast build-feature-matrix \
  --keepa-file outputs/keepa_B09V3KXJPB.csv \
  --trend-file outputs/trends_ipad_air.csv \
  --trend-keyword "ipad air" \
  --domain 1 \
  --category-id 172282 \
  --output-dir outputs/features/
```

### 5.4 步骤4: 查看品类系数 & 调参
```python
from data_collector.sales_forecast.bsr_sales_converter import (
    CATEGORY_COEFFICIENTS,
    bsr_to_daily_sales,
    estimate_daily_sales,
)

# 查看所有品类系数
for cid, coeff in CATEGORY_COEFFICIENTS.items():
    print(f"{cid}: {coeff.category_name} → a={coeff.coeff_a}, b={coeff.coeff_b}")

# 自定义系数
daily = bsr_to_daily_sales(500, custom_a=40000, custom_b=-0.78, domain=1)
```

---

## 六、后续待做

1. **用 monthly_sold 校准 BSR 系数** — 收集更多有 monthly_sold 的商品, 用线性回归拟合更准确的 a, b 系数
2. **SubCategory BSR** — 目前只用根品类 BSR, 可以用 `salesRanks` 中子品类的排名进一步细化
3. **多 ASIN 批量采集** — Keepa 支持一次请求最多 100 个 ASIN, 但消耗 token 较多
4. **滚动训练窗口** — 实现滑动窗口训练策略
5. **模型训练** — XGBoost 回归或 PyTorch Temporal Fusion Transformer
