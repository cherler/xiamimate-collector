"""BSR (Best Sellers Rank) → 日均销量转换器.

Amazon 的 BSR 与销量之间存在幂律关系:
    daily_sales ≈ a × BSR^b

其中 a, b 是品类相关的经验系数, b 为负数(排名越低, 销量越高).

本模块提供三种估算方式, 按优先级:
1. monthlySoldHistory  — Amazon 官方 "X+ bought in past month" 标签, 最可信
2. salesRankDrops      — Keepa 统计的 BSR 下降次数, 近似为成交笔数
3. 幂律模型           — 用品类系数将 BSR 映射到日销量
"""

from __future__ import annotations

import math
from dataclasses import dataclass


# ---------------------------------------------------------------------------
# Amazon US 主品类的经验回归系数
# 系数来源: 公开的行业经验数据与反算校准
# 公式: daily_sales = coeff_a * bsr ^ coeff_b
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class CategoryCoefficients:
    category_id: int
    category_name: str
    coeff_a: float
    coeff_b: float


# 主品类 ID → 回归系数
# 系数是对 Amazon US 站点的近似估算, 其他站点需要调整
CATEGORY_COEFFICIENTS: dict[int, CategoryCoefficients] = {
    172282:    CategoryCoefficients(172282,    "Electronics",              54670,  -0.822),
    1055398:   CategoryCoefficients(1055398,   "Home & Kitchen",           33600,  -0.740),
    3375251:   CategoryCoefficients(3375251,   "Clothing, Shoes & Jewelry", 30200, -0.730),
    468642:    CategoryCoefficients(468642,    "Toys & Games",             74890,  -0.850),
    3760901:   CategoryCoefficients(3760901,   "Sports & Outdoors",        27300,  -0.715),
    228013:    CategoryCoefficients(228013,    "Tools & Home Improvement",  16780, -0.685),
    283155:    CategoryCoefficients(283155,    "Books",                    102200, -0.900),
    2619525011: CategoryCoefficients(2619525011, "Beauty & Personal Care",  28500, -0.720),
    2972638011: CategoryCoefficients(2972638011, "Health & Household",      22400, -0.700),
    16310101:  CategoryCoefficients(16310101,  "Grocery & Gourmet Food",   15200, -0.668),
    2350149011: CategoryCoefficients(2350149011, "Pet Supplies",            18900, -0.692),
    165796011: CategoryCoefficients(165796011, "Baby",                     20100, -0.705),
    11091801:  CategoryCoefficients(11091801,  "Office Products",          12500, -0.660),
    541966:    CategoryCoefficients(541966,    "Computers & Accessories",   34500, -0.760),
}

# 通用 fallback 系数 (当品类未知时使用)
DEFAULT_COEFFICIENTS = CategoryCoefficients(0, "General (fallback)", 32000, -0.750)

# Amazon 不同站点的销量倍率 (相对于 US=1.0)
# US 市场最大, 其他站点按经验比例缩放
DOMAIN_MULTIPLIER: dict[int, float] = {
    1: 1.0,    # US
    2: 0.30,   # UK
    3: 0.35,   # DE
    4: 0.15,   # FR
    5: 0.40,   # JP
    6: 0.20,   # CA
    8: 0.12,   # IT
    9: 0.10,   # ES
}


def bsr_to_daily_sales(
    bsr: int | float,
    *,
    category_id: int | None = None,
    domain: int = 1,
    custom_a: float | None = None,
    custom_b: float | None = None,
) -> float | None:
    """将 BSR 转换为估算日均销量.

    Parameters
    ----------
    bsr : int
        Best Sellers Rank (必须 > 0).
    category_id : int, optional
        Amazon 根品类 ID, 用于查找该品类的回归系数.
    domain : int
        Keepa domain ID (1=US, 2=UK, ...), 用于站点销量倍率调整.
    custom_a, custom_b : float, optional
        自定义系数, 优先于品类系数.

    Returns
    -------
    float or None
        估算日均销量, BSR 无效时返回 None.
    """
    if bsr is None or bsr <= 0:
        return None

    if custom_a is not None and custom_b is not None:
        a, b = custom_a, custom_b
    elif category_id and category_id in CATEGORY_COEFFICIENTS:
        coeff = CATEGORY_COEFFICIENTS[category_id]
        a, b = coeff.coeff_a, coeff.coeff_b
    else:
        a, b = DEFAULT_COEFFICIENTS.coeff_a, DEFAULT_COEFFICIENTS.coeff_b

    daily = a * math.pow(bsr, b)
    multiplier = DOMAIN_MULTIPLIER.get(domain, 0.15)
    return round(daily * multiplier, 2)


def salesrank_drops_to_daily_sales(drops: int, days: int = 30) -> float | None:
    """将 Keepa 的 salesRankDrops 转换为日均销量.

    Keepa 记录的 BSR 下降次数近似为成交笔数.
    但这个值是低估的: 同一小时内多笔成交只会被计为一次 drop.

    Parameters
    ----------
    drops : int
        salesRankDrops30/90/180/365 中的值.
    days : int
        对应的天数 (30/90/180/365).

    Returns
    -------
    float or None
        日均销量估算.
    """
    if drops is None or drops <= 0 or days <= 0:
        return None
    # 经验校正: drops 通常低估实际销量约 20-40%
    # 使用 1.3 作为保守校正系数
    correction_factor = 1.3
    return round(drops * correction_factor / days, 2)


def monthly_sold_to_daily_sales(monthly_sold: int) -> float | None:
    """将 Amazon 的 "bought in past month" 标签转换为日均销量.

    Amazon 显示的是阶梯值 (50, 100, 200, 300, 400, 500, 1000, 2000, ...),
    并非精确值, 但它是最接近真实销量的公开数据.

    Parameters
    ----------
    monthly_sold : int
        monthlySoldHistory 中的值.

    Returns
    -------
    float or None
        日均销量估算.
    """
    if monthly_sold is None or monthly_sold <= 0:
        return None
    return round(monthly_sold / 30.0, 2)


def estimate_daily_sales(
    *,
    bsr: int | float | None = None,
    monthly_sold: int | None = None,
    salesrank_drops_30: int | None = None,
    category_id: int | None = None,
    domain: int = 1,
) -> tuple[float | None, str]:
    """综合多个数据源估算日均销量, 返回 (估算值, 估算方法).

    优先级: monthly_sold > salesrank_drops > BSR 幂律模型.

    Returns
    -------
    (estimated_daily_sales, method_used)
        method_used: "monthly_sold" | "salesrank_drops" | "bsr_power_law" | "no_data"
    """
    # 优先级 1: Amazon 官方月销量标签
    if monthly_sold is not None and monthly_sold > 0:
        return monthly_sold_to_daily_sales(monthly_sold), "monthly_sold"

    # 优先级 2: Keepa BSR drops
    if salesrank_drops_30 is not None and salesrank_drops_30 > 0:
        return salesrank_drops_to_daily_sales(salesrank_drops_30, 30), "salesrank_drops"

    # 优先级 3: BSR 幂律模型
    if bsr is not None and bsr > 0:
        return bsr_to_daily_sales(bsr, category_id=category_id, domain=domain), "bsr_power_law"

    return None, "no_data"
