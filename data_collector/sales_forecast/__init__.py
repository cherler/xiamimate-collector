"""Sales forecasting dataset builders."""

__all__ = [
    "main",
    "bsr_to_daily_sales",
    "estimate_daily_sales",
    "salesrank_drops_to_daily_sales",
    "monthly_sold_to_daily_sales",
    "FeatureMatrixBuilder",
    "Week1FeatureFoundationBuilder",
]


def __getattr__(name):
    if name == "main":
        from .cli import main

        return main

    if name in {
        "bsr_to_daily_sales",
        "estimate_daily_sales",
        "salesrank_drops_to_daily_sales",
        "monthly_sold_to_daily_sales",
    }:
        from .bsr_sales_converter import (
            bsr_to_daily_sales,
            estimate_daily_sales,
            monthly_sold_to_daily_sales,
            salesrank_drops_to_daily_sales,
        )

        exports = {
            "bsr_to_daily_sales": bsr_to_daily_sales,
            "estimate_daily_sales": estimate_daily_sales,
            "salesrank_drops_to_daily_sales": salesrank_drops_to_daily_sales,
            "monthly_sold_to_daily_sales": monthly_sold_to_daily_sales,
        }
        return exports[name]

    if name == "FeatureMatrixBuilder":
        from .feature_matrix import FeatureMatrixBuilder

        return FeatureMatrixBuilder

    if name == "Week1FeatureFoundationBuilder":
        from .week1_feature_foundation import Week1FeatureFoundationBuilder

        return Week1FeatureFoundationBuilder

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
