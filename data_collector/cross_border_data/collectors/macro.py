from __future__ import annotations

from typing import Any

from .base import BaseCollector


class USCensusCollector(BaseCollector):
    """Collector for the US Census international trade API (未实装)."""

    def __init__(self, api_key: str | None = None, timeout: int = 60) -> None:
        super().__init__(timeout=timeout)

    def fetch_imports(self, **kwargs: Any) -> list[dict]:
        raise NotImplementedError("USCensusCollector 尚未实装")


class EurostatCollector(BaseCollector):
    """Collector for Eurostat JSON-stat datasets (未实装)."""

    def __init__(self, base_url: str = "", timeout: int = 60) -> None:
        super().__init__(timeout=timeout)

    def fetch_dataset(self, dataset_code: str, **kwargs: Any) -> list[dict]:
        raise NotImplementedError("EurostatCollector 尚未实装")


class UNComtradeCollector(BaseCollector):
    """Collector for the UN Comtrade API (未实装)."""

    def __init__(self, base_url: str = "", api_key: str | None = None, timeout: int = 60) -> None:
        super().__init__(timeout=timeout)

    def fetch_trade_data(self, **kwargs: Any) -> list[dict]:
        raise NotImplementedError("UNComtradeCollector 尚未实装")
