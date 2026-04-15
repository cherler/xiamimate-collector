from __future__ import annotations

from typing import Any
import requests


class CollectorError(RuntimeError):
    """Raised when a source request or normalization step fails."""


class BaseCollector:
    """Shared HTTP logic for all collectors."""

    def __init__(self, timeout: int = 60) -> None:
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "xiamimate-data-collector/1.0"})

    def get_json(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> Any:
        response = self.session.get(url, params=params, headers=headers, timeout=self.timeout)
        self._raise_for_status(response)
        return response.json()

    def get_text(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> str:
        response = self.session.get(url, params=params, headers=headers, timeout=self.timeout)
        self._raise_for_status(response)
        return response.text

    @staticmethod
    def _raise_for_status(response: requests.Response) -> None:
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            snippet = response.text[:500]
            raise CollectorError(f"HTTP {response.status_code}: {snippet}") from exc
