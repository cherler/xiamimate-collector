from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os

from dotenv import load_dotenv

# Load .env from the data_collector directory (parent of cross_border_data)
_env_path = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(_env_path)


@dataclass(frozen=True)
class Settings:
    """Runtime settings loaded from environment variables."""

    base_dir: Path
    output_dir: Path
    request_timeout: int
    census_api_key: str | None
    uncomtrade_api_key: str | None
    semrush_api_key: str | None
    ahrefs_api_key: str | None
    keepa_api_key: str | None
    uncomtrade_base_url: str
    eurostat_base_url: str
    semrush_base_url: str
    ahrefs_base_url: str
    ahrefs_site_overview_path: str
    keepa_base_url: str


def load_settings(base_dir: str | Path | None = None) -> Settings:
    resolved_base_dir = Path(base_dir or Path(__file__).resolve().parents[1]).resolve()
    output_dir = resolved_base_dir / os.getenv("OUTPUT_DIR", "outputs")

    return Settings(
        base_dir=resolved_base_dir,
        output_dir=output_dir,
        request_timeout=int(os.getenv("REQUEST_TIMEOUT", "60")),
        census_api_key=os.getenv("CENSUS_API_KEY"),
        uncomtrade_api_key=os.getenv("UNCOMTRADE_API_KEY"),
        semrush_api_key=os.getenv("SEMRUSH_API_KEY"),
        ahrefs_api_key=os.getenv("AHREFS_API_KEY"),
        keepa_api_key=os.getenv("KEEPA_API_KEY"),
        uncomtrade_base_url=os.getenv(
            "UNCOMTRADE_BASE_URL",
            "https://comtradeapi.worldbank.org/data/v1/get/C/A/HS",
        ),
        eurostat_base_url=os.getenv(
            "EUROSTAT_BASE_URL",
            "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data",
        ),
        semrush_base_url=os.getenv("SEMRUSH_BASE_URL", "https://api.semrush.com/"),
        ahrefs_base_url=os.getenv("AHREFS_BASE_URL", "https://api.ahrefs.com/v3"),
        ahrefs_site_overview_path=os.getenv(
            "AHREFS_SITE_OVERVIEW_PATH",
            "site-explorer/overview",
        ),
        keepa_base_url=os.getenv("KEEPA_BASE_URL", "https://api.keepa.com/product"),
    )
