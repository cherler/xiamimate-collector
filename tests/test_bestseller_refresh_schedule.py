from datetime import datetime, timedelta

from data_collector.cross_border_data.storage import (
    DuckDBStorage,
    _anchor_refresh_window_hours,
    _build_dynamic_stale_hours_sql,
    _business_tier_refresh_windows_days,
    _compute_bestseller_refresh_interval_days,
)


def _insert_category(storage: DuckDBStorage, *, category_id: int = 100, domain: int = 1) -> None:
    storage.conn.execute(
        """
        INSERT INTO curated.keepa_category_registry (
            category_id, domain, category_en, product_count, depth, is_active
        ) VALUES (?, ?, ?, ?, 1, TRUE)
        """,
        [category_id, domain, f"Category {category_id}", 100000],
    )


def test_bestseller_refresh_interval_defaults_and_dynamic_rules() -> None:
    assert _compute_bestseller_refresh_interval_days(1, 0.10) == 7
    assert _compute_bestseller_refresh_interval_days(2, 0.10) == 14
    assert _compute_bestseller_refresh_interval_days(1, 0.25) == 4
    assert _compute_bestseller_refresh_interval_days(2, 0.25) == 7
    assert _compute_bestseller_refresh_interval_days(1, 0.01) == 14
    assert _compute_bestseller_refresh_interval_days(2, 0.01) == 28


def test_bestseller_category_becomes_due_only_after_next_refresh(tmp_path) -> None:
    storage = DuckDBStorage(tmp_path / "test.duckdb")
    _insert_category(storage, category_id=100, domain=1)

    assert storage.get_next_category_for_bestseller(1)["category_id"] == 100

    storage.mark_category_bestseller_done(
        100,
        1,
        100,
        new_asin_count=10,
        existing_asin_count=90,
    )
    assert storage.get_next_category_for_bestseller(1) is None
    assert storage.get_category_stats(1)["bestseller_pending"] == 0

    past = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
    storage.conn.execute(
        """
        UPDATE curated.keepa_category_registry
        SET bestseller_next_refresh_at = ?
        WHERE category_id = 100 AND domain = 1
        """,
        [past],
    )

    due = storage.get_next_category_for_bestseller(1)
    assert due is not None
    assert due["category_id"] == 100
    assert storage.get_category_stats(1)["bestseller_pending"] == 1


def test_count_registered_asins_for_bestseller_new_rate(tmp_path) -> None:
    storage = DuckDBStorage(tmp_path / "test.duckdb")
    storage.register_asins([
        {"asin": "A1", "domain": 1},
        {"asin": "A2", "domain": 1},
        {"asin": "A1", "domain": 2},
    ])

    assert storage.count_registered_asins(["A1", "A2", "A3"], 1) == 2
    assert storage.count_registered_asins(["A1", "A2", "A3"], 2) == 1


def test_refresh_windows_defaults(monkeypatch) -> None:
    for tier in ("P0", "P1", "P2"):
        monkeypatch.delenv(f"AUTO_REFRESH_WINDOW_{tier}_MIN_DAYS", raising=False)
        monkeypatch.delenv(f"AUTO_REFRESH_WINDOW_{tier}_MAX_DAYS", raising=False)
    monkeypatch.delenv("AUTO_REFRESH_WINDOW_ANCHOR_DAYS", raising=False)

    windows = _business_tier_refresh_windows_days()
    assert windows["P0"] == (14, 21)
    assert windows["P1"] == (30, 45)
    assert windows["P2"] == (60, 75)
    assert _anchor_refresh_window_hours() == 30 * 24


def test_refresh_windows_env_override(monkeypatch) -> None:
    monkeypatch.setenv("AUTO_REFRESH_WINDOW_P0_MIN_DAYS", "7")
    monkeypatch.setenv("AUTO_REFRESH_WINDOW_P0_MAX_DAYS", "10")
    monkeypatch.setenv("AUTO_REFRESH_WINDOW_ANCHOR_DAYS", "45")

    windows = _business_tier_refresh_windows_days()
    assert windows["P0"] == (7, 10)
    assert _anchor_refresh_window_hours() == 45 * 24


def test_refresh_windows_swaps_inverted_bounds(monkeypatch) -> None:
    monkeypatch.setenv("AUTO_REFRESH_WINDOW_P1_MIN_DAYS", "60")
    monkeypatch.setenv("AUTO_REFRESH_WINDOW_P1_MAX_DAYS", "30")

    windows = _business_tier_refresh_windows_days()
    assert windows["P1"] == (30, 60)


def test_dynamic_stale_hours_sql_includes_anchor_branch(monkeypatch) -> None:
    monkeypatch.delenv("AUTO_REFRESH_WINDOW_ANCHOR_DAYS", raising=False)
    sql = _build_dynamic_stale_hours_sql(1440)
    assert "WHEN business_tier = 'Anchor' THEN 720" in sql  # 30 天 * 24
    assert "WHEN business_tier = 'P0'" in sql
    assert "ELSE 1440" in sql

