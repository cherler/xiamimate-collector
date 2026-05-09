from datetime import datetime, timedelta

from data_collector.cross_border_data.storage import (
    DuckDBStorage,
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
