from __future__ import annotations

import unittest

from data_collector.cross_border_data.asin_discovery import extract_keywords_from_title
from data_collector.cross_border_data.seller_scope import (
    evaluate_seller_scope,
    filter_seller_scope_keywords,
)


class SellerScopeTests(unittest.TestCase):
    def test_blocks_digital_software_and_copyright_media(self) -> None:
        software = evaluate_seller_scope(
            category_path="Digital Software > Antivirus & Security > Antivirus"
        )
        movies = evaluate_seller_scope(category_path="Movies & TV > Movies")
        chinese_query = evaluate_seller_scope(query="杀毒软件和电影")

        self.assertFalse(software.allowed)
        self.assertEqual(software.reason_code, "digital_or_licensed_goods")
        self.assertFalse(movies.allowed)
        self.assertEqual(movies.reason_code, "copyright_media")
        self.assertFalse(chinese_query.allowed)

    def test_allows_physical_cross_border_categories(self) -> None:
        for category_path in [
            "Clothing, Shoes & Jewelry > Men > Shirts",
            "Sports & Outdoors > Golf > Golf Balls",
            "Electronics > Headphones, Earbuds & Accessories > Earbud Headphones",
        ]:
            decision = evaluate_seller_scope(category_path=category_path)
            self.assertTrue(decision.allowed, category_path)

    def test_filters_scope_keywords_before_trends_or_search(self) -> None:
        kept, blocked = filter_seller_scope_keywords(
            ["insulated lunch bag", "antivirus software license", "movies streaming"]
        )

        self.assertEqual(kept, ["insulated lunch bag"])
        self.assertEqual(len(blocked), 2)

    def test_title_keyword_extraction_drops_out_of_scope_terms(self) -> None:
        keywords = extract_keywords_from_title(
            "Norton Antivirus Software License Download Code, 1 Year Subscription",
            max_keywords=3,
        )

        self.assertEqual(keywords, [])


if __name__ == "__main__":
    unittest.main()
