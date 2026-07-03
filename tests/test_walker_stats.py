from __future__ import annotations

import pytest

from crawl_db import DbTimings
from walker_stats import WalkerStats


class TestWalkerStats:
    def test_initial_state(self):
        stats = WalkerStats(window_size=50)
        assert stats.window_size == 50
        assert stats.window_len == 0

    def test_window_size_must_be_positive(self):
        with pytest.raises(ValueError, match="window_size"):
            WalkerStats(window_size=0)
        with pytest.raises(ValueError, match="window_size"):
            WalkerStats(window_size=-1)

    def test_record_page_adds_observation(self):
        stats = WalkerStats(window_size=10)
        stats.record_page(
            visited_title="Test",
            page_wall_seconds=1.0,
            pages_created=3,
            pages_existing=2,
            was_fetched=True,
            api_fetch_links_seconds=0.5,
            api_resolve_titles_seconds=0.3,
            api_http_requests=2,
            rate_limited_responses=0,
            db_timings=DbTimings(claim_seconds=0.01, expand_cache_seconds=0.02, persist_links_seconds=0.03),
        )
        assert stats.window_len == 1

    def test_window_respects_max_len(self):
        stats = WalkerStats(window_size=3)
        for i in range(5):
            stats.record_page(
                visited_title=f"Page {i}",
                page_wall_seconds=1.0,
                pages_created=0,
                pages_existing=0,
                was_fetched=False,
                api_fetch_links_seconds=0.0,
                api_resolve_titles_seconds=0.0,
                api_http_requests=0,
                rate_limited_responses=0,
            )
        assert stats.window_len == 3

    def test_record_error_and_clear(self):
        stats = WalkerStats(window_size=10)
        assert stats._last_error is None

        stats.record_error(page_title="Bad Page", exc=RuntimeError("fail"))
        assert stats._last_error is not None

        stats.clear_error()
        assert stats._last_error is None

    def test_patch_last_db_progress_counts(self):
        stats = WalkerStats(window_size=10)
        stats.record_page(
            visited_title="Test",
            page_wall_seconds=1.0,
            pages_created=0,
            pages_existing=0,
            was_fetched=False,
            api_fetch_links_seconds=0.0,
            api_resolve_titles_seconds=0.0,
            api_http_requests=0,
            rate_limited_responses=0,
        )
        stats.patch_last_db_progress_counts(0.123)
        assert stats._window[-1].db_progress_counts_seconds == 0.123

    def test_patch_on_empty_window_does_nothing(self):
        stats = WalkerStats(window_size=10)
        stats.patch_last_db_progress_counts(0.5)
        assert stats.window_len == 0

    def test_to_table_rows_basic(self):
        stats = WalkerStats(window_size=50)
        stats.record_page(
            visited_title="First Page",
            page_wall_seconds=2.0,
            pages_created=5,
            pages_existing=3,
            was_fetched=True,
            api_fetch_links_seconds=0.4,
            api_resolve_titles_seconds=0.2,
            api_http_requests=3,
            rate_limited_responses=0,
            db_timings=DbTimings(
                claim_seconds=0.01,
                expand_cache_seconds=0.02,
                persist_links_seconds=0.03,
                progress_counts_seconds=0.005,
            ),
        )

        rows = stats.to_table_rows(run_pages=1, queued_count=10, crawled_page_count=5)

        row_map = {k: v for k, v in rows}

        assert "Visited title" in row_map
        assert row_map["Run pages"] == "1"
        assert row_map["Progress queued"] == "10"
        assert row_map["Progress crawled pages"] == "5"

    def test_to_table_rows_error_appears(self):
        stats = WalkerStats(window_size=50)
        stats.record_page(
            visited_title="Page",
            page_wall_seconds=1.0,
            pages_created=0,
            pages_existing=0,
            was_fetched=False,
            api_fetch_links_seconds=0.0,
            api_resolve_titles_seconds=0.0,
            api_http_requests=0,
            rate_limited_responses=0,
        )
        stats.record_error(page_title="Page", exc=ValueError("bug"))

        rows = stats.to_table_rows(run_pages=1, queued_count=0, crawled_page_count=1)
        row_map = {k: v for k, v in rows}
        assert "Last error" in row_map

    def test_cache_hit_rate(self):
        stats = WalkerStats(window_size=50)
        stats.record_page(
            visited_title="P",
            page_wall_seconds=1.0,
            pages_created=1,
            pages_existing=3,
            was_fetched=True,
            api_fetch_links_seconds=0.5,
            api_resolve_titles_seconds=0.3,
            api_http_requests=2,
            rate_limited_responses=0,
        )

        rows = stats.to_table_rows(run_pages=1, queued_count=0, crawled_page_count=1)
        row_map = {k: v for k, v in rows}
        assert "Link cache hit rate" in row_map
        # 3 existing / 4 total = 75%
        assert "75.0%" in row_map["Link cache hit rate"]

    def test_no_fetched_entries_omits_api_section(self):
        stats = WalkerStats(window_size=50)
        stats.record_page(
            visited_title="Cached Page",
            page_wall_seconds=0.1,
            pages_created=0,
            pages_existing=2,
            was_fetched=False,
            api_fetch_links_seconds=0.0,
            api_resolve_titles_seconds=0.0,
            api_http_requests=0,
            rate_limited_responses=0,
        )

        rows = stats.to_table_rows(run_pages=1, queued_count=0, crawled_page_count=1)
        keys = [k for k, v in rows]
        assert "Avg API fetch_links" not in keys
