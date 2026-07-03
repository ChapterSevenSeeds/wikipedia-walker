from __future__ import annotations

from pathlib import Path
import threading
from unittest.mock import MagicMock

import pytest

from config import BackupConfig
from crawl_db import DbTimings, utc_now
from mediawiki_api import MediaWikiFetchResult, MediaWikiFetchStats, MediaWikiPageReference
from walker import _maybe_backup_database, _resolve_start_page, walk


class TestResolveStartPage:
    def test_resolves_successfully(self, mocker):
        mock_resolve = mocker.patch("walker.mediawiki_resolve_titles_to_pages")
        mock_resolve.return_value = {MediaWikiPageReference(42, "Dream Theater")}

        result = _resolve_start_page(
            start_title="Dream Theater",
            sleep_seconds=0,
            user_agent="test/1.0",
        )

        assert result.media_wiki_page_id == 42
        assert result.title == "Dream Theater"
        mock_resolve.assert_called_once_with(
            {"Dream Theater"},
            sleep_seconds=0,
            user_agent="test/1.0",
        )

    def test_not_found_exits(self, mocker):
        mock_resolve = mocker.patch("walker.mediawiki_resolve_titles_to_pages")
        mock_resolve.return_value = set()

        with pytest.raises(SystemExit, match="Start page not found"):
            _resolve_start_page(
                start_title="NonExistentPageXYZ",
                sleep_seconds=0,
                user_agent="test/1.0",
            )


class TestMaybeBackupDatabase:
    def test_skipped_when_disabled(self, mocker):
        mock_backup = mocker.patch("walker.run_sqlite_backup")

        _maybe_backup_database(
            backup=BackupConfig(enabled=False, backup_dir=None, max_count=0, run_after_crawl_count=0),
            db_path="test.db",
            crawled_pages_this_run=100,
        )
        mock_backup.assert_not_called()

    def test_skipped_when_zero_pages(self, mocker):
        mock_backup = mocker.patch("walker.run_sqlite_backup")

        _maybe_backup_database(
            backup=BackupConfig(enabled=True, backup_dir=Path("backups"), max_count=5, run_after_crawl_count=10),
            db_path="test.db",
            crawled_pages_this_run=0,
        )
        mock_backup.assert_not_called()

    def test_skipped_when_not_divisible(self, mocker):
        mock_backup = mocker.patch("walker.run_sqlite_backup")

        _maybe_backup_database(
            backup=BackupConfig(enabled=True, backup_dir=Path("backups"), max_count=5, run_after_crawl_count=10),
            db_path="test.db",
            crawled_pages_this_run=5,
        )
        mock_backup.assert_not_called()

    def test_backup_called_when_divisible(self, mocker):
        mock_backup = mocker.patch("walker.run_sqlite_backup")
        mock_backup.return_value = "backups/test.db.20260101T000000Z"

        _maybe_backup_database(
            backup=BackupConfig(enabled=True, backup_dir=Path("backups"), max_count=5, run_after_crawl_count=10),
            db_path="test.db",
            crawled_pages_this_run=10,
        )
        mock_backup.assert_called_once_with(
            db_path="test.db",
            backup_dir="backups",
            max_count=5,
        )


_MOCK_BACKUP = BackupConfig(enabled=False, backup_dir=None, max_count=0, run_after_crawl_count=0)


def _make_fetch_result(page_id: int, title: str, link_ids: list[tuple[int, str]]):
    return MediaWikiFetchResult(
        page=MediaWikiPageReference(page_id, title),
        links={MediaWikiPageReference(pid, t) for pid, t in link_ids},
        stats=MediaWikiFetchStats(
            fetch_links_wall_seconds=0.1,
            resolve_titles_wall_seconds=0.05,
            fetch_links_http_requests=1,
            resolve_titles_http_requests=1,
            rate_limited_responses=0,
            sleep_seconds_start=0.0,
            sleep_seconds_end=0.0,
        ),
    )


class TestWalk:
    def test_normal_flow_then_queue_empty(self, mocker):
        mock_resolve = mocker.patch("walker.mediawiki_resolve_titles_to_pages")
        mock_resolve.return_value = {MediaWikiPageReference(1, "Start Page")}

        mock_init_queue = mocker.patch("walker.initialize_queue")

        mock_server_cls = mocker.patch("walker.StatsWebServer")
        mock_server = MagicMock()
        mock_server.port = 0
        mock_server_cls.return_value.__enter__.return_value = mock_server

        mock_claim = mocker.patch("walker.claim_next_page_from_queue")
        mock_claim.side_effect = [
            (1, "Start Page", DbTimings(claim_seconds=0.01)),
            None,
        ]

        mock_expand = mocker.patch("walker.expand_page_from_cached_links")
        mock_expand.return_value = (False, 2, 1, DbTimings(expand_cache_seconds=0.02))

        mock_fetch = mocker.patch("walker.mediawiki_fetch_links")
        mock_fetch.return_value = _make_fetch_result(1, "Start Page", [(2, "Link A"), (3, "Link B")])

        mock_persist = mocker.patch("walker.persist_fetched_links")
        mock_persist.return_value = (2, 1, DbTimings(persist_links_seconds=0.03))

        mock_progress = mocker.patch("walker.get_progress_counts")
        mock_progress.return_value = (1, 5, 2, DbTimings(progress_counts_seconds=0.005))

        mock_backup = mocker.patch("walker._maybe_backup_database")

        engine = MagicMock()
        stop_event = threading.Event()

        walk(
            engine,
            db_path=":memory:",
            backup=_MOCK_BACKUP,
            start_title="Start Page",
            max_pages=10,
            sleep_seconds=0,
            user_agent="test/1.0",
            web_port=0,
            stats_window_size=10,
            stop_requested=stop_event,
        )

        mock_resolve.assert_called_once()
        mock_init_queue.assert_called_once()
        assert mock_claim.call_count >= 2
        mock_expand.assert_called_once_with(engine, page_id=1)
        mock_fetch.assert_called_once()
        mock_persist.assert_called_once()
        mock_backup.assert_called_once()
        assert mock_server.set_status.call_count >= 1
        assert mock_server.publish.call_count >= 1

    def test_stop_requested_returns_early(self, mocker):
        mock_resolve = mocker.patch("walker.mediawiki_resolve_titles_to_pages")
        mock_resolve.return_value = {MediaWikiPageReference(1, "Start")}

        mock_init_queue = mocker.patch("walker.initialize_queue")

        mock_server_cls = mocker.patch("walker.StatsWebServer")
        mock_server = MagicMock()
        mock_server.port = 0
        mock_server_cls.return_value.__enter__.return_value = mock_server

        mock_claim = mocker.patch("walker.claim_next_page_from_queue")
        mock_expand = mocker.patch("walker.expand_page_from_cached_links")
        mock_progress = mocker.patch("walker.get_progress_counts")

        engine = MagicMock()
        stop_event = threading.Event()
        stop_event.set()

        walk(
            engine,
            db_path=":memory:",
            backup=_MOCK_BACKUP,
            start_title="Start",
            max_pages=10,
            sleep_seconds=0,
            user_agent="test/1.0",
            web_port=0,
            stats_window_size=10,
            stop_requested=stop_event,
        )

        mock_claim.assert_not_called()
        mock_server.set_status.assert_called_with("stopping…")

    def test_error_during_crawl_records_error_and_continues(self, mocker):
        mock_resolve = mocker.patch("walker.mediawiki_resolve_titles_to_pages")
        mock_resolve.return_value = {MediaWikiPageReference(1, "Start")}

        mock_init_queue = mocker.patch("walker.initialize_queue")

        mock_server_cls = mocker.patch("walker.StatsWebServer")
        mock_server = MagicMock()
        mock_server.port = 0
        mock_server_cls.return_value.__enter__.return_value = mock_server

        mock_claim = mocker.patch("walker.claim_next_page_from_queue")
        mock_claim.side_effect = [
            (1, "First Page", DbTimings(claim_seconds=0.01)),
            None,
        ]

        mock_expand = mocker.patch("walker.expand_page_from_cached_links")
        mock_expand.side_effect = RuntimeError("unexpected DB error")

        mock_record_error = mocker.patch("walker.record_page_error")

        mock_progress = mocker.patch("walker.get_progress_counts")
        mock_progress.return_value = (0, 0, 0, DbTimings(progress_counts_seconds=0.005))

        engine = MagicMock()
        stop_event = threading.Event()

        walk(
            engine,
            db_path=":memory:",
            backup=_MOCK_BACKUP,
            start_title="Start",
            max_pages=10,
            sleep_seconds=0,
            user_agent="test/1.0",
            web_port=0,
            stats_window_size=10,
            stop_requested=stop_event,
        )

        mock_record_error.assert_called_once_with(engine, page_id=1, exc=mock_expand.side_effect)

    def test_max_pages_reached_stops(self, mocker):
        mock_resolve = mocker.patch("walker.mediawiki_resolve_titles_to_pages")
        mock_resolve.return_value = {MediaWikiPageReference(1, "Start")}

        mock_init_queue = mocker.patch("walker.initialize_queue")

        mock_server_cls = mocker.patch("walker.StatsWebServer")
        mock_server = MagicMock()
        mock_server.port = 0
        mock_server_cls.return_value.__enter__.return_value = mock_server

        # Only return one page - max_pages=1 means we stop after the first
        mock_claim = mocker.patch("walker.claim_next_page_from_queue")
        mock_claim.return_value = (1, "Only Page", DbTimings(claim_seconds=0.01))

        mock_expand = mocker.patch("walker.expand_page_from_cached_links")
        mock_expand.return_value = (True, 0, 0, DbTimings())

        mock_progress = mocker.patch("walker.get_progress_counts")
        mock_progress.return_value = (1, 0, 1, DbTimings())

        engine = MagicMock()
        stop_event = threading.Event()

        walk(
            engine,
            db_path=":memory:",
            backup=_MOCK_BACKUP,
            start_title="Start",
            max_pages=1,
            sleep_seconds=0,
            user_agent="test/1.0",
            web_port=0,
            stats_window_size=10,
            stop_requested=stop_event,
        )

        mock_server.set_status.assert_called_with("stopping")
