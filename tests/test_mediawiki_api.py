from __future__ import annotations

import pytest

from mediawiki_api import (
    MEDIAWIKI_MAX_TITLES_PER_QUERY,
    MediaWikiPageReference,
    _resolve_titles_to_pages_with_telemetry,
    mediawiki_fetch_links,
    mediawiki_resolve_titles_to_pages,
)


@pytest.fixture(autouse=True)
def _no_sleep(mocker):
    mocker.patch("mediawiki_api.time.sleep")


class TestMediawikiResolveTitlesToPages:
    def test_simple_resolution(self, mocker):
        mock_get_json = mocker.patch("mediawiki_api._get_json")
        mock_get_json.return_value = {
            "batchcomplete": "",
            "query": {
                "normalized": [],
                "redirects": [],
                "pages": [
                    {"pageid": 42, "ns": 0, "title": "Test Article"},
                ],
            },
        }

        result = mediawiki_resolve_titles_to_pages(
            {"Test Article"},
            sleep_seconds=0,
            user_agent="test/1.0",
        )

        assert len(result) == 1
        ref = next(iter(result))
        assert ref.media_wiki_page_id == 42
        assert ref.title == "Test Article"

    def test_follows_redirects(self, mocker):
        mock_get_json = mocker.patch("mediawiki_api._get_json")
        mock_get_json.return_value = {
            "batchcomplete": "",
            "query": {
                "normalized": [],
                "redirects": [
                    {"from": "Old Name", "to": "New Name"},
                ],
                "pages": [
                    {"pageid": 99, "ns": 0, "title": "New Name"},
                ],
            },
        }

        result = mediawiki_resolve_titles_to_pages(
            {"Old Name"},
            sleep_seconds=0,
            user_agent="test/1.0",
        )

        assert len(result) == 1
        ref = next(iter(result))
        assert ref.media_wiki_page_id == 99
        assert ref.title == "New Name"

    def test_normalizes_titles(self, mocker):
        mock_get_json = mocker.patch("mediawiki_api._get_json")
        mock_get_json.return_value = {
            "batchcomplete": "",
            "query": {
                "normalized": [
                    {"from": "wikipedia walker", "to": "Wikipedia walker"},
                ],
                "redirects": [],
                "pages": [
                    {"pageid": 100, "ns": 0, "title": "Wikipedia walker"},
                ],
            },
        }

        result = mediawiki_resolve_titles_to_pages(
            {"wikipedia walker"},
            sleep_seconds=0,
            user_agent="test/1.0",
        )

        assert len(result) == 1
        ref = next(iter(result))
        assert ref.media_wiki_page_id == 100
        assert ref.title == "Wikipedia walker"

    def test_redirect_with_fragment(self, mocker):
        mock_get_json = mocker.patch("mediawiki_api._get_json")
        mock_get_json.return_value = {
            "batchcomplete": "",
            "query": {
                "normalized": [],
                "redirects": [
                    {"from": "Shortcut", "to": "Long Title", "tofragment": "section"},
                ],
                "pages": [
                    {"pageid": 200, "ns": 0, "title": "Long Title"},
                ],
            },
        }

        result = mediawiki_resolve_titles_to_pages(
            {"Shortcut"},
            sleep_seconds=0,
            user_agent="test/1.0",
        )

        assert len(result) == 1
        ref = next(iter(result))
        assert ref.media_wiki_page_id == 200
        assert ref.title == "Long Title"

    def test_skips_missing_pages(self, mocker):
        mock_get_json = mocker.patch("mediawiki_api._get_json")
        mock_get_json.return_value = {
            "batchcomplete": "",
            "query": {
                "normalized": [],
                "redirects": [],
                "pages": [
                    {"ns": 0, "title": "Missing Page", "missing": True},
                    {"pageid": 50, "ns": 0, "title": "Existing Page"},
                ],
            },
        }

        result = mediawiki_resolve_titles_to_pages(
            {"Missing Page", "Existing Page"},
            sleep_seconds=0,
            user_agent="test/1.0",
        )

        assert len(result) == 1
        ref = next(iter(result))
        assert ref.media_wiki_page_id == 50

    def test_empty_input(self, mocker):
        mock_get_json = mocker.patch("mediawiki_api._get_json")

        result = mediawiki_resolve_titles_to_pages(
            set(),
            sleep_seconds=0,
            user_agent="test/1.0",
        )

        assert len(result) == 0
        mock_get_json.assert_not_called()

    def test_multiple_titles_batched(self, mocker):
        mock_get_json = mocker.patch("mediawiki_api._get_json")
        mock_get_json.return_value = {
            "batchcomplete": "",
            "query": {
                "normalized": [],
                "redirects": [],
                "pages": [
                    {"pageid": i, "ns": 0, "title": f"Page {i}"}
                    for i in range(1, 61)
                ],
            },
        }

        titles = {f"Page {i}" for i in range(1, 61)}
        result = mediawiki_resolve_titles_to_pages(
            titles,
            sleep_seconds=0,
            user_agent="test/1.0",
        )

        assert len(result) == 60
        assert mock_get_json.call_count == 2  # 60 titles, 50 per batch

    def test_single_redirect_resolved(self, mocker):
        """A single redirect is followed to the target page."""
        mock_get_json = mocker.patch("mediawiki_api._get_json")
        mock_get_json.return_value = {
            "batchcomplete": "",
            "query": {
                "normalized": [],
                "redirects": [
                    {"from": "A", "to": "B"},
                ],
                "pages": [
                    {"pageid": 300, "ns": 0, "title": "B"},
                ],
            },
        }

        result = mediawiki_resolve_titles_to_pages(
            {"A"},
            sleep_seconds=0,
            user_agent="test/1.0",
        )

        assert len(result) == 1
        ref = next(iter(result))
        assert ref.media_wiki_page_id == 300
        assert ref.title == "B"

    def test_normalized_then_redirected(self, mocker):
        mock_get_json = mocker.patch("mediawiki_api._get_json")
        mock_get_json.return_value = {
            "batchcomplete": "",
            "query": {
                "normalized": [
                    {"from": "a page", "to": "A Page"},
                ],
                "redirects": [
                    {"from": "A Page", "to": "Final Title"},
                ],
                "pages": [
                    {"pageid": 400, "ns": 0, "title": "Final Title"},
                ],
            },
        }

        result = mediawiki_resolve_titles_to_pages(
            {"a page"},
            sleep_seconds=0,
            user_agent="test/1.0",
        )

        assert len(result) == 1
        ref = next(iter(result))
        assert ref.media_wiki_page_id == 400
        assert ref.title == "Final Title"

    def test_partial_resolution_some_missing(self, mocker):
        mock_get_json = mocker.patch("mediawiki_api._get_json")
        mock_get_json.return_value = {
            "batchcomplete": "",
            "query": {
                "normalized": [],
                "redirects": [],
                "pages": [
                    {"pageid": 1, "ns": 0, "title": "Exists"},
                    {"ns": 0, "title": "Gone", "missing": True},
                ],
            },
        }

        result = mediawiki_resolve_titles_to_pages(
            {"Exists", "Gone"},
            sleep_seconds=0,
            user_agent="test/1.0",
        )

        assert len(result) == 1
        ref = next(iter(result))
        assert ref.media_wiki_page_id == 1


class TestResolveTitlesToPagesWithTelemetry:
    def test_returns_http_request_count(self, mocker):
        mock_get_json = mocker.patch("mediawiki_api._get_json")
        mock_get_json.return_value = {
            "batchcomplete": "",
            "query": {
                "normalized": [],
                "redirects": [],
                "pages": [{"pageid": 1, "ns": 0, "title": "Test"}],
            },
        }

        resolved, requests, wall = _resolve_titles_to_pages_with_telemetry(
            {"Test"},
            sleep_seconds=0,
            user_agent="test/1.0",
        )

        assert requests == 1
        assert wall >= 0
        assert len(resolved) == 1


class TestMediawikiFetchLinks:
    def test_single_page_no_continuation(self, mocker):
        mock_get_json = mocker.patch("mediawiki_api._get_json")
        mock_resolve = mocker.patch("mediawiki_api._resolve_titles_to_pages_with_telemetry")
        mock_resolve.return_value = (
            {MediaWikiPageReference(10, "Link One"), MediaWikiPageReference(20, "Link Two")},
            1,
            0.05,
        )

        mock_get_json.return_value = {
            "batchcomplete": "",
            "query": {
                "pages": [
                    {
                        "pageid": 1,
                        "ns": 0,
                        "title": "Source Page",
                        "links": [
                            {"ns": 0, "title": "Link One"},
                            {"ns": 0, "title": "Link Two"},
                        ],
                    },
                ],
            },
        }

        result = mediawiki_fetch_links("Source Page", sleep_seconds=0, user_agent="test/1.0")

        assert result.page.media_wiki_page_id == 1
        assert result.page.title == "Source Page"
        assert len(result.links) == 2
        assert MediaWikiPageReference(10, "Link One") in result.links
        assert MediaWikiPageReference(20, "Link Two") in result.links

    def test_pagination(self, mocker):
        mock_get_json = mocker.patch("mediawiki_api._get_json")
        mock_resolve = mocker.patch("mediawiki_api._resolve_titles_to_pages_with_telemetry")
        mock_resolve.return_value = (
            {MediaWikiPageReference(i, f"Link {i}") for i in range(1, 4)},
            1,
            0.05,
        )

        mock_get_json.side_effect = [
            # First page
            {
                "batchcomplete": "",
                "continue": {"plcontinue": "1|0|Link_3", "continue": "||"},
                "query": {
                    "pages": [
                        {
                            "pageid": 1,
                            "ns": 0,
                            "title": "Source",
                            "links": [
                                {"ns": 0, "title": "Link 1"},
                                {"ns": 0, "title": "Link 2"},
                            ],
                        },
                    ],
                },
            },
            # Second page
            {
                "batchcomplete": "",
                "query": {
                    "pages": [
                        {
                            "pageid": 1,
                            "ns": 0,
                            "title": "Source",
                            "links": [
                                {"ns": 0, "title": "Link 3"},
                            ],
                        },
                    ],
                },
            },
        ]

        result = mediawiki_fetch_links("Source", sleep_seconds=0, user_agent="test/1.0")

        assert result.page.media_wiki_page_id == 1
        assert len(result.links) == 3

    def test_uses_canonical_title(self, mocker):
        mock_get_json = mocker.patch("mediawiki_api._get_json")
        mock_resolve = mocker.patch("mediawiki_api._resolve_titles_to_pages_with_telemetry")
        mock_resolve.return_value = (set(), 0, 0.0)

        mock_get_json.return_value = {
            "batchcomplete": "",
            "query": {
                "pages": [
                    {
                        "pageid": 42,
                        "ns": 0,
                        "title": "Redirected Title",
                        "links": [],
                    },
                ],
            },
        }

        result = mediawiki_fetch_links("Original Title", sleep_seconds=0, user_agent="test/1.0")

        assert result.page.title == "Redirected Title"
        assert result.page.media_wiki_page_id == 42

    def test_missing_pageid_raises(self, mocker):
        mock_get_json = mocker.patch("mediawiki_api._get_json")

        mock_get_json.return_value = {
            "batchcomplete": "",
            "query": {
                "pages": [
                    {
                        "ns": 0,
                        "title": "No ID",
                        "missing": True,
                    },
                ],
            },
        }

        with pytest.raises(RuntimeError, match="Could not resolve pageid"):
            mediawiki_fetch_links("No ID", sleep_seconds=0, user_agent="test/1.0")

    def test_telemetry_in_result(self, mocker):
        mock_get_json = mocker.patch("mediawiki_api._get_json")
        mock_resolve = mocker.patch("mediawiki_api._resolve_titles_to_pages_with_telemetry")
        mock_resolve.return_value = (set(), 1, 0.05)

        mock_get_json.return_value = {
            "batchcomplete": "",
            "query": {
                "pages": [
                    {
                        "pageid": 7,
                        "ns": 0,
                        "title": "Stats Test",
                        "links": [],
                    },
                ],
            },
        }

        result = mediawiki_fetch_links("Stats Test", sleep_seconds=0, user_agent="test/1.0")

        assert result.stats.fetch_links_http_requests == 1
        assert result.stats.resolve_titles_http_requests == 1
        assert result.stats.fetch_links_wall_seconds >= 0
        assert result.stats.resolve_titles_wall_seconds >= 0

    def test_no_links_returns_empty(self, mocker):
        mock_get_json = mocker.patch("mediawiki_api._get_json")
        mock_resolve = mocker.patch("mediawiki_api._resolve_titles_to_pages_with_telemetry")
        mock_resolve.return_value = (set(), 0, 0.0)

        mock_get_json.return_value = {
            "batchcomplete": "",
            "query": {
                "pages": [
                    {
                        "pageid": 99,
                        "ns": 0,
                        "title": "No Links",
                    },
                ],
            },
        }

        result = mediawiki_fetch_links("No Links", sleep_seconds=0, user_agent="test/1.0")

        assert len(result.links) == 0
