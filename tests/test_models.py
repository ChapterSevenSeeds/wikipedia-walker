from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy import inspect
from sqlalchemy.orm import Session

from models import (
    Base,
    Page,
    PageCrawlStatus,
    PageLink,
    init_db,
    make_engine,
    utc_now,
)


class TestUtcNow:
    def test_returns_datetime(self):
        result = utc_now()
        assert isinstance(result, datetime)
        assert result.tzinfo is not None

    def test_returns_utc(self):
        result = utc_now()
        assert result.tzname() == "UTC" or str(result.tzinfo) == "UTC"


class TestMakeEngine:
    def test_in_memory(self):
        from sqlalchemy import create_engine as _ce
        engine = _ce("sqlite://", echo=False)
        init_db(engine)
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        assert "pages" in tables
        assert "page_links" in tables


class TestPageModel:
    def test_create_page(self, db_session):
        page = Page(
            mw_page_id=1,
            title="Test Article",
            crawl_status=PageCrawlStatus.queued,
        )
        db_session.add(page)
        db_session.commit()

        fetched = db_session.get(Page, 1)
        assert fetched.title == "Test Article"
        assert fetched.crawl_status == PageCrawlStatus.queued

    def test_default_crawl_status(self, db_session):
        page = Page(mw_page_id=2, title="Default Status")
        db_session.add(page)
        db_session.commit()

        fetched = db_session.get(Page, 2)
        assert fetched.crawl_status == PageCrawlStatus.queued

    def test_created_at_default(self, db_session):
        page = Page(mw_page_id=3, title="With Timestamps")
        db_session.add(page)
        db_session.commit()

        fetched = db_session.get(Page, 3)
        assert fetched.created_at is not None
        assert fetched.updated_at is not None


class TestPageLinkModel:
    def test_create_link(self, db_session):
        page1 = Page(mw_page_id=10, title="Source")
        page2 = Page(mw_page_id=11, title="Target")
        db_session.add_all([page1, page2])
        db_session.flush()

        link = PageLink(from_page_id=10, to_page_id=11)
        db_session.add(link)
        db_session.commit()

        fetched = db_session.get(PageLink, link.id)
        assert fetched.from_page_id == 10
        assert fetched.to_page_id == 11

    def test_unique_constraint(self, db_session):
        page1 = Page(mw_page_id=20, title="Src")
        page2 = Page(mw_page_id=21, title="Dst")
        db_session.add_all([page1, page2])
        db_session.flush()

        db_session.add(PageLink(from_page_id=20, to_page_id=21))
        db_session.commit()

        db_session.add(PageLink(from_page_id=20, to_page_id=21))
        with pytest.raises(Exception):
            db_session.commit()

    def test_relationships(self, db_session):
        src = Page(mw_page_id=30, title="Source")
        dst = Page(mw_page_id=31, title="Target")
        db_session.add_all([src, dst])
        db_session.flush()

        link = PageLink(from_page_id=30, to_page_id=31)
        db_session.add(link)
        db_session.commit()

        assert len(src.out_links) == 1
        assert src.out_links[0].to_page_id == 31
        assert len(dst.in_links) == 1
        assert dst.in_links[0].from_page_id == 30


class TestPageCrawlStatus:
    def test_enum_values(self):
        assert PageCrawlStatus.queued == "queued"
        assert PageCrawlStatus.in_progress == "in_progress"
        assert PageCrawlStatus.done == "done"
        assert PageCrawlStatus.error == "error"
