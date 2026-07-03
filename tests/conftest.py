from __future__ import annotations

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from mediawiki_api import MediaWikiFetchResult, MediaWikiFetchStats, MediaWikiPageReference
from models import Base, Page, PageCrawlStatus, utc_now


@pytest.fixture
def in_memory_engine():
    engine = create_engine("sqlite://", echo=False)
    Base.metadata.create_all(engine)
    return engine


@pytest.fixture
def db_session(in_memory_engine):
    with Session(in_memory_engine) as session:
        yield session


@pytest.fixture
def sample_page_ref() -> MediaWikiPageReference:
    return MediaWikiPageReference(media_wiki_page_id=1, title="Test Page")


@pytest.fixture
def sample_fetch_result(sample_page_ref) -> MediaWikiFetchResult:
    links = {
        MediaWikiPageReference(2, "Link A"),
        MediaWikiPageReference(3, "Link B"),
    }
    stats = MediaWikiFetchStats(
        fetch_links_wall_seconds=0.1,
        resolve_titles_wall_seconds=0.05,
        fetch_links_http_requests=1,
        resolve_titles_http_requests=1,
        rate_limited_responses=0,
        sleep_seconds_start=0.0,
        sleep_seconds_end=0.0,
    )
    return MediaWikiFetchResult(page=sample_page_ref, links=links, stats=stats)


@pytest.fixture
def seeded_engine(in_memory_engine, sample_page_ref):
    """An engine with a single page pre-inserted (sample page)."""
    with Session(in_memory_engine) as session:
        page = Page(
            mw_page_id=sample_page_ref.media_wiki_page_id,
            title=sample_page_ref.title,
            crawl_status=PageCrawlStatus.queued,
            last_enqueued_at=utc_now(),
        )
        session.add(page)
        session.commit()
    return in_memory_engine
