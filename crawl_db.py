"""Database helper functions for the Wikipedia walker.

This module holds the SQLAlchemy/SQLite operations that implement the crawler's
DB-backed queue and edge persistence.

Design goal:
- `walker.py` should not need to open sessions or import ORM models.
    All database reads/writes should flow through functions in this module.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Iterable, Sequence

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from mediawiki_api import MediaWikiFetchResult, MediaWikiPageReference
from models import Page, PageCrawlStatus, PageLink, init_db, make_engine as _make_engine, utc_now


def make_engine(db_path: str):
    """Create an engine for the given SQLite DB path and ensure schema exists.

    This is a small facade so higher-level code (e.g. `walker.py`) does not need
    to import anything from `models.py`.
    """

    engine = _make_engine(db_path)
    init_db(engine)
    return engine


def ensure_schema(engine) -> None:
    """Ensure DB schema exists (creates tables if needed)."""

    init_db(engine)


def get_progress_counts(engine) -> tuple[int, int, int]:
    """Return (done_count, queued_count, crawled_page_count)."""

    with Session(engine) as session:
        queued_count = session.scalar(
            select(func.count()).select_from(Page).where(Page.crawl_status == PageCrawlStatus.queued)
        )
        done_count = session.scalar(
            select(func.count()).select_from(Page).where(Page.crawl_status == PageCrawlStatus.done)
        )
        crawled_page_count = session.scalar(
            select(func.count()).select_from(Page).where(Page.last_links_recorded_at.is_not(None))
        )

    return int(done_count or 0), int(queued_count or 0), int(crawled_page_count or 0)


def print_progress(engine) -> None:
    done_count, queued_count, crawled_page_count = get_progress_counts(engine)
    print(f"done={done_count} queued={queued_count} crawled_pages={crawled_page_count}")


def get_or_create_page(session: Session, ref: MediaWikiPageReference) -> Page:
    """Get or create a Page row for the given MediaWikiPageReference."""

    page = session.get(Page, ref.media_wiki_page_id)
    if page is None:
        page = Page(mw_page_id=ref.media_wiki_page_id, title=ref.title)
        session.add(page)
        return page

    # Keep canonical title fresh.
    if page.title != ref.title:
        page.title = ref.title

    return page


def enqueue_page(session: Session, ref: MediaWikiPageReference) -> Page:
    """Enqueue a page for crawling if not already done or in-progress.
    Returns the Page row.
    """
    now = utc_now()
    page = get_or_create_page(session, ref)

    # Already crawled pages should never be crawled again (until we add explicit recrawl logic).
    if page.last_links_recorded_at is not None:
        page.crawl_status = PageCrawlStatus.done
        return page

    if page.crawl_status not in {PageCrawlStatus.done, PageCrawlStatus.in_progress}:
        page.crawl_status = PageCrawlStatus.queued
        page.last_enqueued_at = now

    return page


def record_links(
    session: Session,
    from_page: Page,
    to_pages: Iterable[MediaWikiPageReference],
    now: datetime,
) -> tuple[int, int]:
    """Upsert pages + edges; enqueue discovered pages.

    Returns (links_added, links_already_existing).
    """

    links_added = 0
    links_existing = 0
    for to_ref in to_pages:
        to_page = get_or_create_page(session, to_ref)
        enqueue_page(session, to_ref)

        link = session.scalar(
            select(PageLink).where(
                PageLink.from_page_id == from_page.mw_page_id,
                PageLink.to_page_id == to_page.mw_page_id,
            )
        )
        if link is None:
            session.add(PageLink(from_page=from_page, to_page=to_page, last_seen_at=now))
            links_added += 1
        else:
            link.last_seen_at = now
            links_existing += 1

    return links_added, links_existing


def next_queued_page(session: Session) -> Page | None:
    """Return the next queued page to crawl, or None if none exists."""

    return session.scalar(
        select(Page)
        .where(
            Page.crawl_status == PageCrawlStatus.queued,
            Page.last_links_recorded_at.is_(None),
        )
        .order_by(Page.last_enqueued_at.asc().nulls_last(), Page.mw_page_id.asc())
        .limit(1)
    )


def get_outgoing_pages(session: Session, page: Page) -> Sequence[MediaWikiPageReference]:
    """Return the known outgoing pages from a given page."""

    rows = session.execute(
        select(Page.mw_page_id, Page.title)
        .join(PageLink, Page.mw_page_id == PageLink.to_page_id)
        .where(PageLink.from_page_id == page.mw_page_id)
    ).all()

    return [MediaWikiPageReference(media_wiki_page_id=pid, title=title) for pid, title in rows]


def seed_from_start_page(session: Session, *, start_page: MediaWikiPageReference) -> None:
    """Start page is only a seed; the database/queue is global.

    If the start page has never had its links recorded, enqueue it.
    Otherwise, skip crawling it and enqueue its currently-known outgoing pages.
    """

    start_row = get_or_create_page(session, start_page)
    session.flush()

    if start_row.last_links_recorded_at is None:
        enqueue_page(session, start_page)
        return

    for ref in get_outgoing_pages(session, start_row):
        enqueue_page(session, ref)


def requeue_interrupted_in_progress_pages(session: Session) -> None:
    """Restart-safety: move interrupted in-progress pages back to queued.

    If the script exits mid-crawl, the page would stay in `in_progress`.
    On startup, we re-queue anything that is still missing `last_links_recorded_at`.
    """

    session.query(Page).where(
        Page.crawl_status == PageCrawlStatus.in_progress,
        Page.last_links_recorded_at.is_(None),
    ).update(
        {
            Page.crawl_status: PageCrawlStatus.queued,
            Page.last_enqueued_at: utc_now(),
        },
        synchronize_session=False,
    )


def initialize_queue(engine, *, start_page: MediaWikiPageReference) -> None:
    """Initialize queue state for a run (restart-safe, DB-backed)."""

    with Session(engine) as session:
        requeue_interrupted_in_progress_pages(session)
        seed_from_start_page(session, start_page=start_page)
        session.commit()


def claim_next_page_from_queue(engine) -> tuple[int, str] | None:
    """Atomically claim the next queued page (single-process design).

    Returns (mw_page_id, title) or None if nothing claimable exists.
    """

    with Session(engine) as session:
        page = next_queued_page(session)
        if page is None:
            return None

        page.crawl_status = PageCrawlStatus.in_progress
        page.last_started_at = utc_now()
        session.commit()

        return int(page.mw_page_id), page.title


def expand_page_from_cached_links(engine, *, page_id: int) -> tuple[bool, int, int]:
    """If the page was already crawled, enqueue its known outgoing pages.

    Returns (expanded_from_cache, links_added, links_already_existing).

    For cached expansion, links are not added; they already exist in DB.
    """

    with Session(engine) as session:
        db_page = session.get(Page, page_id)
        if db_page is None:
            return True, 0, 0  # Nothing to do; treat as done.

        if db_page.last_links_recorded_at is None:
            return False, 0, 0

        # Crawl-once policy: do not refetch; just propagate queue from known edges.
        outgoing_refs = list(get_outgoing_pages(session, db_page))
        for outgoing_ref in outgoing_refs:
            enqueue_page(session, outgoing_ref)

        db_page.crawl_status = PageCrawlStatus.done
        db_page.last_finished_at = utc_now()
        session.commit()
        return True, 0, len(outgoing_refs)
    
def persist_fetched_links(
    engine,
    *,
    page_id: int,
    fetch: MediaWikiFetchResult,
    now: datetime,
) -> tuple[int, int]:
    """Persist canonical title + outbound edges for a fetched page."""

    with Session(engine) as session:
        db_page = session.get(Page, page_id)
        assert db_page is not None, f"persist_fetched_links: page_id {page_id} not found"

        # Identity is mw_page_id. Titles can change; keep the stored title fresh.
        if fetch.page.title != db_page.title:
            db_page.title = fetch.page.title

        # Persist edges and enqueue discovered pages.
        links_added, links_existing = record_links(session, from_page=db_page, to_pages=fetch.links, now=now)

        # Record timestamps used for crawl-once and future recrawl policies.
        db_page.last_crawled_at = now
        db_page.last_links_recorded_at = now

        # Clear any previous error state.
        db_page.last_error = None
        db_page.last_error_at = None

        # Mark as completed.
        db_page.crawl_status = PageCrawlStatus.done
        db_page.last_finished_at = now
        session.commit()

        return links_added, links_existing


def record_page_error(engine, *, page_id: int, exc: Exception) -> None:
    """Record an error for a page and mark it as failed."""

    with Session(engine) as session:
        db_page = session.get(Page, page_id)
        if db_page is None:
            return

        db_page.crawl_status = PageCrawlStatus.error
        db_page.last_error = f"{type(exc).__name__}: {exc}"
        db_page.last_error_at = utc_now()
        db_page.last_finished_at = utc_now()
        session.commit()


def export_json(engine, output_path: str) -> None:
    """Optional helper: exports current DB state to a JSON file."""

    with Session(engine) as session:
        pages = session.scalars(select(Page)).all()
        by_id: dict[str, dict[str, object]] = {}

        for page in pages:
            out_titles = [
                session.scalar(select(Page.title).where(Page.mw_page_id == link.to_page_id))
                for link in page.out_links
            ]
            by_id[str(page.mw_page_id)] = {
                "title": page.title,
                "crawl_status": str(page.crawl_status),
                "last_crawled_at": page.last_crawled_at.isoformat() if page.last_crawled_at else None,
                "last_links_recorded_at": page.last_links_recorded_at.isoformat()
                if page.last_links_recorded_at
                else None,
                "links_to": [t for t in out_titles if t],
            }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(by_id, f, indent=2, ensure_ascii=False)
