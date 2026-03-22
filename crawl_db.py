"""Database helper functions for the Wikipedia walker.

This module holds the SQLAlchemy/SQLite operations that implement the crawler's
DB-backed queue and edge persistence.

Design goal:
- `walker.py` should not need to open sessions or import ORM models.
    All database reads/writes should flow through functions in this module.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterable, Sequence

from sqlalchemy import func, select, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from mediawiki_api import MediaWikiFetchResult, MediaWikiPageReference
from models import Page, PageCrawlStatus, PageLink, init_db, make_engine as _make_engine, utc_now


@dataclass
class DbTimings:
    """Timing breakdown for a single DB operation."""
    claim_seconds: float = 0.0
    expand_cache_seconds: float = 0.0
    persist_links_seconds: float = 0.0
    progress_counts_seconds: float = 0.0
    record_error_seconds: float = 0.0


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


def get_progress_counts(engine) -> tuple[int, int, int, DbTimings]:
    """Return (done_count, queued_count, crawled_page_count, timings)."""

    t0 = time.monotonic()
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
    elapsed = time.monotonic() - t0

    return int(done_count or 0), int(queued_count or 0), int(crawled_page_count or 0), DbTimings(progress_counts_seconds=elapsed)


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

    Returns (pages_created, pages_already_existing).
    """

    # This is a hot path. Avoid per-link SELECTs by batching:
    # - Query existing pages before upserting to compute accurate page counts
    # - UPSERT pages in bulk
    # - bulk enqueue pages (single UPDATE)
    # - bulk UPSERT edges (single INSERT..ON CONFLICT DO UPDATE)

    to_refs = list(to_pages)
    if not to_refs:
        return 0, 0

    # De-dupe by page_id (titles can vary; last one wins).
    id_to_title: dict[int, str] = {}
    for ref in to_refs:
        id_to_title[int(ref.media_wiki_page_id)] = ref.title

    to_ids = list(id_to_title.keys())

    # SQLite has a default max variable count
    def _chunks(items: list[int], chunk_size: int = 500):
        for i in range(0, len(items), chunk_size):
            yield items[i : i + chunk_size]

    # 1) Query which pages already exist before upserting.
    existing_page_ids: set[int] = set()
    for chunk in _chunks(to_ids):
        rows = session.execute(
            select(Page.mw_page_id).where(Page.mw_page_id.in_(chunk))
        ).scalars()
        existing_page_ids.update(int(v) for v in rows)

    pages_existing = len(existing_page_ids)
    pages_created = len(to_ids) - pages_existing

    # 2) Ensure all referenced pages exist, and keep titles fresh.
    page_rows = [
        {
            "mw_page_id": pid,
            "title": title,
            "crawl_status": PageCrawlStatus.queued,
            "last_enqueued_at": now,
        }
        for pid, title in id_to_title.items()
    ]
    pages_stmt = sqlite_insert(Page).values(page_rows)
    pages_stmt = pages_stmt.on_conflict_do_update(
        index_elements=[Page.mw_page_id],
        set_={"title": pages_stmt.excluded.title},
    )
    session.execute(pages_stmt)

    # 3) Enqueue pages that are eligible (not crawled and not in progress).
    #    This mimics the old `enqueue_page()` policy but does it in bulk.
    for chunk in _chunks(to_ids):
        session.execute(
            update(Page)
            .where(
                Page.mw_page_id.in_(chunk),
                Page.last_links_recorded_at.is_(None),
                Page.crawl_status.not_in({PageCrawlStatus.done, PageCrawlStatus.in_progress}),
            )
            .values(crawl_status=PageCrawlStatus.queued, last_enqueued_at=now)
        )

    # 4) Upsert edges (update last_seen_at for existing rows).
    edge_rows = [
        {"from_page_id": int(from_page.mw_page_id), "to_page_id": int(to_id), "last_seen_at": now}
        for to_id in to_ids
    ]
    edges_stmt = sqlite_insert(PageLink).values(edge_rows)
    edges_stmt = edges_stmt.on_conflict_do_update(
        index_elements=[PageLink.from_page_id, PageLink.to_page_id],
        set_={"last_seen_at": edges_stmt.excluded.last_seen_at},
    )
    session.execute(edges_stmt)

    return pages_created, pages_existing


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


def claim_next_page_from_queue(engine) -> tuple[int, str, DbTimings] | None:
    """Atomically claim the next queued page (single-process design).

    Returns (mw_page_id, title, timings) or None if nothing claimable exists.
    """

    t0 = time.monotonic()
    with Session(engine) as session:
        page = next_queued_page(session)
        if page is None:
            return None

        page.crawl_status = PageCrawlStatus.in_progress
        page.last_started_at = utc_now()
        session.commit()

        elapsed = time.monotonic() - t0
        return int(page.mw_page_id), page.title, DbTimings(claim_seconds=elapsed)


def expand_page_from_cached_links(engine, *, page_id: int) -> tuple[bool, int, int, DbTimings]:
    """If the page was already crawled, enqueue its known outgoing pages.

    Returns (expanded_from_cache, pages_created, pages_already_existing, timings).

    For cached expansion, pages are not created; they already exist in DB.
    """

    t0 = time.monotonic()
    with Session(engine) as session:
        db_page = session.get(Page, page_id)
        if db_page is None:
            elapsed = time.monotonic() - t0
            return True, 0, 0, DbTimings(expand_cache_seconds=elapsed)

        if db_page.last_links_recorded_at is None:
            elapsed = time.monotonic() - t0
            return False, 0, 0, DbTimings(expand_cache_seconds=elapsed)

        # Crawl-once policy: do not refetch; just propagate queue from known edges.
        outgoing_refs = list(get_outgoing_pages(session, db_page))
        for outgoing_ref in outgoing_refs:
            enqueue_page(session, outgoing_ref)

        db_page.crawl_status = PageCrawlStatus.done
        db_page.last_finished_at = utc_now()
        session.commit()
        elapsed = time.monotonic() - t0
        return True, 0, len(outgoing_refs), DbTimings(expand_cache_seconds=elapsed)
    
def persist_fetched_links(
    engine,
    *,
    page_id: int,
    fetch: MediaWikiFetchResult,
    now: datetime,
) -> tuple[int, int, DbTimings]:
    """Persist canonical title + outbound edges for a fetched page.

    Returns (pages_added, pages_existing, timings).
    """

    t0 = time.monotonic()
    with Session(engine) as session:
        db_page = session.get(Page, page_id)
        assert db_page is not None, f"persist_fetched_links: page_id {page_id} not found"

        # Identity is mw_page_id. Titles can change; keep the stored title fresh.
        if fetch.page.title != db_page.title:
            db_page.title = fetch.page.title

        # Persist edges and enqueue discovered pages.
        pages_added, pages_existing = record_links(session, from_page=db_page, to_pages=fetch.links, now=now)

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

        elapsed = time.monotonic() - t0
        return pages_added, pages_existing, DbTimings(persist_links_seconds=elapsed)


def record_page_error(engine, *, page_id: int, exc: Exception) -> DbTimings:
    """Record an error for a page and mark it as failed.

    Returns timings.
    """

    t0 = time.monotonic()
    with Session(engine) as session:
        db_page = session.get(Page, page_id)
        if db_page is None:
            return DbTimings(record_error_seconds=float(time.monotonic() - t0))

        db_page.crawl_status = PageCrawlStatus.error
        db_page.last_error = f"{type(exc).__name__}: {exc}"
        db_page.last_error_at = utc_now()
        db_page.last_finished_at = utc_now()
        session.commit()
    return DbTimings(record_error_seconds=float(time.monotonic() - t0))


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
