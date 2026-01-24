"""Wikipedia walker (resumable) using SQLite + SQLAlchemy and the MediaWiki API.

What this script does:
- Persists crawl progress in a SQLite database (no in-memory frontier).
- Uses the MediaWiki API to fetch outbound links (no HTML scraping).
- Can be stopped/restarted arbitrarily without losing progress.
- Stores timestamps for when pages/links were last recorded, enabling future recrawls.

Environment variables:
- WIKI_START_PAGE_TITLE (required): e.g. "Dream Theater" or "Python (programming language)"
- WIKI_DB_PATH (optional): path to SQLite DB (default: wikipedia_walker.sqlite3)
- WIKI_MAX_PAGES (optional): max pages to process per execution (default: 200)
- WIKI_SLEEP_SECONDS (optional): politeness delay per page (default: 0.5)
- WIKI_USER_AGENT (optional): HTTP User-Agent header

Notes:
- This is designed for single-process use. If you later want parallelism, we can add
  robust locking around the queue selection.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Iterable, Sequence

import requests
from sqlalchemy import (
    DateTime,
    Enum as SAEnum,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
    create_engine,
    func,
    select,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, relationship


WIKIPEDIA_API_ENDPOINT = "https://en.wikipedia.org/w/api.php"


def utc_now() -> datetime:
    return datetime.now(UTC)


class Base(DeclarativeBase):
    pass


class PageCrawlStatus(StrEnum):
    queued = "queued"
    in_progress = "in_progress"
    done = "done"
    error = "error"


class Page(Base):
    __tablename__ = "pages"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    title: Mapped[str] = mapped_column(String, unique=True, index=True)

    crawl_status: Mapped[PageCrawlStatus] = mapped_column(
        SAEnum(PageCrawlStatus, native_enum=False, name="page_crawl_status"),
        default=PageCrawlStatus.queued,
        index=True,
    )
    last_enqueued_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), default=None)
    last_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), default=None)
    last_finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), default=None)

    discovered_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    last_crawled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), default=None)

    # When we last recorded this page's outbound links.
    last_links_recorded_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), default=None)

    last_error: Mapped[str | None] = mapped_column(String, default=None)
    last_error_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), default=None)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )

    out_links: Mapped[list[PageLink]] = relationship(
        back_populates="from_page",
        cascade="all, delete-orphan",
        foreign_keys="PageLink.from_page_id",
    )
    in_links: Mapped[list[PageLink]] = relationship(
        back_populates="to_page",
        cascade="all, delete-orphan",
        foreign_keys="PageLink.to_page_id",
    )


class PageLink(Base):
    __tablename__ = "page_links"
    __table_args__ = (UniqueConstraint("from_page_id", "to_page_id", name="uq_page_links_from_to"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    from_page_id: Mapped[int] = mapped_column(ForeignKey("pages.id"), index=True)
    to_page_id: Mapped[int] = mapped_column(ForeignKey("pages.id"), index=True)

    # When we last observed this edge.
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, index=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )

    from_page: Mapped[Page] = relationship("Page", foreign_keys=[from_page_id], back_populates="out_links")
    to_page: Mapped[Page] = relationship("Page", foreign_keys=[to_page_id], back_populates="in_links")


@dataclass(frozen=True)
class FetchResult:
    title: str
    links: set[str]


def make_engine(db_path: str):
    # future=True is the default in SQLAlchemy 2.x
    return create_engine(f"sqlite:///{db_path}", echo=False)


def init_db(engine) -> None:
    Base.metadata.create_all(engine)


def get_user_agent() -> str:
    return os.getenv(
        "WIKI_USER_AGENT",
        "wikipedia-walker/1.0 (https://example.invalid; contact: you@example.invalid)",
    )


def mediawiki_fetch_links(title: str) -> FetchResult:
    """Fetch outbound article links for a single Wikipedia page title.

    Uses MediaWiki API (action=query&prop=links) and follows pagination via `continue`.
    Only namespace 0 links (articles) are returned.
    """
    headers = {"User-Agent": get_user_agent()}

    links: set[str] = set()
    cont: dict[str, str] = {}
    canonical_title: str | None = None

    while True:
        params: dict[str, str] = {
            "action": "query",
            "format": "json",
            "formatversion": "2",
            "redirects": "1",
            "prop": "links",
            "plnamespace": "0",
            "pllimit": "max",
            "titles": title,
        }
        params.update(cont)

        resp = requests.get(WIKIPEDIA_API_ENDPOINT, params=params, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        pages = (data.get("query") or {}).get("pages") or []
        if pages:
            page0 = pages[0]
            canonical_title = page0.get("title") or canonical_title
            for link in page0.get("links") or []:
                link_title = link.get("title")
                if link_title:
                    links.add(link_title)

        cont_data = data.get("continue")
        if not cont_data:
            break

        # MediaWiki returns e.g. {"plcontinue": "...", "continue": "-||"}
        cont = {k: str(v) for k, v in cont_data.items() if k != "continue"}

    return FetchResult(title=canonical_title or title, links=links)


def get_or_create_page(session: Session, title: str) -> Page:
    page = session.scalar(select(Page).where(Page.title == title))
    if page is not None:
        return page
    page = Page(title=title)
    session.add(page)
    return page



def enqueue_page(session: Session, *, title: str) -> Page:
    now = utc_now()
    page = get_or_create_page(session, title)

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
    to_titles: Iterable[str],
    now: datetime,
) -> int:
    """Upsert pages + edges; enqueue discovered pages."""
    created_or_touched = 0
    for to_title in to_titles:
        to_page = get_or_create_page(session, to_title)

        enqueue_page(session, title=to_page.title)

        link = session.scalar(
            select(PageLink).where(
                PageLink.from_page_id == from_page.id,
                PageLink.to_page_id == to_page.id,
            )
        )
        if link is None:
            session.add(PageLink(from_page=from_page, to_page=to_page, last_seen_at=now))
        else:
            link.last_seen_at = now

        created_or_touched += 1

    return created_or_touched


def next_queued_page(session: Session) -> Page | None:
    return session.scalar(
        select(Page)
        .where(
            Page.crawl_status == PageCrawlStatus.queued,
            Page.last_links_recorded_at.is_(None),
        )
        .order_by(Page.last_enqueued_at.asc().nulls_last(), Page.id.asc())
        .limit(1)
    )


def get_outgoing_titles(session: Session, page: Page) -> Sequence[str]:
    # Relationship is available, but this is safer if objects are detached.
    return session.scalars(
        select(Page.title)
        .join(PageLink, Page.id == PageLink.to_page_id)
        .where(PageLink.from_page_id == page.id)
    ).all()


def seed_from_start_page(session: Session, *, start_title: str) -> None:
    """Start page is only a seed; the database/queue is global.

    If the start page has never had its links recorded, enqueue it.
    Otherwise, skip crawling it and enqueue its currently-known outgoing pages.
    """
    start_page = get_or_create_page(session, start_title)
    session.flush()

    if start_page.last_links_recorded_at is None:
        enqueue_page(session, title=start_page.title)
        return

    for t in get_outgoing_titles(session, start_page):
        enqueue_page(session, title=t)


def walk(engine, *, start_title: str, max_pages: int, sleep_seconds: float) -> None:
    """Run/resume a walk using the `pages` table as the source-of-truth queue."""
    with Session(engine) as session:
        # If the script was interrupted, any in-progress pages that never recorded links
        # should be re-queued.
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

        seed_from_start_page(session, start_title=start_title)
        session.commit()

    visited = 0
    while True:
        if max_pages > 0 and visited >= max_pages:
            print(f"Reached WIKI_MAX_PAGES={max_pages}; stopping (progress saved)")
            return

        with Session(engine) as session:
            page = next_queued_page(session)
            if page is None:
                remaining = session.scalar(
                    select(func.count()).select_from(Page).where(Page.crawl_status == PageCrawlStatus.queued)
                )
                print(f"Queue empty. Done. queued={remaining}")
                return

            page.crawl_status = PageCrawlStatus.in_progress
            page.last_started_at = utc_now()
            session.commit()

            page_id = page.id

        # Do the HTTP fetch outside of a long-running DB transaction.
        try:
            now = utc_now()

            # First check if we can expand without refetching.
            with Session(engine) as session:
                db_page = session.scalar(select(Page).where(Page.id == page_id))
                if db_page is None:
                    continue

                if db_page.last_links_recorded_at is not None:
                    outgoing = get_outgoing_titles(session, db_page)
                    for t in outgoing:
                        enqueue_page(session, title=t)

                    db_page.crawl_status = PageCrawlStatus.done
                    db_page.last_finished_at = utc_now()
                    session.commit()
                else:
                    # Need an API fetch.
                    do_fetch_title = db_page.title
                    session.commit()

                    if sleep_seconds > 0:
                        time.sleep(sleep_seconds)
                    fetch = mediawiki_fetch_links(do_fetch_title)
                    now = utc_now()

                    with Session(engine) as session2:
                        db_page2 = session2.scalar(select(Page).where(Page.id == page_id))
                        if db_page2 is None:
                            continue

                        # If the API canonicalizes the title (redirect), store canonical title.
                        if fetch.title != db_page2.title:
                            existing = session2.scalar(select(Page).where(Page.title == fetch.title))
                            if existing is None:
                                db_page2.title = fetch.title
                            else:
                                # Prefer the existing canonical row; keep both for now, but
                                # mark this one with an error so it's noticeable.
                                db_page2.last_error = "Duplicate title row detected; canonical title already exists"
                                db_page2.last_error_at = utc_now()

                        record_links(
                            session2,
                            from_page=db_page2,
                            to_titles=fetch.links,
                            now=now,
                        )

                        db_page2.last_crawled_at = now
                        db_page2.last_links_recorded_at = now
                        db_page2.last_error = None
                        db_page2.last_error_at = None

                        db_page2.crawl_status = PageCrawlStatus.done
                        db_page2.last_finished_at = now
                        session2.commit()

            visited += 1

            with Session(engine) as session:
                queued = session.scalar(
                    select(func.count()).select_from(Page).where(Page.crawl_status == PageCrawlStatus.queued)
                )
                done = session.scalar(
                    select(func.count()).select_from(Page).where(Page.crawl_status == PageCrawlStatus.done)
                )
                crawled_pages = session.scalar(select(func.count()).select_from(Page).where(Page.last_links_recorded_at.is_not(None)))
                print(f"done={done} queued={queued} crawled_pages={crawled_pages}")

        except Exception as exc:
            with Session(engine) as session:
                db_page = session.scalar(select(Page).where(Page.id == page_id))
                if db_page is not None:
                    db_page.crawl_status = PageCrawlStatus.error
                    db_page.last_error = f"{type(exc).__name__}: {exc}"
                    db_page.last_error_at = utc_now()
                    db_page.last_finished_at = utc_now()
                session.commit()
            title_for_log = "(unknown)"
            with Session(engine) as session:
                p = session.scalar(select(Page).where(Page.id == page_id))
                if p is not None:
                    title_for_log = p.title
            print(f"Error crawling '{title_for_log}': {exc}")


def export_json(engine, output_path: str) -> None:
    """Optional helper: exports current DB state to a JSON file."""
    with Session(engine) as session:
        pages = session.scalars(select(Page)).all()
        by_title: dict[str, dict[str, object]] = {}

        for page in pages:
            out_titles = [
                session.scalar(select(Page.title).where(Page.id == link.to_page_id))
                for link in page.out_links
            ]
            by_title[page.title] = {
                "crawl_status": str(page.crawl_status),
                "last_crawled_at": page.last_crawled_at.isoformat() if page.last_crawled_at else None,
                "last_links_recorded_at": page.last_links_recorded_at.isoformat() if page.last_links_recorded_at else None,
                "links_to": [t for t in out_titles if t],
            }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(by_title, f, indent=2, ensure_ascii=False)


if __name__ == "__main__":
    start_title = os.getenv("WIKI_START_PAGE_TITLE")
    if not start_title:
        raise SystemExit(
            "Missing env var WIKI_START_PAGE_TITLE. Example: set WIKI_START_PAGE_TITLE=Dream Theater"
        )

    db_path = os.getenv("WIKI_DB_PATH", "wikipedia_walker.sqlite3")
    max_pages = int(os.getenv("WIKI_MAX_PAGES", "200"))
    sleep_seconds = float(os.getenv("WIKI_SLEEP_SECONDS", "0.5"))

    engine = make_engine(db_path)
    init_db(engine)

    walk(engine, start_title=start_title, max_pages=max_pages, sleep_seconds=sleep_seconds)

    if os.getenv("WIKI_EXPORT_JSON") == "1":
        export_json(engine, output_path="wikipedia_pages.json")