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
from datetime import datetime
from typing import Iterable, Iterator, Sequence

import requests
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from models import Page, PageCrawlStatus, PageLink, init_db, make_engine, utc_now


WIKIPEDIA_API_ENDPOINT = "https://en.wikipedia.org/w/api.php"
MEDIAWIKI_MAX_TITLES_PER_QUERY = 50


@dataclass(frozen=True)
class MWPageRef:
    mw_page_id: int
    title: str


@dataclass(frozen=True)
class FetchResult:
    page: MWPageRef
    links: set[MWPageRef]


def get_user_agent() -> str:
    return os.getenv(
        "WIKI_USER_AGENT",
        "wikipedia-walker/1.0 (https://example.invalid; contact: you@example.invalid)",
    )


def _chunked(items: Sequence[str], chunk_size: int) -> Iterator[list[str]]:
    for i in range(0, len(items), chunk_size):
        yield list(items[i : i + chunk_size])


def mediawiki_resolve_titles_to_pages(
    titles: set[str],
    *,
    sleep_seconds: float,
) -> dict[str, MWPageRef]:
    """Resolve a set of titles to canonical pages.

    This call follows redirects (including redirects that ultimately land on a
    fragment). When the API reports a redirect with `tofragment`, we intentionally
    ignore the fragment and treat it as a link to the whole target page.

    Returns a mapping from the *input title* to an MWPageRef for the resolved page.
    Missing pages are omitted.
    """
    if not titles:
        return {}

    headers = {"User-Agent": get_user_agent()}
    titles_list = sorted(titles)

    resolved: dict[str, MWPageRef] = {}

    for batch in _chunked(titles_list, MEDIAWIKI_MAX_TITLES_PER_QUERY):
        params: dict[str, str] = {
            "action": "query",
            "format": "json",
            "formatversion": "2",
            "redirects": "1",
            "prop": "info",
            "titles": "|".join(batch),
        }

        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

        resp = requests.get(WIKIPEDIA_API_ENDPOINT, params=params, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        query = data.get("query") or {}
        normalized_entries = query.get("normalized") or []
        normalized_map: dict[str, str] = {}
        for n in normalized_entries:
            from_title = n.get("from")
            to_title = n.get("to")
            if not from_title or not to_title:
                continue
            normalized_map[from_title] = to_title

        redirects = query.get("redirects") or []
        redirect_map: dict[str, str] = {}
        for r in redirects:
            from_title = r.get("from")
            to_title = r.get("to")
            if not from_title or not to_title:
                continue

            # If there's a fragment (tofragment), we intentionally ignore it and
            # treat the link as a link to the whole target page.
            redirect_map[from_title] = to_title

        pages = query.get("pages") or []
        by_title: dict[str, MWPageRef] = {}
        for p in pages:
            if p.get("missing"):
                continue
            page_id = p.get("pageid")
            page_title = p.get("title")
            if not isinstance(page_id, int) or not isinstance(page_title, str):
                continue
            by_title[page_title] = MWPageRef(mw_page_id=page_id, title=page_title)

        for original in batch:
            normalized_title = normalized_map.get(original, original)
            resolved_title = redirect_map.get(normalized_title, normalized_title)
            page_ref = by_title.get(resolved_title)
            if page_ref is not None:
                resolved[original] = page_ref

    return resolved


def mediawiki_fetch_links(title: str, *, sleep_seconds: float) -> FetchResult:
    """Fetch outbound article links for a single Wikipedia page title.

    Uses MediaWiki API (action=query&prop=links) and follows pagination via `continue`.
    Only namespace 0 links (articles) are returned.
    """
    headers = {"User-Agent": get_user_agent()}

    link_titles: set[str] = set()
    cont: dict[str, str] = {}
    canonical_page_id: int | None = None
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

        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
            
        resp = requests.get(WIKIPEDIA_API_ENDPOINT, params=params, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        pages = (data.get("query") or {}).get("pages") or []
        if pages:
            page0 = pages[0]
            canonical_page_id = page0.get("pageid") or canonical_page_id
            canonical_title = page0.get("title") or canonical_title
            for link in page0.get("links") or []:
                link_title = link.get("title")
                if link_title:
                    link_titles.add(link_title)

        cont_data = data.get("continue")
        if not cont_data:
            break

        # MediaWiki returns e.g. {"plcontinue": "...", "continue": "-||"}
        cont = {k: str(v) for k, v in cont_data.items() if k != "continue"}

    if canonical_page_id is None:
        raise RuntimeError(f"Could not resolve pageid for '{title}'")

    resolved_links = mediawiki_resolve_titles_to_pages(link_titles, sleep_seconds=sleep_seconds)
    # Ensure uniqueness by page id (titles can change; redirects can cause duplicates).
    by_id: dict[int, str] = {}
    for ref in resolved_links.values():
        by_id.setdefault(ref.mw_page_id, ref.title)

    return FetchResult(
        page=MWPageRef(mw_page_id=int(canonical_page_id), title=canonical_title or title),
        links={MWPageRef(mw_page_id=k, title=v) for k, v in by_id.items()},
    )


def get_or_create_page(session: Session, ref: MWPageRef) -> Page:
    page = session.get(Page, ref.mw_page_id)
    if page is None:
        page = Page(mw_page_id=ref.mw_page_id, title=ref.title)
        session.add(page)
        return page

    # Keep canonical title fresh.
    if page.title != ref.title:
        page.title = ref.title

    return page


def enqueue_page(session: Session, ref: MWPageRef) -> Page:
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
    to_pages: Iterable[MWPageRef],
    now: datetime,
) -> int:
    """Upsert pages + edges; enqueue discovered pages."""
    created_or_touched = 0
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
        .order_by(Page.last_enqueued_at.asc().nulls_last(), Page.mw_page_id.asc())
        .limit(1)
    )


def get_outgoing_pages(session: Session, page: Page) -> Sequence[MWPageRef]:
    # Relationship is available, but this is safer if objects are detached.
    rows = session.execute(
        select(Page.mw_page_id, Page.title)
        .join(PageLink, Page.mw_page_id == PageLink.to_page_id)
        .where(PageLink.from_page_id == page.mw_page_id)
    ).all()
    return [MWPageRef(mw_page_id=pid, title=title) for pid, title in rows]


def seed_from_start_page(session: Session, *, start_page: MWPageRef) -> None:
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


def walk(engine, *, start_title: str, max_pages: int, sleep_seconds: float) -> None:
    """Run/resume a walk using the `pages` table as the source-of-truth queue."""
    start_map = mediawiki_resolve_titles_to_pages({start_title}, sleep_seconds=sleep_seconds)
    start_page = start_map.get(start_title)
    if start_page is None:
        raise SystemExit(f"Start page not found: '{start_title}'")

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

        seed_from_start_page(session, start_page=start_page)
        session.commit()

    visited = 0
    while True:
        if max_pages > 0 and visited >= max_pages:
            print(f"Reached WIKI_MAX_PAGES={max_pages}; stopping (progress saved)")
            return

        page_id: int | None = None
        page_title: str = "(unknown)"

        # 1) Claim a page from the global queue.
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
            page_id = page.mw_page_id
            page_title = page.title
            session.commit()

        # 2) Decide whether we can expand from stored links or must call the API.
        try:
            assert page_id is not None

            fetch_title: str | None = None

            with Session(engine) as session:
                db_page = session.get(Page, page_id)
                if db_page is None:
                    continue

                # Crawl each page only once: if links were already recorded, do not refetch.
                if db_page.last_links_recorded_at is not None:
                    for ref in get_outgoing_pages(session, db_page):
                        enqueue_page(session, ref)

                    db_page.crawl_status = PageCrawlStatus.done
                    db_page.last_finished_at = utc_now()
                    session.commit()
                    visited += 1
                else:
                    fetch_title = db_page.title

            # 3) HTTP fetch (outside any DB session).
            if fetch_title is not None:
                fetch = mediawiki_fetch_links(fetch_title, sleep_seconds=sleep_seconds)
                now = utc_now()

                # 4) Persist results.
                with Session(engine) as session:
                    db_page = session.get(Page, page_id)
                    if db_page is None:
                        continue

                    # Keep canonical title fresh (identity is mw_page_id).
                    if fetch.page.title != db_page.title:
                        db_page.title = fetch.page.title

                    record_links(session, from_page=db_page, to_pages=fetch.links, now=now)

                    db_page.last_crawled_at = now
                    db_page.last_links_recorded_at = now
                    db_page.last_error = None
                    db_page.last_error_at = None

                    db_page.crawl_status = PageCrawlStatus.done
                    db_page.last_finished_at = now
                    session.commit()

                visited += 1

            # 5) Progress print.
            with Session(engine) as session:
                queued = session.scalar(
                    select(func.count()).select_from(Page).where(Page.crawl_status == PageCrawlStatus.queued)
                )
                done = session.scalar(
                    select(func.count()).select_from(Page).where(Page.crawl_status == PageCrawlStatus.done)
                )
                crawled_pages = session.scalar(
                    select(func.count()).select_from(Page).where(Page.last_links_recorded_at.is_not(None))
                )
                print(f"done={done} queued={queued} crawled_pages={crawled_pages}")

        except Exception as exc:
            with Session(engine) as session:
                db_page = session.get(Page, page_id) if page_id is not None else None
                if db_page is not None:
                    db_page.crawl_status = PageCrawlStatus.error
                    db_page.last_error = f"{type(exc).__name__}: {exc}"
                    db_page.last_error_at = utc_now()
                    db_page.last_finished_at = utc_now()
                session.commit()
            print(f"Error crawling '{page_title}': {exc}")


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
                "last_links_recorded_at": page.last_links_recorded_at.isoformat() if page.last_links_recorded_at else None,
                "links_to": [t for t in out_titles if t],
            }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(by_id, f, indent=2, ensure_ascii=False)


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
