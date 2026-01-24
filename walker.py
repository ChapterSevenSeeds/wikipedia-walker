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

import os
from crawl_db import (
    claim_next_page_from_queue,
    expand_page_from_cached_links,
    initialize_queue,
    make_engine,
    persist_fetched_links,
    print_progress,
    record_page_error,
    utc_now,
)
from mediawiki_api import MediaWikiPageReference, mediawiki_fetch_links, mediawiki_resolve_titles_to_pages
from utils import BackupConfig, load_backup_config_from_env, run_sqlite_backup


def _resolve_start_page(*, start_title: str, sleep_seconds: float) -> MediaWikiPageReference:
    """Resolve the user-provided start title into a canonical page reference."""

    resolved = mediawiki_resolve_titles_to_pages({start_title}, sleep_seconds=sleep_seconds)
    if len(resolved) == 0:
        raise SystemExit(f"Start page not found: '{start_title}'")
    
    return next(iter(resolved))


def _maybe_backup_database(
    *,
    backup: BackupConfig,
    db_path: str,
    crawled_pages_this_run: int,
) -> None:
    """Perform a periodic SQLite backup, if enabled."""

    if not backup.enabled:
        return

    if crawled_pages_this_run <= 0:
        return

    if crawled_pages_this_run % backup.run_after_crawl_count != 0:
        return

    assert backup.backup_dir is not None
    backup_path = run_sqlite_backup(
        db_path=db_path,
        backup_dir=backup.backup_dir,
        max_count=backup.max_count,
    )
    print(f"Backup created: {backup_path}")


def walk(
    engine,
    *,
    db_path: str,
    backup: BackupConfig,
    start_title: str,
    max_pages: int,
    sleep_seconds: float,
) -> None:
    """Run/resume a walk using the `pages` table as the source-of-truth queue.

    Design goals:
    - Start-page agnostic database: the start page is only a seed.
    - Crawl-once policy by default: if `last_links_recorded_at` is set, we never
      refetch that page in the walker.
    - Restart-safe: pages claimed but not finished are returned to the queue.
    """

    # Resolve the start page up-front so we store a canonical title and a stable
    # mw_page_id (identity is page_id, not title).
    start_page = _resolve_start_page(start_title=start_title, sleep_seconds=sleep_seconds)

    # Prepare the queue.
    initialize_queue(engine, start_page=start_page)

    crawled_pages_this_run = 0
    while True:
        # Stop condition for a bounded run.
        if max_pages > 0 and crawled_pages_this_run >= max_pages:
            print(f"Reached WIKI_MAX_PAGES={max_pages}; stopping (progress saved)")
            return

        # STEP 1: Claim a page from the global queue.
        claim = claim_next_page_from_queue(engine)
        if claim is None:
            print("Queue empty. Done.")
            return

        page_id, page_title = claim

        try:
            # STEP 2: Expand from cached links if the page is already crawled.
            expanded_from_cache = expand_page_from_cached_links(engine, page_id=page_id)

            # STEP 3: If not crawled, fetch outbound links from MediaWiki.
            if not expanded_from_cache:
                fetch = mediawiki_fetch_links(page_title, sleep_seconds=sleep_seconds)
                now = utc_now()

                # STEP 4: Persist the fetched canonical title + edges.
                persist_fetched_links(engine, page_id=page_id, fetch=fetch, now=now)

            # A page has been fully processed for this run (either cached or freshly fetched).
            crawled_pages_this_run += 1

            # STEP 5: Optional periodic SQLite backup.
            _maybe_backup_database(
                backup=backup,
                db_path=db_path,
                crawled_pages_this_run=crawled_pages_this_run,
            )

            # STEP 6: Progress output.
            print_progress(engine)

        except Exception as exc:
            # If anything fails during processing, record it on the page and
            # continue to the next queued item.
            record_page_error(engine, page_id=page_id, exc=exc)
            print(f"Error crawling '{page_title}': {exc}")


if __name__ == "__main__":
    start_title = os.getenv("WIKI_START_PAGE_TITLE")
    if not start_title:
        raise SystemExit(
            "Missing env var WIKI_START_PAGE_TITLE. Example: set WIKI_START_PAGE_TITLE=Dream Theater"
        )

    db_path = os.getenv("WIKI_DB_PATH", "wikipedia_walker.sqlite3")
    max_pages = int(os.getenv("WIKI_MAX_PAGES", "200"))
    sleep_seconds = float(os.getenv("WIKI_SLEEP_SECONDS", "0.5"))

    backup = load_backup_config_from_env()

    engine = make_engine(db_path)

    walk(
        engine,
        db_path=db_path,
        backup=backup,
        start_title=start_title,
        max_pages=max_pages,
        sleep_seconds=sleep_seconds,
    )
