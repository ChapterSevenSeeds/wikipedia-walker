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

import time

import humanfriendly
from tabulate import tabulate

from config import BackupConfig, WalkerConfig, load_walker_config_from_env

from crawl_db import (
    claim_next_page_from_queue,
    expand_page_from_cached_links,
    initialize_queue,
    make_engine,
    persist_fetched_links,
    get_progress_counts,
    record_page_error,
    utc_now,
)
from mediawiki_api import MediaWikiPageReference, mediawiki_fetch_links, mediawiki_resolve_titles_to_pages
from utils import run_sqlite_backup


def _truncate_ascii(text: str, max_len: int) -> str:
    if max_len <= 0:
        return ""
    if len(text) <= max_len:
        return text
    if max_len <= 3:
        return text[:max_len]
    return text[: max_len - 3] + "..."


def _resolve_start_page(
    *,
    start_title: str,
    sleep_seconds: float,
    user_agent: str,
) -> MediaWikiPageReference:
    """Resolve the user-provided start title into a canonical page reference."""

    resolved = mediawiki_resolve_titles_to_pages(
        {start_title},
        sleep_seconds=sleep_seconds,
        user_agent=user_agent,
    )
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
    user_agent: str,
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
    start_page = _resolve_start_page(
        start_title=start_title,
        sleep_seconds=sleep_seconds,
        user_agent=user_agent,
    )

    # Prepare the queue.
    initialize_queue(engine, start_page=start_page)

    crawled_pages_this_run = 0
    run_started = time.monotonic()

    pages_fetched_this_run = 0
    pages_expanded_from_cache_this_run = 0

    total_pages_created_this_run = 0
    total_pages_existing_this_run = 0

    total_page_wall_seconds = 0.0

    total_api_fetch_links_seconds = 0.0
    total_api_resolve_titles_seconds = 0.0
    total_api_http_requests = 0
    total_rate_limited_responses = 0

    def _fmt_duration(seconds: float) -> str:
        if seconds < 0:
            seconds = 0.0
        # humanfriendly expects seconds.
        return humanfriendly.format_timespan(seconds)

    def _fmt_percent(value: float) -> str:
        return f"{value * 100.0:.1f}%"

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

        page_started = time.monotonic()

        try:
            # STEP 2: Expand from cached links if the page is already crawled.
            expanded_from_cache, pages_created, pages_existing = expand_page_from_cached_links(
                engine, page_id=page_id
            )

            visited_title = page_title
            fetch = None

            # STEP 3: If not crawled, fetch outbound links from MediaWiki.
            if not expanded_from_cache:
                fetch = mediawiki_fetch_links(
                    page_title,
                    sleep_seconds=sleep_seconds,
                    user_agent=user_agent,
                )
                visited_title = fetch.page.title
                now = utc_now()

                # STEP 4: Persist the fetched canonical title + edges.
                pages_created, pages_existing = persist_fetched_links(
                    engine, page_id=page_id, fetch=fetch, now=now
                )

                pages_fetched_this_run += 1
                total_api_fetch_links_seconds += float(fetch.stats.fetch_links_wall_seconds)
                total_api_resolve_titles_seconds += float(fetch.stats.resolve_titles_wall_seconds)
                total_api_http_requests += int(
                    fetch.stats.fetch_links_http_requests + fetch.stats.resolve_titles_http_requests
                )
                total_rate_limited_responses += int(fetch.stats.rate_limited_responses)
            else:
                pages_expanded_from_cache_this_run += 1

            # A page has been fully processed for this run (either cached or freshly fetched).
            crawled_pages_this_run += 1

            page_wall = time.monotonic() - page_started
            total_page_wall_seconds += float(page_wall)

            total_pages_created_this_run += int(pages_created)
            total_pages_existing_this_run += int(pages_existing)

            # STEP 5: Optional periodic SQLite backup.
            _maybe_backup_database(
                backup=backup,
                db_path=db_path,
                crawled_pages_this_run=crawled_pages_this_run,
            )

            # STEP 6: Verbose per-page stats.
            done_count, queued_count, crawled_page_count = get_progress_counts(engine)
            avg_page_seconds = total_page_wall_seconds / max(1, crawled_pages_this_run)
            eta_seconds = avg_page_seconds * queued_count

            total_pages_seen = total_pages_created_this_run + total_pages_existing_this_run
            page_cache_hit_rate = (
                (total_pages_existing_this_run / total_pages_seen) if total_pages_seen > 0 else 0.0
            )

            avg_api_total_seconds = 0.0
            avg_api_fetch_seconds = 0.0
            avg_api_resolve_seconds = 0.0
            avg_api_http_requests = 0.0
            if pages_fetched_this_run > 0:
                avg_api_fetch_seconds = total_api_fetch_links_seconds / pages_fetched_this_run
                avg_api_resolve_seconds = total_api_resolve_titles_seconds / pages_fetched_this_run
                avg_api_total_seconds = avg_api_fetch_seconds + avg_api_resolve_seconds
                avg_api_http_requests = total_api_http_requests / pages_fetched_this_run

            run_wall = time.monotonic() - run_started

            rows: list[tuple[str, str]] = [
                ("Visited title", repr(visited_title)),
                ("Run pages", str(crawled_pages_this_run)),
                ("Progress queued", str(queued_count)),
                ("Progress crawled_pages", str(crawled_page_count)),
                ("Avg page time", _fmt_duration(avg_page_seconds)),
                ("Run wall time", _fmt_duration(run_wall)),
                ("ETA (drain queue)", _fmt_duration(eta_seconds)),
                (
                    "Link cache hit rate",
                    f"{_fmt_percent(page_cache_hit_rate)} ({total_pages_existing_this_run}/{total_pages_seen})",
                ),
            ]

            # Keep output stable across platforms:
            # - `grid` format is ASCII-only
            # - `disable_numparse=True` avoids locale/float formatting surprises
            max_key_width = 34
            max_value_width = 110
            table_rows = [
                (_truncate_ascii(k, max_key_width), _truncate_ascii(v, max_value_width)) for k, v in rows
            ]
            print(
                tabulate(
                    table_rows,
                    headers=["Metric", "Value"],
                    tablefmt="grid",
                    colalign=("left", "left"),
                    disable_numparse=True,
                )
            )

        except Exception as exc:
            # If anything fails during processing, record it on the page and
            # continue to the next queued item.
            record_page_error(engine, page_id=page_id, exc=exc)
            print(f"Error crawling '{page_title}': {exc}")


if __name__ == "__main__":
    config: WalkerConfig = load_walker_config_from_env()

    engine = make_engine(config.db_path)

    walk(
        engine,
        db_path=config.db_path,
        backup=config.backup,
        start_title=config.start_title,
        max_pages=config.max_pages,
        sleep_seconds=config.sleep_seconds,
        user_agent=config.user_agent,
    )
