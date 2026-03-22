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

import signal
import threading
import time

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

from stats_server import StatsWebServer
from walker_stats import WalkerStats


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
    web_port: int,
    stats_window_size: int,
    stop_requested: threading.Event,
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

    stats = WalkerStats(window_size=int(stats_window_size))

    with StatsWebServer(port=web_port) as server:
        print(f"Stats server: http://localhost:{server.port}")
        server.set_status("running")

        crawled_pages_this_run = 0

        while True:
            if stop_requested.is_set():
                print("Stop requested; exiting gracefully (progress saved)")
                server.set_status("stopping…")
                server.publish([["Status", "Stopping…"]])
                return

            # Stop condition for a bounded run.
            if max_pages > 0 and crawled_pages_this_run >= max_pages:
                print(f"Reached WIKI_MAX_PAGES={max_pages}; stopping (progress saved)")
                server.set_status("stopping")
                server.publish([["Status", f"Reached max pages ({max_pages})"]])
                return

            # STEP 1: Claim a page from the global queue.
            claim = claim_next_page_from_queue(engine)
            if claim is None:
                print("Queue empty. Done.")
                server.set_status("done")
                server.publish([["Status", "Queue empty"]])
                return

            page_id, page_title, db_timings = claim
            page_started = time.monotonic()

            try:
                # STEP 2: Expand from cached links if the page is already crawled.
                expanded_from_cache, pages_created, pages_existing, expand_timings = expand_page_from_cached_links(
                    engine, page_id=page_id
                )
                db_timings.expand_cache_seconds = expand_timings.expand_cache_seconds

                visited_title = page_title
                was_fetched = False
                api_fetch_links_seconds = 0.0
                api_resolve_titles_seconds = 0.0
                api_http_requests = 0
                rate_limited_responses = 0

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
                    pages_created, pages_existing, persist_timings = persist_fetched_links(
                        engine, page_id=page_id, fetch=fetch, now=now
                    )
                    db_timings.persist_links_seconds = persist_timings.persist_links_seconds

                    was_fetched = True
                    api_fetch_links_seconds = float(fetch.stats.fetch_links_wall_seconds)
                    api_resolve_titles_seconds = float(fetch.stats.resolve_titles_wall_seconds)
                    api_http_requests = int(fetch.stats.fetch_links_http_requests + fetch.stats.resolve_titles_http_requests)
                    rate_limited_responses = int(fetch.stats.rate_limited_responses)

                # A page has been fully processed for this run (either cached or freshly fetched).
                crawled_pages_this_run += 1

                page_wall = time.monotonic() - page_started

                stats.clear_error()
                stats.record_page(
                    visited_title=str(visited_title),
                    page_wall_seconds=float(page_wall),
                    pages_created=int(pages_created),
                    pages_existing=int(pages_existing),
                    was_fetched=bool(was_fetched),
                    api_fetch_links_seconds=float(api_fetch_links_seconds),
                    api_resolve_titles_seconds=float(api_resolve_titles_seconds),
                    api_http_requests=int(api_http_requests),
                    rate_limited_responses=int(rate_limited_responses),
                    db_timings=db_timings,
                )

                # STEP 5: Optional periodic SQLite backup.
                _maybe_backup_database(
                    backup=backup,
                    db_path=db_path,
                    crawled_pages_this_run=crawled_pages_this_run,
                )

                # STEP 6: Push per-page stats to the web UI.
                _, queued_count, crawled_page_count, progress_timings = get_progress_counts(engine)
                server.set_status("running")

                # Patch the last observation with the progress-counts timing.
                stats.patch_last_db_progress_counts(progress_timings.progress_counts_seconds)

                server.publish(
                    stats.to_table_rows(
                        run_pages=crawled_pages_this_run,
                        queued_count=queued_count,
                        crawled_page_count=crawled_page_count,
                    )
                )

            except Exception as exc:
                # If anything fails during processing, record it on the page and
                # continue to the next queued item.
                record_page_error(engine, page_id=page_id, exc=exc)
                print(f"Error crawling '{page_title}': {exc}")

                stats.record_error(page_title=page_title, exc=exc)
                server.set_status("error")

                _, queued_count, crawled_page_count, _ = get_progress_counts(engine)
                server.publish(
                    stats.to_table_rows(
                        run_pages=crawled_pages_this_run,
                        queued_count=queued_count,
                        crawled_page_count=crawled_page_count,
                    )
                )


if __name__ == "__main__":
    config: WalkerConfig = load_walker_config_from_env()

    stop_requested = threading.Event()

    def _handle_stop_signal(signum: int, _frame) -> None:  # type: ignore[no-untyped-def]
        stop_requested.set()
        signame = None
        try:
            signame = signal.Signals(signum).name
        except Exception:
            pass
        if signame:
            print(f"Received {signame}; will stop after current page")
        else:
            print("Stop requested; will stop after current page")

    signal.signal(signal.SIGINT, _handle_stop_signal)
    if hasattr(signal, "SIGTERM"):
        try:
            signal.signal(signal.SIGTERM, _handle_stop_signal)
        except Exception:
            # Some platforms / environments may not permit SIGTERM handlers.
            pass

    engine = make_engine(config.db_path)

    walk(
        engine,
        db_path=config.db_path,
        backup=config.backup,
        start_title=config.start_title,
        max_pages=config.max_pages,
        sleep_seconds=config.sleep_seconds,
        user_agent=config.user_agent,
        web_port=config.web_port,
        stats_window_size=config.stats_window_size,
        stop_requested=stop_requested,
    )
