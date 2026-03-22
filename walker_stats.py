"""Sliding-window stats aggregation for the Wikipedia walker.

Keeps `walker.py` focused on crawling, while this module owns how stats are
computed (windowed vs run-wide) and how they are presented in table form.
"""

from __future__ import annotations

import time
from collections import deque
from collections.abc import Iterable
from dataclasses import dataclass, replace

import humanfriendly

from crawl_db import DbTimings
from utils import truncate_ascii


@dataclass(frozen=True)
class PageObservation:
    page_wall_seconds: float
    pages_created: int
    pages_existing: int
    was_fetched: bool
    api_fetch_links_seconds: float
    api_resolve_titles_seconds: float
    api_http_requests: int
    rate_limited_responses: int
    db_claim_seconds: float
    db_expand_cache_seconds: float
    db_persist_links_seconds: float
    db_progress_counts_seconds: float
    db_record_error_seconds: float


class WalkerStats:
    def __init__(self, *, window_size: int) -> None:
        if window_size < 1:
            raise ValueError("window_size must be >= 1")

        self._window_size = int(window_size)
        self._window: deque[PageObservation] = deque(maxlen=self._window_size)
        self._run_started = time.monotonic()
        self._last_visited_title: str | None = None
        self._last_error: str | None = None

        self._max_key_width = 40
        self._max_value_width = 160

    @property
    def window_size(self) -> int:
        return self._window_size

    @property
    def window_len(self) -> int:
        return len(self._window)

    def record_page(
        self,
        *,
        visited_title: str,
        page_wall_seconds: float,
        pages_created: int,
        pages_existing: int,
        was_fetched: bool,
        api_fetch_links_seconds: float,
        api_resolve_titles_seconds: float,
        api_http_requests: int,
        rate_limited_responses: int,
        db_timings: DbTimings | None = None,
    ) -> None:
        self._last_visited_title = visited_title
        t = db_timings or DbTimings()
        self._window.append(
            PageObservation(
                page_wall_seconds=float(page_wall_seconds),
                pages_created=int(pages_created),
                pages_existing=int(pages_existing),
                was_fetched=bool(was_fetched),
                api_fetch_links_seconds=float(api_fetch_links_seconds),
                api_resolve_titles_seconds=float(api_resolve_titles_seconds),
                api_http_requests=int(api_http_requests),
                rate_limited_responses=int(rate_limited_responses),
                db_claim_seconds=float(t.claim_seconds),
                db_expand_cache_seconds=float(t.expand_cache_seconds),
                db_persist_links_seconds=float(t.persist_links_seconds),
                db_progress_counts_seconds=float(t.progress_counts_seconds),
                db_record_error_seconds=float(t.record_error_seconds),
            )
        )

    def record_error(self, *, page_title: str, exc: BaseException, db_timings: DbTimings | None = None) -> None:
        self._last_error = f"{page_title}: {exc}"
        self._last_error_db_timings = db_timings

    def patch_last_db_progress_counts(self, seconds: float) -> None:
        """Update the most recent observation's progress_counts timing.

        This is called after record_page because the progress query
        happens after the page is recorded.
        """
        if not self._window:
            return
        old = self._window[-1]
        self._window[-1] = replace(old, db_progress_counts_seconds=float(seconds))

    def clear_error(self) -> None:
        self._last_error = None

    def _fmt_duration(self, seconds: float) -> str:
        if seconds < 0:
            seconds = 0.0
        return humanfriendly.format_timespan(seconds)

    def _fmt_percent(self, value: float) -> str:
        return f"{value * 100.0:.1f}%"

    def _avg(self, values: Iterable[float]) -> float:
        """Compute average from an iterable without creating intermediate lists."""
        total = 0.0
        count = 0
        for val in values:
            total += val
            count += 1
        if count == 0:
            return 0.0
        return total / count

    def _run_wall_seconds(self) -> float:
        return max(0.0, float(time.monotonic() - self._run_started))

    def _window_avg_page_seconds(self) -> float:
        return self._avg(o.page_wall_seconds for o in self._window)

    def to_table_rows(
        self,
        *,
        run_pages: int,
        queued_count: int,
        crawled_page_count: int,
    ) -> list[list[str]]:
        visited_title = self._last_visited_title or ""

        avg_page_seconds = self._window_avg_page_seconds()
        eta_seconds = avg_page_seconds * float(queued_count)

        total_pages_created_window = sum(o.pages_created for o in self._window)
        total_pages_existing_window = sum(o.pages_existing for o in self._window)
        total_pages_seen = total_pages_created_window + total_pages_existing_window
        cache_hit_rate = (
            (total_pages_existing_window / total_pages_seen) if total_pages_seen > 0 else 0.0
        )

        fetched = [o for o in self._window if o.was_fetched]
        avg_api_fetch_seconds = self._avg(o.api_fetch_links_seconds for o in fetched)
        avg_api_resolve_seconds = self._avg(o.api_resolve_titles_seconds for o in fetched)
        avg_api_total_seconds = avg_api_fetch_seconds + avg_api_resolve_seconds
        avg_api_http_requests = self._avg(float(o.api_http_requests) for o in fetched)
        avg_rate_limited_responses = self._avg(float(o.rate_limited_responses) for o in fetched)

        rows: list[tuple[str, str]] = [
            ("Visited title", repr(visited_title)),
            ("Run pages", str(int(run_pages))),
            ("Progress queued", str(int(queued_count))),
            ("Progress crawled pages", str(int(crawled_page_count))),
            (
                f"Avg page time (last {len(self._window)}/{self._window_size})",
                self._fmt_duration(avg_page_seconds),
            ),
            ("Run wall time", self._fmt_duration(self._run_wall_seconds())),
            ("ETA (drain queue)", self._fmt_duration(eta_seconds)),
            (
                "Link cache hit rate",
                f"{self._fmt_percent(cache_hit_rate)} ({total_pages_existing_window}/{total_pages_seen})",
            ),
        ]

        if fetched:
            rows.extend(
                [
                    (
                        f"Avg API total (last {len(fetched)} fetched)",
                        self._fmt_duration(avg_api_total_seconds),
                    ),
                    ("Avg API fetch_links", self._fmt_duration(avg_api_fetch_seconds)),
                    ("Avg API resolve_titles", self._fmt_duration(avg_api_resolve_seconds)),
                    ("Avg API HTTP requests", f"{avg_api_http_requests:.1f}"),
                    ("Avg rate-limited responses", f"{avg_rate_limited_responses:.2f}"),
                ]
            )

        # --- DB timing breakdown ---
        wlen = len(self._window)
        avg_db_claim = self._avg(o.db_claim_seconds for o in self._window)
        avg_db_expand = self._avg(o.db_expand_cache_seconds for o in self._window)
        avg_db_persist = self._avg(o.db_persist_links_seconds for o in self._window)
        avg_db_progress = self._avg(o.db_progress_counts_seconds for o in self._window)
        avg_db_error = self._avg(o.db_record_error_seconds for o in self._window)
        avg_db_total = avg_db_claim + avg_db_expand + avg_db_persist + avg_db_progress + avg_db_error

        rows.append((
            f"Avg DB total (last {wlen})",
            self._fmt_duration(avg_db_total),
        ))
        rows.append(("Avg DB claim_next_page", self._fmt_duration(avg_db_claim)))
        rows.append(("Avg DB expand_cache", self._fmt_duration(avg_db_expand)))
        rows.append(("Avg DB persist_links", self._fmt_duration(avg_db_persist)))
        rows.append(("Avg DB progress_counts", self._fmt_duration(avg_db_progress)))
        if avg_db_error > 0:
            rows.append(("Avg DB record_error", self._fmt_duration(avg_db_error)))

        if self._last_error:
            rows.append(("Last error", self._last_error))

        table_rows = [
            [
                truncate_ascii(k, self._max_key_width),
                truncate_ascii(v, self._max_value_width),
            ]
            for (k, v) in rows
        ]
        return table_rows
