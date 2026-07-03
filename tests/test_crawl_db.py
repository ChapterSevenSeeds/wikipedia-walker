from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from sqlalchemy.orm import Session

from crawl_db import (
    DbTimings,
    claim_next_page_from_queue,
    enqueue_page,
    ensure_schema,
    expand_page_from_cached_links,
    export_json,
    get_or_create_page,
    get_outgoing_pages,
    get_progress_counts,
    initialize_queue,
    make_engine,
    persist_fetched_links,
    record_links,
    record_page_error,
    requeue_interrupted_in_progress_pages,
    seed_from_start_page,
    utc_now,
)
from mediawiki_api import MediaWikiPageReference
from models import Page, PageCrawlStatus, PageLink


class TestEngine:
    def test_make_engine_in_memory(self):
        # Use a direct create_engine since models.make_engine uses
        # pool_size/max_overflow which SingletonThreadPool rejects for :memory:
        from sqlalchemy import create_engine as ce
        engine = ce("sqlite://", echo=False)
        ensure_schema(engine)
        assert engine is not None

    def test_ensure_schema(self, in_memory_engine):
        ensure_schema(in_memory_engine)
        with Session(in_memory_engine) as session:
            pages = session.query(Page).all()
            assert pages == []


class TestGetOrCreatePage:
    def test_creates_new_page(self, db_session):
        ref = MediaWikiPageReference(100, "New Page")
        page = get_or_create_page(db_session, ref)

        assert page.mw_page_id == 100
        assert page.title == "New Page"
        assert page in db_session

    def test_returns_existing_page(self, db_session):
        ref = MediaWikiPageReference(200, "Existing")
        get_or_create_page(db_session, ref)
        db_session.commit()

        page2 = get_or_create_page(db_session, ref)
        assert page2.mw_page_id == 200

    def test_updates_title(self, db_session):
        page = Page(mw_page_id=300, title="Old Title")
        db_session.add(page)
        db_session.commit()

        ref = MediaWikiPageReference(300, "New Title")
        updated = get_or_create_page(db_session, ref)
        assert updated.title == "New Title"

    def test_preserves_title_if_same(self, db_session):
        page = Page(mw_page_id=400, title="Stable")
        db_session.add(page)
        db_session.commit()

        ref = MediaWikiPageReference(400, "Stable")
        updated = get_or_create_page(db_session, ref)
        assert updated.title == "Stable"


class TestEnqueuePage:
    def test_enqueues_fresh_page(self, db_session):
        ref = MediaWikiPageReference(1, "Fresh")
        page = enqueue_page(db_session, ref)
        assert page.crawl_status == PageCrawlStatus.queued
        assert page.last_enqueued_at is not None

    def test_already_crawled_stays_done(self, db_session):
        now = utc_now()
        existing = Page(
            mw_page_id=2,
            title="Done Page",
            crawl_status=PageCrawlStatus.done,
            last_links_recorded_at=now,
        )
        db_session.add(existing)
        db_session.commit()

        ref = MediaWikiPageReference(2, "Done Page")
        page = enqueue_page(db_session, ref)
        assert page.crawl_status == PageCrawlStatus.done

    def test_does_not_requeue_in_progress(self, db_session):
        existing = Page(
            mw_page_id=3,
            title="In Progress",
            crawl_status=PageCrawlStatus.in_progress,
        )
        db_session.add(existing)
        db_session.commit()

        ref = MediaWikiPageReference(3, "In Progress")
        page = enqueue_page(db_session, ref)
        assert page.crawl_status == PageCrawlStatus.in_progress


class TestRecordLinks:
    def test_creates_pages_and_edges(self, db_session):
        src = Page(mw_page_id=1, title="Source")
        db_session.add(src)
        db_session.commit()

        now = utc_now()
        refs = [
            MediaWikiPageReference(10, "Target A"),
            MediaWikiPageReference(20, "Target B"),
        ]

        created, existing = record_links(db_session, from_page=src, to_pages=refs, now=now)

        assert created == 2
        assert existing == 0

        t1 = db_session.get(Page, 10)
        assert t1 is not None
        assert t1.title == "Target A"
        assert t1.crawl_status == PageCrawlStatus.queued

        edge = db_session.query(PageLink).filter_by(from_page_id=1, to_page_id=10).first()
        assert edge is not None
        assert edge.last_seen_at is not None

    def test_deduplicates_by_page_id(self, db_session):
        src = Page(mw_page_id=1, title="Source")
        db_session.add(src)
        db_session.commit()

        now = utc_now()
        refs = [
            MediaWikiPageReference(10, "Same ID"),
            MediaWikiPageReference(10, "Same ID Different Title"),
        ]

        created, existing = record_links(db_session, from_page=src, to_pages=refs, now=now)

        assert created == 1
        assert existing == 0

        page = db_session.get(Page, 10)
        assert page.title == "Same ID Different Title"  # Last title wins

    def test_counts_existing_pages(self, db_session):
        src = Page(mw_page_id=1, title="Source")
        db_session.add(src)
        existing_page = Page(mw_page_id=50, title="Already Exists")
        db_session.add(existing_page)
        db_session.commit()

        now = utc_now()
        refs = [
            MediaWikiPageReference(50, "Already Exists"),
            MediaWikiPageReference(60, "New Page"),
        ]

        created, existing = record_links(db_session, from_page=src, to_pages=refs, now=now)

        assert created == 1
        assert existing == 1

    def test_empty_links(self, db_session):
        src = Page(mw_page_id=1, title="Source")
        db_session.add(src)
        db_session.commit()

        created, existing = record_links(db_session, from_page=src, to_pages=[], now=utc_now())

        assert created == 0
        assert existing == 0

    def test_updates_last_seen_at(self, db_session):
        src = Page(mw_page_id=1, title="Source")
        dst = Page(mw_page_id=10, title="Target")
        db_session.add_all([src, dst])
        db_session.commit()

        old_time = datetime(2020, 1, 1, tzinfo=UTC)
        edge = PageLink(from_page_id=1, to_page_id=10, last_seen_at=old_time)
        db_session.add(edge)

        new_time = utc_now()
        created, existing = record_links(
            db_session, from_page=src, to_pages=[MediaWikiPageReference(10, "Target")], now=new_time,
        )

        assert created == 0
        assert existing == 1

        db_session.expire_all()
        edge = db_session.query(PageLink).filter_by(from_page_id=1, to_page_id=10).first()
        assert edge.last_seen_at.timestamp() > old_time.timestamp()


class TestGetOutgoingPages:
    def test_returns_links(self, in_memory_engine):
        with Session(in_memory_engine) as session:
            src = Page(mw_page_id=1, title="Source")
            dst = Page(mw_page_id=2, title="Target")
            session.add_all([src, dst])
            session.commit()

            link = PageLink(from_page_id=1, to_page_id=2)
            session.add(link)
            session.commit()

        with Session(in_memory_engine) as session:
            src = session.get(Page, 1)
            outgoing = get_outgoing_pages(session, src)

        assert len(outgoing) == 1
        assert outgoing[0].media_wiki_page_id == 2
        assert outgoing[0].title == "Target"

    def test_no_links(self, in_memory_engine):
        with Session(in_memory_engine) as session:
            src = Page(mw_page_id=1, title="Lonely")
            session.add(src)
            session.commit()

        with Session(in_memory_engine) as session:
            src = session.get(Page, 1)
            outgoing = get_outgoing_pages(session, src)

        assert outgoing == []


class TestSeedFromStartPage:
    def test_fresh_start_page_is_enqueued(self, in_memory_engine):
        ref = MediaWikiPageReference(1, "Fresh Start")
        with Session(in_memory_engine) as session:
            seed_from_start_page(session, start_page=ref)
            session.commit()

        with Session(in_memory_engine) as session:
            page = session.get(Page, 1)
            assert page is not None
            assert page.crawl_status == PageCrawlStatus.queued
            assert page.last_links_recorded_at is None

    def test_crawled_start_enqueues_outgoing(self, in_memory_engine):
        ref = MediaWikiPageReference(1, "Crawled Start")
        with Session(in_memory_engine) as session:
            page = Page(
                mw_page_id=1,
                title="Crawled Start",
                crawl_status=PageCrawlStatus.done,
                last_links_recorded_at=utc_now(),
                last_crawled_at=utc_now(),
            )
            session.add(page)
            dst = Page(mw_page_id=2, title="Outgoing")
            session.add(dst)
            session.commit()

            link = PageLink(from_page_id=1, to_page_id=2, last_seen_at=utc_now())
            session.add(link)
            session.commit()

            seed_from_start_page(session, start_page=ref)
            session.commit()

        with Session(in_memory_engine) as session:
            dst = session.get(Page, 2)
            assert dst.crawl_status == PageCrawlStatus.queued


class TestInitializeQueue:
    def test_requeues_interrupted_and_seeds(self, in_memory_engine, sample_page_ref):
        with Session(in_memory_engine) as session:
            page = Page(
                mw_page_id=99,
                title="Interrupted",
                crawl_status=PageCrawlStatus.in_progress,
            )
            session.add(page)
            session.commit()

        initialize_queue(in_memory_engine, start_page=sample_page_ref)

        with Session(in_memory_engine) as session:
            interrupted = session.get(Page, 99)
            assert interrupted.crawl_status == PageCrawlStatus.queued

            start = session.get(Page, sample_page_ref.media_wiki_page_id)
            assert start.crawl_status == PageCrawlStatus.queued

    def test_empty_db(self, in_memory_engine, sample_page_ref):
        initialize_queue(in_memory_engine, start_page=sample_page_ref)

        with Session(in_memory_engine) as session:
            page = session.get(Page, sample_page_ref.media_wiki_page_id)
            assert page is not None
            assert page.crawl_status == PageCrawlStatus.queued


class TestRequeueInterrupted:
    def test_requeues_in_progress_pages(self, db_session):
        page = Page(mw_page_id=1, title="Interrupted", crawl_status=PageCrawlStatus.in_progress)
        db_session.add(page)
        db_session.commit()

        requeue_interrupted_in_progress_pages(db_session)
        db_session.commit()

        updated = db_session.get(Page, 1)
        assert updated.crawl_status == PageCrawlStatus.queued

    def test_does_not_requeue_done_pages(self, db_session):
        page = Page(
            mw_page_id=1, title="Done", crawl_status=PageCrawlStatus.done,
            last_links_recorded_at=utc_now(),
        )
        db_session.add(page)
        db_session.commit()

        requeue_interrupted_in_progress_pages(db_session)

        assert db_session.get(Page, 1).crawl_status == PageCrawlStatus.done

    def test_requires_missing_last_links_recorded(self, db_session):
        page = Page(
            mw_page_id=1, title="Stuck", crawl_status=PageCrawlStatus.in_progress,
            last_links_recorded_at=utc_now(),
        )
        db_session.add(page)
        db_session.commit()

        requeue_interrupted_in_progress_pages(db_session)

        assert db_session.get(Page, 1).crawl_status == PageCrawlStatus.in_progress


class TestClaimNextPageFromQueue:
    def test_claims_oldest_queued(self, in_memory_engine):
        ref = MediaWikiPageReference(1, "First")
        initialize_queue(in_memory_engine, start_page=ref)

        claim = claim_next_page_from_queue(in_memory_engine)
        assert claim is not None
        page_id, title, timings = claim
        assert page_id == 1
        assert title == "First"
        assert isinstance(timings, DbTimings)
        assert timings.claim_seconds >= 0

    def test_marks_as_in_progress(self, in_memory_engine, seeded_engine):
        claim = claim_next_page_from_queue(seeded_engine)
        assert claim is not None

        with Session(seeded_engine) as session:
            page = session.get(Page, claim[0])
            assert page.crawl_status == PageCrawlStatus.in_progress

    def test_empty_queue_returns_none(self, in_memory_engine):
        result = claim_next_page_from_queue(in_memory_engine)
        assert result is None

    def test_skips_pages_with_last_links_recorded(self, in_memory_engine):
        with Session(in_memory_engine) as session:
            page = Page(
                mw_page_id=1, title="Already Crawled",
                crawl_status=PageCrawlStatus.done,
                last_links_recorded_at=utc_now(),
            )
            session.add(page)
            session.commit()

        result = claim_next_page_from_queue(in_memory_engine)
        assert result is None

    def test_does_not_claim_in_progress(self, in_memory_engine):
        with Session(in_memory_engine) as session:
            page = Page(
                mw_page_id=1, title="In Progress",
                crawl_status=PageCrawlStatus.in_progress,
            )
            session.add(page)
            session.commit()

        result = claim_next_page_from_queue(in_memory_engine)
        assert result is None

    def test_respects_enqueue_order(self, in_memory_engine):
        with Session(in_memory_engine) as session:
            now = utc_now()
            p1 = Page(mw_page_id=1, title="First", crawl_status=PageCrawlStatus.queued, last_enqueued_at=now)
            p2 = Page(mw_page_id=2, title="Second", crawl_status=PageCrawlStatus.queued, last_enqueued_at=now)
            p3 = Page(mw_page_id=3, title="Last", crawl_status=PageCrawlStatus.queued, last_enqueued_at=now)
            session.add_all([p1, p2, p3])
            session.commit()

        claim = claim_next_page_from_queue(in_memory_engine)
        assert claim is not None
        assert claim[0] == 1


class TestExpandPageFromCachedLinks:
    def test_expands_cached_page(self, in_memory_engine):
        with Session(in_memory_engine) as session:
            now = utc_now()
            src = Page(
                mw_page_id=1, title="Cached",
                crawl_status=PageCrawlStatus.done,
                last_links_recorded_at=now,
                last_crawled_at=now,
            )
            session.add(src)
            dst = Page(mw_page_id=2, title="Outgoing")
            session.add(dst)
            session.commit()
            link = PageLink(from_page_id=1, to_page_id=2, last_seen_at=now)
            session.add(link)
            session.commit()

        expanded, created, existing, timings = expand_page_from_cached_links(
            in_memory_engine, page_id=1,
        )

        assert expanded is True
        assert created == 0
        assert existing == 1

        with Session(in_memory_engine) as session:
            dst = session.get(Page, 2)
            assert dst.crawl_status == PageCrawlStatus.queued

    def test_non_cached_page_returns_false(self, in_memory_engine):
        with Session(in_memory_engine) as session:
            page = Page(mw_page_id=1, title="Not Crawled", crawl_status=PageCrawlStatus.queued)
            session.add(page)
            session.commit()

        expanded, created, existing, timings = expand_page_from_cached_links(
            in_memory_engine, page_id=1,
        )

        assert expanded is False
        assert created == 0
        assert existing == 0

    def test_missing_page_returns_true_with_no_work(self, in_memory_engine):
        expanded, created, existing, timings = expand_page_from_cached_links(
            in_memory_engine, page_id=999,
        )

        assert expanded is True
        assert created == 0
        assert existing == 0


class TestPersistFetchedLinks:
    def test_persists_links_and_updates_page(self, in_memory_engine, sample_page_ref, sample_fetch_result):
        with Session(in_memory_engine) as session:
            page = Page(
                mw_page_id=sample_page_ref.media_wiki_page_id,
                title=sample_page_ref.title,
                crawl_status=PageCrawlStatus.in_progress,
            )
            session.add(page)
            session.commit()

        now = utc_now()
        pages_added, pages_existing, timings = persist_fetched_links(
            in_memory_engine,
            page_id=sample_page_ref.media_wiki_page_id,
            fetch=sample_fetch_result,
            now=now,
        )

        assert pages_added == 2
        assert pages_existing == 0

        with Session(in_memory_engine) as session:
            page = session.get(Page, sample_page_ref.media_wiki_page_id)
            assert page.crawl_status == PageCrawlStatus.done
            assert page.last_links_recorded_at is not None
            stored_ts = page.last_links_recorded_at.replace(tzinfo=now.tzinfo)
            assert abs((stored_ts - now).total_seconds()) < 1.0
            assert page.last_error is None

            links = session.query(PageLink).filter_by(from_page_id=sample_page_ref.media_wiki_page_id).all()
            assert len(links) == 2

    def test_updates_canonical_title(self, in_memory_engine):
        with Session(in_memory_engine) as session:
            page = Page(mw_page_id=1, title="Old Title", crawl_status=PageCrawlStatus.in_progress)
            session.add(page)
            session.commit()

        new_ref = MediaWikiPageReference(1, "New Title")
        from mediawiki_api import MediaWikiFetchResult, MediaWikiFetchStats
        result = MediaWikiFetchResult(
            page=new_ref,
            links=set(),
            stats=MediaWikiFetchStats(
                fetch_links_wall_seconds=0.0,
                resolve_titles_wall_seconds=0.0,
                fetch_links_http_requests=0,
                resolve_titles_http_requests=0,
                rate_limited_responses=0,
                sleep_seconds_start=0.0,
                sleep_seconds_end=0.0,
            ),
        )

        persist_fetched_links(
            in_memory_engine,
            page_id=1,
            fetch=result,
            now=utc_now(),
        )

        with Session(in_memory_engine) as session:
            page = session.get(Page, 1)
            assert page.title == "New Title"

    def test_raises_on_missing_page(self, in_memory_engine, sample_fetch_result):
        with pytest.raises(AssertionError, match="page_id"):
            persist_fetched_links(
                in_memory_engine,
                page_id=999,
                fetch=sample_fetch_result,
                now=utc_now(),
            )


class TestRecordPageError:
    def test_records_error(self, seeded_engine):
        claim = claim_next_page_from_queue(seeded_engine)
        assert claim is not None
        page_id = claim[0]

        record_page_error(seeded_engine, page_id=page_id, exc=ValueError("something broke"))

        with Session(seeded_engine) as session:
            page = session.get(Page, page_id)
            assert page.crawl_status == PageCrawlStatus.error
            assert "ValueError" in page.last_error
            assert "something broke" in page.last_error
            assert page.last_error_at is not None
            assert page.last_finished_at is not None

    def test_handles_missing_page(self, in_memory_engine):
        record_page_error(in_memory_engine, page_id=999, exc=RuntimeError("missing"))
        # Should not raise


class TestGetProgressCounts:
    def test_initial_counts(self, in_memory_engine):
        done, queued, crawled, timings = get_progress_counts(in_memory_engine)
        assert done == 0
        assert queued == 0
        assert crawled == 0
        assert isinstance(timings, DbTimings)
        assert timings.progress_counts_seconds >= 0

    def test_counts_after_seeding(self, seeded_engine, sample_page_ref):
        done, queued, crawled, timings = get_progress_counts(seeded_engine)
        assert queued == 1
        assert done == 0
        assert crawled == 0


class TestExportJson:
    def test_exports_to_file(self, in_memory_engine, sample_page_ref):
        import tempfile
        initialize_queue(in_memory_engine, start_page=sample_page_ref)

        with tempfile.TemporaryDirectory() as tmpdir:
            from pathlib import Path
            output = Path(tmpdir) / "export.json"
            export_json(in_memory_engine, str(output))

            data = json.loads(output.read_text(encoding="utf-8"))
            assert str(sample_page_ref.media_wiki_page_id) in data
            assert data[str(sample_page_ref.media_wiki_page_id)]["title"] == sample_page_ref.title
