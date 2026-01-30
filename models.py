from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum

from sqlalchemy import DateTime, Enum as SAEnum, ForeignKey, Integer, String, UniqueConstraint, create_engine, event, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


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

    # Stable unique identity for Wikipedia pages.
    # https://www.mediawiki.org/wiki/Manual:Page_table
    mw_page_id: Mapped[int] = mapped_column(Integer, primary_key=True)

    # Canonical page title as returned by the API at the time we last saw it.
    # Titles can change; mw_page_id is the stable identity.
    title: Mapped[str] = mapped_column(String, index=True)

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
    from_page_id: Mapped[int] = mapped_column(ForeignKey("pages.mw_page_id"), index=True)
    to_page_id: Mapped[int] = mapped_column(ForeignKey("pages.mw_page_id"), index=True)

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


def make_engine(db_path: str):
    engine = create_engine(
        f"sqlite:///{db_path}",
        echo=False,
        connect_args={"timeout": 30},
        # This app is single-process and effectively single-threaded.
        # Keep the pool at a single connection so SQLite caches/PRAGMAs are not
        # duplicated across multiple connections (which can waste a lot of RAM).
        pool_size=1,
        max_overflow=0,
    )

    @event.listens_for(engine, "connect")
    def _set_sqlite_pragmas(dbapi_connection, connection_record) -> None:  # type: ignore[no-redef]
        # Pragmas are per-connection.
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("PRAGMA foreign_keys=ON;")

            # WAL drastically reduces writer fsync pressure for write-heavy workloads.
            cursor.execute("PRAGMA journal_mode=WAL;")

            # NORMAL is a common tradeoff: much faster than FULL with acceptable durability
            # for a crawler DB (you may lose last transaction on OS crash).
            cursor.execute("PRAGMA synchronous=NORMAL;")

            # Keep temporary data in memory.
            cursor.execute("PRAGMA temp_store=MEMORY;")

            # Allow readers/writers to wait rather than erroring immediately.
            cursor.execute("PRAGMA busy_timeout=5000;")

            # Page cache size (negative means KiB). Use more RAM to cut disk IO.
            # -524288 ~= 512 MiB.
            cursor.execute("PRAGMA cache_size=-524288;")

            # Memory-map up to 1 GiB of the DB file (best-effort).
            cursor.execute("PRAGMA mmap_size=1073741824;")

            # Cap WAL file growth (best-effort; still may exceed temporarily).
            cursor.execute("PRAGMA journal_size_limit=268435456;")

            # Reduce checkpoint frequency to avoid extra write bursts.
            # (Pages are 4 KiB by default; 10000 pages ~= 40 MiB.)
            cursor.execute("PRAGMA wal_autocheckpoint=10000;")
        finally:
            cursor.close()

    return engine


def init_db(engine) -> None:
    Base.metadata.create_all(engine)
