from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum

from sqlalchemy import DateTime, Enum as SAEnum, ForeignKey, Integer, String, UniqueConstraint, create_engine, func
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


def make_engine(db_path: str):
    return create_engine(f"sqlite:///{db_path}", echo=False)


def init_db(engine) -> None:
    Base.metadata.create_all(engine)
