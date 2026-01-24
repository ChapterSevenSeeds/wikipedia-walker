"""Environment-driven configuration for the Wikipedia walker.

Goal: centralize all environment variable parsing + validation so the rest of the
codebase deals in typed config objects instead of raw strings.

Walker environment variables:
- WIKI_START_PAGE_TITLE (required)
- WIKI_DB_PATH (optional, default: wikipedia_walker.sqlite3)
- WIKI_MAX_PAGES (optional, default: 200; 0 means unlimited)
- WIKI_SLEEP_SECONDS (optional, default: 0.5)
- WIKI_USER_AGENT (optional)

Backup environment variables are parsed by `utils.load_backup_config_from_env()`.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

ENV_WIKI_START_PAGE_TITLE = "WIKI_START_PAGE_TITLE"
ENV_WIKI_DB_PATH = "WIKI_DB_PATH"
ENV_WIKI_MAX_PAGES = "WIKI_MAX_PAGES"
ENV_WIKI_SLEEP_SECONDS = "WIKI_SLEEP_SECONDS"
ENV_WIKI_USER_AGENT = "WIKI_USER_AGENT"

ENV_BACKUP_ENABLE = "BACKUP_ENABLE"
ENV_BACKUP_PATH = "BACKUP_PATH"
ENV_BACKUP_MAX_COUNT = "BACKUP_MAX_COUNT"
ENV_BACKUP_RUN_AFTER_CRAWL_COUNT = "BACKUP_RUN_AFTER_CRAWL_COUNT"

DEFAULT_DB_PATH = "wikipedia_walker.sqlite3"
DEFAULT_MAX_PAGES = 200
DEFAULT_SLEEP_SECONDS = 0.5


_DEFAULT_USER_AGENT = "wikipedia-walker/1.0 (https://example.invalid; contact: you@example.invalid)"


@dataclass(frozen=True)
class BackupConfig:
    """Configuration for SQLite backups."""

    enabled: bool
    backup_dir: Path | None
    max_count: int
    run_after_crawl_count: int


@dataclass(frozen=True)
class WalkerConfig:
    start_title: str
    db_path: str
    max_pages: int
    sleep_seconds: float
    user_agent: str
    backup: BackupConfig


def _env_truthy(value: str | None) -> bool:
    """Interpret environment-variable style booleans.

    Truthy values: 1, true, t, yes, y, on (case-insensitive)
    """

    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def _require_env_str(name: str) -> str:
    value = os.getenv(name)
    if value is None or not value.strip():
        raise SystemExit(
            f"Missing env var {name}. Example: set {name}=Dream Theater"
        )
    return value.strip()


def _env_str(name: str, default: str) -> str:
    value = os.getenv(name)
    if value is None:
        return default
    value = value.strip()
    return value if value else default


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise SystemExit(f"{name} must be an int (got {raw!r})") from exc


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise SystemExit(f"{name} must be a float (got {raw!r})") from exc


def load_backup_config_from_env() -> BackupConfig:
    """Read backup configuration from BACKUP_* environment variables."""

    enabled = _env_truthy(os.getenv(ENV_BACKUP_ENABLE))
    if not enabled:
        # When disabled, treat backup configuration as non-existent.
        return BackupConfig(enabled=False, backup_dir=None, max_count=0, run_after_crawl_count=0)

    backup_path = os.getenv(ENV_BACKUP_PATH)
    if not backup_path:
        raise SystemExit(f"{ENV_BACKUP_ENABLE} is set but {ENV_BACKUP_PATH} is missing")

    max_count = int(os.getenv(ENV_BACKUP_MAX_COUNT, "5"))
    if max_count < 1:
        raise SystemExit(f"{ENV_BACKUP_MAX_COUNT} must be >= 1")

    run_after = int(os.getenv(ENV_BACKUP_RUN_AFTER_CRAWL_COUNT, "200"))
    if run_after < 1:
        raise SystemExit(f"{ENV_BACKUP_RUN_AFTER_CRAWL_COUNT} must be >= 1")

    backup_dir = Path(backup_path)
    backup_dir.mkdir(parents=True, exist_ok=True)

    return BackupConfig(
        enabled=True,
        backup_dir=backup_dir,
        max_count=max_count,
        run_after_crawl_count=run_after,
    )


def load_walker_config_from_env() -> WalkerConfig:
    """Load and validate walker configuration from environment variables."""

    start_title = _require_env_str(ENV_WIKI_START_PAGE_TITLE)
    db_path = _env_str(ENV_WIKI_DB_PATH, DEFAULT_DB_PATH)

    max_pages = _env_int(ENV_WIKI_MAX_PAGES, DEFAULT_MAX_PAGES)
    if max_pages < 0:
        raise SystemExit(f"{ENV_WIKI_MAX_PAGES} must be >= 0 (0 means unlimited)")

    sleep_seconds = _env_float(ENV_WIKI_SLEEP_SECONDS, DEFAULT_SLEEP_SECONDS)
    if sleep_seconds < 0:
        raise SystemExit(f"{ENV_WIKI_SLEEP_SECONDS} must be >= 0")

    user_agent = _env_str(ENV_WIKI_USER_AGENT, _DEFAULT_USER_AGENT)

    backup = load_backup_config_from_env()

    return WalkerConfig(
        start_title=start_title,
        db_path=db_path,
        max_pages=max_pages,
        sleep_seconds=sleep_seconds,
        user_agent=user_agent,
        backup=backup,
    )
