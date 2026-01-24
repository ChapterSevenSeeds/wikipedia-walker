from __future__ import annotations

"""Shared utilities used across the repo.

This module is intentionally kept free of MediaWiki and SQLAlchemy concepts.
The goal is to keep `walker.py` focused on the crawl workflow and persistence
logic while grouping reusable helpers here.
"""

import os
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Iterator, Sequence


@dataclass(frozen=True)
class BackupConfig:
    """Configuration for SQLite backups."""

    enabled: bool
    backup_dir: Path | None
    max_count: int
    run_after_crawl_count: int


def env_truthy(value: str | None) -> bool:
    """Interpret environment-variable style booleans.

    Truthy values: 1, true, t, yes, y, on (case-insensitive)
    """

    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def load_backup_config_from_env() -> BackupConfig:
    """Read backup configuration from BACKUP_* environment variables."""

    enabled = env_truthy(os.getenv("BACKUP_ENABLE"))
    if not enabled:
        # When disabled, treat backup configuration as non-existent.
        return BackupConfig(enabled=False, backup_dir=None, max_count=0, run_after_crawl_count=0)

    backup_path = os.getenv("BACKUP_PATH")
    if not backup_path:
        raise SystemExit("BACKUP_ENABLE is set but BACKUP_PATH is missing")

    max_count = int(os.getenv("BACKUP_MAX_COUNT", "5"))
    if max_count < 1:
        raise SystemExit("BACKUP_MAX_COUNT must be >= 1")

    run_after = int(os.getenv("BACKUP_RUN_AFTER_CRAWL_COUNT", "200"))
    if run_after < 1:
        raise SystemExit("BACKUP_RUN_AFTER_CRAWL_COUNT must be >= 1")

    backup_dir = Path(backup_path)
    backup_dir.mkdir(parents=True, exist_ok=True)

    return BackupConfig(
        enabled=True,
        backup_dir=backup_dir,
        max_count=max_count,
        run_after_crawl_count=run_after,
    )


def timestamp_for_filename(dt: datetime) -> str:
    """Format a timestamp for filesystem-safe filenames.

    Example: 20260124T183045Z

    Notes:
    - Uses UTC
    - Avoids ':' which is awkward on Windows
    """

    return dt.strftime("%Y%m%dT%H%M%SZ")


def run_sqlite_backup(*, db_path: str, backup_dir: Path, max_count: int) -> Path:
    """Create a consistent SQLite backup using SQLite's Online Backup API.

    This is the SQLite-recommended approach for backing up a database that may
    be in use: https://www.sqlite.org/backup.html

    The backup file is created in `backup_dir` and named like:
      <db_filename>.<timestamp>

    After creation, older backups are pruned (best-effort) so that at most
    `max_count` backups remain.
    """

    source_path = Path(db_path)
    backup_name = f"{source_path.name}.{timestamp_for_filename(datetime.now(UTC))}"
    dest_path = backup_dir / backup_name

    # Use the SQLite Online Backup API (sqlite3.Connection.backup).
    with sqlite3.connect(db_path, timeout=30) as src_connection:
        with sqlite3.connect(str(dest_path), timeout=30) as dst_connection:
            src_connection.backup(dst_connection)

    # Prune old backups beyond max_count.
    candidates = sorted(
        (p for p in backup_dir.glob(f"{source_path.name}.*") if p.is_file()),
        key=lambda p: p.stat().st_mtime,
    )
    if len(candidates) > max_count:
        for old_path in candidates[: len(candidates) - max_count]:
            try:
                old_path.unlink()
            except OSError:
                # Best-effort cleanup; backups are still valid even if pruning fails.
                pass

    return dest_path


def chunked(items: Sequence[str], chunk_size: int) -> Iterator[list[str]]:
    """Yield items in fixed-size chunks."""

    for start_index in range(0, len(items), chunk_size):
        yield list(items[start_index : start_index + chunk_size])
