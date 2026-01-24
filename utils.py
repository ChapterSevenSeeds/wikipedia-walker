from __future__ import annotations

"""Shared utilities used across the repo.

This module is intentionally kept free of MediaWiki and SQLAlchemy concepts.
The goal is to keep `walker.py` focused on the crawl workflow and persistence
logic while grouping reusable helpers here.
"""

import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Iterator, Sequence


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
