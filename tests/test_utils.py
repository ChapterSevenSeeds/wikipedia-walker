from __future__ import annotations

import sqlite3
import tempfile
from datetime import UTC, datetime
from pathlib import Path

from utils import chunked, run_sqlite_backup, timestamp_for_filename, truncate_ascii


class TestTimestampForFilename:
    def test_format(self):
        dt = datetime(2026, 1, 24, 18, 30, 45, tzinfo=UTC)
        result = timestamp_for_filename(dt)
        assert result == "20260124T183045Z"

    def test_avoids_colons(self):
        dt = datetime(2026, 6, 15, 12, 0, 0, tzinfo=UTC)
        result = timestamp_for_filename(dt)
        assert ":" not in result


class TestTruncateAscii:
    def test_short_string(self):
        assert truncate_ascii("hello", 10) == "hello"

    def test_exact_length(self):
        assert truncate_ascii("hello", 5) == "hello"

    def test_truncates_with_ellipsis(self):
        result = truncate_ascii("hello world", 8)
        assert result == "hello..."

    def test_max_len_three(self):
        assert truncate_ascii("hello", 3) == "hel"

    def test_max_len_two(self):
        assert truncate_ascii("hello", 2) == "he"

    def test_max_len_one(self):
        assert truncate_ascii("hello", 1) == "h"

    def test_max_len_zero(self):
        assert truncate_ascii("hello", 0) == ""

    def test_negative_max_len(self):
        assert truncate_ascii("hello", -1) == ""


class TestChunked:
    def test_empty(self):
        assert list(chunked([], 10)) == []

    def test_single_chunk(self):
        assert list(chunked(["a", "b", "c"], 5)) == [["a", "b", "c"]]

    def test_exact_chunks(self):
        assert list(chunked([1, 2, 3, 4], 2)) == [[1, 2], [3, 4]]

    def test_partial_last_chunk(self):
        assert list(chunked([1, 2, 3, 4, 5], 2)) == [[1, 2], [3, 4], [5]]

    def test_chunk_size_one(self):
        assert list(chunked([1, 2, 3], 1)) == [[1], [2], [3]]


class TestRunSqliteBackup:
    def test_creates_backup_file(self):
        import gc
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            conn = sqlite3.connect(str(db_path))
            conn.execute("CREATE TABLE t (x INTEGER)")
            conn.execute("INSERT INTO t VALUES (42)")
            conn.commit()
            conn.close()
            gc.collect()

            backup_dir = Path(tmpdir) / "backups"
            backup_dir.mkdir()

            result = run_sqlite_backup(
                db_path=str(db_path),
                backup_dir=backup_dir,
                max_count=5,
            )
            gc.collect()

            assert result.exists(), f"Backup file not found at {result}"
            assert result.stat().st_size > 0, f"Backup file {result} is empty"

            # Verify backup content via a fresh connection
            conn2 = sqlite3.connect(str(result))
            try:
                tables = conn2.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
                assert ("t",) in tables, f"Table 't' not found in backup. Tables: {tables}"
                row = conn2.execute("SELECT x FROM t").fetchone()
                assert row is not None, "No rows in table 't'"
                assert row == (42,), f"Expected (42,), got {row}"
            finally:
                conn2.close()
                gc.collect()

    def test_prunes_old_backups(self):
        import gc
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            conn = sqlite3.connect(str(db_path))
            conn.execute("CREATE TABLE t (x INTEGER)")
            conn.execute("INSERT INTO t VALUES (1)")
            conn.close()
            gc.collect()

            backup_dir = Path(tmpdir) / "backups"
            backup_dir.mkdir()

            # Create two older backups manually to exceed max_count
            (backup_dir / "test.db.20250101T000000Z").touch()
            (backup_dir / "test.db.20250102T000000Z").touch()

            run_sqlite_backup(
                db_path=str(db_path),
                backup_dir=backup_dir,
                max_count=2,
            )
            gc.collect()

            remaining = sorted(backup_dir.glob("test.db.*"))
            assert len(remaining) <= 2

    def test_no_prune_below_max(self):
        import gc
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            conn = sqlite3.connect(str(db_path))
            conn.execute("CREATE TABLE t (x INTEGER)")
            conn.execute("INSERT INTO t VALUES (1)")
            conn.close()
            gc.collect()

            backup_dir = Path(tmpdir) / "backups"
            backup_dir.mkdir()

            (backup_dir / "test.db.20250101T000000Z").touch()

            run_sqlite_backup(
                db_path=str(db_path),
                backup_dir=backup_dir,
                max_count=5,
            )
            gc.collect()

            remaining = sorted(backup_dir.glob("test.db.*"))
            assert len(remaining) == 2
