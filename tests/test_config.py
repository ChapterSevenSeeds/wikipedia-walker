from __future__ import annotations

import os
from pathlib import Path

import pytest

from config import (
    BackupConfig,
    WalkerConfig,
    load_backup_config_from_env,
    load_walker_config_from_env,
)


ENV_VARS = [
    "WIKI_START_PAGE_TITLE", "WIKI_DB_PATH", "WIKI_MAX_PAGES",
    "WIKI_SLEEP_SECONDS", "WIKI_USER_AGENT", "WIKI_WEB_PORT",
    "WIKI_STATS_WINDOW_SIZE",
    "BACKUP_ENABLE", "BACKUP_PATH", "BACKUP_MAX_COUNT", "BACKUP_RUN_AFTER_CRAWL_COUNT",
]


@pytest.fixture(autouse=True)
def _clean_env():
    saved = {}
    for var in ENV_VARS:
        saved[var] = os.environ.get(var)
        if var in os.environ:
            del os.environ[var]
    yield
    for var, val in saved.items():
        if val is not None:
            os.environ[var] = val
        elif var in os.environ:
            del os.environ[var]


class TestLoadWalkerConfigFromEnv:
    def test_all_defaults_except_start_title(self):
        os.environ["WIKI_START_PAGE_TITLE"] = "Dream Theater"
        config = load_walker_config_from_env()
        assert config.start_title == "Dream Theater"
        assert config.db_path == "wikipedia_walker.sqlite3"
        assert config.max_pages == 200
        assert config.sleep_seconds == 0.5
        assert config.user_agent == "wikipedia-walker/1.0 (https://example.invalid; contact: you@example.invalid)"
        assert config.web_port == 8000
        assert config.stats_window_size == 50
        assert config.backup.enabled is False

    def test_custom_values(self):
        os.environ["WIKI_START_PAGE_TITLE"] = "Python (programming language)"
        os.environ["WIKI_DB_PATH"] = "custom.db"
        os.environ["WIKI_MAX_PAGES"] = "100"
        os.environ["WIKI_SLEEP_SECONDS"] = "1.5"
        os.environ["WIKI_USER_AGENT"] = "test-bot/1.0"
        os.environ["WIKI_WEB_PORT"] = "9000"
        os.environ["WIKI_STATS_WINDOW_SIZE"] = "20"

        config = load_walker_config_from_env()
        assert config.start_title == "Python (programming language)"
        assert config.db_path == "custom.db"
        assert config.max_pages == 100
        assert config.sleep_seconds == 1.5
        assert config.user_agent == "test-bot/1.0"
        assert config.web_port == 9000
        assert config.stats_window_size == 20

    def test_max_pages_zero(self):
        os.environ["WIKI_START_PAGE_TITLE"] = "Test"
        os.environ["WIKI_MAX_PAGES"] = "0"
        config = load_walker_config_from_env()
        assert config.max_pages == 0

    def test_missing_start_title_exits(self):
        with pytest.raises(SystemExit, match="Missing env var WIKI_START_PAGE_TITLE"):
            load_walker_config_from_env()

    def test_negative_max_pages_exits(self):
        os.environ["WIKI_START_PAGE_TITLE"] = "Test"
        os.environ["WIKI_MAX_PAGES"] = "-1"
        with pytest.raises(SystemExit, match="must be >= 0"):
            load_walker_config_from_env()

    def test_negative_sleep_seconds_exits(self):
        os.environ["WIKI_START_PAGE_TITLE"] = "Test"
        os.environ["WIKI_SLEEP_SECONDS"] = "-0.5"
        with pytest.raises(SystemExit, match="must be >= 0"):
            load_walker_config_from_env()

    def test_invalid_int_exits(self):
        os.environ["WIKI_START_PAGE_TITLE"] = "Test"
        os.environ["WIKI_MAX_PAGES"] = "not-a-number"
        with pytest.raises(SystemExit, match="WIKI_MAX_PAGES must be an int"):
            load_walker_config_from_env()

    def test_invalid_float_exits(self):
        os.environ["WIKI_START_PAGE_TITLE"] = "Test"
        os.environ["WIKI_SLEEP_SECONDS"] = "not-a-float"
        with pytest.raises(SystemExit, match="WIKI_SLEEP_SECONDS must be a float"):
            load_walker_config_from_env()

    def test_web_port_out_of_range_low(self):
        os.environ["WIKI_START_PAGE_TITLE"] = "Test"
        os.environ["WIKI_WEB_PORT"] = "0"
        with pytest.raises(SystemExit, match="WIKI_WEB_PORT must be between"):
            load_walker_config_from_env()

    def test_web_port_out_of_range_high(self):
        os.environ["WIKI_START_PAGE_TITLE"] = "Test"
        os.environ["WIKI_WEB_PORT"] = "65536"
        with pytest.raises(SystemExit, match="WIKI_WEB_PORT must be between"):
            load_walker_config_from_env()

    def test_stats_window_size_zero(self):
        os.environ["WIKI_START_PAGE_TITLE"] = "Test"
        os.environ["WIKI_STATS_WINDOW_SIZE"] = "0"
        with pytest.raises(SystemExit, match="WIKI_STATS_WINDOW_SIZE must be >= 1"):
            load_walker_config_from_env()

    def test_empty_start_title_exits(self):
        os.environ["WIKI_START_PAGE_TITLE"] = ""
        with pytest.raises(SystemExit, match="Missing env var WIKI_START_PAGE_TITLE"):
            load_walker_config_from_env()

    def test_whitespace_start_title_exits(self):
        os.environ["WIKI_START_PAGE_TITLE"] = "   "
        with pytest.raises(SystemExit, match="Missing env var WIKI_START_PAGE_TITLE"):
            load_walker_config_from_env()


class TestLoadBackupConfigFromEnv:
    def test_disabled_by_default(self):
        config = load_backup_config_from_env()
        assert config.enabled is False
        assert config.backup_dir is None

    def test_enabled_with_path(self):
        os.environ["BACKUP_ENABLE"] = "1"
        os.environ["BACKUP_PATH"] = "/tmp/backups"
        config = load_backup_config_from_env()
        assert config.enabled is True
        assert config.backup_dir is not None
        assert config.max_count == 5
        assert config.run_after_crawl_count == 200

    def test_enabled_without_path_exits(self):
        os.environ["BACKUP_ENABLE"] = "true"
        with pytest.raises(SystemExit, match="BACKUP_PATH"):
            load_backup_config_from_env()

    def test_custom_backup_values(self):
        os.environ["BACKUP_ENABLE"] = "yes"
        os.environ["BACKUP_PATH"] = "/tmp/mybups"
        os.environ["BACKUP_MAX_COUNT"] = "10"
        os.environ["BACKUP_RUN_AFTER_CRAWL_COUNT"] = "50"
        config = load_backup_config_from_env()
        assert config.max_count == 10
        assert config.run_after_crawl_count == 50

    def test_backup_max_count_below_one_exits(self):
        os.environ["BACKUP_ENABLE"] = "1"
        os.environ["BACKUP_PATH"] = "/tmp/x"
        os.environ["BACKUP_MAX_COUNT"] = "0"
        with pytest.raises(SystemExit, match="BACKUP_MAX_COUNT must be >= 1"):
            load_backup_config_from_env()

    def test_backup_run_after_below_one_exits(self):
        os.environ["BACKUP_ENABLE"] = "1"
        os.environ["BACKUP_PATH"] = "/tmp/x"
        os.environ["BACKUP_RUN_AFTER_CRAWL_COUNT"] = "0"
        with pytest.raises(SystemExit, match="BACKUP_RUN_AFTER_CRAWL_COUNT must be >= 1"):
            load_backup_config_from_env()

    def test_truthy_values(self):
        for val in ["1", "true", "t", "yes", "y", "on", "TRUE", "True"]:
            os.environ["BACKUP_ENABLE"] = val
            os.environ["BACKUP_PATH"] = "/tmp/x"
            config = load_backup_config_from_env()
            assert config.enabled is True, f"Expected {val!r} to be truthy"
            del os.environ["BACKUP_PATH"]

    def test_falsy_values(self):
        for val in ["0", "false", "f", "no", "n", "off", "", "random"]:
            os.environ["BACKUP_ENABLE"] = val
            config = load_backup_config_from_env()
            assert config.enabled is False, f"Expected {val!r} to be falsy"
