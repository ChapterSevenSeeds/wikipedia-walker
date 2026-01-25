# Wikipedia Walker

A resumable Wikipedia link graph crawler.

- Persists crawl state in SQLite (no in-memory frontier)
- Uses the MediaWiki Action API (no HTML scraping)
- Restart-safe: you can stop/re-run and it continues where it left off
- Uses MediaWiki `pageid` as stable identity (titles are snapshots)
- Optional periodic SQLite online backups
- Exports the stored graph to Graphviz DOT

## Quickstart (local)

### 1) Install

```bash
python -m venv .venv
# activate your venv, then:
pip install -r requirements.txt
```

### 2) Run the crawler

You must set `WIKI_START_PAGE_TITLE`.

PowerShell:

```powershell
$env:WIKI_START_PAGE_TITLE = "Dream Theater"
python walker.py
```

Bash:

```bash
export WIKI_START_PAGE_TITLE="Dream Theater"
python walker.py
```

This creates/updates a SQLite DB (default: `wikipedia_walker.sqlite3`) and stores pages + edges as it crawls.

## Configuration (environment variables)

Parsed/validated in `config.py`.

### Walker

| Env var | Required | Default | Meaning |
|---|---:|---:|---|
| `WIKI_START_PAGE_TITLE` | yes | - | Seed page title (only used to seed/expand the DB-backed queue) |
| `WIKI_DB_PATH` | no | `wikipedia_walker.sqlite3` | SQLite database file path |
| `WIKI_MAX_PAGES` | no | `200` | Max pages to process per run (`0` = unlimited) |
| `WIKI_SLEEP_SECONDS` | no | `0.5` | Politeness delay before each API request |
| `WIKI_USER_AGENT` | no | see `config.py` | HTTP `User-Agent` header (set this to something meaningful) |

### Backups

Backups are optional and use SQLite’s online backup API.

| Env var | Required | Default | Meaning |
|---|---:|---:|---|
| `BACKUP_ENABLE` | no | unset/false | Enable backups (`1`, `true`, `yes`, `on`, ...) |
| `BACKUP_PATH` | if enabled | - | Directory to write backups into |
| `BACKUP_MAX_COUNT` | no | `5` | Retention count (keeps newest N backups) |
| `BACKUP_RUN_AFTER_CRAWL_COUNT` | no | `200` | Run a backup every N processed pages |

Example (PowerShell):

```powershell
$env:WIKI_START_PAGE_TITLE = "Dream Theater"
$env:BACKUP_ENABLE = "1"
$env:BACKUP_PATH = "C:\\temp\\wiki-backups"
$env:BACKUP_MAX_COUNT = "10"
$env:BACKUP_RUN_AFTER_CRAWL_COUNT = "100"
python walker.py
```

## How it works

- `walker.py` orchestrates a run.
- `mediawiki_api.py` talks to the MediaWiki Action API:
  - resolves titles → canonical pages (`pageid`) with redirects handled
  - fetches outbound links with pagination via `prop=links`
- `crawl_db.py` implements all persistence and queue logic:
  - the `pages` table is the queue source-of-truth (`crawl_status`)
  - a page is treated as “crawl-once” when `last_links_recorded_at` is set
  - on startup, interrupted `in_progress` pages are re-queued
- `models.py` contains SQLAlchemy ORM models (`Page`, `PageLink`).

This is designed for single-process use. If you want multi-process/parallel crawling, you’ll need a stronger claim/lock strategy around queue claiming.

## Export Graphviz DOT

The crawler stores edges in the DB. You can export them later:

```bash
python create_dot.py --db wikipedia_walker.sqlite3 --out graph.dot
```

Useful options:

- Include all pages (even not-yet-crawled):

```bash
python create_dot.py --db wikipedia_walker.sqlite3 --out graph.dot --all-pages
```

- Limit to a subgraph within N hops from a start title:

```bash
python create_dot.py --db wikipedia_walker.sqlite3 --out graph.dot --depth 2 --start-title "Dream Theater"
```

- Simplify (keep at most one incoming edge per node):

```bash
python create_dot.py --db wikipedia_walker.sqlite3 --out graph.dot --simplified
```

### Rendering DOT to SVG/PNG

`create_dot.py` only writes DOT. To render (optional), install Graphviz system binaries and run:

```bash
dot -Tsvg graph.dot -o graph.svg
```

## Docker

### Build

```bash
docker build -t wikipedia-walker .
```

### Run (with a persisted DB)

Because the container runs as a non-root user, mount a writable directory and put the DB inside it:

```bash
docker run --rm \
  -e WIKI_START_PAGE_TITLE="Dream Theater" \
  -e WIKI_DB_PATH=/data/wikipedia_walker.sqlite3 \
  -v "$(pwd)/data:/data" \
  wikipedia-walker
```

Optional backups (mounted to `/backups`):

```bash
docker run --rm \
  -e WIKI_START_PAGE_TITLE="Dream Theater" \
  -e WIKI_DB_PATH=/data/wikipedia_walker.sqlite3 \
  -e BACKUP_ENABLE=1 \
  -e BACKUP_PATH=/backups \
  -v "$(pwd)/data:/data" \
  -v "$(pwd)/backups:/backups" \
  wikipedia-walker
```

## GitHub Actions

### PR checks

Workflow runs on pull requests:

- Validates PR title follows Conventional Commits (important for squash merges)
- Runs `python -m py_compile *.py`

### Release

Manual `workflow_dispatch` release:

- Computes a new semver tag from Conventional Commits (`smlx/ccv`)
- Creates a GitHub Release (`ncipollo/release-action`)
- Builds and pushes a Docker image to Docker Hub

Repo secrets required for Docker push:

- `DOCKERHUB_USERNAME`
- `DOCKERHUB_TOKEN`

## Notes / gotchas

- The DB is global. `WIKI_START_PAGE_TITLE` is only a seed and doesn’t “scope” the crawl.
- Titles are not stable identifiers; the crawler uses `pageid` as identity.
- If you want recrawling, the current policy is “crawl once”. That can be extended later by introducing a recrawl schedule based on timestamps.
