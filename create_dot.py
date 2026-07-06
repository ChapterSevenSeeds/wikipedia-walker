"""Export the Wikipedia link graph in the SQLite database to Graphviz DOT or CSV.

Usage:
    python create_dot.py --db path/to/db.sqlite3 --out graph.dot
    python create_dot.py --db path/to/db.sqlite3 --out graph.dot --all-pages
    python create_dot.py --db path/to/db.sqlite3 --out graph.dot --depth 2 --start-title "Dream Theater"
    python create_dot.py --db path/to/db.sqlite3 --out graph.dot --simplified
    python create_dot.py --db path/to/db.sqlite3 --out graph --format csv
    python create_dot.py --db path/to/db.sqlite3 --out graph --format csv --depth 3 --start-title "Dream Theater"

Notes:
- Nodes are pages; node labels are page titles.
- Edges are page_links (from_page -> to_page).
- By default, only crawled pages are included (where last_links_recorded_at is set).
- If --depth is provided, a subgraph is produced containing only pages within N hops
    from the --start-title (following outgoing links).
- If --simplified is provided, the output includes at most one incoming edge per node.
- --format dot (default) emits a Graphviz DOT file.
- --format csv emits {out}.nodes.csv and {out}.edges.csv, directly importable into
    Cosmograph, Gephi, Tulip, and other large-graph tools.  Rows are streamed so
    memory usage stays low even for very large databases.
"""

from __future__ import annotations

import argparse
import csv
import sys
from collections.abc import Iterable, Iterator
from pathlib import Path

from graphviz import Digraph
from sqlalchemy import select
from sqlalchemy.orm import Session, aliased

from models import Page, PageLink, make_engine


# SQLite default SQLITE_MAX_VARIABLE_NUMBER is 999.
_IN_CHUNK_SIZE = 900


def _chunked_in(session, stmt_fn, ids: Iterable[int]):
    """Execute *stmt_fn(chunk)* for successive chunks and yield all rows."""
    id_list = list(ids)
    for i in range(0, len(id_list), _IN_CHUNK_SIZE):
        chunk = id_list[i : i + _IN_CHUNK_SIZE]
        yield from session.execute(stmt_fn(chunk)).all()


def _node_filter(stmt, *, all_pages: bool):
    if all_pages:
        return stmt
    return stmt.where(Page.last_links_recorded_at.is_not(None))


def _iter_simplified_edges(rows) -> Iterator[tuple[int, int]]:
    """Yield edges with at most one incoming edge per node.

    Deterministic: callers should order rows by (to_id, from_id) so the selected
    incoming edge is stable.
    """
    seen_to: set[int] = set()
    for from_id, to_id in rows:
        fi = int(from_id)
        ti = int(to_id)
        if ti in seen_to:
            continue
        seen_to.add(ti)
        yield (fi, ti)


def _find_start_page(session, start_title: str, *, all_pages: bool) -> int:
    """Return *mw_page_id* for *start_title*, or exit with an error."""
    matches = session.execute(
        select(Page.mw_page_id, Page.last_links_recorded_at).where(Page.title == start_title)
    ).all()

    if not matches:
        print(f"Error: start title not found in DB: '{start_title}'", file=sys.stderr)
        raise SystemExit(1)

    if len(matches) > 1:
        ids = ", ".join(str(mw_page_id) for mw_page_id, _ in matches)
        print(
            f"Error: multiple rows found with title '{start_title}' (mw_page_id: {ids}).",
            file=sys.stderr,
        )
        raise SystemExit(1)

    start_page_id, start_last_links_recorded_at = matches[0]
    if not all_pages and start_last_links_recorded_at is None:
        print(
            f"Error: start title '{start_title}' exists but has not been crawled yet. "
            "Either crawl it first or re-run with --all-pages.",
            file=sys.stderr,
        )
        raise SystemExit(1)

    return int(start_page_id)


def _bfs_edges(
    session,
    start_page_id: int,
    depth: int,
    *,
    all_pages: bool,
) -> tuple[set[int], set[tuple[int, int]]]:
    """BFS outward from *start_page_id* for *depth* hops.

    Returns ``(visited_ids, edges)`` where every id is a plain ``int``.
    """
    from_page = aliased(Page)
    to_page = aliased(Page)

    visited: set[int] = {start_page_id}
    frontier: set[int] = {start_page_id}
    edges: set[tuple[int, int]] = set()

    for _ in range(depth):
        if not frontier:
            break

        def _edges_for(chunk, *, _all=all_pages):
            s = (
                select(PageLink.from_page_id, PageLink.to_page_id)
                .join(from_page, from_page.mw_page_id == PageLink.from_page_id)
                .join(to_page, to_page.mw_page_id == PageLink.to_page_id)
                .where(PageLink.from_page_id.in_(chunk))
            )
            if not _all:
                s = s.where(
                    from_page.last_links_recorded_at.is_not(None),
                    to_page.last_links_recorded_at.is_not(None),
                )
            return s

        rows = list(_chunked_in(session, _edges_for, frontier))

        next_frontier: set[int] = set()
        for from_id, to_id in rows:
            fi = int(from_id)
            ti = int(to_id)
            edges.add((fi, ti))
            if ti not in visited:
                visited.add(ti)
                next_frontier.add(ti)

        frontier = next_frontier

    return visited, edges


def build_graph(db_path: str, *, all_pages: bool, simplified: bool) -> Digraph:
    engine = make_engine(db_path)

    graph = Digraph("wikipedia")
    graph.attr("graph", rankdir="LR")
    graph.attr("node", shape="box")

    from_page = aliased(Page)
    to_page = aliased(Page)

    with Session(engine) as session:
        # Add all nodes.
        nodes_stmt = _node_filter(select(Page.mw_page_id, Page.title), all_pages=all_pages)

        for page_id, title in session.execute(nodes_stmt):
            graph.node(str(page_id), label=title)

        # Add all edges.
        edges_stmt = select(PageLink.from_page_id, PageLink.to_page_id)
        if not all_pages:
            edges_stmt = (
                edges_stmt.join(from_page, from_page.mw_page_id == PageLink.from_page_id)
                .join(to_page, to_page.mw_page_id == PageLink.to_page_id)
                .where(
                    from_page.last_links_recorded_at.is_not(None),
                    to_page.last_links_recorded_at.is_not(None),
                )
            )

        edges_stmt = edges_stmt.order_by(PageLink.to_page_id.asc(), PageLink.from_page_id.asc())

        rows = session.execute(edges_stmt)
        if simplified:
            for from_id, to_id in _iter_simplified_edges(rows):
                graph.edge(str(from_id), str(to_id))
        else:
            for from_id, to_id in rows:
                graph.edge(str(from_id), str(to_id))

    return graph


def build_graph_from_start(
    db_path: str,
    *,
    start_title: str,
    depth: int,
    all_pages: bool,
    simplified: bool,
) -> Digraph:
    engine = make_engine(db_path)

    graph = Digraph("wikipedia")
    graph.attr("graph", rankdir="LR")
    graph.attr("node", shape="box")

    with Session(engine) as session:
        start_id = _find_start_page(session, start_title, all_pages=all_pages)
        visited, edges = _bfs_edges(session, start_id, depth, all_pages=all_pages)

        def _nodes_for(chunk):
            s = select(Page.mw_page_id, Page.title).where(Page.mw_page_id.in_(chunk))
            return _node_filter(s, all_pages=all_pages)

        for page_id, title in _chunked_in(session, _nodes_for, visited):
            graph.node(str(page_id), label=title)

    if simplified:
        for from_id, to_id in _iter_simplified_edges(sorted(edges, key=lambda e: (e[1], e[0]))):
            graph.edge(str(from_id), str(to_id))
    else:
        for from_id, to_id in sorted(edges):
            graph.edge(str(from_id), str(to_id))

    return graph


def write_csv(db_path: str, out_base: Path, *, all_pages: bool, simplified: bool) -> None:
    """Stream the full graph to CSV node/edge files."""
    engine = make_engine(db_path)
    nodes_path = out_base.with_suffix(".nodes.csv")
    edges_path = out_base.with_suffix(".edges.csv")

    from_page = aliased(Page)
    to_page = aliased(Page)

    with Session(engine) as session:
        with open(nodes_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["id", "label"])
            stmt = _node_filter(select(Page.mw_page_id, Page.title), all_pages=all_pages)
            for page_id, title in session.execute(stmt).yield_per(10_000):
                w.writerow([page_id, title])

        with open(edges_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["source", "target"])
            edges_stmt = select(PageLink.from_page_id, PageLink.to_page_id)
            if not all_pages:
                edges_stmt = (
                    edges_stmt.join(from_page, from_page.mw_page_id == PageLink.from_page_id)
                    .join(to_page, to_page.mw_page_id == PageLink.to_page_id)
                    .where(
                        from_page.last_links_recorded_at.is_not(None),
                        to_page.last_links_recorded_at.is_not(None),
                    )
                )
            edges_stmt = edges_stmt.order_by(
                PageLink.to_page_id.asc(), PageLink.from_page_id.asc()
            )
            rows = session.execute(edges_stmt).yield_per(10_000)
            if simplified:
                rows = _iter_simplified_edges(rows)
            for from_id, to_id in rows:
                w.writerow([from_id, to_id])

    print(f"Wrote {nodes_path} and {edges_path}", file=sys.stderr)


def write_csv_from_start(
    db_path: str,
    out_base: Path,
    *,
    start_title: str,
    depth: int,
    all_pages: bool,
    simplified: bool,
) -> None:
    """BFS subgraph to CSV node/edge files."""
    engine = make_engine(db_path)
    nodes_path = out_base.with_suffix(".nodes.csv")
    edges_path = out_base.with_suffix(".edges.csv")

    with Session(engine) as session:
        start_id = _find_start_page(session, start_title, all_pages=all_pages)
        visited, edges = _bfs_edges(session, start_id, depth, all_pages=all_pages)

        with open(nodes_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["id", "label"])

            def _nodes_for(chunk):
                s = select(Page.mw_page_id, Page.title).where(Page.mw_page_id.in_(chunk))
                return _node_filter(s, all_pages=all_pages)

            for page_id, title in _chunked_in(session, _nodes_for, visited):
                w.writerow([page_id, title])

    with open(edges_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["source", "target"])
        if simplified:
            edge_iter = _iter_simplified_edges(sorted(edges, key=lambda e: (e[1], e[0])))
        else:
            edge_iter = sorted(edges)
        for from_id, to_id in edge_iter:
            w.writerow([from_id, to_id])

    print(f"Wrote {nodes_path} and {edges_path}", file=sys.stderr)


def main() -> None:
    parser = argparse.ArgumentParser(description="Export the SQLite Wikipedia graph to Graphviz DOT")
    parser.add_argument("--db", required=True, help="Path to SQLite database file")
    parser.add_argument("--out", required=True, help="Output file path (for csv: used as base name)")
    parser.add_argument(
        "--format",
        choices=["dot", "csv"],
        default="dot",
        help="Output format: dot (Graphviz DOT) or csv (node/edge CSVs for Cosmograph, Gephi, etc.)",
    )
    parser.add_argument(
        "--all-pages",
        action="store_true",
        help="Include all pages (default: include only crawled pages)",
    )
    parser.add_argument(
        "--simplified",
        action="store_true",
        help="Only keep at most one incoming edge per node",
    )
    parser.add_argument(
        "--depth",
        type=int,
        default=None,
        help="Optional max hop depth from --start-title (directed, following outbound links)",
    )
    parser.add_argument(
        "--start-title",
        default=None,
        help="Start page title to use when --depth is specified",
    )
    args = parser.parse_args()

    if args.depth is None and args.start_title is not None:
        parser.error("--start-title is only valid when --depth is specified")
    if args.depth is not None and args.start_title is None:
        parser.error("--depth requires --start-title")
    if args.depth is not None and args.depth < 0:
        parser.error("--depth must be >= 0")

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    fmt = args.format

    if fmt == "csv":
        if args.depth is None:
            write_csv(
                args.db, out_path, all_pages=bool(args.all_pages), simplified=bool(args.simplified)
            )
        else:
            write_csv_from_start(
                args.db,
                out_path,
                start_title=str(args.start_title),
                depth=int(args.depth),
                all_pages=bool(args.all_pages),
                simplified=bool(args.simplified),
            )
    else:
        if args.depth is None:
            graph = build_graph(
                args.db, all_pages=bool(args.all_pages), simplified=bool(args.simplified)
            )
        else:
            graph = build_graph_from_start(
                args.db,
                start_title=str(args.start_title),
                depth=int(args.depth),
                all_pages=bool(args.all_pages),
                simplified=bool(args.simplified),
            )
        out_path.write_text(graph.source, encoding="utf-8")


if __name__ == "__main__":
    main()
