"""Export the Wikipedia link graph in the SQLite database to Graphviz DOT.

Usage:
    python create_dot.py --db path/to/db.sqlite3 --out graph.dot
    python create_dot.py --db path/to/db.sqlite3 --out graph.dot --all-pages
    python create_dot.py --db path/to/db.sqlite3 --out graph.dot --depth 2 --start-title "Dream Theater"

Notes:
- Nodes are pages; node labels are page titles.
- Edges are page_links (from_page -> to_page).
- By default, only crawled pages are included (where last_links_recorded_at is set).
- If --depth is provided, a subgraph is produced containing only pages within N hops
    from the --start-title (following outgoing links).
- This emits a DOT file only; it does not require the Graphviz system binaries.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from graphviz import Digraph
from sqlalchemy import select
from sqlalchemy.orm import Session, aliased

from models import Page, PageLink, make_engine


def _node_filter(stmt, *, all_pages: bool):
    if all_pages:
        return stmt
    return stmt.where(Page.last_links_recorded_at.is_not(None))


def build_graph(db_path: str, *, all_pages: bool) -> Digraph:
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

        for from_id, to_id in session.execute(edges_stmt):
            graph.edge(str(from_id), str(to_id))

    return graph


def build_graph_from_start(
    db_path: str,
    *,
    start_title: str,
    depth: int,
    all_pages: bool,
) -> Digraph:
    engine = make_engine(db_path)

    graph = Digraph("wikipedia")
    graph.attr("graph", rankdir="LR")
    graph.attr("node", shape="box")

    from_page = aliased(Page)
    to_page = aliased(Page)

    with Session(engine) as session:
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

        visited: set[int] = {int(start_page_id)}
        frontier: set[int] = {int(start_page_id)}
        edges: set[tuple[int, int]] = set()

        for _ in range(depth):
            if not frontier:
                break

            edges_stmt = (
                select(PageLink.from_page_id, PageLink.to_page_id)
                .join(from_page, from_page.mw_page_id == PageLink.from_page_id)
                .join(to_page, to_page.mw_page_id == PageLink.to_page_id)
                .where(PageLink.from_page_id.in_(frontier))
            )

            if not all_pages:
                edges_stmt = edges_stmt.where(
                    from_page.last_links_recorded_at.is_not(None),
                    to_page.last_links_recorded_at.is_not(None),
                )

            rows = session.execute(edges_stmt).all()

            next_frontier: set[int] = set()
            for from_id, to_id in rows:
                fi = int(from_id)
                ti = int(to_id)
                edges.add((fi, ti))
                if ti not in visited:
                    visited.add(ti)
                    next_frontier.add(ti)

            frontier = next_frontier

        # Add nodes (titles) for the visited set.
        nodes_stmt = select(Page.mw_page_id, Page.title).where(Page.mw_page_id.in_(visited))
        nodes_stmt = _node_filter(nodes_stmt, all_pages=all_pages)

        for page_id, title in session.execute(nodes_stmt):
            graph.node(str(page_id), label=title)

    # Add edges (after nodes; Graphviz will still accept edges first, but this is tidy).
    for from_id, to_id in sorted(edges):
        graph.edge(str(from_id), str(to_id))

    return graph


def main() -> None:
    parser = argparse.ArgumentParser(description="Export the SQLite Wikipedia graph to Graphviz DOT")
    parser.add_argument("--db", required=True, help="Path to SQLite database file")
    parser.add_argument("--out", required=True, help="Output DOT file path")
    parser.add_argument(
        "--all-pages",
        action="store_true",
        help="Include all pages (default: include only crawled pages)",
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

    if args.depth is None:
        graph = build_graph(args.db, all_pages=bool(args.all_pages))
    else:
        graph = build_graph_from_start(
            args.db,
            start_title=str(args.start_title),
            depth=int(args.depth),
            all_pages=bool(args.all_pages),
        )
    out_path.write_text(graph.source, encoding="utf-8")


if __name__ == "__main__":
    main()
