"""Export the Wikipedia link graph in the SQLite database to Graphviz DOT.

Usage:
  python create_dot.py --db path/to/db.sqlite3 --out graph.dot

Notes:
- Nodes are pages; node labels are page titles.
- Edges are page_links (from_page -> to_page).
- This emits a DOT file only; it does not require the Graphviz system binaries.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from graphviz import Digraph
from sqlalchemy import select
from sqlalchemy.orm import Session

from models import Page, PageLink, make_engine


def build_graph(db_path: str) -> Digraph:
    engine = make_engine(db_path)

    graph = Digraph("wikipedia")
    graph.attr("graph", rankdir="LR")
    graph.attr("node", shape="box")

    with Session(engine) as session:
        # Add all nodes.
        for page_id, title in session.execute(select(Page.id, Page.title)):
            graph.node(str(page_id), label=title)

        # Add all edges.
        for from_id, to_id in session.execute(select(PageLink.from_page_id, PageLink.to_page_id)):
            graph.edge(str(from_id), str(to_id))

    return graph


def main() -> None:
    parser = argparse.ArgumentParser(description="Export the SQLite Wikipedia graph to Graphviz DOT")
    parser.add_argument("--db", required=True, help="Path to SQLite database file")
    parser.add_argument("--out", required=True, help="Output DOT file path")
    args = parser.parse_args()

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    graph = build_graph(args.db)
    out_path.write_text(graph.source, encoding="utf-8")


if __name__ == "__main__":
    main()
