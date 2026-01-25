"""Find the shortest path between two Wikipedia pages in the crawl database.

This uses a simple Dijkstra algorithm (unit edge weights) over the directed
graph stored in SQLite:
- nodes: pages (pages.mw_page_id, pages.title)
- edges: page_links (from_page_id -> to_page_id)

Examples:
  python dijkstra.py --db wikipedia_walker.sqlite3 --from "Dream Theater" --to "James LaBrie"
"""

from __future__ import annotations

import argparse
import heapq
import sys
from collections.abc import Iterable

from sqlalchemy import select
from sqlalchemy.orm import Session

from models import Page, PageLink, make_engine


def _find_unique_page_id_by_title(session: Session, title: str) -> tuple[int, str]:
	rows = session.execute(select(Page.mw_page_id, Page.title).where(Page.title == title)).all()
	if not rows:
		raise SystemExit(f"Title not found in DB: {title!r}")
	if len(rows) > 1:
		ids = ", ".join(str(int(pid)) for pid, _ in rows)
		raise SystemExit(
			f"Multiple pages found with title {title!r} (mw_page_id: {ids}). "
			"This DB contains duplicate titles; disambiguate by editing the DB or extending the tool to accept IDs."
		)
	page_id, canonical_title = rows[0]
	return int(page_id), str(canonical_title)


def _iter_neighbors(session: Session, page_id: int) -> Iterable[int]:
	# Directed edges: from_page_id -> to_page_id
	for (to_id,) in session.execute(
		select(PageLink.to_page_id).where(PageLink.from_page_id == page_id)
	):
		yield int(to_id)


def dijkstra_shortest_path(
	engine,
	*,
	start_id: int,
	goal_id: int,
) -> list[int] | None:
	"""Return the shortest path of page IDs from start to goal (inclusive)."""

	if start_id == goal_id:
		return [start_id]

	dist: dict[int, int] = {start_id: 0}
	prev: dict[int, int] = {}
	visited: set[int] = set()
	heap: list[tuple[int, int]] = [(0, start_id)]

	# Cache neighbors to avoid repeated DB roundtrips if we revisit a node.
	neighbors_cache: dict[int, list[int]] = {}

	with Session(engine) as session:
		while heap:
			current_dist, current = heapq.heappop(heap)
			if current in visited:
				continue
			visited.add(current)

			if current == goal_id:
				break

			if current_dist != dist.get(current, current_dist):
				continue

			neighbors = neighbors_cache.get(current)
			if neighbors is None:
				neighbors = list(_iter_neighbors(session, current))
				neighbors_cache[current] = neighbors

			for neighbor in neighbors:
				if neighbor in visited:
					continue

				alt = current_dist + 1
				if alt < dist.get(neighbor, sys.maxsize):
					dist[neighbor] = alt
					prev[neighbor] = current
					heapq.heappush(heap, (alt, neighbor))

	if goal_id not in dist:
		return None

	# Reconstruct path.
	path: list[int] = [goal_id]
	cur = goal_id
	while cur != start_id:
		cur = prev[cur]
		path.append(cur)
	path.reverse()
	return path


def _fetch_titles_for_ids(session: Session, ids: list[int]) -> dict[int, str]:
	rows = session.execute(select(Page.mw_page_id, Page.title).where(Page.mw_page_id.in_(ids))).all()
	return {int(pid): str(title) for pid, title in rows}


def main() -> None:
	parser = argparse.ArgumentParser(description="Shortest path between two pages in the crawl DB")
	parser.add_argument("--db", required=True, help="Path to SQLite DB file")
	parser.add_argument(
		"--from",
		dest="from_title",
		required=True,
		help="Start page title (must match pages.title exactly)",
	)
	parser.add_argument(
		"--to",
		dest="to_title",
		required=True,
		help="Destination page title (must match pages.title exactly)",
	)
	args = parser.parse_args()

	engine = make_engine(str(args.db))

	with Session(engine) as session:
		start_id, start_title = _find_unique_page_id_by_title(session, str(args.from_title))
		goal_id, goal_title = _find_unique_page_id_by_title(session, str(args.to_title))

	path_ids = dijkstra_shortest_path(engine, start_id=start_id, goal_id=goal_id)

	if path_ids is None:
		print(f"No path found from {start_title!r} -> {goal_title!r}.")
		return

	with Session(engine) as session:
		titles_by_id = _fetch_titles_for_ids(session, path_ids)

	path_titles = [titles_by_id.get(pid, f"<missing title for {pid}>") for pid in path_ids]
	print(f"Shortest path length (edges): {len(path_ids) - 1}")
	print("Path:")
	for i, (pid, title) in enumerate(zip(path_ids, path_titles, strict=False), start=1):
		print(f"  {i:>2}. {title} (mw_page_id={pid})")

	print("\nAs titles:")
	print(" -> ".join(path_titles))


if __name__ == "__main__":
	main()

