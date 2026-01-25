"""MediaWiki Action API helpers.

See https://www.mediawiki.org/wiki/API:Query

This module isolates the network-facing MediaWiki API operations from the crawler
workflow in `walker.py`.

It provides:
- Title normalization + redirect resolution to stable page IDs (`pageid`).
- Outbound link discovery for a single page via `prop=links` with pagination.

Notes:
- Identity is MediaWiki `pageid`.
- Redirects that land on a fragment (`tofragment`) are treated as a link to the
  whole target page (fragment is ignored).
"""

from __future__ import annotations

import time
from dataclasses import dataclass

import requests

from utils import chunked

WIKIPEDIA_API_ENDPOINT = "https://en.wikipedia.org/w/api.php"
MEDIAWIKI_MAX_TITLES_PER_QUERY = 50


@dataclass(frozen=True)
class MediaWikiPageReference:
    """
    Reference to a MediaWiki page by stable ID and title.
    """
    media_wiki_page_id: int
    title: str

    def __hash__(self) -> int:
        return hash(self.media_wiki_page_id)


@dataclass(frozen=True)
class MediaWikiFetchResult:
    """
    Result of fetching outbound links for a single Wikipedia page.
    """
    page: MediaWikiPageReference
    links: set[MediaWikiPageReference]


def mediawiki_resolve_titles_to_pages(
    titles: set[str],
    *,
    sleep_seconds: float,
    user_agent: str,
) -> set[MediaWikiPageReference]:
    """Resolve a set of titles to canonical pages.

    This call follows redirects (including redirects that ultimately land on a
    fragment). When the API reports a redirect with `tofragment`, we intentionally
    ignore the fragment and treat it as a link to the whole target page.

    Returns a set of MediaWikiPageReference for the resolved pages.
    Missing pages are omitted.
    """
    if not titles:
        return set()

    headers = {"User-Agent": user_agent}
    titles_list = sorted(titles)

    resolved: set[MediaWikiPageReference] = set()

    for titles_batch in chunked(titles_list, MEDIAWIKI_MAX_TITLES_PER_QUERY):
        params: dict[str, str] = {
            "action": "query",
            "format": "json",
            "formatversion": "2",
            "redirects": "1",
            "prop": "info",
            "titles": "|".join(titles_batch),
        }

        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

        response = requests.get(WIKIPEDIA_API_ENDPOINT, params=params, headers=headers, timeout=30)
        response.raise_for_status()
        query = response.json().get("query") or {}

        normalized_entries = query.get("normalized") or []
        normalized_map: dict[str, str] = {}
        for normalized_entry in normalized_entries:
            # Map original title to normalized title. E.g. "wikipedia walker" -> "Wikipedia walker"
            from_title = normalized_entry.get("from")
            to_title = normalized_entry.get("to")
            if not from_title or not to_title:
                continue

            normalized_map[from_title] = to_title

        redirects = query.get("redirects") or []
        redirect_map: dict[str, str] = {}
        for redirect_entry in redirects:
            # Map from redirected title to target title. E.g. "Wiki walker" -> "Wikipedia walker"
            from_title = redirect_entry.get("from")
            to_title = redirect_entry.get("to")
            if not from_title or not to_title:
                continue

            # If there's a fragment (tofragment), we intentionally ignore it and
            # treat the link as a link to the whole target page.
            redirect_map[from_title] = to_title

        pages = query.get("pages") or []
        by_title: dict[str, MediaWikiPageReference] = {}
        for page in pages:
            # Map resolved title to MediaWikiPageReference.

            if page.get("missing"):
                # Skip missing pages.
                continue

            page_id = page.get("pageid")
            page_title = page.get("title")
            if not isinstance(page_id, int) or not isinstance(page_title, str):
                continue

            by_title[page_title] = MediaWikiPageReference(media_wiki_page_id=page_id, title=page_title)

        for original_input_title in titles_batch:
            normalized_title = normalized_map.get(original_input_title, original_input_title)
            resolved_title = redirect_map.get(normalized_title, normalized_title)
            page_reference = by_title.get(resolved_title)
            if page_reference is not None:
                resolved.add(page_reference)

    return resolved


def mediawiki_fetch_links(
    title: str,
    *,
    sleep_seconds: float,
    user_agent: str,
) -> MediaWikiFetchResult:
    """Fetch outbound article links for a single Wikipedia page title.

    Uses MediaWiki API (action=query&prop=links) and follows pagination via `continue`.
    Only namespace 0 links (articles) are returned.
    """
    headers = {"User-Agent": user_agent}

    link_titles: set[str] = set()
    continuation_params: dict[str, str] = {}
    canonical_page_id: int | None = None
    canonical_title: str | None = None

    while True:
        params: dict[str, str] = {
            "action": "query",
            "format": "json",
            "formatversion": "2",
            "redirects": "1",
            "prop": "links",
            "plnamespace": "0",
            "pllimit": "max",
            "titles": title,
        }
        # The MediaWiki API paginates large link lists using a continuation token.
        # We keep the opaque continuation values here and apply them to the next
        # request.
        params.update(continuation_params)

        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

        response = requests.get(WIKIPEDIA_API_ENDPOINT, params=params, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()

        pages = (data.get("query") or {}).get("pages") or []
        if pages:
            # Go grab all the links from the first (and only) page in the response. Add them to the set.
            first_page = pages[0]
            canonical_page_id = first_page.get("pageid") or canonical_page_id
            canonical_title = first_page.get("title") or canonical_title
            for link in first_page.get("links") or []:
                link_title = link.get("title")
                if link_title:
                    link_titles.add(link_title)

        continuation_data = data.get("continue")
        if not continuation_data:
            break

        # Prepare for the next iteration with the continuation token.
        continuation_params["plcontinue"] = continuation_data["plcontinue"]

    if canonical_page_id is None:
        raise RuntimeError(f"Could not resolve pageid for '{title}'")

    # Go resolve link titles to stable page references.
    resolved_links = mediawiki_resolve_titles_to_pages(
        link_titles,
        sleep_seconds=sleep_seconds,
        user_agent=user_agent,
    )

    return MediaWikiFetchResult(
        page=MediaWikiPageReference(media_wiki_page_id=int(canonical_page_id), title=canonical_title or title),
        links=resolved_links,
    )
