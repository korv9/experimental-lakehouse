"""Page-number pagination strategy.

Encapsulates *how we walk pages* so the client stays generic. This one builds
the query params for a given page number; the runner drives the loop and stops
when the API reports no more pages (`total_pages` in the response).

Other strategies (cursor, offset, link-header) would live beside this file and
expose the same idea.
"""
from __future__ import annotations


def page_params(page: int, cfg: dict) -> dict:
    """Return the query params for one page, from the source's pagination config."""
    return {
        cfg["page_parameter"]: page,
        cfg["page_size_parameter"]: cfg["page_size"],
    }
