"""Cursor pagination with loop protection and per-page checkpoints."""
from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from typing import Any, Protocol


class JsonPageClient(Protocol):
    def get(self, endpoint: str, params: dict[str, Any] | None = None) -> dict: ...


@dataclass(frozen=True)
class CursorPage:
    number: int
    cursor_used: str | None
    next_cursor: str | None
    records: tuple[dict[str, Any], ...]
    response: dict[str, Any]


def nested_value(document: Mapping[str, Any], path: str, default: Any = None) -> Any:
    value: Any = document
    for segment in path.split("."):
        if not isinstance(value, Mapping) or segment not in value:
            return default
        value = value[segment]
    return value


def cursor_params(cursor: str | None, cfg: Mapping[str, Any]) -> dict[str, Any]:
    params = dict(cfg.get("base_params", {}))
    if cursor:
        params[str(cfg.get("cursor_parameter", "cursor"))] = cursor
    page_size = cfg.get("page_size")
    if page_size is not None:
        params[str(cfg.get("page_size_parameter", "limit"))] = page_size
    return params


def paginate(
    client: JsonPageClient,
    endpoint: str,
    cfg: Mapping[str, Any],
    *,
    initial_cursor: str | None = None,
    checkpoint: Callable[[str | None, int], None] | None = None,
) -> Iterator[CursorPage]:
    """Yield pages and checkpoint the next cursor after each successful page."""
    cursor = initial_cursor
    seen: set[str] = set()
    max_pages = int(cfg.get("max_pages", 10_000))
    records_path = str(cfg.get("records_path", "results"))
    next_cursor_path = str(cfg.get("next_cursor_path", "next_cursor"))

    for page_number in range(1, max_pages + 1):
        body = client.get(endpoint, params=cursor_params(cursor, cfg))
        raw_records = nested_value(body, records_path, [])
        if not isinstance(raw_records, list):
            raise TypeError(f"Cursor records path {records_path!r} did not resolve to a list")
        raw_next = nested_value(body, next_cursor_path)
        next_cursor = str(raw_next) if raw_next not in (None, "") else None
        page = CursorPage(
            number=page_number,
            cursor_used=cursor,
            next_cursor=next_cursor,
            records=tuple(raw_records),
            response=body,
        )
        yield page

        if checkpoint:
            checkpoint(next_cursor, page_number)
        if next_cursor is None:
            return
        if next_cursor in seen or next_cursor == cursor:
            raise RuntimeError(f"API returned a repeated cursor: {next_cursor!r}")
        seen.add(next_cursor)
        cursor = next_cursor

    raise RuntimeError(f"Cursor pagination exceeded max_pages={max_pages}")
