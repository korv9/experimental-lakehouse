"""Parse a Libris XL /find item (JSON-LD, KBV/BIBFRAME) into the silver shape.

Two things make Libris different from the earlier examples, and both are handled
here:

  * Data is split across two levels. The ``Instance`` is the edition (title,
    publication, ISBN); ``instanceOf`` is the ``Work`` (creators, subjects,
    language). We read from both.
  * Person names are already split into ``givenName``/``familyName`` — so here we
    JOIN them, the opposite of the messy demo where we split "Last, First".

URIs such as ``https://id.kb.se/language/swe`` are reduced to their last path
segment (``swe``). ``parse_year``/``parse_date``/``clean_text`` are reused from
the shared cleaning module so behaviour stays consistent across sources.
"""
from __future__ import annotations

from src.transformations.cleaning import clean_text, parse_date, parse_year


def _as_list(v) -> list:
    if v is None:
        return []
    return v if isinstance(v, list) else [v]


def _last_segment(uri) -> str | None:
    if not uri:
        return None
    return str(uri).rstrip("/").split("/")[-1] or None


def join_name(agent) -> str | None:
    """givenName + familyName -> 'First Last'; fall back to name/label."""
    if not isinstance(agent, dict):
        return None
    given = (agent.get("givenName") or "").strip()
    family = (agent.get("familyName") or "").strip()
    if given or family:
        return " ".join(p for p in (given, family) if p)
    return clean_text(agent.get("name") or agent.get("label"))


def extract_id(item: dict) -> str | None:
    meta = item.get("meta")
    if isinstance(meta, dict) and meta.get("@id"):
        return _last_segment(meta["@id"])
    if item.get("@id"):
        return _last_segment(str(item["@id"]).split("#")[0])
    return None


def extract_title(item: dict) -> str | None:
    for t in _as_list(item.get("hasTitle")):
        if isinstance(t, dict) and t.get("mainTitle"):
            return clean_text(t["mainTitle"])
    return clean_text(item.get("title"))


def extract_creators(work: dict) -> list[str]:
    out: list[str] = []
    for c in _as_list(work.get("contribution")):
        name = join_name(c.get("agent")) if isinstance(c, dict) else None
        if name and name not in out:
            out.append(name)
    return out


def extract_subjects(work: dict) -> list[str]:
    out: list[str] = []
    for s in _as_list(work.get("subject")):
        if isinstance(s, dict):
            label = clean_text(s.get("prefLabel") or s.get("label"))
            if label and label not in out:
                out.append(label)
    return out


def extract_year(item: dict) -> int | None:
    for p in _as_list(item.get("publication")):
        if isinstance(p, dict) and p.get("year"):
            return parse_year(p["year"])
    return None


def extract_publisher(item: dict) -> str | None:
    for p in _as_list(item.get("publication")):
        if isinstance(p, dict) and isinstance(p.get("agent"), dict):
            label = clean_text(p["agent"].get("label") or p["agent"].get("name"))
            if label:
                return label
    return None


def extract_isbn(item: dict) -> str | None:
    for idf in _as_list(item.get("identifiedBy")):
        if isinstance(idf, dict) and idf.get("@type") == "ISBN" and idf.get("value"):
            return str(idf["value"]).strip()
    return None


def extract_language(work: dict) -> str | None:
    for lang in _as_list(work.get("language")):
        if isinstance(lang, dict):
            seg = _last_segment(lang.get("@id"))
            if seg:
                return seg
    return None


def extract_modified(item: dict) -> str | None:
    meta = item.get("meta")
    return parse_date(meta.get("modified")) if isinstance(meta, dict) else None


def parse_libris_item(item: dict) -> dict:
    """One Libris /find item -> the flat, typed silver.libris_works record."""
    work = item.get("instanceOf") if isinstance(item.get("instanceOf"), dict) else {}
    return {
        "record_id": extract_id(item),
        "title": extract_title(item),
        "creators": extract_creators(work),
        "subjects": extract_subjects(work),
        "year": extract_year(item),
        "language": extract_language(work),
        "isbn": extract_isbn(item),
        "publisher": extract_publisher(item),
        "updated_at": extract_modified(item),
    }
