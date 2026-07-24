"""Pure-Python cleaning helpers: messy raw values -> typed, structured values.

These are the heart of bronze -> silver. Each function takes ONE messy value (the
kind you actually get from real APIs — wrong type, wrong case, null-ish strings,
mixed formats) and returns a clean, typed value or ``None``.

Why pure Python (no Spark) here:
  * they are trivially unit-testable and runnable with zero dependencies
  * the Spark transform reuses them verbatim as a UDF, so local demo, tests, and
    the cluster all apply the exact same logic
For fields whose type is *stable*, prefer native Spark functions (faster); a
cleaning UDF like this earns its keep when the raw data is genuinely
heterogeneous (a field that is sometimes a string, sometimes an object).
"""
from __future__ import annotations

import html
import re
from datetime import datetime, timezone

# Strings that really mean "missing", however they show up in the wild.
NULLISH = {"", "n/a", "na", "null", "none", "unknown", "-", "?", "tbd"}


def _nullish(v) -> bool:
    return v is None or (isinstance(v, str) and v.strip().lower() in NULLISH)


def clean_text(v) -> str | None:
    """Trim, unescape HTML entities, collapse whitespace runs. Unicode/emoji kept.

    'The Great &amp; Small  ' -> 'The Great & Small'
    """
    if _nullish(v):
        return None
    s = html.unescape(str(v))
    s = re.sub(r"\s+", " ", s).strip()   # tabs/newlines/repeats -> single space
    return s or None


def clean_id(v) -> str | None:
    """Ids arrive as ' rec-004 ', 3 (int), 'REC-001'. Trim to a stable string."""
    if _nullish(v):
        return None
    return str(v).strip()


def normalize_name(v) -> str | None:
    """One person name -> 'First Last', trimmed and cased. Handles 'Last, First'.

    'ZOLA, Émile' -> 'Émile Zola'   '  john  smith ' -> 'John Smith'
    """
    if _nullish(v):
        return None
    s = re.sub(r"\s+", " ", str(v).strip())
    if "," in s:
        last, first = (p.strip() for p in s.split(",", 1))
        if first:
            s = f"{first} {last}"
    return " ".join(w.capitalize() for w in s.split()) or None


def split_names(v) -> list[str]:
    """Any 'creator' shape -> list of clean names.

    Accepts null, a string ('A; B', 'A & B', 'A and B'), a {'name': ...} object,
    or a list. Multi-valued author fields are extremely common and messy.
    """
    if _nullish(v):
        return []
    if isinstance(v, dict):
        items = [v.get("name")]
    elif isinstance(v, list):
        items = v
    else:
        items = re.split(r"\s*[;&/]\s*|\s+and\s+", str(v))
    return [n for n in (normalize_name(i) for i in items) if n]


def standardize_category(v) -> str | None:
    """Collapse case/spacing/hyphen noise and map synonyms to a canonical label.

    'NON-FICTION ' -> 'nonfiction'   'Fiction' -> 'fiction'
    """
    if _nullish(v):
        return None
    key = re.sub(r"[^a-z]", "", str(v).strip().lower())
    synonyms = {"nonfiction": "nonfiction", "fiction": "fiction",
                "poetry": "poetry", "drama": "drama"}
    return synonyms.get(key, key or None)


def split_labels(v) -> list[str]:
    """Labels come as ['a','b'], 'a, b, c', '' or null -> lowercased unique list."""
    if _nullish(v):
        return []
    items = v if isinstance(v, list) else str(v).split(",")
    out: list[str] = []
    for it in items:
        if _nullish(it):
            continue
        t = str(it).strip().lower()
        if t and t not in out:
            out.append(t)
    return out


def _roman_to_int(s: str) -> int | None:
    vals = {"I": 1, "V": 5, "X": 10, "L": 50, "C": 100, "D": 500, "M": 1000}
    s = s.upper()
    if not s or any(c not in vals for c in s):
        return None
    total = prev = 0
    for c in reversed(s):
        v = vals[c]
        total += -v if v < prev else v
        prev = v
    return total


def parse_year(v) -> int | None:
    """Year from 1999, '1999', 'c. 1200', '2,010', 'MCMXCIX', 'N/A' -> int|None."""
    if _nullish(v) or isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    s = str(v).strip().replace(",", "")
    m = re.search(r"\d{3,4}", s)          # 'c. 1200' -> 1200
    if m:
        return int(m.group())
    return _roman_to_int(s)                # 'MCMXCIX' -> 1999


def parse_double(v) -> float | None:
    """Number from '4.5', '4,5', '$12.99', '12,99 €', 3, 'N/A' -> float|None.

    Handles currency symbols, European decimal commas, and thousands separators.
    """
    if _nullish(v):
        return None
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return float(v)
    s = re.sub(r"[^\d,.\-]", "", str(v))          # strip $, €, letters, spaces
    if "," in s and "." not in s:
        s = s.replace(",", ".")                    # '4,5' -> '4.5'
    else:
        s = s.replace(",", "")                      # '2,010' -> '2010'
    try:
        return float(s)
    except ValueError:
        return None


def parse_bool(v) -> bool | None:
    """true/'yes'/'Y'/1 -> True ; false/'no'/'N'/0 -> False ; else None."""
    if _nullish(v):
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return {0: False, 1: True}.get(int(v))
    s = str(v).strip().lower()
    if s in {"true", "yes", "y", "t", "1"}:
        return True
    if s in {"false", "no", "n", "f", "0"}:
        return False
    return None


def normalize_email(v) -> str | None:
    """Lowercase + trim, return only if it looks like an email, else None."""
    if _nullish(v):
        return None
    s = str(v).strip().lower()
    return s if re.fullmatch(r"[^@\s]+@[^@\s]+\.[a-z]{2,}", s) else None


def normalize_url(v) -> str | None:
    """Trim and ensure a scheme ('example.com/b' -> 'https://example.com/b')."""
    if _nullish(v):
        return None
    s = str(v).strip()
    if not s:
        return None
    return s if re.match(r"^https?://", s) else f"https://{s}"


def parse_geo(v) -> tuple[float | None, float | None]:
    """{'lat':'59.33','lon':'18.06'} -> (59.33, 18.06). 'Stockholm'/null -> (None, None)."""
    if isinstance(v, dict):
        return parse_double(v.get("lat")), parse_double(v.get("lon"))
    return None, None


def normalize_language(v) -> str | None:
    """'english'/'EN'/'en' -> 'en'. Falls back to first two letters."""
    if _nullish(v):
        return None
    s = str(v).strip().lower()
    known = {"english": "en", "french": "fr", "swedish": "sv"}
    if s in known:
        return known[s]
    return s[:2] if len(s) >= 2 else None


def parse_date(v) -> str | None:
    """Many date shapes -> ISO 'YYYY-MM-DD' (or None).

    Handles '2024-01-15', '15/01/2024', '2024-01-15T08:30:00Z', epoch seconds
    (1705305600), 'Jan 2024', 'unknown', null.
    """
    if _nullish(v):
        return None
    if isinstance(v, bool):
        return None
    if isinstance(v, int):                              # epoch seconds
        return datetime.fromtimestamp(v, tz=timezone.utc).date().isoformat()
    s = str(v).strip()
    if re.fullmatch(r"\d{10,}", s):                     # epoch as string
        return datetime.fromtimestamp(int(s), tz=timezone.utc).date().isoformat()
    s = s.replace("Z", "+00:00")
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y-%m-%dT%H:%M:%S%z",
                "%b %Y", "%B %Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc).date().isoformat()
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(s).date().isoformat()
    except ValueError:
        return None


def clean_record(raw: dict) -> dict:
    """Apply every helper to one raw record -> the structured silver shape.

    This is the single function the Spark UDF and the local demo both call, so
    the cleaning logic lives in exactly one place.
    """
    lat, lon = parse_geo(raw.get("location"))
    return {
        "record_id": clean_id(raw.get("id")),
        "title": clean_text(raw.get("title")),
        "creators": split_names(raw.get("creator")),
        "summary": clean_text(raw.get("summary")),
        "category": standardize_category(raw.get("category")),
        "labels": split_labels(raw.get("labels")),
        "year": parse_year(raw.get("year")),
        "rating": parse_double(raw.get("rating")),
        "is_public": parse_bool(raw.get("is_public")),
        "price": parse_double(raw.get("price")),
        "email": normalize_email(raw.get("email")),
        "url": normalize_url(raw.get("url")),
        "lat": lat,
        "lon": lon,
        "language": normalize_language(raw.get("language")),
        "updated_at": parse_date(raw.get("updated")),
    }
