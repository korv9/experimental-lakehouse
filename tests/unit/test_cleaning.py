"""Unit tests for the pure-Python cleaning helpers (no Spark required).

One assertion per messy case — this doubles as living documentation of exactly
what each helper promises.
"""
from src.transformations import cleaning as c


def test_clean_text_trims_and_unescapes():
    assert c.clean_text("  The Great &amp; Small   ") == "The Great & Small"
    assert c.clean_text("") is None
    assert c.clean_text("N/A") is None


def test_normalize_name_last_first_and_case():
    assert c.normalize_name("ZOLA, Émile") == "Émile Zola"
    assert c.normalize_name("  john  smith ") == "John Smith"
    assert c.normalize_name(None) is None


def test_split_names_handles_every_shape():
    assert c.split_names({"name": "john smith"}) == ["John Smith"]
    assert c.split_names("Alice Smith; Bob Jones") == ["Alice Smith", "Bob Jones"]
    assert c.split_names(["Ada Lovelace", "Charles Babbage"]) == ["Ada Lovelace", "Charles Babbage"]
    assert c.split_names(None) == []


def test_standardize_category():
    assert c.standardize_category("NON-FICTION ") == "nonfiction"
    assert c.standardize_category("Fiction") == "fiction"
    assert c.standardize_category("unknown") is None


def test_split_labels_dedups_and_lowercases():
    assert c.split_labels(["Drama", "drama ", " "]) == ["drama"]
    assert c.split_labels("poetry, epic, poetry") == ["poetry", "epic"]
    assert c.split_labels(None) == []


def test_parse_year_variants():
    assert c.parse_year("c. 1200") == 1200
    assert c.parse_year("2,010") == 2010
    assert c.parse_year("MCMXCIX") == 1999
    assert c.parse_year("N/A") is None


def test_parse_double_currency_and_european():
    assert c.parse_double("$12.99") == 12.99
    assert c.parse_double("9,99 €") == 9.99
    assert c.parse_double("1 234,56 kr") == 1234.56
    assert c.parse_double("N/A") is None


def test_parse_bool_variants():
    assert c.parse_bool("yes") is True
    assert c.parse_bool(0) is False
    assert c.parse_bool("false") is False
    assert c.parse_bool("maybe") is None


def test_normalize_email_validates():
    assert c.normalize_email("Contact@Example.COM ") == "contact@example.com"
    assert c.normalize_email("invalid-email") is None
    assert c.normalize_email("   ") is None


def test_normalize_url_adds_scheme():
    assert c.normalize_url("example.com/b") == "https://example.com/b"
    assert c.normalize_url("https://x.org") == "https://x.org"
    assert c.normalize_url("  ") is None


def test_parse_geo_and_language():
    assert c.parse_geo({"lat": "59.33", "lon": "18.06"}) == (59.33, 18.06)
    assert c.parse_geo("Stockholm") == (None, None)
    assert c.normalize_language("english") == "en"


def test_parse_date_many_formats():
    assert c.parse_date("2024-01-15") == "2024-01-15"
    assert c.parse_date("15/01/2024") == "2024-01-15"
    assert c.parse_date("2024-01-15T08:30:00Z") == "2024-01-15"
    assert c.parse_date(1705305600) == "2024-01-15"
    assert c.parse_date("Jan 2024") == "2024-01-01"
    assert c.parse_date("not a date") is None


def test_clean_record_shape():
    raw = {"id": " r1 ", "title": "x", "creator": "Doe, Jane", "year": "1999"}
    rec = c.clean_record(raw)
    assert rec["record_id"] == "r1"
    assert rec["creators"] == ["Jane Doe"]
    assert rec["year"] == 1999
    # all schema keys present even when the raw omitted them
    assert set(rec) >= {"record_id", "title", "creators", "labels", "lat", "lon", "updated_at"}
