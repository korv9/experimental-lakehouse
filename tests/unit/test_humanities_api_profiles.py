from pathlib import Path

import yaml

from lakehouse_platform.tools.api_explorer import load_request

CONFIG = Path("config/api/humanities.yaml")
PUBLIC_PROFILES = {
    "gutendex_plato",
    "gutenberg_republic_full_text",
    "wikisource_republic_rendered",
    "internet_archive_plato",
    "open_library_plato",
    "wikidata_plato",
    "libris_nietzsche",
    "library_of_congress_philosophy",
    "riksdagen_anforanden",
    "riksdagen_speech_full_text",
    "pubmed_digital_humanities",
    "openalex_digital_humanities",
    "arxiv_digital_humanities",
}


def test_public_humanities_profiles_are_loadable_https_requests():
    document = yaml.safe_load(CONFIG.read_text(encoding="utf-8"))

    assert PUBLIC_PROFILES <= document["endpoints"].keys()
    for profile in PUBLIC_PROFILES:
        request = load_request(CONFIG, profile)
        assert request.method == "GET"
        assert request.url.startswith("https://")
        assert request.timeout > 0


def test_europeana_profile_keeps_api_key_out_of_source(monkeypatch):
    monkeypatch.setenv("EUROPEANA_API_KEY", "test-key")

    request = load_request(CONFIG, "europeana_philosophy")

    assert request.params["wskey"] == "test-key"
