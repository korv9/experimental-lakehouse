import pytest

from lakehouse_platform.transforms.text import clean_document, clean_ocr_text, html_to_text


def test_html_extraction_removes_scripts_and_preserves_blocks():
    source = """
    <html><style>.hidden {display:none}</style><body>
      <h1>The Republic</h1><p>Justice &amp; society.</p>
      <script>steal_the_text()</script>
    </body></html>
    """

    result = html_to_text(source)

    assert "The Republic" in result
    assert "Justice & society." in result
    assert "steal_the_text" not in result
    assert ".hidden" not in result


def test_ocr_cleanup_is_conservative_and_deterministic():
    source = "PHILOSO-\r\nPHY\r\n\r\n  Page 12 \r\njustice   and  society\u00ad"

    assert clean_ocr_text(source) == "PHILOSOPHY\n\njustice and society"


def test_clean_document_handles_html_then_ocr_artifacts():
    assert clean_document("<p>demo-\ncracy</p>", input_format="html") == "democracy"


def test_clean_document_rejects_unknown_format():
    with pytest.raises(ValueError, match="input_format"):
        clean_document("text", input_format="pdf")
