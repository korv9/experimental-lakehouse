"""Reusable normalization for HTML and noisy OCR-derived text."""
from __future__ import annotations

import html
import re
import unicodedata
from html.parser import HTMLParser

BLOCK_TAGS = {
    "article",
    "blockquote",
    "br",
    "div",
    "footer",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "header",
    "li",
    "p",
    "section",
    "table",
    "tr",
}
HIDDEN_TAGS = {"script", "style", "noscript", "svg"}


class _TextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self.hidden_depth = 0

    def handle_starttag(self, tag: str, attrs) -> None:
        if tag in HIDDEN_TAGS:
            self.hidden_depth += 1
        elif tag in BLOCK_TAGS and not self.hidden_depth:
            self.parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        if tag in HIDDEN_TAGS and self.hidden_depth:
            self.hidden_depth -= 1
        elif tag in BLOCK_TAGS and not self.hidden_depth:
            self.parts.append("\n")

    def handle_data(self, data: str) -> None:
        if not self.hidden_depth:
            self.parts.append(data)


def html_to_text(value: str) -> str:
    """Extract visible text while preserving useful block boundaries."""
    parser = _TextExtractor()
    parser.feed(value)
    parser.close()
    return html.unescape("".join(parser.parts))


def clean_ocr_text(
    value: str,
    *,
    remove_page_numbers: bool = True,
    dehyphenate_line_breaks: bool = True,
) -> str:
    """Normalize common OCR artifacts without guessing spelling corrections."""
    text = unicodedata.normalize("NFKC", value)
    text = text.replace("\u00ad", "")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    if dehyphenate_line_breaks:
        text = re.sub(r"(?<=\w)-[ \t]*\n[ \t]*(?=\w)", "", text)
    if remove_page_numbers:
        text = re.sub(
            r"(?m)^[ \t]*(?:page[ \t]+)?\d{1,4}[ \t]*$",
            "",
            text,
            flags=re.IGNORECASE,
        )
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r" *\n *", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def clean_document(value: str, *, input_format: str = "text") -> str:
    """Apply deterministic HTML extraction followed by conservative OCR cleanup."""
    if input_format not in {"text", "html"}:
        raise ValueError("input_format must be 'text' or 'html'")
    extracted = html_to_text(value) if input_format == "html" else value
    return clean_ocr_text(extracted)
