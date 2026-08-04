from __future__ import annotations

import html
import re
from datetime import date
from html.parser import HTMLParser
from typing import Any

MONTHS = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}
MONTH_DATE_RE = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|October|"
    r"November|December)\s+(\d{1,2}),?\s+(\d{4})\b",
    re.IGNORECASE,
)
ISO_DATE_RE = re.compile(r"\b(\d{4})-(\d{2})-(\d{2})\b")
SLASH_DATE_RE = re.compile(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b")
TICKER_PATTERNS = (
    re.compile(r"trading symbol[s]?[\s“”\"'(]*([A-Z][A-Z0-9.\-]{0,11})[”\")]?", re.IGNORECASE),
    re.compile(r"under the symbol[\s“”\"'(]*([A-Z][A-Z0-9.\-]{0,11})[”\")]?", re.IGNORECASE),
    re.compile(r"symbol[\s“”\"']+\(?([A-Z][A-Z0-9.\-]{0,11})\)?", re.IGNORECASE),
)
EFFECTIVE_DATE_RE = re.compile(
    r"(?:effective|delisting|removal|withdrawal)[^.;]{0,120}?" + MONTH_DATE_RE.pattern,
    re.IGNORECASE,
)
DISPLAY_TICKER_RE = re.compile(r"\(([A-Za-z][A-Za-z0-9.\-]{0,11})\)\s*\(CIK")
TICKER_STOPWORDS = {"THE", "A", "AN", "ITS", "OF", "ON", "FROM", "UNDER", "FORM"}


def normalize_ticker(raw: Any) -> str:
    return re.sub(r"\s+", "", str(raw or "")).strip().upper()


def parse_month_date(text: str) -> date | None:
    match = MONTH_DATE_RE.search(text or "")
    if not match:
        return None
    return _safe_date(int(match.group(3)), MONTHS[match.group(1).lower()], int(match.group(2)))


def parse_slash_date(text: str) -> date | None:
    match = SLASH_DATE_RE.search(text or "")
    if not match:
        return None
    return _safe_date(int(match.group(3)), int(match.group(1)), int(match.group(2)))


def parse_loose_date(text: str) -> date | None:
    parsed = parse_month_date(text)
    if parsed:
        return parsed
    iso = ISO_DATE_RE.search(text or "")
    if iso:
        return _safe_date(int(iso.group(1)), int(iso.group(2)), int(iso.group(3)))
    return parse_slash_date(text)


def parse_yyyymmdd(text: Any) -> date | None:
    raw = str(text or "").strip()
    if not re.fullmatch(r"\d{8}", raw):
        return None
    return _safe_date(int(raw[:4]), int(raw[4:6]), int(raw[6:8]))


def parse_nasdaq_delisted(text: str, source: str) -> tuple[list[dict[str, Any]], int]:
    rows: list[dict[str, Any]] = []
    failures = 0
    for line in (text or "").splitlines():
        parts = [part.strip() for part in line.split("|")]
        if _is_nasdaq_skip_line(parts):
            continue
        ticker = normalize_ticker(parts[0])
        event = parse_slash_date(parts[2] if len(parts) > 2 else "")
        if not ticker or event is None:
            failures += 1
            continue
        rows.append(
            {
                "ticker": ticker,
                "name": parts[1],
                "issuer": parts[1],
                "event_date": event.isoformat(),
                "inception_date": None,
                "source": source,
            }
        )
    return rows, failures


def parse_form25(text: str) -> dict[str, Any]:
    lines = _strip_html_lines(text or "")
    plain = " ".join(lines)
    return {
        "issuer": _extract_form25_issuer(lines),
        "security_name": _extract_security_name(lines),
        "doc_ticker": _extract_form25_ticker(plain),
        "effective_date": _extract_effective_date(plain),
    }


def _extract_security_name(lines: list[str]) -> str | None:
    for index, line in enumerate(lines):
        if "17 CFR 240.12d2-2" in line:
            for cursor in range(index - 1, max(index - 4, -1), -1):
                if _is_security_name_candidate(lines[cursor]):
                    return lines[cursor]
            return None
        if "Description of class of securities" in line:
            prefix = re.split(r"\(\s*Description of class", line, flags=re.IGNORECASE)[0].strip()
            if _is_security_name_candidate(prefix):
                return prefix
            for cursor in range(index - 1, max(index - 4, -1), -1):
                if _is_security_name_candidate(lines[cursor]):
                    return lines[cursor]
            return None
    return None


def _is_security_name_candidate(line: str) -> bool:
    lowered = line.lower()
    if not (3 <= len(line) <= 120) or "cfr" in lowered or "☐" in line or "☒" in line:
        return False
    if re.fullmatch(r"[\d\-]+", line) or re.fullmatch(r"[\d\s\-()]+", line):
        return False
    return not any(word in lowered for word in ("exchange", "pursuant", "commission", "form 25"))


def ticker_from_display_names(names: list[str]) -> str | None:
    for name in names or []:
        match = DISPLAY_TICKER_RE.search(str(name))
        if match:
            return normalize_ticker(match.group(1))
    return None


def _is_nasdaq_skip_line(parts: list[str]) -> bool:
    head = parts[0].lower() if parts else ""
    return not head or head == "symbol" or head.startswith("file creation")


def _strip_html_lines(text: str) -> list[str]:
    plain = re.sub(r"<[^>]+>", "\n", text)
    plain = html.unescape(plain).replace("\xa0", " ")
    return [line for line in (re.sub(r"\s+", " ", ln).strip() for ln in plain.splitlines()) if line]


EXCHANGE_NAME_RE = re.compile(
    r"\b(THE NASDAQ STOCK MARKET LLC|NASDAQ GLOBAL SELECT MARKET|NASDAQ GLOBAL MARKET"
    r"|NASDAQ CAPITAL MARKET|NEW YORK STOCK EXCHANGE( LLC)?|NYSE AMERICAN( LLC)?"
    r"|NYSE|NASDAQ|CBOE BZX( EXCHANGE)?(,? INC\.)?|CBOE|BZX)\b",
    re.IGNORECASE,
)


def _extract_form25_issuer(lines: list[str]) -> str | None:
    for index, line in enumerate(lines):
        if "Exact name of Issuer" not in line:
            continue
        prefix = re.split(r"\(\s*Exact name of Issuer", line, maxsplit=1, flags=re.IGNORECASE)[0]
        candidates = ([prefix] if prefix.strip() else []) + _lines_before_marker(lines, index)
        for candidate in candidates:
            cleaned = _clean_issuer_candidate(candidate)
            if cleaned:
                return cleaned
    return None


def _clean_issuer_candidate(text: str) -> str | None:
    cleaned = EXCHANGE_NAME_RE.sub(" ", text)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" ,:;-")
    if len(cleaned) < 3 or "commission file" in cleaned.lower():
        return None
    return cleaned


def _lines_before_marker(lines: list[str], index: int) -> list[str]:
    collected: list[str] = []
    cursor = index - 1
    while cursor >= 0 and len(collected) < 3:
        line = lines[cursor]
        if "Commission File Number" in line:
            break
        if not re.fullmatch(r"[\d\-]+", line):
            collected.append(line)
        cursor -= 1
    return collected


def _extract_form25_ticker(plain: str) -> str | None:
    for pattern in TICKER_PATTERNS:
        match = pattern.search(plain)
        if match:
            ticker = normalize_ticker(match.group(1))
            if ticker and ticker not in TICKER_STOPWORDS:
                return ticker
    return None


def _extract_effective_date(plain: str) -> str | None:
    match = EFFECTIVE_DATE_RE.search(plain)
    if not match:
        return None
    parsed = _safe_date(int(match.group(3)), MONTHS[match.group(1).lower()], int(match.group(2)))
    return parsed.isoformat() if parsed else None


def _safe_date(year: int, month: int, day: int) -> date | None:
    try:
        return date(year, month, day)
    except ValueError:
        return None


class WikiTableParser(HTMLParser):
    """Extracts <table> rows as lists of cell texts using only stdlib."""

    def __init__(self) -> None:
        super().__init__()
        self.tables: list[list[list[str]]] = []
        self._table: list[list[str]] | None = None
        self._row: list[str] | None = None
        self._cell: list[str] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "table":
            self._table = []
        elif tag == "tr" and self._table is not None:
            self._row = []
        elif tag in {"td", "th"} and self._row is not None:
            self._cell = []

    def handle_endtag(self, tag: str) -> None:
        if tag in {"td", "th"} and self._cell is not None and self._row is not None:
            self._row.append(re.sub(r"\s+", " ", "".join(self._cell)).strip())
            self._cell = None
        elif tag == "tr" and self._row is not None and self._table is not None:
            if self._row:
                self._table.append(self._row)
            self._row = None
        elif tag == "table" and self._table is not None:
            if self._table:
                self.tables.append(self._table)
            self._table = None

    def handle_data(self, data: str) -> None:
        if self._cell is not None:
            self._cell.append(data)


def parse_wiki_defunct_etfs(html_text: str) -> tuple[list[dict[str, Any]], int]:
    parser = WikiTableParser()
    parser.feed(html_text or "")
    rows: list[dict[str, Any]] = []
    failures = 0
    for table in parser.tables:
        parsed, failed = _wiki_table_rows(table)
        rows.extend(parsed)
        failures += failed
    return rows, failures


def _wiki_table_rows(table: list[list[str]]) -> tuple[list[dict[str, Any]], int]:
    header = [cell.lower() for cell in table[0]]
    columns = _wiki_columns(header)
    if columns is None:
        return [], 0
    rows: list[dict[str, Any]] = []
    failures = 0
    for cells in table[1:]:
        row = _wiki_row(cells, columns)
        if row is None:
            failures += 1
        else:
            rows.append(row)
    return rows, failures


def _wiki_columns(header: list[str]) -> dict[str, int] | None:
    columns: dict[str, int] = {}
    for index, cell in enumerate(header):
        if "ticker" in cell or "symbol" in cell:
            columns.setdefault("ticker", index)
        elif "fund" in cell or "name" in cell:
            columns.setdefault("name", index)
        elif "issuer" in cell or "sponsor" in cell or "manager" in cell:
            columns.setdefault("issuer", index)
        elif "inception" in cell or "launch" in cell:
            columns.setdefault("inception", index)
        elif "closure" in cell or "closed" in cell or "liquidat" in cell:
            columns.setdefault("closure", index)
    return columns if "ticker" in columns else None


def _wiki_row(cells: list[str], columns: dict[str, int]) -> dict[str, Any] | None:
    def cell(key: str) -> str:
        index = columns.get(key)
        return cells[index] if index is not None and index < len(cells) else ""

    ticker = normalize_ticker(cell("ticker"))
    if not ticker or not re.fullmatch(r"[A-Z][A-Z0-9.\-]{0,11}", ticker):
        return None
    closure = parse_loose_date(cell("closure"))
    inception = parse_loose_date(cell("inception"))
    return {
        "ticker": ticker,
        "name": cell("name"),
        "issuer": cell("issuer") or cell("name"),
        "event_date": closure.isoformat() if closure else None,
        "inception_date": inception.isoformat() if inception else None,
        "source": "wiki_defunct_etf",
    }
