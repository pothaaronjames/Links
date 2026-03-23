#!/usr/bin/env python3
"""Build a CSV of top American films by year using Wikidata box office totals.

Source pages are Wikipedia pages in this form:
  List_of_American_films_of_<YEAR>

The script parses every film listed in each page's monthly release wikitables
(not the "Highest-grossing films" section), resolves each film to a Wikidata
QID when possible, fetches box office (P2142), and keeps only the top N films
for each year.
"""

from __future__ import annotations

import argparse
import csv
import http.client
import json
import re
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set

import certifi

WIKIPEDIA_API = "https://en.wikipedia.org/w/api.php"
WIKIDATA_API = "https://www.wikidata.org/w/api.php"
USER_AGENT = "wikidata-movie-data-cli/0.1 (https://www.wikidata.org/wiki/Wikidata:Data_access)"
RETRYABLE_HTTP_CODES = {429, 500, 502, 503, 504}
WIKILINK_NAMESPACE_PREFIXES = {
    "category",
    "draft",
    "file",
    "help",
    "image",
    "media",
    "module",
    "portal",
    "special",
    "talk",
    "template",
    "timedtext",
    "user",
    "wikipedia",
    "wp",
}


@dataclass
class FilmSourceRow:
    year: int
    source_order: int
    wikipedia_title: str


@dataclass
class FilmOutputRow:
    year: int
    source_page: str
    source_order: int
    wikipedia_title: str
    qid: str
    wikidata_label: str
    release_year: Optional[int]
    box_office: Optional[int]


def build_ssl_context() -> ssl.SSLContext:
    return ssl.create_default_context(cafile=certifi.where())


def request_json_with_retries(
    request: urllib.request.Request, timeout: int, attempts: int = 4, base_delay: float = 1.0
) -> dict:
    for attempt in range(1, attempts + 1):
        try:
            with urllib.request.urlopen(request, timeout=timeout, context=build_ssl_context()) as resp:
                charset = resp.headers.get_content_charset() or "utf-8"
                return json.loads(resp.read().decode(charset))
        except urllib.error.HTTPError as exc:
            if exc.code in RETRYABLE_HTTP_CODES and attempt < attempts:
                time.sleep(base_delay * (2 ** (attempt - 1)))
                continue
            raise
        except urllib.error.URLError:
            if attempt < attempts:
                time.sleep(base_delay * (2 ** (attempt - 1)))
                continue
            raise
        except (http.client.RemoteDisconnected, ConnectionError, TimeoutError, OSError):
            if attempt < attempts:
                time.sleep(base_delay * (2 ** (attempt - 1)))
                continue
            raise


def http_get_json(url: str, params: dict[str, str], timeout: int = 40, attempts: int = 4) -> dict:
    full_url = f"{url}?{urllib.parse.urlencode(params)}"
    # Force non-persistent connections; this reduces TLS EOF issues seen on some networks.
    req = urllib.request.Request(full_url, headers={"User-Agent": USER_AGENT, "Connection": "close"})
    return request_json_with_retries(req, timeout=timeout, attempts=attempts)


def chunked(items: Sequence[str], size: int) -> Iterable[List[str]]:
    for i in range(0, len(items), size):
        yield list(items[i : i + size])


def is_non_article_wikilink(raw: str) -> bool:
    if ":" not in raw:
        return False

    prefix, rest = raw.split(":", 1)
    prefix = prefix.strip()
    if not prefix or not rest:
        return True
    if " " in prefix:
        return False

    prefix_lower = prefix.lower()
    if prefix_lower in WIKILINK_NAMESPACE_PREFIXES:
        return True
    if re.fullmatch(r"[a-z]{2,3}", prefix_lower):
        return True
    return False


def fetch_list_page_wikitext(year: int) -> str:
    page = f"List_of_American_films_of_{year}"
    payload = http_get_json(
        WIKIPEDIA_API,
        {
            "action": "parse",
            "format": "json",
            "page": page,
            "prop": "wikitext",
            "formatversion": "2",
        },
        timeout=40,
        attempts=6,
    )
    return payload.get("parse", {}).get("wikitext", "")


def split_wikitable_blocks(wikitext: str) -> List[str]:
    return re.findall(r"\{\|[^\n]*wikitable[\s\S]*?\|\}", wikitext, flags=re.IGNORECASE)


def extract_month_sections_wikitext(wikitext: str) -> str:
    month_tokens = {
        "january",
        "february",
        "march",
        "april",
        "may",
        "june",
        "july",
        "august",
        "september",
        "october",
        "november",
        "december",
    }

    lines = wikitext.splitlines()
    out_lines: List[str] = []
    in_month_section = False

    for line in lines:
        heading_match = re.match(r"^(=+)\s*([^=]+?)\s*\1\s*$", line.strip())
        if heading_match:
            heading_title = heading_match.group(2).strip().lower()
            in_month_section = any(token in heading_title for token in month_tokens)
            continue

        if in_month_section:
            out_lines.append(line)

    return "\n".join(out_lines)


def extract_row_cells(row_text: str) -> List[str]:
    cells: List[str] = []
    for line in row_text.splitlines():
        if not line.startswith("|"):
            continue
        content = line[1:]
        parts = content.split("||")
        for part in parts:
            piece = part.strip()
            if "|" in piece:
                left, right = piece.split("|", 1)
                if "=" in left:
                    piece = right.strip()
            if piece:
                cells.append(piece)
    return cells


def extract_first_article_link(text: str) -> Optional[str]:
    for m in re.finditer(r"\[\[([^\]|#]+)(?:\|[^\]]+)?\]\]", text):
        raw = m.group(1).strip()
        if not raw or is_non_article_wikilink(raw):
            continue
        return raw
    return None


def looks_like_release_date_cell(text: str) -> bool:
    normalized = text.strip().lower()
    if not normalized:
        return False
    if normalized.startswith("{{dts|"):
        return True

    if any(
        month in normalized
        for month in (
            "january",
            "february",
            "march",
            "april",
            "may",
            "june",
            "july",
            "august",
            "september",
            "october",
            "november",
            "december",
        )
    ):
        return True

    # Handles simple date forms used in list tables (for example, 1/29/2021).
    if re.search(r"\b\d{1,2}[/-]\d{1,2}(?:[/-]\d{2,4})?\b", normalized):
        return True

    return False


def extract_titles_from_wikitable(wikitable_text: str) -> List[str]:
    titles: List[str] = []
    seen: Set[str] = set()
    rows = re.split(r"\n\|-\s*\n", wikitable_text)

    for row in rows:
        if "[[" not in row:
            continue
        cells = extract_row_cells(row)
        if not cells:
            continue

        # Title is usually second column, except rows that continue a rowspan date block.
        candidate_cells = cells[1:] if len(cells) > 1 and looks_like_release_date_cell(cells[0]) else cells
        title: Optional[str] = None

        for cell in candidate_cells:
            title = extract_first_article_link(cell)
            if title:
                break

        if not title:
            continue
        if title in seen:
            continue
        seen.add(title)
        titles.append(title)

    return titles


def extract_titles_from_list_page(wikitext: str) -> List[str]:
    month_only_text = extract_month_sections_wikitext(wikitext)
    titles: List[str] = []
    seen: Set[str] = set()
    for table in split_wikitable_blocks(month_only_text):
        for title in extract_titles_from_wikitable(table):
            if title in seen:
                continue
            seen.add(title)
            titles.append(title)
    return titles


def resolve_titles_to_qids(titles: List[str]) -> Dict[str, str]:
    if not titles:
        return {}

    out: Dict[str, str] = {}
    for batch in chunked(titles, 50):
        payload = http_get_json(
            WIKIPEDIA_API,
            {
                "action": "query",
                "format": "json",
                "prop": "pageprops",
                "redirects": "1",
                "ppprop": "wikibase_item",
                "titles": "|".join(batch),
            },
            timeout=40,
            attempts=3,
        )

        redirects: Dict[str, str] = {}
        for item in payload.get("query", {}).get("normalized", []):
            src = item.get("from")
            dst = item.get("to")
            if isinstance(src, str) and isinstance(dst, str):
                redirects[src] = dst
        for item in payload.get("query", {}).get("redirects", []):
            src = item.get("from")
            dst = item.get("to")
            if isinstance(src, str) and isinstance(dst, str):
                redirects[src] = dst

        pages = payload.get("query", {}).get("pages", {})
        qid_by_page_title: Dict[str, str] = {}
        for page_data in pages.values():
            page_title = page_data.get("title")
            qid = page_data.get("pageprops", {}).get("wikibase_item", "")
            if isinstance(page_title, str) and isinstance(qid, str) and qid:
                qid_by_page_title[page_title] = qid

        for title in batch:
            current = title
            seen_titles: Set[str] = set()
            while current in redirects and current not in seen_titles:
                seen_titles.add(current)
                current = redirects[current]

            qid = qid_by_page_title.get(current) or qid_by_page_title.get(title)
            if qid:
                out[title] = qid

        time.sleep(0.05)

    return out


def get_entities_by_qid(qids: List[str], props: str) -> Dict[str, dict]:
    entities: Dict[str, dict] = {}
    for batch in chunked(qids, 50):
        payload = http_get_json(
            WIKIDATA_API,
            {
                "action": "wbgetentities",
                "format": "json",
                "ids": "|".join(batch),
                "props": props,
                "languages": "en",
            },
            timeout=40,
            attempts=3,
        )
        entities.update(payload.get("entities", {}))
        time.sleep(0.05)
    return entities


def parse_release_year(entity: dict) -> Optional[int]:
    for claim in entity.get("claims", {}).get("P577", []):
        value = claim.get("mainsnak", {}).get("datavalue", {}).get("value", {})
        time_value = value.get("time")
        if not isinstance(time_value, str):
            continue
        match = re.match(r"^[+-](\d{4})-", time_value)
        if not match:
            continue
        try:
            return int(match.group(1))
        except ValueError:
            continue
    return None


def parse_box_office(entity: dict) -> Optional[int]:
    best: Optional[int] = None
    for claim in entity.get("claims", {}).get("P2142", []):
        value = claim.get("mainsnak", {}).get("datavalue", {}).get("value", {})
        amount_text = value.get("amount")
        if not isinstance(amount_text, str):
            continue
        try:
            amount = int(abs(float(amount_text)))
        except ValueError:
            continue
        if best is None or amount > best:
            best = amount
    return best


def write_rows(path: str, rows: List[FilmOutputRow]) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "year",
                "source_page",
                "source_order",
                "wikipedia_title",
                "qid",
                "wikidata_label",
                "release_year",
                "box_office",
            ]
        )
        for row in rows:
            writer.writerow(
                [
                    row.year,
                    row.source_page,
                    row.source_order,
                    row.wikipedia_title,
                    row.qid,
                    row.wikidata_label,
                    row.release_year if row.release_year is not None else "",
                    row.box_office if row.box_office is not None else "",
                ]
            )


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Extract films from List_of_American_films_of_YEAR pages and output "
            "Wikidata box office values for each listed film."
        )
    )
    parser.add_argument("--start-year", type=int, default=1970)
    parser.add_argument("--end-year", type=int, default=2025)
    parser.add_argument(
        "--top-n",
        type=int,
        default=15,
        help="Number of films to keep per year, ranked by box office descending",
    )
    parser.add_argument("--output", default="data/top_movies_by_year.csv")
    parser.add_argument("--verbose", action="store_true", default=True)
    args = parser.parse_args()

    if args.start_year < 1900 or args.end_year > 2100 or args.start_year > args.end_year:
        print("Invalid year range")
        return 1
    if args.top_n <= 0:
        print("--top-n must be a positive integer")
        return 1

    output_rows: List[FilmOutputRow] = []
    title_to_qid_cache: Dict[str, str] = {}
    qid_entity_cache: Dict[str, dict] = {}

    for year in range(args.start_year, args.end_year + 1):
        try:
            wikitext = fetch_list_page_wikitext(year)
        except Exception as exc:
            if args.verbose:
                print(f"Year {year}: failed to fetch page ({exc})")
            continue

        if not wikitext:
            if args.verbose:
                print(f"Year {year}: page exists but has no wikitext")
            continue

        titles = extract_titles_from_list_page(wikitext)
        if args.verbose:
            print(f"Year {year}: extracted {len(titles)} listed films")

        source_rows: List[FilmSourceRow] = [
            FilmSourceRow(year=year, source_order=idx, wikipedia_title=title)
            for idx, title in enumerate(titles, start=1)
        ]

        # Resolve only missing titles for this year, then use cache for assembly.
        missing_titles = [title for title in titles if title not in title_to_qid_cache]
        if missing_titles:
            resolved = resolve_titles_to_qids(missing_titles)
            title_to_qid_cache.update(resolved)

        year_qids = sorted({title_to_qid_cache.get(row.wikipedia_title, "") for row in source_rows if title_to_qid_cache.get(row.wikipedia_title, "")})
        missing_qids = [qid for qid in year_qids if qid not in qid_entity_cache]
        if missing_qids:
            qid_entity_cache.update(get_entities_by_qid(missing_qids, props="claims|labels"))

        year_rows: List[FilmOutputRow] = []
        for row in source_rows:
            qid = title_to_qid_cache.get(row.wikipedia_title, "")
            entity = qid_entity_cache.get(qid, {}) if qid else {}
            label = (
                entity.get("labels", {}).get("en", {}).get("value", row.wikipedia_title)
                if entity
                else row.wikipedia_title
            )

            year_rows.append(
                FilmOutputRow(
                    year=row.year,
                    source_page=f"List_of_American_films_of_{row.year}",
                    source_order=row.source_order,
                    wikipedia_title=row.wikipedia_title,
                    qid=qid,
                    wikidata_label=label,
                    release_year=parse_release_year(entity) if entity else None,
                    box_office=parse_box_office(entity) if entity else None,
                )
            )

        ranked_year_rows = [r for r in year_rows if r.box_office is not None]
        ranked_year_rows.sort(key=lambda r: ((r.box_office or 0), -r.source_order), reverse=True)
        kept = ranked_year_rows[: args.top_n]
        output_rows.extend(kept)

        if args.verbose:
            print(
                f"Year {year}: kept {len(kept)} of {len(ranked_year_rows)} films with box office "
                f"(top_n={args.top_n})"
            )

        time.sleep(0.05)

    write_rows(args.output, output_rows)

    with_qid = sum(1 for r in output_rows if r.qid)
    with_box = sum(1 for r in output_rows if r.box_office is not None)
    print(f"Saved {len(output_rows)} rows to {args.output}")
    print(f"Rows with QID: {with_qid}")
    print(f"Rows with box office: {with_box}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())