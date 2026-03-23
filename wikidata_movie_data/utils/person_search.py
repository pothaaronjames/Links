#!/usr/bin/env python3
"""Fetch films linked to a person (cast/director) from Wikidata.

Usage:
    python person_search.py Q172678
    python person_search.py Q25191 --role director --limit 100
"""

from __future__ import annotations

import argparse
import json
import re
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import date
from typing import List

import certifi

WIKIDATA_API = "https://www.wikidata.org/w/api.php"
SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
PAGEVIEWS_API = "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article"
USER_AGENT = "wikidata-movie-data-cli/0.1 (https://www.wikidata.org/wiki/Wikidata:Data_access)"
RETRYABLE_HTTP_CODES = {429, 500, 502, 503, 504}


@dataclass
class FilmRow:
    title: str
    qid: str
    role: str
    release_date: str
    box_office: int | None


@dataclass
class AggregateRow:
    role: str
    film_count: int
    films_with_box_office: int
    total_box_office: int
    pageviews_12mo: int


def build_ssl_context() -> ssl.SSLContext:
    return ssl.create_default_context(cafile=certifi.where())


def request_json_with_retries(
    request: urllib.request.Request, timeout: int, attempts: int = 5, base_delay: float = 1.0
) -> dict:
    for attempt in range(1, attempts + 1):
        try:
            with urllib.request.urlopen(request, timeout=timeout, context=build_ssl_context()) as response:
                charset = response.headers.get_content_charset() or "utf-8"
                return json.loads(response.read().decode(charset))
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


def http_get_json(url: str, params: dict[str, str]) -> dict:
    query = urllib.parse.urlencode(params)
    full_url = f"{url}?{query}"
    req = urllib.request.Request(full_url, headers={"User-Agent": USER_AGENT})
    return request_json_with_retries(req, timeout=25)


def http_get_json_absolute(url: str) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    return request_json_with_retries(req, timeout=25)


def sparql_query(query: str) -> dict:
    params = {
        "query": query,
        "format": "json",
    }
    req = urllib.request.Request(
        f"{SPARQL_ENDPOINT}?{urllib.parse.urlencode(params)}",
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "application/sparql-results+json",
        },
    )
    return request_json_with_retries(req, timeout=45)


def normalize_qid(value: str) -> str:
    qid = value.strip().upper()
    if not re.fullmatch(r"Q[1-9]\d*", qid):
        raise ValueError("QID must look like Q172678")
    return qid


def get_person_metadata(person_qid: str) -> tuple[str, str, str]:
    payload = http_get_json(
        WIKIDATA_API,
        {
            "action": "wbgetentities",
            "format": "json",
            "ids": person_qid,
            "props": "labels|descriptions|sitelinks",
            "languages": "en",
            "sitefilter": "enwiki",
        },
    )
    entity = payload.get("entities", {}).get(person_qid, {})
    label = entity.get("labels", {}).get("en", {}).get("value", person_qid)
    description = entity.get("descriptions", {}).get("en", {}).get("value", "")
    wiki_title = entity.get("sitelinks", {}).get("enwiki", {}).get("title", "")
    return label, description, wiki_title


def build_person_films_query(person_qid: str, role: str, limit: int) -> str:
        if role == "cast":
                role_block = """
    ?film wdt:P161 ?person .
    BIND("cast" AS ?role)
"""
        elif role == "director":
                role_block = """
    ?film wdt:P57 ?person .
    BIND("director" AS ?role)
"""
        else:
                role_block = """
    {
        ?film wdt:P161 ?person .
        BIND("cast" AS ?role)
    }
    UNION
    {
        ?film wdt:P57 ?person .
        BIND("director" AS ?role)
    }
"""

        return f"""
SELECT ?film ?filmLabel ?role ?releaseDate ?boxOffice WHERE {{
    BIND(wd:{person_qid} AS ?person)
    ?film wdt:P31 wd:Q11424 .
    FILTER(?film != wd:Q40786)
    {role_block}
    OPTIONAL {{ ?film wdt:P577 ?releaseDate . }}
    OPTIONAL {{ ?film wdt:P2142 ?boxOffice . }}
    SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
}}
ORDER BY DESC(?releaseDate) ?filmLabel
LIMIT {limit}
"""


def get_person_films(person_qid: str, role: str, limit: int) -> List[FilmRow]:
    payload = sparql_query(build_person_films_query(person_qid, role, limit))
    rows = payload.get("results", {}).get("bindings", [])
    films: List[FilmRow] = []
    seen: set[tuple[str, str]] = set()

    for row in rows:
        film_uri = row.get("film", {}).get("value", "")
        title = row.get("filmLabel", {}).get("value", "")
        row_role = row.get("role", {}).get("value", "")
        release_date_raw = row.get("releaseDate", {}).get("value", "")
        box_office_raw = row.get("boxOffice", {}).get("value")
        if not film_uri or not title or not row_role:
            continue

        film_qid = film_uri.rsplit("/", 1)[-1]
        key = (film_qid, row_role)
        if key in seen:
            continue
        seen.add(key)

        films.append(
            FilmRow(
                title=title,
                qid=film_qid,
                role=row_role,
                release_date=release_date_raw[:10] if release_date_raw else "",
                box_office=int(float(box_office_raw)) if box_office_raw else None,
            )
        )

    return films


def pageview_date_range() -> tuple[str, str]:
    today = date.today()
    first_of_this_month = today.replace(day=1)

    if first_of_this_month.month == 1:
        end_year = first_of_this_month.year - 1
        end_month = 12
    else:
        end_year = first_of_this_month.year
        end_month = first_of_this_month.month - 1

    start_year = end_year - 1 if end_month < 12 else end_year
    start_month = (end_month % 12) + 1

    start = f"{start_year}{start_month:02d}01"
    end = f"{end_year}{end_month:02d}01"
    return start, end


def fetch_person_pageviews_12mo(wiki_title: str) -> int:
    if not wiki_title:
        return 0
    start, end = pageview_date_range()
    encoded_title = urllib.parse.quote(wiki_title.replace(" ", "_"), safe="")
    url = f"{PAGEVIEWS_API}/en.wikipedia/all-access/all-agents/{encoded_title}/monthly/{start}/{end}"
    try:
        payload = http_get_json_absolute(url)
        return sum(item.get("views", 0) for item in payload.get("items", []))
    except Exception:
        return 0


def compute_aggregates(
    films: List[FilmRow], role_filter: str, pageviews_12mo: int
) -> List[AggregateRow]:
    roles = ["cast", "director"] if role_filter == "both" else [role_filter]
    rows: List[AggregateRow] = []

    for role in roles:
        role_films = [film for film in films if film.role == role]
        film_count = len(role_films)
        films_with_bo = [film for film in role_films if film.box_office is not None]
        rows.append(
            AggregateRow(
                role=role,
                film_count=film_count,
                films_with_box_office=len(films_with_bo),
                total_box_office=sum(film.box_office or 0 for film in role_films),
                pageviews_12mo=pageviews_12mo,
            )
        )

    if role_filter == "both":
        films_with_bo_all = [film for film in films if film.box_office is not None]
        rows.append(
            AggregateRow(
                role="overall",
                film_count=len(films),
                films_with_box_office=len(films_with_bo_all),
                total_box_office=sum(film.box_office or 0 for film in films),
                pageviews_12mo=pageviews_12mo,
            )
        )

    return rows


def format_money(value: int | None) -> str:
    if value is None:
        return "N/A"
    return f"{value:,}"


def print_aggregates(rows: List[AggregateRow]) -> None:
    print("Aggregate Metrics:")
    if not rows:
        print("- None")
        return
    for row in rows:
        print(
            f"- role={row.role} | films={row.film_count} | films_with_box_office={row.films_with_box_office} "
            f"| total_box_office={row.total_box_office:,} | pageviews_12mo={row.pageviews_12mo:,}"
        )
    print()


def print_grouped_films(rows: List[FilmRow], role: str) -> None:
    if role in {"cast", "director"}:
        title = "Films"
        print(f"{title}:")
        if not rows:
            print("- None found")
            return
        for row in rows:
            suffix = f" ({row.release_date})" if row.release_date else ""
            print(f"- {row.title} [{row.qid}] - {row.role}{suffix} | box_office={format_money(row.box_office)}")
        return

    cast_rows = [row for row in rows if row.role == "cast"]
    director_rows = [row for row in rows if row.role == "director"]

    print("Cast Films:")
    if cast_rows:
        for row in cast_rows:
            suffix = f" ({row.release_date})" if row.release_date else ""
            print(f"- {row.title} [{row.qid}]{suffix} | box_office={format_money(row.box_office)}")
    else:
        print("- None found")

    print()
    print("Directed Films:")
    if director_rows:
        for row in director_rows:
            suffix = f" ({row.release_date})" if row.release_date else ""
            print(f"- {row.title} [{row.qid}]{suffix} | box_office={format_money(row.box_office)}")
    else:
        print("- None found")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Given a person QID, print films they acted in and/or directed."
    )
    parser.add_argument("qid", help="Person QID, e.g. Q172678")
    parser.add_argument(
        "--role",
        choices=["cast", "director", "both"],
        default="both",
        help="Which relationship to fetch (default: both).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum number of rows to return (default: 100).",
    )
    args = parser.parse_args()

    if args.limit < 1 or args.limit > 5000:
        print("--limit must be between 1 and 5000")
        return 1

    try:
        person_qid = normalize_qid(args.qid)
    except ValueError as exc:
        print(exc)
        return 1

    try:
        person_name, person_description, wiki_title = get_person_metadata(person_qid)
        rows = get_person_films(person_qid, role=args.role, limit=args.limit)
        pageviews_12mo = fetch_person_pageviews_12mo(wiki_title)
        aggregates = compute_aggregates(rows, args.role, pageviews_12mo)
    except Exception as exc:  # noqa: BLE001 - CLI-friendly error output.
        print(f"Wikidata request failed: {exc}")
        return 1

    print(f"Person: {person_name} ({person_qid})")
    if person_description:
        print(f"Description: {person_description}")
    print(f"English Wikipedia pageviews (12mo): {pageviews_12mo:,}")
    print(f"Role filter: {args.role}")
    print(f"Rows returned: {len(rows)}")
    print()
    print_aggregates(aggregates)
    print_grouped_films(rows, role=args.role)

    return 0


if __name__ == "__main__":
    sys.exit(main())