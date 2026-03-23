#!/usr/bin/env python3
"""Fetch cast and directors for a movie from Wikidata.

Usage:
    python movie_search.py Q25188
"""

from __future__ import annotations

import argparse
import json
import re
import ssl
import sys
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import List

import certifi

WIKIDATA_API = "https://www.wikidata.org/w/api.php"
SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
USER_AGENT = "wikidata-movie-data-cli/0.1 (https://www.wikidata.org/wiki/Wikidata:Data_access)"


@dataclass
class MoviePersonRow:
    qid: str
    name: str
    role: str


def build_ssl_context() -> ssl.SSLContext:
    context = ssl.create_default_context(cafile=certifi.where())
    return context


def http_get_json(url: str, params: dict[str, str]) -> dict:
    query = urllib.parse.urlencode(params)
    full_url = f"{url}?{query}"
    req = urllib.request.Request(full_url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=20, context=build_ssl_context()) as response:
        charset = response.headers.get_content_charset() or "utf-8"
        return json.loads(response.read().decode(charset))


def normalize_qid(value: str) -> str:
    qid = value.strip().upper()
    if not re.fullmatch(r"Q[1-9]\d*", qid):
        raise ValueError("QID must look like Q25188")
    return qid


def get_movie_metadata(movie_qid: str) -> tuple[str, str]:
    payload = http_get_json(
        WIKIDATA_API,
        {
            "action": "wbgetentities",
            "format": "json",
            "ids": movie_qid,
            "props": "labels|descriptions",
            "languages": "en",
        },
    )
    entity = payload.get("entities", {}).get(movie_qid, {})
    label = entity.get("labels", {}).get("en", {}).get("value", movie_qid)
    description = entity.get("descriptions", {}).get("en", {}).get("value", "")
    return label, description


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
    with urllib.request.urlopen(req, timeout=25, context=build_ssl_context()) as response:
        charset = response.headers.get_content_charset() or "utf-8"
        return json.loads(response.read().decode(charset))


def get_movie_people_rows(movie_qid: str) -> List[MoviePersonRow]:
    query = f"""
        SELECT ?person ?personLabel ?role WHERE {{
      BIND(wd:{movie_qid} AS ?movie)

      {{
        ?movie wdt:P161 ?person .
        BIND("cast" AS ?role)
      }}
      UNION
      {{
        ?movie wdt:P57 ?person .
        BIND("director" AS ?role)
      }}

      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    """

    payload = sparql_query(query)
    bindings = payload.get("results", {}).get("bindings", [])

    rows: List[MoviePersonRow] = []
    seen: set[tuple[str, str]] = set()

    for row in bindings:
        person_uri = row.get("person", {}).get("value")
        person_label = row.get("personLabel", {}).get("value")
        role = row.get("role", {}).get("value")
        if not person_uri or not person_label or not role:
            continue

        person_qid = person_uri.rsplit("/", 1)[-1]
        key = (person_qid, role)
        if key in seen:
            continue
        seen.add(key)
        rows.append(MoviePersonRow(qid=person_qid, name=person_label, role=role))

    return rows


def get_movie_people(movie_qid: str) -> tuple[List[str], List[str]]:
    rows = get_movie_people_rows(movie_qid)

    cast: List[str] = []
    directors: List[str] = []

    for row in rows:
        if row.role == "cast":
            cast.append(row.name)
        elif row.role == "director":
            directors.append(row.name)

    # Remove duplicates while preserving order.
    cast = list(dict.fromkeys(cast))
    directors = list(dict.fromkeys(directors))

    return cast, directors


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Given a movie QID, print cast and directors from Wikidata."
    )
    parser.add_argument("qid", help="Movie QID, e.g. Q25188")
    args = parser.parse_args()

    try:
        movie_qid = normalize_qid(args.qid)
    except ValueError as exc:
        print(exc)
        return 1

    try:
        label, description = get_movie_metadata(movie_qid)
        cast, directors = get_movie_people(movie_qid)
    except Exception as exc:  # noqa: BLE001 - CLI-friendly error output.
        print(f"Wikidata request failed: {exc}")
        return 1

    print(f"Movie: {label} ({movie_qid})")
    if description:
        print(f"Description: {description}")
    print()

    print("Directors:")
    if directors:
        for name in directors:
            print(f"- {name}")
    else:
        print("- None found")

    print()
    print("Cast:")
    if cast:
        for name in cast:
            print(f"- {name}")
    else:
        print("- None found")

    return 0


if __name__ == "__main__":
    sys.exit(main())