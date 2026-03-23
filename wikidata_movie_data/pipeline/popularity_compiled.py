#!/usr/bin/env python3
"""Build a popularity dataset from a precomputed top-movies CSV.

Pipeline:
1) Read per-year top movies from top_movies_by_year.csv.
2) Pull cast members for those movies from Wikidata claims.
3) For unique cast members, compute sitelinks count and 12-month enwiki pageviews.
4) Add age-adjusted film productivity (films per active year) and rank by blended score.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

import certifi

WIKIDATA_API = "https://www.wikidata.org/w/api.php"
WIKIPEDIA_API = "https://en.wikipedia.org/w/api.php"
SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
PAGEVIEWS_API = "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article"
USER_AGENT = "wikidata-movie-data-cli/0.1 (https://www.wikidata.org/wiki/Wikidata:Data_access)"
RETRYABLE_HTTP_CODES = {429, 500, 502, 503, 504}
ACTOR_OCCUPATIONS = {"Q33999", "Q2405480"}
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
class ActorRow:
    rank: int
    name: str
    qid: str
    sitelinks: int
    pageviews_12mo: int
    source_movie_count: int
    total_film_count: int
    age_years: Optional[int] = None
    films_per_active_year: float = 0.0
    popularity_score: float = 0.0
    source_movies: List[str] = field(default_factory=list)
    enwiki_title: str = ""


@dataclass
class TopMovieRow:
    year: int
    rank_in_year: int
    title: str
    qid: str
    release_year: int


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


def http_get_json(url: str, params: dict[str, str], timeout: int = 40, attempts: int = 4) -> dict:
    full_url = f"{url}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(full_url, headers={"User-Agent": USER_AGENT})
    return request_json_with_retries(req, timeout=timeout, attempts=attempts)


def sparql_query(query: str, timeout: int = 60, attempts: int = 4) -> dict:
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
    return request_json_with_retries(req, timeout=timeout, attempts=attempts)


def chunked(items: Sequence[str], size: int) -> Iterable[List[str]]:
    for i in range(0, len(items), size):
        yield list(items[i : i + size])


def pageview_date_range() -> Tuple[str, str]:
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


def fetch_pageviews_12mo(wiki_title: str, start: str, end: str) -> int:
    if not wiki_title:
        return 0
    encoded_title = urllib.parse.quote(wiki_title.replace(" ", "_"), safe="")
    url = f"{PAGEVIEWS_API}/en.wikipedia/all-access/all-agents/{encoded_title}/monthly/{start}/{end}"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        payload = request_json_with_retries(req, timeout=30, attempts=3)
        return sum(item.get("views", 0) for item in payload.get("items", []))
    except Exception:
        return 0


def extract_highest_grossing_section(wikitext: str) -> str:
    lines = wikitext.splitlines()
    start_idx = -1
    heading_level = 0

    for i, line in enumerate(lines):
        m = re.match(r"^(=+)\s*([^=]+?)\s*\1\s*$", line.strip())
        if not m:
            continue
        title = m.group(2).strip().lower()
        if "highest-grossing films" in title:
            start_idx = i + 1
            heading_level = len(m.group(1))
            break

    if start_idx < 0:
        return ""

    section_lines: List[str] = []
    for line in lines[start_idx:]:
        m = re.match(r"^(=+)\s*([^=]+?)\s*\1\s*$", line.strip())
        if m and len(m.group(1)) <= heading_level:
            break
        section_lines.append(line)

    return "\n".join(section_lines)


def extract_wikilinks(section_text: str) -> List[str]:
    links: List[str] = []
    seen: Set[str] = set()
    for m in re.finditer(r"\[\[([^\]|#]+)(?:\|[^\]]+)?\]\]", section_text):
        raw = m.group(1).strip()
        if not raw or is_non_article_wikilink(raw):
            continue
        if raw in seen:
            continue
        seen.add(raw)
        links.append(raw)
    return links


def is_non_article_wikilink(raw: str) -> bool:
    """Return True for links that point to namespaces/interwiki, not article titles."""
    if ":" not in raw:
        return False

    prefix, rest = raw.split(":", 1)
    prefix = prefix.strip()
    if not prefix or not rest:
        return True

    # Namespaces/interwiki prefixes never contain spaces.
    if " " in prefix:
        return False

    prefix_lower = prefix.lower()
    if prefix_lower in WIKILINK_NAMESPACE_PREFIXES:
        return True

    # Language interwiki prefixes (for example, "fr:Title").
    if re.fullmatch(r"[a-z]{2,3}", prefix_lower):
        return True

    return False


def fetch_top_movie_titles_for_year(year: int) -> List[str]:
    page = f"{year}_in_film"
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
        attempts=3,
    )
    wikitext = payload.get("parse", {}).get("wikitext", "")
    if not wikitext:
        return []

    section = extract_highest_grossing_section(wikitext)
    if not section:
        return []

    return extract_wikilinks(section)


def get_wikidata_qids_for_titles(titles: List[str]) -> Dict[str, str]:
    if not titles:
        return {}

    title_to_qid: Dict[str, str] = {}
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
        pages = payload.get("query", {}).get("pages", {})
        for page_data in pages.values():
            title = page_data.get("title", "")
            qid = page_data.get("pageprops", {}).get("wikibase_item", "")
            if title and qid:
                title_to_qid[title] = qid
        time.sleep(0.05)
    return title_to_qid


def parse_p106_ids(entity: dict) -> Set[str]:
    out: Set[str] = set()
    for claim in entity.get("claims", {}).get("P106", []):
        value = claim.get("mainsnak", {}).get("datavalue", {}).get("value", {})
        qid = value.get("id")
        if isinstance(qid, str):
            out.add(qid)
    return out


def parse_cast_qids(entity: dict) -> List[str]:
    out: List[str] = []
    for claim in entity.get("claims", {}).get("P161", []):
        value = claim.get("mainsnak", {}).get("datavalue", {}).get("value", {})
        qid = value.get("id")
        if isinstance(qid, str):
            out.append(qid)
    return out


def parse_release_years(entity: dict) -> Set[int]:
    years: Set[int] = set()
    for claim in entity.get("claims", {}).get("P577", []):
        value = claim.get("mainsnak", {}).get("datavalue", {}).get("value", {})
        time_value = value.get("time")
        if not isinstance(time_value, str):
            continue
        match = re.match(r"^[+-](\d{4})-", time_value)
        if not match:
            continue
        try:
            years.add(int(match.group(1)))
        except ValueError:
            continue
    return years


def parse_year_from_claims(entity: dict, prop_id: str) -> Optional[int]:
    for claim in entity.get("claims", {}).get(prop_id, []):
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


def compute_age_and_film_density(total_film_count: int, birth_year: Optional[int], death_year: Optional[int]) -> Tuple[Optional[int], float]:
    if not birth_year:
        return None, float(total_film_count)

    current_year = date.today().year
    end_year = death_year if death_year and death_year >= birth_year else current_year
    age_years = max(0, end_year - birth_year)

    # Estimate active film years from adulthood to today/death with a floor to avoid extreme values.
    active_years = max(5, end_year - (birth_year + 16))
    films_per_active_year = total_film_count / active_years
    return age_years, films_per_active_year


def min_max_normalize(values: List[float]) -> List[float]:
    if not values:
        return []
    lo = min(values)
    hi = max(values)
    if math.isclose(lo, hi):
        return [1.0 for _ in values]
    scale = hi - lo
    return [(v - lo) / scale for v in values]


def get_entities_by_qid(qids: List[str], props: str, sitefilter: Optional[str] = None) -> Dict[str, dict]:
    entities: Dict[str, dict] = {}
    for batch in chunked(qids, 50):
        params = {
            "action": "wbgetentities",
            "format": "json",
            "ids": "|".join(batch),
            "props": props,
            "languages": "en",
        }
        if sitefilter:
            params["sitefilter"] = sitefilter
        payload = http_get_json(
            WIKIDATA_API,
            params,
            timeout=40,
            attempts=3,
        )
        entities.update(payload.get("entities", {}))
        time.sleep(0.05)
    return entities


def read_top_movies_csv(path: str) -> List[TopMovieRow]:
    rows: List[TopMovieRow] = []
    rank_counter_by_year: Dict[int, int] = defaultdict(int)
    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            qid = (row.get("qid") or "").strip()
            title = (
                (row.get("title") or "").strip()
                or (row.get("wikidata_label") or "").strip()
                or (row.get("wikipedia_title") or "").strip()
            )
            if not qid or not title:
                continue
            try:
                year = int((row.get("year") or "0").strip())
                release_year_text = (row.get("release_year") or "").strip()
                release_year = int(release_year_text) if release_year_text else year
            except ValueError:
                continue

            rank_text = (row.get("rank_in_year") or "").strip()
            if rank_text:
                try:
                    rank_in_year = int(rank_text)
                except ValueError:
                    rank_counter_by_year[year] += 1
                    rank_in_year = rank_counter_by_year[year]
            else:
                rank_counter_by_year[year] += 1
                rank_in_year = rank_counter_by_year[year]

            rows.append(
                TopMovieRow(
                    year=year,
                    rank_in_year=rank_in_year,
                    title=title,
                    qid=qid,
                    release_year=release_year,
                )
            )
    return rows


def build_total_film_count_query(actor_qids: List[str]) -> str:
    values = " ".join(f"wd:{qid}" for qid in actor_qids)
    return f"""
SELECT ?person (COUNT(DISTINCT ?film) AS ?filmCount) WHERE {{
  VALUES ?person {{ {values} }}
  ?film wdt:P31 wd:Q11424 .
  ?film wdt:P161 ?person .
  FILTER(?film != wd:Q40786)
}}
GROUP BY ?person
"""


def fetch_total_film_counts(actor_qids: List[str], verbose: bool) -> Dict[str, int]:
    out: Dict[str, int] = {qid: 0 for qid in actor_qids}
    batch_size = 40
    total_batches = (len(actor_qids) + batch_size - 1) // batch_size
    for idx, batch in enumerate(chunked(actor_qids, batch_size), start=1):
        payload = sparql_query(build_total_film_count_query(batch), timeout=90, attempts=4)
        rows = payload.get("results", {}).get("bindings", [])
        for row in rows:
            person_uri = row.get("person", {}).get("value", "")
            if not person_uri:
                continue
            qid = person_uri.rsplit("/", 1)[-1]
            try:
                count = int(float(row.get("filmCount", {}).get("value", "0")))
            except ValueError:
                count = 0
            out[qid] = count
        if verbose:
            print(f"Resolved total film counts: batch {idx}/{total_batches}")
        time.sleep(0.05)
    return out


def build_actor_rows_from_top_movies(
    top_movies_rows: List[TopMovieRow],
    cast_per_movie: int,
    min_source_movies: int,
    min_total_films: int,
    limit: int,
    verbose: bool,
) -> List[ActorRow]:
    selected_movie_qids = {row.qid for row in top_movies_rows}
    movie_name_by_qid = {row.qid: row.title for row in top_movies_rows}

    movie_entities = get_entities_by_qid(sorted(selected_movie_qids), props="claims|labels")

    # Collect actor QIDs from cast claims.
    actor_to_movies: Dict[str, Set[str]] = defaultdict(set)
    for movie_qid in sorted(selected_movie_qids):
        movie_name = movie_name_by_qid.get(movie_qid, movie_qid)
        cast_qids = parse_cast_qids(movie_entities.get(movie_qid, {}))[:cast_per_movie]
        for actor_qid in cast_qids:
            actor_to_movies[actor_qid].add(movie_name)

    actor_qids = sorted(actor_to_movies.keys())
    if verbose:
        print(f"Collected {len(actor_qids)} unique cast members from source movies")

    # Remove one-off cameos/noise from source movies early.
    if min_source_movies > 1:
        actor_qids = [qid for qid in actor_qids if len(actor_to_movies.get(qid, set())) >= min_source_movies]
        actor_to_movies = {qid: actor_to_movies[qid] for qid in actor_qids}
        if verbose:
            print(f"After source-movie count filter (>= {min_source_movies}): {len(actor_qids)} cast members")

    # Apply threshold using each actor's global filmography size, not source-movie overlap.
    total_film_count_by_qid = fetch_total_film_counts(actor_qids, verbose=verbose)
    actor_qids = [qid for qid in actor_qids if total_film_count_by_qid.get(qid, 0) >= min_total_films]
    actor_to_movies = {qid: actor_to_movies[qid] for qid in actor_qids}
    if verbose:
        print(f"After total film count filter (>= {min_total_films}): {len(actor_qids)} cast members")

    # Load actor entities: labels, sitelinks, occupations.
    actor_entities = get_entities_by_qid(actor_qids, props="labels|claims|sitelinks")
    start, end = pageview_date_range()

    rows: List[ActorRow] = []
    for idx, actor_qid in enumerate(actor_qids, start=1):
        entity = actor_entities.get(actor_qid, {})
        occs = parse_p106_ids(entity)
        # Keep only explicit actor occupations to reduce musician/other non-actor noise.
        if not occs.intersection(ACTOR_OCCUPATIONS):
            continue

        sitelinks_obj = entity.get("sitelinks", {})
        enwiki_title = sitelinks_obj.get("enwiki", {}).get("title", "")
        sitelinks = len(sitelinks_obj)

        # Prefer a human-readable label; fall back to enwiki title before raw QID.
        labels_obj = entity.get("labels", {})
        en_label = labels_obj.get("en", {}).get("value", "")
        if en_label and en_label != actor_qid:
            name = en_label
        elif enwiki_title:
            name = enwiki_title.replace("_", " ")
        else:
            any_label = next(iter(labels_obj.values()), {}) if labels_obj else {}
            name = any_label.get("value", "") or actor_qid

        views = fetch_pageviews_12mo(enwiki_title, start, end)
        total_films = total_film_count_by_qid.get(actor_qid, 0)
        birth_year = parse_year_from_claims(entity, "P569")
        death_year = parse_year_from_claims(entity, "P570")
        age_years, films_per_active_year = compute_age_and_film_density(total_films, birth_year, death_year)

        movies = sorted(actor_to_movies.get(actor_qid, set()))
        rows.append(
            ActorRow(
                rank=0,
                name=name,
                qid=actor_qid,
                sitelinks=sitelinks,
                pageviews_12mo=views,
                source_movie_count=len(movies),
                total_film_count=total_films,
                age_years=age_years,
                films_per_active_year=films_per_active_year,
                source_movies=movies,
                enwiki_title=enwiki_title,
            )
        )

        if verbose and idx % 25 == 0:
            print(f"Processed {idx}/{len(actor_qids)} cast members")
        time.sleep(0.03)

    pageview_norm = min_max_normalize([math.log1p(r.pageviews_12mo) for r in rows])
    sitelink_norm = min_max_normalize([math.log1p(r.sitelinks) for r in rows])
    productivity_norm = min_max_normalize([math.log1p(r.films_per_active_year) for r in rows])

    for i, row in enumerate(rows):
        # Blend traffic/reach with age-adjusted film productivity.
        row.popularity_score = (
            0.60 * pageview_norm[i] + 0.20 * sitelink_norm[i] + 0.20 * productivity_norm[i]
        )

    ranked = sorted(
        rows,
        key=lambda r: (r.popularity_score, r.pageviews_12mo, r.sitelinks, r.source_movie_count, r.name.lower()),
        reverse=True,
    )
    selected = ranked[:limit]
    for i, row in enumerate(selected, start=1):
        row.rank = i
    return selected


def write_csv(path: str, rows: List[ActorRow]) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "rank",
                "name",
                "qid",
                "pageviews_12mo",
                "sitelinks",
                "source_movie_count",
                "total_film_count",
                "age_years",
                "films_per_active_year",
                "popularity_score",
                "source_movies",
                "enwiki_title",
            ]
        )
        for row in rows:
            writer.writerow(
                [
                    row.rank,
                    row.name,
                    row.qid,
                    row.pageviews_12mo,
                    row.sitelinks,
                    row.source_movie_count,
                    row.total_film_count,
                    row.age_years if row.age_years is not None else "",
                    f"{row.films_per_active_year:.4f}",
                    f"{row.popularity_score:.6f}",
                    " | ".join(row.source_movies),
                    row.enwiki_title,
                ]
            )


def print_preview(rows: List[ActorRow], top_n: int = 15) -> None:
    print(f"Top {min(top_n, len(rows))} actors by blended popularity score:")
    for row in rows[:top_n]:
        print(
            f"{row.rank:>4}. {row.name} ({row.qid}) | score={row.popularity_score:.4f} "
            f"| views12mo={row.pageviews_12mo:,} | sitelinks={row.sitelinks} "
            f"| films_per_active_year={row.films_per_active_year:.3f} "
            f"| source_movies={row.source_movie_count} | total_films={row.total_film_count}"
        )


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Build actor popularity from a top-movies CSV: "
            "top_movies_by_year.csv -> cast -> sitelinks/pageviews + age-adjusted film productivity ranking."
        )
    )
    parser.add_argument("--start-year", type=int, default=1970)
    parser.add_argument("--end-year", type=int, default=2025)
    parser.add_argument("--movies-per-year", type=int, default=15)
    parser.add_argument("--cast-per-movie", type=int, default=40)
    parser.add_argument(
        "--min-source-movies",
        type=int,
        default=2,
        help=(
            "Minimum number of selected source movies an actor must appear in. "
            "Useful for excluding one-off cameos/non-actor noise (default: 2)."
        ),
    )
    parser.add_argument("--min-total-films", type=int, default=3)
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument(
        "--top-movies-input",
        default="data/top_movies_by_year.csv",
        help="Input CSV from extract_american_films_box_office.py (default: data/top_movies_by_year.csv)",
    )
    parser.add_argument("--output", default="data/compiled_popularity.csv")
    parser.add_argument("--verbose", action="store_true", default=True)
    args = parser.parse_args()

    if args.start_year < 1900 or args.end_year > 2100 or args.start_year > args.end_year:
        print("Invalid year range")
        return 1
    if args.movies_per_year < 1 or args.movies_per_year > 30:
        print("--movies-per-year must be between 1 and 30")
        return 1
    if args.cast_per_movie < 1 or args.cast_per_movie > 50:
        print("--cast-per-movie must be between 1 and 50")
        return 1
    if args.min_source_movies < 1 or args.min_source_movies > 100:
        print("--min-source-movies must be between 1 and 100")
        return 1
    if args.min_total_films < 1 or args.min_total_films > 1000:
        print("--min-total-films must be between 1 and 1000")
        return 1
    if args.limit < 1 or args.limit > 10000:
        print("--limit must be between 1 and 10000")
        return 1

    try:
        top_movie_rows_all = read_top_movies_csv(args.top_movies_input)
        rows_by_year: Dict[int, List[TopMovieRow]] = defaultdict(list)
        for row in top_movie_rows_all:
            rows_by_year[row.year].append(row)

        top_movie_rows: List[TopMovieRow] = []
        for year in range(args.start_year, args.end_year + 1):
            year_rows = rows_by_year.get(year, [])
            if not year_rows:
                if args.verbose:
                    print(f"Year {year}: no rows found in {args.top_movies_input}")
                continue
            selected = year_rows[: args.movies_per_year]
            top_movie_rows.extend(selected)
            if args.verbose:
                print(
                    f"Year {year}: using {len(selected)} top-movie rows from {args.top_movies_input} "
                    f"(available={len(year_rows)}, cap={args.movies_per_year})"
                )

        if not top_movie_rows:
            print(f"No usable movie rows found in {args.top_movies_input} for the selected year range")
            return 1

        rows = build_actor_rows_from_top_movies(
            top_movies_rows=top_movie_rows,
            cast_per_movie=args.cast_per_movie,
            min_source_movies=args.min_source_movies,
            min_total_films=args.min_total_films,
            limit=args.limit,
            verbose=args.verbose,
        )
        write_csv(args.output, rows)
        print(f"Loaded {len(top_movie_rows)} rows from {args.top_movies_input}")
        print_preview(rows)
        print(f"Saved {len(rows)} rows to {args.output}")
        return 0
    except Exception as exc:
        print(f"Error: {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
