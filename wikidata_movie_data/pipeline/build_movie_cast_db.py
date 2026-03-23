#!/usr/bin/env python3
"""Build a movie-cast database from compiled_popularity.csv actors.

For each of the top-N actors in compiled_popularity.csv:
  1. Fetch their filmography via person_search.get_person_films
  2. Invert the actor→films mapping to build movie → [actors]

Because we query each actor's filmography, the cast list for a given movie
will contain only actors who also appear in our top-N set — which is exactly
what the game needs (you can only navigate to actors in the database).

Outputs:
    - wikidata_movie_data/movie_cast_db.csv   (movie_qid, movie_title, box_office, actor_qid, actor_name)
  - actor_path_game/movie_cast_db.js        (JS constant _GENERATED_MOVIE_CAST_DB)
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import ssl
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

import certifi

SCRIPT_DIR = Path(__file__).resolve().parent
UTILS_DIR = SCRIPT_DIR.parent / "utils"
sys.path.insert(0, str(UTILS_DIR))

from person_search import get_person_films  # type: ignore[import-not-found]

WIKIDATA_API = "https://www.wikidata.org/w/api.php"
USER_AGENT = "wikidata-movie-data-cli/0.1 (https://www.wikidata.org/wiki/Wikidata:Data_access)"
EXCLUDED_SUBTYPE_KEYWORDS = (
    "documentary",
    "concert film",
    "concert movie",
)
EXCLUDED_MOVIE_TITLES = {
    "a century of cinema",
    "barbra: the concert",
}

# Known trilogy/collection mappings: collection title → [(individual_title, qid, year_released)]
# This allows us to split collection entities into their constituent films
# QIDs are fetched from Wikidata to ensure proper cast attribution
COLLECTION_EXPANSIONS = {
    "the godfather trilogy": [
        ("The Godfather", "Q11399", 1972),
        ("The Godfather Part II", "Q34215", 1974),
        ("The Godfather Part III", "Q179010", 1990),
    ],
}


def is_collection_title(title: str) -> bool:
    """Check if a title matches a known collection pattern."""
    title_lower = title.lower().strip()
    # Check against known collection expansions
    if any(title_lower.startswith(k) for k in COLLECTION_EXPANSIONS.keys()):
        return True
    # Check for generic collection patterns
    if any(pattern in title_lower for pattern in ["trilogy", "quadrilogy", "pentology", "collection"]):
        return True
    return False


def expand_collection_title(title: str) -> list[tuple[str, str]]:
    """Expand collection titles into individual film (title, qid) tuples.
    
    Returns a list of (title, qid) tuples for individual films,
    or [(title, "")] for the original title if not a known collection.
    """
    title_lower = title.lower().strip()
    
    # Check known expansions
    for collection_key, films in COLLECTION_EXPANSIONS.items():
        if title_lower.startswith(collection_key):
            return [(film_title, qid) for film_title, qid, _ in films]
    
    # If it's a collection pattern but not in our mapping, skip it entirely
    if is_collection_title(title):
        return []
    
    return [(title, "")]


def load_actors(csv_path: Path, top_n: int) -> list[tuple[str, str]]:
    """Return [(qid, name), ...] for the top-N rows in compiled_popularity.csv."""
    actors: list[tuple[str, str]] = []
    with csv_path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            if len(actors) >= top_n:
                break
            qid = row.get("qid", "").strip()
            name = row.get("name", "").strip()
            if qid and name:
                actors.append((qid, name))
    return actors


def build_ssl_context() -> ssl.SSLContext:
    return ssl.create_default_context(cafile=certifi.where())


def parse_series_ordinal(value: object) -> int | None:
    """Parse Wikidata series ordinal values (P1545) into an integer when possible."""
    text = str(value or "").strip()
    if not text:
        return None
    match = re.match(r"^(\d+)", text)
    if not match:
        return None
    try:
        n = int(match.group(1))
    except ValueError:
        return None
    return n if n > 0 else None


def fetch_movie_cast_claims(movie_qids: list[str]) -> dict[str, list[tuple[str, int | None, int]]]:
    cast_by_movie: dict[str, list[tuple[str, int | None, int]]] = {}
    for start in range(0, len(movie_qids), 50):
        batch = movie_qids[start : start + 50]
        params = {
            "action": "wbgetentities",
            "format": "json",
            "ids": "|".join(batch),
            "props": "claims",
            "languages": "en",
        }
        url = f"{WIKIDATA_API}?{urllib.parse.urlencode(params)}"
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=40, context=build_ssl_context()) as resp:
            payload = json.loads(resp.read().decode(resp.headers.get_content_charset() or "utf-8"))

        for movie_qid, entity in payload.get("entities", {}).items():
            cast_entries: list[tuple[str, int | None, int]] = []
            for claim_idx, claim in enumerate(entity.get("claims", {}).get("P161", []), start=1):
                value = claim.get("mainsnak", {}).get("datavalue", {}).get("value", {})
                actor_qid = value.get("id")
                if isinstance(actor_qid, str):
                    qualifiers = claim.get("qualifiers", {})
                    ordinal_raw = None
                    for q in qualifiers.get("P1545", []):
                        qv = q.get("datavalue", {}).get("value")
                        parsed = parse_series_ordinal(qv)
                        if parsed is not None:
                            ordinal_raw = parsed
                            break
                    cast_entries.append((actor_qid, ordinal_raw, claim_idx))
            cast_by_movie[movie_qid] = cast_entries

        time.sleep(0.05)

    return cast_by_movie


def fetch_entity_labels(entity_qids: list[str]) -> dict[str, str]:
    labels: dict[str, str] = {}
    for start in range(0, len(entity_qids), 50):
        batch = entity_qids[start : start + 50]
        if not batch:
            continue

        params = {
            "action": "wbgetentities",
            "format": "json",
            "ids": "|".join(batch),
            "props": "labels",
            "languages": "en",
        }
        url = f"{WIKIDATA_API}?{urllib.parse.urlencode(params)}"
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=40, context=build_ssl_context()) as resp:
            payload = json.loads(resp.read().decode(resp.headers.get_content_charset() or "utf-8"))

        for qid, entity in payload.get("entities", {}).items():
            labels[qid] = entity.get("labels", {}).get("en", {}).get("value", "")

        time.sleep(0.05)

    return labels


def fetch_movie_type_and_genre_qids(movie_qids: list[str]) -> dict[str, set[str]]:
    tags_by_movie: dict[str, set[str]] = {}

    for start in range(0, len(movie_qids), 50):
        batch = movie_qids[start : start + 50]
        if not batch:
            continue

        params = {
            "action": "wbgetentities",
            "format": "json",
            "ids": "|".join(batch),
            "props": "claims",
            "languages": "en",
        }
        url = f"{WIKIDATA_API}?{urllib.parse.urlencode(params)}"
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=40, context=build_ssl_context()) as resp:
            payload = json.loads(resp.read().decode(resp.headers.get_content_charset() or "utf-8"))

        for movie_qid, entity in payload.get("entities", {}).items():
            tags: set[str] = set()
            claims = entity.get("claims", {})

            for prop in ("P31", "P136"):
                for claim in claims.get(prop, []):
                    value = claim.get("mainsnak", {}).get("datavalue", {}).get("value", {})
                    tag_qid = value.get("id")
                    if isinstance(tag_qid, str):
                        tags.add(tag_qid)

            tags_by_movie[movie_qid] = tags

        time.sleep(0.05)

    return tags_by_movie


def should_exclude_movie_by_subject(subject_labels: set[str]) -> bool:
    lowered = {label.lower().strip() for label in subject_labels if label}
    if not lowered:
        return False
    return any(any(keyword in label for keyword in EXCLUDED_SUBTYPE_KEYWORDS) for label in lowered)


def filter_out_documentaries_and_concert_films(movies: dict[str, dict]) -> int:
    movie_qids = sorted(movies.keys())
    if not movie_qids:
        return 0

    tags_by_movie = fetch_movie_type_and_genre_qids(movie_qids)
    referenced_tag_qids = sorted({tag_qid for tags in tags_by_movie.values() for tag_qid in tags})
    labels_by_qid = fetch_entity_labels(referenced_tag_qids)

    qids_to_drop: list[str] = []
    for movie_qid, tags in tags_by_movie.items():
        movie_title = str(movies.get(movie_qid, {}).get("title", "")).strip().lower()
        if movie_title in EXCLUDED_MOVIE_TITLES:
            qids_to_drop.append(movie_qid)
            continue

        subject_labels = {labels_by_qid.get(tag_qid, "") for tag_qid in tags}
        if should_exclude_movie_by_subject(subject_labels):
            qids_to_drop.append(movie_qid)

    for movie_qid in qids_to_drop:
        movies.pop(movie_qid, None)

    if qids_to_drop:
        print(
            f"Excluded {len(qids_to_drop)} documentary/concert-film titles "
            f"based on Wikidata type/genre metadata."
        )
    else:
        print("Excluded 0 documentary/concert-film titles based on Wikidata type/genre metadata.")

    return len(qids_to_drop)


def build_movie_cast(
    actors: list[tuple[str, str]],
    films_per_actor: int,
) -> dict[str, dict]:
    """
    Query each actor's filmography and invert to movie → cast.

    Returns:
        {movie_qid: {"title": str, "box_office": int|None, "cast": [(actor_qid, actor_name), ...]}}
    """
    movies: dict[str, dict] = {}
    total = len(actors)

    for idx, (qid, name) in enumerate(actors, start=1):
        print(f"[{idx}/{total}] {name} ({qid})", flush=True)
        try:
            films = get_person_films(qid, role="cast", limit=films_per_actor)
        except Exception as exc:
            print(f"  WARNING: failed — {exc}", flush=True)
            time.sleep(1.0)
            continue

        for film in films:
            if not film.qid or not film.title:
                continue
            
            # Expand collection titles (e.g., trilogies) into individual films
            expanded_films = expand_collection_title(film.title)
            
            for expanded_title, expanded_qid in expanded_films:
                # For expanded films with known QIDs, use those; otherwise use the original QID
                movie_key = expanded_qid if expanded_qid else film.qid
                box_office = film.box_office
                
                if movie_key not in movies:
                    movies[movie_key] = {"title": expanded_title, "box_office": box_office, "cast": []}
                elif movies[movie_key].get("box_office") is None and box_office is not None:
                    movies[movie_key]["box_office"] = box_office
                seen_qids = {c[0] for c in movies[movie_key]["cast"]}
                if qid not in seen_qids:
                    movies[movie_key]["cast"].append((qid, name))

        # Polite delay between actors to avoid rate-limiting
        time.sleep(0.3)

    return movies


def enrich_movie_cast_from_movie_claims(
    movies: dict[str, dict],
    actors: list[tuple[str, str]],
    main_cast_mode: str,
    main_cast_max_billing: int,
    main_cast_fallback_claim_order_max: int,
) -> None:
    actor_name_by_qid = {qid: name for qid, name in actors}
    cast_claims = fetch_movie_cast_claims(sorted(movies.keys()))

    movies_with_any_billing = 0
    movies_filtered_by_billing = 0
    actors_pruned = 0
    actors_added = 0

    for movie_qid, cast_entries in cast_claims.items():
        if movie_qid not in movies:
            continue

        has_any_billing = any(ordinal is not None for _, ordinal, _ in cast_entries)
        if has_any_billing:
            movies_with_any_billing += 1

        allowed_qids: set[str] | None = None
        if main_cast_mode == "strict":
            allowed_qids = {
                actor_qid
                for actor_qid, ordinal, _ in cast_entries
                if ordinal is not None and ordinal <= main_cast_max_billing
            }
            movies_filtered_by_billing += 1
        elif main_cast_mode == "hybrid":
            if has_any_billing:
                allowed_qids = {
                    actor_qid
                    for actor_qid, ordinal, _ in cast_entries
                    if ordinal is not None and ordinal <= main_cast_max_billing
                }
                movies_filtered_by_billing += 1
            else:
                # Billing qualifiers are often missing; use claim order as a practical
                # fallback and keep only the first N listed cast entries.
                allowed_qids = {
                    actor_qid
                    for actor_qid, _ordinal, claim_idx in cast_entries
                    if claim_idx <= main_cast_fallback_claim_order_max
                }
                movies_filtered_by_billing += 1

        if allowed_qids is not None:
            before = len(movies[movie_qid]["cast"])
            movies[movie_qid]["cast"] = [
                (actor_qid, actor_name)
                for actor_qid, actor_name in movies[movie_qid]["cast"]
                if actor_qid in allowed_qids
            ]
            actors_pruned += max(0, before - len(movies[movie_qid]["cast"]))

        existing_qids = {actor_qid for actor_qid, _ in movies[movie_qid]["cast"]}
        for actor_qid, _ordinal, _claim_idx in cast_entries:
            if actor_qid not in actor_name_by_qid or actor_qid in existing_qids:
                continue
            if allowed_qids is not None and actor_qid not in allowed_qids:
                continue
            movies[movie_qid]["cast"].append((actor_qid, actor_name_by_qid[actor_qid]))
            existing_qids.add(actor_qid)
            actors_added += 1

    print("\nMain-cast filter summary:")
    print(
        f"  mode={main_cast_mode} max_billing={main_cast_max_billing} "
        f"fallback_claim_order_max={main_cast_fallback_claim_order_max}"
    )
    print(f"  movies checked: {len(cast_claims)}")
    print(f"  movies with billing qualifiers: {movies_with_any_billing}")
    print(f"  movies filtered by billing rule: {movies_filtered_by_billing}")
    print(f"  cast links pruned: {actors_pruned}")
    print(f"  cast links added from movie claims: {actors_added}")


def write_csv(movies: dict[str, dict], output_path: Path) -> None:
    """Write one row per (movie, actor) pair to CSV."""
    with output_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["movie_qid", "movie_title", "box_office", "actor_qid", "actor_name"])
        for movie_qid, data in sorted(movies.items(), key=lambda x: x[1]["title"].lower()):
            box_office = data.get("box_office")
            for actor_qid, actor_name in data["cast"]:
                writer.writerow([movie_qid, data["title"], box_office if box_office is not None else "", actor_qid, actor_name])
    print(f"\nCSV written → {output_path}")


def write_js(movies: dict[str, dict], output_path: Path) -> None:
    """
    Write a JS file defining _GENERATED_MOVIE_CAST_DB.

    Keys are lowercase movie titles; values are arrays of actor name strings.
    Duplicate titles (different QIDs, same label) are merged.
    """
    db: dict[str, list[str]] = {}
    title_display: dict[str, str] = {}
    for data in movies.values():
        key = data["title"].lower()
        names = [n for _, n in data["cast"]]
        if not names:
            continue
        title_display.setdefault(key, data["title"])
        if key in db:
            existing = set(db[key])
            db[key].extend(n for n in names if n not in existing)
        else:
            db[key] = names

    # Only keep movies with ≥2 cast members (so there's actually someone to jump to)
    db = {k: v for k, v in db.items() if len(v) >= 2}

    lines = [
        "// Auto-generated by build_movie_cast_db.py — do not edit manually.",
        f"// {len(db)} movies • sourced from Wikidata via compiled_popularity.csv",
        "// eslint-disable-next-line no-unused-vars",
        "const _GENERATED_MOVIE_CAST_DB = {",
    ]
    for title in sorted(db):
        safe_title = title.replace("\\", "\\\\").replace('"', '\\"')
        cast_json = json.dumps(db[title], ensure_ascii=False)
        lines.append(f'  "{safe_title}": {cast_json},')
    lines.append("};")
    lines.append("")
    lines.append("// eslint-disable-next-line no-unused-vars")
    lines.append("const _GENERATED_MOVIE_TITLE_CASE = {")
    for title in sorted(title_display):
        safe_title = title.replace("\\", "\\\\").replace('"', '\\"')
        safe_display = title_display[title].replace("\\", "\\\\").replace('"', '\\"')
        lines.append(f'  "{safe_title}": "{safe_display}",')
    lines.append("};")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"JS  written → {output_path}")
    print(f"    {len(db)} movies included")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build movie-cast DB from compiled_popularity.csv."
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=500,
        help="Number of top actors to process (default: 500).",
    )
    parser.add_argument(
        "--films-per-actor",
        type=int,
        default=100,
        help="Max films to fetch per actor (default: 100).",
    )
    parser.add_argument(
        "--compiled-csv",
        default="data/compiled_popularity.csv",
        help="Path to compiled_popularity.csv (default: data/compiled_popularity.csv).",
    )
    parser.add_argument(
        "--csv-out",
        default="data/movie_cast_db.csv",
        help="Output CSV path (default: data/movie_cast_db.csv).",
    )
    parser.add_argument(
        "--js-out",
        default="../actor_path_game/movie_cast_db.js",
        help="Output JS path (default: ../actor_path_game/movie_cast_db.js).",
    )
    parser.add_argument(
        "--main-cast-mode",
        choices=["off", "hybrid", "strict"],
        default="hybrid",
        help=(
            "How to apply main-cast filtering using Wikidata cast statement billing qualifiers (P1545). "
            "off=keep all cast links; hybrid=if billing exists for a movie keep top-billed only, "
            "else keep first N cast claims by claim order; "
            "strict=keep only top-billed where billing qualifier is present. Default: hybrid."
        ),
    )
    parser.add_argument(
        "--main-cast-max-billing",
        type=int,
        default=8,
        help="Maximum billing position to keep when main-cast filtering applies (default: 8).",
    )
    parser.add_argument(
        "--main-cast-fallback-claim-order-max",
        type=int,
        default=20,
        help=(
            "When mode=hybrid and a movie has no billing qualifiers, keep only the first N cast claims "
            "from Wikidata claim order (default: 20)."
        ),
    )
    parser.add_argument("--verbose", action="store_true", default=True)
    args = parser.parse_args()

    if args.main_cast_max_billing < 1:
        print("ERROR: --main-cast-max-billing must be >= 1", file=sys.stderr)
        return 1
    if args.main_cast_fallback_claim_order_max < 1:
        print("ERROR: --main-cast-fallback-claim-order-max must be >= 1", file=sys.stderr)
        return 1

    compiled_csv = Path(args.compiled_csv)
    if not compiled_csv.exists():
        print(f"ERROR: {compiled_csv} not found", file=sys.stderr)
        return 1

    actors = load_actors(compiled_csv, args.top_n)
    print(f"Loaded {len(actors)} actors from {compiled_csv}\n")

    movies = build_movie_cast(actors, args.films_per_actor)
    filter_out_documentaries_and_concert_films(movies)
    enrich_movie_cast_from_movie_claims(
        movies,
        actors,
        main_cast_mode=args.main_cast_mode,
        main_cast_max_billing=args.main_cast_max_billing,
        main_cast_fallback_claim_order_max=args.main_cast_fallback_claim_order_max,
    )
    print(f"\nBuilt mapping: {len(movies)} unique movies")

    write_csv(movies, Path(args.csv_out))
    write_js(movies, Path(args.js_out))

    return 0


if __name__ == "__main__":
    sys.exit(main())
