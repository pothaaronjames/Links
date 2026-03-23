#!/usr/bin/env python3
"""Run the full Wikidata movie pipeline end-to-end.

Usage:
  python pipeline/__init__.py

Environment variable overrides are supported for all primary knobs:
  PYTHON_BIN, START_YEAR, END_YEAR, TOP_N_PER_YEAR, MOVIES_PER_YEAR,
  CAST_PER_MOVIE, MIN_TOTAL_FILMS, POPULARITY_LIMIT, DB_TOP_N,
  FILMS_PER_ACTOR, MAIN_CAST_MODE, MAIN_CAST_MAX_BILLING,
  MAIN_CAST_FALLBACK_MAX, PORTRAIT_TOP_N, PORTRAIT_WIDTH
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Sequence


def env_or_default_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer (got {raw!r})") from exc


def env_or_default_str(name: str, default: str) -> str:
    raw = os.getenv(name)
    return raw if raw is not None and raw.strip() != "" else default


def resolve_python_bin(root_dir: Path) -> str:
    env_python = os.getenv("PYTHON_BIN")
    if env_python:
        return env_python

    candidate1 = root_dir.parent / ".venv" / "bin" / "python"
    if candidate1.exists() and os.access(candidate1, os.X_OK):
        return str(candidate1)

    candidate2 = root_dir / ".venv" / "bin" / "python"
    if candidate2.exists() and os.access(candidate2, os.X_OK):
        return str(candidate2)

    python3_bin = shutil.which("python3")
    if python3_bin:
        return python3_bin

    return sys.executable


def run_step(cmd: Sequence[str], label: str) -> None:
    print(label)
    subprocess.run(cmd, check=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run all 4 Wikidata movie data pipeline steps")
    parser.add_argument("--python-bin", default=env_or_default_str("PYTHON_BIN", ""))
    parser.add_argument("--start-year", type=int, default=env_or_default_int("START_YEAR", 1970))
    parser.add_argument("--end-year", type=int, default=env_or_default_int("END_YEAR", 2025))
    parser.add_argument("--top-n-per-year", type=int, default=env_or_default_int("TOP_N_PER_YEAR", 15))
    parser.add_argument("--movies-per-year", type=int, default=env_or_default_int("MOVIES_PER_YEAR", 15))
    parser.add_argument("--cast-per-movie", type=int, default=env_or_default_int("CAST_PER_MOVIE", 40))
    parser.add_argument("--min-total-films", type=int, default=env_or_default_int("MIN_TOTAL_FILMS", 3))
    parser.add_argument("--popularity-limit", type=int, default=env_or_default_int("POPULARITY_LIMIT", 500))
    parser.add_argument("--db-top-n", type=int, default=env_or_default_int("DB_TOP_N", 500))
    parser.add_argument("--films-per-actor", type=int, default=env_or_default_int("FILMS_PER_ACTOR", 100))
    parser.add_argument("--main-cast-mode", default=env_or_default_str("MAIN_CAST_MODE", "hybrid"))
    parser.add_argument("--main-cast-max-billing", type=int, default=env_or_default_int("MAIN_CAST_MAX_BILLING", 8))
    parser.add_argument(
        "--main-cast-fallback-max",
        type=int,
        default=env_or_default_int("MAIN_CAST_FALLBACK_MAX", 20),
    )
    parser.add_argument("--portrait-top-n", type=int, default=env_or_default_int("PORTRAIT_TOP_N", 500))
    parser.add_argument("--portrait-width", type=int, default=env_or_default_int("PORTRAIT_WIDTH", 180))
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    root_dir = Path(__file__).resolve().parent.parent
    pipeline_dir = root_dir / "pipeline"
    data_dir = root_dir / "data"

    python_bin = args.python_bin or resolve_python_bin(root_dir)
    if not Path(python_bin).exists() and shutil.which(python_bin) is None:
        print(f"ERROR: Python executable not found: {python_bin}", file=sys.stderr)
        return 1

    data_dir.mkdir(parents=True, exist_ok=True)

    top_movies_csv = data_dir / "top_movies_by_year.csv"
    compiled_csv = data_dir / "compiled_popularity.csv"
    movie_cast_csv = data_dir / "movie_cast_db.csv"
    movie_cast_js = root_dir.parent / "actor_path_game" / "movie_cast_db.js"
    portraits_dir = root_dir.parent / "actor_path_game" / "actor_portraits"
    portraits_js = root_dir.parent / "actor_path_game" / "actor_portraits.js"

    try:
        run_step(
            [
                python_bin,
                str(pipeline_dir / "extract_american_films_box_office.py"),
                "--start-year",
                str(args.start_year),
                "--end-year",
                str(args.end_year),
                "--top-n",
                str(args.top_n_per_year),
                "--output",
                str(top_movies_csv),
                "--verbose",
            ],
            f"[1/4] Extract top movies by year -> {top_movies_csv}",
        )

        run_step(
            [
                python_bin,
                str(pipeline_dir / "popularity_compiled.py"),
                "--start-year",
                str(args.start_year),
                "--end-year",
                str(args.end_year),
                "--movies-per-year",
                str(args.movies_per_year),
                "--top-movies-input",
                str(top_movies_csv),
                "--cast-per-movie",
                str(args.cast_per_movie),
                "--min-total-films",
                str(args.min_total_films),
                "--limit",
                str(args.popularity_limit),
                "--output",
                str(compiled_csv),
                "--verbose",
            ],
            f"[2/4] Build compiled popularity -> {compiled_csv}",
        )

        run_step(
            [
                python_bin,
                str(pipeline_dir / "build_movie_cast_db.py"),
                "--top-n",
                str(args.db_top_n),
                "--films-per-actor",
                str(args.films_per_actor),
                "--compiled-csv",
                str(compiled_csv),
                "--csv-out",
                str(movie_cast_csv),
                "--js-out",
                str(movie_cast_js),
                "--main-cast-mode",
                str(args.main_cast_mode),
                "--main-cast-max-billing",
                str(args.main_cast_max_billing),
                "--main-cast-fallback-claim-order-max",
                str(args.main_cast_fallback_max),
                "--verbose",
            ],
            f"[3/4] Build movie cast DB -> {movie_cast_csv}",
        )

        run_step(
            [
                python_bin,
                str(pipeline_dir / "download_actor_portraits.py"),
                "--compiled-csv",
                str(compiled_csv),
                "--top-n",
                str(args.portrait_top_n),
                "--width",
                str(args.portrait_width),
                "--images-dir",
                str(portraits_dir),
                "--js-out",
                str(portraits_js),
                "--verbose",
            ],
            f"[4/4] Download actor portraits -> {portraits_dir}",
        )
    except subprocess.CalledProcessError as exc:
        print(f"\nPipeline failed with exit code {exc.returncode}", file=sys.stderr)
        return exc.returncode

    print("Pipeline complete.")
    print(f"  Top movies:        {top_movies_csv}")
    print(f"  Compiled actors:   {compiled_csv}")
    print(f"  Movie cast DB CSV: {movie_cast_csv}")
    print(f"  Movie cast DB JS:  {movie_cast_js}")
    print(f"  Portraits dir:     {portraits_dir}")
    print(f"  Portraits JS:      {portraits_js}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
