# Wikidata Movie Data

This folder is organized around a 4-step data pipeline:

1. pipeline/extract_american_films_box_office.py -> data/top_movies_by_year.csv
2. pipeline/popularity_compiled.py -> data/compiled_popularity.csv
3. pipeline/build_movie_cast_db.py -> data/movie_cast_db.csv
4. pipeline/download_actor_portraits.py -> actor portraits + JS map for the game

Folder layout:
- pipeline/: main pipeline scripts and the Python pipeline runner (pipeline/__init__.py).
- data/: pipeline CSV outputs and inputs between steps.
- utils/: utility scripts not part of the main pipeline.

## Setup

```bash
cd /Users/aaronpoth/Documents/Links/wikidata_movie_data
/Users/aaronpoth/Documents/Links/.venv/bin/python -m pip install -r requirements.txt
```

## Step 1: Build top_movies_by_year.csv

Script: pipeline/extract_american_films_box_office.py

What it does:
- For each year in range, reads List_of_American_films_of_YEAR on Wikipedia.
- Resolves films to Wikidata.
- Fetches box office (P2142) for each film.
- Keeps only the top N films per year by box office.

Command:

```bash
/Users/aaronpoth/Documents/Links/.venv/bin/python pipeline/extract_american_films_box_office.py \
  --start-year 1990 \
  --end-year 2025 \
  --top-n 15 \
  --output data/top_movies_by_year.csv \
  --verbose
```

Arguments:
- --start-year: first year to process. Default: 1970.
- --end-year: last year to process. Default: 2025.
- --top-n: films to keep per year after box office ranking. Default: 15.
- --output: CSV output path. Default: data/top_movies_by_year.csv.
- --verbose: print per-year progress. Default: true.

Output columns:
- year
- source_page
- source_order
- wikipedia_title
- qid
- wikidata_label
- release_year
- box_office

## Step 2: Build compiled_popularity.csv

Script: pipeline/popularity_compiled.py

What it does:
- Reads top_movies_by_year.csv from Step 1.
- Uses selected movie QIDs to fetch cast members.
- Scores actors with blended popularity signals and outputs compiled_popularity.csv.

Command:

```bash
/Users/aaronpoth/Documents/Links/.venv/bin/python pipeline/popularity_compiled.py \
  --start-year 1990 \
  --end-year 2025 \
  --movies-per-year 15 \
  --top-movies-input data/top_movies_by_year.csv \
  --cast-per-movie 40 \
  --min-source-movies 2 \
  --min-total-films 3 \
  --limit 500 \
  --output data/compiled_popularity.csv \
  --verbose
```

Arguments:
- --start-year: first year included from top_movies_by_year.csv. Default: 1970.
- --end-year: last year included from top_movies_by_year.csv. Default: 2025.
- --movies-per-year: maximum movies consumed per year from top_movies_by_year.csv. Default: 15.
- --top-movies-input: path to Step 1 CSV input. Default: data/top_movies_by_year.csv.
- --cast-per-movie: maximum cast entries used per movie. Default: 40.
- --min-source-movies: minimum number of selected source movies an actor must appear in. Default: 2.
- --min-total-films: minimum global film count threshold for actors. Default: 3.
- --limit: number of ranked actor rows to output. Default: 500.
- --output: CSV output path. Default: data/compiled_popularity.csv.
- --verbose: print progress. Default: true.

Output columns:
- rank
- name
- qid
- pageviews_12mo
- sitelinks
- source_movie_count
- total_film_count
- age_years
- films_per_active_year
- popularity_score
- source_movies
- enwiki_title

## Step 3: Build movie_cast_db.csv

Script: pipeline/build_movie_cast_db.py

What it does:
- Reads compiled_popularity.csv from Step 2.
- Expands top actors into filmography and cast graph data.
- Writes movie_cast_db.csv (and optional JS export for the game).

Command:

```bash
/Users/aaronpoth/Documents/Links/.venv/bin/python pipeline/build_movie_cast_db.py \
  --top-n 500 \
  --films-per-actor 100 \
  --compiled-csv data/compiled_popularity.csv \
  --csv-out data/movie_cast_db.csv \
  --js-out ../actor_path_game/movie_cast_db.js \
  --main-cast-mode hybrid \
  --main-cast-max-billing 8 \
  --main-cast-fallback-claim-order-max 8 \
  --verbose
```

Arguments:
- --top-n: number of ranked actors to read from compiled_popularity.csv. Default: 500.
- --films-per-actor: maximum films fetched per actor. Default: 100.
- --compiled-csv: path to Step 2 input. Default: data/compiled_popularity.csv.
- --csv-out: CSV output path. Default: data/movie_cast_db.csv.
- --js-out: JS output path for game usage. Default: ../actor_path_game/movie_cast_db.js.
- --main-cast-mode: cast filtering strategy. Choices: off, hybrid, strict. Default: hybrid.
- --main-cast-max-billing: billing cutoff when filtering by P1545. Default: 8.
- --main-cast-fallback-claim-order-max: hybrid-mode fallback cast-claim cutoff when no billing qualifier exists. Default: 8.
- --verbose: print progress. Default: true.

Output columns in movie_cast_db.csv:
- movie_qid
- movie_title
- box_office
- actor_qid
- actor_name

## Step 4: Download actor portraits

Script: pipeline/download_actor_portraits.py

What it does:
- Reads actor QIDs from data/compiled_popularity.csv.
- Fetches Wikidata P18 image references.
- Downloads resized actor portraits into the game assets directory.
- Generates actor_path_game/actor_portraits.js mapping QID -> local image path.

Command:

```bash
/Users/aaronpoth/Documents/Links/.venv/bin/python pipeline/download_actor_portraits.py \
  --compiled-csv data/compiled_popularity.csv \
  --top-n 500 \
  --width 180 \
  --images-dir ../actor_path_game/actor_portraits \
  --js-out ../actor_path_game/actor_portraits.js \
  --verbose
```

Arguments:
- --compiled-csv: path to Step 2 output CSV. Default: data/compiled_popularity.csv.
- --top-n: number of ranked actors to consider for portraits. Default: 500.
- --width: thumbnail width in pixels. Default: 180.
- --images-dir: output directory for downloaded portrait images. Default: ../actor_path_game/actor_portraits.
- --js-out: output JS map for game portrait lookups. Default: ../actor_path_game/actor_portraits.js.
- --clean: delete existing files in --images-dir before downloading.
- --verbose: print progress. Default: true.

## End-to-end run

```bash
cd /Users/aaronpoth/Documents/Links/wikidata_movie_data

/Users/aaronpoth/Documents/Links/.venv/bin/python pipeline/extract_american_films_box_office.py \
  --start-year 1990 \
  --end-year 2025 \
  --top-n 15 \
  --output data/top_movies_by_year.csv

/Users/aaronpoth/Documents/Links/.venv/bin/python pipeline/popularity_compiled.py \
  --start-year 1990 \
  --end-year 2025 \
  --movies-per-year 15 \
  --top-movies-input data/top_movies_by_year.csv \
  --cast-per-movie 40 \
  --min-source-movies 2 \
  --min-total-films 3 \
  --limit 500 \
  --output data/compiled_popularity.csv \
  --verbose

/Users/aaronpoth/Documents/Links/.venv/bin/python pipeline/build_movie_cast_db.py \
  --top-n 500 \
  --films-per-actor 100 \
  --compiled-csv data/compiled_popularity.csv \
  --csv-out data/movie_cast_db.csv \
  --js-out ../actor_path_game/movie_cast_db.js \
  --main-cast-mode hybrid \
  --main-cast-max-billing 8 \
  --main-cast-fallback-claim-order-max 8 \
  --verbose

/Users/aaronpoth/Documents/Links/.venv/bin/python pipeline/download_actor_portraits.py \
  --compiled-csv data/compiled_popularity.csv \
  --top-n 500 \
  --width 180 \
  --images-dir ../actor_path_game/actor_portraits \
  --js-out ../actor_path_game/actor_portraits.js \
  --verbose
```

## Utility scripts

Non-pipeline helper scripts are in utils/:
- utils/movie_search.py
- utils/person_search.py

## Run Entire Pipeline

Use the Python runner in pipeline/ to execute all 4 steps:

```bash
cd /Users/aaronpoth/Documents/Links/wikidata_movie_data
/Users/aaronpoth/Documents/Links/.venv/bin/python pipeline/__init__.py
```

Optional environment variable overrides:
- PYTHON_BIN
- START_YEAR
- END_YEAR
- TOP_N_PER_YEAR
- MOVIES_PER_YEAR
- CAST_PER_MOVIE
- MIN_TOTAL_FILMS
- POPULARITY_LIMIT
- DB_TOP_N
- FILMS_PER_ACTOR
- MAIN_CAST_MODE
- MAIN_CAST_MAX_BILLING
- MAIN_CAST_FALLBACK_MAX
- PORTRAIT_TOP_N
- PORTRAIT_WIDTH
