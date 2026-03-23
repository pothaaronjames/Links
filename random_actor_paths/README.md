# Random Actor Paths

This folder now contains a precomputed puzzle generator for the Links game.

The generator builds puzzles from:
- ../wikidata_movie_data/compiled_popularity.csv
- ../wikidata_movie_data/movie_cast_db.csv

It is designed for your current dataset shape where compiled_popularity has 300 rows.

## Why this approach

The game should not search graph paths live in the browser.
Instead, generate a pool of valid puzzles offline, then serve random puzzles from that pool.

Benefits:
- fast game load
- guaranteed solvable puzzles
- guaranteed shortest path between 2 and 5 actor hops
- controllable difficulty mix

## Files

- generate_puzzle_pool.py
  - creates puzzle_pool.json, puzzle_pool.csv, and ../actor_path_game/puzzle_pool.js
- puzzle_pool.json
  - runtime-friendly puzzle payload with canonical step-by-step path
- puzzle_pool.csv
  - human-readable export for validation and QA
- ../actor_path_game/puzzle_pool.js
  - browser-loadable puzzle bundle used by the game on page load

## Algorithm

1. Load top actors from compiled_popularity.csv.
   - by default, ranks 1 through 300 are loaded
2. Load movie_cast_db.csv and keep only rows where actor_qid is in that top-300 set.
3. Build an undirected actor graph.
   - edge exists between two actors if they share at least one movie
  - each edge stores one representative movie title
  - if multiple shared movies exist, prefer one with known/higher box office
4. Build candidate pools for start and end actors.
  - start: popular actors (default rank <= 100)
  - end: popular actors (default rank 1 to 100)
   - degree filters remove dead ends and super-hubs
5. Sample start/end pairs and run bounded shortest-path BFS.
  - keep only shortest path length from 1 to 5 hops
  - for equal-hop options, favor edges with known/higher box office
6. Label difficulty.
  - easy: 1 to 2 hops
  - medium: 3 hops
  - hard: 4 to 5 hops
7. Keep puzzle diversity constraints.
   - avoid repeated endpoints too often
   - avoid repeated start/end pairs
   - avoid duplicate path signatures
8. Write output pool.
   - target mix: 60 percent easy, 35 percent medium, 5 percent hard
  - if hard (5-hop shortest paths) are unavailable, generator backfills with easier puzzles to keep full pool size

Box office preference notes:
- If `movie_cast_db.csv` includes `box_office`, puzzle quality prefers paths with known box office values.
- Between known values, higher box office paths are scored better.
- Missing box office does not disqualify a movie; it is treated as neutral and can still be used.

## Default assumptions for your 300-row dataset

- top_actors = 300
- starts are sampled from rank 1 to 100
- ends are sampled from rank 1 to 100
- shortest path must be between 1 and 5 actor hops

In practice, with the current top-300 graph, most valid pairs are 2 to 3 hops apart.
That means large puzzle pools will be mostly easy unless you expand the actor universe or relax endpoint filters.

## Usage

Run from this folder:

```bash
python3 generate_puzzle_pool.py
```

This creates:
- puzzle_pool.json
- puzzle_pool.csv
- ../actor_path_game/puzzle_pool.js

### Example calls

1. Default run (recommended first pass)

```bash
python3 generate_puzzle_pool.py
```

2. Small, fast smoke test

```bash
python3 generate_puzzle_pool.py \
  --pool-size 100 \
  --seed 42
```

3. Bigger pool with wider endpoint ranks (more variety)

```bash
python3 generate_puzzle_pool.py \
  --pool-size 1000 \
  --top-actors 300 \
  --min-hops 1 \
  --max-hops 5 \
  --start-rank-max 180 \
  --end-rank-min 40 \
  --end-rank-max 300 \
  --min-degree 3 \
  --max-degree 180 \
  --seed 42
```

4. Harder-leaning pool (fewer trivial short links)

```bash
python3 generate_puzzle_pool.py \
  --pool-size 500 \
  --min-hops 3 \
  --max-hops 5 \
  --start-rank-max 220 \
  --end-rank-min 60 \
  --end-rank-max 300 \
  --min-degree 2 \
  --max-degree 120 \
  --seed 7
```

5. Write outputs to custom locations

```bash
python3 generate_puzzle_pool.py \
  --output-json ./out/puzzle_pool.json \
  --output-csv ./out/puzzle_pool.csv \
  --output-js ../actor_path_game/puzzle_pool.js
```

## Argument reference

- --compiled-csv (default: ../wikidata_movie_data/compiled_popularity.csv)
  - Input popularity file used to load actor metadata and rank.
- --movie-cast-csv (default: ../wikidata_movie_data/movie_cast_db.csv)
  - Input movie-to-cast edge file used to build the actor graph.
- --output-json (default: puzzle_pool.json)
  - JSON output consumed by tooling and QA.
- --output-csv (default: puzzle_pool.csv)
  - Flat CSV export for inspection.
- --output-js (default: ../actor_path_game/puzzle_pool.js)
  - Browser-ready JS bundle loaded by the game.

- --pool-size (default: 500)
  - Requested number of puzzles in the final pool.
- --top-actors (default: 300)
  - Keep actors with rank in [1, top-actors] from compiled_popularity.

- --min-hops (default: 1)
  - Minimum shortest-path actor hops for accepted puzzles.
- --max-hops (default: 5)
  - Maximum shortest-path actor hops for accepted puzzles.

- --start-rank-max (default: 100)
  - Start actor must have rank <= this value.
- --end-rank-min (default: 1)
  - End actor must have rank >= this value.
- --end-rank-max (default: 100)
  - End actor must have rank <= this value.

- --min-degree (default: 4)
  - Candidate endpoints must have at least this many graph neighbors.
- --max-degree (default: 140)
  - Candidate endpoints must have at most this many graph neighbors.

- --seed (default: 42)
  - Random seed for deterministic puzzle sampling.
- --max-tries (default: 400000)
  - Reserved compatibility parameter (currently parsed and stored in metadata).

### Parameter tuning tips

- To get more easy puzzles:
  - lower --min-hops
  - raise --start-rank-max and --end-rank-max toward top-ranked actors
  - raise --max-degree to allow hub actors

- To get more hard puzzles:
  - raise --min-hops (for example 3)
  - widen end rank range (for example --end-rank-max 300)
  - lower --max-degree to avoid ultra-connected hubs

- If generation returns fewer puzzles than requested:
  - lower --min-hops
  - widen rank windows
  - relax degree bounds

## Output schema summary

puzzle_pool.json stores:
- metadata
- puzzles[] where each puzzle has:
  - start actor (qid, name, rank)
  - end actor (qid, name, rank)
  - shortest_hops
  - difficulty
  - quality_score
  - path_metadata
    - box_office_total
    - box_office_known_edges
    - box_office_avg_known
  - canonical_path[]
    - actor at each step and movie_to_next label

Top-level metadata also includes `box_office_path_summary` with pool-level coverage and aggregate box office metrics.

## Runtime recommendation

In actor_path_game:
- load puzzle_pool.json once
- choose a random puzzle by difficulty bucket
- validate player moves against the same actor graph logic (or precomputed edge map)
- compare player hop count to shortest_hops for scoring
