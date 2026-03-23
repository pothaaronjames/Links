#!/usr/bin/env python3
"""Generate precomputed actor path puzzles for the Links game.

This script builds a graph from movie_cast_db.csv, constrained to actors listed in
compiled_popularity.csv (top 300 by default). It then samples start/end actors and
keeps only puzzles whose shortest path is within a bounded hop count.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import random
from collections import defaultdict, deque
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple


@dataclass
class ActorMeta:
    qid: str
    name: str
    rank: int
    pageviews_12mo: int


@dataclass
class Puzzle:
    puzzle_id: int
    difficulty: str
    shortest_hops: int
    quality_score: float
    start_qid: str
    end_qid: str
    path_qids: List[str]


@dataclass
class CandidatePuzzle:
    difficulty: str
    shortest_hops: int
    quality_score: float
    start_qid: str
    end_qid: str
    path_qids: List[str]


def load_compiled_popularity(path: Path, top_actors: int) -> Dict[str, ActorMeta]:
    actors: Dict[str, ActorMeta] = {}
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                rank = int((row.get("rank") or "0").strip())
            except ValueError:
                continue
            if rank < 1 or rank > top_actors:
                continue

            qid = (row.get("qid") or "").strip()
            name = (row.get("name") or "").strip()
            if not qid:
                continue
            if not name:
                name = qid

            try:
                pageviews = int((row.get("pageviews_12mo") or "0").strip())
            except ValueError:
                pageviews = 0

            actors[qid] = ActorMeta(qid=qid, name=name, rank=rank, pageviews_12mo=pageviews)
    return actors


def load_movie_cast_by_movie(path: Path, allowed_qids: Set[str], actors: Dict[str, ActorMeta]) -> Dict[str, Dict[str, str]]:
    movie_to_cast: Dict[str, dict] = defaultdict(lambda: {"cast": {}, "box_office": None})
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            movie_title = (row.get("movie_title") or "").strip()
            actor_qid = (row.get("actor_qid") or "").strip()
            actor_name = (row.get("actor_name") or "").strip()
            box_office_raw = (row.get("box_office") or "").strip()
            if not movie_title or not actor_qid:
                continue
            if actor_qid not in allowed_qids:
                continue
            canonical_name = actors.get(actor_qid).name if actor_qid in actors else actor_name or actor_qid
            movie_to_cast[movie_title]["cast"][actor_qid] = canonical_name

            if box_office_raw:
                try:
                    box_office_value = int(float(box_office_raw))
                except ValueError:
                    box_office_value = None
                if box_office_value is not None:
                    existing = movie_to_cast[movie_title].get("box_office")
                    if existing is None or box_office_value > existing:
                        movie_to_cast[movie_title]["box_office"] = box_office_value
    return movie_to_cast


def build_actor_graph(movie_to_cast: Dict[str, dict]) -> Dict[str, Dict[str, dict]]:
    adjacency: Dict[str, Dict[str, dict]] = defaultdict(dict)
    for movie_title, movie_data in movie_to_cast.items():
        cast_map = movie_data.get("cast", {})
        box_office = movie_data.get("box_office")
        cast_qids = sorted(cast_map.keys())
        for i in range(len(cast_qids)):
            a = cast_qids[i]
            adjacency[a]
            for j in range(i + 1, len(cast_qids)):
                b = cast_qids[j]
                existing = adjacency[a].get(b)
                should_replace = False
                if existing is None:
                    should_replace = True
                else:
                    existing_box_office = existing.get("box_office")
                    if box_office is not None and (existing_box_office is None or box_office > existing_box_office):
                        should_replace = True

                if should_replace:
                    edge_meta = {"movie": movie_title, "box_office": box_office}
                    adjacency[a][b] = edge_meta
                    adjacency[b][a] = edge_meta
    return adjacency


def reconstruct_path(parent: Dict[str, Optional[str]], end_qid: str) -> List[str]:
    path: List[str] = []
    cur: Optional[str] = end_qid
    while cur is not None:
        path.append(cur)
        cur = parent[cur]
    path.reverse()
    return path


def difficulty_from_hops(hops: int) -> str:
    if hops <= 2:
        return "easy"
    if hops == 3:
        return "medium"
    return "hard"


def compute_quality_score(
    path_qids: List[str],
    actors: Dict[str, ActorMeta],
    adjacency: Dict[str, Dict[str, dict]],
) -> float:
    hops = len(path_qids) - 1
    start_rank = actors[path_qids[0]].rank
    end_rank = actors[path_qids[-1]].rank
    rank_gap = abs(start_rank - end_rank)
    actor_count = max(len(actors), 1)

    endpoint_degree = len(adjacency[path_qids[0]]) + len(adjacency[path_qids[-1]])
    avg_endpoint_degree = endpoint_degree / 2.0
    avg_rank = (start_rank + end_rank) / 2.0

    edge_box_office_values: List[int] = []
    for i in range(len(path_qids) - 1):
        a = path_qids[i]
        b = path_qids[i + 1]
        edge_meta = adjacency[a][b]
        edge_box_office = edge_meta.get("box_office")
        if isinstance(edge_box_office, int) and edge_box_office > 0:
            edge_box_office_values.append(edge_box_office)

    # Score rewards simple paths and recognizable endpoints while avoiding super-trivial hub pairs.
    distance_term = min(hops, 5) / 5.0
    popularity_term = max(0.0, 1.0 - ((avg_rank - 1.0) / actor_count))
    balance_term = max(0.0, 1.0 - (rank_gap / 300.0))
    degree_term = max(0.0, 1.0 - (avg_endpoint_degree / 220.0))

    if hops > 0:
        known_ratio = len(edge_box_office_values) / hops
    else:
        known_ratio = 0.0
    if edge_box_office_values:
        avg_box_office = sum(edge_box_office_values) / len(edge_box_office_values)
        value_term = min(1.0, math.log1p(avg_box_office) / math.log1p(2_500_000_000))
    else:
        value_term = 0.0
    box_office_term = (0.6 * known_ratio) + (0.4 * value_term)

    return round(
        0.28 * distance_term
        + 0.24 * popularity_term
        + 0.14 * balance_term
        + 0.09 * degree_term
        + 0.25 * box_office_term,
        4,
    )


def create_path_steps(
    path_qids: List[str],
    actors: Dict[str, ActorMeta],
    adjacency: Dict[str, Dict[str, dict]],
) -> List[dict]:
    steps: List[dict] = []
    for idx, qid in enumerate(path_qids):
        next_movie = None
        if idx + 1 < len(path_qids):
            next_qid = path_qids[idx + 1]
            next_movie = adjacency[qid][next_qid]["movie"]
        steps.append(
            {
                "actor_qid": qid,
                "actor_name": actors[qid].name,
                "actor_rank": actors[qid].rank,
                "movie_to_next": next_movie,
            }
        )
    return steps


def compute_path_box_office_stats(
    path_qids: List[str],
    adjacency: Dict[str, Dict[str, dict]],
) -> Tuple[int, int, float]:
    total_box_office = 0
    known_edges = 0
    for i in range(len(path_qids) - 1):
        a = path_qids[i]
        b = path_qids[i + 1]
        edge_meta = adjacency[a][b]
        box_office = edge_meta.get("box_office")
        if isinstance(box_office, int) and box_office > 0:
            total_box_office += box_office
            known_edges += 1

    avg_known_box_office = (total_box_office / known_edges) if known_edges else 0.0
    return total_box_office, known_edges, avg_known_box_office


def enumerate_candidate_puzzles(
    actors: Dict[str, ActorMeta],
    adjacency: Dict[str, Dict[str, dict]],
    min_hops: int,
    max_hops: int,
    start_rank_max: int,
    end_rank_min: int,
    end_rank_max: int,
    min_degree: int,
    max_degree: int,
) -> Tuple[List[CandidatePuzzle], Dict[str, int]]:
    degrees = {qid: len(adjacency.get(qid, {})) for qid in actors.keys()}

    start_candidates = [
        qid
        for qid, meta in actors.items()
        if meta.rank <= start_rank_max and min_degree <= degrees.get(qid, 0) <= max_degree
    ]
    end_candidates = {
        qid
        for qid, meta in actors.items()
        if end_rank_min <= meta.rank <= end_rank_max and min_degree <= degrees.get(qid, 0) <= max_degree
    }

    if not start_candidates or not end_candidates:
        raise RuntimeError("No valid start/end candidates after rank and degree filtering")

    candidates: List[CandidatePuzzle] = []
    bucket_counts = {"easy": 0, "medium": 0, "hard": 0}
    pair_seen: Set[Tuple[str, str]] = set()

    for start_qid in start_candidates:
        queue: deque[str] = deque([start_qid])
        parent: Dict[str, Optional[str]] = {start_qid: None}
        depth: Dict[str, int] = {start_qid: 0}

        while queue:
            node = queue.popleft()
            node_depth = depth[node]
            if node_depth >= max_hops:
                continue

            neighbors = sorted(
                adjacency[node].keys(),
                key=lambda qid: (
                    adjacency[node][qid].get("box_office") is not None,
                    adjacency[node][qid].get("box_office") or 0,
                ),
                reverse=True,
            )
            for neighbor in neighbors:
                if neighbor in parent:
                    continue
                parent[neighbor] = node
                next_depth = node_depth + 1
                depth[neighbor] = next_depth
                queue.append(neighbor)

                if neighbor == start_qid or neighbor not in end_candidates:
                    continue
                if next_depth < min_hops or next_depth > max_hops:
                    continue

                pair_key = tuple(sorted((start_qid, neighbor)))
                if pair_key in pair_seen:
                    continue

                path_qids = reconstruct_path(parent, neighbor)
                difficulty = difficulty_from_hops(next_depth)
                quality = compute_quality_score(path_qids, actors, adjacency)
                min_quality = {"easy": 0.25, "medium": 0.30, "hard": 0.33}[difficulty]
                if quality < min_quality:
                    continue

                candidates.append(
                    CandidatePuzzle(
                        difficulty=difficulty,
                        shortest_hops=next_depth,
                        quality_score=quality,
                        start_qid=start_qid,
                        end_qid=neighbor,
                        path_qids=path_qids,
                    )
                )
                bucket_counts[difficulty] += 1
                pair_seen.add(pair_key)

    return candidates, bucket_counts


def select_puzzles_from_candidates(
    candidates: List[CandidatePuzzle],
    pool_size: int,
    seed: int,
) -> List[Puzzle]:
    rng = random.Random(seed)
    buckets: Dict[str, List[CandidatePuzzle]] = {"easy": [], "medium": [], "hard": []}
    for candidate in candidates:
        buckets[candidate.difficulty].append(candidate)

    for items in buckets.values():
        rng.shuffle(items)
    buckets["easy"].sort(
        key=lambda item: (
            item.shortest_hops,
            -item.quality_score,
        )
    )
    buckets["medium"].sort(
        key=lambda item: (
            item.quality_score,
            -max(item.shortest_hops, 0),
        ),
        reverse=True,
    )
    buckets["hard"].sort(
        key=lambda item: (
            item.quality_score,
            -max(item.shortest_hops, 0),
        ),
        reverse=True,
    )

    targets = {
        "easy": int(round(pool_size * 0.60)),
        "medium": int(round(pool_size * 0.35)),
        "hard": pool_size - int(round(pool_size * 0.60)) - int(round(pool_size * 0.35)),
    }

    selected: List[Puzzle] = []
    path_seen: Set[Tuple[str, ...]] = set()
    endpoint_uses: Dict[str, int] = defaultdict(int)
    max_endpoint_uses = max(8, pool_size // 6)

    def try_take_from_bucket(bucket_name: str, limit: int) -> None:
        for candidate in buckets[bucket_name]:
            if len(selected) >= pool_size or limit <= 0:
                return

            path_sig = tuple(candidate.path_qids)
            reverse_sig = tuple(reversed(candidate.path_qids))
            if path_sig in path_seen or reverse_sig in path_seen:
                continue
            if endpoint_uses[candidate.start_qid] >= max_endpoint_uses:
                continue
            if endpoint_uses[candidate.end_qid] >= max_endpoint_uses:
                continue

            selected.append(
                Puzzle(
                    puzzle_id=len(selected) + 1,
                    difficulty=candidate.difficulty,
                    shortest_hops=candidate.shortest_hops,
                    quality_score=candidate.quality_score,
                    start_qid=candidate.start_qid,
                    end_qid=candidate.end_qid,
                    path_qids=candidate.path_qids,
                )
            )
            path_seen.add(path_sig)
            endpoint_uses[candidate.start_qid] += 1
            endpoint_uses[candidate.end_qid] += 1
            limit -= 1

    for bucket_name in ("easy", "medium", "hard"):
        try_take_from_bucket(bucket_name, targets[bucket_name])

    if len(selected) < pool_size:
        remainder = []
        for bucket_name in ("medium", "easy", "hard"):
            remainder.extend(buckets[bucket_name])
        remainder.sort(key=lambda item: item.quality_score, reverse=True)

        for candidate in remainder:
            if len(selected) >= pool_size:
                break
            path_sig = tuple(candidate.path_qids)
            reverse_sig = tuple(reversed(candidate.path_qids))
            if path_sig in path_seen or reverse_sig in path_seen:
                continue
            if endpoint_uses[candidate.start_qid] >= max_endpoint_uses:
                continue
            if endpoint_uses[candidate.end_qid] >= max_endpoint_uses:
                continue

            selected.append(
                Puzzle(
                    puzzle_id=len(selected) + 1,
                    difficulty=candidate.difficulty,
                    shortest_hops=candidate.shortest_hops,
                    quality_score=candidate.quality_score,
                    start_qid=candidate.start_qid,
                    end_qid=candidate.end_qid,
                    path_qids=candidate.path_qids,
                )
            )
            path_seen.add(path_sig)
            endpoint_uses[candidate.start_qid] += 1
            endpoint_uses[candidate.end_qid] += 1

    return selected


def generate_puzzle_pool(
    actors: Dict[str, ActorMeta],
    adjacency: Dict[str, Dict[str, dict]],
    pool_size: int,
    min_hops: int,
    max_hops: int,
    start_rank_max: int,
    end_rank_min: int,
    end_rank_max: int,
    min_degree: int,
    max_degree: int,
    max_tries: int,
    seed: int,
) -> Tuple[List[Puzzle], Dict[str, int], Dict[str, int], int]:
    candidates, candidate_counts = enumerate_candidate_puzzles(
        actors=actors,
        adjacency=adjacency,
        min_hops=min_hops,
        max_hops=max_hops,
        start_rank_max=start_rank_max,
        end_rank_min=end_rank_min,
        end_rank_max=end_rank_max,
        min_degree=min_degree,
        max_degree=max_degree,
    )
    puzzles = select_puzzles_from_candidates(candidates, pool_size=pool_size, seed=seed)
    counts = {"easy": 0, "medium": 0, "hard": 0}
    for puzzle in puzzles:
        counts[puzzle.difficulty] += 1
    return puzzles, counts, candidate_counts, len(candidates)


def write_outputs(
    json_path: Path,
    csv_path: Path,
    js_path: Optional[Path],
    puzzles: List[Puzzle],
    actors: Dict[str, ActorMeta],
    adjacency: Dict[str, Dict[str, dict]],
    metadata: dict,
) -> None:
    json_rows: List[dict] = []
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "puzzle_id",
                "difficulty",
                "shortest_hops",
                "quality_score",
                "start_qid",
                "start_name",
                "start_rank",
                "end_qid",
                "end_name",
                "end_rank",
                "path_qids",
                "path_names",
                "path_movies",
                "path_box_office_total",
                "path_box_office_known_edges",
            ]
        )

        for puzzle in puzzles:
            path_names = [actors[qid].name for qid in puzzle.path_qids]
            path_movies: List[str] = []
            for i in range(len(puzzle.path_qids) - 1):
                a = puzzle.path_qids[i]
                b = puzzle.path_qids[i + 1]
                path_movies.append(adjacency[a][b]["movie"])

            path_box_office_total, path_box_office_known_edges, path_box_office_avg_known = compute_path_box_office_stats(
                puzzle.path_qids,
                adjacency,
            )

            writer.writerow(
                [
                    puzzle.puzzle_id,
                    puzzle.difficulty,
                    puzzle.shortest_hops,
                    puzzle.quality_score,
                    puzzle.start_qid,
                    actors[puzzle.start_qid].name,
                    actors[puzzle.start_qid].rank,
                    puzzle.end_qid,
                    actors[puzzle.end_qid].name,
                    actors[puzzle.end_qid].rank,
                    " -> ".join(puzzle.path_qids),
                    " -> ".join(path_names),
                    " -> ".join(path_movies),
                    path_box_office_total,
                    path_box_office_known_edges,
                ]
            )

            json_rows.append(
                {
                    "puzzle_id": puzzle.puzzle_id,
                    "difficulty": puzzle.difficulty,
                    "shortest_hops": puzzle.shortest_hops,
                    "quality_score": puzzle.quality_score,
                    "start": {
                        "qid": puzzle.start_qid,
                        "name": actors[puzzle.start_qid].name,
                        "rank": actors[puzzle.start_qid].rank,
                    },
                    "end": {
                        "qid": puzzle.end_qid,
                        "name": actors[puzzle.end_qid].name,
                        "rank": actors[puzzle.end_qid].rank,
                    },
                    "path_metadata": {
                        "box_office_total": path_box_office_total,
                        "box_office_known_edges": path_box_office_known_edges,
                        "box_office_avg_known": round(path_box_office_avg_known, 2),
                    },
                    "canonical_path": create_path_steps(puzzle.path_qids, actors, adjacency),
                }
            )

    payload = {
        "metadata": metadata,
        "puzzles": json_rows,
    }
    with json_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    if js_path is not None:
        js_path.write_text(
            "// Auto-generated by generate_puzzle_pool.py — do not edit manually.\n"
            "// eslint-disable-next-line no-unused-vars\n"
            f"const _GENERATED_PUZZLE_POOL = {json.dumps(payload, ensure_ascii=False, indent=2)};\n",
            encoding="utf-8",
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate precomputed actor path puzzle pool")
    parser.add_argument("--compiled-csv", default="../wikidata_movie_data/compiled_popularity.csv")
    parser.add_argument("--movie-cast-csv", default="../wikidata_movie_data/movie_cast_db.csv")
    parser.add_argument("--output-json", default="puzzle_pool.json")
    parser.add_argument("--output-csv", default="puzzle_pool.csv")
    parser.add_argument("--output-js", default="../actor_path_game/puzzle_pool.js")
    parser.add_argument("--pool-size", type=int, default=500)
    parser.add_argument("--top-actors", type=int, default=300)
    parser.add_argument("--min-hops", type=int, default=1)
    parser.add_argument("--max-hops", type=int, default=5)
    parser.add_argument("--start-rank-max", type=int, default=100)
    parser.add_argument("--end-rank-min", type=int, default=1)
    parser.add_argument("--end-rank-max", type=int, default=100)
    parser.add_argument("--min-degree", type=int, default=4)
    parser.add_argument("--max-degree", type=int, default=140)
    parser.add_argument("--max-tries", type=int, default=400000)
    parser.add_argument("--seed", type=int, default=42)
    return parser


def main() -> int:
    args = build_parser().parse_args()

    if args.top_actors != 300:
        print("Warning: this project currently assumes compiled_popularity has 300 rows")

    if args.min_hops < 1 or args.max_hops < args.min_hops:
        print("Invalid hop bounds")
        return 1

    compiled_path = Path(args.compiled_csv)
    cast_path = Path(args.movie_cast_csv)
    json_path = Path(args.output_json)
    csv_path = Path(args.output_csv)
    js_path = Path(args.output_js) if args.output_js else None

    actors = load_compiled_popularity(compiled_path, top_actors=args.top_actors)
    if len(actors) < 50:
        print(f"Too few actors loaded from {compiled_path}: {len(actors)}")
        return 1

    movie_to_cast = load_movie_cast_by_movie(cast_path, set(actors.keys()), actors)
    adjacency = build_actor_graph(movie_to_cast)

    puzzles, counts, candidate_counts, candidate_total = generate_puzzle_pool(
        actors=actors,
        adjacency=adjacency,
        pool_size=args.pool_size,
        min_hops=args.min_hops,
        max_hops=args.max_hops,
        start_rank_max=args.start_rank_max,
        end_rank_min=args.end_rank_min,
        end_rank_max=args.end_rank_max,
        min_degree=args.min_degree,
        max_degree=args.max_degree,
        max_tries=args.max_tries,
        seed=args.seed,
    )

    total_path_box_office = 0
    total_known_path_edges = 0
    total_path_edges = 0
    puzzles_with_known_box_office = 0
    for puzzle in puzzles:
        path_total, known_edges, _ = compute_path_box_office_stats(puzzle.path_qids, adjacency)
        total_path_box_office += path_total
        total_known_path_edges += known_edges
        total_path_edges += max(0, len(puzzle.path_qids) - 1)
        if known_edges > 0:
            puzzles_with_known_box_office += 1

    metadata = {
        "generator": "generate_puzzle_pool.py",
        "pool_size_requested": args.pool_size,
        "pool_size_created": len(puzzles),
        "difficulty_counts": counts,
        "candidate_counts": candidate_counts,
        "candidates_enumerated": candidate_total,
        "constraints": {
            "top_actors": args.top_actors,
            "min_hops": args.min_hops,
            "max_hops": args.max_hops,
            "start_rank_max": args.start_rank_max,
            "end_rank_min": args.end_rank_min,
            "end_rank_max": args.end_rank_max,
            "min_degree": args.min_degree,
            "max_degree": args.max_degree,
            "seed": args.seed,
        },
        "difficulty_targets": {
            "easy": int(round(args.pool_size * 0.60)),
            "medium": int(round(args.pool_size * 0.35)),
            "hard": args.pool_size - int(round(args.pool_size * 0.60)) - int(round(args.pool_size * 0.35)),
        },
        "box_office_path_summary": {
            "puzzles_with_known_box_office_edges": puzzles_with_known_box_office,
            "known_box_office_edge_coverage": round((total_known_path_edges / total_path_edges), 4) if total_path_edges else 0.0,
            "total_path_box_office": total_path_box_office,
            "avg_path_box_office": round((total_path_box_office / len(puzzles)), 2) if puzzles else 0.0,
            "avg_known_box_office_per_known_edge": round((total_path_box_office / total_known_path_edges), 2) if total_known_path_edges else 0.0,
        },
    }

    write_outputs(
        json_path=json_path,
        csv_path=csv_path,
        js_path=js_path,
        puzzles=puzzles,
        actors=actors,
        adjacency=adjacency,
        metadata=metadata,
    )

    print(f"Loaded actors: {len(actors)}")
    print(f"Graph nodes: {len(adjacency)}")
    print(f"Generated puzzles: {len(puzzles)} / {args.pool_size}")
    print(f"Difficulty counts: {counts}")
    print(f"Candidate counts: {candidate_counts}")
    print(f"Output JSON: {json_path}")
    print(f"Output CSV: {csv_path}")
    if js_path is not None:
        print(f"Output JS: {js_path}")

    if len(puzzles) < args.pool_size:
        print("Warning: could not reach requested pool size under current constraints")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
