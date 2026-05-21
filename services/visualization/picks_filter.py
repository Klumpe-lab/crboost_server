"""Curator-driven filtering of subtomo-extracted particles.

The candidate-extract dashboard's subtomo gallery is the natural place to
keep/drop individual picks: the user can see each particle's cutout next to
the template + low-score noise references, and decide whether it looks
real. "Save picks" persists that decision as an alt-output file pair next to
the SUBTOMO_EXTRACTION job's canonical outputs:

    <subtomo_job_dir>/particles.star               (original; never touched)
    <subtomo_job_dir>/optimisation_set.star        (original; never touched)
    <subtomo_job_dir>/particles_filtered.star      (curator subset)
    <subtomo_job_dir>/optimisation_set_filtered.star  (points at filtered particles)

The filtered file is declared as a second OutputSlot on
SubtomoExtractionParams with `prefer_if_exists=True`, so downstream consumers
(ReconstructParticle, Class3D, …) automatically use it when present via the
existing path-resolution scoring — no driver changes needed.

Per-TS incrementally: a save only affects one TS at a time, but the filtered
file is a project-level artifact spanning all TSs in the subtomo job. The
save function reads the existing filtered file (if any) to preserve
previously-curated TSs, swaps in the freshly-computed kept rows for the
current TS, and writes back. Untouched TSs default to "all kept" (=
original-particles rows).

Mapping pick_idx (gallery, score-sorted candidate-extract order) → row in
subtomo's particles.star happens by Å-coord matching, mirroring the join
logic in subtomo_link.py: candidate-extract's rlnCenteredCoordinate{X,Y,Z}Angst
on its candidates.star → same columns on subtomo's particles.star, rounded
to one decimal.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

import pandas as pd

from services.visualization.subtomo_link import _coord_key, _read_subtomo_particles

logger = logging.getLogger(__name__)

OPTIMISATION_SET_NAME = "optimisation_set.star"
OPTIMISATION_SET_FILTERED_NAME = "optimisation_set_filtered.star"
PARTICLES_NAME = "particles.star"
PARTICLES_FILTERED_NAME = "particles_filtered.star"
# {ts_name: kept_count} for TS the user has explicitly curated. The filtered
# star can't distinguish "reviewed, kept all" from "never reviewed" (both have
# all rows), so this sidecar records review intent + kept count for the roster.
REVIEWED_SIDECAR_NAME = "curation_reviewed.json"


def _read_candidates_for_ts(candidates_star: Path, ts_name: str) -> Optional[pd.DataFrame]:
    """Read candidates.star, filter to one TS, sort by score-desc to match
    the gallery's pick_idx order. Returns None if the star is missing or
    the score column isn't present."""
    if not candidates_star.exists():
        return None
    try:
        import starfile

        data = starfile.read(candidates_star, always_dict=True)
    except Exception as e:
        logger.warning("Could not read candidates.star %s: %s", candidates_star, e)
        return None
    parts = data.get("particles")
    if parts is None:
        for v in data.values():
            if isinstance(v, pd.DataFrame) and "rlnTomoName" in v.columns:
                parts = v
                break
    if parts is None or not isinstance(parts, pd.DataFrame):
        return None
    for_ts = parts[parts["rlnTomoName"] == ts_name]
    # Sort score-desc to match write_picks_data's order in picks.json.
    # SCORE_COL_PRIORITY (preview_orchestrator) is (LCC, AutopickFigureOfMerit, ProbDist).
    for col in ("rlnLCCmax", "rlnAutopickFigureOfMerit", "rlnMaxValueProbDistribution"):
        if col in for_ts.columns:
            for_ts = for_ts.sort_values(col, ascending=False).reset_index(drop=True)
            break
    else:
        for_ts = for_ts.reset_index(drop=True)
    return for_ts


def derive_keep_state_for_ts(
    subtomo_job_dir: Path, candidate_extract_job_dir: Path, ts_name: str, n_picks_for_ts: int
) -> Optional[set[int]]:
    """Look up which pick_idx values are currently kept in the filtered file.

    Returns:
      - None if no filtered file exists (= all picks implicitly kept).
      - set[int] of pick_idx values (0-based, matching picks.json order) that
        are present in particles_filtered.star for this TS.

    A non-None empty set means "all picks dropped for this TS" — distinct
    from None. The two states encode different intents and persist differently
    (None = no curation yet; empty = explicitly dropped all).
    """
    filtered = subtomo_job_dir / PARTICLES_FILTERED_NAME
    if not filtered.exists():
        return None

    cands = _read_candidates_for_ts(candidate_extract_job_dir / "candidates.star", ts_name)
    if cands is None or cands.empty:
        return set()

    coord_cols = ("rlnCenteredCoordinateXAngst", "rlnCenteredCoordinateYAngst", "rlnCenteredCoordinateZAngst")
    if not all(c in cands.columns for c in coord_cols):
        # No Å coords on candidates → can't match. Treat as "no filter info".
        return None

    df_filt = _read_subtomo_particles(filtered, subtomo_job_dir)
    if df_filt is None:
        return None
    if not all(c in df_filt.columns for c in coord_cols):
        return None

    filt_for_ts = df_filt[df_filt["rlnTomoName"] == ts_name]
    if filt_for_ts.empty:
        return set()  # explicitly nothing kept for this TS

    kept_keys: set[tuple[int, int, int]] = set()
    for _, row in filt_for_ts.iterrows():
        try:
            kept_keys.add(_coord_key(row[coord_cols[0]], row[coord_cols[1]], row[coord_cols[2]]))
        except (TypeError, ValueError):
            continue

    kept_pick_indices: set[int] = set()
    n = min(n_picks_for_ts, len(cands))
    for i in range(n):
        row = cands.iloc[i]
        try:
            key = _coord_key(row[coord_cols[0]], row[coord_cols[1]], row[coord_cols[2]])
        except (TypeError, ValueError):
            continue
        if key in kept_keys:
            kept_pick_indices.add(i)
    return kept_pick_indices


def save_filtered_picks_for_ts(
    subtomo_job_dir: Path, candidate_extract_job_dir: Path, ts_name: str, kept_pick_indices: set[int]
) -> dict:
    """Write/update particles_filtered.star + optimisation_set_filtered.star.

    Semantics:
      - For the current TS: kept rows are the subtomo rows whose Å-coord
        matches a kept pick (by `kept_pick_indices` → candidates.star Å
        lookup → particles.star Å match).
      - For OTHER TSs: rows are preserved from any existing filtered file,
        OR from the original particles.star if no prior filter file exists
        (= effectively "all kept" for those TSs).

    Returns a small status dict: {"kept_for_ts": int, "total_kept": int,
    "filtered_path": str, "optset_path": str}.
    """
    orig_particles = subtomo_job_dir / PARTICLES_NAME
    if not orig_particles.exists():
        raise FileNotFoundError(f"Original subtomo particles.star not found: {orig_particles}")

    cands = _read_candidates_for_ts(candidate_extract_job_dir / "candidates.star", ts_name)
    if cands is None:
        raise RuntimeError(f"Could not read candidates for {ts_name} from {candidate_extract_job_dir}")

    coord_cols = ("rlnCenteredCoordinateXAngst", "rlnCenteredCoordinateYAngst", "rlnCenteredCoordinateZAngst")
    kept_keys: set[tuple[int, int, int]] = set()
    for i in sorted(kept_pick_indices):
        if i < 0 or i >= len(cands):
            continue
        row = cands.iloc[i]
        try:
            kept_keys.add(_coord_key(row[coord_cols[0]], row[coord_cols[1]], row[coord_cols[2]]))
        except (TypeError, ValueError):
            continue

    import starfile

    # Source for OTHER TSs: existing filtered (preserve prior curation) else original.
    existing_filtered = subtomo_job_dir / PARTICLES_FILTERED_NAME
    source_for_others = existing_filtered if existing_filtered.exists() else orig_particles

    df_others_src = _read_subtomo_particles(source_for_others, subtomo_job_dir)
    if df_others_src is None:
        raise RuntimeError(f"Could not read subtomo particles at {source_for_others}")
    df_other_ts = df_others_src[df_others_src["rlnTomoName"] != ts_name]

    # Source for THIS TS: always original — re-filter from scratch every save
    # so a prior accidental keep/drop doesn't bleed forward.
    df_orig = _read_subtomo_particles(orig_particles, subtomo_job_dir)
    if df_orig is None:
        raise RuntimeError(f"Could not read original subtomo particles at {orig_particles}")
    df_this_ts = df_orig[df_orig["rlnTomoName"] == ts_name]

    if not all(c in df_this_ts.columns for c in coord_cols):
        raise RuntimeError(f"Subtomo particles missing Å coord columns: {coord_cols}")

    def _row_in_kept(r) -> bool:
        try:
            return _coord_key(r[coord_cols[0]], r[coord_cols[1]], r[coord_cols[2]]) in kept_keys
        except (TypeError, ValueError, KeyError):
            return False

    df_this_ts_kept = df_this_ts[df_this_ts.apply(_row_in_kept, axis=1)]

    df_combined = pd.concat([df_other_ts, df_this_ts_kept], ignore_index=True)

    # Preserve the original optics + particles blocks structure — overwrite
    # only the particles table. Other blocks (data_optics, data_general, …)
    # remain identical to the source.
    orig_data = starfile.read(orig_particles, always_dict=True)
    if "particles" in orig_data:
        orig_data["particles"] = df_combined
    else:
        # Single-block .star — find the particles-like block and swap.
        for k, v in list(orig_data.items()):
            if isinstance(v, pd.DataFrame) and "rlnTomoName" in v.columns:
                orig_data[k] = df_combined
                break

    out_particles = subtomo_job_dir / PARTICLES_FILTERED_NAME
    starfile.write(orig_data, out_particles, overwrite=True)

    # optimisation_set.star is a tiny RELION key-value star (one block, one
    # row per key like `_rlnTomoParticlesFile particles.star`). Rewriting it
    # textually is safer than going through starfile (which is fussy about
    # key-value form). Replace only the value on the rlnTomoParticlesFile
    # line so we don't accidentally rewrite a path that happens to contain
    # the substring "particles.star".
    orig_opt = subtomo_job_dir / OPTIMISATION_SET_NAME
    if not orig_opt.exists():
        raise FileNotFoundError(f"Original optimisation_set.star not found: {orig_opt}")
    out_opt = subtomo_job_dir / OPTIMISATION_SET_FILTERED_NAME
    out_lines: list[str] = []
    swapped = False
    for line in orig_opt.read_text().splitlines():
        stripped = line.lstrip()
        if stripped.startswith("_rlnTomoParticlesFile"):
            # Preserve any leading whitespace + the key, replace only the value.
            indent = line[: len(line) - len(stripped)]
            parts = stripped.split(None, 1)
            if len(parts) == 2:
                out_lines.append(f"{indent}{parts[0]} {PARTICLES_FILTERED_NAME}")
                swapped = True
                continue
        out_lines.append(line)
    if not swapped:
        raise RuntimeError(
            f"optimisation_set.star at {orig_opt} has no _rlnTomoParticlesFile line — "
            "filtered set would not be loaded by downstream consumers"
        )
    out_opt.write_text("\n".join(out_lines) + "\n")

    # Mark this TS reviewed (+ record kept count) for the roster's review column.
    _update_reviewed_sidecar(subtomo_job_dir, ts_name, int(len(df_this_ts_kept)))

    return {
        "kept_for_ts": int(len(df_this_ts_kept)),
        "total_kept": int(len(df_combined)),
        "filtered_path": str(out_particles),
        "optset_path": str(out_opt),
    }


def resolve_canonical_optset(subtomo_job_dir: Path) -> Path:
    """The optimisation_set a downstream consumer should read: the curated
    `_filtered` one if it exists, else the original. Single definition of
    "which star is canonical", shared by the IO-slot resolver's intent and by
    cross-project aggregation (PICKS_FILTER_AGGREGATION_ROADMAP.md §factor out)."""
    filtered = subtomo_job_dir / OPTIMISATION_SET_FILTERED_NAME
    if filtered.exists():
        return filtered
    return subtomo_job_dir / OPTIMISATION_SET_NAME


def has_filtered_set(subtomo_job_dir: Path) -> bool:
    """True if a curated particles_filtered.star exists for this subtomo job."""
    return (subtomo_job_dir / PARTICLES_FILTERED_NAME).exists()


def read_reviewed_counts(subtomo_job_dir: Path) -> dict[str, int]:
    """{ts_name: kept_count} for TS the user has curated in this subtomo job.
    Empty when nothing has been reviewed (no sidecar)."""
    p = subtomo_job_dir / REVIEWED_SIDECAR_NAME
    if not p.exists():
        return {}
    try:
        data = json.loads(p.read_text())
    except (OSError, ValueError):
        return {}
    if not isinstance(data, dict):
        return {}
    out: dict[str, int] = {}
    for k, v in data.items():
        try:
            out[str(k)] = int(v)
        except (TypeError, ValueError):
            continue
    return out


def _update_reviewed_sidecar(subtomo_job_dir: Path, ts_name: str, kept_count: int) -> None:
    data = read_reviewed_counts(subtomo_job_dir)
    data[ts_name] = kept_count
    try:
        (subtomo_job_dir / REVIEWED_SIDECAR_NAME).write_text(json.dumps(data))
    except OSError as e:
        logger.warning("Could not write review sidecar in %s: %s", subtomo_job_dir, e)


def discard_filter(subtomo_job_dir: Path) -> bool:
    """Delete the filtered file pair, reverting downstream consumers to the
    original. Returns True if at least one file was removed.

    This is the WHOLE-species reset (every TS). For reverting a single
    tilt-series while keeping the rest curated, use `discard_ts_filter`."""
    removed = False
    for name in (PARTICLES_FILTERED_NAME, OPTIMISATION_SET_FILTERED_NAME, REVIEWED_SIDECAR_NAME):
        p = subtomo_job_dir / name
        if p.exists():
            try:
                p.unlink()
                removed = True
            except OSError as e:
                logger.warning("Failed to remove %s: %s", p, e)
    return removed


def discard_ts_filter(subtomo_job_dir: Path, ts_name: str) -> str:
    """Revert ONE tilt-series to its original (all-kept) picks while leaving
    every other TS's curation intact — the per-tomogram counterpart to
    `discard_filter`.

    If removing this TS leaves nothing curated, the whole filtered pair is
    deleted (a clean full revert). Returns:
      - "noop"        : no filtered set exists, nothing to do
      - "removed_all" : no curation remained → filtered pair deleted
      - "reverted_ts" : this TS reset to original, other TSs still curated
    """
    filtered = subtomo_job_dir / PARTICLES_FILTERED_NAME
    if not filtered.exists():
        return "noop"

    # Drop this TS from the review sidecar. If nothing remains reviewed, the
    # filtered set no longer represents any curation → remove it wholesale.
    reviewed = read_reviewed_counts(subtomo_job_dir)
    reviewed.pop(ts_name, None)
    if not reviewed:
        discard_filter(subtomo_job_dir)
        return "removed_all"

    orig_particles = subtomo_job_dir / PARTICLES_NAME
    if not orig_particles.exists():
        raise FileNotFoundError(f"Original subtomo particles.star not found: {orig_particles}")

    df_filt = _read_subtomo_particles(filtered, subtomo_job_dir)
    df_orig = _read_subtomo_particles(orig_particles, subtomo_job_dir)
    if df_filt is None or df_orig is None:
        raise RuntimeError("Could not read particles for per-TS discard")

    # Keep other TSs as-curated; replace this TS's rows with its full original set.
    df_other = df_filt[df_filt["rlnTomoName"] != ts_name]
    df_this_orig = df_orig[df_orig["rlnTomoName"] == ts_name]
    df_combined = pd.concat([df_other, df_this_orig], ignore_index=True)

    import starfile

    orig_data = starfile.read(orig_particles, always_dict=True)
    if "particles" in orig_data:
        orig_data["particles"] = df_combined
    else:
        for k, v in list(orig_data.items()):
            if isinstance(v, pd.DataFrame) and "rlnTomoName" in v.columns:
                orig_data[k] = df_combined
                break
    starfile.write(orig_data, filtered, overwrite=True)

    # Persist the trimmed sidecar (optimisation_set_filtered.star already points
    # at particles_filtered.star and is unchanged).
    try:
        (subtomo_job_dir / REVIEWED_SIDECAR_NAME).write_text(json.dumps(reviewed))
    except OSError as e:
        logger.warning("Could not write review sidecar in %s: %s", subtomo_job_dir, e)
    return "reverted_ts"


# NOTE: `find_subtomo_job_dir_for_cutouts` (lex-greatest-subtomo-job heuristic)
# was removed 2026-05-21 — it mis-targeted every species at one subtomo job.
# The gallery now passes the species-matched subtomo_job_dir directly (resolved
# via _matching_subtomo_instance in the dashboard). See
# PICKS_FILTER_AGGREGATION_ROADMAP.md.
