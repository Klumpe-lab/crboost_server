#!/usr/bin/env python3
"""
Auxiliary merge logic for STA subtomo extraction outputs.

Input: optimisation_set.star files (or directories containing one)
Each optimisation_set.star points to:
  - rlnTomoParticlesFile -> particles.star
  - rlnTomoTomogramsFile -> tomograms.star

Output (written into a target job_dir):
  - particles.star (merged)
  - tomograms.star (merged)
  - optimisation_set.star (rewritten to point at merged outputs)
  - merge_summary.json (UI-friendly summary)

By default, enforces:
  - identical core optics/acquisition fields across sources
  - unique rlnTomoName across tomograms tables
  - all particle rlnTomoName entries must exist in merged tomograms
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd
import starfile


# ----------------------------
# Small parsing helpers
# ----------------------------

def _resolve_path(p: str | Path, *, base: Path) -> Path:
    pp = Path(str(p))
    if pp.is_absolute():
        return pp
    return (base / pp).resolve()


def _find_optimisation_set_in_dir(d: Path) -> Path:
    """
    Find optimisation_set.star within a job directory.
    We prefer exact match in the directory root; otherwise search shallowly.
    """
    direct = d / "optimisation_set.star"
    if direct.exists():
        return direct

    # Common layout: External/jobXXX/optimisation_set.star already is the root, but just in case:
    hits = list(d.glob("**/optimisation_set.star"))
    if not hits:
        raise FileNotFoundError(f"No optimisation_set.star found under: {d}")
    # Choose the shortest path (closest to root)
    hits = sorted(hits, key=lambda p: len(p.parts))
    return hits[0]


def _parse_optimisation_set(opt_path: Path) -> Tuple[Path, Path]:
    """
    Robust minimal parser for optimisation_set.star in key-value format:

    data_
    _rlnTomoParticlesFile <path>
    _rlnTomoTomogramsFile <path>
    """
    particles = None
    tomograms = None

    for raw in opt_path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("_rlnTomoParticlesFile"):
            particles = line.split(None, 1)[1].strip()
        elif line.startswith("_rlnTomoTomogramsFile"):
            tomograms = line.split(None, 1)[1].strip()

    if not particles or not tomograms:
        raise ValueError(
            f"Invalid optimisation_set.star (missing required keys): {opt_path}\n"
            f"Found particles={particles} tomograms={tomograms}"
        )

    base = opt_path.parent
    return (_resolve_path(particles, base=base), _resolve_path(tomograms, base=base))


def _read_star_as_dict(path: Path) -> Dict[str, Any]:
    return starfile.read(path, always_dict=True)


def _find_df_block(star_dict: Dict[str, Any], required_cols: Sequence[str]) -> pd.DataFrame:
    req = set(required_cols)
    for _, v in star_dict.items():
        if isinstance(v, pd.DataFrame) and req.issubset(set(v.columns)):
            return v
    raise KeyError(f"Could not find a DataFrame block with required columns: {sorted(req)}")


def _read_particles_star(particles_star: Path) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    """
    Returns: (optics_df, particles_df, general_kv)
    general_kv is optional and may be empty if not present/parseable.
    """
    d = _read_star_as_dict(particles_star)

    optics_df = _find_df_block(d, required_cols=["rlnVoltage", "rlnSphericalAberration", "rlnAmplitudeContrast"])
    particles_df = _find_df_block(d, required_cols=["rlnTomoName", "rlnImageName", "rlnOpticsGroup"])

    # Try to preserve data_general key-values if starfile gave them to us in some form.
    # If not available, we'll synthesize the most important one later.
    general_kv: Dict[str, Any] = {}
    for k, v in d.items():
        # starfile sometimes parses non-loop blocks as dict-like; this is best-effort.
        if isinstance(v, dict):
            # Heuristic: only keep rlnTomoSubTomosAre2DStacks if present
            if "rlnTomoSubTomosAre2DStacks" in v:
                general_kv["rlnTomoSubTomosAre2DStacks"] = v["rlnTomoSubTomosAre2DStacks"]

    return optics_df.copy(), particles_df.copy(), general_kv


def _read_tomograms_star(tomograms_star: Path) -> pd.DataFrame:
    d = _read_star_as_dict(tomograms_star)
    # Your tomograms.star is data_global with a loop, so it should be a DF block.
    df = _find_df_block(d, required_cols=["rlnTomoName", "rlnTomoReconstructedTomogram"])
    return df.copy()


def _write_optimisation_set(path: Path, *, particles_star: Path, tomograms_star: Path) -> None:
    txt = "\n".join(
        [
            "# version 50001",
            "",
            "data_",
            "",
            f"_rlnTomoParticlesFile            {str(particles_star.resolve())}",
            f"_rlnTomoTomogramsFile            {str(tomograms_star.resolve())}",
            "",
        ]
    )
    path.write_text(txt)


def _format_star_value(x: Any) -> str:
    if pd.isna(x):
        return ""
    # keep list-like strings as-is
    s = str(x)
    # STAR is whitespace-delimited; paths and tokens here don't contain spaces in your use-case.
    # If you ever do hit spaces, you'll want to quote them.
    return s


def _write_loop_block(f, block_name: str, df: pd.DataFrame) -> None:
    f.write("# version 50001\n\n")
    f.write(f"data_{block_name}\n\n")
    f.write("loop_\n")
    for i, col in enumerate(df.columns, 1):
        # Ensure STAR tag format
        tag = col if col.startswith("rln") else col
        f.write(f"_{tag} #{i}\n")
    for _, row in df.iterrows():
        vals = [_format_star_value(row[c]) for c in df.columns]
        f.write(" ".join(vals) + "\n")
    f.write("\n")


def _write_particles_star(
    out_path: Path,
    *,
    optics_df: pd.DataFrame,
    particles_df: pd.DataFrame,
    general_kv: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Write a RELION5-ish particles.star containing:
      - data_general (best-effort)
      - data_optics (loop)
      - data_particles (loop)
    """
    general_kv = dict(general_kv or {})
    # This one is important for 2D stack subtomos; default to 1
    general_kv.setdefault("rlnTomoSubTomosAre2DStacks", 1)

    with open(out_path, "w") as f:
        f.write("# version 50001\n\n")
        f.write("data_general\n\n")
        for k, v in general_kv.items():
            f.write(f"_{k}                       {v}\n")
        f.write("\n\n")

        _write_loop_block(f, "optics", optics_df)
        _write_loop_block(f, "particles", particles_df)


def _write_tomograms_star(out_path: Path, df: pd.DataFrame) -> None:
    with open(out_path, "w") as f:
        f.write("# version 50001\n\n")
        f.write("data_global\n\n")
        f.write("loop_\n")
        for i, col in enumerate(df.columns, 1):
            tag = col if col.startswith("rln") else col
            f.write(f"_{tag} #{i}\n")
        for _, row in df.iterrows():
            vals = [_format_star_value(row[c]) for c in df.columns]
            f.write("\t".join(vals) + "\n")
        f.write("\n")


# ----------------------------
# Merge logic
# ----------------------------

CORE_OPTICS_COLS = [
    "rlnVoltage",
    "rlnSphericalAberration",
    "rlnAmplitudeContrast",
    "rlnTomoTiltSeriesPixelSize",
    "rlnImageDimensionality",
    "rlnTomoSubtomogramBinning",
    "rlnImagePixelSize",
    "rlnImageSize",
]


@dataclass
class SourceResolved:
    source_input: str
    optimisation_set: Path
    particles_star: Path
    tomograms_star: Path
    n_particles: int
    tomo_names: List[str]


def _resolve_source_to_optset(source: str, *, project_root: Path) -> Path:
    p = Path(source)
    if not p.is_absolute():
        p = (project_root / p).resolve()

    if p.is_dir():
        return _find_optimisation_set_in_dir(p)

    if p.is_file():
        # allow either direct optimisation_set.star path or something else (we only accept optset here)
        if p.name != "optimisation_set.star":
            # if user passed a file, but it's not named as such, still accept if it looks like one
            return p
        return p

    raise FileNotFoundError(f"Source not found: {source} (resolved to {p})")


def ensure_local_primary_tomograms_and_optset(
    *,
    job_dir: Path,
    primary_input_optimisation_set: Path,
    project_root: Path,
) -> Path:
    """
    Ensures this job_dir contains:
      - tomograms.star (copied from the input optimisation_set's tomograms file)
      - optimisation_set.star (rewritten to point to *local* job_dir particles/tomograms)
    Returns: path to job_dir/optimisation_set.star
    """
    job_dir.mkdir(parents=True, exist_ok=True)

    # Where RELION wrote (or will write) the local particles:
    local_particles = (job_dir / "particles.star").resolve()
    local_tomograms = (job_dir / "tomograms.star").resolve()
    local_optset = (job_dir / "optimisation_set.star").resolve()

    # Resolve upstream tomograms path and stage a local copy (so merge produces self-contained outputs)
    _, upstream_tomos = _parse_optimisation_set(primary_input_optimisation_set)

    if not local_tomograms.exists():
        if not upstream_tomos.exists():
            raise FileNotFoundError(
                f"Primary optimisation_set references missing tomograms.star:\n  {upstream_tomos}"
            )
        # copy tomograms.star locally
        local_tomograms.parent.mkdir(parents=True, exist_ok=True)
        local_tomograms.write_text(upstream_tomos.read_text())

    # Always (re)write local optimisation_set.star to point to local outputs.
    # This is intentionally "override-friendly" for your minimal working system.
    _write_optimisation_set(local_optset, particles_star=local_particles, tomograms_star=local_tomograms)

    return local_optset


def merge_optimisation_sets_into_jobdir(
    *,
    job_dir: Path,
    primary_local_optimisation_set: Path,
    additional_sources: List[str],
    project_root: Path,
    strict: bool = True,
    overwrite: bool = True,
) -> None:
    """
    Merges:
      - primary_local_optimisation_set (must be in job_dir; points to job_dir particles/tomograms)
      - each item in additional_sources (path to optset.star or a job directory)

    Writes merged outputs into job_dir, overwriting:
      - job_dir/particles.star
      - job_dir/tomograms.star
      - job_dir/optimisation_set.star
      - job_dir/merge_summary.json
    """
    job_dir = job_dir.resolve()
    if not primary_local_optimisation_set.exists():
        raise FileNotFoundError(f"Primary local optimisation_set missing: {primary_local_optimisation_set}")

    # Resolve all sources to concrete optimisation_set.star paths
    optsets: List[Path] = [primary_local_optimisation_set.resolve()]
    for s in additional_sources:
        optsets.append(_resolve_source_to_optset(s, project_root=project_root))

    # Resolve each optset -> particles/tomograms
    resolved_sources: List[SourceResolved] = []
    all_optics: List[pd.DataFrame] = []
    all_particles: List[pd.DataFrame] = []
    all_tomograms: List[pd.DataFrame] = []
    primary_general_kv: Dict[str, Any] = {}

    for opt in optsets:
        p_star, t_star = _parse_optimisation_set(opt)
        if not p_star.exists():
            raise FileNotFoundError(f"Missing particles.star referenced by {opt}: {p_star}")
        if not t_star.exists():
            raise FileNotFoundError(f"Missing tomograms.star referenced by {opt}: {t_star}")

        optics_df, particles_df, general_kv = _read_particles_star(p_star)
        tomos_df = _read_tomograms_star(t_star)

        if not primary_general_kv and general_kv:
            primary_general_kv = dict(general_kv)

        # Basic per-source stats
        tomo_names = sorted(set(particles_df["rlnTomoName"].astype(str).tolist()))
        resolved_sources.append(
            SourceResolved(
                source_input=str(opt),
                optimisation_set=opt.resolve(),
                particles_star=p_star.resolve(),
                tomograms_star=t_star.resolve(),
                n_particles=int(len(particles_df)),
                tomo_names=tomo_names,
            )
        )

        all_optics.append(optics_df)
        all_particles.append(particles_df)
        all_tomograms.append(tomos_df)

    # ---------------------------------------
    # Validate / Merge optics
    # ---------------------------------------
    optics_merged = pd.concat(all_optics, ignore_index=True)

    # If strict: enforce that "core optics" are identical across all sources (per optics row signature)
    if strict:
        missing = [c for c in CORE_OPTICS_COLS if c not in optics_merged.columns]
        if missing:
            raise KeyError(f"Optics table missing required columns: {missing}")

        sig = optics_merged[CORE_OPTICS_COLS].astype(str)
        unique_sigs = sig.drop_duplicates()
        if len(unique_sigs) > 1:
            raise ValueError(
                "Optics/acquisition mismatch across sources (core optics columns differ). "
                f"Unique signatures: {len(unique_sigs)}"
            )

    optics_merged = optics_merged.drop_duplicates().reset_index(drop=True)

    # ---------------------------------------
    # Merge tomograms and enforce unique rlnTomoName
    # ---------------------------------------
    tomos_merged = pd.concat(all_tomograms, ignore_index=True)
    if "rlnTomoName" not in tomos_merged.columns:
        raise KeyError("tomograms.star missing rlnTomoName")

    # Duplicate tomo names are hard error (they break particle->tomo mapping)
    name_counts = tomos_merged["rlnTomoName"].astype(str).value_counts()
    dupes = name_counts[name_counts > 1]
    if len(dupes) > 0:
        raise ValueError(f"Duplicate rlnTomoName across sources: {dupes.to_dict()}")

    # ---------------------------------------
    # Merge particles; validate their tomo names exist
    # ---------------------------------------
    particles_merged = pd.concat(all_particles, ignore_index=True)
    if "rlnTomoName" not in particles_merged.columns:
        raise KeyError("particles.star missing rlnTomoName")

    tomo_set = set(tomos_merged["rlnTomoName"].astype(str).tolist())
    particle_tomos = set(particles_merged["rlnTomoName"].astype(str).tolist())
    missing_tomos = sorted(particle_tomos - tomo_set)
    if missing_tomos:
        raise ValueError(
            "Some particles reference tomograms not present in merged tomograms.star: "
            + ", ".join(missing_tomos[:20])
            + (" ..." if len(missing_tomos) > 20 else "")
        )

    # ---------------------------------------
    # Write outputs (in-place overwrite)
    # ---------------------------------------
    out_particles = (job_dir / "particles.star").resolve()
    out_tomograms = (job_dir / "tomograms.star").resolve()
    out_optset = (job_dir / "optimisation_set.star").resolve()
    out_summary = (job_dir / "merge_summary.json").resolve()

    if not overwrite:
        for p in [out_particles, out_tomograms, out_optset, out_summary]:
            if p.exists():
                raise FileExistsError(f"Refusing to overwrite existing file: {p}")

    _write_particles_star(out_particles, optics_df=optics_merged, particles_df=particles_merged, general_kv=primary_general_kv)
    _write_tomograms_star(out_tomograms, tomos_merged)
    _write_optimisation_set(out_optset, particles_star=out_particles, tomograms_star=out_tomograms)

    # ---------------------------------------
    # Summary for UI
    # ---------------------------------------
    summary = {
        "merged_outputs": {
            "particles_star": str(out_particles),
            "tomograms_star": str(out_tomograms),
            "optimisation_set_star": str(out_optset),
        },
        "totals": {
            "n_sources": len(resolved_sources),
            "n_particles": int(len(particles_merged)),
            "n_tomograms": int(len(tomos_merged)),
            "tomo_names": sorted(tomos_merged["rlnTomoName"].astype(str).tolist()),
        },
        "sources": [
            {
                "source_input": s.source_input,
                "optimisation_set": str(s.optimisation_set),
                "particles_star": str(s.particles_star),
                "tomograms_star": str(s.tomograms_star),
                "n_particles": s.n_particles,
                "tomo_names": s.tomo_names,
            }
            for s in resolved_sources
        ],
    }
    out_summary.write_text(json.dumps(summary, indent=2))
