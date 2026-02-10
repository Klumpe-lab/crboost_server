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
"""

from __future__ import annotations

import json
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd
import starfile


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------


def _resolve_path(p: str | Path, *, base: Path) -> Path:
    pp = Path(str(p))
    if pp.is_absolute():
        return pp
    return (base / pp).resolve()


def _find_optimisation_set_in_dir(d: Path) -> Path:
    direct = d / "optimisation_set.star"
    if direct.exists():
        return direct
    hits = sorted(d.glob("**/optimisation_set.star"), key=lambda p: len(p.parts))
    if not hits:
        raise FileNotFoundError(f"No optimisation_set.star found under: {d}")
    return hits[0]


def _parse_optimisation_set(opt_path: Path) -> Tuple[Path, Path]:
    """
    Parse optimisation_set.star.  Handles both:
      - RELION key-value format (data_ block with _rlnTomo... keys)
      - starfile-written loop format (single-row DataFrame)
    """
    base = opt_path.parent

    # Try starfile first -- handles both formats
    try:
        data = starfile.read(opt_path, always_dict=True)
        for block in data.values():
            if isinstance(block, pd.DataFrame):
                if {"rlnTomoParticlesFile", "rlnTomoTomogramsFile"}.issubset(block.columns):
                    particles = str(block["rlnTomoParticlesFile"].iloc[0])
                    tomograms = str(block["rlnTomoTomogramsFile"].iloc[0])
                    return (_resolve_path(particles, base=base), _resolve_path(tomograms, base=base))
            elif isinstance(block, dict):
                if "rlnTomoParticlesFile" in block and "rlnTomoTomogramsFile" in block:
                    return (
                        _resolve_path(block["rlnTomoParticlesFile"], base=base),
                        _resolve_path(block["rlnTomoTomogramsFile"], base=base),
                    )
    except Exception:
        pass

    # Fallback: manual line-by-line for edge cases starfile can't handle
    particles = None
    tomograms = None
    for raw in opt_path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or line.startswith("data_") or line.startswith("loop_"):
            continue
        if line.startswith("_rlnTomoParticlesFile"):
            parts = line.split(None, 1)
            if len(parts) == 2:
                particles = parts[1].strip()
        elif line.startswith("_rlnTomoTomogramsFile"):
            parts = line.split(None, 1)
            if len(parts) == 2:
                tomograms = parts[1].strip()

    if not particles or not tomograms:
        raise ValueError(
            f"Cannot parse optimisation_set.star (missing required keys): {opt_path}\n"
            f"Found particles={particles} tomograms={tomograms}"
        )
    return (_resolve_path(particles, base=base), _resolve_path(tomograms, base=base))


def _find_df_block(star_dict: Dict[str, Any], required_cols: Sequence[str]) -> pd.DataFrame:
    req = set(required_cols)
    for v in star_dict.values():
        if isinstance(v, pd.DataFrame) and req.issubset(set(v.columns)):
            return v
    raise KeyError(f"No DataFrame block with required columns: {sorted(req)}")


def _read_particles_star(particles_star: Path) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    """Returns (optics_df, particles_df, general_kv)."""
    d = starfile.read(particles_star, always_dict=True)

    optics_df = _find_df_block(d, required_cols=["rlnVoltage", "rlnSphericalAberration", "rlnAmplitudeContrast"])
    particles_df = _find_df_block(d, required_cols=["rlnTomoName", "rlnImageName", "rlnOpticsGroup"])

    general_kv: Dict[str, Any] = {}
    for v in d.values():
        if isinstance(v, dict) and "rlnTomoSubTomosAre2DStacks" in v:
            general_kv["rlnTomoSubTomosAre2DStacks"] = v["rlnTomoSubTomosAre2DStacks"]

    return optics_df.copy(), particles_df.copy(), general_kv


def _read_tomograms_star(tomograms_star: Path) -> pd.DataFrame:
    d = starfile.read(tomograms_star, always_dict=True)
    df = _find_df_block(d, required_cols=["rlnTomoName", "rlnTomoReconstructedTomogram"])
    return df.copy()


# ---------------------------------------------------------------------------
# STAR writers
# ---------------------------------------------------------------------------


def write_optimisation_set(path: Path, *, particles_star: Path, tomograms_star: Path) -> None:
    """Canonical writer for optimisation_set.star in RELION key-value format."""
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
    return str(x)


def _write_loop_block(f, block_name: str, df: pd.DataFrame) -> None:
    f.write("# version 50001\n\n")
    f.write(f"data_{block_name}\n\n")
    f.write("loop_\n")
    for i, col in enumerate(df.columns, 1):
        f.write(f"_{col} #{i}\n")
    for _, row in df.iterrows():
        vals = [_format_star_value(row[c]) for c in df.columns]
        f.write(" ".join(vals) + "\n")
    f.write("\n")


def _write_particles_star(
    out_path: Path, *, optics_df: pd.DataFrame, particles_df: pd.DataFrame, general_kv: Optional[Dict[str, Any]] = None
) -> None:
    general_kv = dict(general_kv or {})
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
            f.write(f"_{col} #{i}\n")
        for _, row in df.iterrows():
            vals = [_format_star_value(row[c]) for c in df.columns]
            f.write("\t".join(vals) + "\n")
        f.write("\n")


# ---------------------------------------------------------------------------
# Optics validation
# ---------------------------------------------------------------------------

# Hard requirements -- these MUST match across sources
CRITICAL_OPTICS_COLS = ["rlnVoltage", "rlnSphericalAberration", "rlnAmplitudeContrast", "rlnTomoTiltSeriesPixelSize"]

# Checked only if present in all sources
OPTIONAL_OPTICS_COLS = ["rlnImageDimensionality", "rlnTomoSubtomogramBinning", "rlnImagePixelSize", "rlnImageSize"]


# ---------------------------------------------------------------------------
# Source resolution
# ---------------------------------------------------------------------------


@dataclass
class SourceResolved:
    source_input: str
    optimisation_set: Path
    particles_star: Path
    tomograms_star: Path
    n_particles: int
    tomo_names: List[str]


def _resolve_source_to_optset(source: str) -> Path:
    """Resolve a source string (absolute path to dir or file) to an optimisation_set.star."""
    p = Path(source)
    if not p.exists():
        raise FileNotFoundError(f"Source not found: {source}")
    if p.is_dir():
        return _find_optimisation_set_in_dir(p)
    return p


# ---------------------------------------------------------------------------
# Main merge logic
# ---------------------------------------------------------------------------


def merge_optimisation_sets_into_jobdir(
    *, job_dir: Path, additional_sources: List[str], strict: bool = True
) -> Dict[str, Any]:
    """
    Merges the primary job_dir's optimisation_set.star with additional sources.

    Expects job_dir to already contain:
      - optimisation_set.star (from the extraction run)
      - particles.star
      - tomograms.star (or referenced by the optimisation_set)

    Writes merged outputs into job_dir, overwriting:
      - particles.star
      - tomograms.star
      - optimisation_set.star
      - merge_summary.json

    Creates backups before overwriting:
      - particles_primary.star
      - tomograms_primary.star
      - optimisation_set_primary.star

    Returns the summary dict.
    """
    job_dir = job_dir.resolve()

    primary_optset = job_dir / "optimisation_set.star"
    if not primary_optset.exists():
        raise FileNotFoundError(
            f"Primary optimisation_set.star missing in job dir: {job_dir}\nRun extraction first before merging."
        )

    if not additional_sources:
        raise ValueError("No additional_sources provided -- nothing to merge.")

    # ---- Back up primary outputs before we overwrite ----
    for name in ["particles.star", "tomograms.star", "optimisation_set.star"]:
        src = job_dir / name
        backup = job_dir / name.replace(".star", "_primary.star")
        if src.exists() and not backup.exists():
            shutil.copy2(src, backup)
            print(f"[MERGE] Backed up {name} -> {backup.name}")

    # ---- Collect all sources (primary + additional) ----
    all_optsets: List[Path] = [primary_optset.resolve()]
    for s in additional_sources:
        all_optsets.append(_resolve_source_to_optset(s))

    resolved_sources: List[SourceResolved] = []
    all_optics: List[pd.DataFrame] = []
    all_particles: List[pd.DataFrame] = []
    all_tomograms: List[pd.DataFrame] = []
    primary_general_kv: Dict[str, Any] = {}

    for opt in all_optsets:
        p_star, t_star = _parse_optimisation_set(opt)

        if not p_star.exists():
            raise FileNotFoundError(f"Missing particles.star referenced by {opt}: {p_star}")
        if not t_star.exists():
            raise FileNotFoundError(f"Missing tomograms.star referenced by {opt}: {t_star}")

        optics_df, particles_df, general_kv = _read_particles_star(p_star)
        tomos_df = _read_tomograms_star(t_star)

        if not primary_general_kv and general_kv:
            primary_general_kv = dict(general_kv)

        tomo_names = sorted(set(particles_df["rlnTomoName"].astype(str).tolist()))
        resolved_sources.append(
            SourceResolved(
                source_input=str(opt),
                optimisation_set=opt.resolve(),
                particles_star=p_star.resolve(),
                tomograms_star=t_star.resolve(),
                n_particles=len(particles_df),
                tomo_names=tomo_names,
            )
        )

        all_optics.append(optics_df)
        all_particles.append(particles_df)
        all_tomograms.append(tomos_df)

    # ---- Validate optics ----
    optics_merged = pd.concat(all_optics, ignore_index=True)

    if strict:
        # Critical columns must exist
        missing_critical = [c for c in CRITICAL_OPTICS_COLS if c not in optics_merged.columns]
        if missing_critical:
            raise KeyError(f"Optics table missing critical columns: {missing_critical}")

        # Check critical columns are identical across all rows
        sig = optics_merged[CRITICAL_OPTICS_COLS].astype(str)
        if len(sig.drop_duplicates()) > 1:
            raise ValueError(
                "Optics/acquisition mismatch across sources. "
                "Critical columns differ: check voltage, Cs, amplitude contrast, pixel size."
            )

        # Check optional columns only if present in ALL sources
        for col in OPTIONAL_OPTICS_COLS:
            if col in optics_merged.columns and optics_merged[col].nunique(dropna=False) > 1:
                print(f"[MERGE WARN] Optional optics column '{col}' varies across sources -- using primary value.")

    optics_merged = optics_merged.drop_duplicates().reset_index(drop=True)

    # ---- Merge tomograms with deduplication ----
    tomos_merged = pd.concat(all_tomograms, ignore_index=True)

    # Check for rlnTomoName duplicates and validate consistency
    tomo_consistency_col = "rlnTomoReconstructedTomogram"
    grouped = tomos_merged.groupby("rlnTomoName", sort=False)
    conflicts = []
    for name, group in grouped:
        if len(group) <= 1:
            continue
        # Same tomo name from multiple sources -- check that reconstruction path agrees
        if tomo_consistency_col in group.columns:
            unique_paths = group[tomo_consistency_col].nunique()
            if unique_paths > 1:
                paths_seen = group[tomo_consistency_col].unique().tolist()
                conflicts.append(f"{name}: {paths_seen}")

    if conflicts:
        raise ValueError(
            "Tomogram name conflict -- same rlnTomoName but different reconstruction paths:\n"
            + "\n".join(conflicts[:10])
            + ("\n..." if len(conflicts) > 10 else "")
        )

    # Safe to deduplicate
    n_before = len(tomos_merged)
    tomos_merged = tomos_merged.drop_duplicates(subset=["rlnTomoName"], keep="first").reset_index(drop=True)
    n_deduped = n_before - len(tomos_merged)
    if n_deduped > 0:
        print(f"[MERGE] Deduplicated {n_deduped} tomogram rows (same tomo, multiple sources)")

    # ---- Merge particles; validate tomo name references ----
    particles_merged = pd.concat(all_particles, ignore_index=True)

    tomo_set = set(tomos_merged["rlnTomoName"].astype(str))
    particle_tomos = set(particles_merged["rlnTomoName"].astype(str))
    missing_tomos = sorted(particle_tomos - tomo_set)
    if missing_tomos:
        raise ValueError(
            "Particles reference tomograms not in merged tomograms.star: "
            + ", ".join(missing_tomos[:20])
            + (" ..." if len(missing_tomos) > 20 else "")
        )

    # ---- Write outputs ----
    out_particles = (job_dir / "particles.star").resolve()
    out_tomograms = (job_dir / "tomograms.star").resolve()
    out_optset = (job_dir / "optimisation_set.star").resolve()
    out_summary = (job_dir / "merge_summary.json").resolve()

    _write_particles_star(
        out_particles, optics_df=optics_merged, particles_df=particles_merged, general_kv=primary_general_kv
    )
    _write_tomograms_star(out_tomograms, tomos_merged)
    write_optimisation_set(out_optset, particles_star=out_particles, tomograms_star=out_tomograms)

    # ---- Summary ----
    summary = {
        "merged_outputs": {
            "particles_star": str(out_particles),
            "tomograms_star": str(out_tomograms),
            "optimisation_set_star": str(out_optset),
        },
        "totals": {
            "n_sources": len(resolved_sources),
            "n_particles": len(particles_merged),
            "n_tomograms": len(tomos_merged),
            "n_tomograms_deduplicated": n_deduped,
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
    print(
        f"[MERGE] Done. {len(particles_merged)} particles, {len(tomos_merged)} tomograms from {len(resolved_sources)} sources."
    )

    return summary
