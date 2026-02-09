# services/sta_merge_service.py
from __future__ import annotations

import json
import re
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import starfile


# -----------------------------
# Data models for UI / manifest
# -----------------------------

@dataclass
class SourceSummary:
    source_spec: str                  # what user typed/selected
    optimisation_set: str            # resolved path
    particles_star: str              # resolved path
    tomograms_star: str              # resolved path
    n_particles: int
    tomogram_names: List[str]

@dataclass
class MergeSummary:
    ok: bool
    out_dir: str
    out_particles_star: str
    out_tomograms_star: str
    out_optimisation_set: str
    total_particles: int
    total_tomograms: int
    tomogram_names: List[str]
    sources: List[SourceSummary]
    warnings: List[str]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# -----------------------------
# Optimisation-set parsing/writing
# -----------------------------

_OPT_KV_RE = re.compile(r"^(_rlnTomoParticlesFile|_rlnTomoTomogramsFile)\s+(.+?)\s*$")

def _resolve_path_maybe_relative(p: str, base_dir: Path) -> Path:
    pp = Path(p.strip())
    return pp if pp.is_absolute() else (base_dir / pp).resolve()

def resolve_optimisation_set_path(spec: str, *, project_root: Optional[Path] = None) -> Path:
    """
    spec can be:
      - "/abs/path/to/optimisation_set.star"
      - "External/job013" (dir containing optimisation_set.star)
      - "External/job013/optimisation_set.star"
      - relative paths (resolved against project_root if provided, else CWD)
    """
    s = spec.strip()

    p = Path(s)
    if not p.is_absolute():
        if project_root is not None:
            p = (project_root / p).resolve()
        else:
            p = p.resolve()

    if p.is_dir():
        cand = p / "optimisation_set.star"
        if not cand.exists():
            raise FileNotFoundError(f"No optimisation_set.star in directory: {p}")
        return cand.resolve()

    if p.is_file():
        return p.resolve()

    raise FileNotFoundError(f"Could not resolve optimisation_set spec: {spec} -> {p}")

def read_optimisation_set_kv(opt_path: Path) -> Tuple[Path, Path]:
    """
    Reads the key-value style optimisation_set.star:
      data_
      _rlnTomoParticlesFile <path>
      _rlnTomoTomogramsFile <path>
    Returns absolute paths (resolving relative to opt file dir).
    """
    text = opt_path.read_text().splitlines()
    kv: Dict[str, str] = {}
    for line in text:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        m = _OPT_KV_RE.match(line)
        if not m:
            continue
        key, val = m.group(1), m.group(2)
        kv[key] = val.strip()

    if "_rlnTomoParticlesFile" not in kv or "_rlnTomoTomogramsFile" not in kv:
        raise ValueError(
            f"{opt_path} does not look like a valid optimisation_set.star "
            f"(missing particles/tomograms pointers)"
        )

    base = opt_path.parent
    particles = _resolve_path_maybe_relative(kv["_rlnTomoParticlesFile"], base)
    tomograms = _resolve_path_maybe_relative(kv["_rlnTomoTomogramsFile"], base)
    return particles, tomograms

def write_optimisation_set_kv(out_path: Path, *, particles_star: Path, tomograms_star: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    content = (
        "# version 50001\n\n"
        "data_\n\n"
        f"_rlnTomoParticlesFile            {str(particles_star.resolve())}\n"
        f"_rlnTomoTomogramsFile            {str(tomograms_star.resolve())}\n"
        "\n"
    )
    out_path.write_text(content)


# -----------------------------
# STAR reading helpers
# -----------------------------

def _read_star_always_dict(path: Path) -> Dict[str, Any]:
    return starfile.read(path, always_dict=True)

def _find_df_block(d: Dict[str, Any], *, must_have: Iterable[str]) -> pd.DataFrame:
    must_have = set(must_have)
    for v in d.values():
        if isinstance(v, pd.DataFrame):
            cols = set(v.columns)
            if must_have.issubset(cols):
                return v
    raise ValueError(f"Could not find a STAR dataframe block with columns {sorted(must_have)}")

def read_particles_star(particles_star: Path) -> Tuple[Dict[str, Any], pd.DataFrame, pd.DataFrame]:
    """
    Returns: (raw_blocks, optics_df, particles_df)
    """
    d = _read_star_always_dict(particles_star)
    optics = _find_df_block(d, must_have=["rlnOpticsGroup", "rlnImageSize"])
    parts  = _find_df_block(d, must_have=["rlnImageName", "rlnTomoName"])
    return d, optics, parts

def read_tomograms_star(tomograms_star: Path) -> pd.DataFrame:
    d = _read_star_always_dict(tomograms_star)
    tomo = _find_df_block(d, must_have=["rlnTomoName", "rlnTomoReconstructedTomogram"])
    return tomo


# -----------------------------
# Validation (MVP strict)
# -----------------------------

def _float_close(a: Any, b: Any, tol: float = 1e-3) -> bool:
    try:
        return abs(float(a) - float(b)) <= tol
    except Exception:
        return False

def _require_same_scalar(
    label: str,
    values: List[Any],
    *,
    tol: float = 1e-3,
    hard: bool = True,
    warnings: Optional[List[str]] = None,
) -> None:
    if not values:
        return
    base = values[0]
    for v in values[1:]:
        same = _float_close(base, v, tol=tol) if isinstance(base, (float, int)) or isinstance(v, (float, int)) else (base == v)
        if not same:
            msg = f"Mismatch for {label}: {base} vs {v}"
            if hard:
                raise ValueError(msg)
            if warnings is not None:
                warnings.append(msg)

def _optics_fingerprint(optics_df: pd.DataFrame) -> List[Tuple]:
    """
    Minimal fingerprint: enforce same extraction geometry + acquisition essentials.
    """
    cols = [
        "rlnVoltage",
        "rlnSphericalAberration",
        "rlnAmplitudeContrast",
        "rlnTomoTiltSeriesPixelSize",
        "rlnImageDimensionality",
        "rlnTomoSubtomogramBinning",
        "rlnImagePixelSize",
        "rlnImageSize",
    ]
    missing = [c for c in cols if c not in optics_df.columns]
    if missing:
        raise ValueError(f"Optics block missing required columns: {missing}")

    fp = []
    for _, r in optics_df.iterrows():
        fp.append(tuple(r[c] for c in cols))
    return fp

def _sample_check_paths_exist(paths: List[Path], *, n: int = 3) -> List[str]:
    """
    Only sample-check existence for QoL. Avoid scanning huge lists on network FS.
    Returns warnings.
    """
    warns: List[str] = []
    for p in paths[:n]:
        if not p.exists():
            warns.append(f"Sample missing file: {p}")
    return warns


# -----------------------------
# Core: preview + apply merge
# -----------------------------

def _summarize_source(spec: str, *, project_root: Optional[Path] = None) -> Tuple[SourceSummary, Dict[str, Any]]:
    opt = resolve_optimisation_set_path(spec, project_root=project_root)
    particles_star, tomograms_star = read_optimisation_set_kv(opt)

    raw_blocks, optics_df, parts_df = read_particles_star(particles_star)
    tomo_df = read_tomograms_star(tomograms_star)

    tomo_names = list(map(str, tomo_df["rlnTomoName"].tolist()))
    n_parts = int(len(parts_df))

    summary = SourceSummary(
        source_spec=spec,
        optimisation_set=str(opt),
        particles_star=str(particles_star),
        tomograms_star=str(tomograms_star),
        n_particles=n_parts,
        tomogram_names=tomo_names,
    )

    payload = {
        "opt_path": opt,
        "particles_star": particles_star,
        "tomograms_star": tomograms_star,
        "raw_blocks": raw_blocks,
        "optics_df": optics_df,
        "parts_df": parts_df,
        "tomo_df": tomo_df,
    }
    return summary, payload

def preview_merge(
    sources: List[str],
    *,
    project_root: Optional[Path] = None,
) -> MergeSummary:
    """
    Validate + compute a merge summary WITHOUT writing anything.
    Designed for UI: call this every time the source list changes.
    """
    warnings: List[str] = []
    if not sources:
        return MergeSummary(
            ok=False,
            out_dir="",
            out_particles_star="",
            out_tomograms_star="",
            out_optimisation_set="",
            total_particles=0,
            total_tomograms=0,
            tomogram_names=[],
            sources=[],
            warnings=["No sources provided"],
        )

    source_summaries: List[SourceSummary] = []
    payloads: List[Dict[str, Any]] = []

    # Load everything
    for spec in sources:
        ss, pl = _summarize_source(spec, project_root=project_root)
        source_summaries.append(ss)
        payloads.append(pl)

    # Duplicate tomo names: HARD ERROR (per your assumption)
    all_tomo_names: List[str] = []
    for pl in payloads:
        all_tomo_names.extend(list(map(str, pl["tomo_df"]["rlnTomoName"].tolist())))
    dupes = sorted({n for n in all_tomo_names if all_tomo_names.count(n) > 1})
    if dupes:
        raise ValueError(
            "Duplicate rlnTomoName across sources (not allowed for your MVP assumption): "
            + ", ".join(dupes)
        )

    # Microscope identity check from tomograms.star (hard for voltage/Cs; warn for amp)
    voltages = [float(pl["tomo_df"]["rlnVoltage"].iloc[0]) for pl in payloads if "rlnVoltage" in pl["tomo_df"].columns]
    css      = [float(pl["tomo_df"]["rlnSphericalAberration"].iloc[0]) for pl in payloads if "rlnSphericalAberration" in pl["tomo_df"].columns]
    amps     = [float(pl["tomo_df"]["rlnAmplitudeContrast"].iloc[0]) for pl in payloads if "rlnAmplitudeContrast" in pl["tomo_df"].columns]
    _require_same_scalar("rlnVoltage", voltages, hard=True, warnings=warnings)
    _require_same_scalar("rlnSphericalAberration", css, hard=True, warnings=warnings)
    _require_same_scalar("rlnAmplitudeContrast", amps, hard=False, warnings=warnings)

    # Tilt-series pixel size must match (tomograms + optics)
    tilt_pix_tomo = [float(pl["tomo_df"]["rlnTomoTiltSeriesPixelSize"].iloc[0]) for pl in payloads if "rlnTomoTiltSeriesPixelSize" in pl["tomo_df"].columns]
    tilt_pix_opt  = [float(pl["optics_df"]["rlnTomoTiltSeriesPixelSize"].iloc[0]) for pl in payloads if "rlnTomoTiltSeriesPixelSize" in pl["optics_df"].columns]
    _require_same_scalar("rlnTomoTiltSeriesPixelSize(tomograms)", tilt_pix_tomo, hard=True, warnings=warnings)
    _require_same_scalar("rlnTomoTiltSeriesPixelSize(optics)", tilt_pix_opt, hard=True, warnings=warnings)

    # Extracted image geometry / binning must match (via optics fingerprint)
    fp0 = _optics_fingerprint(payloads[0]["optics_df"])
    for pl in payloads[1:]:
        fpi = _optics_fingerprint(pl["optics_df"])
        if fpi != fp0:
            raise ValueError(
                "Optics blocks differ across sources (likely mismatched extraction geometry/binning). "
                "For MVP we require them to be identical."
            )

    # Tomogram-scale pixel size in particles df (your example stores ~12 A/px there)
    # Column name is confusing, but value is useful.
    if "rlnTomoTiltSeriesPixelSize" in payloads[0]["parts_df"].columns:
        tomo_pix_parts = [float(pl["parts_df"]["rlnTomoTiltSeriesPixelSize"].iloc[0]) for pl in payloads]
        _require_same_scalar("rlnTomoTiltSeriesPixelSize(particles)", tomo_pix_parts, hard=True, warnings=warnings)

    # Sample-check a few stack paths exist
    for pl in payloads:
        pcol = "rlnImageName"
        if pcol in pl["parts_df"].columns:
            # rlnImageName in your file is an absolute path to .mrcs
            sample_paths = [Path(x) for x in pl["parts_df"][pcol].astype(str).tolist()[:3]]
            warnings.extend(_sample_check_paths_exist(sample_paths, n=3))

    total_particles = int(sum(len(pl["parts_df"]) for pl in payloads))
    total_tomograms = int(sum(len(pl["tomo_df"]) for pl in payloads))
    all_names_sorted = sorted(all_tomo_names)

    return MergeSummary(
        ok=True,
        out_dir="",
        out_particles_star="",
        out_tomograms_star="",
        out_optimisation_set="",
        total_particles=total_particles,
        total_tomograms=total_tomograms,
        tomogram_names=all_names_sorted,
        sources=source_summaries,
        warnings=warnings,
    )

def apply_merge(
    sources: List[str],
    *,
    out_dir: Path,
    project_root: Optional[Path] = None,
    overwrite: bool = True,
) -> MergeSummary:
    """
    Perform the merge and write:
      - <out_dir>/particles.star
      - <out_dir>/tomograms.star
      - <out_dir>/optimisation_set.star
      - <out_dir>/merge_manifest.json
    Returns a MergeSummary suitable for UI display.
    """
    # First run preview for validation + counts (raises on hard problems)
    preview = preview_merge(sources, project_root=project_root)
    warnings = list(preview.warnings)

    out_dir = out_dir.resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    # Re-load payloads (kept simple; can optimize later)
    payloads: List[Dict[str, Any]] = []
    source_summaries: List[SourceSummary] = []
    for spec in sources:
        ss, pl = _summarize_source(spec, project_root=project_root)
        payloads.append(pl)
        source_summaries.append(ss)

    # Merge tomograms
    merged_tomo_df = pd.concat([pl["tomo_df"] for pl in payloads], ignore_index=True)

    # Merge particles
    merged_parts_df = pd.concat([pl["parts_df"] for pl in payloads], ignore_index=True)

    # Keep optics/general from first (validated identical for MVP)
    raw0 = payloads[0]["raw_blocks"]
    optics0 = payloads[0]["optics_df"].copy()

    # Try to preserve a sensible general block if present; otherwise create minimal.
    # starfile is permissive; RELION mainly cares about optics+particles.
    general_block: Dict[str, Any] = {}
    for k, v in raw0.items():
        # Non-loop blocks may come back as dict
        if isinstance(v, dict) and any(key.startswith("rln") for key in v.keys()):
            general_block = v
            break
    if not general_block:
        # preserve your current behavior: 2D stacks
        general_block = {"rlnTomoSubTomosAre2DStacks": 1}

    out_particles = out_dir / "particles.star"
    out_tomograms = out_dir / "tomograms.star"
    out_optset    = out_dir / "optimisation_set.star"
    out_manifest  = out_dir / "merge_manifest.json"

    if not overwrite:
        for p in [out_particles, out_tomograms, out_optset, out_manifest]:
            if p.exists():
                raise FileExistsError(f"Refusing to overwrite existing file: {p}")

    # Write tomograms.star (RELION tomo uses data_global)
    starfile.write({"global": merged_tomo_df}, out_tomograms, overwrite=True)

    # Write particles.star with 3 blocks
    # starfile expects keys without "data_" prefix; it writes "data_<key>"
    starfile.write(
        {
            "general": general_block,
            "optics": optics0,
            "particles": merged_parts_df,
        },
        out_particles,
        overwrite=True,
    )

    # Write optimisation_set.star in the same key-value style
    write_optimisation_set_kv(out_optset, particles_star=out_particles, tomograms_star=out_tomograms)

    # Manifest for UI/debugging
    manifest = {
        "sources": [asdict(s) for s in source_summaries],
        "outputs": {
            "particles_star": str(out_particles),
            "tomograms_star": str(out_tomograms),
            "optimisation_set": str(out_optset),
        },
        "totals": {
            "particles": int(len(merged_parts_df)),
            "tomograms": int(len(merged_tomo_df)),
            "tomogram_names": sorted(list(map(str, merged_tomo_df["rlnTomoName"].tolist()))),
        },
        "warnings": warnings,
    }
    out_manifest.write_text(json.dumps(manifest, indent=2))

    return MergeSummary(
        ok=True,
        out_dir=str(out_dir),
        out_particles_star=str(out_particles),
        out_tomograms_star=str(out_tomograms),
        out_optimisation_set=str(out_optset),
        total_particles=int(len(merged_parts_df)),
        total_tomograms=int(len(merged_tomo_df)),
        tomogram_names=sorted(list(map(str, merged_tomo_df["rlnTomoName"].tolist()))),
        sources=source_summaries,
        warnings=warnings,
    )
