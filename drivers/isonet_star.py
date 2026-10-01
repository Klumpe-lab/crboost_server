"""Per-tomogram facts the pipeline has measured, written into the star IsoNet's prepare_star makes.

Left alone, prepare_star gives every tomogram a 1 µm defocus, a ±60° tilt range and the
Cs / kV / amplitude contrast of its own defaults. denoise_train and denoise_predict both run
prepare_star and then overwrite those columns from the reconstruct tomograms.star and each
tomogram's tilt-series star.
"""

from dataclasses import dataclass
from pathlib import Path

import starfile


@dataclass(frozen=True)
class IsoNetTomoFacts:
    voltage: float
    cs: float
    amplitude_contrast: float
    defocus_angstrom: float  # mean of U/V at the tilt closest to 0° in the reconstruction's frame
    tilt_min: float
    tilt_max: float
    warp_deconv: Path | None  # tsReconstruct's deconvolved copy of the full map, if it wrote one


def _project_file(project_root: Path, path: str) -> Path:
    p = Path(path)
    return p if p.is_absolute() else project_root / p


def _tilt_block(ts_star: Path):
    for df in starfile.read(ts_star, always_dict=True).values():
        if "rlnTomoYTilt" in df.columns:
            return df
    raise ValueError(f"No per-tilt block with rlnTomoYTilt in {ts_star}")


def read_tomo_facts(input_star: Path, project_root: Path, names: set[str]) -> dict[str, IsoNetTomoFacts]:
    """Facts for the full reconstructions whose file names are in `names`, keyed by file name."""
    df = starfile.read(input_star, always_dict=True).get("global")
    if df is None:
        raise ValueError(f"No 'global' block in {input_star}")
    facts: dict[str, IsoNetTomoFacts] = {}
    for _, row in df.iterrows():
        full = _project_file(project_root, str(row["rlnTomoReconstructedTomogram"]))
        if full.name not in names:
            continue
        tilts = _tilt_block(_project_file(project_root, str(row["rlnTomoTiltSeriesStarFile"])))
        # rlnTomoYTilt is the angle the volume was reconstructed with: hand flip and the
        # alignment's tilt offset applied, so a lamella's pretilt is already levelled out
        # (nominal -64..+50 can be -60..+56 here). ISONET-ASSUMPTION: IsoNet's missing-wedge
        # sign matches rlnTomoYTilt's; for a near-symmetric range a sign error costs only
        # |min + max| degrees.
        y = tilts["rlnTomoYTilt"].astype(float)
        zero = y.abs().idxmin()
        deconv = full.parent / "deconv" / full.name
        facts[full.name] = IsoNetTomoFacts(
            voltage=float(row["rlnVoltage"]),
            cs=float(row["rlnSphericalAberration"]),
            amplitude_contrast=float(row["rlnAmplitudeContrast"]),
            defocus_angstrom=(float(tilts.at[zero, "rlnDefocusU"]) + float(tilts.at[zero, "rlnDefocusV"])) / 2,
            tilt_min=float(y.min()),
            tilt_max=float(y.max()),
            warp_deconv=deconv if deconv.exists() else None,
        )
    missing = names - facts.keys()
    if missing:
        raise ValueError(f"{input_star} has no row for {sorted(missing)}")
    return facts


def annotate_prep_star(prep_star: Path, facts: dict[str, IsoNetTomoFacts]) -> None:
    """Overwrite prepare_star's defaults with `facts`, matched on the full tomogram's file name,
    and point rlnDeconvTomoName at tsReconstruct's deconvolved map wherever one exists."""
    df = starfile.read(prep_star)
    rows = [facts[Path(str(name)).name] for name in df["rlnTomoName"]]
    df["rlnVoltage"] = [f.voltage for f in rows]
    df["rlnSphericalAberration"] = [f.cs for f in rows]
    df["rlnAmplitudeContrast"] = [f.amplitude_contrast for f in rows]
    df["rlnDefocus"] = [f.defocus_angstrom for f in rows]
    df["rlnTiltMin"] = [f.tilt_min for f in rows]
    df["rlnTiltMax"] = [f.tilt_max for f in rows]
    df["rlnDeconvTomoName"] = [
        str(f.warp_deconv) if f.warp_deconv else old for f, old in zip(rows, df["rlnDeconvTomoName"], strict=True)
    ]
    starfile.write(df, prep_star, overwrite=True)


def log_facts(facts: dict[str, IsoNetTomoFacts], log) -> None:
    for name, f in sorted(facts.items()):
        log(
            f"[ISONET] {name}: defocus {f.defocus_angstrom / 10000:.2f} µm, tilts {f.tilt_min:.1f}..{f.tilt_max:.1f}°, "
            f"{f.voltage:g} kV, Cs {f.cs:g}, AC {f.amplitude_contrast:g}, "
            f"deconv map {'from tsReconstruct' if f.warp_deconv else 'none'}"
        )
