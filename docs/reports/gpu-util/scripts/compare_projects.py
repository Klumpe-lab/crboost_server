"""Compare two runs of the same pipeline on the same data, stage by stage: did moving a step change its outputs?

    python3 -B compare_projects.py /groups/klumpe/crboost_data/slurmtest_412_FCIsolation <the other project folder>

Reads only, with the crboost environment's Python (it needs mrcfile and numpy). Jobs pair up by instance (each
job's run.out names it), files by their name after the project's own prefix (import names every movie
<project>_<movie>). Warp's XML files are walked side by side: numbers are compared as numbers, per field the
largest absolute difference against the largest value it compares, and text naming a path is skipped since it names
the project. Frame-series XMLs are sampled (500 at most). Tomograms are compared on a centre crop of 128 x 256 x 256
voxels: identical, or the lowest correlation and the largest difference.
"""

import random
import re
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path

import mrcfile
import numpy as np


def instances(project: Path) -> dict[str, Path]:
    out = {}
    for job in sorted((project / "External").glob("job*")):
        log = job / "run.out"
        m = re.search(r"instance '([^']+)'", log.read_text(errors="replace")[:20000]) if log.exists() else None
        if m:
            out[m.group(1)] = job
    return out


def by_suffix(files: list[Path], prefix: str) -> dict[str, Path]:
    return {f.name[len(prefix) :] if f.name.startswith(prefix) else f.name: f for f in files}


def numbers(text: str) -> list[float] | None:
    tokens = [t for t in re.split(r"[\s,]+", text.strip()) if t]
    try:
        return [float(t) for t in tokens]
    except ValueError:
        return None


class Diff:
    def __init__(self) -> None:
        self.abs = defaultdict(float)
        self.scale = defaultdict(float)
        self.text = defaultdict(int)
        self.files = 0
        self.same = 0

    def value(self, key: str, a: str | None, b: str | None) -> bool:
        """Records one field; True when it differs."""
        if a is None or b is None:
            self.text[key + " (missing on one side)"] += a != b
            return a != b
        if "/" in a or "/" in b:
            return False
        na, nb = numbers(a), numbers(b)
        if na is not None and nb is not None and len(na) == len(nb):
            if not na:
                return False
            d = max(abs(x - y) for x, y in zip(na, nb, strict=True))
            self.abs[key] = max(self.abs[key], d)
            self.scale[key] = max(self.scale[key], *(abs(x) for x in na))
            return d > 0
        self.text[key] += a.strip() != b.strip()
        return a.strip() != b.strip()

    def walk(self, a: ET.Element, b: ET.Element, path: str) -> bool:
        differs = False
        for k in sorted(set(a.attrib) | set(b.attrib)):
            differs |= self.value(f"{path}@{k}", a.attrib.get(k), b.attrib.get(k))
        differs |= self.value(f"{path}#text", a.text or "", b.text or "")
        ca, cb = list(a), list(b)
        if len(ca) != len(cb):
            self.text[f"{path} (children {len(ca)} vs {len(cb)})"] += 1
            return True
        for x, y in zip(ca, cb, strict=True):
            name = x.attrib.get("Name")
            differs |= self.walk(x, y, f"{path}/{x.tag}" + (f"[{name}]" if name else ""))
        return differs


def compare_xml(pairs: list[tuple[Path, Path]]) -> Diff:
    diff = Diff()
    for a, b in pairs:
        diff.files += 1
        ra, rb = ET.parse(a).getroot(), ET.parse(b).getroot()
        diff.same += not diff.walk(ra, rb, ra.tag)
    return diff


def compare_mrc(pairs: list[tuple[Path, Path]]) -> tuple[int, int, float, float]:
    same, corr, diff = 0, [], []
    for a, b in pairs:
        with mrcfile.mmap(a, permissive=True) as ma, mrcfile.mmap(b, permissive=True) as mb:
            z, y, x = (s // 2 for s in ma.data.shape)
            crop = (slice(z - 64, z + 64), slice(y - 128, y + 128), slice(x - 128, x + 128))
            ca, cb = np.asarray(ma.data[crop], dtype=np.float64), np.asarray(mb.data[crop], dtype=np.float64)
        if np.array_equal(ca, cb):
            same += 1
        else:
            corr.append(float(np.corrcoef(ca.ravel(), cb.ravel())[0, 1]))
            diff.append(float(np.abs(ca - cb).max()))
    return same, len(pairs), min(corr) if corr else 1.0, max(diff) if diff else 0.0


def main() -> None:
    pa, pb = Path(sys.argv[1]), Path(sys.argv[2])
    ia, ib = instances(pa), instances(pb)
    print(f"{pa.name} against {pb.name}\n")
    for inst in [i for i in ia if i in ib]:
        ja, jb = ia[inst], ib[inst]
        rows = []
        for sub, sample in (("warp_frameseries", 500), ("warp_tiltseries", None)):
            fa = by_suffix(sorted((ja / sub).glob("*.xml")), pa.name)
            fb = by_suffix(sorted((jb / sub).glob("*.xml")), pb.name)
            common = sorted(set(fa) & set(fb))
            if sample and len(common) > sample:
                common = sorted(random.Random(0).sample(common, sample))
            if common:
                rows.append((sub, len(set(fa) ^ set(fb)), compare_xml([(fa[k], fb[k]) for k in common])))
        ra = by_suffix(sorted((ja / "warp_tiltseries/reconstruction").glob("*.mrc")), pa.name)
        rb = by_suffix(sorted((jb / "warp_tiltseries/reconstruction").glob("*.mrc")), pb.name)
        tomo = sorted(set(ra) & set(rb))
        if not rows and not tomo:
            continue
        print(f"## {inst} ({ja.name} against {jb.name})\n")
        for sub, only_one, diff in rows:
            print(f"{sub}: {diff.files} XML files compared ({only_one} on one side only), {diff.same} the same")
            top = sorted(diff.abs.items(), key=lambda kv: kv[1] / (diff.scale[kv[0]] or 1), reverse=True)[:8]
            for key, d in top:
                if d > 0:
                    print(f"  {key}: largest difference {d:.4g} (values up to {diff.scale[key]:.4g})")
            for key, n in sorted(diff.text.items(), key=lambda kv: -kv[1])[:5]:
                if n:
                    print(f"  {key}: text differs in {n} files")
        if tomo:
            same, n, low, high = compare_mrc([(ra[k], rb[k]) for k in tomo])
            print(
                f"tomograms: {n} compared, {same} identical; lowest correlation {low:.6f}, largest difference "
                f"{high:.3g}"
            )
        print()


if __name__ == "__main__":
    main()
