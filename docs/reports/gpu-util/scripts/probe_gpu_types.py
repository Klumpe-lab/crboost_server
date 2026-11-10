"""P13 (roadmap 45): the pipeline's GPU stages on whatever card this job got, against the first 412 pilot.

Run by probe_gpu_types.sh inside a SLURM job on one GPU. Each stage replays the pilot's own command
(slurmtest_412_FCIsolation, read only) on its own inputs, staged the way production stages a task, pinned to the
CPUs production gives that stage; the outputs are compared with the pilot's and the large files deleted:

    motion   Warp fs_motion_and_ctf, first 2 tilt series   frame defocus and averages against job002
    align    AreTomo2 through Warp ts_aretomo               the .st.aln against job004
    ctf      Warp ts_ctf, after the pilot's --set_flip      per-tilt defocus against job005
    recon    Warp ts_reconstruct, one call a tilt series    the tomogram's centre against job006
    tm       pytom on the first tomogram, I1 at 5°          a centre crop of the scores map, for the other cards

Before them, a kernel on this card in every other GPU image (RELION: the architectures its binary carries). Writes
only under the result folder. `--dry-run` stages everything and prints the commands, without a GPU, and compares the
pilot's outputs with themselves to check the comparisons.

    probe_gpu_types.py <result folder> <tilt series> [--dry-run]
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

import mrcfile
import numpy as np

from drivers.array_job_base import copy_tomostar_with_absolute_paths, stage_tilt_series
from drivers.driver_base import allocated_gpu_memory_bytes
from drivers.template_match_pytom import auto_gpu_split, mrc_shape_xyz
from services.configs.config_service import get_config_service
from services.templating.angle_lists import generate_asymmetric_unit_angles, write_angle_list_file

PILOT = Path("/groups/klumpe/crboost_data/slurmtest_412_FCIsolation")
E = PILOT / "External"
MOTION, IMPORT, ALIGN, CTF, RECON, TM = (E / f"job00{n}" for n in (2, 3, 4, 5, 6, 7))
APIX = "6.20"
# CPUs production gives each stage's task (config/conf.yaml job_resource_profiles).
CPUS = {"motion": 2, "align": 2, "ctf": 2, "recon": 4, "tm": 4}
TM_SYMMETRY, TM_INCREMENT = "I1", 5.0
STAGE_TIMEOUT_S = 3600
CHECK_TIMEOUT_S = 900
COMMAND = re.compile(r"^\[TASK \d+\] Command: (.*)$")


def log(msg: str) -> None:
    print(f"{time.strftime('%H:%M:%S')} {msg}", flush=True)


class Probe:
    def __init__(self, res: Path, dry_run: bool) -> None:
        self.res, self.dry = res, dry_run
        self.cache = Path(os.environ.get("TMPDIR", "/tmp")) / f"probe_cache_{os.environ.get('SLURM_JOB_ID', 'dry')}"
        for d in ("logs", "stage", "out", "crops"):
            (res / d).mkdir(parents=True, exist_ok=True)
        self.cache.mkdir(parents=True, exist_ok=True)
        self.runs = open(res / "runs.tsv", "a", buffering=1)
        self.compare = open(res / "compare.tsv", "a", buffering=1)
        self.checks = open(res / "checks.tsv", "a", buffering=1)
        tools = get_config_service().config.tools
        self.sifs = {name: tools[name].container_path for name in tools if getattr(tools[name], "container_path", None)}

    # -- running ---------------------------------------------------------------------------------------------------

    def cpus(self, n: int) -> str:
        return ",".join(map(str, sorted(os.sched_getaffinity(0))[:n]))

    def container(self, sif: str | Path, cwd: Path, command: str, ncpu: int | None, log_path: Path, timeout: int):
        """One command in the image, production's cache settings with the caches in this job's /tmp. Returns
        (exit code, seconds)."""
        env = (
            f"export XDG_CACHE_HOME={self.cache} CUPY_CACHE_DIR={self.cache}/cupy CUDA_CACHE_PATH={self.cache}/cuda "
            f"CUDA_CACHE_MAXSIZE=4294967296 MPLCONFIGDIR={self.cache}/mpl TQDM_DISABLE=1"
        )
        argv = ["apptainer", "exec", "--nv", "--cleanenv", "-B", "/groups", "-B", "/scratch", "-B", "/tmp", str(sif)]
        argv += ["bash", "-c", f"{env}; {command}"]
        if ncpu is not None:
            argv = ["taskset", "-c", self.cpus(ncpu), *argv]
        if self.dry:
            log(f"[dry-run] in {cwd}: {command}")
            return 0, 0.0
        t0 = time.time()
        with open(log_path, "w") as out:
            try:
                rc = subprocess.run(argv, cwd=cwd, stdout=out, stderr=subprocess.STDOUT, timeout=timeout).returncode
            except subprocess.TimeoutExpired:
                out.write(f"\n[probe] TIMEOUT after {timeout} s\n")
                rc = 124
        return rc, time.time() - t0

    def timed(self, stage: str, item: str, sif: Path, cwd: Path, command: str, ncpu: int | None) -> int:
        t0 = time.time()
        rc, secs = self.container(sif, cwd, command, ncpu, self.res / "logs" / f"{stage}_{item}.log", STAGE_TIMEOUT_S)
        self.runs.write(f"{stage}\t{item}\t{ncpu or ''}\t{t0:.1f}\t{secs:.1f}\t{rc}\n")
        log(f"{stage} {item}: exit {rc}, {secs:.0f} s")
        return rc

    def row(self, stage: str, item: str, what: str, value) -> None:
        self.compare.write(f"{stage}\t{item}\t{what}\t{value}\n")

    # -- the card --------------------------------------------------------------------------------------------------

    def card(self) -> dict:
        if self.dry:
            info = {"gpu": "dry run", "compute_cap": "0.0", "memory_bytes": 24 * 2**30, "listing": ""}
        else:
            listing = subprocess.run(["nvidia-smi", "-L"], capture_output=True, text=True, check=True).stdout
            q = subprocess.run(
                ["nvidia-smi", "--query-gpu=name,compute_cap", "--format=csv,noheader"],
                capture_output=True,
                text=True,
                check=True,
            ).stdout.splitlines()[0]
            name, cap = (s.strip() for s in q.split(","))
            mig = re.search(r"^\s*MIG (\S+)", listing, re.MULTILINE)
            info = {
                "gpu": f"{name} MIG {mig.group(1)}" if mig else name,
                "compute_cap": cap,
                "memory_bytes": allocated_gpu_memory_bytes(),
                "listing": listing,
            }
        info |= {
            "node": os.uname().nodename.split(".")[0],
            "slurm_job": os.environ.get("SLURM_JOB_ID", ""),
            "cpus": len(os.sched_getaffinity(0)),
        }
        (self.res / "card.json").write_text(json.dumps(info, indent=1))
        log(f"card: {info['gpu']}, compute capability {info['compute_cap']}, {info['memory_bytes'] / 2**30:.1f} GiB")
        return info

    def kernel_checks(self, cap: str) -> None:
        """A kernel on this card in each other GPU image; RELION's binary is read for this card's architecture."""
        sm = "sm_" + cap.replace(".", "")
        torch_kernel = (
            "import torch; x = torch.randn(1024, 1024, device='cuda'); "
            "print(torch.cuda.get_device_name(0), torch.__version__, float((x @ x).abs().sum()) > 0)"
        )
        tf_kernel = (
            "import tensorflow as tf\\n"
            "with tf.device('/GPU:0'):\\n"
            "    x = tf.random.normal([1024, 1024]); s = float(tf.reduce_sum(tf.abs(tf.matmul(x, x))))\\n"
            "print(tf.__version__, tf.config.list_physical_devices('GPU'), s > 0)"
        )
        checks = {
            "relion": (
                "c=$(command -v cuobjdump || echo /usr/local/cuda/bin/cuobjdump); "
                "for b in relion_refine relion_refine_mpi relion_autopick; do "
                f'a=$($c --list-elf "$(command -v $b)" 2>/dev/null | grep -o "sm_[0-9]*" | sort -u | tr "\\n" " "); '
                f'echo "$b: $a"; echo "$a" | grep -q -w {sm} || exit 3; done'
            ),
            # isonet.py's own interpreter, from its first line ("#!/usr/bin/env python" splits into two words).
            "isonet": f'$(head -1 "$(command -v isonet.py)" | sed "s/^#!//") -c "{torch_kernel}"',
            "tilt_filter": f'python -c "{torch_kernel}"',
            "miss_alignment": f'python -c "{torch_kernel}"',
            "cryocare": f'printf "{tf_kernel}" | python -',
            "easymode": f'printf "{tf_kernel}" | python -',
        }
        for tool, command in checks.items():
            sif = self.sifs.get(tool)
            if sif is None:
                self.checks.write(f"{tool}\tnot configured\t\t\n")
                continue
            if tool in ("cryocare", "easymode") and sm == "sm_100":
                # TF 2.x carries no sm_100 code: on a B200 the first use JITs for 12-22 min, and production runs
                # these there already.
                self.checks.write(f"{tool}\tskipped on sm_100 (known to run, JIT)\t\t\n")
                continue
            rc, secs = self.container(
                sif, self.res, command, None, self.res / "logs" / f"check_{tool}.log", CHECK_TIMEOUT_S
            )
            tail = ""
            if not self.dry:
                lines = (self.res / "logs" / f"check_{tool}.log").read_text(errors="replace").strip().splitlines()
                tail = lines[-1][:200] if lines else ""
            self.checks.write(f"{tool}\t{'ok' if rc == 0 else f'exit {rc}'}\t{secs:.0f}\t{tail}\n")
            log(f"check {tool}: exit {rc}, {secs:.0f} s")

    # -- the pilot ---------------------------------------------------------------------------------------------------

    @staticmethod
    def pilot_command(job_dir: Path, starts: str, holds: str = "") -> str:
        """The first command the pilot's tasks ran that starts with `starts` and holds `holds`."""
        for log_file in sorted(job_dir.glob("task_*.out"), key=lambda p: int(p.stem.split("_")[1])):
            for line in log_file.read_text(errors="replace").splitlines():
                m = COMMAND.match(line)
                if m and m.group(1).startswith(starts) and holds in m.group(1):
                    return m.group(1)
        raise RuntimeError(f"no command starting {starts!r} holding {holds!r} in {job_dir}/task_*.out")

    @staticmethod
    def tilt_series(n: int) -> list[str]:
        """The first n tilt series the pilot took through every stage to a tomogram."""
        names = sorted(
            p.name[: -len(f"_{APIX}Apx.mrc")]
            for p in (RECON / "warp_tiltseries" / "reconstruction").glob(f"*_{APIX}Apx.mrc")
        )
        ok = [
            ts
            for ts in names
            if (MOTION / ".staging" / f"task_{ts}" / "frames").is_dir()
            and (ALIGN / "warp_tiltseries" / "tiltstack" / ts / f"{ts}.st.aln").exists()
            and (CTF / "warp_tiltseries" / f"{ts}.xml").exists()
        ]
        return ok[:n]

    # -- stages ------------------------------------------------------------------------------------------------------

    def motion(self, ts: str) -> None:
        root = self.res / "stage" / f"motion_{ts}"
        frames = root / "frames"
        frames.mkdir(parents=True)
        for link in sorted((MOTION / ".staging" / f"task_{ts}" / "frames").iterdir()):
            (frames / link.name).symlink_to(os.readlink(link))
        cmd = self.pilot_command(MOTION, "test -f warp_frameseries.settings")
        rc = self.timed("motion", ts, self.sifs["warp_aretomo"], root, cmd, CPUS["motion"])
        out = MOTION / "warp_frameseries" if self.dry else root / "warp_frameseries"
        if rc == 0:
            defocus, corr = [], []
            for frame in sorted(frames.iterdir()):
                stem = Path(frame.name).stem
                a, b = _frame_defocus(out / f"{stem}.xml"), _frame_defocus(MOTION / "warp_frameseries" / f"{stem}.xml")
                if a is not None and b is not None:
                    defocus.append(abs(a - b))
                avg, ref = out / "average" / f"{stem}.mrc", MOTION / "warp_frameseries" / "average" / f"{stem}.mrc"
                if avg.exists() and ref.exists():
                    corr.append(_centre_corr(avg, ref))
            self.row("motion", ts, "frames", len(defocus))
            self.row("motion", ts, "defocus_abs_diff_median_um", _median(defocus))
            self.row("motion", ts, "defocus_abs_diff_max_um", max(defocus, default=""))
            self.row("motion", ts, "average_corr_min", f"{min(corr):.6f}" if corr else "")
        if not self.dry:
            for sub in ("average", "powerspectrum"):
                shutil.rmtree(root / "warp_frameseries" / sub, ignore_errors=True)  # large; the XMLs stay

    def align(self, ts: str) -> None:
        root = self.res / "stage" / f"align_{ts}"
        stage_tilt_series(root, [ts], None, ALIGN / "warp_tiltseries.settings")
        name = ts
        pilot_staged = sorted((ALIGN / ".staging" / f"task_{ts}" / "tomostar").glob("*.tomostar"))
        if pilot_staged and pilot_staged[0].stem != ts:  # the pilot ran it under AreTomo's short alias
            name = pilot_staged[0].stem
            (root / "tomostar" / f"{ts}.tomostar").replace(root / "tomostar" / f"{name}.tomostar")
        cmd = self.pilot_command(ALIGN, "WarpTools ts_aretomo")
        rc = self.timed("align", ts, self.sifs["warp_aretomo"], root, cmd, CPUS["align"])
        stack = (ALIGN / "warp_tiltseries" if self.dry else root / "warp_tiltseries") / "tiltstack" / name
        if rc == 0:
            mine, ref = (
                _aln_rows(stack / f"{name}.st.aln"),
                _aln_rows(ALIGN / "warp_tiltseries" / "tiltstack" / ts / f"{ts}.st.aln"),
            )
            self.row("align", ts, "tilts", f"{len(mine)} vs {len(ref)}")
            if mine and len(mine) == len(ref):
                d = np.abs(np.array(mine) - np.array(ref))
                self.row("align", ts, "rot_abs_diff_max_deg", f"{d[:, 1].max():.3g}")
                self.row("align", ts, "shift_abs_diff_max_px", f"{d[:, 3:5].max():.3g}")
        if not self.dry:
            for f in stack.glob("*"):
                if f.suffix in (".st", ".mrc") or f.name.endswith("_aligned.mrc"):
                    f.unlink()

    def ctf(self, ts: str) -> None:
        root = self.res / "stage" / f"ctf_{ts}"
        (root / "tomostar").mkdir(parents=True)
        (root / "warp_tiltseries").mkdir()
        shutil.copy2(CTF / "warp_tiltseries.settings", root / "warp_tiltseries.settings")
        copy_tomostar_with_absolute_paths(
            CTF / "tomostar" / f"{ts}.tomostar", root / "tomostar" / f"{ts}.tomostar", CTF / "tomostar"
        )
        shutil.copy2(ALIGN / "warp_tiltseries" / f"{ts}.xml", root / "warp_tiltseries" / f"{ts}.xml")
        flip = (
            "WarpTools ts_defocus_hand --settings warp_tiltseries.settings --output_processing warp_tiltseries "
            "--set_flip"
        )
        rc, _ = self.container(
            self.sifs["warp_aretomo"], root, flip, CPUS["ctf"], self.res / "logs" / f"ctf_flip_{ts}.log", 600
        )
        if rc != 0:
            self.row("ctf", ts, "set_flip_exit", rc)
            return
        cmd = self.pilot_command(CTF, "WarpTools ts_ctf")
        rc = self.timed("ctf", ts, self.sifs["warp_aretomo"], root, cmd, CPUS["ctf"])
        xml = (CTF if self.dry else root) / "warp_tiltseries" / f"{ts}.xml"
        if rc == 0:
            mine, ref = _grid_ctf(xml), _grid_ctf(CTF / "warp_tiltseries" / f"{ts}.xml")
            self.row("ctf", ts, "tilts", f"{len(mine)} vs {len(ref)}")
            if mine and len(mine) == len(ref):
                d = np.abs(np.array(mine) - np.array(ref))
                self.row("ctf", ts, "defocus_abs_diff_median_um", f"{np.median(d):.4f}")
                self.row("ctf", ts, "defocus_abs_diff_max_um", f"{d.max():.4f}")

    def recon(self, ts: str) -> None:
        root = self.res / "stage" / f"recon_{ts}"
        (root / "tomostar").mkdir(parents=True)
        (root / "warp_tiltseries").mkdir()
        shutil.copy2(IMPORT / "warp_tiltseries.settings", root / "warp_tiltseries.settings")
        copy_tomostar_with_absolute_paths(
            IMPORT / "tomostar" / f"{ts}.tomostar", root / "tomostar" / f"{ts}.tomostar", IMPORT / "tomostar"
        )
        shutil.copy2(CTF / "warp_tiltseries" / f"{ts}.xml", root / "warp_tiltseries" / f"{ts}.xml")
        out = self.res / "out" / "recon"
        cmd = self.pilot_command(RECON, "WarpTools ts_reconstruct")
        for opt, value in (
            ("--settings", root / "warp_tiltseries.settings"),
            ("--input_processing", root / "warp_tiltseries"),
            ("--output_processing", out),
            ("--task_dir", root / "tasks"),
        ):
            cmd = re.sub(rf" {opt} \S+", f" {opt} {value}", cmd)
        rc = self.timed("recon", ts, self.sifs["warp_aretomo"], root, cmd, CPUS["recon"])
        tomo = (RECON / "warp_tiltseries" if self.dry else out) / "reconstruction" / f"{ts}_{APIX}Apx.mrc"
        ref = RECON / "warp_tiltseries" / "reconstruction" / f"{ts}_{APIX}Apx.mrc"
        if rc == 0 and tomo.exists():
            self.row("recon", ts, "centre_corr", f"{_centre_corr(tomo, ref):.6f}")
        if not self.dry:
            shutil.rmtree(out / "reconstruction", ignore_errors=True)  # 1 GB a tomogram, and its halves

    def tm(self, ts: str, memory_bytes: int) -> None:
        out = self.res / "out" / "tm"
        out.mkdir(parents=True, exist_ok=True)
        cmd = self.pilot_command(TM, "pytom_match_template.py", f"/{ts}.mrc ")
        tomo = Path(re.search(r" -v (\S+)", cmd).group(1))
        template = Path(re.search(r" -t (\S+)", cmd).group(1))
        angles = self.res / "stage" / f"angles_{TM_SYMMETRY}_{TM_INCREMENT:g}.txt"
        write_angle_list_file(generate_asymmetric_unit_angles(TM_SYMMETRY, TM_INCREMENT), angles)
        one = self.res / "stage" / "angles_one.txt"
        one.write_text("0.0 0.0 0.0\n")
        decoy = "--random-phase-correction" in cmd
        split, need = auto_gpu_split(mrc_shape_xyz(tomo), mrc_shape_xyz(template), memory_bytes, decoy=decoy)
        self.row("tm", ts, "split", " ".join(map(str, split)))
        self.row("tm", ts, "estimated_gib_per_piece", f"{need / 2**30:.1f}")
        self.row("tm", ts, "rotations", sum(1 for _ in angles.open()))

        def with_list(listing: Path) -> str:
            c = re.sub(r" -d \S+", f" -d {out}", cmd)
            c = re.sub(r" --angular-search \S+", f" --angular-search {listing}", c)
            c = re.sub(r" -s \d+ \d+ \d+", "", c)
            return f"{c} -s {' '.join(map(str, split))}"

        self.timed("tm_warmup", ts, self.sifs["pytom"], out, with_list(one), CPUS["tm"])
        for f in out.glob("*.mrc"):
            f.unlink()
        rc = self.timed("tm", ts, self.sifs["pytom"], out, with_list(angles), CPUS["tm"])
        scores = out / f"{tomo.stem}_scores.mrc"
        if rc == 0 and scores.exists():
            with mrcfile.mmap(scores, permissive=True) as m:
                data = m.data
                z, y, x = (s // 2 for s in data.shape)
                np.save(
                    self.res / "crops" / f"tm_{ts}.npy",
                    np.asarray(data[z - 16 : z + 16, y - 128 : y + 128, x - 128 : x + 128]),
                )
                self.row("tm", ts, "scores_max", f"{float(np.max(data)):.6f}")
        if not self.dry:
            for f in out.glob("*.mrc"):
                f.unlink()  # 2 GB each


def _median(values: list[float]):
    return f"{float(np.median(values)):.4f}" if values else ""


def _frame_defocus(xml: Path) -> float | None:
    m = (
        re.search(r'<Param Name="Defocus" Value="([-0-9.eE]+)"', xml.read_text(errors="replace"))
        if xml.exists()
        else None
    )
    return float(m.group(1)) if m else None


def _grid_ctf(xml: Path) -> list[float]:
    text = xml.read_text(errors="replace") if xml.exists() else ""
    block = re.search(r"<GridCTF [^>]*>(.*?)</GridCTF>", text, re.DOTALL)
    return [float(v) for v in re.findall(r'Value="([-0-9.eE]+)"', block.group(1))] if block else []


def _aln_rows(path: Path) -> list[list[float]]:
    """AreTomo's .aln alignment rows: SEC ROT GMAG TX TY SMEAN SFIT SCALE BASE TILT."""
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(errors="replace").splitlines():
        if line.startswith("#") or not line.strip():
            continue
        parts = line.split()
        if len(parts) >= 10:
            try:
                rows.append([float(p) for p in parts[:10]])
            except ValueError:  # the local-alignment block after the global rows is not this table
                break
    return rows


def _centre_corr(a: Path, b: Path) -> float:
    """Correlation of the centres of two volumes or images (a 128 x 256 x 256 box, or 512 x 512 in 2-D)."""
    with mrcfile.mmap(a, permissive=True) as ma, mrcfile.mmap(b, permissive=True) as mb:
        da, db = np.squeeze(ma.data), np.squeeze(mb.data)
        if da.shape != db.shape:
            return float("nan")
        if da.ndim == 2:
            y, x = (s // 2 for s in da.shape)
            ca, cb = da[y - 256 : y + 256, x - 256 : x + 256], db[y - 256 : y + 256, x - 256 : x + 256]
        else:
            z, y, x = (s // 2 for s in da.shape)
            ca, cb = (
                da[z - 64 : z + 64, y - 128 : y + 128, x - 128 : x + 128],
                db[z - 64 : z + 64, y - 128 : y + 128, x - 128 : x + 128],
            )
        ca, cb = np.asarray(ca, dtype=np.float64).ravel(), np.asarray(cb, dtype=np.float64).ravel()
        return float(np.corrcoef(ca, cb)[0, 1])


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("res", type=Path)
    ap.add_argument("n", type=int)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    p = Probe(args.res, args.dry_run)
    info = p.card()
    p.kernel_checks(info["compute_cap"])
    series = p.tilt_series(args.n)
    log(f"tilt series: {' '.join(series)}")
    (args.res / "tilt_series.txt").write_text("\n".join(series) + "\n")
    stages = [("motion", series[:2]), ("align", series), ("ctf", series), ("recon", series)]
    for stage, items in stages:
        for ts in items:
            try:
                getattr(p, stage)(ts)
            except Exception as e:  # one stage's failure is a result here; the next stage still runs
                p.row(stage, ts, "error", f"{type(e).__name__}: {e}")
                log(f"{stage} {ts}: {type(e).__name__}: {e}")
    try:
        p.tm(series[0], info["memory_bytes"])
    except Exception as e:  # as above
        p.row("tm", series[0], "error", f"{type(e).__name__}: {e}")
        log(f"tm {series[0]}: {type(e).__name__}: {e}")
    log(f"done: {args.res}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
