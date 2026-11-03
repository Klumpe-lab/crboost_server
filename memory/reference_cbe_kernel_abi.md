---
name: reference_cbe_kernel_abi
description: "CBE/CLIP compute+login nodes run CentOS 7 kernel 3.10 — modern Ubuntu/manylinux GUI libs tagged ABI >=4.11 won't load; strip .note.ABI-tag"
metadata: 
  node_type: memory
  type: reference
  originSessionId: 044b0f85-9282-4c4e-930e-316212cb02ca
---

CBE/CLIP @ VBC nodes (login `clip-login-1` AND partition-`c` compute nodes, verified via
`srun -p c uname -r`) run **CentOS 7, kernel `3.10.0-1160.108.1.el7.x86_64`**.

**The trap:** glibc's dynamic loader refuses to load any shared library whose `NT_GNU_ABI_TAG`
ELF note requires a kernel newer than the running one. Modern Ubuntu-22.04 / manylinux libraries
(e.g. Qt6, which needs Linux **4.11** because of `statx`) are tagged ≥4.11, so on a 3.10 kernel
they fail with the **misleading** error:

```
libQt6Core.so.6: cannot open shared object file: No such file or directory
```

— even though the file IS present, readable, on the loader path, and in the ld cache. The tell:
`ldconfig -p | grep libQt6Core` prints `(libc6,x86-64, OS ABI: Linux 4.11.0) => …`. The "OS ABI"
line is the kernel requirement, and it exceeds 3.10. `--nogui` / CLI paths work because they never
load the high-ABI libs.

**Fix (works inside an Apptainer build):** strip the note section, then rebuild the cache:
```
apt-get install -y binutils
find <tree> \( -name '*.so' -o -name '*.so.*' \) -type f | while read -r so; do
  objcopy --remove-section=.note.ABI-tag "$so" 2>/dev/null || true
done
ldconfig
```
glibc has runtime fallbacks for the few newer syscalls (statx → fstatat) the tag came from, so the
libs run fine on 3.10 once the gate is removed. This is the standard "modern wheels on RHEL7" fix.

**A SIBLING el7 trap — page-alignment, NOT the ABI tag (found 2026-06-07).** Some modern Python wheels
bundle a native lib the same el7 loader rejects with a DIFFERENT error, which the objcopy strip above
does NOT fix (it's a separate ELF check — PT_LOAD segment alignment, not `.note.ABI-tag`):
```
ImportError: libscipy_openblas-c128ec02.so: ELF load command address/offset not page-aligned
```
Hit ChimeraX 1.9's bundled scipy (≥ the `scipy-openblas` packaging, built for glibc ≥ 2.28). On CBE it
SILENTLY killed every ArtiaX command — ArtiaX imports scipy lazily on first use, the ImportError vanished
into ChimeraX's (separately-broken) Log panel, so commands just "did nothing" while `open 4ug0` (no scipy)
worked. **Fix = a different WHEEL, not ELF surgery:** pin the **manylinux2014** (el7-built, page-aligned)
build — `pip install --force-reinstall "scipy==1.11.4"` (numpy 1.26.4 was fine as-is), placed AFTER any
objcopy sweep so the fresh el7 wheels are NOT stripped. Prove headlessly: `apptainer exec "$SIF" <chimerax
python> -c "import scipy.linalg"` fails → OK after the downgrade. **General rule for this cluster: prefer
manylinux2014 wheels; newer manylinux_2_28 native libs may load-fail with EITHER the ABI-tag OR this
page-align error.**

First hit building the ChimeraX+ArtiaX curation appliance ([[project_artiax_bridge]],
`containers/chimerax_artiax/chimerax_artiax.def`). Applies to ANY future container that bakes a
modern GUI/Qt6 toolchain for this cluster. Cluster container areas:
`/groups/klumpe/software/containers/{defs,sifs}`.
