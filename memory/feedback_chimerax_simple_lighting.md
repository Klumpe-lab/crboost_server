---
name: feedback_chimerax_simple_lighting
description: "ChimeraX .cxc figures should always use \"lighting simple\", never soft/full — user doesn't want fancy graphics"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 63ba2e82-63fe-4e3f-a277-987798e2d0d4
---

In every generated ChimeraX `.cxc` script (412 map/mask comparison figures etc.), always set `lighting simple`. Never `lighting soft` or `lighting full`.

**Why:** the user explicitly does not want fancy/expensive graphics. Soft/full lighting (shadows, ambient occlusion) is slow to render — especially over VNC on a CPU node where GL is software Mesa ([[reference_chimerax_vnc_perf]]) — and adds nothing to a side-by-side density/mask shape comparison.

**How to apply:** use `lighting simple` in the setup block of every `.cxc`; drop any `lighting soft`/`lighting full`. Applies going forward to all new/edited scripts.
