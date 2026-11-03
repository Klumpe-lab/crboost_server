---
name: reference-nfs-attr-cache-registry
description: /groups/klumpe/crboost_data is NFSv3 with default attr cache — headnode stat() can show the OLD mtime up to ~60 s after a compute node replaced a file; open() revalidates
metadata:
  node_type: memory
  type: reference
  originSessionId: 5a6d0a5c-bed2-4a6d-8f36-5e4911e696a4
  modified: 2026-10-08T17:49:30.877Z
---

`/groups/klumpe/crboost_data` (storage.vbc.ac.at:/ifs/…) is mounted NFSv3 with no `actimeo`, so the Linux defaults
apply (acregmax/acdirmax 60 s). After a driver on a compute node `os.replace`s a file, the headnode's `stat()` can keep
returning the old mtime for up to about a minute; `open()` + read gets the new contents (close-to-open).

Consequence: `get_registry_for` re-syncs the cached TiltSeriesRegistry only when `stat(index.json)` mtime moves, so a
server reader right after a driver write sees the stale registry (roadmap 06 defect j, first seen on DL run 003,
2026-09-30). A one-shot read at "job landed" time must re-read by content, not by mtime. Polling readers heal
on their own.

**How to apply:** when debugging "UI shows old data until reload" after a SLURM job writes project files, suspect
this before suspecting dirty in-memory state. Related: [[project-dl-tilt-filter]].

**Node-to-node too (2026-10-05):** an afterok consumer's `.exists()` on clip-g2-0 missed a file its producer wrote
on clip-g2-3 10 s earlier (AGG412 SK job014 "optimisation_set.star not found"; clip-g2-0 had run the producer's
tasks minutes before). 1 of ~20 handoffs. A transient "not found" on a file that exists → Run again; retry-on-check
fix proposed as roadmap 29 C2.
