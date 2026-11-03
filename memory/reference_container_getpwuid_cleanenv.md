---
name: reference_container_getpwuid_cleanenv
description: Apptainer --cleanenv + LDAP/SSSD cluster → torch.compile crashes with getpwuid uid-not-found; fix by setting USER/LOGNAME + TORCHINDUCTOR_CACHE_DIR in the in-container env prefix
metadata: 
  node_type: memory
  type: reference
  originSessionId: 62ec17db-127f-4b70-a702-c91ee591da78
---

On CBE, `apptainer exec --cleanenv --no-home` strips `USER`/`LOGNAME`, so `getpass.getuser()` falls back to `pwd.getpwuid(os.getuid())`. User identities come from LDAP/SSSD, NOT the bind-mounted static `/etc/passwd`, so the uid can't be resolved inside the container → `KeyError: 'getpwuid(): uid not found: <uid>'`.

`torch.compile` triggers this: its inductor `default_cache_dir()` calls `getpass.getuser()` to build `torchinductor_<username>`. Any container job that compiles a torch model hits it (miss-alignment did, 2026-07-30, `/…/for_<pi>_20260121_Vermicelli/External/job012` — crashed before training started).

Fix (landed in `drivers/miss_align.py` train env prefix): set `USER=<name> LOGNAME=<name>` (short-circuits `getpass.getuser()` before it touches `pwd`) AND `TORCHINDUCTOR_CACHE_DIR=<jobdir>/…` (torch skips `default_cache_dir()` entirely when it's set — see `torch/_inductor/runtime/cache_dir_utils.py:15`). Either alone fixes the crash; both = robust + keeps compile cache writable in the job dir. Derive the name natively via `os.environ.get("USER") or getpass.getuser()` (works outside the container where SSSD is available). Apply the same prefix to any future SIF torch.compile driver (denoise/IsoNet etc.).

**2026-09-29 hypothesis (roadmap 20 §1.1, unverified):** the real cause may be our own host `/etc/passwd:ro`
bind in `container_service.py` — it hides the user entry apptainer injects on LDAP clusters. Test on a node:
`apptainer exec <sif> getent passwd $(id -u)` with and without the bind; if confirmed, drop the bind and the
USER/LOGNAME workaround (keep the cache dirs).
