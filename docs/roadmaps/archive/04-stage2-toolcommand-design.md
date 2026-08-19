# Roadmap 04 — Stage 2 design: `ToolCommand` + `run_tool()`

Status: DRAFT for maintainer review, 2026-08-13. **No stage-2 code lands before this is approved.**
Grounded in: `04-stage0-census.md` §command-builder (18 builders, injection notes, "what ToolCommand
must support"), the §ASK decisions (2026-08-13), the byte-exact transcripts in
`04-stage2-transcripts.md`, and a fresh read of `container_service.py` / `driver_base.py` /
all 16 drivers. Open decisions for the maintainer are collected in §7.

## 1. The invariants this design must not break

Every tool execution today flows through three string-in/string-out steps:

```
build_*_command(...) -> str
  → wrap_command_for_tool(command: str, cwd, tool_name, additional_binds) -> str
    → run_command[_with_retries](wrapped: str, cwd)   # Popen(shell=True)
```

1. **Flat strings, never argv.** The wrapper `shlex.quote()`s the ENTIRE inner command as the single
   `bash -c` argument (`container_service.py:232-235`), so compound shell inside the string survives
   and is re-parsed in-container. Binary `exec_mode` returns the string unchanged. No argv path
   exists anywhere; `Colors.format_command_log` even regex-parses the emitted string
   (`bash -c '(.+)'$`) for pretty-printing.
2. **Compound shell is load-bearing.** Idempotency guards `test -f X || (A) && B`
   (fs_motion:184), the double-guard chain with in-shell glob probe (ts_import:89-94), the
   defocus-hand mini-script with `$()` capture + pipe-to-grep + `if/else` (ts_ctf:72-79), the error
   shim `echo 'ERROR...'; exit 1;` (ts_alignment:130), in-band env prefixes
   (`TF_FORCE_GPU_ALLOW_GROWTH=true ... cryoCARE_predict.py`, denoise_predict:145;
   `env HOME=... USER=... TORCHINDUCTOR_CACHE_DIR=...`, miss_align:314-315), literal quoted globs
   `'*.eer'` / `--extension '*.tomostar'` as glob-defense tokens.
3. **Per-site quoting policy is part of the bytes.** Exactly 6/18 builders use `shlex.quote`
   (denoise_predict, denoise_train, fs_motion, ts_ctf, ts_import, ts_reconstruct); the relion/pytom
   builders quote nothing. Normalizing quoting changes bytes → all quoting *changes* are quarantined
   in one final flagged commit (roadmap stage-2 text), not sprinkled through migrations.
4. **The parity oracle exists and is fleet-wide.** `run_command` now echoes
   `[run_command] $ <command>` (commit `71a66ff`) — the byte-exact record of what executed,
   container wrap included. This subsumes census echo items #17 (fs_motion 300-char truncation),
   #71 (extract_pick_list lowercase echo), #36/#55 (IsoNet paths never echoed): every driver that
   executes via `run_command` now produces a full-fidelity line. Acceptance for every migration
   commit = byte-identical `[run_command]` lines against `04-stage2-transcripts.md` (§5).
5. **Prime directive** (roadmap): behavior-preserving by construction, one driver per commit,
   runtime-verified before the next; divergences resolve per the stage-0 ledger, ALIGNs as separate
   flagged commits.

## 2. What stage 2 actually consolidates

The duplication is NOT in flag assembly — each tool's flags are irreducible per-driver content.
It is in the **wrap-and-run tail**, repeated at 19 driver wrap call sites (plus 5 more in
services/UI that stage 2 leaves alone) with idiom drift:

- `additional_binds` hygiene: `list(set(...))` (reconstruct_particle — unsorted, nondeterministic
  order in the *Python list*, though the wrapper re-sorts internally so bytes are stable),
  `sorted(set(...))` (subtomo_extraction), set-literal (extract_pick_list).
- tool-name sourcing: 12 hardcoded `tool_name=` literals vs 7 `params.get_tool_name()` sites.
- runner choice: bare `run_command` vs `run_command_with_retries(attempts=3)` (ts_ctf,
  ts_reconstruct), with the retry label conventions ad-hoc.
- cwd threading: the same `cwd` goes to both wrap (bind of `cwd.resolve()`) and run (process cwd)
  at every site — but each site wires it by hand.

Fact established during design (wrap-contract read): the wrapper puts all binds into a `set` and
emits `sorted(binds)`, and it `Path(p).resolve()`s each additional bind itself — so caller-side
dedup/resolve idiom variance is **byte-neutral** and `run_tool()` may normalize it freely.

## 3. Design

### 3a. `run_tool()` — the runner (the real consolidation)

```python
# driver_base.py
def run_tool(
    command: str | ToolCommand,
    *,
    tool_name: str,
    cwd: Path,
    binds: Iterable[str | Path] = (),
    attempts: int = 1,          # 1 = run_command; >1 = run_command_with_retries
    label: str = "",            # retry log label; defaults to tool_name
    timeout: int | None = None,
) -> None
```

Behavior: render `command` if it is a `ToolCommand`; pass `binds` through as strings (no resolve —
the wrapper resolves and sorts); `wrap_command_for_tool(...)`; dispatch to `run_command` or
`run_command_with_retries` with the same `cwd`. Nothing else. The `[run_command]` echo stays where
it is. Raises `CalledProcessError` exactly as today.

Non-goals (explicit): no argv mode; no quoting of the incoming string; no bind *selection* (call
sites still decide which parent dirs to bind); no absorption of the two native `tar` commands in
denoise_train (they bypass wrapping today and continue to call `run_command` directly); no change
to `Colors.format_command_log`.

### 3b. `ToolCommand` — a transcribing builder, not a quoting engine

The roadmap sketch (`.arg()/.flag()/.paths()`) implied a uniform path-quoting verb. The census
evidence rules that out: per-site quoting differs and bytes must hold. So the builder's job is to
make each site's *existing* policy explicit and greppable, while rendering stays a deterministic
`" ".join` in insertion order — no reordering, no dedup, no validation, no implicit quoting.

```python
@dataclass
class ToolCommand:
    parts: list[str]

    def __init__(self, exe: str): ...            # "WarpTools ts_reconstruct" — fused subcommands fine
    def flag(self, flag: str) -> Self             # "--dont_invert"
    def opt(self, flag: str, value) -> Self       # "--angpix", str(value) — bare, no quoting
    def opt_path(self, flag: str, path, *, quote: bool) -> Self
                                                  # quote=True → shlex.quote(str(path));
                                                  # quote=False → str(path) verbatim.
                                                  # REQUIRED keyword: transcribes today's per-site
                                                  # policy visibly; `quote=False` sites double as
                                                  # the greppable injection-hazard inventory.
    def raw(self, fragment: str) -> Self          # escape hatch, appended verbatim: other_args
                                                  # passthrough, fused tokens ("--extension
                                                  # '*.tomostar'"), env prefixes, "-g 0 1 2" splats,
                                                  # colon-split flag pairs, trailing-slash "--o X/"
    def render(self) -> str: return " ".join(self.parts)
```

Why a class at all, instead of keeping hand-joined f-strings + a `q()` helper: the final
quoting-unification flagged commit becomes a change to `opt_path`'s policy in ONE place (flip
defaults, delete overrides) instead of re-touching 18 sites; and `render()` is the natural hook for
the Tier-A `--print-cmd` snapshot harness (FEATURE_recipes.md) without running tools. If the
maintainer prefers the plain-helper route, §7-D1.

Transcription rules for migration commits (byte-parity by construction):

- Existing `shlex.quote(str(p))` → `.opt_path(flag, p, quote=True)`.
- Existing bare `str(p)` / f-string interpolation → `.opt_path(flag, p, quote=False)`.
- Numeric/enum formatting quirks stay at the call site, transcribed exactly:
  `round(float(voltage))`, `int()` truncation, `f"-{eer_ngroups}"` sign-prefix,
  `f"{patch_x}x{patch_y}"` composites, `str()` of computed floats — passed pre-rendered into
  `.opt()`. The builder never re-formats values.
- `params.other_args` verbatim passthrough (reconstruct_particle:97-100) → `.raw()`, keeping its
  "not shell-escaped" comment.
- fs_motion's post-build `warp_command.replace("'*.eer'", f"'{ext}'")` dies: the extension becomes
  an input to the builder (`.raw(f"--extension '{ext}'")`), which produces identical bytes to
  build-then-patch. Trivially diff-reviewable; folded into fs_motion's migration commit.

### 3c. Compound shell stays literal in the drivers

The compound shapes compose AROUND rendered commands, not inside the builder:

```python
create = ToolCommand("WarpTools create_settings")...render()
run    = ToolCommand("WarpTools fs_motion_and_ctf")...render()
warp_command = f"test -f warp_frameseries.settings || ({create}) && {run}"
```

No `guard()`/`chain()` combinators: the guard chain appears in exactly two drivers (fs_motion,
ts_import) with *different* probe expressions, the defocus-hand `$()` mini-script and the alignment
error shim are single-site. Naming single-use shapes is abstraction for its own sake (Rule 2); the
literal f-strings are the most diff-verifiable form. Revisit only if a third guard user appears.

### 3d. Tool-name conversion rides along

The 12 hardcoded `tool_name=` literals become `params.get_tool_name()` in the same per-driver
migration commits (transcript-visible: `get_tool_name()` returns the same strings the literals
hold today). Exception per census #73: `extract_pick_list`'s `tool_name="relion"` literal stays
until the #68 job-identity work (stage 5) gives it a real param class.

`ToolName(StrEnum)` + `get_tool_config` raising on unknown stays where the roadmap put it: a
separate flagged behavior commit, NOT part of stage 2 migrations. **Fact verified while mapping:**
`services/jobs/tilt_filter.py:55` declares `get_tool_name() -> "crboost"`, a name with no `tools:`
entry — it would resolve via the silent terminal fallback `ToolConfig(exec_mode="binary",
bin_path="crboost")` (config_service.py:331). Today this is dead at the wrap layer (the tilt_filter
driver is pure in-process, and only drivers call `get_tool_name()`), so raise-on-unknown breaks
nothing live — but the flagged commit must decide whether "crboost" joins the enum or the method
raises (§7-D3).

### 3e. What migrating one driver looks like (the per-commit recipe)

1. Rewrite `build_*_command()` to construct a `ToolCommand` (transcribing quoting per §3b) and
   return `.render()` — or return the `ToolCommand` and render at the call site.
2. Replace the site's wrap+run tail with one `run_tool(...)` call (`attempts=3` where
   `run_command_with_retries` is used today; keep today's `label` strings).
3. Convert that driver's `tool_name=` literals (§3d).
4. Nothing else moves — staging, enumeration, status writes, aggregation are stage-3 scope.
5. Acceptance per §5, then runtime checklist per the roadmap before the next driver.

## 4. Coverage: which builders, which commits

Order follows transcript availability first, then census risk order. One driver per commit.

| commit | driver | builders touched | transcripts |
|---|---|---|---|
| 1 | ts_reconstruct | build_reconstruct_command (cleanest, shlex-all) | ✅ job008 |
| 2 | ts_ctf | build_ctf_command + run_defocus_hand_globally (mini-script composes rendered commands) | ✅ job007 |
| 3 | ts_alignment | build_alignment_command (method dispatch: two ToolCommands, one function) | ✅ job006 |
| 4 | fs_motion_and_ctf | build_warp_commands (guard chain literal; `.replace` patch dies) | ✅ job002 |
| 5 | ts_import | build_ts_import_commands (double-guard literal) | ✅ job003 |
| 6+ | template_match, extract_candidates, subtomo_extraction, reconstruct_particle, class3d, denoise_predict, denoise_train, miss_align, extract_pick_list | remaining 13 builder sites | ❌ need runs or Tier-A snapshots (§5) |

array_job_base's 5 SLURM-plumbing sites (sbatch script build, argv-safe squeue/scancel/sbatch) are
NOT stage-2 scope — the sbatch-script `driver_cmd` duplication with the orchestrator is stage 5's
"single-source the driver-invocation command string".

## 5. Acceptance: byte-parity procedure

- **Types with live transcripts (commits 1-5):** after migration, user re-runs the job type on
  `stage4refactor_demo` (requeue is cheap: settled TS are skipped, but a fresh project run gives
  full task coverage) and diffs the new `[run_command] $` lines against `04-stage2-transcripts.md`.
  Any byte difference fails the commit.
- **Types without transcripts (commits 6+):** two options, §7-D4. Recommended: build the Tier-A
  `--print-cmd` snapshot mode from FEATURE_recipes.md as part of stage 2 (driver flag that builds
  and prints all commands, then exits before execution — print-and-exit, safe on the headnode),
  snapshot BEFORE migration on the old code, migrate, snapshot again, diff. This unblocks the 9
  uncovered types without waiting for cluster runs, and the blessed snapshots seed the regression
  harness. Live-run verification still happens per the roadmap's runtime checklist, just not as
  the byte-parity gate.
- The old-March transcripts in the census stay useful as shape references only; the census itself
  flags the ts_ctf defocus-hand fragment as quoting-ambiguous in them. `04-stage2-transcripts.md`
  is the canonical corpus.

## 6. Deferred / flagged-commit backlog (accumulated by this stage, landed after it)

1. **Quoting unification** — flip `opt_path` policy in one place; delete per-site `quote=False`
   where the maintainer approves; the injection-hazard inventory = grep `quote=False`. One flagged
   commit, per roadmap.
2. **`ToolName(StrEnum)` + raise-on-unknown** — with the `crboost` landmine resolved (§3d).
3. **`run_command_with_retries` doesn't forward `idle_timeout`** — pre-existing quirk (retried
   commands get the default 45-min idle watchdog regardless of caller intent). Keep for now;
   candidate ALIGN when someone needs it.
4. Bind *selection* consolidation (which parents to bind is duplicated reasoning at call sites) —
  only if stage 3's `ArrayDriver` hooks make a natural home for it; not stage 2.

## 7. Decisions for the maintainer (blocking stage-2 code)

- **D1 — builder shape.** Tagged-verb `ToolCommand` with mandatory `quote=` on `opt_path`
  (recommended: makes the later unification a one-place change, gives `--print-cmd` a hook) vs
  plain `q()`-helper + hand-joined f-strings (smaller, but unification re-touches 18 sites).
  §3b as written assumes the first.
- **D2 — compound shapes.** Stay literal f-strings in drivers (recommended, §3c) vs shared
  `guard()`/`chain()` combinators.
- **D3 — `crboost` under raise-on-unknown.** `get_tool_name() -> "crboost"` is currently dead at
  the wrap layer (§3d). When the flagged StrEnum commit lands: add a `tools:` entry, add it to the
  enum as a declared-native tool, or make tilt_filter's `get_tool_name()` raise? Cosmetic today,
  but the enum should not silently carry names the config cannot resolve.
- **D4 — parity gate for the 9 uncovered job types.** Tier-A `--print-cmd` snapshots (recommended,
  §5) vs live runs of every type before its migration vs migrating only covered types now and
  parking the rest. If Tier-A: it becomes stage-2 scope and its print-and-exit flag needs a name
  (`--print-cmd` / env `CRBOOST_PRINT_CMD=1`).
- **D5 — `run_tool` accepting `str | ToolCommand`.** Accepting plain `str` keeps the compound-shell
  drivers honest (§3c composes into a string anyway); accepting only `ToolCommand` forces `.raw()`
  wrapping of composed strings. Recommended: accept both, `str` is the composed-compound case.
