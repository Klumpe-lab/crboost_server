# Roadmap 03 — Errors, results, and logging: one lightweight idiom

## Design constraints (from maintainer feedback)

- Nothing verbose or industry-grade; the system is early-stage and function shapes will change.
- Centralize/standardize the mechanism that *already exists* (`{"success": bool, ...}` dicts),
  don't impose a foreign one.
- Errors and logs must be lucid and trackable-to-the-file.
- Must not constrain evolution: adding fields to a result must stay trivial.

Given those, the recommendation is **not** a full Rust-style `Result[T, E]` generic (that's the
"industry-grade" trap: it fights Python ergonomics, demands generics plumbing everywhere, and locks
function shapes early). The right size is a **two-helper chokepoint over the existing dict shape**,
upgradeable later if it ever earns it.

## Before → After

**Before:** ~180 hand-built `{"success": ...}` dicts with no agreed failure key (`"error"` ×129,
`"message"` ×33, `"reason"`, `"detail"`, …) — one live bug already shipped from the mismatch
("Failed to start: None"). 213 `except Exception` in services+backend, 164 silent
swallow-and-return-default sites; six distinct failure causes can collapse into one `return None`
(`preview_render.py`). Logging exists but with no format/ownership convention; drivers print with
hand-typed prefixes.

**After:** every service-level outcome is built by `ok(...)` / `err(...)` from one module; failures
always carry `error` (human text) and optionally `code` (stable enum for the few flows where UI
branches on the cause); logs carry `module:line` automatically so every message is trackable to its
file; `except Exception` is allowed only when it *reports* (log + `err(...)`), never bare-swallows.
**Gain:** the class of key-mismatch bugs becomes unconstructable; failures become diagnosable from
the log alone; the CLAUDE.md "never fail silently" rule gets a mechanism instead of a plea.

## The idiom (concrete)

`services/result.py` (~40 lines total):

```python
class ErrorCode(StrEnum):
    NO_COORDS_FOUND = auto()      # migrate the existing string codes as they're found
    NO_JOBS_SELECTED = auto()
    # grows organically; only add a code when a caller *branches* on it

def ok(**data) -> dict[str, Any]:
    return {"success": True, **data}

def err(message: str, *, code: ErrorCode | None = None, **data) -> dict[str, Any]:
    # single place that could also logger.debug the failure origin
    return {"success": False, "error": message, **({"code": code} if code else {}), **data}
```

Deliberately: still a dict (JSON-safe, NiceGUI-safe, zero migration cliff, shape stays free);
payload keys stay ad-hoc (early-stage freedom preserved); the *only* hard contract is
`success` + `error` (+ optional `code`). If, a year from now, typed payloads matter, `ok`/`err` are
the single seam where a `ServiceResult` Pydantic model can be introduced without touching call sites
again. UI consumption rule: failures render `result["error"]`; branching compares
`result.get("code")` against the enum, never against message text
(kills `error == "no_coords_found"`-style string matching, `ui/tomo_dashboard_dialog.py:4242`).

## Logging idiom (same spirit: convention + one config, no framework)

- One `logging.basicConfig`/dictConfig in `main.py` with format
  `%(asctime)s %(levelname).1s %(name)s:%(lineno)d %(message)s` — every record becomes
  clickable-greppable to file:line. Module loggers only (`logger = logging.getLogger(__name__)` —
  already the dominant pattern in services).
- **Exception policy, three allowed forms:**
  1. can't handle → don't catch;
  2. can report → `logger.exception(...)` (full traceback) + `return err(...)`;
  3. genuinely expected-and-ignorable → catch the *narrow* type with a one-line comment saying why.
  Bare `except Exception: pass/return default` fails review; the 164 existing sites get converted
  opportunistically when their file is touched (no big-bang sweep — most are in files other roadmaps
  already rewrite).
- Fallback-with-log for degradable UI reads (`preview_render.py`-style): return
  `err("...", code=...)` variants instead of bare `None` so the UI can distinguish "Pillow missing"
  from "corrupt MRC" — six causes, six messages, one mechanism.
- Drivers keep `print(..., flush=True)` (correct for SLURM logs) but the prefixes (`[SUPERVISOR]`,
  `[TASK n]`) come from Roadmap 04's template-class helpers rather than 8 hand-typed copies.

## Stage 0 — gather first

- Build the full key-usage census per producer/consumer pair (the audit has aggregates; the migration
  wants a table: function → keys produced → consumers → keys read). One grep pass; store alongside
  this file. It will surface any *other* live mismatches like the "None" bug before they're found in
  production.
- Identify the handful of flows where UI genuinely branches on failure cause (known: no-coords-found,
  no-jobs-selected, curation "nothing open") — those seed `ErrorCode`; everything else is text-only.

## Stage 0 record (gathered 2026-08-12)

Full census in `03-stage0-census.md` (producers per module with line numbers, consumers, mismatch
table, silent-drop sites). Headlines: 171 producer sites (not ~180); failure keys are `error` ×107,
`message` ×5 (ALL in `launch_retries` — the live bug), `errors`-list ×2, `detail` ×2 (failure text
under `success: True`). Exactly ONE cause-branching consumer exists
(`tomo_dashboard_dialog.py:4106`, `no_coords_found` — the plan's `:4242` reference was stale), so
`ErrorCode` ships with a single member; "no-jobs-selected" and curation-"nothing open" did NOT earn
codes (nobody branches / it's `success+count:0`). Two parallel idioms flagged for later folding:
drivers' `{"ok": ...}` and per-item `{"reason": ...}` records.

## Stage 1 record (done 2026-08-12)

`services/result.py` landed (`ok`/`err` + `ErrorCode(StrEnum)` with `NO_COORDS_FOUND` only).
Pattern-setter slice converted: `launch_retries` (5 `message` sites → `err()`, kills
"Failed to start: None" at the producer) + `deploy_and_run_scheme` (3 sites). `backend.start_pipeline`
is a pass-through; the panel consumer already renders `error` — contract now holds end-to-end.

## Stage 2 record (done 2026-08-12)

`main.py` log format → `%(asctime)s %(levelname).1s %(name)s:%(lineno)d %(message)s`; exception
policy (three allowed forms, verbatim) + result-idiom rule written into `CLAUDE.md`.

## Stage 3 record (done 2026-08-12)

All producer modules converted to `ok()`/`err()` — 163 sites total, four grouped commits (module
groups rather than strictly one-module-per-commit; each group's consumers checked against the
census before conversion):

- orchestrator + runner (27): includes `stop_and_cleanup`'s `errors`-list shape gaining a joined
  `error` message (the `errors` payload key kept for the panel's success-with-warnings reader).
- backend + project_service + slurm_service + jobs/tilt_filter (52): includes the
  `debug_pipeline_status` missing-`success` fix and dropping two defensive `error: None` keys
  (consumers verified failure-branch-only first).
- templating (41): PyMOL embedded-script `{"success": ...}` strings left untouched (subprocess
  wire format); None-guards added to upstream-error re-wraps.
- session_service (38) + the enum switch: `no_coords_found` now travels as
  `code=ErrorCode.NO_COORDS_FOUND` with real prose in `error`; `tomo_dashboard_dialog` branches on
  the code (StrEnum value keeps wire compat). Variable-success pass-throughs split into explicit
  branches with never-None error text.
- Stragglers converted in the final sweep: `pipeline_deletion_service.preview_deletion` (4),
  `tomo_dashboard_dialog` "no backend" literal (1).

Recurring hazard worth knowing: locals named `err`/`ok` shadowed the helpers in five functions
(`_submit_chain`, `process_volume_async`, `_process_simulated_map`, `send_chimerax_command`,
`extract_pick_list_and_wait`) — all renamed. Sanctioned remaining `"success":` literals:
`result.py` itself, `session_service.py:176` (`data.update` merge over disk-loaded session.json),
the two PyMOL script strings, one docstring. Deferred to stage 4+: driver `{"ok": ...}` idiom,
per-item `{"reason": ...}` records, `DeletionResult` dataclass (dead `error` field), silent-drop
consumer sites listed in the census.

## Stages

1. **Land `services/result.py`** + convert one vertical slice end-to-end as the pattern-setter:
   `deploy_and_run_scheme → backend.start_pipeline → pipeline_builder_panel` (the live-bug path, if
   Roadmap 00 stage 3.2 didn't already patch it — this supersedes that patch).
2. **Logging config in `main.py`** + write the exception policy into `CLAUDE.md` (three allowed
   forms, verbatim from above) so review has a citable rule.
3. **Convert producers module-by-module**, highest-traffic first: `pipeline_orchestrator_service`
   (12 sites) → `pipeline_runner` (23) → `backend.py` (59) → `project_service` (19) →
   `slurm_service` (11) → templating (43). Each module is one commit; consumers of that module
   convert in the same commit (the census from Stage 0 makes this exact).
4. **Silent-swallow triage in the worst files** (`backend.py` 12, `dataset_parsing_service` 11,
   `picks_filter` 9): convert each site to one of the three allowed forms. Skip files that
   Roadmaps 01/02/04 are about to rewrite anyway — they adopt the policy on rewrite.
5. **`ProjectState.load()` hardening** (the six silent-discard blocks): don't refactor the loader
   here (Roadmap 02/the discriminated-union change owns that); minimally, upgrade each
   `log warning + drop` to `logger.exception` + a **visible** load report
   (`state.load_warnings: list[str]` surfaced once in the UI as a toast/badge) so schema drift stops
   silently deleting user curation. This is the CLAUDE.md rule applied to its own worst violation.

## Modern-Python weave-in

- `ErrorCode(StrEnum)` — first `StrEnum` in the codebase; values are the wire format.
- `match result: case {"success": True, **rest}:` reads well at UI consumption sites, but don't
  force it — `if not result["success"]` is fine; use `match` where a site branches on `code`.
- When a helper truly always raises, annotate `-> Never` (`typing.Never`) so type-checkers propagate it.
- If/when `ok`/`err` graduate to a typed `ServiceResult`, it should be a Pydantic model with
  `extra="allow"` — the registry models' strictness (`extra="forbid"`) is right for *entities*,
  wrong for *envelopes* that must stay evolvable.

## Runtime checklist

- Trigger known failure paths and read the toasts: pipeline start with nothing selected; extraction
  on a list with no coords; curation save with no session. Each must show its real message.
- Tail the server log during one pipeline run: every warning/error line must carry `module:line` and
  no traceback may be swallowed into silence.
