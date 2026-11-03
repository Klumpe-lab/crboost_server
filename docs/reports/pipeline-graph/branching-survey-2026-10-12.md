# Survey — from a linear roster to a branching, visual pipeline (2026-10-12)

**Why.** Two needs keep coming up: A/B comparisons on one dataset (a second preprocessing chain, other denoising
or augmentation, other exclusions), and several particle species from one dataset that need different
processing. Both are branches: chains of jobs that start from one upstream job. The question is how far the
scheduler/orchestrator and the browser (NiceGUI) are from a simple branching, visual way of working, and
whether CryoSPARC's model is the one to copy.

**How.** Read-only sweep of the code on branch `species_driven_io`: orchestrator, path resolution, drivers,
registry, roster, read-side views, and the installed NiceGUI 3.0.3. CryoSPARC's Workflows guide
(guide.cryosparc.com/application-guide/workflows) for comparison. Nothing was run. Not a plan; §5 proposes one.

Words used here:
- **branch** — a chain of jobs that starts from an existing job and sits beside the jobs already reading it,
  e.g. missAlign → tsCtf__2 → tsReconstruct__2 beside tsCtf → tsReconstruct;
- **lane** — one column of the proposed graph view, holding one branch;
- **external input** — a producer that is not a pipeline job: imported tomograms, an aggregation merge, a pick
  list, a particle set (`is_synthetic_producer`, `services/path_resolution_service.py:223`).

---

## 0. In short

- **The engine already branches.** Jobs form a DAG of typed inputs and outputs, are submitted as a SLURM afterok
  graph and reconciled per job. Outputs are kept per instance on disk and in the registry. A second chain works
  today (roadmap 29 §6). What is linear is the front: the roster groups by job type, Run takes the whole
  roster, and the project is locked while anything runs.
- **The simplest branchy version needs no node editor.** (1) A graph view that draws the edges the resolver
  already computes; (2) branch verbs on a node: add after, clone, clone from here onto another parent;
  (3) inputs fixed at submit, and running a branch while another runs; (4) read-side views that follow the
  chosen branch instead of "the first job of each type".
- **The browser is not the hard part.** Plain NiceGUI is enough: job cards as ordinary elements, one inline
  SVG for the edges, layout computed in Python. The repo already uses the needed idioms. A drag-to-wire editor
  (Vue Flow) is possible later as the repo's first custom component, but buys little, because wiring is typed
  and mostly automatic.
- **Paradigm: stages × lanes, not a free canvas.** Rows are depth in the graph, columns are lanes; the trunk is
  the first lane and each branch opens one; species lanes take the species colour. Branches are explicit
  (pinned) by construction, which removes the ordering rules of the roadmap 29 recipe.
- **The real model gap is project-wide selection, not the graph.** Which tilt series and which tilts are in
  play is project state, applied by every job when it starts. Two branches cannot differ on it. The cryo-ET
  answer is CryoSPARC's: make selection a job (a TS-subset node); per-branch tilt verdicts need the tilt filter
  to emit an output first.
- **Size:** about 1.5k lines over five stages (§5). Stages 1–2 are new files and low risk; stage 3 touches the
  run path and is the risky one.

---

## 1. What we have

### 1.1 The engine is already a DAG

| Piece | Where | What it gives a branching pipeline |
|---|---|---|
| Typed I/O slots | `services/io_slots.py` (`JobFileType`, `InputSlot.accepts`, `OutputSlot`) | Which job can read which, without hand wiring |
| One answer per input | `PathResolutionService._select_source`, `path_resolution_service.py:707` | Override if set, else automatic selection; the same answer feeds paths, edges and the I/O tab |
| Automatic selection | `_auto_select`, `:1185` | Ranks own species > preferred type > job over external input > on the roster. Two producers at the top rank tie: nothing is chosen and the I/O tab asks (`:738-740`) |
| Explicit wiring | `source_overrides`, keyed `jobtype:instance:<id>` (`instance_override_key`, `:193`) | A pin survives job-dir reallocation |
| Pin on ambiguity | `pin_sources_made_ambiguous`, `:1389-1416`, called by `add_instance_to_pipeline` (`ui/pipeline_builder/pipeline_builder_panel.py:331`, `:414`) | Adding a second producer keeps existing readers where they were |
| The edge list | `resolve_slot_edges`, `:398` → `SlotEdge(producer, consumer, slot, output, explicit)` | The graph to draw, including which wires were set by hand |
| Loop checks | `find_cycle` (`:140`), `cycle_with_override` (`:451`) | A proposed wire can be validated before it is set |
| Refinement following | `JobSpec.refines` (`services/jobs/spec.py:81`), `_follow_refinements` (`:1240`) | missAlign stands in for the alignment it refines |
| Submission | `_submit_chain`, `services/scheduling_and_orchestration/pipeline_orchestrator_service.py:474` | Allocate → resolve → write; Kahn toposort (`:28`); `--dependency=afterok` per edge |
| Gating on a live upstream | `live_after`, `:593-598` | A consumer can be submitted while its producer is queued or running (used today only by Approve's `submit_parked`) |
| Status | `reconcile_afterok`, `pipeline_runner.py:406` | Per job, not per run; `pipeline_active` is only "any job live" (`:528-537`) |
| Per-job cancel | `pipeline_runner.cancel_job`, `:1420` (job tab, `job_tab_component.py:388`) | Dependents get `DependencyNeverSatisfied` and are scancelled, so cancelling a job stops its branch |
| Outputs per instance | registry `outputs` keyed by instance id (`services/tilt_series/models.py:26-28`); stars in each job dir | Reconstructions A/B and denoises X/Y coexist on one tilt series |
| Frozen finished jobs | `is_job_frozen`, `ui/pipeline_builder/config_tab.py:11-16` | RUNNING/SUCCEEDED params read-only, as in CryoSPARC |
| Lineage precedents | `ui/components/recon_compare.py:39-71`, `services/particles/analytics_store.py:73-103`, particle-set `ParentRef` (`services/particles/particle_sets.py:66-91`) | Code that already follows recorded edges instead of "first of type" |

### 1.2 Where it is linear: run and edit

| What | Where | Effect on branches |
|---|---|---|
| Run is refused while any job of the project is live | `_deploy_locked`, `pipeline_orchestrator_service.py:175-176` | During a 2–5-day missAlign (roadmap 29 §6) nothing else in the project can start |
| Adding, removing and species-adding jobs is refused while live | `pipeline_builder_panel.py:114`, `:171`, `:293`, `:465`; roster `+`/`×` hidden, `pipeline_roster.py:597`, `:604`, `:718`, `:732`; "Pipeline running — adding jobs is paused.", `ui/species/jobs_tab.py:135` | A branch cannot even be staged while another runs |
| Run = every unfinished job on the roster | `pipeline_orchestrator_service.py:188-195`; the UI passes the whole roster, `pipeline_builder_panel.py:613-617` | No "run just this branch"; the roadmap 29 recipe relies on everything else being finished |
| Deploy rewrites roster membership with the run selection | `pipeline_orchestrator_service.py:183` | Harmless for a whole-roster Run; a subset Run would shrink `pipeline_order`, which automatic selection ranks by (`path_resolution_service.py:1220`) |
| Deploy clears the review hold | `pipeline_orchestrator_service.py:185` | A second, concurrent run would release the first run's tilt-filter parking |
| Drivers re-resolve their inputs when they start | `drivers/driver_base.py:117-139` (a schemer-era guard against mispredicted job numbers) | A queued job reads whatever the project says at start, not what Run resolved; a tie by then is fatal (`:131-133`). Editing while running would change queued jobs |
| Queued jobs stay editable | `is_job_frozen` freezes RUNNING/SUCCEEDED only; drivers reload `project_params.json` at start | Same: an edit made while queued is what runs |
| New instances start from code defaults | `next_instance_id`, `ui/pipeline_builder/pipeline_constants.py:49`; `ensure_job_initialized`; no clone anywhere | "Same settings except X" is retyped by hand |
| Instance names are `type__N` | `InstanceId`, `services/models_base.py:126` | `tsCtf__2` says nothing about the branch. `display_label` exists (`services/jobs/_base.py:65`, shown by `ui/ui_state.py:180-184`) but nothing sets it |
| The roster groups by job type | `RosterWidget.render`, `pipeline_roster.py:353-447` (phase → type → instances under a count header) | Lineage is invisible: tsCtf__2 and tsReconstruct__2 sit under different headers with nothing linking them |

### 1.3 Wiring and bookkeeping that misfire with two chains

| What | Where | Effect |
|---|---|---|
| Adding missAlign moves every un-pinned consumer of the alignment to it | `_follow_refinements`, `path_resolution_service.py:1240-1302`; a refiner creates no tie, so `pin_sources_made_ambiguous` pins nothing | A tsCtf meant to stay on the raw alignment must be pinned by hand (roadmap 29 §6: "Don't re-run the baseline tsCtf after adding a missAlign") |
| Two refiners of one input: the most recently deployed wins | `:1286-1300`, a log warning only | Sibling branches converge on one refiner |
| An active aggregation merge rewires every optimisation-set input, on every job add | `apply_aggregation_overrides`, `services/aggregation/extraction.py:255-330`, called from `pipeline_builder_panel.py:412` | No species or branch filter, finished jobs included |
| Each input slot is chosen on its own | e.g. tsReconstruct reads two slots from tsCtf and its settings from alignment (`services/jobs/ts_reconstruct.py:29-40`) | Nothing checks that one job's slots share a lineage; a half-pinned job can read two branches |
| missAlign's walltime estimate reads the first alignment on the roster | `planned_stack_apix`, `services/jobs/miss_align.py:527-539` | Wrong stack pixel size when two alignments differ in `rescale_angpixs` |
| Delete's "what breaks downstream" preview reads `default_pipeline.star` | `preview_deletion`, `pipeline_deletion_service.py:323-370`; dialog `pipeline_roster.py:773-792` | Afterok jobs are not in that file, so the dialog always says "No downstream jobs will be affected." Deletion never cascades (`project_service.py:178-252`) |
| Protocol stage identity is `{job}[__{species}]` | `services/protocols/schema.py:90-94`; export drops numeric suffixes (`export.py:51-66`); apply refuses duplicates (`apply.py:64-68`) and never applies explicit wiring (`apply.py:326-331`) | A project with tsCtf and tsCtf__2 cannot be saved as a protocol that applies |
| Job numbers follow type order | `_submit_chain` pass 1 walks the type-sorted run list (`pipeline_orchestrator_service.py:514-558`) | `job012` vs `job013` says nothing about the branch; refinement following still ranks by it |
| Registry writes are whole-file, last writer wins, across processes | `registry.py:353-384`; the lock is per process (`:74`); every preprocessing supervisor loads, ingests and saves (e.g. `drivers/ts_ctf.py:267-284`) | Two branches aggregating at the same moment can drop one's outputs for the tilt series they share |

### 1.4 What decides which data is in play: project-wide

Job outputs are per instance (§1.1). What selects the data is not, and drivers read it when they start:

| State | Where it lives | Who applies it | Two branches |
|---|---|---|---|
| Tilt-series exclusion | `TiltSeries.is_excluded`, `services/tilt_series/models.py:323`, set from the Journey | Every array supervisor at start: `apply_exclusions`, `drivers/array_job_base.py:328-357` → `.skip` markers, dropped from the job's output star | A cannot drop series that B keeps |
| Tilt verdict | `Frame.is_filtered_out`, stamped by the one tilt filter per project (Approve or DL auto); the filter has no output (`OUTPUT_SCHEMA = []`, `services/jobs/tilt_filter.py:119`) | Alignment at start: `registry.filtered_out_frame_ids()`, `drivers/ts_alignment.py:220` | One verdict for every alignment; an unapproved review parks every alignment of the run (`pipeline_orchestrator_service.py:407-427`); the verdict locks once any alignment runs |
| Species template and mask | `species_registry`; snapshot onto TM at creation (`pipeline_builder_panel.py:349-404`) | TM falls back to the species' current selection when its fields are empty | Mostly per job already |
| Pick lists | `state.pick_lists`, keyed (species, tomogram, slug) | Extraction uses the species' first candidate-extraction job, else the first tsReconstruct (`aggregation/extraction.py:158-175`) | Lists don't record which tomograms they were picked on |
| Active merge | `active_merge_slug` | `apply_aggregation_overrides` (§1.3) | Every chain |
| Imported tomograms | one record, replaced on each commit (`project_state.py:757`) | Offered as a source | One set per project |
| Microscope / acquisition | `state.microscope`, `state.acquisition` | fsMotion, tsImport, tsCtf, tsReconstruct, TM at start | Dataset facts; fine shared |

Per job and fine: refined alignments (missAlign writes its own outputs and registry entry), reconstructions,
denoised tomograms (though a denoise output does not record which reconstruction it denoised,
`models.py:195-205`), candidates, curated `*_filtered.star`, and particle sets (immutable, with parents).

### 1.5 Views that show one version of a result

`find_job_by_type` (`services/dashboard_data.py:59-67`) returns the first job of a type in insertion order. A
second branch is invisible in:
- the Journey's per-TS sections (`ui/tomo_dashboard_dialog.py:1386`, `:1521`, `:1605`, `:1716`, `:1734`,
  `:1997`) and its stage pills (`dashboard_data.py:704-719`, `break  # one job per array stage`);
- the pixel-size chain (`services/pixel_chain.py:99-171`), tilt previews (`ui/tilt_previews.py:355-364`), Warp
  hand and reconstruction map (`dashboard_data.py:418`, `:898`);
- tomogram geometry (`services/visualization/tomo_geometry.py:133-147`), which feeds the gallery, the de-novo
  pick frame and list extraction;
- species views that take the first candidate-extraction / subtomo / TM job of a species
  (`dashboard_data.py:793-825`, `aggregation/extraction.py:55-83`, `ui/particles/pick_viewer.py:2150-2173`);
- the template workbench's tomogram pixel size: the first job whose type contains "reconstruct", which also
  matches reconstructParticle (`ui/template_workbench.py:408-415`).

Correct for singletons (import, tilt filter). Already lineage-aware: reconstruction compare, analytics
(`analytics_store.py:73-103`), and the denoise list in the Journey (`tomo_dashboard_dialog.py:2257-2282`).

---

## 2. What the browser can do (NiceGUI 3.0.3)

Bundled: Vue 3.5.22 (full build, template compiler included), Quasar 2.18.5, ECharts 6.0.0, mermaid 11.12.0,
plotly.js 3.1.1. Paths below are inside `venv/lib/python3.11/site-packages/nicegui/`.

| Option | Interaction back to Python | Fit |
|---|---|---|
| Job cards as ordinary NiceGUI elements, absolutely placed; one inline SVG under them for the edges (`ui.html(svg, sanitize=False)`) | Native `.on('click')`, `.on('contextmenu.prevent')`, `ui.menu`; status via the existing `BoundStatusDot` binding, so ticks don't rebuild | **Recommended.** No JS of our own; follows the CLAUDE.md reactivity rules |
| Whole graph as one SVG string in `ui.html`, delegated events | `closest('[data-job]')` + `emit`, as the gallery already does (`ui/tomo_gallery.py:621-622`, `TILT_CLICK_JS` at `ui/tilt_previews.py:71-74`); in-place status patch as `_PATCH_JS` (`tilt_previews.py:1621`) | Works; any structural change replaces the whole innerHTML (hover and an in-flight click are lost), so gate it with `FingerprintedView` |
| `ui.echart` graph series, `layout: 'none'`, x/y from Python | `on_point_click` gives node vs edge (`elements/echart.py:51-78`); any chart event as `chart:<name>` incl. `contextmenu`, `mouseover` | Pan/zoom free; node visuals limited to symbols and labels |
| `ui.mermaid` | No node events; only mermaid's own `click … call` with `securityLevel: 'loose'` → `ui.on` | Re-renders the whole SVG on every change; no |
| `ui.interactive_image` (blank SVG canvas) | Pointer events carry only the innermost shape id and coordinates | Awkward |
| Custom Vue component (`ui.element` subclass, `component='graph.js'`, `esm=`/`dependencies=`) | `this.$emit(...)` → `.on(...)`; `run_method` back | The repo has none yet. URLs are content-hashed and cached a year, keyed at class definition, so edits need a restart; for an `esm=` directory the key is the directory mtime, so editing a file inside keeps a stale URL (`element.py:116-121`) |
| Vue Flow (drag-to-wire editor) as a custom component | Full node-editor events | Needs a Node build with `vue` external and a shim mapping `vue` → `window.Vue` (NiceGUI ships no `vue` import-map entry); a second Vue copy breaks it. Drawflow is UMD and needs a wrapper; LiteGraph is a poor fit |

Layout: for at most ~60 nodes, compute it in Python (§5 stage 1). No layout library needed.

**Side finding (read in NiceGUI's source, not run).** `ui.add_head_html` without `shared=True`, called before the
app starts, goes to a pseudo client that is never served (`functions/html.py:11-17`, `context.py:18-25`,
`client.py:93`, `:131`). `main.py:187` (the `main.css` link, fonts) and `ui/main_ui.py:26` (the `<style>` block)
do exactly that. That would explain the "stale shell" CSS that CLAUDE.md warns about: the rules never reach the
page at all. Check: search the page source for `main.css?v=` or `cb-pulse`; if absent, `shared=True` on those two
calls delivers them.

---

## 3. CryoSPARC, for comparison

How it works:
- Every operation is a job. A job is built (inputs dragged onto its slots), then queued; a queued job waits for
  its parents. A finished job never changes; to change a parameter you clone it.
- Subsets are jobs: Select 2D, Curate Exposures, Remove Duplicates each output a new subset. Nothing edits
  shared state.
- The tree view draws the jobs of a workspace and their connections; workspaces group jobs.
- A **Workflow** is a saved selection of connected jobs with their parameter values (each visible or locked,
  with notes). Jobs outside the selection that feed it become *parent connections*, bound when the workflow is
  applied; a parent can be swapped for any job with the required outputs. Apply creates the jobs, optionally
  queued; *Repeat* applies again at once "so you can create divergent branches quickly". Exported as JSON.
- Limit stated in the guide: connections must exist while jobs are being built; a job that creates its
  outputs only while running is disconnected from jobs a workflow adds after it.

| CryoSPARC | crboost today |
|---|---|
| Finished jobs immutable; clone to change | Finished jobs frozen; no clone |
| Inputs fixed once built | Inputs re-resolved when the driver starts (§1.2) |
| Wires dragged by hand | Automatic selection + pins in the I/O tab |
| Queue one job; it waits for its parents | Run = every unfinished roster job; refused while anything runs |
| Subsets are jobs | Exclusions are project state (§1.4) |
| Tree view | Roster grouped by job type |
| Workflow = sub-graph + parameters + parents bound at apply | Protocol = a whole pipeline, applied only at project creation |
| Workspaces | None; the Species page lists one species' jobs |

Worth taking: inputs fixed once submitted, queue-a-job semantics, clone, clone a chain onto another parent
(Workflow apply + Repeat, without the template registry), a read-mostly tree view with external inputs drawn
as parents, subsets as jobs.

Not worth taking: the drag-to-wire builder, workspaces, a template registry. Our wiring is typed and mostly
automatic, so dragging wires is the least of the friction; protocols stay a registry of known-good flows, not
per-dataset templates (maintainer, 2026-09-09).

---

## 4. A different paradigm for cryo-ET?

Our citizens differ from CryoSPARC's:

| | SPA / CryoSPARC | crboost |
|---|---|---|
| Items | exposures and particles inside result groups that flow through jobs | tilt series with an identity across jobs (the registry); every job is a per-TS array that can partly succeed |
| Selection | subset jobs | TS exclusion and tilt verdicts: project state applied at start (§1.4) |
| Multiplexing | separate chains | species: one tomogram set feeds N particle chains, each with its own template, mask and symmetry |
| Refinement | low-level result overrides | `refines`: a job stands in for an upstream output and un-pinned consumers follow |
| Human steps | interactive jobs | the tilt-filter review parks alignment; picking and curation per tomogram as pick lists |
| Variants | rare | raw vs denoised (cryoCARE, IsoNet) tomograms read by different consumers; segmentations later |
| Other datasets | import result groups | aggregation merges, pick lists, particle sets as external inputs |
| Loops | refine → reconstruct | particles → an M-style refinement → re-reconstruct → re-pick |

What that suggests:

1. **Draw stages × lanes, not a free canvas.** Rows are depth from the import (longest path), columns are
   lanes. The trunk is the first lane; when a job gets a second consumer chain, that chain opens a lane to the
   right; species-bound jobs sit in lanes of the species colour. It reads top to bottom like the roster and like
   `git log --graph`. No layout engine, and it stays put as jobs are added. It is also roadmap 27's grid seen
   from the other side: the graph is the jobs axis (each card shows its per-TS counts), the Journey the TS axis.
2. **Design for the places cryo-ET branches:** TS subset, tilt verdict (later), alignment or refinement,
   CTF/reconstruction settings, denoising, species/template. "Branch here" works everywhere, but those are the
   ones to make one click.
3. **Inputs are fixed once a job is submitted.** Today a queued job re-resolves its inputs at start, and adding
   missAlign moves un-pinned consumers to it (§1.2, §1.3). Record each job's chosen sources when it is submitted
   and let the driver use the submitted paths. Jobs not yet submitted keep automatic selection, so the drop-in
   refinement still works for a fresh chain. Jobs created by a branch verb are pinned at creation. This retires
   the ordering rules in roadmap 29 §6 ("must come before step 2"; "don't re-run the baseline tsCtf").
4. **Loops need explicit wires and a depth layout.** An M-style refiner (tool survey §2) consumes particles and
   produces a refined alignment. The tsCtf and tsReconstruct after it are later in the graph but earlier in
   `PIPELINE_ORDER`, so automatic selection rejects them ("not upstream", `path_resolution_service.py:245-247`)
   and refinement following never fires (`:1273`). Pins can wire them; a layout by job type cannot place them;
   a layout by depth can.
5. **Selection is a job, not an edit.** Project-wide TS exclusion stays for "junk for everyone". A per-branch
   subset is a cheap inline job that writes its input star without the chosen tilt series; downstream branches
   from it, and every array supervisor already drops absent series. Tilt-level subsets per branch need the tilt
   filter's verdict to become an output that alignment reads (today a registry stamp, by design: it keeps
   wiring independent of when the filter is committed, `drivers/ts_alignment.py:211-216`).
6. **The read side follows the lane.** The Journey and friends take the first job of each type (§1.5). Let a view
   pick a lane (default: the trunk) and walk the edges up from its last job, as `recon_compare` and the
   analytics store already do.
7. **Name lanes by what they make**, e.g. "tomograms · missAlign paper · bin 4": one label per branch action,
   written to the `display_label` of its jobs.
8. **Keep the project as the unit.** No workspaces: lanes and a species filter do that job. Merges, pick lists
   and particle sets are drawn as external inputs at the top; particle sets already carry their parents.

---

## 5. The simplest version — proposed stages (nothing built)

Each stage is committable on its own; 1 → 2 is the natural order, 3 is independent and could go first if the
locked project hurts most right now.

**Stage 1 — Graph view, read-only, plus names** (~550 lines, low risk)
- A seventh workspace view, `View.GRAPH` (`ui/routing.py:37`), built lazily like the gallery (`_switch_to`,
  `ui/workspace_page.py:124`), with a rail button beside Journey and Tomograms.
- `services/pipeline_graph.py`, pure and testable without NiceGUI:
  - nodes: roster jobs, finished jobs taken off the roster (dimmed), external inputs;
  - edges: `resolve_slot_edges()`, plus a display-only pass that keeps the external-input edges it skips today
    (`path_resolution_service.py:433-438`) and marks unresolved or tied inputs (from `_select_source`'s
    `problem`);
  - layout: rows by longest path, lanes by a walk in topological order where the first child keeps its
    parent's lane and later children open new ones.
- `ui/pipeline_graph_view.py`: a `FingerprintedView` gated on (node ids, labels, edges). Cards absolutely placed:
  label or instance id, `BoundStatusDot`, per-TS counts (the roster's `_get_array_progress`), species pill.
  Edges in one SVG: solid when set by hand (`SlotEdge.explicit`), dashed when automatic, a red stub for an
  unresolved input. Click a card → the job page (`View.JOB`); click an edge's midpoint chip → the consumer's
  I/O tab.
- Rename on the card writes `display_label`; the roster and tab strip already show it.

**Stage 2 — Branch verbs on a card** (~250 lines)
- **Add after this job…** — job types with an input that accepts one of this job's outputs (the two schemas);
  the new job's input is pinned to this job; existing readers pinned as today.
- **Clone** — same type; USER_PARAMS, SLURM settings and species copied through the typed setters (the
  `numeric_forward` / `enum_forward` rule); same pins.
- **Clone from here…** — this job and everything downstream of it (untick members in the dialog), pins inside
  the copy pointed at the copies. The root's inputs either stay (a parameter A/B) or are pointed at another
  producer from `get_candidates_for_slot` (`path_resolution_service.py:501`), e.g. missAlign instead of
  aligntiltsWarp. One label for the new lane.
- All three go through `add_instance_to_pipeline` (`pipeline_builder_panel.py:290`), the one creation path.

**Stage 3 — Inputs fixed at submit; a branch runs while another runs** (~250 lines, the risky stage)
- Fixed inputs: `_submit_chain` pass 2 (`pipeline_orchestrator_service.py:560-581`) records each job's chosen
  sources on the job; `_select_source` reads them for a submitted job; the driver uses the submitted
  `job_model.paths` (written at `:628`) instead of re-resolving (`drivers/driver_base.py:117-139`). QUEUED joins
  `is_job_frozen`.
- Orchestrator: drop the project-wide refusal (`:175-176`); never resubmit a QUEUED/RUNNING job; their
  consumers wait on them through `live_after` (`:593-598`); stop rewriting `pipeline_order` (`:183`); clear
  `review_hold` (`:185`) only when the run contains the held alignment.
- "Run to here" on a card: the job plus its unfinished upstream. Run keeps meaning "everything unfinished".
- UI: the `ui_mgr.is_running` gates (§1.2) become per job: add, clone, edit or delete anything that is not live
  and not upstream of a live job. Stop stays the emergency brake; cancel on a card stops its branch.
- Registry: a lock file around each supervisor's load → ingest → save, or a re-read-and-merge in `save()`
  (§1.3).
- Runtime check owed: two branches submitted minutes apart, one cancelled; Approve while another branch runs.

**Stage 4 — Read side follows the lane** (~250 lines)
- One helper: the job of type T upstream of a given job, by the edges. It replaces `find_job_by_type` in the
  ~20 readers of §1.5 and the "first of species" lookups.
- A lane chooser in the Journey header (default: the trunk), passed to every section.

**Stage 5 — Selection as a job** (~150 lines)
- A "TS subset" job: runs inline like import (`RUNS_INLINE`, `_submit_chain` pass 3, `:645`); reads any
  tilt-series star and writes it without the chosen series; a drop-in for its input type. Branch from it.
- Tilt-level subsets per branch: later, once the tilt filter emits its verdict as an output.

**Later:** the same lanes as thin lines beside the roster rows; drag-to-wire (Vue Flow) if it is ever missed;
"compare with siblings" on a card, opening reconstruction compare and roadmap 30's readouts on the pair.

---

## 6. What not to build

- A node-editor canvas now: vendored JS, the repo's first custom component, a Node build, for drag-wiring we
  rarely need.
- Workspaces.
- Protocols as branch templates. "Clone from here" covers CryoSPARC's Apply + Repeat without a registry.
- Per-branch tilt filters before the filter emits an output.
- Automatic A/B pairing, verdicts or scorecards (maintainer rule, 2026-09-04).

---

## 7. Decisions for the maintainer

1. Where the graph lives: a seventh view beside the roster (recommended), or the roster itself redrawn as lanes.
2. Inputs fixed at submit (recommended). The cost: a FAILED job's retry keeps its inputs even if a refiner was
   added meanwhile; it is re-pointed by hand.
3. Run semantics: Run = everything unfinished, unchanged, plus "Run to here" on a card (recommended).
4. Per-branch TS subsets: an inline TS-subset job (recommended), or an exclude list on every job.
5. Order: 1 → 2 → 4 → 5 with 3 when needed, or 3 first if the locked project is the main pain now.

---

## Appendix — side findings that are bugs today, branches or not

Verified at the anchors; none fixed.

1. Under afterok, Delete always says "No downstream jobs will be affected." (`pipeline_deletion_service.py:323-370`
   reads `default_pipeline.star`). `resolve_slot_edges` has the real edges.
2. `apply_aggregation_overrides` rewires every optimisation-set input, finished jobs included, whenever any job is
   added (`services/aggregation/extraction.py:255-330`, `pipeline_builder_panel.py:412`).
3. missAlign's `planned_stack_apix` reads the first alignment on the roster, not its own input
   (`services/jobs/miss_align.py:527-539`).
4. The template workbench's tomogram pixel size comes from the first job whose type contains "reconstruct",
   reconstructParticle included (`ui/template_workbench.py:408-415`).
5. Registry saves across processes are last writer wins per sidecar (`services/tilt_series/registry.py:353-384`).
6. Module-level `ui.add_head_html` never reaches served pages (§2, side finding).
7. The tilt filter's `get_output_assets` names `filtered/*.star` files that nothing writes
   (`services/jobs/tilt_filter.py:174-178`).
