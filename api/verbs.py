"""The verb table: one entry per verb, the one list the HTTP routes and the MCP tool list are read from.

A verb's name says what it does; its description is written for the model that calls it. Reads (`get_*`,
`list_*`, `inspect_*`, `plan_*`) change nothing. A write changes only a project the API made, and the verbs
that make a project or submit or cancel SLURM work ask the user every time (`asks`).
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Literal

from pydantic import BaseModel

from api import handlers as h
from api import models as m


@dataclass(frozen=True, slots=True)
class Verb:
    name: str
    description: str
    model: type[BaseModel]
    kind: Literal["read", "write"]
    handler: Callable[[h.Call], Awaitable[dict[str, Any]]]
    # Ask the user on every call, whatever the permission rules say: it makes a project, or spends or ends
    # GPU hours.
    asks: bool = False

    @property
    def names_project(self) -> bool:
        return "project" in self.model.model_fields


_VERBS: tuple[Verb, ...] = (
    # ── Reads ──
    Verb(
        "get_server_info",
        "This CryoBoost server: owner, host, port, checkout, default project base, conf.yaml warnings. The API's "
        "health check.",
        m.NoInput,
        "read",
        h.get_server_info,
    ),
    Verb(
        "list_projects",
        "The projects under a base folder (the server's default when omitted): status, size, and whether the API "
        "made them (only those can be changed through it).",
        m.ListProjectsIn,
        "read",
        h.list_projects,
    ),
    Verb(
        "get_project",
        "A project: its roster of jobs with status, SLURM id and per-tilt-series counts, species, the tilt-filter "
        "review hold, acquisition values, and its link in the UI.",
        m.ProjectIn,
        "read",
        h.get_project,
    ),
    Verb(
        "get_job",
        "One job: every parameter (value, default, bounds, choices, help, edited), its SLURM settings, the source "
        "of each input and how it was chosen, its outputs, and its link.",
        m.JobIn,
        "read",
        h.get_job,
    ),
    Verb(
        "get_job_tasks",
        "An array job's tilt series: each one's state (ok, fail, running, pending, skip) and why it failed. Paged.",
        m.JobTasksIn,
        "read",
        h.get_job_tasks,
    ),
    Verb(
        "get_job_logs",
        "The end of a job's run.out and run.err, or of one tilt series' task log. Read it to report a failure.",
        m.JobLogsIn,
        "read",
        h.get_job_logs,
    ),
    Verb(
        "list_job_types",
        "Every job type the roster adds: its phase, the types it usually reads from, what is added with it, whether "
        "it takes a species, its inputs and outputs.",
        m.NoInput,
        "read",
        h.list_job_types,
    ),
    Verb(
        "get_job_type",
        "The parameters of a job type: type, default, bounds, choices, help.",
        m.JobTypeIn,
        "read",
        h.get_job_type,
    ),
    Verb(
        "get_input_candidates",
        "What one input of a job can read, as its I/O section lists it, and why a source is unreadable or skipped "
        "by automatic selection.",
        m.InputCandidatesIn,
        "read",
        h.get_input_candidates,
    ),
    Verb("list_species", "The project's particle species, and the lab catalog's.", m.ProjectIn, "read", h.list_species),
    Verb(
        "get_species",
        "One species: its templates and masks, which are selected, symmetry, diameter, extraction geometry.",
        m.SpeciesIn,
        "read",
        h.get_species,
    ),
    Verb(
        "list_kitchen_subsets",
        "The STA kitchen of a species in a project: its subsets (name, group, picks) and its members.",
        m.SpeciesIn,
        "read",
        h.list_kitchen_subsets,
    ),
    Verb(
        "get_kitchen_subset_sources",
        "Where a kitchen subset's picks come from, to match those tomograms again in a new project: per source "
        "project its tomograms, the tomogram job each came from and its tilt-series job, what is missing; the "
        "tomograms_from for create_project, the values they bring (or their groups as a need), and the template "
        "the new species takes. Changes nothing in the kitchen.",
        m.KitchenSubsetIn,
        "read",
        h.get_kitchen_subset_sources,
    ),
    Verb(
        "plan_run",
        "What run would do now, doing none of it: the jobs it would start in submit order with their inputs and "
        "resources, the jobs a tilt-filter review would hold, what would refuse the run, and the open decisions.",
        m.ProjectIn,
        "read",
        h.plan_run,
    ),
    Verb(
        "get_task",
        "A background task's progress and, once finished, its result (create_project runs as one).",
        m.TaskIn,
        "read",
        h.get_task,
    ),
    Verb(
        "inspect_dataset",
        "A data folder as Create would import it: the series, the movie kind, gain-reference candidates, folders "
        "that reuse series names, and for each acquisition value what the mdocs say, or that they say nothing.",
        m.InspectDatasetIn,
        "read",
        h.inspect_dataset,
    ),
    # ── Creation ──
    Verb(
        "create_project",
        "Make a new project from a data folder (or with no raw data), or from a protocol bundle with its species "
        "and stages. Every acquisition value comes from the user: a missing one comes back as a need. Or make one "
        "from other projects' Reconstruct or Denoise Predict tomograms (tomograms_from), ready for template "
        "matching, whose values come with them. Runs in the background; follow it with get_task.",
        m.CreateProjectIn,
        "write",
        h.create_project,
        asks=True,
    ),
    # ── Composition and setting ──
    Verb(
        "add_job",
        "Put a job on the roster, as the roster's add does: its prerequisite comes with it, a particle job takes a "
        "species and the species' values, a clone takes another job's settings, and jobs that would tie with it are "
        "kept on their source.",
        m.AddJobIn,
        "write",
        h.add_job,
    ),
    Verb("remove_job", "Take a job that has not run off the roster.", m.JobIn, "write", h.remove_job),
    Verb(
        "exclude_tilt_series",
        "Leave a tilt series out of processing from the next run on, or bring it back. Not the last one.",
        m.ExcludeTiltSeriesIn,
        "write",
        h.exclude_tilt_series,
    ),
    Verb(
        "set_params",
        "Set parameters of a Scheduled or Failed job. Each value is checked (type, bounds, choices); nothing is "
        "written unless all pass.",
        m.SetParamsIn,
        "write",
        h.set_params,
    ),
    Verb(
        "set_tilt_filter",
        "The tilt filter's mode (manual, dl_review, dl_auto), DL model and threshold.",
        m.SetTiltFilterIn,
        "write",
        h.set_tilt_filter,
    ),
    Verb(
        "set_slurm",
        "Set a job's SLURM resources, or reset them to the project defaults and the job type's profile.",
        m.SetSlurmIn,
        "write",
        h.set_slurm,
    ),
    Verb(
        "set_input",
        "Set what one input of a job reads: a source get_input_candidates lists, a file, or automatic.",
        m.SetInputIn,
        "write",
        h.set_input,
    ),
    Verb(
        "reset_inputs",
        "A Failed job chooses its inputs again at its next run instead of those fixed at its last submit.",
        m.JobIn,
        "write",
        h.reset_inputs,
    ),
    Verb("set_gain", "The project's gain reference file, or none.", m.SetGainIn, "write", h.set_gain),
    Verb(
        "predict_tilts",
        "Run the tilt filter's DL prediction on the cluster (it spends GPU time).",
        m.JobIn,
        "write",
        h.predict_tilts,
        asks=True,
    ),
    Verb(
        "cancel_predict_tilts",
        "Cancel the tilt filter's DL prediction run.",
        m.JobIn,
        "write",
        h.cancel_predict_tilts,
        asks=True,
    ),
    Verb(
        "cancel_job",
        "Cancel one queued or running job; the rest of the run goes on.",
        m.JobIn,
        "write",
        h.cancel_job,
        asks=True,
    ),
    # ── Species and templates ──
    Verb(
        "import_species",
        "Bring a species from the lab catalog into the project, with its templates and masks.",
        m.ImportSpeciesIn,
        "write",
        h.import_species,
    ),
    Verb(
        "add_template",
        "Register an MRC file as a template of a species (copied into the project).",
        m.AddTemplateIn,
        "write",
        h.add_template,
    ),
    Verb(
        "add_mask",
        "Register an MRC file as a mask of a species (copied into the project).",
        m.AddMaskIn,
        "write",
        h.add_mask,
    ),
    Verb(
        "make_template",
        "Make a template for a species as the workbench does: an ellipsoid, a density simulated from a PDB entry, "
        "an EMDB map, or a resample, lowpass or polarity flip of a template. Its white and black volumes are "
        "registered and the white selected (a flip selects the flipped one). Runs in the background: follow "
        "get_task. Values it needs and nobody gave come back as needs.",
        m.MakeTemplateIn,
        "write",
        h.make_template,
    ),
    Verb(
        "make_mask",
        "Make a mask from a species' template as the workbench does: RELION's threshold mask, or a soft sphere on "
        "the template's grid. Registered on the species, selected only when it has no mask yet. Runs in the "
        "background: follow get_task. Without a threshold, needs lists the levels each method finds.",
        m.MakeMaskIn,
        "write",
        h.make_mask,
    ),
    Verb(
        "select_template",
        "Make one of a species' templates the one new jobs take.",
        m.SelectTemplateIn,
        "write",
        h.select_template,
    ),
    Verb("select_mask", "Make one of a species' masks the one new jobs take.", m.SelectMaskIn, "write", h.select_mask),
    Verb(
        "create_species",
        "A new particle species: name, colour, diameter, symmetry, notes, and a mask registered as it is. Add its "
        "template with add_template.",
        m.CreateSpeciesIn,
        "write",
        h.create_species,
    ),
    Verb(
        "set_species",
        "Edit a species: name, colour, diameter, symmetry, notes, and the extraction geometry its new extraction "
        "jobs take. Jobs made earlier keep their values.",
        m.SetSpeciesIn,
        "write",
        h.set_species,
    ),
    Verb(
        "import_tomograms",
        "Import tomograms into a project without a Reconstruct job: from MRC volumes, an existing tomograms.star, "
        "or other projects' Reconstruct or Denoise Predict jobs (mode jobs: with their tilt series, ready for "
        "template matching).",
        m.ImportTomogramsIn,
        "write",
        h.import_tomograms,
    ),
    # ── Run controls ──
    Verb(
        "run",
        "Run the roster as the Run button does: what plan_run shows. Submits SLURM work.",
        m.ProjectIn,
        "write",
        h.run,
        asks=True,
    ),
    Verb("stop", "Stop the run: cancel every queued and running job.", m.ProjectIn, "write", h.stop, asks=True),
    Verb(
        "cancel_leftovers",
        "After a refused run, cancel the SLURM work it named as still live in a failed job's folder.",
        m.CancelLeftoversIn,
        "write",
        h.cancel_leftovers,
        asks=True,
    ),
)

VERBS: dict[str, Verb] = {v.name: v for v in _VERBS}
