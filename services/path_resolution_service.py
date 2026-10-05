from __future__ import annotations

import logging
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TYPE_CHECKING
from collections.abc import Sequence

from services.io_slots import InputSlot, OutputSlot, JobFileType, ResolvedInput, ResolvedOutput, ResolvedManifest

from services.models_base import InstanceId, JobType, JobStatus

logger = logging.getLogger(__name__)

# (consumer, *refiners) combinations already reported as ambiguous by _follow_refinements.
_AMBIGUOUS_REFINEMENTS_WARNED: set[tuple[str, ...]] = set()

if TYPE_CHECKING:
    from services.project_state import ProjectState
    from services.job_models import AbstractJobParams


@dataclass(frozen=True)
class OutputCandidate:
    produces: JobFileType
    producer_job_type: JobType
    producer_output_key: str
    path: str

    # Instance identification
    instance_path: str  # e.g., "External/job005"
    producer_instance_id: str  # e.g., "tsReconstruct" or "templatematching__ribosome"

    # Shown in the I/O menu and ordering it (the job number also ranks side-by-side refiners);
    # automatic selection between producers uses neither.
    execution_status: JobStatus
    relion_job_number: int  # 0 if unknown

    species_id: str | None = None  # propagated from producing job model

    # OutputSlot.prefer_if_exists — carried through so the scorer can tier-boost
    # opt-in artifacts (e.g. user-curated filtered files) over same-producer
    # siblings of the same JobFileType.
    prefer_if_exists: bool = False

    # Friendly label for synthetic (non-job) producers — e.g. "Merged sources —
    # <name>". When set, the UI dropdown shows this verbatim instead of the
    # derived "instance_path (jobtype)" form. None for ordinary job producers.
    label: str | None = None

    # False for a candidate offered to an explicit choice in the I/O section only, never to
    # automatic selection: a pick list extracted before another list of the same species on
    # the same tomogram.
    auto_eligible: bool = True

    @property
    def source_key(self) -> str:
        """Key format for source_overrides: 'jobtype:instance:<instance_id>' for a job, which holds
        however job dirs are allocated; 'jobtype:instance_path' for a synthetic producer (merge,
        imported tomograms, pick list), whose instance path is its identity."""
        if is_synthetic_producer(self.producer_instance_id):
            return f"{self.producer_job_type.value}:{self.instance_path}"
        return instance_override_key(self.producer_job_type, self.producer_instance_id)

    @property
    def display_name(self) -> str:
        """Human-readable name for UI dropdowns"""
        if self.label:
            return self.label
        status_icon = {
            JobStatus.SUCCEEDED: "ok",
            JobStatus.RUNNING: "running",
            JobStatus.FAILED: "failed",
            JobStatus.SCHEDULED: "scheduled",
        }.get(self.execution_status, "?")
        base = f"{self.instance_path} ({self.producer_job_type.value}) [{status_icon}]"
        return f"{base} [{self.species_id}]" if self.species_id else base


@dataclass
class InputSlotValidation:
    """Result of validating an input slot's current configuration."""

    slot_key: str
    is_valid: bool
    source_key: str | None
    resolved_path: str | None
    error_message: str | None = None
    file_exists: bool = False
    is_user_override: bool = False
    awaiting_upstream: bool = False
    # Two or more equally good producers and no override: the slot stays ambiguous until one
    # is picked. One output per producer instance.
    tied: tuple[OutputCandidate, ...] = ()


@dataclass(frozen=True)
class AutoChoice:
    """Automatic selection for one input slot: the chosen producer output, or none. `tied` holds
    one output per producer instance when two or more instances are equally good, and then
    nothing is chosen."""

    candidate: OutputCandidate | None = None
    tied: tuple[OutputCandidate, ...] = ()


@dataclass(frozen=True)
class SlotSource:
    """Where one input slot reads from: a producer output, a manual file, or nothing, with the
    reason when there is one. Path resolution, the DAG's edges, the I/O section's validation and
    refinement following all take their answer from here, so they cannot disagree."""

    candidate: OutputCandidate | None = None
    manual_path: str | None = None
    # The override that decided the source; None for automatic selection.
    override_key: str | None = None
    # Why no source was chosen: an override that no longer resolves, or a tie.
    problem: str | None = None
    tied: tuple[OutputCandidate, ...] = ()


@dataclass(frozen=True)
class SlotEdge:
    """One wire of the pipeline DAG: `consumer`'s input `slot` reads `output` (a file name) of
    `producer`. `explicit` when an override set it rather than automatic selection."""

    producer: str
    consumer: str
    slot: str
    output: str
    explicit: bool

    def describe(self) -> str:
        how = "set by hand" if self.explicit else "automatic"
        return f"{self.consumer}.{self.slot} ← {self.producer} ({self.output}, {how})"


def find_cycle(edges: Sequence[SlotEdge]) -> list[SlotEdge]:
    """One loop among `edges`, as its wires in order (each wire's consumer is the next one's
    producer), or [] when they form none."""
    for e in edges:
        back = _path_between(edges, e.consumer, e.producer)
        if back is not None:
            return [e, *back]
    return []


def _path_between(edges: Sequence[SlotEdge], start: str, goal: str) -> list[SlotEdge] | None:
    """The shortest run of wires leading from job `start` to job `goal` (producer to consumer),
    or None when there is none."""
    by_producer: dict[str, list[SlotEdge]] = {}
    for e in edges:
        by_producer.setdefault(e.producer, []).append(e)
    reached_by: dict[str, SlotEdge | None] = {start: None}
    queue = deque([start])
    while queue:
        node = queue.popleft()
        if node == goal:
            path: list[SlotEdge] = []
            while (wire := reached_by[node]) is not None:
                path.append(wire)
                node = wire.producer
            return path[::-1]
        for e in by_producer.get(node, []):
            if e.consumer not in reached_by:
                reached_by[e.consumer] = e
                queue.append(e.consumer)
    return None


class PathResolutionError(ValueError):
    pass


# Instance-path markers for the synthetic (non-job) producers. The merge and the pick-list
# extractions have no JobType of their own and borrow MERGED_SOURCES, told apart by instance
# path. Imported tomograms have their own (JobType.IMPORTED_TOMOGRAMS); only their legacy
# override keys carry the mergedSources prefix.
PICK_LIST_PRODUCER_PREFIX = "pick_list__"
IMPORTED_TOMOGRAMS_INSTANCE_PATH = "Tomograms"
IMPORTED_TOMOGRAMS_PRODUCER_ID = "importedTomograms"


# An override naming a job names its instance: "<jobtype>:instance:<instance_id>". Older project
# files name the job's directory instead ("templatematching:External/job007"), which a later
# allocation can hand to another job; ProjectState.load rewrites those (migrate_override_keys),
# and the form is still read.
INSTANCE_REF = "instance:"


def instance_override_key(job_type: JobType, instance_id: str) -> str:
    """The override key naming producer instance `instance_id`."""
    return f"{job_type.value}:{INSTANCE_REF}{instance_id}"


def override_instance_id(override_key: str) -> str | None:
    """The producer instance an override key names, or None for any other kind of key."""
    if override_key.startswith("manual:"):
        return None
    _, _, ref = override_key.partition(":")
    return ref[len(INSTANCE_REF) :] if ref.startswith(INSTANCE_REF) else None


def pick_list_producer_id(pick_list) -> str:
    """Stable synthetic producer id for one curation pick list.

    Keyed on (species, tomo, slug), NOT slug alone: ``PickList.slug`` is only unique
    WITHIN a (species, tomo), and a hand-picked list is slugged after its ``.coords``
    file (``manual__<stem>``, `services/particles/ingest.py`), a name that recurs on
    every tomogram the user saves it on — so a slug-only id would alias those onto one
    producer, and make `remove_species` purge another species' overrides.
    """
    return f"{PICK_LIST_PRODUCER_PREFIX}{pick_list.species_id}__{pick_list.tomo_name}__{pick_list.slug}"


def pick_list_producer_prefix_for_species(species_id: str) -> str:
    """Producer-id prefix owned by one species — what `remove_species` purges by."""
    return f"{PICK_LIST_PRODUCER_PREFIX}{species_id}__"


def is_synthetic_producer(producer_instance_id: str) -> bool:
    """True for a producer that has no SLURM job — so it can never be an afterok
    dependency. The aggregation merge, imported tomograms, and per-pick-list
    extractions (which run as their own one-off job, outside the pipeline graph)."""
    return producer_instance_id in ("mergedSources", IMPORTED_TOMOGRAMS_PRODUCER_ID) or producer_instance_id.startswith(
        PICK_LIST_PRODUCER_PREFIX
    )


def auto_exclusion(consumer_type: JobType, consumer_species_id: str | None, candidate: OutputCandidate) -> str | None:
    """Why automatic selection never wires `candidate` into a consumer of `consumer_type` bound
    to `consumer_species_id`, or None when it may. A job producer has to run at an earlier stage
    than its consumer (PIPELINE_ORDER), which keeps automatic wiring acyclic; a producer bound to
    another species never feeds a species-bound consumer. Synthetic producers have no stage. An
    explicit override may name either kind."""
    from services.jobs.spec import PIPELINE_ORDER

    if (
        not is_synthetic_producer(candidate.producer_instance_id)
        and consumer_type in PIPELINE_ORDER
        and candidate.producer_job_type in PIPELINE_ORDER
        and PIPELINE_ORDER.index(candidate.producer_job_type) >= PIPELINE_ORDER.index(consumer_type)
    ):
        return "not upstream"
    if consumer_species_id and candidate.species_id and candidate.species_id != consumer_species_id:
        return "other species"
    return None


def _synthetic_override_target(override_key: str) -> str:
    """Which synthetic producer an override names: "merged" | "pick_list" | "imported" | "".

    The merge and the per-pick-list extractions have no JobType of their own, so both wear
    the ``mergedSources:`` prefix and are told apart by instance path — routing a dangling
    pick-list override through the merged-sources branch would tell the user
    "merged-sources optimisation_set not found" about a list that simply needs re-extracting.
    Imported tomograms have their own JobType; the legacy ``mergedSources:Tomograms`` form is
    also recognised so older persisted overrides keep resolving."""
    if override_key.startswith(f"{JobType.IMPORTED_TOMOGRAMS.value}:"):
        return "imported"
    prefix = f"{JobType.MERGED_SOURCES.value}:"
    if not override_key.startswith(prefix):
        return ""
    instance_path = override_key[len(prefix) :]
    if instance_path.startswith(PICK_LIST_PRODUCER_PREFIX):
        return "pick_list"
    if instance_path == IMPORTED_TOMOGRAMS_INSTANCE_PATH:
        return "imported"  # legacy key, minted before IMPORTED_TOMOGRAMS existed
    return "merged"


class PathResolutionService:
    """
    Schema-based path resolution with user overrides, candidate enumeration
    for the UI, and species-aware scoring.
    """

    def __init__(self, state: ProjectState, active_instance_ids: set | None = None):
        self.state = state
        self._active_instance_ids = active_instance_ids
        self._output_index: dict[JobFileType, list[OutputCandidate]] | None = None

    # -------------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------------

    def resolve_all_paths(
        self,
        job_type: JobType,
        job_model: AbstractJobParams,
        job_dir: Path,
        instance_id: str | None = None,
        return_manifest: bool = False,
    ) -> dict[str, Any] | tuple[dict[str, Any], ResolvedManifest]:
        """
        Resolve inputs + outputs using schemas and return a dict compatible with job_model.paths.
        Respects source_overrides from job_model and species-aware scoring.
        """
        outputs_manifest = self.resolve_outputs(job_type, job_dir)
        inputs_manifest = self.resolve_inputs(job_type, job_model, consumer_instance_id=instance_id)

        manifest = ResolvedManifest(
            job_type=job_type.value, instance_id=instance_id, inputs=inputs_manifest, outputs=outputs_manifest
        )

        paths = manifest.as_paths_dict()
        if return_manifest:
            return paths, manifest
        return paths

    def resolve_outputs(self, job_type: JobType, job_dir: Path) -> list[ResolvedOutput]:
        schema = self._get_output_schema(job_type)
        resolved: list[ResolvedOutput] = []
        for slot in schema:
            resolved_path = str((job_dir / slot.path_template).resolve())
            resolved.append(ResolvedOutput(output_key=slot.key, produces=slot.produces, path=resolved_path))
        return resolved

    def resolve_inputs(
        self, job_type: JobType, job_model: AbstractJobParams, consumer_instance_id: str | None = None
    ) -> list[ResolvedInput]:
        """
        Resolve inputs for target job: each slot's override when it resolves, otherwise
        automatic selection (`_select_source`). An ambiguous slot or an override that no
        longer resolves is a missing input, named in the error.

        consumer_instance_id excludes the consumer from its own candidate pool
        (a job's OUTPUT slot is never a valid source for its own INPUT slot).
        """
        input_schema = self._get_input_schema(job_type)
        index = self._build_output_index()

        resolved_inputs: list[ResolvedInput] = []
        missing_required: list[str] = []

        for slot in input_schema:
            src = self._select_source(job_type, job_model, slot, index, consumer_instance_id)

            # A manual override is a file path, not a producer: it goes into the paths as given.
            if src.manual_path is not None:
                resolved_inputs.append(
                    ResolvedInput(
                        input_key=slot.key,
                        chosen_type=slot.accepts[0],
                        source_job_type="manual",
                        source_instance_id=None,
                        source_output_key="manual",
                        path=src.manual_path,
                    )
                )
                continue

            chosen = src.candidate
            if chosen is None:
                if slot.required:
                    missing_required.append(
                        f"{slot.key}: {src.problem}"
                        if src.problem
                        else f"{slot.key} accepts={[t.value for t in slot.accepts]}"
                    )
                continue

            gap = self._interactive_producer_gap(chosen)
            if gap:
                if slot.required:
                    missing_required.append(f"{slot.key}: {gap}")
                continue

            resolved_inputs.append(
                ResolvedInput(
                    input_key=slot.key,
                    chosen_type=chosen.produces,
                    source_job_type=chosen.producer_job_type.value,
                    source_instance_id=chosen.instance_path,
                    source_output_key=chosen.producer_output_key,
                    path=chosen.path,
                )
            )

        if missing_required:
            raise PathResolutionError(
                f"Cannot resolve required inputs for {job_type.value}: " + "; ".join(missing_required)
            )

        return resolved_inputs

    def resolve_edges(self, instance_ids: Sequence[str] | None = None) -> list[tuple[str, str]]:
        """The producer->consumer dependency edges of the pipeline DAG: :meth:`resolve_slot_edges`
        as deduplicated ``(producer_instance_id, consumer_instance_id)`` pairs, suitable for a
        SLURM ``--dependency=afterok`` topological submit."""
        return list(dict.fromkeys((e.producer, e.consumer) for e in self.resolve_slot_edges(instance_ids)))

    def resolve_slot_edges(self, instance_ids: Sequence[str] | None = None) -> list[SlotEdge]:
        """
        Derive the wires of the pipeline DAG, one per input slot fed by another job.

        For every consumer job (restricted to ``instance_ids`` if given, else all
        jobs in state) each input slot takes its source from :meth:`_select_source`,
        exactly as :meth:`resolve_inputs` does, but the chosen producer's *clean*
        ``instance_id`` is recorded instead of a path.

        Producers with no SLURM job are excluded: a ``manual:`` file override
        (user-picked file, no producing job) and every synthetic producer
        (``is_synthetic_producer`` -- the aggregation merge, imported tomograms, and
        per-pick-list extractions) -- an ``afterok`` on any of them would never be
        satisfiable.
        Slots without a source (unresolved, ambiguous, a dangling override) add no
        edge and raise nothing, so this is safe on a partially-configured pipeline;
        resolve_inputs is what reports them. The caller restricts producers to the
        jobs actually being submitted and drops already-SUCCEEDED producers (a
        finished upstream needs no dependency).
        """
        consumers = list(instance_ids) if instance_ids is not None else list(self.state.jobs.keys())
        index = self._build_output_index()
        edges: list[SlotEdge] = []

        for consumer_id in consumers:
            job_model = self.state.jobs.get(consumer_id)
            if job_model is None or job_model.job_type is None:
                continue

            for slot in self._get_input_schema(job_model.job_type):
                src = self._select_source(job_model.job_type, job_model, slot, index, consumer_id)
                chosen = src.candidate
                if chosen is None:
                    continue

                producer_id = chosen.producer_instance_id
                if not producer_id or is_synthetic_producer(producer_id) or producer_id == consumer_id:
                    # synthetic / non-job producer, or a self-edge from a pathological
                    # override (the override path, unlike auto-selection, does not
                    # exclude the consumer) -- neither is a valid afterok dependency.
                    continue
                edges.append(
                    SlotEdge(
                        producer=producer_id,
                        consumer=consumer_id,
                        slot=slot.key,
                        output=Path(chosen.path).name,
                        explicit=src.override_key is not None,
                    )
                )

        return edges

    def cycle_with_override(self, consumer_id: str, slot_key: str, override_key: str) -> list[SlotEdge]:
        """The wires of the loop that setting `override_key` on `consumer_id`'s input `slot_key`
        would close, the new wire first, or [] when it closes none. Automatic wiring only looks
        upstream, so a loop always runs through an override. The job's overrides are left as
        they were."""
        job_model = self.state.jobs.get(consumer_id)
        if job_model is None:
            return []
        if job_model.source_overrides is None:
            job_model.source_overrides = {}
        overrides = job_model.source_overrides
        before = overrides.get(slot_key)
        overrides[slot_key] = override_key
        try:
            edges = self.resolve_slot_edges()
        finally:
            if before is None:
                overrides.pop(slot_key, None)
            else:
                overrides[slot_key] = before
        wire = next((e for e in edges if e.consumer == consumer_id and e.slot == slot_key), None)
        if wire is None:
            return []
        back = _path_between(edges, wire.consumer, wire.producer)
        return [wire, *back] if back is not None else []

    def automatic_sources(self) -> dict[tuple[str, str], OutputCandidate]:
        """What automatic selection wires now: `{(instance_id, slot_key): candidate}` for every
        job's input slot that has no override and one source (unresolved and tied slots are
        absent)."""
        index = self._build_output_index()
        sources: dict[tuple[str, str], OutputCandidate] = {}
        for instance_id, job_model in self.state.jobs.items():
            if job_model.job_type is None:
                continue
            overrides = getattr(job_model, "source_overrides", {}) or {}
            for slot in self._get_input_schema(job_model.job_type):
                if overrides.get(slot.key):
                    continue
                choice = self._auto_select(
                    slot, index, job_model.job_type, getattr(job_model, "species_id", None), instance_id
                )
                if choice.candidate is not None:
                    sources[(instance_id, slot.key)] = choice.candidate
        return sources

    # -------------------------------------------------------------------------
    # Candidate enumeration for UI
    # -------------------------------------------------------------------------

    def get_candidates_for_slot(
        self,
        job_type: JobType,
        slot_key: str,
        consumer_species_id: str | None = None,
        consumer_instance_id: str | None = None,
    ) -> list[OutputCandidate]:
        """
        Every candidate for one input slot, for the I/O section's menu, in the order
        automatic selection ranks them: what it may choose first (the consumer's own
        species, then the slot's preferred source type, then jobs before synthetic
        producers, then roster jobs before ones taken off it), then what only an explicit
        choice can wire (`auto_exclusion`: later-stage jobs, other species; older pick
        lists). Within a rank, finished jobs first, then the newest -- display order only,
        never a selection.

        consumer_instance_id, if provided, excludes the consumer from its own
        pool so the UI doesn't offer a job's own output as a source for itself.
        """
        input_schema = self._get_input_schema(job_type)
        slot = next((s for s in input_schema if s.key == slot_key), None)

        if not slot:
            return []

        index = self._build_output_index()
        candidates = [c for t in slot.accepts for c in index.get(t, [])]
        if consumer_instance_id is not None:
            candidates = [c for c in candidates if c.producer_instance_id != consumer_instance_id]
        preferred = self._parse_preferred_source(slot.preferred_source)
        roster = set(self.state.pipeline_order or ())

        def sort_key(c: OutputCandidate) -> tuple[bool, bool, bool, bool, bool, int, int, str]:
            # Lower value = sorted earlier
            manual_only = not c.auto_eligible or auto_exclusion(job_type, consumer_species_id, c) is not None
            species_match = bool(consumer_species_id) and c.species_id == consumer_species_id
            status_order = {
                JobStatus.SUCCEEDED: 0,
                JobStatus.RUNNING: 1,
                JobStatus.SCHEDULED: 2,
                JobStatus.FAILED: 3,
            }.get(c.execution_status, 4)
            return (
                manual_only,
                not species_match,
                c.producer_job_type != preferred,
                is_synthetic_producer(c.producer_instance_id),
                bool(roster) and c.producer_instance_id not in roster,
                status_order,
                -c.relion_job_number,
                c.instance_path,
            )

        ordered = sorted(candidates, key=sort_key)

        # Collapse candidates that share a source_key. A single producing job can
        # expose several outputs of the SAME JobFileType — e.g. subtomo extraction
        # emits both a raw optimisation_set.star and its curated `_filtered`
        # sibling. They map to the identical override key (source_key),
        # so listing both as separate rows produces confusing duplicate entries
        # with identical labels ("Subtomo Extraction #2 (job017)" twice). Keep one
        # row per source_key, preferring the `prefer_if_exists` (curated) sibling
        # so it matches what _resolve_override / _auto_select actually pick.
        deduped: list[OutputCandidate] = []
        pos_by_key: dict[str, int] = {}
        for c in ordered:
            key = c.source_key
            if key not in pos_by_key:
                pos_by_key[key] = len(deduped)
                deduped.append(c)
            elif c.prefer_if_exists and not deduped[pos_by_key[key]].prefer_if_exists:
                deduped[pos_by_key[key]] = c
        return deduped

    def get_input_schema_for_job(self, job_type: JobType) -> list[InputSlot]:
        """Expose input schema for UI rendering."""
        return self._get_input_schema(job_type)

    def get_output_schema_for_job(self, job_type: JobType) -> list[OutputSlot]:
        """Expose output schema for UI rendering."""
        return self._get_output_schema(job_type)

    def validate_input_slot(
        self,
        job_type: JobType,
        job_model: AbstractJobParams,
        slot_key: str,
        check_filesystem: bool = True,
        consumer_instance_id: str | None = None,
    ) -> InputSlotValidation:
        """
        Validate a single input slot's current configuration.
        Returns detailed validation result for UI feedback; the source is the one
        resolve_inputs would use (`_select_source`).
        """
        input_schema = self._get_input_schema(job_type)
        slot = next((s for s in input_schema if s.key == slot_key), None)

        if not slot:
            return InputSlotValidation(
                slot_key=slot_key,
                is_valid=False,
                source_key=None,
                resolved_path=None,
                error_message=f"Unknown input slot: {slot_key}",
            )

        overrides = getattr(job_model, "source_overrides", {}) or {}
        is_user_override = overrides.get(slot_key) is not None

        index = self._build_output_index()
        src = self._select_source(job_type, job_model, slot, index, consumer_instance_id)

        if src.manual_path is not None:
            manual_path = src.manual_path
            file_exists = Path(manual_path).exists() if check_filesystem else True
            return InputSlotValidation(
                slot_key=slot_key,
                is_valid=file_exists or not slot.required,
                source_key=src.override_key,
                resolved_path=manual_path,
                file_exists=file_exists,
                is_user_override=True,
                error_message=None if file_exists else f"File not found: {manual_path}",
            )

        chosen = src.candidate
        if chosen is None and src.problem:
            # A tie, or an override that no longer resolves: surfaced, never replaced by some
            # other producer.
            return InputSlotValidation(
                slot_key=slot_key,
                is_valid=not slot.required,
                source_key=src.override_key,
                resolved_path=None,
                file_exists=False,
                is_user_override=is_user_override,
                error_message=src.problem,
                tied=src.tied,
            )

        if chosen is None:
            return InputSlotValidation(
                slot_key=slot_key,
                is_valid=not slot.required,
                source_key=None,
                resolved_path=None,
                error_message=(
                    f"No valid source found (accepts: {[t.value for t in slot.accepts]})" if slot.required else None
                ),
                is_user_override=is_user_override,
            )

        gap = self._interactive_producer_gap(chosen)
        if gap:
            return InputSlotValidation(
                slot_key=slot_key,
                is_valid=not slot.required,
                source_key=chosen.source_key,
                resolved_path=chosen.path,
                file_exists=False,
                is_user_override=is_user_override,
                error_message=gap,
            )

        is_pending = "pending_" in chosen.path
        source_in_flight = chosen.execution_status in (JobStatus.RUNNING, JobStatus.SCHEDULED, JobStatus.QUEUED)
        pipeline_active = getattr(self.state, "pipeline_active", False)

        expect_file_later = is_pending or (source_in_flight and pipeline_active)

        file_exists = True
        if check_filesystem and not expect_file_later:
            file_exists = Path(chosen.path).exists()

        is_valid = file_exists or expect_file_later or not slot.required

        if expect_file_later:
            error_message = None
        elif not file_exists:
            error_message = f"File not found: {chosen.path}"
        else:
            error_message = None

        return InputSlotValidation(
            slot_key=slot_key,
            is_valid=is_valid,
            source_key=chosen.source_key,
            resolved_path=chosen.path,
            file_exists=file_exists if not expect_file_later else False,
            is_user_override=is_user_override,
            error_message=error_message,
            awaiting_upstream=expect_file_later,
        )

    def validate_all_inputs(
        self, job_type: JobType, job_model: AbstractJobParams, check_filesystem: bool = True
    ) -> list[InputSlotValidation]:
        """Validate all input slots for a job."""
        input_schema = self._get_input_schema(job_type)
        return [self.validate_input_slot(job_type, job_model, slot.key, check_filesystem) for slot in input_schema]

    # -------------------------------------------------------------------------
    # Override resolution
    # -------------------------------------------------------------------------

    def _select_source(
        self,
        consumer_type: JobType,
        job_model: AbstractJobParams,
        slot: InputSlot,
        index: dict[JobFileType, list[OutputCandidate]],
        consumer_instance_id: str | None,
    ) -> SlotSource:
        """The source of one input slot: the user's override when it is set, otherwise
        automatic selection (`_auto_select`). A `manual:` override is a file path with no
        producer. An override that no longer resolves (a deleted job or merge, a stale pick
        list, a removed import) is reported, never replaced by some other producer such as a
        downstream Class3D output: the user chose that source. So is a tie."""
        override_key = (getattr(job_model, "source_overrides", {}) or {}).get(slot.key)
        if override_key:
            if override_key.startswith("manual:"):
                return SlotSource(manual_path=override_key[len("manual:") :], override_key=override_key)
            hit = self._resolve_override(slot, override_key, index)
            if hit is not None:
                return SlotSource(candidate=hit, override_key=override_key)
            target = _synthetic_override_target(override_key)
            problem = (
                self._dangling_synthetic_message(target, override_key)
                if target
                else self._dangling_job_message(override_key)
            )
            return SlotSource(override_key=override_key, problem=problem)

        choice = self._auto_select(
            slot, index, consumer_type, getattr(job_model, "species_id", None), consumer_instance_id
        )
        if choice.tied:
            names = ", ".join(c.producer_instance_id for c in choice.tied)
            return SlotSource(problem=f"ambiguous between {names}; pick one in the job's I/O section", tied=choice.tied)
        return SlotSource(candidate=choice.candidate)

    def _dangling_synthetic_message(self, target: str, override_key: str) -> str:
        """Why an override naming a synthetic producer stopped resolving (`target` as
        `_synthetic_override_target` returns it)."""
        if target == "pick_list":
            return self._dangling_pick_list_message(override_key)
        if target == "imported":
            return self._dangling_imported_message()
        return "merged-sources optimisation_set not found (merge deleted or active merge switched?)"

    def _dangling_job_message(self, override_key: str) -> str:
        """Why an override naming a job stopped resolving: the job is gone, or offers nothing
        this input accepts."""
        instance_id = override_instance_id(override_key)
        if instance_id is None:
            ref = override_key.partition(":")[2]
            return f"the job directory this input was set to ({ref}) belongs to no job now; pick a source or reset"
        if instance_id not in self.state.jobs:
            return f"the job this input was set to, {instance_id}, is no longer in the project; pick a source or reset"
        return f"{instance_id} offers no output this input accepts; pick a source or reset"

    def _dangling_pick_list_message(self, override_key: str) -> str:
        """Why a pick-list override stopped resolving, in the user's terms. The candidate
        is injected only while the list is EXTRACTED, so losing it means the list was
        re-curated (now STALE) or deleted — not that some generic input went missing."""
        producer_id = override_key.split(":", 1)[1]
        for pl in getattr(self.state, "pick_lists", None) or []:
            if pick_list_producer_id(pl) == producer_id:
                return (
                    f"pick list '{pl.label or pl.slug}' on {pl.tomo_name} is no longer extracted "
                    f"(its picks changed since the last extraction) — re-extract it"
                )
        return f"the pick list behind '{producer_id}' no longer exists — re-extract, or repoint this input"

    def _dangling_imported_message(self) -> str:
        """Why an imported-tomograms override stopped resolving. Kept separate from the
        merged-sources message, which would name the wrong artifact and file type."""
        rec = getattr(self.state, "imported_tomograms", None)
        if not rec or not rec.star_path:
            return "the tomogram import this input points at was removed — re-import, or repoint this input"
        return (
            f"the imported tomograms.star is missing from disk ({rec.star_path}) — "
            "re-import those tomograms, or repoint this input"
        )

    def _resolve_override(
        self, slot: InputSlot, override_key: str, index: dict[JobFileType, list[OutputCandidate]]
    ) -> OutputCandidate | None:
        """
        Resolve a user override to a candidate.

        override_key format:
          - "jobtype:instance:<instance_id>" for a job, e.g. "templatematching:instance:templatematching__2"
          - "jobtype:instance_path" for a synthetic producer, e.g. "mergedSources:MergedSources/<slug>",
            and for a job in older project files, e.g. "tsReconstruct:External/job005"
          - "manual:/path/to/file" for manual paths (handled separately)
        """
        if override_key.startswith("manual:"):
            return None

        if ":" not in override_key:
            return None

        job_type_str, instance_path = override_key.split(":", 1)
        instance_id = override_instance_id(override_key)
        if instance_id is not None:
            matches = [
                candidate
                for accepted_type in slot.accepts
                for candidate in index.get(accepted_type, [])
                if candidate.producer_job_type.value == job_type_str and candidate.producer_instance_id == instance_id
            ]
            return next((m for m in matches if m.prefer_if_exists), matches[0]) if matches else None

        if job_type_str == JobType.MERGED_SOURCES.value and instance_path == IMPORTED_TOMOGRAMS_INSTANCE_PATH:
            # Legacy key: older projects persist imported-tomogram overrides under
            # MERGED_SOURCES. Rewritten at read time rather than migrated on load — the
            # override lives on every job model that carries one, and a read-time rewrite
            # cannot miss a project.
            job_type_str = JobType.IMPORTED_TOMOGRAMS.value

        matches = [
            candidate
            for accepted_type in slot.accepts
            for candidate in index.get(accepted_type, [])
            if candidate.producer_job_type.value == job_type_str and candidate.instance_path == instance_path
        ]
        if not matches:
            # An override picked before its producer was deployed names the placeholder
            # `<category>/pending_<instance_id>`; once that instance has a job directory, its output
            # is what the override meant.
            _category, _, folder = instance_path.partition("/")
            if folder.startswith("pending_"):
                pending_id = folder.removeprefix("pending_")
                matches = [
                    candidate
                    for accepted_type in slot.accepts
                    for candidate in index.get(accepted_type, [])
                    if candidate.producer_job_type.value == job_type_str
                    and candidate.producer_instance_id == pending_id
                ]
        if not matches:
            return None
        # A producer may expose both a raw output and a curated `prefer_if_exists`
        # sibling of the same type under one source_key (e.g. optimisation_set vs
        # optimisation_set_filtered). Honor the curated one — matches automatic
        # selection (_auto_select) and the UI dropdown's collapsed row.
        return next((m for m in matches if m.prefer_if_exists), matches[0])

    # -------------------------------------------------------------------------
    # Indexing producers
    # -------------------------------------------------------------------------

    def _build_output_index(self) -> dict[JobFileType, list[OutputCandidate]]:
        if self._output_index is not None:
            return self._output_index

        project_root = self._project_root()
        index: dict[JobFileType, list[OutputCandidate]] = {t: [] for t in JobFileType}

        for instance_id, producer_model in self.state.jobs.items():
            producer_job_type = producer_model.job_type
            if producer_job_type is None:
                continue

            out_schema = self._get_output_schema(producer_job_type)
            if not out_schema:
                continue

            relion_job_number = int(getattr(producer_model, "relion_job_number", 0) or 0)
            status = getattr(producer_model, "execution_status", JobStatus.UNKNOWN)

            # Interactive jobs (e.g. TiltFilter) are never dispatched by the relion
            # schemer -- they only produce outputs when the user explicitly commits
            # them via the UI (which flips execution_status to SUCCEEDED). Until then
            # they must not appear in the producer pool: otherwise a merely-SCHEDULED
            # interactive job beats a real upstream via preferred_source and the
            # schemer ends up polling an `External/pending_<instance>/...` placeholder
            # that nothing will ever create.
            if getattr(producer_model, "IS_INTERACTIVE", False) and status != JobStatus.SUCCEEDED:
                continue

            instance_path = self._get_instance_path(instance_id, producer_model)
            species_id = getattr(producer_model, "species_id", None)

            for slot in out_schema:
                path = self._get_producer_output_path(
                    instance_id=instance_id,
                    producer_job_type=producer_job_type,
                    producer_model=producer_model,
                    slot=slot,
                    project_root=project_root,
                )
                if not path:
                    continue

                # prefer_if_exists slots are opt-in: don't pollute the candidate
                # pool with a phantom path the resolver might otherwise pick up.
                # The file is only created post-hoc by the UI (curated filters)
                # so its existence is the entire signal.
                if slot.prefer_if_exists and not Path(path).exists():
                    continue

                index[slot.produces].append(
                    OutputCandidate(
                        produces=slot.produces,
                        producer_job_type=producer_job_type,
                        producer_output_key=slot.key,
                        path=path,
                        instance_path=instance_path,
                        producer_instance_id=instance_id,
                        execution_status=status,
                        relion_job_number=relion_job_number,
                        species_id=species_id,
                        prefer_if_exists=slot.prefer_if_exists,
                    )
                )

        self._add_merged_sources_candidates(index, project_root)
        self._add_imported_tomograms_candidates(index, project_root)
        self._add_pick_list_optset_candidates(index)

        for t, lst in index.items():
            index[t] = sorted(lst, key=lambda c: (c.producer_job_type.value, c.instance_path, c.producer_output_key))

        self._output_index = index
        return index

    def _add_merged_sources_candidates(
        self, index: dict[JobFileType, list[OutputCandidate]], project_root: Path
    ) -> None:
        # The merged optimisation_set.star is a project-level resource produced by
        # the aggregation merge card, not a pipeline job. Surface the ACTIVE merge's
        # optset (slug folder: MergedSources/<slug>/optimisation_set.star, or a
        # legacy flat MergedSources/optimisation_set.star) as a synthetic producer so
        # consumers (ReconstructParticle / Class3D / Refine3D) discover it through the
        # normal candidate enumeration -- independent of the state.is_aggregation
        # flag. ProjectState owns the
        # active-merge resolution so the instance_path here matches the source_key
        # that apply_aggregation_overrides writes.
        merged_optset = self.state.active_merged_optset()
        if merged_optset is None or not merged_optset.exists():
            return
        instance_path = self.state.active_merged_optset_instance_path() or "MergedSources"
        m = self.state.active_merge()
        merge_name = (m.name or m.slug) if m is not None else "merged"
        index[JobFileType.OPTIMISATION_SET_STAR].append(
            OutputCandidate(
                produces=JobFileType.OPTIMISATION_SET_STAR,
                producer_job_type=JobType.MERGED_SOURCES,
                producer_output_key="output_optimisation",
                path=str(merged_optset),
                instance_path=instance_path,
                producer_instance_id="mergedSources",
                execution_status=JobStatus.SUCCEEDED,
                relion_job_number=0,
                species_id=None,
                label=f"Merged sources — {merge_name}",
            )
        )

    def _add_imported_tomograms_candidates(
        self, index: dict[JobFileType, list[OutputCandidate]], project_root: Path
    ) -> None:
        # Imported tomograms (PARTICLES-header utility) are a project-level artifact,
        # not a pipeline job. Surface the committed tomograms.star as a synthetic
        # TOMOGRAMS_STAR producer so resolver-based consumers discover it like any
        # reconstructed-tomogram source (mirrors _add_merged_sources_candidates). The
        # manual-picking flow reads the star directly, so this matters only for future
        # resolver consumers (e.g. TemplateMatch -- which separately also needs a
        # tilt-series star, so this alone does not enable TM on imported tomograms).
        rec = getattr(self.state, "imported_tomograms", None)
        if not rec or not rec.star_path:
            return
        star = Path(rec.star_path)
        if not star.is_absolute():
            star = project_root / star
        if not star.exists():
            return
        index[JobFileType.TOMOGRAMS_STAR].append(
            OutputCandidate(
                produces=JobFileType.TOMOGRAMS_STAR,
                producer_job_type=JobType.IMPORTED_TOMOGRAMS,
                producer_output_key="output_star",
                path=str(star),
                instance_path=IMPORTED_TOMOGRAMS_INSTANCE_PATH,
                producer_instance_id=IMPORTED_TOMOGRAMS_PRODUCER_ID,
                execution_status=JobStatus.SUCCEEDED,
                relion_job_number=0,
                species_id=None,
                label=f"Imported tomograms — {rec.count} tomogram(s)" if rec.count else "Imported tomograms",
            )
        )

    def _add_pick_list_optset_candidates(self, index: dict[JobFileType, list[OutputCandidate]]) -> None:
        """Surface every EXTRACTED curation pick list as an OPTIMISATION_SET_STAR producer.

        A manual/imported/merged list becomes consumable downstream only once its picks
        have been subtomo-extracted (`backend.extract_pick_list`, which runs OUTSIDE the
        pipeline graph); `PickList.extracted_path` is that extraction's optset. Injecting
        it here is what lets reconstructParticle / class3d resolve a de-novo species with
        no subtomoExtraction roster row at all. The state is derived
        (`extraction_state()`), so a re-curated list drops out of the pool by itself.

        Multiple EXTRACTED lists can exist for one (species, tomo). All are injected so
        the user can override to any of them; automatic selection considers only the most
        recently extracted one per (species, tomo), and its label says so when there are
        several (the UI dropdown renders labels verbatim). Lists on different tomograms are
        different particle sets: two of them leave a slot ambiguous rather than guessed.
        `relion_job_number`, a free integer for synthetic producers, holds the rank, which
        only orders the menu. A specific list is chosen by overriding the slot, or by naming
        it as a source in the Aggregate-candidates flow.
        """
        from services.models_base import ListExtractionState

        extracted = [
            pl
            for pl in (getattr(self.state, "pick_lists", None) or [])
            if pl.extraction_state() == ListExtractionState.EXTRACTED
        ]
        if not extracted:
            return

        by_tomo: dict[tuple[str, str], list] = {}
        for pl in extracted:
            by_tomo.setdefault((pl.species_id, pl.tomo_name), []).append(pl)

        for (_species_id, tomo_name), group in by_tomo.items():
            # Ascending, so the LAST entry is the latest and rank == list position.
            ranked = sorted(group, key=lambda pl: pl.extracted_at.timestamp() if pl.extracted_at is not None else 0.0)
            for rank, pl in enumerate(ranked):
                latest = rank == len(ranked) - 1
                label = f"Pick list — {pl.label or pl.slug} · {tomo_name}"
                if len(ranked) > 1 and latest:
                    label = f"{label} [latest on this tomogram]"
                producer_id = pick_list_producer_id(pl)
                index[JobFileType.OPTIMISATION_SET_STAR].append(
                    OutputCandidate(
                        produces=JobFileType.OPTIMISATION_SET_STAR,
                        producer_job_type=JobType.MERGED_SOURCES,  # synthetic non-job marker
                        producer_output_key="output_optimisation",
                        path=pl.extracted_path,
                        instance_path=producer_id,
                        producer_instance_id=producer_id,
                        execution_status=JobStatus.SUCCEEDED,
                        relion_job_number=rank,
                        species_id=pl.species_id or None,
                        label=label,
                        auto_eligible=latest,
                    )
                )

    def _get_instance_path(self, instance_id: str, job_model: AbstractJobParams) -> str:
        relion_job_name = getattr(job_model, "relion_job_name", None)
        if relion_job_name:
            return relion_job_name.rstrip("/")

        mapped = (self.state.job_path_mapping or {}).get(instance_id)
        if mapped:
            return mapped.rstrip("/")

        from services.models_base import JobCategory

        category = getattr(job_model, "JOB_CATEGORY", JobCategory.EXTERNAL)
        return f"{category.value}/pending_{instance_id}"

    def _get_producer_output_path(
        self,
        instance_id: str,
        producer_job_type: JobType,
        producer_model: AbstractJobParams,
        slot: OutputSlot,
        project_root: Path,
    ) -> str | None:
        # Prefer relion_job_name + path_template over the cached producer paths dict.
        # The cached dict is a schedule-time snapshot which drifts when the relion
        # schemer allocates a different job number than the orchestrator predicted;
        # relion_job_name is reconciled from default_pipeline.star by sync_all_jobs
        # and is the authoritative identity once the producer has run.
        relion_job_name = getattr(producer_model, "relion_job_name", None)
        if relion_job_name:
            job_dir = (project_root / relion_job_name.rstrip("/")).resolve()
            return str((job_dir / slot.path_template).resolve())

        mapped = (self.state.job_path_mapping or {}).get(instance_id)
        if mapped:
            job_dir = (project_root / mapped.rstrip("/")).resolve()
            return str((job_dir / slot.path_template).resolve())

        # Fall back to the cached paths snapshot only when the producer has no
        # reconciled identity yet (e.g. a fresh job whose relion_job_name was
        # cleared at deploy time and has not yet been picked up by sync_all_jobs).
        stored = (producer_model.paths or {}).get(slot.key)
        if stored:
            return str(Path(stored))

        status = getattr(producer_model, "execution_status", None)
        if status is not None:
            from services.models_base import JobCategory

            category = getattr(producer_model, "JOB_CATEGORY", JobCategory.EXTERNAL)
            predicted_dir = project_root / category.value / f"pending_{instance_id}"
            return str((predicted_dir / slot.path_template).resolve())

        return None

    def _interactive_producer_gap(self, candidate: OutputCandidate) -> str | None:
        """A `pending_<instance>` path is a promise that the dispatcher will create
        the dir when it deploys the producer — a promise interactive jobs (never
        dispatched) cannot make. An interactive producer in the candidate pool is
        SUCCEEDED by construction (see _build_output_index), so a placeholder path
        means its committed output was never recorded on the job model; wiring it
        downstream guarantees a runtime crash. Surface the gap at resolve time."""
        if "pending_" not in candidate.path:
            return None
        jm = self.state.jobs.get(candidate.producer_instance_id)
        if jm is None or not getattr(jm, "IS_INTERACTIVE", False):
            return None
        return (
            f"interactive producer '{candidate.producer_instance_id}' has no committed output on disk "
            f"(path would be the placeholder {candidate.path}, which nothing creates) — "
            f"open its panel, commit/save its output, then requeue"
        )

    def invalidate_cache(self):
        """Call when state changes to rebuild the output index."""
        self._output_index = None

    # -------------------------------------------------------------------------
    # Candidate selection / scoring
    # -------------------------------------------------------------------------

    def _auto_select(
        self,
        slot: InputSlot,
        index: dict[JobFileType, list[OutputCandidate]],
        consumer_type: JobType,
        consumer_species_id: str | None = None,
        consumer_instance_id: str | None = None,
    ) -> AutoChoice:
        """
        Automatic choice of the producer for one input slot.

        The pool is every output of an accepted type that `auto_exclusion` allows (an
        earlier stage, not another species), minus the consumer itself. It is ranked by
        species match (the consumer's own species over species-neutral producers, whether
        or not they have run), then by the slot's preferred source type, then a pipeline
        job over a synthetic producer (imported tomograms, a merge, a pick list), then a
        job on the roster (`state.pipeline_order`) over a finished one taken off it, which
        stays in the project. When the best rank holds one producer instance, that
        producer's output is chosen, its curated `prefer_if_exists` sibling over the raw
        one (the index only admits a curated file that exists). When it holds two or more,
        nothing is chosen and they are returned as tied: an A/B pair is the user's call.
        Status and job numbers never rank, so the choice does not depend on what has run or
        on the order Start allocates job dirs.
        """
        pool = [
            c
            for t in slot.accepts
            for c in index.get(t, [])
            if c.auto_eligible
            and c.producer_instance_id != consumer_instance_id
            and auto_exclusion(consumer_type, consumer_species_id, c) is None
        ]
        if not pool:
            return AutoChoice()
        preferred = self._parse_preferred_source(slot.preferred_source)
        roster = set(self.state.pipeline_order or ())

        def rank(c: OutputCandidate) -> tuple[bool, bool, bool, bool]:
            return (
                bool(consumer_species_id) and c.species_id == consumer_species_id,
                c.producer_job_type == preferred,
                not is_synthetic_producer(c.producer_instance_id),
                not roster or c.producer_instance_id in roster,
            )

        best = max(rank(c) for c in pool)
        per_producer: dict[str, OutputCandidate] = {}
        for c in sorted(pool, key=lambda c: (c.prefer_if_exists, c.producer_output_key, c.path), reverse=True):
            if rank(c) == best:
                per_producer.setdefault(c.producer_instance_id, c)
        if len(per_producer) > 1:
            return AutoChoice(tied=tuple(per_producer[p] for p in sorted(per_producer)))
        (chosen,) = per_producer.values()
        return AutoChoice(candidate=self._follow_refinements(chosen, index, consumer_instance_id))

    def _follow_refinements(
        self, chosen: OutputCandidate, index: dict[JobFileType, list[OutputCandidate]], consumer_instance_id: str | None
    ) -> OutputCandidate:
        """Rebind an automatically chosen input to the job that refines it.

        A job R declaring `JobSpec.refines` ∋ T, in `state.pipeline_order`, before the consumer in
        PIPELINE_ORDER, whose own effective input of type T is exactly `chosen`, takes the
        consumer's T: the consumer gets R's T output instead. Repeats, so refinements chain. User
        overrides never reach here. Without a consumer id there is no order to compare, so the
        choice stands.

        Several such R (refinement runs compared side by side): the most recently deployed one wins
        (highest job number, then the latest in pipeline order), with a logged warning. Deploy
        allocates a refiner before its consumers, so a refiner and its consumers added and run
        together pair up; any other pairing is set in the consumer's IO tab.
        """
        if consumer_instance_id is None:
            return chosen
        from services.jobs.spec import JOB_SPEC_BY_TYPE, PIPELINE_ORDER

        consumer_type = InstanceId.parse(consumer_instance_id).job_type
        if consumer_type not in PIPELINE_ORDER:
            return chosen
        consumer_pos = PIPELINE_ORDER.index(consumer_type)
        t = chosen.produces
        followed: set[str] = set()
        while True:
            refiners: list[tuple[str, OutputCandidate]] = []
            for rid in self.state.pipeline_order or []:
                r_model = self.state.jobs.get(rid)
                if rid == consumer_instance_id or rid in followed or r_model is None:
                    continue
                spec = JOB_SPEC_BY_TYPE.get(r_model.job_type)
                if spec is None or t not in spec.refines or PIPELINE_ORDER.index(spec.job_type) >= consumer_pos:
                    continue
                r_input = self._effective_input(rid, r_model, t, index)
                if r_input is None or (r_input.producer_instance_id, r_input.producer_output_key) != (
                    chosen.producer_instance_id,
                    chosen.producer_output_key,
                ):
                    continue
                r_output = next((c for c in index.get(t, []) if c.producer_instance_id == rid), None)
                if r_output is not None:
                    refiners.append((rid, r_output))
            if not refiners:
                return chosen
            order = {rid: i for i, rid in enumerate(self.state.pipeline_order or [])}
            rid, candidate = max(refiners, key=lambda r: (r[1].relion_job_number or 0, order.get(r[0], -1)))
            if len(refiners) > 1:
                key = (consumer_instance_id, *sorted(r for r, _ in refiners))
                if key not in _AMBIGUOUS_REFINEMENTS_WARNED:  # resolution reruns on every IO-tab render
                    _AMBIGUOUS_REFINEMENTS_WARNED.add(key)
                    logger.warning(
                        "%s: %s all refine %s's %s; following the most recently deployed, %s — set the IO tab "
                        "to pair it with another",
                        consumer_instance_id,
                        ", ".join(r for r, _ in refiners),
                        chosen.producer_instance_id,
                        t.value,
                        rid,
                    )
            chosen = candidate
            followed.add(rid)

    def _effective_input(
        self,
        instance_id: str,
        job_model: AbstractJobParams,
        t: JobFileType,
        index: dict[JobFileType, list[OutputCandidate]],
    ) -> OutputCandidate | None:
        """The producer `instance_id` reads its input of type `t` from, chosen the way
        resolve_inputs chooses it; None for no such slot, a manual file, or no source."""
        slot = next((s for s in self._get_input_schema(job_model.job_type) if t in s.accepts), None)
        if slot is None:
            return None
        return self._select_source(job_model.job_type, job_model, slot, index, instance_id).candidate

    def _parse_preferred_source(self, preferred: str | None) -> JobType | None:
        if not preferred:
            return None
        try:
            return JobType.from_string(preferred)
        except Exception:
            return None

    # -------------------------------------------------------------------------
    # Schema access
    # -------------------------------------------------------------------------

    def _get_input_schema(self, job_type: JobType) -> list[InputSlot]:
        from services.job_models import jobtype_paramclass

        cls = jobtype_paramclass().get(job_type)
        schema = getattr(cls, "INPUT_SCHEMA", None) if cls else None
        return list(schema) if schema else []

    def _get_output_schema(self, job_type: JobType) -> list[OutputSlot]:
        from services.job_models import jobtype_paramclass

        cls = jobtype_paramclass().get(job_type)
        schema = getattr(cls, "OUTPUT_SCHEMA", None) if cls else None
        return list(schema) if schema else []

    def _project_root(self) -> Path:
        if not self.state.project_path:
            raise PathResolutionError("ProjectState.project_path is not set")
        return Path(self.state.project_path).resolve()


def migrate_override_keys(state: ProjectState) -> int:
    """Rewrite overrides that name a job by its directory ("templatematching:External/job007", or
    the "<category>/pending_<instance_id>" placeholder) to name its instance, mapped through the
    directory each job holds now; a later Start that allocates dirs afresh can then never move an
    override onto another job. Keys of synthetic producers and manual files stay as they are, and
    so does a key whose directory belongs to no job (path resolution reports it). Returns how many
    keys were rewritten."""
    resolver = PathResolutionService(state)
    owner: dict[tuple[str, str], str] = {}
    for instance_id, job_model in state.jobs.items():
        if job_model.job_type is not None:
            owner[(job_model.job_type.value, resolver._get_instance_path(instance_id, job_model))] = instance_id

    rewritten = 0
    for job_model in state.jobs.values():
        overrides = getattr(job_model, "source_overrides", None) or {}
        for slot_key, key in list(overrides.items()):
            if (
                key.startswith("manual:")
                or ":" not in key
                or _synthetic_override_target(key)
                or override_instance_id(key) is not None
            ):
                continue
            job_type_str, _, ref = key.partition(":")
            ref = ref.rstrip("/")
            instance_id = owner.get((job_type_str, ref))
            if instance_id is None:
                folder = ref.partition("/")[2]
                pending_id = folder.removeprefix("pending_") if folder.startswith("pending_") else None
                producer = state.jobs.get(pending_id) if pending_id else None
                if producer is not None and producer.job_type is not None and producer.job_type.value == job_type_str:
                    instance_id = pending_id
            if instance_id is not None:
                overrides[slot_key] = instance_override_key(state.jobs[instance_id].job_type, instance_id)
                rewritten += 1
    return rewritten


def pin_sources_made_ambiguous(
    state: ProjectState, before: dict[tuple[str, str], OutputCandidate]
) -> list[tuple[str, str, str]]:
    """Keep jobs on the sources they read before a job was added. `before` is
    `automatic_sources()` from just before the addition; every slot of it that is now ambiguous
    (the new job ties with its source, an A/B copy) gets that source as an explicit override.
    Returns (instance_id, slot_key, producer instance_id) per pin."""
    resolver = PathResolutionService(state)
    index = resolver._build_output_index()
    pinned: list[tuple[str, str, str]] = []
    for (instance_id, slot_key), source in before.items():
        job_model = state.jobs.get(instance_id)
        if job_model is None or job_model.job_type is None:
            continue
        if job_model.source_overrides is None:
            job_model.source_overrides = {}
        if job_model.source_overrides.get(slot_key):
            continue
        slot = next((s for s in resolver._get_input_schema(job_model.job_type) if s.key == slot_key), None)
        if slot is None:
            continue
        choice = resolver._auto_select(
            slot, index, job_model.job_type, getattr(job_model, "species_id", None), instance_id
        )
        if choice.tied:
            job_model.source_overrides[slot_key] = source.source_key
            pinned.append((instance_id, slot_key, source.producer_instance_id))
    return pinned


# -----------------------------------------------------------------------------
# Context Paths Helper
# -----------------------------------------------------------------------------


def get_context_paths(job_type: JobType, job_model: AbstractJobParams, job_dir: Path) -> dict[str, str]:
    project_root = job_model.project_root
    paths: dict[str, str] = {"job_dir": str(job_dir), "project_root": str(project_root)}

    if job_type in [JobType.IMPORT_MOVIES, JobType.FS_MOTION_CTF, JobType.TS_IMPORT]:
        paths["mdoc_dir"] = str(project_root / "mdoc")

    if job_type in [JobType.IMPORT_MOVIES, JobType.FS_MOTION_CTF]:
        paths["frames_dir"] = str(project_root / "frames")

    # tomostar_dir: for TS_IMPORT it's an output (resolved by IO slots).
    # For alignment/ctf/reconstruct it comes from upstream via IO slots.
    # Only import_movies and fs_motion_ctf use the project-root fallback.
    if job_type in [JobType.IMPORT_MOVIES, JobType.FS_MOTION_CTF]:
        paths["tomostar_dir"] = str(project_root / "tomostar")

    if job_type == JobType.IMPORT_MOVIES:
        paths["tilt_series_dir"] = str(job_dir / "tilt_series")

    if job_type == JobType.DENOISE_PREDICT:
        paths["output_dir"] = str(job_dir / "denoised")

    if job_type == JobType.TEMPLATE_MATCH_PYTOM:
        tm_model = job_model
        # Resolution:
        #   1. Job-level template_path / mask_path acts as a per-job
        #      override when explicitly set. The TM plugin's dropdowns
        #      write a specific entry's path into these fields, so the
        #      job carries a resolved snapshot for the driver.
        #   2. If the job-level fields are empty, fall back to the
        #      species's selected template / mask (looked up via UUID
        #      from species.templates / species.masks).
        template_p = getattr(tm_model, "template_path", "") or ""
        mask_p = getattr(tm_model, "mask_path", "") or ""

        if not template_p or not mask_p:
            species = None
            species_id = getattr(tm_model, "species_id", None)
            project_state = getattr(tm_model, "_project_state", None)
            if species_id and project_state is not None:
                get_species = getattr(project_state, "get_species", None)
                if callable(get_species):
                    species = get_species(species_id)

            if species is not None:
                if not template_p:
                    sel_t = species.get_selected_template() if hasattr(species, "get_selected_template") else None
                    if sel_t is not None:
                        template_p = sel_t.template_path or ""
                if not mask_p:
                    sel_m = species.get_selected_mask() if hasattr(species, "get_selected_mask") else None
                    if sel_m is not None:
                        mask_p = sel_m.mask_path or ""

        if template_p:
            paths["template_path"] = str(Path(template_p))
        if mask_p:
            paths["mask_path"] = str(Path(mask_p))

    return paths
