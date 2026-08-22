from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, TYPE_CHECKING
from collections.abc import Sequence

from services.io_slots import InputSlot, OutputSlot, JobFileType, ResolvedInput, ResolvedOutput, ResolvedManifest

from services.models_base import JobType, JobStatus

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

    # metadata for scoring
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

    @property
    def source_key(self) -> str:
        """Key format for source_overrides: 'jobtype:instance_path'"""
        return f"{self.producer_job_type.value}:{self.instance_path}"

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


class PathResolutionError(ValueError):
    pass


# Instance-path markers for the synthetic (non-job) producers. The merge and the pick-list
# extractions have no JobType of their own and borrow MERGED_SOURCES, told apart by instance
# path; imported tomograms got their own (JobType.IMPORTED_TOMOGRAMS, de-novo roadmap D-8/S5),
# so only their LEGACY override keys still carry the mergedSources prefix.
PICK_LIST_PRODUCER_PREFIX = "pick_list__"
IMPORTED_TOMOGRAMS_INSTANCE_PATH = "Tomograms"
IMPORTED_TOMOGRAMS_PRODUCER_ID = "importedTomograms"


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


def _synthetic_override_target(override_key: str) -> str:
    """Which synthetic producer an override names: "merged" | "pick_list" | "imported" | "".

    The merge and the per-pick-list extractions have no JobType of their own, so both wear
    the ``mergedSources:`` prefix and are told apart by instance path — routing a dangling
    pick-list override through the merged-sources branch would tell the user
    "merged-sources optimisation_set not found" about a list that simply needs re-extracting.
    Imported tomograms now have their own JobType (D-8/S5); the old ``mergedSources:Tomograms``
    form is still recognised so overrides persisted before that keep resolving."""
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
    Stage 3: schema-based path resolution.
    Now with user override support, candidate enumeration for UI,
    and species-aware scoring.
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
        Resolve inputs for target job. Checks source_overrides first, then falls
        back to species-aware automatic selection.

        consumer_instance_id excludes the consumer from its own candidate pool
        (a job's OUTPUT slot is never a valid source for its own INPUT slot).
        """
        input_schema = self._get_input_schema(job_type)
        index = self._build_output_index()
        overrides = getattr(job_model, "source_overrides", {}) or {}
        consumer_species_id = getattr(job_model, "species_id", None)

        resolved_inputs: list[ResolvedInput] = []
        missing_required: list[str] = []

        for slot in input_schema:
            chosen = None

            # 1. Check for user override first
            override_key = overrides.get(slot.key)
            if override_key:
                chosen = self._resolve_override(slot, override_key, index)

                # Manual overrides (manual:/abs/path) are never resolvable via
                # the producer index. Materialize them directly into a
                # ResolvedInput so they actually populate job_model.paths.
                if chosen is None and override_key.startswith("manual:"):
                    manual_path = override_key[len("manual:") :]
                    resolved_inputs.append(
                        ResolvedInput(
                            input_key=slot.key,
                            chosen_type=slot.accepts[0],
                            source_job_type="manual",
                            source_instance_id=None,
                            source_output_key="manual",
                            path=manual_path,
                        )
                    )
                    continue

                # A synthetic-producer override that no longer resolves (the active
                # merge's optimisation_set.star was deleted/moved, the active merge
                # switched to a slug whose file is absent, or a consumed pick list
                # went stale) must SURFACE, not silently fall through to some other
                # optset producer (e.g. a downstream Class3D output). Mirrored in
                # validate_input_slot.
                if chosen is None:
                    target = _synthetic_override_target(override_key)
                    if target == "pick_list":
                        if slot.required:
                            missing_required.append(f"{slot.key}: {self._dangling_pick_list_message(override_key)}")
                        continue
                    if target == "imported":
                        if slot.required:
                            missing_required.append(f"{slot.key}: {self._dangling_imported_message()}")
                        continue
                    if target:
                        if slot.required:
                            missing_required.append(f"{slot.key} (merged-sources optimisation_set not found)")
                        continue

            # 2. Fall back to species-aware automatic selection
            if chosen is None:
                chosen = self._choose_candidate_for_slot(
                    slot, index, consumer_species_id, consumer_instance_id=consumer_instance_id
                )

            if chosen is None:
                if slot.required:
                    missing_required.append(f"{slot.key} accepts={[t.value for t in slot.accepts]}")
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
        """
        Derive the producer->consumer dependency edges of the pipeline DAG.

        For every consumer job (restricted to ``instance_ids`` if given, else all
        jobs in state) each input slot is resolved exactly the way
        :meth:`resolve_inputs` selects a source -- user ``source_overrides`` first,
        then species-aware automatic selection -- but the chosen producer's *clean*
        ``instance_id`` is recorded instead of a path. Returns deduplicated
        ``(producer_instance_id, consumer_instance_id)`` pairs, suitable for a SLURM
        ``--dependency=afterok`` topological submit (P1.A of the orchestrator
        rework; see ORCHESTRATOR_REPLACEMENT_PLAN.md §6).

        Producers with no SLURM job are excluded: a ``manual:`` file override
        (user-picked file, no producing job) and every synthetic producer
        (``is_synthetic_producer`` -- the aggregation merge, imported tomograms, and
        per-pick-list extractions) -- an ``afterok`` on any of them would never be
        satisfiable.
        Slots that cannot be resolved yet are skipped silently (no exception), so
        this is safe on a partially-configured pipeline.

        Selection mirrors :meth:`resolve_inputs` but is kept separate so edge
        derivation tolerates unresolved slots and never resolves paths. The caller
        restricts producers to the jobs actually being submitted and drops
        already-SUCCEEDED producers (a finished upstream needs no dependency).
        """
        consumers = list(instance_ids) if instance_ids is not None else list(self.state.jobs.keys())
        index = self._build_output_index()
        edges: list[tuple[str, str]] = []
        seen: set = set()

        for consumer_id in consumers:
            job_model = self.state.jobs.get(consumer_id)
            if job_model is None or job_model.job_type is None:
                continue

            overrides = getattr(job_model, "source_overrides", {}) or {}
            consumer_species_id = getattr(job_model, "species_id", None)

            for slot in self._get_input_schema(job_model.job_type):
                chosen = None
                override_key = overrides.get(slot.key)
                if override_key:
                    if override_key.startswith("manual:"):
                        continue  # user-picked file -- no producing job, no edge
                    chosen = self._resolve_override(slot, override_key, index)
                if chosen is None:
                    chosen = self._choose_candidate_for_slot(
                        slot, index, consumer_species_id, consumer_instance_id=consumer_id
                    )
                if chosen is None:
                    continue

                producer_id = chosen.producer_instance_id
                if not producer_id or is_synthetic_producer(producer_id) or producer_id == consumer_id:
                    # synthetic / non-job producer, or a self-edge from a pathological
                    # override (the override path, unlike auto-selection, does not
                    # exclude the consumer) -- neither is a valid afterok dependency.
                    continue
                edge = (producer_id, consumer_id)
                if edge not in seen:
                    seen.add(edge)
                    edges.append(edge)

        return edges

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
        Get all valid candidates for a specific input slot, sorted with
        species-matched candidates first. Unmatched candidates are included
        but ranked lower -- the UI can dim them to signal the mismatch.

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

        def sort_key(c: OutputCandidate) -> tuple[int, int, int, str]:
            # Lower value = sorted earlier
            species_rank = 0 if (consumer_species_id and c.species_id == consumer_species_id) else 1
            status_order = {
                JobStatus.SUCCEEDED: 0,
                JobStatus.RUNNING: 1,
                JobStatus.SCHEDULED: 2,
                JobStatus.FAILED: 3,
            }.get(c.execution_status, 4)
            return (species_rank, status_order, -c.relion_job_number, c.instance_path)

        ordered = sorted(candidates, key=sort_key)

        # Collapse candidates that share a source_key. A single producing job can
        # expose several outputs of the SAME JobFileType — e.g. subtomo extraction
        # emits both a raw optimisation_set.star and its curated `_filtered`
        # sibling. They map to the identical override key (jobtype:instance_path),
        # so listing both as separate rows produces confusing duplicate entries
        # with identical labels ("Subtomo Extraction #2 (job017)" twice). Keep one
        # row per source_key, preferring the `prefer_if_exists` (curated) sibling
        # so it matches what _resolve_override / the auto-scorer actually pick.
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
        Returns detailed validation result for UI feedback.
        Uses species-aware candidate selection.
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
        override_key = overrides.get(slot_key)
        is_user_override = override_key is not None
        consumer_species_id = getattr(job_model, "species_id", None)

        index = self._build_output_index()
        chosen = None

        if override_key:
            chosen = self._resolve_override(slot, override_key, index)
            if chosen is None and override_key.startswith("manual:"):
                manual_path = override_key[7:]
                file_exists = Path(manual_path).exists() if check_filesystem else True
                return InputSlotValidation(
                    slot_key=slot_key,
                    is_valid=file_exists or not slot.required,
                    source_key=override_key,
                    resolved_path=manual_path,
                    file_exists=file_exists,
                    is_user_override=True,
                    error_message=None if file_exists else f"File not found: {manual_path}",
                )

            # Dangling synthetic-producer override (its optset is gone) -> surface red,
            # don't silently auto-pick a foreign optset producer. Mirrors resolve_inputs.
            if chosen is None:
                target = _synthetic_override_target(override_key)
                if target:
                    return InputSlotValidation(
                        slot_key=slot_key,
                        is_valid=not slot.required,
                        source_key=override_key,
                        resolved_path=None,
                        file_exists=False,
                        is_user_override=True,
                        error_message=(
                            self._dangling_pick_list_message(override_key)
                            if target == "pick_list"
                            else self._dangling_imported_message()
                            if target == "imported"
                            else "Merged-sources optimisation_set not found (merge deleted or active merge switched?)"
                        ),
                    )

        if chosen is None:
            chosen = self._choose_candidate_for_slot(
                slot, index, consumer_species_id, consumer_instance_id=consumer_instance_id
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
        """Why an imported-tomograms override stopped resolving. Its own message since D-8/S5:
        before that it borrowed MERGED_SOURCES and reported itself as a missing merged-sources
        optimisation set, which named the wrong artifact AND the wrong file type."""
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
          - "jobtype:instance_path" e.g., "tsReconstruct:External/job005"
          - "manual:/path/to/file" for manual paths (handled separately)
        """
        if override_key.startswith("manual:"):
            return None

        if ":" not in override_key:
            return None

        job_type_str, instance_path = override_key.split(":", 1)
        if job_type_str == JobType.MERGED_SOURCES.value and instance_path == IMPORTED_TOMOGRAMS_INSTANCE_PATH:
            # Legacy key: imported tomograms borrowed MERGED_SOURCES until D-8/S5 gave them
            # their own JobType, so an override persisted before that names the old producer.
            # Rewritten rather than migrated on load — the override lives on every job model
            # that carries one, and a read-time rewrite cannot miss a project.
            job_type_str = JobType.IMPORTED_TOMOGRAMS.value

        matches = [
            candidate
            for accepted_type in slot.accepts
            for candidate in index.get(accepted_type, [])
            if candidate.producer_job_type.value == job_type_str and candidate.instance_path == instance_path
        ]
        if not matches:
            return None
        # A producer may expose both a raw output and a curated `prefer_if_exists`
        # sibling of the same type under one source_key (e.g. optimisation_set vs
        # optimisation_set_filtered). Honor the curated one — matches the auto
        # scorer (_choose_candidate_for_slot) and the UI dropdown's collapsed row.
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
        # normal candidate enumeration -- works regardless of the state.is_aggregation
        # flag, which has been a single point of failure. ProjectState owns the
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
                producer_job_type=JobType.IMPORTED_TOMOGRAMS,  # its own sentinel since D-8/S5
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
        no subtomoExtraction roster row at all (denovo roadmap D-7). The state is DERIVED
        (`extraction_state()`), so a re-curated list drops out of the pool by itself.

        Multiple EXTRACTED lists can exist for one (species, tomo). All are injected so
        the user can override to any of them, but auto-selection must be deterministic:
        `relion_job_number` — a free integer for synthetic producers, and already the
        scorer's preference rank — is assigned so the authoritative slug outranks the
        rest, then newer extractions outrank older. The winner says so in its label, which
        the UI dropdown renders verbatim, so the choice is never silent.
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

        for (species_id, tomo_name), group in by_tomo.items():
            authoritative = self.state.get_authoritative_slug(species_id, tomo_name)
            # Ascending, so the LAST entry is the winner and rank == list position.
            # Ranks start at 0 so a lone pick list ties the other synthetic producers
            # (mergedSources / importedTomograms) exactly as before; only a genuine
            # multi-list tie spends rank to break itself.
            ranked = sorted(
                group,
                key=lambda pl: (
                    pl.slug == authoritative,
                    pl.extracted_at.timestamp() if pl.extracted_at is not None else 0.0,
                ),
            )
            for rank, pl in enumerate(ranked):
                label = f"Pick list — {pl.label or pl.slug} · {tomo_name}"
                if len(ranked) > 1 and rank == len(ranked) - 1:
                    why = "authoritative" if pl.slug == authoritative else "most recently extracted"
                    label = f"{label} [auto-selected: {why}]"
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

    def _choose_candidate_for_slot(
        self,
        slot: InputSlot,
        index: dict[JobFileType, list[OutputCandidate]],
        consumer_species_id: str | None = None,
        consumer_instance_id: str | None = None,
    ) -> OutputCandidate | None:
        """
        Find best candidate among accepted types using deterministic scoring.

        Priority order (highest to lowest):
          1. Species match -- candidate produced by a job tagged with the same species_id
          2. Preferred source job type (from slot.preferred_source)
          3. Most recent job number
          4. Succeeded status

        The consumer is always excluded from its own candidate pool: a job whose
        INPUT and OUTPUT schemas share a JobFileType (e.g. templatematching both
        consuming and producing TOMOGRAMS_STAR) would otherwise pick its own
        in-flight output once sync_all_jobs has set its relion_job_name, because
        the scoring prefers higher job numbers before SUCCEEDED status.
        """
        candidates: list[OutputCandidate] = []
        for t in slot.accepts:
            candidates.extend(index.get(t, []))

        if consumer_instance_id is not None:
            candidates = [c for c in candidates if c.producer_instance_id != consumer_instance_id]

        if not candidates:
            return None

        preferred_job_type = self._parse_preferred_source(slot.preferred_source)

        def score(c: OutputCandidate) -> tuple[int, int, int, int, int]:
            succeeded = 1 if c.execution_status == JobStatus.SUCCEEDED else 0
            # Species match only counts if the candidate has actually run.
            # This prevents a job's own pending output from circularly winning
            # over a real succeeded upstream (e.g. TM output TOMOGRAMS_STAR
            # outranking tsReconstruct's TOMOGRAMS_STAR as TM's own input).
            species_match = 1 if (consumer_species_id and c.species_id == consumer_species_id and succeeded) else 0
            pref = 1 if (preferred_job_type and c.producer_job_type == preferred_job_type) else 0
            # prefer_if_exists ranks below cross-producer signals (species/pref)
            # but above relion_job_number, so a curated filtered file from
            # producer X beats producer X's original output even when X has the
            # latest job number for its sibling. Build-index already gated on
            # file existence, so a prefer_if_exists=True candidate here is real.
            filtered_pref = 1 if c.prefer_if_exists else 0
            return (species_match, pref, filtered_pref, c.relion_job_number, succeeded)

        return max(candidates, key=lambda c: (score(c), c.producer_job_type.value, c.producer_output_key, c.path))

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


# -----------------------------------------------------------------------------
# Context Paths Helper (unchanged)
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
    # Only import_movies and fs_motion_ctf still use the project-root fallback.
    if job_type in [JobType.IMPORT_MOVIES, JobType.FS_MOTION_CTF]:
        paths["tomostar_dir"] = str(project_root / "tomostar")

    if job_type == JobType.IMPORT_MOVIES:
        paths["tilt_series_dir"] = str(job_dir / "tilt_series")

    if job_type == JobType.DENOISE_PREDICT:
        paths["output_dir"] = str(job_dir / "denoised")

    if job_type == JobType.TEMPLATE_MATCH_PYTOM:
        tm_model = job_model
        # v3 resolution:
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
