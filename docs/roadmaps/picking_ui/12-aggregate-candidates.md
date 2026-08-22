# 12 — Aggregate candidates (stub — noted, not scheduled)

**Status:** stub, 2026-08-21, per the maintainer: *"none of the aggregation controls belong [on the
picks surface]… the merge-sources icon on the Particles is enough except it should probably be
called something like 'aggregate candidates' and just spawn a 'Pick Candidates' job… so the
coordinate lists (whether produced by manual picking, TM or something else) can be united and
directly initiated in a pick coordinates job prepopulated with the combined list. You can note this
as a roadmap for now."* (`q_important_ui_fixes.md:34`)

## The idea

One flow, launched from the roster PARTICLES-header icon
(`ui/pipeline_builder/pipeline_roster.py:1513-1552`, today "Merge particle sources from several
projects" → `ui/aggregation/merge_card.py`):

1. Rename: icon tooltip + dialog title → **"Aggregate candidates"**.
2. The dialog's selection tree (Project → Species → Tomogram, `merge_card.py:412`) stays; the
   terminal action changes from "produce a merged source" to **"spawn a Pick candidates job
   prepopulated with the combined list"** for the current project.
3. Within-project list unions (the merge bar deleted from the picks surface in 09-S3) fold into the
   same flow — selecting several lists of one tomogram is just the degenerate case of the tree.
   `list_actions.merge_lists` / dedup remain the service layer.

## The open design edge (why this is a stub, not a stage list)

"Pick candidates" (`tmExtractCand`) today consumes a TM job's score volumes and *produces* a
candidate list — it does not take a coordinate list as input. Feeding it a pre-combined list needs
the job to grow a **list-input mode** (skip scoring, start from the provided coordinates: dedup /
NMS / threshold → candidates), which lands on the pick-list-as-first-class-job-identity seam
(archived roadmap `07-extract-pick-list-job-identity.md`, #68) and on the aggregation forwarding
capstone in `docs/LIST_EXTRACTION_AND_AGGREGATION.md` §8.9. Alternatively the spawned job is a
*Subtomo extraction* prepopulated with the merged list, if what the maintainer wants downstream is
extraction rather than re-thresholding — **to be decided with the maintainer when this is picked
up.**

## Prerequisites when scheduled

- 09-S3 landed (merge controls off the picks surface).
- Decision on the spawned job type (above).
- The merged-optset resolver work already landed (PICKS_FILTER_AGGREGATION_ROADMAP ①) is the
  downstream consumer — keep its contract.

## Log

- 2026-08-21 — stub written; deliberately unscheduled while the de-novo picking interface
  (roadmaps 08–11) is the focus.
