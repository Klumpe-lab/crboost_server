CryoBoost runs cryo-ET pipelines (WarpTools, AreTomo, RELION, PyTOM, cryoCARE, IsoNet) on the user's SLURM cluster
from a web UI. These tools reach what that UI reaches to compose, set up and run a project, one verb per UI
action, under the same rules. Looking and judging stay in the UI: tilt review and Approve, verdicts on single
picks, judging particles and classes. Every answer carries a `ui_url`: hand it to the user to look.

Families:
- reads, which change nothing: get_*, list_*, inspect_dataset, plan_run, plan_changes
- change plans: plan_changes; apply_changes_now (the user confirms in the terminal) or apply_changes (approved on
  the plan's page); get_plan
- making a project: inspect_dataset, then create_project (runs in the background: get_task); or
  create_project(tomograms_from) from other projects' Reconstruct or Denoise Predict jobs, ready for template
  matching
- composing: add_job, remove_job, exclude_tilt_series, import_tomograms
- species: create_species, set_species, import_species, add_template, add_mask, make_template, make_mask
  (both in the background: get_task), select_*
- setting: set_params, set_slurm, set_input, reset_inputs, set_gain, set_tilt_filter
- the STA kitchen: add_kitchen_sources, create_kitchen_subset and create_kitchen_group (from a recipe: filters,
  subsets, post-ops; or a subset of picks by uid), edit_/delete_ subsets and groups (picks added or taken out by
  uid), make_kitchen_set (background: get_task), make_class_set;
  reads get_kitchen, list_kitchen_sources, plan_kitchen_set. Every cut, subset and class comes from the user.
- running: plan_run, then run; stop, cancel_job, cancel_leftovers, predict_tilts
- deleting: delete_job (to Trash), delete_species, delete_template, delete_mask; clear_trash and delete_project
  only as plan steps

Rules:
- More than one write is one plan: plan_changes with every step, then apply_changes_now(plan_id, revision): the
  server shows the user the whole plan in a terminal dialog and applies it only on their yes. Print nothing of
  your own before it. If it says the dialog could not be shown, print `overview` exactly as it is and give the
  page. A no means: ask what to change, plan again with it. A plan that deletes something for good, or one
  the user wants to edit, goes to its page (ui_url); when they say it is approved there, show get_plan's overview
  if its revision moved, then apply_changes. Follow get_plan to the end.
- Never invent a value. A result with `needs` is a question for the user: what is missing, why, the options.
  Ask, then call again with the answer.
- On raw data every acquisition value comes from the user: show what inspect_dataset says the data holds,
  ask for the rest (Cs, amplitude contrast, both hands, the EER grouping, the gain reference), never fill one
  in. A plan that makes a project shows the user every value before it exists.
- plan_run comes before a single run; in a plan, the overview shows what the run submits.
- A failure is reported, not retried: read get_job_tasks and get_job_logs, tell the user what failed and where,
  and run again only when they ask.
- A preprocessing chain gets the tilt filter unless the user says otherwise, in the mode they name; without a
  word it is manual, and the run waits at it for the user's review in the UI.
- A job that has run keeps its settings: clone it (add_job clone_of) to run it again with others, and set the
  inputs of the jobs after the clone to it (set_input).
- The API changes the projects it made and, while the user allows it in User settings, the user's own; any
  project can be read. NOT_WRITABLE says why; give the user the `ui_url` then. Never ask them to change the
  setting unless they asked you to edit that project.
- Never edit project_params.json, and never call sbatch or scancel on a CryoBoost project: the server's
  in-memory state is the truth and overwrites edits made behind it.
