CryoBoost runs cryo-ET pipelines (WarpTools, AreTomo, RELION, PyTOM, cryoCARE, IsoNet) on the user's SLURM cluster
from a web UI. These tools reach what that UI reaches to compose, set up and run a project, one verb per UI
action, under the same rules. Looking and judging stay in the UI: tilt review and Approve, curating picks,
judging particles, the STA kitchen. Every answer carries a `ui_url`: hand it to the user to look.

Families:
- reads, which change nothing: get_*, list_*, inspect_dataset, plan_run, plan_changes
- change plans: plan_changes, then apply_changes once the user approved the plan on its page; get_plan
- making a project: inspect_dataset, then create_project (runs in the background: get_task); or
  create_project(tomograms_from) from other projects' Reconstruct or Denoise Predict jobs, ready for template
  matching
- composing: add_job, remove_job, exclude_tilt_series, import_tomograms
- species: create_species, set_species, import_species, add_template, add_mask, make_template, make_mask
  (both in the background: get_task), select_*
- setting: set_params, set_slurm, set_input, reset_inputs, set_gain, set_tilt_filter
- running: plan_run, then run; stop, cancel_job, cancel_leftovers, predict_tilts
- deleting: delete_job (to Trash), delete_species, delete_template, delete_mask; clear_trash and delete_project
  only as plan steps

Rules:
- More than one write is one plan: plan_changes with every step, then show the user only its per-project
  lines and its ui_url, nothing else; they review and approve it on that page. Then apply_changes and follow
  get_plan. Never recap the steps as a list in the terminal.
- Never invent a value. A result with `needs` is a question for the user: what is missing, why, the options.
  Ask, then call again with the answer.
- On raw data every acquisition value comes from the user: show what inspect_dataset says the data holds,
  ask for the rest (Cs, amplitude contrast, both hands, the EER grouping, the gain reference), never fill one
  in. create_project shows the user every value before the project exists.
- plan_run comes before a single run; in a plan, the run step is planned on the page.
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
