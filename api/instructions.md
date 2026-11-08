CryoBoost runs cryo-ET pipelines (WarpTools, AreTomo, RELION, PyTOM, cryoCARE, IsoNet) on the user's SLURM cluster
from a web UI. These tools reach what that UI reaches to compose, set up and run a project, one verb per UI
action, under the same rules. Looking and judging stay in the UI: tilt review and Approve, curating picks,
judging particles, the STA kitchen. Every answer carries a `ui_url`: hand it to the user to look.

Families:
- reads, which change nothing: get_*, list_*, inspect_dataset, plan_run
- making a project: inspect_dataset, then create_project (runs in the background: get_task); or
  create_project(tomograms_from) from other projects' Reconstruct or Denoise Predict jobs, ready for template
  matching
- composing: add_job, remove_job, exclude_tilt_series, import_tomograms
- species: create_species, set_species, import_species, add_template, add_mask, make_template, make_mask
  (both in the background: get_task), select_*
- setting: set_params, set_slurm, set_input, reset_inputs, set_gain, set_tilt_filter
- running: plan_run, then run; stop, cancel_job, cancel_leftovers, predict_tilts

Rules:
- Never invent a value. A result with `needs` is a question for the user: what is missing, why, the options.
  Ask, then call again with the answer.
- On raw data every acquisition value comes from the user: show what inspect_dataset says the data holds,
  ask for the rest (Cs, amplitude contrast, both hands, the EER grouping, the gain reference), never fill one
  in. create_project shows the user every value before the project exists.
- plan_run comes before run; run submits what plan_run shows.
- A failure is reported, not retried: read get_job_tasks and get_job_logs, tell the user what failed and where,
  and run again only when they ask.
- A preprocessing chain gets the tilt filter unless the user says otherwise, in the mode they name; without a
  word it is manual, and the run waits at it for the user's review in the UI.
- A job that has run keeps its settings: clone it (add_job clone_of) to run it again with others, and set the
  inputs of the jobs after the clone to it (set_input).
- The API changes only the projects it made; any project can be read. For one made by hand, give the user its
  `ui_url`.
- Never edit project_params.json, and never call sbatch or scancel on a CryoBoost project: the server's
  in-memory state is the truth and overwrites edits made behind it.
