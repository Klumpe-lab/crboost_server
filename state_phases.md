Let my try to spell it out to you what the interactions _should_ be between main state parameters, their snapshots in each directory, the global project paramaters and the sources. We have roughly the following sources at play :

 - <US>   : UI state (completely ephemeral for now, let's not worry about it, it will be persisted separately or calculated from the pipeline state anyway)
 - <PS>   : Project state
 - <JSTAR>: job.star files
 - <PPJ>  : project_params.json
 - <DPS>  : default_pipeline.star
 - <JPJ>  : job_params.json -- this file is obsolete and we should rely completely on the monolithic "project_params.json" file for all things going forward. There is no need to shard and duplicate information there.


And we have roughly the following interactions between them:


# Configuration phase

1. Scenario: The user opens a fresh project. A job is added is added to the pipeline.
Intended effect: the project state should immediately add a new slice of state corresponding to this job type's model. It should be populated via corresponding job.star. It is free to edit. At no other point should the job.star and mdocs be in play.

2. Scenario: User edits the parameter of the job input that hasn't started ("Scheduled" status in default_pipeline.star).
Intendede Effect: The PS should get directly mutated for that partiular parameter of that particular job state.

3 Scenario: The user has edited multiple jobs that has not yet started. They press "reset to Default" or as we call it right now "Autodetect mdoc" -- all parameters that are currently in mdoc and job.star files should immediately override their values for all jobs just as at the project start. 

No project_params.json file exists at this point, all state is in memory' ProjectState class.


# Running phase

As soon as the user clicks "Run Pipeline", a snapshot of the in-memory ProjectState is created as project params as well as the scheme file. Not on "create project".

We start monitoring default_pipeline.star for the statuses of each job (Scheduled/Running/Succeeded/Failed). 
If at least one job is running the pipeline should not be able to add new jobs (we don't have that functionality yet anyway but let's earmark that).
Jobs (and their models) that are "Running", "Succeded" or "Failed" should be absolutely frozen in terms of UI and their parameter state. That is, all UI elements for them should be displayed but strictly disabled and no interaction (wehther via "Reset to Default" or user input) should alter their state slice. The job parameters (typically at the tail end of the pipeline) that are "Scheduled" should still be editable by the user. These state interactions should _immediately_ be persisted to the correct slice of _project_params.json_

# Completed/Loaded Phase

Scenario: a project is loaded from disk when its directory is specified. 

We load all the models for each job from the project_params.json, validate, and populate the in-memory state. We infer the status of each job from the default_pipeline.star. If any of the jobs is running -- all the rules in the #running_phase apply.



