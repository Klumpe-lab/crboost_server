
# Progress

### What Works now

- TM, candidate extraction, subtomo extraction.
- cleanup, shared the setup with Florian

# Infra
- Significantly simplified config
- Got rid of job.star templates (model-first)

# Workflow

- Dynamic job deletion and restart, dependency resolution. (pipeline order still linear)
- warp files now centralized per project, not copied from job to job


----------------------

# Next [Possible] Steps

- []     PUT IN KLUMPELAB 
- [EASY/MUST]     Visualization utilities (containerizing imod and chimerax currently)
- []     Tilt filtering (Michael's project integrate)
- []     Follow up on fastai dataloader
- []     Ingestion of new data into existing pipelines (merging new data with the old at a particular "confluence" job, without processing the old dataset)

- [MEDIUM/MUST]   Further implement Relion jobtypes (Refine, Class3D, Bayesian polish etc.) that are not in the original crboost.

- [EASY/SHOULD]   Tighter slurm monitoring
- [MEDIUM/SHOULD] Users and sessions
- [EASY/SHOULD]   Pipeline-cumulative analytics (logs and suggested parameter values)
- [MEDIUM/SHOULD] Minimal templates registry available across projects (incipient ~`pom` infra) -- stick with a simple internal doc/wiki 
- [MEDIUM/SHOULD] Provide ultimate flexibility (many instances per job type) for job scheduling, configurable inputs, "skip"-connections.

--- 

* Keep in my mind a way to mark tomograms for further template matching, masking etc...(Sven will provide a multi tomo dataset) -- asign to groups.