
# DeepWiki Q&A with Code Context for Repository: 3dem/relion

## Q1

Hi, I'm trying to learn how to manipulate a relion tomography pipeline. 

That is, what combination of cli commands would let me achieve the ability to add jobs to a pipeline that has already ran as well as to restart that pipeline from the fresh job that i just added and more importantly, what files are necessary and in which directories and what each file and field represents to relion for these operations to work correctly and for me to be able to freely interpren them.

My vague understanding right now is that for a relion project, there are a few files like `default_pipeline.star` and `scheme.star` that are responsible for specifying the state of the pipeline at any given moment. For example here is my current project:
```

# version 50001

data_pipeline_general

_rlnPipeLineJobCounter                       6
 

# version 50001

data_pipeline_processes

loop_ 
_rlnPipeLineProcessName #1 
_rlnPipeLineProcessAlias #2 
_rlnPipeLineProcessTypeLabel #3 
_rlnPipeLineProcessStatusLabel #4 
Import/job001/       None relion.importtomo  Succeeded 
External/job002/       None relion.external  Succeeded 
External/job003/       None relion.external  Succeeded 
External/job004/       None relion.external  Succeeded 
External/job005/       None relion.external    Running 
 

# version 50001

data_pipeline_nodes

loop_ 
_rlnPipeLineNodeName #1 
_rlnPipeLineNodeTypeLabel #2 
_rlnPipeLineNodeTypeLabelDepth #3 
Import/job001/tilt_series.star TomogramGroupMetadata.star.relion.tomo.import            1 
External/job002/fs_motion_and_ctf.star MicrographGroupMetadata.star.relion            1 
External/job003/aligned_tilt_series.star MicrographGroupMetadata.star.relion            1 
External/job004/ts_ctf_tilt_series.star MicrographGroupMetadata.star.relion            1 
 

# version 50001

data_pipeline_input_edges

loop_ 
_rlnPipeLineEdgeFromNode #1 
_rlnPipeLineEdgeProcess #2 
Import/job001/tilt_series.star External/job002/ 
External/job002/fs_motion_and_ctf.star External/job003/ 
External/job003/aligned_tilt_series.star External/job004/ 
External/job004/ts_ctf_tilt_series.star External/job005/ 
 

# version 50001

data_pipeline_output_edges

loop_ 
_rlnPipeLineEdgeProcess #1 
_rlnPipeLineEdgeToNode #2 
Import/job001/ Import/job001/tilt_series.star 
External/job002/ External/job002/fs_motion_and_ctf.star 
External/job003/ External/job003/aligned_tilt_series.star 
External/job004/ External/job004/ts_ctf_tilt_series.star 
```
scheme.star:
```

# version 50001

data_scheme_general

_rlnSchemeName                       Schemes/scheme_mon0/
_rlnSchemeCurrentNodeName            tsReconstruct
 

# version 50001

data_scheme_floats

loop_ 
_rlnSchemeFloatVariableName #1 
_rlnSchemeFloatVariableValue #2 
_rlnSchemeFloatVariableResetValue #3 
do_at_most   500.000000   500.000000 
maxtime_hr    48.000000    48.000000 
  wait_sec   180.000000   180.000000 
 

# version 50001

data_scheme_operators

loop_ 
_rlnSchemeOperatorName #1 
_rlnSchemeOperatorType #2 
_rlnSchemeOperatorOutput #3 
_rlnSchemeOperatorInput1 #4 
_rlnSchemeOperatorInput2 #5 
      EXIT       exit  undefined  undefined  undefined 
EXIT_maxtime exit_maxtime  undefined maxtime_hr  undefined 
      WAIT       wait  undefined   wait_sec  undefined 
 

# version 50001

data_scheme_jobs

loop_ 
_rlnSchemeJobNameOriginal #1 
_rlnSchemeJobName #2 
_rlnSchemeJobMode #3 
_rlnSchemeJobHasStarted #4 
aligntiltsWarp External/job003/   continue            1 
denoisetrain denoisetrain   continue            0 
fsMotionAndCtf External/job002/   continue            1 
importmovies Import/job001/   continue            1 
     tsCtf External/job004/   continue            1 
tsReconstruct External/job005/   continue            1 
 

# version 50001

data_scheme_edges

loop_ 
_rlnSchemeEdgeInputNodeName #1 
_rlnSchemeEdgeOutputNodeName #2 
_rlnSchemeEdgeIsFork #3 
_rlnSchemeEdgeOutputNodeNameIfTrue #4 
_rlnSchemeEdgeBooleanVariable #5 
      WAIT EXIT_maxtime            0  undefined  undefined 
EXIT_maxtime importmovies            0  undefined  undefined 
importmovies fsMotionAndCtf            0  undefined  undefined 
fsMotionAndCtf aligntiltsWarp            0  undefined  undefined 
aligntiltsWarp      tsCtf            0  undefined  undefined 
     tsCtf tsReconstruct            0  undefined  undefined 
tsReconstruct denoisetrain            0  undefined  undefined 
denoisetrain       EXIT            0  undefined  undefined 
```

I want to know what each section means, how it changes and what should it's state be if i wanted to "add a job", what cli commands would let me achieve. I'm also a little confused as to what the aliases/jobnameoriginals refer to in relion nomenclature and how it influences where relion looks for stuff.
### Answer
# RELION Pipeline Manipulation Guide

I'll explain the structure of RELION's pipeline files and how to manipulate them programmatically.

## Understanding `default_pipeline.star`

The `default_pipeline.star` file is the core state file for your RELION pipeline. It's read and written by the `PipeLine` class: relion:1904-2131 

### File Structure Breakdown

**1. `data_pipeline_general`**
- `_rlnPipeLineJobCounter`: Tracks the next job number to assign (currently 6 in your case)
- This counter increments each time a new job is added to the pipeline relion:1976-1987 

**2. `data_pipeline_processes`**
Each row represents a job execution with:
- `_rlnPipeLineProcessName`: Directory path (e.g., `External/job002/`)
- `_rlnPipeLineProcessAlias`: Optional human-readable name (can be `None`)
- `_rlnPipeLineProcessTypeLabel`: Job type (e.g., `relion.external`, `relion.importtomo`)
- `_rlnPipeLineProcessStatusLabel`: Status (`Running`, `Scheduled`, `Succeeded`, `Failed`, `Aborted`) relion:2014-2043 

**3. `data_pipeline_nodes`**
Nodes represent data artifacts (files) with:
- `_rlnPipeLineNodeName`: File path
- `_rlnPipeLineNodeTypeLabel`: Data type identifier
- `_rlnPipeLineNodeTypeLabelDepth`: Directory depth for the `.Nodes` index (usually 1) relion:1989-2012 

**4. `data_pipeline_input_edges` and `data_pipeline_output_edges`**
These define the directed acyclic graph (DAG) of data dependencies:
- Input edges: which nodes feed into which processes
- Output edges: which processes produce which nodes relion:2065-2127 

The pipeline writes all this information using: relion:2134-2346 

## Understanding `scheme.star`

Schemes are **automated workflows** that execute jobs in sequence. Your scheme is separate from but interacts with the main pipeline: relion:666-833 

### Scheme Structure Breakdown

**1. `data_scheme_general`**
- `_rlnSchemeName`: Scheme directory path
- `_rlnSchemeCurrentNodeName`: Current execution point (e.g., `tsReconstruct`)

**2. `data_scheme_floats`, `data_scheme_bools`, `data_scheme_strings`**
Variables that can be used in job parameters with `$$variable_name` syntax relion:735-772 

**3. `data_scheme_operators`**
Control flow operations (WAIT, EXIT, file operations, etc.) relion:774-788 

**4. `data_scheme_jobs`**
Maps scheme job names to actual pipeline processes:
- `_rlnSchemeJobNameOriginal`: Original name in scheme (e.g., `aligntiltsWarp`)
- `_rlnSchemeJobName`: Actual pipeline directory (e.g., `External/job003/`)
- `_rlnSchemeJobMode`: `new` or `continue`
- `_rlnSchemeJobHasStarted`: Whether job has been executed (0 or 1) relion:790-804 

**5. `data_scheme_edges`**
Defines the execution order and conditional branches relion:807-822 

## CLI Commands to Add Jobs

### Using `relion_pipeliner`

The command-line tool for pipeline manipulation is `relion_pipeliner`: relion:29-189 

**Add a job by type:**
```bash
relion_pipeliner --addJob <job_type> --addJobOptions <options_file>
```

**Add a job from an existing job.star:**
```bash
relion_pipeliner --addJobFromStar <path/to/job.star> --addJobOptions <options_file>
```

**Set an alias:**
```bash
relion_pipeliner --addJob <job_type> --setJobAlias <alias_name>
```

The implementation shows three overloaded methods for adding scheduled jobs: relion:183-191 

### Example Workflow

To add a new job to your existing pipeline:

1. **Create a job configuration** (job.star file in a temporary directory)
2. **Add it as scheduled**:
   ```bash
   relion_pipeliner --addJobFromStar Import/my_new_job/job.star
   ```
3. **The pipeline will**:
   - Read the existing pipeline with locking
   - Increment `_rlnPipeLineJobCounter` (6 → 7)
   - Add a new process entry with status `Scheduled`
   - Create appropriate nodes and edges
   - Write back the updated pipeline relion:115-132 

## Aliases vs Job Names

This is a critical distinction:

**Process Name** (`_rlnPipeLineProcessName`):
- The actual directory path (e.g., `External/job005/`)
- Where RELION stores job files and looks for outputs
- Always follows the pattern `<JobType>/job<NNN>/`

**Alias** (`_rlnPipeLineProcessAlias`):
- Optional human-readable name
- Creates a symbolic link in your project directory
- Allows referencing jobs by name instead of number relion:2045-2062 

**Scheme Job Names**:
- `_rlnSchemeJobNameOriginal`: The label used within the scheme (e.g., `aligntiltsWarp`)
- `_rlnSchemeJobName`: The actual pipeline directory it maps to (e.g., `External/job003/`)

When a scheme runs, it translates original names to current names: relion:1686-1784 

## Key Files and Directories

**In your project root:**
- `default_pipeline.star` - Main pipeline state
- `.Nodes/` - Directory index of available outputs (for GUI browsing)
- `.relion_lock/` - Lock directory to prevent concurrent access
- `RELION_JOB_EXIT_SUCCESS` / `RELION_JOB_EXIT_FAILURE` - Job status markers

**In each job directory (e.g., `External/job005/`):**
- `job.star` - Job configuration and parameters
- `run.job` - Execution script
- `note.txt` - User notes
- Output files specific to that job relion:1910-1959 

## How Schemes Interact with the Pipeline

When you run a scheme, it:

1. **Prepares each job** by reading from the scheme directory and replacing `$$variables`
2. **Adds the job to the main pipeline** using `addScheduledJob()`
3. **Executes the job** and waits for completion
4. **Updates both scheme.star and default_pipeline.star** relion:1787-1929 

The scheme tracks which jobs have started via `_rlnSchemeJobHasStarted`, allowing it to either create new jobs or continue existing ones based on the `_rlnSchemeJobMode`.

## Notes

- The pipeline uses **file locking** (`.relion_lock` directory) to prevent corruption from concurrent access
- Job counter increments happen when new processes are added via `addNewProcess()`
- Status changes from `Running` → `Succeeded`/`Failed` are detected by checking for output files
- The `.Nodes/` directory provides a filesystem-based index for the GUI's file browser
- Schemes are designed for **automated processing** while direct pipeline manipulation is for **manual control**

To truly understand how jobs flow through your pipeline, trace the edges: an output node from one job becomes an input node to the next, forming the dependency graph that RELION uses to determine execution order and validate data availability.


## Q2
So ok let's say i wanted to build a little ui to be able to pick from a number (~10) of predefined jobs (ex. importmovies, fsmotion, aligntilts, tsReconstruct etc.), each with their job.star pre-defined and then run it. 

What exact commands should my ui dispatch (im using python so i'll just use relion_pipeliner and relion_schemer as a subprocess so you can talk to me in these temrs) when:
A new pipeline is being constructed:
- a job is added (by its name) to the state of pipeline
- a job is deleted from the state of the pipeline
A completed pipeline is loaded
- a new job is added to the state of the completed pipeline
- i "run" a pipeline that's completed but had jobs added to it

in both cases if i delete a job ( is there even a "delete" functionality)??

Can you explain to me what cli actions i should take in each case, what are their effects on eahc of default_pipeline and scheme.star and how should i start the edited pipeline for this whole thing to wokr correctly?
### Answer
## CLI Commands for RELION Pipeline Manipulation

### New Pipeline Construction

**Adding a job:**
```bash
# Add job by type with options
relion_pipeliner --addJob <job_type> --addJobOptions "option1=value1;option2=value2" --setJobAlias <alias>

# Or from pre-defined job.star
relion_pipeliner --addJobFromStar /path/to/job.star --addJobOptions "option1=value1" --setJobAlias <alias>
```
This increments `_rlnPipeLineJobCounter`, adds a process with `Scheduled` status, creates nodes/edges, and updates `default_pipeline.star` relion:115-132 relion:730-785 

**Deleting a job:**
```bash
relion_pipeliner --harsh_clean <job_number>  # Deletes job directory
relion_pipeliner --gentle_clean <job_number> # Removes intermediate files
```
This moves the job directory to `Trash/`, removes the alias symlink, updates `default_pipeline.star` by removing the process and its nodes relion:1104-1156 relion:1424-1468 

### Completed Pipeline Operations

**Adding a job to completed pipeline:**
Same commands as above - they work on any pipeline state relion:741-744 

**Running the edited pipeline:**
```bash
# Run specific jobs
relion_pipeliner --RunJobs "job001 job002 job003" --schedule <schedule_name>

# Or run all scheduled jobs
relion_pipeliner --schedule <schedule_name> --repeat 1
```
This creates `RUNNING_PIPELINER_<schedule_name>` lock file, executes jobs in dependency order, waits for completion, and updates job statuses relion:815-849 relion:227-239 

### Effects on Pipeline Files

**default_pipeline.star:**
- Job counter increments when adding jobs
- New process entries added with `Scheduled` status
- Nodes and edges updated for data flow
- Processes removed when deleting jobs
- Status changes: `Scheduled` → `Running` → `Succeeded`/`Failed`

**scheme.star:**
- NOT affected by direct `relion_pipeliner` commands
- Only modified when running schemes with `relion_schemer`
- Tracks job execution state via `_rlnSchemeJobHasStarted`
- Maps original job names to actual pipeline directories relion:788-822 relion:1787-1856 

### Starting the Edited Pipeline

1. **For manual control:** Use `relion_pipeliner --RunJobs` to execute specific jobs
2. **For automated workflows:** Create/edit a scheme and run with `relion_schemer`
3. **The pipeline will:** Check dependencies, execute jobs, monitor completion via exit marker files, and update statuses automatically relion:787-814 

## Notes

- Delete functionality exists via `--gentle_clean` and `--harsh_clean` options
- `relion_pipeliner` handles file locking automatically to prevent corruption
- Jobs can be added to pipelines in any state (new, running, or completed)
- Schemes provide automated execution but are separate from manual pipeline manipulation
- All operations update `default_pipeline.star`; `scheme.star` is only for automated workflows

Wiki pages you might want to explore:
- [Overview (3dem/relion)](/wiki/3dem/relion#1)

## Q3
Ok can you elaborate exactly on what you mean by "automated workflows" in terms of scheme.star? It's sooo goddamn fucking confusing to me still why relione chooses to have to two of these pipeline state files and why can't a fucking "defaul_pipeline.star" cannot be automated or house the same fields that scheme.star does? 

Ok, concretely, our projects wants to provide some default "schemes" like this one:
```

# version 50001

data_scheme_general

_rlnSchemeName                       Schemes/warp_tomo_prep/
_rlnSchemeCurrentNodeName            WAIT
 

# version 50001

data_scheme_floats

loop_ 
_rlnSchemeFloatVariableName #1 
_rlnSchemeFloatVariableValue #2 
_rlnSchemeFloatVariableResetValue #3 
do_at_most   500.000000   500.000000 
maxtime_hr    48.000000    48.000000 
  wait_sec   180.000000   180.000000 
  

# version 50001

data_scheme_operators

loop_ 
_rlnSchemeOperatorName #1 
_rlnSchemeOperatorType #2 
_rlnSchemeOperatorOutput #3 
_rlnSchemeOperatorInput1 #4 
_rlnSchemeOperatorInput2 #5 
EXIT       exit  undefined  undefined  undefined
EXIT_maxtime exit_maxtime  undefined maxtime_hr  undefined 
WAIT       wait  undefined   wait_sec  undefined 
 

# version 50001

data_scheme_jobs

loop_ 
_rlnSchemeJobNameOriginal #1 
_rlnSchemeJobName #2 
_rlnSchemeJobMode #3 
_rlnSchemeJobHasStarted #4 
importmovies importmovies   continue            0 
fsMotionAndCtf fsMotionAndCtf continue 0 
filtertilts  filtertilts continue 0
filtertiltsInter filtertiltsInter   continue            0
aligntiltsWarp aligntiltsWarp continue 0 
tsCtf tsCtf continue 0 
tsReconstruct tsReconstruct continue 0
denoisetrain    denoisetrain    continue        0      
denoisepredict    denoisepredict    continue        0 
templatematching  templatematching   continue      0 
tmextractcand  tmextractcand   continue      0 
subtomoExtraction subtomoExtraction continue    0

# version 50001

data_scheme_edges

loop_ 
_rlnSchemeEdgeInputNodeName #1 
_rlnSchemeEdgeOutputNodeName #2 
_rlnSchemeEdgeIsFork #3 
_rlnSchemeEdgeOutputNodeNameIfTrue #4 
_rlnSchemeEdgeBooleanVariable #5 
WAIT EXIT_maxtime            0  undefined  undefined
EXIT_maxtime importmovies            0  undefined  undefined
importmovies fsMotionAndCtf       0 undefined  undefined
fsMotionAndCtf filtertilts        0 undefined  undefined
filtertilts  filtertiltsInter     0 undefined  undefined
filtertiltsInter aligntiltsWarp           0 undefined  undefined
aligntiltsWarp    tsCtf               0 undefined  undefined
tsCtf tsReconstruct                  0 undefined  undefined
tsReconstruct  denoisetrain          0 undefined  undefined
denoisetrain    denoisepredict          0 undefined undefined
denoisepredict  templatematching              0 undefined  undefined
templatematching tmextractcand         0 undefined undefined
tmextractcand     subtomoExtraction            0 undefined undefined
subtomoExtraction    EXIT                    0 undefined  undefined
```

for the users, but also let them use their own schemes. It's not alwyas the case that a user will run all of these jobs here at once, it might be that they would only want to run importmovies to tsReconstruct for example. 

Is there any fuckign utility at all to keeping this default file around or can we recreate it on the fly every time since we know the order of jobs?
### Answer
## Why RELION Has Two Pipeline Files

RELION separates pipeline state from workflow automation for good reason:

**`default_pipeline.star`** - The **static state** of your project:
- Tracks all existing jobs and their status
- Defines data dependencies between jobs
- Persistent record of what has been executed
- Managed by `PipeLine` class relion:1904-1990 

**`scheme.star`** - The **dynamic execution plan**:
- Defines automated workflows with control flow
- Contains variables for parameter substitution (`$$var` syntax)
- Tracks execution state (`_rlnSchemeJobHasStarted`)
- Managed by `Scheme` class relion:666-833 

## Automated Workflows Explained

Schemes provide capabilities beyond static pipeline management:

1. **Variable Substitution**: Jobs can reference `$$variables` that get replaced at runtime relion:1704-1755 
2. **Conditional Logic**: Operators can check file existence, count particles, evaluate conditions relion:335-387 
3. **Control Flow**: WAIT operators, EXIT conditions, branching logic relion:61-143 
4. **State Tracking**: Remembers which jobs have started to support resuming relion:1787-1856 

## Recreating Schemes On-the-Fly

Yes, you can recreate `scheme.star` dynamically since you know the job order. Here's how:

### Minimal Scheme Structure
```bash
# Required sections for a working scheme:
data_scheme_general      # Scheme name and current node
data_scheme_jobs         # Job definitions
data_scheme_edges        # Execution flow
```

### Generating Partial Schemes
For users wanting to run only `importmovies` to `tsReconstruct`:

1. **Filter the job list**: Extract only the desired jobs from your full scheme
2. **Update edges**: Keep only edges between selected jobs
3. **Set current node**: Start at the first job (`importmovies`)
4. **Write scheme.star**: Use `relion_schemer` or write directly

The scheme system is flexible - it doesn't need all jobs defined upfront. You can generate schemes with any subset of jobs in any order relion:822-972 

### Practical Approach

```python
# Pseudocode for dynamic scheme generation
def create_scheme(job_list, start_job):
    scheme = {
        'data_scheme_general': {
            '_rlnSchemeName': 'Schemes/dynamic_run/',
            '_rlnSchemeCurrentNodeName': start_job
        },
        'data_scheme_jobs': filter_jobs(job_list),
        'data_scheme_edges': filter_edges(job_list)
    }
    write_scheme_star(scheme)
```

## Notes

- The default scheme file serves as a template but isn't required
- Schemes can be created programmatically for any workflow
- Variable substitution and operators are optional for simple linear workflows
- The separation allows manual pipeline editing without breaking automated schemes
- Your UI can generate schemes on-demand based on user job selection relion:1-120 

Wiki pages you might want to explore:
- [Overview (3dem/relion)](/wiki/3dem/relion#1)


## Q4
Ok you say `Write scheme.star: Use relion_schemer or write directly` -- how can relion_schemer help me here? I'd like to in fact generate this stuff dynamically and my workflow is quite linear currently, but i'm still having to edit this shit manually and try to fit things correctly in the starfile, which i would very much like to avoid.

### Answer
## Using `relion_schemer` for Dynamic Scheme Generation

`relion_schemer` provides several commands to manipulate schemes programmatically, avoiding manual STAR file editing:

### Key `relion_schemer` Commands

**1. Set Scheme Variables**
```bash
relion_schemer --scheme <scheme_name> --set_var <var_name> --value "<value>" --original_value "<value>"
```
Used to set float, boolean, or string variables in `data_scheme_*` sections relion:298-301 

**2. Set Job Has Started Flag**
```bash
relion_schemer --scheme <scheme_name> --set_has_started <job_name> --value False
```
Controls whether a job will be created new or continued when the scheme runs relion:291-293 

**3. Set Current Node**
```bash
relion_schemer --scheme <scheme_name> --set_current_node <node_name>
```
Sets where the scheme will start/resume execution relion:332-334 

**4. Run the Scheme**
```bash
relion_schemer --scheme <scheme_name> --run --pipeline_control Schemes/<scheme_name>/
```
Executes the scheme, creating jobs as needed relion:921-923 

### Dynamic Scheme Creation Workflow

Instead of manually editing STAR files, you can:

1. **Start with a template scheme** (like your `warp_tomo_prep`)
2. **Use `relion_schemer --set_var`** to modify variables dynamically
3. **Use `relion_schemer --set_has_started`** to control which jobs run
4. **Run the scheme** with `relion_schemer --run`

### Example: Partial Pipeline Execution

For running only `importmovies` to `tsReconstruct`:

```python
# Set has_started=False for jobs you want to run
await run_shell_command("relion_schemer --scheme warp_tomo_prep --set_has_started importmovies --value False")
await run_shell_command("relion_schemer --scheme warp_tomo_prep --set_has_started fsMotionAndCtf --value False")
await run_shell_command("relion_schemer --scheme warp_tomo_prep --set_has_started aligntiltsWarp --value False")
await run_shell_command("relion_schemer --scheme warp_tomo_prep --set_has_started tsCtf --value False")
await run_shell_command("relion_schemer --scheme warp_tomo_prep --set_has_started tsReconstruct --value False")

# Set has_started=True for jobs you want to skip
await run_shell_command("relion_schemer --scheme warp_tomo_prep --set_has_started filtertilts --value True")
await run_shell_command("relion_schemer --scheme warp_tomo_prep --set_has_started denoisetrain --value True")

# Start at the first job you want to run
await run_shell_command("relion_schemer --scheme warp_tomo_prep --set_current_node importmovies")

# Run the scheme
await run_shell_command("relion_schemer --scheme warp_tomo_prep --run --pipeline_control Schemes/warp_tomo_prep/")
```

### Why Keep Template Schemes?

The template scheme serves as:
- **Complete workflow definition** - All possible jobs and their order
- **Default parameters** - Initial variable values
- **Edge definitions** - Job dependencies and flow control

You recreate it on-the-fly by:
1. Copying the template
2. Modifying variables with `--set_var`
3. Controlling execution with `--set_has_started`
4. Setting start point with `--set_current_node`

This approach is cleaner than manual DataFrame manipulation and leverages RELION's built-in scheme management relion:880-893 

## Notes

- `relion_schemer` handles file locking automatically when modifying schemes
- The `--original_value` parameter preserves reset values for variables
- Jobs with `has_started=True` will be continued (not recreated) when the scheme runs
- Your current manual approach works but `relion_schemer` provides a cleaner API relion:285-296

## Q5
Ok tell me this. You say that i should set "set_has_started" to "True" for jobs i want to skip, but then you also say that the jobs marked true will "be continued". So which is it? I need to be able to run a subset of jobs defined in the template scheme without creating the later ones or running them at all.. 

Also what exactly does "--pipeliner_control" flag do?
### Answer
## Understanding `set_has_started` and `--pipeline_control`

### `set_has_started` Behavior

You're right to be confused - `set_has_started=True` does **NOT** skip jobs. It tells the scheme to **continue** an existing job rather than create a new one relion:1831-1846 .

To run a subset of jobs:
1. **Set `has_started=False`** for jobs you want to run (they'll be created new)
2. **Set `has_started=True`** for jobs that already exist and should be continued
3. **Use `set_current_node`** to start at the first job you want to run, effectively skipping earlier jobs

Example to run only `importmovies` to `tsReconstruct`:
```bash
# Mark jobs to run as new (has_started=False)
relion_schemer --scheme warp_tomo_prep --set_has_started importmovies --value False
relion_schemer --scheme warp_tomo_prep --set_has_started fsMotionAndCtf --value False
relion_schemer --scheme warp_tomo_prep --set_has_started tsReconstruct --value False

# Start at importmovies (skips any jobs before it)
relion_schemer --scheme warp_tomo_prep --set_current_node importmovies
```

### `--pipeline_control` Flag

The `--pipeline_control` flag specifies the directory where RELION writes job control files relion:708-718 :

- **Exit markers**: `RELION_JOB_EXIT_SUCCESS`/`RELION_JOB_EXIT_FAILURE`
- **Abort signals**: `RELION_JOB_ABORT_NOW`
- **Output logs**: `run.out` and `run.err`

When running schemes, it's set to the scheme directory so all control files are grouped together relion:921-923 :
```bash
relion_schemer --scheme prep --run --pipeline_control Schemes/prep/
```

## Notes

- `has_started=True` + `mode=continue` = continue existing job
- `has_started=False` + `mode=new` = create new job
- To skip jobs entirely, use `set_current_node` to jump past them
- The pipeline control directory is where RELION monitors job status relion:291-293 

Wiki pages you might want to explore:
- [Overview (3dem/relion)](/wiki/3dem/relion#1)

## Q6
is pipeline directory to the default_pipeline.star? are they the same directory? i want it to be my project root basically
### Answer
## Pipeline Directory vs `default_pipeline.star` Location

No, they are not the same directory:

- **`default_pipeline.star`** - Always lives in your project root. This is the state file that tracks all jobs and their relationships relion:1964-1968 

- **Pipeline directory** - Specified by `--pipeline_control` flag. This is where RELION writes runtime control files like:
  - `RELION_JOB_EXIT_SUCCESS`/`RELION_JOB_EXIT_FAILURE` markers
  - `RELION_JOB_ABORT_NOW` signals
  - `run.out` and `run.err` logs relion:708-718 

## Default Behavior

By default:
- `default_pipeline.star` is in your project root
- Control files are written in each job's output directory
- When running schemes, `--pipeline_control` is set to the scheme directory relion:921-923 

## Making Project Root the Pipeline Directory

If you want all control files in your project root:

```bash
# For individual jobs
relion_pipeliner --RunJobs "job001 job002" --pipeline_control .

# For schemes
relion_schemer --scheme my_scheme --run --pipeline_control .
```

The pipeline name itself (defaulting to "default") is just a prefix for the state file and can be changed with `--pipeline` flag relion:75-76 .

## Notes

- `default_pipeline.star` location is fixed - it's always `[project_root]/[pipeline_name]_pipeline.star`
- The pipeline control directory is flexible and specified at runtime
- Using project root as control directory simplifies monitoring but may clutter your workspace relion:1910-1912 

