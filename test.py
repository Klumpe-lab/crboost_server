from pathlib import Path
import starfile

project_path = "/users/artem.kushner/dev/crboost_server/projects/0_statuses_2"  # Replace with actual path
pipeline_star = Path(project_path) / "default_pipeline.star"

if pipeline_star.exists():
    data = starfile.read(pipeline_star, always_dict=True)
    print("Pipeline data keys:", list(data.keys()))
    
    processes = data.get("pipeline_processes")
    if processes is not None:
        print("Processes:")
        for _, process in processes.iterrows():
            print(f"  {process['rlnPipeLineProcessName']}: {process['rlnPipeLineProcessStatusLabel']}")