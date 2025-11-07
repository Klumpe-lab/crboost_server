# Create a quick diagnostic script
from pathlib import Path
from services.starfile_service import StarfileService

project_path = Path("/users/artem.kushner/dev/crboost_server/projects/0friday1")
scheme_path = project_path / "Schemes" / "scheme_0friday1" / "scheme.star"

star_handler = StarfileService()
scheme_data = star_handler.read(scheme_path)

print("=== SCHEME JOBS ===")
jobs_df = scheme_data.get("scheme_jobs")
if jobs_df is not None:
    for idx, row in jobs_df.iterrows():
        print(f"Job {idx}:")
        print(f"  Original: '{row['rlnSchemeJobNameOriginal']}'")
        print(f"  Name: '{row['rlnSchemeJobName']}'")
        print(f"  HasStarted: {row['rlnSchemeJobHasStarted']}")
        print(f"  Mode: {row['rlnSchemeJobMode']}")
