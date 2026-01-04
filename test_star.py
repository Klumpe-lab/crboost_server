from pathlib import Path
from services.starfile_service import StarfileService
import pandas as pd

sh = StarfileService()

# Test: does it write single-row blocks correctly?
test_data = {
    "job": {
        "rlnJobTypeLabel": "relion.external",
        "rlnJobIsContinue": 0,
        "rlnJobIsTomo": 1,
    },
    "joboptions_values": pd.DataFrame([
        ("fn_exe", "echo test"),
        ("in_mic", "test.star"),
    ], columns=["rlnJobOptionVariable", "rlnJobOptionValue"])
}

sh.write(test_data, Path("/tmp/test_job.star"))
print(Path("/tmp/test_job.star").read_text())
