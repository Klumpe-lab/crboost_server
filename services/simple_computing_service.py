# services/simple_computing_service.py

from typing import Dict, Any

class SimpleComputingService:
    """Simple fallback computing parameters"""
    
    DEFAULT_PARAMS = {
        "importmovies": {
            "qsub_extra1": "1",    # nodes
            "qsub_extra3": "c",    # partition (CPU)
            "qsub_extra4": "0",    # gpus
            "qsub_extra5": "16G",  # memory
            "nr_threads": "4"
        },
        "fsMotionAndCtf": {
            "qsub_extra1": "1",    # nodes  
            "qsub_extra3": "g",    # partition (GPU)
            "qsub_extra4": "1",    # gpus
            "qsub_extra5": "32G",  # memory
            "nr_threads": "8"
        }
    }
    
    def get_computing_params(self, job_type: str, partition: str = "g") -> Dict[str, Any]:
        print(f"[SIMPLE COMPUTING] Getting params for {job_type}")
        params = self.DEFAULT_PARAMS.get(job_type, {
            "qsub_extra1": "1",
            "qsub_extra3": "g", 
            "qsub_extra4": "1",
            "qsub_extra5": "32G",
            "nr_threads": "8"
        })
        print(f"[SIMPLE COMPUTING] Returning: {params}")
        return params