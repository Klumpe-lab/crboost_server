"""The one logger that reaches the terminal at INFO.

Every module-level `logger.info()` goes to the log file only (`services/log_setup.py`). The few things
an operator actually watches the console for — projects created and deleted, pipeline started, jobs
queued, job status changes — go through `events` instead, and name their project first:
`events.info("%s · %s queued as SLURM %s", project.name, instance_id, slurm_id)`.
"""

import logging

EVENTS_LOGGER_NAME = "crboost.events"
events = logging.getLogger(EVENTS_LOGGER_NAME)
