"""The one logger that reaches the terminal at INFO.

`setup_logging()` in main.py keeps the root at WARNING, so every module-level
`logger.info()` is diagnostic-only (visible with --debug). The few things an
operator actually watches the console for — pipeline started / finished /
failed, jobs queued, job status changes — go through `events` instead.
"""

import logging

EVENTS_LOGGER_NAME = "crboost.events"
events = logging.getLogger(EVENTS_LOGGER_NAME)
