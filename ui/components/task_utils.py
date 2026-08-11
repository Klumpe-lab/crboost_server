# Compatibility shim (roadmap-01 stage 3a) — everything lives in
# services/array_tasks.py now. Delete after one release; new code imports
# from services.array_tasks directly.
from services.array_tasks import (
    MANIFEST_FILENAME as MANIFEST_FILENAME,
    STATUS_DIR_NAME as STATUS_DIR_NAME,
    escape_html as escape_html,
    read_manifest as read_manifest,
    read_tail as read_tail,
    resolve_job_dir as resolve_job_dir,
    scan_statuses as scan_statuses,
    shorten_ts_names as shorten_ts_names,
    sort_ts_by_position as sort_ts_by_position,
    ts_anchor_id as ts_anchor_id,
    ts_display_name as ts_display_name,
    ts_position_sort_key as ts_position_sort_key,
    ts_pretty_name as ts_pretty_name,
)
