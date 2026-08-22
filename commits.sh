
git add services/configs/config_service.py config/conf.template.yaml containers/chimerax_artiax/curation_session.sh
git commit -m "curation: passwordless-VNC and display-bin config knobs" -m "curation.passwordless_vnc (default off) -> CX_VNC_NOPASS -> the worker starts Xvnc with -SecurityTypes None and mints no password file. curation.display_bin (default 2) selects the block-binneddisplay copy ArtiaX opens. rest_enabled is repointed: the REST channel is quarantined to launch-time health checks, not a load/save switch. Roadmap picking_ui/10-S1, 10-S3."

git add services/particles/ingest.py services/project_state.py services/particles/list_admin.py services/particles/list_ref.py services/path_resolution_service.py services/jobs/extract_pick_list.py
git commit -m "picks: one pick list per saved .coords, not one 'manual' per tomogram" -m "manual_slug_for mints manual__<stem>, so N lists saved in a session are N lists and re-saving a name updates that list (closes W1). migrate_legacy_manual_slugs runs in
ProjectState.load before pipeline_order is derived, re-keying the list, its authoritative choice, its extraction instance and any source_overrides naming the old synthetic producer; files are not renamed. pick_list_files now deletes only the .coords whose stem matches,
and takes the extraction dir from the recorded extracted_path. Roadmap picking_ui/10-S2."

git add services/visualization/artiax_bridge.py services/curation/session_service.py backend.py
git commit -m "curation: Model B - scope declared at launch, nothing drives the session after" -m "Deletes load_into_session, save_curation_picks, save_session_particle_lists, get_curation_loaded, loaded_curation_dirs and swap_chimerax_commands. In their place:
manifest.json per Curation/<species>/<tomo>/ written when the bundle is prepared and stamped launched_at at launch, scope.json beside the session merged into both find_active_curation_session*, per-stem import stars, and assign_unattributed_coords for saves that land
outside a scoped dir. Adds the block-binned display recon: cached as <recon>_cbdisp<N>.mrc, with the exact (N-1)/2*px corner shift subtracted on export and added on import, recorded per dir. Roadmap picking_ui/10-S1, 10-S2, 10-S3."

git add services/curation/watcher.py
git commit -m "curation watcher: ingest every .coords, attribute from the manifest" -m "Was newest-only per dir, collapsed onto one 'manual' slug: of N lists a user saved, N-1 disappeared. Now every settled file is its own pick list. Attribution reads the dir's manifest first and falls back to directory-slug matching only for pre-10 dirs; a manifest naming an unregistered species is reported, never guessed around. The hot set comes from manifest launched_at instead of an in-memory record of what a session was told to load, so it survives a crboost restart. Unattributed rows carry their files and clear once assigned. Roadmap picking_ui/10-S2."

git add ui/curation_session_dialog.py ui/particles/list_actions.py ui/species/picks_tab.py ui/tomo_dashboard_dialog.py
git commit -m "picks UI: control center is status-only, curate is the one scoped launch" -m "Load-into-session and Save-picks-now are gone with the backend calls behind them; picks_tab.curate lost its liveness branch. The panel gains a scope section, a warning when a
live session is on a different scope, and a confirmed 'Restart on this tomogram' - the only way to change what ArtiaX has open now that nothing re-points it. Esc closes the panel (no-backdrop-dismiss plus an on_value_change teardown). Unattributed saves gain 'assig ,with no pre-selected species. Deletes the dead _handle_open_list_in_artiax/_artiax_inputs. Roadmap picking_ui/10-S1, 10-S2."

git add docs/roadmaps/picking_ui/10-external-picker-contract.md
git commit -m "docs: roadmap 10 landing log and runtime checklist"


